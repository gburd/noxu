//! TCP server and command dispatch.

use crate::config::CaskConfig;
use crate::resp::{RespValue, parse_resp};
use crate::store::{CaskStore, StoreError};

use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tracing::{info, warn};

/// The Cask server.
pub struct CaskServer {
    config: CaskConfig,
    store: Arc<CaskStore>,
    /// Limits the number of concurrent connections.
    connection_limit: Arc<Semaphore>,
}

impl CaskServer {
    /// Create a new server from configuration.
    pub fn new(config: CaskConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let store = CaskStore::open(&config.data_dir)?;
        let connection_limit = Arc::new(Semaphore::new(config.max_connections));
        Ok(Self { config, store: Arc::new(store), connection_limit })
    }

    /// Run the server, accepting connections until shutdown is signaled.
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(&self.config.address).await?;
        info!("Cask listening on {}", self.config.address);

        loop {
            let permit = self.connection_limit.clone().acquire_owned().await?;
            let (socket, addr) = listener.accept().await?;
            let store = Arc::clone(&self.store);

            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, store).await {
                    warn!("Connection from {} error: {}", addr, e);
                }
                drop(permit);
            });
        }
    }
}

/// Handle a single client connection.
async fn handle_connection(
    mut socket: TcpStream,
    store: Arc<CaskStore>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = BytesMut::with_capacity(4096);
    let mut multi_queue: Option<Vec<QueuedCommand>> = None;

    loop {
        // Try to parse a complete frame from the buffer.
        if let Some(frame) = parse_resp(&mut buf) {
            let response = match &multi_queue {
                Some(_) if is_exec_or_discard(&frame) => {
                    handle_multi_control(&frame, &mut multi_queue, &store)
                }
                Some(_) => {
                    // Queue the command.
                    if let Some(cmd) = parse_command(&frame) {
                        if cmd.name == "MULTI" {
                            RespValue::error(
                                "ERR MULTI calls can not be nested",
                            )
                        } else {
                            multi_queue.as_mut().unwrap().push(cmd);
                            RespValue::simple("QUEUED")
                        }
                    } else {
                        RespValue::error("ERR invalid command in MULTI")
                    }
                }
                None => dispatch_command(&frame, &store, &mut multi_queue),
            };

            let out = response.serialize();
            socket.write_all(&out).await?;

            // Check for QUIT.
            if is_quit(&frame) {
                return Ok(());
            }
            continue;
        }

        // Need more data.
        let n = socket.read_buf(&mut buf).await?;
        if n == 0 {
            // Connection closed by client.
            return Ok(());
        }
    }
}

/// A parsed command ready for queuing in MULTI mode.
struct QueuedCommand {
    name: String,
    args: Vec<Bytes>,
}

/// Parse a RESP frame into a command name and arguments.
fn parse_command(frame: &RespValue) -> Option<QueuedCommand> {
    let parts = match frame {
        RespValue::Array(parts) if !parts.is_empty() => parts,
        _ => return None,
    };

    let name = match &parts[0] {
        RespValue::BulkString(Some(b)) => {
            String::from_utf8_lossy(b).to_uppercase()
        }
        _ => return None,
    };

    let args: Vec<Bytes> = parts[1..]
        .iter()
        .filter_map(|v| match v {
            RespValue::BulkString(Some(b)) => Some(b.clone()),
            _ => None,
        })
        .collect();

    Some(QueuedCommand { name, args })
}

/// Check if the frame is EXEC or DISCARD.
fn is_exec_or_discard(frame: &RespValue) -> bool {
    if let Some(cmd) = parse_command(frame) {
        matches!(cmd.name.as_str(), "EXEC" | "DISCARD")
    } else {
        false
    }
}

/// Check if the frame is QUIT.
fn is_quit(frame: &RespValue) -> bool {
    if let Some(cmd) = parse_command(frame) {
        cmd.name == "QUIT"
    } else {
        false
    }
}

/// Handle EXEC and DISCARD when in MULTI mode.
fn handle_multi_control(
    frame: &RespValue,
    multi_queue: &mut Option<Vec<QueuedCommand>>,
    store: &Arc<CaskStore>,
) -> RespValue {
    let cmd = match parse_command(frame) {
        Some(c) => c,
        None => return RespValue::error("ERR protocol error"),
    };

    match cmd.name.as_str() {
        "DISCARD" => {
            *multi_queue = None;
            RespValue::ok()
        }
        "EXEC" => {
            let commands = multi_queue.take().unwrap_or_default();
            exec_transaction(commands, store)
        }
        _ => RespValue::error("ERR unexpected command in MULTI context"),
    }
}

/// Execute all queued commands in a single Noxu transaction.
fn exec_transaction(
    commands: Vec<QueuedCommand>,
    store: &Arc<CaskStore>,
) -> RespValue {
    // Begin a transaction.
    let txn = match store.begin_transaction() {
        Ok(t) => t,
        Err(e) => {
            return RespValue::error(format!(
                "ERR transaction begin failed: {e}"
            ));
        }
    };

    let mut results: Vec<RespValue> = Vec::with_capacity(commands.len());

    for cmd in &commands {
        let result = execute_in_txn(cmd, store, &txn);
        results.push(result);
    }

    // Check if any command produced an error that should abort.
    // For Redis semantics, EXEC commits even if individual commands error,
    // so we always commit.
    match txn.commit() {
        Ok(()) => RespValue::Array(results),
        Err(e) => {
            RespValue::error(format!("ERR transaction commit failed: {e}"))
        }
    }
}

/// Execute a single command within a transaction context.
fn execute_in_txn(
    cmd: &QueuedCommand,
    store: &Arc<CaskStore>,
    txn: &noxu::Transaction,
) -> RespValue {
    match cmd.name.as_str() {
        "SET" => {
            if cmd.args.len() < 2 {
                return RespValue::error(
                    "ERR wrong number of arguments for 'SET' command",
                );
            }
            match store.set_in_txn(txn, &cmd.args[0], &cmd.args[1]) {
                Ok(()) => RespValue::ok(),
                Err(e) => RespValue::error(format!("ERR {e}")),
            }
        }
        "DEL" => {
            if cmd.args.is_empty() {
                return RespValue::error(
                    "ERR wrong number of arguments for 'DEL' command",
                );
            }
            let mut count = 0i64;
            for key in &cmd.args {
                match store.del_in_txn(txn, key) {
                    Ok(true) => count += 1,
                    Ok(false) => {}
                    Err(e) => return RespValue::error(format!("ERR {e}")),
                }
            }
            RespValue::integer(count)
        }
        // For commands that don't write, just execute them normally outside
        // the transaction. This matches Redis behavior where reads in MULTI
        // see the state at read time (not transactional snapshot isolation).
        _ => execute_command(&cmd.name, &cmd.args, store),
    }
}

/// Dispatch a single command frame.
fn dispatch_command(
    frame: &RespValue,
    store: &Arc<CaskStore>,
    multi_queue: &mut Option<Vec<QueuedCommand>>,
) -> RespValue {
    let cmd = match parse_command(frame) {
        Some(c) => c,
        None => return RespValue::error("ERR invalid command format"),
    };

    // Handle MULTI specially.
    if cmd.name == "MULTI" {
        *multi_queue = Some(Vec::new());
        return RespValue::ok();
    }

    execute_command(&cmd.name, &cmd.args, store)
}

/// Execute a command and return the response.
fn execute_command(
    name: &str,
    args: &[Bytes],
    store: &Arc<CaskStore>,
) -> RespValue {
    match name {
        "PING" => {
            if args.is_empty() {
                RespValue::simple("PONG")
            } else {
                RespValue::bulk(args[0].clone())
            }
        }
        "ECHO" => {
            if args.is_empty() {
                return RespValue::error(
                    "ERR wrong number of arguments for 'echo' command",
                );
            }
            RespValue::bulk(args[0].clone())
        }
        "SET" => {
            if args.len() < 2 {
                return RespValue::error(
                    "ERR wrong number of arguments for 'set' command",
                );
            }
            match store.set(&args[0], &args[1]) {
                Ok(()) => RespValue::ok(),
                Err(e) => RespValue::error(format!("ERR {e}")),
            }
        }
        "GET" => {
            if args.is_empty() {
                return RespValue::error(
                    "ERR wrong number of arguments for 'get' command",
                );
            }
            match store.get(&args[0]) {
                Ok(Some(val)) => RespValue::bulk(val),
                Ok(None) => RespValue::null(),
                Err(e) => RespValue::error(format!("ERR {e}")),
            }
        }
        "DEL" => {
            if args.is_empty() {
                return RespValue::error(
                    "ERR wrong number of arguments for 'del' command",
                );
            }
            let mut count = 0i64;
            for key in args {
                match store.del(key) {
                    Ok(true) => count += 1,
                    Ok(false) => {}
                    Err(e) => return RespValue::error(format!("ERR {e}")),
                }
            }
            RespValue::integer(count)
        }
        "MSET" => {
            if args.len() < 2 || !args.len().is_multiple_of(2) {
                return RespValue::error(
                    "ERR wrong number of arguments for 'mset' command",
                );
            }
            let pairs: Vec<(Bytes, Bytes)> = args
                .chunks(2)
                .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
                .collect();
            match store.mset(&pairs) {
                Ok(()) => RespValue::ok(),
                Err(e) => RespValue::error(format!("ERR {e}")),
            }
        }
        "MGET" => {
            if args.is_empty() {
                return RespValue::error(
                    "ERR wrong number of arguments for 'mget' command",
                );
            }
            match store.mget(args) {
                Ok(values) => {
                    let items: Vec<RespValue> = values
                        .into_iter()
                        .map(|v| match v {
                            Some(data) => RespValue::bulk(data),
                            None => RespValue::null(),
                        })
                        .collect();
                    RespValue::array(items)
                }
                Err(e) => RespValue::error(format!("ERR {e}")),
            }
        }
        "INCR" => {
            if args.is_empty() {
                return RespValue::error(
                    "ERR wrong number of arguments for 'incr' command",
                );
            }
            match store.incr(&args[0], 1) {
                Ok(val) => RespValue::integer(val),
                Err(StoreError::NotAnInteger) => RespValue::error(
                    "ERR value is not an integer or out of range",
                ),
                Err(StoreError::Overflow) => RespValue::error(
                    "ERR increment or decrement would overflow",
                ),
                Err(e) => RespValue::error(format!("ERR {e}")),
            }
        }
        "INCRBY" => {
            if args.len() < 2 {
                return RespValue::error(
                    "ERR wrong number of arguments for 'incrby' command",
                );
            }
            let by = match std::str::from_utf8(&args[1])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(n) => n,
                None => {
                    return RespValue::error(
                        "ERR value is not an integer or out of range",
                    );
                }
            };
            match store.incr(&args[0], by) {
                Ok(val) => RespValue::integer(val),
                Err(StoreError::NotAnInteger) => RespValue::error(
                    "ERR value is not an integer or out of range",
                ),
                Err(StoreError::Overflow) => RespValue::error(
                    "ERR increment or decrement would overflow",
                ),
                Err(e) => RespValue::error(format!("ERR {e}")),
            }
        }
        "DECR" => {
            if args.is_empty() {
                return RespValue::error(
                    "ERR wrong number of arguments for 'decr' command",
                );
            }
            match store.incr(&args[0], -1) {
                Ok(val) => RespValue::integer(val),
                Err(StoreError::NotAnInteger) => RespValue::error(
                    "ERR value is not an integer or out of range",
                ),
                Err(StoreError::Overflow) => RespValue::error(
                    "ERR increment or decrement would overflow",
                ),
                Err(e) => RespValue::error(format!("ERR {e}")),
            }
        }
        "DECRBY" => {
            if args.len() < 2 {
                return RespValue::error(
                    "ERR wrong number of arguments for 'decrby' command",
                );
            }
            let by = match std::str::from_utf8(&args[1])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(n) => n,
                None => {
                    return RespValue::error(
                        "ERR value is not an integer or out of range",
                    );
                }
            };
            match store.incr(&args[0], -by) {
                Ok(val) => RespValue::integer(val),
                Err(StoreError::NotAnInteger) => RespValue::error(
                    "ERR value is not an integer or out of range",
                ),
                Err(StoreError::Overflow) => RespValue::error(
                    "ERR increment or decrement would overflow",
                ),
                Err(e) => RespValue::error(format!("ERR {e}")),
            }
        }
        "APPEND" => {
            if args.len() < 2 {
                return RespValue::error(
                    "ERR wrong number of arguments for 'append' command",
                );
            }
            match store.append(&args[0], &args[1]) {
                Ok(len) => RespValue::integer(len as i64),
                Err(e) => RespValue::error(format!("ERR {e}")),
            }
        }
        "EXISTS" => {
            if args.is_empty() {
                return RespValue::error(
                    "ERR wrong number of arguments for 'exists' command",
                );
            }
            let mut count = 0i64;
            for key in args {
                match store.exists(key) {
                    Ok(true) => count += 1,
                    Ok(false) => {}
                    Err(e) => return RespValue::error(format!("ERR {e}")),
                }
            }
            RespValue::integer(count)
        }
        "RENAME" => {
            if args.len() < 2 {
                return RespValue::error(
                    "ERR wrong number of arguments for 'rename' command",
                );
            }
            match store.rename(&args[0], &args[1]) {
                Ok(true) => RespValue::ok(),
                Ok(false) => RespValue::error("ERR no such key"),
                Err(e) => RespValue::error(format!("ERR {e}")),
            }
        }
        "KEYS" => {
            if args.is_empty() {
                return RespValue::error(
                    "ERR wrong number of arguments for 'keys' command",
                );
            }
            match store.keys(&args[0]) {
                Ok(keys) => {
                    let items: Vec<RespValue> =
                        keys.into_iter().map(RespValue::bulk).collect();
                    RespValue::array(items)
                }
                Err(e) => RespValue::error(format!("ERR {e}")),
            }
        }
        "DBSIZE" => match store.dbsize() {
            Ok(n) => RespValue::integer(n as i64),
            Err(e) => RespValue::error(format!("ERR {e}")),
        },
        "INFO" => {
            let section = args.first().map(|b| b.as_ref());
            let _ = section; // We return full info regardless of section for simplicity.
            RespValue::bulk(Bytes::from(store.info()))
        }
        "COMMAND" => {
            // redis-cli sends COMMAND DOCS on connect; return empty array.
            RespValue::array(vec![])
        }
        "QUIT" => RespValue::ok(),
        _ => RespValue::error(format!("ERR unknown command '{name}'")),
    }
}
