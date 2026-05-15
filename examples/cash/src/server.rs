use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

use crate::config::CashConfig;
use crate::protocol::{Command, ParseResult, complete_storage, parse_command_line};
use crate::store::{CasResult, CashStore};

/// The Cash TCP server.
pub struct CashServer {
    store: Arc<CashStore>,
    config: CashConfig,
}

impl CashServer {
    /// Create a new server with the given store and config.
    pub fn new(store: Arc<CashStore>, config: CashConfig) -> Self {
        Self { store, config }
    }

    /// Run the server, listening for connections until the shutdown signal fires.
    pub async fn run(self, mut shutdown: tokio::sync::broadcast::Receiver<()>) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.config.address).await?;
        tracing::info!("cash listening on {}", self.config.address);

        let semaphore = Arc::new(Semaphore::new(self.config.max_connections));
        let store = self.store.clone();

        // Spawn background TTL sweeper
        let sweep_store = store.clone();
        let sweep_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
            loop {
                interval.tick().await;
                let flushed = sweep_store.flush_expired();
                if flushed > 0 {
                    tracing::debug!("flushed {flushed} expired entries");
                }
            }
        });

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    let (socket, addr) = accept_result?;
                    let permit = match semaphore.clone().try_acquire_owned() {
                        Ok(p) => p,
                        Err(_) => {
                            tracing::warn!("max connections reached, rejecting {addr}");
                            drop(socket);
                            continue;
                        }
                    };

                    let conn_store = store.clone();
                    conn_store.stats.curr_connections.fetch_add(1, Ordering::Relaxed);
                    conn_store.stats.total_connections.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!("accepted connection from {addr}");

                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(socket, conn_store.clone()).await {
                            tracing::debug!("connection {addr} closed: {e}");
                        }
                        conn_store.stats.curr_connections.fetch_sub(1, Ordering::Relaxed);
                        drop(permit);
                    });
                }
                _ = shutdown.recv() => {
                    tracing::info!("shutdown signal received, stopping server");
                    break;
                }
            }
        }

        sweep_handle.abort();
        Ok(())
    }
}

/// Handle a single client connection.
async fn handle_connection(mut socket: TcpStream, store: Arc<CashStore>) -> std::io::Result<()> {
    let mut buf = BytesMut::with_capacity(4096);

    loop {
        // Read until we have a complete line (\r\n)
        let line = match read_line(&mut socket, &mut buf).await? {
            Some(line) => line,
            None => return Ok(()), // Connection closed
        };

        store
            .stats
            .bytes_read
            .fetch_add(line.len() as u64 + 2, Ordering::Relaxed);

        let result = parse_command_line(&line);

        match result {
            ParseResult::Complete(cmd) => {
                let response = execute_command(cmd, &store).await?;
                if let Some(resp) = response {
                    store
                        .stats
                        .bytes_written
                        .fetch_add(resp.len() as u64, Ordering::Relaxed);
                    socket.write_all(resp.as_bytes()).await?;
                    if resp.is_empty() {
                        // quit
                        return Ok(());
                    }
                }
            }
            ParseResult::NeedData(pending) => {
                let data = read_data_block(&mut socket, &mut buf, pending.bytes).await?;
                store
                    .stats
                    .bytes_read
                    .fetch_add(data.len() as u64 + 2, Ordering::Relaxed);

                let cmd = complete_storage(pending, data);
                let response = execute_command(cmd, &store).await?;
                if let Some(resp) = response {
                    store
                        .stats
                        .bytes_written
                        .fetch_add(resp.len() as u64, Ordering::Relaxed);
                    socket.write_all(resp.as_bytes()).await?;
                }
            }
            ParseResult::Error(err_msg) => {
                socket.write_all(err_msg.as_bytes()).await?;
            }
        }
    }
}

/// Execute a parsed command, returning the response string (None means close connection).
async fn execute_command(
    cmd: Command,
    store: &Arc<CashStore>,
) -> std::io::Result<Option<String>> {
    match cmd {
        Command::Get { keys } => {
            let results = store.get(&keys);
            let mut response = String::new();
            for (key, flags, _cas, data) in &results {
                let key_str = String::from_utf8_lossy(key);
                response.push_str(&format!("VALUE {} {} {}\r\n", key_str, flags, data.len()));
                // Data as raw bytes — we need to handle this carefully
                response.push_str(&String::from_utf8_lossy(data));
                response.push_str("\r\n");
            }
            response.push_str("END\r\n");
            Ok(Some(response))
        }

        Command::Gets { keys } => {
            let results = store.get(&keys);
            let mut response = String::new();
            for (key, flags, cas, data) in &results {
                let key_str = String::from_utf8_lossy(key);
                response.push_str(&format!(
                    "VALUE {} {} {} {}\r\n",
                    key_str,
                    flags,
                    data.len(),
                    cas
                ));
                response.push_str(&String::from_utf8_lossy(data));
                response.push_str("\r\n");
            }
            response.push_str("END\r\n");
            Ok(Some(response))
        }

        Command::Set {
            key,
            flags,
            exptime,
            data,
            noreply,
        } => {
            match store.set(&key, flags, exptime, &data) {
                Ok(()) => {
                    if noreply {
                        Ok(Some(String::new()))
                    } else {
                        Ok(Some("STORED\r\n".into()))
                    }
                }
                Err(e) => {
                    tracing::error!("set error: {e}");
                    Ok(Some(format!("SERVER_ERROR {e}\r\n")))
                }
            }
        }

        Command::Add {
            key,
            flags,
            exptime,
            data,
            noreply,
        } => {
            match store.add(&key, flags, exptime, &data) {
                Ok(true) => {
                    if noreply {
                        Ok(Some(String::new()))
                    } else {
                        Ok(Some("STORED\r\n".into()))
                    }
                }
                Ok(false) => {
                    if noreply {
                        Ok(Some(String::new()))
                    } else {
                        Ok(Some("NOT_STORED\r\n".into()))
                    }
                }
                Err(e) => {
                    tracing::error!("add error: {e}");
                    Ok(Some(format!("SERVER_ERROR {e}\r\n")))
                }
            }
        }

        Command::Replace {
            key,
            flags,
            exptime,
            data,
            noreply,
        } => {
            match store.replace(&key, flags, exptime, &data) {
                Ok(true) => {
                    if noreply {
                        Ok(Some(String::new()))
                    } else {
                        Ok(Some("STORED\r\n".into()))
                    }
                }
                Ok(false) => {
                    if noreply {
                        Ok(Some(String::new()))
                    } else {
                        Ok(Some("NOT_STORED\r\n".into()))
                    }
                }
                Err(e) => {
                    tracing::error!("replace error: {e}");
                    Ok(Some(format!("SERVER_ERROR {e}\r\n")))
                }
            }
        }

        Command::Append {
            key,
            flags: _,
            exptime: _,
            data,
            noreply,
        } => {
            match store.append(&key, &data) {
                Ok(true) => {
                    if noreply {
                        Ok(Some(String::new()))
                    } else {
                        Ok(Some("STORED\r\n".into()))
                    }
                }
                Ok(false) => {
                    if noreply {
                        Ok(Some(String::new()))
                    } else {
                        Ok(Some("NOT_STORED\r\n".into()))
                    }
                }
                Err(e) => {
                    tracing::error!("append error: {e}");
                    Ok(Some(format!("SERVER_ERROR {e}\r\n")))
                }
            }
        }

        Command::Prepend {
            key,
            flags: _,
            exptime: _,
            data,
            noreply,
        } => {
            match store.prepend(&key, &data) {
                Ok(true) => {
                    if noreply {
                        Ok(Some(String::new()))
                    } else {
                        Ok(Some("STORED\r\n".into()))
                    }
                }
                Ok(false) => {
                    if noreply {
                        Ok(Some(String::new()))
                    } else {
                        Ok(Some("NOT_STORED\r\n".into()))
                    }
                }
                Err(e) => {
                    tracing::error!("prepend error: {e}");
                    Ok(Some(format!("SERVER_ERROR {e}\r\n")))
                }
            }
        }

        Command::Cas {
            key,
            flags,
            exptime,
            data,
            cas_token,
            noreply,
        } => {
            match store.cas(&key, flags, exptime, &data, cas_token) {
                Ok(CasResult::Stored) => {
                    if noreply {
                        Ok(Some(String::new()))
                    } else {
                        Ok(Some("STORED\r\n".into()))
                    }
                }
                Ok(CasResult::Exists) => {
                    if noreply {
                        Ok(Some(String::new()))
                    } else {
                        Ok(Some("EXISTS\r\n".into()))
                    }
                }
                Ok(CasResult::NotFound) => {
                    if noreply {
                        Ok(Some(String::new()))
                    } else {
                        Ok(Some("NOT_FOUND\r\n".into()))
                    }
                }
                Err(e) => {
                    tracing::error!("cas error: {e}");
                    Ok(Some(format!("SERVER_ERROR {e}\r\n")))
                }
            }
        }

        Command::Delete { key, noreply } => {
            match store.delete(&key) {
                Ok(true) => {
                    if noreply {
                        Ok(Some(String::new()))
                    } else {
                        Ok(Some("DELETED\r\n".into()))
                    }
                }
                Ok(false) => {
                    if noreply {
                        Ok(Some(String::new()))
                    } else {
                        Ok(Some("NOT_FOUND\r\n".into()))
                    }
                }
                Err(e) => {
                    tracing::error!("delete error: {e}");
                    Ok(Some(format!("SERVER_ERROR {e}\r\n")))
                }
            }
        }

        Command::Incr {
            key,
            value,
            noreply,
        } => match store.incr(&key, value) {
            Ok(Some(new_val)) => {
                if noreply {
                    Ok(Some(String::new()))
                } else {
                    Ok(Some(format!("{new_val}\r\n")))
                }
            }
            Ok(None) => {
                if noreply {
                    Ok(Some(String::new()))
                } else {
                    Ok(Some("NOT_FOUND\r\n".into()))
                }
            }
            Err(e) => {
                tracing::error!("incr error: {e}");
                Ok(Some(format!("SERVER_ERROR {e}\r\n")))
            }
        },

        Command::Decr {
            key,
            value,
            noreply,
        } => match store.decr(&key, value) {
            Ok(Some(new_val)) => {
                if noreply {
                    Ok(Some(String::new()))
                } else {
                    Ok(Some(format!("{new_val}\r\n")))
                }
            }
            Ok(None) => {
                if noreply {
                    Ok(Some(String::new()))
                } else {
                    Ok(Some("NOT_FOUND\r\n".into()))
                }
            }
            Err(e) => {
                tracing::error!("decr error: {e}");
                Ok(Some(format!("SERVER_ERROR {e}\r\n")))
            }
        },

        Command::Stats => {
            let lines = store.stats_lines();
            let mut response = String::new();
            for (name, value) in lines {
                response.push_str(&format!("STAT {name} {value}\r\n"));
            }
            response.push_str("END\r\n");
            Ok(Some(response))
        }

        Command::FlushAll { noreply } => {
            let _ = store.flush_all();
            if noreply {
                Ok(Some(String::new()))
            } else {
                Ok(Some("OK\r\n".into()))
            }
        }

        Command::Quit => {
            // Return empty string to signal close
            Ok(None)
        }
    }
}

/// Read bytes until \r\n is found, returning the line without the delimiter.
async fn read_line(
    socket: &mut TcpStream,
    buf: &mut BytesMut,
) -> std::io::Result<Option<Vec<u8>>> {
    loop {
        // Check if we already have a complete line in the buffer
        if let Some(pos) = find_crlf(buf) {
            let line = buf[..pos].to_vec();
            // Consume the line + \r\n
            let _ = buf.split_to(pos + 2);
            return Ok(Some(line));
        }

        // Read more data
        let n = socket.read_buf(buf).await?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            // Partial line at EOF — treat as a line
            let line = buf.to_vec();
            buf.clear();
            return Ok(Some(line));
        }
    }
}

/// Read exactly `count` bytes of data plus trailing \r\n.
async fn read_data_block(
    socket: &mut TcpStream,
    buf: &mut BytesMut,
    count: usize,
) -> std::io::Result<Vec<u8>> {
    let needed = count + 2; // data + \r\n
    while buf.len() < needed {
        let n = socket.read_buf(buf).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed while reading data block",
            ));
        }
    }

    let data = buf[..count].to_vec();
    // Consume data + \r\n
    let _ = buf.split_to(needed);
    Ok(data)
}

/// Find the position of \r\n in the buffer.
fn find_crlf(buf: &[u8]) -> Option<usize> {
    if buf.len() < 2 {
        return None;
    }
    (0..buf.len() - 1).find(|&i| buf[i] == b'\r' && buf[i + 1] == b'\n')
}
