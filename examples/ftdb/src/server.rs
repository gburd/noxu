//! TCP server for the FTDB wire protocol.

use crate::account::Account;
use crate::engine::Engine;
use crate::error::FtdbError;
use crate::protocol::{self, Header, MAX_BATCH_SIZE, Operation};
use crate::transfer::Transfer;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

/// FTDB TCP server.
pub struct Server {
    engine: Arc<Engine>,
    address: String,
    max_connections: usize,
}

impl Server {
    pub fn new(
        engine: Engine,
        address: String,
        max_connections: usize,
    ) -> Self {
        Self { engine: Arc::new(engine), address, max_connections }
    }

    /// Runs the server until a shutdown signal is received.
    pub async fn run(
        &self,
        mut shutdown: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), FtdbError> {
        let listener = TcpListener::bind(&self.address).await?;
        let semaphore = Arc::new(Semaphore::new(self.max_connections));

        tracing::info!(address = %self.address, "FTDB server listening");

        loop {
            tokio::select! {
                accept = listener.accept() => {
                    let (stream, addr) = accept?;
                    let engine = Arc::clone(&self.engine);
                    let permit = semaphore.clone().acquire_owned().await.unwrap();

                    tracing::debug!(%addr, "client connected");

                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, engine).await {
                            tracing::debug!(%addr, error = %e, "connection closed");
                        }
                        drop(permit);
                    });
                }
                _ = shutdown.recv() => {
                    tracing::info!("shutting down server");
                    break;
                }
            }
        }

        Ok(())
    }
}

/// Handles a single client connection.
async fn handle_connection(
    mut stream: TcpStream,
    engine: Arc<Engine>,
) -> Result<(), FtdbError> {
    loop {
        // Read message size (4 bytes)
        let mut size_buf = [0u8; 4];
        if stream.read_exact(&mut size_buf).await.is_err() {
            return Ok(()); // Clean disconnect
        }
        let msg_size = u32::from_le_bytes(size_buf) as usize;

        if msg_size < Header::SIZE {
            return Err(FtdbError::Protocol("message too small".into()));
        }
        if msg_size > Header::SIZE + (MAX_BATCH_SIZE as usize * 128) {
            return Err(FtdbError::Protocol("message too large".into()));
        }

        // Read header
        let mut header_buf = [0u8; 32];
        stream.read_exact(&mut header_buf).await?;
        let header = Header::from_bytes(&header_buf);

        // Read body
        let body_size = msg_size - Header::SIZE;
        let mut body = vec![0u8; body_size];
        if body_size > 0 {
            stream.read_exact(&mut body).await?;
        }

        // Verify checksum
        if header.checksum != 0 {
            let computed = protocol::checksum(&body);
            if computed != header.checksum {
                return Err(FtdbError::Protocol("checksum mismatch".into()));
            }
        }

        // Dispatch
        let operation = match Operation::from_u8(header.operation) {
            Some(op) => op,
            None => {
                return Err(FtdbError::Protocol(format!(
                    "unknown operation: {}",
                    header.operation
                )));
            }
        };

        let response_body =
            dispatch(&engine, operation, &body, header.batch_count)?;

        // Send response
        let resp_batch_count = match operation {
            Operation::CreateAccounts | Operation::CreateTransfers => {
                (response_body.len() / 8) as u32
            }
            Operation::LookupAccounts | Operation::LookupTransfers => {
                (response_body.len() / 128) as u32
            }
        };

        let resp_header =
            Header::response(operation, header.request_id, resp_batch_count, 0);

        let resp_size = (Header::SIZE + response_body.len()) as u32;
        stream.write_all(&resp_size.to_le_bytes()).await?;

        let mut resp_header_bytes = resp_header.to_bytes();
        // Set checksum on response
        let resp_checksum = protocol::checksum(&response_body);
        resp_header_bytes[12..16].copy_from_slice(&resp_checksum.to_le_bytes());

        stream.write_all(&resp_header_bytes).await?;
        if !response_body.is_empty() {
            stream.write_all(&response_body).await?;
        }
        stream.flush().await?;
    }
}

/// Dispatches a request to the engine and returns the response body bytes.
fn dispatch(
    engine: &Engine,
    operation: Operation,
    body: &[u8],
    batch_count: u32,
) -> Result<Vec<u8>, FtdbError> {
    match operation {
        Operation::CreateAccounts => {
            let accounts = parse_accounts(body, batch_count as usize)?;
            let results = engine.create_accounts(&accounts)?;
            let mut out = Vec::with_capacity(results.len() * 8);
            for r in results {
                out.extend_from_slice(&r.to_bytes());
            }
            Ok(out)
        }
        Operation::CreateTransfers => {
            let transfers = parse_transfers(body, batch_count as usize)?;
            let results = engine.create_transfers(&transfers)?;
            let mut out = Vec::with_capacity(results.len() * 8);
            for r in results {
                out.extend_from_slice(&r.to_bytes());
            }
            Ok(out)
        }
        Operation::LookupAccounts => {
            let ids = parse_ids(body, batch_count as usize)?;
            let accounts = engine.lookup_accounts(&ids)?;
            let mut out = Vec::with_capacity(accounts.len() * 128);
            for a in accounts {
                out.extend_from_slice(&a.to_bytes());
            }
            Ok(out)
        }
        Operation::LookupTransfers => {
            let ids = parse_ids(body, batch_count as usize)?;
            let transfers = engine.lookup_transfers(&ids)?;
            let mut out = Vec::with_capacity(transfers.len() * 128);
            for t in transfers {
                out.extend_from_slice(&t.to_bytes());
            }
            Ok(out)
        }
    }
}

fn parse_accounts(
    body: &[u8],
    count: usize,
) -> Result<Vec<Account>, FtdbError> {
    if body.len() != count * 128 {
        return Err(FtdbError::Protocol(
            "body size mismatch for accounts".into(),
        ));
    }
    let mut accounts = Vec::with_capacity(count);
    for i in 0..count {
        let chunk: &[u8; 128] =
            body[i * 128..(i + 1) * 128].try_into().unwrap();
        accounts.push(Account::from_bytes(chunk));
    }
    Ok(accounts)
}

fn parse_transfers(
    body: &[u8],
    count: usize,
) -> Result<Vec<Transfer>, FtdbError> {
    if body.len() != count * 128 {
        return Err(FtdbError::Protocol(
            "body size mismatch for transfers".into(),
        ));
    }
    let mut transfers = Vec::with_capacity(count);
    for i in 0..count {
        let chunk: &[u8; 128] =
            body[i * 128..(i + 1) * 128].try_into().unwrap();
        transfers.push(Transfer::from_bytes(chunk));
    }
    Ok(transfers)
}

fn parse_ids(body: &[u8], count: usize) -> Result<Vec<u128>, FtdbError> {
    if body.len() != count * 16 {
        return Err(FtdbError::Protocol("body size mismatch for IDs".into()));
    }
    let mut ids = Vec::with_capacity(count);
    for i in 0..count {
        let chunk: [u8; 16] = body[i * 16..(i + 1) * 16].try_into().unwrap();
        ids.push(u128::from_le_bytes(chunk));
    }
    Ok(ids)
}
