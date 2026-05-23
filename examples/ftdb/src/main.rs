//! FTDB — TigerBeetle-compatible financial transactions database server.

use clap::{Parser, Subcommand};
use noxu_ftdb::{
    Account, AccountFlags, Engine, Header, Operation, Server, Storage, Transfer,
};
use std::path::PathBuf;
use std::process;

#[derive(Parser)]
#[command(
    name = "ftdb",
    about = "TigerBeetle-compatible financial transactions database"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new database.
    Format {
        #[arg(long)]
        file: PathBuf,
    },

    /// Start the FTDB server.
    Start {
        /// Database directory.
        #[arg(long)]
        file: PathBuf,

        /// Listen address (host:port).
        #[arg(long, default_value = "127.0.0.1:3000")]
        address: String,

        /// Maximum concurrent client connections.
        #[arg(long, default_value_t = 256)]
        max_connections: usize,
    },

    /// Run the built-in benchmark.
    Benchmark {
        /// Server address to benchmark.
        #[arg(long, default_value = "127.0.0.1:3000")]
        address: String,

        /// Number of accounts to create.
        #[arg(long, default_value_t = 1000)]
        accounts: u32,

        /// Number of transfers to execute.
        #[arg(long, default_value_t = 100_000)]
        transfers: u32,

        /// Batch size for each request.
        #[arg(long, default_value_t = 8190)]
        batch_size: u32,
    },

    /// Create an account (CLI convenience).
    CreateAccount {
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        id: u128,
        #[arg(long)]
        ledger: u32,
        #[arg(long, default_value_t = 1)]
        code: u16,
        #[arg(long, default_value_t = 0)]
        balance: u128,
    },

    /// Query account balance (CLI convenience).
    Balance {
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        id: u128,
    },
}

fn main() {
    let cli = Cli::parse();

    if let Err(e) = run(cli) {
        eprintln!("error: {e}");
        process::exit(1);
    }
}

fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Commands::Format { file } => {
            std::fs::create_dir_all(&file)?;
            let _storage = Storage::open(&file)?;
            println!("Initialized database at {}", file.display());
        }

        Commands::Start { file, address, max_connections } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| {
                            tracing_subscriber::EnvFilter::new("info")
                        }),
                )
                .init();

            let storage = Storage::open(&file)?;
            let engine = Engine::new(storage);
            let server = Server::new(engine, address, max_connections);

            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async {
                let (shutdown_tx, shutdown_rx) =
                    tokio::sync::broadcast::channel::<()>(1);

                tokio::spawn(async move {
                    let _ = tokio::signal::ctrl_c().await;
                    tracing::info!("received Ctrl-C");
                    let _ = shutdown_tx.send(());
                });

                server.run(shutdown_rx).await
            })?;
        }

        Commands::Benchmark { address, accounts, transfers, batch_size } => {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(run_benchmark(
                address, accounts, transfers, batch_size,
            ))?;
        }

        Commands::CreateAccount { file, id, ledger, code, balance } => {
            let storage = Storage::open(&file)?;
            let engine = Engine::new(storage);
            let mut acct = Account::new(id, ledger);
            acct.code = code;
            if balance > 0 {
                acct.credits_posted = balance;
            }
            let results = engine.create_accounts(&[acct])?;
            if results.is_empty() {
                println!("Created account {id} on ledger {ledger}");
            } else {
                eprintln!(
                    "Failed to create account: result code {}",
                    results[0].result
                );
                process::exit(1);
            }
        }

        Commands::Balance { file, id } => {
            let storage = Storage::open(&file)?;
            let engine = Engine::new(storage);
            let accounts = engine.lookup_accounts(&[id])?;
            match accounts.first() {
                Some(acct) => {
                    println!("Account {id}:");
                    println!("  ledger:          {}", acct.ledger);
                    println!("  debits_pending:  {}", acct.debits_pending);
                    println!("  debits_posted:   {}", acct.debits_posted);
                    println!("  credits_pending: {}", acct.credits_pending);
                    println!("  credits_posted:  {}", acct.credits_posted);
                    println!("  balance:         {}", acct.balance());
                    println!("  available:       {}", acct.available_balance());
                }
                None => {
                    eprintln!("Account {id} not found");
                    process::exit(1);
                }
            }
        }
    }

    Ok(())
}

/// Built-in benchmark client.
async fn run_benchmark(
    address: String,
    num_accounts: u32,
    num_transfers: u32,
    batch_size: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::time::Instant;
    use tokio::net::TcpStream;

    println!("FTDB Benchmark");
    println!("  server:     {address}");
    println!("  accounts:   {num_accounts}");
    println!("  transfers:  {num_transfers}");
    println!("  batch_size: {batch_size}");
    println!();

    let mut stream = TcpStream::connect(&address).await?;

    // Phase 1: Create accounts
    let t0 = Instant::now();
    let mut created = 0u32;
    while created < num_accounts {
        let batch_end = (created + batch_size).min(num_accounts);
        let batch_count = batch_end - created;

        let mut body = Vec::with_capacity(batch_count as usize * 128);
        for i in created..batch_end {
            let id = (i + 1) as u128;
            let mut acct = Account::new(id, 1);
            acct.code = 1;
            acct.credits_posted = u128::MAX / 2; // pre-fund all accounts
            acct.flags = AccountFlags(0);
            body.extend_from_slice(&acct.to_bytes());
        }

        let header =
            Header::new(Operation::CreateAccounts, created, batch_count);
        send_request(&mut stream, &header, &body).await?;
        let (_resp_header, _resp_body) = recv_response(&mut stream).await?;
        created = batch_end;
    }
    let accounts_elapsed = t0.elapsed();
    println!(
        "create_accounts: {} accounts in {:.1}ms ({:.0} accounts/s)",
        num_accounts,
        accounts_elapsed.as_secs_f64() * 1000.0,
        num_accounts as f64 / accounts_elapsed.as_secs_f64()
    );

    // Phase 2: Create transfers
    let t1 = Instant::now();
    let mut created_transfers = 0u32;
    while created_transfers < num_transfers {
        let batch_end = (created_transfers + batch_size).min(num_transfers);
        let batch_count = batch_end - created_transfers;

        let mut body = Vec::with_capacity(batch_count as usize * 128);
        for i in created_transfers..batch_end {
            let id = (i + 1) as u128;
            let debit = ((i % num_accounts) + 1) as u128;
            let credit = (((i + 1) % num_accounts) + 1) as u128;
            let t = Transfer::new(id, debit, credit, 1);
            body.extend_from_slice(&t.to_bytes());
        }

        let header = Header::new(
            Operation::CreateTransfers,
            created_transfers,
            batch_count,
        );
        send_request(&mut stream, &header, &body).await?;
        let (_resp_header, _resp_body) = recv_response(&mut stream).await?;
        created_transfers = batch_end;
    }
    let transfers_elapsed = t1.elapsed();
    println!(
        "create_transfers: {} transfers in {:.1}ms ({:.0} transfers/s)",
        num_transfers,
        transfers_elapsed.as_secs_f64() * 1000.0,
        num_transfers as f64 / transfers_elapsed.as_secs_f64()
    );

    // Phase 3: Lookup accounts
    let t2 = Instant::now();
    let lookup_count = num_accounts.min(batch_size);
    let mut id_body = Vec::with_capacity(lookup_count as usize * 16);
    for i in 0..lookup_count {
        id_body.extend_from_slice(&((i + 1) as u128).to_le_bytes());
    }
    let header = Header::new(Operation::LookupAccounts, 0, lookup_count);
    send_request(&mut stream, &header, &id_body).await?;
    let (_resp_header, resp_body) = recv_response(&mut stream).await?;
    let lookup_elapsed = t2.elapsed();
    let found = resp_body.len() / 128;
    println!(
        "lookup_accounts: {} found in {:.1}ms ({:.0} lookups/s)",
        found,
        lookup_elapsed.as_secs_f64() * 1000.0,
        found as f64 / lookup_elapsed.as_secs_f64()
    );

    let total = t0.elapsed();
    println!();
    println!(
        "Total: {:.1}ms | {:.0} transfers/s throughput",
        total.as_secs_f64() * 1000.0,
        num_transfers as f64 / transfers_elapsed.as_secs_f64()
    );

    Ok(())
}

async fn send_request(
    stream: &mut tokio::net::TcpStream,
    header: &noxu_ftdb::Header,
    body: &[u8],
) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;

    let size = (noxu_ftdb::Header::SIZE + body.len()) as u32;
    stream.write_all(&size.to_le_bytes()).await?;

    let mut header_bytes = header.to_bytes();
    let checksum = noxu_ftdb::protocol::checksum(body);
    header_bytes[12..16].copy_from_slice(&checksum.to_le_bytes());
    stream.write_all(&header_bytes).await?;

    if !body.is_empty() {
        stream.write_all(body).await?;
    }
    stream.flush().await
}

async fn recv_response(
    stream: &mut tokio::net::TcpStream,
) -> std::io::Result<(noxu_ftdb::Header, Vec<u8>)> {
    use tokio::io::AsyncReadExt;

    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let msg_size = u32::from_le_bytes(size_buf) as usize;

    let mut header_buf = [0u8; 32];
    stream.read_exact(&mut header_buf).await?;
    let header = noxu_ftdb::Header::from_bytes(&header_buf);

    let body_size = msg_size - 32;
    let mut body = vec![0u8; body_size];
    if body_size > 0 {
        stream.read_exact(&mut body).await?;
    }

    Ok((header, body))
}
