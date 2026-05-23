use std::path::PathBuf;
use std::process;

use tracing_subscriber::EnvFilter;

use noxu_cash::store::CashStore;
use noxu_cash::{CashConfig, CashServer};

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Load configuration
    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.toml"));

    let config = match CashConfig::from_file(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "error loading config from {}: {e}",
                config_path.display()
            );
            process::exit(1);
        }
    };

    tracing::info!(
        address = %config.address,
        data_dir = %config.data_dir.display(),
        cache_size = config.cache_size,
        max_connections = config.max_connections,
        "starting cash server"
    );

    // Open the store
    let store = match CashStore::open(&config) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error opening store: {e}");
            process::exit(1);
        }
    };

    // Set up shutdown broadcast
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    // Spawn signal handler
    let signal_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                tracing::info!("received Ctrl-C, initiating shutdown");
                let _ = signal_tx.send(());
            }
            Err(e) => {
                tracing::error!("failed to listen for Ctrl-C: {e}");
            }
        }
    });

    // Run the server
    let server = CashServer::new(store.clone(), config);
    if let Err(e) = server.run(shutdown_rx).await {
        tracing::error!("server error: {e}");
        process::exit(1);
    }

    // Clean shutdown
    drop(shutdown_tx);
    store.shutdown();
    tracing::info!("cash server stopped");
}
