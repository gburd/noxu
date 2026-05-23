//! Cask — a Redis RESP2-compatible key-value store powered by Noxu DB.

use noxu_cask::{CaskConfig, CaskServer};
use std::path::Path;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Load config from `config.toml` in the current directory, or use defaults.
    let config =
        CaskConfig::from_file(Path::new("config.toml"))?.unwrap_or_default();

    info!(
        address = %config.address,
        data_dir = %config.data_dir.display(),
        max_connections = config.max_connections,
        "Starting Cask server"
    );

    let server = CaskServer::new(config)?;

    // Run server with graceful shutdown on Ctrl-C.
    tokio::select! {
        result = server.run() => {
            if let Err(e) = result {
                tracing::error!("Server error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl-C, shutting down");
        }
    }

    Ok(())
}
