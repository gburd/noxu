use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Configuration for the Cash server.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CashConfig {
    /// Address to bind the TCP listener to.
    pub address: String,

    /// Directory for Noxu DB storage files.
    pub data_dir: PathBuf,

    /// Maximum number of entries in the in-memory LRU cache.
    pub cache_size: usize,

    /// Maximum number of concurrent client connections.
    pub max_connections: usize,
}

impl Default for CashConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:11211".to_string(),
            data_dir: PathBuf::from("./cash_data"),
            cache_size: 65536,
            max_connections: 1024,
        }
    }
}

impl CashConfig {
    /// Load configuration from a TOML file. Returns defaults if the file does not exist.
    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let contents =
            std::fs::read_to_string(path).map_err(ConfigError::Io)?;
        let config: Self =
            toml::from_str(&contents).map_err(ConfigError::Parse)?;
        Ok(config)
    }
}

/// Errors that can occur while loading configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),

    #[error("failed to parse config file: {0}")]
    Parse(#[from] toml::de::Error),
}
