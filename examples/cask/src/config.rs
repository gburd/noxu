//! Configuration for the Cask server.

use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Server configuration loaded from a TOML file.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CaskConfig {
    /// Listen address (host:port).
    pub address: String,
    /// Directory for Noxu DB data files.
    pub data_dir: PathBuf,
    /// Maximum number of concurrent client connections.
    pub max_connections: usize,
}

impl Default for CaskConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:6379".to_string(),
            data_dir: PathBuf::from("./cask_data"),
            max_connections: 1024,
        }
    }
}

impl CaskConfig {
    /// Load configuration from a TOML file.
    ///
    /// Returns `None` if the file does not exist; returns an error if the file
    /// exists but cannot be parsed.
    pub fn from_file(
        path: &Path,
    ) -> Result<Option<Self>, Box<dyn std::error::Error>> {
        if !path.exists() {
            return Ok(None);
        }
        let contents = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&contents)?;
        Ok(Some(config))
    }
}
