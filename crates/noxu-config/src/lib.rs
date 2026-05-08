#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! Configuration parameter system for Noxu DB.
//!
//! Configuration parameter system — defines all configuration parameters,
//! their types, defaults, ranges, and validation logic.
//!
//! Approximately 400 configuration parameters are defined here covering
//! environment, logging, locking, replication, and background daemon tuning.

pub mod manager;
pub mod param;
pub mod params;

pub use manager::ConfigManager;
pub use param::{ConfigParam, ParamValue};
