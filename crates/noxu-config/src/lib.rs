#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! Configuration parameter system for Noxu DB.
//!
//! Port of `com.sleepycat.je.config` - defines all configuration parameters,
//! their types, defaults, ranges, and validation logic.
//!
//! JE has ~400 configuration parameters. This module ports the configuration
//! infrastructure and the initial set of core parameters. Parameters will be
//! added incrementally as each subsystem is ported.

pub mod manager;
pub mod param;
pub mod params;

pub use manager::ConfigManager;
pub use param::{ConfigParam, ParamValue};
