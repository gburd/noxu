// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT
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
