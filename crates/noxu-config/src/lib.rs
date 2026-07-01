#![forbid(unsafe_code)]
// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT
#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! > **Internal component of the [`noxu`](https://crates.io/crates/noxu) database.**
//! >
//! > This crate is published only so the `noxu` umbrella crate can depend on it.
//! > Use `noxu` (`noxu = "7"`) in applications; depend on this crate directly only
//! > if you are extending the engine internals. Its API may change without a major
//! > version bump.
//!
//! Configuration parameter system for Noxu DB.
//!
//! Configuration parameter system — defines all configuration parameters,
//! their types, defaults, ranges, and validation logic.
//!
//! Approximately 165 configuration parameters are defined here covering
//! environment, logging, locking, replication, and background daemon tuning.

pub mod exception_sink;
pub mod manager;
pub mod param;
pub mod params;

pub use exception_sink::{DaemonExceptionSink, ExceptionDispatcher};
pub use manager::ConfigManager;
pub use param::{ConfigParam, ParamValue};
