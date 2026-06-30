// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Periodic metrics exporter (opt-in, requires the `observability` feature).
//!
//! This is the Rust analogue of BDB-JE's read-only JMX MBean export: a
//! background daemon samples [`Environment::get_stats`] on a fixed interval
//! and publishes every field to the [`metrics`](https://docs.rs/metrics)
//! facade via [`noxu_observe::export::emit`]. Whichever recorder the
//! application installed (Prometheus, StatsD, OpenTelemetry, …) then collects
//! them; with no recorder installed the facade calls are cheap no-ops.
//!
//! Because it samples the same snapshot `get_stats()` returns, it covers the
//! entire stat set that snapshot exposes without touching any hot path.
//!
//! ```no_run
//! # #[cfg(feature = "observability")]
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use std::sync::Arc;
//! use std::time::Duration;
//! use noxu_db::environment::Environment;
//! use noxu_db::metrics_export::MetricsExporter;
//!
//! # let config = unimplemented!();
//! let env = Arc::new(Environment::open(config)?);
//! // Install any `metrics` recorder first, then:
//! let exporter = MetricsExporter::start(env.clone(), Duration::from_secs(10));
//! // ... run workload ...
//! exporter.stop();
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "observability"))]
//! # fn main() {}
//! ```

use crate::environment::Environment;
use noxu_util::daemon::DaemonThread;
use std::sync::Arc;
use std::time::Duration;

/// A handle to the background metrics-sampling daemon.
///
/// Dropping the handle signals shutdown without blocking; call [`stop`] to
/// join the thread.
///
/// [`stop`]: MetricsExporter::stop
pub struct MetricsExporter {
    daemon: DaemonThread,
}

impl MetricsExporter {
    /// Describe Noxu's metrics, then spawn a daemon that samples
    /// `env.get_stats()` every `interval` and emits to the `metrics` facade.
    ///
    /// Call after installing a recorder. If the environment is closed the
    /// daemon stops on the next sample.
    pub fn start(env: Arc<Environment>, interval: Duration) -> Self {
        noxu_observe::export::describe_export_metrics();
        let daemon =
            DaemonThread::spawn("noxu-metrics-export", interval, move || {
                match env.get_stats() {
                    Ok(stats) => {
                        noxu_observe::export::emit(&stats);
                        true
                    }
                    // Environment closed/invalid: stop sampling.
                    Err(_) => false,
                }
            });
        MetricsExporter { daemon }
    }

    /// Emit one sample immediately (in the calling thread). Useful in tests
    /// and for a synchronous scrape just before reading a recorder.
    pub fn sample_once(env: &Environment) {
        if let Ok(stats) = env.get_stats() {
            noxu_observe::export::emit(&stats);
        }
    }

    /// Signal shutdown and join the daemon thread.
    pub fn stop(self) {
        self.daemon.shutdown();
    }
}
