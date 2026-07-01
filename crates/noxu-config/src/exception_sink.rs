// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Background-daemon exception sink (JE `ExceptionListener` substrate).
//!
//! The engine (`noxu-dbi`) spawns background daemon threads (checkpointer,
//! cleaner, evictor, INCompressor, log-flusher) that can encounter recoverable
//! errors.  Historically those errors were swallowed (`let _ = …`) or only
//! `log::warn!`-ed.  A [`DaemonExceptionSink`] lets the higher layer
//! (`noxu-db`) register a callback so an application can OBSERVE async daemon
//! failures it otherwise could not.
//!
//! This type lives in `noxu-config` — a leaf crate both `noxu-dbi` (the
//! producer, at the daemon error sites) and `noxu-db` (the consumer, which
//! adapts the public `ExceptionListener` trait) depend on — so neither has to
//! depend on the other.
//!
//! JE ref: `com.sleepycat.je.ExceptionListener`, invoked from the daemon
//! threads' catch blocks in `EnvironmentImpl`.

use std::sync::Arc;

/// A callback invoked when a background daemon thread encounters a recoverable
/// error.  Arguments: `(source, message)` where `source` names the daemon
/// (e.g. `"Cleaner"`, `"Checkpointer"`) and `message` is the error text.
///
/// Must be cheap and non-panicking: it runs on the daemon thread.
pub type DaemonExceptionSink = Arc<dyn Fn(&str, &str) + Send + Sync>;

/// A shared, late-bindable slot holding an optional [`DaemonExceptionSink`].
///
/// The engine creates this before spawning daemons and hands each daemon a
/// clone; the higher layer installs the sink via [`set`](Self::set) right
/// after construction (before the daemons perform any work, which they do only
/// after their first sleep interval).  Reads on the daemon error path take a
/// short read lock and clone the `Arc`, so dispatch never blocks a producer.
#[derive(Clone, Default)]
pub struct ExceptionDispatcher {
    sink: Arc<std::sync::RwLock<Option<DaemonExceptionSink>>>,
}

impl std::fmt::Debug for ExceptionDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let installed = self.sink.read().map(|g| g.is_some()).unwrap_or(false);
        f.debug_struct("ExceptionDispatcher")
            .field("installed", &installed)
            .finish()
    }
}

impl ExceptionDispatcher {
    /// Create an empty dispatcher (no sink installed).
    pub fn new() -> Self {
        Self::default()
    }

    /// Install (or replace) the sink.
    pub fn set(&self, sink: DaemonExceptionSink) {
        if let Ok(mut g) = self.sink.write() {
            *g = Some(sink);
        }
    }

    /// Returns `true` if a sink is installed.
    pub fn is_installed(&self) -> bool {
        self.sink.read().map(|g| g.is_some()).unwrap_or(false)
    }

    /// Dispatch a `(source, message)` event to the installed sink, if any.
    ///
    /// A no-op when no sink is installed.  Called from daemon error sites.
    pub fn dispatch(&self, source: &str, message: &str) {
        // Clone the Arc under a short read lock, then release before calling
        // the user callback so a slow callback never holds the slot lock.
        let sink = self.sink.read().ok().and_then(|g| g.clone());
        if let Some(sink) = sink {
            sink(source, message);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn dispatch_no_sink_is_noop() {
        let d = ExceptionDispatcher::new();
        assert!(!d.is_installed());
        d.dispatch("Cleaner", "boom"); // must not panic
    }

    #[test]
    fn dispatch_invokes_installed_sink() {
        let d = ExceptionDispatcher::new();
        let count = Arc::new(AtomicUsize::new(0));
        let last = Arc::new(std::sync::Mutex::new(String::new()));
        let c2 = count.clone();
        let l2 = last.clone();
        d.set(Arc::new(move |source: &str, msg: &str| {
            c2.fetch_add(1, Ordering::SeqCst);
            *l2.lock().unwrap() = format!("{source}:{msg}");
        }));
        assert!(d.is_installed());
        d.dispatch("Checkpointer", "disk full");
        d.dispatch("Cleaner", "io error");
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert_eq!(&*last.lock().unwrap(), "Cleaner:io error");
    }
}
