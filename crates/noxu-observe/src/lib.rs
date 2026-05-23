//! Optional observability integration for Noxu DB.
//!
//! Enable with `features = ["observability"]` on the `noxu-db` crate.
//! Uses the [`metrics`] crate facade — install any compatible recorder
//! (prometheus, statsd, etc.) in your application to collect metrics.
//! Uses the [`tracing`] crate for structured spans and events.
//!
//! When the `observability` feature is disabled on `noxu-db`, all
//! instrumentation compiles away to zero-cost no-ops.

pub use metrics;
pub use tracing;

#[cfg(feature = "otel")]
pub use opentelemetry;
#[cfg(feature = "otel")]
pub use tracing_opentelemetry;

// ─── Metrics helpers ─────────────────────────────────────────────────────────

/// Describe all Noxu DB metrics. Call once at application startup after
/// installing a metrics recorder.
pub fn describe_metrics() {
    use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

    describe_counter!(
        "noxu_db_operations_total",
        Unit::Count,
        "Total database operations (labels: op={get,put,delete,commit,abort})"
    );
    describe_histogram!(
        "noxu_db_operation_duration_seconds",
        Unit::Seconds,
        "Duration of database operations (labels: op=...)"
    );
    describe_counter!(
        "noxu_db_cache_hit_total",
        Unit::Count,
        "Total cache hits"
    );
    describe_counter!(
        "noxu_db_cache_miss_total",
        Unit::Count,
        "Total cache misses"
    );
    describe_histogram!(
        "noxu_db_lock_wait_duration_seconds",
        Unit::Seconds,
        "Time spent waiting to acquire locks"
    );
    describe_counter!(
        "noxu_db_lock_deadlocks_total",
        Unit::Count,
        "Total deadlocks detected"
    );
    describe_histogram!(
        "noxu_db_fsync_duration_seconds",
        Unit::Seconds,
        "Duration of fsync operations"
    );
    describe_histogram!(
        "noxu_db_fsync_batch_size",
        Unit::Count,
        "Number of log entries coalesced per fsync"
    );
    describe_counter!(
        "noxu_db_log_bytes_written_total",
        Unit::Bytes,
        "Total bytes written to the write-ahead log"
    );
    describe_gauge!(
        "noxu_db_active_transactions",
        Unit::Count,
        "Number of currently active transactions"
    );
    describe_counter!(
        "noxu_db_cleaner_runs_total",
        Unit::Count,
        "Total cleaner daemon runs"
    );
    describe_counter!(
        "noxu_db_evictor_evictions_total",
        Unit::Count,
        "Total pages evicted from the cache"
    );
}

// ─── Macros for conditional instrumentation ──────────────────────────────────
//
// These are exported for use within noxu-db. When the `observability` feature
// is disabled, the noxu-db crate uses stub macros that expand to nothing.

/// Increment a counter metric.
#[macro_export]
macro_rules! counter_inc {
    ($name:expr, $labels:expr) => {
        ::metrics::counter!($name, $labels).increment(1);
    };
    ($name:expr, $labels:expr, $val:expr) => {
        ::metrics::counter!($name, $labels).increment($val);
    };
}

/// Record a histogram value.
#[macro_export]
macro_rules! histogram_record {
    ($name:expr, $labels:expr, $val:expr) => {
        ::metrics::histogram!($name, $labels).record($val);
    };
}

/// Increment a gauge.
#[macro_export]
macro_rules! gauge_inc {
    ($name:expr) => {
        ::metrics::gauge!($name).increment(1.0);
    };
}

/// Decrement a gauge.
#[macro_export]
macro_rules! gauge_dec {
    ($name:expr) => {
        ::metrics::gauge!($name).decrement(1.0);
    };
}
