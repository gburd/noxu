//! Compile-time observability stubs.
//!
//! When the `observability` feature is enabled, these macros emit real
//! metrics and tracing spans. When disabled, they compile to nothing.
//!
//! This is a deliberate, complete macro family: a few members
//! (`observe_counter_add`, `observe_histogram`) are not yet called from
//! anywhere in the engine but are kept so the set is uniform and ready to
//! use. Hence the scoped `unused_macros` allow (preferred over a
//! crate-wide allow — review P2-2).
#![allow(unused_macros)]

/// Increment a counter metric. No-op when `observability` is disabled.
macro_rules! observe_counter {
    ($name:expr, $($label_key:expr => $label_val:expr),* $(,)?) => {
        #[cfg(feature = "observability")]
        {
            ::metrics::counter!($name, $($label_key => $label_val),*).increment(1);
        }
        #[cfg(not(feature = "observability"))]
        {
            $(let _ = ($label_key, $label_val);)*
            let _ = $name;
        }
    };
}

/// Increment a counter by a given value. No-op when `observability` is disabled.
macro_rules! observe_counter_add {
    ($name:expr, $val:expr, $($label_key:expr => $label_val:expr),* $(,)?) => {
        #[cfg(feature = "observability")]
        {
            ::metrics::counter!($name, $($label_key => $label_val),*).increment($val);
        }
        #[cfg(not(feature = "observability"))]
        {
            $(let _ = ($label_key, $label_val);)*
            let _ = ($name, $val);
        }
    };
}

/// Record a histogram value. No-op when `observability` is disabled.
macro_rules! observe_histogram {
    ($name:expr, $val:expr, $($label_key:expr => $label_val:expr),* $(,)?) => {
        #[cfg(feature = "observability")]
        {
            ::metrics::histogram!($name, $($label_key => $label_val),*).record($val);
        }
        #[cfg(not(feature = "observability"))]
        {
            $(let _ = ($label_key, $label_val);)*
            let _ = ($name, $val);
        }
    };
}

/// Increment a gauge by 1. No-op when `observability` is disabled.
macro_rules! observe_gauge_inc {
    ($name:expr) => {
        #[cfg(feature = "observability")]
        {
            ::metrics::gauge!($name).increment(1.0);
        }
        #[cfg(not(feature = "observability"))]
        {
            let _ = $name;
        }
    };
}

/// Decrement a gauge by 1. No-op when `observability` is disabled.
macro_rules! observe_gauge_dec {
    ($name:expr) => {
        #[cfg(feature = "observability")]
        {
            ::metrics::gauge!($name).decrement(1.0);
        }
        #[cfg(not(feature = "observability"))]
        {
            let _ = $name;
        }
    };
}

/// Create a tracing span. No-op when `observability` is disabled.
/// Returns an `Option<tracing::span::EnteredSpan>` when enabled, `()` when disabled.
macro_rules! observe_span {
    ($name:expr, $($field_key:ident = $field_val:expr),* $(,)?) => {
        #[cfg(feature = "observability")]
        let _obs_span = {
            let _span = ::tracing::info_span!($name, $($field_key = $field_val),*);
            Some(_span.entered())
        };
        #[cfg(not(feature = "observability"))]
        let _obs_span: () = {
            $(let _ = $field_val;)*
            let _ = $name;
        };
    };
}

/// Start a timer. Returns `Option<std::time::Instant>` when enabled, `()` when disabled.
macro_rules! observe_timer_start {
    () => {{
        #[cfg(feature = "observability")]
        {
            Some(std::time::Instant::now())
        }
        #[cfg(not(feature = "observability"))]
        {
            None::<std::time::Instant>
        }
    }};
}

/// Record elapsed time from a timer into a histogram. No-op when disabled.
macro_rules! observe_timer_record {
    ($timer:expr, $name:expr, $($label_key:expr => $label_val:expr),* $(,)?) => {
        #[cfg(feature = "observability")]
        {
            if let Some(start) = $timer {
                let elapsed = start.elapsed().as_secs_f64();
                ::metrics::histogram!($name, $($label_key => $label_val),*).record(elapsed);
            }
        }
        #[cfg(not(feature = "observability"))]
        {
            let _ = ($timer, $name);
            $(let _ = ($label_key, $label_val);)*
        }
    };
}
