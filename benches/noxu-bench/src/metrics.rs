//! Metrics collection helpers for the noxu workload benchmark.
//!
//! All reads are Linux-specific (/proc/self/status and /proc/self/io).
//! On non-Linux platforms every function returns 0 rather than panicking.

/// Snapshot of per-run performance metrics.
#[derive(Debug, Default)]
pub struct Metrics {
    pub elapsed_ms: f64,
    pub ns_per_op: f64,
    pub ops_per_sec: f64,
    /// RSS after workload minus RSS before workload (kilobytes).
    pub rss_delta_kb: i64,
    /// Bytes read from storage during the workload (kilobytes, from /proc/self/io).
    pub read_bytes_kb: u64,
    /// Bytes written to storage during the workload (kilobytes, from /proc/self/io).
    pub write_bytes_kb: u64,
    /// Total size of the data directory after the workload (kilobytes).
    pub disk_kb: u64,
}

/// Read VmRSS from /proc/self/status and return the value in kilobytes.
///
/// Returns 0 on any parse error or on non-Linux systems.
pub fn rss_kb() -> i64 {
    #[cfg(target_os = "linux")]
    {
        let text = match std::fs::read_to_string("/proc/self/status") {
            Ok(t) => t,
            Err(_) => return 0,
        };
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                // Format: "VmRSS:    12345 kB"
                let digits: String = rest
                    .split_whitespace()
                    .next()
                    .unwrap_or("0")
                    .chars()
                    .filter(|c| c.is_ascii_digit())
                    .collect();
                return digits.parse().unwrap_or(0);
            }
        }
        0
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

/// Read (read_bytes, write_bytes) from /proc/self/io and return them in bytes.
///
/// Returns (0, 0) on any parse error or on non-Linux systems.
pub fn proc_io() -> (u64, u64) {
    #[cfg(target_os = "linux")]
    {
        let text = match std::fs::read_to_string("/proc/self/io") {
            Ok(t) => t,
            Err(_) => return (0, 0),
        };
        let mut read_bytes: u64 = 0;
        let mut write_bytes: u64 = 0;
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("read_bytes:") {
                read_bytes = rest.trim().parse().unwrap_or(0);
            } else if let Some(rest) = line.strip_prefix("write_bytes:") {
                write_bytes = rest.trim().parse().unwrap_or(0);
            }
        }
        (read_bytes, write_bytes)
    }
    #[cfg(not(target_os = "linux"))]
    {
        (0, 0)
    }
}

/// Recursively sum the sizes of all files under `path` (in kilobytes).
///
/// Directories themselves contribute 0; only regular file sizes are summed.
/// Returns 0 on any I/O error.
pub fn dir_size_kb(path: &std::path::Path) -> u64 {
    let mut total_bytes: u64 = 0;
    dir_size_recursive(path, &mut total_bytes);
    total_bytes / 1024
}

fn dir_size_recursive(path: &std::path::Path, total: &mut u64) {
    let entries = match std::fs::read_dir(path) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let meta = match entry.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };
        if meta.is_dir() {
            dir_size_recursive(&entry.path(), total);
        } else {
            *total += meta.len();
        }
    }
}
