//! Metrics collection helpers for the noxu workload benchmark.
//!
//! All reads are Linux-specific (/proc/self/status, /proc/self/io, /proc/self/stat).
//! On non-Linux platforms every function returns 0 rather than panicking.

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

/// Read process CPU time (user + system) from /proc/self/stat, in milliseconds.
///
/// Fields 14 (utime) and 15 (stime) are in jiffies (USER_HZ = 100), so
/// each jiffy = 10 ms.  Returns 0 on any parse error or non-Linux.
pub fn cpu_time_ms() -> u64 {
    #[cfg(target_os = "linux")]
    {
        let text = match std::fs::read_to_string("/proc/self/stat") {
            Ok(t) => t,
            Err(_) => return 0,
        };
        // The comm field (field 2) may contain spaces; skip past the last ')'.
        let after_comm = match text.rfind(')') {
            Some(pos) => &text[pos + 2..],
            None => return 0,
        };
        // After ')': state(1) ppid(2) ... utime is index 11, stime is index 12
        let fields: Vec<&str> = after_comm.split_whitespace().collect();
        if fields.len() < 14 {
            return 0;
        }
        let utime: u64 = fields[11].parse().unwrap_or(0);
        let stime: u64 = fields[12].parse().unwrap_or(0);
        (utime + stime) * 10 // jiffies → ms (USER_HZ = 100)
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
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
