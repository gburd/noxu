//! Small utility functions for tree operations.
//!
//! Port of `com.sleepycat.je.tree.TreeUtils` from JE.

/// Creates an indentation string of N spaces.
///
/// Used for pretty-printing tree structures and debugging output.
///
/// # Arguments
/// * `n_spaces` - The number of spaces to include in the indentation
///
/// # Returns
/// A String containing `n_spaces` space characters
///
/// # Examples
/// ```
/// use noxu_tree::tree_utils::indent;
///
/// assert_eq!(indent(0), "");
/// assert_eq!(indent(4), "    ");
/// assert_eq!(indent(10), "          ");
/// ```
pub fn indent(n_spaces: usize) -> String {
    " ".repeat(n_spaces)
}

/// Returns a truncated view of a byte slice for display purposes.
///
/// If the slice is longer than `max_len`, it is truncated and "..." is appended.
///
/// # Arguments
/// * `data` - The byte slice to format
/// * `max_len` - Maximum number of bytes to display
///
/// # Returns
/// A String representation of the byte slice
pub fn format_bytes(data: &[u8], max_len: usize) -> String {
    if data.len() <= max_len {
        format!("{:?}", data)
    } else {
        format!("{:?}...", &data[..max_len])
    }
}

/// Converts a byte slice to a hex string for display.
///
/// # Arguments
/// * `data` - The byte slice to convert
///
/// # Returns
/// A hex string representation (e.g., "0x1a2b3c")
pub fn bytes_to_hex(data: &[u8]) -> String {
    if data.is_empty() {
        return "0x".to_string();
    }

    let hex_chars: Vec<String> =
        data.iter().map(|b| format!("{:02x}", b)).collect();
    format!("0x{}", hex_chars.join(""))
}

/// Returns a human-readable size string (e.g., "1.5 KB", "2.3 MB").
///
/// # Arguments
/// * `bytes` - Size in bytes
///
/// # Returns
/// A formatted size string
pub fn format_size(bytes: usize) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;

    let bytes_f64 = bytes as f64;

    if bytes_f64 >= GB {
        format!("{:.2} GB", bytes_f64 / GB)
    } else if bytes_f64 >= MB {
        format!("{:.2} MB", bytes_f64 / MB)
    } else if bytes_f64 >= KB {
        format!("{:.2} KB", bytes_f64 / KB)
    } else {
        format!("{} bytes", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_indent_zero() {
        assert_eq!(indent(0), "");
    }

    #[test]
    fn test_indent_positive() {
        assert_eq!(indent(1), " ");
        assert_eq!(indent(4), "    ");
        assert_eq!(indent(10), "          ");
    }

    #[test]
    fn test_indent_length() {
        for n in 0..20 {
            assert_eq!(indent(n).len(), n);
        }
    }

    #[test]
    fn test_format_bytes_short() {
        let data = b"hello";
        let result = format_bytes(data, 10);
        // Debug format of &[u8] shows byte values, not string
        assert!(result.contains("104")); // 'h' = 104
        assert!(!result.contains("..."));
    }

    #[test]
    fn test_format_bytes_truncated() {
        let data = b"hello world this is a long message";
        let result = format_bytes(data, 5);
        assert!(result.contains("..."));
    }

    #[test]
    fn test_format_bytes_exact_max() {
        let data = b"exact";
        let result = format_bytes(data, 5);
        assert!(!result.contains("..."));
    }

    #[test]
    fn test_bytes_to_hex_empty() {
        let data = &[];
        assert_eq!(bytes_to_hex(data), "0x");
    }

    #[test]
    fn test_bytes_to_hex_single() {
        let data = &[0xAB];
        assert_eq!(bytes_to_hex(data), "0xab");
    }

    #[test]
    fn test_bytes_to_hex_multiple() {
        let data = &[0x01, 0x23, 0xAB, 0xCD];
        assert_eq!(bytes_to_hex(data), "0x0123abcd");
    }

    #[test]
    fn test_format_size_bytes() {
        assert_eq!(format_size(0), "0 bytes");
        assert_eq!(format_size(100), "100 bytes");
        assert_eq!(format_size(1023), "1023 bytes");
    }

    #[test]
    fn test_format_size_kb() {
        assert_eq!(format_size(1024), "1.00 KB");
        assert_eq!(format_size(2048), "2.00 KB");
        assert_eq!(format_size(1536), "1.50 KB");
    }

    #[test]
    fn test_format_size_mb() {
        assert_eq!(format_size(1024 * 1024), "1.00 MB");
        assert_eq!(format_size(2 * 1024 * 1024), "2.00 MB");
        assert_eq!(format_size(1024 * 1024 + 512 * 1024), "1.50 MB");
    }

    #[test]
    fn test_format_size_gb() {
        assert_eq!(format_size(1024 * 1024 * 1024), "1.00 GB");
        assert_eq!(format_size(2 * 1024 * 1024 * 1024), "2.00 GB");
    }
}
