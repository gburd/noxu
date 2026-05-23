//! XA Transaction Identifier (Xid).

/// Maximum length of the global transaction ID component.
pub const MAXGTRIDSIZE: usize = 64;
/// Maximum length of the branch qualifier component.
pub const MAXBQUALSIZE: usize = 64;

/// XA Transaction Identifier.
///
/// Uniquely identifies a branch of a distributed transaction.
/// Consists of a format ID, a global transaction ID, and a branch qualifier.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Xid {
    /// Format identifier (application-defined; -1 = null Xid).
    pub format_id: i32,
    /// Global transaction identifier (up to 64 bytes).
    pub global_transaction_id: Vec<u8>,
    /// Branch qualifier (up to 64 bytes).
    pub branch_qualifier: Vec<u8>,
}

impl Xid {
    /// Creates a new Xid.
    ///
    /// # Errors
    /// Returns error if gtrid or bqual exceed maximum sizes.
    pub fn new(
        format_id: i32,
        gtrid: &[u8],
        bqual: &[u8],
    ) -> Result<Self, XidError> {
        if gtrid.len() > MAXGTRIDSIZE {
            return Err(XidError::GtridTooLong(gtrid.len()));
        }
        if bqual.len() > MAXBQUALSIZE {
            return Err(XidError::BqualTooLong(bqual.len()));
        }
        Ok(Self {
            format_id,
            global_transaction_id: gtrid.to_vec(),
            branch_qualifier: bqual.to_vec(),
        })
    }

    /// Returns true if this is the null Xid (format_id == -1).
    pub fn is_null(&self) -> bool {
        self.format_id == -1
    }

    /// Creates a null Xid.
    pub fn null() -> Self {
        Self {
            format_id: -1,
            global_transaction_id: Vec::new(),
            branch_qualifier: Vec::new(),
        }
    }
}

impl std::fmt::Debug for Xid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Xid(fmt={}, gtrid={}, bqual={})",
            self.format_id,
            hex_str(&self.global_transaction_id),
            hex_str(&self.branch_qualifier),
        )
    }
}

impl std::fmt::Display for Xid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.format_id,
            hex_str(&self.global_transaction_id),
            hex_str(&self.branch_qualifier),
        )
    }
}

fn hex_str(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Errors related to Xid construction.
#[derive(Debug, Clone, thiserror::Error)]
pub enum XidError {
    #[error("global transaction ID too long ({0} bytes, max {MAXGTRIDSIZE})")]
    GtridTooLong(usize),
    #[error("branch qualifier too long ({0} bytes, max {MAXBQUALSIZE})")]
    BqualTooLong(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_valid() {
        let xid = Xid::new(1, b"gtrid123", b"bqual456").unwrap();
        assert_eq!(xid.format_id, 1);
        assert_eq!(xid.global_transaction_id, b"gtrid123");
        assert_eq!(xid.branch_qualifier, b"bqual456");
        assert!(!xid.is_null());
    }

    #[test]
    fn test_null_xid() {
        let xid = Xid::null();
        assert!(xid.is_null());
        assert_eq!(xid.format_id, -1);
    }

    #[test]
    fn test_gtrid_too_long() {
        let long = vec![0u8; 65];
        let result = Xid::new(1, &long, b"ok");
        assert!(matches!(result, Err(XidError::GtridTooLong(65))));
    }

    #[test]
    fn test_bqual_too_long() {
        let long = vec![0u8; 65];
        let result = Xid::new(1, b"ok", &long);
        assert!(matches!(result, Err(XidError::BqualTooLong(65))));
    }

    #[test]
    fn test_display() {
        let xid = Xid::new(42, b"\x01\x02", b"\x03\x04").unwrap();
        let s = format!("{xid}");
        assert_eq!(s, "42:0102:0304");
    }

    #[test]
    fn test_equality() {
        let a = Xid::new(1, b"g", b"b").unwrap();
        let b = Xid::new(1, b"g", b"b").unwrap();
        let c = Xid::new(2, b"g", b"b").unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
