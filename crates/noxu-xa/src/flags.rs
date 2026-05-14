//! XA flags and return codes.

/// XA flags passed to XA operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct XaFlags(pub u32);

impl XaFlags {
    /// No flags set.
    pub const NOFLAGS: Self = Self(0x00000000);
    /// Join an existing transaction branch.
    pub const JOIN: Self = Self(0x00200000);
    /// Resume a suspended branch.
    pub const RESUME: Self = Self(0x08000000);
    /// End: branch portion completed successfully.
    pub const TMSUCCESS: Self = Self(0x04000000);
    /// End: branch was unsuccessful.
    pub const TMFAIL: Self = Self(0x20000000);
    /// End: suspend (not end) the branch.
    pub const TMSUSPEND: Self = Self(0x02000000);
    /// Commit: one-phase optimization.
    pub const ONEPHASE: Self = Self(0x40000000);
    /// Recover: start scan.
    pub const STARTRSCAN: Self = Self(0x01000000);
    /// Recover: end scan.
    pub const ENDRSCAN: Self = Self(0x00800000);

    /// Check if a specific flag is set.
    pub fn contains(self, flag: Self) -> bool {
        (self.0 & flag.0) != 0
    }

    /// Combine two flag sets.
    pub fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_flags() {
        assert_eq!(XaFlags::NOFLAGS.0, 0);
        assert!(!XaFlags::NOFLAGS.contains(XaFlags::JOIN));
    }

    #[test]
    fn test_contains() {
        let flags = XaFlags::TMSUCCESS.union(XaFlags::ONEPHASE);
        assert!(flags.contains(XaFlags::TMSUCCESS));
        assert!(flags.contains(XaFlags::ONEPHASE));
        assert!(!flags.contains(XaFlags::TMFAIL));
    }
}
