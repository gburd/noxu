//! Database type enumeration.
//!
//! Port of internal database type tracking from JE.

use std::fmt;

/// The type of a database.
///
/// Internal databases have specific types (ID, Name, Utilization).
/// User databases are marked as User type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DbType {
    /// The ID database (maps DatabaseId -> database name).
    Id,
    /// The Name database (maps database name -> DatabaseId).
    Name,
    /// The Utilization database (tracks log file utilization).
    Utilization,
    /// A user-created database.
    User,
}

impl DbType {
    /// Returns true if this is an internal database type.
    pub fn is_internal(&self) -> bool {
        matches!(self, DbType::Id | DbType::Name | DbType::Utilization)
    }

    /// Returns the database name for internal databases.
    pub fn internal_name(&self) -> Option<&'static str> {
        match self {
            DbType::Id => Some("_jeIdMap"),
            DbType::Name => Some("_jeNameMap"),
            DbType::Utilization => Some("_jeUtilization"),
            DbType::User => None,
        }
    }
}

impl fmt::Display for DbType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbType::Id => write!(f, "ID"),
            DbType::Name => write!(f, "NAME"),
            DbType::Utilization => write!(f, "UTILIZATION"),
            DbType::User => write!(f, "USER"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_internal() {
        assert!(DbType::Id.is_internal());
        assert!(DbType::Name.is_internal());
        assert!(DbType::Utilization.is_internal());
        assert!(!DbType::User.is_internal());
    }

    #[test]
    fn test_internal_name() {
        assert_eq!(DbType::Id.internal_name(), Some("_jeIdMap"));
        assert_eq!(DbType::Name.internal_name(), Some("_jeNameMap"));
        assert_eq!(DbType::Utilization.internal_name(), Some("_jeUtilization"));
        assert_eq!(DbType::User.internal_name(), None);
    }

    #[test]
    fn test_display() {
        assert_eq!(DbType::Id.to_string(), "ID");
        assert_eq!(DbType::Name.to_string(), "NAME");
        assert_eq!(DbType::Utilization.to_string(), "UTILIZATION");
        assert_eq!(DbType::User.to_string(), "USER");
    }
}
