/// Result of a database truncate operation.
///
///
#[derive(Debug, Clone)]
pub struct TruncateResult {
    /// The new (empty) database.
    pub new_db_id: crate::dbi::DatabaseId,
    /// Number of records that were in the old database.
    pub record_count: i64,
}
