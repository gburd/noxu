//! Auto-incrementing sequences for entity ID generation.
//!
//! Sequence`. Provides atomic sequence generation
//! backed by a database record.

use crate::error::Result;
use noxu_db::{Database, DatabaseEntry, OperationStatus};
use std::sync::atomic::{AtomicU64, Ordering};

/// Auto-incrementing sequence for generating unique IDs.
///
/// A `Sequence` generates monotonically increasing `u64` values. It
/// uses an in-memory atomic counter that is periodically persisted to
/// the underlying database. The `cache_size` controls how many values
/// are pre-allocated in memory before a database write is required.
///
/// 
///
/// # Example
///
/// ```ignore
/// use noxu_persist::sequence::Sequence;
///
/// let seq = Sequence::new(&db, "user_id").unwrap();
/// let id1 = seq.next().unwrap();
/// let id2 = seq.next().unwrap();
/// assert_eq!(id2, id1 + 1);
/// ```
pub struct Sequence<'db> {
    /// Reference to the database storing sequence state.
    db: &'db Database,
    /// The key under which this sequence's value is stored.
    key: Vec<u8>,
    /// Current in-memory counter.
    current: AtomicU64,
    /// Upper limit of the current cached range (exclusive).
    cached_limit: AtomicU64,
    /// Number of values to pre-allocate per database write.
    cache_size: u64,
}

impl<'db> Sequence<'db> {
    /// Creates or opens a sequence stored under the given name.
    ///
    /// If the sequence already exists in the database, its current value
    /// is read. Otherwise, it starts from 1.
    ///
    /// # Arguments
    /// * `db` - The database to store sequence state in.
    /// * `name` - The logical name of this sequence.
    ///
    /// # Errors
    /// Returns an error if the database read fails.
    pub fn new(db: &'db Database, name: &str) -> Result<Self> {
        Self::with_cache_size(db, name, 100)
    }

    /// Creates or opens a sequence with a specific cache size.
    ///
    /// # Arguments
    /// * `db` - The database to store sequence state in.
    /// * `name` - The logical name of this sequence.
    /// * `cache_size` - How many IDs to pre-allocate per database write.
    ///
    /// # Errors
    /// Returns an error if the database read fails.
    pub fn with_cache_size(
        db: &'db Database,
        name: &str,
        cache_size: u64,
    ) -> Result<Self> {
        let key = format!("seq:{}", name).into_bytes();
        let key_entry = DatabaseEntry::from_bytes(&key);
        let mut data_entry = DatabaseEntry::new();

        let initial_value = match db.get(None, &key_entry, &mut data_entry)? {
            OperationStatus::Success => {
                let bytes = data_entry.data();
                if bytes.len() >= 8 {
                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(&bytes[..8]);
                    u64::from_be_bytes(arr)
                } else {
                    1
                }
            }
            _ => 1,
        };

        // Pre-allocate the first cache range and persist.
        let limit = initial_value + cache_size;
        let limit_entry = DatabaseEntry::from_bytes(&limit.to_be_bytes());
        db.put(None, &key_entry, &limit_entry)?;

        Ok(Self {
            db,
            key,
            current: AtomicU64::new(initial_value),
            cached_limit: AtomicU64::new(limit),
            cache_size,
        })
    }

    /// Returns the next value from this sequence.
    ///
    /// This method is thread-safe and lock-free for most calls. A
    /// database write only occurs when the in-memory cache is exhausted.
    ///
    /// # Errors
    /// Returns an error if a database write fails during cache refill.
    pub fn next(&self) -> Result<u64> {
        loop {
            let val = self.current.fetch_add(1, Ordering::Relaxed);
            let limit = self.cached_limit.load(Ordering::Acquire);

            if val < limit {
                return Ok(val);
            }

            // Need to refill the cache. Use compare_exchange to ensure
            // only one thread does the refill.
            let new_limit = val + self.cache_size;
            match self.cached_limit.compare_exchange(
                limit,
                new_limit,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // We won the race, persist the new limit.
                    let key_entry = DatabaseEntry::from_bytes(&self.key);
                    let limit_entry =
                        DatabaseEntry::from_bytes(&new_limit.to_be_bytes());
                    self.db.put(None, &key_entry, &limit_entry)?;
                    return Ok(val);
                }
                Err(_) => {
                    // Another thread refilled, retry.
                    continue;
                }
            }
        }
    }

    /// Returns the last value returned by `next()`, or 0 if `next()` has
    /// not been called yet.
    ///
    /// Note: Due to concurrent access this may not reflect the most
    /// recent call to `next()` by another thread.
    pub fn current(&self) -> u64 {
        let val = self.current.load(Ordering::Relaxed);
        if val == 0 { 0 } else { val.saturating_sub(1) }
    }

    /// Returns the cache size (number of IDs pre-allocated per DB write).
    pub fn cache_size(&self) -> u64 {
        self.cache_size
    }

    /// Returns the name key used to store this sequence.
    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

/// An in-memory-only sequence that does not persist to a database.
///
/// Useful for testing or when persistence is not needed.
#[derive(Debug)]
pub struct MemorySequence {
    current: AtomicU64,
}

impl MemorySequence {
    /// Creates a new memory sequence starting from 1.
    pub fn new() -> Self {
        Self { current: AtomicU64::new(1) }
    }

    /// Creates a new memory sequence starting from the given value.
    pub fn starting_at(start: u64) -> Self {
        Self { current: AtomicU64::new(start) }
    }

    /// Returns the next value from this sequence.
    pub fn next(&self) -> u64 {
        self.current.fetch_add(1, Ordering::Relaxed)
    }

    /// Returns the current counter value (the next value that will be returned).
    pub fn peek(&self) -> u64 {
        self.current.load(Ordering::Relaxed)
    }
}

impl Default for MemorySequence {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
    use tempfile::TempDir;

    fn setup_db() -> (TempDir, Environment, Database) {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(false);
        let env = Environment::open(env_config).unwrap();
        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "seq_db", &db_config).unwrap();
        (temp_dir, env, db)
    }

    #[test]
    fn test_sequence_starts_at_one() {
        let (_td, _env, db) = setup_db();
        let seq = Sequence::new(&db, "test").unwrap();
        assert_eq!(seq.next().unwrap(), 1);
    }

    #[test]
    fn test_sequence_increments() {
        let (_td, _env, db) = setup_db();
        let seq = Sequence::new(&db, "test").unwrap();
        assert_eq!(seq.next().unwrap(), 1);
        assert_eq!(seq.next().unwrap(), 2);
        assert_eq!(seq.next().unwrap(), 3);
    }

    #[test]
    fn test_sequence_current() {
        let (_td, _env, db) = setup_db();
        let seq = Sequence::new(&db, "test").unwrap();
        seq.next().unwrap();
        seq.next().unwrap();
        // current() returns the last value returned by next()
        // After calling next() twice (returning 1, 2), current should be ~2
        let cur = seq.current();
        assert!(cur >= 1);
    }

    #[test]
    fn test_sequence_cache_size() {
        let (_td, _env, db) = setup_db();
        let seq = Sequence::with_cache_size(&db, "test", 10).unwrap();
        assert_eq!(seq.cache_size(), 10);
        // Generate values within the cache
        for expected in 1..=10 {
            assert_eq!(seq.next().unwrap(), expected);
        }
    }

    #[test]
    fn test_sequence_exceeds_cache() {
        let (_td, _env, db) = setup_db();
        let seq = Sequence::with_cache_size(&db, "test", 5).unwrap();
        // Generate more values than the cache size
        for expected in 1..=12 {
            assert_eq!(seq.next().unwrap(), expected);
        }
    }

    #[test]
    fn test_sequence_key() {
        let (_td, _env, db) = setup_db();
        let seq = Sequence::new(&db, "my_seq").unwrap();
        assert_eq!(seq.key(), b"seq:my_seq");
    }

    #[test]
    fn test_multiple_sequences_independent() {
        let (_td, _env, db) = setup_db();
        let seq1 = Sequence::new(&db, "seq1").unwrap();
        let seq2 = Sequence::new(&db, "seq2").unwrap();
        assert_eq!(seq1.next().unwrap(), 1);
        assert_eq!(seq2.next().unwrap(), 1);
        assert_eq!(seq1.next().unwrap(), 2);
        assert_eq!(seq2.next().unwrap(), 2);
    }

    // --- MemorySequence tests ---

    #[test]
    fn test_memory_sequence_starts_at_one() {
        let seq = MemorySequence::new();
        assert_eq!(seq.next(), 1);
    }

    #[test]
    fn test_memory_sequence_increments() {
        let seq = MemorySequence::new();
        assert_eq!(seq.next(), 1);
        assert_eq!(seq.next(), 2);
        assert_eq!(seq.next(), 3);
    }

    #[test]
    fn test_memory_sequence_starting_at() {
        let seq = MemorySequence::starting_at(100);
        assert_eq!(seq.next(), 100);
        assert_eq!(seq.next(), 101);
    }

    #[test]
    fn test_memory_sequence_peek() {
        let seq = MemorySequence::new();
        assert_eq!(seq.peek(), 1);
        seq.next();
        assert_eq!(seq.peek(), 2);
    }

    #[test]
    fn test_memory_sequence_default() {
        let seq = MemorySequence::default();
        assert_eq!(seq.next(), 1);
    }
}
