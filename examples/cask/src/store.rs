//! Storage layer wrapping Noxu DB.

use bytes::Bytes;
use noxu::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    Get, OperationStatus, Transaction,
};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

/// Errors from the storage layer.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("noxu db error: {0}")]
    Db(#[from] noxu::NoxuError),
    #[error("value is not a valid integer")]
    NotAnInteger,
    #[error("integer overflow")]
    Overflow,
}

/// Statistics counters for the store.
#[derive(Debug, Default)]
pub struct StoreStats {
    pub gets: AtomicU64,
    pub sets: AtomicU64,
    pub deletes: AtomicU64,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
}

/// The Cask storage engine backed by Noxu DB.
pub struct CaskStore {
    env: Environment,
    db: Database,
    pub stats: Arc<StoreStats>,
}

impl CaskStore {
    /// Open the store, creating the data directory and database if needed.
    pub fn open(data_dir: &Path) -> Result<Self, StoreError> {
        let env_cfg = EnvironmentConfig::new(data_dir.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        let env = Environment::open(env_cfg)?;

        let db_cfg = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "cask_store", &db_cfg)?;

        Ok(Self { env, db, stats: Arc::new(StoreStats::default()) })
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>, StoreError> {
        self.stats.gets.fetch_add(1, Ordering::Relaxed);
        match self.db.get(key)? {
            Some(val) => {
                self.stats.hits.fetch_add(1, Ordering::Relaxed);
                Ok(Some(val))
            }
            None => {
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
                Ok(None)
            }
        }
    }

    /// Set a key to a value.
    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.stats.sets.fetch_add(1, Ordering::Relaxed);
        self.db.put(key, value)?;
        Ok(())
    }

    /// Delete a key. Returns true if the key existed.
    pub fn del(&self, key: &[u8]) -> Result<bool, StoreError> {
        self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        Ok(self.db.delete(key)?)
    }

    /// Check if a key exists.
    pub fn exists(&self, key: &[u8]) -> Result<bool, StoreError> {
        Ok(self.db.get(key)?.is_some())
    }

    /// Return all keys matching a glob pattern.
    ///
    /// Supports `*` (match all) and `prefix*` patterns. Other patterns
    /// fall back to a byte-level glob match.
    pub fn keys(&self, pattern: &[u8]) -> Result<Vec<Bytes>, StoreError> {
        let mut results = Vec::new();
        let mut cursor = self.db.open_cursor(None)?;
        let mut key_out = DatabaseEntry::new();
        let mut val_out = DatabaseEntry::new();

        let status =
            cursor.get(&mut key_out, &mut val_out, Get::First, None)?;
        if status != OperationStatus::Success {
            return Ok(results);
        }

        loop {
            if let Some(k) = key_out.get_data()
                && glob_match(pattern, k)
            {
                results.push(Bytes::copy_from_slice(k));
            }
            if cursor.get(&mut key_out, &mut val_out, Get::Next, None)?
                != OperationStatus::Success
            {
                break;
            }
        }

        Ok(results)
    }

    /// Increment a key by `by`. If the key does not exist, it is initialized to 0
    /// before incrementing. Returns the new value.
    pub fn incr(&self, key: &[u8], by: i64) -> Result<i64, StoreError> {
        let current = self.get(key)?;
        let current_val: i64 = match &current {
            Some(data) => {
                let s = std::str::from_utf8(data)
                    .map_err(|_| StoreError::NotAnInteger)?;
                s.parse::<i64>().map_err(|_| StoreError::NotAnInteger)?
            }
            None => 0,
        };
        let new_val =
            current_val.checked_add(by).ok_or(StoreError::Overflow)?;
        let new_bytes = new_val.to_string().into_bytes();
        self.set(key, &new_bytes)?;
        Ok(new_val)
    }

    /// Get multiple keys at once.
    pub fn mget(
        &self,
        keys: &[Bytes],
    ) -> Result<Vec<Option<Bytes>>, StoreError> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key)?);
        }
        Ok(results)
    }

    /// Set multiple key-value pairs at once.
    pub fn mset(&self, pairs: &[(Bytes, Bytes)]) -> Result<(), StoreError> {
        for (key, value) in pairs {
            self.set(key, value)?;
        }
        Ok(())
    }

    /// Rename a key. Returns false if the source key does not exist.
    pub fn rename(&self, old: &[u8], new: &[u8]) -> Result<bool, StoreError> {
        let val = match self.get(old)? {
            Some(v) => v,
            None => return Ok(false),
        };
        self.set(new, &val)?;
        self.del(old)?;
        Ok(true)
    }

    /// Append a value to an existing key (or set it if the key does not exist).
    /// Returns the length of the resulting value.
    pub fn append(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Result<usize, StoreError> {
        let existing = self.get(key)?;
        let new_val = match existing {
            Some(existing_data) => {
                let mut combined = existing_data.to_vec();
                combined.extend_from_slice(value);
                combined
            }
            None => value.to_vec(),
        };
        let len = new_val.len();
        self.set(key, &new_val)?;
        Ok(len)
    }

    /// Return the number of keys in the database.
    pub fn dbsize(&self) -> Result<u64, StoreError> {
        Ok(self.db.count()?)
    }

    /// Begin a new transaction for MULTI/EXEC support.
    pub fn begin_transaction(&self) -> Result<Transaction, StoreError> {
        Ok(self.env.begin_transaction(None)?)
    }

    /// Put within a transaction.
    pub fn set_in_txn(
        &self,
        txn: &Transaction,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), StoreError> {
        self.db.put_in(txn, key, value)?;
        Ok(())
    }

    /// Delete within a transaction.
    pub fn del_in_txn(
        &self,
        txn: &Transaction,
        key: &[u8],
    ) -> Result<bool, StoreError> {
        Ok(self.db.delete_in(txn, key)?)
    }

    /// Produce a Redis-like INFO string.
    pub fn info(&self) -> String {
        let stats = &self.stats;
        format!(
            "# Server\r\n\
             cask_version:0.1.0\r\n\
             engine:noxu-db\r\n\
             \r\n\
             # Stats\r\n\
             total_gets:{}\r\n\
             total_sets:{}\r\n\
             total_deletes:{}\r\n\
             keyspace_hits:{}\r\n\
             keyspace_misses:{}\r\n\
             \r\n\
             # Keyspace\r\n\
             db0:keys={}\r\n",
            stats.gets.load(Ordering::Relaxed),
            stats.sets.load(Ordering::Relaxed),
            stats.deletes.load(Ordering::Relaxed),
            stats.hits.load(Ordering::Relaxed),
            stats.misses.load(Ordering::Relaxed),
            self.dbsize().unwrap_or(0),
        )
    }
}

/// Simple glob matching supporting `*` (match everything) and `prefix*` patterns.
/// For more complex patterns, a byte-level recursive glob is used.
fn glob_match(pattern: &[u8], value: &[u8]) -> bool {
    if pattern == b"*" {
        return true;
    }

    // Recursive byte-level glob supporting `*` and `?`.
    glob_match_recursive(pattern, value)
}

fn glob_match_recursive(pattern: &[u8], value: &[u8]) -> bool {
    let mut pi = 0;
    let mut vi = 0;

    // Track the last `*` position for backtracking.
    let mut star_pi: Option<usize> = None;
    let mut star_vi: usize = 0;

    while vi < value.len() {
        if pi < pattern.len()
            && (pattern[pi] == b'?' || pattern[pi] == value[vi])
        {
            pi += 1;
            vi += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = Some(pi);
            star_vi = vi;
            pi += 1;
        } else if let Some(sp) = star_pi {
            pi = sp + 1;
            star_vi += 1;
            vi = star_vi;
        } else {
            return false;
        }
    }

    // Consume trailing stars.
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_star() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"*", b""));
    }

    #[test]
    fn test_glob_prefix() {
        assert!(glob_match(b"user:*", b"user:123"));
        assert!(glob_match(b"user:*", b"user:"));
        assert!(!glob_match(b"user:*", b"item:123"));
    }

    #[test]
    fn test_glob_question_mark() {
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"h?llo", b"hallo"));
        assert!(!glob_match(b"h?llo", b"hllo"));
    }

    #[test]
    fn test_glob_exact() {
        assert!(glob_match(b"hello", b"hello"));
        assert!(!glob_match(b"hello", b"world"));
    }
}
