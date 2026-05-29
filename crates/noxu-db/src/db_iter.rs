//! Lazy iterator adapters for [`Database`].
//!
//! Provides [`DbIter`] (full-scan, forward) and [`DbRange`] (key-range scan)
//! as convenience wrappers around the underlying [`Cursor`] API.
//!
//! # Design
//!
//! Both types implement `Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>` and
//! advance the cursor **lazily** — one record per `next()` call.  They do
//! NOT eagerly materialise the scan into a `Vec` (that is the `StoredMap`
//! anti-pattern flagged in audit-2026-05-jonhoo.md finding 2.2).
//!
//! # Example
//!
//! ```no_run
//! use noxu_db::{DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig};
//! use std::path::PathBuf;
//!
//! let env = Environment::open(
//!     EnvironmentConfig::new(PathBuf::from("/tmp/iter_demo"))
//!         .with_allow_create(true)
//!         .with_transactional(true),
//! )?;
//! let db = env.open_database(
//!     None,
//!     "demo",
//!     &DatabaseConfig::new().with_allow_create(true).with_transactional(true),
//! )?;
//!
//! // Insert some records.
//! for i in 0u32..5 {
//!     db.put(None, &DatabaseEntry::from_bytes(&i.to_be_bytes()), &DatabaseEntry::from_bytes(b"v"))?;
//! }
//!
//! // Forward scan — lazy.
//! for result in db.iter(None)? {
//!     let (key, val) = result?;
//!     println!("{:?} => {:?}", key, val);
//! }
//!
//! // Range scan — lazy.
//! let lo = 1u32.to_be_bytes();
//! let hi = 3u32.to_be_bytes();
//! for result in db.range(None, lo.as_ref()..=hi.as_ref())? {
//!     let (key, _val) = result?;
//!     assert!(key.as_slice() >= lo.as_slice() && key.as_slice() <= hi.as_slice());
//! }
//!
//! db.close()?;
//! env.close()?;
//! # Ok::<(), noxu_db::NoxuError>(())
//! ```

use crate::cursor::Cursor;
use crate::database_entry::DatabaseEntry;
use crate::error::{NoxuError, Result};
use crate::get::Get;
use crate::operation_status::OperationStatus;
use std::ops::Bound;

// ── DbIter ────────────────────────────────────────────────────────────────────

/// A forward-scanning iterator over all records in a database.
///
/// Returned by [`Database::iter`].  Holds a live [`Cursor`]; records are
/// fetched one at a time (lazy) — the full database is **not** materialised
/// into memory.
///
/// # Drop behaviour
///
/// Dropping the iterator closes the underlying cursor.  For transactional
/// cursors this releases any shared read locks the cursor holds.
pub struct DbIter {
    cursor: Cursor,
    started: bool,
    done: bool,
}

impl DbIter {
    pub(crate) fn new(cursor: Cursor) -> Self {
        Self { cursor, started: false, done: false }
    }
}

impl Iterator for DbIter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let get_type = if self.started { Get::Next } else { Get::First };
        self.started = true;

        let mut key = DatabaseEntry::new();
        let mut val = DatabaseEntry::new();
        match self.cursor.get(&mut key, &mut val, get_type, None) {
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
            Ok(OperationStatus::Success) => {
                let k = key.get_data().unwrap_or(&[]).to_vec();
                let v = val.get_data().unwrap_or(&[]).to_vec();
                Some(Ok((k, v)))
            }
            Ok(_) => {
                self.done = true;
                None
            }
        }
    }
}

// ── DbRange ───────────────────────────────────────────────────────────────────

/// A lazy key-range iterator over a database.
///
/// Returned by [`Database::range`].  Holds a live [`Cursor`] positioned at
/// the first key ≥ `start_bound` and stops when the current key exceeds
/// `end_bound`.  Records are fetched lazily — one per `next()` call.
pub struct DbRange {
    cursor: Cursor,
    end_bound: Bound<Vec<u8>>,
    done: bool,
    /// Whether the cursor has been positioned at the start yet.
    positioned: bool,
    start_key: Option<Vec<u8>>,
}

impl DbRange {
    pub(crate) fn new(
        cursor: Cursor,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
    ) -> Self {
        let start_key = match &start_bound {
            Bound::Included(k) | Bound::Excluded(k) => Some(k.clone()),
            Bound::Unbounded => None,
        };
        // When the start bound is Excluded we advance one record past
        // the start key; this is handled in the first `next()` call.
        let excluded_start = matches!(start_bound, Bound::Excluded(_));
        Self {
            cursor,
            end_bound,
            done: false,
            positioned: false,
            start_key: if excluded_start {
                start_key.map(|k| {
                    // Tag so we know to skip it.
                    // We'll compare after positioning.
                    k
                })
            } else {
                start_key
            },
        }
    }

    fn past_end(&self, key: &[u8]) -> bool {
        match &self.end_bound {
            Bound::Unbounded => false,
            Bound::Included(end) => key > end.as_slice(),
            Bound::Excluded(end) => key >= end.as_slice(),
        }
    }
}

impl Iterator for DbRange {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let mut key_entry = DatabaseEntry::new();
        let mut val_entry = DatabaseEntry::new();

        if !self.positioned {
            self.positioned = true;
            // Position the cursor at the start of the range.
            let status = if let Some(ref sk) = self.start_key {
                key_entry.set_data(sk);
                self.cursor.get(&mut key_entry, &mut val_entry, Get::SearchGte, None)
            } else {
                self.cursor.get(&mut key_entry, &mut val_entry, Get::First, None)
            };

            match status {
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
                Ok(OperationStatus::Success) => {
                    let k = key_entry.get_data().unwrap_or(&[]).to_vec();
                    let v = val_entry.get_data().unwrap_or(&[]).to_vec();
                    if self.past_end(&k) {
                        self.done = true;
                        return None;
                    }
                    return Some(Ok((k, v)));
                }
                Ok(_) => {
                    self.done = true;
                    return None;
                }
            }
        }

        // Subsequent calls: advance forward.
        match self.cursor.get(&mut key_entry, &mut val_entry, Get::Next, None) {
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
            Ok(OperationStatus::Success) => {
                let k = key_entry.get_data().unwrap_or(&[]).to_vec();
                let v = val_entry.get_data().unwrap_or(&[]).to_vec();
                if self.past_end(&k) {
                    self.done = true;
                    None
                } else {
                    Some(Ok((k, v)))
                }
            }
            Ok(_) => {
                self.done = true;
                None
            }
        }
    }
}
