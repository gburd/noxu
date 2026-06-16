//! Lazy iterator adapters for [`crate::Database`].
//!
//! Provides [`DbIter`] (full-scan, forward) and [`DbRange`] (key-range scan)
//! as convenience wrappers around the underlying [`crate::Cursor`] API.
//!
//! # Design
//!
//! Both types implement `Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>` and
//! advance the cursor **lazily** — one record per `next()` call.  They do
//! NOT eagerly materialise the scan into a `Vec` (that is the `StoredMap`
//! anti-pattern flagged in 2026 audit finding 2.2).
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
use crate::transaction::Transaction;
use std::marker::PhantomData;
use std::ops::Bound;

// ── DbIter ────────────────────────────────────────────────────────────────────

/// A forward-scanning iterator over all records in a database.
///
/// Returned by [`crate::Database::iter`].  Holds a live [`crate::Cursor`]; records are
/// fetched one at a time (lazy) — the full database is **not** materialised
/// into memory.
///
/// The lifetime `'txn` ensures the iterator cannot outlive the transaction
/// it was opened against.  This prevents use-after-commit bugs at compile
/// time: the borrow checker rejects any code that commits or drops the
/// transaction while `DbIter` is still alive.
///
/// # Drop behaviour
///
/// Dropping the iterator closes the underlying cursor.  For transactional
/// cursors this releases any shared read locks the cursor holds.
pub struct DbIter<'txn> {
    cursor: Cursor,
    started: bool,
    done: bool,
    _txn: PhantomData<&'txn Transaction>,
}

impl<'txn> DbIter<'txn> {
    pub(crate) fn new(cursor: Cursor) -> Self {
        Self { cursor, started: false, done: false, _txn: PhantomData }
    }
}

impl<'txn> Iterator for DbIter<'txn> {
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
/// Returned by [`crate::Database::range`].  Holds a live [`crate::Cursor`] positioned at
/// the first key ≥ `start_bound` and stops when the current key exceeds
/// `end_bound`.  Records are fetched lazily — one per `next()` call.
///
/// The lifetime `'txn` ensures the iterator cannot outlive the transaction
/// it was opened against.  See [`DbIter`] for the rationale.
pub struct DbRange<'txn> {
    cursor: Cursor,
    end_bound: Bound<Vec<u8>>,
    done: bool,
    /// Whether the cursor has been positioned at the start yet.
    positioned: bool,
    start_key: Option<Vec<u8>>,
    /// When true, skip a record whose key exactly equals `start_key` (Excluded bound).
    exclude_start: bool,
    _txn: PhantomData<&'txn Transaction>,
}

impl<'txn> DbRange<'txn> {
    pub(crate) fn new(
        cursor: Cursor,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
    ) -> Self {
        let (start_key, exclude_start) = match start_bound {
            Bound::Included(k) => (Some(k), false),
            Bound::Excluded(k) => (Some(k), true),
            Bound::Unbounded => (None, false),
        };
        Self {
            cursor,
            end_bound,
            done: false,
            positioned: false,
            start_key,
            exclude_start,
            _txn: PhantomData,
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

impl<'txn> Iterator for DbRange<'txn> {
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
                self.cursor.get(
                    &mut key_entry,
                    &mut val_entry,
                    Get::SearchGte,
                    None,
                )
            } else {
                self.cursor.get(
                    &mut key_entry,
                    &mut val_entry,
                    Get::First,
                    None,
                )
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
                    // Excluded start: skip the exact start key.
                    if self.exclude_start
                        && self
                            .start_key
                            .as_ref()
                            .is_some_and(|sk| k.as_slice() == sk.as_slice())
                    {
                        // Fall through to the Get::Next block below.
                        self.positioned = true;
                    } else {
                        return Some(Ok((k, v)));
                    }
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
