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
//!     db.put(&i.to_be_bytes(), b"v")?;
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
use crate::error::Result;
use crate::get::Get;
use crate::operation_status::OperationStatus;
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
    cursor: Cursor<'txn>,
    started: bool,
    done: bool,
}

impl<'txn> DbIter<'txn> {
    pub(crate) fn new(cursor: Cursor<'txn>) -> Self {
        Self { cursor, started: false, done: false }
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
                let k = key.data_opt().unwrap_or(&[]).to_vec();
                let v = val.data_opt().unwrap_or(&[]).to_vec();
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
    cursor: Cursor<'txn>,
    end_bound: Bound<Vec<u8>>,
    done: bool,
    /// Whether the cursor has been positioned at the start yet.
    positioned: bool,
    start_key: Option<Vec<u8>>,
    /// When true, skip a record whose key exactly equals `start_key` (Excluded bound).
    exclude_start: bool,
}

impl<'txn> DbRange<'txn> {
    pub(crate) fn new(
        cursor: Cursor<'txn>,
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
                    let k = key_entry.data_opt().unwrap_or(&[]).to_vec();
                    let v = val_entry.data_opt().unwrap_or(&[]).to_vec();
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
                let k = key_entry.data_opt().unwrap_or(&[]).to_vec();
                let v = val_entry.data_opt().unwrap_or(&[]).to_vec();
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

#[cfg(test)]
mod tests {
    use crate::database::Database;
    use crate::database_config::DatabaseConfig;
    use crate::environment::Environment;
    use crate::environment_config::EnvironmentConfig;
    use tempfile::TempDir;

    /// Ten records keyed `k0`..`k9` (lexicographic == numeric here), each with
    /// value `v<N>`.
    fn seeded_db() -> (TempDir, Environment, Database) {
        let dir = TempDir::new().unwrap();
        let env = Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
        let db = env
            .open_database(
                None,
                "iter",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();
        for i in 0u8..10 {
            db.put(format!("k{i}"), format!("v{i}")).unwrap();
        }
        (dir, env, db)
    }

    fn keys(
        it: impl Iterator<Item = crate::error::Result<(Vec<u8>, Vec<u8>)>>,
    ) -> Vec<String> {
        it.map(|r| String::from_utf8(r.unwrap().0).unwrap()).collect()
    }

    #[test]
    fn db_iter_yields_every_record_in_key_order_then_stops() {
        let (_d, _e, db) = seeded_db();
        let got = keys(db.iter(None).unwrap());
        let want: Vec<String> = (0..10).map(|i| format!("k{i}")).collect();
        assert_eq!(got, want);
    }

    /// A `DbIter` that has run to completion must stay exhausted. The `done`
    /// latch exists so a spent iterator does not re-issue `Get::Next` against
    /// a cursor that is already off the end of the tree.
    #[test]
    fn db_iter_stays_exhausted_after_completion() {
        let (_d, _e, db) = seeded_db();
        let mut it = db.iter(None).unwrap();
        assert_eq!(it.by_ref().count(), 10);
        assert!(it.next().is_none());
        assert!(it.next().is_none());
    }

    /// The iterator must be lazy: taking two records must not walk the rest of
    /// the database. Observable proxy — after `take(2)` the underlying cursor
    /// is dropped mid-scan and the next fresh scan still sees all 10 records
    /// (i.e. the partial scan left no positional residue).
    #[test]
    fn db_iter_is_lazy_and_a_partial_scan_leaves_no_residue() {
        let (_d, _e, db) = seeded_db();
        let first_two = keys(db.iter(None).unwrap().take(2));
        assert_eq!(first_two, vec!["k0".to_string(), "k1".to_string()]);
        assert_eq!(keys(db.iter(None).unwrap()).len(), 10);
    }

    #[test]
    fn db_iter_on_empty_database_yields_nothing() {
        let dir = TempDir::new().unwrap();
        let env = Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
        let db = env
            .open_database(
                None,
                "empty",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();
        assert!(db.iter(None).unwrap().next().is_none());
    }

    /// Every `RangeBounds` shape must map onto the right half-open/closed
    /// interval. This is the invariant most at risk in `DbRange`: the start
    /// bound is honoured by a `SearchGte` seek plus an explicit
    /// skip-the-exact-key step for `Excluded`, and the end bound by
    /// `past_end`, so the four combinations are four distinct code paths.
    #[test]
    fn db_range_honours_every_bound_combination() {
        let (_d, _e, db) = seeded_db();
        let k = |i: u8| format!("k{i}").into_bytes();

        // inclusive..=inclusive
        assert_eq!(
            keys(db.range(None, k(3)..=k(6)).unwrap()),
            ["k3", "k4", "k5", "k6"]
        );
        // inclusive..exclusive
        assert_eq!(
            keys(db.range(None, k(3)..k(6)).unwrap()),
            ["k3", "k4", "k5"]
        );
        // exclusive start (Bound::Excluded is not expressible with `..`
        // syntax, so build the tuple form)
        use std::ops::Bound;
        assert_eq!(
            keys(
                db.range(None, (Bound::Excluded(k(3)), Bound::Included(k(6))))
                    .unwrap()
            ),
            ["k4", "k5", "k6"]
        );
        assert_eq!(
            keys(
                db.range(None, (Bound::Excluded(k(3)), Bound::Excluded(k(6))))
                    .unwrap()
            ),
            ["k4", "k5"]
        );
        // unbounded start
        assert_eq!(keys(db.range(None, ..=k(2)).unwrap()), ["k0", "k1", "k2"]);
        assert_eq!(keys(db.range(None, ..k(2)).unwrap()), ["k0", "k1"]);
        // unbounded end
        assert_eq!(keys(db.range(None, k(8)..).unwrap()), ["k8", "k9"]);
        // fully unbounded
        let all: std::ops::RangeFull = ..;
        assert_eq!(keys(db.range::<Vec<u8>>(None, all).unwrap()).len(), 10);
    }

    /// A start bound that names a key which does not exist must seek forward
    /// to the next existing key (`SearchGte`), not fail and not skip a record.
    #[test]
    fn db_range_start_bound_on_a_missing_key_seeks_forward() {
        let (_d, _e, db) = seeded_db();
        // "k2z" sorts after "k2" and before "k3".
        assert_eq!(
            keys(db.range(None, b"k2z".to_vec()..=b"k4".to_vec()).unwrap()),
            ["k3", "k4"]
        );
    }

    /// An empty or inverted range must yield nothing rather than running away
    /// to the end of the database. Both are decided by `past_end` on the very
    /// first record, which is the boundary case worth pinning.
    #[test]
    fn db_range_empty_and_inverted_ranges_yield_nothing() {
        let (_d, _e, db) = seeded_db();
        let k = |i: u8| format!("k{i}").into_bytes();
        // Excluded == Included on the same key: empty.
        assert!(db.range(None, k(5)..k(5)).unwrap().next().is_none());
        // Inverted.
        assert!(db.range(None, k(7)..k(2)).unwrap().next().is_none());
        // Start beyond the last key.
        assert!(db.range(None, b"z".to_vec()..).unwrap().next().is_none());
        // End before the first key.
        assert!(db.range(None, ..b"a".to_vec()).unwrap().next().is_none());
    }

    /// `DbRange` must also latch `done`, for the same reason `DbIter` does.
    #[test]
    fn db_range_stays_exhausted_after_completion() {
        let (_d, _e, db) = seeded_db();
        let mut r = db.range(None, b"k1".to_vec()..=b"k3".to_vec()).unwrap();
        assert_eq!(r.by_ref().count(), 3);
        assert!(r.next().is_none());
        assert!(r.next().is_none());
    }

    /// Values must travel with their keys — a scan that returned the right
    /// keys paired with the wrong data would pass every key-only assertion
    /// above.
    #[test]
    fn scans_pair_each_key_with_its_own_value() {
        let (_d, _e, db) = seeded_db();
        for r in db.iter(None).unwrap() {
            let (k, v) = r.unwrap();
            let ks = String::from_utf8(k).unwrap();
            let vs = String::from_utf8(v).unwrap();
            assert_eq!(vs, ks.replace('k', "v"), "key {ks} carried value {vs}");
        }
    }

    /// Records written inside an uncommitted transaction must be visible to a
    /// scan on that same transaction, and invisible after it aborts.
    #[test]
    fn range_within_a_transaction_sees_that_transaction_writes() {
        let (_d, env, db) = seeded_db();
        let txn = env.begin_transaction(None).unwrap();
        db.put_in(&txn, b"k5a", b"extra").unwrap();
        let seen = keys(
            db.range(Some(&txn), b"k5".to_vec()..=b"k6".to_vec()).unwrap(),
        );
        assert_eq!(seen, ["k5", "k5a", "k6"]);
        drop(seen);
        txn.abort().unwrap();
        assert_eq!(
            keys(db.range(None, b"k5".to_vec()..=b"k6".to_vec()).unwrap()),
            ["k5", "k6"],
            "aborted insert must vanish from a later scan"
        );
    }
}
