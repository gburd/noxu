//! Join cursor for multi-secondary-index intersection queries.
//!
//! Implements `JoinCursor`.  A join cursor is obtained by calling
//! [`Database::join`] with an array of
//! [`SecondaryCursor`] objects,
//! each pre-positioned at the desired secondary key value.
//!
//! # Algorithm
//!
//! The join algorithm is a faithful port of 's natural-join algorithm:
//!
//! 1. Iterate through all candidate primary keys from cursor C(0) —
//!    in are the "duplicate" records sharing C(0)'s secondary key.
//!    In Noxu's current one-to-one secondary model there is at most one
//!    candidate per secondary key position.
//!
//! 2. For each candidate primary key, probe cursors C(1) through C(n) to
//!    confirm the candidate also appears in each of their secondary keys.
//!    The probe does **not** read the primary database.
//!
//! 3. If all probes succeed, optionally read the primary record and
//!    return the key (and data if requested).
//!
//! 4. Cursor order matters: cursors by ascending duplicate count
//!    unless `JoinConfig::no_sort` is set.
//!
//! # Example
//!
//! ```ignore
//! let mut cur1 = sec_db1.open_cursor(None, None)?;
//! let mut cur2 = sec_db2.open_cursor(None, None)?;
//!
//! // Position each cursor at its desired secondary key.
//! cur1.get_search_key(&sec_key1, &mut p_key, &mut data)?;
//! cur2.get_search_key(&sec_key2, &mut p_key, &mut data)?;
//!
//! let mut join = primary_db.join(vec![cur1, cur2], None)?;
//!
//! let mut pri_key = DatabaseEntry::new();
//! let mut pri_data = DatabaseEntry::new();
//! while join.get_next(&mut pri_key, &mut pri_data)? == OperationStatus::Success {
//!     // use pri_key, pri_data
//! }
//! join.close();
//! ```

use crate::database::Database;
use crate::database_entry::DatabaseEntry;
use crate::error::Result;
use crate::join_config::JoinConfig;
use crate::operation_status::OperationStatus;
use crate::secondary_cursor::SecondaryCursor;

/// A cursor that returns records satisfying all secondary-key constraints.
///
/// Obtained via [`Database::join`][crate::database::Database::join].
///
/// The cursor owns the [`SecondaryCursor`] objects for the duration of the
/// join.  When [`close`][JoinCursor::close] is called (or the cursor is
/// dropped), the internal cursors are released.  The caller's original
/// cursor variables have been moved in so they are no longer accessible.
pub struct JoinCursor<'a> {
    /// Primary database (for final record retrieval).
    primary_db: &'a Database,
    /// Internal (optionally sorted) copies of the secondary cursors.
    cursors: Vec<SecondaryCursor<'a>>,
    config: JoinConfig,
    /// Pending candidate primary keys collected from cursor[0].
    candidates: std::collections::VecDeque<Vec<u8>>,
    /// `true` once there are no more candidates to process.
    exhausted: bool,
}

impl<'a> JoinCursor<'a> {
    /// Creates a new `JoinCursor`.
    ///
    /// Sorts `cursors` by ascending `count_estimate()` unless
    /// `config.no_sort` is `true`, mirroring 's optimisation.
    pub(crate) fn new(
        primary_db: &'a Database,
        mut cursors: Vec<SecondaryCursor<'a>>,
        config: Option<JoinConfig>,
    ) -> Result<Self> {
        let config = config.unwrap_or_default();

        if !config.no_sort && cursors.len() > 1 {
            // Collect estimates first (avoids repeated mutable borrows).
            let estimates: Vec<u64> =
                cursors.iter_mut().map(|c| c.count_estimate()).collect();
            // Stable sort by estimate ascending (smallest first = fewest candidates).
            let mut indexed: Vec<(usize, u64)> =
                estimates.iter().copied().enumerate().collect();
            indexed.sort_by_key(|&(_, est)| est);
            let order: Vec<usize> =
                indexed.into_iter().map(|(i, _)| i).collect();
            let mut sorted = Vec::with_capacity(cursors.len());
            let mut slots: Vec<Option<SecondaryCursor<'a>>> =
                cursors.into_iter().map(Some).collect();
            for idx in order {
                sorted.push(slots[idx].take().unwrap());
            }
            cursors = sorted;
        }

        // Collect the initial set of candidate primary keys from cursor[0].
        //  these are all "duplicate" records with the same secondary key.
        // In Noxu's current one-to-one secondary model there is at most one.
        let mut candidates = std::collections::VecDeque::new();
        if let Some(first) = cursors.first_mut()
            && let Some(pk) = first.get_current_primary_key_only()?
        {
            candidates.push_back(pk);
            // Collect all duplicates at this secondary key position.
            // For non-dup secondaries this loop runs at most once; for
            // sorted-dup secondaries it drains all entries sharing the
            // same secondary key value ( JoinCursor.getNext() pattern).
            while first.get_next_dup()? == OperationStatus::Success {
                if let Some(pk_extra) = first.get_current_primary_key_only()? {
                    candidates.push_back(pk_extra);
                }
            }
        }

        let exhausted = candidates.is_empty();
        Ok(Self { primary_db, cursors, config, candidates, exhausted })
    }

    /// Returns the next primary key **and** primary record data from the join.
    ///
    /// Returns `OperationStatus::Success` with `key` and `data` filled in,
    /// or `OperationStatus::NotFound` when there are no more matching records.
    pub fn get_next(
        &mut self,
        key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        loop {
            let candidate = match self.next_matching_candidate()? {
                Some(c) => c,
                None => return Ok(OperationStatus::NotFound),
            };

            // Fetch primary record.
            let pri_key_entry = DatabaseEntry::from_bytes(&candidate);
            let status = self.primary_db.get(None, &pri_key_entry, data)?;
            if status != OperationStatus::Success {
                // Primary was concurrently deleted (read-uncommitted path); skip.
                continue;
            }
            key.set_data(&candidate);
            return Ok(OperationStatus::Success);
        }
    }

    /// Returns the next primary key **only** — does not read primary data.
    ///
    /// Equivalent to 's `JoinCursor.getNext(key, lockMode)` single-arg
    /// overload.  Useful when only the key is needed and avoiding a primary
    /// read is desirable.
    pub fn get_next_key(
        &mut self,
        key: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        match self.next_matching_candidate()? {
            None => Ok(OperationStatus::NotFound),
            Some(candidate) => {
                key.set_data(&candidate);
                Ok(OperationStatus::Success)
            }
        }
    }

    /// Closes the join cursor, releasing all internal secondary cursors.
    pub fn close(self) {
        // Dropping self drops cursors via SecondaryCursor::drop.
    }

    /// Returns a reference to the primary database associated with this cursor.
    pub fn get_database(&self) -> &Database {
        self.primary_db
    }

    /// Returns a clone of this cursor's configuration.
    pub fn get_config(&self) -> JoinConfig {
        self.config.clone()
    }

    // ------------------------------------------------------------------
    // Internal join algorithm
    // ------------------------------------------------------------------

    /// Advances the join state and returns the next candidate primary key
    /// that satisfies all secondary-cursor probes, or `None` when exhausted.
    ///
    /// On each call:
    /// 1. Pop a candidate from the deque.  If empty, try to advance
    ///    cursor[0] to its next duplicate.
    /// 2. Probe cursor[1..n] — if any probe fails, loop to next candidate.
    /// 3. Return the matching candidate bytes.
    fn next_matching_candidate(&mut self) -> Result<Option<Vec<u8>>> {
        if self.exhausted {
            return Ok(None);
        }

        loop {
            // --- Refill candidates from cursor[0]'s next duplicate ---
            if self.candidates.is_empty() {
                match self.cursors[0].get_next_dup()? {
                    OperationStatus::Success => {
                        if let Some(pk) =
                            self.cursors[0].get_current_primary_key_only()?
                        {
                            self.candidates.push_back(pk);
                        }
                    }
                    _ => {
                        self.exhausted = true;
                        return Ok(None);
                    }
                }
            }

            let candidate = match self.candidates.pop_front() {
                Some(c) => c,
                None => {
                    self.exhausted = true;
                    return Ok(None);
                }
            };

            // --- Probe cursors[1..n] ---
            let mut all_match = true;
            for cursor in &mut self.cursors[1..] {
                if !cursor.has_candidate_primary_key(&candidate)? {
                    all_match = false;
                    break;
                }
            }

            if all_match {
                return Ok(Some(candidate));
            }
            // Probe failed — advance cursor[0] to next duplicate on next iter.
        }
    }
}

impl Drop for JoinCursor<'_> {
    fn drop(&mut self) {
        // SecondaryCursors in self.cursors are dropped here automatically.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use crate::database_config::DatabaseConfig;
    use crate::environment::Environment;
    use crate::environment_config::EnvironmentConfig;
    use crate::secondary_config::{SecondaryConfig, SecondaryKeyCreator};
    use crate::secondary_database::SecondaryDatabase;
    use noxu_sync::Mutex;
    use std::sync::Arc;
    use tempfile::TempDir;

    // ------------------------------------------------------------------
    // Helper key creators
    // ------------------------------------------------------------------

    /// Extracts the first byte of the data as the secondary key.
    struct FirstByteCreator;
    impl SecondaryKeyCreator for FirstByteCreator {
        fn create_secondary_key(
            &self,
            _db: &Database,
            _key: &DatabaseEntry,
            data: &DatabaseEntry,
            result: &mut DatabaseEntry,
        ) -> bool {
            if let Some(d) = data.get_data()
                && !d.is_empty()
            {
                result.set_data(&d[..1]);
                true
            } else {
                false
            }
        }
    }

    /// Extracts the last byte of the data as the secondary key.
    struct LastByteCreator;
    impl SecondaryKeyCreator for LastByteCreator {
        fn create_secondary_key(
            &self,
            _db: &Database,
            _key: &DatabaseEntry,
            data: &DatabaseEntry,
            result: &mut DatabaseEntry,
        ) -> bool {
            if let Some(d) = data.get_data()
                && !d.is_empty()
            {
                result.set_data(&d[d.len() - 1..]);
                true
            } else {
                false
            }
        }
    }

    // ------------------------------------------------------------------
    // Fixture
    // ------------------------------------------------------------------

    struct Fixture {
        _tmp: TempDir,
        _env: Environment,
        primary: Arc<Mutex<Database>>,
        sec1: SecondaryDatabase,
        sec2: SecondaryDatabase,
    }

    impl Fixture {
        fn new() -> Self {
            let tmp = TempDir::new().unwrap();
            let env_cfg = EnvironmentConfig::new(tmp.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true);
            let env = Environment::open(env_cfg).unwrap();

            let db_cfg = DatabaseConfig::new().with_allow_create(true);
            let pri_db = env.open_database(None, "primary", &db_cfg).unwrap();
            let primary = Arc::new(Mutex::new(pri_db));

            let sec_db_cfg = DatabaseConfig::new()
                .with_allow_create(true)
                .with_sorted_duplicates(true);
            let sec1_store =
                env.open_database(None, "sec1", &sec_db_cfg).unwrap();
            let sec1 = SecondaryDatabase::open(
                Arc::clone(&primary),
                sec1_store,
                SecondaryConfig::new()
                    .with_allow_create(true)
                    .with_key_creator(Box::new(FirstByteCreator)),
            )
            .unwrap();

            let sec2_store =
                env.open_database(None, "sec2", &sec_db_cfg).unwrap();
            let sec2 = SecondaryDatabase::open(
                Arc::clone(&primary),
                sec2_store,
                SecondaryConfig::new()
                    .with_allow_create(true)
                    .with_key_creator(Box::new(LastByteCreator)),
            )
            .unwrap();

            Fixture { _tmp: tmp, _env: env, primary, sec1, sec2 }
        }

        fn insert(&self, pk: &[u8], val: &[u8]) {
            let k = DatabaseEntry::from_bytes(pk);
            let v = DatabaseEntry::from_bytes(val);
            self.primary.lock().put(None, &k, &v).unwrap();
            self.sec1.update_secondary(None, &k, None, Some(&v)).unwrap();
            self.sec2.update_secondary(None, &k, None, Some(&v)).unwrap();
        }
    }

    // ------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------

    /// Two secondary cursors positioned at keys where only pk1 matches both.
    ///
    /// Data layout:
    ///   pk1 → b"AB"  (first byte 'A', last byte 'B')
    ///   pk2 → b"AC"  (first byte 'A', last byte 'C')
    ///   pk3 → b"XB"  (first byte 'X', last byte 'B')
    ///
    /// sec1 (first byte) at 'A' → {pk1, pk2}
    /// sec2 (last byte)  at 'B' → {pk1, pk3}
    /// Intersection → {pk1}
    ///
    /// v1.6 / wave 2A step 2: sorted-dup secondaries store every
    /// (sec_key, pri_key) pair so JoinCursor's duplicate-set walk
    /// finds the true intersection (closes audit finding F7).
    #[test]
    fn test_join_intersection_finds_single_match() {
        let fix = Fixture::new();
        fix.insert(b"pk1", b"AB");
        fix.insert(b"pk2", b"AC");
        fix.insert(b"pk3", b"XB");

        let mut cursor1 = fix.sec1.open_cursor(None, None).unwrap();
        {
            let mut p_key = DatabaseEntry::new();
            let mut data = DatabaseEntry::new();
            let s = cursor1
                .get_search_key(
                    &DatabaseEntry::from_bytes(b"A"),
                    &mut p_key,
                    &mut data,
                )
                .unwrap();
            assert_eq!(s, OperationStatus::Success);
        }

        let mut cursor2 = fix.sec2.open_cursor(None, None).unwrap();
        {
            let mut p_key = DatabaseEntry::new();
            let mut data = DatabaseEntry::new();
            let s = cursor2
                .get_search_key(
                    &DatabaseEntry::from_bytes(b"B"),
                    &mut p_key,
                    &mut data,
                )
                .unwrap();
            assert_eq!(s, OperationStatus::Success);
        }

        let pri_guard = fix.primary.lock();
        let mut join = pri_guard.join(vec![cursor1, cursor2], None).unwrap();

        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let status = join.get_next(&mut key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(key.get_data().unwrap(), b"pk1");
        assert_eq!(data.get_data().unwrap(), b"AB");

        // No more results.
        let status2 = join.get_next(&mut key, &mut data).unwrap();
        assert_eq!(status2, OperationStatus::NotFound);
    }

    /// Join over an empty secondary cursor returns NotFound immediately.
    #[test]
    fn test_join_empty_cursor_returns_not_found() {
        let fix = Fixture::new();

        let cursor1 = fix.sec1.open_cursor(None, None).unwrap();
        let cursor2 = fix.sec2.open_cursor(None, None).unwrap();

        // Cursors not positioned (no records) → join returns NotFound.
        let pri_guard = fix.primary.lock();
        let mut join = pri_guard.join(vec![cursor1, cursor2], None).unwrap();

        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let status = join.get_next(&mut key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    /// `get_next_key` returns only the primary key without primary data.
    #[test]
    fn test_join_get_next_key_only() {
        let fix = Fixture::new();
        fix.insert(b"mypk", b"AB");

        let mut cursor1 = fix.sec1.open_cursor(None, None).unwrap();
        {
            let mut p_key = DatabaseEntry::new();
            let mut data = DatabaseEntry::new();
            cursor1
                .get_search_key(
                    &DatabaseEntry::from_bytes(b"A"),
                    &mut p_key,
                    &mut data,
                )
                .unwrap();
        }
        let mut cursor2 = fix.sec2.open_cursor(None, None).unwrap();
        {
            let mut p_key = DatabaseEntry::new();
            let mut data = DatabaseEntry::new();
            cursor2
                .get_search_key(
                    &DatabaseEntry::from_bytes(b"B"),
                    &mut p_key,
                    &mut data,
                )
                .unwrap();
        }

        let pri_guard = fix.primary.lock();
        let mut join = pri_guard.join(vec![cursor1, cursor2], None).unwrap();

        let mut key = DatabaseEntry::new();
        let status = join.get_next_key(&mut key).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(key.get_data().unwrap(), b"mypk");
    }

    /// `no_sort = true` preserves cursor order and still finds the match.
    #[test]
    fn test_join_config_no_sort() {
        let fix = Fixture::new();
        fix.insert(b"pk1", b"AB");

        let mut cursor1 = fix.sec1.open_cursor(None, None).unwrap();
        {
            let mut p_key = DatabaseEntry::new();
            let mut data = DatabaseEntry::new();
            cursor1
                .get_search_key(
                    &DatabaseEntry::from_bytes(b"A"),
                    &mut p_key,
                    &mut data,
                )
                .unwrap();
        }
        let mut cursor2 = fix.sec2.open_cursor(None, None).unwrap();
        {
            let mut p_key = DatabaseEntry::new();
            let mut data = DatabaseEntry::new();
            cursor2
                .get_search_key(
                    &DatabaseEntry::from_bytes(b"B"),
                    &mut p_key,
                    &mut data,
                )
                .unwrap();
        }

        let config = JoinConfig::new().with_no_sort(true);
        let pri_guard = fix.primary.lock();
        let mut join =
            pri_guard.join(vec![cursor1, cursor2], Some(config)).unwrap();
        assert!(join.get_config().no_sort);

        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let status = join.get_next(&mut key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(key.get_data().unwrap(), b"pk1");
    }

    /// No match when secondary keys do not overlap.
    #[test]
    fn test_join_no_intersection() {
        let fix = Fixture::new();
        // pk1: first='A', last='A'
        // pk2: first='B', last='B'
        // sec1 at 'A' → pk1, sec2 at 'B' → pk2 — no intersection.
        fix.insert(b"pk1", b"AA");
        fix.insert(b"pk2", b"BB");

        let mut cursor1 = fix.sec1.open_cursor(None, None).unwrap();
        {
            let mut p_key = DatabaseEntry::new();
            let mut data = DatabaseEntry::new();
            cursor1
                .get_search_key(
                    &DatabaseEntry::from_bytes(b"A"),
                    &mut p_key,
                    &mut data,
                )
                .unwrap();
        }
        let mut cursor2 = fix.sec2.open_cursor(None, None).unwrap();
        {
            let mut p_key = DatabaseEntry::new();
            let mut data = DatabaseEntry::new();
            cursor2
                .get_search_key(
                    &DatabaseEntry::from_bytes(b"B"),
                    &mut p_key,
                    &mut data,
                )
                .unwrap();
        }

        let pri_guard = fix.primary.lock();
        let mut join = pri_guard.join(vec![cursor1, cursor2], None).unwrap();

        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let status = join.get_next(&mut key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    /// Single-cursor join acts as a filtered scan over one secondary.
    #[test]
    fn test_join_single_cursor() {
        let fix = Fixture::new();
        fix.insert(b"pk1", b"AB");

        let mut cursor1 = fix.sec1.open_cursor(None, None).unwrap();
        {
            let mut p_key = DatabaseEntry::new();
            let mut data = DatabaseEntry::new();
            cursor1
                .get_search_key(
                    &DatabaseEntry::from_bytes(b"A"),
                    &mut p_key,
                    &mut data,
                )
                .unwrap();
        }

        let pri_guard = fix.primary.lock();
        let mut join = pri_guard.join(vec![cursor1], None).unwrap();

        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let status = join.get_next(&mut key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(key.get_data().unwrap(), b"pk1");
    }

    /// `get_database()` returns the primary database.
    #[test]
    fn test_join_get_database() {
        let fix = Fixture::new();
        fix.insert(b"pk1", b"AB");

        let mut cursor1 = fix.sec1.open_cursor(None, None).unwrap();
        {
            let mut p_key = DatabaseEntry::new();
            let mut data = DatabaseEntry::new();
            cursor1
                .get_search_key(
                    &DatabaseEntry::from_bytes(b"A"),
                    &mut p_key,
                    &mut data,
                )
                .unwrap();
        }

        let pri_guard = fix.primary.lock();
        let join = pri_guard.join(vec![cursor1], None).unwrap();
        assert_eq!(join.get_database().get_database_name(), "primary");
    }

    /// `close()` releases the cursors without panicking.
    #[test]
    fn test_join_close() {
        let fix = Fixture::new();
        fix.insert(b"pk1", b"AB");

        let mut cursor1 = fix.sec1.open_cursor(None, None).unwrap();
        {
            let mut p_key = DatabaseEntry::new();
            let mut data = DatabaseEntry::new();
            cursor1
                .get_search_key(
                    &DatabaseEntry::from_bytes(b"A"),
                    &mut p_key,
                    &mut data,
                )
                .unwrap();
        }

        let pri_guard = fix.primary.lock();
        let join = pri_guard.join(vec![cursor1], None).unwrap();
        join.close(); // must not panic
    }
}
