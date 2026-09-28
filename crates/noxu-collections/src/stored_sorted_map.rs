//! Typed sorted-map view of a database.
//!
//! `StoredSortedMap<K, V, KB, VB>` adds
//! sorted-map operations (`first_key`, `last_key`, `iter_from`,
//! `iter_reverse`) on top of [`StoredMap`].  Every operation accepts
//! `txn: Option<&Transaction>`, matching the BDB-JE shape.

use noxu_bind::EntryBinding;
use noxu_db::{Database, Transaction};

use crate::error::Result;
use crate::internal::{
    ScanDirection, StartKey, cursor_endpoint, decode_key, encode_key,
    scan_iter, scan_iter_owned_start,
};
use crate::stored_iterator::StoredIterator;
use crate::stored_map::StoredMap;

/// A typed sorted-map view of a database.
///
/// All `StoredMap` operations are forwarded to the inner map; this
/// type adds sorted-map navigation (`first_key`, `last_key`,
/// `iter_from`, `iter_reverse`).
pub struct StoredSortedMap<'db, K, V, KB, VB>
where
    KB: EntryBinding<K>,
    VB: EntryBinding<V>,
{
    inner: StoredMap<'db, K, V, KB, VB>,
}

impl<'db, K, V, KB, VB> StoredSortedMap<'db, K, V, KB, VB>
where
    KB: EntryBinding<K>,
    VB: EntryBinding<V>,
{
    /// Creates a new typed sorted-map view of the given database.
    pub fn new(db: &'db Database, key_binding: KB, value_binding: VB) -> Self {
        StoredSortedMap {
            inner: StoredMap::new(db, key_binding, value_binding),
        }
    }

    /// Creates a new read-only typed sorted-map view.
    pub fn new_read_only(
        db: &'db Database,
        key_binding: KB,
        value_binding: VB,
    ) -> Self {
        StoredSortedMap {
            inner: StoredMap::new_read_only(db, key_binding, value_binding),
        }
    }

    /// Returns whether this view is read-only.
    pub fn is_read_only(&self) -> bool {
        self.inner.is_read_only()
    }

    /// Returns a reference to the underlying database.
    pub fn database(&self) -> &'db Database {
        self.inner.database()
    }

    /// Returns a reference to the inner [`StoredMap`].
    pub fn as_map(&self) -> &StoredMap<'db, K, V, KB, VB> {
        &self.inner
    }

    /// Inserts or updates a key-value pair.  See [`StoredMap::put`].
    pub fn put(
        &self,
        txn: Option<&Transaction>,
        key: &K,
        value: &V,
    ) -> Result<Option<V>> {
        self.inner.put(txn, key, value)
    }

    /// Retrieves the value associated with the given key.
    pub fn get(&self, txn: Option<&Transaction>, key: &K) -> Result<Option<V>> {
        self.inner.get(txn, key)
    }

    /// Removes the entry for `key`.
    pub fn remove(
        &self,
        txn: Option<&Transaction>,
        key: &K,
    ) -> Result<Option<V>> {
        self.inner.remove(txn, key)
    }

    /// Returns whether `key` is present.
    pub fn contains_key(
        &self,
        txn: Option<&Transaction>,
        key: &K,
    ) -> Result<bool> {
        self.inner.contains_key(txn, key)
    }

    /// Returns the number of records.
    pub fn len(&self, txn: Option<&Transaction>) -> Result<usize> {
        self.inner.len(txn)
    }

    /// Returns whether the database is empty.
    pub fn is_empty(&self, txn: Option<&Transaction>) -> Result<bool> {
        self.inner.is_empty(txn)
    }

    /// Removes every record.
    pub fn clear(&self, txn: Option<&Transaction>) -> Result<()> {
        self.inner.clear(txn)
    }

    /// Lazy forward iterator over every (key, value) pair (review P1-7).
    /// See [`StoredMap::iter`](crate::StoredMap::iter) for the
    /// laziness/lifetime contract.
    pub fn iter<'a>(
        &'a self,
        txn: Option<&'a Transaction>,
    ) -> Result<impl Iterator<Item = Result<(K, V)>> + 'a>
    where
        K: 'a,
        V: 'a,
    {
        self.inner.iter(txn)
    }

    /// Lazy forward iterator over keys.
    pub fn keys<'a>(
        &'a self,
        txn: Option<&'a Transaction>,
    ) -> Result<impl Iterator<Item = Result<K>> + 'a>
    where
        K: 'a,
        V: 'a,
    {
        self.inner.keys(txn)
    }

    /// Lazy forward iterator over values.
    pub fn values<'a>(
        &'a self,
        txn: Option<&'a Transaction>,
    ) -> Result<impl Iterator<Item = Result<V>> + 'a>
    where
        K: 'a,
        V: 'a,
    {
        self.inner.values(txn)
    }

    /// Eager snapshot iterator over every (key, value) pair.
    /// See [`StoredMap::snapshot`](crate::StoredMap::snapshot).
    pub fn snapshot(
        &self,
        txn: Option<&Transaction>,
    ) -> Result<StoredIterator<(K, V)>> {
        self.inner.snapshot(txn)
    }

    /// Eager snapshot iterator over keys.
    pub fn keys_snapshot(
        &self,
        txn: Option<&Transaction>,
    ) -> Result<StoredIterator<K>> {
        self.inner.keys_snapshot(txn)
    }

    /// Eager snapshot iterator over values.
    pub fn values_snapshot(
        &self,
        txn: Option<&Transaction>,
    ) -> Result<StoredIterator<V>> {
        self.inner.values_snapshot(txn)
    }

    /// Returns the smallest key, or `None` if the database is empty.
    pub fn first_key(&self, txn: Option<&Transaction>) -> Result<Option<K>> {
        Ok(self.first_entry(txn)?.map(|(k, _)| k))
    }

    /// Returns the largest key, or `None` if the database is empty.
    pub fn last_key(&self, txn: Option<&Transaction>) -> Result<Option<K>> {
        Ok(self.last_entry(txn)?.map(|(k, _)| k))
    }

    /// Returns the (key, value) pair with the smallest key, or `None`.
    pub fn first_entry(
        &self,
        txn: Option<&Transaction>,
    ) -> Result<Option<(K, V)>> {
        cursor_endpoint(
            self.inner.database(),
            txn,
            self.inner.key_binding(),
            self.inner.value_binding(),
            noxu_db::Get::First,
        )
    }

    /// Returns the (key, value) pair with the largest key, or `None`.
    pub fn last_entry(
        &self,
        txn: Option<&Transaction>,
    ) -> Result<Option<(K, V)>> {
        cursor_endpoint(
            self.inner.database(),
            txn,
            self.inner.key_binding(),
            self.inner.value_binding(),
            noxu_db::Get::Last,
        )
    }

    /// Lazy forward iterator starting at `start_key` (inclusive lower
    /// bound).
    ///
    /// Encodes `start_key` via the key binding and walks the cursor
    /// from the smallest key `>= encoded(start_key)`.  Lazy (review
    /// P1-7); see [`iter`](Self::iter) for the lifetime contract.
    pub fn iter_from<'a>(
        &'a self,
        txn: Option<&'a Transaction>,
        start_key: &K,
    ) -> Result<impl Iterator<Item = Result<(K, V)>> + 'a>
    where
        K: 'a,
        V: 'a,
    {
        let start_entry = encode_key(self.inner.key_binding(), start_key)?;
        let bytes = start_entry.data_opt().unwrap_or(&[]).to_vec();
        scan_iter_owned_start(
            self.inner.database(),
            txn,
            Some(bytes),
            ScanDirection::Forward,
            self.inner.key_binding(),
            self.inner.value_binding(),
            |k, v| (k, v),
        )
    }

    /// Lazy reverse iterator over every (key, value) pair (largest key
    /// first).  See [`iter`](Self::iter) for the lifetime contract.
    pub fn iter_reverse<'a>(
        &'a self,
        txn: Option<&'a Transaction>,
    ) -> Result<impl Iterator<Item = Result<(K, V)>> + 'a>
    where
        K: 'a,
        V: 'a,
    {
        scan_iter(
            self.inner.database(),
            txn,
            StartKey::None,
            ScanDirection::Reverse,
            self.inner.key_binding(),
            self.inner.value_binding(),
            |k, v| (k, v),
        )
    }

    /// Returns the smallest key strictly greater than `key`, or `None`.
    ///
    /// Useful for stepping through keys when only the bindings are
    /// available.  Walks forward from `Get::First` and skips keys
    /// `<= bound` (the `noxu-dbi` `SearchGte`-then-`Next` path is
    /// known to mis-position; see `internal::scan_records` for the
    /// rationale).
    pub fn higher_key(
        &self,
        txn: Option<&Transaction>,
        key: &K,
    ) -> Result<Option<K>> {
        let key_entry = encode_key(self.inner.key_binding(), key)?;
        let bound = key_entry.data_opt().unwrap_or(&[]).to_vec();

        let mut cursor =
            crate::internal::open_cursor(self.inner.database(), txn, None)?;
        let mut k_buf = noxu_db::DatabaseEntry::new();
        let mut d_buf = noxu_db::DatabaseEntry::new();
        let mut status =
            cursor.get(&mut k_buf, &mut d_buf, noxu_db::Get::First, None)?;
        let mut result: Option<K> = None;
        while matches!(status, noxu_db::OperationStatus::Success) {
            let cur = k_buf.data_opt().unwrap_or(&[]);
            if cur > bound.as_slice() {
                result = Some(decode_key(self.inner.key_binding(), &k_buf)?);
                break;
            }
            status =
                cursor.get(&mut k_buf, &mut d_buf, noxu_db::Get::Next, None)?;
        }
        cursor.close()?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_bind::{IntBinding, StringBinding};
    use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
    use tempfile::TempDir;

    fn setup() -> (TempDir, Environment, noxu_db::Database) {
        let td = TempDir::new().unwrap();
        let env = Environment::open(
            EnvironmentConfig::new(td.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
        let db = env
            .open_database(
                None,
                "ssm",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();
        (td, env, db)
    }

    fn populate(
        map: &StoredSortedMap<'_, i32, String, IntBinding, StringBinding>,
    ) {
        for (k, v) in
            [(3, "three"), (1, "one"), (2, "two"), (5, "five"), (4, "four")]
        {
            map.put(None, &k, &v.to_string()).unwrap();
        }
    }

    #[test]
    fn first_and_last_key() {
        let (_td, _env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);
        populate(&map);

        assert_eq!(map.first_key(None).unwrap(), Some(1));
        assert_eq!(map.last_key(None).unwrap(), Some(5));
    }

    #[test]
    fn first_and_last_entry() {
        let (_td, _env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);
        populate(&map);

        assert_eq!(
            map.first_entry(None).unwrap(),
            Some((1, "one".to_string())),
        );
        assert_eq!(
            map.last_entry(None).unwrap(),
            Some((5, "five".to_string())),
        );
    }

    #[test]
    fn first_last_empty() {
        let (_td, _env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);
        assert_eq!(map.first_key(None).unwrap(), None);
        assert_eq!(map.last_key(None).unwrap(), None);
        assert_eq!(map.first_entry(None).unwrap(), None);
        assert_eq!(map.last_entry(None).unwrap(), None);
    }

    #[test]
    fn iter_reverse() {
        let (_td, _env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);
        populate(&map);

        let items: Vec<_> =
            map.iter_reverse(None).unwrap().map(Result::unwrap).collect();
        let keys: Vec<i32> = items.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![5, 4, 3, 2, 1]);
    }

    #[test]
    fn iter_from_inclusive() {
        let (_td, _env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);
        populate(&map);

        let items: Vec<_> =
            map.iter_from(None, &3).unwrap().map(Result::unwrap).collect();
        let keys: Vec<i32> = items.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![3, 4, 5]);
    }

    #[test]
    fn iter_from_between_keys() {
        let (_td, _env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);
        // 1, 2, 4, 5
        for k in [1, 2, 4, 5] {
            map.put(None, &k, &format!("{k}")).unwrap();
        }
        // start key 3 → smallest key >= 3 is 4
        let items: Vec<_> =
            map.iter_from(None, &3).unwrap().map(Result::unwrap).collect();
        let keys: Vec<i32> = items.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![4, 5]);
    }

    #[test]
    fn higher_key() {
        let (_td, _env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);
        populate(&map);

        assert_eq!(map.higher_key(None, &1).unwrap(), Some(2));
        assert_eq!(map.higher_key(None, &3).unwrap(), Some(4));
        assert_eq!(map.higher_key(None, &5).unwrap(), None);
        // For a key not in the map, we get the smallest key strictly
        // greater than it.  IntBinding sorts ints two's-complement so
        // 0 < 1 < ... < 5.
        assert_eq!(map.higher_key(None, &0).unwrap(), Some(1));
    }

    #[test]
    fn participates_in_user_txn() {
        let (_td, env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);

        let txn = env.begin_transaction(None).unwrap();
        map.put(Some(&txn), &1, &"one".to_string()).unwrap();
        map.put(Some(&txn), &2, &"two".to_string()).unwrap();
        assert_eq!(map.first_key(Some(&txn)).unwrap(), Some(1));
        txn.commit().unwrap();

        assert_eq!(map.first_key(None).unwrap(), Some(1));
    }

    /// `new_read_only` must reject mutation and `is_read_only`/`database`
    /// must report accurately -- proves the read-only construction path
    /// (distinct from `new`) actually threads through to `StoredMap` and
    /// isn't silently ignored.
    #[test]
    fn new_read_only_rejects_writes_and_reports_state() {
        let (_td, _env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);
        map.put(None, &1, &"one".to_string()).unwrap();

        let ro: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new_read_only(&db, IntBinding, StringBinding);
        assert!(ro.is_read_only());
        assert_eq!(ro.database() as *const _, &db as *const _);

        // Reads still work.
        assert_eq!(ro.get(None, &1).unwrap(), Some("one".to_string()));

        // Writes must be rejected.
        let err = ro.put(None, &2, &"two".to_string()).unwrap_err();
        assert!(matches!(err, crate::error::CollectionError::ReadOnly));
    }

    /// `clear` must remove every entry, and the sorted navigation methods
    /// (`first_key` / `last_key`) must reflect the emptied map afterwards.
    #[test]
    fn clear_empties_the_map() {
        let (_td, _env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);
        populate(&map);
        assert_eq!(map.first_key(None).unwrap(), Some(1));

        map.clear(None).unwrap();

        assert_eq!(map.first_key(None).unwrap(), None);
        assert_eq!(map.last_key(None).unwrap(), None);
    }

    /// The delegating `iter` / `keys` / `values` (lazy) accessors must
    /// yield the same records as the sorted-navigation-specific methods
    /// already tested above -- these three just forward to the inner
    /// `StoredMap`, but the forwarding itself was previously untested.
    #[test]
    fn iter_keys_values_delegate_correctly() {
        let (_td, _env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);
        populate(&map);

        let mut pairs: Vec<(i32, String)> =
            map.iter(None).unwrap().map(Result::unwrap).collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                (1, "one".to_string()),
                (2, "two".to_string()),
                (3, "three".to_string()),
                (4, "four".to_string()),
                (5, "five".to_string()),
            ]
        );

        let mut keys: Vec<i32> =
            map.keys(None).unwrap().map(Result::unwrap).collect();
        keys.sort();
        assert_eq!(keys, vec![1, 2, 3, 4, 5]);

        let mut values: Vec<String> =
            map.values(None).unwrap().map(Result::unwrap).collect();
        values.sort();
        assert_eq!(
            values,
            vec!["five", "four", "one", "three", "two"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>()
        );
    }

    /// The eager `snapshot` / `keys_snapshot` / `values_snapshot`
    /// delegating accessors must also forward correctly -- distinct code
    /// path from the lazy `iter`/`keys`/`values` above (materialises a
    /// `Vec` up front via `StoredIterator::from_vec`).
    #[test]
    fn eager_snapshots_delegate_correctly() {
        let (_td, _env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);
        populate(&map);

        assert_eq!(map.snapshot(None).unwrap().count(), 5);
        assert_eq!(map.keys_snapshot(None).unwrap().count(), 5);
        assert_eq!(map.values_snapshot(None).unwrap().count(), 5);
    }

    // ───────────────────────────────────────────────────────────────────
    // JE parity: com.sleepycat.collections.KeyRangeTest
    //
    // JE's KeyRangeTest exercises com.sleepycat.util.keyrange.KeyRange —
    // the bounded key-range abstraction underpinning StoredSortedMap's
    // subMap(from,to)/headMap(to)/tailMap(from): a begin-key + end-key
    // pair with begin-inclusive / end-inclusive flags, key containment
    // (is K in [begin,end]?), sub-range intersection (subRange), and
    // unsigned-byte comparator ordering.
    //
    // Noxu's collections layer has no `KeyRange` *class* (a JE-internal
    // shape) and its StoredSortedMap exposes bounded scans only as an
    // inclusive lower bound (`iter_from`) plus full-range / reverse
    // scans — it does not expose a begin+end inclusive/exclusive bounded
    // subMap collection view.  The *portable* content of KeyRangeTest is
    // the range CONTAINMENT + inclusive/exclusive BOUND arithmetic + the
    // subRange intersection-validation algorithm and the comparator
    // ordering.  Those we port faithfully:
    //
    //  * `KeyRange` below is a faithful, test-local port of JE's
    //    KeyRange.check / checkBegin / checkEnd / subRange (same
    //    algorithm, same names), over raw `Vec<u8>` keys.
    //  * `testScan` / `testScanComparator`'s begin/end × inclusive/
    //    exclusive matrix over the exact JE KEYS[] array is asserted
    //    against that containment predicate (this is precisely what JE's
    //    `checkRange` proves via cursor navigation: which keys of KEYS[]
    //    fall in each range) — plus the DB-observable inclusive-begin
    //    subset is verified against a real StoredSortedMap.
    //  * The reverse comparator is exercised both at the KeyRange
    //    predicate level and end-to-end against a real DB opened with a
    //    reverse btree comparator.
    //
    // The full JE DataCursor forward+reverse navigation matrix over a
    // begin+end bounded cursor is N/A at the collections layer (Noxu has
    // no begin+end bounded StoredSortedMap view); the underlying
    // comparator-ordered cursor scan itself is covered by noxu-db's
    // dbi14_comparator_test.  See report tp-collections-top.md.

    /// Unsigned-byte comparison, JE `KeyRange.compareBytes`: the default
    /// JE/DB ordering.  `Vec<u8>`'s natural `Ord` already compares
    /// lexicographically over `u8` (unsigned), so this is the identity of
    /// `a.cmp(b)` — spelled out to match the JE algorithm name/intent.
    fn compare_bytes(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b)
    }

    /// Faithful test-local port of `com.sleepycat.util.keyrange.KeyRange`
    /// (JE: KeyRange.java) — begin/end bounds with inclusive/exclusive
    /// flags, containment (`check`), and sub-range intersection
    /// (`sub_range`).  Comparator is an optional key ordering (JE's
    /// `Comparator<byte[]>`); `None` means default unsigned-byte order.
    #[derive(Clone, Debug)]
    struct KeyRange {
        comparator: Option<fn(&[u8], &[u8]) -> std::cmp::Ordering>,
        begin_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
        begin_inclusive: bool,
        end_inclusive: bool,
    }

    /// JE: KeyRangeException — a requested sub-range is not contained in
    /// the parent range.
    #[derive(Debug, PartialEq, Eq)]
    struct KeyRangeException(&'static str);

    impl KeyRange {
        /// JE: `KeyRange(Comparator)` — an unconstrained range.
        fn new(
            comparator: Option<fn(&[u8], &[u8]) -> std::cmp::Ordering>,
        ) -> Self {
            KeyRange {
                comparator,
                begin_key: None,
                end_key: None,
                begin_inclusive: false,
                end_inclusive: false,
            }
        }

        /// JE: `KeyRange.compare` — user comparator if present, else
        /// unsigned-byte order.
        fn compare(&self, k1: &[u8], k2: &[u8]) -> std::cmp::Ordering {
            match self.comparator {
                Some(c) => c(k1, k2),
                None => compare_bytes(k1, k2),
            }
        }

        /// JE: `KeyRange.checkBegin`.  `inclusive=true` when checking a key
        /// read from the database (must be within range); `inclusive=false`
        /// when checking a new exclusive sub-range bound (allowed to equal
        /// beginKey).
        fn check_begin(&self, key: &[u8], inclusive: bool) -> bool {
            match &self.begin_key {
                None => true,
                Some(bk) => {
                    if !self.begin_inclusive && inclusive {
                        self.compare(key, bk) == std::cmp::Ordering::Greater
                    } else {
                        self.compare(key, bk) != std::cmp::Ordering::Less
                    }
                }
            }
        }

        /// JE: `KeyRange.checkEnd`.
        fn check_end(&self, key: &[u8], inclusive: bool) -> bool {
            match &self.end_key {
                None => true,
                Some(ek) => {
                    if !self.end_inclusive && inclusive {
                        self.compare(key, ek) == std::cmp::Ordering::Less
                    } else {
                        self.compare(key, ek) != std::cmp::Ordering::Greater
                    }
                }
            }
        }

        /// JE: `KeyRange.check(key)` — is `key` within range (as a DB key)?
        fn check(&self, key: &[u8]) -> bool {
            self.check_begin(key, true) && self.check_end(key, true)
        }

        /// JE: `KeyRange.subRange(begin, beginInclusive, end, endInclusive)`
        /// — the intersection of this range with the given bounds.  A new
        /// bound outside the parent range raises KeyRangeException.
        fn sub_range(
            &self,
            begin_key: &[u8],
            begin_inclusive: bool,
            end_key: &[u8],
            end_inclusive: bool,
        ) -> std::result::Result<KeyRange, KeyRangeException> {
            // JE KeyRange.subRange validates each new bound with the FULL
            // check() = checkBegin && checkEnd (not a single-sided check), so a
            // new begin that violates the parent's END constraint (e.g. an
            // empty [k,k) request) is reported as "beginKey out of range".
            if !(self.check_begin(begin_key, begin_inclusive)
                && self.check_end(begin_key, begin_inclusive))
            {
                return Err(KeyRangeException("beginKey out of range"));
            }
            if !(self.check_begin(end_key, end_inclusive)
                && self.check_end(end_key, end_inclusive))
            {
                return Err(KeyRangeException("endKey out of range"));
            }
            Ok(KeyRange {
                comparator: self.comparator,
                begin_key: Some(begin_key.to_vec()),
                end_key: Some(end_key.to_vec()),
                begin_inclusive,
                end_inclusive,
            })
        }
    }

    const FF: u8 = 0xFF;

    /// JE: KeyRangeTest.KEYS — the ordered key set used by testScan.
    fn keys() -> Vec<Vec<u8>> {
        vec![
            vec![1],            // 0
            vec![FF],           // 1
            vec![FF, 0],        // 2
            vec![FF, 0x7F],     // 3
            vec![FF, FF],       // 4
            vec![FF, FF, 0],    // 5
            vec![FF, FF, 0x7F], // 6
            vec![FF, FF, FF],   // 7
        ]
    }

    /// Collect the indices of KEYS[] that a range containing begin[i..] /
    /// end[..=j] (with the given inclusive/exclusive flags) admits, using
    /// KeyRange::check — the observable "which keys are in range" that JE's
    /// `checkRange`/`expectRange` proves via cursor navigation.
    fn keys_in_range(range: &KeyRange, ks: &[Vec<u8>]) -> Vec<usize> {
        ks.iter()
            .enumerate()
            .filter(|(_, k)| range.check(k))
            .map(|(i, _)| i)
            .collect()
    }

    /// JE: KeyRangeTest.testScan (containment subset).
    ///
    /// JE's testScan opens a DB, inserts KEYS[0..=7], then for every
    /// (begin, beginInclusive, end, endInclusive) combination walks a
    /// bounded cursor forwards and backwards and asserts exactly
    /// KEYS[i..=j] (adjusted for exclusivity) is returned.  We assert the
    /// same admitted-index set via KeyRange::check over the exact KEYS[]
    /// array — the bound arithmetic JE proves.  The inclusive-begin subset
    /// is additionally verified against a real StoredSortedMap below.
    #[test]
    fn key_range_scan_containment() {
        let ks = keys();
        let end = ks.len() - 1; // 7
        let base = KeyRange::new(None); // unconstrained

        // Empty range: all keys.
        assert_eq!(keys_in_range(&base, &ks), (0..=end).collect::<Vec<_>>());

        // Begin key only, inclusive → KEYS[i..=end].
        for i in 0..=end {
            let r = base
                .sub_range(&ks[i], true, &ks[end], true)
                .expect("inclusive begin within base");
            assert_eq!(
                keys_in_range(&r, &ks),
                (i..=end).collect::<Vec<_>>(),
                "begin inclusive i={i}",
            );
        }

        // Begin key only, exclusive → KEYS[(i+1)..=end].
        for i in 0..=end {
            let r = base
                .sub_range(&ks[i], false, &ks[end], true)
                .expect("exclusive begin within base");
            assert_eq!(
                keys_in_range(&r, &ks),
                ((i + 1)..=end).collect::<Vec<_>>(),
                "begin exclusive i={i}",
            );
        }

        // End key only, inclusive → KEYS[0..=i].
        for i in 0..=end {
            let r = base
                .sub_range(&ks[0], true, &ks[i], true)
                .expect("inclusive end within base");
            assert_eq!(
                keys_in_range(&r, &ks),
                (0..=i).collect::<Vec<_>>(),
                "end inclusive i={i}",
            );
        }

        // End key only, exclusive → KEYS[0..=(i-1)] (empty when i==0).
        for i in 0..=end {
            let r = base
                .sub_range(&ks[0], true, &ks[i], false)
                .expect("exclusive end within base");
            let expect: Vec<usize> =
                if i == 0 { vec![] } else { (0..=(i - 1)).collect() };
            assert_eq!(keys_in_range(&r, &ks), expect, "end exclusive i={i}");
        }

        // Begin and end, all four inclusive/exclusive combinations.
        for i in 0..=end {
            for j in i..=end {
                // begin inclusive, end inclusive → [i, j]
                let r = base.sub_range(&ks[i], true, &ks[j], true).unwrap();
                assert_eq!(
                    keys_in_range(&r, &ks),
                    (i..=j).collect::<Vec<_>>(),
                    "incl/incl i={i} j={j}",
                );
                // begin inclusive, end exclusive → [i, j-1] (empty if j<=i)
                let r = base.sub_range(&ks[i], true, &ks[j], false).unwrap();
                let expect: Vec<usize> =
                    if j > i { (i..j).collect() } else { vec![] };
                assert_eq!(
                    keys_in_range(&r, &ks),
                    expect,
                    "incl/excl i={i} j={j}",
                );
                // begin exclusive, end inclusive → [i+1, j]
                let r = base.sub_range(&ks[i], false, &ks[j], true).unwrap();
                let expect: Vec<usize> =
                    if j > i { ((i + 1)..=j).collect() } else { vec![] };
                assert_eq!(
                    keys_in_range(&r, &ks),
                    expect,
                    "excl/incl i={i} j={j}",
                );
                // begin exclusive, end exclusive → [i+1, j-1]
                let r = base.sub_range(&ks[i], false, &ks[j], false).unwrap();
                let expect: Vec<usize> =
                    if j > i + 1 { ((i + 1)..j).collect() } else { vec![] };
                assert_eq!(
                    keys_in_range(&r, &ks),
                    expect,
                    "excl/excl i={i} j={j}",
                );
            }
        }
    }

    /// JE: KeyRangeTest.testScan (DB-observable inclusive-begin subset).
    ///
    /// The one bounded shape Noxu's StoredSortedMap exposes is an
    /// inclusive lower bound (`iter_from`).  Insert KEYS[] into a real DB
    /// and assert `iter_from(KEYS[i])` yields exactly KEYS[i..=end] — the
    /// same inclusive-begin bound the containment test proves, now against
    /// the engine.  (Vec<u8> keys sort by unsigned byte order, matching
    /// JE's default DB order for these keys.)
    #[test]
    fn key_range_scan_iter_from_matches_db() {
        let (_td, _env, db) = setup();
        let map: StoredSortedMap<'_, Vec<u8>, Vec<u8>, _, _> =
            StoredSortedMap::new(
                &db,
                noxu_bind::ByteArrayBinding,
                noxu_bind::ByteArrayBinding,
            );
        let ks = keys();
        for k in &ks {
            map.put(None, k, k).unwrap();
        }
        let end = ks.len() - 1;
        for i in 0..=end {
            let got: Vec<Vec<u8>> = map
                .iter_from(None, &ks[i])
                .unwrap()
                .map(|r| r.unwrap().0)
                .collect();
            let expect: Vec<Vec<u8>> = ks[i..=end].to_vec();
            assert_eq!(got, expect, "iter_from KEYS[{i}]");
        }
        // Lower extreme (before any key) yields all.
        let all: Vec<Vec<u8>> = map
            .iter_from(None, &vec![0u8])
            .unwrap()
            .map(|r| r.unwrap().0)
            .collect();
        assert_eq!(all, ks, "iter_from lower-extreme yields all");
    }

    /// JE: KeyRangeTest.testScanComparator (containment subset).
    ///
    /// JE re-runs testScan with a ReverseComparator (descending byte
    /// order).  Under a reverse comparator, the KEYS[] array — which is in
    /// ascending order — is *descending* per the comparator, so a range
    /// bounded by begin=KEYS[i], end=KEYS[j] (i<=j ascending) is empty
    /// unless i==j: begin > end under the reverse order.  Assert the
    /// containment predicate honours the comparator: a single-key range
    /// admits exactly that key, and check() flips with the ordering.
    #[test]
    fn key_range_scan_comparator_containment() {
        fn reverse(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
            compare_bytes(b, a)
        }
        let ks = keys();
        let base = KeyRange::new(Some(reverse));

        // Single-key range [k, k] admits exactly that key under either
        // ordering.
        for i in 0..ks.len() {
            let r = base.sub_range(&ks[i], true, &ks[i], true).unwrap();
            assert_eq!(keys_in_range(&r, &ks), vec![i], "single-key i={i}");
        }

        // Under the reverse comparator KEYS[0] (=[1]) is the reverse-order
        // MAXIMUM (byte-smallest) and KEYS[7] (=[FF,FF,FF]) is the
        // reverse-order MINIMUM (byte-largest).
        //
        // begin=KEYS[7] (the reverse-order minimum), inclusive, no end →
        // every key is >= the minimum under the reverse ordering, so all
        // keys are admitted.
        let full = KeyRange {
            comparator: Some(reverse),
            begin_key: Some(ks[7].clone()),
            end_key: None,
            begin_inclusive: true,
            end_inclusive: false,
        };
        assert_eq!(
            keys_in_range(&full, &ks),
            (0..ks.len()).collect::<Vec<_>>(),
            "reverse begin=KEYS[7] (reverse-min) admits all",
        );

        // begin=KEYS[0] (the reverse-order MAXIMUM), inclusive, no end →
        // only the key equal to the maximum qualifies (nothing is greater
        // under the reverse ordering), so only KEYS[0].
        let r = KeyRange {
            comparator: Some(reverse),
            begin_key: Some(ks[0].clone()),
            end_key: None,
            begin_inclusive: true,
            end_inclusive: false,
        };
        assert_eq!(
            keys_in_range(&r, &ks),
            vec![0],
            "reverse begin=KEYS[0] (reverse-max) admits only KEYS[0]",
        );

        // Discriminating control: the SAME begin=KEYS[0] under the DEFAULT
        // (unsigned-byte) order admits ALL keys — [1] is the byte-smallest,
        // so every key is >= it.  The default admits {0..7} where reverse
        // admits {0}: the comparator, not raw byte order, drives
        // containment.
        let default_begin0 = KeyRange {
            comparator: None,
            begin_key: Some(ks[0].clone()),
            end_key: None,
            begin_inclusive: true,
            end_inclusive: false,
        };
        assert_eq!(
            keys_in_range(&default_begin0, &ks),
            (0..ks.len()).collect::<Vec<_>>(),
            "default begin=KEYS[0] admits all (byte order)",
        );
    }

    /// JE: KeyRangeTest.testScanComparator (DB-observable, real reverse
    /// btree comparator).
    ///
    /// Open a real DB with a reverse btree comparator and assert the
    /// cursor walk (via StoredSortedMap::iter) yields KEYS[] in DESCENDING
    /// order — the comparator drives DB sort order end to end.  This is the
    /// engine-level counterpart of JE's ReverseComparator scan.
    #[test]
    fn key_range_scan_comparator_orders_db() {
        use noxu_db::{
            Comparator, DatabaseConfig, Environment, EnvironmentConfig,
        };
        let td = tempfile::TempDir::new().unwrap();
        let env = Environment::open(
            EnvironmentConfig::new(td.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
        let cmp =
            Comparator::new("keyrange-reverse", |a: &[u8], b: &[u8]| b.cmp(a));
        let db = env
            .open_database(
                None,
                "rev",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true)
                    .with_btree_comparator(cmp),
            )
            .unwrap();
        let map: StoredSortedMap<'_, Vec<u8>, Vec<u8>, _, _> =
            StoredSortedMap::new(
                &db,
                noxu_bind::ByteArrayBinding,
                noxu_bind::ByteArrayBinding,
            );
        let ks = keys();
        for k in &ks {
            map.put(None, k, k).unwrap();
        }
        // Forward iteration under a reverse comparator = descending byte
        // order = KEYS[] reversed.
        let got: Vec<Vec<u8>> =
            map.iter(None).unwrap().map(|r| r.unwrap().0).collect();
        let mut expect = ks.clone();
        expect.reverse();
        assert_eq!(got, expect, "reverse comparator yields descending walk");
        // first_key / last_key must honour the comparator too.
        assert_eq!(map.first_key(None).unwrap(), Some(ks[7].clone()));
        assert_eq!(map.last_key(None).unwrap(), Some(ks[0].clone()));
    }

    /// JE: KeyRangeTest.testSubRanges — faithful port.
    ///
    /// Base range [1, 2] (both inclusive).  A sub-range is valid iff both
    /// its new bounds fall within the parent range; otherwise subRange
    /// raises KeyRangeException.  The exact JE cases and expected
    /// valid/invalid outcomes are reproduced.
    #[test]
    fn key_range_sub_ranges() {
        // Base range [1, 2] (both inclusive).
        let base = KeyRange {
            comparator: None,
            begin_key: Some(vec![1]),
            end_key: Some(vec![2]),
            begin_inclusive: true,
            end_inclusive: true,
        };

        // Subrange (0, 1] is invalid: begin 0 < parent begin 1.
        assert_eq!(
            base.sub_range(&[0], false, &[1], true).unwrap_err(),
            KeyRangeException("beginKey out of range"),
        );

        // Subrange [1, 3) is invalid: end 3 > parent end 2.
        assert_eq!(
            base.sub_range(&[1], true, &[3], false).unwrap_err(),
            KeyRangeException("endKey out of range"),
        );

        // Subrange [2, 2] is valid.
        assert!(base.sub_range(&[2], true, &[2], true).is_ok());

        // Subrange [0, 1] is invalid: begin 0 < parent begin 1.
        assert_eq!(
            base.sub_range(&[0], true, &[1], true).unwrap_err(),
            KeyRangeException("beginKey out of range"),
        );

        // Subrange (0, 3] is invalid: begin 0 < parent begin 1.
        assert_eq!(
            base.sub_range(&[0], false, &[3], true).unwrap_err(),
            KeyRangeException("beginKey out of range"),
        );

        // Subrange [3, 3) is invalid: JE validates the new BEGIN bound with
        // the full check() = checkBegin && checkEnd, so begin 3 — though it
        // satisfies the parent begin (3 >= 1) — fails the parent END constraint
        // (3 > 2), and JE reports "beginKey out of range" (KeyRange.subRange
        // checks begin before end).
        assert_eq!(
            base.sub_range(&[3], true, &[3], false).unwrap_err(),
            KeyRangeException("beginKey out of range"),
        );
    }
}
