//! Internal helpers shared by the typed Stored* views.
//!
//! These helpers centralise the
//! cursor-scan / encode / decode plumbing that the four typed views
//! (`StoredMap`, `StoredSortedMap`, `StoredKeySet`, `StoredValueSet`)
//! all need.  Keeping it in one place means the txn-threading shape
//! is identical across views — every typed Stored* method ultimately
//! lands in [`scan_records`] for reads and in
//! [`encode_key`] / [`encode_value`] / [`decode_key`] / [`decode_value`]
//! for individual point operations.

use std::marker::PhantomData;

use noxu_bind::ByteArrayBinding;
use noxu_bind::EntryBinding;
use noxu_db::{
    Cursor, CursorConfig, Database, DatabaseEntry, Get, OperationStatus,
    Transaction,
};

use crate::error::{CollectionError, Result};

/// A shared `'static` byte-array binding for the discarded side of
/// key-only / value-only lazy scans, so a [`ScanIter`] can borrow it for
/// any iterator lifetime without keeping a local temporary alive.
pub(crate) static BYTE_ARRAY_BINDING: ByteArrayBinding = ByteArrayBinding;

/// Opens a cursor honouring the optional transaction (review P0-1/P0-2:
/// `open_cursor` no longer takes `Option<&Transaction>`; auto-commit and
/// transactional are now separate entry points).  The returned
/// `Cursor<'a>` borrows the txn when present.
pub(crate) fn open_cursor<'a>(
    db: &Database,
    txn: Option<&'a Transaction>,
    config: Option<&CursorConfig>,
) -> Result<Cursor<'a>> {
    match txn {
        Some(t) => Ok(db.open_cursor_in(t, config)?),
        None => Ok(db.open_cursor(config)?),
    }
}

/// Point read honouring the optional transaction (review P0-2/P0-3:
/// `get` now returns `Result<Option<Bytes>>` off named entry points).
/// Returns the value bytes, or `None` if the key is absent.
pub(crate) fn db_get(
    db: &Database,
    txn: Option<&Transaction>,
    key: &DatabaseEntry,
) -> Result<Option<Vec<u8>>> {
    let k = key.data_opt().unwrap_or(&[]);
    let found = match txn {
        Some(t) => db.get_in(t, k)?,
        None => db.get(k)?,
    };
    Ok(found.map(|b| b.to_vec()))
}

/// Point put honouring the optional transaction (review P0-2).
pub(crate) fn db_put(
    db: &Database,
    txn: Option<&Transaction>,
    key: &DatabaseEntry,
    data: &DatabaseEntry,
) -> Result<()> {
    let k = key.data_opt().unwrap_or(&[]);
    let v = data.data_opt().unwrap_or(&[]);
    match txn {
        Some(t) => db.put_in(t, k, v)?,
        None => db.put(k, v)?,
    }
    Ok(())
}

/// No-overwrite put honouring the optional transaction (review P0-2/P0-3).
/// Returns `true` if the key was newly inserted.
pub(crate) fn db_put_no_overwrite(
    db: &Database,
    txn: Option<&Transaction>,
    key: &DatabaseEntry,
    data: &DatabaseEntry,
) -> Result<bool> {
    let k = key.data_opt().unwrap_or(&[]);
    let v = data.data_opt().unwrap_or(&[]);
    let inserted = match txn {
        Some(t) => db.put_no_overwrite_in(t, k, v)?,
        None => db.put_no_overwrite(k, v)?,
    };
    Ok(inserted)
}

/// Point delete honouring the optional transaction (review P0-2/P0-3).
/// Returns `true` if a record was removed.
pub(crate) fn db_delete(
    db: &Database,
    txn: Option<&Transaction>,
    key: &DatabaseEntry,
) -> Result<bool> {
    let k = key.data_opt().unwrap_or(&[]);
    let deleted = match txn {
        Some(t) => db.delete_in(t, k)?,
        None => db.delete(k)?,
    };
    Ok(deleted)
}

/// Encodes a typed key into a fresh [`DatabaseEntry`].
pub(crate) fn encode_key<K, KB: EntryBinding<K>>(
    binding: &KB,
    key: &K,
) -> Result<DatabaseEntry> {
    let mut entry = DatabaseEntry::new();
    binding
        .object_to_entry(key, &mut entry)
        .map_err(|e| CollectionError::BindingError(e.to_string()))?;
    Ok(entry)
}

/// Encodes a typed value into a fresh [`DatabaseEntry`].
pub(crate) fn encode_value<V, VB: EntryBinding<V>>(
    binding: &VB,
    value: &V,
) -> Result<DatabaseEntry> {
    let mut entry = DatabaseEntry::new();
    binding
        .object_to_entry(value, &mut entry)
        .map_err(|e| CollectionError::BindingError(e.to_string()))?;
    Ok(entry)
}

/// Decodes a [`DatabaseEntry`] into a typed key.
pub(crate) fn decode_key<K, KB: EntryBinding<K>>(
    binding: &KB,
    entry: &DatabaseEntry,
) -> Result<K> {
    binding
        .entry_to_object(entry)
        .map_err(|e| CollectionError::BindingError(e.to_string()))
}

/// Decodes a [`DatabaseEntry`] into a typed value.
pub(crate) fn decode_value<V, VB: EntryBinding<V>>(
    binding: &VB,
    entry: &DatabaseEntry,
) -> Result<V> {
    binding
        .entry_to_object(entry)
        .map_err(|e| CollectionError::BindingError(e.to_string()))
}

/// Selects which decoded fields a scan should produce.
#[derive(Copy, Clone, Debug)]
pub(crate) enum ScanShape {
    /// Decode key and value (yield `(K, V)`).
    KeyValue,
    /// Decode key only.
    Key,
    /// Decode value only.
    Value,
}

/// Direction for a scan.
#[derive(Copy, Clone, Debug)]
pub(crate) enum ScanDirection {
    /// Iterate keys in ascending byte order.
    Forward,
    /// Iterate keys in descending byte order.
    Reverse,
}

/// Lower bound for a forward scan, expressed in raw key bytes.
///
/// `None` → start from the first record.  `Some(bytes)` → start from
/// the smallest key `>= bytes` (inclusive lower bound).
pub(crate) type StartKey<'a> = Option<&'a [u8]>;

/// Walk every record reachable from the given starting position and
/// return the decoded items.  This is the snapshot the typed
/// iterators yield from.
///
/// The cursor is opened under `txn` so reads acquire shared locks via
/// the caller's locker.  Passing `None` issues an auto-commit cursor
/// (the v1.6 default).
#[allow(clippy::type_complexity)]
pub(crate) fn scan_records<'a, K, V, KB, VB, T, F>(
    db: &Database,
    txn: Option<&'a Transaction>,
    start: StartKey<'a>,
    direction: ScanDirection,
    key_binding: &KB,
    value_binding: &VB,
    mut project: F,
) -> Result<Vec<T>>
where
    KB: EntryBinding<K>,
    VB: EntryBinding<V>,
    F: FnMut(K, V) -> T,
{
    let mut out: Vec<T> = Vec::new();
    let mut cursor = open_cursor(db, txn, None)?;

    // Position cursor on the first record we want.
    //
    // We deliberately *do not* use `Get::SearchGte` to position on
    // the start key in v1.6: the noxu-dbi `cursor_impl::search` path
    // resets `current_index` to 0 after a SetRange match, which makes
    // a subsequent `Get::Next` walk from index 0 of the same BIN
    // instead of advancing from the actual found position.  That is a
    // real engine bug (the 2026 review),
    // but it lives in `noxu-dbi` which is out of scope for this wave;
    // the v1.6 collections workaround is to walk from the appropriate
    // endpoint (`First` or `Last`) and skip records that fall outside
    // the requested range.  That costs an O(K) prefix scan instead of
    // landing directly, but it is correct under every cursor mode the
    // engine supports today.
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();

    let initial_op = match direction {
        ScanDirection::Forward => Get::First,
        ScanDirection::Reverse => Get::Last,
    };
    let mut status = cursor.get(&mut key, &mut data, initial_op, None)?;

    if !matches!(status, OperationStatus::Success) {
        let _ = cursor.close();
        return Ok(out);
    }

    // Skip records that fall outside the requested half-range.
    if let Some(bound) = start {
        loop {
            let cur = key.data_opt().unwrap_or(&[]);
            let in_range = match direction {
                ScanDirection::Forward => cur >= bound,
                ScanDirection::Reverse => cur <= bound,
            };
            if in_range {
                break;
            }
            let step = match direction {
                ScanDirection::Forward => Get::Next,
                ScanDirection::Reverse => Get::Prev,
            };
            status = cursor.get(&mut key, &mut data, step, None)?;
            if !matches!(status, OperationStatus::Success) {
                let _ = cursor.close();
                return Ok(out);
            }
        }
    }

    loop {
        let k = decode_key(key_binding, &key)?;
        let v = decode_value(value_binding, &data)?;
        out.push(project(k, v));

        let step = match direction {
            ScanDirection::Forward => Get::Next,
            ScanDirection::Reverse => Get::Prev,
        };
        match cursor.get(&mut key, &mut data, step, None)? {
            OperationStatus::Success => continue,
            _ => break,
        }
    }

    cursor.close()?;
    Ok(out)
}

/// A lazy, cursor-backed iterator over the records of a Stored* view
/// (review P1-7).
///
/// Unlike the eager [`crate::stored_iterator::StoredIterator`] (which
/// materialises the whole keyspace into a `Vec` at construction time),
/// this holds a live [`Cursor`] and fetches+decodes one record per
/// `next()` call — O(1) to create and bounded memory regardless of the
/// database size, matching `noxu_db::Database::iter`.
///
/// The lifetime `'a` ties the iterator to both the borrowed bindings
/// (from the Stored* view) and — when iterating under an explicit
/// transaction — to that transaction, so the borrow checker rejects any
/// code that commits or drops the transaction while the iterator is
/// still alive (the same guarantee `Cursor<'txn>` gives, review P0-1).
pub(crate) struct ScanIter<'a, K, V, KB, VB, T, F>
where
    KB: EntryBinding<K>,
    VB: EntryBinding<V>,
    F: FnMut(K, V) -> T,
{
    cursor: Cursor<'a>,
    key_binding: &'a KB,
    value_binding: &'a VB,
    start: Option<Vec<u8>>,
    direction: ScanDirection,
    project: F,
    started: bool,
    done: bool,
    _marker: PhantomData<fn() -> (K, V, T)>,
}

impl<'a, K, V, KB, VB, T, F> ScanIter<'a, K, V, KB, VB, T, F>
where
    KB: EntryBinding<K>,
    VB: EntryBinding<V>,
    F: FnMut(K, V) -> T,
{
    fn step_op(&self) -> Get {
        match self.direction {
            ScanDirection::Forward => Get::Next,
            ScanDirection::Reverse => Get::Prev,
        }
    }

    fn initial_op(&self) -> Get {
        match self.direction {
            ScanDirection::Forward => Get::First,
            ScanDirection::Reverse => Get::Last,
        }
    }

    /// Returns `true` if `cur` is within the requested half-range
    /// relative to the `start` lower/upper bound.
    fn in_range(&self, cur: &[u8]) -> bool {
        match (&self.start, self.direction) {
            (None, _) => true,
            (Some(bound), ScanDirection::Forward) => cur >= bound.as_slice(),
            (Some(bound), ScanDirection::Reverse) => cur <= bound.as_slice(),
        }
    }
}

impl<'a, K, V, KB, VB, T, F> Iterator for ScanIter<'a, K, V, KB, VB, T, F>
where
    KB: EntryBinding<K>,
    VB: EntryBinding<V>,
    F: FnMut(K, V) -> T,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        loop {
            let op = if self.started {
                self.step_op()
            } else {
                self.started = true;
                self.initial_op()
            };
            match self.cursor.get(&mut key, &mut data, op, None) {
                Err(e) => {
                    self.done = true;
                    return Some(Err(e.into()));
                }
                Ok(OperationStatus::Success) => {
                    // Skip records before the (inclusive) start bound.
                    let cur = key.data_opt().unwrap_or(&[]);
                    if !self.in_range(cur) {
                        continue;
                    }
                    let k = match decode_key(self.key_binding, &key) {
                        Ok(k) => k,
                        Err(e) => {
                            self.done = true;
                            return Some(Err(e));
                        }
                    };
                    let v = match decode_value(self.value_binding, &data) {
                        Ok(v) => v,
                        Err(e) => {
                            self.done = true;
                            return Some(Err(e));
                        }
                    };
                    return Some(Ok((self.project)(k, v)));
                }
                Ok(_) => {
                    self.done = true;
                    return None;
                }
            }
        }
    }
}

/// Builds a lazy [`ScanIter`] over the view's records (review P1-7).
///
/// O(1) to create: it only opens the cursor; records are fetched and
/// decoded one at a time as the iterator is advanced.  `start` is an
/// owned lower/upper bound (or `None` for a full scan) so the iterator
/// does not borrow the caller's start buffer.
#[allow(clippy::type_complexity)]
pub(crate) fn scan_iter_owned_start<'a, K, V, KB, VB, T, F>(
    db: &Database,
    txn: Option<&'a Transaction>,
    start: Option<Vec<u8>>,
    direction: ScanDirection,
    key_binding: &'a KB,
    value_binding: &'a VB,
    project: F,
) -> Result<ScanIter<'a, K, V, KB, VB, T, F>>
where
    KB: EntryBinding<K>,
    VB: EntryBinding<V>,
    F: FnMut(K, V) -> T,
{
    let cursor = open_cursor(db, txn, None)?;
    Ok(ScanIter {
        cursor,
        key_binding,
        value_binding,
        start,
        direction,
        project,
        started: false,
        done: false,
        _marker: PhantomData,
    })
}

/// Builds a lazy [`ScanIter`] over the view's records (review P1-7),
/// taking a borrowed [`StartKey`].
#[allow(clippy::type_complexity)]
pub(crate) fn scan_iter<'a, K, V, KB, VB, T, F>(
    db: &Database,
    txn: Option<&'a Transaction>,
    start: StartKey<'a>,
    direction: ScanDirection,
    key_binding: &'a KB,
    value_binding: &'a VB,
    project: F,
) -> Result<ScanIter<'a, K, V, KB, VB, T, F>>
where
    KB: EntryBinding<K>,
    VB: EntryBinding<V>,
    F: FnMut(K, V) -> T,
{
    scan_iter_owned_start(
        db,
        txn,
        start.map(|s| s.to_vec()),
        direction,
        key_binding,
        value_binding,
        project,
    )
}

/// Marker used by the typed Stored* views to signal that the binding
/// type parameters do not need to outlive the value itself.
///
/// Using `fn() -> (K, V)` keeps the marker `Send + Sync` regardless of
/// `K` / `V` so the views can be moved across threads as long as the
/// bindings themselves are `Send + Sync`.
pub(crate) type Phantom<K, V> = PhantomData<fn() -> (K, V)>;

/// Reads a single endpoint of the database (typically `Get::First` or
/// `Get::Last`) and returns the decoded `(K, V)` pair.
///
/// Returns `Ok(None)` if the database is empty.
pub(crate) fn cursor_endpoint<K, V, KB, VB>(
    db: &Database,
    txn: Option<&Transaction>,
    key_binding: &KB,
    value_binding: &VB,
    which: Get,
) -> Result<Option<(K, V)>>
where
    KB: EntryBinding<K>,
    VB: EntryBinding<V>,
{
    let mut cursor = open_cursor(db, txn, None)?;
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let status = cursor.get(&mut key, &mut data, which, None)?;
    let result = match status {
        OperationStatus::Success => {
            let k = decode_key(key_binding, &key)?;
            let v = decode_value(value_binding, &data)?;
            Some((k, v))
        }
        _ => None,
    };
    cursor.close()?;
    Ok(result)
}
