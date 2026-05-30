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

use crate::bind::EntryBinding;
use crate::db::{Database, DatabaseEntry, Get, OperationStatus, Transaction};

use crate::collections::error::{CollectionError, Result};

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
    let mut cursor = db.open_cursor(txn, None)?;

    // Position cursor on the first record we want.
    //
    // We deliberately *do not* use `Get::SearchGte` to position on
    // the start key in v1.6: the noxu-dbi `cursor_impl::search` path
    // resets `current_index` to 0 after a SetRange match, which makes
    // a subsequent `Get::Next` walk from index 0 of the same BIN
    // instead of advancing from the actual found position.  That is a
    // real engine bug (`docs/src/internal/wave-2b-collections-typed.md`),
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
            let cur = key.get_data().unwrap_or(&[]);
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
    let mut cursor = db.open_cursor(txn, None)?;
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
