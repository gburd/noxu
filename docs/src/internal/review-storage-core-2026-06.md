# Noxu DB Storage-Core Code Review
**Date**: 2026-06-03  
**Branch**: `fix/zb-stale-docs`  
**Reviewers (persona)**: Margo Seltzer, Keith Bostic, Linda Lee  
**Scope**: `noxu-tree`, `noxu-log`, `noxu-cleaner`, `noxu-evictor`  
**Reference**: BDB-JE source at `/home/gburd/ws/je`  

---

## Summary Table

| ID  | Severity | File : Line | One-line description |
|-----|----------|-------------|----------------------|
| C-1 | **Critical** | `noxu-log/src/log_manager.rs:569–580,553–564` | `flush_no_sync` advances `last_flush_lsn`, letting `flush_sync_if_needed` skip fsync after a non-durable flush — committed transactions silently lost on crash |
| C-2 | **Critical** (latent) | `noxu-tree/src/tree.rs:1076–1089` | `BinStub::apply_delta` stores full (uncompressed) keys directly into prefix-compressed `base.entries[idx].key` — BIN key corruption; function is dead code today but documented as the recovery path |
| C-3 | **Critical** | `noxu-log/src/file_header.rs` | File header written as raw 32-byte blob with no checksum — a torn-write during crash leaves the file header silently corrupt and undetectable |
| H-1 | **High** | `noxu-log/src/file_header.rs:66`, `entry_header.rs:12` | `byte_order = 0x00` in file header claims big-endian, but all log entry header fields are little-endian — an external reader following the documented contract decodes entries wrong |
| H-2 | **High** | `noxu-evictor/src/evictor.rs:869–920` | `real_node_size()` recursively walks the entire tree on every eviction decision — O(n²) eviction cost for n cached nodes |
| H-3 | **High** | `noxu-log/src/entry_header.rs:12`, `entry/bin_delta_log_entry.rs:88,97`, `noxu-tree/src/tree.rs:871` | Log entry headers are little-endian; entry payloads (BINDelta, BinStub ser/deser) use big-endian — mixed on-disk endianness with no documentation |
| H-4 | **High** | `noxu-tree/src/tree.rs:1619–1638, 1731–1744` | Upper-IN descent uses O(n) linear scan; JE uses binary search — 64× slower for full-width INs |
| H-5 | **High** | `noxu-tree/src/tree.rs:1136–1165` | `TreeNode::find_entry` (non-exact, Internal variant) returns insertion point instead of floor entry — any caller using this for non-BIN descent routes into wrong child |
| H-6 | **High** | `noxu-tree/src/tree.rs:980` | `BinStub::deserialize_full` hardcodes `expiration_in_hours: true` regardless of what was logged — TTL values read back 3600× too large whenever BINs were written with seconds-granularity expiration |
| M-1 | **Medium** | `noxu-log/src/log_manager.rs:602–623` | Hot-path `read_entry` (write-buffer hit) returns bytes without CRC32 verification — silent corruption in the buffer goes undetected |
| M-2 | **Medium** | `noxu-log/src/log_manager.rs:320` | `log_internal` always uses `MIN_HEADER_SIZE` (14 B); the `REPLICATED_MASK` / `VLSN_PRESENT_MASK` flags and the VLSN field are never written — replicated entries logged via `LogManager::log()` lose their VLSN silently |
| M-3 | **Medium** | `noxu-tree/src/tree.rs:991–1090`, `noxu-recovery/src/recovery_manager.rs:1244` | `BinStub::apply_delta` is unreachable dead code; the recovery path only mentions it in a comment — yet the docstring claims it is "`BIN.reconstituteBIN()`", so future readers will trust it |
| L-1 | **Low** | `noxu-tree/src/tree.rs:395–443` | `compute_key_prefix(exclude_idx)` with `exclude_idx = Some(i)` where `i` is the only non-zero-index entry returns the full key of the seed entry as the "prefix" — `debug_assert!` fire or silent key corruption in release builds if called with a non-None argument |

**Totals: 3 Critical, 5 High, 3 Medium, 1 Low (12 findings)**

---

## Findings in Detail

---

### C-1 · Critical — `flush_no_sync` poisons the fsync-coalescing fast path

**File:line**: `crates/noxu-log/src/log_manager.rs`  
Lines 569–580 (`flush_no_sync`), 553–564 (`flush_sync_if_needed`), 143 (`last_flush_lsn` initializer)

**What JE does**  
JE's `LogManager` tracks two watermarks independently:  
- `lastFlushedLsn` — data written to the kernel page cache (pwrite64 done, no fsync).  
- `lastSyncedLsn` / FSyncManager — data confirmed durable via fdatasync.  
`LogManager.flushTo(lsn)` (the fast-commit coalescing path) checks only the *synced* watermark; a prior `flush()` (no-fsync) does not advance it.

**What Noxu does**  
```rust
// flush_no_sync (line 579):
self.last_flush_lsn.store(eol.as_u64(), Ordering::Release);

// flush_sync_if_needed (line 553-564):
let already_flushed = self.last_flush_lsn.load(Ordering::Acquire);
if already_flushed > lsn.as_u64() {
    return Ok(Lsn::from_u64(already_flushed));  // ← skips fsync!
}
```
A single `last_flush_lsn` is advanced by **both** `flush_no_sync` (page cache only) and `flush_sync` (durable). The fast-path in `flush_sync_if_needed` treats any advancement of this counter as evidence of durability.

**Why it matters — concrete failure scenario**  
1. Transaction T logs its commit record at LSN X with `fsync_required=true, flush_required=false`. The record lands in the write buffer.  
2. A concurrent checkpoint loop logs dirty BINs with `flush_required=true, fsync_required=false`, triggering `flush_no_sync()`.  
3. `flush_no_sync` drains all dirty buffers (including T's commit at X) to the kernel page cache via `pwrite64`. It then stores `last_flush_lsn = Y` where Y > X.  
4. T's commit path calls `flush_sync_if_needed(X)`. It loads `already_flushed = Y > X` and **returns immediately without calling `flush_sync()`**.  
5. Power loss. T's commit is in the page cache but was never `fdatasync`'d. Transaction T is silently lost.

The comment on `flush_sync_if_needed` (line 536) claims "a concurrent or preceding `flush_sync()` already covers our data", which is false when `last_flush_lsn` was advanced by `flush_no_sync`.

**Suggested fix**  
Introduce a separate `last_synced_lsn: AtomicU64` that is only updated by `flush_sync`. Have `flush_sync_if_needed` compare against `last_synced_lsn`, not `last_flush_lsn`. `flush_no_sync` continues to update a separate `last_written_lsn` used only for non-durability consumers (e.g., recovery scan start point).

---

### C-2 · Critical (latent) — BIN delta reconstruction corrupts prefix-compressed keys

**File:line**: `crates/noxu-tree/src/tree.rs:1000–1089` (`BinStub::apply_delta`)  
Cross-reference: JE `BIN.reconstituteBIN()`, `BINDeltaLogEntry.readEntry()`

**What JE does**  
`BIN.reconstituteBIN(binDelta)` applies delta slots via key-aware upsert, then recalculates the key prefix on the merged result.

**What Noxu does**  
`BinStub::deserialize_full` correctly loads a full BIN and then calls `recompute_key_prefix()`, so `base.entries[i].key` holds **suffix** bytes (relative to `base.key_prefix`).

`BinStub::apply_delta` (the bytes-based reconstruction path) then does:
```rust
// delta_bytes encodes full (uncompressed) keys:
let key = delta_bytes[pos..pos + key_len].to_vec();  // full key

if slot_idx < base.entries.len() {
    base.entries[slot_idx].key = key;   // ← stores FULL key into a SUFFIX slot
    // No recompute_key_prefix() call
}
```
After the call, `base.key_prefix` is still the old prefix, but the touched slots contain full uncompressed keys rather than suffixes. Any subsequent `get_full_key(i)` prepends the prefix to a full key, producing `prefix + full_key` — garbage. Binary searches also break because suffix ordering is violated.

Additionally, when `slot_idx >= base.entries.len()` (new slot), the key is appended as a full key but no sorted-insertion is performed, violating the BIN's sorted-key invariant.

**Current exploitability**  
`apply_delta` is dead code: the only reference in the production tree is a comment at `recovery_manager.rs:1244` (`// or BinStub::apply_delta to reconstruct the node`). The live reconstruction path goes through `mutate_to_full_bin` → `apply_delta_to_bin` → `insert_with_prefix`, which correctly handles prefix recomputation.

**Risk**  
The docstring declares this function to be "`BINDeltaLogEntry.readEntry()` / `BIN.reconstituteBIN()`". Any future developer or recovery path that calls it will silently corrupt the B-tree. This must be either fixed (use `insert_with_prefix` + `recompute_key_prefix`) or removed with an `unreachable!()`.

---

### C-3 · Critical — File header has no checksum protection

**File:line**: `crates/noxu-log/src/file_header.rs` (entire file)  
Cross-reference: JE `FileManager.java:1509–1527`, `FileHeader.java`, `LogEntryHeader.java`

**What JE does**  
JE wraps the file header as a full log entry (`LOG_FILE_HEADER` type) with an Adler32 checksum in the `LogEntryHeader`. If the file header write is torn by a crash, the checksum mismatch is detected during `readAndValidateFileHeader()` and the environment fails to open, prompting recovery from the prior file or reporting a specific integrity error.

**What Noxu does**  
```rust
// create_file_internal:
let header = FileHeader::new(file_num, last_entry_offset);
header.write_to(&mut file)?;   // raw 32-byte blob, no checksum
file.flush()?;
file.sync_all()?;
```
The 32-byte file header (magic, version, timestamp, file number, last-entry-offset) is written as a raw blob. `FileHeader::read_from` validates the magic bytes and version number, but there is **no checksum field**. A partial write that corrupts the `file_number` or `last_entry_in_prev_file` fields (e.g., a torn 4-byte write during a power failure after `pwrite64` but before `fdatasync`) passes all existing validation and silently provides wrong recovery metadata.

**Note on `sync_all`**  
Noxu does call `file.sync_all()` after writing the header (including the parent-directory fsync fix, C-1 in the previous audit). This reduces the torn-write window but does not eliminate it on systems where `sync_all` does not provide power-fail atomicity at the 32-byte granularity.

**Suggested fix**  
Either:  
(a) Append a CRC32 field to the file header (requiring a format-version bump to version 3), or  
(b) Wrap the file header as a log entry using the existing `LogEntryHeader` framing so that the CRC32 computed by `log_internal` covers the header payload.  
Option (b) matches JE's design and also allows `LastFileReader` to scan file headers the same way it scans all other entries.

---

### H-1 · High — File header claims big-endian; entry headers are little-endian

**File:line**:  
- `crates/noxu-log/src/file_header.rs:66` (`BYTE_ORDER_BIG_ENDIAN = 0x00`), line 109 (rejection of `!= 0x00`), module doc comment  
- `crates/noxu-log/src/entry_header.rs:12` (`use byteorder::LittleEndian`)

**What JE does**  
JE uses Java's `ByteBuffer` which defaults to big-endian. All numeric fields in both the file header and log entry headers are big-endian. There is no byte-order marker field.

**What Noxu does**  
The file header module says:  
> "Files written by this implementation always use big-endian byte order (`byte_order = 0x00`). The `byte_order` field is reserved for future little-endian native format support."

But `entry_header.rs` uses `byteorder::LittleEndian` for **all** log entry header fields: `checksum`, `entry_type`, `flags`, `prev_offset`, `item_size`, and VLSN. The file header itself is written big-endian, while log entries are written little-endian.

The documented `byte_order` field therefore promises semantics it does not deliver. An external reader (a future Noxu compatibility layer, a disaster-recovery tool, or a replication feeder) that:
1. Reads the file header in big-endian ✓
2. Sees `byte_order = 0x00` (big-endian)
3. Reads entry headers in big-endian ✗ — checksum, item_size, prev_offset will all be byte-swapped wrong

**Suggested fix**  
Either:  
(a) Change the file header comment to accurately say: "File header fields are big-endian; log entry header and payload fields are little-endian. The `byte_order` field applies to the file header framing only." And bump `LOG_VERSION` to make the clarification part of the version contract.  
(b) Consistently use little-endian everywhere (matching modern x86-64 POSIX convention) and remove the byte_order field entirely, updating the comment.

---

### H-2 · High — Evictor `real_node_size` performs O(n) tree traversal per eviction

**File:line**: `crates/noxu-evictor/src/evictor.rs:869–920` (`real_node_size`, `find_node_size_recursive`)

**What JE does**  
Each `IN` tracks its in-memory size via `IN.inMemorySize` (updated on every insert/delete/eviction via explicit `MemoryBudget.updateCacheUsage` calls). The evictor reads `IN.getInMemorySize()` in O(1).

**What Noxu does**  
```rust
fn real_node_size(tree: &Tree, node_id: u64) -> u64 {
    let root_arc = match tree.get_root() {
        Some(r) => r,
        None => return 1024,
    };
    find_node_size_recursive(&root_arc, node_id).unwrap_or(1024)
}

fn find_node_size_recursive(node_arc: &NodeRwLock<TreeNode>, target_id: u64) -> Option<u64> {
    // Recursively walks ALL nodes until target_id is found
    ...
}
```
For a tree with N nodes, one call to `real_node_size` is O(N). One eviction batch of size B is O(N × B). Under memory pressure (the case that triggers eviction), N can be very large and B is the configured batch size (default: `DEFAULT_BATCH_SIZE`). This makes the evictor quadratically expensive — the more data in cache, the slower eviction becomes.

Additionally, `find_node_size_recursive` holds a `read()` guard on every node it traverses while descending, and drops guards only after scanning children. This is a latency spike for all readers during heavy eviction.

**Suggested fix**  
Add `in_memory_size: AtomicU64` to `BinStub` and `InNodeStub`. Update it on every `insert_with_prefix`, `delete_entry`, `strip_lns`, and `apply_new_prefix`. The evictor reads this field directly.

---

### H-3 · High — Mixed endianness: entry headers are LE, entry payloads are BE

**File:line**:  
- `noxu-log/src/entry_header.rs:12` (LE)  
- `noxu-log/src/entry/bin_delta_log_entry.rs:88,97` (BE: `read_u64::<BigEndian>`, `put_u64`)  
- `noxu-tree/src/tree.rs:871–892` (`serialize_delta`: `to_be_bytes()`), `924–968` (`deserialize_full`: `from_be_bytes`)

The log entry header fields (`checksum`, `prev_offset`, `item_size`, VLSN) are little-endian. The `BinDeltaLogEntry` payload fields (`db_id`, `prev_full_lsn`, `prev_delta_lsn`) are big-endian. `BinStub::serialize_delta` and `deserialize_full` also use big-endian (`to_be_bytes()` / `from_be_bytes()`).

**Why it matters**  
The mixed encoding is not documented anywhere in the on-disk format specification. It survives today only because serialization and deserialization are always handled by matching Rust code. If any other code path reads the `db_id` or `prev_full_lsn` from the raw payload bytes using the entry header's assumed LE encoding (e.g., a log scanner, a recovery redo path that uses the LE-decoded `Lsn::from_u64`), it will silently misparse the LSN.

The inconsistency also violates the principle that the `byte_order` field (H-1) should govern the file's encoding.

**Suggested fix**  
Standardize on little-endian for **all** fields in both entry headers and entry payloads. Update `BinDeltaLogEntry::write_to_log` / `read_from_log` and `BinStub::serialize_delta` / `deserialize_full` to use `to_le_bytes()` / `from_le_bytes()`. Bump `LOG_VERSION`. Add a regression test that checks the encoding of a known `BinDeltaLogEntry` byte-by-byte.

---

### H-4 · High — Upper-IN descent uses O(n) linear scan instead of binary search

**File:line**: `crates/noxu-tree/src/tree.rs:1619–1638` (`Tree::search`, upper-IN arm), `1731–1744` (`Tree::search_with_data`, upper-IN arm)

**What JE does**  
`IN.findEntry(key, false, false)` performs a binary search. For 128-slot INs this is at most 7 comparisons.

**What Noxu does**  
```rust
// Tree::search, line 1619–1638:
let mut idx = 0usize;
for (i, entry) in n.entries.iter().enumerate() {
    if i == 0 {
        idx = 0;
    } else if self.key_cmp(entry.key.as_slice(), key)
        != std::cmp::Ordering::Greater
    {
        idx = i;
    } else {
        break;  // early exit helps best-case but not average
    }
}
```
This iterates all entries until finding the first one greater than the search key. For an IN with 128 entries, the average cost is 64 comparisons vs. JE's 7. The `break` provides an early exit for keys in the first half, but worst-case (key larger than all entries) is still 128 comparisons.

Both `Tree::search` and `Tree::search_with_data` contain identical linear scans in the upper-IN arm. Any workload with more than one level of internal nodes is impacted.

**Suggested fix**  
Replace the linear scan with `InNodeStub::find_entry` (treating slot 0 as virtual -∞ exactly as JE does) or use `entries.partition_point(|e| self.key_cmp(e.key.as_slice(), key) != Greater)` for a one-line binary partition. `find_entry` on `TreeNode::Internal` is already implemented but is not called from the descent path (see also H-5).

---

### H-5 · High — `TreeNode::find_entry` returns insertion point, not floor entry, for upper INs

**File:line**: `crates/noxu-tree/src/tree.rs:1136–1165` (`TreeNode::find_entry`)

**What JE does**  
`IN.findEntry(key, false /*indicateIfDuplicate*/, false /*exact*/)` returns `high` after the binary search loop — the **floor** entry: the largest index `i` such that `entry[i].key ≤ key`. This is the child slot to descend into.

**What Noxu does**  
```rust
TreeNode::Internal(n) => {
    let result = n.entries.binary_search_by(|e| e.key.as_slice().cmp(key));
    match result {
        Ok(idx)  => (idx as i32) | EXACT_MATCH,
        Err(idx) => {
            if exact { -1 } else { idx as i32 }  // ← insertion point, not floor
        }
    }
}
```
`Vec::binary_search_by` on `Err(idx)` returns the **insertion point** — the first slot whose key is strictly greater than the search key. This is one higher than the JE floor.

**Concrete error**: keys `["b", "d", "f"]` (upper IN), search for `"c"`:  
- JE returns 0 (entry `"b"` ≤ `"c"` → descend into child 0)  
- Noxu returns 1 (insertion point between `"b"` and `"d"` → descend into child 1)

Child 1 covers keys `["d", "f"]`. Searching there for `"c"` would fail or return a wrong result.

**Current exploitability**  
The live descent paths (`Tree::search`, `Tree::search_with_data`) bypass `find_entry` for the upper-IN case and use the linear scan (H-4). So this bug is latent — any future refactoring that wires up `find_entry` for the descent path will exhibit wrong tree routing.

**Suggested fix**  
Replace the `Err(idx)` arm with `if exact { -1 } else { (idx as i32) - 1 }` (returning `idx - 1` = the floor). Also handle the virtual key at slot 0 (return 0 even when `idx == 0`). Add a unit test that verifies floor semantics against a 3-entry upper IN.

---

### H-6 · High — `expiration_in_hours` not serialized in BIN format

**File:line**: `crates/noxu-tree/src/tree.rs:980` (`BinStub::deserialize_full`), `865–891` (`serialize_delta`), `908–968` (`deserialize_full` full body), `1105–1113` (`clear_dirty_after_delta_log`)

**What JE does**  
The BIN's `expirationInHours` flag is stored in the `IN_EXPIRATION_IN_HOURS` bit of the persistent `flags` field written to the log entry.

**What Noxu does**  
`BinStub::serialize_delta` and `BinStub::serialize_full` (not shown but implied by `deserialize_full`'s format) do not include the `expiration_in_hours` field in the byte stream. On deserialization:
```rust
let mut bin = BinStub {
    ...
    expiration_in_hours: true,  // hardcoded regardless of what was logged
    ...
};
```

**Why it matters**  
If a BIN was written when `expiration_in_hours` was `false` (seconds-based TTL), it is loaded back with `expiration_in_hours = true` (hours-based TTL). A 1-hour TTL (3600 s) stored as seconds looks like a 3600-hour (~150-day) TTL on reload. Records that should have expired in 1 hour will be treated as live for 150 days. Conversely, a very large seconds-value could wrap around. The result is incorrect TTL behavior after any BIN is evicted and reloaded.

Also affects the `is_expired` check used in `Tree::search` (line 1601) and `Tree::search_with_data` (line 1701).

**Suggested fix**  
Add a `flags: u8` byte to the BIN wire format. Set bit `0x01` when `expiration_in_hours` is true. Read it back during deserialization. Bump `LOG_VERSION`.

---

### M-1 · Medium — Hot-path `read_entry` skips CRC32 verification

**File:line**: `crates/noxu-log/src/log_manager.rs:602–623` (write-buffer hit path inside `read_entry`)

**What JE does**  
JE also skips checksum verification for buffer reads (the checksum is only verified on disk reads). This is an accepted trade-off in JE.

**What Noxu does**  
Same: no checksum verification on buffer hit. The inconsistency with the cold path (which validates CRC32) means that if a bug in the write path (e.g., a wrong offset, a race in `LogBufferSegment::put`) corrupts bytes in the buffer, the hot-path read silently returns corrupt data while the cold-path read would reject it.

```rust
// Hot path (line 602–623): directly reads payload bytes with NO CRC check.
if slice.len() >= entry_size {
    let entry_type_num = slice[4];
    let payload = slice[header_size..entry_size].to_vec();
    // ← No checksum validation
    return Ok((entry_type, payload));
}
```

The code comment at the module level describes CRC32 validation as step 5 of the read path, but this description only applies to the cold (disk) path. This creates a false documentation claim.

**Suggested fix**  
Update the module-level doc comment to explicitly note that CRC validation is skipped on buffer hits. Optionally add a `debug_cfg` assertion that verifies the checksum even on buffer hits (zero production cost) to catch write-path bugs during testing.

---

### M-2 · Medium — `log_internal` never writes VLSN into the entry header

**File:line**: `crates/noxu-log/src/log_manager.rs:320` (header_size), `330–336` (flags computation), `322–337` (header bytes built inline)

**What JE does**  
`LogManager.serialLogWork()` calls `LogEntryHeader.writeToLog()` which checks `isReplicated()` and `isVLSNPresent()` to emit the extended 22-byte header with VLSN.

**What Noxu does**  
`log_internal` always uses `MIN_HEADER_SIZE = 14` bytes:
```rust
let header_size = MIN_HEADER_SIZE; // no VLSN for non-replicated entries
```
The flags byte is built from `provisional` only:
```rust
let flags: u8 = match provisional {
    Provisional::Yes          => 0x80,
    Provisional::BeforeCkptEnd => 0x40,
    Provisional::No            => 0x00,
};
```
`REPLICATED_MASK (0x20)` and `VLSN_PRESENT_MASK (0x08)` are never set. If `noxu-rep` uses `LogManager::log()` to write replicated entries (expecting the 22-byte header format that `LogEntryHeader::read_from_log` understands), the VLSN will be silently dropped, and the replication reader will misparse subsequent entries (no VLSN in header → offset for next entry wrong).

The `LogEntryHeader` struct supports full VLSN serialization (tested in `entry_header.rs`), but `LogManager::log_internal` builds the header bytes independently, bypassing `LogEntryHeader::write_to_log`.

**Suggested fix**  
Either extend `log_internal` (or add an `log_replicated` variant) that accepts a `Vlsn` parameter and uses `LogEntryHeader::write_to_log` to emit the full 22-byte header. Alternatively, refactor `log_internal` to call `LogEntryHeader::write_to_log` instead of hand-building the bytes, and add a test that round-trips a replicated entry through the full `log` → `read_entry` path.

---

### M-3 · Medium — `BinStub::apply_delta` is dead code documented as the recovery path

**File:line**: `crates/noxu-tree/src/tree.rs:991–1090`, `noxu-recovery/src/recovery_manager.rs:1244`

`apply_delta` is described as "`BINDeltaLogEntry.readEntry()` / `BIN.reconstituteBIN()`". The recovery manager mentions it only in a comment (`// or BinStub::apply_delta to reconstruct the node`). There are no call sites in the production tree that invoke `apply_delta`.

This creates three risks:
1. Future developers trust the documented path and call it, triggering C-2.
2. The recovery path has a documented but unimplemented step, leaving an unclear gap in the recovery correctness story.
3. The function's test coverage (if any) does not exercise the actually-used path (`mutate_to_full_bin` → `apply_delta_to_bin` → `insert_with_prefix`).

**Suggested fix**  
Remove `BinStub::apply_delta` entirely or replace its body with `unimplemented!("use mutate_to_full_bin instead")`. Update the recovery manager comment to point at `mutate_to_full_bin_from_log`.

---

### L-1 · Low — `compute_key_prefix(Some(i))` edge case with one remaining entry

**File:line**: `crates/noxu-tree/src/tree.rs:395–443`

When `entries.len() == 2` and `exclude_idx = Some(1)`, the function uses entry 0 as the seed but the inner loop body finds no second entry to compare against. `prefix_len` is never reduced from `seed_full.len()`, so the function returns the **entire key of entry 0** as the "prefix". `apply_new_prefix` then calls `compress_key` on entry 1's key, triggering `debug_assert!(key.starts_with(full_key_0))`, which fires in debug builds if entry 1 does not start with entry 0's key.

In release builds, `full_key_1[plen..]` silently produces an incorrect suffix when `full_key_1` does not start with `full_key_0`.

**Current exploitability**: None — all call sites use `compute_key_prefix(None)`. The `exclude_idx` parameter is only exercised in one unit test (`test_compute_key_prefix_exclude_first` at line 2107 of `bin.rs`, which passes `Some(0)`).

**Suggested fix**  
After the seed selection and before returning, add: `if the number of non-excluded entries < 2, return Vec::new()`. This matches the spirit of the `n < 2` early-exit at the top.

---

## Cross-cutting observations (not individual findings)

### Checksum algorithm (intentional deviation)
JE uses Adler32; Noxu uses CRC32 (crc32fast). This is documented as an intentional design choice in `docs/src/internal/checksum-selection.md` and is consistent with the "Rust-native format" claim. Not a finding, noted for completeness.

### Two-checkpoint deletion barrier (correct)
`FileSelector::process_checkpoint_end` correctly implements the two-checkpoint barrier as described in JE. The snapshot is taken at checkpoint-start (`get_checkpoint_state`), and `after_checkpoint` is called twice in tests to advance cleaned files to `safe_to_delete`. This matches JE `FileSelector.afterCheckpoint()`.

### `LogBufferSegment::put()` Release/Acquire ordering (correct)
The pin count is decremented with `Ordering::Release` in `put()` and loaded with `Ordering::Acquire` in `wait_for_zero_and_latch()`. The C-7 comment in the code correctly describes why this ordering is required. No finding.

### File creation directory fsync (correct)
`create_file_internal` calls `parent_dir.sync_all()` after the new file's `sync_all()`, ensuring the directory entry is durable (C-1 fix from prior audit). No finding.

### Two-InNode implementations (`bin.rs::InNode` vs `in_node.rs::InNode`)
`bin.rs` contains a lightweight `InNode` type that is separate from `in_node.rs::InNode`. The `tree.rs` uses its own `BinStub` / `InNodeStub`. This three-way split of the conceptually same type increases maintenance surface and makes it unclear which type is authoritative. Not a stand-alone finding, but compounds H-4 and H-5 by making it harder to share a single correct `findEntry` implementation.

---

*Report written to `/tmp/review-storage-core.md`.*
