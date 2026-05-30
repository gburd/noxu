# Cursor public-API audit — 2026-05

**Auditor:** read-only audit by automated agent
**Date:** 2026-05-25
**Branch:** `fix/cursor-search-gte-cross-bin-walk`
**Scope:** Public `Cursor` API in `noxu-db`, plus the `Get` / `Put` enums and
`CursorConfig`, cross-referenced against the `CursorImpl` implementation in
`noxu-dbi` and the published mdBook chapters.

> **Trigger:** v1.4.2 (`SearchGte` panic on short prefix) and v1.4.3
> (`SearchGte` cross-BIN walk fix). This audit looks for bugs of the same
> shape elsewhere in the cursor surface.

---

## 1. Scope

### Audited

* `crates/noxu-db/src/cursor.rs` — the public `Cursor` type, its `get` /
  `put` / `delete` / `count` / `close` methods, and the `CursorState`
  state machine.
* `crates/noxu-db/src/get.rs` — the `Get` enum (every variant +
  classification helpers).
* `crates/noxu-db/src/put.rs` — the `Put` enum.
* `crates/noxu-db/src/cursor_config.rs` — `CursorConfig`.
* `crates/noxu-db/src/database.rs::open_cursor` — only the entry point
  that produces `Cursor` handles.
* `crates/noxu-dbi/src/cursor_impl.rs` — every method reachable from the
  public path: `search`, `search_dup`, `get_first`, `get_last`,
  `get_current`, `is_current_slot_deleted`, `retrieve_next`,
  `apply_dup_filter`, `put`, `put_dup`, `delete`, `count`, `dup`,
  `update_bin_pin`, `close`, `find_range_entry`, `find_bin_for_key`,
  `lock_ln`.
* `docs/src/getting-started/cursors.md`
* `docs/src/transactions/cursors.md`
* Every test under `crates/noxu-db/tests/` whose name matches
  `cursor` / `sorted_dup` and the in-module tests in `cursor.rs` and
  `cursor_impl.rs`.

### Explicitly **not** exercised

* No code was modified; no tests were run. All findings are derived from
  reading sources and tests. Reproducer sketches are written but not
  executed.
* I did not exercise concurrent paths under a real environment: every
  finding about concurrency / deadlock is by code reading only.
* I did not audit `noxu-db::SecondaryCursor`, `JoinCursor`, or the
  `noxu-collections` / `noxu-persist` cursor wrappers.
* I did not audit `noxu-tree::Tree::{get_next_bin, get_prev_bin,
  search}` — the v1.4.3 fix in `find_range_entry` is taken at face
  value.
* I did not audit replication / XA / recovery interactions with cursors.
* I did not audit the secondary-database write path (which opens
  cursors internally via `make_cursor_for_txn`).

---

## 2. Methodology

For each public surface element I:

1. Listed every variant / method (`Cursor::{get, put, delete, count,
   close, is_valid, get_state, is_read_only}`, all 16 `Get` variants,
   all 4 `Put` variants, all 5 fields of `CursorConfig`).
2. Read the rustdoc on the type and the corresponding paragraph in
   `docs/src/getting-started/cursors.md` and
   `docs/src/transactions/cursors.md`.
3. Read the dispatch table in `Cursor::get` /
   `Cursor::put` (cursor.rs:81–254) and traced into `CursorImpl`.
4. Cross-referenced the documented behaviour against BDB / BDB-JE
   semantics (`Cursor.getSearchKey`, `getSearchKeyRange`,
   `getSearchBoth`, `getSearchBothRange`, `getNextDup`,
   `getNextNoDup`, `getCurrent` after delete, `count`,
   `putNoOverwrite`, `putCurrent`, `putNoDupData`).
5. Looked specifically for the v1.4.2 / v1.4.3 issue shapes:
   * unguarded `compress_key` calls on user-controlled keys
     (panic on short prefix);
   * single-BIN inspection where a cross-BIN walk is required
     (silent `NotFound`).
6. Looked for `unwrap` / `panic!` / `unimplemented!` / `todo!` /
   `expect` on user-controlled inputs in the public path.
7. Checked which test files exercise each variant; flagged variants
   with no coverage.

---

## 3. Findings table

| # | Severity | Method / Variant | Doc claim | Actual | Recommendation |
|---|----------|------------------|-----------|--------|----------------|
| 1 | **Critical** | `Database::open_cursor(Some(&txn), …)` | `docs/src/transactions/cursors.md` teaches users to pass `Some(&txn)` so all cursor ops participate in the txn | `database.rs:648` binds the txn argument as `_txn` and drops it on the floor; cursor is built via `make_cursor()`, not `make_cursor_for_txn()`, so writes are auto-commit and reads do not participate in the txn's read-locks | Plumb `txn` through to `make_cursor_for_txn(t)`; remove the leading underscore; add an integration test that asserts a txn-bound cursor's writes vanish on `txn.abort()` |
| 2 | **High** | `docs/src/transactions/cursors.md` | Example calls `cursor.put_current(&new_data)` | No `put_current` method exists on the public `Cursor`; the real API is `cursor.put(&key, &data, Put::Current)` | Fix the doc example, or add `put_current` as a thin wrapper (the tests call it as such — see `cursor_test.rs:305` test name) |
| 3 | **High** | `Get::SearchLte`, `Get::FirstDup`, `Get::LastDup` | Documented in `get.rs` rustdoc as fully-functional variants ("Positions the cursor at the last record with a key less than or equal …", "positions at the first duplicate of the current key", "positions at the last duplicate of the current key") | All three fall through to the wildcard arm `_ => return Ok(OperationStatus::NotFound)` at `cursor.rs:206` and silently return `NotFound`. No log warning, no error | Either implement them (the underlying machinery for a reverse range search is straightforward given v1.4.3's `find_range_entry`) or fail loudly with `NoxuError::Unimplemented` until they are |
| 4 | **High** | `Get::SearchBoth` on a non-dup database | `get.rs` rustdoc: "matching both the key and the data … Returns an error if not found" and `requires_duplicates()` returns `true` | On a non-dup DB the public path goes into the non-dup `search` branch (`cursor_impl.rs:524–593`), which only checks key existence; the user-supplied data is **never compared to the stored data**. `Success` is returned even when the data differs | On a non-dup DB either compare the user data against the stored slot before returning `Success`, or refuse the call with an error; tests under `sorted_dup_test.rs:238` cover the dup case — the non-dup case is not exercised |
| 5 | **High** | `Get::NextDup` / `Get::PrevDup` on a non-dup database | `get.rs` rustdoc says "Returns an error if at the last duplicate"; `requires_duplicates()` is `true` | `is_sorted_dup()` is `false` on a non-dup DB, so `apply_dup_filter` is **never invoked** (`cursor_impl.rs:1284`). `NextDup` then degenerates into `Next`, `PrevDup` into `Prev`. The internal cursor.rs test `test_get_other_variant_returns_not_found` only happens to pass because the test DB has a single record | Reject `NextDup` / `PrevDup` on a non-dup DB with `NotFound` (BDB-JE) or an explicit error; add a regression test with two records |
| 6 | **High** | `Cursor::put(_, _, Put::Overwrite \| NoOverwrite \| NoDupData)` | rustdoc: "Stores a record using the cursor … cursor is positioned on the inserted record" | `cursor_impl.rs:1791,1814,1830` unconditionally set `self.current_index = 0` and never call `update_bin_pin` after a successful insert. If the inserted key is **not** at slot 0 of its BIN, `current_index` no longer reflects the cursor's actual position; a subsequent `Get::Next` traversal advances to slot 1 of a stale BIN (which is a different record than expected). The cursor pin / BIN-arc are also not updated, so the evictor may free the BIN the cursor "thinks" it is on | Compute the actual slot index after the insert (from the `BinEntry` returned by the tree) and call `update_bin_pin(Some(bin_arc))`; or, more conservatively, set `state = NotInitialized` after a non-`Current` put so the next `Get::Next` does a full re-position |
| 7 | **High** | `Cursor::delete()` followed by `Get::Next` / `Get::Prev` | docs/src/getting-started/cursors.md: "removes the record at the current cursor position. The cursor must have been successfully positioned … before calling delete" — silent on what happens next | `delete()` sets outer state to `NotInitialized` and clears inner `current_key`. `Get::Next` from `NotInitialized` then **resets to the first record** (`cursor.rs:121`), not to the record after the deleted slot. This breaks the ubiquitous "iterate-and-delete" idiom: after deleting any record except the last, the next `Get::Next` jumps back to the start of the database, leading to either an infinite loop or duplicate processing | Mirror BDB-JE: keep an "anchor" key after delete (the deleted key) and have `Get::Next` resume from `find_range_entry(anchor)` followed by one step. Alternatively, document the contract loudly and refuse `Get::Next` after `delete()` until the cursor is repositioned |
| 8 | **High** | `Cursor::get(.., Get::Current, ..)` after `delete()` | rustdoc: "Returns the record at the current cursor position. Useful after positioning" | `cursor.rs:91-93` calls `check_initialized()`, which fails because `delete()` reset outer `state` to `NotInitialized`. BDB-JE returns `KEYEMPTY` (NotFound) here, not an error | Have `Get::Current` return `OperationStatus::NotFound` when state is `NotInitialized` *because of a recent delete*, not an error. The cursor.rs:178-181 already handles the "deleted while cursor was positioned" case via `is_current_slot_deleted` — that branch is unreachable today because of the earlier `check_initialized` |
| 9 | **High** | `CursorConfig::{read_committed, non_sticky, evict_ln, prefix_constraint}` | All four fields are described in detail in rustdoc and shown on `Cursor` open in mdBook | `database.rs:655-657` only inspects `config.read_uncommitted` (and even then, conflates "read-uncommitted" with "read-only", `read_only = … config.read_uncommitted …`). The other four fields are silently ignored. `prefix_constraint` is documented as halting iteration at a prefix boundary — there is **no** code that reads it | Plumb each field into `CursorImpl` (read-committed → release read locks at each step; evict_ln → call evictor; prefix_constraint → bound `retrieve_next`); or remove the fields and update the docs |
| 10 | **Medium** | `Get::SearchBoth` empty-key handling | Sibling search variants (`Get::Search`, `Get::SearchGte`, `Get::SearchRange`) short-circuit on empty key (`cursor.rs:96-110`) and return `NotFound` | `cursor.rs:177-184` uses `key.get_data().unwrap_or(&[])` and proceeds with an empty key — inconsistent with the other Search variants. For sorted-dup databases this still works because `dup_key_data::combine(b"", data)` is a valid composite; for non-dup it walks into the non-dup search path and either finds an empty-key record (legal!) or returns NotFound | Either remove the empty-key short-circuit from `Search` / `SearchGte` (so empty keys are first-class) or add the same guard to `SearchBoth` for symmetry. The docs do not say empty keys are illegal — making them legal is the more honest fix |
| 11 | **Medium** | `Cursor::count()` on sorted-dup databases inside an active txn | rustdoc: "the count of records, or 0 if the cursor is not positioned" | `cursor_impl.rs:2099-2118` calls `self.dup(true)` to clone the cursor, then walks `PrevDup` / `NextDup`. `dup()` (`cursor_impl.rs:2140-2161`) **does not propagate `txn_ref`**. The scratch cursor's `lock_ln` falls into the auto-commit branch with a fresh cursor id; if the parent txn already holds WRITE locks on the dup LSNs (e.g., the txn just inserted them), the scratch cursor's READ-lock attempt blocks waiting on the parent txn → deadlock. Even without contention, the scratch cursor reads bypass the txn's lock set, so isolation is wrong | In `dup()` propagate `txn_ref` (and locker_id semantics) when `same_position == true`. Add a regression test: txn-bound cursor inserts N dups, calls `count()` — must return N without blocking |
| 12 | **Medium** | `Get::SearchBoth` requires_duplicates classification | `Get::requires_duplicates()` returns `true` for `SearchBoth` (get.rs:127) | The implementation does not enforce this — it routes to `search()` regardless of DB type. There is no `Get::SearchBothRange` variant in the public enum at all, even though `SearchMode::BothRange` exists in `cursor_impl.rs:280` and `search_dup` already implements it | Either add `Get::SearchBothRange` to the public surface (a small addition; the `SearchMode::BothRange` codepath is already there) or drop the dead code in `search_dup` |
| 13 | **Medium** | `Cursor::close()` is **not** idempotent at the public layer | rustdoc: "The cursor handle may not be used again after this call" — silent on double-close | `cursor.rs:295-302` returns `Err(OperationNotAllowed("Cursor already closed"))` on second close. The inner `CursorImpl::close()` is idempotent (`cursor_impl.rs:2207-2222` returns `Ok` on already-closed). The outer test `test_close_twice` codifies the error-on-double-close as intended, but it diverges from BDB-JE (`Cursor.close()` is documented idempotent) and from the layer below | Make outer `close()` idempotent (return `Ok` on already-closed), or document the divergence loudly. Consider also that `test_close_twice` may need updating |
| 14 | **Medium** | Outer `Cursor::close()` does **not** propagate to inner `CursorImpl::close()` | rustdoc: "The cursor handle may not be used again" implies resource release | `cursor.rs:295-302` only sets `self.state = Closed`. The BIN pin (`current_bin_arc`) is held by the inner `CursorImpl` and only released when the inner is dropped. If the outer `Cursor` is moved/leaked or kept alive between `close()` and drop, the evictor cannot free the BIN | Forward `close()` to `self.inner.close()` in the outer wrapper |
| 15 | **Medium** | `Cursor::Drop` warning fires even on normal explicit close | rustdoc on `close`: "The cursor handle may not be used again" | `cursor.rs:317-321`: `if self.state != CursorState::Closed { log::warn!("Cursor dropped without close"); }`. After `close()` outer state is `Closed` → no warning. But after `delete()` state is `NotInitialized` → warning fires when the cursor is dropped, even though the cursor was used correctly | Only warn when `state == Initialized` and the user did not call `close()`; or drop the warning entirely (Drop already runs `inner.close()` via `CursorImpl::Drop`) |
| 16 | **Medium** | `Cursor::count()` `.max(1)` mask | rustdoc: "For databases without duplicates, this always returns 1 if positioned" | `cursor.rs:281-283` does `.map(\|c\| c.max(1) as u64)`. For sorted-dup, if`inner.count()` ever returned 0 (it cannot today, but the API permits a future change), the public layer would silently report 1. This is pure defence in depth, but it hides correctness regressions in the dup-count path | Remove the `.max(1)`; if `inner.count()` is documented to return ≥1 when initialized, let it. Otherwise, log when the cap fires |
| 17 | **Low** | `Cursor::put(_, _, Put::*)` on a closed cursor returns `OperationNotAllowed` rather than `CursorClosed`-shaped error | rustdoc on `Cursor::put`: silent | `cursor.rs:226` calls `check_open()` which returns `NoxuError::OperationNotAllowed("Cursor has been closed")` — this is fine, but the *typed* error is the same one returned for "tried to put with a read-only cursor", "operation not allowed", etc. The caller cannot distinguish "closed" from "read-only" without parsing the message string | Introduce explicit `NoxuError::CursorClosed` and `NoxuError::CursorReadOnly` variants |
| 18 | **Low** | `Cursor::get` writes user-supplied `key` even on `Get::Current` | rustdoc: "Returns the record at the current cursor position" | `cursor.rs:194-195`: `data.set_data(&v); key.set_data(&k);`. For `Get::Current` this means the user's key buffer is overwritten with the canonical stored key (which may differ from what they passed in if they had used `set_data` for some reason). Mostly harmless but undocumented | Document that `Get::Current` overwrites both buffers, matching the other arms |
| 19 | **Low** | `Cursor::put(.., Put::Overwrite)` on a *previously-deleted* cursor | rustdoc: silent | After `delete()` outer state is `NotInitialized`, `read_only` is whatever it was. `put(.., Put::Overwrite)` does **not** call `check_initialized` (correct — Overwrite doesn't require positioning), so it inserts at `key`. Inner cursor moves to the inserted key. This is fine but noteworthy when reading `cursor_test.rs::cursor_delete_removes_current_record` — the inner state machine is more permissive than docs suggest | Add a doc note in the iterate-and-delete section of `getting-started/cursors.md` |
| 20 | **Info** | `compress_key` panic shape (v1.4.2) — **mitigated correctly** | — | Verified: the only callers of `bin.compress_key` from the cursor public path are `find_range_entry` (`cursor_impl.rs:847-869`) and `get_data_from_tree` (`cursor_impl.rs:801-827`). Both guard with `key.starts_with(bin.key_prefix.as_slice())` before calling `compress_key`. No other public-API path reaches `compress_key` with a user-controlled key | None — the v1.4.2 fix shape is contained |
| 21 | **Info** | Cross-BIN walk in `find_range_entry` (v1.4.3) — **correct, with caveat** | rustdoc says one probe is enough | The two-step probe argument in the rustdoc (cursor_impl.rs:711-755) is sound for non-empty BINs. The "empty intermediate BIN" caveat — already disclosed at line 766-770 — means a transiently-empty next BIN under heavy delete load yields `NotFound` rather than walking another step. This is consistent with `Get::Next`'s behaviour but is a known limitation | None — caveat is documented inline, with a follow-up reference |
| 22 | **Info** | `Get::SearchRange` is just an alias for `Get::SearchGte` | rustdoc: "Alias for `SearchGte`" | Verified at `cursor.rs:106-110` (combined match arm) | None — alias is consistent |

---

## 4. Detailed findings

### Finding 1 — `open_cursor(Some(&txn), …)` silently drops the txn (CRITICAL)

* **File:** `crates/noxu-db/src/database.rs:648-666`
* **Doc claim:** `docs/src/transactions/cursors.md` teaches:

  ```rust
  let txn = env.begin_transaction(None, None)?;
  let mut cursor = db.open_cursor(Some(&txn), None)?;
  …
  cursor.put_current(&new_data)?;       // expected to participate in txn
  …
  txn.commit()?;
  ```

  The intent is that **all** cursor operations join the txn — writes are
  rolled back on `txn.abort()` and reads pick up the txn's read locks.
* **Actual behaviour:**

  ```rust
  pub fn open_cursor(
      &self,
      _txn: Option<&Transaction>,           //  ← bound with leading underscore
      config: Option<&CursorConfig>,
  ) -> Result<Cursor> {
      …
      let cursor_impl = if read_only {
          CursorImpl::new(Arc::clone(&self.db_impl), 0)
      } else {
          self.make_cursor()                //  ← not make_cursor_for_txn
      };
  ```

  `make_cursor_for_txn` exists (database.rs:151-156) and is used by
  `db.get()`, `db.put()`, `db.delete()` — but **not** by `open_cursor`.
  Result: cursor writes are auto-commit, cursor reads are not txn-locked.
* **Expected (BDB-JE):** Cursor opened with a Transaction handle binds
  every `get` / `put` / `delete` to the txn; the cursor must be closed
  before `txn.commit/abort`.
* **Reproducer sketch:**

  ```rust
  let txn = env.begin_transaction(None, None)?;
  let mut cursor = db.open_cursor(Some(&txn), None)?;
  cursor.put(&k("a"), &v("1"), Put::Overwrite)?;
  cursor.close()?;
  txn.abort()?;
  // BDB-JE: db.get(None, &k("a"), &mut out) -> NotFound
  // Noxu DB today: returns Success with v="1" (write was auto-committed)
  ```

* **Recommendation:** Plumb `txn` through:

  ```rust
  let cursor_impl = match txn {
      Some(t) => self.make_cursor_for_txn(t),
      None    => self.make_cursor(),
  };
  ```

  Add an integration test asserting that aborting a txn rolls back
  cursor writes.

---

### Finding 2 — `cursor.put_current` does not exist (HIGH)

* **File:** `docs/src/transactions/cursors.md:53` (`cursor.put_current(&new_data)?;`)
* **Doc claim:** Snippet implies a `Cursor::put_current(&data)` shortcut
  for "replace the value at the current position".
* **Actual:** No such method on `Cursor`. The real call is
  `cursor.put(&key, &new_data, Put::Current)` (verified by grepping
  `crates/noxu-db/src/`). The internal test name
  `cursor_put_current_updates_current_record` (cursor_test.rs:305) refers
  to `Put::Current`, not a method.
* **Reproducer:** copy-paste the doc example into a project — it fails
  to compile.
* **Recommendation:** Either fix the doc to use
  `cursor.put(&key, &new_data, Put::Current)`, or add a
  `put_current(&self, data: &DatabaseEntry)` convenience wrapper.

---

### Finding 3 — `Get::SearchLte` / `Get::FirstDup` / `Get::LastDup` silently return `NotFound` (HIGH)

* **File:** `crates/noxu-db/src/cursor.rs:206`

  ```rust
  _ => return Ok(OperationStatus::NotFound),
  ```

* **Doc claim:** `crates/noxu-db/src/get.rs:73-89, 91-99`
  document each variant as a working positioning operator.
* **Actual:** None of these variants have a match arm in
  `Cursor::get`; all three fall through to the wildcard arm and
  return `NotFound` regardless of database contents.
* **Expected (BDB-JE):** `getSearchKeyRange` (LTE), `getFirstDup`,
  `getLastDup` are real operations.
* **Reproducer:**

  ```rust
  // DB contains [("a","1"), ("b","2"), ("c","3")]
  let mut k = DatabaseEntry::from_bytes(b"b");
  let mut v = DatabaseEntry::new();
  cursor.get(&mut k, &mut v, Get::SearchLte, None).unwrap();
  // Returns NotFound — should return ("b","2") or ("a","1")
  ```

* **Recommendation:** Either implement (the existing
  `find_range_entry` in `cursor_impl.rs:756-810` adapts
  straightforwardly to LTE by walking left), or change the wildcard
  arm to:

  ```rust
  _ => return Err(NoxuError::Unimplemented(format!("{:?}", get_type))),
  ```

  so users see a loud error instead of a silent miss.

---

### Finding 4 — `Get::SearchBoth` ignores data on non-dup databases (HIGH)

* **File:** `crates/noxu-db/src/cursor.rs:177-184` →
  `crates/noxu-dbi/src/cursor_impl.rs:524-593`
* **Doc claim:** `get.rs:18-21`: "For duplicate databases, searches for
  a record matching both the key and the data. Returns an error if not
  found."
* **Actual:** On a non-dup database the request goes through the
  non-dup `search()` arm (`is_sorted_dup() == false`). That arm only
  consults `tree.search(key).exact_parent_found` — the user-supplied
  `data` parameter is **never compared** to the slot's data. The
  cursor returns `Success` whenever the *key* is present, regardless
  of what `data` was passed.
* **Expected (BDB-JE):** `getSearchBoth` returns `NotFound` if the
  data does not match the slot's data even on a non-dup DB.
* **Reproducer sketch:**

  ```rust
  // Non-dup DB: insert ("k","stored")
  let mut k = DatabaseEntry::from_bytes(b"k");
  let mut d = DatabaseEntry::from_bytes(b"different");
  let s = cursor.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
  assert_eq!(s, OperationStatus::NotFound);   // currently fails — returns Success
  ```

* **Recommendation:** In the non-dup `search` arm under
  `SearchMode::Both`, compare `slot_data` against `data.unwrap_or(&[])`
  and return `NotFound` if they differ. Add a regression test in
  `cursor_test.rs`.

---

### Finding 5 — `Get::NextDup` / `Get::PrevDup` degenerate on non-dup DBs (HIGH)

* **File:** `crates/noxu-dbi/src/cursor_impl.rs:1284` and
  `1380-1395`, `1467-1481`.
* **Doc claim:** `get.rs:96-99`: "Returns an error if at the last
  duplicate." `Get::requires_duplicates()` returns `true`.
* **Actual:** `retrieve_next` reads `is_dup = self.is_sorted_dup();`
  and only invokes `apply_dup_filter` when `is_dup == true`. On a
  non-dup DB, `NextDup` falls through into the same code path as
  `Next` — it advances to the next entry regardless of key. This
  means `Get::NextDup` quietly returns the next record (i.e., a
  *different* key), violating the documented contract.
* **Reproducer:**

  ```rust
  // Non-dup DB: insert ("a","1"), ("b","2")
  cursor.get(&mut k_of_a, &mut v, Get::Search, None);
  let s = cursor.get(&mut k, &mut v, Get::NextDup, None).unwrap();
  // BDB-JE: NotFound
  // Noxu DB today: Success, key=="b"
  ```

* **Recommendation:** Early-return `NotFound` from `retrieve_next`
  when `mode` is `NextDup` / `PrevDup` and the DB is non-dup, before
  any traversal.

---

### Finding 6 — `put` resets `current_index = 0` and skips the BIN-pin update (HIGH)

* **File:** `crates/noxu-dbi/src/cursor_impl.rs`:
  * `1791` (NoOverwrite), `1814` (NoDupData), `1830` (Overwrite).
* **Doc claim:** rustdoc on `Cursor::put` and `CursorImpl::put`: "the
  cursor is positioned at the newly written record".
* **Actual:** Each non-`Current` put assigns

  ```rust
  self.current_index = 0;
  ```

  *unconditionally*, regardless of where the inserted/updated key
  actually lives in its BIN. None of these arms call
  `update_bin_pin(...)`. Two consequences:
  1. A subsequent `Get::Next` runs the fast path
     (`current_bin_arc` happens to still be set to the previous
     BIN, or `None`) with `next_index = current_index + 1 = 1`,
     reading slot 1 of either the wrong BIN or the right BIN — but
     not from the just-inserted slot.
  2. The evictor's `cursor_count` invariant is violated: the BIN the
     cursor is logically positioned on is not pinned.
* **Reproducer (sketch):**

  ```rust
  // DB pre-populated with 100 records so the leaf has multiple slots
  cursor.put(&k("z"), &v("v"), Put::Overwrite)?;        // ends up at slot 99
  // current_index is now 0, but real position is 99.
  let s = cursor.get(&mut k, &mut d, Get::Next, None)?;
  // Expectation: NotFound (z was last); actual: returns slot 1 of stale BIN.
  ```

* **Recommendation:** After a successful insert, look up the actual
  slot index in the resulting BIN (via `find_bin_for_key` + binary
  search on the suffix) and call `update_bin_pin(Some(bin_arc))`.
  Cheaper alternative: set `state = NotInitialized` after a non-`Current`
  put so the next traversal does a full re-position from the key (this
  matches some BDB drivers' "post-put cursor is logically at the new
  record but a re-fetch is required to iterate from there").

---

### Finding 7 — `delete()` then `Get::Next` jumps to first record, not next (HIGH)

* **File:** `crates/noxu-db/src/cursor.rs:117-130` (`Get::Next`
  branch), `crates/noxu-dbi/src/cursor_impl.rs:2070-2080` (`delete`).
* **Doc claim:** `docs/src/getting-started/cursors.md` shows iterate-
  and-delete as a routine pattern; rustdoc on `delete()` says
  "removes the record at the current cursor position", silent on
  what `Get::Next` does afterwards.
* **Actual:** `CursorImpl::delete` clears `current_key` and resets
  `state` to `NotInitialized`; the outer `Cursor` mirrors this.
  `Cursor::get(.., Get::Next, ..)` then sees
  `state == NotInitialized` and routes to `inner.get_first()`,
  jumping back to the start of the database.
* **Expected (BDB-JE):** After delete the cursor remains "logically
  at" the deleted record; `getNext` moves to the record that
  followed it.
* **Reproducer:**

  ```rust
  // DB = [("a","1"), ("b","2"), ("c","3")]
  let mut k = DatabaseEntry::from_bytes(b"b");
  let mut v = DatabaseEntry::new();
  cursor.get(&mut k, &mut v, Get::Search, None)?;     // positioned on b
  cursor.delete()?;                                   // delete b
  let s = cursor.get(&mut k, &mut v, Get::Next, None)?;
  // BDB-JE: returns ("c","3")
  // Noxu DB: returns ("a","1") — back to first
  ```

  This breaks the canonical sweep-and-delete loop:

  ```rust
  let mut s = cursor.get(.., Get::First, None)?;
  while s == Success {
      if predicate(&data) { cursor.delete()?; }
      s = cursor.get(.., Get::Next, None)?;        // ← will infinite-loop on first match
  }
  ```

* **Recommendation:** On delete, store the deleted key as a separate
  `anchor_after_delete: Option<Vec<u8>>` field. In `retrieve_next` /
  `Get::Next` from `NotInitialized`, if the anchor is set, use
  `find_range_entry` to position at the smallest key > anchor and
  return that record (and clear the anchor). The reverse direction
  is symmetric.

---

### Finding 8 — `Get::Current` after `delete()` returns an error, not `NotFound` (HIGH)

* **File:** `crates/noxu-db/src/cursor.rs:91-93`
* **Doc claim:** rustdoc on `Get::Current` (get.rs:60-65): "Returns
  the record at the current cursor position. Useful after positioning
  the cursor to re-read the record."
* **Actual:** `if matches!(get_type, Get::Current) { self.check_initialized()?; }`.
  Because `delete()` resets state to `NotInitialized`, `Get::Current`
  errors with `OperationNotAllowed("Cursor is not positioned on a
  record")`. The follow-up `is_current_slot_deleted()` branch at
  cursor.rs:177-181 is therefore unreachable post-delete.
* **Expected (BDB-JE):** `getCurrent` after a delete returns
  `KEYEMPTY` (the equivalent of `OperationStatus::NotFound`), not an
  error.
* **Recommendation:** Either keep state as `Initialized` after
  `delete()` and let `is_current_slot_deleted()` translate to
  `NotFound`, or add a third state `Deleted` and special-case
  `Get::Current` to return `NotFound` for it. The latter matches the
  `CursorState` doc comment which already mentions `Deleted` in the
  audit prompt.

---

### Finding 9 — `CursorConfig::{read_committed, non_sticky, evict_ln, prefix_constraint}` are silently ignored (HIGH)

* **File:** `crates/noxu-db/src/database.rs:655-657`

  ```rust
  let read_only = config.map(|c| c.read_uncommitted).unwrap_or(false)
      || self.config.read_only;
  ```

* **Doc claim:** `cursor_config.rs:13-44` documents all five fields.
  In particular:
  * `read_committed` — "Read locks are released when the cursor moves
    to a new position."
  * `read_uncommitted` — "No read locks are acquired, allowing dirty
    reads."
  * `non_sticky` — "Non-sticky cursors are automatically closed when
    the transaction commits."
  * `evict_ln` — "fetched LN data is evicted from the cache after the
    cursor operation completes."
  * `prefix_constraint` — "the cursor will stop advancing (return
    NotFound) when the fetched key no longer shares this prefix."
* **Actual:**
  * `read_committed` — never read in `database.rs`.
  * `read_uncommitted` — read **only** to set `read_only = true`,
    which is the *opposite* of dirty-read semantics: a "read
    uncommitted" cursor in BDB-JE is read-only in the sense that
    it doesn't take read locks; here it is mistranslated into
    "cannot perform writes". A cursor with
    `set_read_uncommitted(true)` is unable to call `put` or
    `delete`.
  * `non_sticky` — never read.
  * `evict_ln` — never read.
  * `prefix_constraint` — never read; `retrieve_next` knows nothing
    about prefixes. The user-visible bound from the v1.4.x
    `cursor_search_gte_*` test suite uses `Get::SearchGte` plus
    manual `starts_with` in user code, not `prefix_constraint`.
* **Recommendation:**
  * Drop the `read_only = … read_uncommitted …` conflation; treat
    `read_uncommitted == true` as "do not call `lock_ln`", not "deny
    writes". (Read-only cursors should be a separate concept tied to
    `db.config.read_only`.)
  * Plumb the other fields or remove them from the public surface.
  * If keeping the surface, add an `#[doc(hidden)]` "currently a
    no-op" warning to each unused field, or — better — make
    `with_*` methods that touch unimplemented fields return an error.

---

### Finding 10 — Empty-key contract is asymmetric across `Get` variants (MEDIUM)

* **File:** `crates/noxu-db/src/cursor.rs:96-110, 177-184`
* **Doc claim:** Neither rustdoc nor mdBook mention empty keys.
* **Actual:** `Get::Search` and `Get::SearchGte` / `Get::SearchRange`
  short-circuit on empty key (`Some(k) if !k.is_empty() => k, _ =>
  return NotFound`). `Get::SearchBoth` uses
  `key.get_data().unwrap_or(&[])` and proceeds. The inner
  `CursorImpl::search` accepts empty keys throughout.
* **Recommendation:** Pick one rule and apply it everywhere:
  * either empty keys are first-class (matches BDB-JE) — drop the
    short-circuit from Search/SearchGte and document that empty keys
    are valid;
  * or empty keys are illegal — apply the same guard to SearchBoth
    and document the contract.

---

### Finding 11 — `count()` on dup DB inside a txn may deadlock or under-count (MEDIUM)

* **File:** `crates/noxu-dbi/src/cursor_impl.rs:2099-2118`
  (`count`) and `2140-2161` (`dup`).
* **Doc claim:** rustdoc on `Cursor::count`: "Count the number of
  records with the same key."
* **Actual:** `count` builds a scratch cursor via `self.dup(true)`.
  `dup()` clones `current_*` fields but **does not propagate
  `txn_ref`** — it sets it to `None` implicitly. Inside the loop
  the scratch cursor's `lock_ln` therefore goes through the
  `lock_manager` auto-commit branch with `self.id` as the locker
  (a fresh cursor id). Two consequences:
  1. The scratch cursor's reads are not part of the parent txn, so
     other isolation-level guarantees are violated.
  2. If the parent txn holds WRITE locks on the dup LSNs (e.g., the
     txn just inserted the dups it is now counting), the auto-commit
     READ-lock attempt blocks on the WRITE lock owner — which is
     the parent txn itself. The scratch cursor's lock manager call
     may dead-lock.
* **Reproducer (sketch):**

  ```rust
  let txn = env.begin_transaction(None, None)?;
  let mut cur = db.open_cursor(Some(&txn), None)?;     // (also see Finding 1)
  cur.put(&k("k"), &v("a"), Put::Overwrite)?;
  cur.put(&k("k"), &v("b"), Put::Overwrite)?;
  cur.put(&k("k"), &v("c"), Put::Overwrite)?;
  cur.get(&mut DatabaseEntry::from_bytes(b"k"), &mut DatabaseEntry::new(),
          Get::Search, None)?;
  let n = cur.count()?;        // expected 3 — may block forever
  ```

* **Recommendation:** In `dup()` propagate `txn_ref` whenever
  `same_position == true`, or have `count()` walk the dups in-place
  using a saved key + `find_range_entry` rather than allocating a
  scratch cursor.

---

### Finding 12 — `Get::SearchBothRange` is missing from the public enum (MEDIUM)

* **File:** `crates/noxu-db/src/get.rs` — there is no `SearchBothRange`
  variant. `crates/noxu-dbi/src/cursor_impl.rs:280, 333-339, 362-364`
  fully implements `SearchMode::BothRange` for sorted-dup databases.
* **Doc claim:** mdBook does not promise the variant; but BDB / BDB-JE
  callers expect `getSearchBothRange` for the
  "first record with key == K and data >= D" use case.
* **Recommendation:** Either add the public variant (it already maps
  cleanly to `SearchMode::BothRange`) or remove the dead implementation
  paths.

---

### Finding 13 — `Cursor::close()` is non-idempotent at the public layer (MEDIUM)

* **File:** `crates/noxu-db/src/cursor.rs:295-302`
* **Doc claim:** rustdoc — "The cursor handle may not be used again
  after this call". Silent on idempotency.
* **Actual:** Second `close()` returns
  `Err(OperationNotAllowed("Cursor already closed"))`. The inner
  `CursorImpl::close()` is idempotent (`cursor_impl.rs:2207-2213`).
* **Expected (BDB-JE):** `Cursor.close()` is documented to be safe to
  call multiple times.
* **Recommendation:** Make outer `close()` idempotent; remove
  `test_close_twice` or update it to the new contract.

---

### Finding 14 — Outer `close()` does not propagate to inner (MEDIUM)

* **File:** `crates/noxu-db/src/cursor.rs:295-302`
* **Actual:** Sets only `self.state = Closed`; the inner
  `CursorImpl` keeps its `current_bin_arc` pinned. The pin is
  released only when the outer `Cursor` is dropped (which fires
  `CursorImpl::Drop` → `close()`).
* **Recommendation:**

  ```rust
  pub fn close(&mut self) -> Result<()> {
      if self.state == CursorState::Closed { return Ok(()); }
      self.state = CursorState::Closed;
      self.inner.close().map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))
  }
  ```

---

### Finding 15 — `Cursor::Drop` warns for legitimate post-`delete` drops (MEDIUM)

* **File:** `crates/noxu-db/src/cursor.rs:317-321`
* **Actual:** Warning fires whenever `state != Closed` at drop time,
  including when state is `NotInitialized` because of a recent
  `delete()` (Finding 7) followed by a `?`-bubble that aborts the
  function before `cursor.close()`.
* **Recommendation:** Only warn for `Initialized`; or fold the
  warning into `inner.close()` (which has more context to decide
  whether the leak is real).

---

### Finding 16 — `count()` `.max(1)` masks underlying bugs (MEDIUM)

* **File:** `crates/noxu-db/src/cursor.rs:281-283`
* **Actual:** `c.max(1) as u64` clamps any zero or negative return
  from `inner.count()` to 1. Today `inner.count()` returns 1 for
  non-dup and ≥1 for dup-with-positioned-cursor, so the clamp is
  inert — but it would silently hide a regression that returned 0.
* **Recommendation:** Drop the clamp, or assert when it fires.

---

### Findings 17–22 — see findings table for low-severity / informational items

---

## 5. Coverage gaps in tests

The following code paths are not exercised by any test under
`crates/noxu-db/tests/` or the in-module tests in `cursor.rs` /
`cursor_impl.rs`:

| Path | Where | Why it matters |
|------|-------|----------------|
| `Get::SearchLte` | nowhere | Finding 3 — silent NotFound never asserted |
| `Get::FirstDup` | nowhere | Finding 3 |
| `Get::LastDup` | nowhere | Finding 3 |
| `Get::SearchBoth` on a non-dup DB | nowhere (only `sorted_dup_test.rs:238`) | Finding 4 — would expose the data-comparison bug |
| `Get::NextDup` / `Get::PrevDup` on a non-dup DB **with > 1 record** | only the trivially-passing `cursor.rs:test_get_other_variant_returns_not_found` (single record) | Finding 5 |
| Iterate-and-delete loop (`First`, then `Search`+`delete`+`Next` in a loop) | nowhere — `cursor_test.rs:cursor_delete_*` tests delete a single record then close | Finding 7 |
| `Get::Current` after `delete()` | nowhere | Finding 8 |
| `put(.., Put::Overwrite)` followed by `Get::Next` | nowhere — every `put` test calls `cursor.close()` immediately or does another `Get::Search` (which masks the stale `current_index`) | Finding 6 |
| `CursorConfig` field plumbing — `read_committed`, `non_sticky`, `evict_ln`, `prefix_constraint` | nowhere reads or asserts effect | Finding 9 |
| `open_cursor(Some(&txn), …)` followed by `txn.abort()` and observing the writes are gone | nowhere (`isolation_test.rs:781` opens with `Some(&txn)` but only commits) | Finding 1 |
| `count()` on a sorted-dup DB inside a txn that just inserted the dups | nowhere — `sorted_dup_test.rs:265 test_dup_count` runs without an explicit txn | Finding 11 |
| Concurrent split during cursor traversal (does the BIN-pin keep the cursor on the original BIN?) | `concurrent_reads_during_splits.rs` exists but does not pin-and-step a cursor explicitly | Defence-in-depth |
| Empty-key `Get::Search` | only `cursor.rs:test_search_empty_key_returns_not_found` (asserts NotFound short-circuit) | Confirms the asymmetry of Finding 10 |
| Empty-key `Get::SearchBoth` | nowhere | Finding 10 |

---

## 6. Summary by severity

| Severity | Count | Findings |
|----------|------:|----------|
| Critical | 1 | #1 (txn ignored on `open_cursor`) |
| High | 8 | #2, #3, #4, #5, #6, #7, #8, #9 |
| Medium | 7 | #10, #11, #12, #13, #14, #15, #16 |
| Low | 3 | #17, #18, #19 |
| Info | 3 | #20, #21, #22 |
| **Total** | **22** | |

### Tally of bugs of the v1.4.2 / v1.4.3 shape

* **v1.4.2-shape (panic on `compress_key` with user-controlled key):**
  none found in the public path. Both call sites
  (`get_data_from_tree`, `find_range_entry`) carry the
  `starts_with` guard. (Finding 20)

* **v1.4.3-shape (single-BIN inspection where a cross-BIN walk is
  needed):** none found in the *audited* code paths — `retrieve_next`
  does walk to the adjacent BIN, `find_range_entry` does its
  one-probe + cross-BIN step, and `apply_dup_filter` walks BIN
  boundaries in the `NextNoDup` / `PrevNoDup` loops. The known
  caveat is the "transient empty next BIN" case already documented
  inline (Finding 21).

The two recent fixes therefore appear to be properly contained, but
the *broader* cursor surface has accumulated a number of
documentation-vs-implementation gaps and at least one critical
correctness gap (Finding 1) that should be prioritized.
