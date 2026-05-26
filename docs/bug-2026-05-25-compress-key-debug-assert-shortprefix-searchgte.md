# Bug: `compress_key` debug-assert fires on `Cursor::get(Get::SearchGte)` over short prefix on a many-key tree

**Labels**: bug, noxu-tree, noxu-dbi
**Affects**: `noxu-tree`, `noxu-dbi`
**Severity**: high (panics in debug, undefined-but-likely-incorrect-behavior in release)

---

## 1. Expected result

`Cursor::get(&mut key, &mut value, Get::SearchGte, None)` with a short
seed key (e.g. 2 bytes) on a database that holds many keys sharing the
seed as a common prefix should:

* Position the cursor on the first key `>= seed`, OR
* Return `OperationStatus::NotFound` if no such key exists.

The cursor should never panic; the search-key length should not need to
match or exceed the BIN's internal key-prefix length. From an API user's
perspective, "give me the first key at or after `K\0`" is a primitive
range-scan operation, so a 2-byte seed across thousands of keys whose
full bytes look like `K\0bucket\0objectkey\0...` should work uniformly.

## 2. Actual result

`Cursor::get(... Get::SearchGte ...)` panics inside
`noxu_tree::tree::compress_key` with:

```
thread 'main' panicked at crates/noxu-tree/src/tree.rs:316:13:
compress_key: key does not start with current prefix
```

The debug-only `assert!` is at noxu-tree/src/tree.rs:316, in the
`compress_key` method on a tree node:

```rust
pub fn compress_key(&self, full_key: &[u8]) -> Vec<u8> {
    let plen = self.key_prefix.len();
    if plen == 0 {
        full_key.to_vec()
    } else {
        debug_assert!(
            full_key.starts_with(&self.key_prefix),
            "compress_key: key does not start with current prefix"
        );
        full_key[plen..].to_vec()
    }
}
```

It is reached from `cursor_impl::find_range_entry` when `Get::SearchGte`
seeks downward into a BIN whose live `key_prefix` is longer than the
2-byte search seed. The 2-byte search key cannot satisfy
`starts_with(self.key_prefix)` when `self.key_prefix.len() > 2`, so the
assertion fires.

In a release build the `debug_assert!` is a no-op, but the line that
follows (`full_key[plen..].to_vec()`) panics with a slice-out-of-bounds
panic when `full_key.len() < plen` -- so release builds also fail, just
with a different message.

## 3. noxu-db version

* Workspace `Cargo.toml` `[workspace.package] version = "0.1.0"`.
* Git HEAD: `79e14fe` ("Merge branch 'chore/stateright-and-hegel' into main",
  dated 2026-05-25).
* The bug reproduces against noxu-tree's current `tree.rs::compress_key`
  (line 316) and noxu-dbi's current `cursor_impl::find_range_entry`
  (line 854 region).

## 4. rustc version

```
rustc 1.95.0 (59807616e 2026-04-14)
```

## 5. Operating system

NixOS 25.11 (build 25.11.20260522.b77b3de), Linux 7.0.9 x86_64.

## 6. Minimal code sample

Below is the smallest standalone reproduction we constructed while
chasing this in `dyn-riak::datastore::noxu`. Drop into a new
`examples/repro_searchgte_short_prefix.rs` under `crates/noxu-db/` and
run with `cargo run --example repro_searchgte_short_prefix`.

```rust
//! Reproduces the `compress_key: key does not start with current
//! prefix` debug-assert that fires when `Cursor::get(SearchGte)`
//! seeds at a short prefix on a many-key tree.
//!
//! Expected: the cursor positions on the first key >= seed.
//! Actual: panic in noxu_tree::tree::compress_key.

use std::path::PathBuf;

use noxu_db::{
    Cursor, CursorConfig, Database, DatabaseConfig, DatabaseEntry, Environment,
    EnvironmentConfig, Get, OperationStatus,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let env_path: PathBuf = dir.path().to_path_buf();

    let env = Environment::open(&env_path, EnvironmentConfig::new().allow_create(true))?;
    let db = env.open_database(None, "repro", DatabaseConfig::new().allow_create(true))?;

    // Insert ~1000 keys all sharing the 2-byte primary tag b"K\0"
    // followed by a longer per-record body. Once the BIN structure
    // has been split a few times, internal nodes settle on a
    // key_prefix that extends BEYOND the 2-byte tag (e.g. b"K\0bucket\0").
    // That is the configuration in which the cursor seek below
    // panics.
    {
        let txn = env.begin_txn(None)?;
        for i in 0..1000u32 {
            let mut key = Vec::new();
            key.extend_from_slice(b"K\0");          // primary tag
            key.extend_from_slice(b"the-bucket\0"); // bucket name
            key.extend_from_slice(format!("object-{i:08}").as_bytes());
            let value = format!("payload-{i}");
            db.put(
                Some(&txn),
                &mut DatabaseEntry::from_bytes(&key),
                &mut DatabaseEntry::from_bytes(value.as_bytes()),
            )?;
        }
        txn.commit()?;
    }

    // Now seed a cursor with the SHORT 2-byte prefix only.
    let mut cursor: Cursor =
        db.open_cursor(None, Some(&CursorConfig::new()))?;
    let mut key = DatabaseEntry::from_bytes(b"K\0");
    let mut value = DatabaseEntry::new();

    // Panics here:
    //   thread 'main' panicked at crates/noxu-tree/src/tree.rs:316:13:
    //   compress_key: key does not start with current prefix
    let _status = cursor.get(&mut key, &mut value, Get::SearchGte, None)?;

    println!("first key after K\\0: {:?}", key.data());
    Ok(())
}
```

Notes on reliable reproduction:

* The bug only fires once the tree has split enough that a BIN's
  `key_prefix` is longer than the search seed. With fewer than ~50-100
  keys it usually does not split deeply enough; 1000 makes it
  reliable.
* Any short prefix triggers it; we hit it with `b"K\0"` (2 bytes) but
  any seed shorter than `tree.compress_key`'s
  `self.key_prefix.len()` will reach the same panic.
* The panic does NOT depend on the bucket name in the inserted keys.
  The same shape happens with synthetic `b"K\0xxx..."` keys; we use
  `the-bucket\0` here only so the BIN settles into a realistic
  per-bucket prefix.

## 7. Logs, panic messages, stack traces

```
thread 'main' panicked at /home/gburd/ws/lamdb/crates/noxu-tree/src/tree.rs:316:13:
compress_key: key does not start with current prefix
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

Backtrace (truncated to the relevant frames):

   3: core::panicking::panic_fmt
             at .../core/src/panicking.rs:75:14
   4: <noxu_tree::tree::Tree>::compress_key
             at noxu-tree/src/tree.rs:316:13
   5: noxu_dbi::cursor_impl::CursorImpl::find_range_entry
             at noxu-dbi/src/cursor_impl.rs:854
   6: noxu_dbi::cursor_impl::CursorImpl::get
             at noxu-dbi/src/cursor_impl.rs:645
   7: noxu_db::Cursor::get
             at noxu-db/src/lib.rs:...
   8: repro_searchgte_short_prefix::main
             at examples/repro_searchgte_short_prefix.rs:N
```

## 8. Where this was hit

We hit this in [Dynomite's `dyn-riak`](https://codeberg.org/gregburd/dynomite)
crate at `crates/dyn-riak/src/datastore/noxu.rs::scan_prefix` when
trying to walk every primary key under the `b"K\0"` tag for an AAE
(active anti-entropy) tree rebuild against a `NoxuDatastore` backend.

We worked around the bug by switching that specific code path to a
`Get::First` + `Get::Next` full cursor walk plus a per-record prefix
filter. That avoids the `SearchGte` panic but pays a full table scan
where a prefix-bounded scan would be cheaper. The 2i path (which uses
longer prefixes carrying both the bucket name and index name) does not
trigger the bug in our current tests, so it remains on the
`SearchGte`-seeded path.

## 9. Desired outcome

Pick whichever of these is correct for your model:

1. **`SearchGte` should accept seeds shorter than the encountered BIN's
   `key_prefix`.** That is, the cursor implementation should detect the
   short-seed case and either descend the tree without calling
   `compress_key` on the seed, or position itself on the first key
   `>= seed` directly via a comparison that does not require seed-
   `starts_with(key_prefix)`. This matches every other key-ordered
   store I have used; an LMDB or RocksDB cursor `seek_ge(b"K\0")` over
   a many-key DB does not require the seed to share the live
   prefix-compression layout.

2. **OR**: `compress_key` returns an explicit `Result<Vec<u8>, Error>`
   on prefix mismatch, and the caller (`find_range_entry`) treats
   "seed shorter than this BIN's prefix" as "the seed is to the left
   of this BIN's first key" -- i.e. position on the BIN's first slot
   and return `OperationStatus::Success`. That preserves the internal
   prefix-compression invariants while making the public cursor API
   work uniformly.

Option (2) is closer to the current code structure and probably the
smaller change.

## 10. Suggested test addition

When the fix lands, please add a regression test along the lines of
the reproducer in section 6 to noxu-dbi's cursor test suite. The
shape "many keys sharing a short tag, cursor seeded at the tag" is
the reliable trigger; any tighter or longer seed exits the bug
window.

Thank you. Happy to test a candidate fix against Dynomite's AAE
rebuild path on request.
