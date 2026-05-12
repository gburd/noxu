# The Binding Layer

## Why Bindings?

`DatabaseEntry` holds raw bytes. To store typed Rust values with sort-preserving key encodings, Noxu DB provides the `noxu-bind` crate. The binding layer converts typed values to and from byte arrays in a way that:

- Preserves sort order: sorted byte comparison produces the same order as sorted value comparison.
- Is compact and fast to encode/decode.
- Handles edge cases like negative integers and NaN-free floating-point values.

## Available Bindings

Add `noxu-bind` to your `Cargo.toml`:

```toml
[dependencies]
noxu-bind = { path = "crates/noxu-bind" }
```

Available bindings in `noxu_bind`:

| Type | Binding | Notes |
|---|---|---|
| `i32` | `IntBinding` | Sort-preserving signed 32-bit integer |
| `i64` | `LongBinding` | Sort-preserving signed 64-bit integer |
| `f64` | `SortedDoubleBinding` | Sort-preserving IEEE 754 double |
| `String` | `StringBinding` | UTF-8 string, null-byte safe |

All bindings implement the `EntryBinding<T>` trait with two methods:
- `object_to_entry(&self, value: &T, entry: &mut DatabaseEntry)` — encode value into entry
- `entry_to_object(&self, entry: &DatabaseEntry) -> Result<T>` — decode entry back to value

## Integer Keys

```rust
use noxu_bind::{EntryBinding, IntBinding};
use noxu_db::{DatabaseEntry, OperationStatus};

let binding = IntBinding::new();

// Store an integer key
let mut key_entry = DatabaseEntry::new();
let value: i32 = 42;
binding.object_to_entry(&value, &mut key_entry)?;
db.put(None, &key_entry, &DatabaseEntry::from_bytes(b"forty-two"))?;

// Look up by integer key
let mut search_key = DatabaseEntry::new();
binding.object_to_entry(&42i32, &mut search_key)?;
let mut data = DatabaseEntry::new();
if db.get(None, &search_key, &mut data)? == OperationStatus::Success {
    println!("{}", std::str::from_utf8(data.data())?);
}
```

Because `IntBinding` produces sort-preserving byte encodings, records are stored and retrieved in numeric order. `i32::MIN` sorts before -1 sorts before 0 sorts before 1 sorts before `i32::MAX`.

## String Keys

```rust
use noxu_bind::{EntryBinding, StringBinding};

let binding = StringBinding::new();

let mut key_entry = DatabaseEntry::new();
binding.object_to_entry(&"Alice".to_string(), &mut key_entry)?;
db.put(None, &key_entry, &DatabaseEntry::from_bytes(b"alice's data"))?;

// Decode a string from an entry after retrieval
let recovered: String = binding.entry_to_object(&key_entry)?;
assert_eq!(recovered, "Alice");
```

## Sorted Double Keys

```rust
use noxu_bind::{EntryBinding, SortedDoubleBinding};

let binding = SortedDoubleBinding::new();

let temperatures = [-273.15f64, -40.0, 0.0, 37.0, 100.0];
for &temp in &temperatures {
    let mut key_entry = DatabaseEntry::new();
    binding.object_to_entry(&temp, &mut key_entry)?;
    let label = format!("{:.2}°C", temp);
    db.put(None, &key_entry, &DatabaseEntry::from_bytes(label.as_bytes()))?;
}
// When iterated, records appear in ascending numeric temperature order.
```

## Long Keys with Round-Trip

```rust
use noxu_bind::{EntryBinding, LongBinding};

let binding = LongBinding::new();

let mut key_entry = DatabaseEntry::new();
binding.object_to_entry(&i64::MAX, &mut key_entry)?;

// ... store and retrieve ...

let mut data_entry = DatabaseEntry::new();
db.get(None, &key_entry, &mut data_entry)?;
let recovered: i64 = binding.entry_to_object(&data_entry)?;
```

## Custom Encodings

For complex types you implement your own encoding. Write the fields to a `Vec<u8>` in the order that determines sort priority. The first bytes written have the highest sort weight.

```rust
struct Point { x: i32, y: i32 }

fn encode_point(p: &Point) -> DatabaseEntry {
    let mut buf = Vec::with_capacity(8);
    // Sort by x first, then y (big-endian so bytes sort correctly)
    buf.extend_from_slice(&(p.x ^ i32::MIN).to_be_bytes()); // sign-bit flip for signed sort
    buf.extend_from_slice(&(p.y ^ i32::MIN).to_be_bytes());
    DatabaseEntry::from_vec(buf)
}

fn decode_point(entry: &DatabaseEntry) -> Point {
    let bytes = entry.data();
    let x = i32::from_be_bytes(bytes[0..4].try_into().unwrap()) ^ i32::MIN;
    let y = i32::from_be_bytes(bytes[4..8].try_into().unwrap()) ^ i32::MIN;
    Point { x, y }
}
```

This technique (XOR with `MIN` before big-endian encoding) is the same approach used internally by `IntBinding` and `LongBinding`.

---

