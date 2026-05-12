# Database Records

## The DatabaseEntry Type

Every Noxu DB record consists of two parts: a key and a data value. Both are represented as `DatabaseEntry` objects, which are essentially wrappers around a byte slice (`&[u8]`).

`DatabaseEntry` is the universal container for moving data in and out of the database. Any type that can be serialized to bytes can be stored in Noxu DB.

## Creating DatabaseEntry Objects

```rust
use noxu_db::DatabaseEntry;

// From a byte literal
let key = DatabaseEntry::from_bytes(b"employee:1001");

// From a String (always use explicit UTF-8 encoding)
let name = "Alice".to_string();
let key = DatabaseEntry::from_bytes(name.as_bytes());

// From a Vec<u8>
let raw: Vec<u8> = vec![0x01, 0x02, 0x03];
let entry = DatabaseEntry::from_vec(raw);

// Empty entry (used as an output buffer for get operations)
let mut data_out = DatabaseEntry::new();
```

## Reading Data Back

After a `get` operation populates a `DatabaseEntry`, use `.data()` to access the raw bytes:

```rust
let mut data = DatabaseEntry::new();
let status = db.get(None, &key, &mut data)?;
if status == OperationStatus::Success {
    let bytes: &[u8] = data.data();
    let text = std::str::from_utf8(bytes)?;
    println!("Got: {}", text);
}
```

Use `.get_data()` when you want an `Option<&[u8]>` (returns `None` for an empty entry):

```rust
if let Some(bytes) = data.get_data() {
    // bytes is &[u8]
}
```

## Encoding Structured Data

Because `DatabaseEntry` stores raw bytes, you must decide how to encode your application's data types. Options include:

- **UTF-8 strings** — human-readable, easy for debugging.
- **`bincode` or `serde` serialization** — compact, works with any `Serialize`/`Deserialize` type.
- **Noxu bind APIs** — sort-preserving encodings for integers, floats, and strings (described in [Section 8](#8-the-binding-layer)).
- **Custom encoding** — write fields in a fixed order for maximum control over sort order.

Example using `bincode` for a structured value:

```rust
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Employee {
    id: u64,
    name: String,
    department: String,
    salary: f64,
}

// Serialize to bytes
let employee = Employee { id: 1001, name: "Alice".into(), department: "Engineering".into(), salary: 95000.0 };
let encoded = bincode::serialize(&employee)?;
let data_entry = DatabaseEntry::from_vec(encoded);

// Deserialize from bytes
let bytes = data_entry.data();
let decoded: Employee = bincode::deserialize(bytes)?;
```

## Key Design

Key design has a direct impact on performance and sort order. Because records are sorted lexicographically by key bytes:

- Numeric keys encoded as big-endian integers sort correctly as unsigned values. The Noxu bind APIs provide sort-preserving encodings for signed integers and floating-point numbers.
- String keys in UTF-8 sort in lexicographic order, which is usually correct for text data.
- Composite keys (e.g., `namespace:id`) enable prefix scans: iterate all records in a namespace by seeking to `namespace:` and reading forward.

---

