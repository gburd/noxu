# Record Expiration (TTL)

Noxu DB supports per-record **time-to-live (TTL)**: a record can be given an
expiration time, after which it becomes invisible to reads and its space is
reclaimed by the log cleaner. This is a faithful port of the JE 7.5 TTL
feature.

## Setting a TTL on a write

TTL is supplied through `WriteOptions` on a `put_with_options` call:

```rust
use noxu::{WriteOptions, TtlUnit};

// Expire this record 30 days from now (day granularity — recommended).
let opts = WriteOptions::new().with_ttl_unit(30, TtlUnit::Days);
db.put_with_options(None, b"session:1001", b"payload", &opts)?;

// Or in hours:
let opts = WriteOptions::with_expiration(48); // 48 hours
db.put_with_options(None, b"otp:abc", b"123456", &opts)?;
```

A TTL of `0` (the default) means the record never expires.

## Granularity: hours vs days

Records expire on **hour** or **day** boundaries, chosen by the `TtlUnit`
passed to `with_ttl_unit`. At write time the current system time is rounded up
to the next hour (or day) boundary and the TTL is added, so the stored
expiration time is always aligned to that boundary. This matches JE's
`WriteOptions.setTTL(int, TimeUnit)`.

`TtlUnit::Days` is recommended when day-level precision is enough: it minimizes
the per-record expiration storage in the B-tree. Both units are stored
internally as hours-since-epoch (a day-granular expiration lands on a 24-hour
boundary), so switching units does not change the on-disk representation shape.

## How expiration behaves

- **Reads.** A `get` or cursor step that lands on an expired record returns
  as if the record were not found. Expiration is checked against the system
  clock at read time, per record.
- **Cleaning.** Expired records count as obsolete when the cleaner computes
  per-file utilization, so files that hold mostly expired data are selected
  for cleaning and their space reclaimed. This happens in the background;
  there is no guarantee that a given record's space is reclaimed at any
  particular instant.
- **Recovery.** The expiration time is written into the write-ahead log with
  the record, so a record keeps its expiration across a crash and restart.
- **Updates.** Updating a record leaves its existing expiration unchanged
  unless you opt in with `WriteOptions::with_update_ttl(true)`, in which case
  the new TTL is applied (or the expiration is cleared if the new TTL is `0`).
  A fresh insert always takes the specified TTL.

## Environment configuration

Expiration is controlled by three environment parameters, all defaulting to
JE's values so the feature is on out of the box:

| Parameter | Default | Effect |
|---|---|---|
| `env_expiration_enabled` | `true` | Master switch. `false` disables all expiration filtering and purging (a kill switch for debugging or migration). |
| `env_ttl_clock_tolerance_ms` | `7_200_000` (2 h) | Grace window the cleaner applies before reclaiming an expired record's space, so a small backward clock adjustment cannot purge a still-live record. Does not affect read visibility. |
| `cleaner_expiration_enabled` | `true` | Whether the cleaner counts expired records as obsolete when selecting files to clean. |

```rust
use noxu::EnvironmentConfig;

let cfg = EnvironmentConfig::new(path)
    .with_allow_create(true)
    // Widen the purge grace window (rarely needed):
    .with_env_ttl_clock_tolerance_ms(4 * 60 * 60 * 1000); // 4 hours
```

## Clock synchronization

Expiration is evaluated against each node's local system clock. In a
replicated deployment, filtering and purging happen independently on each
node, so for consistent query results across a group the node clocks should be
synchronized.

## Notes and caveats

- Records with the same expiration time do **not** expire atomically; each is
  evaluated individually at read time, so a query may return some but not all
  records that expire at nearly the same instant.
- Locking a record protects it from expiring from the viewpoint of the
  locking transaction or cursor (repeatable-read), with the same caveats JE
  documents for records accessed via secondary databases.
