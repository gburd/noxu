# Noxu DB Examples

This directory contains both simple single-file examples and full application
examples that demonstrate Noxu DB's capabilities as an embedded transactional
database engine.

## Single-File Examples

Run any of these with `cargo run --example <name>`:

| Example | Description |
|---------|-------------|
| `quickstart` | Basic open/put/get/delete operations |
| `transactions` | ACID transactions with commit/abort |
| `cursor_scan` | Cursor-based range scans and iteration |
| `binding` | Type-safe bindings with `noxu-bind` |
| `collections` | Stored collections (`StoredMap`, `StoredList`) |
| `persist` | Struct persistence with `noxu-persist` |
| `secondary` | Secondary index lookups |
| `sequence` | Sequence (auto-increment) generation |
| `getting_started` | Introductory walkthrough |
| `scale_validation` | Large-scale insert/read validation |
| `xa_distributed` | XA distributed transactions |
| `transaction_config` | TransactionConfig options |

## Application Examples

These are standalone crates that implement real protocol-compatible servers
on top of Noxu DB. Each demonstrates a different class of application.

### cash — Memcache Protocol Server

A drop-in replacement for memcached that speaks the standard memcache text
protocol. Any existing memcache client library works unmodified. Unlike
memcached, data is persisted with full ACID guarantees and survives restarts.

```bash
cd examples/cash
cargo run --release
# Then: telnet localhost 11211
```

### cask — Redis Protocol Server

A Redis-compatible key-value store that speaks RESP2. Works with `redis-cli`
and all Redis client libraries. Provides ACID transactions (MULTI/EXEC) with
real durability guarantees, not just isolation.

```bash
cd examples/cask
cargo run --release
# Then: redis-cli -p 6379
```

### ftdb — Financial Transactions Database

A TigerBeetle-compatible double-entry bookkeeping server. Uses TigerBeetle's
exact data model (128-byte Account/Transfer structs, u128 amounts) and a binary
wire protocol with TB-compatible operation codes (128–131). Implements batched
account creation, immediate and two-phase transfers, balance constraints, and
sub-millisecond lookups — all backed by Noxu DB with full ACID guarantees.

```bash
cd examples/ftdb
# Initialize and start the server
cargo run --release -- format --file bank.db
cargo run --release -- start --file bank.db --address 127.0.0.1:3000

# In another terminal: run the built-in benchmark
cargo run --release -- benchmark --address 127.0.0.1:3000 --transfers 100000

# Or use CLI convenience commands
cargo run --release -- create-account --file bank.db --id 1 --ledger 1 --code 1
cargo run --release -- balance --file bank.db --id 1
```

## Building All Examples

```bash
# Build all application examples
cargo build --release -p noxu-cash -p noxu-cask -p noxu-ftdb

# Run single-file examples from the workspace root
cargo run --example quickstart
```
