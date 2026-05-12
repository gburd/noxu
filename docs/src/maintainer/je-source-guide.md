# JE Source Navigation

The Java reference source is kept in the development tree at:

```
/home/gburd/ws/lamdb/_/je/               BDB JE 7.5.11 standalone
/home/gburd/ws/lamdb/_/nosql/            Oracle NoSQL enhanced JE fork
```

These directories are read-only references. Do NOT modify them.

## Directory Structure of `_/je/`

```
_/je/src/com/sleepycat/je/
    ├── tree/           B+tree: IN.java, BIN.java, LN.java, Tree.java
    ├── log/            WAL: FileManager.java, LogManager.java, LogBuffer.java
    ├── txn/            Transaction.java, LockManager.java, Locker.java
    ├── dbi/            EnvironmentImpl.java, DatabaseImpl.java, CursorImpl.java
    ├── evictor/        Evictor.java
    ├── cleaner/        Cleaner.java, UtilizationProfile.java, FileSelector.java
    ├── recovery/       RecoveryManager.java, Checkpointer.java
    ├── rep/            ReplicatedEnvironment.java, MasterFeeder.java, Replica.java
    ├── config/         EnvironmentConfig.java, DatabaseConfig.java
    └── ...
```

## NoSQL Extensions in `_/nosql/`

```
_/nosql/kvmain/src/main/java/com/sleepycat/
    ├── je/             Same package as JE, with 10 extensions applied as patches
    └── ...
```

## Java → Rust Naming Rules

| Java convention | Rust convention | Example |
|---|---|---|
| `ClassName` | `TypeName` (same) | `RecoveryManager` → `RecoveryManager` |
| `methodName()` | `method_name()` | `getLastKnownMasterAddress()` → `get_last_known_master_address()` |
| `CONSTANT_NAME` | `CONSTANT_NAME` | `NULL_TXN_ID` → `NULL_TXN_ID` |
| `boolean isX()` | `fn is_x() -> bool` | `isMaster()` → `is_master()` |
| `void setX(v)` | `fn set_x(&mut self, v)` | `setMasterAddress(a)` → `set_master_address(a)` |
| `Collection<T>` | `Vec<T>` or `HashMap<K,V>` | as appropriate |
| `synchronized` | `parking_lot::Mutex/RwLock` | latch wrapper |
| `volatile T field` | `AtomicT field` | `AtomicU64`, `AtomicBool` |

## Finding the JE Source for a Noxu Type

1. Look at the type name in Noxu (e.g., `UtilizationProfile`)
2. The JE source is at `_/je/src/com/sleepycat/je/cleaner/UtilizationProfile.java`
3. For NoSQL extensions: `_/nosql/kvmain/src/main/java/com/sleepycat/je/cleaner/UtilizationProfile.java`

Sub-packages in JE map to sub-modules in Noxu:
- `je.tree` → `noxu-tree/src/`
- `je.log` → `noxu-log/src/`
- `je.txn` → `noxu-txn/src/`
- `je.dbi` → `noxu-dbi/src/`
- `je.evictor` → `noxu-evictor/src/`
- `je.cleaner` → `noxu-cleaner/src/`
- `je.recovery` → `noxu-recovery/src/`
- `je.rep` → `noxu-rep/src/`

## What to Preserve from JE

1. **Algorithm structure** — control flow, invariants, edge case handling
2. **Naming** — type names, method names, parameter names, constant names
3. **Doc comments** — JE's Javadoc explains the "why"; port it as Rust `///`
4. **Assertions** — JE uses `assert` heavily; port as `debug_assert!`
5. **Error conditions** — JE throws specific exceptions; map to `NoxuError` variants

## What to Adapt for Rust

1. **Memory management** — JE relies on GC; Noxu uses RAII + explicit `MemoryBudget`
2. **Class hierarchies** — use enums for closed sets, traits for open extension points
3. **Exception hierarchy** — collapse to `NoxuError` enum with `thiserror`
4. **Thread model** — JE uses `synchronized`; Noxu uses `parking_lot` + RAII guards
5. **Collections** — Java generics map to Rust generics with explicit bounds
