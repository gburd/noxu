# Wave 11-M — crates.io Publish Preparation

**Status**: in progress

This wave restructures the workspace dependency graph for crates.io publishing:
adds `version = "..."` to all `[workspace.dependencies]` entries for public
`noxu-*` crates, removes `publish = false` from the 19 intended-public crates,
fills in required publish metadata, and verifies `cargo publish --dry-run`
succeeds for all public crates in dependency order.

See the task description for full acceptance gates.

## Dry-run results table

*To be filled in after dry-run execution.*

| Crate | Dry-run result | Notes |
|---|---|---|
| noxu-util | pending | |
| noxu-sync | pending | |
| noxu-latch | pending | |
| noxu-config | pending | |
| noxu-log | pending | |
| noxu-tree | pending | |
| noxu-txn | pending | |
| noxu-evictor | pending | |
| noxu-cleaner | pending | |
| noxu-recovery | pending | |
| noxu-dbi | pending | |
| noxu-engine | pending | |
| noxu-db | pending | |
| noxu-bind | pending | |
| noxu-collections | pending | |
| noxu-persist-derive | pending | |
| noxu-persist | pending | |
| noxu-xa | pending | |
| noxu-rep | pending | |
