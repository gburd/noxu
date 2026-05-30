# Docs: Recommend `noxu` Umbrella Crate (v3.0.2)

**Status**: placeholder — work in progress on branch `fix/docs-recommend-noxu`

This document tracks the v3.0.2 docs-correction release that updates all
user-facing documentation, the README, and examples to recommend the `noxu`
umbrella crate (`noxu = "3"`) instead of the internal `noxu-db` component.

## Motivation

Since v3.0.1 the `noxu` umbrella crate is published on crates.io and provides:
- `use noxu::{Environment, …}` — all types formerly at `noxu_db::…`
- `noxu::collections::…`, `noxu::persist::…`, `noxu::xa::…`, `noxu::bind::…`
- opt-in `noxu::replication::…` (feature `replication`)

Applications should add `noxu = "3"` to their `Cargo.toml`, not `noxu-db`.

## Files to change

*This placeholder will be replaced with a full list once the work is complete.*

## Status: placeholder
