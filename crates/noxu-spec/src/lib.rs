// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Executable specifications for the noxu protocols.
//!
//! Each module in this crate is a [Stateright][1] model of a
//! protocol the production code implements. Models capture the
//! state transitions and the safety / liveness properties the
//! protocol must preserve; `cargo test -p noxu-spec` runs the
//! Stateright BFS checker over each model with bounded constants,
//! and a counterexample is treated as a test failure.
//!
//! [1]: https://docs.rs/stateright
//!
//! # Models
//!
//! | Module | Production code under model |
//! |---|---|
//! | [`btree_latching`] | `noxu-tree::Tree::insert` / `split_child` |
//! | [`flexible_paxos`] | `noxu-rep::elections::paxos::run_election` / `run_acceptor` |
//! | [`wal_commit`] | `noxu-log::LogManager` + `noxu-txn::Txn::commit_with_durability` |
//! | [`recovery_three_phase`] | `noxu-recovery::recovery_manager` (analysis → redo → undo) |
//! | [`lock_manager_deadlock`] | `noxu-txn::lock_manager` + `deadlock_detector` |
//! | [`vlsn_streaming`] | `noxu-rep::stream::feeder` + `replica_stream` |
//! | [`master_transfer`] | `noxu-rep::master_transfer` |
//! | [`network_restore`] | `noxu-rep::network_restore` |
//! | [`xa_two_phase_commit`] | `noxu-xa::environment` |
//! | [`cleaner_safety`] | `noxu-cleaner::file_processor` (deletion vs in-flight refs) |
//! | [`cache_vs_cleaner`] | `noxu-evictor` ↔ `noxu-cleaner` ordering |
//!
//! # How to keep specs in sync with the implementation
//!
//! Each spec module's preamble lists the Rust files it models. When
//! one of those files changes the spec must be re-validated; this
//! is enforced by:
//!
//!   - The CI workflow `.github/workflows/spec.yml` runs
//!     `cargo test -p noxu-spec --release` on every PR.
//!   - Reviewers are expected to update the relevant spec module
//!     when changing a protocol's state-transition shape.
//!
//! Beyond CI, two spec modules take a *direct* dep on the
//! production crates so a refactor of the relevant types breaks
//! the spec build:
//!
//!   - [`lock_manager_deadlock`] re-exports
//!     [`noxu_txn::LockType`] as `HeldKind`. Its
//!     `spec_lock_kind()` projection uses an exhaustive `match`
//!     over every `LockType` variant — adding a new variant
//!     forces a build break and a spec-level decision (extend
//!     the alphabet, or map onto the existing read/write set).
//!   - [`xa_two_phase_commit`] declares a compile-time anchor
//!     `_FLAG_ANCHOR` referencing every public constant on
//!     [`noxu_xa::XaFlags`]. Removing or renaming any of them on
//!     the production side breaks the spec build.
//!
//! For specs whose state/action types have no direct production
//! analogue (`master_transfer::NodeRole`,
//! `recovery_three_phase::Phase`, …) the type stays
//! spec-internal; reviewers are responsible for keeping the
//! abstract model in sync with the implementation when the
//! protocol's shape changes.
//!
//! See the `tests` module inside [`btree_latching`] for the
//! convention used to keep regression bait alive after the
//! corresponding production bug is fixed: the same `Model` is
//! parameterised on a `Variant` enum (`HandOverHand` /
//! `DropParentEarly`), and we ship two `#[test]`s — one that
//! `assert_properties` for the fixed variant and one that
//! `assert_discovery` for the buggy variant. Stateright lets us
//! consolidate what would have been two TLA+ specs into one.

#![allow(missing_docs)]
// The state/action types in each spec module are model-internal —
// their meaning is captured in the module preamble, not per-field.
// Keeping rustdoc lints on the public model `struct`s and on the
// `lib.rs` module list is enough.
#![allow(clippy::module_name_repetitions)]
// Spec modules deliberately mirror the structure of the modelled
// protocol pseudocode, which uses nested `if`s and explicit
// `iter().any()` patterns. Allow those here so spec readers can
// trace each line back to the protocol description.
#![allow(
    clippy::collapsible_if,
    clippy::collapsible_match,
    clippy::if_same_then_else,
    clippy::needless_lifetimes,
    clippy::needless_pass_by_value,
    clippy::redundant_pattern_matching,
    clippy::should_implement_trait,
    clippy::single_match
)]

pub mod btree_latching;
pub mod cache_vs_cleaner;
pub mod cleaner_safety;
pub mod flexible_paxos;
pub mod lock_manager_deadlock;
pub mod master_transfer;
pub mod network_restore;
pub mod recovery_three_phase;
pub mod vlsn_streaming;
pub mod wal_commit;
pub mod xa_two_phase_commit;
