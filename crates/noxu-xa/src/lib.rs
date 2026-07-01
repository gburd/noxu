// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! > **Internal component of the [`noxu`](https://crates.io/crates/noxu) database.**
//! >
//! > This crate is published only so the `noxu` umbrella crate can depend on it.
//! > Use `noxu` (`noxu = "7"`) in applications; depend on this crate directly only
//! > if you are extending the engine internals. Its API may change without a major
//! > version bump.
//!
//! XA distributed transaction support for Noxu DB.
//!
//! This crate implements the X/Open XA interface for coordinating distributed
//! transactions across multiple Noxu environments. It provides:
//!
//! - `Xid` — XA transaction identifier (format_id + global_transaction_id + branch_qualifier)
//! - `XaFlags` — flags for XA operations (JOIN, RESUME, TMSUCCESS, ONEPHASE, etc.)
//! - `XaResource` — trait defining the XA resource manager interface
//! - `XaEnvironment` — implementation of `XaResource` backed by a Noxu Environment
//!
//! # Example
//!
//! ```ignore
//! use noxu_xa::{XaEnvironment, XaResource, Xid, XaFlags, PrepareResult};
//!
//! let xa = XaEnvironment::new(env);
//! let xid = Xid::new(1, b"global_txn_1", b"branch_1").unwrap();
//!
//! xa.xa_start(&xid, XaFlags::NOFLAGS)?;
//! // ... perform database operations using xa.get_transaction(&xid) ...
//! xa.xa_end(&xid, XaFlags::TMSUCCESS)?;
//!
//! match xa.xa_prepare(&xid, XaFlags::NOFLAGS)? {
//!     PrepareResult::Ok => xa.xa_commit(&xid, XaFlags::NOFLAGS)?,
//!     PrepareResult::ReadOnly => {} // no commit needed
//! }
//! ```

// noxu-xa contains no `unsafe` (the former transaction-pointer dereference in
// `environment.rs` was replaced by an `Arc<Transaction>` handle in the v3.x
// soundness pass). Forbid it to keep the crate safe.
#![forbid(unsafe_code)]

pub mod environment;
pub mod error;
pub mod flags;
pub mod prepared_log;
pub mod resource;
pub mod xid;

pub use environment::XaEnvironment;
pub use error::{PrepareResult, XaError, XaResult};
pub use flags::XaFlags;
pub use prepared_log::PreparedLog;
pub use resource::XaResource;
pub use xid::{Xid, XidError};
