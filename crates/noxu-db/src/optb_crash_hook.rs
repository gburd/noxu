//! WITNESS-ONLY crash-injection hook for the v7.11.1 single-locker
//! auto-commit fix (Option B).  **Not a production feature.**
//!
//! The two entry points abort the process at a precise point of
//! [`crate::Database::with_auto_commit_transaction`] so a deterministic
//! crash-recovery witness can prove the primary + secondary + trigger writes
//! are ONE atomic unit:
//!
//! * [`maybe_crash_before_commit`] fires AFTER the primary write and all
//!   secondary maintenance are buffered but BEFORE the single `TxnCommit`
//!   fsync — after recovery NEITHER the primary NOR its secondary is present.
//! * [`maybe_crash_after_commit`] fires just AFTER the single commit — after
//!   recovery BOTH are present.
//!
//! Both are gated on an environment variable naming the primary key of the
//! op to crash on (`NOXU_OPTB_CRASH_BEFORE_COMMIT` / `_AFTER_COMMIT`), matched
//! against a thread-local set by the write entry point.  When neither env var
//! is set they are a single `thread_local!` borrow + `is_empty()` check and a
//! cheap `Option` read — negligible, and never abort.  The env vars are read
//! once per op; a real deployment never sets them.

use std::cell::RefCell;

thread_local! {
    /// The primary key of the auto-commit op currently in flight on this
    /// thread, set by `Database::put_bytes` / `delete_bytes` just before
    /// entering `with_auto_commit_transaction`.  Empty when no op is armed.
    static CURRENT_KEY: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Records the primary key of the op about to run, so the crash hooks can
/// match it against the env-var key.  Cleared by [`clear_current_key`].
pub(crate) fn set_current_key(key: &[u8]) {
    CURRENT_KEY.with(|k| {
        let mut g = k.borrow_mut();
        g.clear();
        g.extend_from_slice(key);
    });
}

/// Clears the armed key once the op completes.
pub(crate) fn clear_current_key() {
    CURRENT_KEY.with(|k| k.borrow_mut().clear());
}

fn current_key_matches(env_var: &str) -> bool {
    let Ok(want) = std::env::var(env_var) else {
        return false;
    };
    CURRENT_KEY.with(|k| k.borrow().as_slice() == want.as_bytes())
}

/// Abort BEFORE the single auto-commit — witness (a)/(b): neither durable.
#[inline]
pub(crate) fn maybe_crash_before_commit() {
    if current_key_matches("NOXU_OPTB_CRASH_BEFORE_COMMIT") {
        std::process::abort();
    }
}

/// Abort AFTER the single auto-commit — witness (a): both durable.
#[inline]
pub(crate) fn maybe_crash_after_commit() {
    if current_key_matches("NOXU_OPTB_CRASH_AFTER_COMMIT") {
        std::process::abort();
    }
}
