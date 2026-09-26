//! JE `com.sleepycat.je.dbi.MemoryBudgetTest.testDefaults` port.
//!
//! JE: on a freshly-opened env, `MemoryBudget.getMaxMemory() > 0`,
//! `getLogBufferBudget() > 0`, and `getMaxMemory() <= Runtime.maxMemory()`.
//! Noxu has no JVM heap, so the `<= Runtime.maxMemory()` clause is N/A
//! (see tp-je-dbi.md); the portable invariants are the two positivity checks
//! against the live `MemoryBudget` on `EnvironmentImpl`.
//!
//! `MemoryBudgetTest.testCacheSizing`'s JVM-heap-percentage sizing is N/A
//! (no JVM heap); its explicit-cache-size-override half is ported in
//! `crates/noxu-db/tests/je_dbi_misc_test.rs`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_dbi::EnvironmentImpl;
use tempfile::TempDir;

#[test]
fn memory_budget_test_defaults() {
    let dir = TempDir::new().unwrap();
    let env = EnvironmentImpl::new(
        dir.path(),
        /*read_only=*/ false,
        /*transactional=*/ true,
    )
    .expect("open environment");

    let mb = env.get_memory_budget();

    // JE: getMaxMemory() > 0.
    assert!(
        mb.max_memory() > 0,
        "max memory budget must be positive (JE MemoryBudget.getMaxMemory)"
    );
    // JE: getLogBufferBudget() > 0.
    assert!(
        mb.log_buffer_budget() > 0,
        "log buffer budget must be positive (JE getLogBufferBudget)"
    );
    // JE also asserts getMaxMemory() <= Runtime.maxMemory(): N/A (no JVM heap).
    // Sanity: the log buffer budget is a portion of the cache, so it must not
    // exceed the max memory budget.
    assert!(
        mb.log_buffer_budget() <= mb.max_memory(),
        "log buffer budget must not exceed the total memory budget"
    );
}
