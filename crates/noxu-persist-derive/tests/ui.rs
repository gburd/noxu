//! Compile-fail / compile-pass tests for the `noxu-persist-derive` macros.
//!
//! These are `trybuild` tests: each fixture under `tests/ui/` is compiled
//! by the test harness; `*.rs` fixtures must produce the matching
//! `*.stderr` output (or compile cleanly for the pass cases).
//!
//! Set `TRYBUILD=overwrite` to re-bless `.stderr` golden files when the
//! macro changes.

#[test]
fn ui_compile_fail_and_pass() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass_basic_entity.rs");
    t.pass("tests/ui/pass_secondary_with_options.rs");
    t.pass("tests/ui/pass_composite_primary_key.rs");
    t.pass("tests/ui/pass_crate_override_standalone.rs");
    t.pass("tests/ui/pass_crate_override_composite_key.rs");
    t.compile_fail("tests/ui/fail_missing_primary_key.rs");
    t.compile_fail("tests/ui/fail_two_primary_keys.rs");
    t.compile_fail("tests/ui/fail_invalid_relate.rs");
    t.compile_fail("tests/ui/fail_invalid_on_delete.rs");
    t.compile_fail("tests/ui/fail_secondary_without_name.rs");
    t.compile_fail("tests/ui/fail_unknown_secondary_attr.rs");
    t.compile_fail("tests/ui/fail_unknown_entity_attr.rs");
    t.compile_fail("tests/ui/fail_secondary_no_fields.rs");
}
