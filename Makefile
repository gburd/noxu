.PHONY: build test check fmt doc clean bench fuzz test-crate tc-helper torture torture-quic \
        docs docs-serve docs-check docs-spell docs-lint docs-clean \
        spec shuttle spec-and-shuttle coverage

build:
	cargo build --workspace

test:
	cargo test --workspace

check: fmt-check clippy

clippy:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

deny:
	cargo deny check

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

docs:
	mdbook build docs/

docs-serve:
	mdbook serve docs/ --open

docs-check: docs-spell docs-lint docs
	@echo "All docs quality gates passed."

docs-spell:
	typos docs/src/

docs-lint:
	markdownlint-cli2 "docs/src/**/*.md"

docs-clean:
	rm -rf docs/book/

clean:
	cargo clean
	$(MAKE) docs-clean

bench:
	cargo bench --workspace

fuzz:
	@echo "Run: cargo +nightly fuzz run <target>"

# Run every Stateright executable specification under
# crates/noxu-spec/. See crates/noxu-spec/src/lib.rs for the list of
# protocols modelled. Each model is a `cargo test` case; failures
# print a Stateright counterexample trace.
spec:
	cargo test -p noxu-spec --release -- --include-ignored

# Run every shuttle concurrency-permutation (DST Milestone 2) test file.
# Gated behind `#[cfg(noxu_shuttle)]`, so these compile to empty test
# binaries without the RUSTFLAGS cfg — see
# docs/src/contributing/testing-guide.md "DST Milestone 2" for what each
# target covers. --release matters here: shuttle re-runs the closure
# thousands of times per test, and debug builds are ~20-30x slower per
# iteration (measured: shuttle_bin_split 189s debug vs 6s release).
shuttle:
	RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-sync    --test shuttle_rwlock_reservation --release
	RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-util    --test shuttle_dst_sync_pl --release
	RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-engine  --test shuttle_daemon_shutdown --release
	RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-log     --test shuttle_fsync_manager --release
	RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-txn     --test shuttle_lock_manager --release
	RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-txn     --test shuttle_txn_commit --release
	RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-evictor --test shuttle_shared_cache --release
	RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-tree    --test shuttle_bin_split --release
	RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-tree    --test shuttle_checkpoint_mutation --release
	RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-dbi     --test shuttle_cursor --release
	RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-rep     --test shuttle_rep_sync --release

# Run the Stateright specs and the shuttle DST gate back-to-back — the two
# halves of "protocol design proven abstractly" + "real code proven under
# interleaving". Used by the nightly/dispatch CI job.
spec-and-shuttle: spec shuttle

# Run the test suite under cargo-llvm-cov and emit both an HTML report
# and a textual summary. Requires `cargo install cargo-llvm-cov`.
coverage:
	cargo llvm-cov --workspace --no-fail-fast --html
	cargo llvm-cov --workspace --no-fail-fast --summary-only

test-crate:
ifndef CRATE
	$(error CRATE is not set. Usage: make test-crate CRATE=noxu-util)
endif
	cargo test -p $(CRATE)

# Build the setuid tc helper for kernel-level netem fault injection.
# After running this, do: sudo chown root:root scripts/tc_netem_helper && sudo chmod u+s scripts/tc_netem_helper
tc-helper:
	gcc -O2 -Wall -o scripts/tc_netem_helper scripts/tc_netem_helper.c
	@echo "Built scripts/tc_netem_helper"
	@echo "To enable kernel fault injection, run:"
	@echo "  sudo chown root:root scripts/tc_netem_helper"
	@echo "  sudo chmod u+s       scripts/tc_netem_helper"

# Run the torture test over all transports (TCP only if no quic feature).
# Override duration: TORTURE_SECS=600 make torture
torture:
	TORTURE_SECS=$${TORTURE_SECS:-120} scripts/torture_all.sh

# Run torture with QUIC transports enabled.
torture-quic:
	TORTURE_SECS=$${TORTURE_SECS:-120} TRANSPORTS="tcp quic quic_mux mix" scripts/torture_all.sh
