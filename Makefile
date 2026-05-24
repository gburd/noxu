.PHONY: build test check fmt doc clean bench fuzz test-crate tc-helper torture torture-quic \
        docs docs-serve docs-check docs-spell docs-lint docs-clean \
        spec coverage

build:
	cargo build --workspace

test:
	cargo test --workspace

check: fmt-check clippy

clippy:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

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
	cargo test -p noxu-spec --release

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
