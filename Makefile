.PHONY: build test check fmt doc clean bench fuzz test-crate

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

clean:
	cargo clean

bench:
	cargo bench --workspace

fuzz:
	@echo "Run: cargo +nightly fuzz run <target>"

test-crate:
ifndef CRATE
	$(error CRATE is not set. Usage: make test-crate CRATE=noxu-util)
endif
	cargo test -p $(CRATE)
