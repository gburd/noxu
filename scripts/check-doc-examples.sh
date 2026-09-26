#!/usr/bin/env bash
# Compile the standalone Rust examples embedded in the mdBook docs.
#
# WHY: `make docs-check` runs typos + markdownlint + `mdbook build`, none of
# which compile the ```rust fences. Audit findings V20/F5/F6/F7/F8/F10/F29/F31
# were all doc examples that failed to compile because nothing ever built them.
# This script closes that gap for the *standalone* examples (fences that
# contain a `fn main`); fragment fences that assume an ambient `env`/`db`/
# `txn`/`rep_env` are skipped (they cannot be compiled in isolation).
#
# USAGE:  scripts/check-doc-examples.sh
# ENV:    DOCS_DIR (default docs/src), CRATE (default crates/noxu),
#         FEATURES (default "replication")
#
# It is intentionally conservative: it only compiles blocks it can compile
# without inventing surrounding context, so it never produces a false red on a
# deliberately-abbreviated snippet. Extend the standalone set over time by
# giving more doc examples a full `fn main`.
set -euo pipefail

DOCS_DIR="${DOCS_DIR:-docs/src}"
CRATE="${CRATE:-crates/noxu}"
FEATURES="${FEATURES:-replication}"

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

# CRATE may be absolute or relative to the repo root.
case "$CRATE" in
  /*) crate_abs="$CRATE" ;;
  *)  crate_abs="$repo_root/$CRATE" ;;
esac
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

cat > "$work/Cargo.toml" <<EOF
[package]
name = "doc-examples"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
noxu = { path = "$crate_abs", features = ["$FEATURES"] }
bytes = "1"

[workspace]
EOF
mkdir -p "$work/src/bin"
# Inherit the pinned toolchain so `cargo` in a bare temp dir resolves it.
[ -f "$repo_root/rust-toolchain.toml" ] && cp "$repo_root/rust-toolchain.toml" "$work/"

# Extract every ```rust fence that contains `fn main` into its own bin.
# Skip fences tagged `ignore`, `no_run,ignore`, `text`, `toml`, etc.
python3 - "$DOCS_DIR" "$work/src/bin" <<'PY'
import os, re, sys
docs_dir, out_dir = sys.argv[1], sys.argv[2]
n = 0
fence = re.compile(r'^```(\w[\w,-]*)?\s*$')
for root, _, files in os.walk(docs_dir):
    for f in sorted(files):
        if not f.endswith('.md'):
            continue
        path = os.path.join(root, f)
        lines = open(path, encoding='utf-8').read().splitlines()
        i = 0
        while i < len(lines):
            m = fence.match(lines[i])
            if m and (m.group(1) or '').split(',')[0] == 'rust' \
                    and 'ignore' not in (m.group(1) or ''):
                block, i = [], i + 1
                while i < len(lines) and not lines[i].startswith('```'):
                    block.append(lines[i]); i += 1
                body = '\n'.join(block)
                if 'fn main' in body:  # standalone only
                    n += 1
                    name = f'{os.path.splitext(f)[0]}_{n}'.replace('-', '_')
                    open(os.path.join(out_dir, name + '.rs'), 'w',
                         encoding='utf-8').write(body + '\n')
            i += 1
print(f'extracted {n} standalone doc example(s)')
PY

echo "Compiling extracted doc examples against $CRATE (features: $FEATURES)..."
( cd "$work" && cargo build --bins )
echo "All standalone doc examples compile."
