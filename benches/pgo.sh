#!/usr/bin/env bash
# benches/pgo.sh — Three-phase PGO build for noxu-workload-bench.
#
# Usage:
#   bash benches/pgo.sh [--train-scale N]
#
# Produces:
#   target/release/noxu-workload-bench   (PGO-optimised binary, profile pgo)
#   /tmp/noxu-pgo/merged.profdata        (merged LLVM profile data)
#
# Prerequisites:
#   llvm-profdata must be on PATH (provided by the llvm package in nixpkgs or
#   the matching LLVM version for the active rustc toolchain).
#
# Training scale:
#   --train-scale N (default: 10000) controls NOXU_MAX_SCALE during the
#   training run.  10K is sufficient to cover all hot paths without taking
#   more than ~30 seconds; use 100K for higher coverage at the cost of ~5 min.
#
# Integration with run_comparison.sh:
#   Run this script once, then pass --pgo to run_comparison.sh.  The benchmark
#   script checks for /tmp/noxu-pgo/merged.profdata and, if present, rebuilds
#   the bench binary with -Cprofile-use before running the comparison.

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

TRAIN_SCALE=10000
for arg in "$@"; do
    case "$arg" in
        --train-scale=*) TRAIN_SCALE="${arg#--train-scale=}" ;;
        --train-scale)   ;;  # consumed via positional
    esac
done
# space-separated handling
prev=""
for arg in "$@"; do
    if [[ "$prev" == "--train-scale" ]]; then
        TRAIN_SCALE="$arg"
    fi
    prev="$arg"
done

PGO_DIR="/tmp/noxu-pgo"
PROFDATA="$PGO_DIR/merged.profdata"
mkdir -p "$PGO_DIR"

# Use the llvm-profdata that ships with the active rustc toolchain.  The
# system llvm-profdata may be a different LLVM version and will reject the
# .profraw files with a "version mismatch" error.
RUSTC_SYSROOT="$(rustc --print sysroot)"
RUSTLIB_BIN="$RUSTC_SYSROOT/lib/rustlib/x86_64-unknown-linux-gnu/bin"
if [[ -x "$RUSTLIB_BIN/llvm-profdata" ]]; then
    LLVM_PROFDATA="$RUSTLIB_BIN/llvm-profdata"
else
    LLVM_PROFDATA="$(command -v llvm-profdata)"
    echo "  WARNING: using system llvm-profdata ($LLVM_PROFDATA) — may have version mismatch" >&2
fi
echo "  llvm-profdata: $LLVM_PROFDATA"

echo "══════════════════════════════════════════════════════════════"
echo "  PGO Phase 1: Instrumented build  (profile = pgo)"
echo "══════════════════════════════════════════════════════════════"

# Remove stale profraw files from a previous run.
rm -f "$PGO_DIR"/*.profraw

RUSTFLAGS="-Cprofile-generate=$PGO_DIR" \
    cargo build --profile pgo --package noxu-workload-bench 2>&1 \
    | grep -E "^(Compiling|Finished|error)" || true

INSTRUMENTED_BIN="./target/pgo/noxu-workload-bench"
if [[ ! -f "$INSTRUMENTED_BIN" ]]; then
    echo "ERROR: instrumented binary not found at $INSTRUMENTED_BIN" >&2
    exit 1
fi
echo "  Instrumented binary: $INSTRUMENTED_BIN"

echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  PGO Phase 2: Training run  (NOXU_MAX_SCALE=$TRAIN_SCALE)"
echo "══════════════════════════════════════════════════════════════"

# Run the full workload suite at reduced scale so every hot path is exercised.
# The binary writes .profraw files into PGO_DIR on exit.
NOXU_MAX_SCALE="$TRAIN_SCALE" \
NOXU_PGO_TRAINING=1 \
    "$INSTRUMENTED_BIN" 2>&1 | grep -v "^$" || true

PROFRAW_COUNT=$(ls "$PGO_DIR"/*.profraw 2>/dev/null | wc -l)
if [[ "$PROFRAW_COUNT" -eq 0 ]]; then
    echo "ERROR: no .profraw files generated in $PGO_DIR" >&2
    echo "       Make sure the instrumented binary ran successfully." >&2
    exit 1
fi
echo "  $PROFRAW_COUNT .profraw file(s) generated in $PGO_DIR"

echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  PGO Phase 3: Merge profiles"
echo "══════════════════════════════════════════════════════════════"

"$LLVM_PROFDATA" merge \
    --output="$PROFDATA" \
    "$PGO_DIR"/*.profraw

echo "  Merged profdata: $PROFDATA ($(du -sh "$PROFDATA" | cut -f1))"

echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  PGO Phase 4: Optimised build  (profile = pgo, -Cprofile-use)"
echo "══════════════════════════════════════════════════════════════"

RUSTFLAGS="-Cprofile-use=$PROFDATA -Cllvm-args=-pgo-warn-missing-function" \
    cargo build --profile pgo --package noxu-workload-bench 2>&1 \
    | grep -E "^(Compiling|Finished|error)" || true

PGO_BIN="./target/pgo/noxu-workload-bench"
echo "  PGO-optimised binary: $PGO_BIN"
echo ""
echo "  To run the comparison with PGO:"
echo "    bash benches/run_comparison.sh --pgo [--max-scale N]"
echo ""
echo "  PGO build complete."
