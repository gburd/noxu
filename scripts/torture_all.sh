#!/usr/bin/env bash
# torture_all.sh — Run noxu-rep torture_replication for every transport
# permutation, capture results, and print a summary table.
#
# Usage:
#   scripts/torture_all.sh [TORTURE_SECS=N] [TRANSPORTS="tcp quic quic_mux mix"]
#
# Environment variables:
#   TORTURE_SECS    Per-run duration in seconds  (default: 120)
#   TRANSPORTS      Space-separated list of transport names to test
#                   (default: "tcp quic quic_mux mix")
#   CARGO_OPTS      Extra flags passed to cargo test (e.g. --release)
#
# tc netem (kernel fault injection):
#   Build and install the setuid helper first:
#     gcc -O2 -Wall -o scripts/tc_netem_helper scripts/tc_netem_helper.c
#     sudo chown root:root scripts/tc_netem_helper
#     sudo chmod u+s       scripts/tc_netem_helper
#   The torture test will auto-detect and use it.
#
# Example — quick 2-minute run over all transports:
#   TORTURE_SECS=120 scripts/torture_all.sh
#
# Example — 10-minute soak, TCP only:
#   TORTURE_SECS=600 TRANSPORTS=tcp scripts/torture_all.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# ── Configuration ────────────────────────────────────────────────────────────
TORTURE_SECS="${TORTURE_SECS:-120}"
TRANSPORTS="${TRANSPORTS:-tcp quic quic_mux mix}"
CARGO_OPTS="${CARGO_OPTS:-}"
LOG_DIR="$REPO_ROOT/target/torture_logs"
HELPER="$SCRIPT_DIR/tc_netem_helper"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"

mkdir -p "$LOG_DIR"

# ── Helper detection ─────────────────────────────────────────────────────────
if [[ -u "$HELPER" ]]; then
    echo "[torture_all] tc_netem_helper found (setuid) — kernel fault injection enabled"
    TC_ACTIVE=1
else
    echo "[torture_all] tc_netem_helper NOT setuid — software-only fault injection"
    echo "             To enable: gcc -O2 -Wall -o scripts/tc_netem_helper scripts/tc_netem_helper.c"
    echo "                        sudo chown root:root scripts/tc_netem_helper"
    echo "                        sudo chmod u+s       scripts/tc_netem_helper"
    TC_ACTIVE=0
fi

# ── System diagnostics ───────────────────────────────────────────────────────
DIAG_FILE="$LOG_DIR/diag_${TIMESTAMP}.txt"
{
    echo "=== System Diagnostics ==="
    echo "Date: $(date)"
    echo "Host: $(uname -n)"
    echo "Kernel: $(uname -r)"
    echo "Rust: $(rustc --version 2>/dev/null || echo 'not found')"
    echo "Cargo: $(cargo --version 2>/dev/null || echo 'not found')"
    echo ""
    echo "=== Network: loopback ==="
    ip link show lo 2>/dev/null || ifconfig lo 2>/dev/null || echo "(ip/ifconfig not available)"
    echo ""
    echo "=== Current tc qdisc on lo ==="
    tc qdisc show dev lo 2>/dev/null || echo "(tc not available or no CAP_NET_ADMIN)"
    echo ""
    echo "=== CPU ==="
    nproc 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo "unknown"
    echo ""
    echo "=== Memory ==="
    free -h 2>/dev/null || vm_stat 2>/dev/null || echo "unknown"
} > "$DIAG_FILE" 2>&1
echo "[torture_all] diagnostics written to: $DIAG_FILE"

# ── Build once ───────────────────────────────────────────────────────────────
echo ""
echo "[torture_all] Building noxu-rep (no-features + quic feature)..."
cargo build -p noxu-rep $CARGO_OPTS 2>&1 | tail -5
cargo build -p noxu-rep --features quic $CARGO_OPTS 2>&1 | tail -5
echo "[torture_all] Build complete."

# ── Run loop ─────────────────────────────────────────────────────────────────
declare -A RESULTS   # transport → "pass|fail|skip"
declare -A SUMMARIES # transport → last FINAL block

run_transport() {
    local transport="$1"
    local features_flag=""
    case "$transport" in
        quic|quic_mux|mix) features_flag="--features quic" ;;
    esac

    local log_file="$LOG_DIR/${transport}_${TIMESTAMP}.log"
    echo ""
    echo "╔══════════════════════════════════════════════════════════════╗"
    printf  "║  TRANSPORT=%-20s  TORTURE_SECS=%-6s         ║\n" "$transport" "$TORTURE_SECS"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo "[torture_all] log: $log_file"

    local start_ts
    start_ts=$(date +%s)

    set +e
    TRANSPORT="$transport" TORTURE_SECS="$TORTURE_SECS" \
        cargo test -p noxu-rep $features_flag $CARGO_OPTS \
            --test torture_test -- --ignored --nocapture \
            2>&1 | tee "$log_file"
    local exit_code=${PIPESTATUS[0]}
    set -e

    local end_ts
    end_ts=$(date +%s)
    local elapsed=$(( end_ts - start_ts ))

    # Extract the FINAL block from the log
    local summary
    summary=$(grep -A 20 '\[torture\] ═══' "$log_file" | head -25 || true)
    SUMMARIES["$transport"]="$summary"

    if [[ $exit_code -eq 0 ]]; then
        RESULTS["$transport"]="PASS  (${elapsed}s)"
        echo "[torture_all] $transport: PASS in ${elapsed}s"
    else
        RESULTS["$transport"]="FAIL  (${elapsed}s, exit=$exit_code)"
        echo "[torture_all] $transport: FAIL (exit=$exit_code) in ${elapsed}s"
    fi
}

for transport in $TRANSPORTS; do
    run_transport "$transport"
done

# ── Summary table ─────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                     TORTURE TEST SUMMARY                        ║"
echo "╠══════════════════════════════════════════════════════════════════╣"
printf "║  %-12s  %-8s  %-6s  %-26s ║\n" "TRANSPORT" "RESULT" "tc" "NOTES"
echo "╠══════════════════════════════════════════════════════════════════╣"
for transport in $TRANSPORTS; do
    result="${RESULTS[$transport]:-NOT_RUN}"
    tc_label="sw-only"
    if [[ $TC_ACTIVE -eq 1 ]]; then tc_label="kernel"; fi
    printf "║  %-12s  %-8s  %-6s                               ║\n" \
        "$transport" "${result:0:8}" "$tc_label"
done
echo "╠══════════════════════════════════════════════════════════════════╣"
printf "║  duration/run: %-4s s    logs: %-30s ║\n" "$TORTURE_SECS" "${LOG_DIR##*/}"
echo "╚══════════════════════════════════════════════════════════════════╝"

echo ""
echo "=== Per-transport final reports ==="
for transport in $TRANSPORTS; do
    echo ""
    echo "--- $transport ---"
    echo "${SUMMARIES[$transport]:-  (no FINAL block found)}"
done

# ── Violations check ─────────────────────────────────────────────────────────
any_fail=0
for transport in $TRANSPORTS; do
    result="${RESULTS[$transport]:-NOT_RUN}"
    if [[ "$result" != PASS* ]]; then
        any_fail=1
    fi
done

echo ""
if [[ $any_fail -eq 0 ]]; then
    echo "[torture_all] ALL TRANSPORTS PASSED"
else
    echo "[torture_all] ONE OR MORE TRANSPORTS FAILED — review logs in $LOG_DIR"
    exit 1
fi
