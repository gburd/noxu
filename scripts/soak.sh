#!/usr/bin/env bash
# soak.sh — 6-hour replication soak: repeats torture_all.sh across all
# transports until the deadline, continuing on failure.
#
# Usage:
#   SOAK_SECS=21600 scripts/soak.sh          # 6 hours (default)
#   SOAK_SECS=3600  scripts/soak.sh          # quick 1-hour soak
#
# Each iteration runs all 4 transports for ITER_SECS seconds each.
# ITER_SECS adapts to remaining time (capped at MAX_ITER_SECS).
#
# Output:
#   target/soak_logs/soak_<timestamp>.log    master log
#   target/soak_logs/iter_<N>_<transport>_<timestamp>.log   per-run logs
#   (torture_all.sh also writes its own logs to target/torture_logs/)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

SOAK_SECS="${SOAK_SECS:-21600}"   # total soak duration (default 6 hours)
MAX_ITER_SECS=1800                  # max per-transport time per iteration
MIN_ITER_SECS=60                    # don't start an iteration shorter than this
TRANSPORTS="${TRANSPORTS:-tcp quic quic_mux mix}"
CARGO_OPTS="${CARGO_OPTS:-}"

SOAK_LOG_DIR="$REPO_ROOT/target/soak_logs"
mkdir -p "$SOAK_LOG_DIR"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
MASTER_LOG="$SOAK_LOG_DIR/soak_${TIMESTAMP}.log"

START_TS=$(date +%s)
DEADLINE=$(( START_TS + SOAK_SECS ))

iter=0
pass=0
fail=0
total_rounds=0
total_violations=0

log() { echo "[soak $(date +%H:%M:%S)] $*" | tee -a "$MASTER_LOG"; }

log "======================================================"
log "SOAK START  duration=${SOAK_SECS}s  transports=${TRANSPORTS}"
log "master log: $MASTER_LOG"
log "======================================================"

# ── Build once up front ───────────────────────────────────────────────────────
log "Building noxu-rep (no-features + quic)..."
cargo build -p noxu-rep $CARGO_OPTS 2>&1 | tail -3 | tee -a "$MASTER_LOG"
cargo build -p noxu-rep --features quic $CARGO_OPTS 2>&1 | tail -3 | tee -a "$MASTER_LOG"
log "Build complete."

# ── Iteration loop ────────────────────────────────────────────────────────────
while true; do
    now=$(date +%s)
    remaining=$(( DEADLINE - now ))

    if [[ $remaining -le 0 ]]; then
        log "Deadline reached. Stopping."
        break
    fi

    # Compute per-transport seconds for this iteration
    n_transports=$(echo "$TRANSPORTS" | wc -w)
    per_transport=$(( remaining / n_transports ))
    per_transport=$(( per_transport > MAX_ITER_SECS ? MAX_ITER_SECS : per_transport ))

    if [[ $per_transport -lt $MIN_ITER_SECS ]]; then
        log "Only ${remaining}s left — too little for another iteration. Stopping."
        break
    fi

    iter=$(( iter + 1 ))
    iter_ts="$(date +%Y%m%d_%H%M%S)"
    log ""
    log "╔══════════════════════════════════════════════════════╗"
    log "║  ITERATION $iter  remaining=${remaining}s  per_transport=${per_transport}s"
    log "╚══════════════════════════════════════════════════════╝"

    set +e
    TORTURE_SECS="$per_transport" TRANSPORTS="$TRANSPORTS" CARGO_OPTS="$CARGO_OPTS" \
        scripts/torture_all.sh 2>&1 | tee -a "$MASTER_LOG"
    iter_exit=${PIPESTATUS[0]}
    set -e

    if [[ $iter_exit -eq 0 ]]; then
        pass=$(( pass + 1 ))
        log "Iteration $iter: PASS"
    else
        fail=$(( fail + 1 ))
        log "Iteration $iter: FAIL (exit=$iter_exit)"
    fi

    # Extract violation counts from this iteration's output
    iter_violations=$(grep -c 'violation' "$MASTER_LOG" 2>/dev/null || true)
    log "Cumulative violations seen in log so far: $iter_violations"
done

# ── Final summary ─────────────────────────────────────────────────────────────
elapsed=$(( $(date +%s) - START_TS ))
log ""
log "╔══════════════════════════════════════════════════════════════╗"
log "║                    SOAK COMPLETE                            ║"
log "╠══════════════════════════════════════════════════════════════╣"
log "║  elapsed        : ${elapsed}s"
log "║  iterations     : $iter  (pass=$pass  fail=$fail)"
log "║  master log     : $MASTER_LOG"
log "╚══════════════════════════════════════════════════════════════╝"

# Final violation audit across all logs
all_violations=$(grep -h '\[torture\].*violations=[1-9]' "$SOAK_LOG_DIR"/*.log 2>/dev/null || true)

if [[ -n "$all_violations" ]]; then
    log ""
    log "!!! VIOLATION LINES FOUND !!!"
    echo "$all_violations" | tee -a "$MASTER_LOG"
fi

if [[ $fail -gt 0 ]]; then
    log ""
    log "ONE OR MORE ITERATIONS FAILED — check $MASTER_LOG"
    exit 1
fi

log "ALL ITERATIONS PASSED"
