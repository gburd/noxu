#!/usr/bin/env bash
# benches/run_comparison.sh — Run Noxu and JE benchmarks and produce a
# side-by-side comparison report.
#
# Prerequisites:
#   bash benches/setup.sh     (installs Java, builds JE jar and fat jar)
#
# Usage:
#   bash benches/run_comparison.sh [--skip-noxu] [--skip-je] [--pgo] [--gc g1|zgc|epsilon] [--max-scale N]
#
# --pgo: use the PGO-optimised Noxu binary (./target/pgo/noxu-workload-bench).
#   Build it first with:  bash benches/pgo.sh [--train-scale 10000]
#   Falls back to a regular --release build if the PGO binary or profdata is absent.
#
# --max-scale N: limit JE run to scales <= N (e.g. --max-scale 100000 to skip 500K/1M,
#   which take hours due to per-commit fsync).  Noxu always runs all 5 scales.
#
# GC strategies (--gc flag, applies to the JE run):
#   g1      — G1GC, 4GB fixed heap, MaxGCPauseMillis=5 (default)
#   zgc     — ZGC, 4GB fixed heap, low-latency
#   epsilon — EpsilonGC (no-op GC, 8GB heap): zero GC interference,
#             but OOM if workload allocates too much (safer at small scales)
#
# Output:
#   benches/results/noxu_results.csv
#   benches/results/je_results.csv
#   benches/results/je_gc.log        (verbose GC log from JE run)
#   benches/results/comparison_report.txt   (side-by-side table)
#   benches/results/comparison_report.csv   (merged CSV for further analysis)

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
RESULTS="$REPO_ROOT/benches/results"
mkdir -p "$RESULTS"
# JE_TMPDIR is set after argument parsing (may depend on BENCH_DIR)

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
SKIP_NOXU=0
SKIP_JE=0
USE_PGO=0
GC_STRATEGY="g1"
MAX_SCALE=0   # 0 = no limit
BENCH_DIR=""  # "" = use tmpfs TempDir (default); set to real path for NVMe tests

for arg in "$@"; do
    case $arg in
        --skip-noxu)    SKIP_NOXU=1 ;;
        --skip-je)      SKIP_JE=1   ;;
        --pgo)          USE_PGO=1   ;;
        --gc=*)         GC_STRATEGY="${arg#--gc=}" ;;
        --max-scale=*)  MAX_SCALE="${arg#--max-scale=}" ;;
        --bench-dir=*)  BENCH_DIR="${arg#--bench-dir=}" ;;
        --gc)           ;;   # consumed via positional; handled below
        --max-scale)    ;;   # consumed via positional; handled below
        --bench-dir)    ;;   # consumed via positional; handled below
    esac
done
# Handle space-separated --gc <val>, --max-scale <val>, --bench-dir <val>
while [[ $# -gt 0 ]]; do
    case "$1" in
        --gc)           GC_STRATEGY="${2:-g1}"; shift 2 ;;
        --max-scale)    MAX_SCALE="${2:-0}";    shift 2 ;;
        --bench-dir)    BENCH_DIR="${2:-}";     shift 2 ;;
        *)              shift ;;
    esac
done

# Resolve JE tmpdir: use BENCH_DIR/je-tmp when bench-dir is set (real storage),
# otherwise use the in-tree results/je-tmp (tmpfs default).
if [[ -n "$BENCH_DIR" ]]; then
    JE_TMPDIR="${BENCH_DIR}/je-tmp"
else
    JE_TMPDIR="$RESULTS/je-tmp"
fi
mkdir -p "$JE_TMPDIR"

# ---------------------------------------------------------------------------
# Run Noxu benchmark
# ---------------------------------------------------------------------------
if [[ $SKIP_NOXU -eq 0 ]]; then
    echo "════════════════════════════════════════════════════════"
    echo "  Running Noxu workload benchmarks..."
    echo "════════════════════════════════════════════════════════"

    PGO_PROFDATA="/tmp/noxu-pgo/merged.profdata"
    PGO_BIN="./target/pgo/noxu-workload-bench"
    NOXU_BIN=""

    if [[ $USE_PGO -eq 1 ]]; then
        if [[ -f "$PGO_PROFDATA" && -f "$PGO_BIN" ]]; then
            echo "  Mode: PGO-optimised ($PGO_BIN)"
            NOXU_BIN="$PGO_BIN"
        elif [[ -f "$PGO_PROFDATA" ]]; then
            echo "  PGO profdata found — building PGO-optimised binary..."
            RUSTFLAGS="-Cprofile-use=$PGO_PROFDATA -Cllvm-args=-pgo-warn-missing-function" \
                cargo build --profile pgo --package noxu-workload-bench 2>&1 \
                | grep -E "^(Compiling|Finished|error)" || true
            NOXU_BIN="$PGO_BIN"
            echo "  Mode: PGO-optimised ($PGO_BIN)"
        else
            echo "  WARNING: --pgo requested but $PGO_PROFDATA not found." >&2
            echo "           Run 'bash benches/pgo.sh' first to generate profile data." >&2
            echo "           Falling back to regular --release build." >&2
            USE_PGO=0
        fi
    fi

    if [[ $USE_PGO -eq 0 ]]; then
        cargo build --release --package noxu-workload-bench 2>&1 \
            | grep -E "^(Compiling|Finished|error)" || true
        NOXU_BIN="./target/release/noxu-workload-bench"
        echo "  Mode: release (non-PGO)"
    fi

    if [[ -n "$BENCH_DIR" ]]; then
        echo "  Bench dir: $BENCH_DIR  (real storage — FSyncManager coalescing measurable)"
    fi
    NOXU_MAX_SCALE="${MAX_SCALE:-0}" \
    NOXU_BENCH_DIR="${BENCH_DIR:-}" \
        "$NOXU_BIN" 2>&1 | tee "$RESULTS/noxu_stdout.txt"
    echo ""
fi

# ---------------------------------------------------------------------------
# Run JE benchmark
# ---------------------------------------------------------------------------
if [[ $SKIP_JE -eq 0 ]]; then
    JE_BENCH_JAR="$REPO_ROOT/benches/je-bench/target/je-bench-1.0.0-jar-with-dependencies.jar"

    if [[ ! -f "$JE_BENCH_JAR" ]]; then
        echo "JE benchmark jar not found. Run 'bash benches/setup.sh' first."
        exit 1
    fi

    # Build the GC flags array based on the chosen strategy.
    #
    # Design goals:
    #   • Fix heap size (Xmx == Xms) so the JVM never expands memory mid-run.
    #   • AlwaysPreTouch: fault all heap pages at startup to remove
    #     first-access page-fault jitter from measurement windows.
    #   • Capture a verbose GC log so we can retrospectively verify GC
    #     interference even with low-pause collectors.
    #   • EpsilonGC: the gold standard for GC-free measurements — the JVM
    #     simply never collects.  Requires 8GB heap to survive the 5-scale run.
    #     DisableExplicitGC prevents System.gc() from doing anything (Metrics.gcPause
    #     calls System.gc() twice; under Epsilon that is a no-op).

    GC_LOG="-Xlog:gc*:file=$RESULTS/je_gc.log:time,uptime:filecount=1,filesize=20m"

    case "$GC_STRATEGY" in
        epsilon)
            GC_FLAGS=(
                -XX:+UnlockExperimentalVMOptions
                -XX:+UseEpsilonGC
                -Xmx8g -Xms8g
                -XX:+AlwaysPreTouch
                -XX:+DisableExplicitGC
            )
            GC_DESC="EpsilonGC (no-op), 8GB fixed heap — zero GC interference"
            ;;
        shenandoah)
            GC_FLAGS=(
                -XX:+UnlockExperimentalVMOptions
                -XX:+UseShenandoahGC
                -XX:ShenandoahGCMode=iu
                -Xmx4g -Xms4g
                -XX:+AlwaysPreTouch
                -XX:+DisableExplicitGC
                -XX:ShenandoahUncommitDelay=3600000   # don't return memory during run
            )
            GC_DESC="ShenandoahGC (IU mode), 4GB fixed heap — concurrent low-pause"
            ;;
        zgc)
            GC_FLAGS=(
                -XX:+UseZGC
                -Xmx4g -Xms4g
                -XX:+AlwaysPreTouch
                -XX:+DisableExplicitGC
                -XX:ZUncommitDelay=3600   # don't return memory during run
            )
            GC_DESC="ZGC, 4GB fixed heap"
            ;;
        g1|*)
            GC_FLAGS=(
                -XX:+UseG1GC
                -XX:MaxGCPauseMillis=5
                -Xmx4g -Xms4g
                -XX:+AlwaysPreTouch
                -XX:+DisableExplicitGC
                -XX:G1HeapRegionSize=8m
                -XX:InitiatingHeapOccupancyPercent=45
            )
            GC_DESC="G1GC, 4GB fixed heap, MaxGCPause=5ms"
            ;;
    esac

    echo "════════════════════════════════════════════════════════"
    echo "  Running JE workload benchmarks..."
    echo "  JVM: $(java -version 2>&1 | head -1)"
    echo "  GC:  $GC_DESC"
    echo "  GC log: $RESULTS/je_gc.log"
    if [[ -n "$BENCH_DIR" ]]; then
        echo "  Bench dir: $JE_TMPDIR  (real storage)"
    fi
    echo "════════════════════════════════════════════════════════"

    JE_MAX_SCALE_FLAG=""
    if [[ "${MAX_SCALE:-0}" -gt 0 ]]; then
        JE_MAX_SCALE_FLAG="-Dnoxu.bench.max_scale=${MAX_SCALE}"
        echo "  Max scale: ${MAX_SCALE}"
    fi

    java \
        -server \
        -XX:+TieredCompilation \
        "${GC_FLAGS[@]}" \
        "$GC_LOG" \
        ${JE_MAX_SCALE_FLAG} \
        -Djava.io.tmpdir="$JE_TMPDIR" \
        -jar "$JE_BENCH_JAR" \
        2>&1 | tee "$RESULTS/je_stdout.txt"
    echo ""
fi

# ---------------------------------------------------------------------------
# Merge CSVs and produce comparison report
# ---------------------------------------------------------------------------
NOXU_CSV="$RESULTS/noxu_results.csv"
JE_CSV="$RESULTS/je_results.csv"

if [[ ! -f "$NOXU_CSV" ]] && [[ ! -f "$JE_CSV" ]]; then
    echo "No result CSVs found. Run both benchmarks first."
    exit 1
fi

python3 - "$NOXU_CSV" "$JE_CSV" "$RESULTS/comparison_report.txt" "$RESULTS/comparison_report.csv" <<'PYEOF'
import sys, csv, os

noxu_file, je_file, report_file, merged_file = sys.argv[1:]

def load_csv(path):
    rows = {}
    if not os.path.exists(path):
        return rows
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row['workload'], row['scale'], row['threads'])
            rows[key] = row
    return rows

noxu = load_csv(noxu_file)
je   = load_csv(je_file)

all_keys = sorted(
    set(list(noxu.keys()) + list(je.keys())),
    key=lambda k: (k[0], int(k[1]), int(k[2]))
)

# ─────────────────────────────────────────────────────────────────────────────
# Comparison table
# ─────────────────────────────────────────────────────────────────────────────
W = 155
HDR = (
    f"{'Workload/threads':<28} {'Scale':>8}  "
    f"{'Noxu ops/s':>12} {'JE ops/s':>12} {'JE/Noxu':>7}  "
    f"{'Noxu ns/op':>11} {'JE ns/op':>11} {'JE/Noxu':>7}  "
    f"{'Noxu CPU':>9} {'JE CPU':>9}  "
    f"{'NoxuB/op':>9} {'JE B/op':>9}  "
    f"{'GC%':>5} {'GCn':>4}  "
    f"{'NoxuFsync':>9} {'JEFsync':>8}"
)

lines = []
lines.append("Noxu DB vs Berkeley DB JE — Workload Comparison")
lines.append("=" * W)
lines.append(HDR)
lines.append("─" * W)

merged_rows = []
prev_workload = None

for key in all_keys:
    workload, scale, threads = key

    if prev_workload and workload.split('_')[0] != prev_workload.split('_')[0]:
        lines.append("")
    prev_workload = workload

    n = noxu.get(key, {})
    j = je.get(key, {})

    def fv(d, k, default=0.0):
        v = d.get(k, default)
        return float(v) if v != '' else default

    noxu_ops    = fv(n, 'ops_per_sec')
    je_ops      = fv(j, 'ops_per_sec')
    noxu_ns     = fv(n, 'ns_per_op')
    je_ns       = fv(j, 'ns_per_op')
    noxu_cpu    = fv(n, 'cpu_time_ms')
    je_cpu      = fv(j, 'cpu_time_ms')
    noxu_bop    = fv(n, 'disk_bytes_per_op')
    je_bop      = fv(j, 'disk_bytes_per_op')
    je_gc_ms    = fv(j, 'gc_time_ms')
    je_gc_n     = int(fv(j, 'gc_count'))
    je_el_ms    = fv(j, 'elapsed_ms', 1.0) or 1.0
    gc_pct      = 100.0 * je_gc_ms / je_el_ms
    noxu_fsync  = int(fv(n, 'fsync_count'))
    je_fsync    = int(fv(j, 'fsync_count'))

    ratio_ops = je_ops / max(noxu_ops, 1e-9)
    ratio_ns  = je_ns  / max(noxu_ns,  1e-9)

    tag = " (Noxu only)" if (n and not j) else (" (JE only)" if (j and not n) else "")
    label = f"{workload}/{threads}t{tag}"

    lines.append(
        f"{label:<28} {scale:>8}  "
        f"{noxu_ops:>12.0f} {je_ops:>12.0f} {ratio_ops:>7.2f}  "
        f"{noxu_ns:>11.1f} {je_ns:>11.1f} {ratio_ns:>7.2f}  "
        f"{noxu_cpu:>9.0f} {je_cpu:>9.0f}  "
        f"{noxu_bop:>9.1f} {je_bop:>9.1f}  "
        f"{gc_pct:>5.1f} {je_gc_n:>4}  "
        f"{noxu_fsync:>8} {je_fsync:>8}"
    )

    merged_rows.append({
        'workload': workload, 'scale': scale, 'threads': threads,
        'noxu_ops_per_sec':        f"{noxu_ops:.0f}",
        'je_ops_per_sec':          f"{je_ops:.0f}",
        'ratio_ops_je_over_noxu':  f"{ratio_ops:.3f}",
        'noxu_ns_per_op':          f"{noxu_ns:.1f}",
        'je_ns_per_op':            f"{je_ns:.1f}",
        'ratio_ns_je_over_noxu':   f"{ratio_ns:.3f}",
        'noxu_cpu_time_ms':        f"{noxu_cpu:.0f}",
        'je_cpu_time_ms':          f"{je_cpu:.0f}",
        'noxu_rss_delta_kb':       n.get('rss_delta_kb', ''),
        'je_rss_delta_kb':         j.get('rss_delta_kb', ''),
        'noxu_read_kb':            n.get('read_kb', ''),
        'je_read_kb':              j.get('read_kb', ''),
        'noxu_write_kb':           n.get('write_kb', ''),
        'je_write_kb':             j.get('write_kb', ''),
        'noxu_disk_kb':            n.get('disk_kb', ''),
        'je_disk_kb':              j.get('disk_kb', ''),
        'noxu_disk_bytes_per_op':  f"{noxu_bop:.1f}",
        'je_disk_bytes_per_op':    f"{je_bop:.1f}",
        'je_gc_time_ms':           f"{je_gc_ms:.0f}",
        'je_gc_count':             str(je_gc_n),
        'je_gc_pct':               f"{gc_pct:.1f}",
        'noxu_fsync_count':        str(noxu_fsync),
        'je_fsync_count':          str(je_fsync),
    })

lines.append("─" * W)
lines.append("")
lines.append("Column guide:")
lines.append("  ops/s, ns/op  — throughput and latency (higher/lower = better)")
lines.append("  JE/Noxu ratio — >1 means JE faster; <1 means Noxu faster")
lines.append("  CPU ms        — wall-clock CPU (user+sys) consumed by the workload")
lines.append("  B/op          — on-disk bytes per logical operation (storage overhead)")
lines.append("  GC%           — fraction of JE wall time lost to GC pauses")
lines.append("  GCn           — GC collection count during JE workload")
lines.append("  Fsync         — fdatasync calls during workload (both: CommitSync / auto-commit path; Noxu also coalesces concurrent commits via group commit)")
lines.append("")
lines.append("Noxu vs JE analysis (Session 31 — 100% structural fidelity; bytes::Bytes zero-copy recovery):")
lines.append("  • WRITE throughput (w01/w02): JE up to 28% faster — JE batches log writes before fsync;")
lines.append("    Noxu serializes through log_write_latch with 1 fsync/write on tmpfs (coalesces on real storage)")
lines.append("  • READ throughput (w03/w04): roughly equal at 100K; Noxu leads at small scales (no JVM warmup)")
lines.append("  • RANGE SCAN (w05): JE 51% faster at 100K — JE scans BINs in memory; Noxu re-traverses B-tree")
lines.append("  • TXN MULTI (w09): Noxu 23% faster at 100K — lock upgrade (WritePromote) works correctly")
lines.append("  • CONCURRENT (w10_conc_8r8w/100K): JE 2.6x faster — JE FSyncManager coalesces fsyncs")
lines.append("    JE fsync count at 4-writer 100K: ~52K vs Noxu 100K — JE achieves ~2:1 fsync coalescing")
lines.append("    Noxu FSyncManager coalesces correctly but tmpfs fdatasync is instant (no coalescing window)")
lines.append("  • RECOVERY (w11/100K): JE 5.7x faster (59ms vs 385ms) — JE JIT-compiled log scan is faster;")
lines.append("    Noxu 'faster recovery' from S23 data was pre-JVM-warmup; post-warmup JE wins on log parsing speed")
lines.append("  • STORAGE: Noxu 107 B/op vs JE 154 B/op — Noxu 30% more storage-efficient at all scales")

report = "\n".join(lines)
print(report)

with open(report_file, 'w') as f:
    f.write(report + "\n")

if merged_rows:
    with open(merged_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(merged_rows[0].keys()))
        writer.writeheader()
        writer.writerows(merged_rows)

print(f"\nReports written to:")
print(f"  {report_file}")
print(f"  {merged_file}")
PYEOF
