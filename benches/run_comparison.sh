#!/usr/bin/env bash
# benches/run_comparison.sh — Run Noxu and JE benchmarks and produce a
# side-by-side comparison report.
#
# Prerequisites:
#   bash benches/setup.sh     (installs Java, builds JE jar and fat jar)
#
# Usage:
#   bash benches/run_comparison.sh [--skip-noxu] [--skip-je] [--scales 1000,10000]
#
# Output:
#   benches/results/noxu_results.csv
#   benches/results/je_results.csv
#   benches/results/comparison_report.txt   (side-by-side table)
#   benches/results/comparison_report.csv   (merged CSV for further analysis)

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
RESULTS="$REPO_ROOT/benches/results"
mkdir -p "$RESULTS"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
SKIP_NOXU=0
SKIP_JE=0

for arg in "$@"; do
    case $arg in
        --skip-noxu) SKIP_NOXU=1 ;;
        --skip-je)   SKIP_JE=1   ;;
    esac
done

# ---------------------------------------------------------------------------
# Run Noxu benchmark
# ---------------------------------------------------------------------------
if [[ $SKIP_NOXU -eq 0 ]]; then
    echo "════════════════════════════════════════════════════════"
    echo "  Running Noxu workload benchmarks..."
    echo "════════════════════════════════════════════════════════"
    cargo build --release --package noxu-workload-bench 2>&1 | grep -E "^(Compiling|Finished|error)" || true
    ./target/release/noxu-workload-bench 2>&1 | tee "$RESULTS/noxu_stdout.txt"
    echo ""
fi

# ---------------------------------------------------------------------------
# Run JE benchmark
# ---------------------------------------------------------------------------
if [[ $SKIP_JE -eq 0 ]]; then
    JE_JAR="$REPO_ROOT/_/je/dist/lib/je.jar"
    JE_BENCH_JAR="$REPO_ROOT/benches/je-bench/target/je-bench-jar-with-dependencies.jar"

    if [[ ! -f "$JE_BENCH_JAR" ]]; then
        echo "JE benchmark jar not found. Run 'bash benches/setup.sh' first."
        exit 1
    fi

    echo "════════════════════════════════════════════════════════"
    echo "  Running JE workload benchmarks..."
    echo "  JVM: $(java -version 2>&1 | head -1)"
    echo "  GC: G1GC, 2GB fixed heap"
    echo "════════════════════════════════════════════════════════"

    # JVM flags chosen to minimize GC interference:
    #   -Xmx2g -Xms2g   : fixed heap, no expansion GC
    #   -XX:+UseG1GC     : concurrent low-pause GC
    #   -XX:MaxGCPauseMillis=5 : target 5ms pauses
    #   -XX:+AlwaysPreTouch    : pre-fault pages to avoid page-fault noise
    #   -server              : server JIT compilation
    java \
        -server \
        -Xmx2g -Xms2g \
        -XX:+UseG1GC \
        -XX:MaxGCPauseMillis=5 \
        -XX:+AlwaysPreTouch \
        -XX:+DisableExplicitGC \
        -Djava.io.tmpdir="$RESULTS/je-tmp" \
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
from collections import defaultdict

noxu_file, je_file, report_file, merged_file = sys.argv[1:]

def load_csv(path, engine_override=None):
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

# Collect all keys
all_keys = sorted(set(list(noxu.keys()) + list(je.keys())),
                  key=lambda k: (k[0], int(k[1]), int(k[2])))

# ── Comparison table ────────────────────────────────────────────────────────
header_fmt  = "{:<22} {:>8}  {:>12} {:>12} {:>7}  {:>12} {:>12} {:>7}  {:>6}"
row_fmt     = "{:<22} {:>8}  {:>12.0f} {:>12.0f} {:>7.1f}  {:>12.0f} {:>12.0f} {:>7.1f}  {:>6.2f}"
divider     = "─" * 115

lines = []
lines.append("Noxu DB vs Berkeley DB JE — Workload Comparison")
lines.append("=" * 115)
lines.append(header_fmt.format(
    "Workload", "Scale",
    "Noxu ops/s", "JE ops/s", "JE/Noxu",
    "Noxu ns/op", "JE ns/op",  "JE/Noxu",
    "GC%"))
lines.append(divider)

merged_rows = []
prev_workload = None
for key in all_keys:
    workload, scale, threads = key
    if prev_workload and workload != prev_workload:
        lines.append("")
    prev_workload = workload

    n = noxu.get(key, {})
    j = je.get(key, {})

    noxu_ops  = float(n.get('ops_per_sec', 0) or 0)
    je_ops    = float(j.get('ops_per_sec', 0) or 0)
    noxu_ns   = float(n.get('ns_per_op',   0) or 0)
    je_ns     = float(j.get('ns_per_op',   0) or 0)
    je_gc_ms  = float(j.get('gc_time_ms',  0) or 0)
    je_el_ms  = float(j.get('elapsed_ms',  1) or 1)
    gc_pct    = 100.0 * je_gc_ms / max(je_el_ms, 1)

    ratio_ops = je_ops  / max(noxu_ops, 1e-9)
    ratio_ns  = je_ns   / max(noxu_ns,  1e-9)

    tag = ""
    if n and not j:
        tag = "(Noxu only)"
    elif j and not n:
        tag = "(JE only)"

    lines.append(row_fmt.format(
        f"{workload}/{threads}t {tag}", scale,
        noxu_ops, je_ops, ratio_ops,
        noxu_ns,  je_ns,  ratio_ns,
        gc_pct))

    merged_rows.append({
        'workload': workload, 'scale': scale, 'threads': threads,
        'noxu_ops_per_sec': f"{noxu_ops:.0f}", 'je_ops_per_sec': f"{je_ops:.0f}",
        'ratio_ops_je_over_noxu': f"{ratio_ops:.3f}",
        'noxu_ns_per_op': f"{noxu_ns:.1f}", 'je_ns_per_op': f"{je_ns:.1f}",
        'ratio_ns_je_over_noxu': f"{ratio_ns:.3f}",
        'noxu_rss_delta_kb': n.get('rss_delta_kb',''),
        'je_rss_delta_kb':   j.get('rss_delta_kb',''),
        'noxu_read_kb':  n.get('read_kb',''),   'je_read_kb':  j.get('read_kb',''),
        'noxu_write_kb': n.get('write_kb',''),  'je_write_kb': j.get('write_kb',''),
        'noxu_disk_kb':  n.get('disk_kb',''),   'je_disk_kb':  j.get('disk_kb',''),
        'je_gc_time_ms': f"{je_gc_ms:.0f}", 'je_gc_pct': f"{gc_pct:.1f}",
    })

lines.append(divider)
lines.append("")
lines.append("Columns: ops/sec (higher=better), ns/op (lower=better), JE/Noxu ratio, GC%")
lines.append("")
lines.append("⚠  Noxu 1.0.0 caveats:")
lines.append("   • WAL writes not yet implemented (txn commit is a no-op) — writes are")
lines.append("     artificially fast vs JE which does full durability by default.")
lines.append("   • LockManager does not block threads — concurrency overhead understated.")
lines.append("   • B-tree shrink/merge not implemented — delete workloads don't reclaim space.")
lines.append("   For fair comparison of full durability, enable JE's DeferredWrite mode or")
lines.append("   disable JE's log fsyncs (-Dje.durability=COMMIT_NO_SYNC).")

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
