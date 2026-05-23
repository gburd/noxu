#!/usr/bin/env bash
#
# Run every TLA+ specification in tla/ via TLC.
#
# Two classes of spec:
#
#   - Specs whose `.cfg` companion lists invariants we expect to hold.
#     Any TLC violation here is a real failure of the modelled
#     protocol; exit non-zero so CI fails.
#
#   - Specs whose name ends in `Buggy.tla`. These intentionally model a
#     pre-fix variant of a protocol whose invariants we expect TLC to
#     break. If TLC reports "No error has been found" on a buggy spec,
#     that is itself an error — the regression bait stopped catching
#     the bug it was designed to catch — and we exit non-zero.
#
# Locating tla2tools.jar:
#   1. honour $TLA_JAR if set;
#   2. fall back to /Applications/TLA+ Toolbox.app/Contents/Eclipse/tla2tools.jar
#      (the macOS Toolbox install path);
#   3. fall back to /usr/local/lib/tla2tools.jar (Linux convention).
#
# Usage:
#   tla/run.sh                  # run every spec
#   tla/run.sh BTreeLatching    # run a single spec by basename
#

set -euo pipefail

if [[ -z "${TLA_JAR:-}" ]]; then
    if [[ -f "/Applications/TLA+ Toolbox.app/Contents/Eclipse/tla2tools.jar" ]]; then
        TLA_JAR="/Applications/TLA+ Toolbox.app/Contents/Eclipse/tla2tools.jar"
    elif [[ -f "/usr/local/lib/tla2tools.jar" ]]; then
        TLA_JAR="/usr/local/lib/tla2tools.jar"
    else
        echo "ERROR: TLA_JAR not set and tla2tools.jar not found in default locations." >&2
        echo "Install the TLA+ Toolbox or download tla2tools.jar and set TLA_JAR." >&2
        exit 2
    fi
fi
export TLA_JAR

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Determine which specs to run.
specs=()
if [[ $# -gt 0 ]]; then
    for arg in "$@"; do
        specs+=("$arg")
    done
else
    while IFS= read -r f; do
        specs+=("$(basename "$f" .tla)")
    done < <(find . -maxdepth 1 -name '*.tla' | sort)
fi

mkdir -p states
overall_status=0

for spec in "${specs[@]}"; do
    cfg="${spec}.cfg"
    if [[ ! -f "$cfg" ]]; then
        echo "ERROR: $cfg not found; skipping $spec" >&2
        overall_status=1
        continue
    fi
    echo "============================================================"
    echo "  TLC: $spec"
    echo "============================================================"
    out_dir="states/$spec"
    rm -rf "$out_dir"
    set +e
    java -XX:+UseParallelGC \
         -cp "$TLA_JAR" tlc2.TLC \
         -workers auto \
         -metadir "$out_dir" \
         -config "$cfg" \
         "$spec" \
         > "states/${spec}.log" 2>&1
    tlc_status=$?
    set -e

    # Classify: a Buggy spec must have FOUND an invariant violation.
    if [[ "$spec" == *Buggy ]]; then
        if grep -q "Invariant .* is violated" "states/${spec}.log"; then
            echo "  OK ${spec}: TLC correctly found the regression bait."
        else
            echo "  FAIL ${spec}: regression bait did not catch the bug — see states/${spec}.log"
            overall_status=1
        fi
    else
        if [[ $tlc_status -eq 0 ]] && \
           grep -q "Model checking completed. No error has been found." \
                "states/${spec}.log"; then
            echo "  OK ${spec}: TLC finished cleanly."
        else
            echo "  FAIL ${spec}: TLC reported a violation (exit ${tlc_status})"
            tail -30 "states/${spec}.log" | sed 's/^/    /'
            overall_status=1
        fi
    fi
done

if [[ $overall_status -eq 0 ]]; then
    echo
    echo "All TLA+ specs OK."
else
    echo
    echo "TLA+ run reported one or more failures; see states/*.log"
fi
exit $overall_status
