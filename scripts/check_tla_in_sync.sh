#!/usr/bin/env bash
#
# Advisory: warn when a Rust file modelled by a TLA+ spec changed
# without the spec itself being touched in the same PR.
#
# Each .tla file in tla/ that wishes to be checked declares the Rust
# files it models with `MODELS:` lines in its header comment. Example:
#
#     (* MODELS: crates/noxu-tree/src/tree.rs *)
#     (* MODELS: crates/noxu-rep/src/elections/paxos.rs *)
#
# `tla/check_tla_in_sync.sh` reads those declarations, computes the set
# of files changed in the working tree relative to origin/main (or the
# argument passed as $1), and prints a warning per spec whose tracked
# files have been modified without the spec being touched.
#
# By default the script exits 0 (advisory); set NOXU_TLA_CHECK_STRICT=1
# to make warnings fatal.

set -euo pipefail

base_ref="${1:-origin/main}"
strict="${NOXU_TLA_CHECK_STRICT:-0}"

# Collect the diff once.
if ! git -P rev-parse --verify "$base_ref" > /dev/null 2>&1; then
    echo "WARNING: base ref $base_ref does not exist locally; check skipped"
    exit 0
fi
changed=$(git -P diff --name-only "$base_ref"...HEAD)

warnings=0
for spec in tla/*.tla; do
    [[ -f "$spec" ]] || continue
    base=$(basename "$spec" .tla)
    spec_changed=$(echo "$changed" | grep -F "tla/$(basename "$spec")" || true)

    # Read MODELS declarations from the spec header.
    models=$(grep -E '^\s*\(\*\s*MODELS:' "$spec" \
        | sed -E 's|.*MODELS:\s*||; s|\s*\*\)$||' \
        | tr -d '[:space:]' \
        | sort -u)

    if [[ -z "$models" ]]; then
        # Spec didn't declare any models — silently OK.
        continue
    fi

    rust_changed=()
    while IFS= read -r f; do
        [[ -n "$f" ]] || continue
        if echo "$changed" | grep -Fxq "$f"; then
            rust_changed+=("$f")
        fi
    done <<< "$models"

    if [[ ${#rust_changed[@]} -gt 0 && -z "$spec_changed" ]]; then
        echo "WARNING: $spec models the following Rust files which were"
        echo "         changed in this PR without the spec being touched:"
        for f in "${rust_changed[@]}"; do
            echo "           - $f"
        done
        echo "         If the protocol semantics changed, update $spec; if"
        echo "         only the implementation refactored, add a comment"
        echo "         in the CR description asserting the spec is still"
        echo "         valid."
        warnings=$((warnings + 1))
    fi
done

if [[ "$warnings" -eq 0 ]]; then
    echo "TLA+ sync check: OK (no spec/code drift detected)."
    exit 0
fi

if [[ "$strict" == "1" ]]; then
    echo "TLA+ sync check FAILED ($warnings warning(s)) — strict mode."
    exit 1
fi

echo "TLA+ sync check: $warnings advisory warning(s) above; not fatal."
exit 0
