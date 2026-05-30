#!/usr/bin/env bash
# copy_module.sh <crate_name_without_noxu_prefix> <optional: "nostrip">
# Copies src/ from crates/noxu-<mod>/ into crates/noxu/src/<mod>/
# and rewrites imports.
set -euo pipefail

MOD="$1"
CRATE="crates/noxu-${MOD}"
DEST="crates/noxu/src/${MOD}"
SCRIPT="scripts/rewrite_imports.py"

echo "=== Copying module: ${MOD} ==="

# Create destination
mkdir -p "${DEST}"

# Copy all .rs files from src/ (recursively)
rsync -a --include="*.rs" --include="*/" --exclude="*" "${CRATE}/src/" "${DEST}/"

# Rename lib.rs -> mod.rs
if [ -f "${DEST}/lib.rs" ]; then
    mv "${DEST}/lib.rs" "${DEST}/mod.rs"
fi

# Rewrite imports in all .rs files
find "${DEST}" -name "*.rs" | while read -r f; do
    python3 "${SCRIPT}" src "${f}"
done

echo "=== Done: ${MOD} ==="
