#!/usr/bin/env bash
# benches/setup.sh — Install Java tooling and build the JE jar.
#
# Run from the repository root:
#   bash benches/setup.sh
#
# After this script completes:
#   _/je/dist/lib/je.jar  — Berkeley DB JE 7.5.11 library
#   benches/je-bench/     — ready to build with Maven

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# ---------------------------------------------------------------------------
# 1. Check / install JDK and build tools via Nix
# ---------------------------------------------------------------------------
if ! command -v java &>/dev/null; then
    echo "Installing OpenJDK 21 and Ant via Nix..."
    nix-env -iA nixpkgs.openjdk21_headless nixpkgs.ant nixpkgs.maven
    # Reload profile
    source ~/.nix-profile/etc/profile.d/nix.sh 2>/dev/null || true
fi

JAVA_VER=$(java -version 2>&1 | head -1)
echo "Java: $JAVA_VER"
echo "Ant:  $(ant -version 2>&1 | head -1)"
echo "Maven: $(mvn -version 2>&1 | head -1)"

# ---------------------------------------------------------------------------
# 2. Build JE from source
# ---------------------------------------------------------------------------
JE_DIR="$REPO_ROOT/_/je"
JE_JAR="$JE_DIR/dist/lib/je.jar"

if [[ -f "$JE_JAR" ]]; then
    echo "JE jar already exists: $JE_JAR"
else
    echo ""
    echo "Building Berkeley DB JE from source..."
    cd "$JE_DIR"

    # JE's build.xml checks for JDK 1.8 or 1.9 specifically.
    # Patch the version check temporarily to allow JDK 11+.
    if grep -q "jdk.allowed.versions" build.xml; then
        sed -i.bak 's/value="1.8 or 1.9"/value="1.8 or 1.9 or 11 or 17 or 21"/' build.xml
        sed -i 's/<antversion atleast="1.8.0"\/>/<antversion atleast="1.8.0"\/>/' build.xml || true
        # Remove the version fail condition entirely to be safe
        python3 - <<'PYEOF'
import re, sys

with open("build.xml", "r") as f:
    content = f.read()

# Remove the Java version check block
content = re.sub(
    r'<fail message="Using Java[^"]*"[^>]*>.*?</fail>',
    '<!-- version check removed for JDK 21 compatibility -->',
    content, flags=re.DOTALL
)

with open("build.xml", "w") as f:
    f.write(content)
print("Patched build.xml for JDK 21")
PYEOF
    fi

    # Build using ant — 'jar' target produces the library jar
    ant jar -Djava.source.version=11 -Djava.target.version=11 2>&1 | tail -20

    # Find the output jar (location varies by JE version)
    BUILT_JAR=$(find . -name "je-*.jar" -not -path "*/test/*" 2>/dev/null | head -1)
    if [[ -z "$BUILT_JAR" ]]; then
        echo "ERROR: ant jar did not produce a je-*.jar. Build output:"
        find . -name "*.jar" 2>/dev/null
        exit 1
    fi

    mkdir -p "$(dirname "$JE_JAR")"
    cp "$BUILT_JAR" "$JE_JAR"
    echo "Copied $BUILT_JAR → $JE_JAR"
    cd "$REPO_ROOT"
fi

# ---------------------------------------------------------------------------
# 3. Build the JE benchmark fat jar
# ---------------------------------------------------------------------------
JE_BENCH_DIR="$REPO_ROOT/benches/je-bench"
JE_BENCH_JAR="$JE_BENCH_DIR/target/je-bench-jar-with-dependencies.jar"

if [[ ! -f "$JE_BENCH_JAR" ]]; then
    echo ""
    echo "Building JE benchmark..."
    cd "$JE_BENCH_DIR"
    mvn -q package -Dje.jar.path="$JE_JAR"
    cd "$REPO_ROOT"
fi

echo ""
echo "✓ Setup complete."
echo "  JE jar:       $JE_JAR"
echo "  Benchmark jar: $JE_BENCH_JAR"
echo ""
echo "Run the full comparison with:  bash benches/run_comparison.sh"
