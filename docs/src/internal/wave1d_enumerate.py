#!/usr/bin/env python3
"""Enumerate JE @Test methods for a single package and emit TSV rows.

Usage: wave1d_enumerate.py <je_package_subpath> <output_tsv>

Where <je_package_subpath> is relative to $JE_HOME/test/, e.g.
  com/sleepycat/je/cleaner

Environment:
  JE_HOME             Path to the BDB-JE checkout (required).  The
                      script reads $JE_HOME/test/<package>/*.java and
                      derives test paths relative to $JE_HOME.
  NOXU_INDEX_PATH     Path to the Noxu test index TSV (default:
                      /tmp/noxu_test_index.tsv).  Build it once with
                      `find crates -name '*.rs' | xargs grep -l '#\[test\]'
                      | ...` (see the wave 1D narrative for the exact
                      command).

Strategy:
- find *.java files in that directory (maxdepth 1)
- skip pure utility / non-test files
- extract @Test methods + first javadoc sentence
- name-match against Noxu test index
"""
import os, re, sys, json

JE_HOME = os.environ.get("JE_HOME")
if not JE_HOME:
    sys.exit(
        "error: $JE_HOME is not set.  Point it at your local BDB-JE\n"
        "checkout (e.g. export JE_HOME=$HOME/ws/je) and re-run."
    )
JE_TEST_ROOT = os.path.join(JE_HOME, "test")
NOXU_INDEX_PATH = os.environ.get(
    "NOXU_INDEX_PATH", "/tmp/noxu_test_index.tsv"
)


def load_noxu_index():
    """Return dict: snake_name -> [list of paths]."""
    by_name = {}
    with open(NOXU_INDEX_PATH) as f:
        for line in f:
            line=line.rstrip('\n')
            if not line: continue
            name, path = line.split('\t', 1)
            by_name.setdefault(name.lower(), []).append(path)
    return by_name


def camel_to_snake(s):
    # testFoo -> foo
    if s.startswith('test') and len(s) > 4 and (s[4].isupper() or s[4].isdigit()):
        s = s[4:]
    s2 = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', s)
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s2)
    return s2.lower()


def name_match(je_method, noxu_idx):
    """Return (status, paths_list, matched_name) or (None, [], None)."""
    snake = camel_to_snake(je_method)
    candidates = [snake, "test_" + snake]
    # Also try without trailing _v2-style suffixes; conservative.
    seen = set()
    hits = []
    for c in candidates:
        if c in seen: continue
        seen.add(c)
        if c in noxu_idx:
            for p in noxu_idx[c]:
                hits.append((c, p))
    return hits


# Patterns for parsing
TEST_RE = re.compile(r'@Test\b')
METHOD_DECL = re.compile(r'^\s*(?:public\s+|private\s+|protected\s+)?(?:static\s+)?(?:final\s+)?void\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(')
CLASS_DECL = re.compile(r'^\s*(?:public\s+)?(?:abstract\s+)?(?:final\s+)?class\s+([a-zA-Z_][a-zA-Z0-9_]*)')


def parse_java_file(path):
    """Yield dicts: {class, method, doc} for each @Test method.

    Doc is the first sentence (up to '.') of the immediately-preceding
    javadoc /** ... */, or empty string. Truncated to 200 chars.
    """
    try:
        with open(path, errors='replace') as f:
            text = f.read()
    except OSError:
        return
    lines = text.split('\n')
    # Find class name
    class_name = os.path.splitext(os.path.basename(path))[0]
    for ln in lines:
        m = CLASS_DECL.match(ln)
        if m:
            class_name = m.group(1)
            break
    out = []
    i = 0
    n = len(lines)
    while i < n:
        if TEST_RE.search(lines[i]):
            # Walk forward to method decl, allow other annotations
            method = None
            doc = ""
            for j in range(i+1, min(i+12, n)):
                m = METHOD_DECL.search(lines[j])
                if m:
                    method = m.group(1)
                    break
                # also non-void? sometimes test methods are public Object foo() but @Test usually void
                m2 = re.match(r'^\s*(?:public\s+|private\s+|protected\s+)?(?:static\s+)?[A-Za-z_][A-Za-z0-9_<>,\s]*\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(', lines[j])
                if m2 and '(' in lines[j] and 'class ' not in lines[j]:
                    # Avoid matching annotation params
                    if not lines[j].strip().startswith('@'):
                        method = m2.group(1)
                        break
            # Walk backward to find javadoc
            k = i - 1
            # skip other annotations
            while k >= 0 and (lines[k].strip().startswith('@') or lines[k].strip()==''):
                k -= 1
            if k >= 0 and lines[k].strip().endswith('*/'):
                # find */
                end = k
                start = end
                while start >= 0 and '/**' not in lines[start]:
                    start -= 1
                if start >= 0:
                    raw = '\n'.join(lines[start:end+1])
                    # strip /** ... */ and *
                    raw = re.sub(r'/\*\*|\*/', '', raw)
                    raw = re.sub(r'(?m)^\s*\*\s?', '', raw)
                    raw = raw.strip()
                    # First sentence: split on '.' followed by space/newline
                    m = re.match(r'(.{1,500}?[\.!?])(?:\s|$)', raw, re.DOTALL)
                    sentence = (m.group(1) if m else raw[:500]).strip()
                    sentence = re.sub(r'\s+', ' ', sentence)
                    doc = sentence[:200]
            if method:
                out.append({'class': class_name, 'method': method, 'doc': doc})
            # Advance i past the method we just processed (if found)
            i = (j+1) if method else (i+1)
        else:
            i += 1
    return out


def classify(je_method, noxu_idx, je_class_snake, file_index):
    hits = name_match(je_method, noxu_idx)
    if hits:
        paths = sorted(set(p for _, p in hits))
        names = sorted(set(n for n,_ in hits))
        return ("PORTED-EQUIVALENT", paths[0], names[0], "name-match heuristic" if len(paths)==1 else f"name-match heuristic; {len(paths)} candidate Noxu tests")
    # Try substring fuzzy match against the je_class snake stem (e.g. cleaner_test)
    snake = camel_to_snake(je_method)
    # Look for a PARTIAL hit: any noxu test in a file that matches je_class_snake
    related_files = file_index.get(je_class_snake, [])
    if related_files:
        return ("PORTED-PARTIAL", related_files[0], "", f"class-level match: Noxu file {related_files[0]} covers same class but no method-name twin for {snake}")
    return ("NOT-PORTED", "", "", f"no matching test by name (snake={snake}); behaviour-level check needed")


def is_test_file(path):
    name = os.path.basename(path)
    if not name.endswith('.java'):
        return False
    # Must contain @Test
    try:
        with open(path, errors='replace') as f:
            return '@Test' in f.read()
    except OSError:
        return False


def main():
    if len(sys.argv) < 3:
        print("usage: wave1d_enumerate.py <je_pkg> <out_tsv> [priority]", file=sys.stderr)
        sys.exit(2)
    je_pkg = sys.argv[1]
    out_path = sys.argv[2]
    priority = sys.argv[3] if len(sys.argv) > 3 else "medium"
    pkg_dir = os.path.join(JE_TEST_ROOT, je_pkg)
    if not os.path.isdir(pkg_dir):
        print(f"missing dir: {pkg_dir}", file=sys.stderr); sys.exit(1)
    files = sorted(os.path.join(pkg_dir, f) for f in os.listdir(pkg_dir) if f.endswith('.java'))
    files = [f for f in files if is_test_file(f)]
    noxu_idx = load_noxu_index()
    # build file-level fuzzy index: 'cleaner_test' -> [paths] containing that stem
    file_index = {}
    for paths in noxu_idx.values():
        for p in paths:
            base = os.path.splitext(os.path.basename(p))[0]
            file_index.setdefault(base, [])
            if p not in file_index[base]:
                file_index[base].append(p)

    rows = []
    header = "je_package\tje_class\tje_test_method\tje_test_path\tje_test_doc\tnoxu_status\tnoxu_test_path\tnoxu_test_method\teffort_estimate\tpriority\tnotes\n"
    counts = {'PORTED-EQUIVALENT':0,'PORTED-PARTIAL':0,'PORTED-MISSING':0,'NOT-PORTED':0,'OUT-OF-SCOPE':0}
    for fpath in files:
        rel_path = os.path.relpath(fpath, JE_HOME)
        for entry in (parse_java_file(fpath) or []):
            method = entry['method']
            doc = entry['doc'].replace('\t',' ').replace('\n',' ')
            je_class_snake = camel_to_snake(entry['class'])
            status, npath, nname, notes = classify(method, noxu_idx, je_class_snake, file_index)
            counts[status] = counts.get(status,0)+1
            # effort: small if PORTED-EQUIVALENT, medium if NOT-PORTED, ! depends on doc keywords
            effort = "small"
            if status == "NOT-PORTED":
                # heuristic: large/epic if doc mentions threads, recovery, crash, eviction, cleaner, replication
                lc = (method + " " + doc).lower()
                if any(k in lc for k in ('replicat','rollback','eleciton','election','vlsn','syncup','networkrestore')):
                    effort = "large"
                elif any(k in lc for k in ('recovery','crash','cleaner','eviction','checkpoint','split','utilization')):
                    effort = "large"
                elif any(k in lc for k in ('concurren','thread','deadlock','isolation','dup','secondary','xa','cursor','txn','transaction','log')):
                    effort = "medium"
                else:
                    effort = "medium"
            row = "\t".join([
                je_pkg, entry['class'], method, rel_path,
                doc, status, npath, nname,
                effort, priority,
                notes,
            ])
            rows.append(row)
    with open(out_path,'w') as f:
        f.write(header)
        for r in rows: f.write(r + "\n")
    # report
    print(json.dumps({
        'package': je_pkg,
        'files': len(files),
        'methods': len(rows),
        'counts': counts,
        'output': out_path,
    }))


if __name__ == '__main__':
    main()
