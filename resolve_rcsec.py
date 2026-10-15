import re
p="crates/noxu-dbi/src/cursor_impl.rs"
s=open(p).read()
MARK_H="<<<<<<< HEAD"
MARK_M="======="
MARK_E=">>>>>>> refs/remotes/secgone/merge-prep/rc-secgone"

# map: HEAD lock_ln_mode call -> the mode expr, so we can reinject into rc side
def head_mode(headtext):
    m=re.search(r"self\.lock_ln_mode\(([^,]+),\s*([^)]+)\)\?;", headtext)
    assert m, "no lock_ln_mode in HEAD block:\n"+headtext[:300]
    return m.group(1).strip(), m.group(2).strip()

count=0
while MARK_H in s:
    i=s.index(MARK_H)
    mid=s.index("\n"+MARK_M+"\n", i)
    j=s.index(MARK_E, mid)
    jend=s.index("\n", j)+1
    head=s[i+len(MARK_H):mid]
    rc=s[mid+len("\n"+MARK_M+"\n"):j]
    lsn_arg, mode = head_mode(head)
    # swap the rc side's lock_ln(X)? -> lock_ln_mode(X, mode)?  (keep rc var name)
    def repl(mm):
        var=mm.group(1); arg=mm.group(2)
        return "let %s = self.lock_ln_mode(%s, %s)?;" % (var, arg, mode)
    rc2, n = re.subn(r"let (\w+) = self\.lock_ln\(([^)]+)\)\?;", repl, rc, count=1)
    assert n==1, "no lock_ln in rc block:\n"+rc[:300]
    # prepend a one-line note that this is the additive merge
    note="            // merge-prep/d: additive C2-RCSCAN re-read + D PHANTOM range mode.\n"
    # (indentation varies; use the rc block's own leading whitespace of the lock line)
    s = s[:i] + rc2 + s[jend:]
    count+=1
open(p,"w").write(s)
print("resolved %d rc-secgone conflicts" % count)
