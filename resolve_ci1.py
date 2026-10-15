p="crates/noxu-dbi/src/cursor_impl.rs"
s=open(p).read()
# Conflict 1: Set/Both anchor
h1="<<<<<<< HEAD"
i=s.index(h1)
j=s.index(">>>>>>> refs/remotes/rev/c2-on-stack", i)+len(">>>>>>> refs/remotes/rev/c2-on-stack")
block=s[i:j]
assert "BUG-PHANTOM-1: an exact SET/BOTH match is the search" in block, "not conflict 1"
resolved=(
"                    // BUG-PHANTOM-1 (D): an exact SET/BOTH match is the search\n"
"                    // ANCHOR; JE locks it with searchLockType = READ (not a\n"
"                    // range lock) and does not advance, so an insert outside\n"
"                    // the matched key is not blocked (Cursor.java:4031,\n"
"                    // 4426-4435 exactKeyMatch branch).  Anchor READ via\n"
"                    // lock_ln_mode(.., false) (A15), NOT the forward RANGE_READ.\n"
"                    let contended = self.lock_ln_mode(slot_lsn, false)?;\n"
"                    // X-10 + D1 (C2): if the slot LSN moved (or our lock had to\n"
"                    // wait), the pre-fetched bytes may be a concurrent writer's\n"
"                    // UNCOMMITTED in-place mutation.  Re-read under a lock on the\n"
"                    // slot's CURRENT lsn so we block out the writer's txn\n"
"                    // instead of returning its uncommitted value.  The re-read\n"
"                    // keeps the slot's residency (Option<Bytes>) and returns the\n"
"                    // stable current lsn (C2 tuple), so a non-resident LN is\n"
"                    // fetched below at the lsn it was re-read with (D1).\n"
"                    let (final_data, slot_lsn) = if contended"
)
s=s[:i]+resolved+s[j:]
open(p,"w").write(s)
print("conflict1 resolved")
