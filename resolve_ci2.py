p="crates/noxu-dbi/src/cursor_impl.rs"
s=open(p).read()
i=s.index("<<<<<<< HEAD")
j=s.index(">>>>>>> refs/remotes/rev/c2-on-stack", i)+len(">>>>>>> refs/remotes/rev/c2-on-stack")
block=s[i:j]
assert "a range search whose key EXACTLY matches a" in block, "not conflict 2: "+block[:80]
resolved=(
"                    // BUG-PHANTOM-1 (D): a range search whose key EXACTLY matches\n"
"                    // a slot is the search ANCHOR (JE searchRangeSerializable\n"
"                    // exactKeyMatch -> searchLockType = READ,\n"
"                    // Cursor.java:4426-4435).  The advance-to-next-slot case\n"
"                    // (search key absent) is handled in the else arm below\n"
"                    // with the RANGE_READ advance lock.  Anchor READ via\n"
"                    // lock_ln_mode(.., false) (A15), NOT the forward RANGE_READ.\n"
"                    let contended = self.lock_ln_mode(slot_lsn, false)?;\n"
"                    // X-10 + D1 (C2): re-read under a lock on the slot's CURRENT\n"
"                    // lsn (see the Set/Both branch) so a moved slot never returns\n"
"                    // a concurrent writer's uncommitted bytes; the re-read keeps\n"
"                    // residency and returns the stable current lsn so a\n"
"                    // non-resident LN is fetched below at that lsn (D1).\n"
"                    let (final_data, slot_lsn) = if contended"
)
s=s[:i]+resolved+s[j:]
open(p,"w").write(s)
print("conflict2 resolved")
