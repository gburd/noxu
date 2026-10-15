p="crates/noxu-dbi/src/cursor_impl.rs"
s=open(p).read()
i=s.index("<<<<<<< HEAD")
j=s.index(">>>>>>> refs/remotes/rev/c2-on-stack", i)+len(">>>>>>> refs/remotes/rev/c2-on-stack")
block=s[i:j]
assert "BUG-PHANTOM-3 (inserter side)" in block and "SLOT-RELOCK FIX" in block, "not conflict 3"
# Split C2 side (after =======)
sep="======="
c2 = block.split(sep,1)[1]
# strip trailing >>>>>>> marker line
c2 = c2.rsplit(">>>>>>> refs/remotes/rev/c2-on-stack",1)[0]
# C2 side begins with a leading newline; it ends right after the log_ln_write(...)?; block.
# Insert D's PHANTOM-3 range-insert lock immediately BEFORE the C2 log_ln_write call.
needle="        let new_lsn = self\n            .log_ln_write(&two_part_key, Some(b\"\"), self.locker_id, old_lsn)"
assert needle in c2, "log_ln_write anchor not found in C2 side"
phantom3=(
"        // BUG-PHANTOM-3 (D, inserter side): a brand-new dup (two-part key) must\n"
"        // next-key-lock its successor so a concurrent SERIALIZABLE scanner that\n"
"        // range-locked the dup set / next key (RANGE_READ) blocks or restarts\n"
"        // this insert.  Mirror of the non-dup NoOverwrite/NoDupData path\n"
"        // (lock_range_insert(key, old_lsn)): the dup variant keys on the full\n"
"        // TWO-PART key so the successor is the next two-part slot.  Taken AFTER\n"
"        // the C2 slot-relock + KeyExist recheck (so we lock the successor only\n"
"        // on the real-insert path), BEFORE logging.  Skipped for updates\n"
"        // (old_lsn != NULL) and auto-commit inside lock_range_insert.\n"
"        self.lock_range_insert(&two_part_key, old_lsn)?;\n"
+needle
)
c2 = c2.replace(needle, phantom3, 1)
s = s[:i] + c2.lstrip("\n") + s[j:]
open(p,"w").write(s)
print("conflict3 resolved")
