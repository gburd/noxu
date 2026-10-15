p="crates/noxu-db/src/cursor.rs"
s=open(p).read()
start=s.index("<<<<<<< HEAD")
end=s.index(">>>>>>> refs/remotes/rev/c2-on-stack")+len(">>>>>>> refs/remotes/rev/c2-on-stack")
block=s[start:end]
resolved=(
"        // BUG-PHANTOM-1/3 (D): a no-wait serializable inserter blocked by a\n"
"        // scanner RANGE_READ (or any record-lock conflict / range restart)\n"
"        // must surface the TYPED lock error, not a stringy OperationFailed,\n"
"        // so callers can matches!(.., NoxuError::LockNotAvailable) and retry\n"
"        // (JE throws the typed LockConflictException subclass).\n"
"        // D1 / A6 (C2): an LN-fetch / env failure under a read keeps its typed\n"
"        // JE reason instead of being flattened into OperationFailed.\n"
"        // A11 Get::Current (C2): a lock timeout / conflict from the re-read\n"
"        // (relock_and_reread_current -> lock_ln) surfaces as typed\n"
"        // NoxuError::LockTimeout / LockConflict (is_retryable).\n"
"        DbiError::EnvironmentFailure { .. }\n"
"        | DbiError::LnFetchFailure { .. }"
)
assert block in s
s=s.replace(block,resolved)
open(p,"w").write(s)
print("cursor.rs resolved")
