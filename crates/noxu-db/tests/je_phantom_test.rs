//! JE `PhantomTest` port — serializable phantom prevention via next-key /
//! range locking.  Faithful port of a representative slice of
//! `test/com/sleepycat/je/test/PhantomTest.java`.
//!
//! JE citation:
//!   - `PhantomTest.testGetFirst_Success` — under SERIALIZABLE isolation, a
//!     reader that positions at `getFirst` and reads key 2 must prevent a
//!     concurrent insert of key 1 (a phantom BEFORE the first read record):
//!     the insert conflicts on the range lock.  Under a non-serializable
//!     reader the insert is allowed.
//!
//! Scope note (see report): PhantomTest is a 51-method matrix asserting that
//! EVERY cursor read operation (getFirst/getLast/getNext/getPrev/getNextDup/
//! getSearchKey/getSearchKeyRange/getSearchBoth/... × Success/NotFound × Dup)
//! prevents phantoms under serializable via JE's two-thread
//! `startInsert`/`waitForInsert` harness.  The substantive property — a
//! serializable reader's next-key/range lock blocks a phantom insert while a
//! non-serializable reader allows it — is covered by
//! `isolation_test.rs::{test_serializable_prevents_phantom_insert,
//! test_default_isolation_allows_phantom_insert}` (which use `no_wait` to
//! surface the conflict deterministically without the JE thread harness).
//! This file adds one faithful representative (getFirst / insert-before-first)
//! and cites the class; the exhaustive per-operation matrix + the two-thread
//! harness is N/A test-infra (its property is the same range-lock rule).
//!
//! Deviation: JE races a background `startInsert` thread and asserts the
//! serializable reader re-reads the same first key; Noxu uses a `no_wait`
//! inserter txn to surface the range-lock conflict deterministically
//! (LockNotAvailable), the same conflict the JE reader's held lock causes.

use noxu_db::{DatabaseConfig, EnvironmentConfig, TransactionConfig};
use tempfile::TempDir;

fn open(dir: &TempDir) -> (noxu_db::Environment, noxu_db::Database) {
    let env = noxu_db::Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "phantom",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    (env, db)
}

fn ikey(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

/// JE `PhantomTest.testGetFirst_Success` — serializable getFirst prevents an
/// insert BEFORE the first record; non-serializable allows it.
#[test]
fn je_phantom_test_test_get_first_success() {
    // --- Serializable: insert-before-first is blocked. ---
    {
        let dir = TempDir::new().unwrap();
        let (env, db) = open(&dir);

        // Insert key 2 (committed).
        let t = env.begin_transaction(None).unwrap();
        db.put_in(&t, ikey(2), b"v2").unwrap();
        t.commit().unwrap();

        // Serializable reader positions at getFirst → reads key 2, holding a
        // range lock that guards the gap before/at key 2.
        let ser = TransactionConfig::new().with_serializable_isolation(true);
        let reader = env.begin_transaction(Some(&ser)).unwrap();
        let mut c = db.open_cursor_in(&reader, None).unwrap();
        let first = c.next().unwrap();
        assert_eq!(
            first.as_ref().map(|(k, _)| k.as_ref()),
            Some(ikey(2).as_ref()),
            "getFirst must return key 2"
        );

        // A no_wait inserter tries to insert key 1 (a phantom before key 2).
        // The serializable reader's range lock must make this conflict.
        let no_wait = TransactionConfig::new().with_no_wait(true);
        let writer = env.begin_transaction(Some(&no_wait)).unwrap();
        let r = db.put_in(&writer, ikey(1), b"v1");
        let _ = writer.abort();
        assert!(
            r.is_err(),
            "SERIALIZABLE getFirst must block a phantom insert of key 1 \
             before the first record; got {r:?}"
        );

        drop(c);
        reader.commit().unwrap();

        // After the reader commits, the insert of key 1 succeeds.
        let w2 = env.begin_transaction(Some(&no_wait)).unwrap();
        assert!(
            db.put_in(&w2, ikey(1), b"v1").is_ok(),
            "after the serializable reader commits, the insert must succeed"
        );
        w2.commit().unwrap();
    }

    // --- Non-serializable (default): insert-before-first is ALLOWED. ---
    {
        let dir = TempDir::new().unwrap();
        let (env, db) = open(&dir);

        let t = env.begin_transaction(None).unwrap();
        db.put_in(&t, ikey(2), b"v2").unwrap();
        t.commit().unwrap();

        // Default (non-serializable) reader at getFirst.
        let reader = env.begin_transaction(None).unwrap();
        let mut c = db.open_cursor_in(&reader, None).unwrap();
        let _ = c.next().unwrap();

        // no_wait insert of key 1 must be ALLOWED (no range lock held).
        let no_wait = TransactionConfig::new().with_no_wait(true);
        let writer = env.begin_transaction(Some(&no_wait)).unwrap();
        let r = db.put_in(&writer, ikey(1), b"v1");
        assert!(
            r.is_ok(),
            "non-serializable getFirst must NOT block a phantom insert; got {r:?}"
        );
        writer.commit().unwrap();
        drop(c);
        reader.commit().unwrap();
    }
}
