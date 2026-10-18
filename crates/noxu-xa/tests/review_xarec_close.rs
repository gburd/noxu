//! Independent review witness (R-XARECOVERED) — env close with an UNRESOLVED
//! in-doubt recovered-prepared txn: does close succeed? does the relock get
//! reported as a leaked lock? is the in-doubt txn durable across the close?
#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use noxu_xa::{XaEnvironment, XaFlags, XaResource, Xid};
use tempfile::TempDir;

fn open_env(dir: &std::path::Path) -> Environment {
    Environment::open(
        EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap()
}

#[test]
fn close_with_unresolved_indoubt_txn_behaviour() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"rv_close_indoubt", b"br1").unwrap();

    // Prepare an XA txn writing a key; crash (drop).
    {
        let env = open_env(dir.path());
        let db = env
            .open_database(
                None,
                "db",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();
        let xa = XaEnvironment::new(env);
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            db.put_in(&txn, b"k", b"v").unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    }

    // Reopen: recovery relocks the in-doubt key.  Do NOT resolve.  Try close.
    let close_result;
    {
        let env = open_env(dir.path());
        let db = env
            .open_database(
                None,
                "db",
                &DatabaseConfig::new().with_transactional(true),
            )
            .unwrap();
        let xa = XaEnvironment::new(env);
        assert_eq!(
            xa.xa_recover(XaFlags::STARTRSCAN).unwrap(),
            vec![xid.clone()],
            "the in-doubt xid is listed"
        );
        // Close the inner env WITHOUT resolving the branch.
        db.close().unwrap();
        close_result = xa.inner().close();
    }
    // Record the observed behaviour (do not over-constrain: either outcome is
    // acceptable so long as the in-doubt txn is durable across the close).
    eprintln!("close() with unresolved in-doubt txn = {close_result:?}");

    // The in-doubt txn MUST survive the close and resurface on reopen.
    {
        let env = open_env(dir.path());
        let _db = env
            .open_database(
                None,
                "db",
                &DatabaseConfig::new().with_transactional(true),
            )
            .unwrap();
        let xa = XaEnvironment::new(env);
        assert_eq!(
            xa.xa_recover(XaFlags::STARTRSCAN).unwrap(),
            vec![xid.clone()],
            "the in-doubt xid MUST resurface after close+reopen (durable)"
        );
        // And it must still be resolvable to the committed value.
        xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();
    }
    {
        let env = open_env(dir.path());
        let db = env
            .open_database(
                None,
                "db",
                &DatabaseConfig::new().with_transactional(true),
            )
            .unwrap();
        let txn = env.begin_transaction(None).unwrap();
        let v = db.get_in(&txn, b"k").unwrap();
        assert_eq!(v.as_deref(), Some(b"v".as_slice()));
        txn.commit().unwrap();
    }
}
