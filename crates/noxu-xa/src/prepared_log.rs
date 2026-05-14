//! Persistent prepared-transaction log for XA crash recovery.
//!
//! Stores prepared XIDs in a dedicated database within the environment so that
//! after a crash, `xa_recover()` can return XIDs that were prepared but not yet
//! committed or rolled back.

use noxu_db::{Database, DatabaseConfig, DatabaseEntry, Environment, OperationStatus};

use crate::xid::Xid;

/// Database name for the internal XA prepared-transaction log.
const XA_PREPARED_DB: &str = "_xa_prepared";

/// Persistent log of prepared XA branches.
///
/// Stores Xid→timestamp mappings in a hidden database. Written during
/// `xa_prepare`, deleted during `xa_commit`/`xa_rollback`/`xa_forget`.
pub struct PreparedLog {
    db: Database,
}

impl PreparedLog {
    /// Open (or create) the prepared-transaction log database.
    pub fn open(env: &Environment) -> Result<Self, noxu_db::NoxuError> {
        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let db = env.open_database(None, XA_PREPARED_DB, &db_config)?;
        Ok(Self { db })
    }

    /// Record that a branch has been prepared.
    pub fn record_prepare(&self, xid: &Xid) -> Result<(), noxu_db::NoxuError> {
        let key = Self::xid_to_key(xid);
        let k = DatabaseEntry::from_vec(key);
        // Value: current timestamp as u64 (nanos since epoch)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let v = DatabaseEntry::from_vec(now.to_le_bytes().to_vec());
        self.db.put(None, &k, &v)?;
        Ok(())
    }

    /// Remove the prepared record (on commit, rollback, or forget).
    pub fn remove(&self, xid: &Xid) -> Result<(), noxu_db::NoxuError> {
        let key = Self::xid_to_key(xid);
        let k = DatabaseEntry::from_vec(key);
        let _ = self.db.delete(None, &k);
        Ok(())
    }

    /// Recover all prepared XIDs from the persistent log.
    ///
    /// Called on environment startup to report in-doubt transactions
    /// to the Transaction Manager for re-resolution.
    pub fn recover_all(&self) -> Result<Vec<Xid>, noxu_db::NoxuError> {
        use noxu_db::{CursorConfig, Get};

        let mut cursor = self.db.open_cursor(None, Some(&CursorConfig::new()))?;
        let mut xids = Vec::new();
        let mut key = DatabaseEntry::new();
        let mut val = DatabaseEntry::new();

        let mut status = cursor.get(&mut key, &mut val, Get::First, None)?;
        while status == OperationStatus::Success {
            if let Some(data) = key.get_data() {
                if let Some(xid) = Self::key_to_xid(data) {
                    xids.push(xid);
                }
            }
            status = cursor.get(&mut key, &mut val, Get::Next, None)?;
        }

        Ok(xids)
    }

    /// Serialize Xid to a database key.
    ///
    /// Format: [format_id:4 LE][gtrid_len:1][gtrid bytes][bqual bytes]
    fn xid_to_key(xid: &Xid) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + 1 + xid.global_transaction_id.len() + xid.branch_qualifier.len());
        buf.extend_from_slice(&xid.format_id.to_le_bytes());
        buf.push(xid.global_transaction_id.len() as u8);
        buf.extend_from_slice(&xid.global_transaction_id);
        buf.extend_from_slice(&xid.branch_qualifier);
        buf
    }

    /// Deserialize Xid from a database key.
    fn key_to_xid(data: &[u8]) -> Option<Xid> {
        if data.len() < 5 {
            return None;
        }
        let format_id = i32::from_le_bytes(data[0..4].try_into().ok()?);
        let gtrid_len = data[4] as usize;
        if data.len() < 5 + gtrid_len {
            return None;
        }
        let gtrid = &data[5..5 + gtrid_len];
        let bqual = &data[5 + gtrid_len..];
        Xid::new(format_id, gtrid, bqual).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_env() -> (Environment, TempDir) {
        let dir = TempDir::new().unwrap();
        let config = noxu_db::EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        let env = Environment::open(config).unwrap();
        (env, dir)
    }

    #[test]
    fn test_roundtrip_xid_serialization() {
        let xid = Xid::new(42, b"hello_gtrid", b"world_bqual").unwrap();
        let key = PreparedLog::xid_to_key(&xid);
        let recovered = PreparedLog::key_to_xid(&key).unwrap();
        assert_eq!(recovered, xid);
    }

    #[test]
    fn test_record_and_recover() {
        let (env, _dir) = make_env();
        let log = PreparedLog::open(&env).unwrap();

        let xid1 = Xid::new(1, b"g1", b"b1").unwrap();
        let xid2 = Xid::new(1, b"g2", b"b2").unwrap();

        log.record_prepare(&xid1).unwrap();
        log.record_prepare(&xid2).unwrap();

        let recovered = log.recover_all().unwrap();
        assert_eq!(recovered.len(), 2);
        assert!(recovered.contains(&xid1));
        assert!(recovered.contains(&xid2));
    }

    #[test]
    fn test_remove_clears_record() {
        let (env, _dir) = make_env();
        let log = PreparedLog::open(&env).unwrap();

        let xid = Xid::new(1, b"removable", b"branch").unwrap();
        log.record_prepare(&xid).unwrap();
        log.remove(&xid).unwrap();

        let recovered = log.recover_all().unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn test_recover_empty() {
        let (env, _dir) = make_env();
        let log = PreparedLog::open(&env).unwrap();
        let recovered = log.recover_all().unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn test_persist_across_reopen() {
        let dir = TempDir::new().unwrap();
        let xid = Xid::new(7, b"crash_test", b"branch_1").unwrap();

        // First open: prepare
        {
            let config = noxu_db::EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true);
            let env = Environment::open(config).unwrap();
            let log = PreparedLog::open(&env).unwrap();
            log.record_prepare(&xid).unwrap();
            drop(log);
            drop(env);
        }

        // Second open: recover (simulating crash + restart)
        {
            let config = noxu_db::EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true);
            let env = Environment::open(config).unwrap();
            let log = PreparedLog::open(&env).unwrap();
            let recovered = log.recover_all().unwrap();
            assert_eq!(recovered.len(), 1);
            assert_eq!(recovered[0], xid);
        }
    }
}
