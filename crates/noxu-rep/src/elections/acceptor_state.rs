//! Persistent acceptor state for the Paxos election protocol (F5/F31).
//!
//! Closes findings F5 and F31 of the 2026 review.
//!
//! The Paxos safety property is "an acceptor never accepts a proposal at
//! a term lower than its highest promise."  Without crash durability of
//! the promise, a node that restarts forgets every promise it has ever
//! made.  An old proposer (whose proposal would have been rejected on
//! the basis of a higher promise) can then win a fresh majority and
//! become a second master at the same effective term — split-brain.
//!
//! This module persists `(promised_term, accepted_term, accepted_master)`
//! to a small file `<env_home>/acceptor.state` and reloads it on
//! startup.  Every state change is atomic (write+rename) and CRC32-protected.
//!
//! ## Property tests
//!
//! Paxos safety properties (promise/accept contracts, monotonicity,
//! restart-preserves-promise) live in `crates/noxu-rep/tests/prop_tests.rs`
//! (Wave 11-E).  These complement the Stateright spec by exercising the
//! production code path end-to-end.
//!
//! # On-disk format
//!
//! ```text
//!   bytes  field
//!   ---------------------------------------------------------
//!   0..4   magic            = b"PXST"
//!   4..6   version          = u16, currently 1
//!   6..14  promised_term    = u64
//!  14..22  accepted_term    = u64
//!  22..24  master_len       = u16  (0 if no accepted master yet)
//!  24..    master_bytes     = [u8; master_len]  (UTF-8)
//!  ...end  crc32            = u32 over all preceding bytes
//! ```

use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

const MAGIC: &[u8; 4] = b"PXST";
const VERSION: u16 = 1;
const MIN_LEN: usize = 4 + 2 + 8 + 8 + 2 + 4; // header + crc

/// File name used inside env_home.
pub const ACCEPTOR_STATE_FILE: &str = "acceptor.state";
const ACCEPTOR_STATE_TMP: &str = "acceptor.state.tmp";

#[derive(Debug, thiserror::Error)]
pub enum AcceptorPersistError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("bad magic in acceptor.state (got {0:?})")]
    BadMagic([u8; 4]),
    #[error("unsupported acceptor.state version {0}")]
    BadVersion(u16),
    #[error(
        "truncated acceptor.state (expected at least {expected} bytes, got {got})"
    )]
    Truncated { expected: usize, got: usize },
    #[error(
        "acceptor.state checksum mismatch: stored {stored:08x}, computed {computed:08x}"
    )]
    BadChecksum { stored: u32, computed: u32 },
    #[error("invalid UTF-8 master name in acceptor.state")]
    BadMasterName,
}

pub type Result<T> = std::result::Result<T, AcceptorPersistError>;

/// Path to the acceptor state file inside `env_home`.
pub fn state_path(env_home: &Path) -> PathBuf {
    env_home.join(ACCEPTOR_STATE_FILE)
}

fn tmp_path(env_home: &Path) -> PathBuf {
    env_home.join(ACCEPTOR_STATE_TMP)
}

/// Persistent acceptor state.
///
/// Holds the highest term we have promised, the highest term whose
/// `ElectionResult` we have accepted, and the master we accepted in
/// that term.  All three are flushed atomically to disk whenever
/// `try_promise` or `try_accept` succeed.
///
/// When `persist_path` is `None`, the state is in-memory only — used
/// for tests and for the legacy non-persistent code path.  Production
/// callers obtain a path via `load_or_default(env_home)`.
pub struct PersistentAcceptorState {
    promised_term: AtomicU64,
    accepted_term: AtomicU64,
    accepted_master: Mutex<Option<String>>,
    persist_path: Option<PathBuf>,
    // Serializes flushes so two concurrent acceptor threads don't race
    // each other into the rename.
    flush_lock: Mutex<()>,
}

impl PersistentAcceptorState {
    /// Construct an in-memory-only acceptor state (no persistence).
    pub fn in_memory() -> Self {
        Self {
            promised_term: AtomicU64::new(0),
            accepted_term: AtomicU64::new(0),
            accepted_master: Mutex::new(None),
            persist_path: None,
            flush_lock: Mutex::new(()),
        }
    }

    /// Construct a state that persists every change to
    /// `<env_home>/acceptor.state`.  If a state file exists at that
    /// path, it is loaded and the in-memory state seeded from it.
    /// Corrupt files are logged and removed — the caller is treated
    /// as a fresh acceptor.
    pub fn load_or_default(env_home: &Path) -> Self {
        let path = state_path(env_home);
        let st = Self {
            promised_term: AtomicU64::new(0),
            accepted_term: AtomicU64::new(0),
            accepted_master: Mutex::new(None),
            persist_path: Some(path.clone()),
            flush_lock: Mutex::new(()),
        };
        match load_from_disk(&path) {
            Ok(Some((p, a, m))) => {
                st.promised_term.store(p, Ordering::SeqCst);
                st.accepted_term.store(a, Ordering::SeqCst);
                *st.accepted_master.lock().unwrap() = m;
                log::info!(
                    "Loaded acceptor.state from {}: promised={}, accepted={}, master={:?}",
                    path.display(),
                    p,
                    a,
                    st.accepted_master.lock().unwrap(),
                );
            }
            Ok(None) => {
                log::debug!(
                    "No acceptor.state at {} (fresh acceptor)",
                    path.display()
                );
            }
            Err(e) => {
                log::warn!(
                    "Corrupt acceptor.state at {}: {}; treating as fresh acceptor",
                    path.display(),
                    e
                );
                let _ = std::fs::remove_file(&path);
            }
        }
        st
    }

    /// Snapshot the (promised_term, accepted_term, accepted_master).
    pub fn snapshot(&self) -> (u64, u64, Option<String>) {
        (
            self.promised_term.load(Ordering::SeqCst),
            self.accepted_term.load(Ordering::SeqCst),
            self.accepted_master.lock().unwrap().clone(),
        )
    }

    /// Highest promised term.
    pub fn promised_term(&self) -> u64 {
        self.promised_term.load(Ordering::SeqCst)
    }

    /// Highest accepted term.
    pub fn accepted_term(&self) -> u64 {
        self.accepted_term.load(Ordering::SeqCst)
    }

    /// The master name accepted in `accepted_term`, if any.
    pub fn accepted_master(&self) -> Option<String> {
        self.accepted_master.lock().unwrap().clone()
    }

    /// Try to promise term `t`.
    ///
    /// Returns `true` (with the new state flushed to disk) iff
    /// `t >= self.promised_term()`.  Otherwise the state is unchanged
    /// and the caller must reject the proposer.
    pub fn try_promise(&self, t: u64) -> bool {
        // Coarse-lock under flush_lock so we serialize the
        // load-modify-flush cycle across acceptor threads.
        let _guard = self.flush_lock.lock().unwrap();
        let cur = self.promised_term.load(Ordering::SeqCst);
        if t < cur {
            return false;
        }
        self.promised_term.store(t, Ordering::SeqCst);
        if let Err(e) = self.flush_locked() {
            // Roll back to avoid lying about persistence.
            self.promised_term.store(cur, Ordering::SeqCst);
            log::warn!(
                "acceptor.state: failed to persist promise(t={}): {}",
                t,
                e
            );
            return false;
        }
        true
    }

    /// Try to accept that `master` won at term `t`.
    ///
    /// Returns `true` (with the new state flushed to disk) iff `t` EXACTLY
    /// equals `self.promised_term()`.  JE Acceptor.process(Accept) rejects
    /// unless the Accept's proposal equals the promised proposal
    /// (`promisedProposal.compareTo(accept.getProposal()) == 0`) — an Accept
    /// at a higher term that was never promised in phase 1 must be rejected
    /// to preserve the Paxos invariant that the phase-2 value is fixed to the
    /// specific phase-1 round (split-brain otherwise). Persists the new
    /// (accepted_term, accepted_master) on success.
    pub fn try_accept(&self, t: u64, master: &str) -> bool {
        let _guard = self.flush_lock.lock().unwrap();
        let promised = self.promised_term.load(Ordering::SeqCst);
        if t != promised {
            return false;
        }
        let prev_accepted_term = self.accepted_term.load(Ordering::SeqCst);
        let prev_accepted_master = self.accepted_master.lock().unwrap().clone();
        // Update the acceptance.  Also implicitly bump promised_term
        // to t so subsequent promises at term < t are rejected.
        self.promised_term.store(t, Ordering::SeqCst);
        self.accepted_term.store(t, Ordering::SeqCst);
        *self.accepted_master.lock().unwrap() = Some(master.to_string());
        if let Err(e) = self.flush_locked() {
            self.promised_term.store(promised, Ordering::SeqCst);
            self.accepted_term.store(prev_accepted_term, Ordering::SeqCst);
            *self.accepted_master.lock().unwrap() = prev_accepted_master;
            log::warn!(
                "acceptor.state: failed to persist accept(t={}, master={}): {}",
                t,
                master,
                e
            );
            return false;
        }
        true
    }

    fn flush_locked(&self) -> Result<()> {
        let Some(ref path) = self.persist_path else {
            // In-memory mode: no-op.
            return Ok(());
        };
        let promised = self.promised_term.load(Ordering::SeqCst);
        let accepted = self.accepted_term.load(Ordering::SeqCst);
        let master = self.accepted_master.lock().unwrap().clone();

        let env_home = path.parent().ok_or_else(|| {
            io::Error::other("acceptor.state path has no parent")
        })?;
        let tmp = tmp_path(env_home);

        let mut buf: Vec<u8> = Vec::with_capacity(MIN_LEN + 64);
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&VERSION.to_le_bytes());
        buf.extend_from_slice(&promised.to_le_bytes());
        buf.extend_from_slice(&accepted.to_le_bytes());
        let m_bytes = master.as_deref().unwrap_or("").as_bytes();
        let m_len: u16 = m_bytes.len().try_into().unwrap_or(u16::MAX);
        buf.extend_from_slice(&m_len.to_le_bytes());
        buf.extend_from_slice(&m_bytes[..m_len as usize]);

        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(&buf)?;
        f.sync_all()?;
        drop(f);

        std::fs::rename(&tmp, path)?;
        Ok(())
    }
}

fn load_from_disk(path: &Path) -> Result<Option<(u64, u64, Option<String>)>> {
    let mut f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let mut bytes = Vec::new();
    f.read_to_end(&mut bytes)?;
    if bytes.len() < MIN_LEN {
        return Err(AcceptorPersistError::Truncated {
            expected: MIN_LEN,
            got: bytes.len(),
        });
    }

    // CRC over bytes[..len-4]
    let body_end = bytes.len() - 4;
    let mut buf4 = [0u8; 4];
    buf4.copy_from_slice(&bytes[body_end..]);
    let stored_crc = u32::from_le_bytes(buf4);
    let computed_crc = crc32fast::hash(&bytes[..body_end]);
    if stored_crc != computed_crc {
        return Err(AcceptorPersistError::BadChecksum {
            stored: stored_crc,
            computed: computed_crc,
        });
    }

    let mut magic = [0u8; 4];
    magic.copy_from_slice(&bytes[0..4]);
    if &magic != MAGIC {
        return Err(AcceptorPersistError::BadMagic(magic));
    }
    let mut buf2 = [0u8; 2];
    buf2.copy_from_slice(&bytes[4..6]);
    let version = u16::from_le_bytes(buf2);
    if version != VERSION {
        return Err(AcceptorPersistError::BadVersion(version));
    }

    let mut buf8 = [0u8; 8];
    buf8.copy_from_slice(&bytes[6..14]);
    let promised = u64::from_le_bytes(buf8);
    buf8.copy_from_slice(&bytes[14..22]);
    let accepted = u64::from_le_bytes(buf8);
    buf2.copy_from_slice(&bytes[22..24]);
    let master_len = u16::from_le_bytes(buf2) as usize;

    if bytes.len() < 24 + master_len + 4 {
        return Err(AcceptorPersistError::Truncated {
            expected: 24 + master_len + 4,
            got: bytes.len(),
        });
    }

    let master = if master_len == 0 {
        None
    } else {
        let s = std::str::from_utf8(&bytes[24..24 + master_len])
            .map_err(|_| AcceptorPersistError::BadMasterName)?;
        Some(s.to_string())
    };

    Ok(Some((promised, accepted, master)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn in_memory_state_round_trips() {
        let s = PersistentAcceptorState::in_memory();
        assert_eq!(s.snapshot(), (0, 0, None));
        assert!(s.try_promise(5));
        assert_eq!(s.promised_term(), 5);
        // Stale promise rejected.
        assert!(!s.try_promise(3));
        assert_eq!(s.promised_term(), 5);
        // Higher promise accepted.
        assert!(s.try_promise(7));
        assert_eq!(s.promised_term(), 7);
    }

    #[test]
    fn try_accept_persists() {
        let dir = TempDir::new().unwrap();
        let s = PersistentAcceptorState::load_or_default(dir.path());
        assert!(s.try_promise(5));
        assert!(s.try_accept(5, "node-a"));
        assert_eq!(s.accepted_term(), 5);
        assert_eq!(s.accepted_master(), Some("node-a".into()));

        // Reload from disk: state must persist.
        let s2 = PersistentAcceptorState::load_or_default(dir.path());
        assert_eq!(s2.promised_term(), 5);
        assert_eq!(s2.accepted_term(), 5);
        assert_eq!(s2.accepted_master(), Some("node-a".into()));
    }

    #[test]
    fn restart_does_not_unmake_a_promise() {
        // Acceptor promised term 10; restart; an old proposer with term 7
        // must still be rejected.  This is the F5/F31 invariant.
        let dir = TempDir::new().unwrap();
        {
            let s = PersistentAcceptorState::load_or_default(dir.path());
            assert!(s.try_promise(10));
        }
        // Simulate restart by re-loading.
        let s2 = PersistentAcceptorState::load_or_default(dir.path());
        assert_eq!(s2.promised_term(), 10);
        assert!(
            !s2.try_promise(7),
            "post-restart acceptor must reject term lower than persisted promise"
        );
    }

    #[test]
    fn corrupt_state_recovers_with_fresh_state() {
        let dir = TempDir::new().unwrap();
        {
            let s = PersistentAcceptorState::load_or_default(dir.path());
            // Must promise before accepting at the same term (D1 faithful
            // semantics: accept only at the promised term).
            assert!(s.try_promise(3));
            assert!(s.try_accept(3, "x"));
        }
        // Corrupt the file.
        let path = state_path(dir.path());
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&path, bytes).unwrap();

        // Reload: corrupt file is removed; state is fresh.
        let s = PersistentAcceptorState::load_or_default(dir.path());
        assert_eq!(s.snapshot(), (0, 0, None));
    }

    #[test]
    fn try_accept_below_promise_rejected() {
        let dir = TempDir::new().unwrap();
        let s = PersistentAcceptorState::load_or_default(dir.path());
        assert!(s.try_promise(5));
        assert!(!s.try_accept(3, "x"));
        assert_eq!(s.accepted_term(), 0);
    }

    #[test]
    fn try_accept_requires_matching_promise() {
        // JE Acceptor.process(Accept) rejects unless the Accept's proposal
        // equals the promised proposal. An accept at a term we never promised
        // (here: no prior promise -> promised == 0) must be rejected.
        let dir = TempDir::new().unwrap();
        let s = PersistentAcceptorState::load_or_default(dir.path());
        assert!(!s.try_accept(7, "winner"), "accept without matching promise");
        assert_eq!(s.accepted_term(), 0);
        // After promising 7, accept at 7 succeeds.
        assert!(s.try_promise(7));
        assert!(s.try_accept(7, "winner"));
        assert_eq!(s.accepted_term(), 7);
    }

    #[test]
    fn try_accept_higher_term_than_promise_rejected_split_brain_guard() {
        // D1 split-brain regression: a proposer that obtained a phase-1
        // promise at term T1 but then sends a phase-2 Accept at T2 > T1
        // (without a fresh phase 1) MUST be rejected. Accepting T2 here would
        // let two proposers at different terms both reach phase-2 quorum.
        let dir = TempDir::new().unwrap();
        let s = PersistentAcceptorState::load_or_default(dir.path());
        assert!(s.try_promise(5));
        assert!(
            !s.try_accept(6, "intruder"),
            "D1: accept at term > promised must be rejected (split-brain)"
        );
        assert_eq!(s.accepted_term(), 0, "no acceptance recorded");
        // The promised term is unchanged, so the legitimate proposer at 5 can
        // still complete phase 2.
        assert_eq!(s.promised_term(), 5);
        assert!(s.try_accept(5, "legit"));
        assert_eq!(s.accepted_term(), 5);
    }
}
