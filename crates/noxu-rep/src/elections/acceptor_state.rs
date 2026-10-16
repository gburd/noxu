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
//! This module persists `(promised_term, promised_proposer, accepted_term,
//! accepted_master)` to a small file `<env_home>/acceptor.state` and reloads
//! it on startup.  Every state change is atomic (write+rename) and
//! CRC32-protected.
//!
//! # FID-ELECT-1: the proposal number is `(term, proposer)`, not a bare term
//!
//! JE's Paxos acceptor is safe because every proposal carries a **globally
//! unique** proposal number — `TimebasedProposalGenerator` produces
//! `ms_time | machineId | uniqueId` where `machineId` is the host IP XORed
//! with a `SecureRandom`, distinct per machine
//! (`TimebasedProposalGenerator.java:28-43,118-182`).  The acceptor compares
//! the **whole** number: `process(Propose)` rejects only when
//! `promisedProposal.compareTo(propose) > 0` (strictly greater) and
//! `process(Accept)` grants only when `promisedProposal.compareTo(accept)
//! == 0` (exact equality) — `Acceptor.java:177-185,210-215`.  Because the
//! number is globally unique, those two rules force at most one proposer to
//! hold the promise on any acceptor, so a second proposer at the same
//! `ms_time` differs in `machineId` and is either rejected or overwrites the
//! first (whose later Accept, carrying the now-stale number, then fails the
//! `== 0` test).
//!
//! Noxu's election term is a per-node-local `u64` counter that every node's
//! driver starts at `1`, so the term **alone** is not unique across
//! proposers.  To stay JE-faithful we form the proposal number from
//! `(term, proposer)`, where `proposer` is the self-reported candidate name
//! already carried in the phase-1 `ElectionProposal` (the analogue of JE's
//! `machineId` segment) and the term plays the role of the ascending
//! `ms_time`.  `try_promise`/`try_accept` key on the whole `(term, proposer)`
//! pair with JE's two comparison rules, so two proposers that happen to use
//! the same term value differ in `proposer` and an acceptor that promised
//! `(1, A)` rejects `(1, B)`'s Accept — restoring JE's one-winner-per-term
//! guarantee and closing the split-brain hole.
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
//! ## On-disk format (version 2)
//!
//! ```text
//!   bytes  field
//!   ---------------------------------------------------------
//!   0..4   magic            = b"PXST"
//!   4..6   version          = u16, currently 2
//!   6..14  promised_term    = u64
//!  14..22  accepted_term    = u64
//!  22..24  master_len       = u16  (0 if no accepted master yet)
//!  24..    master_bytes     = [u8; master_len]  (UTF-8)
//!  +2     proposer_len     = u16  (0 if no promise recorded yet) [v2+]
//!  ...    proposer_bytes   = [u8; proposer_len] (UTF-8)         [v2+]
//!  ...end  crc32            = u32 over all preceding bytes
//! ```
//!
//! Version 1 files (written by v7.11.0 / the v7.11.1 RC) have no
//! `proposer_len`/`proposer_bytes` fields; they are read back with
//! `promised_proposer = None` so a node upgraded in place keeps its durable
//! `(promised_term, accepted_term, accepted_master)` promise.  A `None`
//! promised proposer compares LESS than any named proposer, so the first
//! post-upgrade Propose at `promised_term` re-promises to the named
//! `(term, proposer)` without ever regressing the term — the F5/F31
//! restart-preserves-promise invariant is unchanged.  Writes always use the
//! v2 layout.

use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
// DST rep-sync coverage: route the acceptor's `Mutex` and atomics through the
// `noxu_util::dst_sync` seam so a shuttle gate can schedule concurrent
// `try_promise`/`try_accept` (the Paxos vote-tally state machine).  Under the
// default cfg these are transparent re-exports of `std::sync`, so production
// is byte-identical and shuttle is absent from the dependency graph.  The
// `flush_lock` serialises the load-modify-flush cycle across acceptor threads;
// under shuttle (in-memory mode) `flush_locked` is a no-op, so no real file I/O
// runs on the cooperative scheduler.
use noxu_util::dst_sync::Mutex;
use noxu_util::dst_sync::atomic::{AtomicU64, Ordering};

const MAGIC: &[u8; 4] = b"PXST";
/// Current on-disk version.  Version 1 omitted `promised_proposer`
/// (FID-ELECT-1 added it); v1 files are still read, see `load_from_disk`.
const VERSION: u16 = 2;
/// The smallest valid v1 file (no `promised_proposer`): header + crc.
const MIN_LEN: usize = 4 + 2 + 8 + 8 + 2 + 4;

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

/// A Paxos proposal number: `(term, proposer)`.
/// The Paxos proposal number is `(term, proposer)`.
///
/// JE-faithful analogue of `TimebasedProposalGenerator`'s
/// `ms_time | machineId | uniqueId`.  The `term` is the ascending component
/// (JE's `ms_time`); the `proposer` name is the per-node unique
/// disambiguator (JE's `machineId`).  The acceptor compares the whole pair
/// (`term` first, then `proposer` lexicographically) — exactly JE
/// `StringProposal.compareTo`, which compares the full concatenated string.
///
/// Two proposers that happen to use the same local `term` value differ in
/// `proposer`, so the pair is unique across proposers in a group (member
/// names are unique by `RepGroup` invariant), which is what makes the
/// acceptor's one-winner-per-term guarantee hold.  The pair is stored inline
/// in `PersistentAcceptorState` as `(promised_term, promised_proposer)`.
///
/// The sentinel proposer used by the backward-compatible public
/// `try_promise(t)` / `try_accept(t, master)` entry points (FID-ELECT-1).
///
/// Those two methods keep their original 1-/2-parameter signatures (a patch
/// release must not change the public API), so they have no proposer name to
/// key on.  They route through the proposer-aware core with this fixed
/// sentinel, which keeps the old "promise at term t, then accept value m at
/// term t" sequence working exactly as before.  Even two concurrent old-API
/// callers at the same term — both keyed on `(t, SENTINEL)` — are kept
/// split-brain-safe by the "no different value at an accepted term" guard in
/// `try_accept_proposal`.  The production election path never uses the
/// sentinel: it calls the `*_proposal` methods with the real proposer name.
pub(crate) const COMPAT_PROPOSER: &str = "";

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
    /// The proposer name of the current promise (FID-ELECT-1): the second
    /// component of the `(term, proposer)` proposal number.  `None` means no
    /// named proposer has been promised yet (fresh acceptor, or a v1 file
    /// upgraded in place).  Always read/written under `flush_lock` together
    /// with `promised_term` so the pair is consistent.
    promised_proposer: Mutex<Option<String>>,
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
            promised_proposer: Mutex::new(None),
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
            promised_proposer: Mutex::new(None),
            accepted_term: AtomicU64::new(0),
            accepted_master: Mutex::new(None),
            persist_path: Some(path.clone()),
            flush_lock: Mutex::new(()),
        };
        match load_from_disk(&path) {
            Ok(Some((p, pp, a, m))) => {
                st.promised_term.store(p, Ordering::SeqCst);
                *st.promised_proposer.lock().unwrap() = pp;
                st.accepted_term.store(a, Ordering::SeqCst);
                *st.accepted_master.lock().unwrap() = m;
                log::info!(
                    "Loaded acceptor.state from {}: promised={} (proposer={:?}), accepted={}, master={:?}",
                    path.display(),
                    p,
                    st.promised_proposer.lock().unwrap(),
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

    /// The proposer name of the current promise, if any (FID-ELECT-1).
    ///
    /// Together with [`Self::promised_term`] this is the full `(term,
    /// proposer)` proposal number the acceptor has promised.
    #[doc(hidden)]
    pub fn promised_proposer(&self) -> Option<String> {
        self.promised_proposer.lock().unwrap().clone()
    }

    /// Highest accepted term.
    pub fn accepted_term(&self) -> u64 {
        self.accepted_term.load(Ordering::SeqCst)
    }

    /// The master name accepted in `accepted_term`, if any.
    pub fn accepted_master(&self) -> Option<String> {
        self.accepted_master.lock().unwrap().clone()
    }

    /// Backward-compatible public promise entry point (FID-ELECT-1).
    ///
    /// Keeps its original single-`term` signature so the public API is
    /// unchanged in this patch release.  Routes through the proposer-aware
    /// core with the [`COMPAT_PROPOSER`] sentinel.  The production election
    /// path does NOT use this method; it calls [`Self::try_promise_proposal`]
    /// with the real proposer name.
    pub fn try_promise(&self, t: u64) -> bool {
        self.try_promise_proposal(t, COMPAT_PROPOSER)
    }

    /// Try to promise the proposal number `(t, proposer)`.
    ///
    /// Returns `true` (with the new state flushed to disk) iff the current
    /// promise is NOT strictly greater than `(t, proposer)`.  Otherwise the
    /// state is unchanged and the caller must reject the proposer.
    ///
    /// JE `Acceptor.process(Propose)` (`Acceptor.java:177-185`) rejects only
    /// when `promisedProposal.compareTo(propose) > 0` (strictly greater) and
    /// otherwise moves the promise to the incoming proposal.  We compare the
    /// whole `(term, proposer)` proposal number — JE compares the whole
    /// `ms_time|machineId|uniqueId` string — so two proposers that share a
    /// `term` value are disambiguated by `proposer` (FID-ELECT-1).  A `None`
    /// recorded proposer (fresh acceptor, or a v1 file upgraded in place)
    /// compares LESS than any named proposer at the same term, so the first
    /// post-upgrade Propose at `promised_term` re-promises without regressing
    /// the term.
    #[doc(hidden)]
    pub fn try_promise_proposal(&self, t: u64, proposer: &str) -> bool {
        // Coarse-lock under flush_lock so we serialize the
        // load-modify-flush cycle across acceptor threads.
        let _guard = self.flush_lock.lock().unwrap();
        let cur_term = self.promised_term.load(Ordering::SeqCst);
        let cur_proposer = self.promised_proposer.lock().unwrap().clone();
        // JE: reject iff promised proposal is STRICTLY greater than the
        // incoming one.  `(cur_term, cur_proposer)` vs `(t, Some(proposer))`;
        // `Option<String>` orders `None < Some(_)`, which is exactly the
        // v1-upgrade re-promise rule documented above.
        let incoming = (t, Some(proposer.to_string()));
        let promised = (cur_term, cur_proposer.clone());
        if promised > incoming {
            return false;
        }
        self.promised_term.store(t, Ordering::SeqCst);
        *self.promised_proposer.lock().unwrap() = Some(proposer.to_string());
        if let Err(e) = self.flush_locked() {
            // Roll back to avoid lying about persistence.
            self.promised_term.store(cur_term, Ordering::SeqCst);
            *self.promised_proposer.lock().unwrap() = cur_proposer;
            log::warn!(
                "acceptor.state: failed to persist promise(t={}, proposer={}): {}",
                t,
                proposer,
                e
            );
            return false;
        }
        true
    }

    /// Backward-compatible public accept entry point (FID-ELECT-1).
    ///
    /// Keeps its original `(term, master)` signature so the public API is
    /// unchanged in this patch release.  Routes through the proposer-aware
    /// core with the [`COMPAT_PROPOSER`] sentinel as the proposal number's
    /// proposer component and `master` as the value.  The "no different value
    /// at an accepted term" guard still protects two concurrent old-API
    /// callers at the same term.  The production election path does NOT use
    /// this method; it calls [`Self::try_accept_proposal`] with the real
    /// proposer name.
    pub fn try_accept(&self, t: u64, master: &str) -> bool {
        self.try_accept_proposal(t, COMPAT_PROPOSER, master)
    }

    /// Try to accept that `master` won at the proposal number `(t, proposer)`.
    ///
    /// Returns `true` (with the new state flushed to disk) iff the current
    /// promise EXACTLY equals `(t, proposer)`.
    ///
    /// JE `Acceptor.process(Accept)` (`Acceptor.java:210-215`) grants only
    /// when `promisedProposal.compareTo(accept.getProposal()) == 0` — the
    /// Accept must carry the exact proposal number that was promised in
    /// phase 1.  We compare the whole `(term, proposer)` number, so a second
    /// proposer `B` that re-promised the acceptor at `(1, B)` after it had
    /// promised `(1, A)` makes `A`'s later Accept at `(1, A)` fail the
    /// equality test.  The accepted VALUE is `master`, which (as in JE's
    /// `MasterValue`) is independent of the proposal number: a proposer may
    /// propose a different winner than itself, so `master` need not equal
    /// `proposer`.  Persists `(accepted_term, accepted_master)` on success.
    ///
    /// # Why the "already accepted a different value at this term" guard
    ///
    /// JE's acceptor, on a legitimate re-promise to a strictly-higher
    /// proposal, is allowed to accept a NEW value at the same decree, because
    /// JE's proposer adopts the highest previously-accepted value it saw in
    /// phase 1 (`Proposer.tallyPhase1Results` tracks `acceptedValue`), so the
    /// new value equals the old one — no split brain.  Noxu's `RankingProposer`
    /// chooses the phase-2 value from the Promise *suggestions* (VLSN
    /// ranking), NOT from the acceptor's previously-accepted value, so it does
    /// NOT perform that adoption.  Without value adoption the only safe rule
    /// for a value-less acceptor is: once a value is ACCEPTED at a term,
    /// refuse to accept a DIFFERENT value at that SAME term.  With the
    /// time-based proposal number (`TimebasedProposalGenerator`) a term is
    /// never reused by two proposers, so this guard never fires in normal
    /// operation; it is the belt-and-suspenders that makes a single acceptor
    /// split-brain-proof even if a term were somehow reused, and it is what
    /// makes the FID-ELECT-1 witness (two masters at one term) go green at
    /// the acceptor level.  It can only REJECT more than JE, never accept
    /// more, so it is sound.
    #[doc(hidden)]
    pub fn try_accept_proposal(
        &self,
        t: u64,
        proposer: &str,
        master: &str,
    ) -> bool {
        let _guard = self.flush_lock.lock().unwrap();
        let cur_term = self.promised_term.load(Ordering::SeqCst);
        let cur_proposer = self.promised_proposer.lock().unwrap().clone();
        // JE: accept iff the promised proposal equals the accept's proposal.
        if (cur_term, cur_proposer.as_deref()) != (t, Some(proposer)) {
            return false;
        }
        let prev_accepted_term = self.accepted_term.load(Ordering::SeqCst);
        let prev_accepted_master = self.accepted_master.lock().unwrap().clone();
        // FID-ELECT-1: refuse to accept a DIFFERENT value at a term already
        // accepted (no value adoption in Noxu's RankingProposer — see above).
        if prev_accepted_term == t
            && prev_accepted_master.as_deref() != Some(master)
        {
            return false;
        }
        // The promise already equals (t, proposer); record the acceptance.
        self.accepted_term.store(t, Ordering::SeqCst);
        *self.accepted_master.lock().unwrap() = Some(master.to_string());
        if let Err(e) = self.flush_locked() {
            self.accepted_term.store(prev_accepted_term, Ordering::SeqCst);
            *self.accepted_master.lock().unwrap() = prev_accepted_master;
            log::warn!(
                "acceptor.state: failed to persist accept(t={}, proposer={}, master={}): {}",
                t,
                proposer,
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
        let promised_proposer = self.promised_proposer.lock().unwrap().clone();
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
        // v2: promised_proposer (FID-ELECT-1). Length 0 = None.
        let p_bytes = promised_proposer.as_deref().unwrap_or("").as_bytes();
        let p_len: u16 = p_bytes.len().try_into().unwrap_or(u16::MAX);
        buf.extend_from_slice(&p_len.to_le_bytes());
        buf.extend_from_slice(&p_bytes[..p_len as usize]);

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

fn load_from_disk(
    path: &Path,
) -> Result<Option<(u64, Option<String>, u64, Option<String>)>> {
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
    // v1 (no promised_proposer) and v2 are both readable; v1 is upgraded in
    // place on the next flush (FID-ELECT-1).
    if version != 1 && version != VERSION {
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

    // v2: promised_proposer follows the master bytes.  v1 files have none
    // (`None`), which compares LESS than any named proposer so the first
    // post-upgrade Propose re-promises without regressing the term.
    let after_master = 24 + master_len;
    let promised_proposer = if version >= 2 {
        if bytes.len() < after_master + 2 + 4 {
            return Err(AcceptorPersistError::Truncated {
                expected: after_master + 2 + 4,
                got: bytes.len(),
            });
        }
        buf2.copy_from_slice(&bytes[after_master..after_master + 2]);
        let proposer_len = u16::from_le_bytes(buf2) as usize;
        let p_start = after_master + 2;
        if bytes.len() < p_start + proposer_len + 4 {
            return Err(AcceptorPersistError::Truncated {
                expected: p_start + proposer_len + 4,
                got: bytes.len(),
            });
        }
        if proposer_len == 0 {
            None
        } else {
            let s =
                std::str::from_utf8(&bytes[p_start..p_start + proposer_len])
                    .map_err(|_| AcceptorPersistError::BadMasterName)?;
            Some(s.to_string())
        }
    } else {
        None
    };

    Ok(Some((promised, promised_proposer, accepted, master)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn in_memory_state_round_trips() {
        let s = PersistentAcceptorState::in_memory();
        assert_eq!(s.snapshot(), (0, 0, None));
        assert!(s.try_promise_proposal(5, "a"));
        assert_eq!(s.promised_term(), 5);
        // Stale promise (lower term) rejected regardless of proposer.
        assert!(!s.try_promise_proposal(3, "a"));
        assert!(!s.try_promise_proposal(3, "z"));
        assert_eq!(s.promised_term(), 5);
        // Higher promise accepted.
        assert!(s.try_promise_proposal(7, "a"));
        assert_eq!(s.promised_term(), 7);
    }

    #[test]
    fn try_accept_persists() {
        let dir = TempDir::new().unwrap();
        let s = PersistentAcceptorState::load_or_default(dir.path());
        assert!(s.try_promise_proposal(5, "node-a"));
        assert!(s.try_accept_proposal(5, "node-a", "node-a"));
        assert_eq!(s.accepted_term(), 5);
        assert_eq!(s.accepted_master(), Some("node-a".into()));

        // Reload from disk: state must persist (incl. the promised proposer).
        let s2 = PersistentAcceptorState::load_or_default(dir.path());
        assert_eq!(s2.promised_term(), 5);
        assert_eq!(s2.promised_proposer(), Some("node-a".into()));
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
            assert!(s.try_promise_proposal(10, "a"));
        }
        // Simulate restart by re-loading.
        let s2 = PersistentAcceptorState::load_or_default(dir.path());
        assert_eq!(s2.promised_term(), 10);
        assert!(
            !s2.try_promise_proposal(7, "a"),
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
            assert!(s.try_promise_proposal(3, "x"));
            assert!(s.try_accept_proposal(3, "x", "x"));
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
        assert_eq!(s.promised_proposer(), None);
    }

    #[test]
    fn try_accept_below_promise_rejected() {
        let dir = TempDir::new().unwrap();
        let s = PersistentAcceptorState::load_or_default(dir.path());
        assert!(s.try_promise_proposal(5, "x"));
        assert!(!s.try_accept_proposal(3, "x", "x"));
        assert_eq!(s.accepted_term(), 0);
    }

    #[test]
    fn try_accept_requires_matching_promise() {
        // JE Acceptor.process(Accept) rejects unless the Accept's proposal
        // equals the promised proposal. An accept at a term we never promised
        // (here: no prior promise -> promised == 0) must be rejected.
        let dir = TempDir::new().unwrap();
        let s = PersistentAcceptorState::load_or_default(dir.path());
        assert!(
            !s.try_accept_proposal(7, "winner", "winner"),
            "accept without matching promise"
        );
        assert_eq!(s.accepted_term(), 0);
        // After promising 7, accept at 7 succeeds.
        assert!(s.try_promise_proposal(7, "winner"));
        assert!(s.try_accept_proposal(7, "winner", "winner"));
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
        assert!(s.try_promise_proposal(5, "legit"));
        assert!(
            !s.try_accept_proposal(6, "intruder", "intruder"),
            "D1: accept at term > promised must be rejected (split-brain)"
        );
        assert_eq!(s.accepted_term(), 0, "no acceptance recorded");
        // The promised term is unchanged, so the legitimate proposer at 5 can
        // still complete phase 2.
        assert_eq!(s.promised_term(), 5);
        assert!(s.try_accept_proposal(5, "legit", "legit"));
        assert_eq!(s.accepted_term(), 5);
    }

    // ---------------------------------------------------------------------
    // FID-ELECT-1 regression tests: the proposal number is `(term, proposer)`.
    // ---------------------------------------------------------------------

    #[test]
    fn two_proposers_same_term_only_one_accepted() {
        // The split-brain hole: two proposers A and B both drive term 1 on
        // one acceptor.  JE rejects the second via the unique proposal
        // number; we must too.
        let s = PersistentAcceptorState::in_memory();
        // A promises + accepts at (1, A).
        assert!(s.try_promise_proposal(1, "node-A"));
        assert!(s.try_accept_proposal(1, "node-A", "node-A"));
        assert_eq!(s.accepted_master().as_deref(), Some("node-A"));
        // B re-promises the acceptor at (1, B) ((1,B) > (1,A), not strictly
        // less, so JE's Propose rule re-promises).
        assert!(s.try_promise_proposal(1, "node-B"));
        assert_eq!(s.promised_proposer().as_deref(), Some("node-B"));
        // A's phase-2 Accept now carries the stale proposal (1, A) != the
        // promised (1, B): rejected.
        assert!(
            !s.try_accept_proposal(1, "node-A", "node-A"),
            "A's stale Accept at (1,A) must be rejected after B re-promised"
        );
        // The acceptance recorded for A is untouched.
        assert_eq!(s.accepted_master().as_deref(), Some("node-A"));
        assert_eq!(s.accepted_term(), 1);
    }

    #[test]
    fn lower_proposer_cannot_overwrite_higher_promise_same_term() {
        // Acceptor promised (1, node-B); a Propose (1, node-A) is strictly
        // less, so it is rejected (JE Propose rule) and B's promise stands.
        let s = PersistentAcceptorState::in_memory();
        assert!(s.try_promise_proposal(1, "node-B"));
        assert!(
            !s.try_promise_proposal(1, "node-A"),
            "(1,A) < (1,B) -> Propose rejected"
        );
        assert_eq!(s.promised_proposer().as_deref(), Some("node-B"));
        // Only B can be accepted at term 1.
        assert!(!s.try_accept_proposal(1, "node-A", "node-A"));
        assert!(s.try_accept_proposal(1, "node-B", "node-B"));
    }

    #[test]
    fn accept_value_may_differ_from_proposer() {
        // JE's MasterValue is independent of the proposal number: a proposer
        // may propose a different winner than itself.  The acceptor keys on
        // (term, proposer) but records the master value as given.
        let s = PersistentAcceptorState::in_memory();
        assert!(s.try_promise_proposal(3, "proposer-X"));
        assert!(s.try_accept_proposal(3, "proposer-X", "winner-Y"));
        assert_eq!(s.accepted_master().as_deref(), Some("winner-Y"));
    }

    #[test]
    fn v1_file_is_read_and_upgraded_in_place() {
        // A v7.11.0 / RC acceptor.state (version 1, no promised_proposer)
        // must load with promised_proposer=None and keep its durable promise;
        // the first post-upgrade Propose at the promised term re-promises to
        // the named proposer without regressing the term.
        let dir = TempDir::new().unwrap();
        let path = state_path(dir.path());
        // Hand-write a version-1 file: promised=9, accepted=9, master="old".
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&1u16.to_le_bytes()); // version 1
        buf.extend_from_slice(&9u64.to_le_bytes()); // promised_term
        buf.extend_from_slice(&9u64.to_le_bytes()); // accepted_term
        let m = b"old";
        buf.extend_from_slice(&(m.len() as u16).to_le_bytes());
        buf.extend_from_slice(m);
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, &buf).unwrap();

        let s = PersistentAcceptorState::load_or_default(dir.path());
        assert_eq!(s.promised_term(), 9);
        assert_eq!(s.promised_proposer(), None, "v1 has no proposer");
        assert_eq!(s.accepted_term(), 9);
        assert_eq!(s.accepted_master().as_deref(), Some("old"));
        // A stale term is still rejected (promise preserved).
        assert!(!s.try_promise_proposal(8, "n"));
        // The first Propose at the promised term re-promises (None < Some).
        assert!(s.try_promise_proposal(9, "n"));
        assert_eq!(s.promised_proposer().as_deref(), Some("n"));
        assert_eq!(s.promised_term(), 9, "term must not regress on upgrade");

        // Reload: it is now a v2 file with the proposer persisted.
        let s2 = PersistentAcceptorState::load_or_default(dir.path());
        assert_eq!(s2.promised_proposer().as_deref(), Some("n"));
    }
}
