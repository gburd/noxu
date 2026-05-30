//! Sequence handle.
//!
//!
//! A `Sequence` is an auto-incrementing (or decrementing) counter backed by
//! a single key-value record in a `Database`.  The persistent record stores
//! the *next* batch boundary so that multiple handles can share the same
//! database key without requiring coordination on every call.
//!
//! ## Record format (ported exactly from the)
//!
//! ```text
//! byte 0   : version (always 1)
//! byte 1   : flags
//!              bit 0 (FLAG_INCR) — sequence increments
//!              bit 1 (FLAG_WRAP) — wrap-around allowed
//!              bit 2 (FLAG_OVER) — overflow has occurred
//! bytes 2+ : range_min  (big-endian i64, 8 bytes)
//! bytes 10+: range_max  (big-endian i64, 8 bytes)
//! bytes 18+: stored_value (big-endian i64, 8 bytes)
//! ```
//!
//! Total: 26 bytes.  uses packed-long encoding; we use fixed big-endian
//! i64 for simplicity (compatible with the noxu-persist pattern).

use crate::db::database::Database;
use crate::db::database_entry::DatabaseEntry;
use crate::db::error::{NoxuError, Result};
use crate::db::operation_status::OperationStatus;
use crate::db::sequence_config::SequenceConfig;
use crate::db::sequence_stats::SequenceStats;
use crate::db::transaction::Transaction;
use std::sync::Mutex;

// ── record flags ──────────────────────────────────────────────────────────────
const FLAG_INCR: u8 = 0x1;
const FLAG_WRAP: u8 = 0x2;
const FLAG_OVER: u8 = 0x4;

/// Current on-disk record version.
const CURRENT_VERSION: u8 = 1;

/// Fixed size of the serialised sequence record.
const RECORD_SIZE: usize = 26; // 1 + 1 + 8 + 8 + 8

// ── mutable cache state, protected by a Mutex ────────────────────────────────
struct CacheState {
    /// Persistent fields (mirrored from the DB record).
    wrap_allowed: bool,
    increment: bool,
    overflow: bool,
    range_min: i64,
    range_max: i64,
    /// The value that was last written to the database (the batch boundary).
    stored_value: i64,

    /// Next value to hand out from the local cache.
    cache_value: i64,
    /// Last value reserved in the local cache (inclusive).
    cache_last: i64,

    /// Whether the cache has been filled at least once.
    /// When false the first `get` always triggers a DB refill, regardless of
    /// the `cache_value`/`cache_last` sentinel values (which can be ambiguous
    /// for sequences at the i64 extremes).
    cache_initialized: bool,

    /// Statistics.
    n_gets: u64,
    n_cache_hits: u64,
}

/// A handle for manipulating a sequence record stored in a `Database`.
///
///
///
/// Multiple threads may share a single `Sequence` handle safely; all cache
/// manipulation is protected by an internal `Mutex`.  For higher throughput
/// open separate handles to the same database key.
///
/// # Example
///
/// ```ignore
/// use crate::db::{SequenceConfig, DatabaseEntry};
///
/// let config = SequenceConfig::new().with_allow_create(true);
/// let key = DatabaseEntry::from_bytes(b"my_counter");
/// let seq = db.open_sequence(&key, config).unwrap();
///
/// let v1 = seq.get(None, 1).unwrap();
/// let v2 = seq.get(None, 1).unwrap();
/// assert_eq!(v2, v1 + 1);
/// ```
pub struct Sequence<'db> {
    /// The database backing the sequence.
    db: &'db Database,
    /// The key under which the sequence record is stored (owned copy).
    key: Vec<u8>,
    /// Cache size chosen at open time.
    cache_size: i32,
    /// All mutable state is behind a Mutex so `get` can take `&self`.
    state: Mutex<CacheState>,
}

impl<'db> Sequence<'db> {
    /// Opens (and optionally creates) a sequence.
    ///
    /// Called by `Database::open_sequence`.
    pub(crate) fn open(
        db: &'db Database,
        key: &DatabaseEntry,
        config: SequenceConfig,
    ) -> Result<Self> {
        // ── validate config ───────────────────────────────────────────────
        if config.range_min >= config.range_max {
            return Err(NoxuError::IllegalArgument(
                "Minimum sequence value must be less than the maximum".into(),
            ));
        }
        if config.initial_value > config.range_max
            || config.initial_value < config.range_min
        {
            return Err(NoxuError::IllegalArgument(
                "Initial sequence value is out of range".into(),
            ));
        }
        // cache_size == 0 means no caching; any positive cache_size must fit
        // within the range.  Use saturating_sub to avoid overflow when the
        // range spans the full i64 range.
        if config.cache_size > 0
            && config.range_max.saturating_sub(config.range_min)
                < config.cache_size as i64
        {
            return Err(NoxuError::IllegalArgument(
                "The cache size is larger than the sequence range".into(),
            ));
        }

        let key_bytes = key.get_data().unwrap_or(&[]).to_vec();
        let key_entry = DatabaseEntry::from_bytes(&key_bytes);

        // ── try to read an existing record ────────────────────────────────
        let mut data_entry = DatabaseEntry::new();
        let found = db.get(None, &key_entry, &mut data_entry)?
            == OperationStatus::Success;

        if found {
            if config.allow_create && config.exclusive_create {
                return Err(NoxuError::IllegalArgument(
                    "ExclusiveCreate=true and the sequence record already exists."
                        .into(),
                ));
            }
            // Decode the existing record.
            let rec = Self::decode_record(data_entry.data())?;
            let cache_size = config.cache_size;
            let (cache_value, cache_last) = Self::init_cache(&rec, cache_size);
            return Ok(Sequence {
                db,
                key: key_bytes,
                cache_size,
                state: Mutex::new(CacheState {
                    wrap_allowed: rec.wrap_allowed,
                    increment: rec.increment,
                    overflow: rec.overflow,
                    range_min: rec.range_min,
                    range_max: rec.range_max,
                    stored_value: rec.stored_value,
                    cache_value,
                    cache_last,
                    cache_initialized: false,
                    n_gets: 0,
                    n_cache_hits: 0,
                }),
            });
        }

        // ── record not found ──────────────────────────────────────────────
        if !config.allow_create {
            return Err(NoxuError::NotFound);
        }

        // Create a new record from the config.
        let increment = !config.decrement;
        let stored_value = config.initial_value;
        let rec = PersistedSeq {
            wrap_allowed: config.wrap,
            increment,
            overflow: false,
            range_min: config.range_min,
            range_max: config.range_max,
            stored_value,
        };
        let encoded = Self::encode_record(&rec);
        let data_entry = DatabaseEntry::from_bytes(&encoded);

        // putNoOverwrite so a concurrent creator wins and we just read theirs.
        let status = db.put_no_overwrite(None, &key_entry, &data_entry)?;
        let final_rec = if status == OperationStatus::KeyExists {
            // Lost the race — read the winner's record.
            let mut d = DatabaseEntry::new();
            if db.get(None, &key_entry, &mut d)? != OperationStatus::Success {
                return Err(NoxuError::IllegalArgument(
                    "Sequence record removed during open_sequence.".into(),
                ));
            }
            Self::decode_record(d.data())?
        } else {
            rec
        };

        let cache_size = config.cache_size;
        let (cache_value, cache_last) =
            Self::init_cache(&final_rec, cache_size);
        Ok(Sequence {
            db,
            key: key_bytes,
            cache_size,
            state: Mutex::new(CacheState {
                wrap_allowed: final_rec.wrap_allowed,
                increment: final_rec.increment,
                overflow: final_rec.overflow,
                range_min: final_rec.range_min,
                range_max: final_rec.range_max,
                stored_value: final_rec.stored_value,
                cache_value,
                cache_last,
                cache_initialized: false,
                n_gets: 0,
                n_cache_hits: 0,
            }),
        })
    }

    // ── public API ────────────────────────────────────────────────────────────

    /// Returns the next available element in the sequence and advances by
    /// `delta`.
    ///
    ///
    ///
    /// `delta` must be > 0 and must fit within the configured range.
    ///
    /// The `txn` parameter, if provided, is used for the cache-refill database
    /// write, making it participate in the caller's transaction.
    /// `Sequence.get(Transaction txn, int delta)`.
    pub fn get(&self, txn: Option<&Transaction>, delta: i32) -> Result<i64> {
        if delta <= 0 {
            return Err(NoxuError::IllegalArgument(
                "Sequence delta must be greater than zero".into(),
            ));
        }

        let mut state = self.state.lock().unwrap();

        // "if (rangeMin > rangeMax - delta)" — use saturating to avoid
        // overflow when range_max is near i64::MIN.
        if state.range_min > state.range_max.saturating_sub(delta as i64) {
            return Err(NoxuError::IllegalArgument(
                "Sequence delta is larger than the range".into(),
            ));
        }

        // ── check cache availability ──────────────────────────────────────
        // If the cache has never been filled we always need a refill, even if
        // the sentinel cache_last/cache_value happen to look non-empty (this
        // can occur for sequences whose initial_value is at an i64 extreme).
        let cache_available = if !state.cache_initialized {
            0
        } else if state.increment {
            // cache_last < cache_value when empty: result is negative or zero,
            // so need_refill will be true (delta > cache_available).
            (state.cache_last - state.cache_value) + 1
        } else {
            (state.cache_value - state.cache_last) + 1
        };
        let need_refill = delta as i64 > cache_available;

        // Check overflow unconditionally: when checked_add overflows i64 we
        // set overflow=true but still serve the last batch from cache.  On
        // the very next call (cache may still appear non-empty) we must error.
        if state.overflow {
            return Err(NoxuError::OperationNotAllowed(format!(
                "Sequence overflow at {}",
                state.stored_value
            )));
        }

        if need_refill {
            // ── refill the cache from the database ────────────────────────

            let adjust = if delta > self.cache_size {
                delta as i64
            } else {
                self.cache_size as i64
            };

            // How many values remain, inclusive of stored_value itself?
            // stored_value is the first un-allocated value in the sequence, so
            // the count of allocatable values is:
            //   increment:  range_max - stored_value + 1
            //   decrement:  stored_value - range_min + 1
            // A negative result means stored_value has moved past the boundary
            // (overflow has occurred).
            //
            // Potential overflow: range_max + 1 could overflow when
            // range_max == i64::MAX, and similarly for range_min - 1 when
            // range_min == i64::MIN.  Use checked_add/sub to cap at 0 in those
            // cases — if stored_value == i64::MAX the +1 to range_max would
            // overflow, but stored_value must have already been clamped by the
            // checked_add above (overflow = true), so the early-return guard
            // at the top of the block already handles that path.
            // Compute how many values remain at stored_value (inclusive).
            //
            // For increment: stored_value must be ≤ range_max to have any.
            //   avail = range_max - stored_value + 1  (clamp to i64::MAX)
            // For decrement: stored_value must be ≥ range_min to have any.
            //   avail = stored_value - range_min + 1  (clamp to i64::MAX)
            //
            // Overflow edge cases handled explicitly:
            //   - If stored_value > range_max (increment) → exhausted → avail=0.
            //   - If stored_value < range_min (decrement) → exhausted → avail=0.
            //   - The subtraction itself can overflow when the range spans the
            //     full i64 extent; use saturating to cap at i64::MAX.
            let avail: i64 = if state.increment {
                if state.stored_value > state.range_max {
                    0
                } else {
                    // range_max - stored_value ≥ 0 and fits in i64 because
                    // both are within i64; +1 may overflow only if the diff
                    // is already i64::MAX (range is the full i64 span).
                    state
                        .range_max
                        .saturating_sub(state.stored_value)
                        .saturating_add(1)
                }
            } else {
                if state.stored_value < state.range_min {
                    0
                } else {
                    state
                        .stored_value
                        .saturating_sub(state.range_min)
                        .saturating_add(1)
                }
            };

            let actual_adjust: i64 = if avail < adjust {
                if avail < delta as i64 {
                    if state.wrap_allowed {
                        // Wrap: reset stored_value to the opposite end.
                        state.stored_value = if state.increment {
                            state.range_min
                        } else {
                            state.range_max
                        };
                        // After wrapping, stored_value is at the opposite end.
                        // Compute how many values are available in the full range.
                        let full_avail = if state.increment {
                            // range_max - range_min + 1 = full range size
                            // Approximate with checked arithmetic; for extremes
                            // the range must be small (validated at open time).
                            state
                                .range_max
                                .saturating_sub(state.stored_value)
                                .saturating_add(1)
                        } else {
                            state
                                .stored_value
                                .saturating_sub(state.range_min)
                                .saturating_add(1)
                        };
                        full_avail.min(adjust)
                    } else {
                        // Range exhausted and wrap not allowed — error now,
                        // matching SequenceImpl which throws immediately.
                        return Err(NoxuError::OperationNotAllowed(format!(
                            "Sequence overflow at {}",
                            state.stored_value
                        )));
                    }
                } else {
                    // Not enough for a full cache batch but enough for delta.
                    avail
                }
            } else {
                adjust
            };

            // Apply the adjustment.
            // Record the batch start (= old stored_value) before advancing.
            // For increment: the batch covers [batch_start, batch_start + actual_adjust - 1].
            // For decrement: the batch covers [batch_start - actual_adjust + 1, batch_start].
            let batch_start = state.stored_value;
            let signed_adjust =
                if state.increment { actual_adjust } else { -actual_adjust };
            // Use checked add: if the new stored_value would overflow i64 (only
            // possible when range_max == i64::MAX or range_min == i64::MIN), we
            // mark overflow immediately so the NEXT get returns an error.
            match state.stored_value.checked_add(signed_adjust) {
                Some(new_sv) => state.stored_value = new_sv,
                None => {
                    // Overflow past i64 bounds — mark it and use a sentinel.
                    state.overflow = true;
                    state.stored_value =
                        if state.increment { i64::MAX } else { i64::MIN };
                }
            }

            // Persist the new stored_value.
            let rec = PersistedSeq {
                wrap_allowed: state.wrap_allowed,
                increment: state.increment,
                overflow: state.overflow,
                range_min: state.range_min,
                range_max: state.range_max,
                stored_value: state.stored_value,
            };
            let encoded = Self::encode_record(&rec);
            let key_entry = DatabaseEntry::from_bytes(&self.key);
            let data_entry = DatabaseEntry::from_bytes(&encoded);
            self.db.put(txn, &key_entry, &data_entry)?;

            // Update the local cache window using batch_start (pre-advance).
            // cache_value = batch_start (first value to hand out)
            // cache_last  = batch_start + actual_adjust - 1 (incr, inclusive end)
            //             = batch_start - actual_adjust + 1 (decr, inclusive end)
            // Use saturating arithmetic to avoid overflow at i64 extremes.
            state.cache_value = batch_start;
            state.cache_last = if state.increment {
                batch_start.saturating_add(actual_adjust - 1)
            } else {
                batch_start.saturating_sub(actual_adjust - 1)
            };
            state.cache_initialized = true;
        }

        // ── serve from cache ──────────────────────────────────────────────
        let ret_val = state.cache_value;
        if state.increment {
            state.cache_value = state.cache_value.saturating_add(delta as i64);
        } else {
            state.cache_value = state.cache_value.saturating_sub(delta as i64);
        }

        state.n_gets += 1;
        if !need_refill {
            state.n_cache_hits += 1;
        }

        Ok(ret_val)
    }

    /// Returns a snapshot of statistics for this handle.
    ///
    ///
    pub fn get_stats(&self) -> SequenceStats {
        let state = self.state.lock().unwrap();
        SequenceStats {
            n_gets: state.n_gets,
            n_cache_hits: state.n_cache_hits,
            current_value: state.stored_value,
            cache_value: state.cache_value,
            cache_last: state.cache_last,
            range_min: state.range_min,
            range_max: state.range_max,
            cache_size: self.cache_size,
        }
    }

    /// Closes the sequence handle.
    ///
    /// After calling this method the handle must not be used again.  Unused
    /// cached values are discarded.
    ///
    ///
    pub fn close(&self) -> Result<()> {
        // Nothing to flush; the DB record already holds the batch boundary.
        Ok(())
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /// Initialise `(cache_value, cache_last)` from a just-read record so that
    /// the cache appears empty and the first `get` call triggers a refill.
    ///
    /// does: `cacheLast = increment ? (storedValue - 1) : (storedValue + 1)`
    fn init_cache(rec: &PersistedSeq, _cache_size: i32) -> (i64, i64) {
        let cache_value = rec.stored_value;
        // cache_last is set so the cache appears empty on first get:
        //   increment: cache_last = stored_value - 1  (below cache_value)
        //   decrement: cache_last = stored_value + 1  (above cache_value)
        // Use saturating arithmetic to avoid overflow at i64 extremes.
        let cache_last = if rec.increment {
            rec.stored_value.saturating_sub(1)
        } else {
            rec.stored_value.saturating_add(1)
        };
        (cache_value, cache_last)
    }

    /// Encodes persistent sequence fields into a 26-byte array.
    fn encode_record(rec: &PersistedSeq) -> [u8; RECORD_SIZE] {
        let mut buf = [0u8; RECORD_SIZE];
        buf[0] = CURRENT_VERSION;
        let mut flags: u8 = 0;
        if rec.increment {
            flags |= FLAG_INCR;
        }
        if rec.wrap_allowed {
            flags |= FLAG_WRAP;
        }
        if rec.overflow {
            flags |= FLAG_OVER;
        }
        buf[1] = flags;
        buf[2..10].copy_from_slice(&rec.range_min.to_be_bytes());
        buf[10..18].copy_from_slice(&rec.range_max.to_be_bytes());
        buf[18..26].copy_from_slice(&rec.stored_value.to_be_bytes());
        buf
    }

    /// Decodes a 26-byte array into persistent sequence fields.
    fn decode_record(data: &[u8]) -> Result<PersistedSeq> {
        if data.len() < RECORD_SIZE {
            return Err(NoxuError::IllegalArgument(format!(
                "Sequence record too short: {} bytes (expected {})",
                data.len(),
                RECORD_SIZE
            )));
        }
        // byte 0: version (ignored for forward compatibility)
        let flags = data[1];
        let increment = (flags & FLAG_INCR) != 0;
        let wrap_allowed = (flags & FLAG_WRAP) != 0;
        let overflow = (flags & FLAG_OVER) != 0;
        let range_min = i64::from_be_bytes(data[2..10].try_into().unwrap());
        let range_max = i64::from_be_bytes(data[10..18].try_into().unwrap());
        let stored_value = i64::from_be_bytes(data[18..26].try_into().unwrap());
        Ok(PersistedSeq {
            wrap_allowed,
            increment,
            overflow,
            range_min,
            range_max,
            stored_value,
        })
    }
}

// ── internal helper struct ────────────────────────────────────────────────────

/// The subset of sequence state that is persisted to the database.
struct PersistedSeq {
    wrap_allowed: bool,
    increment: bool,
    overflow: bool,
    range_min: i64,
    range_max: i64,
    stored_value: i64,
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::database_config::DatabaseConfig;
    use crate::db::environment::Environment;
    use crate::db::environment_config::EnvironmentConfig;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Environment) {
        let dir = TempDir::new().unwrap();
        let env = Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(false),
        )
        .unwrap();
        (dir, env)
    }

    fn open_db(env: &Environment) -> crate::db::database::Database {
        env.open_database(
            None,
            "seqdb",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap()
    }

    #[test]
    fn test_sequence_create_and_get() {
        let (_dir, env) = setup();
        let db = open_db(&env);

        let key = DatabaseEntry::from_bytes(b"counter");
        let config = SequenceConfig::new().with_allow_create(true);
        let seq = db.open_sequence(&key, config).unwrap();

        let v0 = seq.get(None, 1).unwrap();
        let v1 = seq.get(None, 1).unwrap();
        let v2 = seq.get(None, 1).unwrap();

        // Must be monotonically increasing.
        assert!(v1 > v0, "v1={v1} should be > v0={v0}");
        assert!(v2 > v1, "v2={v2} should be > v1={v1}");
    }

    #[test]
    fn test_sequence_five_values_monotonic() {
        let (_dir, env) = setup();
        let db = open_db(&env);

        let key = DatabaseEntry::from_bytes(b"mono");
        let config =
            SequenceConfig::new().with_allow_create(true).with_cache_size(5);
        let seq = db.open_sequence(&key, config).unwrap();

        let mut prev = seq.get(None, 1).unwrap();
        for _ in 0..4 {
            let next = seq.get(None, 1).unwrap();
            assert!(next > prev, "sequence not monotonic: {next} <= {prev}");
            prev = next;
        }
    }

    #[test]
    fn test_sequence_delta_greater_than_one() {
        let (_dir, env) = setup();
        let db = open_db(&env);

        let key = DatabaseEntry::from_bytes(b"delta");
        let config = SequenceConfig::new()
            .with_allow_create(true)
            .with_initial_value(0)
            .with_cache_size(0);
        let seq = db.open_sequence(&key, config).unwrap();

        let v0 = seq.get(None, 3).unwrap();
        let v1 = seq.get(None, 3).unwrap();
        assert_eq!(v1 - v0, 3);
    }

    #[test]
    fn test_sequence_stats() {
        let (_dir, env) = setup();
        let db = open_db(&env);

        let key = DatabaseEntry::from_bytes(b"stats");
        let config =
            SequenceConfig::new().with_allow_create(true).with_cache_size(10);
        let seq = db.open_sequence(&key, config).unwrap();

        seq.get(None, 1).unwrap();
        seq.get(None, 1).unwrap();
        seq.get(None, 1).unwrap();

        let stats = seq.get_stats();
        assert_eq!(stats.n_gets, 3);
        assert_eq!(stats.range_min, i64::MIN);
        assert_eq!(stats.range_max, i64::MAX);
    }

    #[test]
    fn test_sequence_wrap() {
        let (_dir, env) = setup();
        let db = open_db(&env);

        let key = DatabaseEntry::from_bytes(b"wrap");
        // Small range [0, 4] with wrap enabled.
        let config = SequenceConfig::new()
            .with_allow_create(true)
            .with_range(0, 4)
            .with_wrap(true)
            .with_initial_value(0)
            .with_cache_size(0);
        let seq = db.open_sequence(&key, config).unwrap();

        // Consume all 5 values (0, 1, 2, 3, 4).
        let mut values: Vec<i64> =
            (0..5).map(|_| seq.get(None, 1).unwrap()).collect();

        // Next call should wrap to 0 again.
        let after_wrap = seq.get(None, 1).unwrap();
        values.push(after_wrap);

        // The wrapped value must be in [0, 4].
        assert!(
            (0..=4).contains(&after_wrap),
            "wrapped value {after_wrap} not in [0, 4]"
        );
    }

    #[test]
    fn test_sequence_no_overwrite_on_existing() {
        let (_dir, env) = setup();
        let db = open_db(&env);

        let key = DatabaseEntry::from_bytes(b"exist");
        let config = SequenceConfig::new().with_allow_create(true);
        let seq1 = db.open_sequence(&key, config.clone()).unwrap();
        let v1 = seq1.get(None, 1).unwrap();

        // Second open should succeed and continue from where seq1 left off.
        let seq2 = db.open_sequence(&key, config).unwrap();
        let v2 = seq2.get(None, 1).unwrap();
        assert!(v2 >= v1, "seq2 should continue from seq1: v2={v2} v1={v1}");
    }

    #[test]
    fn test_sequence_exclusive_create_fails_on_existing() {
        let (_dir, env) = setup();
        let db = open_db(&env);

        let key = DatabaseEntry::from_bytes(b"excl");
        db.open_sequence(&key, SequenceConfig::new().with_allow_create(true))
            .unwrap();

        let result = db.open_sequence(
            &key,
            SequenceConfig::new()
                .with_allow_create(true)
                .with_exclusive_create(true),
        );
        assert!(
            result.is_err(),
            "exclusive_create should fail when record exists"
        );
    }

    #[test]
    fn test_sequence_not_found_without_allow_create() {
        let (_dir, env) = setup();
        let db = open_db(&env);

        let key = DatabaseEntry::from_bytes(b"missing");
        let result = db.open_sequence(
            &key,
            SequenceConfig::new().with_allow_create(false),
        );
        assert!(result.is_err(), "should fail without allow_create");
    }

    #[test]
    fn test_sequence_decrement() {
        let (_dir, env) = setup();
        let db = open_db(&env);

        let key = DatabaseEntry::from_bytes(b"decr");
        let config = SequenceConfig::new()
            .with_allow_create(true)
            .with_decrement(true)
            .with_initial_value(100)
            .with_cache_size(0);
        let seq = db.open_sequence(&key, config).unwrap();

        let v0 = seq.get(None, 1).unwrap();
        let v1 = seq.get(None, 1).unwrap();
        let v2 = seq.get(None, 1).unwrap();
        assert!(v1 < v0, "decrement: v1={v1} should be < v0={v0}");
        assert!(v2 < v1, "decrement: v2={v2} should be < v1={v1}");
    }

    #[test]
    fn test_sequence_close() {
        let (_dir, env) = setup();
        let db = open_db(&env);

        let key = DatabaseEntry::from_bytes(b"close");
        let seq = db
            .open_sequence(&key, SequenceConfig::new().with_allow_create(true))
            .unwrap();
        assert!(seq.close().is_ok());
    }
}
