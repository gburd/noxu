//! Checkpoint end log entry.
//!

use crate::error::Result;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use noxu_util::Lsn;
use std::io::Cursor;
use std::time::SystemTime;

/// Encapsulates the information needed by a checkpoint end log entry.
///
/// This is written when a checkpoint completes successfully. It contains all the
/// metadata needed to perform recovery from this checkpoint, including LSNs of
/// critical tree nodes and transaction state, as well as the last allocated IDs
/// for various database objects.
///
///
#[derive(Debug, Clone)]
pub struct CheckpointEnd {
    /// Checkpoint ID - matches the corresponding CheckpointStart.
    id: u64,
    /// Who invoked this checkpoint.
    invoker: String,
    /// Time when the checkpoint ended.
    end_time: SystemTime,
    /// LSN of the checkpoint start entry.
    checkpoint_start_lsn: Lsn,
    /// Root LSN of the mapping tree (None if no root).
    root_lsn: Option<Lsn>,
    /// LSN of the first active transaction at checkpoint time.
    first_active_lsn: Lsn,
    /// Last allocated local node ID.
    last_local_node_id: u64,
    /// Last allocated replicated node ID.
    last_replicated_node_id: i64,
    /// Last allocated local database ID.
    last_local_db_id: u64,
    /// Last allocated replicated database ID.
    last_replicated_db_id: i64,
    /// Last allocated local transaction ID.
    last_local_txn_id: u64,
    /// Last allocated replicated transaction ID.
    last_replicated_txn_id: i64,
    /// True if there were cleaned files to delete after this checkpoint.
    cleaned_files_to_delete: bool,
    /// Per-database tree root LSNs captured at checkpoint time (v2 field).
    ///
    /// Each entry is `(db_id, root_lsn)` where `root_lsn` is the LSN the
    /// database's tree root IN/BIN was last logged at as of this checkpoint
    /// (`Tree::get_root_lsn`, the Noxu equivalent of JE `Tree.getRootLsn()`
    /// = `root.getLsn()`).  Recovery seeds each reconstructed tree from these
    /// (`Tree::set_root_lsn`) so it can lazily fetch pre-checkpoint BINs
    /// (`fetchTarget`) instead of replaying every pre-checkpoint LN.
    ///
    /// # On-disk backward compatibility
    ///
    /// This field is serialized AFTER the original (v1) trailer, guarded by a
    /// trailing presence marker (see `write_to_log` / `read_from_log`).  A v1
    /// `CheckpointEnd` written before this field existed has no trailing
    /// bytes, so `read_from_log` yields an EMPTY `per_db_roots`.  Recovery
    /// treats an empty map as "no seeded roots" and falls back to full LN
    /// redo — the exact pre-existing (safe) behaviour.  Old readers reading a
    /// new entry stop after the v1 trailer and ignore the extra bytes.
    per_db_roots: Vec<(u64, Lsn)>,
}

impl CheckpointEnd {
    /// Creates a new checkpoint end entry.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        id: u64,
        invoker: &str,
        checkpoint_start_lsn: Lsn,
        root_lsn: Option<Lsn>,
        first_active_lsn: Lsn,
        last_local_node_id: u64,
        last_replicated_node_id: i64,
        last_local_db_id: u64,
        last_replicated_db_id: i64,
        last_local_txn_id: u64,
        last_replicated_txn_id: i64,
        cleaned_files_to_delete: bool,
    ) -> Self {
        Self {
            id,
            invoker: invoker.to_string(),
            end_time: SystemTime::now(),
            checkpoint_start_lsn,
            root_lsn,
            first_active_lsn,
            last_local_node_id,
            last_replicated_node_id,
            last_local_db_id,
            last_replicated_db_id,
            last_local_txn_id,
            last_replicated_txn_id,
            cleaned_files_to_delete,
            per_db_roots: Vec::new(),
        }
    }

    /// Creates a checkpoint end entry with a specific timestamp.
    ///
    /// Used primarily for testing and deserialization.
    #[expect(clippy::too_many_arguments)]
    pub fn with_time(
        id: u64,
        invoker: &str,
        end_time: SystemTime,
        checkpoint_start_lsn: Lsn,
        root_lsn: Option<Lsn>,
        first_active_lsn: Lsn,
        last_local_node_id: u64,
        last_replicated_node_id: i64,
        last_local_db_id: u64,
        last_replicated_db_id: i64,
        last_local_txn_id: u64,
        last_replicated_txn_id: i64,
        cleaned_files_to_delete: bool,
    ) -> Self {
        Self {
            id,
            invoker: invoker.to_string(),
            end_time,
            checkpoint_start_lsn,
            root_lsn,
            first_active_lsn,
            last_local_node_id,
            last_replicated_node_id,
            last_local_db_id,
            last_replicated_db_id,
            last_local_txn_id,
            last_replicated_txn_id,
            cleaned_files_to_delete,
            per_db_roots: Vec::new(),
        }
    }

    // Getters
    pub fn get_id(&self) -> u64 {
        self.id
    }

    pub fn get_invoker(&self) -> &str {
        &self.invoker
    }

    pub fn get_end_time(&self) -> SystemTime {
        self.end_time
    }

    pub fn get_checkpoint_start_lsn(&self) -> Lsn {
        self.checkpoint_start_lsn
    }

    pub fn get_root_lsn(&self) -> Option<Lsn> {
        self.root_lsn
    }

    pub fn get_first_active_lsn(&self) -> Lsn {
        self.first_active_lsn
    }

    pub fn get_last_local_node_id(&self) -> u64 {
        self.last_local_node_id
    }

    pub fn get_last_replicated_node_id(&self) -> i64 {
        self.last_replicated_node_id
    }

    pub fn get_last_local_db_id(&self) -> u64 {
        self.last_local_db_id
    }

    pub fn get_last_replicated_db_id(&self) -> i64 {
        self.last_replicated_db_id
    }

    pub fn get_last_local_txn_id(&self) -> u64 {
        self.last_local_txn_id
    }

    pub fn get_last_replicated_txn_id(&self) -> i64 {
        self.last_replicated_txn_id
    }

    pub fn get_cleaned_files_to_delete(&self) -> bool {
        self.cleaned_files_to_delete
    }

    /// Per-database tree root LSNs recorded at checkpoint time.
    ///
    /// Empty for a v1 (pre-per-db-roots) checkpoint or when no user database
    /// had a materialisable root; recovery then falls back to full LN redo.
    pub fn get_per_db_roots(&self) -> &[(u64, Lsn)] {
        &self.per_db_roots
    }

    /// Builder-style setter for the per-database root LSNs (v2 field).
    ///
    /// Called by the checkpointer after flushing every open user database's
    /// dirty INs, passing each tree's post-flush root LSN
    /// (`Tree::get_root_lsn`).
    pub fn with_per_db_roots(mut self, per_db_roots: Vec<(u64, Lsn)>) -> Self {
        self.per_db_roots = per_db_roots;
        self
    }

    /// Returns the serialized size in bytes.
    ///
    /// Format:
    /// - id: 8 bytes (u64, big-endian)
    /// - invoker_len: 2 bytes (u16, big-endian)
    /// - invoker: variable length UTF-8 string
    /// - checkpoint_start_lsn: 8 bytes (u64, big-endian)
    /// - flags: 1 byte (bit 0 = has_root, bit 1 = cleaned_files)
    /// - root_lsn: 8 bytes (u64, big-endian) if has_root flag set
    /// - first_active_lsn: 8 bytes (u64, big-endian)
    /// - last_local_node_id: 8 bytes (u64, big-endian)
    /// - last_replicated_node_id: 8 bytes (i64, big-endian)
    /// - last_local_db_id: 8 bytes (u64, big-endian)
    /// - last_replicated_db_id: 8 bytes (i64, big-endian)
    /// - last_local_txn_id: 8 bytes (u64, big-endian)
    /// - last_replicated_txn_id: 8 bytes (i64, big-endian)
    /// - timestamp_secs: 8 bytes (i64, big-endian)
    /// - timestamp_nanos: 4 bytes (u32, big-endian)
    ///
    /// v2 trailer (present only when `per_db_roots` is non-empty; a v1 entry
    /// and a v2 entry with no roots are byte-identical, preserving backward
    /// compatibility):
    /// - marker: 1 byte (0x01 = per_db_roots follow)
    /// - count: 4 bytes (u32, big-endian)
    /// - count * { db_id: 8 bytes (u64), root_lsn: 8 bytes (u64) }
    pub fn log_size(&self) -> usize {
        let mut size = 8 + 2 + self.invoker.len() + 8 + 1; // id, invoker_len, invoker, ckpt_start_lsn, flags
        if self.root_lsn.is_some() {
            size += 8; // root_lsn
        }
        size += 8; // first_active_lsn
        size += 8 * 6; // 6 ID fields (u64/i64 all 8 bytes)
        size += 8 + 4; // timestamp
        if !self.per_db_roots.is_empty() {
            // marker + count + 16 bytes per (db_id, root_lsn) pair
            size += 1 + 4 + self.per_db_roots.len() * 16;
        }
        size
    }

    /// Writes this entry to the log buffer.
    pub fn write_to_log(&self, buf: &mut Vec<u8>) -> Result<()> {
        // Write checkpoint ID
        buf.write_u64::<BigEndian>(self.id)?;

        // Write invoker string
        buf.write_u16::<BigEndian>(self.invoker.len() as u16)?;
        buf.extend_from_slice(self.invoker.as_bytes());

        // Write checkpoint start LSN
        buf.write_u64::<BigEndian>(self.checkpoint_start_lsn.as_u64())?;

        // Write flags
        let mut flags: u8 = 0;
        if self.root_lsn.is_some() {
            flags |= 0x01;
        }
        if self.cleaned_files_to_delete {
            flags |= 0x02;
        }
        buf.write_u8(flags)?;

        // Write root LSN if present
        if let Some(root) = self.root_lsn {
            buf.write_u64::<BigEndian>(root.as_u64())?;
        }

        // Write first active LSN
        buf.write_u64::<BigEndian>(self.first_active_lsn.as_u64())?;

        // Write ID fields
        buf.write_u64::<BigEndian>(self.last_local_node_id)?;
        buf.write_i64::<BigEndian>(self.last_replicated_node_id)?;
        buf.write_u64::<BigEndian>(self.last_local_db_id)?;
        buf.write_i64::<BigEndian>(self.last_replicated_db_id)?;
        buf.write_u64::<BigEndian>(self.last_local_txn_id)?;
        buf.write_i64::<BigEndian>(self.last_replicated_txn_id)?;

        // Write timestamp
        let duration = self
            .end_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        buf.write_i64::<BigEndian>(duration.as_secs() as i64)?;
        buf.write_u32::<BigEndian>(duration.subsec_nanos())?;

        // v2 trailer: per-database root LSNs.  Written only when non-empty so
        // that a checkpoint with no seeded roots is byte-identical to a v1
        // entry (backward compatibility — see the struct-field doc comment).
        if !self.per_db_roots.is_empty() {
            buf.write_u8(PER_DB_ROOTS_MARKER)?;
            buf.write_u32::<BigEndian>(self.per_db_roots.len() as u32)?;
            for (db_id, root_lsn) in &self.per_db_roots {
                buf.write_u64::<BigEndian>(*db_id)?;
                buf.write_u64::<BigEndian>(root_lsn.as_u64())?;
            }
        }

        Ok(())
    }

    /// Reads a checkpoint end entry from the log.
    pub fn read_from_log(buf: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(buf);

        // Read checkpoint ID
        let id = cursor.read_u64::<BigEndian>()?;

        // Read invoker string
        let invoker_len = cursor.read_u16::<BigEndian>()? as usize;
        let pos = cursor.position() as usize;
        if pos + invoker_len > buf.len() {
            return Err(crate::error::RecoveryError::InvalidCheckpoint(
                "invoker string length exceeds buffer".to_string(),
            ));
        }
        let invoker = String::from_utf8(buf[pos..pos + invoker_len].to_vec())
            .map_err(|e| {
            crate::error::RecoveryError::InvalidCheckpoint(format!(
                "invalid UTF-8: {}",
                e
            ))
        })?;
        cursor.set_position((pos + invoker_len) as u64);

        // Read checkpoint start LSN
        let checkpoint_start_lsn =
            Lsn::from_u64(cursor.read_u64::<BigEndian>()?);

        // Read flags
        let flags = cursor.read_u8()?;
        let has_root = (flags & 0x01) != 0;
        let cleaned_files_to_delete = (flags & 0x02) != 0;

        // Read root LSN if present
        let root_lsn = if has_root {
            Some(Lsn::from_u64(cursor.read_u64::<BigEndian>()?))
        } else {
            None
        };

        // Read first active LSN
        let first_active_lsn = Lsn::from_u64(cursor.read_u64::<BigEndian>()?);

        // Read ID fields
        let last_local_node_id = cursor.read_u64::<BigEndian>()?;
        let last_replicated_node_id = cursor.read_i64::<BigEndian>()?;
        let last_local_db_id = cursor.read_u64::<BigEndian>()?;
        let last_replicated_db_id = cursor.read_i64::<BigEndian>()?;
        let last_local_txn_id = cursor.read_u64::<BigEndian>()?;
        let last_replicated_txn_id = cursor.read_i64::<BigEndian>()?;

        // Read timestamp
        let secs = cursor.read_i64::<BigEndian>()?;
        let nanos = cursor.read_u32::<BigEndian>()?;
        let end_time = SystemTime::UNIX_EPOCH
            + std::time::Duration::new(secs as u64, nanos);

        // v2 trailer: per-database root LSNs.  Absent in a v1 entry — detect
        // it by the trailing presence marker.  Any read error here degrades
        // gracefully to an empty map (full LN redo), never to corruption:
        // a truncated/garbled trailer must not fail recovery of an otherwise
        // valid checkpoint.
        let per_db_roots = read_per_db_roots(&mut cursor, buf);

        Ok(Self {
            id,
            invoker,
            end_time,
            checkpoint_start_lsn,
            root_lsn,
            first_active_lsn,
            last_local_node_id,
            last_replicated_node_id,
            last_local_db_id,
            last_replicated_db_id,
            last_local_txn_id,
            last_replicated_txn_id,
            cleaned_files_to_delete,
            per_db_roots,
        })
    }
}

/// Marker byte introducing the v2 per-database-roots trailer.
const PER_DB_ROOTS_MARKER: u8 = 0x01;

/// Read the optional v2 per-database-roots trailer.
///
/// Returns an empty vector when the trailer is absent (v1 entry), when the
/// marker byte does not match, or when the trailer is truncated/inconsistent.
/// Degrading to empty (→ full LN redo) is always safe; the alternative
/// (failing recovery of a valid checkpoint) is not.
fn read_per_db_roots(
    cursor: &mut Cursor<&[u8]>,
    buf: &[u8],
) -> Vec<(u64, Lsn)> {
    let pos = cursor.position() as usize;
    // No trailing bytes, or not even a marker byte: v1 entry.
    if pos >= buf.len() {
        return Vec::new();
    }
    let marker = match cursor.read_u8() {
        Ok(m) => m,
        Err(_) => return Vec::new(),
    };
    if marker != PER_DB_ROOTS_MARKER {
        return Vec::new();
    }
    let count = match cursor.read_u32::<BigEndian>() {
        Ok(c) => c as usize,
        Err(_) => return Vec::new(),
    };
    // Bound the allocation to what the buffer can actually hold (16 bytes per
    // pair) so a garbled count cannot trigger a huge reservation.
    let remaining = buf.len().saturating_sub(cursor.position() as usize);
    if count > remaining / 16 {
        return Vec::new();
    }
    let mut roots = Vec::with_capacity(count);
    for _ in 0..count {
        let db_id = match cursor.read_u64::<BigEndian>() {
            Ok(v) => v,
            Err(_) => return roots,
        };
        let lsn = match cursor.read_u64::<BigEndian>() {
            Ok(v) => Lsn::from_u64(v),
            Err(_) => return roots,
        };
        roots.push((db_id, lsn));
    }
    roots
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_util::NULL_LSN;
    use std::time::Duration;

    #[test]
    fn test_new() {
        let ckpt_start = Lsn::new(1, 100);
        let root = Lsn::new(2, 200);
        let first_active = Lsn::new(3, 300);

        let ckpt = CheckpointEnd::new(
            123,
            "daemon",
            ckpt_start,
            Some(root),
            first_active,
            1000,
            -1,
            2000,
            -2,
            3000,
            -3,
            true,
        );

        assert_eq!(ckpt.get_id(), 123);
        assert_eq!(ckpt.get_invoker(), "daemon");
        assert_eq!(ckpt.get_checkpoint_start_lsn(), ckpt_start);
        assert_eq!(ckpt.get_root_lsn(), Some(root));
        assert_eq!(ckpt.get_first_active_lsn(), first_active);
        assert_eq!(ckpt.get_last_local_node_id(), 1000);
        assert_eq!(ckpt.get_last_replicated_node_id(), -1);
        assert_eq!(ckpt.get_last_local_db_id(), 2000);
        assert_eq!(ckpt.get_last_replicated_db_id(), -2);
        assert_eq!(ckpt.get_last_local_txn_id(), 3000);
        assert_eq!(ckpt.get_last_replicated_txn_id(), -3);
        assert!(ckpt.get_cleaned_files_to_delete());
    }

    #[test]
    fn test_with_time() {
        let time = SystemTime::UNIX_EPOCH + Duration::from_secs(5000);
        let ckpt = CheckpointEnd::with_time(
            456,
            "api",
            time,
            Lsn::new(1, 0),
            None,
            NULL_LSN,
            0,
            0,
            0,
            0,
            0,
            0,
            false,
        );

        assert_eq!(ckpt.get_id(), 456);
        assert_eq!(ckpt.get_end_time(), time);
        assert_eq!(ckpt.get_root_lsn(), None);
        assert!(!ckpt.get_cleaned_files_to_delete());
    }

    #[test]
    fn test_log_size_with_root() {
        let ckpt = CheckpointEnd::new(
            1,
            "daemon",
            Lsn::new(1, 0),
            Some(Lsn::new(2, 0)),
            NULL_LSN,
            0,
            0,
            0,
            0,
            0,
            0,
            false,
        );

        // 8 (id) + 2 (len) + 6 (daemon) + 8 (ckpt_start) + 1 (flags) +
        // 8 (root) + 8 (first_active) + 48 (6 IDs) + 8 (secs) + 4 (nanos) = 101
        assert_eq!(ckpt.log_size(), 101);
    }

    #[test]
    fn test_log_size_without_root() {
        let ckpt = CheckpointEnd::new(
            1,
            "daemon",
            Lsn::new(1, 0),
            None,
            NULL_LSN,
            0,
            0,
            0,
            0,
            0,
            0,
            false,
        );

        // Same as above but no root LSN (8 bytes less) = 93
        assert_eq!(ckpt.log_size(), 93);
    }

    #[test]
    fn test_serialization_round_trip_with_root() {
        let original = CheckpointEnd::new(
            12345,
            "cleaner",
            Lsn::new(10, 100),
            Some(Lsn::new(20, 200)),
            Lsn::new(30, 300),
            1000,
            -1000,
            2000,
            -2000,
            3000,
            -3000,
            true,
        );

        let mut buf = Vec::new();
        original.write_to_log(&mut buf).unwrap();
        assert_eq!(buf.len(), original.log_size());

        let deserialized = CheckpointEnd::read_from_log(&buf).unwrap();
        assert_eq!(deserialized.get_id(), original.get_id());
        assert_eq!(deserialized.get_invoker(), original.get_invoker());
        assert_eq!(
            deserialized.get_checkpoint_start_lsn(),
            original.get_checkpoint_start_lsn()
        );
        assert_eq!(deserialized.get_root_lsn(), original.get_root_lsn());
        assert_eq!(
            deserialized.get_first_active_lsn(),
            original.get_first_active_lsn()
        );
        assert_eq!(
            deserialized.get_last_local_node_id(),
            original.get_last_local_node_id()
        );
        assert_eq!(
            deserialized.get_last_replicated_node_id(),
            original.get_last_replicated_node_id()
        );
        assert_eq!(
            deserialized.get_last_local_db_id(),
            original.get_last_local_db_id()
        );
        assert_eq!(
            deserialized.get_last_replicated_db_id(),
            original.get_last_replicated_db_id()
        );
        assert_eq!(
            deserialized.get_last_local_txn_id(),
            original.get_last_local_txn_id()
        );
        assert_eq!(
            deserialized.get_last_replicated_txn_id(),
            original.get_last_replicated_txn_id()
        );
        assert_eq!(
            deserialized.get_cleaned_files_to_delete(),
            original.get_cleaned_files_to_delete()
        );

        let orig_duration = original
            .get_end_time()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        let deser_duration = deserialized
            .get_end_time()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        assert_eq!(orig_duration.as_secs(), deser_duration.as_secs());
        assert_eq!(orig_duration.subsec_nanos(), deser_duration.subsec_nanos());
    }

    #[test]
    fn test_serialization_round_trip_without_root() {
        let original = CheckpointEnd::new(
            999,
            "recovery",
            Lsn::new(5, 50),
            None,
            Lsn::new(6, 60),
            100,
            -100,
            200,
            -200,
            300,
            -300,
            false,
        );

        let mut buf = Vec::new();
        original.write_to_log(&mut buf).unwrap();
        assert_eq!(buf.len(), original.log_size());

        let deserialized = CheckpointEnd::read_from_log(&buf).unwrap();
        assert_eq!(deserialized.get_id(), original.get_id());
        assert_eq!(deserialized.get_root_lsn(), None);
        assert!(!deserialized.get_cleaned_files_to_delete());
    }

    #[test]
    fn test_flags_all_combinations() {
        let test_cases = vec![
            (None, false),
            (None, true),
            (Some(Lsn::new(1, 1)), false),
            (Some(Lsn::new(1, 1)), true),
        ];

        for (root, cleaned) in test_cases {
            let ckpt = CheckpointEnd::new(
                1,
                "test",
                Lsn::new(1, 0),
                root,
                NULL_LSN,
                0,
                0,
                0,
                0,
                0,
                0,
                cleaned,
            );

            let mut buf = Vec::new();
            ckpt.write_to_log(&mut buf).unwrap();

            let restored = CheckpointEnd::read_from_log(&buf).unwrap();
            assert_eq!(restored.get_root_lsn(), root);
            assert_eq!(restored.get_cleaned_files_to_delete(), cleaned);
        }
    }

    #[test]
    fn test_null_lsn() {
        let ckpt = CheckpointEnd::new(
            1, "test", NULL_LSN, None, NULL_LSN, 0, 0, 0, 0, 0, 0, false,
        );

        let mut buf = Vec::new();
        ckpt.write_to_log(&mut buf).unwrap();

        let restored = CheckpointEnd::read_from_log(&buf).unwrap();
        assert_eq!(restored.get_checkpoint_start_lsn(), NULL_LSN);
        assert_eq!(restored.get_first_active_lsn(), NULL_LSN);
    }

    #[test]
    fn test_large_id_values() {
        let ckpt = CheckpointEnd::new(
            u64::MAX,
            "test",
            Lsn::new(u32::MAX, u32::MAX),
            Some(Lsn::new(u32::MAX, u32::MAX)),
            Lsn::new(u32::MAX, u32::MAX),
            u64::MAX,
            i64::MIN,
            u64::MAX,
            i64::MAX,
            u64::MAX,
            i64::MIN,
            true,
        );

        let mut buf = Vec::new();
        ckpt.write_to_log(&mut buf).unwrap();

        let restored = CheckpointEnd::read_from_log(&buf).unwrap();
        assert_eq!(restored.get_id(), u64::MAX);
        assert_eq!(restored.get_last_local_node_id(), u64::MAX);
        assert_eq!(restored.get_last_replicated_node_id(), i64::MIN);
        assert_eq!(restored.get_last_replicated_db_id(), i64::MAX);
    }

    #[test]
    fn test_empty_invoker() {
        let ckpt = CheckpointEnd::new(
            1,
            "",
            Lsn::new(1, 0),
            None,
            NULL_LSN,
            0,
            0,
            0,
            0,
            0,
            0,
            false,
        );

        let mut buf = Vec::new();
        ckpt.write_to_log(&mut buf).unwrap();

        let restored = CheckpointEnd::read_from_log(&buf).unwrap();
        assert_eq!(restored.get_invoker(), "");
    }

    #[test]
    fn test_invalid_buffer_too_short() {
        let buf = vec![0u8; 5];
        let result = CheckpointEnd::read_from_log(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_per_db_roots_round_trip() {
        let roots = vec![
            (1u64, Lsn::new(3, 100)),
            (7u64, Lsn::new(9, 4096)),
            (42u64, NULL_LSN),
        ];
        let ckpt = CheckpointEnd::new(
            5,
            "daemon",
            Lsn::new(1, 0),
            None,
            NULL_LSN,
            0,
            0,
            0,
            0,
            0,
            0,
            false,
        )
        .with_per_db_roots(roots.clone());

        let mut buf = Vec::new();
        ckpt.write_to_log(&mut buf).unwrap();
        assert_eq!(buf.len(), ckpt.log_size());

        let restored = CheckpointEnd::read_from_log(&buf).unwrap();
        assert_eq!(restored.get_per_db_roots(), roots.as_slice());
    }

    #[test]
    fn test_no_per_db_roots_is_byte_identical_to_v1() {
        // A checkpoint with no seeded roots must serialize EXACTLY like a
        // pre-per-db-roots (v1) entry, so old and new readers agree and the
        // trailer is truly optional.
        let ckpt = CheckpointEnd::new(
            9,
            "api",
            Lsn::new(2, 200),
            Some(Lsn::new(4, 40)),
            Lsn::new(6, 60),
            11,
            -11,
            22,
            -22,
            33,
            -33,
            true,
        );
        let mut buf = Vec::new();
        ckpt.write_to_log(&mut buf).unwrap();

        // No trailing marker byte was written.
        // (log_size accounts for exactly the v1 layout when roots are empty.)
        assert_eq!(buf.len(), ckpt.log_size());
        let restored = CheckpointEnd::read_from_log(&buf).unwrap();
        assert!(restored.get_per_db_roots().is_empty());
    }

    #[test]
    fn test_v1_entry_reads_back_with_empty_roots() {
        // Simulate reading an OLD (v1) on-disk CheckpointEnd: serialize with
        // no roots (byte-identical to v1), then read it back.  Recovery must
        // see an empty per_db_roots and fall back to full redo.
        let ckpt = CheckpointEnd::new(
            1,
            "recovery",
            Lsn::new(5, 50),
            None,
            Lsn::new(6, 60),
            100,
            -100,
            200,
            -200,
            300,
            -300,
            false,
        );
        let mut buf = Vec::new();
        ckpt.write_to_log(&mut buf).unwrap();
        let restored = CheckpointEnd::read_from_log(&buf).unwrap();
        assert!(restored.get_per_db_roots().is_empty());
    }

    #[test]
    fn test_garbled_trailer_degrades_to_empty() {
        // A v1 entry with junk appended (e.g. from a torn write) must not fail
        // recovery; the trailer parser degrades to an empty map.
        let ckpt = CheckpointEnd::new(
            1,
            "t",
            Lsn::new(1, 0),
            None,
            NULL_LSN,
            0,
            0,
            0,
            0,
            0,
            0,
            false,
        );
        let mut buf = Vec::new();
        ckpt.write_to_log(&mut buf).unwrap();
        // Append a bogus marker + truncated count.
        buf.push(PER_DB_ROOTS_MARKER);
        buf.push(0xFF); // partial u32 count
        let restored = CheckpointEnd::read_from_log(&buf).unwrap();
        assert!(restored.get_per_db_roots().is_empty());
    }
}
