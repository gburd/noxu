//! VLSN tracking subsystem.
//!
//! tracks the mapping from VLSNs
//! (Virtual Log Sequence Numbers) to LSNs (Log Sequence Numbers) and
//! maintains the range of VLSNs available on this node.

pub mod persist;
pub mod vlsn_bucket;
pub mod vlsn_index;
pub mod vlsn_range;

pub use persist::{
    VlsnPersistError, flush_to_disk, index_path, load_from_disk,
};
pub use vlsn_bucket::VlsnBucket;
pub use vlsn_index::VlsnIndex;
pub use vlsn_range::VlsnRange;
