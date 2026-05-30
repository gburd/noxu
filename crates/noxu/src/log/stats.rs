//! Log statistics definitions.
//!
//!
//! Defines stat definitions for the log subsystem using the noxu-util
//! stats framework.

use crate::util::stats::{StatDefinition, StatType};

/// Group name for all I/O statistics.
pub const IO_GROUP_NAME: &str = "I/O";
pub const IO_GROUP_DESC: &str = "The I/O portion of the append-only storage system includes \
     access to data files and caching of file handles.";

/// Group name for log buffer pool statistics.
pub const LOG_BUFFER_POOL_GROUP: &str = "LogBufferPool";

/// Group name for file manager statistics.
pub const FILE_MANAGER_GROUP: &str = "FileManager";

/// Group name for fsync manager statistics.
pub const FSYNC_MANAGER_GROUP: &str = "FSyncManager";

// File Manager stats
pub const RANDOM_READS: StatDefinition = StatDefinition {
    name: "nRandomReads",
    description: "Number of disk reads which required repositioning the disk head \
                  more than 1MB from the previous file position",
    stat_type: StatType::Incremental,
};

pub const RANDOM_WRITES: StatDefinition = StatDefinition {
    name: "nRandomWrites",
    description: "Number of disk writes which required repositioning the disk head \
                  by more than 1MB from the previous file position",
    stat_type: StatType::Incremental,
};

pub const SEQUENTIAL_READS: StatDefinition = StatDefinition {
    name: "nSequentialReads",
    description: "Number of disk reads which did not require repositioning the disk \
                  head more than 1MB from the previous file position",
    stat_type: StatType::Incremental,
};

pub const SEQUENTIAL_WRITES: StatDefinition = StatDefinition {
    name: "nSequentialWrites",
    description: "Number of disk writes which did not require repositioning the disk \
                  head by more than 1MB from the previous file position",
    stat_type: StatType::Incremental,
};

pub const RANDOM_READ_BYTES: StatDefinition = StatDefinition {
    name: "nRandomReadBytes",
    description: "Number of bytes read which required repositioning the disk head \
                  more than 1MB from the previous file position",
    stat_type: StatType::Incremental,
};

pub const RANDOM_WRITE_BYTES: StatDefinition = StatDefinition {
    name: "nRandomWriteBytes",
    description: "Number of bytes written which required repositioning the disk head \
                  more than 1MB from the previous file position",
    stat_type: StatType::Incremental,
};

pub const SEQUENTIAL_READ_BYTES: StatDefinition = StatDefinition {
    name: "nSequentialReadBytes",
    description: "Number of bytes read which did not require repositioning the disk \
                  head more than 1MB from the previous file position",
    stat_type: StatType::Incremental,
};

pub const SEQUENTIAL_WRITE_BYTES: StatDefinition = StatDefinition {
    name: "nSequentialWriteBytes",
    description: "Number of bytes written which did not require repositioning the \
                  disk head more than 1MB from the previous file position",
    stat_type: StatType::Incremental,
};

pub const FILE_OPENS: StatDefinition = StatDefinition {
    name: "nFileOpens",
    description: "Number of times a log file has been opened",
    stat_type: StatType::Incremental,
};

pub const OPEN_FILES: StatDefinition = StatDefinition {
    name: "nOpenFiles",
    description: "Number of files currently open in the file cache",
    stat_type: StatType::Cumulative,
};

// FSyncManager stats
pub const FSYNCS: StatDefinition = StatDefinition {
    name: "nFSyncs",
    description: "Number of fsyncs issued for actions such as transaction commits \
                  and checkpoints",
    stat_type: StatType::Incremental,
};

pub const FSYNC_REQUESTS: StatDefinition = StatDefinition {
    name: "nFSyncRequests",
    description: "Number of fsyncs requested for actions such as transaction commits \
                  and checkpoints",
    stat_type: StatType::Incremental,
};

pub const FSYNC_TIMEOUTS: StatDefinition = StatDefinition {
    name: "nGrpCommitTimeouts",
    description: "Number of fsync requests which timed out",
    stat_type: StatType::Incremental,
};

pub const LOG_FSYNCS: StatDefinition = StatDefinition {
    name: "nLogFSyncs",
    description: "Total number of fsyncs of the log",
    stat_type: StatType::Incremental,
};

// LogManager stats
pub const REPEAT_FAULT_READS: StatDefinition = StatDefinition {
    name: "nRepeatFaultReads",
    description: "Number of reads which had to be repeated when faulting in an object \
                  from disk because the read chunk size was too small",
    stat_type: StatType::Incremental,
};

pub const TEMP_BUFFER_WRITES: StatDefinition = StatDefinition {
    name: "nTempBufferWrites",
    description: "Number of writes which had to be completed using a temporary \
                  marshalling buffer because the fixed size log buffers were not \
                  large enough",
    stat_type: StatType::Incremental,
};

pub const END_OF_LOG: StatDefinition = StatDefinition {
    name: "endOfLog",
    description: "The location of the next entry to be written to the log",
    stat_type: StatType::Cumulative,
};

// LogBufferPool stats
pub const NO_FREE_BUFFER: StatDefinition = StatDefinition {
    name: "nNoFreeBuffer",
    description: "Number of requests to get a free buffer that forced a log write",
    stat_type: StatType::Incremental,
};

pub const NOT_RESIDENT: StatDefinition = StatDefinition {
    name: "nNotResident",
    description: "Number of requests for database objects not contained within the \
                  in-memory data structure",
    stat_type: StatType::Incremental,
};

pub const CACHE_MISS: StatDefinition = StatDefinition {
    name: "nCacheMiss",
    description: "Total number of requests for database objects which were not in memory",
    stat_type: StatType::Incremental,
};

pub const LOG_BUFFERS: StatDefinition = StatDefinition {
    name: "nLogBuffers",
    description: "Number of log buffers currently instantiated",
    stat_type: StatType::Cumulative,
};

pub const BUFFER_BYTES: StatDefinition = StatDefinition {
    name: "bufferBytes",
    description: "Total memory currently consumed by log buffers, in bytes",
    stat_type: StatType::Cumulative,
};
