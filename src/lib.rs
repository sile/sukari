//! Shared segmented Raft log storage for applications that run one or more
//! local Raft nodes.
//!
//! `sukari` stores durable records for one or more local Raft nodes in shared
//! append segment files. It owns the on-disk storage format, recovery, replay,
//! snapshot checkpoints, node registry metadata, and whole-segment garbage
//! collection.
//!
//! A [`StorageEngine`] has one writer. It does not add internal synchronization
//! for concurrent callers; runtimes that need concurrent access serialize
//! storage requests outside this crate.

#![forbid(unsafe_code)]

mod bytes;
mod crc32c;
mod registry;
mod stats;
mod storage;

pub use bytes::Bytes;
pub use registry::NodeMetadata;
pub use stats::{OperationKindStats, RecordKindStats, RejectedOperationStats, StorageStats};
pub use storage::{
    CommandPayload, LogAppend, NodeState, Snapshot, SnapshotCheckpoint, StorageEngine, SyncPolicy,
};
