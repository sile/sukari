//! Shared segmented Raft log storage for multi-Raft workloads.
//!
//! This crate is currently under initial development.
//!
//! `sukari` stores durable records for many local Raft nodes in shared append
//! segment files. It owns the on-disk storage format, recovery, replay,
//! snapshot checkpoints, node registry metadata, and whole-segment garbage
//! collection. Raft protocol decisions, networking, timers, and application
//! command execution remain the caller's responsibility.
//!
//! A [`StorageEngine`] has one writer. It does not add internal synchronization
//! for concurrent callers; runtimes that need concurrent access serialize
//! storage requests outside this crate.

#![forbid(unsafe_code)]

mod bytes;
mod registry;
mod stats;
mod storage;

pub use bytes::Bytes;
pub use registry::NodeMetadata;
pub use stats::{OperationKindStats, RecordKindStats, RejectedOperationStats, StorageStats};
pub use storage::{
    CommandPayload, LogAppend, NodeState, Snapshot, SnapshotCheckpoint, StorageEngine, SyncPolicy,
};
