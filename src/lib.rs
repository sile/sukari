//! Shared segmented Raft log storage for multi-Raft workloads.
//!
//! This crate is currently under initial development.

#![forbid(unsafe_code)]

mod bytes;
mod registry;
mod stats;
mod storage;

pub use bytes::Bytes;
pub use registry::NodeMetadata;
pub use stats::{OperationKindStats, RecordKindStats, RejectedOperationStats, StorageStats};
pub use storage::{LogAppend, NodeState, Snapshot, SnapshotCheckpoint, StorageEngine, SyncPolicy};
