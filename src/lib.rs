//! Shared append-segment Raft state storage for
//! [`noraft`](https://github.com/sile/noraft)-based applications that run one
//! or more local Raft nodes.
//!
//! `sukari` uses `noraft` protocol types directly and stores durable records
//! for one or more local Raft nodes in shared append segment files. It owns the
//! on-disk storage format, recovery, replay, snapshot checkpoints, node
//! registry metadata, and whole-segment garbage collection.
//!
//! A [`StorageEngine`] has one writer and takes an exclusive OS file lock on
//! `write.lock`. The read-only [`load`] and [`load_all`] functions can access
//! the same directory concurrently, but never modify it. The storage directory
//! must be on a local filesystem;
//! network filesystems are not supported.
//!
//! `StorageEngine` does not add internal synchronization for concurrent callers;
//! runtimes that need concurrent access serialize storage requests outside this crate.
//!
//! ## Key Characteristics
//!
//! `sukari` favors a simple runtime path: ordinary writes append records to
//! shared segments, and full reads are mainly for startup or recovery. This
//! should make typical Raft storage writes predictable. Recovery APIs read
//! snapshots and retained log suffixes as whole values, so huge payloads and
//! random-read log paging are out of scope.
//!
//! Ordinary node-state writes do not synchronize the active segment. Callers
//! decide when pending segment appends become durable by calling
//! [`StorageEngine::sync`]. Metadata JSON files, directory updates, segment
//! rotation boundaries, and checkpoint records referenced from
//! `checkpoints.json` are synchronized by the storage engine.

#![forbid(unsafe_code)]

mod bytes;
mod codec;
mod crc32c;
mod error;
mod metrics;
mod registry;
mod segment;
mod storage;

pub use bytes::Bytes;
pub use metrics::{
    OperationKindMetrics, RecordKindMetrics, RejectedOperationMetrics, StorageMetrics,
};
pub use registry::NodeMetadata;
pub use storage::{
    CommandPayload, LogAppend, NodeState, Snapshot, SnapshotCheckpoint, StorageEngine, load,
    load_all, nodes,
};
