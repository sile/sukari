//! Shared segmented Raft log storage for multi-Raft workloads.
//!
//! This crate is currently under initial development.

#![forbid(unsafe_code)]

mod bytes;
mod storage;

pub use bytes::Bytes;
pub use storage::{LogAppend, Snapshot, StorageEngine, StorageState, SyncPolicy};
