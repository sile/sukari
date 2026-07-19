//! Storage statistics.

use std::sync::atomic::{AtomicU64, Ordering};

/// Runtime storage counter and gauge snapshot.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StorageStats {
    /// Segment records written by record kind.
    pub records_written: RecordKindStats,

    /// Segment frame bytes written by record kind.
    pub bytes_written: RecordKindStats,

    /// Segment records replayed by record kind.
    pub records_replayed: RecordKindStats,

    /// Segment frame bytes replayed by record kind.
    pub bytes_replayed: RecordKindStats,

    /// Number of append segment rotations.
    pub segment_rotations: u64,

    /// Number of flush operations.
    pub flushes: u64,

    /// Number of successful durable data synchronizations.
    pub durable_syncs: u64,

    /// Number of partial trailing records truncated during recovery or replay.
    pub replay_truncations: u64,

    /// Number of segment record checksum failures.
    pub checksum_failures: u64,

    /// Number of nodes created successfully.
    pub nodes_created: u64,

    /// Number of nodes removed successfully.
    pub nodes_removed: u64,

    /// Rejected node operations.
    pub rejected_operations: RejectedOperationStats,

    /// Number of user snapshot checkpoints saved successfully.
    pub snapshot_checkpoints_saved: u64,

    /// Number of whole-segment garbage collection runs.
    pub gc_runs: u64,

    /// Number of append segment files deleted by garbage collection.
    pub gc_segments_deleted: u64,

    /// Number of active registered nodes.
    pub active_nodes: u64,

    /// Number of removed registered nodes.
    pub removed_nodes: u64,

    /// Number of active checkpoint index entries.
    pub checkpoint_index_nodes: u64,

    /// Active append segment ID.
    pub active_append_segment_id: u64,

    /// Active append segment length in bytes.
    pub active_append_segment_len_bytes: u64,

    /// Records written after the last durable data synchronization.
    pub unsynced_records: u64,

    /// Bytes written after the last durable data synchronization.
    pub unsynced_bytes: u64,
}

/// Statistics keyed by segment record kind.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RecordKindStats {
    /// Current-term records.
    pub current_term: u64,

    /// Voted-for records.
    pub voted_for: u64,

    /// Log append records.
    pub log_append: u64,

    /// Snapshot checkpoint records.
    pub snapshot_checkpoint: u64,
}

/// Rejected operation statistics.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RejectedOperationStats {
    /// Operations rejected because the node has not been created.
    pub unknown_nodes: OperationKindStats,

    /// Operations rejected because the node has been removed.
    pub removed_nodes: OperationKindStats,
}

/// Statistics keyed by storage operation kind.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OperationKindStats {
    /// `StorageEngine::load` rejections.
    pub load: u64,

    /// `StorageEngine::save_current_term` rejections.
    pub save_current_term: u64,

    /// `StorageEngine::save_voted_for` rejections.
    pub save_voted_for: u64,

    /// `StorageEngine::append_entries` rejections.
    pub append_entries: u64,

    /// `StorageEngine::save_snapshot` rejections.
    pub save_snapshot: u64,

    /// `StorageEngine::remove_node` rejections.
    pub remove_node: u64,
}

#[derive(Debug, Default)]
pub(crate) struct StorageStatsCounters {
    records_written: AtomicRecordKindStats,
    bytes_written: AtomicRecordKindStats,
    records_replayed: AtomicRecordKindStats,
    bytes_replayed: AtomicRecordKindStats,
    segment_rotations: AtomicU64,
    flushes: AtomicU64,
    durable_syncs: AtomicU64,
    replay_truncations: AtomicU64,
    checksum_failures: AtomicU64,
    nodes_created: AtomicU64,
    nodes_removed: AtomicU64,
    rejected_operations: AtomicRejectedOperationStats,
    snapshot_checkpoints_saved: AtomicU64,
    gc_runs: AtomicU64,
    gc_segments_deleted: AtomicU64,
}

impl StorageStatsCounters {
    pub(crate) fn snapshot(&self) -> StorageStats {
        StorageStats {
            records_written: self.records_written.snapshot(),
            bytes_written: self.bytes_written.snapshot(),
            records_replayed: self.records_replayed.snapshot(),
            bytes_replayed: self.bytes_replayed.snapshot(),
            segment_rotations: load(&self.segment_rotations),
            flushes: load(&self.flushes),
            durable_syncs: load(&self.durable_syncs),
            replay_truncations: load(&self.replay_truncations),
            checksum_failures: load(&self.checksum_failures),
            nodes_created: load(&self.nodes_created),
            nodes_removed: load(&self.nodes_removed),
            rejected_operations: self.rejected_operations.snapshot(),
            snapshot_checkpoints_saved: load(&self.snapshot_checkpoints_saved),
            gc_runs: load(&self.gc_runs),
            gc_segments_deleted: load(&self.gc_segments_deleted),
            active_nodes: 0,
            removed_nodes: 0,
            checkpoint_index_nodes: 0,
            active_append_segment_id: 0,
            active_append_segment_len_bytes: 0,
            unsynced_records: 0,
            unsynced_bytes: 0,
        }
    }

    pub(crate) fn record_written(&self, kind: RecordKindMetric, bytes: u64) {
        self.records_written.increment(kind, 1);
        self.bytes_written.increment(kind, bytes);
    }

    pub(crate) fn record_replayed(&self, kind: RecordKindMetric, bytes: u64) {
        self.records_replayed.increment(kind, 1);
        self.bytes_replayed.increment(kind, bytes);
    }

    pub(crate) fn segment_rotated(&self) {
        increment(&self.segment_rotations, 1);
    }

    pub(crate) fn flushed(&self) {
        increment(&self.flushes, 1);
    }

    pub(crate) fn durable_synced(&self) {
        increment(&self.durable_syncs, 1);
    }

    pub(crate) fn replay_truncated(&self) {
        increment(&self.replay_truncations, 1);
    }

    pub(crate) fn checksum_failed(&self) {
        increment(&self.checksum_failures, 1);
    }

    pub(crate) fn node_created(&self) {
        increment(&self.nodes_created, 1);
    }

    pub(crate) fn node_removed(&self) {
        increment(&self.nodes_removed, 1);
    }

    pub(crate) fn rejected_operation(
        &self,
        operation: StorageOperationKind,
        error: NodeAccessErrorKind,
    ) {
        self.rejected_operations.increment(operation, error);
    }

    pub(crate) fn snapshot_checkpoint_saved(&self) {
        increment(&self.snapshot_checkpoints_saved, 1);
    }

    pub(crate) fn gc_ran(&self) {
        increment(&self.gc_runs, 1);
    }

    pub(crate) fn gc_deleted_segments(&self, count: u64) {
        increment(&self.gc_segments_deleted, count);
    }
}

#[derive(Debug, Default)]
struct AtomicRecordKindStats {
    current_term: AtomicU64,
    voted_for: AtomicU64,
    log_append: AtomicU64,
    snapshot_checkpoint: AtomicU64,
}

impl AtomicRecordKindStats {
    fn snapshot(&self) -> RecordKindStats {
        RecordKindStats {
            current_term: load(&self.current_term),
            voted_for: load(&self.voted_for),
            log_append: load(&self.log_append),
            snapshot_checkpoint: load(&self.snapshot_checkpoint),
        }
    }

    fn increment(&self, kind: RecordKindMetric, value: u64) {
        match kind {
            RecordKindMetric::CurrentTerm => increment(&self.current_term, value),
            RecordKindMetric::VotedFor => increment(&self.voted_for, value),
            RecordKindMetric::LogAppend => increment(&self.log_append, value),
            RecordKindMetric::SnapshotCheckpoint => {
                increment(&self.snapshot_checkpoint, value);
            }
        }
    }
}

#[derive(Debug, Default)]
struct AtomicRejectedOperationStats {
    unknown_nodes: AtomicOperationKindStats,
    removed_nodes: AtomicOperationKindStats,
}

impl AtomicRejectedOperationStats {
    fn snapshot(&self) -> RejectedOperationStats {
        RejectedOperationStats {
            unknown_nodes: self.unknown_nodes.snapshot(),
            removed_nodes: self.removed_nodes.snapshot(),
        }
    }

    fn increment(&self, operation: StorageOperationKind, error: NodeAccessErrorKind) {
        match error {
            NodeAccessErrorKind::UnknownNode => self.unknown_nodes.increment(operation),
            NodeAccessErrorKind::RemovedNode => self.removed_nodes.increment(operation),
        }
    }
}

#[derive(Debug, Default)]
struct AtomicOperationKindStats {
    load: AtomicU64,
    save_current_term: AtomicU64,
    save_voted_for: AtomicU64,
    append_entries: AtomicU64,
    save_snapshot: AtomicU64,
    remove_node: AtomicU64,
}

impl AtomicOperationKindStats {
    fn snapshot(&self) -> OperationKindStats {
        OperationKindStats {
            load: load(&self.load),
            save_current_term: load(&self.save_current_term),
            save_voted_for: load(&self.save_voted_for),
            append_entries: load(&self.append_entries),
            save_snapshot: load(&self.save_snapshot),
            remove_node: load(&self.remove_node),
        }
    }

    fn increment(&self, operation: StorageOperationKind) {
        match operation {
            StorageOperationKind::Load => increment(&self.load, 1),
            StorageOperationKind::SaveCurrentTerm => increment(&self.save_current_term, 1),
            StorageOperationKind::SaveVotedFor => increment(&self.save_voted_for, 1),
            StorageOperationKind::AppendEntries => increment(&self.append_entries, 1),
            StorageOperationKind::SaveSnapshot => increment(&self.save_snapshot, 1),
            StorageOperationKind::RemoveNode => increment(&self.remove_node, 1),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum RecordKindMetric {
    CurrentTerm,
    VotedFor,
    LogAppend,
    SnapshotCheckpoint,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum StorageOperationKind {
    Load,
    SaveCurrentTerm,
    SaveVotedFor,
    AppendEntries,
    SaveSnapshot,
    RemoveNode,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum NodeAccessErrorKind {
    UnknownNode,
    RemovedNode,
}

fn increment(counter: &AtomicU64, value: u64) {
    counter.fetch_add(value, Ordering::Relaxed);
}

fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}
