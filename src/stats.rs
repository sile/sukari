//! Storage statistics.

use core::fmt;

/// Runtime storage counters and gauges.
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

    /// Number of explicit segment synchronization requests.
    pub syncs: u64,

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

impl nojson::DisplayJson for StorageStats {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> fmt::Result {
        f.object(|f| {
            f.member("records_written", self.records_written)?;
            f.member("bytes_written", self.bytes_written)?;
            f.member("records_replayed", self.records_replayed)?;
            f.member("bytes_replayed", self.bytes_replayed)?;
            f.member("segment_rotations", self.segment_rotations)?;
            f.member("syncs", self.syncs)?;
            f.member("durable_syncs", self.durable_syncs)?;
            f.member("replay_truncations", self.replay_truncations)?;
            f.member("checksum_failures", self.checksum_failures)?;
            f.member("nodes_created", self.nodes_created)?;
            f.member("nodes_removed", self.nodes_removed)?;
            f.member("rejected_operations", &self.rejected_operations)?;
            f.member(
                "snapshot_checkpoints_saved",
                self.snapshot_checkpoints_saved,
            )?;
            f.member("gc_runs", self.gc_runs)?;
            f.member("gc_segments_deleted", self.gc_segments_deleted)?;
            f.member("active_nodes", self.active_nodes)?;
            f.member("removed_nodes", self.removed_nodes)?;
            f.member("checkpoint_index_nodes", self.checkpoint_index_nodes)?;
            f.member("active_append_segment_id", self.active_append_segment_id)?;
            f.member(
                "active_append_segment_len_bytes",
                self.active_append_segment_len_bytes,
            )?;
            f.member("unsynced_records", self.unsynced_records)?;
            f.member("unsynced_bytes", self.unsynced_bytes)
        })
    }
}

impl fmt::Display for StorageStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        display_json(self, f)
    }
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

impl nojson::DisplayJson for RecordKindStats {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> fmt::Result {
        f.object(|f| {
            f.member("current_term", self.current_term)?;
            f.member("voted_for", self.voted_for)?;
            f.member("log_append", self.log_append)?;
            f.member("snapshot_checkpoint", self.snapshot_checkpoint)
        })
    }
}

impl fmt::Display for RecordKindStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        display_json(self, f)
    }
}

/// Rejected operation statistics.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RejectedOperationStats {
    /// Operations rejected because the node has not been created.
    pub unknown_nodes: OperationKindStats,

    /// Operations rejected because the node has been removed.
    pub removed_nodes: OperationKindStats,
}

impl nojson::DisplayJson for RejectedOperationStats {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> fmt::Result {
        f.object(|f| {
            f.member("unknown_nodes", self.unknown_nodes)?;
            f.member("removed_nodes", self.removed_nodes)
        })
    }
}

impl fmt::Display for RejectedOperationStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        display_json(self, f)
    }
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

impl nojson::DisplayJson for OperationKindStats {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> fmt::Result {
        f.object(|f| {
            f.member("load", self.load)?;
            f.member("save_current_term", self.save_current_term)?;
            f.member("save_voted_for", self.save_voted_for)?;
            f.member("append_entries", self.append_entries)?;
            f.member("save_snapshot", self.save_snapshot)?;
            f.member("remove_node", self.remove_node)
        })
    }
}

impl fmt::Display for OperationKindStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        display_json(self, f)
    }
}

#[derive(Debug, Default)]
pub(crate) struct StorageStatsCounters {
    stats: StorageStats,
}

impl StorageStatsCounters {
    pub(crate) fn as_ref(&self) -> &StorageStats {
        &self.stats
    }

    pub(crate) fn record_written(&mut self, kind: RecordKindMetric, bytes: u64) {
        self.stats.records_written.increment(kind, 1);
        self.stats.bytes_written.increment(kind, bytes);
        increment(&mut self.stats.unsynced_records, 1);
        increment(&mut self.stats.unsynced_bytes, bytes);
    }

    pub(crate) fn record_replayed(&mut self, kind: RecordKindMetric, bytes: u64) {
        self.stats.records_replayed.increment(kind, 1);
        self.stats.bytes_replayed.increment(kind, bytes);
    }

    pub(crate) fn segment_opened(&mut self, segment_id: u64, segment_len: u64) {
        self.stats.active_append_segment_id = segment_id;
        self.stats.active_append_segment_len_bytes = segment_len;
    }

    pub(crate) fn segment_written(&mut self, segment_len: u64) {
        self.stats.active_append_segment_len_bytes = segment_len;
    }

    pub(crate) fn segment_rotated(&mut self, segment_id: u64, segment_len: u64) {
        increment(&mut self.stats.segment_rotations, 1);
        self.segment_opened(segment_id, segment_len);
    }

    pub(crate) fn sync_requested(&mut self) {
        increment(&mut self.stats.syncs, 1);
    }

    pub(crate) fn segment_synced(&mut self) {
        increment(&mut self.stats.durable_syncs, 1);
        self.stats.unsynced_records = 0;
        self.stats.unsynced_bytes = 0;
    }

    pub(crate) fn replay_truncated(&mut self) {
        increment(&mut self.stats.replay_truncations, 1);
    }

    pub(crate) fn checksum_failed(&mut self) {
        increment(&mut self.stats.checksum_failures, 1);
    }

    pub(crate) fn node_created(&mut self) {
        increment(&mut self.stats.nodes_created, 1);
    }

    pub(crate) fn node_removed(&mut self) {
        increment(&mut self.stats.nodes_removed, 1);
    }

    pub(crate) fn rejected_operation(
        &mut self,
        operation: StorageOperationKind,
        error: NodeAccessErrorKind,
    ) {
        self.stats.rejected_operations.increment(operation, error);
    }

    pub(crate) fn snapshot_checkpoint_saved(&mut self) {
        increment(&mut self.stats.snapshot_checkpoints_saved, 1);
    }

    pub(crate) fn gc_ran(&mut self) {
        increment(&mut self.stats.gc_runs, 1);
    }

    pub(crate) fn gc_deleted_segments(&mut self, count: u64) {
        increment(&mut self.stats.gc_segments_deleted, count);
    }

    pub(crate) fn registry_loaded(&mut self, active_nodes: u64, removed_nodes: u64) {
        self.stats.active_nodes = active_nodes;
        self.stats.removed_nodes = removed_nodes;
    }

    pub(crate) fn checkpoint_index_loaded(&mut self, nodes: u64) {
        self.stats.checkpoint_index_nodes = nodes;
    }

    pub(crate) fn node_counts_changed(&mut self, active_nodes: u64, removed_nodes: u64) {
        self.stats.active_nodes = active_nodes;
        self.stats.removed_nodes = removed_nodes;
    }

    pub(crate) fn checkpoint_index_changed(&mut self, nodes: u64) {
        self.stats.checkpoint_index_nodes = nodes;
    }
}

impl RecordKindStats {
    fn increment(&mut self, kind: RecordKindMetric, value: u64) {
        match kind {
            RecordKindMetric::CurrentTerm => increment(&mut self.current_term, value),
            RecordKindMetric::VotedFor => increment(&mut self.voted_for, value),
            RecordKindMetric::LogAppend => increment(&mut self.log_append, value),
            RecordKindMetric::SnapshotCheckpoint => {
                increment(&mut self.snapshot_checkpoint, value);
            }
        }
    }
}

impl RejectedOperationStats {
    fn increment(&mut self, operation: StorageOperationKind, error: NodeAccessErrorKind) {
        match error {
            NodeAccessErrorKind::UnknownNode => self.unknown_nodes.increment(operation),
            NodeAccessErrorKind::RemovedNode => self.removed_nodes.increment(operation),
        }
    }
}

impl OperationKindStats {
    fn increment(&mut self, operation: StorageOperationKind) {
        match operation {
            StorageOperationKind::Load => increment(&mut self.load, 1),
            StorageOperationKind::SaveCurrentTerm => increment(&mut self.save_current_term, 1),
            StorageOperationKind::SaveVotedFor => increment(&mut self.save_voted_for, 1),
            StorageOperationKind::AppendEntries => increment(&mut self.append_entries, 1),
            StorageOperationKind::SaveSnapshot => increment(&mut self.save_snapshot, 1),
            StorageOperationKind::RemoveNode => increment(&mut self.remove_node, 1),
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

fn increment(counter: &mut u64, value: u64) {
    *counter = (*counter).saturating_add(value);
}

fn display_json<T: nojson::DisplayJson>(value: &T, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
        f,
        "{}",
        nojson::json(|json| nojson::DisplayJson::fmt(value, json))
    )
}
