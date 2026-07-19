//! Shared segmented storage model for Raft node state.

use crate::bytes::Bytes;
use crate::registry::{NodeMetadata, NodeRegistry, node_not_found_error, node_removed_error};
use crate::stats::{
    NodeAccessErrorKind, RecordKindMetric, StorageOperationKind, StorageStats, StorageStatsCounters,
};

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsStr,
    fmt,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const SEGMENT_FORMAT_MAGIC: &[u8; 4] = b"SKR1";
const SEGMENT_BASE_HEADER_LEN: usize = 8;
const SEGMENT_CHECKSUM_LEN: usize = 4;
const SEGMENT_HEADER_LEN: usize = SEGMENT_BASE_HEADER_LEN + SEGMENT_CHECKSUM_LEN;
const SEGMENT_FILE_SUFFIX: &str = ".segment";
const SEGMENT_ID_WIDTH: usize = 6;
const MANIFEST_FILE_NAME: &str = "manifest";
const MANIFEST_TMP_FILE_NAME: &str = "manifest.tmp";
const MANIFEST_VERSION: u64 = 1;
const CHECKPOINT_INDEX_FILE_NAME: &str = "checkpoints.json";
const CHECKPOINT_INDEX_TMP_FILE_NAME: &str = "checkpoints.json.tmp";
const CHECKPOINT_INDEX_VERSION: u64 = 1;
const DEFAULT_MAX_SEGMENT_LEN: u64 = 128 * 1024 * 1024;
const MAX_RECORD_BODY_LEN: u32 = 1024 * 1024 * 1024;
const MAX_SET_ITEMS: u64 = 1_000_000;

/// Storage synchronization policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// Synchronize durable data after every storage record.
    Strict,

    /// Synchronize durable data after record or byte thresholds are reached.
    ///
    /// A zero threshold is ignored. If both thresholds are zero, writes are
    /// synchronized only when [`StorageEngine::flush`] is called.
    Batch {
        /// Maximum number of unsynchronized records.
        max_records: usize,

        /// Maximum number of unsynchronized bytes.
        max_bytes: u64,
    },

    /// Never synchronize durable data explicitly.
    UnsafeNoSync,
}

/// Shared segmented storage engine.
#[derive(Debug)]
pub struct StorageEngine {
    dir: PathBuf,
    sync: SyncPolicy,
    stats: StorageStatsCounters,
    registry: NodeRegistry,
    checkpoint_index: CheckpointIndex,
    writer: SegmentWriter,
}

impl StorageEngine {
    /// Makes a new shared storage engine.
    pub fn new<P: AsRef<Path>>(dir: P, sync: SyncPolicy) -> io::Result<Self> {
        Self::with_max_segment_len(dir, sync, DEFAULT_MAX_SEGMENT_LEN)
    }

    /// Makes a new shared storage engine with a maximum append segment length.
    ///
    /// If a single record is larger than `max_segment_len`, it is written to an
    /// empty segment and that segment is allowed to exceed the limit.
    pub fn with_max_segment_len<P: AsRef<Path>>(
        dir: P,
        sync: SyncPolicy,
        max_segment_len: u64,
    ) -> io::Result<Self> {
        if max_segment_len == 0 {
            return Err(invalid_input("max segment length must be non-zero"));
        }

        let dir = dir.as_ref().to_path_buf();
        create_dir_all_synced(&dir, sync)?;
        let active_segment = select_active_append_segment(&dir)?;
        let stats = StorageStatsCounters::default();
        recover_storage_dir(&dir, active_segment, &stats)?;
        let registry = NodeRegistry::load(&dir)?;
        let checkpoint_index = CheckpointIndex::load(&dir, &registry)?;
        let writer = SegmentWriter::open(&dir, sync, active_segment, max_segment_len)?;
        Ok(Self {
            dir,
            sync,
            stats,
            registry,
            checkpoint_index,
            writer,
        })
    }

    /// Creates a Raft node in this storage instance.
    pub fn create_node(
        &mut self,
        node_id: noraft::NodeId,
        metadata: NodeMetadata,
    ) -> io::Result<()> {
        let mut registry = self.registry.clone();
        registry.create_node(node_id, metadata)?;
        let append = self.writer.append(
            node_id,
            &Record::SnapshotCheckpoint(initial_checkpoint()),
            &self.stats,
        )?;
        if should_sync_metadata(self.sync) {
            self.writer.flush(&self.stats)?;
        }

        let mut checkpoint_index = self.checkpoint_index.clone();
        checkpoint_index.set_checkpoint_position(node_id, append);
        checkpoint_index.save(&self.dir, self.sync)?;
        registry.save(&self.dir, self.sync)?;
        self.registry = registry;
        self.checkpoint_index = checkpoint_index;
        self.stats.node_created();
        Ok(())
    }

    /// Returns metadata for an active Raft node.
    pub fn node_metadata(&self, node_id: noraft::NodeId) -> Option<&NodeMetadata> {
        self.registry.metadata(node_id)
    }

    /// Returns all active Raft nodes and their metadata.
    pub fn nodes(&self) -> impl Iterator<Item = (noraft::NodeId, &NodeMetadata)> + '_ {
        self.registry.nodes()
    }

    /// Returns active Raft nodes that should be considered during process startup.
    pub fn startup_nodes(&self) -> impl Iterator<Item = (noraft::NodeId, &NodeMetadata)> + '_ {
        self.registry.startup_nodes()
    }

    /// Loads the current state for the given Raft node.
    pub fn load(&self, node_id: noraft::NodeId) -> io::Result<NodeState> {
        self.ensure_node_exists(node_id, StorageOperationKind::Load)?;
        replay_node_state(
            &self.dir,
            self.writer.active_segment,
            node_id,
            self.checkpoint_index.checkpoint_position(node_id),
            &self.stats,
        )
    }

    /// Loads the latest state of all non-removed nodes.
    pub fn load_all(&self) -> io::Result<BTreeMap<noraft::NodeId, NodeState>> {
        let active_node_ids = self.registry.active_node_ids();
        if active_node_ids.is_empty() {
            return Ok(BTreeMap::new());
        }

        let checkpoint_hints = self.checkpoint_index.checkpoint_positions(&active_node_ids);
        let mut replay = replay_storage_dir(
            &self.dir,
            self.writer.active_segment,
            Some(&active_node_ids),
            checkpoint_hints,
            &self.stats,
        )?;
        let mut nodes = BTreeMap::new();
        for node_id in active_node_ids {
            nodes.insert(node_id, replay.nodes.remove(&node_id).unwrap_or_default());
        }
        Ok(nodes)
    }

    /// Saves the current term for the given Raft node.
    pub fn save_current_term(
        &mut self,
        node_id: noraft::NodeId,
        term: noraft::Term,
    ) -> io::Result<()> {
        self.save_record(
            node_id,
            Record::CurrentTerm(term),
            StorageOperationKind::SaveCurrentTerm,
        )
    }

    /// Saves the node voted for in the current term.
    pub fn save_voted_for(
        &mut self,
        node_id: noraft::NodeId,
        voted_for: Option<noraft::NodeId>,
    ) -> io::Result<()> {
        self.save_record(
            node_id,
            Record::VotedFor(voted_for),
            StorageOperationKind::SaveVotedFor,
        )
    }

    /// Appends log entries and their command payloads.
    pub fn append_entries(&mut self, node_id: noraft::NodeId, append: LogAppend) -> io::Result<()> {
        self.save_record(
            node_id,
            Record::Append(append),
            StorageOperationKind::AppendEntries,
        )
    }

    /// Saves a snapshot checkpoint.
    ///
    /// The checkpoint suffix must start at the snapshot's last included
    /// position.
    pub fn save_snapshot(
        &mut self,
        node_id: noraft::NodeId,
        checkpoint: SnapshotCheckpoint,
    ) -> io::Result<()> {
        self.ensure_node_exists(node_id, StorageOperationKind::SaveSnapshot)?;
        checkpoint.validate()?;
        let append = self.writer.append(
            node_id,
            &Record::SnapshotCheckpoint(checkpoint),
            &self.stats,
        )?;
        if should_sync_metadata(self.sync) {
            self.writer.flush(&self.stats)?;
        }

        let mut checkpoint_index = self.checkpoint_index.clone();
        checkpoint_index.set_checkpoint_position(node_id, append);
        checkpoint_index.save(&self.dir, self.sync)?;
        self.checkpoint_index = checkpoint_index;
        self.collect_garbage()?;
        self.stats.snapshot_checkpoint_saved();
        Ok(())
    }

    /// Marks the given Raft node as removed and reserves its node ID.
    pub fn remove_node(&mut self, node_id: noraft::NodeId) -> io::Result<()> {
        self.ensure_node_exists(node_id, StorageOperationKind::RemoveNode)?;
        let mut registry = self.registry.clone();
        registry.remove_node(node_id)?;
        registry.save(&self.dir, self.sync)?;
        self.registry = registry;

        let mut checkpoint_index = self.checkpoint_index.clone();
        checkpoint_index.remove_node(node_id);
        checkpoint_index.save(&self.dir, self.sync)?;
        self.checkpoint_index = checkpoint_index;
        self.collect_garbage()?;
        self.stats.node_removed();
        Ok(())
    }

    /// Flushes pending writes.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush(&self.stats)?;
        self.stats.flushed();
        Ok(())
    }

    /// Removes all storage data managed by this engine.
    pub fn remove_all(self) -> io::Result<()> {
        drop(self.writer);
        remove_storage_dir_if_exists(&self.dir, self.sync)
    }

    /// Returns the storage directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Returns the storage synchronization policy.
    pub fn sync_policy(&self) -> SyncPolicy {
        self.sync
    }

    /// Returns a snapshot of storage statistics since this engine was opened.
    pub fn stats(&self) -> StorageStats {
        let mut stats = self.stats.snapshot();
        stats.active_nodes = usize_to_u64(self.registry.active_node_count());
        stats.removed_nodes = usize_to_u64(self.registry.removed_node_count());
        stats.checkpoint_index_nodes = usize_to_u64(self.checkpoint_index.len());
        stats.active_append_segment_id = self.writer.active_segment.id.get();
        stats.active_append_segment_len_bytes = self.writer.segment_len;
        stats.unsynced_records = usize_to_u64(self.writer.unsynced_records);
        stats.unsynced_bytes = self.writer.unsynced_bytes;
        stats
    }

    fn save_record(
        &mut self,
        node_id: noraft::NodeId,
        record: Record,
        operation: StorageOperationKind,
    ) -> io::Result<()> {
        self.ensure_node_exists(node_id, operation)?;
        self.writer.append(node_id, &record, &self.stats)?;
        Ok(())
    }

    fn ensure_node_exists(
        &self,
        node_id: noraft::NodeId,
        operation: StorageOperationKind,
    ) -> io::Result<()> {
        if self.registry.is_active(node_id) {
            return Ok(());
        }
        if self.registry.is_removed(node_id) {
            self.stats
                .rejected_operation(operation, NodeAccessErrorKind::RemovedNode);
            return Err(node_removed_error());
        }
        self.stats
            .rejected_operation(operation, NodeAccessErrorKind::UnknownNode);
        Err(node_not_found_error())
    }

    fn collect_garbage(&self) -> io::Result<()> {
        self.stats.gc_ran();
        let active_node_ids = self.registry.active_node_ids();
        let Some(barrier) = self.checkpoint_index.gc_barrier(&active_node_ids) else {
            return Ok(());
        };

        let mut deleted_segments = 0;
        for segment in discover_segment_paths(&self.dir)? {
            if segment.name.kind != SegmentKind::Append {
                continue;
            }
            if segment.name == self.writer.active_segment {
                continue;
            }
            if !barrier.allows_delete(segment.name) {
                continue;
            }

            match std::fs::remove_file(&segment.path) {
                Ok(()) => deleted_segments += 1,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }

        if deleted_segments != 0 {
            self.stats.gc_deleted_segments(deleted_segments);
        }
        if deleted_segments != 0 && should_sync_metadata(self.sync) {
            sync_dir(&self.dir)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct SegmentWriter {
    dir: PathBuf,
    active_segment: SegmentName,
    file: File,
    sync: SyncPolicy,
    segment_len: u64,
    max_segment_len: u64,
    unsynced_records: usize,
    unsynced_bytes: u64,
}

impl SegmentWriter {
    fn open(
        dir: &Path,
        sync: SyncPolicy,
        active_segment: SegmentName,
        max_segment_len: u64,
    ) -> io::Result<Self> {
        let segment_path = active_segment.path(dir);
        let file_existed = segment_path.exists();
        let mut file = open_active_segment_file(&segment_path)?;
        if !file_existed && should_sync_metadata(sync) {
            sync_parent_dir(&segment_path)?;
        }
        let segment_len = file.seek(SeekFrom::End(0))?;
        Manifest::active_append(active_segment).save(dir, sync)?;

        Ok(Self {
            dir: dir.to_path_buf(),
            active_segment,
            file,
            sync,
            segment_len,
            max_segment_len,
            unsynced_records: 0,
            unsynced_bytes: 0,
        })
    }

    fn append(
        &mut self,
        node_id: noraft::NodeId,
        record: &Record,
        stats: &StorageStatsCounters,
    ) -> io::Result<RecordPosition> {
        debug_assert_eq!(self.active_segment.kind, SegmentKind::Append);
        let frame = encode_record_frame(node_id, record)?;
        let written_bytes =
            u64::try_from(frame.len()).map_err(|_| invalid_input("record is too large"))?;
        if self.should_rotate(written_bytes) {
            self.rotate(stats)?;
        }
        let record_position = RecordPosition {
            segment: self.active_segment,
            offset: self.segment_len,
        };
        self.file.write_all(&frame)?;
        self.segment_len = self
            .segment_len
            .checked_add(written_bytes)
            .ok_or_else(|| invalid_data("segment length overflow"))?;
        stats.record_written(record.metric_kind(), written_bytes);
        self.after_write(written_bytes, stats)?;
        Ok(record_position)
    }

    fn should_rotate(&self, written_bytes: u64) -> bool {
        if self.segment_len == 0 {
            return false;
        }
        self.segment_len
            .checked_add(written_bytes)
            .is_none_or(|len| self.max_segment_len < len)
    }

    fn rotate(&mut self, stats: &StorageStatsCounters) -> io::Result<()> {
        self.flush(stats)?;
        let next_segment = self.active_segment.next_append()?;
        let segment_path = next_segment.path(&self.dir);
        let file = create_active_segment_file(&segment_path)?;
        if should_sync_metadata(self.sync) {
            sync_parent_dir(&segment_path)?;
        }
        Manifest::active_append(next_segment).save(&self.dir, self.sync)?;

        self.active_segment = next_segment;
        self.file = file;
        self.segment_len = 0;
        self.unsynced_records = 0;
        self.unsynced_bytes = 0;
        stats.segment_rotated();
        Ok(())
    }

    fn after_write(&mut self, written_bytes: u64, stats: &StorageStatsCounters) -> io::Result<()> {
        match self.sync {
            SyncPolicy::Strict => self.sync_data(stats),
            SyncPolicy::Batch {
                max_records,
                max_bytes,
            } => {
                self.unsynced_records += 1;
                self.unsynced_bytes += written_bytes;
                let records_reached = max_records != 0 && max_records <= self.unsynced_records;
                let bytes_reached = max_bytes != 0 && max_bytes <= self.unsynced_bytes;
                if records_reached || bytes_reached {
                    self.sync_data(stats)?;
                }
                Ok(())
            }
            SyncPolicy::UnsafeNoSync => Ok(()),
        }
    }

    fn sync_data(&mut self, stats: &StorageStatsCounters) -> io::Result<()> {
        self.file.sync_data()?;
        self.unsynced_records = 0;
        self.unsynced_bytes = 0;
        stats.durable_synced();
        Ok(())
    }

    fn flush(&mut self, stats: &StorageStatsCounters) -> io::Result<()> {
        match self.sync {
            SyncPolicy::UnsafeNoSync => {}
            SyncPolicy::Strict | SyncPolicy::Batch { .. } => self.sync_data(stats)?,
        }
        Ok(())
    }
}

/// A log append operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogAppend {
    /// Log entries emitted by `noraft`.
    entries: noraft::LogEntries,

    /// Command payloads keyed by log index.
    commands: BTreeMap<noraft::LogIndex, Bytes>,
}

impl LogAppend {
    /// Makes a new append operation after validating command payload mapping.
    pub fn new(
        entries: noraft::LogEntries,
        commands: BTreeMap<noraft::LogIndex, Bytes>,
    ) -> io::Result<Self> {
        let this = Self { entries, commands };
        this.validate()?;
        Ok(this)
    }

    /// Returns the log entries emitted by `noraft`.
    pub fn entries(&self) -> &noraft::LogEntries {
        &self.entries
    }

    /// Returns command payloads keyed by log index.
    pub fn commands(&self) -> &BTreeMap<noraft::LogIndex, Bytes> {
        &self.commands
    }

    fn validate(&self) -> io::Result<()> {
        for (position, entry) in self.entries.iter_with_positions() {
            if entry == noraft::LogEntry::Command && !self.commands.contains_key(&position.index) {
                return Err(invalid_input("missing command payload"));
            }
        }

        for index in self.commands.keys().copied() {
            if self.entries.get_entry(index) != Some(noraft::LogEntry::Command) {
                return Err(invalid_input(
                    "command payload index does not match a command entry",
                ));
            }
        }

        Ok(())
    }
}

/// Snapshot data saved by the storage backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Last log position included in the snapshot.
    pub last_included: noraft::LogPosition,

    /// Cluster configuration at `last_included`.
    pub config: noraft::ClusterConfig,

    /// User-defined snapshot payload.
    pub data: Bytes,
}

/// Snapshot checkpoint data saved by the storage backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotCheckpoint {
    /// Current term at the checkpoint.
    pub current_term: noraft::Term,

    /// Node voted for in the current term at the checkpoint.
    pub voted_for: Option<noraft::NodeId>,

    /// Latest snapshot at the checkpoint.
    pub snapshot: Snapshot,

    /// Log suffix retained after the snapshot.
    ///
    /// The suffix must start at `snapshot.last_included`.
    pub suffix: LogAppend,
}

impl SnapshotCheckpoint {
    fn validate(&self) -> io::Result<()> {
        self.suffix.validate()?;
        if self.suffix.entries.prev_position() != self.snapshot.last_included {
            return Err(invalid_input(
                "checkpoint suffix must start at the snapshot position",
            ));
        }
        Ok(())
    }

    fn into_state(self) -> io::Result<NodeState> {
        self.validate()?;
        let mut state = NodeState::default();
        state.apply_current_term(self.current_term);
        state.apply_voted_for(self.voted_for);
        state.apply_snapshot(self.snapshot)?;
        state.apply_append_owned(self.suffix)?;
        Ok(state)
    }
}

fn initial_checkpoint() -> SnapshotCheckpoint {
    SnapshotCheckpoint {
        current_term: noraft::Term::ZERO,
        voted_for: None,
        snapshot: Snapshot {
            last_included: noraft::LogPosition::ZERO,
            config: noraft::ClusterConfig::new(),
            data: Bytes::default(),
        },
        suffix: LogAppend::new(
            noraft::LogEntries::new(noraft::LogPosition::ZERO),
            BTreeMap::new(),
        )
        .expect("bug: empty initial checkpoint suffix should be valid"),
    }
}

/// Loaded persistent state for a Raft node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeState {
    /// Current term.
    pub current_term: noraft::Term,

    /// Node voted for in the current term.
    pub voted_for: Option<noraft::NodeId>,

    /// Local Raft log.
    pub log: noraft::Log,

    /// Command payloads keyed by log index.
    pub commands: BTreeMap<noraft::LogIndex, Bytes>,

    /// Latest snapshot.
    pub snapshot: Option<Snapshot>,
}

impl Default for NodeState {
    fn default() -> Self {
        Self {
            current_term: noraft::Term::ZERO,
            voted_for: None,
            log: noraft::Log::new(
                noraft::ClusterConfig::new(),
                noraft::LogEntries::new(noraft::LogPosition::ZERO),
            ),
            commands: BTreeMap::new(),
            snapshot: None,
        }
    }
}

impl NodeState {
    /// Applies a current-term record to this state.
    pub fn apply_current_term(&mut self, term: noraft::Term) {
        self.current_term = term;
    }

    /// Applies a voted-for record to this state.
    pub fn apply_voted_for(&mut self, voted_for: Option<noraft::NodeId>) {
        self.voted_for = voted_for;
    }

    /// Applies a log append record to this state.
    pub fn apply_append(&mut self, append: &LogAppend) -> io::Result<()> {
        self.apply_append_owned(append.clone())
    }

    fn apply_append_owned(&mut self, append: LogAppend) -> io::Result<()> {
        append.validate()?;
        if !self.log.entries().contains(append.entries.prev_position()) {
            return Err(invalid_data("append anchor does not exist in local log"));
        }

        let keep_len = append.entries.prev_position().index.get()
            - self.log.entries().prev_position().index.get();
        let keep_len = usize::try_from(keep_len)
            .map_err(|_| invalid_data("log suffix length exceeds usize"))?;

        let mut entries = self.log.entries().clone();
        entries.truncate(keep_len);
        for entry in append.entries.iter() {
            entries.push(entry);
        }
        self.log = noraft::Log::new(self.log.snapshot_config().clone(), entries);

        let prev_index = append.entries.prev_position().index;
        self.commands.retain(|index, _| *index <= prev_index);
        self.commands.extend(append.commands);
        Ok(())
    }

    /// Applies snapshot data to this state.
    pub fn apply_snapshot(&mut self, snapshot: Snapshot) -> io::Result<()> {
        let current_entries = self.log.entries();
        if snapshot.last_included.index < current_entries.prev_position().index {
            return Err(invalid_data("snapshot is older than the current snapshot"));
        }

        let entries = if let Some(entries) = current_entries.since(snapshot.last_included) {
            entries
        } else if current_entries.last_position().index < snapshot.last_included.index {
            noraft::LogEntries::new(snapshot.last_included)
        } else {
            return Err(invalid_data("snapshot position conflicts with local log"));
        };

        if entries.prev_position() != snapshot.last_included {
            return Err(invalid_data("invalid snapshot log suffix"));
        }

        self.commands
            .retain(|index, _| snapshot.last_included.index < *index);
        self.log = noraft::Log::new(snapshot.config.clone(), entries);
        self.snapshot = Some(snapshot);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Record {
    CurrentTerm(noraft::Term),
    VotedFor(Option<noraft::NodeId>),
    Append(LogAppend),
    SnapshotCheckpoint(SnapshotCheckpoint),
}

impl Record {
    fn metric_kind(&self) -> RecordKindMetric {
        match self {
            Self::CurrentTerm(_) => RecordKindMetric::CurrentTerm,
            Self::VotedFor(_) => RecordKindMetric::VotedFor,
            Self::Append(_) => RecordKindMetric::LogAppend,
            Self::SnapshotCheckpoint(_) => RecordKindMetric::SnapshotCheckpoint,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeRecord {
    node_id: noraft::NodeId,
    record: Record,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SegmentId(u64);

impl SegmentId {
    const FIRST: Self = Self(1);

    fn new(id: u64) -> Option<Self> {
        (id != 0).then_some(Self(id))
    }

    fn get(self) -> u64 {
        self.0
    }

    fn next(self) -> io::Result<Self> {
        let id = self
            .0
            .checked_add(1)
            .ok_or_else(|| invalid_data("segment ID overflow"))?;
        Self::new(id).ok_or_else(|| invalid_data("segment ID overflow"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SegmentKind {
    Append,
    Rewrite,
}

impl SegmentKind {
    fn prefix(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Rewrite => "rewrite",
        }
    }

    fn parse_prefix(prefix: &str) -> Option<Self> {
        match prefix {
            "append" => Some(Self::Append),
            "rewrite" => Some(Self::Rewrite),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SegmentName {
    kind: SegmentKind,
    id: SegmentId,
}

impl SegmentName {
    fn first_append() -> Self {
        Self {
            kind: SegmentKind::Append,
            id: SegmentId::FIRST,
        }
    }

    fn next_append(self) -> io::Result<Self> {
        debug_assert_eq!(self.kind, SegmentKind::Append);
        Ok(Self {
            kind: SegmentKind::Append,
            id: self.id.next()?,
        })
    }

    fn parse_file_name(file_name: &OsStr) -> Option<Self> {
        let file_name = file_name.to_str()?;
        let name = file_name.strip_suffix(SEGMENT_FILE_SUFFIX)?;
        let (prefix, id) = name.split_once('-')?;
        let kind = SegmentKind::parse_prefix(prefix)?;
        if id.len() != SEGMENT_ID_WIDTH || !id.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        let id = id.parse().ok().and_then(SegmentId::new)?;
        Some(Self { kind, id })
    }

    fn parse_str(s: &str) -> Option<Self> {
        Self::parse_file_name(OsStr::new(s))
    }

    fn path(self, dir: &Path) -> PathBuf {
        dir.join(self.file_name())
    }

    fn file_name(self) -> String {
        format!(
            "{}-{:0width$}{}",
            self.kind.prefix(),
            self.id.get(),
            SEGMENT_FILE_SUFFIX,
            width = SEGMENT_ID_WIDTH
        )
    }
}

fn select_active_append_segment(dir: &Path) -> io::Result<SegmentName> {
    let hinted_active_segment = match Manifest::load_advisory(dir)? {
        Some(manifest) => {
            select_active_append_segment_from_hint(dir, manifest.active_append_segment)?
        }
        None => None,
    };
    if let Some(active_segment) = hinted_active_segment {
        return Ok(active_segment);
    }
    select_active_append_segment_by_scan(dir)
}

fn select_active_append_segment_from_hint(
    dir: &Path,
    hinted_segment: SegmentName,
) -> io::Result<Option<SegmentName>> {
    if !segment_file_exists(dir, hinted_segment)? {
        return Ok(None);
    }

    let mut active_segment = hinted_segment;
    loop {
        let next_segment = active_segment.next_append()?;
        if !segment_file_exists(dir, next_segment)? {
            return Ok(Some(active_segment));
        }
        active_segment = next_segment;
    }
}

fn select_active_append_segment_by_scan(dir: &Path) -> io::Result<SegmentName> {
    Ok(discover_segment_paths(dir)?
        .into_iter()
        .map(|segment| segment.name)
        .filter(|segment| segment.kind == SegmentKind::Append)
        .max()
        .unwrap_or_else(SegmentName::first_append))
}

fn segment_file_exists(dir: &Path, name: SegmentName) -> io::Result<bool> {
    match std::fs::metadata(name.path(dir)) {
        Ok(metadata) => Ok(metadata.is_file()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

#[derive(Debug)]
struct SegmentPath {
    name: SegmentName,
    path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RecordPosition {
    segment: SegmentName,
    offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Manifest {
    active_append_segment: SegmentName,
}

impl Manifest {
    fn active_append(active_append_segment: SegmentName) -> Self {
        debug_assert_eq!(active_append_segment.kind, SegmentKind::Append);
        Self {
            active_append_segment,
        }
    }

    fn load_advisory(dir: &Path) -> io::Result<Option<Self>> {
        let path = dir.join(MANIFEST_FILE_NAME);
        let mut text = String::new();
        match File::open(path) {
            Ok(mut file) => {
                file.read_to_string(&mut text)?;
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        }

        let Ok(json) = nojson::RawJsonOwned::parse(text) else {
            return Ok(None);
        };
        Ok(parse_manifest(json.value()).ok())
    }

    fn save(self, dir: &Path, sync: SyncPolicy) -> io::Result<()> {
        let path = dir.join(MANIFEST_FILE_NAME);
        let tmp_path = dir.join(MANIFEST_TMP_FILE_NAME);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(format_manifest(self).as_bytes())?;
        if should_sync_metadata(sync) {
            file.sync_all()?;
        }
        drop(file);

        std::fs::rename(&tmp_path, &path)?;
        if should_sync_metadata(sync) {
            sync_dir(dir)?;
        }
        Ok(())
    }
}

fn parse_manifest(value: nojson::RawJsonValue<'_, '_>) -> Result<Manifest, nojson::JsonParseError> {
    let version_value = value.to_member("version")?.required()?;
    let version: u64 = version_value.try_into()?;
    if version != MANIFEST_VERSION {
        return Err(version_value.invalid("unsupported manifest version"));
    }

    let active_segment_value = value.to_member("active_append_segment")?.required()?;
    let active_segment_name: String = active_segment_value.try_into()?;
    let active_append_segment = SegmentName::parse_str(&active_segment_name)
        .ok_or_else(|| active_segment_value.invalid("invalid active append segment name"))?;
    if active_append_segment.kind != SegmentKind::Append {
        return Err(active_segment_value.invalid("active segment must be an append segment"));
    }

    Ok(Manifest::active_append(active_append_segment))
}

fn format_manifest(manifest: Manifest) -> String {
    let mut text = nojson::json(|f| {
        f.set_indent_size(2);
        f.set_spacing(true);
        f.object(|f| {
            f.member("version", MANIFEST_VERSION)?;
            f.member(
                "active_append_segment",
                manifest.active_append_segment.file_name(),
            )
        })
    })
    .to_string();
    text.push('\n');
    text
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct CheckpointIndex {
    nodes: BTreeMap<noraft::NodeId, CheckpointNodeIndex>,
}

impl CheckpointIndex {
    fn load(dir: &Path, registry: &NodeRegistry) -> io::Result<Self> {
        let path = dir.join(CHECKPOINT_INDEX_FILE_NAME);
        let mut text = String::new();
        match File::open(&path) {
            Ok(mut file) => {
                file.read_to_string(&mut text)?;
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(e) => return Err(e),
        }

        let json = nojson::RawJsonOwned::parse(text).map_err(invalid_json)?;
        let mut index = parse_checkpoint_index(json.value()).map_err(invalid_json)?;
        index
            .nodes
            .retain(|node_id, _| registry.is_active(*node_id));
        index.validate_checkpoint_positions(dir)?;
        Ok(index)
    }

    fn save(&self, dir: &Path, sync: SyncPolicy) -> io::Result<()> {
        let path = dir.join(CHECKPOINT_INDEX_FILE_NAME);
        let tmp_path = dir.join(CHECKPOINT_INDEX_TMP_FILE_NAME);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(format_checkpoint_index(self).as_bytes())?;
        if should_sync_metadata(sync) {
            file.sync_all()?;
        }
        drop(file);

        std::fs::rename(&tmp_path, &path)?;
        if should_sync_metadata(sync) {
            sync_dir(dir)?;
        }
        Ok(())
    }

    fn checkpoint_position(&self, node_id: noraft::NodeId) -> Option<RecordPosition> {
        self.nodes
            .get(&node_id)
            .map(|state| state.checkpoint_position)
    }

    fn checkpoint_positions(
        &self,
        node_ids: &BTreeSet<noraft::NodeId>,
    ) -> Option<BTreeMap<noraft::NodeId, RecordPosition>> {
        let mut positions = BTreeMap::new();
        for node_id in node_ids {
            positions.insert(*node_id, self.checkpoint_position(*node_id)?);
        }
        Some(positions)
    }

    fn gc_barrier(&self, active_node_ids: &BTreeSet<noraft::NodeId>) -> Option<GcBarrier> {
        let mut oldest_checkpoint_segment = None;
        for node_id in active_node_ids {
            let checkpoint_segment = self.checkpoint_position(*node_id)?.segment;
            oldest_checkpoint_segment = Some(
                oldest_checkpoint_segment
                    .map(|oldest: SegmentName| oldest.min(checkpoint_segment))
                    .unwrap_or(checkpoint_segment),
            );
        }

        match oldest_checkpoint_segment {
            Some(segment) => Some(GcBarrier::Before(segment)),
            None => Some(GcBarrier::AllInactiveAppendSegments),
        }
    }

    fn set_checkpoint_position(&mut self, node_id: noraft::NodeId, position: RecordPosition) {
        debug_assert_eq!(position.segment.kind, SegmentKind::Append);
        self.nodes.insert(
            node_id,
            CheckpointNodeIndex {
                checkpoint_position: position,
            },
        );
    }

    fn remove_node(&mut self, node_id: noraft::NodeId) {
        self.nodes.remove(&node_id);
    }

    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn validate_checkpoint_positions(&self, dir: &Path) -> io::Result<()> {
        for (node_id, state) in &self.nodes {
            validate_checkpoint_position(dir, *node_id, state.checkpoint_position)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CheckpointNodeIndex {
    checkpoint_position: RecordPosition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GcBarrier {
    AllInactiveAppendSegments,
    Before(SegmentName),
}

impl GcBarrier {
    fn allows_delete(self, segment: SegmentName) -> bool {
        match self {
            Self::AllInactiveAppendSegments => true,
            Self::Before(checkpoint_segment) => segment < checkpoint_segment,
        }
    }
}

fn parse_checkpoint_index(
    value: nojson::RawJsonValue<'_, '_>,
) -> Result<CheckpointIndex, nojson::JsonParseError> {
    let version_value = value.to_member("version")?.required()?;
    let version: u64 = version_value.try_into()?;
    if version != CHECKPOINT_INDEX_VERSION {
        return Err(version_value.invalid("unsupported checkpoint index version"));
    }

    let nodes_value = value.to_member("nodes")?.required()?;
    let mut index = CheckpointIndex::default();
    for (key, value) in nodes_value.to_object()? {
        let key_text = key.to_unquoted_string_str()?;
        let node_id = key_text
            .parse()
            .map(noraft::NodeId::new)
            .map_err(|e| key.invalid(e))?;
        let node_index = parse_checkpoint_node_index(value)?;
        if index.nodes.insert(node_id, node_index).is_some() {
            return Err(key.invalid("duplicate node ID"));
        }
    }
    Ok(index)
}

fn parse_checkpoint_node_index(
    value: nojson::RawJsonValue<'_, '_>,
) -> Result<CheckpointNodeIndex, nojson::JsonParseError> {
    let segment_value = value.to_member("checkpoint_segment")?.required()?;
    let segment_name: String = segment_value.try_into()?;
    let checkpoint_segment = SegmentName::parse_str(&segment_name)
        .ok_or_else(|| segment_value.invalid("invalid checkpoint segment name"))?;
    if checkpoint_segment.kind != SegmentKind::Append {
        return Err(segment_value.invalid("checkpoint segment must be an append segment"));
    }

    let offset_value = value.to_member("checkpoint_offset")?.required()?;
    let checkpoint_offset = offset_value.try_into()?;

    Ok(CheckpointNodeIndex {
        checkpoint_position: RecordPosition {
            segment: checkpoint_segment,
            offset: checkpoint_offset,
        },
    })
}

fn format_checkpoint_index(index: &CheckpointIndex) -> String {
    let mut text = nojson::json(|f| {
        f.set_indent_size(2);
        f.set_spacing(true);
        f.object(|f| {
            f.member("version", CHECKPOINT_INDEX_VERSION)?;
            f.member("nodes", CheckpointIndexNodesJson(index))
        })
    })
    .to_string();
    text.push('\n');
    text
}

struct CheckpointIndexNodesJson<'a>(&'a CheckpointIndex);

impl nojson::DisplayJson for CheckpointIndexNodesJson<'_> {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> fmt::Result {
        f.object(|f| {
            for (node_id, state) in &self.0.nodes {
                f.member(node_id.get(), CheckpointNodeIndexJson(state))?;
            }
            Ok(())
        })
    }
}

struct CheckpointNodeIndexJson<'a>(&'a CheckpointNodeIndex);

impl nojson::DisplayJson for CheckpointNodeIndexJson<'_> {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> fmt::Result {
        f.object(|f| {
            f.member(
                "checkpoint_segment",
                self.0.checkpoint_position.segment.file_name(),
            )?;
            f.member("checkpoint_offset", self.0.checkpoint_position.offset)
        })
    }
}

fn validate_checkpoint_position(
    dir: &Path,
    node_id: noraft::NodeId,
    position: RecordPosition,
) -> io::Result<()> {
    if !segment_file_exists(dir, position.segment)? {
        return Err(invalid_data("checkpoint index segment does not exist"));
    }

    let segment_path = position.segment.path(dir);
    let mut file = OpenOptions::new().read(true).open(&segment_path)?;
    let file_len = file.metadata()?.len();
    if file_len < position.offset {
        return Err(invalid_data(
            "checkpoint index offset exceeds segment length",
        ));
    }
    file.seek(SeekFrom::Start(position.offset))?;
    let body = read_record_body(&mut file, None)?
        .ok_or_else(|| invalid_data("checkpoint index offset does not point to a record"))?;
    let node_record = decode_node_record(&body)?;
    if node_record.node_id != node_id {
        return Err(invalid_data("checkpoint index node ID mismatch"));
    }
    if !matches!(node_record.record, Record::SnapshotCheckpoint(_)) {
        return Err(invalid_data(
            "checkpoint index does not point to a checkpoint",
        ));
    }
    Ok(())
}

#[derive(Debug, Default)]
struct ReplayState {
    nodes: BTreeMap<noraft::NodeId, NodeState>,
}

impl ReplayState {
    fn apply(&mut self, node_record: NodeRecord) -> io::Result<()> {
        let NodeRecord { node_id, record } = node_record;
        let state = self.nodes.entry(node_id).or_default();
        apply_record_to_state(state, record)
    }
}

fn apply_record_to_state(state: &mut NodeState, record: Record) -> io::Result<()> {
    match record {
        Record::CurrentTerm(term) => {
            state.apply_current_term(term);
            Ok(())
        }
        Record::VotedFor(voted_for) => {
            state.apply_voted_for(voted_for);
            Ok(())
        }
        Record::Append(append) => state.apply_append_owned(append),
        Record::SnapshotCheckpoint(checkpoint) => {
            *state = checkpoint.into_state()?;
            Ok(())
        }
    }
}

fn replay_node_state(
    dir: &Path,
    active_segment: SegmentName,
    node_id: noraft::NodeId,
    checkpoint_hint: Option<RecordPosition>,
    stats: &StorageStatsCounters,
) -> io::Result<NodeState> {
    let mut target_nodes = BTreeSet::new();
    target_nodes.insert(node_id);
    let checkpoint_positions = find_checkpoint_positions_from(
        dir,
        active_segment,
        Some(&target_nodes),
        checkpoint_hint,
        stats,
    )?;
    let checkpoint_position = checkpoint_positions.get(&node_id).copied();

    let mut state = NodeState::default();
    for segment in discover_segment_paths(dir)? {
        if checkpoint_position.is_some_and(|checkpoint| segment.name < checkpoint.segment) {
            continue;
        }
        let allow_partial = segment.name == active_segment;
        replay_node_segment(
            &segment,
            allow_partial,
            node_id,
            checkpoint_position,
            &mut state,
            stats,
        )?;
    }
    Ok(state)
}

fn replay_storage_dir(
    dir: &Path,
    active_segment: SegmentName,
    node_filter: Option<&BTreeSet<noraft::NodeId>>,
    checkpoint_hints: Option<BTreeMap<noraft::NodeId, RecordPosition>>,
    stats: &StorageStatsCounters,
) -> io::Result<ReplayState> {
    let (checkpoint_positions, replay_start_segment) =
        checkpoint_positions_for_replay(dir, active_segment, node_filter, checkpoint_hints, stats)?;
    let mut replay = ReplayState::default();
    for segment in discover_segment_paths(dir)? {
        if replay_start_segment.is_some_and(|start| segment.name < start) {
            continue;
        }
        let allow_partial = segment.name == active_segment;
        replay_segment(
            &segment,
            allow_partial,
            node_filter,
            &checkpoint_positions,
            &mut replay,
            stats,
        )?;
    }
    Ok(replay)
}

fn checkpoint_positions_for_replay(
    dir: &Path,
    active_segment: SegmentName,
    node_filter: Option<&BTreeSet<noraft::NodeId>>,
    checkpoint_hints: Option<BTreeMap<noraft::NodeId, RecordPosition>>,
    stats: &StorageStatsCounters,
) -> io::Result<(
    BTreeMap<noraft::NodeId, RecordPosition>,
    Option<SegmentName>,
)> {
    let Some(mut checkpoint_positions) = checkpoint_hints else {
        return find_checkpoint_positions(dir, active_segment, node_filter, stats)
            .map(|positions| (positions, None));
    };

    let Some(start_position) = checkpoint_positions.values().copied().min() else {
        return Ok((checkpoint_positions, None));
    };
    let discovered = find_checkpoint_positions_from(
        dir,
        active_segment,
        node_filter,
        Some(start_position),
        stats,
    )?;
    checkpoint_positions.extend(discovered);
    Ok((checkpoint_positions, Some(start_position.segment)))
}

fn recover_storage_dir(
    dir: &Path,
    active_segment: SegmentName,
    stats: &StorageStatsCounters,
) -> io::Result<()> {
    for segment in discover_segment_paths(dir)? {
        let allow_partial = segment.name == active_segment;
        scan_segment(&segment.path, allow_partial, stats)?;
    }
    Ok(())
}

fn discover_segment_paths(dir: &Path) -> io::Result<Vec<SegmentPath>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };

    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let Some(name) = SegmentName::parse_file_name(&entry.file_name()) else {
            continue;
        };
        paths.push(SegmentPath {
            name,
            path: entry.path(),
        });
    }
    paths.sort_by_key(|segment| segment.name);
    Ok(paths)
}

fn find_checkpoint_positions(
    dir: &Path,
    active_segment: SegmentName,
    node_filter: Option<&BTreeSet<noraft::NodeId>>,
    stats: &StorageStatsCounters,
) -> io::Result<BTreeMap<noraft::NodeId, RecordPosition>> {
    find_checkpoint_positions_from(dir, active_segment, node_filter, None, stats)
}

fn find_checkpoint_positions_from(
    dir: &Path,
    active_segment: SegmentName,
    node_filter: Option<&BTreeSet<noraft::NodeId>>,
    start_position: Option<RecordPosition>,
    stats: &StorageStatsCounters,
) -> io::Result<BTreeMap<noraft::NodeId, RecordPosition>> {
    let mut checkpoints = BTreeMap::new();
    for segment in discover_segment_paths(dir)? {
        if start_position.is_some_and(|start| segment.name < start.segment) {
            continue;
        }
        let allow_partial = segment.name == active_segment;
        let start_offset = start_position
            .filter(|start| segment.name == start.segment)
            .map(|start| start.offset);
        scan_checkpoint_positions(
            &segment,
            allow_partial,
            node_filter,
            start_offset,
            &mut checkpoints,
            stats,
        )?;
    }
    Ok(checkpoints)
}

fn scan_checkpoint_positions(
    segment: &SegmentPath,
    allow_partial: bool,
    node_filter: Option<&BTreeSet<noraft::NodeId>>,
    start_offset: Option<u64>,
    checkpoints: &mut BTreeMap<noraft::NodeId, RecordPosition>,
    stats: &StorageStatsCounters,
) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(allow_partial)
        .open(&segment.path)?;
    let file_len = file.metadata()?.len();
    let start_offset = start_offset.unwrap_or(0);
    if file_len < start_offset {
        return Err(invalid_data(
            "checkpoint scan offset exceeds segment length",
        ));
    }
    file.seek(SeekFrom::Start(start_offset))?;

    loop {
        let record_start = file.stream_position()?;
        let Some(body) = read_record_body(&mut file, Some(stats))? else {
            if record_start == file_len {
                break;
            }
            if allow_partial {
                stats.replay_truncated();
                file.set_len(record_start)?;
                file.seek(SeekFrom::Start(record_start))?;
                break;
            }
            return Err(invalid_data("partial record in inactive segment"));
        };

        let node_id = decode_record_node_id(&body)?;
        if node_filter.is_none_or(|nodes| nodes.contains(&node_id)) {
            let node_record = decode_node_record(&body)?;
            if matches!(node_record.record, Record::SnapshotCheckpoint(_)) {
                checkpoints.insert(
                    node_id,
                    RecordPosition {
                        segment: segment.name,
                        offset: record_start,
                    },
                );
            }
        }
    }

    Ok(())
}

fn replay_segment(
    segment: &SegmentPath,
    allow_partial: bool,
    node_filter: Option<&BTreeSet<noraft::NodeId>>,
    checkpoint_positions: &BTreeMap<noraft::NodeId, RecordPosition>,
    replay: &mut ReplayState,
    stats: &StorageStatsCounters,
) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(allow_partial)
        .open(&segment.path)?;
    file.seek(SeekFrom::Start(0))?;
    let file_len = file.metadata()?.len();

    loop {
        let record_start = file.stream_position()?;
        let Some(body) = read_record_body(&mut file, Some(stats))? else {
            if record_start == file_len {
                break;
            }
            if allow_partial {
                stats.replay_truncated();
                file.set_len(record_start)?;
                file.seek(SeekFrom::Start(record_start))?;
                break;
            }
            return Err(invalid_data("partial record in inactive segment"));
        };
        let node_id = decode_record_node_id(&body)?;
        if node_filter.is_none_or(|nodes| nodes.contains(&node_id)) {
            let record_position = RecordPosition {
                segment: segment.name,
                offset: record_start,
            };
            if checkpoint_positions
                .get(&node_id)
                .is_some_and(|checkpoint| record_position < *checkpoint)
            {
                continue;
            }
            let node_record = decode_node_record(&body)?;
            let record_kind = node_record.record.metric_kind();
            replay.apply(node_record)?;
            stats.record_replayed(record_kind, frame_len_from_body(&body)?);
        }
    }

    Ok(())
}

fn replay_node_segment(
    segment: &SegmentPath,
    allow_partial: bool,
    target_node_id: noraft::NodeId,
    checkpoint_position: Option<RecordPosition>,
    state: &mut NodeState,
    stats: &StorageStatsCounters,
) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(allow_partial)
        .open(&segment.path)?;
    file.seek(SeekFrom::Start(0))?;
    let file_len = file.metadata()?.len();

    loop {
        let record_start = file.stream_position()?;
        let Some(body) = read_record_body(&mut file, Some(stats))? else {
            if record_start == file_len {
                break;
            }
            if allow_partial {
                stats.replay_truncated();
                file.set_len(record_start)?;
                file.seek(SeekFrom::Start(record_start))?;
                break;
            }
            return Err(invalid_data("partial record in inactive segment"));
        };

        if decode_record_node_id(&body)? == target_node_id {
            let record_position = RecordPosition {
                segment: segment.name,
                offset: record_start,
            };
            if checkpoint_position.is_some_and(|checkpoint| record_position < checkpoint) {
                continue;
            }
            let node_record = decode_node_record(&body)?;
            let record_kind = node_record.record.metric_kind();
            apply_record_to_state(state, node_record.record)?;
            stats.record_replayed(record_kind, frame_len_from_body(&body)?);
        }
    }

    Ok(())
}

fn scan_segment(path: &Path, allow_partial: bool, stats: &StorageStatsCounters) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(allow_partial)
        .open(path)?;
    file.seek(SeekFrom::Start(0))?;
    let file_len = file.metadata()?.len();

    loop {
        let record_start = file.stream_position()?;
        if scan_record_frame(&mut file, stats)?.is_some() {
            continue;
        }

        if record_start == file_len {
            break;
        }
        if allow_partial {
            stats.replay_truncated();
            file.set_len(record_start)?;
            file.seek(SeekFrom::Start(record_start))?;
            break;
        }
        return Err(invalid_data("partial record in inactive segment"));
    }

    Ok(())
}

fn scan_record_frame(file: &mut File, stats: &StorageStatsCounters) -> io::Result<Option<()>> {
    let mut header = [0; SEGMENT_BASE_HEADER_LEN];
    match file.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    if &header[..4] != SEGMENT_FORMAT_MAGIC {
        return Err(invalid_data("unsupported segment format"));
    }

    let body_len = u32::from_le_bytes(
        header[4..8]
            .try_into()
            .expect("segment header length should be four bytes"),
    );
    if MAX_RECORD_BODY_LEN < body_len {
        return Err(invalid_data("segment record is too large"));
    }

    let mut checksum = [0; SEGMENT_CHECKSUM_LEN];
    match file.read_exact(&mut checksum) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let expected_checksum = u32::from_le_bytes(checksum);

    let mut crc = crc32c_initial();
    let mut remaining = u64::from(body_len);
    let mut buffer = [0; 8192];
    while remaining != 0 {
        let read_len = remaining.min(buffer.len() as u64) as usize;
        match file.read_exact(&mut buffer[..read_len]) {
            Ok(()) => {
                crc = crc32c_extend(crc, &buffer[..read_len]);
                remaining -= read_len as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
    }

    if crc32c_finish(crc) != expected_checksum {
        stats.checksum_failed();
        return Err(invalid_data("segment record checksum mismatch"));
    }

    Ok(Some(()))
}

fn open_active_segment_file(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)
}

fn create_active_segment_file(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create_new(true)
        .read(true)
        .append(true)
        .open(path)
}

fn should_sync_metadata(sync: SyncPolicy) -> bool {
    !matches!(sync, SyncPolicy::UnsafeNoSync)
}

fn create_dir_all_synced(path: &Path, sync: SyncPolicy) -> io::Result<()> {
    let existed = path.exists();
    std::fs::create_dir_all(path)?;
    if !existed && should_sync_metadata(sync) {
        sync_parent_dir(path)?;
    }
    Ok(())
}

fn sync_parent_dir(path: &Path) -> io::Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    sync_dir(parent)
}

fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn encode_record_frame(node_id: noraft::NodeId, record: &Record) -> io::Result<Vec<u8>> {
    let mut body = Encoder::new();
    encode_node_id(node_id, &mut body);
    encode_record(record, &mut body)?;
    let body = body.finish();

    let body_len = u32::try_from(body.len()).map_err(|_| invalid_input("record is too large"))?;
    if MAX_RECORD_BODY_LEN < body_len {
        return Err(invalid_input("record is too large"));
    }

    let mut frame = Vec::new();
    frame.extend_from_slice(SEGMENT_FORMAT_MAGIC);
    frame.extend_from_slice(&body_len.to_le_bytes());
    frame.extend_from_slice(&crc32c(&body).to_le_bytes());
    debug_assert_eq!(frame.len(), SEGMENT_HEADER_LEN);
    frame.extend_from_slice(&body);
    Ok(frame)
}

fn read_record_body(
    file: &mut File,
    stats: Option<&StorageStatsCounters>,
) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0; SEGMENT_BASE_HEADER_LEN];
    match file.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    if &header[..4] != SEGMENT_FORMAT_MAGIC {
        return Err(invalid_data("unsupported segment format"));
    }

    let body_len = u32::from_le_bytes(
        header[4..8]
            .try_into()
            .expect("segment header length should be four bytes"),
    );
    if MAX_RECORD_BODY_LEN < body_len {
        return Err(invalid_data("segment record is too large"));
    }

    let mut checksum = [0; SEGMENT_CHECKSUM_LEN];
    match file.read_exact(&mut checksum) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let expected_checksum = u32::from_le_bytes(checksum);

    let mut body = Vec::new();
    let mut reader = file.take(u64::from(body_len));
    reader.read_to_end(&mut body)?;
    if body.len() != body_len as usize {
        return Ok(None);
    }

    let actual_checksum = crc32c(&body);
    if actual_checksum != expected_checksum {
        if let Some(stats) = stats {
            stats.checksum_failed();
        }
        return Err(invalid_data("segment record checksum mismatch"));
    }
    Ok(Some(body))
}

fn frame_len_from_body(body: &[u8]) -> io::Result<u64> {
    u64::try_from(SEGMENT_HEADER_LEN + body.len())
        .map_err(|_| invalid_data("segment frame length overflow"))
}

fn encode_record(record: &Record, encoder: &mut Encoder) -> io::Result<()> {
    match record {
        Record::CurrentTerm(term) => {
            encoder.put_u8(0);
            encode_term(*term, encoder);
        }
        Record::VotedFor(voted_for) => {
            encoder.put_u8(1);
            encode_optional_node_id(*voted_for, encoder);
        }
        Record::Append(append) => {
            encoder.put_u8(2);
            encode_log_append(append, encoder)?;
        }
        Record::SnapshotCheckpoint(checkpoint) => {
            encoder.put_u8(3);
            encode_snapshot_checkpoint(checkpoint, encoder)?;
        }
    }
    Ok(())
}

fn decode_node_record(bytes: &[u8]) -> io::Result<NodeRecord> {
    let mut decoder = Decoder::new(bytes);
    let node_id = decode_node_id(&mut decoder)?;
    let record = match decoder.get_u8()? {
        0 => Record::CurrentTerm(decode_term(&mut decoder)?),
        1 => Record::VotedFor(decode_optional_node_id(&mut decoder)?),
        2 => Record::Append(decode_log_append(&mut decoder)?),
        3 => Record::SnapshotCheckpoint(decode_snapshot_checkpoint(&mut decoder)?),
        _ => return Err(invalid_data("unknown segment record tag")),
    };
    decoder.finish()?;
    Ok(NodeRecord { node_id, record })
}

fn decode_record_node_id(bytes: &[u8]) -> io::Result<noraft::NodeId> {
    let mut decoder = Decoder::new(bytes);
    decode_node_id(&mut decoder)
}

fn encode_log_append(append: &LogAppend, encoder: &mut Encoder) -> io::Result<()> {
    append.validate()?;
    encode_log_entries(&append.entries, encoder)?;
    encoder.put_u64(
        u64::try_from(append.commands.len()).map_err(|_| invalid_input("too many commands"))?,
    );
    for (index, payload) in &append.commands {
        encode_log_index(*index, encoder);
        encoder.put_bytes(payload.as_slice())?;
    }
    Ok(())
}

fn decode_log_append(decoder: &mut Decoder<'_>) -> io::Result<LogAppend> {
    let entries = decode_log_entries(decoder)?;
    let command_count = decoder.get_u64()?;
    if MAX_SET_ITEMS < command_count {
        return Err(invalid_data("too many commands"));
    }

    let mut commands = BTreeMap::new();
    for _ in 0..command_count {
        let index = decode_log_index(decoder)?;
        let payload = Bytes::from(decoder.get_bytes()?);
        if commands.insert(index, payload).is_some() {
            return Err(invalid_data("duplicate command payload index"));
        }
    }

    LogAppend::new(entries, commands).map_err(|_| invalid_data("invalid command payload mapping"))
}

fn encode_snapshot(snapshot: &Snapshot, encoder: &mut Encoder) -> io::Result<()> {
    encode_log_position(snapshot.last_included, encoder);
    encode_cluster_config(&snapshot.config, encoder)?;
    encoder.put_bytes(snapshot.data.as_slice())
}

fn decode_snapshot(decoder: &mut Decoder<'_>) -> io::Result<Snapshot> {
    Ok(Snapshot {
        last_included: decode_log_position(decoder)?,
        config: decode_cluster_config(decoder)?,
        data: Bytes::from(decoder.get_bytes()?),
    })
}

fn encode_snapshot_checkpoint(
    checkpoint: &SnapshotCheckpoint,
    encoder: &mut Encoder,
) -> io::Result<()> {
    checkpoint.validate()?;
    encode_term(checkpoint.current_term, encoder);
    encode_optional_node_id(checkpoint.voted_for, encoder);
    encode_snapshot(&checkpoint.snapshot, encoder)?;
    encode_log_append(&checkpoint.suffix, encoder)
}

fn decode_snapshot_checkpoint(decoder: &mut Decoder<'_>) -> io::Result<SnapshotCheckpoint> {
    let checkpoint = SnapshotCheckpoint {
        current_term: decode_term(decoder)?,
        voted_for: decode_optional_node_id(decoder)?,
        snapshot: decode_snapshot(decoder)?,
        suffix: decode_log_append(decoder)?,
    };
    checkpoint
        .validate()
        .map_err(|_| invalid_data("invalid snapshot checkpoint"))?;
    Ok(checkpoint)
}

fn encode_log_entries(entries: &noraft::LogEntries, encoder: &mut Encoder) -> io::Result<()> {
    encode_log_position(entries.prev_position(), encoder);
    encoder
        .put_u64(u64::try_from(entries.len()).map_err(|_| invalid_input("too many log entries"))?);
    for entry in entries.iter() {
        encode_log_entry(&entry, encoder)?;
    }
    Ok(())
}

fn decode_log_entries(decoder: &mut Decoder<'_>) -> io::Result<noraft::LogEntries> {
    let prev_position = decode_log_position(decoder)?;
    let len = decoder.get_u64()?;
    if MAX_SET_ITEMS < len {
        return Err(invalid_data("too many log entries"));
    }

    let mut entries = noraft::LogEntries::new(prev_position);
    for _ in 0..len {
        entries.push(decode_log_entry(decoder)?);
    }
    Ok(entries)
}

fn encode_log_entry(entry: &noraft::LogEntry, encoder: &mut Encoder) -> io::Result<()> {
    match entry {
        noraft::LogEntry::Term(term) => {
            encoder.put_u8(0);
            encode_term(*term, encoder);
        }
        noraft::LogEntry::ClusterConfig(config) => {
            encoder.put_u8(1);
            encode_cluster_config(config, encoder)?;
        }
        noraft::LogEntry::Command => encoder.put_u8(2),
    }
    Ok(())
}

fn decode_log_entry(decoder: &mut Decoder<'_>) -> io::Result<noraft::LogEntry> {
    match decoder.get_u8()? {
        0 => Ok(noraft::LogEntry::Term(decode_term(decoder)?)),
        1 => Ok(noraft::LogEntry::ClusterConfig(decode_cluster_config(
            decoder,
        )?)),
        2 => Ok(noraft::LogEntry::Command),
        _ => Err(invalid_data("unknown log entry tag")),
    }
}

fn encode_cluster_config(config: &noraft::ClusterConfig, encoder: &mut Encoder) -> io::Result<()> {
    encode_node_id_set(&config.voters, encoder)?;
    encode_node_id_set(&config.new_voters, encoder)?;
    encode_node_id_set(&config.non_voters, encoder)
}

fn decode_cluster_config(decoder: &mut Decoder<'_>) -> io::Result<noraft::ClusterConfig> {
    Ok(noraft::ClusterConfig {
        voters: decode_node_id_set(decoder)?,
        new_voters: decode_node_id_set(decoder)?,
        non_voters: decode_node_id_set(decoder)?,
    })
}

fn encode_node_id_set(
    nodes: &std::collections::BTreeSet<noraft::NodeId>,
    encoder: &mut Encoder,
) -> io::Result<()> {
    encoder.put_u64(u64::try_from(nodes.len()).map_err(|_| invalid_input("too many node IDs"))?);
    for node in nodes {
        encode_node_id(*node, encoder);
    }
    Ok(())
}

fn decode_node_id_set(
    decoder: &mut Decoder<'_>,
) -> io::Result<std::collections::BTreeSet<noraft::NodeId>> {
    let len = decoder.get_u64()?;
    if MAX_SET_ITEMS < len {
        return Err(invalid_data("too many node IDs"));
    }

    let mut nodes = std::collections::BTreeSet::new();
    for _ in 0..len {
        if !nodes.insert(decode_node_id(decoder)?) {
            return Err(invalid_data("duplicate node ID"));
        }
    }
    Ok(nodes)
}

fn encode_optional_node_id(node_id: Option<noraft::NodeId>, encoder: &mut Encoder) {
    match node_id {
        Some(node_id) => {
            encoder.put_u8(1);
            encode_node_id(node_id, encoder);
        }
        None => encoder.put_u8(0),
    }
}

fn decode_optional_node_id(decoder: &mut Decoder<'_>) -> io::Result<Option<noraft::NodeId>> {
    match decoder.get_u8()? {
        0 => Ok(None),
        1 => Ok(Some(decode_node_id(decoder)?)),
        _ => Err(invalid_data("invalid optional node ID tag")),
    }
}

fn encode_log_position(position: noraft::LogPosition, encoder: &mut Encoder) {
    encode_term(position.term, encoder);
    encode_log_index(position.index, encoder);
}

fn decode_log_position(decoder: &mut Decoder<'_>) -> io::Result<noraft::LogPosition> {
    Ok(noraft::LogPosition {
        term: decode_term(decoder)?,
        index: decode_log_index(decoder)?,
    })
}

fn encode_term(term: noraft::Term, encoder: &mut Encoder) {
    encoder.put_u64(term.get());
}

fn decode_term(decoder: &mut Decoder<'_>) -> io::Result<noraft::Term> {
    Ok(noraft::Term::new(decoder.get_u64()?))
}

fn encode_node_id(node_id: noraft::NodeId, encoder: &mut Encoder) {
    encoder.put_u64(node_id.get());
}

fn decode_node_id(decoder: &mut Decoder<'_>) -> io::Result<noraft::NodeId> {
    Ok(noraft::NodeId::new(decoder.get_u64()?))
}

fn encode_log_index(index: noraft::LogIndex, encoder: &mut Encoder) {
    encoder.put_u64(index.get());
}

fn decode_log_index(decoder: &mut Decoder<'_>) -> io::Result<noraft::LogIndex> {
    Ok(noraft::LogIndex::new(decoder.get_u64()?))
}

const CRC32C_POLYNOMIAL: u32 = 0x82F6_3B78;
const CRC32C_TABLE: [u32; 256] = make_crc32c_table();

const fn make_crc32c_table() -> [u32; 256] {
    let mut table = [0; 256];
    let mut i = 0;
    while i < table.len() {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 0 {
                crc >> 1
            } else {
                (crc >> 1) ^ CRC32C_POLYNOMIAL
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

fn crc32c(bytes: &[u8]) -> u32 {
    crc32c_finish(crc32c_extend(crc32c_initial(), bytes))
}

fn crc32c_initial() -> u32 {
    0xFFFF_FFFF
}

fn crc32c_extend(mut crc: u32, bytes: &[u8]) -> u32 {
    for byte in bytes {
        let index = ((crc ^ u32::from(*byte)) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC32C_TABLE[index];
    }
    crc
}

fn crc32c_finish(crc: u32) -> u32 {
    !crc
}

#[derive(Debug)]
struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    fn put_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn put_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn put_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.put_u64(
            u64::try_from(bytes.len()).map_err(|_| invalid_input("byte slice is too large"))?,
        );
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

#[derive(Debug)]
struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn get_u8(&mut self) -> io::Result<u8> {
        let bytes = self.take(1)?;
        Ok(bytes[0])
    }

    fn get_u64(&mut self) -> io::Result<u64> {
        let bytes = self.take(8)?;
        Ok(u64::from_le_bytes(
            bytes
                .try_into()
                .expect("u64 encoding should be eight bytes"),
        ))
    }

    fn get_bytes(&mut self) -> io::Result<&'a [u8]> {
        let len = self.get_u64()?;
        if u64::from(MAX_RECORD_BODY_LEN) < len {
            return Err(invalid_data("byte slice is too large"));
        }
        let len = usize::try_from(len).map_err(|_| invalid_data("byte slice is too large"))?;
        self.take(len)
    }

    fn take(&mut self, len: usize) -> io::Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .ok_or_else(|| invalid_data("record offset overflow"))?;
        if self.bytes.len() < end {
            return Err(invalid_data("record is too short"));
        }
        let slice = &self.bytes[self.position..end];
        self.position = end;
        Ok(slice)
    }

    fn finish(&self) -> io::Result<()> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(invalid_data("record has trailing bytes"))
        }
    }
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn invalid_json(error: nojson::JsonParseError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).expect("usize value should fit in u64")
}

fn remove_storage_dir_if_exists(path: &Path, sync: SyncPolicy) -> io::Result<()> {
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };

    for entry in entries {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            std::fs::remove_dir_all(entry.path())?;
        } else {
            std::fs::remove_file(entry.path())?;
        }
    }
    if should_sync_metadata(sync) {
        sync_dir(path)?;
    }

    std::fs::remove_dir(path)?;
    if should_sync_metadata(sync) {
        sync_parent_dir(path)?;
    }
    Ok(())
}
