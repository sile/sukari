//! Shared segmented storage model for Raft node state.

use crate::bytes::Bytes;
use crate::codec::{
    decode_node_record, decode_record_node_id, frame_len_from_body, read_record_body,
    scan_record_frame,
};
use crate::error::{invalid_data, invalid_input, invalid_json, usize_to_u64};
use crate::registry::{NodeMetadata, NodeRegistry, node_not_found_error, node_removed_error};
use crate::segment::{
    RecordPosition, SEGMENT_FILE_HEADER_LEN_U64, SegmentName, SegmentPath, SegmentWriter,
    discover_segment_paths, read_segment_header, segment_file_exists, select_active_append_segment,
};
use crate::stats::{
    NodeAccessErrorKind, RecordKindMetric, StorageOperationKind, StorageStats, StorageStatsCounters,
};

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const CHECKPOINT_INDEX_FILE_NAME: &str = "checkpoints.json";
const CHECKPOINT_INDEX_TMP_FILE_NAME: &str = "checkpoints.json.tmp";
const CHECKPOINT_INDEX_VERSION: u64 = 1;
const DEFAULT_MAX_SEGMENT_LEN: u64 = 128 * 1024 * 1024;

/// Storage synchronization policy for segment data and metadata files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// Synchronize durable data after every storage record and metadata update.
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

    /// Never synchronize durable data or metadata explicitly.
    ///
    /// This policy leaves persistence timing to the operating system.
    UnsafeNoSync,
}

/// Shared segmented storage engine for registered Raft nodes.
///
/// The engine appends storage records for many nodes into shared append
/// segments. A storage directory treats `noraft::NodeId` values as globally
/// unique across all nodes stored in it. Node IDs must be registered with
/// [`StorageEngine::create_node`] before node state records can be written or
/// loaded. Once registered, a node ID remains permanently reserved, even after
/// [`StorageEngine::remove_node`].
///
/// The engine validates record-local invariants before writing, such as command
/// payload mappings in [`LogAppend`]. It does not validate a log append against
/// the node's currently loaded log before writing. Replay applies records in
/// append order and resolves divergent log suffixes.
///
/// Storage write and metadata persistence errors are fatal to the engine
/// instance. After such an error, callers should stop using the instance,
/// diagnose the storage state, and reopen only after choosing an appropriate
/// recovery action.
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
    /// Opens or creates a storage engine in `dir`.
    ///
    /// Opening a storage directory recovers the active segment, loads the node
    /// registry and checkpoint index, and prepares the active append segment for
    /// new writes.
    pub fn new<P: AsRef<Path>>(dir: P, sync: SyncPolicy) -> io::Result<Self> {
        Self::with_max_segment_len(dir, sync, DEFAULT_MAX_SEGMENT_LEN)
    }

    /// Opens or creates a storage engine with a maximum append segment length.
    ///
    /// `max_segment_len` is a rotation threshold, not a hard per-record limit.
    /// If a record frame is larger than `max_segment_len`, it is written to an
    /// empty segment and that segment is allowed to exceed the threshold.
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
        let mut stats = StorageStatsCounters::default();
        recover_storage_dir(&dir, active_segment, &mut stats)?;
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
    ///
    /// This also writes an initial empty snapshot checkpoint so the node has a
    /// garbage-collection barrier from creation. A node ID cannot be created
    /// again after it has been created, even if it is later removed.
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
            &mut self.stats,
        )?;
        if should_sync_metadata(self.sync) {
            self.writer.flush(&mut self.stats)?;
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

    /// Returns active Raft nodes marked for process startup.
    pub fn startup_nodes(&self) -> impl Iterator<Item = (noraft::NodeId, &NodeMetadata)> + '_ {
        self.registry.startup_nodes()
    }

    /// Loads the current state for the given Raft node.
    ///
    /// The state is built on demand by replaying segment records. When a valid
    /// checkpoint hint exists, replay starts at that checkpoint record instead
    /// of scanning from the first segment.
    pub fn load(&mut self, node_id: noraft::NodeId) -> io::Result<NodeState> {
        self.ensure_node_exists(node_id, StorageOperationKind::Load)?;
        let checkpoint_position = self.checkpoint_index.checkpoint_position(node_id);
        replay_node_state(
            &self.dir,
            self.writer.active_segment(),
            node_id,
            checkpoint_position,
            &mut self.stats,
        )
    }

    /// Loads the latest state of all non-removed nodes.
    ///
    /// If every active node has a checkpoint hint, replay starts from the
    /// oldest hinted checkpoint segment. If any active node lacks a hint, replay
    /// scans all append segments.
    pub fn load_all(&mut self) -> io::Result<BTreeMap<noraft::NodeId, NodeState>> {
        let active_node_ids = self.registry.active_node_ids();
        if active_node_ids.is_empty() {
            return Ok(BTreeMap::new());
        }

        let checkpoint_hints = self.checkpoint_index.checkpoint_positions(&active_node_ids);
        let mut replay = replay_storage_dir(
            &self.dir,
            self.writer.active_segment(),
            Some(&active_node_ids),
            checkpoint_hints,
            &mut self.stats,
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
    ///
    /// The append is recorded as received. The storage layer checks only that
    /// command entries and command payloads match each other.
    pub fn append_entries(&mut self, node_id: noraft::NodeId, append: LogAppend) -> io::Result<()> {
        self.save_record(
            node_id,
            Record::Append(append),
            StorageOperationKind::AppendEntries,
        )
    }

    /// Saves a snapshot checkpoint.
    ///
    /// A checkpoint is a complete recovery point for one node. It stores the
    /// current term, voted-for node, latest snapshot, and retained log suffix.
    /// Earlier records for the node become obsolete for replay and whole-segment
    /// garbage collection. The checkpoint suffix must start at the snapshot's
    /// last included position.
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
            &mut self.stats,
        )?;
        if should_sync_metadata(self.sync) {
            self.writer.flush(&mut self.stats)?;
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
    ///
    /// Removal is persisted in the node registry. No segment record is appended
    /// for node removal. The removed node ID cannot be created again.
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

    /// Flushes pending segment writes according to the synchronization policy.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush(&mut self.stats)?;
        self.stats.flushed();
        Ok(())
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
        stats.active_append_segment_id = self.writer.active_segment().id();
        stats.active_append_segment_len_bytes = self.writer.segment_len();
        stats.unsynced_records = usize_to_u64(self.writer.unsynced_records());
        stats.unsynced_bytes = self.writer.unsynced_bytes();
        stats
    }

    fn save_record(
        &mut self,
        node_id: noraft::NodeId,
        record: Record,
        operation: StorageOperationKind,
    ) -> io::Result<()> {
        self.ensure_node_exists(node_id, operation)?;
        self.writer.append(node_id, &record, &mut self.stats)?;
        Ok(())
    }

    fn ensure_node_exists(
        &mut self,
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

    fn collect_garbage(&mut self) -> io::Result<()> {
        self.stats.gc_ran();
        let active_node_ids = self.registry.active_node_ids();
        let Some(barrier) = self.checkpoint_index.gc_barrier(&active_node_ids) else {
            return Ok(());
        };

        let mut deleted_segments = 0;
        for segment in discover_segment_paths(&self.dir)? {
            if segment.name == self.writer.active_segment() {
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

/// Application-defined payload for a command log entry.
///
/// The storage layer persists the tag and bytes but does not interpret either
/// value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandPayload {
    tag: u8,
    bytes: Bytes,
}

impl CommandPayload {
    /// Makes a command payload with an application-defined tag.
    pub fn new(tag: u8, bytes: Bytes) -> Self {
        Self { tag, bytes }
    }

    /// Returns the application-defined payload tag.
    pub fn tag(&self) -> u8 {
        self.tag
    }

    /// Returns the opaque payload bytes.
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Converts this payload into opaque bytes.
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

/// A persisted log append operation.
///
/// The value pairs `noraft` log entries with tagged opaque command payloads.
/// Construction validates only the record-local payload mapping: every command
/// entry must have one payload, and payloads must not exist for non-command
/// entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogAppend {
    /// Log entries emitted by `noraft`.
    entries: noraft::LogEntries,

    /// Command payloads keyed by log index.
    command_payloads: BTreeMap<noraft::LogIndex, CommandPayload>,
}

impl LogAppend {
    /// Makes a new append operation after validating command payload mapping.
    pub fn new(
        entries: noraft::LogEntries,
        command_payloads: BTreeMap<noraft::LogIndex, CommandPayload>,
    ) -> io::Result<Self> {
        let this = Self {
            entries,
            command_payloads,
        };
        this.validate()?;
        Ok(this)
    }

    /// Returns the log entries emitted by `noraft`.
    pub fn entries(&self) -> &noraft::LogEntries {
        &self.entries
    }

    /// Returns command payloads keyed by log index.
    pub fn command_payloads(&self) -> &BTreeMap<noraft::LogIndex, CommandPayload> {
        &self.command_payloads
    }

    pub(crate) fn validate(&self) -> io::Result<()> {
        for (position, entry) in self.entries.iter_with_positions() {
            if entry == noraft::LogEntry::Command
                && !self.command_payloads.contains_key(&position.index)
            {
                return Err(invalid_input("missing command payload"));
            }
        }

        for index in self.command_payloads.keys().copied() {
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
///
/// Snapshot payload bytes are application-defined and are loaded into memory
/// with the rest of the node state.
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
///
/// A checkpoint replaces the earlier replay history for one node. Callers that
/// need log entries after the snapshot position must include them in
/// [`SnapshotCheckpoint::suffix`] or append them after saving the checkpoint.
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
    pub(crate) fn validate(&self) -> io::Result<()> {
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
///
/// `StorageEngine` does not keep this full state in memory for normal writes.
/// It is constructed by [`StorageEngine::load`] or [`StorageEngine::load_all`]
/// when callers need to rebuild a Raft node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeState {
    /// Current term.
    pub current_term: noraft::Term,

    /// Node voted for in the current term.
    pub voted_for: Option<noraft::NodeId>,

    /// Local Raft log.
    pub log: noraft::Log,

    /// Command payloads keyed by log index.
    pub command_payloads: BTreeMap<noraft::LogIndex, CommandPayload>,

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
            command_payloads: BTreeMap::new(),
            snapshot: None,
        }
    }
}

impl NodeState {
    fn apply_current_term(&mut self, term: noraft::Term) {
        self.current_term = term;
    }

    fn apply_voted_for(&mut self, voted_for: Option<noraft::NodeId>) {
        self.voted_for = voted_for;
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
        self.command_payloads
            .retain(|index, _| *index <= prev_index);
        self.command_payloads.extend(append.command_payloads);
        Ok(())
    }

    fn apply_snapshot(&mut self, snapshot: Snapshot) -> io::Result<()> {
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

        self.command_payloads
            .retain(|index, _| snapshot.last_included.index < *index);
        self.log = noraft::Log::new(snapshot.config.clone(), entries);
        self.snapshot = Some(snapshot);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Record {
    CurrentTerm(noraft::Term),
    VotedFor(Option<noraft::NodeId>),
    Append(LogAppend),
    SnapshotCheckpoint(SnapshotCheckpoint),
}

impl Record {
    pub(crate) fn metric_kind(&self) -> RecordKindMetric {
        match self {
            Self::CurrentTerm(_) => RecordKindMetric::CurrentTerm,
            Self::VotedFor(_) => RecordKindMetric::VotedFor,
            Self::Append(_) => RecordKindMetric::LogAppend,
            Self::SnapshotCheckpoint(_) => RecordKindMetric::SnapshotCheckpoint,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeRecord {
    pub(crate) node_id: noraft::NodeId,
    pub(crate) record: Record,
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
    read_segment_header(&mut file, false, None)?;
    let file_len = file.metadata()?.len();
    if position.offset < SEGMENT_FILE_HEADER_LEN_U64 {
        return Err(invalid_data("checkpoint index offset precedes records"));
    }
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
    stats: &mut StorageStatsCounters,
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
    stats: &mut StorageStatsCounters,
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
    stats: &mut StorageStatsCounters,
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
    stats: &mut StorageStatsCounters,
) -> io::Result<()> {
    for segment in discover_segment_paths(dir)? {
        let allow_partial = segment.name == active_segment;
        scan_segment(&segment.path, allow_partial, stats)?;
    }
    Ok(())
}

fn find_checkpoint_positions(
    dir: &Path,
    active_segment: SegmentName,
    node_filter: Option<&BTreeSet<noraft::NodeId>>,
    stats: &mut StorageStatsCounters,
) -> io::Result<BTreeMap<noraft::NodeId, RecordPosition>> {
    find_checkpoint_positions_from(dir, active_segment, node_filter, None, stats)
}

fn find_checkpoint_positions_from(
    dir: &Path,
    active_segment: SegmentName,
    node_filter: Option<&BTreeSet<noraft::NodeId>>,
    start_position: Option<RecordPosition>,
    stats: &mut StorageStatsCounters,
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
    stats: &mut StorageStatsCounters,
) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(allow_partial)
        .open(&segment.path)?;
    if !read_segment_header(&mut file, allow_partial, Some(&mut *stats))? {
        return Ok(());
    }
    let file_len = file.metadata()?.len();
    let start_offset = start_offset.unwrap_or(SEGMENT_FILE_HEADER_LEN_U64);
    if start_offset < SEGMENT_FILE_HEADER_LEN_U64 {
        return Err(invalid_data("checkpoint scan offset precedes records"));
    }
    if file_len < start_offset {
        return Err(invalid_data(
            "checkpoint scan offset exceeds segment length",
        ));
    }
    file.seek(SeekFrom::Start(start_offset))?;

    loop {
        let record_start = file.stream_position()?;
        let Some(body) = read_record_body(&mut file, Some(&mut *stats))? else {
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
    stats: &mut StorageStatsCounters,
) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(allow_partial)
        .open(&segment.path)?;
    if !read_segment_header(&mut file, allow_partial, Some(&mut *stats))? {
        return Ok(());
    }
    let file_len = file.metadata()?.len();

    loop {
        let record_start = file.stream_position()?;
        let Some(body) = read_record_body(&mut file, Some(&mut *stats))? else {
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
    stats: &mut StorageStatsCounters,
) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(allow_partial)
        .open(&segment.path)?;
    if !read_segment_header(&mut file, allow_partial, Some(&mut *stats))? {
        return Ok(());
    }
    let file_len = file.metadata()?.len();

    loop {
        let record_start = file.stream_position()?;
        let Some(body) = read_record_body(&mut file, Some(&mut *stats))? else {
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

fn scan_segment(
    path: &Path,
    allow_partial: bool,
    stats: &mut StorageStatsCounters,
) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(allow_partial)
        .open(path)?;
    if !read_segment_header(&mut file, allow_partial, Some(stats))? {
        return Ok(());
    }
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

pub(crate) fn should_sync_metadata(sync: SyncPolicy) -> bool {
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

pub(crate) fn sync_parent_dir(path: &Path) -> io::Result<()> {
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
