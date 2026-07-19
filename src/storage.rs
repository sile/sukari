//! Shared segmented storage model for Raft node state.

use crate::bytes::Bytes;

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsStr,
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
const MAX_RECORD_LEN: u32 = 64 * 1024 * 1024;
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
    writer: SegmentWriter,
    replay: ReplayState,
}

impl StorageEngine {
    /// Makes a new shared storage engine.
    pub fn new<P: AsRef<Path>>(dir: P, sync: SyncPolicy) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        create_dir_all_synced(&dir, sync)?;
        let active_segment = SegmentName::first_append();
        let replay = replay_storage_dir(&dir, active_segment)?;
        let writer = SegmentWriter::open(&dir, sync, active_segment)?;
        Ok(Self {
            dir,
            sync,
            writer,
            replay,
        })
    }

    /// Loads the current state for the given Raft node.
    pub fn load(&self, node_id: noraft::NodeId) -> io::Result<StorageState> {
        if self.replay.removed_nodes.contains(&node_id) {
            return Err(node_removed_error());
        }
        Ok(self.replay.nodes.get(&node_id).cloned().unwrap_or_default())
    }

    /// Loads the latest state of all non-removed nodes.
    pub fn load_all(&self) -> io::Result<BTreeMap<noraft::NodeId, StorageState>> {
        Ok(self.replay.nodes.clone())
    }

    /// Saves the current term for the given Raft node.
    pub fn save_current_term(
        &mut self,
        node_id: noraft::NodeId,
        term: noraft::Term,
    ) -> io::Result<()> {
        self.save_record(node_id, Record::CurrentTerm(term))
    }

    /// Saves the node voted for in the current term.
    pub fn save_voted_for(
        &mut self,
        node_id: noraft::NodeId,
        voted_for: Option<noraft::NodeId>,
    ) -> io::Result<()> {
        self.save_record(node_id, Record::VotedFor(voted_for))
    }

    /// Appends log entries and their command payloads.
    pub fn append_entries(&mut self, node_id: noraft::NodeId, append: LogAppend) -> io::Result<()> {
        self.save_record(node_id, Record::Append(append))
    }

    /// Saves a snapshot.
    pub fn save_snapshot(&mut self, node_id: noraft::NodeId, snapshot: Snapshot) -> io::Result<()> {
        self.save_record(node_id, Record::Snapshot(snapshot))
    }

    /// Records removal of all durable data for the given Raft node.
    pub fn remove_node(&mut self, node_id: noraft::NodeId) -> io::Result<()> {
        if self.replay.removed_nodes.contains(&node_id) {
            return Ok(());
        }

        let record = Record::NodeRemoved;
        self.writer.append(node_id, &record)?;
        self.replay.apply(NodeRecord { node_id, record })?;
        Ok(())
    }

    /// Flushes pending writes.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
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

    fn save_record(&mut self, node_id: noraft::NodeId, record: Record) -> io::Result<()> {
        if self.replay.removed_nodes.contains(&node_id) {
            return Err(node_removed_error());
        }

        let mut next_state = self.replay.nodes.get(&node_id).cloned().unwrap_or_default();
        apply_record_to_state(&mut next_state, &record)?;
        self.writer.append(node_id, &record)?;
        self.replay.nodes.insert(node_id, next_state);
        Ok(())
    }
}

#[derive(Debug)]
struct SegmentWriter {
    active_segment: SegmentName,
    file: File,
    sync: SyncPolicy,
    unsynced_records: usize,
    unsynced_bytes: u64,
}

impl SegmentWriter {
    fn open(dir: &Path, sync: SyncPolicy, active_segment: SegmentName) -> io::Result<Self> {
        let segment_path = active_segment.path(dir);
        let file_existed = segment_path.exists();
        let mut file = open_active_segment_file(&segment_path)?;
        if !file_existed && should_sync_metadata(sync) {
            sync_parent_dir(&segment_path)?;
        }
        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            active_segment,
            file,
            sync,
            unsynced_records: 0,
            unsynced_bytes: 0,
        })
    }

    fn append(&mut self, node_id: noraft::NodeId, record: &Record) -> io::Result<()> {
        debug_assert_eq!(self.active_segment.kind, SegmentKind::Append);
        let frame = encode_record_frame(node_id, record)?;
        let written_bytes =
            u64::try_from(frame.len()).map_err(|_| invalid_input("record is too large"))?;
        self.file.write_all(&frame)?;
        self.after_write(written_bytes)
    }

    fn after_write(&mut self, written_bytes: u64) -> io::Result<()> {
        match self.sync {
            SyncPolicy::Strict => self.sync_data(),
            SyncPolicy::Batch {
                max_records,
                max_bytes,
            } => {
                self.unsynced_records += 1;
                self.unsynced_bytes += written_bytes;
                let records_reached = max_records != 0 && max_records <= self.unsynced_records;
                let bytes_reached = max_bytes != 0 && max_bytes <= self.unsynced_bytes;
                if records_reached || bytes_reached {
                    self.sync_data()?;
                }
                Ok(())
            }
            SyncPolicy::UnsafeNoSync => Ok(()),
        }
    }

    fn sync_data(&mut self) -> io::Result<()> {
        self.file.sync_data()?;
        self.unsynced_records = 0;
        self.unsynced_bytes = 0;
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.sync {
            SyncPolicy::UnsafeNoSync => Ok(()),
            SyncPolicy::Strict | SyncPolicy::Batch { .. } => self.sync_data(),
        }
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

/// Loaded persistent state for a Raft node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageState {
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

impl Default for StorageState {
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

impl StorageState {
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
        self.commands.extend(append.commands.clone());
        Ok(())
    }

    /// Applies a snapshot record to this state.
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
    Snapshot(Snapshot),
    NodeRemoved,
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

#[derive(Debug)]
struct SegmentPath {
    name: SegmentName,
    path: PathBuf,
}

#[derive(Debug, Default)]
struct ReplayState {
    nodes: BTreeMap<noraft::NodeId, StorageState>,
    removed_nodes: BTreeSet<noraft::NodeId>,
}

impl ReplayState {
    fn apply(&mut self, node_record: NodeRecord) -> io::Result<()> {
        if node_record.record == Record::NodeRemoved {
            self.nodes.remove(&node_record.node_id);
            self.removed_nodes.insert(node_record.node_id);
            return Ok(());
        }

        if self.removed_nodes.contains(&node_record.node_id) {
            return Ok(());
        }

        let state = self.nodes.entry(node_record.node_id).or_default();
        apply_record_to_state(state, &node_record.record)
    }
}

fn apply_record_to_state(state: &mut StorageState, record: &Record) -> io::Result<()> {
    match record {
        Record::CurrentTerm(term) => {
            state.apply_current_term(*term);
            Ok(())
        }
        Record::VotedFor(voted_for) => {
            state.apply_voted_for(*voted_for);
            Ok(())
        }
        Record::Append(append) => state.apply_append(append),
        Record::Snapshot(snapshot) => state.apply_snapshot(snapshot.clone()),
        Record::NodeRemoved => Ok(()),
    }
}

fn replay_storage_dir(dir: &Path, active_segment: SegmentName) -> io::Result<ReplayState> {
    let mut replay = ReplayState::default();
    for segment in discover_segment_paths(dir)? {
        let allow_partial = segment.name == active_segment;
        replay_segment(&segment.path, allow_partial, &mut replay)?;
    }
    Ok(replay)
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

fn replay_segment(path: &Path, allow_partial: bool, replay: &mut ReplayState) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(allow_partial)
        .open(path)?;
    file.seek(SeekFrom::Start(0))?;

    loop {
        let record_start = file.stream_position()?;
        let Some(record) = read_record(&mut file)? else {
            if allow_partial {
                file.set_len(record_start)?;
                file.seek(SeekFrom::Start(record_start))?;
                break;
            }
            return Err(invalid_data("partial record in inactive segment"));
        };
        replay.apply(record)?;
    }

    Ok(())
}

fn open_active_segment_file(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
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
    if MAX_RECORD_LEN < body_len {
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

fn read_record(file: &mut File) -> io::Result<Option<NodeRecord>> {
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
    if MAX_RECORD_LEN < body_len {
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
        return Err(invalid_data("segment record checksum mismatch"));
    }
    decode_node_record(&body).map(Some)
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
        Record::Snapshot(snapshot) => {
            encoder.put_u8(3);
            encode_snapshot(snapshot, encoder)?;
        }
        Record::NodeRemoved => encoder.put_u8(4),
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
        3 => Record::Snapshot(decode_snapshot(&mut decoder)?),
        4 => Record::NodeRemoved,
        _ => return Err(invalid_data("unknown segment record tag")),
    };
    decoder.finish()?;
    Ok(NodeRecord { node_id, record })
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
    let mut crc = 0xFFFF_FFFF;
    for byte in bytes {
        let index = ((crc ^ u32::from(*byte)) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC32C_TABLE[index];
    }
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
        if u64::from(MAX_RECORD_LEN) < len {
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

fn node_removed_error() -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, "node storage has been removed")
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
