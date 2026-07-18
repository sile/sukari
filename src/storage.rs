//! Shared storage model for Raft node state.

use crate::bytes::Bytes;

use std::{
    collections::BTreeMap,
    fs::File,
    io,
    path::{Path, PathBuf},
};

/// Storage synchronization policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// Synchronize durable data after every storage record.
    Strict,

    /// Synchronize durable data after record or byte thresholds are reached.
    ///
    /// A zero threshold is ignored. If both thresholds are zero, writes are
    /// synchronized only when an explicit flush operation is added.
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
}

impl StorageEngine {
    /// Makes a new shared storage engine.
    pub fn new<P: AsRef<Path>>(dir: P, sync: SyncPolicy) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        create_dir_all_synced(&dir, sync)?;
        Ok(Self { dir, sync })
    }

    /// Returns the storage directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Returns the storage synchronization policy.
    pub fn sync_policy(&self) -> SyncPolicy {
        self.sync
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

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
