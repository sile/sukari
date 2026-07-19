use sukari::{
    bytes::Bytes,
    storage::{LogAppend, Snapshot, StorageEngine, StorageState, SyncPolicy},
};

use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::{self, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

const SEGMENT_FILE_NAME: &str = "append-000001.segment";
const SEGMENT_HEADER_LEN: u64 = 12;

#[test]
fn log_append_requires_command_payloads() {
    let entries =
        noraft::LogEntries::from_iter(noraft::LogPosition::ZERO, [noraft::LogEntry::Command]);

    let err = LogAppend::new(entries, BTreeMap::new())
        .expect_err("command entries should require payloads");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn log_append_rejects_payloads_without_command_entries() {
    let entries = noraft::LogEntries::from_iter(
        noraft::LogPosition::ZERO,
        [noraft::LogEntry::Term(noraft::Term::new(1))],
    );
    let mut commands = BTreeMap::new();
    commands.insert(noraft::LogIndex::new(1), Bytes::from(b"command".as_slice()));

    let err =
        LogAppend::new(entries, commands).expect_err("term entries should not accept payloads");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn storage_state_applies_log_suffix_replacement() {
    let mut state = StorageState::default();
    let mut commands = BTreeMap::new();
    commands.insert(noraft::LogIndex::new(2), Bytes::from(b"old-2".as_slice()));
    commands.insert(noraft::LogIndex::new(3), Bytes::from(b"old-3".as_slice()));

    let initial_entries = noraft::LogEntries::from_iter(
        noraft::LogPosition::ZERO,
        [
            noraft::LogEntry::Term(noraft::Term::new(1)),
            noraft::LogEntry::Command,
            noraft::LogEntry::Command,
        ],
    );
    let initial_append = LogAppend::new(initial_entries, commands)
        .expect("initial append should have matching command payloads");
    state
        .apply_append(&initial_append)
        .expect("initial append should apply");

    let mut replacement_commands = BTreeMap::new();
    replacement_commands.insert(noraft::LogIndex::new(2), Bytes::from(b"new-2".as_slice()));
    let replacement_entries =
        noraft::LogEntries::from_iter(position(1, 1), [noraft::LogEntry::Command]);
    let replacement_append = LogAppend::new(replacement_entries, replacement_commands)
        .expect("replacement append should have matching command payloads");
    state
        .apply_append(&replacement_append)
        .expect("replacement append should apply");

    assert_eq!(state.log.entries().last_position(), position(1, 2));
    assert_eq!(state.commands.len(), 1);
    assert_eq!(
        state
            .commands
            .get(&noraft::LogIndex::new(2))
            .expect("replacement payload should exist")
            .as_slice(),
        b"new-2"
    );
}

#[test]
fn storage_engine_replays_records_for_multiple_nodes() {
    let dir = unique_temp_dir("sukari-storage-replay");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");

    engine
        .save_current_term(noraft::NodeId::new(1), noraft::Term::new(4))
        .expect("term should be stored");
    engine
        .save_voted_for(noraft::NodeId::new(1), Some(noraft::NodeId::new(9)))
        .expect("vote should be stored");
    let append = append(
        position(0, 0),
        [
            noraft::LogEntry::Term(noraft::Term::new(4)),
            noraft::LogEntry::Command,
        ],
        [(2, Bytes::from(b"command".as_slice()))],
    );
    engine
        .append_entries(noraft::NodeId::new(1), append)
        .expect("entries should be stored");

    engine
        .save_current_term(noraft::NodeId::new(2), noraft::Term::new(7))
        .expect("node 2 term should be stored");
    drop(engine);

    let engine = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should reopen");
    let state1 = engine
        .load(noraft::NodeId::new(1))
        .expect("node 1 state should load");
    assert_eq!(state1.current_term, noraft::Term::new(4));
    assert_eq!(state1.voted_for, Some(noraft::NodeId::new(9)));
    assert_eq!(state1.log.entries().last_position(), position(4, 2));
    assert_eq!(
        state1.commands.get(&index(2)).map(Bytes::as_slice),
        Some(&b"command"[..])
    );

    let state2 = engine
        .load(noraft::NodeId::new(2))
        .expect("node 2 state should load");
    assert_eq!(state2.current_term, noraft::Term::new(7));

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_replays_snapshots() {
    let dir = unique_temp_dir("sukari-storage-snapshot");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");

    let append = append(
        position(0, 0),
        [
            noraft::LogEntry::Term(noraft::Term::new(2)),
            noraft::LogEntry::Command,
            noraft::LogEntry::Command,
        ],
        [
            (2, Bytes::from(b"two".as_slice())),
            (3, Bytes::from(b"three".as_slice())),
        ],
    );
    engine
        .append_entries(noraft::NodeId::new(3), append)
        .expect("entries should be stored");

    let snapshot = Snapshot {
        last_included: position(2, 2),
        config: noraft::ClusterConfig::new(),
        data: Bytes::from(b"snapshot".as_slice()),
    };
    engine
        .save_snapshot(noraft::NodeId::new(3), snapshot)
        .expect("snapshot should be stored");
    drop(engine);

    let engine = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should reopen");
    let state = engine
        .load(noraft::NodeId::new(3))
        .expect("node state should load");
    assert_eq!(state.log.entries().prev_position(), position(2, 2));
    assert_eq!(state.log.entries().last_position(), position(2, 3));
    assert_eq!(state.commands.get(&index(2)), None);
    assert_eq!(
        state.commands.get(&index(3)).map(Bytes::as_slice),
        Some(&b"three"[..])
    );
    assert_eq!(
        state
            .snapshot
            .expect("snapshot should be loaded")
            .data
            .as_slice(),
        b"snapshot"
    );

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_tombstone_hides_removed_node() {
    let dir = unique_temp_dir("sukari-storage-remove");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");
    engine
        .save_current_term(noraft::NodeId::new(5), noraft::Term::new(8))
        .expect("term should be stored");

    engine
        .remove_node(noraft::NodeId::new(5))
        .expect("node tombstone should be stored");

    let all = engine.load_all().expect("states should load");
    assert!(!all.contains_key(&noraft::NodeId::new(5)));

    let err = engine
        .load(noraft::NodeId::new(5))
        .expect_err("removed node should not load");
    assert_eq!(err.kind(), io::ErrorKind::NotFound);

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_truncates_trailing_partial_record() {
    let dir = unique_temp_dir("sukari-storage-partial");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");
    engine
        .save_current_term(noraft::NodeId::new(2), noraft::Term::new(6))
        .expect("term should be stored");
    drop(engine);

    let path = segment_path(&dir);
    let stable_len = std::fs::metadata(&path)
        .expect("segment should exist")
        .len();
    let mut file = OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("segment should open");
    file.write_all(b"SKR1")
        .expect("partial record should be written");
    drop(file);

    let engine = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should reopen");
    assert_eq!(
        engine
            .load(noraft::NodeId::new(2))
            .expect("node state should load")
            .current_term,
        noraft::Term::new(6)
    );
    assert_eq!(
        std::fs::metadata(&path)
            .expect("segment should still exist")
            .len(),
        stable_len
    );

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_rejects_corrupted_checksum() {
    let dir = unique_temp_dir("sukari-storage-corrupt");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");
    engine
        .save_current_term(noraft::NodeId::new(2), noraft::Term::new(6))
        .expect("term should be stored");
    drop(engine);

    let path = segment_path(&dir);
    let mut file = OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("segment should open");
    file.seek(SeekFrom::Start(SEGMENT_HEADER_LEN))
        .expect("segment body should be reachable");
    file.write_all(&[0xFF])
        .expect("segment body should be corrupted");
    drop(file);

    let err = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync)
        .expect_err("corrupted segment should fail");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_removes_all_data() {
    let dir = unique_temp_dir("sukari-storage-remove-all");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");
    engine
        .save_current_term(noraft::NodeId::new(1), noraft::Term::new(1))
        .expect("term should be stored");

    engine
        .remove_all()
        .expect("storage directory should be removed");
    assert!(!dir.exists());
}

fn append<const N: usize, I>(
    prev_position: noraft::LogPosition,
    entries: I,
    commands: [(u64, Bytes); N],
) -> LogAppend
where
    I: IntoIterator<Item = noraft::LogEntry>,
{
    LogAppend::new(
        noraft::LogEntries::from_iter(prev_position, entries),
        commands
            .into_iter()
            .map(|(index, payload)| (noraft::LogIndex::new(index), payload))
            .collect(),
    )
    .expect("append should have matching command payloads")
}

fn index(index: u64) -> noraft::LogIndex {
    noraft::LogIndex::new(index)
}

fn position(term: u64, index: u64) -> noraft::LogPosition {
    noraft::LogPosition {
        term: noraft::Term::new(term),
        index: noraft::LogIndex::new(index),
    }
}

fn segment_path(dir: &Path) -> PathBuf {
    dir.join(SEGMENT_FILE_NAME)
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);

    let dir = std::env::temp_dir().join(format!(
        "{}-{}-{}",
        prefix,
        std::process::id(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    ));
    match std::fs::remove_dir_all(&dir) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => panic!("failed to remove old temporary directory {dir:?}: {e}"),
    }
    dir
}
