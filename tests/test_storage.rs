use sukari::{Bytes, LogAppend, NodeMetadata, Snapshot, StorageEngine, StorageState, SyncPolicy};

use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::{self, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

const SEGMENT_FILE_NAME: &str = "append-000001.segment";
const SECOND_SEGMENT_FILE_NAME: &str = "append-000002.segment";
const THIRD_SEGMENT_FILE_NAME: &str = "append-000003.segment";
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
fn storage_engine_persists_node_registry_metadata() {
    let dir = unique_temp_dir("sukari-storage-registry");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");

    let control_metadata = node_metadata(true, r#"{"role":"control"}"#);
    let worker_metadata = node_metadata(false, r#""worker""#);
    engine
        .create_node(noraft::NodeId::new(1), control_metadata.clone())
        .expect("control node should be created");
    engine
        .create_node(noraft::NodeId::new(2), worker_metadata.clone())
        .expect("worker node should be created");

    assert_eq!(
        engine
            .node_metadata(noraft::NodeId::new(1))
            .expect("control metadata should exist"),
        &control_metadata
    );
    assert_eq!(
        engine
            .nodes()
            .map(|(node_id, metadata)| (node_id, metadata.metadata().text().to_owned()))
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([
            (noraft::NodeId::new(1), r#"{"role":"control"}"#.to_owned()),
            (noraft::NodeId::new(2), r#""worker""#.to_owned()),
        ])
    );
    assert_eq!(
        engine
            .startup_nodes()
            .map(|(node_id, _)| node_id)
            .collect::<Vec<_>>(),
        vec![noraft::NodeId::new(1)]
    );
    drop(engine);

    let engine = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should reopen");
    assert_eq!(
        engine
            .node_metadata(noraft::NodeId::new(1))
            .expect("control metadata should persist"),
        &control_metadata
    );
    assert_eq!(
        engine
            .node_metadata(noraft::NodeId::new(2))
            .expect("worker metadata should persist"),
        &worker_metadata
    );
    assert_eq!(
        engine
            .load(noraft::NodeId::new(1))
            .expect("empty created node should load")
            .current_term,
        noraft::Term::ZERO
    );

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_rejects_uncreated_nodes() {
    let dir = unique_temp_dir("sukari-storage-uncreated-node");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");
    let node_id = noraft::NodeId::new(1);

    let err = engine
        .load(node_id)
        .expect_err("uncreated node should not load");
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
    let err = engine
        .save_current_term(node_id, noraft::Term::new(1))
        .expect_err("uncreated node should reject term writes");
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
    let err = engine
        .append_entries(
            node_id,
            append(
                position(0, 0),
                [noraft::LogEntry::Term(noraft::Term::new(1))],
                [],
            ),
        )
        .expect_err("uncreated node should reject log appends");
    assert_eq!(err.kind(), io::ErrorKind::NotFound);

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_reserves_removed_node_ids() {
    let dir = unique_temp_dir("sukari-storage-removed-node-id");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");
    create_node(&mut engine, 7);

    engine
        .remove_node(noraft::NodeId::new(7))
        .expect("node should be removed");
    assert_eq!(engine.node_metadata(noraft::NodeId::new(7)), None);
    let err = engine
        .create_node(noraft::NodeId::new(7), NodeMetadata::default())
        .expect_err("removed node ID should not be reusable");
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    let err = engine
        .save_current_term(noraft::NodeId::new(7), noraft::Term::new(2))
        .expect_err("removed node should reject writes");
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
    drop(engine);

    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should reopen");
    let err = engine
        .create_node(noraft::NodeId::new(7), NodeMetadata::default())
        .expect_err("removed node ID should remain reserved after reopen");
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
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
    create_node(&mut engine, 1);
    create_node(&mut engine, 2);

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
fn storage_engine_load_skips_other_node_replay_state() {
    let dir = unique_temp_dir("sukari-storage-targeted-load");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");
    create_node(&mut engine, 1);
    create_node(&mut engine, 2);

    engine
        .save_current_term(noraft::NodeId::new(1), noraft::Term::new(4))
        .expect("term should be stored");

    let invalid_append = append(
        position(9, 9),
        [noraft::LogEntry::Term(noraft::Term::new(10))],
        [],
    );
    engine
        .append_entries(noraft::NodeId::new(2), invalid_append)
        .expect("other node append should be stored without consulting current state");
    drop(engine);

    let engine = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should reopen");
    let state = engine
        .load(noraft::NodeId::new(1))
        .expect("target node should load without applying other node records");
    assert_eq!(state.current_term, noraft::Term::new(4));

    let err = engine
        .load(noraft::NodeId::new(2))
        .expect_err("invalid target node replay should fail");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);

    let err = engine
        .load_all()
        .expect_err("loading all nodes should apply the invalid node record");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_rotates_and_replays_append_segments() {
    let dir = unique_temp_dir("sukari-storage-rotate");
    let mut engine = StorageEngine::with_max_segment_len(&dir, SyncPolicy::UnsafeNoSync, 1)
        .expect("storage should open");
    create_node(&mut engine, 1);

    engine
        .save_current_term(noraft::NodeId::new(1), noraft::Term::new(1))
        .expect("first term should be stored");
    engine
        .save_current_term(noraft::NodeId::new(1), noraft::Term::new(2))
        .expect("second term should be stored");
    drop(engine);

    assert!(segment_path(&dir).exists());
    assert!(segment_path_named(&dir, SECOND_SEGMENT_FILE_NAME).exists());

    let mut engine = StorageEngine::with_max_segment_len(&dir, SyncPolicy::UnsafeNoSync, 1)
        .expect("storage should reopen");
    assert_eq!(
        engine
            .load(noraft::NodeId::new(1))
            .expect("node state should load")
            .current_term,
        noraft::Term::new(2)
    );

    engine
        .save_current_term(noraft::NodeId::new(1), noraft::Term::new(3))
        .expect("third term should be stored");
    drop(engine);

    assert!(segment_path_named(&dir, THIRD_SEGMENT_FILE_NAME).exists());

    let engine = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should reopen");
    assert_eq!(
        engine
            .load(noraft::NodeId::new(1))
            .expect("node state should load")
            .current_term,
        noraft::Term::new(3)
    );

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_does_not_validate_log_anchor_before_write() {
    let dir = unique_temp_dir("sukari-storage-raw-append");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");
    create_node(&mut engine, 1);

    let append = append(
        position(9, 9),
        [noraft::LogEntry::Term(noraft::Term::new(10))],
        [],
    );
    engine
        .append_entries(noraft::NodeId::new(1), append)
        .expect("append should be stored without consulting current state");

    let err = engine
        .load(noraft::NodeId::new(1))
        .expect_err("invalid replay stream should fail when loaded");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_replays_snapshots() {
    let dir = unique_temp_dir("sukari-storage-snapshot");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");
    create_node(&mut engine, 3);

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
    create_node(&mut engine, 5);
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
    create_node(&mut engine, 2);
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
fn storage_engine_truncates_trailing_partial_record_in_latest_segment() {
    let dir = unique_temp_dir("sukari-storage-latest-partial");
    let mut engine = StorageEngine::with_max_segment_len(&dir, SyncPolicy::UnsafeNoSync, 1)
        .expect("storage should open");
    create_node(&mut engine, 2);
    engine
        .save_current_term(noraft::NodeId::new(2), noraft::Term::new(6))
        .expect("first term should be stored");
    engine
        .save_current_term(noraft::NodeId::new(2), noraft::Term::new(7))
        .expect("second term should be stored");
    drop(engine);

    let path = segment_path_named(&dir, SECOND_SEGMENT_FILE_NAME);
    let stable_len = std::fs::metadata(&path)
        .expect("latest segment should exist")
        .len();
    let mut file = OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("latest segment should open");
    file.write_all(b"SKR1")
        .expect("partial record should be written");
    drop(file);

    let engine = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should reopen");
    assert_eq!(
        engine
            .load(noraft::NodeId::new(2))
            .expect("node state should load")
            .current_term,
        noraft::Term::new(7)
    );
    assert_eq!(
        std::fs::metadata(&path)
            .expect("latest segment should still exist")
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
    create_node(&mut engine, 2);
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
    create_node(&mut engine, 1);
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

fn create_node(engine: &mut StorageEngine, node_id: u64) {
    engine
        .create_node(noraft::NodeId::new(node_id), NodeMetadata::default())
        .expect("node should be created");
}

fn node_metadata(startup: bool, metadata: &str) -> NodeMetadata {
    NodeMetadata::new(
        startup,
        nojson::RawJsonOwned::parse(metadata).expect("metadata should be valid JSON"),
    )
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
    segment_path_named(dir, SEGMENT_FILE_NAME)
}

fn segment_path_named(dir: &Path, file_name: &str) -> PathBuf {
    dir.join(file_name)
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
