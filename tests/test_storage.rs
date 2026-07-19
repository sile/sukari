use sukari::{
    Bytes, LogAppend, NodeMetadata, Snapshot, SnapshotCheckpoint, StorageEngine, StorageState,
    SyncPolicy,
};

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
const MANIFEST_FILE_NAME: &str = "manifest";
const MANIFEST_TMP_FILE_NAME: &str = "manifest.tmp";
const NODE_REGISTRY_FILE_NAME: &str = "nodes.json";
const NODE_REGISTRY_TMP_FILE_NAME: &str = "nodes.json.tmp";
const CHECKPOINT_INDEX_FILE_NAME: &str = "checkpoints.json";
const CHECKPOINT_INDEX_TMP_FILE_NAME: &str = "checkpoints.json.tmp";
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
    let err = engine
        .save_snapshot(
            node_id,
            checkpoint(
                noraft::Term::new(1),
                None,
                snapshot(noraft::LogPosition::ZERO, b"checkpoint"),
                append(noraft::LogPosition::ZERO, std::iter::empty(), []),
            ),
        )
        .expect_err("uncreated node should reject checkpoint writes");
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
fn storage_engine_ignores_stale_node_registry_tmp() {
    let dir = unique_temp_dir("sukari-storage-stale-registry-tmp");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");
    let metadata = node_metadata(true, r#"{"role":"control"}"#);
    engine
        .create_node(noraft::NodeId::new(1), metadata.clone())
        .expect("node should be created");
    drop(engine);

    std::fs::write(dir.join(NODE_REGISTRY_TMP_FILE_NAME), "not-json")
        .expect("stale registry tmp file should be written");

    let engine = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should reopen");
    assert_eq!(
        engine
            .node_metadata(noraft::NodeId::new(1))
            .expect("metadata should come from nodes.json"),
        &metadata
    );
    assert_eq!(engine.node_metadata(noraft::NodeId::new(2)), None);

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_rejects_invalid_node_registry_files() {
    for (name, text) in [
        ("malformed", "{"),
        ("unsupported-version", r#"{"version":2,"nodes":{}}"#),
        ("missing-version", r#"{"nodes":{}}"#),
        ("missing-nodes", r#"{"version":1}"#),
        (
            "invalid-node-id",
            r#"{"version":1,"nodes":{"abc":{"startup":false,"metadata":{},"removed":false}}}"#,
        ),
        (
            "missing-startup",
            r#"{"version":1,"nodes":{"1":{"metadata":{},"removed":false}}}"#,
        ),
        (
            "missing-metadata",
            r#"{"version":1,"nodes":{"1":{"startup":false,"removed":false}}}"#,
        ),
        (
            "missing-removed",
            r#"{"version":1,"nodes":{"1":{"startup":false,"metadata":{}}}}"#,
        ),
    ] {
        let dir = unique_temp_dir(&format!("sukari-storage-invalid-registry-{name}"));
        write_node_registry(&dir, text);

        let err = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync)
            .expect_err("invalid registry should fail to load");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "case: {name}");

        std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
    }
}

#[test]
fn storage_engine_persists_checkpoint_index_positions() {
    let dir = unique_temp_dir("sukari-storage-checkpoint-index");
    let mut engine = StorageEngine::with_max_segment_len(&dir, SyncPolicy::UnsafeNoSync, 1)
        .expect("storage should open");
    create_node(&mut engine, 1);
    engine
        .save_current_term(noraft::NodeId::new(1), noraft::Term::new(1))
        .expect("term should be stored in the first segment");
    engine
        .save_snapshot(
            noraft::NodeId::new(1),
            checkpoint(
                noraft::Term::new(2),
                None,
                snapshot(noraft::LogPosition::ZERO, b"checkpoint"),
                append(noraft::LogPosition::ZERO, std::iter::empty(), []),
            ),
        )
        .expect("checkpoint should be stored in the second segment");

    let checkpoint_index = read_checkpoint_index(&dir);
    assert!(checkpoint_index.contains(r#""version": 1"#));
    assert!(checkpoint_index.contains(r#""1": {"#));
    assert!(checkpoint_index.contains(r#""checkpoint_segment": "append-000002.segment""#));
    assert!(checkpoint_index.contains(r#""checkpoint_offset": 0"#));
    drop(engine);

    let engine = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should reopen");
    assert_eq!(
        engine
            .load(noraft::NodeId::new(1))
            .expect("node state should load")
            .current_term,
        noraft::Term::new(2)
    );

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_ignores_stale_checkpoint_index_tmp() {
    let dir = unique_temp_dir("sukari-storage-stale-checkpoint-index-tmp");
    create_checkpoint_index_store(&dir);

    std::fs::write(dir.join(CHECKPOINT_INDEX_TMP_FILE_NAME), "not-json")
        .expect("stale checkpoint index tmp file should be written");

    let engine = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should reopen");
    assert_eq!(
        engine
            .load(noraft::NodeId::new(1))
            .expect("node state should load")
            .current_term,
        noraft::Term::new(1)
    );

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_rejects_invalid_checkpoint_index_files() {
    for (name, text) in [
        ("malformed", "{"),
        ("unsupported-version", r#"{"version":2,"nodes":{}}"#),
        ("missing-version", r#"{"nodes":{}}"#),
        ("missing-nodes", r#"{"version":1}"#),
        (
            "invalid-node-id",
            r#"{"version":1,"nodes":{"abc":{"checkpoint_segment":"append-000001.segment","checkpoint_offset":0}}}"#,
        ),
        (
            "missing-checkpoint-segment",
            r#"{"version":1,"nodes":{"1":{"checkpoint_offset":0}}}"#,
        ),
        (
            "missing-checkpoint-offset",
            r#"{"version":1,"nodes":{"1":{"checkpoint_segment":"append-000001.segment"}}}"#,
        ),
        (
            "invalid-segment-name",
            r#"{"version":1,"nodes":{"1":{"checkpoint_segment":"bad.segment","checkpoint_offset":0}}}"#,
        ),
        (
            "rewrite-segment",
            r#"{"version":1,"nodes":{"1":{"checkpoint_segment":"rewrite-000001.segment","checkpoint_offset":0}}}"#,
        ),
        (
            "missing-segment",
            r#"{"version":1,"nodes":{"1":{"checkpoint_segment":"append-999999.segment","checkpoint_offset":0}}}"#,
        ),
        (
            "offset-too-large",
            r#"{"version":1,"nodes":{"1":{"checkpoint_segment":"append-000001.segment","checkpoint_offset":999999}}}"#,
        ),
        (
            "offset-inside-record",
            r#"{"version":1,"nodes":{"1":{"checkpoint_segment":"append-000001.segment","checkpoint_offset":1}}}"#,
        ),
    ] {
        let dir = unique_temp_dir(&format!("sukari-storage-invalid-checkpoint-index-{name}"));
        create_checkpoint_index_store(&dir);
        write_checkpoint_index(&dir, text);

        let err = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync)
            .expect_err("invalid checkpoint index should fail to load");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "case: {name}");

        std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
    }
}

#[test]
fn storage_engine_rejects_checkpoint_index_that_points_to_non_checkpoint_record() {
    let dir = unique_temp_dir("sukari-storage-checkpoint-index-non-checkpoint");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");
    create_node(&mut engine, 1);
    engine
        .save_current_term(noraft::NodeId::new(1), noraft::Term::new(1))
        .expect("term should be stored");
    drop(engine);

    write_checkpoint_index(
        &dir,
        r#"{"version":1,"nodes":{"1":{"checkpoint_segment":"append-000001.segment","checkpoint_offset":0}}}"#,
    );

    let err = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync)
        .expect_err("checkpoint index should point to checkpoint records");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_rejects_checkpoint_index_node_id_mismatch() {
    let dir = unique_temp_dir("sukari-storage-checkpoint-index-node-mismatch");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");
    create_node(&mut engine, 1);
    create_node(&mut engine, 2);
    engine
        .save_snapshot(
            noraft::NodeId::new(1),
            checkpoint(
                noraft::Term::new(1),
                None,
                snapshot(noraft::LogPosition::ZERO, b"checkpoint"),
                append(noraft::LogPosition::ZERO, std::iter::empty(), []),
            ),
        )
        .expect("checkpoint should be stored");
    drop(engine);

    write_checkpoint_index(
        &dir,
        r#"{"version":1,"nodes":{"2":{"checkpoint_segment":"append-000001.segment","checkpoint_offset":0}}}"#,
    );

    let err = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync)
        .expect_err("checkpoint index node ID should match the record");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_scans_after_stale_checkpoint_index_hint() {
    let dir = unique_temp_dir("sukari-storage-stale-checkpoint-index-hint");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");
    create_node(&mut engine, 1);
    engine
        .save_snapshot(
            noraft::NodeId::new(1),
            checkpoint(
                noraft::Term::new(1),
                None,
                snapshot(noraft::LogPosition::ZERO, b"old-checkpoint"),
                append(noraft::LogPosition::ZERO, std::iter::empty(), []),
            ),
        )
        .expect("old checkpoint should be stored");
    let stale_checkpoint_index = read_checkpoint_index(&dir);
    engine
        .append_entries(
            noraft::NodeId::new(1),
            append(
                position(9, 9),
                [noraft::LogEntry::Term(noraft::Term::new(10))],
                [],
            ),
        )
        .expect("invalid append should be stored");
    engine
        .save_snapshot(
            noraft::NodeId::new(1),
            checkpoint(
                noraft::Term::new(2),
                None,
                snapshot(noraft::LogPosition::ZERO, b"new-checkpoint"),
                append(noraft::LogPosition::ZERO, std::iter::empty(), []),
            ),
        )
        .expect("new checkpoint should be stored");
    drop(engine);
    write_checkpoint_index(&dir, &stale_checkpoint_index);

    let engine = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should reopen");
    let state = engine
        .load(noraft::NodeId::new(1))
        .expect("newer checkpoint should be found after the stale hint");
    assert_eq!(state.current_term, noraft::Term::new(2));
    assert_eq!(
        state
            .snapshot
            .expect("new checkpoint should be loaded")
            .data
            .as_slice(),
        b"new-checkpoint"
    );

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_removes_removed_nodes_from_checkpoint_index() {
    let dir = unique_temp_dir("sukari-storage-remove-checkpoint-index");
    create_checkpoint_index_store(&dir);

    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should reopen");
    engine
        .remove_node(noraft::NodeId::new(1))
        .expect("node should be removed");

    let checkpoint_index = read_checkpoint_index(&dir);
    assert!(checkpoint_index.contains(r#""nodes": {}"#));

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
    create_two_segment_store(&dir);

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
fn storage_engine_writes_manifest_for_active_append_segment() {
    let dir = unique_temp_dir("sukari-storage-manifest");
    create_two_segment_store(&dir);

    let manifest = read_manifest(&dir);
    assert!(manifest.contains(r#""version": 1"#));
    assert!(manifest.contains(r#""active_append_segment": "append-000002.segment""#));

    let engine = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should reopen");
    assert_eq!(
        engine
            .load(noraft::NodeId::new(1))
            .expect("node state should load")
            .current_term,
        noraft::Term::new(2)
    );

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_ignores_stale_manifest_tmp() {
    let dir = unique_temp_dir("sukari-storage-manifest-tmp");
    create_two_segment_store(&dir);

    std::fs::write(dir.join(MANIFEST_TMP_FILE_NAME), "not-json")
        .expect("stale manifest tmp file should be written");

    let engine = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should reopen");
    assert_eq!(
        engine
            .load(noraft::NodeId::new(1))
            .expect("node state should load")
            .current_term,
        noraft::Term::new(2)
    );

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_falls_back_from_invalid_manifest() {
    let dir = unique_temp_dir("sukari-storage-invalid-manifest");
    create_two_segment_store(&dir);
    write_manifest(&dir, "{");

    let engine = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should reopen");
    assert_eq!(
        engine
            .load(noraft::NodeId::new(1))
            .expect("node state should load")
            .current_term,
        noraft::Term::new(2)
    );
    drop(engine);

    assert!(read_manifest(&dir).contains(r#""active_append_segment": "append-000002.segment""#));

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_falls_back_from_stale_manifest() {
    for (case, manifest) in [
        (
            "lower-existing-segment",
            r#"{"version":1,"active_append_segment":"append-000001.segment"}"#,
        ),
        (
            "missing-segment",
            r#"{"version":1,"active_append_segment":"append-999999.segment"}"#,
        ),
    ] {
        let dir = unique_temp_dir(&format!("sukari-storage-stale-manifest-{case}"));
        create_two_segment_store(&dir);
        write_manifest(&dir, manifest);

        let mut engine = StorageEngine::with_max_segment_len(&dir, SyncPolicy::UnsafeNoSync, 1)
            .expect("storage should reopen");
        assert_eq!(
            engine
                .load(noraft::NodeId::new(1))
                .expect("node state should load")
                .current_term,
            noraft::Term::new(2),
            "case: {case}"
        );
        engine
            .save_current_term(noraft::NodeId::new(1), noraft::Term::new(3))
            .expect("third term should be stored");
        drop(engine);

        assert!(segment_path_named(&dir, THIRD_SEGMENT_FILE_NAME).exists());
        assert!(!segment_path_named(&dir, "append-999999.segment").exists());

        std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
    }
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

    let initial_append = append(
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
        .append_entries(noraft::NodeId::new(3), initial_append)
        .expect("entries should be stored");

    let checkpoint = checkpoint(
        noraft::Term::new(2),
        None,
        snapshot(position(2, 2), b"snapshot"),
        append(
            position(2, 2),
            [noraft::LogEntry::Command],
            [(3, Bytes::from(b"three".as_slice()))],
        ),
    );
    engine
        .save_snapshot(noraft::NodeId::new(3), checkpoint)
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
fn storage_engine_replays_snapshot_checkpoints() {
    let dir = unique_temp_dir("sukari-storage-checkpoint");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");
    create_node(&mut engine, 3);

    engine
        .save_current_term(noraft::NodeId::new(3), noraft::Term::new(1))
        .expect("old term should be stored");
    engine
        .save_voted_for(noraft::NodeId::new(3), Some(noraft::NodeId::new(2)))
        .expect("old vote should be stored");
    let old_append = append(
        position(0, 0),
        [noraft::LogEntry::Command],
        [(1, Bytes::from(b"old-command".as_slice()))],
    );
    engine
        .append_entries(noraft::NodeId::new(3), old_append)
        .expect("old append should be stored");

    let checkpoint = checkpoint(
        noraft::Term::new(8),
        Some(noraft::NodeId::new(7)),
        snapshot(position(3, 3), b"checkpoint-snapshot"),
        append(
            position(3, 3),
            [
                noraft::LogEntry::Term(noraft::Term::new(4)),
                noraft::LogEntry::Command,
            ],
            [(5, Bytes::from(b"checkpoint-command".as_slice()))],
        ),
    );
    engine
        .save_snapshot(noraft::NodeId::new(3), checkpoint)
        .expect("checkpoint should be stored");

    let later_append = append(
        position(4, 5),
        [noraft::LogEntry::Command],
        [(6, Bytes::from(b"later-command".as_slice()))],
    );
    engine
        .append_entries(noraft::NodeId::new(3), later_append)
        .expect("later append should be stored");
    drop(engine);

    let engine = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should reopen");
    let state = engine
        .load(noraft::NodeId::new(3))
        .expect("node state should load");
    assert_eq!(state.current_term, noraft::Term::new(8));
    assert_eq!(state.voted_for, Some(noraft::NodeId::new(7)));
    assert_eq!(state.log.entries().prev_position(), position(3, 3));
    assert_eq!(state.log.entries().last_position(), position(4, 6));
    assert_eq!(state.commands.get(&index(1)), None);
    assert_eq!(
        state.commands.get(&index(5)).map(Bytes::as_slice),
        Some(&b"checkpoint-command"[..])
    );
    assert_eq!(
        state.commands.get(&index(6)).map(Bytes::as_slice),
        Some(&b"later-command"[..])
    );
    assert_eq!(
        state
            .snapshot
            .expect("checkpoint snapshot should be loaded")
            .data
            .as_slice(),
        b"checkpoint-snapshot"
    );

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_checkpoint_ignores_older_invalid_append() {
    let dir = unique_temp_dir("sukari-storage-checkpoint-invalid-old");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");
    create_node(&mut engine, 1);

    let invalid_old_append = append(
        position(9, 9),
        [noraft::LogEntry::Term(noraft::Term::new(10))],
        [],
    );
    engine
        .append_entries(noraft::NodeId::new(1), invalid_old_append)
        .expect("invalid old append should be stored");
    engine
        .save_snapshot(
            noraft::NodeId::new(1),
            checkpoint(
                noraft::Term::new(3),
                None,
                snapshot(noraft::LogPosition::ZERO, b"checkpoint"),
                append(noraft::LogPosition::ZERO, std::iter::empty(), []),
            ),
        )
        .expect("checkpoint should be stored");
    drop(engine);

    let engine = StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should reopen");
    let state = engine
        .load(noraft::NodeId::new(1))
        .expect("checkpoint should supersede the invalid old append");
    assert_eq!(state.current_term, noraft::Term::new(3));
    assert_eq!(
        state.log.entries().last_position(),
        noraft::LogPosition::ZERO
    );

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_rejects_invalid_snapshot_checkpoints() {
    let dir = unique_temp_dir("sukari-storage-invalid-checkpoint");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");
    create_node(&mut engine, 1);

    let err = engine
        .save_snapshot(
            noraft::NodeId::new(1),
            checkpoint(
                noraft::Term::new(3),
                None,
                snapshot(position(2, 2), b"checkpoint"),
                append(
                    position(1, 1),
                    [noraft::LogEntry::Term(noraft::Term::new(2))],
                    [],
                ),
            ),
        )
        .expect_err("checkpoint suffix should start at the snapshot position");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

    std::fs::remove_dir_all(&dir).expect("temporary directory should be removed");
}

#[test]
fn storage_engine_registry_hides_removed_node() {
    let dir = unique_temp_dir("sukari-storage-remove");
    let mut engine =
        StorageEngine::new(&dir, SyncPolicy::UnsafeNoSync).expect("storage should open");
    create_node(&mut engine, 5);
    engine
        .save_current_term(noraft::NodeId::new(5), noraft::Term::new(8))
        .expect("term should be stored");
    let segment_len_after_term = std::fs::metadata(segment_path(&dir))
        .expect("segment should exist")
        .len();

    engine
        .remove_node(noraft::NodeId::new(5))
        .expect("node should be removed");
    assert_eq!(
        std::fs::metadata(segment_path(&dir))
            .expect("segment should exist")
            .len(),
        segment_len_after_term
    );

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

fn snapshot(last_included: noraft::LogPosition, data: &[u8]) -> Snapshot {
    Snapshot {
        last_included,
        config: noraft::ClusterConfig::new(),
        data: Bytes::from(data),
    }
}

fn checkpoint(
    current_term: noraft::Term,
    voted_for: Option<noraft::NodeId>,
    snapshot: Snapshot,
    suffix: LogAppend,
) -> SnapshotCheckpoint {
    SnapshotCheckpoint {
        current_term,
        voted_for,
        snapshot,
        suffix,
    }
}

fn create_node(engine: &mut StorageEngine, node_id: u64) {
    engine
        .create_node(noraft::NodeId::new(node_id), NodeMetadata::default())
        .expect("node should be created");
}

fn create_two_segment_store(dir: &Path) {
    let mut engine = StorageEngine::with_max_segment_len(dir, SyncPolicy::UnsafeNoSync, 1)
        .expect("storage should open");
    create_node(&mut engine, 1);
    engine
        .save_current_term(noraft::NodeId::new(1), noraft::Term::new(1))
        .expect("first term should be stored");
    engine
        .save_current_term(noraft::NodeId::new(1), noraft::Term::new(2))
        .expect("second term should be stored");
}

fn create_checkpoint_index_store(dir: &Path) {
    let mut engine =
        StorageEngine::new(dir, SyncPolicy::UnsafeNoSync).expect("storage should open");
    create_node(&mut engine, 1);
    engine
        .save_snapshot(
            noraft::NodeId::new(1),
            checkpoint(
                noraft::Term::new(1),
                None,
                snapshot(noraft::LogPosition::ZERO, b"checkpoint"),
                append(noraft::LogPosition::ZERO, std::iter::empty(), []),
            ),
        )
        .expect("checkpoint should be stored");
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

fn write_node_registry(dir: &Path, text: &str) {
    std::fs::create_dir_all(dir).expect("temporary directory should be created");
    std::fs::write(dir.join(NODE_REGISTRY_FILE_NAME), text)
        .expect("node registry should be written");
}

fn write_manifest(dir: &Path, text: &str) {
    std::fs::create_dir_all(dir).expect("temporary directory should be created");
    std::fs::write(dir.join(MANIFEST_FILE_NAME), text).expect("manifest should be written");
}

fn write_checkpoint_index(dir: &Path, text: &str) {
    std::fs::create_dir_all(dir).expect("temporary directory should be created");
    std::fs::write(dir.join(CHECKPOINT_INDEX_FILE_NAME), text)
        .expect("checkpoint index should be written");
}

fn read_manifest(dir: &Path) -> String {
    std::fs::read_to_string(dir.join(MANIFEST_FILE_NAME)).expect("manifest should exist")
}

fn read_checkpoint_index(dir: &Path) -> String {
    std::fs::read_to_string(dir.join(CHECKPOINT_INDEX_FILE_NAME))
        .expect("checkpoint index should exist")
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
