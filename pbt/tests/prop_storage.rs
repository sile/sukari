use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use proptest::prelude::*;
use sukari::{
    Bytes, CommandPayload, LogAppend, NodeMetadata, NodeState, Snapshot, SnapshotCheckpoint,
    StorageEngine, SyncPolicy,
};

const NODE_ID: noraft::NodeId = noraft::NodeId::new(1);

#[derive(Debug, Clone)]
enum Operation {
    CurrentTerm(u64),
    VotedFor(Option<u64>),
    Append {
        anchor: usize,
        entries: Vec<GeneratedEntry>,
    },
    SnapshotCheckpoint {
        current_term: u64,
        voted_for: Option<u64>,
        snapshot_position: usize,
        snapshot_config: GeneratedConfig,
        snapshot_data: Vec<u8>,
        suffix_entries: Vec<GeneratedEntry>,
    },
}

#[derive(Debug, Clone)]
enum MultiNodeOperation {
    Create(u64),
    Remove(usize),
    Flush,
    Node { node: usize, operation: Operation },
}

#[derive(Debug, Clone)]
enum GeneratedEntry {
    Term(u64),
    ClusterConfig(GeneratedConfig),
    Command { tag: u8, payload: Vec<u8> },
}

#[derive(Debug, Clone)]
struct GeneratedConfig {
    voters: Vec<u64>,
    new_voters: Vec<u64>,
    non_voters: Vec<u64>,
}

#[derive(Debug)]
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(prefix: &str) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);

        let path = std::env::temp_dir().join(format!(
            "{prefix}-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        match std::fs::remove_dir_all(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("failed to remove old temporary directory {path:?}: {e}"),
        }
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        match std::fs::remove_dir_all(&self.path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => {}
        }
    }
}

fn generated_config() -> impl Strategy<Value = GeneratedConfig> {
    (
        proptest::collection::vec(1u64..=5, 0..=4),
        proptest::collection::vec(1u64..=5, 0..=4),
        proptest::collection::vec(1u64..=5, 0..=4),
    )
        .prop_map(|(voters, new_voters, non_voters)| GeneratedConfig {
            voters,
            new_voters,
            non_voters,
        })
}

fn generated_entry() -> impl Strategy<Value = GeneratedEntry> {
    prop_oneof![
        (0u64..=8).prop_map(GeneratedEntry::Term),
        generated_config().prop_map(GeneratedEntry::ClusterConfig),
        (any::<u8>(), proptest::collection::vec(any::<u8>(), 0..=24))
            .prop_map(|(tag, payload)| GeneratedEntry::Command { tag, payload }),
    ]
}

fn operation() -> impl Strategy<Value = Operation> {
    prop_oneof![
        (0u64..=16).prop_map(Operation::CurrentTerm),
        proptest::option::of(1u64..=5).prop_map(Operation::VotedFor),
        (
            0usize..=32,
            proptest::collection::vec(generated_entry(), 0..=6),
        )
            .prop_map(|(anchor, entries)| Operation::Append { anchor, entries }),
        (
            0u64..=16,
            proptest::option::of(1u64..=5),
            0usize..=32,
            generated_config(),
            proptest::collection::vec(any::<u8>(), 0..=32),
            proptest::collection::vec(generated_entry(), 0..=6),
        )
            .prop_map(
                |(
                    current_term,
                    voted_for,
                    snapshot_position,
                    snapshot_config,
                    snapshot_data,
                    suffix_entries,
                )| Operation::SnapshotCheckpoint {
                    current_term,
                    voted_for,
                    snapshot_position,
                    snapshot_config,
                    snapshot_data,
                    suffix_entries,
                },
            ),
    ]
}

fn operations() -> impl Strategy<Value = Vec<Operation>> {
    proptest::collection::vec(operation(), 0..=32)
}

fn sync_policy() -> impl Strategy<Value = SyncPolicy> {
    prop_oneof![
        6 => Just(SyncPolicy::UnsafeNoSync),
        2 => (0usize..=3, 0u64..=256).prop_map(|(max_records, max_bytes)| {
            SyncPolicy::Batch {
                max_records,
                max_bytes,
            }
        }),
        1 => Just(SyncPolicy::Strict),
    ]
}

fn multi_node_operation() -> impl Strategy<Value = MultiNodeOperation> {
    prop_oneof![
        1 => (1u64..=4).prop_map(MultiNodeOperation::Create),
        1 => (0usize..=8).prop_map(MultiNodeOperation::Remove),
        1 => Just(MultiNodeOperation::Flush),
        8 => (0usize..=8, operation()).prop_map(|(node, operation)| {
            MultiNodeOperation::Node { node, operation }
        }),
    ]
}

fn multi_node_operations() -> impl Strategy<Value = Vec<MultiNodeOperation>> {
    proptest::collection::vec(multi_node_operation(), 0..=32)
}

fn cluster_config(config: GeneratedConfig) -> noraft::ClusterConfig {
    noraft::ClusterConfig {
        voters: node_set(config.voters),
        new_voters: node_set(config.new_voters),
        non_voters: node_set(config.non_voters),
    }
}

fn node_set(ids: Vec<u64>) -> BTreeSet<noraft::NodeId> {
    ids.into_iter().map(noraft::NodeId::new).collect()
}

fn log_positions(state: &NodeState) -> Vec<noraft::LogPosition> {
    std::iter::once(state.log.entries().prev_position())
        .chain(
            state
                .log
                .entries()
                .iter_with_positions()
                .map(|(position, _)| position),
        )
        .collect()
}

fn choose_position(state: &NodeState, choice: usize) -> noraft::LogPosition {
    let positions = log_positions(state);
    positions[choice % positions.len()]
}

fn log_append(prev_position: noraft::LogPosition, entries: Vec<GeneratedEntry>) -> LogAppend {
    let mut log_entries = Vec::new();
    let mut commands = BTreeMap::new();
    for (next_index, entry) in (prev_position.index.get() + 1..).zip(entries) {
        match entry {
            GeneratedEntry::Term(term) => {
                log_entries.push(noraft::LogEntry::Term(noraft::Term::new(term)));
            }
            GeneratedEntry::ClusterConfig(config) => {
                log_entries.push(noraft::LogEntry::ClusterConfig(cluster_config(config)));
            }
            GeneratedEntry::Command { tag, payload } => {
                log_entries.push(noraft::LogEntry::Command);
                commands.insert(
                    noraft::LogIndex::new(next_index),
                    CommandPayload::new(tag, Bytes::from(payload)),
                );
            }
        }
    }

    LogAppend::new(
        noraft::LogEntries::from_iter(prev_position, log_entries),
        commands,
    )
    .expect("generated append should have matching command payloads")
}

fn initial_state() -> NodeState {
    let snapshot = Snapshot {
        last_included: noraft::LogPosition::ZERO,
        config: noraft::ClusterConfig::new(),
        data: Bytes::default(),
    };
    NodeState {
        current_term: noraft::Term::ZERO,
        voted_for: None,
        log: noraft::Log::new(
            snapshot.config.clone(),
            noraft::LogEntries::new(snapshot.last_included),
        ),
        command_payloads: BTreeMap::new(),
        snapshot: Some(snapshot),
    }
}

fn apply_append_to_expected(state: &mut NodeState, append: &LogAppend) -> Result<(), String> {
    if !state
        .log
        .entries()
        .contains(append.entries().prev_position())
    {
        return Err("append anchor does not exist in expected log".to_owned());
    }

    let keep_len = append.entries().prev_position().index.get()
        - state.log.entries().prev_position().index.get();
    let keep_len =
        usize::try_from(keep_len).map_err(|_| "log suffix length exceeds usize".to_owned())?;

    let mut entries = state.log.entries().clone();
    entries.truncate(keep_len);
    for entry in append.entries().iter() {
        entries.push(entry);
    }
    state.log = noraft::Log::new(state.log.snapshot_config().clone(), entries);

    let prev_index = append.entries().prev_position().index;
    state
        .command_payloads
        .retain(|index, _| *index <= prev_index);
    state
        .command_payloads
        .extend(append.command_payloads().clone());
    Ok(())
}

fn apply_operation(engine: &mut StorageEngine, expected: &mut NodeState, operation: Operation) {
    apply_node_operation(engine, NODE_ID, expected, operation);
}

fn apply_node_operation(
    engine: &mut StorageEngine,
    node_id: noraft::NodeId,
    expected: &mut NodeState,
    operation: Operation,
) {
    match operation {
        Operation::CurrentTerm(term) => {
            let term = noraft::Term::new(term);
            engine
                .save_current_term(node_id, term)
                .expect("term should be stored");
            expected.current_term = term;
        }
        Operation::VotedFor(voted_for) => {
            let voted_for = voted_for.map(noraft::NodeId::new);
            engine
                .save_voted_for(node_id, voted_for)
                .expect("vote should be stored");
            expected.voted_for = voted_for;
        }
        Operation::Append { anchor, entries } => {
            let append = log_append(choose_position(expected, anchor), entries);
            engine
                .append_entries(node_id, append.clone())
                .expect("append should be stored");
            apply_append_to_expected(expected, &append).expect("generated append should apply");
        }
        Operation::SnapshotCheckpoint {
            current_term,
            voted_for,
            snapshot_position,
            snapshot_config,
            snapshot_data,
            suffix_entries,
        } => {
            let snapshot = Snapshot {
                last_included: choose_position(expected, snapshot_position),
                config: cluster_config(snapshot_config),
                data: Bytes::from(snapshot_data),
            };
            let suffix = log_append(snapshot.last_included, suffix_entries);
            let checkpoint = SnapshotCheckpoint {
                current_term: noraft::Term::new(current_term),
                voted_for: voted_for.map(noraft::NodeId::new),
                snapshot: snapshot.clone(),
                suffix,
            };
            engine
                .save_snapshot(node_id, checkpoint.clone())
                .expect("snapshot checkpoint should be stored");

            expected.current_term = checkpoint.current_term;
            expected.voted_for = checkpoint.voted_for;
            expected.log =
                noraft::Log::new(snapshot.config.clone(), checkpoint.suffix.entries().clone());
            expected.command_payloads = checkpoint.suffix.command_payloads().clone();
            expected.snapshot = Some(snapshot);
        }
    }
}

fn choose_active_node(
    active: &BTreeMap<noraft::NodeId, NodeState>,
    choice: usize,
) -> Option<noraft::NodeId> {
    if active.is_empty() {
        return None;
    }
    active.keys().copied().nth(choice % active.len())
}

fn apply_multi_node_operation(
    engine: &mut StorageEngine,
    active: &mut BTreeMap<noraft::NodeId, NodeState>,
    created: &mut BTreeSet<noraft::NodeId>,
    operation: MultiNodeOperation,
) {
    match operation {
        MultiNodeOperation::Create(node) => {
            let node_id = noraft::NodeId::new(node);
            if created.insert(node_id) {
                engine
                    .create_node(node_id, NodeMetadata::default())
                    .expect("node should be created");
                active.insert(node_id, initial_state());
            }
        }
        MultiNodeOperation::Remove(choice) => {
            if let Some(node_id) = choose_active_node(active, choice) {
                engine.remove_node(node_id).expect("node should be removed");
                active.remove(&node_id);
            }
        }
        MultiNodeOperation::Flush => {
            engine.flush().expect("flush should succeed");
        }
        MultiNodeOperation::Node { node, operation } => {
            if let Some(node_id) = choose_active_node(active, node) {
                let expected = active
                    .get_mut(&node_id)
                    .expect("chosen active node should exist");
                apply_node_operation(engine, node_id, expected, operation);
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn storage_replay_roundtrip(
        max_segment_len in 1u64..=512,
        operations in operations(),
    ) {
        let dir = TempDir::new("sukari-pbt-storage-replay");
        let mut engine = StorageEngine::with_max_segment_len(
            dir.path(),
            SyncPolicy::UnsafeNoSync,
            max_segment_len,
        )
        .expect("storage should open");
        engine
            .create_node(NODE_ID, NodeMetadata::default())
            .expect("node should be created");
        let mut expected = initial_state();

        for operation in operations {
            apply_operation(&mut engine, &mut expected, operation);
        }
        drop(engine);

        let mut engine =
            StorageEngine::new(dir.path(), SyncPolicy::UnsafeNoSync).expect("storage should reopen");
        let loaded = engine.load(NODE_ID).expect("node state should load");
        prop_assert_eq!(&loaded, &expected);

        let mut all = engine.load_all().expect("all states should load");
        prop_assert_eq!(all.remove(&NODE_ID), Some(expected));
        prop_assert!(all.is_empty());
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn multi_node_storage_replay_roundtrip(
        sync in sync_policy(),
        max_segment_len in 1u64..=512,
        operations in multi_node_operations(),
    ) {
        let dir = TempDir::new("sukari-pbt-multi-node-storage-replay");
        let mut engine = StorageEngine::with_max_segment_len(
            dir.path(),
            sync,
            max_segment_len,
        )
        .expect("storage should open");

        let initial_node = NODE_ID;
        engine
            .create_node(initial_node, NodeMetadata::default())
            .expect("initial node should be created");
        let mut active = BTreeMap::from([(initial_node, initial_state())]);
        let mut created = BTreeSet::from([initial_node]);

        for operation in operations {
            apply_multi_node_operation(&mut engine, &mut active, &mut created, operation);
        }
        drop(engine);

        let mut engine =
            StorageEngine::new(dir.path(), sync).expect("storage should reopen");
        for (node_id, expected) in &active {
            let loaded = engine.load(*node_id).expect("node state should load");
            prop_assert_eq!(&loaded, expected);
        }

        let all = engine.load_all().expect("all states should load");
        prop_assert_eq!(all, active);
    }
}
