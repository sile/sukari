//! Property-based tests for the `StorageEngine` replay round trip.

use std::{
    cell::Cell,
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use sukari::{
    Bytes, CommandPayload, LogAppend, NodeMetadata, NodeState, Snapshot, SnapshotCheckpoint,
    StorageEngine,
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
    Sync,
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
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

// Include 1 (rotation on every record) among the boundaries and let the interior
// distribution reach up to 512 so both frequent-rotation and no-rotation cases
// are exercised.
fn sample_max_segment_len(ctx: &mut noprop::TestCaseContext) -> u64 {
    noprop::sample_with_boundaries(
        ctx,
        &[1u64, 2, 128, 512],
        noprop::Ratio::one_nth(4),
        |ctx| noprop::sample_u64_in(ctx, 1..=512),
    )
}

fn sample_generated_config(ctx: &mut noprop::TestCaseContext) -> GeneratedConfig {
    GeneratedConfig {
        voters: sample_u64_vec(ctx, 0..=4, 1..=5),
        new_voters: sample_u64_vec(ctx, 0..=4, 1..=5),
        non_voters: sample_u64_vec(ctx, 0..=4, 1..=5),
    }
}

fn sample_u64_vec(
    ctx: &mut noprop::TestCaseContext,
    len_range: std::ops::RangeInclusive<usize>,
    value_range: std::ops::RangeInclusive<u64>,
) -> Vec<u64> {
    let len = noprop::sample_usize_in(ctx, len_range);
    (0..len)
        .map(|_| noprop::sample_u64_in(ctx, value_range.clone()))
        .collect()
}

fn sample_u8_vec(
    ctx: &mut noprop::TestCaseContext,
    len_range: std::ops::RangeInclusive<usize>,
) -> Vec<u8> {
    let len = noprop::sample_usize_in(ctx, len_range);
    (0..len).map(|_| noprop::sample_u8(ctx)).collect()
}

fn sample_generated_entry(ctx: &mut noprop::TestCaseContext) -> GeneratedEntry {
    match noprop::sample_weighted_index(ctx, &[1, 1, 1]) {
        0 => GeneratedEntry::Term(noprop::sample_u64_in(ctx, 0..=8)),
        1 => GeneratedEntry::ClusterConfig(sample_generated_config(ctx)),
        _ => GeneratedEntry::Command {
            tag: noprop::sample_u8(ctx),
            payload: sample_u8_vec(ctx, 0..=24),
        },
    }
}

fn sample_generated_entries(ctx: &mut noprop::TestCaseContext) -> Vec<GeneratedEntry> {
    let len = noprop::sample_usize_in(ctx, 0..=6);
    (0..len).map(|_| sample_generated_entry(ctx)).collect()
}

fn sample_optional_voted_for(ctx: &mut noprop::TestCaseContext) -> Option<u64> {
    if noprop::sample_bool(ctx) {
        Some(noprop::sample_u64_in(ctx, 1..=5))
    } else {
        None
    }
}

fn sample_operation(ctx: &mut noprop::TestCaseContext) -> Operation {
    match noprop::sample_weighted_index(ctx, &[1, 1, 1, 1]) {
        0 => Operation::CurrentTerm(noprop::sample_u64_in(ctx, 0..=16)),
        1 => Operation::VotedFor(sample_optional_voted_for(ctx)),
        2 => Operation::Append {
            anchor: noprop::sample_usize_in(ctx, 0..=32),
            entries: sample_generated_entries(ctx),
        },
        _ => Operation::SnapshotCheckpoint {
            current_term: noprop::sample_u64_in(ctx, 0..=16),
            voted_for: sample_optional_voted_for(ctx),
            snapshot_position: noprop::sample_usize_in(ctx, 0..=32),
            snapshot_config: sample_generated_config(ctx),
            snapshot_data: sample_u8_vec(ctx, 0..=32),
            suffix_entries: sample_generated_entries(ctx),
        },
    }
}

// Boundaries 0, 1, 32 make sure empty, single-operation, and full-length runs
// are all covered explicitly.
fn sample_operations(ctx: &mut noprop::TestCaseContext) -> Vec<Operation> {
    let len =
        noprop::sample_with_boundaries(ctx, &[0usize, 1, 32], noprop::Ratio::one_nth(4), |ctx| {
            noprop::sample_usize_in(ctx, 0..=32)
        });
    (0..len).map(|_| sample_operation(ctx)).collect()
}

fn sample_multi_node_operation(ctx: &mut noprop::TestCaseContext) -> MultiNodeOperation {
    // Weight Node-scoped operations heavily so cases mostly exercise state
    // transitions of existing nodes rather than churning the node set.
    match noprop::sample_weighted_index(ctx, &[1, 1, 1, 8]) {
        0 => MultiNodeOperation::Create(noprop::sample_u64_in(ctx, 1..=4)),
        1 => MultiNodeOperation::Remove(noprop::sample_usize_in(ctx, 0..=8)),
        2 => MultiNodeOperation::Sync,
        _ => MultiNodeOperation::Node {
            node: noprop::sample_usize_in(ctx, 0..=8),
            operation: sample_operation(ctx),
        },
    }
}

fn sample_multi_node_operations(ctx: &mut noprop::TestCaseContext) -> Vec<MultiNodeOperation> {
    let len =
        noprop::sample_with_boundaries(ctx, &[0usize, 1, 32], noprop::Ratio::one_nth(4), |ctx| {
            noprop::sample_usize_in(ctx, 0..=32)
        });
    (0..len).map(|_| sample_multi_node_operation(ctx)).collect()
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
    let mut next_index = prev_position.index.next();
    for entry in entries {
        match entry {
            GeneratedEntry::Term(term) => {
                log_entries.push(noraft::LogEntry::Term(noraft::Term::new(term)));
            }
            GeneratedEntry::ClusterConfig(config) => {
                log_entries.push(noraft::LogEntry::ClusterConfig(cluster_config(config)));
            }
            GeneratedEntry::Command { tag, payload } => {
                log_entries.push(noraft::LogEntry::Command);
                commands.insert(next_index, CommandPayload::new(tag, Bytes::from(payload)));
            }
        }
        next_index = next_index.next();
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

fn apply_append_to_expected(state: &mut NodeState, append: &LogAppend) {
    assert!(
        state.log.append_suffix(append.entries()),
        "append anchor does not exist in expected log"
    );

    let prev_index = append.entries().prev_position().index;
    state
        .command_payloads
        .retain(|index, _| *index <= prev_index);
    state
        .command_payloads
        .extend(append.command_payloads().clone());
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
            apply_append_to_expected(expected, &append);
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
        MultiNodeOperation::Sync => {
            engine.sync().expect("sync should succeed");
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

#[test]
fn storage_replay_roundtrip() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("SUKARI_PBT_SEED")?;

    // Independent coverage gates so any change to the search space that
    // silently drops one of these regions is caught up front.
    let cases_with_rotation = Cell::new(0usize);
    let cases_with_extra_snapshot = Cell::new(0usize);
    let cases_with_non_empty_log = Cell::new(0usize);

    let mut runner = noprop::Runner::new(seed);
    runner.run(128, |ctx| {
        let max_segment_len = sample_max_segment_len(ctx);
        let operations = sample_operations(ctx);

        let dir = TempDir::new("sukari-pbt-storage-replay");
        let mut engine = StorageEngine::with_max_segment_len(dir.path(), max_segment_len)
            .expect("storage should open");
        engine
            .create_node(NODE_ID, NodeMetadata::default())
            .expect("node should be created");
        let mut expected = initial_state();

        for operation in operations {
            apply_operation(&mut engine, &mut expected, operation);
        }

        let write_metrics = engine.metrics().clone();
        drop(engine);

        let mut engine = StorageEngine::new(dir.path()).expect("storage should reopen");
        let loaded = engine.load(NODE_ID).expect("node state should load");
        assert_eq!(loaded, expected);

        let mut all = engine.load_all().expect("all states should load");
        assert_eq!(all.remove(&NODE_ID), Some(expected.clone()));
        assert!(all.is_empty());

        if write_metrics.segment_rotations > 0 {
            cases_with_rotation.set(cases_with_rotation.get() + 1);
        }
        // create_node writes one initial checkpoint, so an extra snapshot save
        // is signaled by seeing more than one checkpoint for a single node.
        if write_metrics.snapshot_checkpoints_saved > 1 {
            cases_with_extra_snapshot.set(cases_with_extra_snapshot.get() + 1);
        }
        if !expected.log.entries().is_empty() {
            cases_with_non_empty_log.set(cases_with_non_empty_log.get() + 1);
        }

        Ok(())
    })?;

    assert!(
        cases_with_rotation.get() > 0,
        "no case exercised segment rotation\n{runner}"
    );
    assert!(
        cases_with_extra_snapshot.get() > 0,
        "no case saved an additional snapshot checkpoint\n{runner}"
    );
    assert!(
        cases_with_non_empty_log.get() > 0,
        "no case ended with a non-empty log\n{runner}"
    );
    Ok(())
}

#[test]
fn multi_node_storage_replay_roundtrip() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("SUKARI_PBT_SEED")?;

    let cases_with_multiple_active_nodes = Cell::new(0usize);
    let cases_with_removed_node = Cell::new(0usize);
    let cases_with_rotation = Cell::new(0usize);
    let cases_with_extra_snapshot = Cell::new(0usize);

    let mut runner = noprop::Runner::new(seed);
    runner.run(64, |ctx| {
        let max_segment_len = sample_max_segment_len(ctx);
        let operations = sample_multi_node_operations(ctx);

        let dir = TempDir::new("sukari-pbt-multi-node-storage-replay");
        let mut engine = StorageEngine::with_max_segment_len(dir.path(), max_segment_len)
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

        let write_metrics = engine.metrics().clone();
        drop(engine);

        let mut engine = StorageEngine::new(dir.path()).expect("storage should reopen");
        for (node_id, expected) in &active {
            let loaded = engine.load(*node_id).expect("node state should load");
            assert_eq!(&loaded, expected);
        }

        let all = engine.load_all().expect("all states should load");
        assert_eq!(all, active);

        if active.len() >= 2 {
            cases_with_multiple_active_nodes.set(cases_with_multiple_active_nodes.get() + 1);
        }
        if created.len() > active.len() {
            cases_with_removed_node.set(cases_with_removed_node.get() + 1);
        }
        if write_metrics.segment_rotations > 0 {
            cases_with_rotation.set(cases_with_rotation.get() + 1);
        }
        // create_node writes one initial checkpoint per node, so subtract that
        // baseline before deciding whether an extra snapshot was saved.
        if write_metrics.snapshot_checkpoints_saved > created.len() as u64 {
            cases_with_extra_snapshot.set(cases_with_extra_snapshot.get() + 1);
        }

        Ok(())
    })?;

    assert!(
        cases_with_multiple_active_nodes.get() > 0,
        "no case ended with two or more active nodes\n{runner}"
    );
    assert!(
        cases_with_removed_node.get() > 0,
        "no case removed a node\n{runner}"
    );
    assert!(
        cases_with_rotation.get() > 0,
        "no case exercised segment rotation\n{runner}"
    );
    assert!(
        cases_with_extra_snapshot.get() > 0,
        "no case saved an additional snapshot checkpoint\n{runner}"
    );
    Ok(())
}
