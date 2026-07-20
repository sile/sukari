# sukari

[![sukari](https://img.shields.io/crates/v/sukari.svg)](https://crates.io/crates/sukari)
[![Documentation](https://docs.rs/sukari/badge.svg)](https://docs.rs/sukari)
[![Actions Status](https://github.com/sile/sukari/workflows/CI/badge.svg)](https://github.com/sile/sukari/actions)
![License](https://img.shields.io/crates/l/sukari)

`sukari` is shared append-segment Raft state storage for
[`noraft`](https://github.com/sile/noraft)-based applications that run one or
more local Raft nodes.

The name `sukari` refers to a Japanese fishing basket kept in water, evoking a
small storage container beside a raft.

## Key Characteristics

`sukari` uses `noraft` protocol types directly and owns the shared storage
format, replay, checkpoints, and whole-segment garbage collection. It favors a
simple runtime path: ordinary writes append records to shared segments, and full
reads are mainly for startup or recovery. This keeps typical Raft storage writes
on an append-only path with storage-local validation. `load()` and `load_all()`
read snapshots and retained log suffixes as whole values, so huge payloads and
random-read log paging are out of scope.

## Example

The public API uses `noraft` and `nojson` types directly.

```rust
use std::collections::BTreeMap;

use sukari::{Bytes, CommandPayload, LogAppend, NodeMetadata, StorageEngine};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join("sukari-readme-example");
    let _ = std::fs::remove_dir_all(&dir);

    // Open one storage directory.
    let mut storage = StorageEngine::new(&dir)?;
    let node_id = noraft::NodeId::new(1);

    // Node IDs must be registered before node state can be written.
    storage.create_node(
        node_id,
        NodeMetadata::new(
            true,
            nojson::RawJsonOwned::parse(r#"{"role":"control"}"#)?,
        ),
    )?;

    // Persist Raft hard state as append records.
    storage.save_current_term(node_id, noraft::Term::new(1))?;
    storage.save_voted_for(node_id, None)?;

    // Command entries carry opaque payload bytes plus a small application tag.
    let entries = noraft::LogEntries::from_iter(
        noraft::LogPosition::ZERO,
        [
            noraft::LogEntry::Term(noraft::Term::new(1)),
            noraft::LogEntry::Command,
        ],
    );
    let mut command_payloads = BTreeMap::new();
    command_payloads.insert(
        noraft::LogIndex::new(2),
        CommandPayload::new(0, Bytes::from(b"command".as_slice())),
    );

    storage.append_entries(node_id, LogAppend::new(entries, command_payloads)?)?;
    storage.sync()?;

    // Loading replays the retained records for the node into a NodeState.
    let state = storage.load(node_id)?;
    assert_eq!(state.current_term, noraft::Term::new(1));

    Ok(())
}
```

## Storage Model

`StorageEngine` owns one append writer for a storage directory. It acquires an
exclusive OS file lock on the persistent `write.lock` file, so a second writer
for the same directory fails to open. The lock is released when the engine is
dropped; the `write.lock` file itself remains. `sukari` supports local
filesystems only, not network filesystems.

Read-only startup and recovery can run concurrently with the writer through
`sukari::load()`, `sukari::load_all()`, `sukari::nodes()`, and
`sukari::startup_nodes()`. These functions do not acquire `write.lock` or
modify the storage directory. They read `nodes.json` and `checkpoints.json` for
each call, so node creation and removal by the writer are reflected by later
calls. A concurrent read observes complete records available while it runs; it
is not a point-in-time snapshot.

`StorageEngine` does not add internal mutexes around writes; runtimes that need
concurrent access serialize storage requests outside this crate.

Segment append durability is caller-managed. Ordinary node-state writes append
records without synchronizing the active segment; callers decide when to make
pending segment appends durable by calling `sync()`. Metadata JSON files,
directory updates, segment rotation boundaries, and checkpoint records that are
referenced from `checkpoints.json` are synchronized by the storage engine.

Node IDs must be created with `create_node()` before node state can be written
or loaded. Removed node IDs remain reserved. Writes are appended as received:
the storage layer validates record-local invariants, but it does not load the
node state to validate log append anchors before writing. Replay applies records
in append order and resolves divergent log suffixes.

Storage write and metadata persistence errors are fatal to the engine instance.
Callers should stop using that instance and diagnose the storage state before
reopening it.

Snapshots are saved as checkpoints. A checkpoint contains the current term,
voted-for node, latest snapshot, and retained log suffix. Earlier records for
the node become obsolete for replay and whole-segment garbage collection.

`load()` and `load_all()` construct `NodeState` values on demand. The design
assumes that the loaded snapshot payload and retained log suffix are sized for
in-memory loading.

## Documents

- [Architecture Overview](docs/architecture-overview.md) explains the storage
  model, node registry, checkpoints, replay, metrics, and garbage collection.
- [Segment Format](docs/segment-format.md) specifies the on-disk segment record
  format.
