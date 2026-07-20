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

use sukari::{Bytes, CommandPayload, LogAppend, NodeMetadata, StorageEngine, SyncPolicy};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join("sukari-readme-example");
    let _ = std::fs::remove_dir_all(&dir);

    // Open one storage directory and synchronize every durable update.
    let mut storage = StorageEngine::new(&dir, SyncPolicy::Strict)?;
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
    storage.flush()?;

    // Loading replays the retained records for the node into a NodeState.
    let state = storage.load(node_id)?;
    assert_eq!(state.current_term, noraft::Term::new(1));

    Ok(())
}
```

## Storage Model

`StorageEngine` owns one append writer for a storage directory. It does not add
internal mutexes around writes; runtimes that need concurrent access serialize
storage requests outside this crate.

`SyncPolicy` controls explicit durability. `Strict` synchronizes every storage
record and metadata update. `Batch` synchronizes segment data after configured
record or byte thresholds, while metadata replacements remain synchronized;
`flush()` also synchronizes pending segment data. `UnsafeNoSync` skips explicit
synchronization and leaves persistence timing to the operating system.

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
