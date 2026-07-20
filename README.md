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

The crate provides a storage backend that uses `noraft` protocol types
directly. When multiple nodes share a storage directory, it can batch durable
writes into shared append-only segment files. Higher-level runtime and
control-plane crates can use it without taking ownership of shared storage
format, replay, compaction, and recovery details.

## Key Characteristics

`sukari` favors a simple runtime path: ordinary writes append records to shared
segments, and full reads are mainly for startup or recovery. This should make
typical Raft storage writes predictable. Recovery APIs read snapshots and
retained log suffixes as whole values, so huge payloads and random-read log
paging are out of scope.

## Storage Model

`StorageEngine` owns one append writer for a storage directory. It does not add
internal mutexes around writes; runtimes that need concurrent access serialize
storage requests outside this crate.

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
assumes that the loaded snapshot payload and retained log suffix fit comfortably
in memory.

## Example

The public API uses `noraft` and `nojson` types directly.

```rust
use std::collections::BTreeMap;

use sukari::{Bytes, CommandPayload, LogAppend, NodeMetadata, StorageEngine, SyncPolicy};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join("sukari-readme-example");
    let _ = std::fs::remove_dir_all(&dir);

    let mut storage = StorageEngine::new(&dir, SyncPolicy::Strict)?;
    let node_id = noraft::NodeId::new(1);

    storage.create_node(
        node_id,
        NodeMetadata::new(
            true,
            nojson::RawJsonOwned::parse(r#"{"role":"control"}"#)?,
        ),
    )?;

    storage.save_current_term(node_id, noraft::Term::new(1))?;
    storage.save_voted_for(node_id, None)?;

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

    let state = storage.load(node_id)?;
    assert_eq!(state.current_term, noraft::Term::new(1));

    Ok(())
}
```

## Documents

- [Architecture overview](docs/architecture-overview.md)
- [Segment format](docs/segment-format.md)
