# Architecture Overview

This document records the current design direction of `sukari`.
Update it as the implementation changes.

## Scope

`sukari` is a shared segmented storage engine for Raft state.

The crate should own:

- shared segment file format
- manifest format
- deterministic replay
- per-node replay state
- rewrite and purge state machines
- node removal tombstones
- node registry metadata for storage namespace ownership
- storage metrics
- migration tools from per-node WAL if needed

The crate should not own Raft protocol logic, networking, runtime tasks, timers,
or application command execution. Those responsibilities belong to `noraft` or
higher-level runtime and adapter crates.

## Crate Boundary

`sukari` exists as a separate crate because shared segmented storage has complex
failure modes and garbage collection rules. The API should stay compatible with
the storage operations commonly emitted by `noraft`-based runtimes:

- save current term
- save voted-for node
- append log entries and command payloads
- save latest snapshot state
- replay a node's persistent state at startup
- remove all durable data for a removed node

The standard architecture treats `noraft::NodeId` values as globally unique.
`sukari` should therefore route records by node ID. A small node registry may
belong in this crate so the storage layer can define which node IDs exist in a
storage instance and so a process can discover startup nodes before loading an
external control plane. Cluster membership, group placement, and orchestration
state should stay in the control plane above the storage layer.

## Current Baseline

The current implementation provides a conservative append-only baseline with
one active shared append segment at a time:

```text
storage/
  append-000001.segment
  append-000002.segment
```

Segment file names are parsed into internal segment names with a segment kind
and non-zero numeric segment ID. Startup selects the highest-numbered append
segment as the active segment. New writes rotate to the next append segment when
the configured segment length would be exceeded. A single record that exceeds
the limit is written to an empty segment by itself.

The public API exposes node ID based storage operations on a single engine
writer:

- load a node state
- save current term
- save voted-for node
- append log entries and command payloads
- save snapshot state
- flush pending writes
- load all non-removed node states
- record a node removal tombstone
- remove all storage data

`sukari` does not add internal mutexes around writes. Callers that need
concurrent runtime integration should own serialization outside this crate, for
example by routing storage requests through a dedicated storage task.

Write operations append storage records as they are received. The storage layer
validates record-local invariants, such as frame checksums and command payload
mapping, but it should not check whether a log append anchor matches the
currently loaded log before writing. Divergent log suffixes caused by leader
changes are reconciled by deterministic replay, which applies records in their
original append order.

Each segment record uses the `SKR1` frame format:

- magic
- body length
- CRC-32C checksum of the body
- node ID
- record kind
- encoded payload

The current record kinds are:

- current term
- voted-for node
- log append
- snapshot
- node removal tombstone

`SKR1` is unstable while the crate is unreleased. Incompatible storage changes
can still move to a new magic value if keeping experimental data is not useful.

## Future Layout

The intended full design is a shared segmented append-only design:

```text
storage/
  nodes.json
  manifest
  append-000001.segment
  append-000002.segment
  rewrite-000010.segment
```

## Node Registry

The intended design includes a small JSON node registry file:

```text
storage/
  nodes.json
  append-000001.segment
  append-000002.segment
```

The registry records which `noraft::NodeId` values are valid for this storage
instance. Nodes are added with `create_node()` and removed with `remove_node()`.
Storage operations such as `load()`, `save_current_term()`, `save_voted_for()`,
`append_entries()`, and `save_snapshot()` should fail for node IDs that have not
been created.

Each node entry should contain a typed `startup` flag and opaque JSON metadata.
The `startup` flag means the node should be considered during process startup
before any external control plane has been loaded. The metadata JSON is
application-defined and should be stored and returned without interpretation by
`sukari`. The JSON representation should use the `nojson` crate.

`nodes.json` should contain all node entries in one small file and be updated by
atomic replacement. The update protocol should write a temporary file, sync it,
rename it over `nodes.json`, and sync the parent directory when the sync policy
requires durable metadata.

The registry is a storage namespace and startup-discovery mechanism. It is not
the authoritative Raft cluster membership, group placement, or orchestration
state; those belong to the control plane above `sukari`.

## Replay

Startup discovers `append-*.segment` and `rewrite-*.segment` files, replays them
in deterministic file-name order, and rebuilds per-node state:

- current term
- voted-for node
- log entries
- command payloads
- latest snapshot metadata and data or reference
- node removal tombstones

The first version should not require an on-disk random-read index. Normal reads
are expected to be rare and mostly limited to startup. If replay becomes too
slow, a manifest or hint file can be added later as an accelerator.

The active segment tolerates a trailing partial record and truncates it during
replay. Checksum mismatches are treated as corruption.

## Memory Model

`StorageEngine` should not keep fully replayed node state in memory for normal
writes. Opening the engine may scan segment frames for recovery, but full
`StorageState` construction should happen only when loading state. `load()`
constructs the requested node state, while `load_all()` constructs all
non-removed node states.

The initial design assumes that the loaded snapshot payload and the log entries
after that snapshot fit comfortably in memory. This keeps the storage API simple
and matches the expected Raft usage. Very large snapshots and large blob
payloads are poor fits for Raft and are not target use cases. Random-read log
paging is also not a target use case. Lagging-node catch-up should read the
already loaded log suffix from memory; synchronous disk reads can block leader
replication, while asynchronous paging complicates the runtime for little gain.

## Compaction And Garbage Collection

Garbage collection is the main complexity of shared storage.

Records for many nodes can be mixed in one segment, so a segment can only be
deleted when all records in that segment are obsolete. Snapshot progress for one
node is not sufficient by itself.

The engine will need one or more of these mechanisms:

- segment-level live record accounting
- rewrite of still-live records from old segments into rewrite segments
- grouping by node shard or traffic class to reduce mixed-lifetime segments
- temporary space amplification while old mixed segments remain live

The first implementation should keep these rules explicit and conservative.

## Crash Recovery

The storage format needs explicit recovery rules for:

- partial record at the end of the active segment
- checksum mismatch
- manifest update after segment creation
- segment rotation
- rewrite segment creation
- rewrite completion
- old segment deletion
- node removal tombstones
- atomic replacement of `nodes.json`
- process crash after fsyncing records before updating manifests

The manifest should accelerate discovery, not be the only source of truth,
unless its update protocol is very carefully specified.
