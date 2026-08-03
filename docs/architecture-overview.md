# Architecture Overview

This document describes the current storage architecture of `sukari`.

## Key Characteristics

`sukari` favors a simple runtime path: ordinary writes append records to shared
segments, and full reads are mainly for startup or recovery. This keeps typical
Raft storage writes on an append-only path with storage-local validation.
`load()` and `load_all()` read snapshots and retained log suffixes as whole
values, so huge payloads and random-read log paging are out of scope.

## Scope

`sukari` is a shared segmented storage engine for Raft state.

The crate owns:

- shared segment file format
- garbage-collection metadata format
- deterministic replay
- per-node replay state
- snapshot checkpoint semantics
- automatic whole-segment garbage collection
- node registry metadata for storage namespace ownership
- storage metrics

The crate does not own Raft protocol logic, networking, runtime tasks, timers,
or application command execution. Those responsibilities belong to `noraft` or
higher-level runtime and adapter crates.

## Crate Boundary

`sukari` exists as a separate crate because shared segmented storage has complex
failure modes and garbage collection rules. The API follows the storage
operations commonly emitted by `noraft`-based runtimes:

- save current term
- save voted-for node
- append log entries and command payloads
- save snapshot checkpoints that supersede earlier node records
- replay a node's persistent state at startup
- mark a node removed and reserve its node ID

The standard architecture treats `noraft::NodeId` values as globally unique.
`sukari` routes records by node ID. The node registry belongs in this crate so
the storage layer can define which node IDs exist in a storage instance and so a
process can inspect startup metadata before loading an external control plane.
Cluster membership, group placement, and orchestration state stay in the control
plane above the storage layer.

## Storage Layout

The implementation uses one active shared append segment at a time. A populated
storage directory uses shared append segments plus small JSON metadata files:

```text
storage/
  write.lock
  nodes.json
  checkpoints.json
  append-0.segment
  append-1.segment
```

Segment file names are parsed into internal append segment names with canonical
decimal segment IDs. Append segment IDs start at `0` and are written without
zero padding. Startup selects the active append segment by scanning canonical
append segment file names and opening the greatest segment ID. Non-segment files
and non-canonical append segment names are ignored. New writes rotate to the next
append segment when the configured segment length would be exceeded. The
configured length is a rotation threshold, not a hard per-record limit. If a
record frame is larger than the threshold, the writer rotates once when needed,
writes the frame to an empty segment, and lets that segment exceed the
threshold.

## Access Modes

`StorageEngine` is the read-write access mode. It opens or creates the
persistent `write.lock` file and acquires an exclusive OS file lock for its
lifetime. A second `StorageEngine` for the same directory fails to open while
the lock is held. The lock is released when the engine drops, but `write.lock`
is intentionally not removed: its existence does not mean a writer is active.

The crate supports local filesystems only. Network filesystem behavior,
including file locking, is not supported.

The read-only functions `sukari::load()`, `sukari::load_all()`,
and `sukari::nodes()` do not acquire `write.lock`. They can run while a writer
is active and never recover, truncate, garbage collect, or otherwise modify the
directory. Each call reads `nodes.json` and `checkpoints.json` again, so node
creation and removal become visible to later read-only calls. A concurrent read
can observe complete records available while it runs, but does not provide a
point-in-time snapshot.

The public API exposes node ID based storage operations on a single engine
writer:

- create a node with startup metadata
- inspect active node metadata
- load a node state
- save current term
- save voted-for node
- append log entries and command payloads
- save snapshot checkpoint
- synchronize pending segment appends
- load all non-removed node states
- remove a node and reserve its node ID

Storage operations for node state fail unless the target node ID has been
created with `create_node()`. Removed node IDs are permanently reserved and
cannot be created again.

`sukari` does not add internal mutexes around writes. Callers that need
concurrent runtime integration own serialization outside this crate, for example
by routing storage requests through a dedicated storage task.

Segment append durability is caller-managed. Ordinary node-state writes append
records without synchronizing the active segment; callers decide when to make
pending segment appends durable by calling `sync()`. This lets a runtime batch
multiple storage requests before replying to their callers. Metadata JSON files,
directory updates, segment rotation boundaries, and checkpoint records that are
referenced from `checkpoints.json` are synchronized by the storage engine.

Write operations append storage records as they are received. The storage layer
validates record-local invariants, such as frame checksums and command payload
mapping, but it does not check whether a log append anchor matches the currently
loaded log before writing. Divergent log suffixes caused by leader changes are
reconciled by deterministic replay, which applies records in their original
append order.

Storage write and metadata persistence errors are fatal to the engine instance.
They are not treated as per-record retryable errors because storage failure can
affect Raft durability. After such an error, callers should stop using the
instance, diagnose the storage state, and reopen only after choosing an
appropriate recovery action.

Command payload tags are persisted and replayed without interpretation. Their
meaning belongs to the caller.

The on-disk segment record format is specified in the
[Segment Format](segment-format.md) document.

## Node Registry

The implementation stores a small JSON node registry file:

```text
storage/
  nodes.json
  append-0.segment
  append-1.segment
```

The registry records which `noraft::NodeId` values belong to this storage
instance. Nodes are added with `create_node()` and removed with `remove_node()`.
Storage operations such as `load()`, `save_current_term()`, `save_voted_for()`,
`append_entries()`, and `save_snapshot()` fail for node IDs that are not active.

`nodes.json` has this schema:

```json
{
  "version": 1,
  "nodes": {
    "1": {
      "startup": true,
      "metadata": { "role": "control" },
      "removed": false
    }
  }
}
```

Node IDs are encoded as decimal string keys. The `removed` flag keeps removed
node IDs reserved, so `create_node()` rejects an ID even after `remove_node()`
has marked it as removed. `remove_node()` updates `nodes.json` only; it does not
append a segment record.

`create_node()` appends an initial empty checkpoint record before it makes the
node visible in `nodes.json`. The initial checkpoint uses term zero, no vote, a
zero-position snapshot with an empty payload, and an empty suffix. Its location
is stored in `checkpoints.json`. This gives every normally created active node a
checkpoint barrier without adding garbage-collection-specific fields to
`nodes.json`.

Each node entry contains a typed `startup` flag and opaque JSON metadata. The
`startup` flag means the node is considered during process startup before any
external control plane has been loaded. The metadata JSON is application-defined.
`sukari` validates it with the `nojson` crate and writes the raw JSON value
without interpreting it.

`nodes.json` contains all node entries in one small file and is updated by
atomic replacement. The update protocol writes `nodes.json.tmp`, syncs it,
renames it over `nodes.json`, and syncs the parent directory. Startup only reads
`nodes.json`; a stale `nodes.json.tmp` is ignored. If `nodes.json` exists but is
malformed or violates the registry schema, opening `StorageEngine` fails with
`InvalidData`.

`remove_node()` persists removal by the same atomic `nodes.json` replacement.
A crash before the rename leaves the previous registry authoritative. A crash
after the rename makes the `removed` flag authoritative. There is no second
segment append step for node removal.

The registry is a storage namespace. Callers can filter nodes by the `startup`
flag when they need startup discovery. It is not the authoritative Raft cluster
membership, group placement, or orchestration state; those belong to the
control plane above `sukari`.

## Snapshot Checkpoints

The implementation provides a snapshot checkpoint operation. A checkpoint is
stronger than saving a snapshot alone: it records a complete recovery point for
one node and declares that earlier records for that node are no longer needed.

The API shape is:

```rust
pub struct SnapshotCheckpoint {
    pub current_term: noraft::Term,
    pub voted_for: Option<noraft::NodeId>,
    pub snapshot: Snapshot,
    pub suffix: LogAppend,
}

pub fn save_snapshot(
    &mut self,
    node_id: noraft::NodeId,
    checkpoint: SnapshotCheckpoint,
) -> io::Result<()>;
```

The checkpoint record contains the current term, voted-for node, snapshot, and
retained log suffix after the snapshot. If a caller still needs log entries
after the snapshot position, it must include them in the checkpoint suffix or
append them again after the checkpoint has been saved. Replay may ignore older
records for the same node once it sees a valid checkpoint. The checkpoint suffix
must start at the snapshot's last included position.

`save_snapshot()` also updates `checkpoints.json` with the segment name and
record offset of the checkpoint record. The checkpoint record is synchronized
before `checkpoints.json` is replaced. This prevents the index from pointing at
a checkpoint record that was never made durable.

`create_node()` uses the same ordering for its initial checkpoint. If a crash
happens after the checkpoint index update but before the node registry update,
the checkpoint index entry is ignored on the next open because the node is not
active.

## Replay

`load()` and `load_all()` discover `append-*.segment` files when they need node
state, replay them in deterministic append segment ID order, and rebuild
per-node state:

- current term
- voted-for node
- log entries
- tagged command payloads
- latest snapshot metadata and payload

The implementation does not require an on-disk random-read log index.
Normal reads are expected to be rare and mostly limited to startup.
`checkpoints.json` is only a checkpoint index: `load(node_id)` can use it as a
hint to skip segments before the latest known checkpoint, and `load_all()` can
use it when every active node has a checkpoint hint. In that case, `load_all()`
starts from the oldest hinted checkpoint segment and still scans forward for
newer checkpoints. If any active node has no checkpoint hint, `load_all()` falls
back to the full replay path because that node may need records before its
first checkpoint. The checkpoint index is not a general random-read index for
log paging. If the index is stale, replay scans from the hinted checkpoint
record forward and can still discover a newer checkpoint.

During `StorageEngine` recovery, the active segment tolerates a trailing partial
record and truncates it. Read-only functions instead ignore a trailing partial
record without changing the segment. Inactive segments do not tolerate trailing
partial records because they must have been completed before a later active
segment became visible. Checksum mismatches are treated as corruption.

## Memory Model

`StorageEngine` does not keep fully replayed node state in memory for normal
writes. Opening the engine may scan segment frames for recovery, but full
`NodeState` construction happens only when loading state. `load()` constructs
the requested node state, while `load_all()` constructs all non-removed node
states.

The design assumes that the loaded snapshot payload and the log entries after
that snapshot are sized for in-memory loading. Payload bytes are stored in a
reference-counted `Bytes` wrapper so cloning loaded command and snapshot
payloads is cheap, but the bytes themselves are still expected to fit in memory.
This keeps the storage API simple and matches the expected Raft usage. Very
large snapshots and large blob payloads are poor fits for Raft and are not
target use cases. Random-read log paging is also not a target use case.
Lagging-node catch-up reads the already loaded log suffix from memory;
synchronous disk reads can block leader replication, while asynchronous paging
complicates the runtime for little gain.

## Observability

`sukari` exposes detailed storage metrics without depending on a metrics
backend or async runtime. The storage crate keeps cheap typed counters and
gauges internally, but it does not expose a generic metric-entry or
backend-specific API.

The typed metrics snapshot covers:

- segment records written and replayed
- bytes written and replayed
- segment rotations
- explicit sync requests and durable segment syncs
- replay truncations and checksum failures
- node creation, node removal, and rejected operations for unknown nodes
- snapshot checkpoints and whole-segment garbage collection

Update paths use typed counters so normal storage operations do not allocate
strings or perform map lookups. `StorageEngine::metrics()` returns a reference to
the current storage counters and gauges. Runtime integration crates can combine
that view with transport metrics, add deployment labels, and convert the result
to Prometheus text or another scrape format.

Metric names and labels belong at the runtime integration boundary. If an
integration exports these metrics, it should use a crate-specific prefix such as
`sukari_` and keep labels low-cardinality. Useful dimensions include operation
kind, record kind, and error kind. Segment IDs, log indexes, request IDs, and
stream IDs are not labels.

## Compaction and Garbage Collection

Garbage collection is the main complexity of shared storage.

Records for many nodes can be mixed in one segment, so a segment can only be
deleted when all records in that segment are obsolete. Snapshot progress for one
node is not sufficient by itself. Removed nodes are identified from `nodes.json`;
the segment stream does not contain node removal records.

The implementation avoids rewrite segments. It does not copy live records into
separate rewrite files. This keeps crash recovery and replay ordering simple and
avoids a data-loss-prone rewrite completion protocol.

Instead, the engine uses whole-segment garbage collection. Whole-segment GC
deletes only inactive append segments that are older than every active node's
checkpoint barrier. It never deletes the active segment or the segment that
contains an active node's latest checkpoint. If any active node has no
checkpoint barrier, GC keeps older segments because that node may still need
records before its first snapshot checkpoint. Normally, `create_node()` gives
each active node an initial checkpoint barrier. Removed nodes do not participate
in the minimum barrier calculation. If no active nodes remain, GC may delete all
inactive append segments.

The implementation stores checkpoint index entries in a separate JSON file,
`checkpoints.json`:

```json
{
  "version": 1,
  "nodes": {
    "1": {
      "checkpoint_segment": "append-9.segment",
      "checkpoint_offset": 1234
    },
    "2": {
      "checkpoint_segment": "append-7.segment",
      "checkpoint_offset": 4
    }
  }
}
```

`checkpoints.json` is authoritative for deletion decisions: GC uses only
barriers recorded in this file. It is also a replay hint. A crash can leave the
index stale, so a recorded checkpoint may be older than the latest checkpoint
record in the segment stream. If `checkpoints.json` is missing, the engine
behaves as if no checkpoint barriers exist. If it exists but is malformed,
violates the schema, names a non-append segment, points at a non-existent
segment, or points at an offset that is not a checkpoint record for the target
node, opening the engine fails with `InvalidData`. Well-formed entries for
removed or unknown nodes are ignored on open, and `remove_node()` removes the
node from the checkpoint index.

`checkpoints.json` is updated by atomic replacement. A checkpoint update appends
and durably syncs the checkpoint record first. Then it writes
`checkpoints.json.tmp`, syncs it, renames it over `checkpoints.json`, and syncs
the parent directory. Node creation follows the same order before updating
`nodes.json`. A stale `checkpoints.json.tmp` is ignored.

Garbage collection is automatic from the library user's perspective. The engine
checks for deletable segments after a successful snapshot checkpoint and after
node removal. Ordinary record appends and node creation are not GC triggers
because they do not make earlier records obsolete. If those operations leave an
older segment inactive, a later snapshot checkpoint or node removal can collect
it when the checkpoint barriers allow deletion. Segment deletion is followed by
a directory sync.

## Crash Recovery

The storage format needs explicit recovery rules for:

- partial record at the end of the active segment
- checksum mismatch
- segment rotation
- initial checkpoint record append during node creation
- snapshot checkpoint record append
- `checkpoints.json` update after checkpoint append
- old segment deletion
- stale `nodes.json.tmp` after registry update
- stale `checkpoints.json.tmp` after checkpoint update
- process crash after fsyncing records before updating metadata files

The current test strategy uses post-crash file-state tests instead of direct
fault injection. Tests construct states that can be left behind by crashes, such
as stale temporary metadata files, lost checkpoint index updates, empty segments
created during rotation, and incomplete whole-segment deletion. This avoids a
filesystem abstraction layer while the storage API and on-disk format remain
compact.
