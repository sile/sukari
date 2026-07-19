# Architecture Overview

This document records the current design direction of `sukari`.
Update it as the implementation changes.

## Scope

`sukari` is a shared segmented storage engine for Raft state.

The crate should own:

- shared segment file format
- advisory manifest format
- garbage-collection metadata format
- deterministic replay
- per-node replay state
- snapshot checkpoint semantics
- automatic whole-segment garbage collection
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
- save snapshot checkpoints that supersede earlier node records
- replay a node's persistent state at startup
- mark a node removed and reserve its node ID

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
  nodes.json
  manifest
  append-000001.segment
  append-000002.segment
```

Segment file names are parsed into internal segment names with a segment kind
and non-zero numeric segment ID. Startup reads an advisory manifest when it is
available, validates the hinted active append segment against existing segment
files, and follows any subsequent contiguous append segment files before opening
the writer. Startup falls back to a file-name scan when the manifest is missing,
malformed, unsupported, names a non-append segment, or points at a non-existent
segment. New writes rotate to the next append segment when the configured
segment length would be exceeded. A single record that exceeds the limit is
written to an empty segment by itself.

The public API exposes node ID based storage operations on a single engine
writer:

- create a node with startup metadata
- inspect active node metadata
- inspect startup nodes
- load a node state
- save current term
- save voted-for node
- append log entries and command payloads
- save snapshot state
- flush pending writes
- load all non-removed node states
- remove a node and reserve its node ID
- remove all storage data

Storage operations for node state fail unless the target node ID has been
created with `create_node()`. Removed node IDs are permanently reserved and
cannot be created again.

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
- snapshot checkpoint

`SKR1` is unstable while the crate is unreleased. Incompatible storage changes
can still move to a new magic value if keeping experimental data is not useful.

## Future Layout

The intended full design is a shared segmented append-only design:

```text
storage/
  nodes.json
  manifest
  checkpoints.json
  append-000001.segment
  append-000002.segment
```

## Node Registry

The current implementation stores a small JSON node registry file:

```text
storage/
  nodes.json
  append-000001.segment
  append-000002.segment
```

The registry records which `noraft::NodeId` values are valid for this storage
instance. Nodes are added with `create_node()` and removed with `remove_node()`.
Storage operations such as `load()`, `save_current_term()`, `save_voted_for()`,
`append_entries()`, and `save_snapshot()` fail for node IDs that have not been
created.

`nodes.json` currently has this schema:

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
checkpoint barrier without adding GC-specific fields to `nodes.json`.

Each node entry should contain a typed `startup` flag and opaque JSON metadata.
The `startup` flag means the node should be considered during process startup
before any external control plane has been loaded. The metadata JSON is
application-defined. `sukari` validates it with the `nojson` crate and writes the
raw JSON value without interpreting it.

`nodes.json` contains all node entries in one small file and is updated by
atomic replacement. The update protocol writes `nodes.json.tmp`, syncs it,
renames it over `nodes.json`, and syncs the parent directory when the sync
policy requires durable metadata. Startup only reads `nodes.json`; a stale
`nodes.json.tmp` is ignored. If `nodes.json` exists but is malformed or violates
the registry schema, opening `StorageEngine` fails with `InvalidData`.

`remove_node()` persists removal by the same atomic `nodes.json` replacement.
A crash before the rename leaves the previous registry authoritative. A crash
after the rename makes the `removed` flag authoritative. There is no second
segment append step for node removal.

The registry is a storage namespace and startup-discovery mechanism. It is not
the authoritative Raft cluster membership, group placement, or orchestration
state; those belong to the control plane above `sukari`.

## Manifest

The current implementation stores a small advisory JSON manifest file:

```json
{
  "version": 1,
  "active_append_segment": "append-000002.segment"
}
```

The manifest accelerates active append segment selection, but it is not
authoritative. Startup ignores `manifest.tmp`. If `manifest` is missing,
malformed, has an unsupported version, names a non-append segment, or points at a
non-existent segment, startup falls back to scanning segment file names. If the
hint names an older existing append segment, startup advances through subsequent
contiguous append segment file names instead of trusting the old hint as-is.

Segment rotation creates the next append segment first, syncs the segment file
metadata when required by the sync policy, and then atomically replaces
`manifest` through `manifest.tmp`. A crash before the rename leaves the old
manifest in place. A crash after the rename records the new hint. Recovery must
still discover segment files and must not depend only on the manifest.

## Snapshot Checkpoints

The current implementation provides a snapshot checkpoint operation. A
checkpoint is stronger than saving a snapshot alone: it records a complete
recovery point for one node and declares that earlier records for that node are
no longer needed.

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

The checkpoint record should contain the current term, voted-for node,
snapshot, and retained log suffix after the snapshot. If a caller still needs
log entries after the snapshot position, it must include them in the checkpoint
suffix or append them again after the checkpoint has been saved. Replay may
ignore older records for the same node once it sees a valid checkpoint. The
checkpoint suffix should start at the snapshot's last included position.

`save_snapshot()` also updates `checkpoints.json` with the segment name and
record offset of the checkpoint record. When the sync policy requires durable
metadata, the checkpoint record is flushed before `checkpoints.json` is
replaced. This ordering can leave the index stale after a crash, but it should
not point at a checkpoint record that was never made durable.

`create_node()` uses the same ordering for its initial checkpoint. If a crash
happens after the checkpoint index update but before the node registry update,
the checkpoint index entry is ignored on the next open because the node is not
active.

## Replay

Startup discovers `append-*.segment` files, replays them in deterministic
file-name order, and rebuilds per-node state:

- current term
- voted-for node
- log entries
- command payloads
- latest snapshot metadata and data or reference

The first version should not require an on-disk random-read log index. Normal
reads are expected to be rare and mostly limited to startup. `checkpoints.json`
is only a checkpoint index: `load(node_id)` can use it as a hint to skip
segments before the latest known checkpoint, and `load_all()` can use it when
every active node has a checkpoint hint. In that case, `load_all()` starts from
the oldest hinted checkpoint segment and still scans forward for newer
checkpoints. If any active node has no checkpoint hint, `load_all()` falls back
to the full replay path because that node may need records before its first
checkpoint. The checkpoint index is not a general random-read index for log
paging. If the index is stale, replay scans from the hinted checkpoint record
forward and can still discover a newer checkpoint.

The active segment tolerates a trailing partial record and truncates it during
replay. Inactive segments do not tolerate trailing partial records because they
should have been completed before a later active segment became visible.
Checksum mismatches are treated as corruption.

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
node is not sufficient by itself. Removed nodes are identified from `nodes.json`;
the segment stream does not contain node removal records.

The initial design should avoid rewrite segments. It should not copy live
records into separate rewrite files. This keeps crash recovery and replay
ordering simple and avoids a data-loss-prone rewrite completion protocol.

Instead, the engine uses whole-segment garbage collection. Whole-segment GC
deletes only inactive append segments that are older than every active node's
checkpoint barrier. It never deletes the active segment or the segment that
contains an active node's latest checkpoint. If any active node has no
checkpoint barrier, GC keeps older segments because that node may still need
records before its first snapshot checkpoint. Normally, `create_node()` gives
each active node an initial checkpoint barrier. Removed nodes do not participate
in the minimum barrier calculation. If no active nodes remain, GC may delete all
inactive append segments.

The current implementation stores latest checkpoint locations in a separate
authoritative JSON file, `checkpoints.json`:

```json
{
  "version": 1,
  "nodes": {
    "1": {
      "checkpoint_segment": "append-000010.segment",
      "checkpoint_offset": 1234
    },
    "2": {
      "checkpoint_segment": "append-000008.segment",
      "checkpoint_offset": 0
    }
  }
}
```

This file should be separate from `manifest`. The manifest is advisory and can
be ignored when it is stale or malformed. `checkpoints.json` is authoritative
for future deletion and useful as a replay hint. If `checkpoints.json` is
missing, the engine behaves as if no checkpoint barriers exist. If it exists but
is malformed, violates the schema, names a non-append segment, points at a
non-existent segment, or points at an offset that is not a checkpoint record for
the target node, opening the engine fails with `InvalidData`. Well-formed
entries for removed or unknown nodes are ignored on open, and `remove_node()`
removes the node from the checkpoint index.

`checkpoints.json` is updated by atomic replacement. A checkpoint update appends
and durably syncs the checkpoint record first when the sync policy requires it.
Then it writes `checkpoints.json.tmp`, syncs it when durable metadata is
required, renames it over `checkpoints.json`, and syncs the parent directory
when durable metadata is required. Node creation follows the same order before
updating `nodes.json`. A stale `checkpoints.json.tmp` is ignored.

Garbage collection is automatic from the library user's perspective. The engine
checks for deletable segments after a successful snapshot checkpoint and after
node removal. Ordinary record appends and node creation are not GC triggers
because they do not make earlier records obsolete. If those operations leave an
older segment inactive, a later snapshot checkpoint or node removal can collect
it when the checkpoint barriers allow deletion. When durable metadata is
required, segment deletion is followed by a directory sync.

## Crash Recovery

The storage format needs explicit recovery rules for:

- partial record at the end of the active segment
- checksum mismatch
- stale `manifest` or `manifest.tmp` after segment creation
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
filesystem abstraction layer while the storage API and on-disk format are still
small and experimental.
