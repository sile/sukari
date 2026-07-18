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
`sukari` should therefore route records by node ID. Cluster and group metadata
should stay in the control plane above the storage layer.

## Initial Baseline

The initial codebase defines the shared state model and crate boundary only.
It does not yet implement segment files, manifests, compaction, or recovery.

The first implementation target is a conservative append-only design:

```text
storage/
  manifest
  append-000001.segment
  append-000002.segment
  rewrite-000010.segment
```

Each record should carry enough metadata to replay state without consulting a
random-read index:

- magic
- record length
- checksum
- node ID
- record kind
- log position or range
- payload length
- payload

## Replay

Startup should discover segment files, replay them in deterministic order, and
rebuild per-node state:

- current term
- voted-for node
- log entries
- command payloads
- latest snapshot metadata and data or reference
- node removal tombstones

The first version should not require an on-disk random-read index. Normal reads
are expected to be rare and mostly limited to startup. If replay becomes too
slow, a manifest or hint file can be added later as an accelerator.

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
- process crash after fsyncing records before updating manifests

The manifest should accelerate discovery, not be the only source of truth,
unless its update protocol is very carefully specified.
