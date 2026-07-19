# Segment Format

This document specifies the current `sukari` segment record format.

`SKR1` is unstable while the crate is unreleased. Incompatible storage changes
can still move to a new magic value if keeping experimental data is not useful.

## Frame Layout

Segment files contain a sequence of `SKR1` frames. They have no file-level
header. All integer fields are little-endian.

Each frame has this layout:

```text
4 bytes  magic: "SKR1"
4 bytes  body length as u32
4 bytes  CRC-32C checksum of the body as u32
N bytes  body
```

Readers treat checksum mismatches, unknown tags, unsupported formats,
over-limit lengths, and trailing garbage in inactive segments as corruption. A
trailing partial frame is tolerated only in the active append segment and is
truncated during recovery or replay.

## Record Body

The frame body begins with the target Raft node ID and a record tag:

```text
8 bytes  node_id as u64
1 byte   record tag
...      record payload
```

Current record tags are:

```text
0  current term
1  voted-for node
2  log append
3  snapshot checkpoint
```

## Current Term

The current term payload is:

```text
8 bytes  term as u64
```

## Voted-For Node

The voted-for payload uses the common optional node ID encoding:

```text
1 byte   0 for none, 1 for some
8 bytes  node_id as u64, only when the tag is 1
```

## Log Append

A log append payload is:

```text
LogEntries
8 bytes       command payload count as u64
repeated:
  8 bytes     command log index as u64
  Bytes       command payload
```

`LogEntries` is encoded as:

```text
LogPosition   previous log position
8 bytes       entry count as u64
repeated:
  LogEntry
```

`LogEntry` variants are:

```text
0  term entry: 8-byte term payload
1  cluster config entry: ClusterConfig payload
2  command entry: no inline payload
```

Command bytes are stored in the append command map keyed by log index rather
than inside the `LogEntry::Command` item. The storage layer validates that every
command entry has exactly one matching command payload and that no payload is
provided for a non-command entry.

## Snapshot Checkpoint

A snapshot checkpoint payload is:

```text
8 bytes       current term as u64
OptionalNode  voted-for node, using the same optional node ID encoding
Snapshot
LogAppend     retained suffix after the snapshot
```

`Snapshot` is encoded as:

```text
LogPosition    last included position
ClusterConfig  cluster configuration at that position
Bytes          snapshot payload
```

## Common Types

`LogPosition` is encoded as a term followed by a log index:

```text
8 bytes  term as u64
8 bytes  index as u64
```

`ClusterConfig` is encoded as three node ID sets in this order:

```text
NodeIdSet  voters
NodeIdSet  new_voters
NodeIdSet  non_voters
```

Each node ID set is encoded in ascending node ID order:

```text
8 bytes  node count as u64
repeated:
  8 bytes  node_id as u64
```

`Bytes` is encoded as:

```text
8 bytes  byte length as u64
N bytes  byte payload
```

## Limits

The current implementation rejects frame bodies larger than 1 GiB. It also
rejects decoded byte slices larger than 1 GiB and decoded set or entry counts
larger than 1,000,000. These limits are parser safety bounds for malformed
storage files, not recommended snapshot or append-entry size limits.

Operational payload size limits belong to higher layers that know the workload.
At this layer, large records primarily matter because frame bodies are encoded
and decoded in memory.
