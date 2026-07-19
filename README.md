# sukari

`sukari` is an experimental shared segmented Raft log storage engine for
multi-Raft workloads.

The crate is intended to provide a storage backend that can batch durable writes
from many local Raft nodes into shared append-only segment files. Higher-level
runtime and control-plane crates can use it without taking ownership of shared
storage format, replay, compaction, and recovery details.

This crate is currently under initial development.
The public API and on-disk format are not ready for use yet.

## Documents

- [Architecture overview](docs/architecture-overview.md)
- [Segment format](docs/segment-format.md)
