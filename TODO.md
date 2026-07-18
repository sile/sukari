# TODO

## Repository Baseline

- Confirm the final crate name before publishing.
- Decide whether the initial `noraft` dependency should stay on git `main` or
  move to the crates.io release once the required API is available there.

## Storage API

- Define the public storage engine API around per-node operations compatible
  with `noraft`-based runtimes.
- Decide whether loaded command and snapshot payloads should stay as `Vec<u8>`
  wrappers or move to a shared representation.
- Define node removal semantics and tombstone visibility during replay.

## Segment Format

- Choose the first segment magic value.
- Define record headers, record kinds, and checksum coverage.
- Add bounded binary encoding and decoding.
- Reject unknown magic values, unknown record kinds, oversized records, and
  malformed payloads clearly.

## Replay

- Implement deterministic segment discovery.
- Rebuild per-node hard state, log entries, command payload maps, and snapshot
  metadata from append and rewrite segments.
- Truncate trailing partial records in the active segment.
- Treat checksum mismatch as corruption.

## Compaction And Garbage Collection

- Track segment-level liveness.
- Define rewrite segment creation and completion.
- Define old segment deletion rules.
- Bound temporary space amplification during long-lived mixed segments.

## Testing

- Expand focused unit tests for explicit error paths.
- Add property-based tests for record encoding and replay round trips.
- Add fuzzing for arbitrary segment input.
- Add crash-recovery tests for manifest, segment rotation, rewrite completion,
  and deletion ordering.
