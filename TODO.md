# TODO

## Repository Baseline

- Confirm the final crate name before publishing.
- Decide whether the initial `noraft` dependency should stay on git `main` or
  move to the crates.io release once the required API is available there.

## Storage API

- Decide whether loaded command and snapshot payloads should stay as `Vec<u8>`
  wrappers or move to a shared representation.

## Segment Format

- Document the `SKR1` format once the first release boundary is clear.
- Define the snapshot checkpoint record encoding.

## Replay

- Implement replay semantics for snapshot checkpoint records.
- Decide whether inactive segments should ever tolerate trailing partial records.

## Compaction And Garbage Collection

- Define the `gc.json` schema and atomic update protocol.
- Implement automatic whole-segment garbage collection.
- Define `GcPolicy` thresholds for opportunistic deletion.
- Define old segment deletion and directory sync ordering.

## Testing

- Expand focused unit tests for explicit error paths.
- Add property-based tests for record encoding and replay round trips.
- Add fuzzing for arbitrary segment input.
- Add crash-recovery tests for segment rotation, checkpoint persistence,
  `gc.json` updates, and segment deletion ordering.
- Decide whether manifest, registry, and `gc.json` sync ordering need
  fault-injection tests.
