# TODO

## Repository Baseline

- Confirm the final crate name before publishing.
- Decide whether the initial `noraft` dependency should stay on git `main` or
  move to the crates.io release once the required API is available there.

## Storage API

- Decide whether loaded command and snapshot payloads should stay as `Vec<u8>`
  wrappers or move to a shared representation.
- Revisit node tombstone behavior after the first runtime integration.
- Decide whether `load_all()` should expose removed node IDs to callers.

## Segment Format

- Document the `SKR1` format once the first release boundary is clear.
- Add segment rotation and active segment selection.
- Define manifest format and whether it is authoritative or advisory.
- Define rewrite segment ordering and completion records.

## Replay

- Validate replay behavior when both append and rewrite segments exist.
- Add manifest-assisted replay once the manifest format exists.
- Decide whether inactive segments should ever tolerate trailing partial records.

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
