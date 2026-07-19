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

## Testing

- Expand focused unit tests for explicit error paths.
- Add property-based tests for record encoding and replay round trips.
- Add fuzzing for arbitrary segment input.
- Add crash-recovery tests for segment rotation, checkpoint persistence,
  `checkpoints.json` updates, and segment deletion ordering.
- Decide whether manifest, registry, and `checkpoints.json` sync ordering need
  fault-injection tests.
