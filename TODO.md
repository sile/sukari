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
