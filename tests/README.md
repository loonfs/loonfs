# Tests

This repository treats tests as a product surface.

## Test layers

- `scenarios/`: readable fixtures
- `conformance/`: storage-provider contract tests
- `snapshots/`: rendered outputs for fixture review

Snapshot outputs are organized by command family, for example:

- `snapshots/render-case/...`
- `snapshots/replay-seed/...`
- `snapshots/minimize-case/...`

## How to add a new scenario

1. Copy an existing YAML fixture.
2. Give it a stable name.
3. Keep the initial state small.
4. Name the expected invariant explicitly.
5. Add the matching model / implementation test.

## Output philosophy

A good failing test should answer three questions quickly:

- what happened?
- why is it wrong?
- how do I replay it?
