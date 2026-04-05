# Proposal: spec-lock governance for the rewrite branch

## Type

clarifying

## Affected specs

- `docs/specs/*`

## Problem

The rewrite needs a stable source of truth while the implementation is being rebuilt from scratch.
Editing the active spec set in parallel with the implementation would make review and recovery
harder.

## Proposed change

Treat `docs/specs/*` as immutable on the rewrite branch.

All suggested changes, reinterpretations, and intentional divergences must be documented under
`proposals/*` instead of being applied directly to the locked spec files.

## Rewrite decision

The rewrite follows this proposal now.

## Consequences

- implementation work must cite proposals instead of silently changing spec meaning
- review can compare code against a fixed spec set
- future spec refresh can be done deliberately from the proposal backlog
