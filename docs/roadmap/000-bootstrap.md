# Roadmap 000: bootstrap sequence

## Goal

Turn the current scaffold into a buildable, test-first development base.

## Phase 1: provider contract

- implement the local filesystem object-store adapter
- build the conformance harness
- make S3 pass
- make R2 pass

Exit criteria:
- `create_if_absent` works
- CAS update on small control objects works
- list-after-write and list-after-delete behavior are verified

## Phase 2: canonical namespace rules

- namespace head object
- lease / fencing token record
- commit request validation
- immutable WAL object + head publish

Exit criteria:
- one namespace can accept deterministic metadata commits
- stale writers cannot publish

## Phase 3: testing foundation

- scenario loader
- reference model
- deterministic simulator shell
- seed replay workflow

Exit criteria:
- at least ten readable fixtures exist
- at least one model-vs-core differential test exists

## Phase 4: background work

- shard queue state
- broker lease protocol
- `BuildSnapshot` work class

Exit criteria:
- derived work is idempotent and recoverable after queue loss


## Next reading

Once bootstrap is understood, continue with `docs/roadmap/010-foundation-workstreams.md`.
