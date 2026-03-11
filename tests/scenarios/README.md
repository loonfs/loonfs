# Scenario fixtures

Scenario fixtures are meant to be readable by developers and product reviewers.

## Fixture shape

- `name`: stable case name
- `seed`: optional deterministic seed
- `initial`: starting namespace/client/server state
- `actions`: ordered user or server events
- `faults`: injected faults or reorderings
- `expect`: invariants and final-state expectations

## Naming guidance

Prefer names that describe the race or rule being tested.

Good examples:
- `delete_then_stale_local_edit`
- `queue_claim_timeout_then_steal`
- `mount_subtree_resolution`
