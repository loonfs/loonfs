# CONTRIBUTING.md

## Branch and PR guidance

- Keep branches short-lived.
- Prefer one feature or one decision per PR.
- Link the relevant spec and ADR in the PR description.
- Include the scenario fixture name in the PR description when behavior changes.

## Minimum PR contents

- code
- tests
- docs

At least one of the following should exist for every non-trivial change:

- scenario fixture
- reference-model test
- deterministic simulator seed

## Naming

Use the `loon-` crate prefix consistently.

## Formatting and lints

The intended baseline is:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Documentation style

When you introduce a new term:

1. define it plainly
2. explain why it exists
3. give a concrete example
4. mention what failure mode it prevents
