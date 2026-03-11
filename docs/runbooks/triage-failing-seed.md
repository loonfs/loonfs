# Runbook: triage a failing randomized seed

## Inputs you need

- scenario name
- seed
- commit hash
- rendered trace or snapshot if available

## Steps

1. Replay the failing case with the same seed.
2. Confirm the failure is deterministic.
3. Reduce the fixture if the failure came from a large generated case.
4. Classify the failure:
   - spec bug
   - model bug
   - implementation bug
   - simulator bug
5. Add the minimized fixture to `tests/scenarios/` before fixing the code.
6. Land the fix together with the regression case.

## What to avoid

- Do not “fix” the test by adding sleeps.
- Do not add retries to hide flakes.
- Do not merge a concurrency fix without a deterministic reproduction path.
