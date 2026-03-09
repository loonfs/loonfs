# SKILLS.md

This file tracks repeatable engineering workflows we want the team to use consistently.

## Current skills to standardize

### 1. Add a readable scenario fixture
- write a YAML file in `tests/scenarios/`
- render it with `xtask render-case`
- review the trace as a product artifact, not only as a test input

### 2. Add a namespace mutation
- spec first
- model second
- implementation third
- simulator/native tests later if needed

### 3. Add (object storage) provider support or a new provider assumption
- update provider profile
- add a conformance test
- document the reason in the object-store spec

### 4. Investigate a failing randomized seed
- capture seed and commit hash
- replay locally
- minimize if possible
- land the minimized repro with the fix

## How this file should evolve

If we discover a workflow that repeats three or more times, add it here.
The goal is to make good engineering habits easy to follow and easy to review.
