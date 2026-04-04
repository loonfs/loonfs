# xtask

Repository automation entrypoints for the spec-locked rewrite.

`xtask` is intentionally small in this branch. It owns smoke acceptance only.

## Commands

- `smoke --client-config <path> --namespace <id>`
  - run the full v0 acceptance path against an already running `loond`
- `smoke --server-config <path> --client-config <path> --namespace <id>`
  - start `loond`, wait for `GET /healthz`, run the smoke path, then stop the child process

## Smoke Sequence

The smoke command proves the current product surface end to end:

- create namespace
- list namespaces
- put one local file
- ls the parent path
- stat the uploaded file
- get the file and verify bytes
- move the file
- rm the moved file
- verify removal with final `ls` and `stat`

The command prints a compact success line with the mode, namespace, and completed steps.
