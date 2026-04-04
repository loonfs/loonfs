# xtask

Repository automation entrypoints for the spec-locked rewrite.

`xtask` is intentionally small in this branch. It owns smoke acceptance only.

## Commands

- `smoke local --server-config <path> --namespace <id>`
  - create a temp local `loon` profile, run `local up`, exercise the CLI surface, then run `local down`
- `smoke remote --server-url <url> [--auth-token <token>] --namespace <id>`
  - create a temp remote `loon` profile and run the full smoke path against an already running `loond`

## Smoke Sequence

The smoke command proves the current product surface end to end:

- create namespace
- list namespaces
- put one local file
- ls the parent path
- stat the uploaded file
- cat the uploaded file and verify raw bytes
- get the file and verify bytes
- cp the uploaded file and verify a distinct inode
- move the copied file
- rm the moved file
- verify removal with final `ls` and `stat` while the original source remains

The command prints a compact success line with the mode, namespace, and completed steps.
