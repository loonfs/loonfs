# ADR 0025: authoritative recursive put stays atomic and create-only

Status: accepted

`loon file put --recursive <local-dir> <namespace:/absolute/path>` adds the first authoritative
directory upload path.

Rules:

- recursive put is directory-only and create-only
- `--recursive` and `--replace` are mutually exclusive
- the remote selector is the exact absent destination root, not a container directory
- recursive put never routes through observe/import/sync or the shared single-op client mutation
  protocol
- the server helper:
  - walks the local source tree deterministically
  - rejects unsupported local descendants before metadata commit
  - uploads all file content objects first
  - acquires or renews the namespace lease
  - loads one verified basis
  - builds one ordered multi-op `CommitRequest`
  - validates, writes WAL, applies metadata, and publishes head once

Commit ordering:

- root `CreateDir`
- descendant `CreateDir` ops in lexicographic relative-path order
- descendant `CreateFile` ops in lexicographic relative-path order

Consequences:

- recursive upload preserves the simpler product rule: exact-root, fail-closed destination
- subtree visibility is atomic at the metadata layer
- durable content still lands before metadata publish
- the shared client mutation request/response contract stays narrow and single-op
