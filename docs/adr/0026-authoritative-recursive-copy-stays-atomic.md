# ADR 0026: authoritative recursive copy stays atomic and exact-root

Status: accepted

`loon file cp --recursive <from-namespace:/absolute/path> <to-namespace:/absolute/path>` adds the
first authoritative directory-to-directory copy path.

Rules:

- recursive copy is directory-only, same-namespace only, and create-only
- `--recursive` and `--replace` are mutually exclusive
- the destination selector is the exact absent destination root, not a container directory
- recursive copy never routes through recursive get plus recursive put, observe/import/sync, or
  the shared single-op client mutation protocol
- the server helper:
  - acquires or renews the namespace lease
  - loads one verified basis
  - resolves the source subtree and destination create target under that same basis
  - reuses source file manifest digests without re-uploading content objects
  - validates durable content before metadata publish
  - builds one ordered multi-op `CommitRequest`
  - validates, writes WAL, applies metadata, and publishes head once

Commit ordering:

- root `CreateDir`
- descendant `CreateDir` ops in lexicographic relative-path order
- descendant `CreateFile` ops in lexicographic relative-path order

Consequences:

- recursive copy preserves the simpler product rule: exact-root, fail-closed destination
- subtree visibility is atomic at the metadata layer
- copied entries get new inode identities while the source subtree remains unchanged
- the shared client mutation request/response contract stays narrow and single-op
