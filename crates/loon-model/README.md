# loon-model

Pure reference model for namespace and queue semantics.

This crate serves as the specification-in-code for LoonDB's protocol behavior. It defines the
expected outcomes for every mutation, precondition check, conflict scenario, and queue operation —
without performing any I/O or side effects. The reference model is the authority that production
code is tested against.

This crate must remain side-effect free.

`#![forbid(unsafe_code)]`

## Key types

- `ModelNamespace` — reference namespace state (head, fence token, inode cursor, metadata)
- `ModelWalCommit` — expected WAL commit structure
- `ModelCheckpoint` — expected checkpoint structure and table families
- `ModelMetadataMutation` — the 6 mutation types (CreateDir, CreateFile, ReplaceFile, Rename,
  RestoreRevision, DeleteSubtree)
- `ModelError` — exhaustive error variants covering every protocol rejection
- `ModelQueueShard`, `ModelQueueJob` — reference queue behavior
