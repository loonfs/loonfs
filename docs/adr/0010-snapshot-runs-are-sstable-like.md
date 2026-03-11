# ADR 0010: checkpoint data is stored as immutable SSTable-like runs

Status: accepted

Each checkpoint will write sorted immutable runs for inode, direntry, revision, and tombstone record families.

Consequences:
- checkpoint files are range-readable and cacheable
- object counts stay bounded without giant monolithic snapshot blobs
- snapshot building remains append-only and deterministic
