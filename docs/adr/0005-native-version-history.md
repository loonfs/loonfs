# ADR 0005: version history is native

Status: accepted

Every inode may have immutable revisions. Restoring an old revision creates a new current revision.

Consequences:
- history is monotonic
- revision listing is a first-class API concern
- restore never rewinds the head
