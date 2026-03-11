# ADR 0009: metadata replay uses a verified checkpoint plus WAL tail

Status: accepted

We will make durable namespace replay depend on the latest verified checkpoint plus later immutable WAL entries.
We will not make correctness depend on a rewritten compacted-WAL format in v1.

Consequences:
- checkpoints become the unit of retention advancement, so `retention_floor_seq` moves only after a newer checkpoint is built, verified, and published
- raw WAL entries remain immutable until policy allows them to age out, so WAL history stays append-only and is dropped by retention rather than rewritten
- archive bundles, if added later, are cold-history optimization only, so any later archival packing is not part of the correctness or normal read path
