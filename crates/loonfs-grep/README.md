# loonfs-grep

`loonfs-grep` contains LoonFS's optional full-text grep subsystem. It owns the gram codec, the
independent durable root and keyspace, and the explicitly driven `GrepWorker`; a standalone worker
process arrives in a later change.
