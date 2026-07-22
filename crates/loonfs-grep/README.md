# loonfs-grep

`loonfs-grep` contains LoonFS's optional full-text grep subsystem. It owns the gram codec, the
independent durable root and keyspace, and the explicitly driven `GrepWorker`; a standalone worker
process and server-embedded mode share one fair all-namespaces `GrepWorkerLoop`.

Run the standalone worker with a strict TOML config containing `[store]` (the same provider shape
as `loonfs-server`) and an optional `[grep]` pacing/budget table:

```console
loonfs-grep --config loonfs-grep.toml
loonfs-grep --config loonfs-grep.toml --once
```

`--once` performs one complete build/fold sweep plus grep garbage collection and exits.
