# loonfs-grep

`loonfs-grep` contains LoonFS's optional full-text grep subsystem. It owns the gram codec, the
namespace-scoped `extensions/grep/` pointer, manifests, and segments, plus the explicitly driven
`GrepWorker`; a standalone process and server-embedded mode share one fair worker loop.

Run the standalone worker with a strict TOML config containing `[store]` (the same provider shape
as `loonfs-server`) and an optional `[grep]` pacing/budget table:

```console
loonfs-grep --config loonfs-grep.toml
loonfs-grep --config loonfs-grep.toml --once
```

```toml
[grep]
step_interval_ms = 1000
gc_interval_ms = 60000
rescan_interval_ms = 300000
```

`--once` performs startup rediscovery, one complete build/fold sweep, and grep garbage collection,
then exits. Rediscovery lists the entire `namespaces/` prefix and is proportional to the store's
total key count. It is required for standalone and serve-only deployments until LoonFS has a
namespace catalog; the five-minute default keeps periodic scans rare.
