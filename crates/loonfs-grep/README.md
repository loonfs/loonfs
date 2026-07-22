# loonfs-grep

`loonfs-grep` owns LoonFS's optional, namespace-scoped gram index under
`namespaces/{namespace_id}/extensions/grep/`. Maintenance is assigned explicitly; the binary never
enumerates the store.

Name each namespace with a repeatable `--namespace` flag:

```console
loonfs-grep --config loonfs-grep.toml --namespace docs --namespace source --once
loonfs-grep --config loonfs-grep.toml --namespace docs
loonfs-grep --config loonfs-grep.toml --namespace docs --once --gc
```

`--once` drives each named namespace to a caught-up steady root and exits, making it suitable for
cron. Without `--once`, each assigned namespace polls only its own durable head at the configured
`poll_interval_ms`; a head advance nudges that namespace's parked driver. This is the grep analog
of SlateDB's detached manifest poll: one small read per assigned namespace per interval, never a
namespace scan. `--gc` explicitly collects each named namespace's grep-owned keyspace once before
maintenance starts, including reaping aged extension state for an absent or tombstoned namespace.

The strict config keeps the provider under `[store]`, puts the one detached-deployment timer at the
top level, and uses `[grep]` only for bounded step budgets:

```toml
poll_interval_ms = 1000

[store]
kind = "local-fs"
root = "./.loonfs-store"

[grep]
max_files_per_step = 256
max_content_bytes_per_step = 67108864
max_rows_per_segment = 65536
max_l0_runs = 8
max_mid_runs = 8
max_fold_rows_per_step = 131072
```

`poll_interval_ms` defaults to 1000 and must be greater than zero. Every step budget must also be
greater than zero.
