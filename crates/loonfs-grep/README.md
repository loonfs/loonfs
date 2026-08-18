# loonfs-grep

`loonfs-grep` owns LoonFS's optional, namespace-scoped gram index under
`namespaces/{namespace_id}/extensions/grep/`. It is an extension, not a service: the crate
implements `loonfs`'s public `MaintenanceJob` trait and a host registers that job on a writer's
maintenance runner. Maintenance is assigned explicitly and grep never enumerates the store.

Two hosts run it. A server whose `[grep]` mode maintains registers the job on its own writer, and
the runner schedules it beside metadata upkeep and collection. A detached host names its
namespaces on the command line:

```console
loonfs admin maintenance run --namespaces docs --namespaces source --job grep-index
loonfs admin maintenance run --namespaces docs --job grep-index --drain
loonfs admin maintenance run --namespaces docs --job grep-gc --drain
loonfs admin index gc --namespace docs
```

Without `--drain` the command hosts the runner until a signal, asserting its assignment on an
interval so a quiet namespace stays covered. With `--drain` it catches every assigned namespace up
and exits, which suits cron; `--max-steps` and `--deadline-ms` bound it and it reports where each
namespace stopped. Omitting `--job` hosts metadata and collection too, so one process can own
everything a namespace nobody is writing to needs. Collection is its own job, `grep-gc`: it shares
the runner's admission and permits and resumes where the last pass stopped, but nothing schedules
it on grep's behalf. `loonfs admin index gc` runs the same passes directly against one namespace's
grep-owned keyspace, including reaping aged extension state for an absent or tombstoned namespace.

Bounded-step budgets are `GrepWorkerConfig`, which a server reads from its `[grep]` table:

```toml
[grep]
mode = "serve_and_maintain"
max_files_per_step = 256
max_content_bytes_per_step = 67108864
max_rows_per_segment = 65536
max_l0_runs = 8
max_mid_runs = 8
max_decoded_input_rows_per_step = 131072
```

Every step budget must be greater than zero. How many steps may run at once is not configured
here: every host schedules grep through the runtime's maintenance runner, whose one permit pool
`max_concurrent_maintenance` bounds every maintenance family together.
