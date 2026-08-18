# loonfs-grep

`loonfs-grep` implements LoonFS's optional gram index. Each namespace stores
its index under `namespaces/{namespace_id}/extensions/grep/`. A server or
maintenance process registers the grep job with a writer. Grep never scans
the store to discover namespaces.

A server can run the job with its other maintenance work. A separate process
can maintain namespaces named on the command line:

```console
loonfs admin maintenance run --namespaces docs --namespaces source --job grep-index
loonfs admin maintenance run --namespaces docs --job grep-index --drain
loonfs admin maintenance run --namespaces docs --job grep-gc --drain
loonfs admin index gc --namespace docs
```

Without `--drain`, the command runs until it receives a stop signal and
periodically refreshes its assignments. With `--drain`, it brings each
assigned namespace up to date and exits. `--max-steps` and `--deadline-ms`
limit that work. Omitting `--job` also runs metadata and core garbage
collection.

The `grep-gc` job resumes bounded collection passes where the previous pass
stopped. `loonfs admin index gc` runs those passes directly for one namespace,
including an absent or deleted namespace whose old index data remains.

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
