# loonfs-grep

`loonfs-grep` implements LoonFS's optional gram index. Each namespace stores
its index under `namespaces/{namespace_id}/extensions/grep/`. A server or
maintenance process registers the grep job with a writer. Grep never scans
the store to discover namespaces.

A server can run the job with its other maintenance work. A separate process
can maintain namespaces named on the command line:

```console
loonfs maintenance run --namespaces docs --namespaces source --job grep-index
loonfs maintenance run --namespaces docs --job grep-index --drain
loonfs maintenance run --namespaces docs --job grep-gc --drain
loonfs maintenance index gc --namespace docs
```

Without `--drain`, the command runs until it receives a stop signal and
periodically refreshes its assignments. With `--drain`, it brings each
assigned namespace up to date and exits. `--max-steps` and `--deadline-ms`
limit that work. Omitting `--job` also runs metadata and core garbage
collection.

The `grep-gc` job resumes bounded collection passes where the previous pass
stopped. `loonfs maintenance index gc` runs those passes directly for one namespace,
including an absent or deleted namespace whose old index data remains.

`GrepWorkerConfig` controls how much work one step may perform. A server reads
these values from its `[grep]` table:

```toml
[grep]
mode = "serve_and_maintain"
max_files_per_step = 256
max_content_bytes_per_step = 67108864
max_rows_per_segment = 65536
max_delta_runs = 8
max_mid_runs = 8
max_decoded_input_rows_per_step = 131072
```

Every step budget must be greater than zero. These values do not control
concurrency. The runtime's shared `max_concurrent_maintenance` limit applies
across all maintenance jobs, including grep.
