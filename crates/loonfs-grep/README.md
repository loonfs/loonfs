# loonfs-grep

`loonfs-grep` implements LoonFS's optional gram index. Each namespace stores
its index under `namespaces/{namespace_id}/extensions/grep/`. Grep never scans
the store to discover namespaces. Its host drives it through `GrepWorker`'s
typed steps: `build_step`, `reorganize_step`, and
`garbage_collect_namespace`.

Commits do not schedule index work. The reference server's maintenance sweep
visits every namespace on a cadence. Each visit runs build steps while they
publish, at most 16, then one reorganize step once the index is up to date,
and a collection pass also runs grep garbage collection. A second pass every
5 seconds builds the index of each writer session the server holds whose
last published seq moved since that pass last indexed it. A query stays
correct while the index is behind: it scans the files committed after the
index.

A separate process can maintain namespaces named on the command line:

```console
loonfs maintenance loop --namespaces docs,source --jobs grep-index,grep-gc
loonfs maintenance loop --namespaces docs --jobs grep-index --drain
loonfs maintenance loop --namespaces docs --jobs grep-gc --drain
loonfs maintenance grep-gc --namespace docs
```

Without `--drain`, the command runs until it receives a stop signal and
periodically refreshes its assignments. With `--drain`, it brings each
assigned namespace up to date and exits. `--max-steps` and `--deadline-ms`
limit that work. `--namespaces` and `--jobs` accept comma-separated lists or
repeated flags. Omitting `--jobs` also runs metadata, metadata compaction,
and core garbage collection.

The `grep-gc` job completes one collection pass per call.
`loonfs maintenance grep-gc` runs a pass directly for one namespace,
including an absent or deleted namespace whose old index data remains.
Every pass reads the current manifest and hint before deletion. Manifests use
contiguous numbers and put-if-absent publication. `hint.json` starts forward discovery
and may lag. Queries validate a cached manifest with one HEAD of its
successor. The durable layout and collection rules are in
[grep format](../../docs/specs/format.md#appendix-d-grep-extension-format).

`GrepWorkerConfig` controls how much work one step may perform. A server reads
these values from its `[grep]` table:

```toml
[grep]
mode = "serve_and_maintain"
max_files_per_step = 256
max_content_bytes_per_step = 67108864
```

Both input limits must be greater than zero. These values do not control
concurrency. The server's `max_concurrent_maintenance` bounds how many
namespaces its sweep visits at once, grep indexing included.
