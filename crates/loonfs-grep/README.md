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

The CLI runs the same steps one command at a time:

```console
loonfs maintenance index enable --namespace docs
loonfs maintenance grep-gc --namespace docs
```

On an embedded profile, `maintenance index enable` runs build steps until the
index reaches the namespace's seq at the start of the command. On a remote
profile it waits for the server to get there. `--max-steps` and
`--deadline-ms` limit the wait. An embedded write does not move the index.
Run `maintenance index enable` again to bring an active index up to the
namespace head.

`loonfs maintenance grep-gc` runs one collection pass for one namespace,
including an absent or deleted namespace whose old index data remains.
Every pass reads the current manifest and hint before deletion. Manifests use
contiguous numbers and put-if-absent publication. `hint.json` starts forward discovery
and may lag. Queries validate a cached manifest with one HEAD of its
successor. The durable layout and collection rules are in
[grep format](../../docs/specs/format.md#appendix-d-grep-extension-format).

`GrepWorkerConfig` controls how much work one step may perform, and how many
steps hold content at once. A server reads these values from its `[grep]`
table:

```toml
[grep]
mode = "serve_and_maintain"
max_files_per_step = 256
max_content_bytes_per_step = 67108864
max_concurrent_steps = 2
```

Every value must be greater than zero. The host passes
`max_concurrent_steps` to `GrepWorker::new`. A build or reorganize step that
finds work waits for one of the worker's permits before it reads file
content or index segments, and holds it until it publishes. Every clone of a
worker shares its permits. A step that finds the index up to date, or
nothing to merge, takes no permit.
The server's `max_concurrent_maintenance` bounds how many namespaces its
sweep visits at once. `max_concurrent_steps` bounds how many steps hold
content at once, however many visits run.
