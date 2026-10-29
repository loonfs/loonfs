# loonfs-grep

`loonfs-grep` implements LoonFS's optional gram index, which `loonfs grep` uses to search file content. Each namespace stores its own index under `namespaces/{namespace_id}/extensions/grep/`. You need this crate only if you're writing a host (the program that runs the index steps). If you run `loonfs-server`, the server already runs these steps, and you configure them in its `[grep]` table (shown below). To build or collect an index by hand, use the `loonfs maintenance` commands described below.

Grep never scans the store to discover namespaces. Instead, its host calls these typed steps on `GrepWorker`, passing the ID of the namespace to work on: `build_step`, `reorganize_step`, and `gc_namespace`.

Commits don't schedule index work. The reference server (`loonfs-server`) runs index work in two passes:

- The maintenance sweep visits every namespace on a regular schedule. Each visit runs build steps for as long as they publish (at most 16), and then runs one reorganize step once the index is up to date. When the sweep runs a collection pass, that pass also runs grep garbage collection.
- A second pass runs every 5 seconds. For each writer session the server has open, it builds that namespace's index if the session's last published sequence number has changed since this pass last indexed it.

A query still returns correct results while the index is behind, because it scans the files that were committed after the last commit the index covers.

The CLI runs the same steps one command at a time:

```console
loonfs maintenance index enable --namespace docs
loonfs maintenance gc --namespace docs
```

On an embedded profile, `maintenance index enable` runs build steps until the index reaches the namespace's sequence number from when the command started. On a remote profile, it waits for the server to get there. `--max-steps` and `--deadline-ms` limit the wait. A write through an embedded profile doesn't update the index, so to bring an active index up to the namespace head, run `maintenance index enable` again.

`loonfs maintenance gc` runs the core garbage collector for one namespace, and then one grep collection pass, which reports its counts in the result's `extensions.grep` object. A host built on `loonfs-http` runs the same pair through `loonfs_http::gc_namespace`. The grep pass also runs for a deleted namespace, and for a namespace that doesn't exist, as long as its old index data is still stored. For a namespace that doesn't exist, the command reports `namespace_not_found` after the pass. Every pass reads the current manifest and hint before it deletes anything. Grep manifests are numbered without gaps (1, 2, 3, and so on) and published with put-if-absent. `hint.json` is where forward discovery starts, and it can lag behind the newest manifest. A query checks that a cached manifest is still current with one HEAD request for the next manifest number. [Grep extension format](../../docs/specs/format.md#appendix-d-grep-extension-format) defines the stored layout and the collection rules.

`GrepWorkerConfig` controls how much work one step can do, and how many steps can read and process content at once. A server reads these values from its `[grep]` table:

```toml
[grep]
mode = "serve_and_maintain"
max_files_per_step = 256
max_content_bytes_per_step = 67108864
max_concurrent_steps = 2
```

Every value must be greater than zero, and `max_files_per_step` can be at most 1,000, the largest page a build step reads. The host builds one `GrepPassBudget` from `max_concurrent_steps` and passes it to `GrepWorker::new`. When a build or reorganize step finds work to do, it waits for one of the budget's permits before it reads file content or index segments, and it keeps the permit until it publishes. A step that finds the index up to date, or finds nothing to merge, doesn't take a permit. `GrepPassBudget::stats` reports how many steps are running and how many are waiting.

The server's `max_concurrent_maintenance` limits how many namespaces its sweep visits at once. Separately, `max_concurrent_steps` limits how many steps read and process content at once, no matter how many visits are running.

Clones of a budget share its permits. If your host runs more than one worker (for example, one for each runtime or store), pass a clone of the same budget to each worker. `max_concurrent_steps` then limits the steps of all the workers together. A worker that's given its own budget runs its steps in addition to that limit.
