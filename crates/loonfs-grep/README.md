# loonfs-grep

`loonfs-grep` implements LoonFS's optional gram index, which `loonfs grep` uses to search file content. A namespace's index state is grep's entry in the namespace manifest, which lists the index's segments. The segments are stored under `namespaces/{owner_namespace_id}/extensions/grep/segments/`. You need this crate only if you're writing a host (the program that runs the index steps). If you run `loonfs-server`, the server already runs these steps, and you configure them in its `[grep]` table (shown below). To build or collect an index by hand, use the `loonfs maintenance` commands described below.

The index records content values rather than file revisions. A "content value" is the bytes that a content reference refers to, identified by the reference's content ID and size, so the index reads and tokenizes those bytes once, however many files refer to them. Each run of index segments stores its gram postings as positions in the run's own table of content values, and a query finds the files that refer to a matching value through the index's `content_revisions` rows. The [fork-family grep note](../../docs/design/grep-fork-family.md) explains the design.

Grep never scans the store to discover namespaces. Instead, its host calls typed steps, passing the ID of the namespace to work on. `GrepWorker::steps` starts a run of steps over one namespace, whose changes to grep's entry are kept in memory and published together in one manifest once a publish interval has passed, and when the host calls `GrepSteps::publish`. `GrepSteps::step` runs the next step, which is a reorganize step when a reorganization is in progress or a level of the index has reached its run limit, and a build step otherwise. (`GrepSteps::build_step` and `GrepSteps::reorganize_step` run one kind of step.)

Commits don't schedule index work. The reference server (`loonfs-server`) runs index work as the index step of its maintenance scheduler:

- Every maintenance interval (`maintenance_interval_ms`, 5 seconds by default), the scheduler runs an index step for each writer session the server has open, if the session's last published sequence number has changed since the step last indexed it.
- A sweep, which runs at start and then every `sweep_interval_ms` (a day by default), runs an index step for every namespace in the store.

An index step runs `GrepSteps::step` for as long as each step does some work (at most 16 steps), so the index is reorganized while it's behind a writer. The steps' changes are published once per maintenance interval, and again when the index step ends.

A query still returns correct results while the index is behind, because it scans the files that were committed after the last commit the index covers.

The CLI runs the same steps one command at a time:

```console
loonfs maintenance index enable --namespace docs
```

On an embedded profile, `maintenance index enable` runs the same steps until the index reaches the namespace's sequence number from when the command started. On a remote profile, it waits for the server to get there. `--max-steps` and `--deadline-ms` limit the wait. A write through an embedded profile doesn't update the index, so to bring an active index up to the namespace head, run `maintenance index enable` again.

The core garbage collector (`loonfs maintenance gc`) collects the index's segments that no current, pinned, or recently superseded manifest lists, and it counts them in the result's `deleted.extension_segments`. A query reads grep's entry from the manifest of its read view, so it sends no request for the index beyond the segment reads. [Grep extension format](../../docs/specs/format.md#appendix-d-grep-extension-format) defines the entry, its publication, and the stored layout.

`GrepWorkerConfig` controls how much work one step can do, and how many steps can read and process content at once. A server reads these values from its `[grep]` table:

```toml
[grep]
mode = "serve_and_maintain"
max_files_per_step = 256
max_content_bytes_per_step = 67108864
max_concurrent_steps = 2
```

Every value must be greater than zero, and `max_files_per_step` can be at most 1,000, the largest page a build step reads. The host builds one `GrepStepBudget` from `max_concurrent_steps` and passes it to `GrepWorker::new`. When a build or reorganize step finds work to do, it waits for one of the budget's permits before it reads file content or index segments, and it keeps the permit until it writes its segments. A step that finds the index up to date, or finds nothing to reorganize, doesn't take a permit. `GrepStepBudget::stats` reports how many steps are running and how many are waiting, and `GrepWorker::publication_stats` counts the worker's publications of grep's entry by how each one ended (published, rebuilt over another manifest, or superseded).

The server's `max_concurrent_maintenance` limits how many namespaces its scheduler runs steps on at once. Separately, `max_concurrent_steps` limits how many grep steps read and process content at once, no matter how many namespaces have steps running.

Clones of a budget share its permits. If your host runs more than one worker (for example, one for each runtime or store), pass a clone of the same budget to each worker. `max_concurrent_steps` then limits the steps of all the workers together. A worker that's given its own budget runs its steps in addition to that limit.
