# Maintenance hosting and recovery

The hosting layer assigns namespace/job pairs explicitly. On startup and at a
regular interval, it nudges every assigned pair. The embedded CLI host does this
immediately and every 60 seconds by default. A replacement host repeats that
assignment from its own configuration; it does not need the old process's hints
or runner state. Writer namespace assignment and handoff remain separate from
maintenance scheduling.

A nudge is a scheduling hint. The runner coalesces hints, limits concurrency,
retries failures with backoff, and probes admitted keys periodically. Idle keys
can leave the admitted set. Those probes do not enumerate namespaces or replace
the hosting layer's assignment loop. GC and compaction jobs have no cheap
standalone debt probe, so periodic assignment runs them to inspect durable state.

Metadata probes inspect the WAL and manifest descriptors using the same WAL
threshold and compaction policy as the job's execution. A probe can report work
while a lease still prevents that work from running. The next assigned pass
checks the lease again; a remembered deadline is not required for correctness or
recovery.

After a compaction worker stops, its incomplete output remains unpublished.
Once its lease expires, maintenance plans a fresh job from the current manifest.
It repeats the abandoned work rather than resuming partial segments. GC removes
unreferenced staged output under its lease-claim rules. Successful jobs also
retain their lease until expiry to protect published output from older GC scans.
This protection can delay the next compaction window for the same family group.

GC cursors currently resume candidate enumeration only. Every invocation rebuilds
reachability, so a budget too small to finish that work can still prevent
progress. Periodic assignment and restart recovery do not solve resumable marking.
A safe marking continuation needs a protocol for protecting its basis while new
checkpoints, forks, and manifests are published.
