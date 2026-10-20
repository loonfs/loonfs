# Waiting at the write stop

This note describes what a [writer session](../specs/glossary.md#writer-session) does with a new commit when its [WAL tail](../specs/glossary.md#wal-tail) is full. The commit waits in the session's queue for a [fold](../specs/glossary.md#fold) to free room, and LoonFS refuses it with `maintenance_required` if a bounded wait passes first.

## The problem

A writer session keeps its WAL tail in its [writer position](../specs/glossary.md#writer-position) until a fold moves the tail into metadata segments. The "write stop" is the longest tail that a session lets grow: 128 unfolded WAL objects (`MAX_UNFOLDED_WAL_OBJECTS`). The session starts a fold at 32 WAL objects or 2 MiB of inline bytes, and it keeps publishing while the fold runs. (Past 32 MiB of unfolded inline bytes, LoonFS stages new inline content instead of refusing commits.)

When a batch reaches the write stop, the session refuses each new commit in it with `maintenance_required` and starts a fold. The commit engine returns the refusal while it prepares the batch, after the commit was admitted to the namespace's queue. The server returns it as a 503 without `Retry-After`, so neither the Rust client nor the generated SDKs retry it, and each application has to. An embedded caller can wait with `Namespace::wait_for_fold` and resubmit, but an HTTP client has no route for that and has to guess at a backoff.

The lab's 100k agent-sessions phase shows the cost under an append storm. Its clients wait for the writer's fold and resubmit, and they were refused between 64 and 3,999 times per run, depending on the store's latency. Their waiting, summed over the phase's clients, came to between 7.5 minutes and almost 6 hours per run.

The fold that frees the tail runs in the process that refused the commit, so the refusal asks the client to wait for work that the runtime is already doing.

## The rule

A new commit that reaches the write stop waits for the session's fold, for at most the "write-stop wait" (30 s by default), measured from when the commit entered the queue. If the wait passes first, LoonFS returns `maintenance_required`, and the commit isn't committed.

What waits is the commit's place in the namespace's queue, where the commit is already charged to the namespace's admission limits. No thread blocks. The caller's task waits for its result, as it does for any queued commit, and the namespace's worker task waits for the fold. To publish a batch, the worker:

1. Publishes the batch as it does now. A retry with a retained [receipt](../specs/glossary.md#commit-receipt) still returns its original result, and a refusal at the write stop starts a fold.
2. Keeps the refused commits at the front of the queue instead of returning the refusal.
3. Releases its publication slot, so other namespaces keep publishing, and waits until the fold finishes or the oldest waiting commit reaches the write-stop wait.
4. Returns `maintenance_required` to each queued commit that has reached the write-stop wait.
5. Takes a publication slot again, and publishes the queue in arrival order.

The fold's completion wakes the worker. The publisher already signals it, because the fold runs as a session task, and `Namespace::wait_for_fold` waits for that task to end. A fold also ends when another process already folded the tail (the position moves onto that process's manifest), and when it fails (the next publish starts another fold).

A fold frees the tail up to where it started, and commits published while it ran stay in the tail, so a fold can free less room than the queue needs. For example, if a fold starts at 32 WAL objects and 96 more are published while it runs, the next batch waits at the stop. The fold leaves 96 objects, so 32 more batches publish in arrival order (the first of them starts the next fold), and the commits behind them keep waiting.

The write stop itself doesn't move. It bounds the memory of the tail in the session's position, and waiting commits don't add to that tail.

Why keep waiting commits in the queue and not in a list of their own? The queue is already in arrival order, its worker already publishes one batch at a time, and the admission limits already bound it. A separate list would need its own order, bound, and wakeup.

## The write-stop wait

`CommitOptions` has no deadline, and the HTTP binding's `request_deadline_ms` (60 s by default) cancels the request handler, but the cancellation doesn't remove the queued commit, which can still publish after the server returns `deadline_exceeded`. So the runtime needs one setting, the write-stop wait, in its publication policy.

The reference server sets the write-stop wait to half its `request_deadline_ms`, and gains no setting. A commit that waits out the bound then receives `maintenance_required` (the commit didn't happen) before the server's deadline would return `deadline_exceeded` (the outcome is unknown).

## Costs

- The latency tail replaces the retry loop. A commit at the stop takes as long as the fold that frees room, about what the lab's clients spent waiting, without their round trips or guessed backoff.
- A commit that waits past the bound still fails. By then the fold is failing or far slower than the bound, so `maintenance_required` still needs an operator.
- Waiting commits keep their HTTP request slots, so `max_in_flight_requests` (256 by default) bounds how many wait at once on the reference server, and past the publication admission limits a commit still receives `commit_queue_full` at once. One namespace's waiting commits can take every slot, so other requests receive `server_busy` until the fold finishes, unless the host sets `max_requests_per_namespace` below `max_in_flight_requests`.
- A session close or a shutdown drain that starts while commits wait finishes once they publish or reach the bound.
- The runtime has one new setting.

## What the format and API specs change

No object, field, or bound in the format changes. In the format specification, step 2 of [Publication and group commit](../specs/format.md#63-publication-and-group-commit), the write-stop paragraph of [Commit identity and retries](../specs/format.md#65-commit-identity-and-retries), the write-stop row of [Recovery material and maintenance policy](../specs/format.md#73-recovery-material-and-maintenance-policy), and the write-stop rows of [Reference-implementation sizing and admission defaults](../specs/format.md#c4-reference-implementation-sizing-and-admission-defaults) describe the wait.

In the API specification's [Standard error contract](../specs/api.md#3-standard-error-contract), `maintenance_required` stays an error, and its row describes the deadline case (the wait passed, and the commit wasn't committed). The code stays out of automatic retry and without `Retry-After`, because a retry would wait again for a fold that isn't finishing, and `commit_queue_full` keeps `Retry-After: 1`. [Commit responses and safe retry](../specs/api.md#52-commit-responses-and-safe-retry) says that commits at the write stop wait.

The [self-hosting guide](../../crates/loonfs-server/docs/self-hosting.md#resource-sizing) describes the wait in its publication admission, inline tail limit, and `request_deadline_ms` paragraphs.

## Alternatives considered

Keep refusing. Each client then runs the wait that the runtime could run, and every SDK repeats it. A `Retry-After` hint or a route that waits for the fold still leaves a retry loop in every client.

Throttle commits before the stop, with a delay that grows as the tail goes from a low watermark to a high one. That adds a second threshold and a rate controller, and it delays commits even when the fold would finish before the stop. The wait delays a commit when the tail is at the stop, for as long as the fold runs.

Raise the stop. A burst that outruns the fold reaches any stop, so a higher stop moves the refusal later without removing it. It also raises each session's tail memory, and a longer tail makes the fold that frees it slower.
