# Single-owner Publisher Evaluation

## Recommendation

**DEFER**, with **87% confidence**.

Reopen this decision when an accepted feature needs queue-aware,
per-namespace scheduling that cannot be expressed as one more short state
transition under the existing namespace mutex. Examples are adaptive pacing
from queue pressure, priorities between mutation classes, or a namespace-local
quota that must change while a publish is in flight. At that point the owner
loop would be buying a required control plane, rather than replacing working
coordination with a different coordination mechanism.

The present publisher is intricate, but the proposed owner task does not make
enough of that intricacy disappear. It would make mutation, delete, and pacing
decisions sequential by construction. In exchange, it would introduce a
request protocol, a permanently parked task for every touched namespace,
non-terminal flush semantics, channel backpressure, and a harder panic-recovery
problem. The cached commit engine would still need its asynchronous mutex unless
the public exact-batch path were also rerouted and given new ordering semantics
(`crates/loonfs/src/handle/writer.rs:421-435`,
`crates/loonfs/src/fs/writes.rs:501-523`).

The recommendation is to keep the current design until there is evidence that
its lock-based state machine is blocking a real feature or causing another
correctness defect. A later refactor should start from the tests named below;
it should not treat an actor as a correctness proof by itself.

## Current shape

One registry mutex protects the shutdown gate, the namespace-to-publisher map,
and spawned task handles (`crates/loonfs/src/publisher.rs:101-111`). Each
namespace publisher has a second, synchronous mutex over its open batch,
pending delete, terminal flags, in-flight commit map, single-flight claim, and
next CAS time (`crates/loonfs/src/publisher.rs:301-340`). The registry deliberately
avoids taking those locks in the opposite order when it closes admission
(`crates/loonfs/src/publisher.rs:257-267`). A submitter takes the namespace lock,
performs all admission work, optionally claims and registers a publish task,
and only then awaits its result channel (`crates/loonfs/src/publisher.rs:413-510`).

The active task repeatedly takes one work unit in admission order, releases the
namespace lock, and performs the store work (`crates/loonfs/src/publisher.rs:576-660`).
For mutations, it retries the same candidates and commit IDs on stale-head or
unknown-outcome results (`crates/loonfs/src/publisher.rs:696-725`,
`crates/loonfs/src/publisher.rs:1002-1012`). For deletion, it runs the sealed
batch first, then the delete, and either fails work queued after a successful
delete or lets it proceed after a failed delete
(`crates/loonfs/src/publisher.rs:579-614`,
`crates/loonfs/src/publisher.rs:738-795`).

Below that task, the runtime locks the cached `NamespaceCommitEngine` across
the complete asynchronous publish (`crates/loonfs/src/fs/writes.rs:501-523`).
The engine owns a warm tail projection and catalog entry
(`crates/loonfs-core/src/commit_engine.rs:278-294`), and a publish may await
writer acquisition, catalog loading, publish-view loading, WAL writing, and
the head CAS (`crates/loonfs-core/src/commit_engine.rs:392-470`,
`crates/loonfs-core/src/commit_engine.rs:489-520`). Namespace deletion takes
the same engine lock across its durable delete so it cannot pass an in-flight
publish (`crates/loonfs/src/fs/namespaces.rs:77-103`).

The core batch operation remains important regardless of the outer scheduling
shape. It prepares candidates in order against one evolving batch view
(`crates/loonfs-core/src/protocol/batch.rs:101-204`), writes all accepted records
to one WAL segment (`crates/loonfs-core/src/protocol/batch.rs:217-236`), and
advances the head with one CAS (`crates/loonfs-core/src/protocol/batch.rs:238-287`).
It also resolves commit IDs against durable receipts and earlier candidates in
the same batch (`crates/loonfs-core/src/protocol/candidates.rs:45-117`,
`crates/loonfs-core/src/protocol/candidates.rs:213-237`). An owner task would
schedule this protocol; it would not replace it.

## Await audit

No standard mutex guard in the publisher is held across an await. The relevant
boundaries are:

| Await | Relationship to shared state |
| --- | --- |
| Submitter waits for its commit result | Admission, in-flight insertion, and any task spawn are complete before `receiver.await` (`crates/loonfs/src/publisher.rs:418-438`). |
| Delete submitter waits for its result | The delete is joined or installed, its batch is sealed, and any task spawn is complete before `receiver.await` (`crates/loonfs/src/publisher.rs:518-556`). |
| Registry drains tasks | The task vector is moved out of the registry before any join handle is awaited (`crates/loonfs/src/publisher.rs:277-290`). |
| Work loop waits for pacing | Queue depth is copied under the lock and the guard is gone before the pacing await; the work unit is taken in a later critical section (`crates/loonfs/src/publisher.rs:585-629`). |
| Work loop publishes a batch | Candidates have been moved out of shared state before the engine call is awaited (`crates/loonfs/src/publisher.rs:663-735`). |
| Work loop deletes a namespace | The durable delete is awaited before the publisher locks state to mark the namespace deleted and collect later waiters (`crates/loonfs/src/publisher.rs:740-785`). |
| Pacing sleeps | The deadline is copied under the mutex; both sleeps occur after the guard is dropped (`crates/loonfs/src/publisher.rs:798-835`). |
| Runtime uses the cached commit engine | This is the deliberate exception: a Tokio mutex guard is held across the full core publish (`crates/loonfs/src/fs/writes.rs:501-523`) and across the durable delete (`crates/loonfs/src/fs/namespaces.rs:85-94`). |

This matters to the evaluation. The current publisher mutex protects short,
plain state transitions, not I/O. The owner task would eliminate that mutex's
discipline, but it would not remove waiting from the protocol or remove the
cached-engine mutex while callers may use the exact-batch method directly
(`crates/loonfs/src/handle/writer.rs:421-435`).

## Correctness surface

“Structural” below means that one task's exclusive local state makes the
concurrent part of the invariant true without a mutex convention. It does not
mean the behavior comes for free. “Re-prove” means the actor boundary changes
the behavior or failure window enough that the named tests must be treated as
acceptance criteria again.

| Guarantee | Current location | Pinning test | Under one owner task |
| --- | --- | --- | --- |
| First-poll admission: submission is await-free through admission, so one pending poll has transferred ownership of the work. | Semantic identity is computed, the oneshot is created, and `admit` completes before the only await (`crates/loonfs/src/publisher.rs:413-438`). Admission itself performs the terminal checks, duplicate decision, capacity check, task claim, batch insertion, and in-flight insertion synchronously (`crates/loonfs/src/publisher.rs:441-510`). | `park_two_puts` polls the second future once and proves it is pending only after synchronous admission (`crates/loonfs/tests/publication.rs:138-149`, `crates/loonfs/tests/publication.rs:209-230`). | **Re-prove.** A bounded `send().await` breaks the await-free path. `try_send` can preserve a first-poll handoff, but enqueueing a command is earlier than the current duplicate, terminal, and batch-capacity decision. Calling enqueue “admission” would change the boundary unless the owner protocol gives the accepted command irrevocable service. |
| Cancelling a caller never cancels work already admitted. | The registry-spawned task owns the batch, while the caller only awaits a oneshot receiver (`crates/loonfs/src/publisher.rs:40-48`, `crates/loonfs/src/publisher.rs:423-438`, `crates/loonfs/src/publisher.rs:477-485`). Failed result delivery is ignored after state is settled (`crates/loonfs/src/publisher.rs:874-896`). | `cancelled_caller_does_not_cancel_admitted_publication` (`crates/loonfs/tests/publication.rs:241-268`) and `all_callers_cancelled_publication_still_lands` (`crates/loonfs/tests/publication.rs:270-297`). | **Mixed.** Once a command is non-blockingly accepted by a live owner, cancellation independence becomes structural: dropping the reply receiver does not remove the command. The first-poll handoff, a send racing task death, and restart ownership still need re-proving. |
| Group commit: concurrently admitted candidates can share one WAL segment and one head CAS while results remain positional. | New primaries enter one open batch (`crates/loonfs/src/publisher.rs:471-509`); the task takes the complete batch as one work unit (`crates/loonfs/src/publisher.rs:596-650`); the core writes one segment and performs one CAS (`crates/loonfs-core/src/protocol/batch.rs:217-287`). Delivery rejects a result-count mismatch rather than mispairing results (`crates/loonfs/src/publisher.rs:838-879`). | `publisher_batches_concurrent_distinct_commits_into_one_wal_segment` (`crates/loonfs/src/publisher.rs:2108-2162`) and the public-path test `concurrent_puts_coalesce_into_one_wal_segment` (`crates/loonfs/tests/runtime.rs:2473-2595`). | **Re-prove.** Exclusive queue ownership is structural, but the exact cold-first/hot-next collection rule depends on when the owner drains its mailbox relative to pacing and store awaits. A naive receive loop can make batches larger or smaller than today. |
| Commit-ID deduplication and join: an identical in-flight request joins the primary result; conflicting semantic reuse fails; a duplicate can join even when the pending batch is full. | The in-flight map compares semantic identity before appending the waiter (`crates/loonfs/src/publisher.rs:462-468`), new primaries are inserted only after the capacity check (`crates/loonfs/src/publisher.rs:488-509`), and all waiters receive the primary result (`crates/loonfs/src/publisher.rs:874-877`). Durable and same-batch reuse is independently resolved in core (`crates/loonfs-core/src/protocol/candidates.rs:45-117`, `crates/loonfs-core/src/protocol/candidates.rs:213-237`). | `publisher_duplicate_active_request_joins_while_conflict_fails` (`crates/loonfs/src/publisher.rs:1734-1772`) and `publisher_pending_batch_full_rejects_distinct_but_allows_duplicate` (`crates/loonfs/src/publisher.rs:1774-1838`). | **Mixed.** Exclusive access to the in-flight map becomes structural. Semantic comparison, the duplicate-before-capacity ordering, durable receipt replay, and waiter fan-out remain ordinary logic and must be re-proved. |
| Delete barrier ordering: work admitted before the delete publishes first; later work publishes only if deletion fails and otherwise receives `namespace_deleted`. | A new delete seals the current batch and later mutations use a fresh batch (`crates/loonfs/src/publisher.rs:513-548`). The single task drains sealed batch, delete, then later work (`crates/loonfs/src/publisher.rs:579-614`); successful deletion marks the publisher terminal and rejects the later batch (`crates/loonfs/src/publisher.rs:748-785`), while failed deletion leaves it runnable (`crates/loonfs/src/publisher.rs:787-794`). | `delete_barrier_publishes_admitted_work_and_rejects_later_work` (`crates/loonfs/src/publisher.rs:2022-2106`) and the single-flight regression test `delete_queued_mid_publish_waits_behind_the_sealed_batch` (`crates/loonfs/src/publisher.rs:2467-2605`). | **Mixed.** One FIFO command stream makes one event order structural. The protocol must still define whether order is successful channel enqueue, owner receive, or completed admission. Batch sealing, joined deletes, failed-delete continuation, and rejection of already-queued later mutations all need re-proving. |
| Stale-head and unknown-outcome retries preserve commit IDs and resolve ambiguity through durable receipts. | The retry loop clones the same candidates for up to eight attempts and retries only stale-head or unknown-outcome results (`crates/loonfs/src/publisher.rs:696-725`, `crates/loonfs/src/publisher.rs:1002-1012`). Core receipt lookup turns the same semantic identity into the original response (`crates/loonfs-core/src/protocol/candidates.rs:219-247`). | `stale_head_retry_preserves_content_admission` (`crates/loonfs/tests/content_request_accounting.rs:1123-1154`) and `publisher_resolves_unknown_head_outcome_by_replaying_receipt` (`crates/loonfs/src/publisher.rs:1945-1969`). | **Re-prove.** Ownership does not make retry classification, retry limits, pacing between attempts, candidate identity, or durable receipt replay structural. This logic should move unchanged if an actor is ever introduced. |
| Shutdown closes admission and drains every admitted publication, including work respawned after a task panic. A non-terminal writer drain leaves foreground publishing usable. | A task is claimed and registered while the namespace admission lock is held (`crates/loonfs/src/publisher.rs:124-132`, `crates/loonfs/src/publisher.rs:477-485`). Close marks the registry and all existing publishers closed (`crates/loonfs/src/publisher.rs:254-267`); drain repeatedly takes and joins the task list so it sees panic respawns (`crates/loonfs/src/publisher.rs:270-297`, `crates/loonfs/src/publisher.rs:979-990`). `FsWriter::shutdown_background` deliberately drains without closing publisher admission (`crates/loonfs/src/handle/writer.rs:444-467`). | `registry_close_admission_refuses_new_work_while_admitted_work_drains` (`crates/loonfs/src/publisher.rs:2259-2317`), `registry_drain_surfaces_panics_and_settles_respawned_work` (`crates/loonfs/src/publisher.rs:2319-2398`), and `all_callers_cancelled_publication_still_lands` (`crates/loonfs/tests/publication.rs:270-297`). | **Re-prove.** A long-lived owner cannot be joined by the non-terminal drain. It needs a `Flush` barrier that covers every command accepted before it while allowing later commands, plus a distinct terminal `Close`. The registry must make close versus sender cloning atomic and must define how flush behaves across owner restart. |
| Cold submissions publish immediately; later submissions inside the interval coalesce, and retries take another CAS token. | The initial deadline is in the past (`crates/loonfs/src/publisher.rs:382-391`); the work loop paces before taking a unit and stamps the next deadline when it does (`crates/loonfs/src/publisher.rs:585-629`); retries claim a later token (`crates/loonfs/src/publisher.rs:716-725`, `crates/loonfs/src/publisher.rs:816-835`). | `cold_submission_publishes_without_a_coalescing_delay` (`crates/loonfs/src/publisher.rs:1881-1911`) and `hot_submissions_wait_out_the_pacing_interval` (`crates/loonfs/src/publisher.rs:1913-1943`). | **Mixed.** Local ownership makes the deadline race-free. The drain-versus-timer policy and the rule that retries consume later tokens still need re-proving. |
| A publish-task panic gives the taken batch an honest unknown outcome, respawns queued work, and leaves the namespace serviceable. | `PublishAbortGuard` remembers the taken commit IDs, clears the claim on abnormal drop, fails their waiters with unknown outcome, and registers a replacement for queued work (`crates/loonfs/src/publisher.rs:925-999`). | `publisher_survives_publish_task_panic_and_keeps_serving` (`crates/loonfs/src/publisher.rs:1971-2020`) and `registry_drain_surfaces_panics_and_settles_respawned_work` (`crates/loonfs/src/publisher.rs:2319-2398`). | **Re-prove, with greater difficulty.** If the whole actor panics, its receiver, open batch, in-flight map, and reply senders die together. A supervisor can restart the task, but not reconstruct that local state. The owner must instead catch panics around each publish operation while retaining recovery metadata, or externalize enough state to a supervisor, weakening the single-owner premise. |

The table separates two kinds of deduplication. The publisher's in-flight map
joins callers before any store work (`crates/loonfs/src/publisher.rs:462-468`).
Core's batch and durable-receipt logic handles duplicates encountered during
or after store work (`crates/loonfs-core/src/protocol/candidates.rs:45-117`,
`crates/loonfs-core/src/protocol/candidates.rs:213-247`). Both remain necessary
under an actor.

## Complexity delta

The current production portion of `publisher.rs` is 1,068 physical lines
before its test module. About 920 of those lines cover registry lifecycle,
namespace admission, the work loop, retries, delivery, tracing, and panic
recovery (`crates/loonfs/src/publisher.rs:79-1000`). This is large, but much of
it is policy and failure handling that an owner loop would retain.

An owner-task implementation is estimated at **850-1,150 production lines**
for the same behavior, excluding tests and excluding the unchanged core batch
protocol. That range is deliberately broad: the optimistic end assumes panic
is caught inside the owner and an unbounded command channel is acceptable; the
pessimistic end includes bounded backpressure, observable flush barriers, and
a supervisor. The likely result is roughly **100 lines fewer to 100 lines
more**, not a decisive reduction. A credible implementation and flake soak is
roughly **five to eight engineering days** after its behavior is agreed.

### What dissolves

- The `Arc<Mutex<NamespacePublisherState>>` and its short critical sections
  disappear; `batch`, `pending_delete`, `deleted`, `closed`, `in_flight`, and
  `next_allowed_cas_at` become fields used only by the owner
  (`crates/loonfs/src/publisher.rs:301-340`).
- The `publishing` flag and the claim-before-spawn/clear-on-empty dance
  disappear from normal operation (`crates/loonfs/src/publisher.rs:471-485`,
  `crates/loonfs/src/publisher.rs:596-627`).
- The lock-order rule between namespace admission and task registration
  becomes smaller, although the registry still needs synchronization for its
  closed flag, publisher map, and task handles
  (`crates/loonfs/src/publisher.rs:101-132`,
  `crates/loonfs/src/publisher.rs:257-267`).

The cached-engine `AsyncMutex` does **not** automatically dissolve. The public
exact-batch method currently calls the engine-level batch path directly
(`crates/loonfs/src/handle/writer.rs:421-435`), and deletion uses the same lock
as publication (`crates/loonfs/src/fs/namespaces.rs:85-94`). Removing it would
require routing those operations through the actor and specifying how an exact
batch orders against ordinary submissions, pacing, and deletion. That is a
larger behavioral change than the proposal under evaluation.

### What appears

- A command enum needs at least `Submit`, `Delete`, non-terminal `Flush`, and
  terminal `Close`, each with defined acknowledgement and channel-closure
  behavior. `Submit` and `Delete` still need per-request oneshots because every
  caller receives its own result today (`crates/loonfs/src/publisher.rs:342-368`,
  `crates/loonfs/src/publisher.rs:423-438`).
- The channel must reconcile first-poll handoff with backpressure. A bounded
  asynchronous send violates the current await-free admission boundary
  (`crates/loonfs/src/publisher.rs:413-438`). A bounded `try_send` makes channel
  capacity observable but does not implement the existing rule that duplicates
  join even when the pending batch is full
  (`crates/loonfs/src/publisher.rs:462-492`). An unbounded channel preserves a
  synchronous handoff but moves the 1,024-candidate limit behind the channel
  and allows memory to grow before the owner applies it
  (`crates/loonfs/src/publisher.rs:76`,
  `crates/loonfs/src/publisher.rs:488-509`).
- Task lifetime changes from work-scoped tasks to an idle parked task for every
  namespace retained in the registry. Today the registry retains a publisher
  after its work loop exits and only a successful delete evicts it
  (`crates/loonfs/src/publisher.rs:620-632`,
  `crates/loonfs/src/publisher.rs:769-776`). Under the actor shape, every such
  entry also retains a receiver and live Tokio task.
- Panic recovery needs an explicit design. The current abort guard keeps queued
  shared state alive outside the panicking task
  (`crates/loonfs/src/publisher.rs:925-999`). Plain local actor state has no
  equivalent restart source.
- Non-terminal drain needs a flush watermark or barrier. The current drain can
  join all finite work tasks without closing admission
  (`crates/loonfs/src/publisher.rs:270-297`,
  `crates/loonfs/src/handle/writer.rs:460-467`); a parked owner is intentionally
  never finished.

The net is therefore differently shaped more than genuinely smaller. The
central queue state becomes easier to read, but failure and lifecycle state
become more elaborate.

## Comparison with the grep driver

The recent grep driver is the right vocabulary precedent: one per-namespace
task runs bounded steps, drains work until caught up, parks without a timer,
and wakes on a non-blocking nudge (`crates/loonfs-grep/src/driver.rs:130-203`).
Its capacity-one channel deliberately coalesces any number of nudges
(`crates/loonfs-grep/src/driver.rs:59-65`,
`crates/loonfs-grep/src/driver.rs:147-167`). The server keeps task handles in a
mutex-protected namespace map and joins them on stop or shutdown
(`crates/loonfs-server/src/grep_drivers.rs:16-22`,
`crates/loonfs-server/src/grep_drivers.rs:46-70`,
`crates/loonfs-server/src/grep_drivers.rs:124-140`).

That precedent lowers the novelty of nudge, drain, park, and shutdown naming.
It does not supply the publisher's missing protocol. A grep nudge has no
individual result, duplicates are supposed to collapse, work can be recovered
from durable index state, and stopping between bounded steps is sufficient
(`crates/loonfs-grep/src/driver.rs:59-65`,
`crates/loonfs-grep/src/driver.rs:123-127`,
`crates/loonfs-grep/src/driver.rs:177-190`). Publisher submissions must not
collapse, must retain caller-specific waiters, have a first-poll ownership
boundary, and may have crossed the head CAS when a task dies
(`crates/loonfs/src/publisher.rs:413-438`,
`crates/loonfs/src/publisher.rs:925-999`).

## Behavioral risk and migration shape

This code has already needed targeted fixes at exactly the seams an actor would
replace: lifecycle and drain in #247, taking work-loop ownership before spawn in
#248, adaptive pacing in #269, and deterministic admission-before-cancellation
proof in #333. The present regression tests exercise the resulting interleavings
directly at `crates/loonfs/src/publisher.rs:1681-1838`,
`crates/loonfs/src/publisher.rs:1881-2020`,
`crates/loonfs/src/publisher.rs:2022-2162`, and
`crates/loonfs/src/publisher.rs:2259-2605`.

There is no useful “move one field at a time” path to a true owner. The open
batch, in-flight map, and pending delete jointly define one admission order
(`crates/loonfs/src/publisher.rs:317-338`). Moving only pacing and store I/O to
a permanent task leaves admission and delete sealing under the mutex, adds the
new lifecycle costs, and removes little. Moving only the in-flight map behind a
channel makes duplicate admission asynchronous and breaks the current
duplicate-before-capacity decision (`crates/loonfs/src/publisher.rs:462-492`).
Moving only the delete introduces two ordering authorities, which is precisely
what the single-flight `publishing` claim prevents
(`crates/loonfs/src/publisher.rs:331-338`,
`crates/loonfs/src/publisher.rs:596-627`).

The smallest credible future migration would therefore have one discontinuous
stage: switch admission, open-batch formation, in-flight deduplication, delete
sealing, pacing, and delivery to one owner together. Retry and core batch code
should remain unchanged behind that stage
(`crates/loonfs/src/publisher.rs:663-735`,
`crates/loonfs-core/src/protocol/batch.rs:76-287`). Before that switch, a
preparatory change could extract pure state transitions and add black-box
observation hooks, but it would not yet be an actor.

Keeping every existing publication test literally untouched is also not
possible with a final owner-only state shape. Several unit tests directly lock
and inspect `NamespacePublisherState`, including the pending-batch test
(`crates/loonfs/src/publisher.rs:1704-1719`), cold-batch tests
(`crates/loonfs/src/publisher.rs:1864-1871`,
`crates/loonfs/src/publisher.rs:1899-1908`), shutdown panic test
(`crates/loonfs/src/publisher.rs:2352-2374`), and delete-order test
(`crates/loonfs/src/publisher.rs:2493-2572`). Their behavioral assertions should
survive, but their inspection mechanism would need a test-only snapshot or a
mechanical rewrite to observe actor state. The public cancellation tests can
and should remain untouched (`crates/loonfs/tests/publication.rs:138-297`).

This is not an argument that the current layout must remain forever. It is an
argument that the migration is a coordinated state-machine replacement, not a
low-risk sequence of independent moves.

## What a single owner would buy

| Pain point | Benefit from one owner | Assessment today |
| --- | --- | --- |
| Namespace lock discipline | Batch, delete, in-flight, and pacing state could be read and changed without a mutex; only message order would matter. | Real benefit, but the current critical sections contain no awaits and the lock order is documented at `crates/loonfs/src/publisher.rs:124-132` and `crates/loonfs/src/publisher.rs:257-267`. The cached-engine lock would remain for direct exact-batch and delete callers (`crates/loonfs/src/fs/writes.rs:501-523`, `crates/loonfs/src/fs/namespaces.rs:85-94`). |
| Single-flight claim dance | There is only one loop, so `publishing` cannot be claimed twice or cleared while work is queued. | Real benefit aimed directly at the regression pinned by `delete_queued_mid_publish_waits_behind_the_sealed_batch` (`crates/loonfs/src/publisher.rs:2467-2605`). The present claim is nevertheless localized and pinned at `crates/loonfs/src/publisher.rs:471-485` and `crates/loonfs/src/publisher.rs:596-627`. |
| Cancellation reasoning | After a successful non-blocking send, the owner holds the command independently of the caller. | Helpful, but the hard first-poll boundary moves to channel acceptance and still needs proof (`crates/loonfs/src/publisher.rs:413-438`, `crates/loonfs/tests/publication.rs:209-230`). The current implementation already meets the behavior in both cancellation tests (`crates/loonfs/tests/publication.rs:241-297`). |
| Delete ordering | A single command stream provides one obvious order for mutations and deletes. | Strong conceptual benefit. The current sealed-batch rule already supplies the same result and is pinned against the former racing-task failure (`crates/loonfs/src/publisher.rs:513-614`, `crates/loonfs/src/publisher.rs:2467-2605`). |
| Future per-namespace pacing policies | Queue depth, recent outcomes, deadlines, and policy could evolve together as local state, with the owner selecting its next wakeup. | This is the strongest reason to reopen. Today's one fixed interval needs only `next_allowed_cas_at` and is already race-free under the mutex (`crates/loonfs/src/publisher.rs:338-340`, `crates/loonfs/src/publisher.rs:798-835`). |
| Panic isolation | None automatically. Losing the owner loses the only copy of volatile queue state. | A regression unless the design catches panics inside the owner or introduces a supervisor protocol equivalent to `PublishAbortGuard` (`crates/loonfs/src/publisher.rs:925-999`). |
| Backpressure | A channel can expose a conventional capacity boundary. | Not automatically equivalent. Current capacity applies to distinct candidates in the pending open batch after duplicate joining (`crates/loonfs/src/publisher.rs:462-509`); channel capacity counts commands before the owner can make that distinction. |

## Why defer

1. The owner makes the total order clearer, but most load-bearing behavior is
   policy that still needs to be carried over and re-proved. Only exclusive
   access to local queue state becomes automatic.
2. The likely production-line delta is near zero, while permanent task
   lifecycle, non-terminal flush, backpressure, and panic recovery are new
   concepts. The engine lock remains unless the scope grows into public API
   ordering changes.
3. The safe cut is not gradual. Admission, deduplication, batch formation, and
   deletion form one state machine, and current white-box tests depend on that
   shape. The publisher is well pinned now; replacing all of those seams at
   once without a feature payoff is avoidable risk.

## Strongest arguments against deferring

1. The `publishing` claim and delete sealing are the most subtle part of the
   current implementation. One owner would make “only one loop chooses the
   next work unit” true by construction and rule out the class of race fixed in
   #248 (`crates/loonfs/src/publisher.rs:331-338`,
   `crates/loonfs/src/publisher.rs:596-627`).
2. The grep rework gives the repository a current, understandable model for
   per-namespace tasks that drain work, park, wake on a nudge, and shut down
   explicitly (`crates/loonfs-grep/src/driver.rs:130-203`). Reusing that
   vocabulary now could make future concurrency work more uniform.
3. The publisher already has enough fields and recovery machinery that its
   conceptual model is actor-like. Moving the state into its worker while the
   regression suite is unusually strong may be safer than waiting for another
   scheduling feature to increase the number of states
   (`crates/loonfs/src/publisher.rs:317-368`,
   `crates/loonfs/src/publisher.rs:1571-2605`).

Those arguments are substantial, which is why the recommendation is DEFER
rather than DECLINE. They do not outweigh the lack of a net complexity win and
the unsolved owner-panic and non-terminal-drain semantics today.

## Final decision

**DEFER — 87% confidence.** Reopen when an accepted queue-aware,
per-namespace scheduling feature would otherwise make the mutex-owned state
machine materially more complex.
