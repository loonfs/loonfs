# One writer per namespace

This note describes which process writes a namespace's WAL and publishes its manifests. One process at a time does both, the namespace's "writer", whose [writer session](../specs/glossary.md#writer-session) has the namespace's writer epoch, and it publishes every change through one serial queue. Other processes read the namespace, write their own records beside it, collect it, and can build compaction output and index segments for the writer to install.

## The problem

A namespace's WAL and manifests are written by processes that don't coordinate. The writer session commits, any process can fold or compact the namespace and publish a manifest as its [maintainer](../specs/glossary.md#maintainer), and any process can write a [floor object](../specs/glossary.md#floor-object) to the WAL to advance the retention floor. Inside one process, a session's commits, its folds and its compaction units publish beside one another. Each is ordered by put-if-absent at the next number alone, so each is a race that needs rules for its loser. Three consequences have been measured or reproduced:

- A retention advance from a process without the writer session wins the next WAL number about 4% of the time against a saturated writer of small commits (968 of 1,008 floor object puts collided), so an advance takes 25 attempts and about 20 seconds at p50, and the writer pays one refused put and one discovery per advance (lab record `floor-advance-contention-20261126`).
- A process that snapshots or reads a busy namespace from another host is overtaken by the namespace's manifests. Against a writer whose process published about 4.8 manifests a second, 17 to 33 of 100 lookups, listings and forks returned `store_contention` because a discovery found a newer manifest 8 times. Snapshots failed 60 times in 100, 28 of them because the fold before the snapshot lost the next manifest number to the writer's folds and compaction units 8 times (lab record `discovery-under-publication-load-20261126`).
- A late floor object can leave a namespace unreadable. Suppose a new writer publishes claim manifest 8 with epoch 4 over manifest 7 (`folded_wal_no` 30, head 140), and the old writer commits WAL object 31 (sequence 141) before the new writer's fence. A maintainer folds 31 into manifest 9 (epoch 4), a collection deletes 31 a day later, and a floor object that a paused process planned against manifest 7 is then written at the empty number 31 with head 140. The new writer's discovery from manifest 8 replays it, puts the fence at 32 with head 140, and accepts the fence because 32 is above manifest 9's `folded_wal_no`. Every reader of manifest 9 then finds a fence whose head is below the manifest's and reports the namespace corrupt.

## The rule

A namespace has one writer at a time: the process that has claimed its [writer epoch](../specs/glossary.md#writer-epoch), by publishing a claim manifest and writing a [fence](../specs/glossary.md#fence) (see [Writer ownership](../specs/format.md#61-writer-ownership)). Only the namespace's writer writes its WAL and publishes its manifests. That covers every commit, fold, compaction result, extension entry (such as grep's), retention window change, retention floor advance, merge and namespace deletion. The rule applies to an active namespace, and creating the namespace is what makes it active.

Every manifest publication whose basis is an active manifest, other than a claim, checks, in the one function that publishes manifests, that the basis has the publisher's epoch. A basis with a higher epoch returns `writer_fenced`, and one with a lower epoch returns `namespace_corrupt`, as the writer's discovery does. Because the check is in that function, no publication can skip the rule.

The same function takes the segments that a publication adds and the time at which their writing started, and a publication that writes no segment passes an empty set. It refuses a candidate that lists a segment of the namespace that is neither in the basis nor in that set, and it refuses the put when that writing started longer ago than the compaction budget (`METADATA_COMPACTION_BUDGET_MS`, 12 hours) by the writer's clock. Beside that check, every publication keeps the publication budget (`METADATA_PUBLICATION_BUDGET_MS`, 15 minutes from the start of the publication to its put). A fold writes its segments after its publication starts, so the 15-minute budget binds a fold, and the 12-hour check binds a compaction and an install. A fold passes the segments it wrote and the time it started. Install passes a build's outputs and the time the build started, whether the writer or a worker built it (see [Building outside the writer](#building-outside-the-writer)).

An initializing namespace has no WAL and no writer, and the format allows three manifests after an initializing one: a fork's manifest 2, the activation, and the tombstone (see [Publishing a manifest](../specs/format.md#72-publishing-a-manifest)). So four callers race for that manifest with put-if-absent: the creator, a caller in any process that finishes an interrupted creation, a deletion of the initializing namespace, and a collection that tombstones an abandoned one. The first claim after activation makes the first writer.

## The publication lane

The writer's "publication lane" is its writer session's queue, which one worker runs one item at a time. Commits go through it as batches, and so do deletions. Every manifest publication of the writer is an item of the same queue: a fold's publication, a compaction install, an extension entry, a retention window change and a retention floor advance. A snapshot's record put and its verification are one item too, so no publication of the writer replaces the snapshot's manifest in between.

A lane item is a change that the writer applies to its own position's manifest when the item's turn comes. The session has that manifest in memory, so an item reads nothing from the store before its put. The heavy work happens before the item enters the lane: a fold writes its content objects and segments, a compaction unit rewrites its runs, and a grep step writes its segments. A change whose work another item has already covered publishes nothing (for example, an install whose inputs another install replaced), and collection deletes its segments by age. Inside the lane are the put and the boundary read after it, so apart from the cases below, a commit waits for at most one manifest put and one boundary read, about 34 ms on S3 at p50 (24.6 ms and 9.3 ms in `floor-advance-contention-20261126`).

These waits add up, because a session sends no WAL put while a manifest item has the lane. Below saturation, a commit occasionally waits for one item. The saturated writer of 60 KiB commits in `discovery-under-publication-load-20261126` published about 4.8 manifests a second, so by estimate its lane would spend about 160 ms of each second, about 16%, on manifests instead of commits.

Two rules change the order:

- **A fold's publication goes first when commits wait at the write stop.** Those commits can't be written until a fold frees room, so they wait behind it.
- **A floor advance and a deletion keep the lane for a fold of what arrived during an ordinary fold.** The manifest that each publishes then covers every commit queued ahead of it, and every commit queued behind it sees the new floor or the tombstone (see [The retention floor](#the-retention-floor)).

Because the writer publishes one manifest at a time and no other process publishes, a manifest put that the writer loses means another claim. The put either collides with the claim's manifest or, if a collection has already deleted that manifest, is written at its number, and the boundary read after the put reports the publication as lost. Both return `writer_fenced`. The writer builds on a manifest loaded from the store only when its position's manifest is gone from the store, which means another process claimed, and there the epoch check in the publication function returns `writer_fenced`. The check costs no request, because it compares the basis's epoch with the session's.

## The epoch around every put

The writer releases its epoch immediately before it sends any put, a WAL object or a manifest, and takes the epoch back when it learns the outcome. After an unknown outcome, its next publication claims a new epoch and writes a fence before it plans anything, as [Failed and unknown outcomes](../specs/format.md#64-failed-and-unknown-outcomes) describes for a commit. A lane item whose put ends with an unknown outcome ends there and returns the unknown outcome to its caller, as a commit does, and the writer's next publication claims.

A manifest put needs the same rule because a put whose outcome the writer didn't learn can still be written later. For example, if the writer's tombstone put at manifest 72 (`folded_wal_no` 930) returns no response and the writer went on to commit WAL object 931, a late tombstone would hide an acknowledged commit. With the rule, the writer's next publication is a claim at 72, which either collides with the tombstone (and the writer finds the namespace deleted) or takes 72 so that the late tombstone collides. The same order keeps a lost manifest put meaning another claim.

## The retention floor

The retention floor is the manifest's `retention_floor_seq`, and the writer raises it through its lane. To advance the floor, the writer:

1. Plans the target from its position's manifest under the rules in [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor): the three targets, the cutoff walk over the `commits` family, and the grace-window bound. If the target isn't above the floor, it writes nothing.
2. Checks that every segment of that manifest exists.
3. Takes the namespace's fold lock, which keeps its folds one at a time, so it waits for a running fold while commits continue.
4. Folds the tail as an ordinary fold, whose publication is an ordinary lane item.
5. Still under the fold lock, keeps the lane, folds what arrived during step 4, and publishes that fold's manifest with a floor that is the higher of the old floor and the target. It then releases the lane and the lock.

A deletion takes steps 3 to 5. In step 5, once it keeps the lane, it checks `expected_head_seq` against its position's head, which is exact because no commit can arrive while the lane is kept, so a deletion whose head moved fails before it does any work under the lane. It then folds what arrived and publishes that fold's manifest (or none, if nothing arrived), and then publishes the tombstone as its own manifest. If the fold of step 4 fails, the advance or the deletion returns the fold's error, and the floor and the namespace's status haven't changed.

Every fold in the writer's process runs under the fold lock, and the lock always comes before the lane. An ordinary fold has the fold lock while its publication waits for its turn in the lane, so an operation that kept the lane and then waited for the fold lock would stop both. Because the advance and the deletion keep the lock across both folds, no other fold of the namespace starts in between.

Commits wait for the fold of step 5, once per advance (once a day for each namespace under the sweep) and once per deletion. That fold covers what arrived during one ordinary fold, so commits wait at most about as long as an ordinary fold takes. In `floor-advance-contention-20261126`, a fold of a few commits took 156 ms at p50. With 60 KiB commits, a fold took 0.45 s at p50, and by estimate the writer writes 8 MiB, a fold threshold, in about that time, so the fold of step 5 is an ordinary fold's size. With 300-byte commits, a fold of up to 256 WAL objects took 2.7 s at p50 (3.9 s at p90), and the writer writes about 80 WAL objects in that time (one every 34 ms), so by estimate the fold of step 5 is about a third of that size. A tail can't pass the write stop (1,024 objects, 64 MiB, or 32 MiB of inline bytes), and by estimate a fold of a tail at the stop takes 10 to 16 seconds, inside the 30-second write-stop wait, so even then commits are delayed but not refused.

There is no floor object. A view's floor is its manifest's floor, and readers read it there. A restore written before step 5 keeps the lane is folded in step 4 or step 5, so its `revoke` event is in the manifest that records the new floor. A restore queued behind step 5 is validated against the new floor. The writer validates every restore against its own position's floor, which only its lane changes, so nothing has to be ordered against another process. If the writer stops before the manifest is written, the floor hasn't moved, and the next advance moves it.

The expired-deletion invariant then has a short argument. Take a manifest `M` whose floor the advance raised. Every WAL object at or below `M`'s `folded_wal_no` is folded into `M`. Every WAL object above it was written after the advance released the lane, by this writer against the new floor, or by a later writer whose claim built on `M` or a later manifest. A put sent before the advance whose outcome the writer didn't learn can't be written above `M`'s fold, by the argument in [Publication and group commit](../specs/format.md#63-publication-and-group-commit). Only the advance's step 5 and a fork's active manifest raise a manifest's floor, and every other publication copies it.

## The fold age

A session's fold is due once the tail reaches a fold threshold, or once the oldest unfolded commit is older than the fold age (15 minutes by default). So a writer that commits every 10 minutes still folds about every 15 minutes. A host with a scheduler, such as the reference server, checks its sessions' fold age at every maintenance interval, so a live writer whose folds succeed never has a tail much older than the fold age. An embedded session checks the fold age only when it publishes, so one that goes quiet keeps its unfolded tail, and a fork or a merge from another process that needs the tail folded claims the namespace (the embedded writer's next commit claims it back). The fold age is a required setting, and it has to be below `MAX_TAIL_COMMIT_AGE_MS` (11 hours and 45 minutes).

## Everyone else

Other processes read the namespace and create side records under their own keys with put-if-absent: user snapshots, fork snapshots and temporary snapshots, member entries at creation, and upload sessions and their outcome records. A snapshot's deletion and extension stay a compare-and-swap on its record.

Collection is unchanged. A namespace's collection reads the newest manifest and the snapshot listing, deletes what they don't reference once the age rules allow, raises the [manifest boundary](../specs/glossary.md#manifest-boundary) before it deletes manifests, writes a tombstone for an abandoned initializing namespace, and at retirement deletes the fork snapshots and the member entry. A pool's collection deletes content and upload records and writes the aborted outcome of an expired upload session. None of these is a WAL object or a manifest of an active namespace, so collection needs no writer.

## Changing a namespace that another process writes

A process that needs a namespace's WAL or manifest changed claims the namespace first. A server does that when a write or a maintenance call arrives for a namespace that it isn't the writer of. A fork from the current head and a merge fold the source or the fork first when a commit in its tail is older than `MAX_TAIL_COMMIT_AGE_MS`, so they claim it for that fold.

Because of the fold age, a tail that old belongs to a namespace that nobody has claimed since its last commit, or to a writer whose folds have kept failing for hours. A fork's or a merge's claim displaces a live writer only in that second case.

A snapshot's record is a side record, so a snapshot of the current head needs a fold, and so a claim when its process isn't the writer, only when the view has a commit above its manifest's head. (A tail of only fences counts as empty, as it does for a fork's fold of its source's tail.) Otherwise the snapshot writes its record on the current manifest and verifies it with the successor probe and the boundary read, as [Creating and verifying a snapshot](../specs/format.md#82-creating-and-verifying-a-snapshot) describes. If the probe finds a successor, the snapshot deletes its candidate and tries again from a fresh basis, which against a writer that keeps committing usually has commits in its tail, so the retry claims. A control plane that snapshots idle namespaces then leaves no fence, claim manifest or higher epoch on them.

## The reply and one retry

A request that finds a newer writer gets `writer_fenced`, whose details include the fenced epoch, the newer epoch, and the newer writer's writer block: its writer ID and when it claimed. The server's writer ID is unique to the process by default, because the server adds an ID that it generates at start to the configured `writer_id`.

When a session finds that it's fenced, it cancels its compaction unit, so its drain ends once the requests in flight and any running fold finish. The server then retries the fenced request once in a new session, whose first publication claims again. A request is safe to send again when the fenced attempt wrote nothing that the retry could repeat, and the retry checks the request's identity or preconditions against the new state. That's true of every request that can be fenced:

- **A commit, a merge and a replay.** `writer_fenced` comes from a discovery before the put or after a collision, so the fenced attempt's put wasn't written (a claim that finds a newer epoch returns `writer_fenced` before the commit's put). The retry's receipt check, with the same `commit_id` and fingerprint, replays an earlier put of the request that committed. A retry that is fenced before its receipt check hasn't looked for an earlier put whose outcome was unknown, which may have committed or may still commit. Preconditions are evaluated again.
- **A namespace deletion.** The tombstone is its last put, so a fenced deletion wrote none, and the retry checks `expected_head_seq` again while it keeps the lane.
- **A maintenance job and an install.** Each plans from the current state, so a repeat publishes at most what is due, and an install checks its inputs again.
- **A snapshot and a fork.** One that claims is fenced during its fold, before it writes its snapshot record or the fork's manifest 1. A snapshot whose verification finds a claim marks its candidate deleted first, as a failed creation does. A snapshot with nothing to fold doesn't claim, so it isn't fenced, and a successor at its verification means it tries again.

Reads and uploads never get `writer_fenced`. If the retry is displaced too, the client gets the reply, which identifies the newer writer, and a metric records each displacement. When a put of the request had an unknown outcome, the server returns `outcome_unknown` instead, as the publisher does for any failure after an unknown outcome, because that put can still commit.

The core has no other way to choose which process writes a namespace: no timer, liveness check, delay, lease, or directory of writers. Placement is up to whatever sends requests to servers.

## What a deployment uses

A control plane uses four operations:

- **Claim.** A server claims a namespace on first use, when a request first needs to change it. There is no claim route.
- **Release.** A route that closes the namespace's session for a planned move. The close rejects new work with `writer_session_closed`, drains the admitted commits, cancels the compaction unit, waits for a running fold, and writes nothing. It doesn't fold. The next writer pays one claim (2 puts and about 6 + k reads for a tail of k WAL objects, about 200 ms), reads the unfolded tail once, and starts with cold caches.
- **Maintain.** The maintenance jobs (`metadata`, `retention`, `retention_window` and `install`) and the routes that enable and disable the grep index run as the writer on the server that receives them, which claims first when it isn't the writer.
- **The reply.** `writer_fenced` after the one retry, or `outcome_unknown` when a put of the request had an unknown outcome, as above.

To see what needs work, a control plane uses the reads that exist. The namespace diagnostics route reports the head, the floor, the manifest number and the number of unfolded WAL objects, but not the tail's age. The manifest records the retention window, and a worker's planner reports whether a compaction unit is due in the manifest that it read. A `metadata` job runs as the writer and so claims, which makes it an action, not a read.

A single server that maintains its whole store needs no control plane. Its sweep checks each namespace with the reads that show whether a fold, a compaction unit or an advance is due, and claims the ones with work due.

## Building outside the writer

A "build" is the work of a compaction unit or a grep step, and an "install" is its one publication. A control plane can run builds in "workers", processes other than the writer, and chooses where and when they run.

A server setting, `writer_builds` (default `true`), controls whether the writer builds its own compaction output and index segments. With `writer_builds = false`, workers build and the writer only installs. Folding stays with the writer either way, because the writer folds from its session's position without reading the WAL, and the fold's publication has its place in the lane against the write stop, the floor advance and the deletion. If the setting is off and no worker hands anything back, runs accumulate and the planner reports a compaction unit as due. The writer doesn't start building after a delay.

A build needs no claim. A worker reads the namespace as any reader does, plans a unit from the manifest that it read with the planner the writer uses, writes the output segments under the namespace's prefix with fresh IDs as a compaction unit does, and publishes nothing. Its "build result" lists:

| Field | Contents |
| --- | --- |
| `kind` | `metadata_compaction` or `extension` |
| `planned_from` | The [manifest reference](../specs/format.md#25-manifest-references) of the manifest that the build read |
| `started_at_ms` | The worker's wall-clock time before its first output put |
| Inputs | For `metadata_compaction`, the family group, the input run numbers and their segment keys, and the floor that a base compaction applied. For `extension`, the extension's name and the entry that the build started from. |
| Outputs | For `metadata_compaction`, the output segment descriptors. For `extension`, the new entry. |

Install is a writer operation that takes one build result, and the server offers it as the `install` kind of the maintenance job route. The writer's compaction calls the same install with a result it built itself, so there is one path. The writer checks, in order:

1. **Already installed.** If the current manifest lists every output (or, for an extension, has the new entry), the install returns `installed` without publishing, so a retried install is safe.
2. **Inputs.** The current manifest lists the input runs with the same segment keys, or, for an extension, has the entry that the build started from. The base floor is at or below the current floor.
3. **Outputs.** Each output segment is under this namespace's prefix, isn't listed by the current manifest, and exists with the stored checksum that its descriptor records (one HEAD each).

Then the writer publishes the replacement manifest through its lane, with the output placed by `planned_from`'s head. Install passes the publication function the outputs and `started_at_ms` minus the safety margin (`GC_SAFETY_MARGIN_MS`, 5 minutes), so the function refuses the install when `started_at_ms` is more than 11 hours and 55 minutes before the writer's clock. The margin is subtracted because the worker's clock can be up to the margin ahead of the writer's, so the outputs can be that much older than `started_at_ms` says. (Install also refuses a `started_at_ms` more than the margin in the future, which no clock within the margin can report.) A result that fails a check is refused with the check's reason, and nothing references its outputs, so collection deletes them by the age rule for unlisted segments.

Why is the age bound enough? A collection deletes an unlisted segment once its provider age is 24 hours (`GC_GRACE_WINDOW_MS`). The two clocks agree within the safety margin (see [Clock and operation assumptions](../specs/format.md#114-clock-and-operation-assumptions)), so when the check passes, the oldest output is at most 12 hours old, and the install's put is applied within the publication completion bound (25 minutes). The output is referenced before it's 12 hours and 25 minutes old, the bound that the format asserts for a compaction in one process.

What install does in each case:

| Case | Install |
| --- | --- |
| Two workers build the same unit, or the writer compacts the same inputs meanwhile | The first install to reach the lane publishes. The other finds its inputs replaced and is refused. |
| A worker built from a manifest that the writer has since replaced | Installs if the inputs are still listed (a fold or another group's compaction replaced the manifest), and is refused if they aren't. |
| The result lists segments the worker didn't write | Refused if a segment is missing, listed already, under another prefix, or has another checksum. Install doesn't read rows, so a worker is trusted as any process with write access to the store is, and install is in the `admin` scope. A faulty worker's segments can drop, add or change any row of the family group that it compacted (a dropped commit receipt lets a retried commit apply twice, and a dropped tombstone row brings a deleted file back), and the writer publishes them. |
| A retried install arrives after a later compaction replaced its outputs | Refused, because its inputs are gone. The caller plans again from the current manifest, and nothing is lost. |
| The writer changes between build and install | The result doesn't refer to a writer, so the new writer installs it under the same checks. |

The search index fits. A grep step already ends with a publication that lists the entry it started from and the new entry, and the publication refuses a change whose starting entry was replaced. That is install's input check, so grep steps run in a worker hand back the same way.

## A day of one namespace

The namespace `docs` starts the day with manifest 40 (writer epoch 3, `folded_wal_no` 300, head 5,000, floor 4,000, retention window 7 days), and no process has claimed it since its last fold. Two of its deletions are 8 days old: `/old` at sequence 4,500 and `/tmp` at sequence 4,600. Server A has writer ID `a-7k2m` and server B `b-9q4t`.

1. **09:00, first commit.** A receives a commit and opens a session. Its first publication claims: manifest 41 with epoch 4 and A's writer block, a discovery that finds WAL 301 empty, and a fence at 301 with head 5,000. The commit is written at WAL 302 as sequence 5,001.
2. **09:15, a fold.** The oldest unfolded commit, at 302, reaches the 15-minute fold age with the tail at 420 (sequence 5,119). The fold writes its segments outside the lane while commits continue at 421 to 424. Its lane item releases epoch 4, puts manifest 42 (`folded_wal_no` 420, head 5,119), reads the boundary, and takes the epoch back. The commit at 425 waited for that one round trip. By 14:00, such folds and compaction units have published through manifest 60 (`folded_wal_no` 690).
3. **14:00, the floor passes a deletion.** The control plane sends `retention` to A. The cutoff walk from floor 4,000 stops at sequence 4,800, the last commit more than 7 days old. The advance's ordinary fold folds 691 to 698 into manifest 61 while commits continue, and a restore of `/old` is written meanwhile at 701 (sequence 6,301). Then the advance keeps the lane, folds 699 to 701, and publishes manifest 62 (`folded_wal_no` 701, head 6,301, floor 4,800). A restore of `/tmp` queued behind it is validated against floor 4,800, and its deletion at 4,600 has expired, so it fails (see [Expiring deletions and creating floor objects](../specs/format.md#expiring-deletions-and-creating-floor-objects) for the error). `/old` stays restored, because its `revoke` event is in manifest 62.
4. **14:10, a second server.** A misrouted commit reaches B, which claims manifest 63 with epoch 5, finds A's last commit at WAL 710, writes its fence at 711, and commits at 712. A's next commit collides with B's fence, finds epoch 5, and is fenced. A cancels the session's compaction, opens a new session, claims manifest 64 with epoch 6, fences at 713, and commits the request at 714 after its receipt check finds no earlier commit. The commit is about 200 ms slower, and A's displacement metric rises.
5. **17:00, deletion.** A's folds and compaction units have published through manifest 70 (`folded_wal_no` 900) when a client that read the head at WAL 930 sends the namespace's deletion with that head as `expected_head_seq`. A takes the fold lock and folds WAL 901 to 930 into manifest 71, and no commit arrives meanwhile. Then it keeps the lane, checks that the head is still the expected one, finds nothing more to fold, and publishes the tombstone, manifest 72, with epoch 6. B's fenced session finds the namespace deleted on its next open.

## What happens to the three races

- **A retention advance against a busy writer.** The advance is an item in the writer's lane, so it collides with nothing.
- **A process overtaken by manifests.** A snapshot doesn't race the writer's folds, because it folds only as the writer. Lookups, listings, forks and merges from another host still meet the writer's manifests at the same rate, so they still return `store_contention` as often as the record measured.
- **A late marker.** There is no floor object, and between a claim and its fence no other process can publish a manifest (the old writer's fold is written before the claim or is fenced), so no fold covers a WAL number between the claim and the fence.

## What each caller does

| Caller | What it does |
| --- | --- |
| A write | Goes to the writer's server. A server that isn't the writer claims, and a displaced request is retried once. |
| A read | Reads in any process. In the writer's process it reads from the session's position. |
| A maintenance job | Runs as the writer, claiming first when needed. `gc` runs anywhere. |
| A fork | Writes the new namespace's initializing manifests and the snapshots on the source. Claims the source only to fold a tail with a commit older than 11 hours and 45 minutes. |
| A merge, a rebase, a replay | Commits to the target as its writer, and claims the fork only to fold an old tail, as a fork does. |
| A snapshot | Of the current head with a commit in its tail, folds as the writer (its process claims first when it isn't the writer), and writes and verifies its record in the lane. Otherwise, and for deletion and extension, writes the record from any process. |
| A collection | Runs anywhere, unchanged. |
| The CLI | With a remote profile, sends requests to a server. With an embedded profile, its runtime claims for writes and for maintenance, which displaces a running server. |
| The embedded library | Its runtime is a process like any other: writes and maintenance claim, reads don't. Maintenance that publishes is a method of the writable namespace, not of `Maintenance`. The library returns `writer_fenced` without a retry. |

## Costs

- The writer's process does its own folds, and its own compaction and index builds unless `writer_builds` is off.
- Each manifest the writer publishes takes about 34 ms of the lane from commits: an occasional wait below saturation, and by estimate about 16% of the lane's time at 4.8 manifests a second.
- Commits wait for a fold of what arrived during one ordinary fold, at each floor advance and each deletion.
- A slow writer folds at every fold age, and each fold adds a small run for compaction.
- Maintaining a namespace that nobody has claimed costs a claim (about 200 ms). It leaves a fence of at most 572 bytes that collection never deletes, a claim manifest that collection deletes after 24 hours, and a higher epoch, so every other process's cached head reloads at its next check. A namespace that stops changing stops needing work, so these claims follow activity.
- Without placement, two servers that act on one namespace displace each other, so a deployment of several servers needs placement to maintain one store. The retry absorbs one displacement per request, the reply and the displacement metric show the operator, and each displaced request or misrouted install costs a claim, a fence and the displaced session's drain. A claim against a busy writer takes longer than 200 ms, because its manifest put races the writer's manifests. Against a writer that published about 4.8 manifests a second, a takeover took 5.8 to 10.6 seconds (`discovery-under-publication-load-20261126`).
- The tail of a crashed writer, or of an embedded session that goes quiet, stays unfolded until a process claims the namespace. Readers replay it, and a fork or a merge that needs it folded claims the namespace.

## What goes

- **The floor object**, with its rules in [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor): the put without the epoch, the check after its put, the retry up to 1,024 attempts, the session's queued floor object and its fallback, the floor of a view computed from the tail, the fold's maximum over floor objects, the collision with a floor object in [Publication and group commit](../specs/format.md#63-publication-and-group-commit), and the floor field of [WAL records](../specs/format.md#a5-wal-records).
- **The maintainer role and rebuilding over another process's publication**: the fold's reload of its tail over another writer's claim in [Publishing a manifest](../specs/format.md#72-publishing-a-manifest), the compaction's retry over another process's compaction and its "lost every attempt" outcome in [Streaming compaction](../specs/format.md#104-streaming-compaction), the extension entry's `superseded` and `rebuilt` outcomes, the position's walk over manifests of its own epoch that another process published and its drop at a manifest that folds past its tip in [Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal), the HEAD walk before a compaction unit plans, and the dead fence's second fence in [Writer ownership](../specs/format.md#61-writer-ownership). Applying a change to the current manifest stays, because every lane item does it.
- **The idle fold trigger**, timed from the newest commit, which the fold age replaces.
- **The server modes in which a process maintains namespaces it doesn't write**: `maintain_only`, the scheduler of `serve_only` and `disabled` that leaves folds to another process, and a grep host that maintains the index of namespaces that another server writes. The one setting `writer_builds` replaces them, beside the choices to serve the maintenance API group and to sweep the store.
- **The snapshot's shared mode of the fold lock**, and a deletion that waits for the fold lock while it keeps the lane.
- **Tests of rival publishers**: the fold race tests, the cross-process fold, compaction and advance tests, and the floor object tests.

## Changes to the specs, guides and code

The storage format changes in these sections:

- [Objects and references](../specs/format.md#2-objects-and-references), [Writer ownership](../specs/format.md#61-writer-ownership), [Publication and group commit](../specs/format.md#63-publication-and-group-commit) and [Failed and unknown outcomes](../specs/format.md#64-failed-and-unknown-outcomes): a WAL object contains commits or a fence, the writer publishes every manifest, every manifest publication checks the epoch, and the writer releases its epoch around every put.
- [Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal): a view's floor is its manifest's, and a position's successor means a claim.
- [WAL records](../specs/format.md#a5-wal-records): a WAL object has no floor field. Because a decoder rejects a field it doesn't know ([Strict decoding and evolution](../specs/format.md#123-strict-decoding-and-evolution)), a build with this design returns `namespace_corrupt` when it reads a floor object in a namespace's tail.
- [Publishing a manifest](../specs/format.md#72-publishing-a-manifest), [Recovery material and maintenance policy](../specs/format.md#73-recovery-material-and-maintenance-policy), and the folds in [Creating and verifying a snapshot](../specs/format.md#82-creating-and-verifying-a-snapshot), [Forking a namespace](../specs/format.md#92-forking-a-namespace), [Deleting a namespace](../specs/format.md#94-deleting-a-namespace) and [Cross-namespace copies and moves](../specs/format.md#97-cross-namespace-copies-and-moves): every fold is the writer's, the fold age, and the two folds of an advance and a deletion.
- [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor) and [Streaming compaction](../specs/format.md#104-streaming-compaction): the advance in the lane, the invariant's argument, and build and install.
- [Garbage collection](../specs/format.md#11-garbage-collection) and [Retirement](../specs/format.md#95-retirement): fences in place of fences and floor objects. [Alternatives considered](../specs/format.md#13-alternatives-considered) adds the alternatives below.

The API changes in [Writer topology and fencing](../specs/api.md#53-writer-topology-and-fencing), [Maintenance jobs](../specs/api.md#maintenance-jobs) (jobs run as the writer, and the `install` kind), [Retention jobs](../specs/api.md#retention-jobs), [Deleting, retaining, and reclaiming](../specs/api.md#deleting-retaining-and-reclaiming), the hosted modes in [API groups](../specs/api.md#1-api-groups), and the operations table, which adds release. Writer topology and fencing states that writes and maintenance calls for one namespace go to one server at a time, and that two servers that both receive them displace each other at the cost in [Costs](#costs). The glossary removes "Floor object" and "Maintainer", adds "Publication lane", and changes the entries that mention either. In the self-hosting guide's configuration, the `maintenance` modes and the maintaining half of `[grep].mode` are replaced by `writer_builds` (default `true`) and two choices (serve the maintenance API group, and sweep the store), `idle_fold_after_ms` becomes the required fold age, `writer_id` becomes a prefix of a generated ID, and the guide's floor object and several-server passages and the `rebuilt` and `superseded` grep publication outcomes go.

This note replaces [Maintenance roles](maintenance-roles.md) and [Retention advance in the WAL](retention-advance-in-the-wal.md). These notes lose their floor object, maintainer, idle fold and rival publication passages: [Keeping fences and floor objects](kept-fences-and-floors.md), [Retention window](retention-window.md#who-advances-the-floor), [Maintenance hosting](maintenance-hosting.md), [Metadata streaming compaction](metadata-streaming-compaction.md#publication-and-restart), [Content search index](content-search-index.md), [Write-stop wait](write-stop-wait.md#the-rule), [Forks and merges from the tail](forks-and-merges-from-the-tail.md), [The manifest boundary](manifest-boundary.md), [Read freshness](read-freshness.md#the-writers-session-and-its-position), and [The WAL implementation](wal-boundary.md).

In the code, what goes is listed above. What comes is the epoch check in the one manifest publication function, maintenance calls on the writable namespace, the lane's manifest items, the segment and age check in that function, the two folds of an advance and a deletion, the required fold age from the oldest unfolded commit, the release around every put, the release route, the `install` job with the build result and the worker's build call, the retry after `writer_fenced` with the compaction cancel on a fence, and unique writer IDs. The retention tests advance the floor through the test's own writer, whose runtime has an adjustable clock, because an advance through a second runtime would claim the namespace and fence the test's writer.

## Alternatives considered

- **A writer plus one separately claimed compactor.** A second epoch removes rival compactions, but it needs its own claim and fence and a rule for a compaction that runs while the writer folds, and the floor advance still has to be ordered against commits, which only the writer's lane does.
- **One fold of the whole tail under the lane.** An advance or a deletion would keep the lane while it folds the whole tail, which saves one manifest, but commits would wait for that whole fold (2.7 s at p50 for 256 WAL objects of small inline files in `floor-advance-contention-20261126`).
- **A marker in the WAL from another process.** A floor object orders an advance against restores without the epoch, but it loses the next WAL number to a busy writer, has to be kept and checked after its put, and breaks the fence's check as [The problem](#the-problem) shows.
- **Asking the writer through an address in the manifest.** An address can go stale, so a process needs a timer or a liveness check to know when to stop asking and claim, and changing the address is another publication.
- **A lease.** A process would be the writer until a lease expires, renewing it with a write every few seconds. That costs a write per period per namespace, needs a rule for work that outlives the lease, and makes the writer depend on clocks, while a paused writer still needs the epoch to fence it.
