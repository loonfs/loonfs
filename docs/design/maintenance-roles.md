# Maintenance roles

This note describes who may publish a namespace's manifests and delete its objects, and what fences a rival. A namespace has two roles. The writer commits and folds, under the writer epoch. The maintainer compacts, advances the retention floor, and collects garbage, under the compactor epoch. Each manifest publication is made by one role, and the manifest's numbered create plus the role's epoch is the whole fence.

## The problem

A manifest is created at its predecessor's number plus one with put-if-absent, so two publishers can't both land the same number, and the manifest records the writer epoch and the compactor epoch, so a publication can tell whether it still has the role it acts under. That is a complete fencing protocol, and compaction uses all of it: a runtime claims the compactor epoch before its first compaction, every compaction publication has to match the current epoch, and a newer claim stops an older compactor at its next check.

Folds don't use it. A fold needs no epoch and any process may run one, so the format has a rule for a fold whose put loses to another fold of the same tail (the covered check), a rule for a fold whose put loses to a manifest that changed something else (the rebuild, with the list of fields a rebuild may ignore), and a rule that binds a writer session's own fold to the writer epoch so that the session's position can't move onto a manifest after another writer's claim. A retention advance publishes under no epoch either. A pin from the current head folds the observed tail first, so a pin is also a fold, from any process.

The runtime then has two folds that can run on one namespace at once: the session's fold of the tail it retained, and the explicit fold that a maintenance call or the host's daily pass runs. The lab's workloads run both, and the race between them is where the session's read position was lost, because a read's check found the explicit fold's manifest before the session knew of it. The host's maintenance tick has a cost of its own when two hosts maintain one namespace: an unfinished metadata visit that finds another process's compactor claim retries on an interval, six requests per retry.

## Two roles

| Operation | Role | Epoch it publishes under | What fences a rival |
| --- | --- | --- | --- |
| Commit (a WAL publication) | Writer | Writer epoch | The claim manifest and the WAL fence of a newer writer |
| Fold | Writer | Writer epoch | The same |
| Namespace deletion | Writer | Writer epoch | The same |
| Compactor claim | Maintainer | Compactor epoch (its own, plus one) | The numbered create |
| Bounded and streaming compaction | Maintainer | Compactor epoch | A newer claim, found at the next check |
| Retention advance | Maintainer | Compactor epoch | A newer claim, found at the next check |
| Garbage collection | Maintainer | None, it publishes nothing | Nothing. Concurrent collectors are safe by the age rules, and an object that's already gone needs no cleanup |
| Pin creation and deletion | None | None, a pin is its own record | Put-if-absent on the pin, and the verification against the current manifest |
| Fork | None | None for the source pin, and the writer epoch of the new namespace's first manifest | The source pin's verification, and the first manifest's create |
| Namespace creation | None | The writer epoch of manifest 1 | The create of manifest 1 |

A process can have both roles for a namespace. A host with an open writer session has the writer role for it, and it claims the compactor epoch when its first compaction of the namespace is due, so a session maintains its own namespace as it publishes. A maintenance runtime without a writer session has the maintainer role for the namespaces it visits.

A publication whose numbered create loses reloads the current manifest. If the manifest's epoch for the publication's role is higher than the one it has, another process took the role, and the publication stops with `fenced`. Otherwise the publication rebuilds against the new predecessor and tries the next number. That is the one rule for a lost create, for every operation.

Why two epochs and not one? Because the two roles have different lifetimes and different holders. A writer session lives as long as the application that commits through it, and a maintainer visit lives for one compaction or one pass. A deployment runs them in different processes when it wants to keep compaction's memory and CPU away from the writer's, and in one process when it doesn't. One epoch would make every maintenance visit fence the writer.

## Folds belong to the writer

A fold moves the WAL objects after the current manifest's folded boundary into metadata segments and publishes the next manifest. Only the writer session folds, under its writer epoch, whether it folds the tail it retained or reads the tail from the store after it dropped its position. A fold's lost create then has two causes. A maintainer's manifest that landed first, which the rebuild keeps the tail across, because a compaction, a compactor claim, or a retention advance changes nothing that replay reads. Or a newer writer's claim, which fences the session. The rule that treats another fold's manifest as covering this fold goes, because no other fold of the same tail exists. The rule that recognizes a publication's own bytes after an unknown outcome stays, for every publication.

The session folds at its thresholds, after a commit that crosses the fold size, and once its tail has sat at the idle fold age. A maintenance call that asks for a fold in the writer's process runs the session's fold. A checkpoint or a snapshot from the writer's process folds through the session, then pins the manifest it published.

A process without the writer session that needs a tail folded claims the writer role first. The claim is the ordinary writer acquisition: a manifest with the writer epoch plus one and a WAL fence, which fences whatever session held the epoch before. The process then folds as the writer, and its session closes. The next application writer claims the epoch after it, as it would after any other session. So a crashed writer's tail is folded the way a new writer would fold it, and a maintenance pass over a namespace that nobody writes to folds it by becoming its writer for one fold.

A host folds its own sessions' idle tails through the session, at the idle fold age, so a pass from another host finds an idle unfolded tail only where no host maintains the namespace. The pass folds a tail only when the tail's newest object is older than the idle-session close age, which is the age at which a host closes its own idle sessions. A tail younger than that belongs to a session that is still writing or about to, and the pass leaves it. A fold through a claim costs two puts more than a fold through the session, the claim manifest and the WAL fence, and a namespace whose host maintains it never pays it.

A checkpoint or a snapshot from a process without the writer session pins the current manifest, at the head the last fold reached. It folds nothing, because folding is the writer's. The API says which head a pin from another process gets.

Why not let any process fold, as today, and keep the covered check? Because a fold that anyone may run is a fold that two processes do run, and every rule that resolves the pair exists for that. A tail that only its writer folds has one folder at a time by construction, and the session's reads, the session's position, and the session's fold all agree on the tail without a rule between them.

Why claim the writer role for a maintainer's fold instead of a third epoch? Because a fold is a write of the writer's log, and the format already has the protocol for a process that takes over a namespace's log. A third epoch would need its own claim, its own fence, and a rule for a writer that returns while a maintainer's fold runs, which the writer epoch's claim and fence already settle.

## The maintainer

The maintainer claims the compactor epoch once per namespace per runtime, publishes every compaction and every retention advance under it, and checks it before it reads a segment and before each publication attempt, as compaction does. A retention advance under the epoch gains the fence that compaction has: a newer claim stops an older maintainer's advance at its next check, and two maintainers can't both publish advances against the same predecessor. A maintainer compacts whether or not a tail stands above the runs, because compaction reads the manifest's segments and copies the folded boundary forward, and a retention advance stops at the manifest's head, as both do today.

Garbage collection publishes nothing and claims nothing. A pass establishes its roots from the current manifest, the upload sessions, the WAL tail, and the complete pin listing, and it deletes what no root references once the object is older than the grace. Two passes over one namespace delete the same objects, and the second finds them gone. The maintainer runs it because it runs the other passes, not because a fence requires it.

A host that has both roles for a namespace runs its compactions after its folds, under its own claim. A separate maintenance host that visits the same namespace claims the compactor epoch when it compacts, which stops the writer host's next compaction at its check, and the writer host claims again when it next compacts. The later claim wins, and the earlier holder stops at its next check. A deployment that wants one maintainer per namespace runs one maintenance host, or none, and lets the writer hosts maintain their own sessions.

## Pins and forks

A pin is a record under the namespace's pins prefix, created with put-if-absent and verified against the current manifest after the write. It belongs to no role, and the verification is its fence: a pin whose manifest stopped being current between the write and the verification is deleted and retried from a fresh basis. A fork is a pin on the source plus the first manifest of the new namespace, which the new namespace's writer epoch covers. Deleting a namespace is a fold followed by a tombstone, under the writer epoch, so it belongs to the writer.

## Costs

| Operation | Today | With this design |
| --- | ---: | ---: |
| A session's fold at a threshold | the same manifest put and hint raise | the same |
| A fold of a crashed writer's tail by a maintenance pass | one manifest put | one claim manifest, one WAL fence, one manifest put |
| A checkpoint from the writer's process | a fold and a pin | the same, through the session |
| A checkpoint from another process | a fold and a pin | a pin, at the last fold's head |
| A retention advance | one manifest put | one manifest put, under the compactor epoch, plus a claim the first time in a runtime |
| Two folds of one tail at once | the covered check, or a rebuild | doesn't happen |
| A maintenance visit that finds another host's compactor claim | six requests per retry interval | the same |

## What the format and API specs change

In [Publishing a materialized file set](../specs/format.md#72-publishing-a-materialized-file-set), a fold is published under the writer epoch by the writer session, the covered check goes, and the rebuild keeps its list of the fields a maintainer's manifest may change. In [Creating and verifying a pin](../specs/format.md#82-creating-and-verifying-a-pin), a pin from the current head folds through the writer session when the process has it and pins the current manifest otherwise. In [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor), the advance publishes under the compactor epoch. The API's maintenance calls say which role each runs under, and the checkpoint and snapshot calls say which head a pin from a process without the writer session gets. The [maintenance hosting](maintenance-hosting.md) note's daily pass folds through a writer claim, with the idle age above.

## Alternatives considered

Keep folds open to any process and keep the covered check and the rebuild rules. This is the format today, and the race between the session's fold and the explicit fold is where the read position was lost.

A third epoch for folds, taken by whichever process folds. It needs a claim, a fence, and a rule for a writer that commits while a folder has the epoch, all of which the writer epoch already has.

A lease on the maintainer role, with a heartbeat, as SlateDB's compaction workers write every ten seconds to their coordinator's state object. It tells a second maintainer to stay away before it does any work, where the epoch tells it after its first publication loses. The cost is a write per heartbeat per namespace, and a rule for a lease that expires under a running job. LoonFS's compaction jobs are bounded by their budgets instead, and a lost claim costs the job, not the data.

A maintainer that never folds, which is SlateDB's division: its compactor never touches the write-ahead log, and a crashed writer's log waits for the next writer to replay it. It needs no claim for a pass, and it leaves a crashed writer's tail unfolded until a writer opens the namespace, so every cold read replays it, garbage collection keeps its WAL objects, and the retention floor and base merges stop at the manifest's head. A fold through a claim costs two puts once and ends that.

One maintainer process per deployment, by configuration. It removes the second claim, and it is a deployment choice that the format can't enforce, so the epoch stays as the rule and the deployment choice stays available.
