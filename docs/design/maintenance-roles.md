# Maintenance roles

This note describes which processes can publish a namespace's manifests and delete its objects, and what fences a rival. A namespace has two roles:

- The "writer" commits, and deletes the namespace, under the [writer epoch](../specs/glossary.md#writer-epoch).
- The "maintainer" folds, compacts, advances the retention floor, and collects garbage, under the [compactor epoch](../specs/glossary.md#compactor-epoch).

Pins, forks, and namespace creation belong to no role. Every other publication is made by one role, and the manifest's numbered create and the role's epoch fence a rival.

## The problem

A manifest is created at its predecessor's number plus one with put-if-absent, so two publishers can't both create the same number. The manifest records the writer epoch and the compactor epoch, so a publisher can check whether it still has the role it acts under. Compaction uses both mechanisms. A runtime claims the compactor epoch before its first compaction, every compaction publication has to match the current epoch, and a newer claim stops an older compactor at its next check.

Folds don't claim an epoch. Any process can run a fold, so the format has a rule for a fold whose put loses to another fold of the same tail (the covered check), a rule for a fold whose put loses to a manifest that changed something else (the rebuild, with the list of fields that a rebuild can ignore), and a rule that binds a writer session's own fold to the writer epoch, so that the session's position can't move onto a manifest after another writer's claim. A retention advance publishes under no epoch either. A pin from the current head folds the observed tail first, from any process, with no epoch.

So the runtime has two folds that can run on one namespace at once: the session's fold of the tail it retained, and the explicit fold that a maintenance call or the host's daily pass runs. The lab's workloads run both, and a race between them drops the session's read position, because a read's check finds the explicit fold's manifest before the session knows of it. The host's maintenance tick also has a cost when two hosts maintain one namespace. An unfinished metadata visit that finds another process's compactor claim retries on an interval, at six requests per retry.

## Two roles

| Operation | Role | Epoch it publishes under | What fences a rival |
| --- | --- | --- | --- |
| Commit (a WAL publication) | Writer | Writer epoch | The claim manifest and the WAL fence of a newer writer |
| Namespace deletion | Writer | Writer epoch | The same |
| Compactor claim | Maintainer | Compactor epoch plus one | The numbered create |
| Fold | Maintainer | Compactor epoch | A newer claim, found at the next check |
| Bounded and streaming compaction | Maintainer | Compactor epoch | A newer claim, found at the next check |
| Retention advance | Maintainer | Compactor epoch | A newer claim, found at the next check |
| Garbage collection | Maintainer | None (it publishes nothing) | Nothing. Concurrent collectors are safe under the age rules, and an object that's already gone needs no cleanup |
| Pin creation and deletion | None | None (a pin is its own record, and the fold before a pin is the maintainer's) | Put-if-absent on the pin, and the verification against the current manifest |
| Fork | None | None for the source pin, and the writer epoch of the new namespace's first manifest | The source pin's verification, and the first manifest's create |
| Namespace creation | None | The writer epoch of manifest 1 | The create of manifest 1 |

A process can have both roles for a namespace. A host with an open writer session has the writer role, and it claims the compactor epoch before its first fold of the namespace, so it maintains the namespace as the session publishes. A maintenance runtime without a writer session has the maintainer role for the namespaces it visits. A process that creates a pin from the current head claims the compactor epoch for the pin's fold when it has no claim.

If a publication's numbered create loses, the publisher reloads the current manifest. If that manifest's epoch for the publication's role is higher than the publisher's, another process took the role, and the publication stops with `fenced`. Otherwise, the publisher rebuilds against the new predecessor and tries the next number. Every operation handles a lost create this way.

Why does the writer do nothing but commit and delete? The writer epoch exists to fence commits. A new writer publishes a claim manifest and a WAL fence, and the old session's next commit finds them. A fold, a compaction, or a retention advance changes no commit, so it has no reason to use the writer epoch, and a process that only maintains a namespace has no reason to take it. Deletion is the exception, because it ends commits, and only the writer epoch fences a commit.

Why two epochs and not one? The two roles have different lifetimes and can belong to different processes. A writer session lasts as long as the application that commits through it, and a maintainer visit lasts one fold, one compaction, or one pass. A deployment runs the two roles in different processes when its operators want to keep maintenance's memory and CPU away from the writer's, and in one process when they don't. With one epoch, every maintenance visit would fence the writer.

## Folding as the maintainer

A [fold](../specs/glossary.md#fold) moves the WAL objects after the current manifest's folded boundary into metadata segments, and publishes the next manifest under the compactor epoch, with the writer epoch copied from its predecessor. The writer's host folds its own sessions' tails as the maintainer, after a commit that reaches the fold thresholds and when a tail reaches the idle fold age. A maintenance runtime folds a tail that no host is maintaining, such as a crashed writer's, under its own claim, with no claim on the writer role and no fence on a writer. A checkpoint or a snapshot from any process folds the tail the same way, and then pins the resulting manifest.

A runtime folds a namespace one fold at a time, so the session's fold and a maintenance call's fold in the same process take turns, and two folds of one tail never run under one claim at once. Between runtimes, a fold whose create loses acts on the manifest it finds:

- **A newer compactor epoch.** The fold is fenced, whoever published the manifest (including another runtime's claim), and the runtime claims again before its next fold.
- **The fold's own compactor epoch.** The manifest is this runtime's own compaction or retention advance at the same head, which changes nothing that replay reads, so the fold rebuilds against it and keeps the tail it read.
- **A new writer's claim.** The fold reloads, and folds the new writer's log.
- **A namespace tombstone.** The fold stops.

The covered check goes, because no other fold of the same tail runs under the same claim. The rule that recognizes a publication's own bytes after an unknown outcome stays, for every publication.

A writer session's [position](../specs/glossary.md#writer-position) moves onto a fold's manifest as it moves onto any manifest under the session's writer epoch, whether this process or another published it, and never moves onto a manifest with a newer writer epoch. So a fold needs no binding to the writer epoch to keep the old writer's reads off a new writer's log, because the position's epoch rule already keeps them off, and the session finds that it's fenced at its next commit.

Why not keep folds on the writer, under the writer epoch? A tail left by a crashed writer, or by a host that stopped maintaining it, could then be folded only by a process that claims the writer role. That claim fences a session that might be idle rather than dead, and costs an application a failed commit when the application returns. A maintainer that folds under its own claim leaves the writer alone. Also, the format already lets any process fold, so this design adds a fence without changing which processes can fold.

Why not let any process fold with no epoch, as the format allows now? A fold that any process can run without a fence is a fold that two processes do run on one tail, and the covered check and the rebuild rules exist to resolve that pair. A fold under the compactor epoch has the fence that compaction has, and the same rule for a lost create.

## The maintainer

The maintainer claims the compactor epoch once per namespace per runtime, and publishes every fold, compaction, and retention advance under it. Like compaction, it checks the epoch before it reads a segment and before each publication attempt. Under the epoch, a retention advance gains the fence that compaction has, so a newer claim stops an older maintainer's advance at its next check, and two maintainers can't both publish advances against the same predecessor. A maintainer compacts whether or not an unfolded tail exists above the runs, because compaction reads the manifest's segments and copies the folded boundary forward, and a retention advance stops at the manifest's head, as both do now.

Garbage collection publishes nothing and claims nothing. A pass establishes its roots from the current manifest, the upload sessions, the WAL tail, and the complete pin listing, and deletes each object that no root references once the object is older than the grace. Two passes over one namespace delete the same objects, and the second finds them gone. The maintainer runs garbage collection because it runs the other passes, not because a fence requires it.

A host that has both roles for a namespace folds and compacts under its own claim. A separate maintenance host that visits the same namespace claims the compactor epoch when it folds or compacts, which stops the writer host's next fold or compaction at its check, and the writer host claims again when it next needs the epoch. The host's daily pass visits every listed namespace, but a visit folds a tail only when a fold is due (at the fold thresholds or the idle fold age, as on a tick), so a host that maintains its own sessions rarely meets a rival claim on a tail. Two hosts that maintain one namespace alternate claims, and each lost claim costs the loser one fold or one compaction.

## Pins and forks

A [pin](../specs/glossary.md#pin) is a record under the namespace's pins prefix, created with put-if-absent and verified against the current manifest after the write. A pin belongs to no role, and the verification is its fence. A pin whose manifest stopped being current between the write and the verification is deleted and retried from a fresh basis. The fold before a pin from the current head is a maintainer's fold, under the process's claim. A fork is a pin on the source plus the first manifest of the new namespace, which the new namespace's writer epoch covers. Deleting a namespace is a fold followed by a tombstone. The fold is the maintainer's, under the deleting process's claim, and the tombstone is the writer's, because it ends commits.

## Costs

| Operation | Today | With this design |
| --- | ---: | ---: |
| A host's first fold of a namespace | one manifest put | one claim manifest, then the manifest put (the claim moves from the host's first compaction to its first fold) |
| A session's later folds | the same manifest put and hint raise | the same |
| A fold of a crashed writer's tail by a maintenance pass | one manifest put | one claim manifest if the runtime has none for the namespace, then the manifest put |
| A checkpoint from any process | a fold and a pin | the same, under the process's claim |
| A retention advance | one manifest put | one manifest put, under the compactor epoch, plus a claim the first time in a runtime |
| A namespace deletion | a fold and a tombstone | one claim manifest if the process has no claim for the namespace, then the fold and the tombstone |
| Two folds of one tail at once | the covered check, or a rebuild | one is fenced |
| A maintenance visit that finds another host's compactor claim | six requests per retry interval | the same |

## What the format and API specs change

- [Publishing a materialized file set](../specs/format.md#72-publishing-a-materialized-file-set): a fold publishes under the compactor epoch, and the covered check goes. The rebuild's list of fields that a winning manifest can change loses the compactor epoch, because a changed compactor epoch now means the fold is fenced. The writer position's epoch rule in [Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal) replaces the paragraph that binds a session's fold to the writer epoch.
- [Creating and verifying a pin](../specs/format.md#82-creating-and-verifying-a-pin): the fold before a pin is a maintainer's fold.
- [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor): the advance publishes under the compactor epoch.
- [Deleting a namespace](../specs/format.md#94-deleting-a-namespace): the fold before the tombstone is the maintainer's fold, under the deleting process's compactor claim, and the tombstone publishes under the writer epoch as it does now.
- [Streaming compaction](../specs/format.md#104-streaming-compaction): a runtime claims the compactor epoch before its first fold, compaction, or retention advance of the namespace.

The API's maintenance calls say which role each runs under, and in the [maintenance hosting](maintenance-hosting.md) note, the tick and the daily pass fold under the host's claim.

## Alternatives considered

Keep folds open to any process with no epoch, and keep the covered check and the rebuild rules. This is the format today, and a race between the session's fold and the explicit fold drops the read position.

Put folds on the writer, under the writer epoch, with a maintenance pass claiming the writer role to fold a crashed writer's tail. This keeps each fold with its log, but either a pass fences an idle session, which costs the application a failed commit when it returns, or the tail stays unfolded until a writer opens the namespace, as SlateDB's compactor does.

Add a third epoch for folds, claimed by whichever process folds. It needs a claim, a fence, and a rule for a compaction that runs while another process has the fold epoch, and the compactor epoch already has all three once folds publish under it.

Put a lease with a heartbeat on the maintainer role, like the heartbeats SlateDB's compaction workers write every ten seconds to their coordinator's state object. With a lease, a second maintainer stays away before it does any work, and with the epoch, it stops after its first publication loses. A lease costs a write per heartbeat per namespace, and needs a rule for a lease that expires while a job runs. LoonFS bounds each job by its budget instead, and a lost claim costs the job its work but no data.

Run one maintainer process per deployment, set by configuration. This removes the rival claim for the namespaces that no writer host maintains, but it's a deployment choice that the format can't enforce, so the epoch stays as the rule, and the deployment choice stays available.
