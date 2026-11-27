# Maintenance roles

This note describes which processes can publish a namespace's manifests and delete its objects, and what fences a rival. A namespace has two roles:

- The "writer" commits and deletes the namespace, under the [writer epoch](../specs/glossary.md#writer-epoch).
- The "maintainer" folds, compacts, publishes an extension's entry, advances the retention floor, sets the retention window, and collects garbage.

Snapshots, forks, and namespace creation belong to no role. Every other publication is made by one role. The numbered create fences every publication, and a compaction also stops when its input segments change.

## The problem

A manifest is created at its predecessor's number plus one with put-if-absent, so two publishers can't both create the same number. The manifest records the writer epoch, so a writer can check whether it still has its role. A compaction also checks, before each publication attempt, that the manifest it publishes over still lists the segments it compacted, so two compactions of the same runs can't both publish.

Folds start in several places. A writer's host folds its session's tail after a commit that reaches the fold thresholds or the [fold age](../specs/glossary.md#fold-age), a `metadata` maintenance job or the host's sweep folds the tail when a fold is due, a call to `Maintenance::fold_wal` folds the tail whatever its size, and a snapshot from the current head or a namespace deletion folds the tail first. Two folds of one namespace can run at once in two processes. If a session's fold were bound to the writer epoch and the other folds weren't, LoonFS would have two kinds of fold that follow different rules. The binding would exist to keep the session's position off a manifest published after another writer's claim, and the writer position's epoch rule (the position never moves onto a manifest with a newer writer epoch) already does that job.

## Two roles

| Operation | Role | Epoch it publishes under | What fences a rival |
| --- | --- | --- | --- |
| Commit (a WAL publication) | Writer | Writer epoch | The claim manifest and the WAL fence of a newer writer |
| Namespace deletion | Writer | Writer epoch, for the tombstone (the fold before it is the maintainer's). An initializing namespace has no writer, so its tombstone is published without a writer session. | The same, and for an initializing namespace, the numbered create of the tombstone |
| Compaction | Maintainer | None (the manifest copies its predecessor's writer epoch) | The numbered create, and a change to the compaction's input segments after its basis, which ends the compaction without a manifest |
| Fold | Maintainer | None (the manifest copies its predecessor's writer epoch) | The numbered create |
| Extension publication | Maintainer | None (the manifest copies its predecessor's writer epoch) | The numbered create, and a change to the extension's entry after the publication's basis, which ends the publication without a manifest |
| Retention advance | Maintainer | The current writer epoch, copied without a claim into a "[floor object](../specs/glossary.md#floor-object)", a WAL object with no commits that records the new floor (the fold after the floor object is an ordinary fold) | The numbered create |
| Retention window change | Maintainer | None (the manifest copies its predecessor's writer epoch) | The numbered create |
| Garbage collection | Maintainer | None (it publishes nothing) | Nothing. Concurrent collectors are safe under the age rules, and an object that's already gone needs no cleanup |
| Pool collection | Maintainer | None (it publishes nothing) | Nothing, for the same reasons as a namespace's garbage collection |
| Snapshot creation and deletion | None | None (a snapshot is its own record, and the fold before a snapshot is the maintainer's) | Put-if-absent on the snapshot, and the verification against the current manifest |
| Fork | None | None for the snapshots, and the writer epoch of the new namespace's first manifest, which its second manifest copies | The fork snapshot's verification, the first manifest's create, and the numbered create of the second manifest |
| Namespace creation | None | The writer epoch of manifest 1, which manifest 2 copies | The create of manifest 1, and the numbered create of manifest 2 |

A process can have both roles for a namespace. A host with an open writer session has the writer role, and it maintains the namespace as the session publishes. A maintenance runtime without a writer session has the maintainer role for the namespaces it maintains. No maintainer publication claims an epoch.

If a publication's numbered create loses, the publisher reloads the current manifest. For a commit, a higher writer epoch means that another process took the role, so the commit stops and reports that it was fenced. A compaction whose input segments the new predecessor no longer lists stops and reports that it was abandoned. Otherwise, the publisher rebuilds against the new predecessor and tries the next number. A fold finds its work covered, or rebuilds. A retention advance whose floor object loses its WAL number runs discovery again and creates the floor object at the next number, with the writer epoch that the new discovery finds.

Why does the writer commit and delete, and nothing else? The writer epoch exists to fence commits. A new writer publishes a claim manifest and a WAL fence, and the old session's next commit finds them. A fold or a compaction changes no commit, so it has no reason to use the writer epoch, and a process that only folds and compacts a namespace has no reason to take it. Deletion is the exception, because deletion ends commits, so the tombstone has to be ordered against the writer's commits, and only the writer epoch orders a manifest against commits. A retention advance ends the restores of the deletions that it expires, so it has to be ordered against the writer's commits too. The advance writes the new floor to the WAL as a floor object at the next WAL number, so the WAL orders the advance against every restore without the writer epoch (see [Advancing the retention floor](../specs/format.md#101-advancing-the-retention-floor) and [Who advances the floor](retention-window.md#who-advances-the-floor)).

## Folding as the maintainer

A [fold](../specs/glossary.md#fold) moves the WAL objects after the current manifest's folded boundary into metadata segments, and publishes the next manifest as the maintainer's publication, with the writer epoch copied from its predecessor. The writer's host folds its own sessions' tails as the maintainer, after a commit that reaches the fold thresholds and when a tail reaches the fold age. A maintenance runtime folds a tail that no host is maintaining, such as a crashed writer's, with no claim on the writer role and no fence on a writer. A snapshot request from any process folds the tail the same way, and then takes a snapshot of the resulting manifest.

Within one runtime, every fold of a namespace goes through the namespace's fold path ([Folding and compacting a namespace](maintenance-hosting.md#folding-and-compacting-a-namespace)): the session's own folds, a maintenance call's fold, the fold of the fork in a merge or a rebase, the fold before a snapshot or a deletion, and the fold after a retention advance's floor object. Each fold takes the namespace's fold lock, an in-memory read-write lock, in exclusive mode, so one fold at a time has the lock. LoonFS checks whether the fold is due after the fold has the lock. So if a fold waited for another fold, the check finds the tail already folded, and the second fold does nothing. A request that needs a folded manifest uses any fold in the same runtime that took its input after the request arrived. A snapshot keeps the fold lock in shared mode until its verification, so other snapshots can have the lock at the same time, and a fold in the same runtime waits for the snapshot. The fold reads the tail from the writer session's position when the runtime has a session with a position, and from the store otherwise.

A fold in another process can still run at the same time. Both folds try to create the same manifest number with put-if-absent, so one create succeeds and the other fails. A fold whose create loses reloads the current manifest and rebuilds against it, and the rebuild has four outcomes:

- **The tail is already folded.** The current manifest's folded boundary is at or past the fold's tip, and its head and number are at or past the fold's, so there's nothing to publish.
- **The winner is at the same head.** The winner is a compaction or a retention window change, so the rebuild keeps the tail it read.
- **The winner is a new writer's claim.** The rebuild folds the new writer's log.
- **The winner is a namespace tombstone.** The fold stops.

A fold resolves a put whose outcome is unknown the same way every publication does, by checking whether the manifest at its number is its own.

A writer session's [position](../specs/glossary.md#writer-position) moves onto a fold's manifest as it moves onto any manifest under the session's writer epoch, whether this process or another published it, and never moves onto a manifest with a newer writer epoch. So a fold needs no binding to the writer epoch to keep the old writer's reads off a new writer's log, because the position's epoch rule already keeps them off, and the session finds that it's fenced at its next commit.

Why isn't a fold the writer's, under the writer epoch? A tail left by a crashed writer, or by a host that stopped maintaining it, could then be folded only by a process that claims the writer role. That claim fences a session that might be idle rather than dead, and costs an application a failed commit when the application returns. A maintainer's fold leaves the writer alone, and the format lets any process fold.

## The maintainer

A compaction checks, before each publication attempt, that the manifest it publishes over still lists the input segments that it compacted. If two processes compact the same runs at once, the first publication replaces those runs, and the other compaction stops as abandoned, so it loses that one unit of work and no data. A fold and a compaction of one namespace can run at the same time. A compaction's finalization that loses its create to a fold retries over the fold's manifest, because a fold leaves the compaction's inputs in place, and a fold that loses its create to a compaction rebuilds and keeps its tail. A retention advance publishes no manifest. Its floor object copies the current writer epoch, and the fold after the floor object publishes the new floor. A maintainer compacts whether or not an unfolded tail exists above the runs, because compaction reads the manifest's segments and copies the folded boundary forward, and a retention advance stops at the manifest's folded head.

Garbage collection publishes nothing and claims nothing. A collection establishes its roots from the current manifest, the upload sessions, the WAL tail, and the complete snapshot listing, and deletes each object that no root references once the object is older than the grace window. Two collections over one namespace delete the same objects, and the second finds them gone. The maintainer runs garbage collection because it runs the other maintenance steps, not because a fence requires it.

A host that has both roles for a namespace folds and compacts. A separate maintenance host that maintains the same namespace folds and compacts it too, and the two hosts' folds and compactions coexist. The host's sweep runs the steps of every listed namespace, but its metadata step folds a tail only when a fold is due (at the fold thresholds or the fold age, as for the step that runs every maintenance interval; without a writer session in this runtime, the check uses the object count and the fold age). When the two hosts compact the same runs at once, the host whose publication loses abandons that unit.

## Snapshots and forks

A [snapshot](../specs/glossary.md#snapshot) is a record under the namespace's snapshots prefix, created with put-if-absent and verified against the current manifest after the write. A snapshot belongs to no role, and the verification is its fence. A snapshot whose manifest stopped being current between the write and the verification is deleted and retried from a fresh basis. The fold before a snapshot from the current head is a maintainer's fold, with no claim. A fork is a fork snapshot on the source plus the first manifest of the new namespace, which the new namespace's writer epoch covers. Deleting a namespace is a fold followed by a tombstone. The fold is the maintainer's, with no claim, and the tombstone is the writer's, because it ends commits.

## Costs

Folds, compactions, snapshots, and retention advances claim no epoch, so none of them publishes a manifest besides its own result. (A retention advance writes one floor object, and then folds the namespace when no manifest records the new floor.) A writer session's position doesn't show another runtime's publications, so before a compaction unit that's due plans from the position, the runtime sends one HEAD for the manifest after the position's manifest and reads the manifest boundary. When another runtime has published a compaction since, the unit plans from the newest manifest instead of rewriting runs that were already replaced. Two hosts that compact one namespace at once each lose at most the units whose inputs the other replaced.

## What the format and API specs say

- [Publishing a manifest](../specs/format.md#72-publishing-a-manifest) describes a fold as the maintainer's publication, with the covered check and the rebuild. The writer position's epoch rule in [Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal) keeps an old writer's position off a new writer's log.
- [Creating and verifying a snapshot](../specs/format.md#82-creating-and-verifying-a-snapshot) describes the fold before a snapshot as a maintainer's fold.
- [Deleting a namespace](../specs/format.md#94-deleting-a-namespace) describes the fold before the tombstone as a maintainer's fold, and the tombstone as a publication under the writer epoch.

In the API specification, the maintenance jobs say which role each runs under, and the glossary defines the [maintainer](../specs/glossary.md#maintainer).

## Alternatives considered

Bind a writer session's fold to the writer epoch, and leave the explicit fold and the snapshot's fold unbound. This keeps three kinds of fold, and a binding rule whose job the position's epoch rule already does.

Put folds on the writer, under the writer epoch, with a maintenance step claiming the writer role to fold a crashed writer's tail. This keeps each fold with its log, but either the step fences an idle session, which costs the application a failed commit when it returns, or the tail stays unfolded until a writer opens the namespace again.

Add a second epoch for folds, claimed by whichever process folds. It needs a claim, a fence, and a rule for a compaction that runs while another process has the fold epoch, and folds don't need a fence, because the numbered create and the rebuild already resolve two folds of one tail.

Put a lease with a heartbeat on the maintainer role, so that the maintainer writes a heartbeat to a shared state object every few seconds. With a lease, a second maintainer stays away before it does any work. Without one, both maintainers run, and a compaction unit whose inputs the other maintainer replaced ends as abandoned after its rewrite. A lease costs a write per heartbeat per namespace, and needs a rule for a lease that expires while a job runs. LoonFS bounds each job by its budget instead, and an abandoned unit costs the job that unit's work but no data.

Run one maintainer process per deployment, set by configuration. This removes rival compactions for the namespaces that no writer host maintains, but it's a deployment choice that the format can't enforce, so the input check is still the rule, and a deployment can still make that choice.
