# Maintenance roles

This note describes which processes can publish a namespace's manifests and delete its objects, and what fences a rival. A namespace has two roles:

- The "writer" commits, and deletes the namespace, under the [writer epoch](../specs/glossary.md#writer-epoch).
- The "maintainer" folds, compacts, advances the retention floor, and collects garbage. Compaction claims the [compactor epoch](../specs/glossary.md#compactor-epoch), and the maintainer's other publications copy the current compactor epoch from their predecessor.

Pins, forks, and namespace creation belong to no role. Every other publication is made by one role. The numbered create fences every publication, and compaction's claim fences a replaced runtime.

## The problem

A manifest is created at its predecessor's number plus one with put-if-absent, so two publishers can't both create the same number. The manifest records the writer epoch and the compactor epoch, so a publisher can check whether it still has the role it acts under. Compaction uses both mechanisms. A runtime claims the compactor epoch before its first compaction, every compaction publication has to match the current epoch, and a newer claim stops an older compactor at its next check.

Folds start in several places. A writer's host folds its session's tail after a commit that reaches the fold thresholds or when the tail reaches the idle fold age, a maintenance call or the host's daily pass runs an explicit fold, and a pin from the current head or a namespace deletion folds the tail first. Two folds of one namespace can run at once, in one process or in two. If a session's fold were bound to the writer epoch and the other folds weren't, LoonFS would have fold paths with different rules. The binding would exist to keep the session's position off a manifest published after another writer's claim, and the writer position's epoch rule (the position never moves onto a manifest with a newer writer epoch) already does that job.

## Two roles

| Operation | Role | Epoch it publishes under | What fences a rival |
| --- | --- | --- | --- |
| Commit (a WAL publication) | Writer | Writer epoch | The claim manifest and the WAL fence of a newer writer |
| Namespace deletion | Writer | Writer epoch, for the tombstone (the fold before it is the maintainer's) | The same |
| Compactor claim | Maintainer | Compactor epoch plus one | The numbered create |
| Compaction | Maintainer | Compactor epoch | A newer claim, found at the next check |
| Fold | Maintainer | The current compactor epoch, copied from the predecessor | The numbered create |
| Retention advance | Maintainer | The current compactor epoch, copied from the predecessor | The numbered create |
| Garbage collection | Maintainer | None (it publishes nothing) | Nothing. Concurrent collectors are safe under the age rules, and an object that's already gone needs no cleanup |
| Pin creation and deletion | None | None (a pin is its own record, and the fold before a pin is the maintainer's) | Put-if-absent on the pin, and the verification against the current manifest |
| Fork | None | None for the source pin, and the writer epoch of the new namespace's first manifest | The source pin's verification, and the first manifest's create |
| Namespace creation | None | The writer epoch of manifest 1 | The create of manifest 1 |

A process can have both roles for a namespace. A host with an open writer session has the writer role, and it maintains the namespace as the session publishes. A maintenance runtime without a writer session has the maintainer role for the namespaces it visits. A runtime claims the compactor epoch before its first compaction of a namespace, and a fold (including a pin's fold and a deletion's fold) runs under the current compactor epoch with no claim.

If a publication's numbered create loses, the publisher reloads the current manifest. For a commit or a compaction, a higher epoch for the publication's role means that another process took the role, and the publication stops with `fenced`. Otherwise, the publisher rebuilds against the new predecessor and tries the next number. A fold or a retention advance finds its work covered, or rebuilds.

Why does the writer do nothing but commit and delete? The writer epoch exists to fence commits. A new writer publishes a claim manifest and a WAL fence, and the old session's next commit finds them. A fold, a compaction, or a retention advance changes no commit, so it has no reason to use the writer epoch, and a process that only maintains a namespace has no reason to take it. Deletion is the exception, because it ends commits, and only the writer epoch fences a commit.

Why two epochs and not one? The two roles have different lifetimes and can belong to different processes. A writer session lasts as long as the application that commits through it, and a maintainer visit lasts one fold, one compaction, or one pass. A deployment runs the two roles in different processes when its operators want to keep maintenance's memory and CPU away from the writer's, and in one process when they don't. With one epoch, every compactor claim would fence the writer.

## Folding as the maintainer

A [fold](../specs/glossary.md#fold) moves the WAL objects after the current manifest's folded boundary into metadata segments, and publishes the next manifest as the maintainer's publication, with both epochs copied from its predecessor. The writer's host folds its own sessions' tails as the maintainer, after a commit that reaches the fold thresholds and when a tail reaches the idle fold age. A maintenance runtime folds a tail that no host is maintaining, such as a crashed writer's, with no claim on the writer role and no fence on a writer. A snapshot from any process folds the tail the same way, and then pins the resulting manifest.

A writer session runs one fold at a time. A maintenance call's fold can run beside the session's fold, in the same process or in another one, and the numbered create resolves the two. A fold whose create loses reloads the current manifest and rebuilds against it, and the rebuild has four outcomes:

- **The tail is already folded.** The current manifest's folded boundary is at or past the fold's tip, and its head and number are at or past the fold's, so there's nothing to publish.
- **The winner is at the same head.** The winner is a compaction, a compactor claim, or a retention advance, so the rebuild keeps the tail it read.
- **The winner is a new writer's claim.** The rebuild folds the new writer's log.
- **The winner is a namespace tombstone.** The fold stops.

A fold resolves a put whose outcome is unknown the same way every publication does, by checking whether the manifest at its number is its own.

A writer session's [position](../specs/glossary.md#writer-position) moves onto a fold's manifest as it moves onto any manifest under the session's writer epoch, whether this process or another published it, and never moves onto a manifest with a newer writer epoch. So a fold needs no binding to the writer epoch to keep the old writer's reads off a new writer's log, because the position's epoch rule already keeps them off, and the session finds that it's fenced at its next commit.

Why doesn't a fold claim the compactor epoch? A fold is short and frequent (every 64 WAL objects, and every snapshot), and a compaction is long and rare. If each fold claimed the epoch, each fold would fence any compaction running in another runtime. Without a claim, folds and compactions coexist. A compaction's finalization that loses its create to a fold retries over the fold's manifest, and a fold that loses its create to a compaction rebuilds and keeps its tail.

Why isn't a fold the writer's, under the writer epoch? A tail left by a crashed writer, or by a host that stopped maintaining it, could then be folded only by a process that claims the writer role. That claim fences a session that might be idle rather than dead, and costs an application a failed commit when the application returns. A maintainer's fold leaves the writer alone, and the format lets any process fold.

## The maintainer

Compaction claims the compactor epoch before a runtime's first compaction of the namespace, and checks the epoch before it reads a segment and before each publication attempt. Folds and retention advances don't claim or check the epoch. They publish under the current compactor epoch, copied from the predecessor, and rely on the numbered create, so a fold never fences a compaction. A retention advance whose create loses reloads the current manifest and evaluates its target again. A maintainer compacts whether or not an unfolded tail exists above the runs, because compaction reads the manifest's segments and copies the folded boundary forward, and a retention advance stops at the manifest's head.

Garbage collection publishes nothing and claims nothing. A pass establishes its roots from the current manifest, the upload sessions, the WAL tail, and the complete pin listing, and deletes each object that no root references once the object is older than the grace. Two passes over one namespace delete the same objects, and the second finds them gone. The maintainer runs garbage collection because it runs the other passes, not because a fence requires it.

A host that has both roles for a namespace folds and compacts. A separate maintenance host that visits the same namespace claims the compactor epoch when it compacts, which stops the writer host's next compaction at its check, and the writer host claims again when it next compacts. The two hosts' folds coexist. The host's daily pass visits every listed namespace, but a visit folds a tail only when a fold is due (at the fold thresholds or the idle fold age, as on a tick). Two hosts that maintain one namespace alternate claims for compaction only, and each lost claim costs the loser one compaction. An unfinished metadata visit that finds another process's compactor claim retries on an interval, at six requests per retry.

## Pins and forks

A [pin](../specs/glossary.md#pin) is a record under the namespace's pins prefix, created with put-if-absent and verified against the current manifest after the write. A pin belongs to no role, and the verification is its fence. A pin whose manifest stopped being current between the write and the verification is deleted and retried from a fresh basis. The fold before a pin from the current head is a maintainer's fold, under the current compactor epoch with no claim. A fork is a pin on the source plus the first manifest of the new namespace, which the new namespace's writer epoch covers. Deleting a namespace is a fold followed by a tombstone. The fold is the maintainer's, under the current compactor epoch with no claim, and the tombstone is the writer's, because it ends commits.

## Costs

Folds, pins, retention advances, and deletions make no claim, so they send no request beyond their own publications. A runtime claims the compactor epoch before its first compaction of a namespace, and again after another runtime's claim, and each claim is one manifest publication. Two hosts that maintain one namespace alternate claims, and each lost claim costs the loser one compaction.

## What the format and API specs say

- [Publishing a materialized file set](../specs/format.md#72-publishing-a-materialized-file-set) describes a fold as the maintainer's publication, under the current compactor epoch, with the covered check and the rebuild. The writer position's epoch rule in [Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal) keeps an old writer's position off a new writer's log.
- [Creating and verifying a pin](../specs/format.md#82-creating-and-verifying-a-pin) describes the fold before a pin as a maintainer's fold, under the current compactor epoch.
- [Deleting a namespace](../specs/format.md#94-deleting-a-namespace) describes the fold before the tombstone as a maintainer's fold, under the current compactor epoch, and the tombstone as a publication under the writer epoch.

In the API specification, the maintenance jobs say which role each runs under, and the glossary defines the [maintainer](../specs/glossary.md#maintainer).

## Alternatives considered

Bind a writer session's fold to the writer epoch, and leave the explicit fold and the pin's fold unbound. This keeps three fold paths, and a binding rule whose job the position's epoch rule already does.

Put folds on the writer, under the writer epoch, with a maintenance pass claiming the writer role to fold a crashed writer's tail. This keeps each fold with its log, but either a pass fences an idle session, which costs the application a failed commit when it returns, or the tail stays unfolded until a writer opens the namespace again.

Publish folds under the compactor epoch, with a claim per fold. Every fold then fences any compaction that runs in another runtime, so routine folds and snapshots can keep a long compaction from finishing.

Add a third epoch for folds, claimed by whichever process folds. It needs a claim, a fence, and a rule for a compaction that runs while another process has the fold epoch, and folds don't need a fence, because the numbered create and the rebuild already resolve two folds of one tail.

Put a lease with a heartbeat on the maintainer role, so that the maintainer writes a heartbeat to a shared state object every few seconds. With a lease, a second maintainer stays away before it does any work, and with the epoch, it stops after its first publication loses. A lease costs a write per heartbeat per namespace, and needs a rule for a lease that expires while a job runs. LoonFS bounds each job by its budget instead, and a lost claim costs the job its work but no data.

Run one maintainer process per deployment, set by configuration. This removes the rival compaction claim for the namespaces that no writer host maintains, but it's a deployment choice that the format can't enforce, so the epoch is still the rule, and a deployment can still make that choice.
