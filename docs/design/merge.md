# Merging a fork

This note describes which parts of a merge are server operations, and which parts are a loop that a caller runs on top of those operations.

A [fork](../specs/glossary.md#fork) is a namespace that starts as a copy of another namespace at one point in its history, and shares that namespace's stored objects. A "[merge](../specs/glossary.md#merge)" applies a fork's commits to another namespace in its [fork family](../specs/glossary.md#fork-family) as one commit. In this note, "main" is the namespace that a fork was forked from, "the fork" is the namespace whose commits a merge applies, and "the fork point" is the sequence of main that the fork was made at. Most merges apply a fork's commits to main, and a [rebase](#rebasing) applies them to a new fork of main's head instead.

The storage format defines these parts of a merge:

- the merge record in a merge's commit, which identifies the fork and the range of the fork's commits that the merge applied ([WAL records](../specs/format.md#a5-wal-records)),
- the merge index, which lists the merges that a namespace received, by fork ([Metadata rows and row keys](../specs/format.md#a6-metadata-rows-and-row-keys)),
- the range rule, which requires each merge from a fork to continue the previous one, and the floor rule, which refuses a merge from a fork whose fork point is below the retention floor ([Merges](../specs/format.md#merges)),
- the conflict rule, which refuses a merge that would replace a change that the fork didn't see (the same section), and
- a merge's retry identity ([Commit identity and retries](../specs/format.md#65-commit-identity-and-retries)).

LoonFS performs a merge in two layers:

- The "mechanics" are the server operations that the loop needs: a merge route, a rebase route, a query for where a fork's next merge starts, and a hint in the error for a merge that's too large. They're implemented in `loonfs-core`, `loonfs`, and `loonfs-http`, and a deployment advertises them as the `filesystem.merges` feature.
- The "loop" is what a caller does with the mechanics: merge, split a merge that's too large, rebase on a conflict, resolve, and merge again. It's implemented as bounded steps in `loonfs-merge`, a crate outside core, which the CLI and the lab drive through the HTTP client.

The layers are split the same way as core and the grep extension. Everything that writes a WAL commit is in core, and everything that reads core through its public API and keeps its own progress is outside core.

## The problem

A merge needs more than one request, for three reasons.

A merge can conflict. LoonFS refuses a merge if main changed a file that the fork also changed, unless main's change came from an earlier merge of that fork. After such a change on main, the fork can't merge its own version of the file, because main's current row counts as a change that the fork never saw. On its own, the fork can resolve the conflict only by adopting main's version, which takes the file out of the merge. To keep both changes, the caller:

1. Makes a new fork of main's head.
2. Replays the fork's unmerged changes onto the new fork, which applies the changes that don't conflict and reports the ones that do.
3. Resolves the reported files in the new fork.
4. Merges the new fork.

In this note, "the resolver" is the program or person that does step 3. In the workloads that merges are designed for, the resolver is an AI agent that runs outside LoonFS and can take minutes or hours, and LoonFS can't keep a request open that long.

A merge can be too large for one WAL object, and the caller then merges the fork's commits in smaller ranges. Choosing the ranges is guesswork unless the server says where the first range fits.

A caller can crash between steps. The fork, the new fork, and some merged ranges all exist in the store, and whoever resumes the merge has to find them and continue without applying anything twice.

## The conflict rule

[Merges](../specs/format.md#merges) in the storage format defines the conflict rule. This section summarizes the rule and explains why it works the way it does.

A merge applies the fork's "net change" over its range, which is the difference between the fork's state at the end of the range and its state at the start, with one entry for each inode. An inode whose value in the fork is the same as main's current value is left out of the merge, so a fork that adopted main's version of a file has nothing to merge for that file. For every other inode, the planner (the part of LoonFS that turns the fork's net change into the merge's operations) checks the rows of main that the merge would replace. A row is "the fork's own" if it was committed at or below the fork point, or by an earlier merge of this fork, because the fork started from that state or produced it itself. Any other row is "foreign", and if the merge would replace a foreign row, the inode conflicts.

Why does the rule work per inode, instead of comparing the whole merge against one point in main's history? Suppose main has file `Y` at revision 5 at the fork point, main changes `Y` to revision 6 ten commits later, the fork's first merge doesn't touch `Y`, and the fork then edits `Y` based on revision 5. A second merge that compared against main as it was at the first merge would read `Y` at revision 6 and submit revision 6 as the precondition, and the fork's edit would overwrite main's change with no conflict reported. Comparing against the fork point instead would be right for `Y`, but it would report a false conflict on every inode that a previous merge of the fork wrote, because that merge changed those inodes on main after the fork point. The per-inode rule reads the baseline that each inode has, and it needs no view of main other than the head.

Why does the conflict rule need the floor rule? To tell the fork's own rows from foreign ones, the planner reads main's commit rows at the sequences of main's current rows, and for a directory that the fork deleted or moved, it reads main's diff from the fork point to the head. Both reads need main's history above the fork point. Main keeps that history while its retention floor is at or below the fork point, and the floor rule refuses a merge from a fork whose fork point is below the floor.

## The mechanics

### Merging

`POST /v0/namespaces/{ns}/merges` merges a fork into `ns`. The body identifies the fork and, optionally, the fork sequence to merge through (the fork's head by default), and it includes the commit's `commit_id` and optional `message`, like any commit. The response is the `Commit`, with its merge record: the fork, `source_after_seq`, and `source_through_seq`.

What bounds a merge's size? Two bounds apply. The WAL object takes at most 512 MiB of planned bytes, and the "merge allowance" is a memory reservation for the one merge that the server plans and publishes at a time (`max_merge_bytes` in the reference server). The server takes that reservation, the "merge slot", before it plans a merge, so the allowance covers both planning and publication. For a merge at the WAL bound, planning peaked at about 560 MB and publication at about 860 MB. Admission estimates a merge's publication peak at three times its planned WAL bytes, which is what measurements showed across name lengths, and compares that estimate with the allowance. LoonFS doesn't charge a merge to the publication queue's budgets, because those budgets are sized for request bodies. The default allowance is 512 MiB, which admits a merge of about 36,000 file creates as one commit. A deployment that wants merges as large as the WAL object allows sets the allowance to about 1.5 GiB, on a server that's sized for it. A second merge that arrives while another merge has the slot returns `commit_queue_full`, which is transient. The planner also bounds the range, because it materializes every inode that the range's commits touched. It refuses a range whose commits touched more than about 290,000 inodes with `merge_too_large`. The number of touched inodes never falls as the range grows, so the planner knows which commit crossed the bound, and the error's split hint is the sequence before that commit.

To merge a fork, the server:

1. Looks up the receipt for the request's commit ID, and returns the original commit if a merge with that ID was already committed.
2. Takes the merge slot, so that the server plans and publishes one merge at a time. A second merge gets `commit_queue_full` at this step, before it plans anything.
3. Reads the fork's manifest, and fails with `invalid_request` unless the namespace that stores the fork's basis manifest (`fork_basis.manifest.owner_namespace_id`) is `ns`. The fork point is that manifest's head sequence (`fork_basis.manifest.head_seq`).
4. Refuses a fork point below main's retention floor with `rebootstrap_required`.
5. Reads main's merge index for the fork's newest merge. The range starts after that merge's `source_through_seq`, or after the fork point if the fork has never merged, and the caller never supplies the start.
6. Refuses a range start below the fork's own retention floor with `rebootstrap_required`, because the fork's net change can't be computed from history that the fork no longer keeps.
7. [Folds](../specs/glossary.md#fold) the fork, so that every content reference that the merge will admit has a layout ([Cross-namespace copies and moves](../specs/format.md#97-cross-namespace-copies-and-moves)), and the plan reads exact layouts from the folded fork.
8. Computes the fork's net change over the range, plans the commit, and returns `merge_too_large` with a [split hint](#the-split-hint) if the range's commits touched more than about 290,000 inodes, if the commit's planned size doesn't fit in one WAL object, or if its estimated publication peak doesn't fit within the merge allowance.
9. Classifies the net change against main's head under the conflict rule, and returns `merge_conflict` if any inode conflicts (a rebase lists every conflict in a [conflict report](#rebasing)). Nothing is submitted, and a refused merge leaves nothing behind but the fork's fold, which is maintenance that would happen anyway.
10. Builds one commit whose preconditions are main's current values (and main's head, when the net change deletes or moves a directory), whose creates keep the fork's inode IDs, and whose content references come from the fork's rows without a read of their bytes. If nothing is left to apply, because the fork adopted main's version of everything it changed or undid its own changes within the range, the commit has the merge record and no operations, so that the merge index advances and the change feed shows the merge of that range.
11. Publishes the commit as the only commit in its WAL object. The writer checks the range and the floor again against the batch's state, so if two merges of the same fork run at once, the writer refuses the second with `merge_range_conflict`.
12. Folds main through the merge's WAL object, so that readers read a large merge from main's WAL tail only briefly. If the fold would exceed the request's deadline, the route returns and leaves the fold to the next maintenance pass, and a failed fold doesn't fail the merge, because the merge is already committed.

The planner orders the merge's operations by dependency, not by kind. An operation that frees a name or a parent comes before the operation that needs it, a directory create comes before what goes in the directory, and parents come before children. A cycle among moves, such as two files that swap names, is broken with a move to a temporary name inside the same commit. The temporary name is the inode's ID, and the second leg is a path move, so no reader ever sees the temporary name, and the change feed shows two moves. If an entry with the temporary name already exists, the writer refuses the merge.

Why look up the receipt first? The server computes the merge's range, so a retry after a successful merge would otherwise compute the next range, produce a different fingerprint under the same commit ID, and fail with `commit_id_reuse_conflict`. The writer's own receipt check still runs at publication.

Why check the size before the conflicts? A conflict leads to a rebase, which replays the fork's unmerged changes into a new fork. Checking the size first means that a conflict always refers to a range that fits in one merge, so the first replay commit of that range fits. A rebase of a larger unmerged range can still return `merge_too_large` with the hint, and the loop then replays the range in pieces.

Why does a merge that deletes or moves a directory set a head precondition? After the planner classifies the net change, another writer can create a file beneath that directory without changing any row that the merge's other preconditions cover, and the merge would then delete or move the new file with no conflict reported. The planner sets the commit's head precondition (`expected_head_seq`) to the head that it classified against, so the writer fails the commit with `stale_head` if the head moved. The route then plans the merge again, up to three more times (four plans in all), before it returns `stale_head` to the caller. A replay commit doesn't need the head precondition, because nothing else writes to the new fork while the rebase runs.

LoonFS checks the subject's rights against the net change, not against the scheduled operations. Each change needs the rights that the subject would need to make that change directly, checked against main's access rows when the merge is planned ([Access rows](../specs/format.md#18-access-rows)). A temporary move is exempt from the `create` and `remove` checks on the directory that has the temporary name, because the temporary binding doesn't survive the commit, so the exemption escalates nothing. The subject also needs `read` on the inodes that the fork changed, so that it can't merge changes that it can't see.

The route can return these errors, in addition to the ones that any commit can return:

| Code | Status | Meaning |
| --- | --- | --- |
| `merge_conflict` | 409 | The fork's net change conflicts with main. The details include `conflict_count` and `conflict` (the first conflict, in the shape of a conflict report entry), so that a log line shows what conflicted, and a rebase returns the full report. Nothing was submitted. |
| `merge_range_conflict` | 409 | The fork's newest row in the merge index shows that its next merge starts at a different sequence. The route reads the index itself, so this means that two merges of one fork ran at once. |
| `merge_too_large` | 413 | The planned commit doesn't fit in one WAL object, its estimated publication peak doesn't fit within the merge allowance, or the range's commits touched more than about 290,000 inodes. The details say which, and include `split_at_seq` when a shorter range fits (see [The split hint](#the-split-hint)). |
| `rebootstrap_required` | 409 | The fork point is below main's retention floor, or the range's start is below the fork's own floor, so history that the merge needs is gone. The details say which floor. The fork can't merge or rebase. The caller makes a new fork of main's head and copies the changes that it wants to keep through ordinary reads and writes. |
| `invalid_request` | 400 | The identified namespace isn't a fork of `ns`, or the sequence to merge through is outside the fork's history. |

### Rebasing

A "rebase" replays a fork's unmerged changes onto a new fork of main's head, applying everything that doesn't conflict and reporting everything that does. `POST /v0/namespaces/{ns}/rebases` makes one "replay commit", which is itself a merge of the fork, made into the new fork instead of into main. The body identifies the fork, the new fork's namespace ID, the fork sequence to replay through, and the commit's `commit_id` and optional `message`. The response is the new fork's namespace object and a "conflict report", which has one entry for each inode whose change the replay commit didn't apply.

A replay commit skips the changes that conflict, so its merge record identifies the range of the fork's commits that the replay commit examined, and the conflict report identifies the changes that it left out. When it skips the create of a directory because of a `name` or `visibility` conflict, it also skips every change that depends on that directory (a create or a move into it), in parent-before-child order, and each of those changes gets a report entry of kind `parent`.

A rebase can take more than one call. The first call creates the new fork, and every call makes one replay commit, so a caller whose range is too large replays it in pieces, each call continuing where the previous one stopped, into the same new fork.

To rebase a fork, the server:

1. Reads the fork's manifest and checks that the fork was made from `ns`, as the merge route does.
2. If the identified namespace doesn't exist yet, refuses a fork point below main's current retention floor with `rebootstrap_required`, before it creates anything.
3. Creates the new fork of `ns`'s current head if the identified namespace doesn't exist. The new fork's pin keeps main's manifest at that head readable.
4. Records `rebased_from` in that new fork's first manifest, with the fork's namespace ID and `resume_after_seq`. `resume_after_seq` is the fork sequence that the fork's next merge into main would have started after, which the route reads from main's merge index through the pin.
5. If the namespace already existed, checks that its `rebased_from` identifies the fork, or fails with `invalid_request`.
6. Checks the fork point again against main's retention floor, as the pinned manifest records it. This check fails only if a retention advance was published on main between step 2 and the new fork's creation, and then the route deletes the new fork that it just created and returns `rebootstrap_required`.
7. Looks up the receipt for the request's commit ID in the new fork, and returns the original replay commit if one with that ID was already committed, with the report recomputed as the last step describes.
8. Takes the merge slot, as the merge route does, so that no other merge is planned or published at the same time as the replay commit.
9. Looks in the new fork's merge index for the fork's newest "replay row", which is a `merges` row that an earlier replay commit wrote above the new fork's fork point. The range starts after that row's `source_through_seq`, or after `resume_after_seq` if there's no replay row yet, so the route never reads a row that the new fork inherited from main.
10. Refuses a range start below the fork's own retention floor with `rebootstrap_required`.
11. Folds the fork, as the merge route does, so that the plan reads exact layouts from the folded fork.
12. Computes the fork's net change from the range's start through the requested sequence, plans the replay commit, and returns `merge_too_large` with a split hint if the commit wouldn't fit.
13. Classifies the net change against main through the pin, the way a snapshot read works, under the conflict rule.
14. Commits the operations that don't conflict into the new fork as a replay commit, which the writer checks as a merge with the two substitutions described below. A replay commit whose every change conflicted has the merge record and no operations, as a merge with nothing to apply does.
15. Returns the new fork's namespace object and the conflict report for this replay commit. A retry of a range that was already replayed returns the same report by classifying again without committing, because the fork's change over that range and main's pinned state are unchanged.

Why fork first and classify second? If the server classified first and forked second, main could advance in between, and the new fork would inherit rows that the classification never saw. Those rows would be at or below the head that the new fork was made at, so they'd count as the new fork's own, and when the new fork merged, the fork's version would overwrite them with no conflict reported. That's the lost update that the conflict rule exists to prevent. The pin also protects the classification from the new fork's own retention. The new fork's retention floor is the head that it was forked from, so a base compaction of the new fork can remove the commit rows that the new fork inherited from main, which the classification reads, but the pinned manifest keeps them.

Which rules change for a replay commit? The writer recognizes a replay commit because the merge record's source is the fork that the namespace's `rebased_from` identifies, and it substitutes two values in the merge rules. The range rule starts after `resume_after_seq` and considers only the namespace's own `merges` rows (those above its fork point), so it ignores the rows that the new fork inherited from main. The floor rule compares the namespace's own fork point (`fork_basis.manifest.head_seq`) with its retention floor, in place of the fork's fork point, so a rebase fork accepts replay commits only while its floor is still at its fork point. Every other rule for a commit with a merge record applies unchanged: the inode absence check, the fork's inode IDs, the admission of references without an import, the exemption from the request limits, publication alone, and the retry identity.

Why does the floor rule use the new fork's own fork point? The range rule reads the new fork's replay rows. Without the substitution, a base compaction could remove them once the new fork's floor passed them, and the writer would then fall back to `resume_after_seq` and accept a repeated replay commit over the resolver's work. The fork's fork point can't be used, because the new fork's floor starts at the head that it was forked from, which is above the fork point, so the floor rule would refuse every replay commit. The conflict rule's reads of main's history don't depend on the new fork's floor, because the route reads main through the pin, after it checked the fork point against main's floor.

The report has one entry for each conflicting inode:

| Field | Meaning |
| --- | --- |
| `inode_id` | The inode. It's the same ID in every namespace involved. |
| `kind` | Which check failed: `content`, `binding`, `attributes`, or `access` (that row on main is foreign), `visibility` (the inode or an ancestor is hidden on main), `name` (the name that the fork's create needs is taken on main), `subtree` (main changed something beneath a directory that the fork deleted or moved), or `parent` (the change depends on a directory whose create the replay commit skipped). |
| `base` | The version that the fork started from, which is main's version at the fork point: for a file, `revision_no` and `content_ref`, and for any inode, its path there. It's `null` for an inode that the fork created, and for a file whose deletion had expired on main at the fork point. |
| `ours` | The fork's current version: `revision_no` and `content_ref` for a file, and the path. It's `null` for an inode that the fork deleted. |
| `theirs` | Main's current version in the same shape, or `null` when main deleted or hid the inode. For a `name` conflict, it's the inode on main that has the name, because the fork's inode doesn't exist there. For a `subtree` conflict, it's the directory's current version on main. What changed beneath the directory doesn't fit in an entry, so the resolver reads main's diff from the fork point over that directory to see it. For a `parent` conflict, it identifies the entry of the directory whose create was skipped. |

A resolver reads `ours` and `theirs` through the ordinary read routes. It reads the base on main by inode and revision number (`GET /inodes/{inode_id}/revisions/{revision_no}/content`), with the new fork's fork pin as the snapshot (`snapshot_id` set to that pin's ID). The pinned manifest is main's state at the head that the new fork was made at, so it keeps the base revision's row and its content for as long as the new fork exists, whatever main's floor or the new fork's floor does afterwards. The read checks that the content is published in the pinned manifest, not in either namespace's current view. This read needs three extensions to the read API. Reads of a namespace accept one of its fork pins as `snapshot_id`, because a fork pin keeps a manifest just as a snapshot does. The inode revision content read accepts `snapshot_id`. The resolver finds the pin ID in the new fork's namespace object, whose `fork_basis` gains `source_pin_id`. The rebase route's classification reads main through the same pin. (`base.path` is for display.) The resolver writes the resolved content into the new fork with ordinary commits, and the new fork merges into `ns` as a first merge at its own fork point.

If a rebase fails for any other reason after the server created the new fork, the new fork stays, because a later call continues into it.

A rebase needs the rights for each of its two steps. Creating the new fork needs the right to fork `ns`, which an ACL namespace grants to the token holder, and to a supplied subject only when the subject is an administrator ([`POST /forks`](../specs/api.md#613-post-forks)). The replay commit needs a subject context in an ACL namespace, as every commit does, with the merge rights described above. So in an ACL namespace a rebase runs as an administrator subject, and so does deleting a fork afterwards. A subject that can merge into `ns` but isn't an administrator can't recover from `merge_conflict` on its own.

After a rebase, the loop stops using the fork. The fork's created inodes reach main through the new fork with the same IDs, so a later merge from the fork would treat them as inodes that already exist in main. The resolver's changes in the new fork also differ from the fork's versions, so a later merge from the fork would conflict on those files.

Why skip and report instead of failing? The point of a rebase is to leave a fork in which a resolver can work, with everything that doesn't conflict already applied. A rebase that failed on the first conflict would leave nothing to work in.

Why a new fork and not the fork itself? The conflict rule checks which commit wrote main's current row, and LoonFS has no way for a fork to record that it took main's change into account. So after one foreign change, every later merge from the fork conflicts on the same row, unless the fork adopts main's version of the file. A new fork of main's head starts from a fresh baseline, because it inherits every row as its own.

### The next-range query

`GET /v0/namespaces/{ns}/merges?source={fork}` returns the newest merge from the fork in `ns`'s merge index, and `next_after_seq`, the fork sequence that the fork's next merge starts after (the newest merge's `source_through_seq`, or the fork point if the fork has never merged). The loop reads `next_after_seq` when it resumes, and a client that wants to show what's unmerged reads the fork's change feed from that sequence. When `ns` is a rebase's new fork, the query returns the fork's newest replay row (one of the new fork's own `merges` rows, above its fork point) and never a row that the new fork inherited from main, and `next_after_seq` comes from that row, or from `resume_after_seq` before the first replay commit.

### The split hint

A `merge_too_large` error's details include `split_at_seq`, a fork sequence such that a merge of the fork's commits through it fits within the bound that refused the merge. For the touched-inode bound, `split_at_seq` is exact. The number of inodes that a range's commits touched never falls as the range grows, so the planner knows which commit crossed the bound, and it returns the sequence before that commit at no extra cost. For the two size bounds (the WAL object and the merge allowance), the planner finds `split_at_seq` from the fork alone, and it uses the size of the fork's net change over a shorter range as an upper bound on the planned size of a merge of that range. It's an upper bound because inodes whose value is the same as main's are left out of the merge, so a range that fits by this measure fits within both size bounds. The planner measures at most four ranges. The first three bisect on the fork's commit sequences, and if nothing has fit yet, the last probes the first unmerged commit alone, so that a fitting prefix shorter than the smallest bisection probe still gets a hint. The planner reports the largest sequence that a probe showed to fit. If the fork's first unmerged commit alone is too large for any of the bounds, the details have no `split_at_seq`, and that commit can't be merged. The caller merges through `split_at_seq` and then merges the rest, and a caller that gets `merge_too_large` again narrows the range further. Each piece is a complete merge of its range, with a merge record for exactly that range.

## The loop

`loonfs-merge` is a crate that depends on `loonfs-types` and on nothing private in core. It defines a trait, `Mechanics`, with a method for each server operation that the loop uses: merge, rebase, the next-range query, and deleting a namespace. The HTTP client implements the trait for the CLI and the lab. The crate implements the loop as a state machine in which every step is one or two requests.

A "merge session" is the loop's record of one fork's merge, from the first request to the last. A session moves through these states:

```text
                 merge_too_large with split_at_seq:
                 merge through split_at_seq
                 +---------+
                 |         |
                 v         |
              +-----------------+   merge committed   +--------+
   start ---> |     merging     | ------------------> | merged |
              +-----------------+                     +--------+
                 |      ^     |
 merge_conflict: |      |     | merge_too_large without
 rebase          |      |     | split_at_seq, or
                 v      |     | rebootstrap_required
          +-----------+ |     v
          | resolving |-+  +--------+   caller abandons   +-----------+
          +-----------+    | failed | ------------------> | abandoned |
       resolver returns    +--------+                     +-----------+
```

| State | Meaning | Next step |
| --- | --- | --- |
| `merging` | The current fork (the original fork, or the latest new fork) is ready to merge. | Merge it through the session's sequence. On success, the session is `merged`. On `merge_conflict`, rebase the fork's whole unmerged range through the session's sequence into a new fork, in pieces if the rebase returns `merge_too_large`, and the session is `resolving`. On `merge_too_large` with `split_at_seq`, whichever bound refused the merge, merge through `split_at_seq`, and the session stays `merging`. On `merge_too_large` without `split_at_seq` (the fork's first unmerged commit alone is too large), or on `rebootstrap_required`, the session is `failed`. |
| `resolving` | A new fork exists with its conflict reports, and the resolver is working in it. | When the resolver returns, the new fork becomes the current fork, with its own head as the sequence to merge through, and the session is `merging`. |
| `merged` | The session's last merge committed, and nothing remains of the current fork's range. | Delete the new forks that the session's rebases created. |
| `failed` | A merge or a rebase returned an error that the loop can't act on, and the session's report says which. | The caller chooses the next step. The new forks stay until the caller abandons the session. |
| `abandoned` | The caller abandoned the session. | Delete the new forks that the session's rebases created. |

The session record stores main, the original fork, the sequence to merge through, the current fork, the new fork while the session is `resolving`, the commit ID of the merge in flight (so that a retry returns the original commit), the state, and the conflict report of every replay commit in the session. It keeps every report because a rebase in pieces returns one report for each piece, and the resolver needs all of them. The CLI writes the record to a file after every step, so that a run that crashed can resume. A caller that lost its record recovers the forks and the next range from the store. The next range comes from the next-range query, and the new forks are the forks of main whose `rebased_from` identifies the original fork or an earlier new fork, which the checkpoint inventory in `maintenance/v0` lists. The caller regains the reports only by rebasing again into a fresh new fork, which repeats the classification.

The crate represents a resolver as a trait, `Resolver`. Given the new fork and its conflict reports, an implementation writes resolutions into the fork and returns when the fork is ready to merge. The crate's default resolver fails and returns the reports, so a caller with no resolver gets the conflict reports and a fork to inspect, and the conflicting changes aren't merged.

The loop deletes the new forks that it asked the rebase route to create, and the caller deletes the original fork when the session reaches `merged`, or whenever the caller no longer needs the fork.

Two callers drive the steps. `loonfs merge` runs the loop to completion with the default resolver, and the lab's merge scenarios drive it with resolvers that simulate an agent.

Why is the loop outside core? It writes nothing that core doesn't already write through its routes, it needs no private type, and its policy (when to merge, and what to do with a conflict) belongs to the caller.

Why isn't the loop in each SDK? Four copies of a state machine whose correctness depends on the conflict rule's consequences would drift apart. Because the server computes ranges, suggests split points, and rebases, the loop that an SDK would need is small, and the Rust crate is the reference implementation.

## Naming

This operation is a merge, because that's what version control calls applying one line of history to another, and a user who has merged a branch expects the same behavior here. Changes that don't overlap are applied, and the ones that do are reported for the user to resolve. Storage systems that offer their users a merge keep the word out of their compaction vocabulary. RocksDB has a merge operator, and it calls the rewriting of its files compaction. Iceberg and Delta Lake have `MERGE INTO`, and they call theirs compaction, rewrite, and optimize. LoonFS does the same in its specs and guides. A compaction rewrites a window of runs into fewer runs, and its limits are compaction input limits. The one exception is the k-way merge iterator inside compaction, which is called a merge because "k-way merge" is the standard name for that algorithm, and the name never appears in a user-facing sentence. The glossary's [Merge](../specs/glossary.md#merge) entry defines the operation.

## Costs

- The mechanics are two routes and one query in `loonfs-http`, a planner mode that skips and reports instead of failing, a planner path that sizes before it classifies, two writer substitutions for a replay commit (in the range rule and the floor rule), a writer rule that admits a merge with no operations, a merge allowance in admission with one setting, one optional manifest field, and four error details (`split_at_seq`, `conflict_count`, `conflict`, and the details of a `rebootstrap_required` from the fork's own floor). The rebase route's plan is a merge plan, so a replay commit costs what a merge costs, and the first replay commit also costs a fork.
- The split hint costs up to four diffs of the fork for a merge that didn't fit, which is a rare path.
- A cycle among moves, such as two files that swap names, costs one extra move to a temporary name in the commit, so the change feed shows two moves for one of the files.
- The crate is a state machine over public types, with a trait for the mechanics, a trait for the resolver, and a CLI command.
- A new fork is an ordinary fork, with an ordinary pin, and main can't be retired while one exists, as with any fork.

## What the format and API specs change

The rebase widens the definition of a merge, in [Commit identity and retries](../specs/format.md#65-commit-identity-and-retries) and in the glossary's [Merge](../specs/glossary.md#merge) entry, from a merge into the namespace that the fork was forked from to a merge into another namespace in the fork's family, so that a replay commit into a rebase's new fork is a merge. [Merges](../specs/format.md#merges) gains the two substitutions for a merge into a namespace whose `rebased_from` identifies the merge's source. In such a namespace, the range rule starts after `resume_after_seq` and considers only the namespace's own `merges` rows, and the floor rule compares the namespace's own fork point with its retention floor. The same subsection also gains the rule that a commit with a merge record may have no operations. [Control and manifest payloads](../specs/format.md#a4-control-and-manifest-payloads) gains `rebased_from` beside `fork_basis`, with the original fork's namespace ID and `resume_after_seq`. The field is set in the new fork's first manifest and can't change after creation. [Forking a namespace](../specs/format.md#92-forking-a-namespace) says that a rebase creates a fork whose first manifest records `rebased_from`. The change feed's description of the merge record ([Change feed](../specs/format.md#67-change-feed)) says that a replay commit's merge record identifies the range that the replay commit examined, and that its conflict report identifies the changes that it left out. [Fixed logical bounds](../specs/format.md#c3-fixed-logical-bounds) says that the merge allowance bounds a merge, in place of the reference server's limit on the total estimated bytes of the commit requests it has admitted (`max_admitted_bytes`).

The API gains the merge and rebase routes, the next-range query, `rebased_from` on the namespace object, `merge_conflict` in the error registry, and the `filesystem.merges` feature key and its row in the conformance suite's list of features. For the resolver's read of the base, reads of a namespace accept one of its fork pins as `snapshot_id`, the inode revision content read (`GET /inodes/{inode_id}/revisions/{revision_no}/content`) accepts `snapshot_id`, and `fork_basis` on the namespace object gains `source_pin_id`, so that any client can read a fork's source through the fork's pin. It also gains four error details: `split_at_seq`, `conflict_count`, `conflict`, and the details of a `rebootstrap_required` from the fork's own floor, which add `source_namespace_id` and the range's start as `after_seq` to the existing `retention_floor_seq`. `conflict` is an object in the shape of a conflict report entry. It's the first nested object in the otherwise flat details, so the conflict report entry gets its own schema in the OpenAPI document, where the SDK generators see it. The self-hosting guide gains the merge allowance, `max_merge_bytes`, in its settings and in its sizing formula, and says that a merge isn't charged to the publication queue's budgets. The `merge_too_large` and `commit_queue_full` rows in the [Standard error contract](../specs/api.md#3-standard-error-contract) describe the merge allowance in place of `max_admitted_bytes`. [Commit responses and safe retry](../specs/api.md#52-commit-responses-and-safe-retry) states the receipt lookup that both routes make before they read the merge index. The two notes that no route commits a merge come out. The `rebootstrap_required` advice to merge through a new fork changes to say that the caller copies the changes that it wants to keep into a new fork of the head through ordinary reads and writes, because a fork in that state can't merge or rebase.

## What the lab measures

The lab's merge scenario forks namespaces of 10,000 and of 100,000 files, changes a fraction of the files in the fork, and merges, recording how long each step takes against the request deadline, how long the fold after the merge takes, and how many pages a client reads to see the merge. When the route leaves the fold to the next maintenance pass, the scenario measures how large a WAL tail readers read until the pass runs. A second scenario makes main change files that the fork also changed, rebases, resolves with a simulated agent, and merges the new fork, recording the report's size, the cost of a replay commit against a merge, and how long the replay commit's classification through the pinned manifest takes against a merge's classification at the cached head. A third runs the fork family's collection pass against a namespace that has merged, and checks that the fork's content that main now references stays and the rest is collected.

## Alternatives considered

- **A replay commit that isn't a merge.** The replay commit could be a plain commit with the merge's admission exceptions and no merge record. Then every exception that the format ties to the merge record, and the replay commit's retry identity and its continuation across pieces, would need rules of their own. Making the replay commit a merge into the new fork is two substitutions instead of six rules.
- **A rebase that replays the whole range in one call.** The replay commit would then be bounded by the WAL object like a merge, and a range that a merge had to split couldn't be rebased at all. A rebase that continues into the same new fork across calls replays any range that a merge could.
- **End-state validation of a merge's bindings.** The writer could validate a merge commit's bindings as a set instead of operation by operation, checking that every bound name is unique in its parent, that every live inode is bound exactly once, and that every parent is present. The check would run on the merge's own writer path, which already has the admission exemptions and the range and floor rules, and the planner's dependency ordering, the temporary names, and the collision check would go. This design keeps the dependency ordering, because it's contained in one module and the writer's model doesn't change. The set check is the refactor to make if LoonFS adds a second kind of commit that the server plans, or if the dependency ordering grows.
- **The caller supplies the range.** Retries would then be identical by construction, at the cost of one more request per merge for the next-range query. The receipt lookup in the route provides the same guarantee with a simpler client.
- **Reading the base from the fork or from main's current view.** On the fork, the fork's own floor can move past the fork point, and then the fork keeps only the newest revision at or below its floor. On main, main's floor can move past the fork point while the resolver works.
- **Reading the base's bytes by content reference.** The embedded API's bare content reference read could read `base.content_ref` on main or on the new fork. But it has no HTTP route, and it requires the reading namespace's current view to publish the content. Main stops publishing the content once main's floor passes the fork point, and the new fork stops once a base compaction keeps only the newest revision at or below the new fork's floor.
- **Creating the new fork under the merge rights.** A subject that can merge into `ns` but isn't an administrator could then rebase on its own. That would escalate nothing, because the new fork inherits main's access rows, and the subject could do in it only what it can do in main. But forking as a subject needs an administrator, and changing that is a change to the fork route's authorization, not to merging.
- **Hosting the loop on the server.** The server could run merge sessions behind routes that start a session, read it, continue it after the resolver finishes, and abandon it, with the session record under main's extension prefix and a collection pass for finished sessions, as the grep extension hosts its index steps. That's four routes, a record format, and a collection pass that a hand-off to the resolver doesn't need, because a crashed CLI resumes from its record and the store.
- **A server route that keeps the request open until the merge completes.** The server would keep a merge's progress in memory while a resolver works for minutes or hours, and a restart would lose it.
- **The rebase in the loop instead of in core.** The replay commit is the planner's conflict classification applied to a different destination, and a client can't make one without rows that the API doesn't expose or a second implementation of the rule.
- **The resolver inside the server.** The server can't resolve a conflict, because resolving one depends on what the files mean to the application, and for this workload the resolver is an agent.
- **A session as a queue entry with ordering and tests.** That's a merge queue, which is a product built on top of merges, and merging a fork doesn't need one.
