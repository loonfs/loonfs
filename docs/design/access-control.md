# Access control

A namespace can have per-item access grants, which LoonFS core enforces on reads and writes. Grants are ordinary metadata. They're published through the WAL, folded into a row family, retained like attributes, and evaluated against the same view that a commit is validated against. The application is responsible for identity, group membership, role names, and sharing workflows. Core defines the stable set of rights, stores the grants and the inheritance boundaries, and evaluates every access check.

LoonFS checks each request by these rules:

- Every request that reads or changes a namespace's contents includes one of two headers, which say whose authority the request uses. `Loonfs-Principals` lists the [principals](#principals-subjects-and-actors) of the user that the request acts for, and LoonFS checks the request against the access rows, as described in [Effective rights](#effective-rights) and the sections after it. `Loonfs-Context: service` puts the request in the "service context", which means that the application makes the request on its own account, with the whole authority of the deployment token (the credential that the application's backend authenticates to LoonFS with), and LoonFS checks no rights. A request with neither header, or with both, returns `invalid_request`.
- Every namespace can have access rows. A namespace that never receives an access update has no access rows, and LoonFS stores nothing for it. A request in the service context reads such a namespace as usual, and a request with principals has no right on any item in it until the application sets the root row (see [Administrators](#administrators)).
- The operations that require an administrator when a request has principals (the change feed, diffs, a fork's next merge range, bare content reference reads, snapshots, forks, and namespace deletion) are available in the service context.

These operations ignore both headers: creating a namespace (but not forking one), reading a namespace's metadata, fetching the capability document, running maintenance jobs, and the health routes. The [identity header table](../specs/api.md#identity-headers) lists which operations read the headers.

The create request includes nothing about access, and a fork starts with its source's access rows, as it starts with every other row of its source.

In the storage format, [Access rows](../specs/format.md#18-access-rows) defines access rows and effective rights. The API specification defines the identity headers, the `update_access` operation, and the rights each operation requires (see [Identity headers](../specs/api.md#identity-headers) and [Authorization](../specs/api.md#authorization)). This note explains the model and why it takes this shape.

## Goals and limits

This section describes what the access model supports and what it leaves out.

The model supports familiar sharing for a hosted product inside one namespace, such as viewer and editor roles, limited-access folders, upload-only drop boxes, and delegated administration. Permission changes are atomic with hierarchy changes, including moves, because access checks run in the same planner and publication path as every other commit.

The model doesn't include:

- explicit deny entries,
- per-item ownership or ownership transfer,
- "this folder only" grants that don't inherit,
- permanent erasure as a permission,
- commenting or preview-only roles,
- mounts, or
- an index for finding everything that a principal can reach.

Core access control protects each item against bugs in the application backend's authorization, and it provides one evaluation point for every read operation. It doesn't protect against the holder of the deployment token, because the holder asserts the principals, and can send the service context instead. Whoever submits an access change, LoonFS orders it with the commits it affects, as described in [Ordering and revocation](#ordering-and-revocation).

For a request with principals, LoonFS never returns a partly filtered result. Each operation is one of these:

- authorized against the request's principals, or
- administrator-only.

Why is the change feed administrator-only? There's no rule for `moved` and `deleted` events whose source or destination the request's principals can't see. So a client that synchronizes through the change feed either sends an administrator's principals, or sends the service context.

## Principals, subjects, and actors

This section describes who a request acts as, and which grants apply to it.

A "principal" is anyone a grant can apply to, such as a user, a group, or a public audience. A grant identifies a principal by an opaque ID that the application assigns and never reuses, and core doesn't distinguish between the kinds of principal. The application resolves membership, and it sends the applicable principal IDs with each request in the `Loonfs-Principals` header. LoonFS never resolves a principal ID, and it doesn't check which identity domain an ID came from. A request with principals acts for a "subject", whose rights are the union of its principals' grants.

LoonFS has three identity headers, and a request sends at most two of them. The "actor" is the identity of a request, sent in the `Loonfs-Actor` header. LoonFS records the actor as the author of a commit, includes the actor in the commit's fingerprint, and records the actor as the owner of an upload session. A commit and every upload operation require an actor, and a read can leave the actor out. The request also sends either `Loonfs-Principals`, for a user's rights, or `Loonfs-Context: service`, for the application's own requests. The table in [Identity headers](../specs/api.md#identity-headers) lists which operations read each header and which ones require it.

When the application makes a request for itself (from an indexer, a migration, a backfill, or an administration tool, or because the application has no per-user permissions at all), the request sends `Loonfs-Context: service` instead of principals. The application picks one of the two headers when it builds a client, and that client sends the same header on every request. For example, a client that acts for one user sends these headers to read `/reports/q3.csv`:

```text
Loonfs-Actor: usr_8f3c
Loonfs-Principals: usr_8f3c,grp_finance
```

An indexer that reads the same file sends these:

```text
Loonfs-Actor: svc_indexer
Loonfs-Context: service
```

Core doesn't assume that the actor ID is in the principal set, so the application includes the actor ID in the principals when the subject has direct grants. The number of principals in a request is capped, and the capability document advertises the cap. Because the application resolves group membership and keeps each request under the cap, LoonFS doesn't need an index of group membership to check a request.

An upload session records the actor ID when the session is created, and only that actor can use the session. A request from another actor returns `upload_not_found`. Creating an upload doesn't check any rights. The application admits and limits staging, and LoonFS authorizes the destination when a commit uses the upload. The semantic commit fingerprint includes the actor ID, so a retry with the same commit ID and a different actor fails with `commit_id_reuse_conflict`. A retained receipt is returned to the same actor, whatever the actor's current rights are.

Why must a request say its context, instead of acting as the service when it sends no principals? The application's backend uses one deployment token for all of its users, so the token can't say who a request is for, and the headers have to. If a request with no principals acted as the service, a client that left out the principals by mistake would have every right, and nothing would report the mistake. With two explicit headers, the same mistake returns `invalid_request`.

Why is a receipt returned without checking current rights? The write has already committed, and the receipt reveals nothing that the actor didn't author.

## Rights

The format stores two kinds of rows: directory bindings, which belong to the parent directory, and inode rows, which belong to the item. Rights follow the same split:

- `create` and `remove` apply to a directory's entries.
- `read`, `history`, and `write` apply to an item's own data. `read` covers the item's metadata and current content, and `history` covers its older revisions and snapshots, together with `read`.
- `share` and `manage` apply to the item's access row.
- `admin` exists only on the root row.

The application builds roles from these rights and stores the resulting grants, so changing what a role name means never changes a stored grant. These are the reference bundles:

| Role | read | history | write | create | remove | share | manage |
| --- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Upload-only | | | | Yes | | | |
| Viewer | Yes | | | | | | |
| Auditor | Yes | Yes | | | | | |
| Contributor | Yes | Yes | Yes | Yes | | | |
| Editor | Yes | Yes | Yes | Yes | Yes | Policy | |
| Manager | Yes | Yes | Yes | Yes | Yes | Yes | Yes |

"Policy" means that the application chooses whether its Editor bundle includes `share`. Administrator isn't a bundle. It's the `admin` right on the root row.

Why is `history` a separate right? A product can then offer a role that reads an item's current content but not what the content used to be, which is how hosted drive products commonly treat viewers. The cost is that every rule for a historical read says "together with `read`". Grep is also affected. A later page of a search reads files as they were at the first page's head, so a file that changed between pages is an older revision by then, and LoonFS returns it only to a subject with `history`.

Why are there two rights for changing an access row? `share` lets a product allow its editors to share items they have rights on, at their own level or below, without letting them change a boundary (a directory that stops inheritance, see [Effective rights](#effective-rights)) or grant more than they have. `manage` is for a role that controls all access to the item, such as the Manager bundle. [Delegation and boundaries](#delegation-and-boundaries) defines both.

## Effective rights

This section describes how grants on an item and its ancestors combine into a subject's effective rights.

A grant on a directory applies to the directory and every descendant, until a directory marked as a "boundary" stops inheritance. The boundary is the only subtractive concept in the model. There are no deny entries, so the union of grants along a chain of ancestors doesn't depend on order.

To find the effective rights, LoonFS walks the item's ancestors. Every read already performs this walk to find a covering subtree tombstone, and the path depth limit bounds it. A read keeps the rights it finds for each inode, and the walk stops at the first ancestor whose rights the read has already found. So a listing walks the directory's ancestors once, and each child adds its own row to the directory's rights. Only a child whose own row is a boundary doesn't add the directory's rights.

For a deleted item, LoonFS evaluates the item's own row and boundary first. Only then does the walk follow the parent saved in the tombstone into that parent's current ancestors. The tombstone fills in one missing link to the parent, and it never replaces or bypasses the item's own row. So deletion doesn't need to scan descendants.

## Access checks that read few rows

This section describes when an access check reads few rows or none.

A request in the service context has every right, so LoonFS evaluates no rights for it and reads no access rows. An application that sends only the service context makes no additional object-store requests for access control. For a request with principals, LoonFS checks for an administrator first, and that check reads the root row and nothing else. If one of the request's principals has `admin` on the root row, the request has every right, and LoonFS doesn't walk the item's ancestors.

Why does an administrator's check stop at the root row? `admin` is never inherited, and boundaries don't apply to it, so no row below the root can change an administrator's rights.

## Names, paths, and hidden items

This section describes what a subject can see of names, paths, and items that it has limited rights on.

Names belong to the directory. A subject with `read` on a directory sees every entry's name, kind, and inode ID, including a boundary folder that the subject can't open. Attributes are returned only for children that the subject can `read`. LoonFS doesn't hide such folders from listings, so a product that wants to hide them filters the listing itself.

Resolving a path never requires rights on the intermediate directories. LoonFS checks only the resolved target, plus the parents that an operation changes. Sharing one file directly grants rights on that inode alone. Its siblings stay hidden, because listing the parent needs `read` on the parent, and the file's entry response doesn't identify its parent.

An operation on an item fails in one of two ways, depending on the subject's rights:

- If the subject has no right on the item, the request fails with `path_not_found` or `inode_not_found`, the same as if the item didn't exist.
- If the subject has some right on the item, but not the one the operation needs, the request fails with `forbidden`. For example, a subject that has `write` but not `read` on a file gets `forbidden` when it reads that file.

## Authorizing operations

LoonFS authorizes a commit's operations in request order, against the tentative view, inside the planner's per-operation loop. So each operation sees the effects of earlier operations in the request, and of commits that were accepted earlier in the batch. After a lost numbered WAL put, LoonFS plans the retry again from scratch, so it authorizes the retry again.

Trash and grep pages can be shorter than their limit and still include a cursor. A short page reveals that hidden entries exist. (Grep's candidate budget already reveals the same information.)

A presigned download URL stays valid for its whole lifetime after it's issued, and a stream that's in progress can't be retracted. Both are stated limits of the design.

## Moves and relocation

A move changes an inode's ancestors, so it changes which grants the inode inherits. After the move, the moved inode and its descendants inherit whatever the destination's ancestors grant, and the inode keeps its history and direct grants. A copy is different. It creates a new inode with no history and no direct grants, and the copier needs `read` on the source. So a copy discloses only bytes that the copier could already read, and could upload again anywhere it has `create`.

The "entry rights" are `remove` on the source parent and `create` on the destination parent. A move requires the entry rights and `read` on the moved item. A move's "gain" is the set of rights that any principal has on the moved inode after the move and didn't have before. For example, if the destination folder grants `grp_finance` the `read` right and the source folder doesn't, the move's gain includes `read` for `grp_finance`. If a move has a gain, LoonFS authorizes the move as if the mover had granted the gained rights. The mover needs `manage` on the moved item, or `share` together with every gained right, or an administrator's rights. A move that only removes other principals' access has no gain and needs no extra authority. Administrator principals are skipped when LoonFS computes the gain, because their rights don't depend on the item's ancestors.

The rule has these consequences:

- A rename within one directory, and a move between directories whose ancestors grant the same rights, have no gain, so they need the entry rights and `read`.
- A boundary directory has no gain wherever it moves, because its own boundary stops inheritance, so moving it requires only the entry rights and `read`.
- A subject who can read and organize a file, but can't share it, can't move the file into a folder that another principal can read.
- A move can't grant the mover a right on the file that the mover doesn't already have on the file. For example, a subject with `share` but not `write` on a file can't move the file into a folder where the subject has `write`, because the gain (`write`) isn't one of the subject's rights on the file.
- One evaluation at the moved inode covers the subtree. Descendants below a boundary aren't affected, and descendants with direct grants gain at most what the moved inode gains.
- The check walks the source and destination ancestors once each, scans no descendants, and writes no rows. The gained rights are inherited from the destination.

A restore to a destination other than the original parent follows the same rule, with the saved original parent as the source, and it requires `read` on the restored item. A restore in place has no gain and needs only the entry rights.

Why does a move require `read` on the moved item? A subject with `remove` on one directory and `create` on another, and no right on an item between them, could otherwise relocate the item without seeing it. With the requirement, that subject gets `path_not_found` for the item, like every other operation on an item that the subject has no right on. (A file inherits its parent's `remove`, so the subject has some right on every file under the source directory, and a move of a file without `read` returns `forbidden`. The item that the subject has no right on is a boundary directory with no grant for the subject.) The requirement also matches what hosted drive products allow, which is that a subject moves only the items it can at least view.

Why isn't `read` on the item enough? A move isn't a copy. The moved inode keeps its history and direct grants, and after the move its ancestors are the destination's. A subject that can read a file and move it into a folder it manages would gain `manage` on the file itself, and a subject could grant a group `write` on a file by moving the file into the group's folder. Each of these moves has the same effect as a grant, so LoonFS requires the authority that a grant needs.

The cost is that a product can't let a subject organize items freely without letting it share them. LoonFS allows a move that removes other principals' access with only the entry rights and `read`, and a product that wants to restrict such moves (for example, moving a shared file into a folder that one user alone can open) enforces that itself.

## Recursive deletion

Deleting a directory writes one subtree tombstone and doesn't inspect any descendants. So a limited-access boundary protects confidentiality, but it doesn't stop a subject with `remove` on an ancestor from making the subtree unavailable by deleting it, although a restore can bring the subtree back while the deletion is in the trash.

The deleter never sees the restricted subtree's bytes or names. A subject with `remove` and `create` on the original parent can restore the deletion in place. A subject with `manage` on a restricted descendant might need such a subject, or an administrator, to restore the deletion root, because only the root of a deletion can be restored. Permanent erasure is outside this permission model.

There's no server-side operation for a recursive copy or a recursive read. The CLI performs them as sequences of ordinary operations, and each operation is authorized on its own.

## Administrators

An administrator is a principal with the `admin` right on the root inode's row, and an `update_access` commit sets or removes that right, like any other grant. LoonFS checks `admin` only in the root-row check. It never appears in a union of grants along a chain, and boundaries don't apply to it. Ordinary grants on the root are still subject to boundaries, so a namespace-wide membership still respects limited-access folders.

The root inode starts like every other inode, with no stored access row, so a new namespace has no administrators. The application sets the first administrators with an `update_access` request on the root in the service context. Until then, a request with principals has no right on any item, and it gets `path_not_found` or `inode_not_found`. Core allows removing the last administrator, and warning the user is the application's job. The application can set the root row again at any time with the same kind of request.

## Delegation and boundaries

This section describes how a subject changes access for other principals.

Two rights allow a subject to change an item's access row:

- `share` lets a subject add or remove any grant whose rights are a subset of its own effective rights on that inode, without changing the boundary.
- `manage` allows any change to the row, except a change to the root's set of administrators, which requires an administrator.

Group and public audiences are principal IDs, and core doesn't distinguish them from users.

Setting or clearing a boundary is one `update_access` operation that includes the new boundary flag and the complete map of direct grants. LoonFS authorizes it against the preceding state and publishes it atomically. LoonFS doesn't copy any grants when a boundary is set. An application that offers "preserve current access" copies the grants itself: it previews the resulting direct grants, checks the per-row cap, and explains that the retained grants stop following the ancestors.

Core doesn't stop a subject from removing its own `manage` right. Previewing that change is the application's job, and an administrator, or the application in the service context, can restore the right.

## Why grants are their own rows

Grants aren't stored as attributes or as an extension. A copy applies the source's attribute map to the new inode, which must never happen to grants. An extension must be rebuildable from core state and invisible to core reads, and authoritative grants can't be.

## Ordering and revocation

Access changes follow these ordering and revocation rules:

1. Every authority that commit authorization checks is a row in the same view that the write is validated against (the inode rows and the root row).
2. Commits are ordered by numbered WAL puts through the namespace's single writer session. A revocation accepted at sequence N applies to every commit validated after it, in the same batch or a later one. A commit that was authorized before N, and isn't published yet, is planned again on retry, and LoonFS rejects it if it needs a right that the revocation removed.
3. LoonFS acknowledges a revocation only after it loads a view that includes every earlier put, whether or not the earlier put's submitter has received its result. A timeout on the earlier write doesn't show that the write wasn't published.
4. A `strong` read sees a revocation on its next read after the revocation is published. An `eventual` read sees the revocation within the runtime's maximum staleness (1 second by default), and a read in the runtime that has the writer session sees the revocation at once, because that read starts from the writer position (see [Read consistency](../specs/api.md#read-consistency)). Commit authorization (rule 2) doesn't depend on a read's consistency, because commits are validated against the writer's view.
5. A snapshot read is authorized at the namespace's head when the read started (for a read view, when the view was opened), for the historical inode that it returns.
6. Replaying a receipt returns the original result to the same actor without authorizing again.
7. The application is responsible for the freshness of group membership, on each request.

## Forks and merges

This section describes what happens to access rows when a namespace is forked and when a fork is merged.

A fork copies every row of its source at the fork point, and access rows are rows, so a fork starts with its source's grants, boundaries, and administrators. A request in the fork is checked against the fork's rows. A grant that changes on the source after the fork doesn't reach the fork, and a grant that changes in the fork doesn't reach the source, until a merge carries it across.

A merge treats an inode's access row as one of its four row kinds, with its content, binding, and attributes (see [The conflict rule](merge.md#the-conflict-rule)). If the fork changed an inode's row and both sides now hold the same boundary and grants, there's nothing to merge. If the source's current row is one the fork started from, or one that an earlier merge of this fork wrote, the merge replaces it with the fork's row. If the source wrote a different row on its own since the fork point, the merge reports an `access` conflict, and a rebase skips that change and reports it. The row is the unit: there's no merge of two grant maps, so a resolver sets the row it wants once, in the new fork.

A merge with principals is authorized against the source's rows at merge time, as if the subject made each change directly. An access change needs `manage`, or `share` for a change that keeps the boundary and grants only rights that the subject has itself. A change to the root's administrators needs an administrator. The subject also needs `read`, in the fork, on every changed inode that is still visible there; a deletion needs no `read` in the fork, because the inode is already hidden, and the deletion is still authorized against the source. So a subject whose `write` was revoked on the source can't merge a fork's write, whatever the fork's rows say. A merge in the service context checks nothing.

Forking a namespace, creating a rebase's new fork, and reading a fork's next merge range need an administrator or the service context. An application forks on a user's behalf, and the fork carries every access row of its source, so a fork is as open as its source until the application changes the fork's rows. Narrowing a fork means changing each row that grants the access, as in any namespace, because replacing the root row leaves the grants below it in place.

Why is an access row merged like any other row, instead of staying with the source? A long-lived fork, such as one that stands in for a staging environment, manages access like any namespace, and a rule that kept access rows out of merges would be the one row kind with its own merge rule. The cost is the whole-row conflict when both sides change the same item's grants, which the resolver settles by writing the row once.

## Alternatives considered

- **A per-namespace access mode.** A namespace records whether requests are checked, and an "unrestricted" namespace ignores the principals that a request sends. A request in the service context already has every right, so an application that sends only the service context is never checked, and a namespace without access rows costs nothing. The mode adds one behavior to that: every right for a request that sends principals, without reporting it. It lets an application do nothing that the service context doesn't, and it makes every rule in the API specification depend on the namespace.

- **A principal scope.** A namespace records the identity domain that issued its principal IDs, and a request sends the scope with its principals. LoonFS never resolves principal IDs, and the client sends both the principals and the scope to check them against, so the check catches only the client's own wiring mistake, such as sending principals from one identity domain to a namespace that belongs to another. A prefix on every principal ID (for example `acme:usr_8f3c`) avoids that mistake by convention, without a manifest field, a header, a rule about which headers are sent together, and an error path.

- **A subject ID separate from the actor.** A request attributes a commit to the actor and acts as a subject whose ID can differ, as in support impersonation. The actor is already recorded on every commit and included in its fingerprint, and a second ID lets a request attribute a commit to one ID while its uploads and its fingerprint belong to another. An application that impersonates a user can record that in its own audit log or in the commit message.

- **Administrator authority stored in the manifest.** The manifest records the first administrators, and a reader builds the root row from it. Manifests and WAL objects are numbered independently, and the writer revalidates the manifest on an interval instead of before every put, so authority stored only in the manifest isn't ordered with commits. A root row built from the manifest at sequence 0 is also the one row that isn't computed from a WAL delta, and the manifest has to keep the initial grants for the namespace's whole lifetime, because a reader of a namespace with no folded rows builds the row from them. An `update_access` in the service context sets the root row as the namespace's first commit instead, and the manifest stores nothing about access.

- **A maintenance job that restores an administrator.** The job commits a root-row update without an access check when a namespace has lost its last administrator. A request in the service context can update the root row directly, so restoring an administrator is an ordinary commit. The job is also the one maintenance job that commits, and so the one that needs the writer session.
