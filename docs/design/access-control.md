# Access control

A namespace can have per-item access grants, which LoonFS core enforces on reads and writes. Grants are ordinary metadata. They're published through the WAL, folded into a row family, retained like attributes, and evaluated against the same view that a commit is validated against. The application is responsible for identity, group membership, role names, and sharing workflows. Core defines the stable set of rights, stores the grants and the inheritance boundaries, and evaluates every access check.

There's one kind of namespace, and these rules decide how LoonFS checks each request:

- Every namespace can have access rows. A namespace that never receives an access update has none, and LoonFS stores and evaluates nothing for it.
- A request without [principals](#principals-subjects-and-actors) acts as the "token holder" (the holder of the deployment token), which has every right on every item.
- A request with principals is checked against the access rows, as described in [Effective rights](#effective-rights) and the sections after it.
- The operations that require an administrator when a request includes principals (the change feed, diffs, a fork's next merge range, bare content reference reads, snapshots, forks, and namespace deletion) act as the token holder when the request doesn't.

A namespace has no access configuration. The create request includes nothing about access, and a fork starts with its source's access rows, as it starts with every other row of its source.

Why is there one kind of namespace? A request without principals already has every right, so an application that never sends principals is never checked, and a namespace without access rows costs nothing. A per-namespace mode that turned checks off could add one behavior to that, which is ignoring the principals that a client does send. That behavior grants authority silently, and it isn't a capability that the access rows lack. With one model, every rule in the API specification applies to every namespace, and every read path evaluates rights the same way.

In the storage format, [Access rows](../specs/format.md#18-access-rows) defines access rows and effective rights. The API specification defines the identity headers, the `update_access` operation, and the rights each operation requires (see [Identity headers](../specs/api.md#identity-headers) and [Authorization](../specs/api.md#authorization)). This note explains the model and why it takes this shape.

## Goals and limits

This section describes what the access model supports and what it leaves out.

The model supports familiar sharing for a hosted product inside one namespace, such as viewer and editor roles, limited-access folders, upload-only drop boxes, and delegated administration. Permission changes are atomic with hierarchy changes, including moves. There's one filesystem planner, one publication pipeline, and one authoritative permission state.

The model doesn't include:

- explicit deny entries,
- per-item ownership or ownership transfer,
- "this folder only" grants that don't inherit,
- permanent erasure as a permission,
- commenting or preview-only roles,
- mounts, or
- an index for finding everything that a principal can reach.

Core access control protects each item against bugs in the application backend's authorization, and it provides one evaluation point for every read surface. It doesn't protect against the holder of the deployment token, because the token holder asserts the principals, and a request without principals acts as the token holder. Ordering is a separate matter from trust. An authority change made on behalf of ordinary subjects is ordered with the commits it affects, whoever submitted it.

For a request with principals, nothing is partially filtered, and each surface is exactly one of these:

- authorized against the request's principals,
- administrator-only, or
- token-holder-only.

Why is the change feed administrator-only? There's no rule for `moved` and `deleted` events whose source or destination the request's principals can't see. So a client that synchronizes through the change feed either sends an administrator's principals, or sends no principals and acts as the token holder.

## Principals, subjects, and actors

This section describes who a request acts as, and which grants apply to it.

A grant identifies a "principal" by an opaque ID that the application assigns and never reuses. Core doesn't distinguish between a user, a group, and a public audience. The application resolves membership, and it sends the applicable principal IDs with each request in the `Loonfs-Principals` header. LoonFS never resolves a principal ID, and it doesn't check which identity domain an ID came from.

The "actor" is the identity of a request, sent in the `Loonfs-Actor` header. LoonFS records the actor as the author of a commit, includes the actor in the commit's fingerprint, and records the actor as the owner of an upload session. A request with principals acts for a "subject", whose rights are the union of its principals' grants. The table in [Identity headers](../specs/api.md#identity-headers) lists which operations read each header and which ones require it.

Core doesn't assume that the actor ID is in the principal set, so the application includes the actor ID in the principals when the subject has direct grants. The number of principals in a request is capped, and the capability document advertises the cap. The application keeps each request under the cap, and core doesn't need an index to admit a request.

Every upload session operation requires an actor, as a commit does. An upload session records the actor ID when the session is created, and only that actor can use the session. A request from another actor returns `upload_not_found`. Creating an upload doesn't check any rights. The application admits and limits staging, and LoonFS authorizes the destination when a commit uses the upload. The semantic commit fingerprint includes the actor ID, so a retry with the same commit ID and a different actor fails with `commit_id_reuse_conflict`. A retained receipt is returned to the same actor, whatever the actor's current rights are.

Why doesn't a request have a subject ID separate from its actor? The actor is already recorded on every commit and included in its fingerprint. A second ID would let a request attribute a commit to one ID while its uploads and its fingerprint belong to another, as in support impersonation. An application that impersonates a user can record that in its own audit log or in the commit message, so LoonFS doesn't need a second identity for it. A request has two identity headers: the actor, for attribution, fingerprints, and upload ownership, and the principals, for rights.

Why doesn't a namespace record a principal scope? LoonFS never resolves principal IDs, and the client would send both the principals and the scope to check them against. So a scope check could catch only the client's own wiring mistake, such as sending principals from one identity domain to a namespace that belongs to another. A prefix on every principal ID (for example `acme:usr_8f3c`) avoids that mistake by convention, without a manifest field, a header, two rules about which headers are sent together, and an error path.

Why is a receipt returned without checking current rights? The write has already committed, and the receipt reveals nothing that the actor didn't author.

## Rights

The format stores two kinds of rows: directory bindings, which belong to the parent directory, and inode rows, which belong to the item. Rights follow the same split:

- `create` and `remove` apply to a directory's entries.
- `read` and `write` apply to an item's own data. `read` covers the item's current content, its older revisions, and its snapshots.
- `manage` applies to the item's access row.
- `admin` exists only on the root row.

The application builds roles from these rights and stores the resulting grants, so changing what a role name means never changes a stored grant. These are the reference bundles. (Administrator isn't a bundle. It's the `admin` right on the root row.)

| Role | read | write | create | remove | manage |
| --- | :---: | :---: | :---: | :---: | :---: |
| Upload-only | | | Yes | | |
| Viewer | Yes | | | | |
| Contributor | Yes | Yes | Yes | | |
| Editor | Yes | Yes | Yes | Yes | |
| Manager | Yes | Yes | Yes | Yes | Yes |

Why does `read` cover older revisions and snapshots? A separate right for history would add "together with `read`" to every rule for a historical read, and grep would need its own rule for later pages of a search, which serve a file that changed after the first page only to a subject that can read history. With one right, each read rule checks one right. The cost is that a product can't offer a role that reads current content but not older revisions.

## Effective rights

This section describes how grants on an item and its ancestors combine into a subject's effective rights.

A grant on a directory applies to the directory and every descendant, until a directory marked as a "boundary" stops inheritance. The boundary is the only subtractive concept in the model. There are no deny entries, so the union of grants along a chain of ancestors doesn't depend on order.

To find the effective rights, LoonFS walks the item's ancestors. Every read already performs this walk to find a covering subtree tombstone, and the path depth limit bounds it. A read keeps the rights it finds for each inode, and a walk stops at the first ancestor whose rights it already has. So a listing walks the directory's ancestors once, and each child adds its own row to the directory's rights. Only a child whose own row is a boundary doesn't add the directory's rights.

For a deleted item, LoonFS evaluates the item's own row and boundary first. Only then does the walk follow the parent saved in the tombstone into that parent's current ancestors. The tombstone fills in one missing link to the parent, and it never replaces or bypasses the item's own row. So deletion doesn't need to scan descendants.

## Reading without a subject

This section describes when an access check reads few rows or none.

A request without principals acts as the token holder, so LoonFS evaluates no rights for it and reads no access rows. A namespace whose application never sends principals makes no additional object-store requests for access control. For a request with principals, LoonFS checks for an administrator first, and that check reads the root row and nothing else. If one of the request's principals has `admin` on the root row, the request has every right, and LoonFS doesn't walk the item's ancestors.

Why does an administrator's check stop at the root row? `admin` is never inherited, and boundaries don't apply to it, so no row below the root can change an administrator's rights.

## Names, paths, and hidden items

This section describes what a subject can see of names, paths, and items that it has limited rights on.

Names belong to the directory. A subject with `read` on a directory sees every entry's name, kind, and inode ID, including a boundary folder that the subject can't open. Attributes are returned only for children that the subject can `read`. A product that prefers to hide such folders from listings needs a per-listing filter or an additional right.

Resolving a path never requires rights on the intermediate directories. LoonFS checks only the resolved target, plus the parents that an operation changes. Sharing one file directly grants rights on that inode alone. Its siblings stay hidden, because listing the parent needs `read` on the parent, and the file's entry response doesn't identify its parent.

An operation on an item fails in one of two ways, depending on the subject's rights:

- If the subject has no right on the item, the request fails with `path_not_found` or `inode_not_found`, the same as if the item didn't exist.
- If the subject has some right on the item, but not the one the operation needs, the request fails with `forbidden`. For example, a subject that has `write` but not `read` on a file gets `forbidden` when it reads that file.

## Authorizing operations

LoonFS authorizes a commit's operations in request order, against the tentative view, inside the planner's per-operation loop. So each operation sees the effects of earlier operations in the request, and of commits that were accepted earlier in the batch. After a lost numbered WAL put, LoonFS plans the retry again from scratch, so it authorizes the retry again.

Trash and grep pages can be shorter than their limit and still include a cursor. A short page reveals that hidden entries exist. (Grep's candidate budget already reveals the same information.)

A presigned download URL stays valid for its whole lifetime after it's issued, and a stream that's in progress can't be retracted. Both are stated limits of the design.

## Moves and relocation

A move places an inode in a new inheritance context. After the move, the moved inode and its descendants inherit whatever the destination's ancestors grant, and the inode keeps its history and direct grants. A copy is different. It creates a new inode with no history and no direct grants, and the copier needs `read` on the source. So a copy discloses only bytes that the copier could already read, and could upload again anywhere it has `create`.

A move requires `read` on the moved item, besides the "entry rights", which are `remove` on the source parent and `create` on the destination parent. A restore to a destination other than the original parent requires `read` on the restored item in the same way, with the saved original parent as the source. A restore in place needs only the entry rights. The rule has no exception for a rename within one directory or for a boundary directory, so one check covers every move.

The `read` requirement limits what a move can expose to what the mover can already read and copy. Rights inherit downward, so a subject that can read an item can read everything below it, down to the first boundary, and could copy all of it to the destination. A boundary directory's rights don't change when one of its ancestors moves, so a move exposes nothing below a boundary. The check evaluates the mover's rights on the moved inode once, scans no descendants, and writes no rows.

The cost is that a product can't let a subject organize items without sharing them. A subject that can read an item, and has the entry rights, can move the item into a folder that grants other principals more than they had. A product that wants to stop such moves does so with its own policy.

Why is there no `share` right, and why isn't a move authorized as a grant? Authorizing a move as an implicit grant means comparing every principal's effective rights on the moved inode before and after the move, which costs two ancestor walks and a per-principal comparison on every move. The `read` requirement bounds the same exposure with one rights check. A `share` right, which lets a subject grant the rights it has, would be a second way to change an access row, so access rows change only through `manage`. The cost is that an application that wants editors to share what they hold either grants them `manage`, which also allows boundary changes, or enforces that policy itself.

## Recursive deletion

Deleting a directory writes one subtree tombstone and doesn't inspect any descendants. So a limited-access boundary protects confidentiality, but it doesn't stop a subject with `remove` on an ancestor from making the subtree unavailable by deleting it, although a restore can bring the subtree back while the deletion is in the trash.

The deleter never sees the restricted subtree's bytes or names. Anyone with `remove` on the original parent can restore the deletion. A subject with `manage` on a restricted descendant might need a subject with `remove` on the original parent, or an administrator, to restore the deletion root, because only the root of a deletion can be restored. Permanent erasure is outside this permission model.

There's no server-side operation for a recursive copy or a recursive read. The CLI performs them as sequences of ordinary operations, and each operation is authorized on its own.

## Administrators

An administrator is a principal with the `admin` right on the root inode's row, and that right changes through the writer like any other grant. LoonFS checks `admin` only in the root-row check. It never appears in a union of grants along a chain, and boundaries don't apply to it. Ordinary grants on the root are still subject to boundaries, so a namespace-wide membership still respects limited-access folders.

The root inode starts like every other inode, with no stored access row, so a new namespace has no administrators. The application sets the first administrators with an `update_access` request on the root that sends no principals, so the request acts as the token holder. Until then, a request with principals has no right on any item, and it gets `path_not_found` or `inode_not_found`. Core allows removing the last administrator, and warning the user is the application's job. The token holder can set the root row again at any time with the same kind of request.

Why isn't administrator authority stored in the manifest? Manifests and WAL objects are numbered independently, and the writer revalidates the manifest on an interval instead of before every put. So authority stored only in the manifest wouldn't be ordered with commits.

Why is the root row set by an ordinary access update? Every other row is computed from one WAL delta. A root row written from the manifest at sequence 0 would be the one exception, and the manifest would have to keep the initial grants for the namespace's whole lifetime, because a reader of a namespace with no folded rows would build the row from them. Because the token holder can set the root row as the namespace's first commit, the manifest stores nothing about access.

Why is there no maintenance job that restores an administrator? A request without principals acts as the token holder, which can update the root row directly, so restoring an administrator is an ordinary commit. A recovery job would also be the one maintenance job that commits, and so the one that needs the writer session.

## Delegation and boundaries

This section describes how a subject changes access for other principals.

A subject with `manage` on an item can make any change to the item's access row, except a change to the root's set of administrators, which requires an administrator. Group and public audiences are principal IDs, and core doesn't distinguish them from users.

Setting or clearing a boundary is one `update_access` operation that includes the new boundary flag and the complete map of direct grants. LoonFS authorizes it against the preceding state and publishes it atomically. Core doesn't copy any grants when a boundary is set. An application can offer "preserve current access" as a convenience that previews the resulting direct grants, checks the per-row cap, and explains that the retained grants stop following the ancestors. That transformation is always application behavior, never core behavior.

Core doesn't stop a subject from removing its own `manage` right. Previewing that change is the application's job, and an administrator or the token holder can restore the right.

## Why grants are their own rows

Grants aren't stored as attributes or as an extension. A copy applies the source's attribute map to the new inode, which must never happen to grants. An extension must be rebuildable from core state and invisible to core reads, and authoritative grants can't be.

## Ordering and revocation

Access changes follow these ordering and revocation rules:

1. Every authority that commit authorization checks is a row in the same view that the write is validated against (the inode rows and the root row).
2. Commits are ordered by numbered WAL puts through the namespace's single writer session. A revocation accepted at sequence N applies to every commit validated after it, in the same batch or a later one. A commit that was authorized before N, and isn't published yet, is planned again on retry and refused.
3. LoonFS acknowledges a revocation only after it loads a view that includes every earlier put, whether or not the earlier put's submitter has received its result. A timeout on the earlier write doesn't show that the write wasn't published.
4. A `strong` read sees a revocation on its next read after the revocation is published. An `eventual` read sees the revocation within its maximum staleness (1 second by default), and a read in the runtime that has the writer session sees the revocation at once, because that read starts from the writer position (see [Read consistency](../specs/api.md#read-consistency)). Commit authorization (rule 2) doesn't depend on a read's consistency, because commits are validated against the writer's view.
5. A snapshot read is authorized at the current head, for the historical inode that it returns.
6. Replaying a receipt returns the original result to the same actor without authorizing again.
7. The application is responsible for the freshness of group membership, on each request.
