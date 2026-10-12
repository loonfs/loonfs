# Optional access control

A namespace can have per-item access grants, which LoonFS core enforces on reads and writes. Grants are ordinary metadata. They're published through the WAL, materialized into a row family, retained like attributes, and evaluated against the same view that a commit is validated against. The application is responsible for identity, group membership, role names, and sharing workflows. Core defines the stable set of rights, stores the grants and the inheritance boundaries, and evaluates every access check.

A namespace's access mode is set when the namespace is created, and it can't be changed. In the default unrestricted mode, the bearer token identifies the backend, the backend authorizes callers for whole namespaces, and core doesn't look up any permissions. In an ACL (access control list) namespace, per-subject operations require a subject context. No setting makes an ACL namespace unrestricted.

In the storage format, [Access rows](../specs/format.md#18-access-rows) defines access rows, the access mode, and effective rights. The API specification defines the identity headers, the `update_access` operation, and the rights each operation requires (see [Identity headers](../specs/api.md#identity-headers) and [Authorization in ACL namespaces](../specs/api.md#authorization-in-acl-namespaces)). This note explains the model and why it takes this shape.

## Goals and limits

This section describes what the access model supports and what it leaves out.

The model supports familiar sharing for a hosted product inside one namespace, such as viewer and editor roles, limited-access folders, upload-only drop boxes, and delegated administration. Permission changes are atomic with hierarchy changes, including moves. There's one filesystem planner, one publication pipeline, and one authoritative permission state. In an unrestricted namespace, LoonFS makes no additional object-store requests and reads no access rows, because the access mode is read from the manifest that every request already loads.

The model doesn't include:

- explicit deny entries,
- per-item ownership or ownership transfer,
- "this folder only" grants that don't inherit,
- conversion between modes,
- permanent erasure as a permission,
- commenting or preview-only roles, or
- mounts.

There's also no index for finding everything that a principal can reach.

Core access control protects each item against bugs in the backend's authorization, and it provides one evaluation point for every read surface. It doesn't protect against the holder of the deployment token, because the token holder asserts the subject and its principals. Ordering is a separate matter from trust. An authority change made on behalf of ordinary subjects is ordered with the commits it affects, whoever submitted it.

Each surface of an ACL namespace is exactly one of these:

- authorized per subject,
- administrator-only, or
- token holder only.

Nothing is partially filtered, and the change feed is administrator-only.

Why is the change feed administrator-only? There's no rule for `moved` and `deleted` events whose source or destination the subject can't see. So a client that synchronizes through the change feed would need administrator authority, and ACL namespaces don't suit such clients.

## Principals, subjects, and actors

This section describes who a request acts as, and which grants apply to it.

A grant identifies a "principal" by an opaque ID that the application assigns and never reuses. Core doesn't distinguish between a user, a group, and a public audience. The application resolves membership, and it sends the applicable principal IDs with each request. Each ACL namespace records a principal scope in its manifest, and core rejects a subject from another scope before it reads any grant.

The "actor" is who the request is attributed to, and the "subject" is who the request acts as. The subject ID defaults to the actor, so a request needs a separate `Loonfs-Subject` header only when the two are different, such as during support impersonation. The table in [Identity headers](../specs/api.md#identity-headers) lists which operations read each header and which ones require it.

Core doesn't assume that the subject ID is in the principal set, so the application includes it when the subject has direct grants. The number of principals in a request is capped, and the capability document advertises the cap. The application keeps each request under the cap, and core doesn't need an index to admit a request.

In an ACL namespace, an upload session records the subject ID when the session is created, and only that subject can use the session. Creating an upload doesn't check any rights. The application admits and limits staging, and the commit authorizes the destination. The semantic commit fingerprint includes the subject ID alongside the actor ID, so a retry with the same commit ID and a different subject fails with `commit_id_reuse_conflict`. A retained receipt is returned to the same subject and actor, whatever their current rights are.

Why is a receipt returned without checking current rights? The write has already committed, and the receipt reveals nothing that the subject didn't author.

## Rights

The format stores two kinds of rows: directory bindings, which belong to the parent directory, and inode rows, which belong to the item. Rights follow the same split:

- `create` and `remove` apply to a directory's entries.
- `read`, `history`, and `write` apply to an item's own data.
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

## Effective rights

This section describes how grants on an item and its ancestors combine into a subject's effective rights.

A grant on a directory applies to the directory and every descendant, until a directory marked as a "boundary" stops inheritance. The boundary is the only subtractive concept in the model. There are no deny entries, so the union of grants along a chain of ancestors doesn't depend on order.

To find the effective rights, LoonFS walks the item's ancestors. Every read already performs this walk to find a covering subtree tombstone, and the path depth limit bounds it. A listing computes the directory's effective rights once, and each child inherits them in constant time. Only a child whose own row is a boundary has different rights.

For a deleted item, LoonFS evaluates the item's own row and boundary first. Only then does the walk follow the parent saved in the tombstone into that parent's current ancestors. The tombstone fills in one missing link to the parent, and it never replaces or bypasses the item's own row. So deletion doesn't need to scan descendants.

## Names, paths, and hidden items

This section describes what a subject can see of names, paths, and items that it has limited rights on.

Names belong to the directory. A subject with `read` on a directory sees every entry's name, kind, and inode ID, including a boundary folder that the subject can't open. Attributes are returned only for children that the subject can `read`. A product that prefers to hide such folders from listings needs a per-listing filter or an additional right.

Resolving a path never requires rights on the intermediate directories. LoonFS checks only the resolved target, plus the parents that an operation changes. Sharing one file directly grants rights on that inode alone. Its siblings stay hidden, because listing the parent needs `read` on the parent, and the file's entry response doesn't identify its parent.

An operation on an item fails in one of two ways, depending on the subject's rights:

- If the subject has no right on the item, the request fails with `path_not_found` or `inode_not_found`, the same as if the item didn't exist.
- If the subject has some right on the item, but not the one the operation needs, the request fails with `forbidden`. For example, a subject that has `write` but not `read` on a file gets `forbidden` when it reads that file.

## Authorizing operations

LoonFS authorizes a commit's operations in request order, against the tentative view, inside the planner's per-operation loop. So each operation sees the effects of earlier operations in the request, and of commits that were accepted earlier in the batch. A retry after a lost numbered WAL put plans again from scratch, so it's authorized again.

Trash and grep pages can be shorter than their limit and still include a cursor. A short page reveals that hidden entries exist. (Grep's candidate budget already reveals the same information.)

A presigned download URL stays valid for its whole lifetime after it's issued, and a stream that's in progress can't be retracted. Both are stated limits of the design.

## Moves and relocation

A move places an inode in a new inheritance context. After the move, the moved inode and its descendants inherit whatever the destination's ancestors grant, and the inode keeps its history and direct grants. A copy is different. It creates a new inode with no history and no direct grants, and the copier needs `read` on the source. So a copy discloses only bytes that the copier could already read, and could upload again anywhere it has `create`.

LoonFS treats the implicit grant that a move performs exactly like an explicit grant. If a move changes any principal's effective rights on the moved inode, it's authorized as if the mover had granted those rights directly. A move that confers nothing needs only entry rights.

This rule has several consequences:

- Reorganizing within one context, including a rename, confers nothing and needs only entry rights.
- A boundary folder can be moved anywhere, because its own boundary means that its context never changes.
- A subject who can read and organize a file, but can't share it, can't move it into a folder that another principal can read.
- A subject without `read` on an item can't gain `read` by moving the item, even with `share`, because the gain isn't a subset of the rights the subject has.
- The rule covers only gains. A move that removes other principals' access is an organizing action. A product that wants to restrict such moves (for example, moving items into personal boundaries) does so with its own policy.
- One evaluation at the moved inode covers the subtree. Descendants below a boundary aren't affected, and descendants with direct grants gain at most what the moved inode gains.
- The check costs two ancestor walks and no descendant scan, and it writes no rows. The conferred rights are inherited from the destination.

An undelete to a destination other than the original parent uses the same rule, with the saved original parent as the source. An undelete in place confers nothing.

## Recursive deletion

Deleting a directory writes one subtree tombstone and doesn't inspect any descendants. So a limited-access boundary protects confidentiality, but it doesn't stop an authorized manager of an ancestor from making the subtree unavailable with a recoverable deletion.

The deleter never sees the restricted subtree's bytes or names. Anyone with `remove` on the original parent can recover the deletion. A restricted descendant's manager might need an ancestor's manager or an administrator to restore the deletion root, because only the root of a deletion can be undeleted. Permanent erasure is outside this permission model.

There's no server-side operation for a recursive copy or a recursive read. The CLI performs them as sequences of ordinary operations, and each operation is authorized on its own.

## Administrators

Administrators are a right on the root inode's row, and that right changes through the writer like any other grant.

LoonFS consults `admin` only in the root-row check. It never appears in a union of grants along a chain, boundaries don't apply to it, and administrator principals are skipped when LoonFS computes a move's gain. Ordinary grants on the root are still subject to boundaries, so a namespace-wide membership still respects limited-access folders.

The genesis administrators come from the manifest's immutable `access` field, which the genesis view materializes as the root row at access revision 0. Later revisions replace it in commit order. For authorization, the manifest keeps only the mode and the principal scope, and neither one can be changed.

Core allows removing the last administrator, and warning the user is the application's job. The `recover_administrator` maintenance job restores an administrator. It publishes a root-row access revision through the writer, attributed to the request's actor, without a subject check. That revision is a commit, so it's ordered like every other commit.

Why isn't administrator authority stored in the manifest? Manifests and WAL objects are numbered independently, and the writer revalidates the manifest on an interval instead of before every put. So authority stored only in the manifest wouldn't be ordered with commits.

## Delegation and boundaries

This section describes how a subject changes access for other principals.

Two rights allow a subject to change an item's access row:

- `share` lets a subject add or remove any grant whose rights are a subset of its own effective rights on that inode, without changing the boundary.
- `manage` allows any change to the row except assigning `admin`.

Group and public audiences are principal IDs, and core doesn't distinguish them from users.

Setting or clearing a boundary is one `update_access` operation that includes the new boundary flag and the complete map of direct grants. LoonFS authorizes it against the preceding state and publishes it atomically. Core doesn't copy any grants when a boundary is set. An application can offer "preserve current access" as a convenience that previews the resulting direct grants, checks the per-row cap, and explains that the retained grants stop following the ancestors. That transformation is always application behavior, never core behavior.

Core doesn't stop a manager from removing its own `manage` right. Previewing that change is the application's job, and an administrator can restore the right.

## Why grants are their own rows

Grants aren't stored as attributes or as an extension. A copy applies the source's attribute map to the new inode, which must never happen to grants. An extension must be rebuildable from core state and invisible to core reads, and authoritative grants can't be.

## Ordering and revocation

Access changes follow these ordering and revocation rules:

1. Every authority that commit authorization checks is a row in the same view that the write is validated against (the inode rows and the root row).
2. Commits are ordered by numbered WAL puts through the namespace's single writer session. A revocation accepted at sequence N applies to every commit validated after it, in the same batch or a later one. A commit that was authorized before N, and isn't published yet, is planned again on retry and refused.
3. LoonFS acknowledges a revocation only after it loads a view that includes every earlier put, whether or not the earlier put's submitter has received its result. A timeout on the earlier write doesn't show that the write wasn't published.
4. A reader sees a revocation on its next read after the revocation is published, because every warm read probes the next WAL number.
5. A snapshot read is authorized at the current head, for the historical inode that it returns.
6. Replaying a receipt returns the original result to the same subject and actor without authorizing again.
7. The application is responsible for the freshness of group membership, on each request.
