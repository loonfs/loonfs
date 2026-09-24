# Optional access control

A namespace can carry per-item access grants that core enforces on reads and writes. Grants are ordinary metadata: they are published through the WAL, materialized into a row family, retained like attributes, and evaluated against the same view that validates a commit. The application owns identity, group membership, role names, and sharing workflows. Core owns the stable rights, the grants, the inheritance boundaries, and the decision.

A namespace chooses its access mode at creation and never changes it. In the default unrestricted mode, the bearer token identifies the backend, the backend authorizes callers at namespace granularity, and core performs no permission lookups. In an ACL namespace, per-subject operations require a subject context. There is no switch that makes an ACL namespace unrestricted.

The [storage format](../specs/format.md#18-access-rows) defines access rows, the access mode, and effective rights. The API specification defines the [identity headers](../specs/api.md#identity-headers), the `update_access` operation, and the [rights each operation requires](../specs/api.md#authorization-in-acl-namespaces). This note explains the model and why it takes this shape.

## Goals and limits

The model gives a hosted product familiar sharing inside one namespace: viewer and editor roles, limited-access folders, upload-only drop boxes, and delegated administration. Permission changes are atomic with hierarchy changes, including moves. There is one filesystem planner, one publication pipeline, and one authoritative permission state. An unrestricted namespace performs no additional object-store operations and consults no access row; the access mode is read from the manifest that every request already loads.

The model has no explicit deny entries, per-item ownership or ownership transfer, "this folder only" grants that do not inherit, conversion between modes, permanent erasure as a permission, commenting or preview-only roles, or mounts. No index answers "what can this principal reach".

Core access control defends against backend authorization bugs at item granularity and gives one evaluation point for every read surface. It does not defend against the holder of the deployment token, which asserts the subject and its principals. Ordering is a separate matter from trust: an authority change made on behalf of ordinary subjects is ordered with the commits it affects, whoever submitted it.

Each surface of an ACL namespace is exactly one of: authorized per subject, administrator-only, or token holder. Nothing is partially filtered. The change feed is administrator-only because there is no rule for `moved` and `deleted` events whose source or destination the subject cannot see. A client that synchronizes through the change feed would therefore need administrator authority, so ACL namespaces do not suit such clients.

## Principals, subjects, and actors

Grants name principals by opaque ids that the application assigns and never recycles. Core does not distinguish a user from a group from a public audience; the application resolves membership and sends the applicable principal ids with each request. Each ACL namespace records a principal scope in its manifest, and core refuses a subject from another scope before it reads any grant.

The actor is attribution, and the subject is who the request acts as. The subject id defaults to the actor, so a separate `Loonfs-Subject` header is needed only when the two differ, such as during support impersonation. The [identity header table](../specs/api.md#identity-headers) lists which operations read each header and which require it.

Core does not assume that the subject id appears in the principal set; the application includes it when the subject holds direct grants. The number of principals per request has a fixed cap, advertised in the capability document. The application fits under it, and core needs no index to admit a request.

In an ACL namespace, an upload session records the subject id at creation, and only that subject can use the session. Creating an upload checks no rights: the application admits and limits staging, and the commit authorizes the destination. The subject id joins the semantic commit fingerprint beside the actor id, so a retry with the same commit id and a different subject answers `commit_id_reuse_conflict`. A retained receipt is returned to the same subject and actor regardless of their current rights, because the write already committed and the receipt reveals nothing that the subject did not author.

## Rights

The format stores two kinds of thing: directory bindings, which belong to the parent directory, and inode rows, which belong to the item. Rights mirror that split. `create` and `remove` govern a directory's entries. `read`, `history`, and `write` govern an item's own data. `share` and `manage` govern the item's access row, and `admin` exists only on the root row.

The application composes roles from these rights and stores the resulting grants. Changing what a role name means never changes a stored grant. These are reference bundles:

| Role | read | history | write | create | remove | share | manage |
| --- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Upload-only | | | | Yes | | | |
| Viewer | Yes | | | | | | |
| Auditor | Yes | Yes | | | | | |
| Contributor | Yes | Yes | Yes | Yes | | | |
| Editor | Yes | Yes | Yes | Yes | Yes | Policy | |
| Manager | Yes | Yes | Yes | Yes | Yes | Yes | Yes |

"Policy" means the application decides whether its Editor bundle includes `share`. Administrator is not a bundle; it is the `admin` right on the root row.

## Effective rights

A grant on a directory applies to the directory and every descendant, until a directory marked as a boundary stops inheritance. The boundary is the only subtractive concept in the model. There are no deny entries, so the union of grants along a chain does not depend on order.

The walk is the one every read already performs to find a covering subtree tombstone, and the path depth limit bounds it. A listing computes the directory's effective rights once, and each child inherits them in constant time; only a child whose own row is a boundary differs.

A deleted item's own row and boundary are evaluated first. Only then does the walk follow the parent saved in the tombstone into that parent's current ancestry. The tombstone repairs one missing edge; it never replaces or bypasses the item's own row. Deletion therefore needs no descendant scan.

## Names, paths, and hidden items

Names belong to the directory. A subject with `read` on a directory sees every entry's name, kind, and inode id, including a boundary folder that the subject cannot open. Attributes are returned only for children the subject can `read`. A product that prefers to hide such folders from listings needs a per-listing filter or an additional right.

Path resolution never requires rights on intermediate directories. Only the resolved target is checked, plus the parents that an operation mutates. Sharing one file directly grants rights on that inode alone. Its siblings stay hidden, because listing the parent needs `read` on the parent, and the file's entry response does not name its parent.

An item on which the subject holds no right answers `path_not_found` or `inode_not_found`, with no distinction from absence. An item on which the subject holds some right, but not the one the operation needs, answers `forbidden`. A subject that holds `write` but not `read` on a file therefore receives `forbidden` when it reads that file.

## Authorizing operations

Commit operations are authorized in request order against the tentative view, inside the planner's per-operation loop. An operation therefore sees the effects of earlier operations in the request and of earlier accepted commits in the batch. A retry after a lost numbered WAL put plans again from scratch, so it authorizes again.

Trash and grep pages can be shorter than their limit and still carry a cursor. A short page reveals that hidden entries exist; grep's candidate budget already makes the same disclosure.

Presigned download URLs remain valid for their lifetime after issue, and in-flight streams cannot be retracted. Both are stated limits of the design.

## Moves and relocation

A move places an inode in a new inheritance context. Whatever the destination's ancestors grant, the moved inode and its descendants inherit after the move, with the inode's history and direct grants intact. A copy is different: it creates a new inode with no history and no direct grants, and the copier needs `read` on the source. A copy therefore discloses only bytes that the copier could already read and could upload again anywhere it holds `create`.

The rule treats the implicit grant that a move performs exactly like an explicit grant. A move that changes any principal's effective rights on the moved inode is authorized as if the mover had granted those rights directly. A move that confers nothing needs entry rights only.

This has several consequences:

- Reorganizing within one context, including a rename, confers nothing and needs only entry rights.
- A boundary folder can move anywhere, because its own boundary means its context never changes.
- A subject who can read and organize a file but cannot share it cannot move it into a folder that another principal can read.
- A subject without `read` on an item cannot gain it by moving the item, even with `share`, because the gain is not a subset of what the subject holds.
- The rule covers gains only. A move that removes other principals' access is an organizing action. A product that wants to restrict it, for example moving items into personal boundaries, does so by policy.
- One evaluation at the moved inode covers the subtree. Descendants below a boundary are unaffected, and descendants with direct grants gain at most what the moved inode gains.
- The check costs two ancestor walks and no descendant scan, and writes no rows. The conferred rights are inherited from the destination.

An undelete to a destination other than the original parent runs the same rule, with the saved original parent as the source. An undelete in place confers nothing.

## Recursive deletion

Deleting a directory writes one subtree tombstone and inspects no descendants. A limited-access boundary therefore protects confidentiality, but it does not prevent an authorized manager of an ancestor from making the subtree unavailable through recoverable deletion.

The deleter never sees the restricted subtree's bytes or names. Anyone with `remove` on the original parent can recover the deletion. A restricted descendant's manager may need an ancestor manager or an administrator to restore the deletion root, because only the root of a deletion can be undeleted. Permanent erasure is outside this permission model.

Recursive copy and recursive read have no server-side operation. The CLI performs them as sequences of ordinary operations, and each is authorized on its own.

## Administrators

Manifests and WAL objects are numbered independently, and the writer revalidates the manifest on an interval rather than before every put. Authority stored only in the manifest would therefore not be ordered with commits. Administrators are instead a right on the root inode's row, and that right changes through the writer like any grant.

`admin` is consulted only through the root-row check. It never appears in a chain union, boundaries do not apply to it, and administrator principals are skipped when a move's gain is computed. Ordinary grants on the root remain subject to boundaries, so a namespace-wide membership still respects limited-access folders.

The genesis administrators come from the manifest's immutable `access` field, which the genesis view materializes as the root row at access revision 0. Later revisions supersede it in commit order. The manifest keeps only the mode and the principal scope for authorization, and neither ever changes.

Core permits removing the last administrator; warning the user is the application's job. The `recover_administrator` maintenance job restores an administrator. That job publishes a root-row access revision through the writer, attributed to the request's actor and with no subject check. It is a commit, so it is ordered like every other.

## Delegation and boundaries

`share` lets a subject add or remove any grant whose rights are a subset of its own effective rights on that inode, while leaving the boundary unchanged. `manage` allows any change to the row except an `admin` assignment. Group and public audiences are principal ids, and core does not distinguish them from users.

Setting or clearing a boundary is one `update_access` that carries the new boundary flag and the complete direct grant map, authorized against the preceding state and published atomically. Core copies no grants when a boundary is set. An application can offer "preserve current access" as a convenience that previews the resulting direct grants, checks the per-row cap, and explains that the retained grants stop following the ancestors. That transformation is application behavior, never core behavior.

Core does not prevent a manager from removing its own `manage`. Previewing that change is the application's job, and an administrator can restore it.

## Why grants are their own rows

Grants are not attributes and not an extension. A copy propagates the source's attribute map onto the new inode, which grants must never do. An extension must be rebuildable from core state and invisible to core reads, which authoritative grants cannot be.

## Ordering and revocation

1. Every authority that commit authorization consults is a row on the same view the write validates against: the inode rows and the root row.
2. Commits are ordered by numbered WAL puts through the namespace's single writer session. A revocation accepted at sequence N is observed by every commit validated after it, in the same batch or a later one. A commit authorized before N and not yet published plans again on retry and is refused.
3. A revocation is acknowledged only after loading a view that includes every earlier put, whether or not the earlier put's submitter has heard its result. A timeout on the earlier write does not establish that it was not published.
4. Readers observe a revocation on their next read after it publishes, because every warm read probes the next WAL number.
5. Snapshot reads authorize at the current head, for the historical inode returned.
6. Receipt replay returns the original result to the same subject and actor without authorizing again.
7. Group membership freshness belongs to the application, per request.
