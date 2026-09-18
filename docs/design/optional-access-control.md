# Optional access control

A namespace can carry per-item access grants that core enforces on every read and write. Grants are ordinary metadata: they are published through the WAL, materialized into a row family, retained like attributes, and evaluated against the same view that validates a commit. The application still owns identity, group membership, role names, and sharing workflows. Core owns the stable rights, the grants, the inheritance boundaries, and the decision.

A namespace chooses its access mode at creation and never changes it. The default mode is unrestricted, which is today's behavior: the bearer token identifies the backend, the backend authorizes callers at namespace granularity, and core performs no permission lookups. An ACL-enabled namespace requires a subject on every request and refuses requests that lack one. There is no runtime switch that makes an ACL-enabled namespace unrestricted.

This document defines the model, the durable representation, the ordering guarantees, and what ships first. Format rules referenced below are in the [storage format](../specs/format.md); wire rules are in the [API specification](../specs/api.md).

## Goals and non-goals

The design gives a hosted product familiar sharing inside one namespace: viewer and editor roles, limited-access folders, upload-only drop boxes, and delegated administration. It keeps permission changes atomic with hierarchy changes, including moves. It keeps one filesystem planner, one publication pipeline, and one authoritative permission state. It adds no object-store operations to unrestricted namespaces.

Out of scope for this version: explicit deny entries, per-item ownership and ownership transfer, a "this folder only" grant that does not inherit, live conversion between modes, permanent erasure as a permission, commenting or preview-only roles, mounts, a subject-scoped change feed, and a discovery index that answers "what can this principal reach". The last two are named separately at the end.

Core access control defends against backend authorization bugs at item granularity and gives one evaluation point for every read surface. It does not defend against the holder of the deployment token, which asserts the subject and its principals. Ordering is a separate matter from trust: an authority change made on behalf of ordinary subjects is ordered with the commits it affects, whoever submitted it.

## Principals, subjects, and actors

Grants name principals by opaque ids that the application assigns. Core does not distinguish a user from a group from a public audience; the application resolves membership and sends the applicable principal ids with each request. Ids are never recycled. Each ACL-enabled namespace records a principal scope string in its manifest. Core refuses a subject from another scope before it reads any grant.

Four request headers carry identity on an ACL-enabled namespace.

| Header | Meaning | When required |
| --- | --- | --- |
| `Loonfs-Actor` | Attribution, recorded on rows as today. It is also the subject id unless a subject header is present. | Every operation. |
| `Loonfs-Subject` | A stable, host-asserted identity used for upload ownership and commit replay. | Only when it differs from the actor, such as support impersonation. |
| `Loonfs-Principal-Scope` | The identity domain that issued the subject's principal ids. | With `Loonfs-Principals`. |
| `Loonfs-Principals` | The subject's applicable principal ids, comma-separated. | Every operation. |

Core does not assume the subject id appears in the principal set; the application includes it when the subject holds direct grants. The principal header has a fixed cap on count and total bytes, advertised in capabilities. The application fits under it. Core makes no promise that any reduction always fits and needs no index to admit a request.

Unrestricted namespaces ignore a complete subject context, as reads ignore the actor today. The HTTP binding answers `invalid_request` with the header as `param` when the context is incomplete or malformed. An ACL namespace answers `forbidden` with both scopes when the subject scope does not match its manifest.

Upload sessions record the subject id at creation. Only that subject may put, sign, complete, abort, or read the session. The subject id joins the semantic commit fingerprint beside the actor id: a retry with the same commit id and a different subject answers `commit_id_reuse_conflict`, and a retained receipt is returned to the same subject and actor regardless of their current rights, because the write already committed and the receipt reveals nothing that subject did not author.

## Rights

The format stores two kinds of thing: directory bindings, which belong to the parent directory, and inode rows, which belong to the item. Rights mirror that split. Directory rights govern entries. Inode rights govern the item's own data.

| Right | On a directory | On a file |
| --- | --- | --- |
| `read` | List entry names and kinds; stat the directory. | Stat, attributes, current bytes. |
| `history` | Historical listings through a snapshot. | Older revisions, revision listings, historical downloads, snapshot reads. |
| `write` | Attributes. | New revision, restore a revision, attributes. |
| `create` | Add an entry: put, mkdir, move or copy destination, undelete destination. | Not applicable. |
| `remove` | Remove an entry: delete, move source, replacing move destination, undelete source. | Not applicable. |
| `share` | Add or remove grants that are a subset of your own effective rights; read the grant list. | Same. |
| `manage` | Any grant change except `admin`; set or clear the boundary; read the grant list. | Same. |
| `admin` | Root inode only. Every right on every inode, boundaries ignored. Not inheritable. | Not applicable. |

The application composes roles from these rights and stores the resulting grants. Changing what a role name means never changes a stored grant. Reference bundles:

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

A grant on a directory applies to the directory and to every descendant, until a directory marked as a boundary stops inheritance. A boundary is a flag on a directory's own row, the only subtractive concept in the model. There are no deny entries, so the union of grants along a chain is order-independent.

```text
chain(inode):
    current = inode
    loop:
        yield current's row: its direct grants, with admin entries ignored
        if current's row has boundary = true: stop
        if current is the root: stop
        parent = current parent binding of current
        if parent is absent because current is a deletion root:
            parent = the original parent saved in current's tombstone
        current = parent

effective(subject, inode):
    if some principal of subject holds admin on the root row: every right
    else: union over rows r in chain(inode), p in principals(subject) of grants[r][p]
```

The walk is the one every read already performs to find a covering subtree tombstone. It is bounded by the path depth limit. For a listing, the directory's effective rights are computed once and each child inherits them in constant time; only a child whose own row is a boundary differs.

A deleted item's own row and boundary are evaluated first. Only then does the walk follow the parent saved in the tombstone's `deleted_direntry` into that parent's current ancestry. The tombstone repairs one missing edge; it never replaces or bypasses the item's row. A descendant inside a deleted subtree walks normal ancestry up to the detached deletion root and substitutes the saved edge there. Deletion itself needs no descendant scan.

## Names, paths, and hidden items

Names belong to the directory. A subject with `read` on a directory sees every entry's name, kind, and inode id, including a boundary folder they cannot open. Attributes and capabilities are returned only for children the subject can `read`. A product that prefers to hide such folders from listings needs a per-listing filter or an additional right, and should choose one.

Path resolution never requires rights on intermediate directories. Only the resolved target is checked, plus the parents an operation mutates. Sharing one file directly grants rights on that inode alone; its siblings stay hidden because listing the parent needs `read` on the parent, and the file's entry response does not name its parent.

An item the subject cannot `read` answers `path_not_found` or `inode_not_found`, with no distinction from absence, as a tombstoned inode does today. A visible item the subject lacks a right on answers `forbidden`, a new 403 code.

## Authorizing operations

Commit operations are authorized in request order against the tentative view, inside the planner's per-operation loop, so an operation sees the effects of earlier operations in the request and of earlier accepted commits in the batch. A CAS retry re-plans from scratch and therefore re-authorizes.

| Operation | Required rights |
| --- | --- |
| `put_file`, path absent | `create` on the parent directory. |
| `put_file`, path occupied, `replace` | `write` on the existing file. |
| `put_file`, path occupied, `no_replace` | `create` on the parent, then `path_conflict`. |
| `create_directory` | `create` on the parent; with `parents: true`, `create` on the deepest existing ancestor. |
| `create_directory_by_inode`, `create_file_by_inode` | `create` on the parent inode. |
| `put_file_revision_by_inode` | `write` on the file. |
| `move_path`, `move_by_inode` | `remove` on the source parent, `create` on the destination parent, and the relocation rule below. A replacing move also needs `remove` on the destination parent, because it deletes the destination file. |
| `copy_path` | `read` on the source file and `create` on the destination parent. A replacing copy needs `write` on the destination file instead of `create`. |
| `delete_path`, `delete_by_inode` | `remove` on the parent. Recursive deletes need nothing more. |
| `undelete` | `remove` on the original parent saved in the tombstone and `create` on the destination parent. A destination other than the original parent also runs the relocation rule. |
| `restore_revision` | `write` and `history` on the file. |
| `update_attributes` | `write` on the item. |
| `update_access` | `share` on the item when every added or removed grant is a subset of the caller's effective rights on it and the boundary flag is unchanged; `manage` otherwise. On the root row, any change to an `admin` assignment requires `admin`, and other updates must carry the `admin` assignments unchanged. |

Read operations.

| Operation | Required rights |
| --- | --- |
| `get_path_entry`, `get_inode` | `read` on the target. |
| `list_path_entries`, `list_inode_children` | `read` on the directory. |
| `get_file_bytes` for the current revision, with or without its number | `read` on the file. |
| `get_file_bytes` for an older revision, `get_file_revision_bytes_by_inode` for an older revision, `list_file_revisions`, `list_file_revisions_by_inode` | `read` and `history` on the file. |
| `create_download`, `create_download_by_inode` | Same as the corresponding content read. |
| Any read with `snapshot_id` | `read` and `history` on the historical inode the snapshot resolves, evaluated at the current head, never on today's occupant of the same path. A snapshot listing evaluates each historical child at head and returns more than name and kind only for children the subject can `read`. |
| `list_trash` | `read` on each entry's original parent. Entries the subject cannot see are filtered. |
| `grep` | `read` on the scope directory named by `path_prefix`, the root when absent, else `path_not_found`. `read` on each candidate file, evaluated at the page's head before its content is read. Candidates the subject cannot read are filtered and count against the candidate budget. |
| `list_changes` | Administrator. |
| `get_namespace`, `get_capabilities` | The bearer token. `get_namespace` reports the access mode. |

Trash and grep pages may be shorter than their limit and still carry a cursor, as grep already allows. A short page reveals that hidden entries exist; that is the disclosure grep's candidate budget already makes.

Namespace, upload, and maintenance operations.

| Operation | Required rights |
| --- | --- |
| `create_namespace` | The bearer token. An ACL-enabled namespace names its principal scope and root grants in the request. |
| `fork_namespace` | Administrator of the source. The fork carries the source's mode, rows, and scope, and records the actor as `created_by`. |
| `delete_namespace`, snapshot create, list, extend, and delete | Administrator. |
| `create_upload` | A subject id. No rights check at begin; the application admits and quotas staging, and the destination is authorized at commit. |
| Every other upload-session operation | The session's recorded subject. |
| Every `maintenance/v0` operation, including checkpoints, grep index management, and administrator recovery | The bearer token, as today. |

Presigned download URLs remain valid for their lifetime after issue and in-flight streams are not retractable. Both are stated limits of the design, not enforcement gaps.

## Moves and relocation

A move relocates an inode into a new inheritance context. Whatever the destination's ancestors grant, the moved inode's descendants now inherit, with the inode's history and direct grants intact. A copy does not have this property: it creates a new inode with no history and no direct grants, and the copier needs `read` on the source, so it discloses only bytes the copier could already read and could re-upload anywhere they hold `create`.

The rule treats the implicit grant a move performs exactly like an explicit one.

> A move that changes any principal's effective rights on the moved inode is authorized as if the mover had granted those rights directly. A move that confers nothing needs entry rights only.

For a move of inode X from source parent S to destination parent D:

```text
require remove on S and create on D

if X.boundary:
    gain = empty
else:
    for each principal P in chain(S) or chain(D),
            skipping principals that hold admin on the root row:
        before(P) = X.direct[P] | union over rows r in chain(S) of grants[r][P]
        after(P)  = X.direct[P] | union over rows r in chain(D) of grants[r][P]
        gain(P)   = after(P) - before(P)
    gain = union over P of gain(P)

if gain is empty:                                    allowed
else if mover holds admin:                           allowed
else if mover holds manage on X, before the move:    allowed
else if mover holds share on X, before the move,
        and gain is a subset of effective(mover, X): allowed
else:                                                forbidden
```

Consequences:

- Reorganizing within one context, including a rename, confers nothing and passes on entry rights alone.
- Moving a boundary folder passes anywhere, because its own boundary means its context never changes.
- A subject who can read and organize a file but cannot share it cannot move it into a folder another principal can read.
- A subject without `read` on X cannot gain it by moving X, even with `share`, because the gain is not a subset of what they hold.
- The rule covers gains only. A move that removes other principals' access is an organizing action. A product that wants to restrict it, for example moving items into personal boundaries, does so by policy.
- One evaluation at X covers the subtree. Descendants below a boundary are unaffected, and descendants with direct grants gain at most what X gains.
- The check costs two ancestor walks and no descendant scan, and writes no rows; the conferred rights are inherited from the destination.

An undelete to a destination other than the original parent runs the same rule with the saved original parent as S. An undelete in place confers nothing.

## Recursive deletion

Deleting a directory writes one subtree tombstone and inspects no descendants. That stays. The consequence for access control is stated as a rule of the design:

> A limited-access boundary protects confidentiality. It does not prevent an authorized manager of an ancestor from making the subtree unavailable through recoverable deletion.

The deleter never sees the restricted subtree's bytes or names. The deletion is recoverable by anyone with `remove` on the original parent. A restricted descendant's manager may need an ancestor manager or an administrator to restore the deletion root, because only the root of a deletion can be undeleted. Permanent erasure is outside this permission entirely.

Recursive copy and recursive read have no server-side operation. The CLI realizes them as sequences of ordinary operations, each authorized on its own.

## Administrators

Manifests and WAL objects are numbered independently, and the writer revalidates the manifest on an interval rather than before every put. Authority stored only in the manifest would therefore not be ordered with commits. Administrators are instead a right on the root inode's row and change through the writer like any grant.

- `admin` is valid only on the root inode's row. A grant of `admin` anywhere else is `invalid_request`.
- A principal holding `admin` has every right on every inode. Boundaries do not apply to it. Ordinary grants on the root remain blockable by boundaries, so a namespace-wide membership still respects limited-access folders.
- `admin` is not inheritable. It is consulted only through the root-row check, never appears in a chain union, and administrator principals are skipped when a move's gain is computed.
- Adding, removing, or changing an `admin` assignment requires administrator authority in the preceding state. A root-row update by a non-administrator must carry every `admin` assignment unchanged; validation compares the `admin` subsets of the old and new maps and answers `forbidden` on any difference.
- Core permits removing the last administrator. The application warns; operator recovery exists.
- The genesis value comes from the manifest. Manifest 1 carries an immutable `access` field with `kind`, `principal_scope`, and `root_grants`, the root row's grants at genesis (normally `admin` for each initial administrator), inside the identity set that every successor manifest copies verbatim. The genesis view materializes the root row at access revision 0 from that field. Later revisions supersede it in order.
- Operator recovery is a maintenance operation that publishes a root-row access revision through the writer with the system actor and no subject checks. It is a commit, so it is ordered like every other.

Nothing consulted by authorization lives only in the manifest. The manifest keeps the mode and the principal scope, which never change.

## Delegation and boundaries

`share` lets a subject add or remove any grant whose rights are a subset of their own effective rights on that inode, leaving the boundary flag unchanged. `manage` lets a subject make any change to the row except an `admin` assignment. Group and public audiences are principal ids; core does not distinguish them from users.

Setting or clearing a boundary is one `update_access` carrying `expected_access_revision_no`, the new boundary flag, and the complete direct grant map. It is authorized against the preceding state and published atomically. Core copies nothing. The application may offer "preserve current access" as a convenience that previews the resulting direct grants, checks the per-row cap, and explains that retained grants become independent of ancestors from then on. That transformation is never core behavior.

Core does not prevent a manager from removing their own `manage`. The application previews it; an administrator can restore it.

## Durable records

An inode that has direct grants or a boundary has one row in the by-inode family, beside its inode row, so a listing or stat fetches it in the same scan. An inode with neither has no row. Illustrative, not a wire schema:

```json
{
  "kind": "access_revision",
  "inode_id": 42,
  "access_revision_no": 3,
  "committed_seq": 118,
  "commit_id": "cmt_...",
  "delta_index": 0,
  "updated_by": "usr_8f3c",
  "updated_at_ms": 1789776000000,
  "boundary": false,
  "grants": {
    "prn_550e8400": ["read", "history", "write"],
    "prn_group_eng": ["read"]
  }
}
```

| Element | Rule |
| --- | --- |
| WAL delta | `append_access_revision`, mirroring `append_attributes_revision`. The complete map and the boundary flag every time, never a diff. |
| Precondition | `expected_access_revision_no`, paired with the inode as attribute revisions are, with the same mismatch error. |
| Retention | The attributes rule verbatim: keep every revision above the floor and the newest at or below it; keep an empty map when it is the state at the floor, so a revocation never resurfaces an older grant; never drop rows because the inode is deleted, so undelete and trash evaluation see the same grants. |
| Root row | Revision 0 is materialized from the manifest's `access` field at genesis. It is the only row that may carry `admin`. |
| Caps | A fixed maximum of principals per row and a closed set of right names. Invalid durable maps are rejected, not truncated. |
| Manifest | An immutable `access` field: `{"kind": "unrestricted"}` or `{"kind": "acl", "principal_scope": "...", "root_grants": {...}}`. |
| Upload session | A recorded subject id. |
| Change feed | One new event, `access_changed`, carrying the inode id, revision, boundary flag, and complete grant map, the way `attributes_changed` carries the whole map. |
| Fingerprint | The `update_access` operation joins the preimage appendix, and the subject id joins it beside the actor id. |
| Errors | `forbidden` (403) for a visible item the subject lacks a right on. |

Grants are not attributes and not an extension. A copy propagates the source's attribute map onto the new inode, which grants must never do. An extension must be rebuildable from core state and invisible to core reads, which authoritative grants cannot be.

The namespace manifest, WAL segment, metadata segment, and upload session families change. There is no compatibility path: an older binary rejects the new family versions at load.

## Ordering and revocation

1. Every authority consulted by commit authorization is a WAL row on the same view the write validates against: the inode rows and the root row.
2. Commits are ordered by numbered WAL puts through the namespace's single writer session. A revocation accepted at sequence N is observed by every commit validated after it, in the same batch or a later one. A commit authorized before N and not yet published re-plans on retry and is refused.
3. A revocation is acknowledged only after loading a view that includes every earlier put, whether or not the earlier put's submitter has heard its result. A timeout on the earlier write does not establish non-publication.
4. Readers observe a revocation on their next read after it publishes, because every warm read probes the next WAL number. Cached views are never trusted past a probe.
5. Snapshot reads authorize at the current head, for the historical inode returned.
6. Receipt replay returns the original result to the same subject and actor without re-authorizing.
7. Group membership freshness is the application's, per request.

## What ships first

Each surface of an ACL-enabled namespace is exactly one of: authorized per subject, administrator-only, or token holder. Nothing is partially filtered.

| Surface | State | Notes |
| --- | --- | --- |
| Stat, listings, current content, current downloads | Per subject | One ancestor walk beside the tombstone walk; children in constant time. |
| Older revisions, revision listings, historical downloads | Per subject | `read` and `history`. |
| Snapshot reads | Per subject | On the historical inode, evaluated at head. |
| All commit operations | Per subject | Including the relocation rule. |
| Trash listing | Per subject | Same walk from the saved original parent. |
| Upload sessions | Per subject | Bound to the subject; destination authorized at commit. |
| Grep | Per subject | Enforced in the query's candidate step before the content read, memoized per parent directory. |
| Change feed | Administrator-only | Per-subject filtering of `moved` and `deleted` events is undefined in this version. |
| Snapshot management, fork, delete namespace | Administrator-only | |
| Maintenance group, checkpoints, grep index management, administrator recovery | Token holder | As today. Hosted deployments hide the group. |

A client that synchronizes through the change feed must not run with administrator authority to do so. ACL-enabled namespaces are therefore not offered to such clients until a subject-scoped feed exists.

Unrestricted namespaces perform no additional object-store operations and consult no new family. The mode is read from the manifest already loaded for every request; when it is unrestricted, the resolver is skipped entirely.

## Acceptance tests

Each guarantee above is demonstrated rather than inferred. The harness is the existing integration suite and the request-accounting tests.

Revocation and retention:

1. A warmed independent reader holds a cached view. The writer revokes principal P on inode X. The reader's next stat of X answers not found, and its next listing of X's parent shows X as name and kind only.
2. The same, after the WAL tail folds into segments.
3. Revocation to an empty map, then the retention floor advances past it and compaction runs. The empty row is retained at the floor and the earlier grant does not reappear.
4. A snapshot pinned before the revocation answers not found for X to P after it.

Administrators:

5. Remove an initial administrator through the writer, restart the process, replay. The root row lacks them. Fold and compact. The genesis value does not resurface.
6. A manager on the root replaces the map and drops an `admin` assignment: `forbidden`. The same replacement carrying `admin` unchanged: allowed.
7. A write authorized under P's `admin` that has not yet published re-plans after P's removal and is refused.
8. Operator recovery restores an administrator after all were removed and is ordered after every earlier put.

Snapshots and history:

9. A path-addressed snapshot read resolves the historical inode. The subject holds `read` and `history` on that inode at head but nothing on today's occupant of the path: allowed. The converse: not found.
10. A since-deleted file inside a deleted limited-access folder is read through a snapshot by a member of the enclosing folder without rights inside the boundary: not found.
11. A snapshot listing evaluates each historical child at head; unreadable children return name and kind only. A subject without `history` on the directory is refused the listing.
12. The current revision addressed by number needs `read` only; an older revision needs `history`.

Moves:

13. Same-context move and rename by an Editor without `share`: allowed.
14. A subject with `read` and organizing rights but no `share` moves a file into a folder another principal can read: `forbidden`. With `share`: allowed. With `manage`: allowed.
15. A subject without `read` on X moves X into their own folder, holding `share` on X: `forbidden`.
16. Moving a boundary folder anywhere: entry rights only.
17. An administrator principal appears in the destination chain: no gain is reported.
18. Relocating undelete follows the same matrix; in-place undelete needs entry rights only.
19. A move into a restricted folder that removes others' access: allowed with entry rights.

Deleted items and trash:

20. Trash listing is filtered by `read` on each entry's original parent, and a shortened page carries a cursor.
21. A subject with `remove` on the original parent and nothing on the item undeletes a deletion root: allowed.

Grep:

22. Matches inside a boundary subfolder are filtered for a member of the enclosing folder and visible to the subfolder's members; a page shortened by filtering carries a cursor.
23. A `path_prefix` the subject cannot `read` answers `path_not_found`.

Unrestricted namespaces and headers:

24. Every operation on an unrestricted namespace performs no additional object-store operations compared with the baseline before this design, and ignores the subject and principal headers.
25. An ACL-enabled namespace without the headers answers `invalid_request` naming the header.
26. An ACL-enabled namespace refuses a subject from another scope before reading a grant.

Uploads and replay:

27. Upload session actions by a different subject are refused.
28. A commit retry with the same commit id and a different subject answers `commit_id_reuse_conflict`. The same subject retrying after revocation receives the original receipt.

## Not in this version

A subject-scoped change feed requires a rule for `moved` and `deleted` events whose source or destination the subject cannot see. Until it exists the feed is administrator-only.

A discovery index keyed by principal, answering "what can this principal reach", is a derived family whose scope and freshness are defined when a product surface needs it. Nothing in this version depends on it.

Live conversion of an active namespace between modes is a separate fenced operation. Introducing this design converts no existing namespace.

Mounts, reserved by the format, are the eventual shape for sharing across namespaces. They are complementary to grants within one.
