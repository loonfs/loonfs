# LoonFS Glossary

| Term | Meaning |
| --- | --- |
| **Namespace** | The unit of ordered metadata history. Each namespace has its own head, WAL segments, manifests, checkpoints, and retention policy. |
| **Head** | A small mutable object that names the current visible sequence number (`seq`), the next inode id, and replay hints such as the latest checkpoint, the retention floor, and the visible WAL tip. |
| **Seq** | The namespace-local number that gives the visible order of committed metadata changes. |
| **Commit** | One accepted client commit request that records one ordered set of metadata changes and is assigned a seq. |
| **WAL segment** | One immutable WAL object containing one or more commits with a contiguous sequence range. |
| **WAL** | The write-ahead log formed by the visible chain of immutable WAL segments. |
| **Inode** | The durable identity of a filesystem item within one namespace. An inode stays the same when its path changes. |
| **Direntry** | A directory binding that places one inode under one parent directory and one name. |
| **Path** | A human-friendly name built by walking visible directory bindings. Paths can change; inode identity does not. |
| **Revision** | One immutable committed version of a file's content. Revisions are ordered by `revision_no` within an inode. |
| **Namespace manifest** | The immutable object that describes one durable namespace file set: metadata SSTs, manifest sequence, head summary, fork references, and checkpoint records. |
| **Checkpoint** | A durable pinned reference to one manifest version and namespace sequence. It lets readers and retention logic rely on that manifest without replaying the entire WAL history. |
| **Snapshot** | An in-process read view. It may be stable for one operation or session, but it is not a durable checkpoint unless explicitly recorded as one. |
| **Content object** | One immutable object containing file bytes. In v0, each file revision stores the whole file as one object. |
| **Content ref** | The metadata pointer for a file revision. In v0 it has `kind: "whole_file_v0"`, a `sha256:<hex>` digest, and `size_bytes`. |
| **NamePolicy** | The versioned rule that decides how sibling names are compared for collisions. |
| **Tombstone** | A metadata record that hides a deleted inode or subtree without erasing history. |
| **Retention floor** | The oldest sequence number from which the system still promises incremental replay. Older clients must re-bootstrap. |
| **Change feed** | The ordered stream of committed metadata changes after a chosen `seq`. |
| **Cursor** | A bookmark such as `after_seq` used to resume incremental reads. |
| **Mount** | Reserved for the future: presenting another namespace, or a subtree of one, inside a visible tree. Not a v0 inode kind. |
| **ACL** | An access-control rule granting a principal a role over a namespace or subtree. ACLs are not part of namespace metadata history. |
| **Share** | An access grant to a namespace or subtree. A share may later be presented through a mount in another tree. |
| **Precondition** | A rule that must still hold at an explicit namespace history point before a commit is accepted. |
| **Commit id** | A stable client-generated id used for idempotent retries of one commit request. |
| **Operation id** | Optional commit metadata used to correlate multiple commits that belong to one higher-level workflow. |
| **Control object** | A server-side control-plane object used to preserve authoritative state across multiple requests, such as a pinned read snapshot, a resumable upload, or a stable destination binding. |
| **Profile** | An all-or-nothing API conformance unit covering one functional plane (for example `core/v0`). A deployment advertises a profile only when every required op in it is implemented. |
| **Feature** | A named optional capability inside an advertised profile, keyed `plane.area.name` (for example `core.uploads.direct_put`). |
| **Capability document** | The self-description a deployment returns from `GET /v0/config` (or exposes as a constant when embedded): protocol version, advertised profiles, features, and advisory limits. |
| **Extension keyspace** | Namespace-scoped durable state owned and versioned by one derived subsystem below `namespaces/{namespace_id}/extensions/{name}/`. |
