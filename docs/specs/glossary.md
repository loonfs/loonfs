# LoonFS Glossary

| Term | Meaning |
| --- | --- |
| **Namespace** | The unit of ordered metadata history. Each namespace has its own head, WAL segments, manifests, checkpoints, and retention policy. |
| **Head** | A small mutable object that names the current visible sequence number (`seq`), the next inode id, and replay hints such as the latest checkpoint, the retention floor, and the visible WAL tip. |
| **Seq** | The namespace-local number that gives the visible order of committed metadata changes. |
| **Commit** | One accepted client commit request that records one ordered set of metadata changes and is assigned a seq. |
| **WAL segment** | One immutable WAL object containing one or more commits with a contiguous sequence range. |
| **WAL** | The write-ahead log formed by the visible chain of immutable WAL segments. |
| **WAL fold** | A WAL fold is not name-key folding. It folds a namespace's WAL tail, the segments published since the last manifest, into metadata segments and publishes a new manifest, so readers replay fewer segments. The writer folds in the background once the tail reaches the fold threshold. |
| **Inode** | The durable identity of a filesystem item within one namespace. An inode stays the same when its path changes. |
| **Direntry** | A directory binding that places one inode under one parent directory and one name. |
| **Path** | A human-friendly name built by walking visible directory bindings. Paths can change; inode identity does not. |
| **Binding generation** | An opaque token identifying one generation of a parent/name binding. It changes when an entry is created, moved, or undeleted. |
| **Revision** | One immutable committed version of a file's content. Revisions are ordered by `revision_no` within an inode. |
| **Namespace manifest** | The immutable object that describes one durable namespace file set: metadata segments, manifest number, head summary, fork references, and checkpoint records. |
| **Checkpoint** | A durable pinned reference to one manifest version and namespace sequence. It lets readers and retention logic rely on that manifest without replaying the entire WAL history. |
| **Compaction lease** | A compaction lease is the durable, family-group-specific claim that fences concurrent metadata compactions and permits takeover after expiry. |
| **Family group** | A family group is one related set of metadata families that bounded reorganization and streaming compaction plan and publish together. |
| **Snapshot** | An in-process read view. It may be stable for one operation or session, but it is not a durable checkpoint unless explicitly recorded as one. |
| **Content object** | One immutable object containing file bytes. In v0, each file revision stores the whole file as one object. |
| **Content ref** | The metadata pointer for a file revision: `kind: "blob_v1"`, the `content_id` naming the object, `size_bytes`, and one mandatory full-object `checksum`. |
| **Name key** | The folded form of a display name that sibling-collision checks compare on. The v0 rule is NFC, Unicode default case folding, then NFC again. |
| **Fold** | A fold means name-key folding, not a WAL fold. It normalizes and case-folds a display name into the value used for sibling-collision checks. |
| **Tombstone** | A metadata record that hides a deleted inode or subtree without erasing history. |
| **Retention floor** | The oldest sequence number from which the system still promises incremental replay. Older clients must re-bootstrap. It bounds replay only: file revision history is retained in full regardless of the floor, and the floor never advances unless an operator opts in. |
| **Change feed** | The ordered stream of committed metadata changes after a chosen `seq`. |
| **Cursor** | A bookmark such as `after_seq` used to resume incremental reads. |
| **Mount** | Reserved for the future: presenting another namespace, or a subtree of one, inside a visible tree. Not a v0 inode kind. |
| **ACL** | An access-control rule granting a principal a role over a namespace or subtree. ACLs are not part of namespace metadata history. |
| **Share** | An access grant to a namespace or subtree. A share may later be presented through a mount in another tree. |
| **Precondition** | A rule that must still hold at an explicit namespace history point before a commit is accepted. |
| **Commit id** | A stable client-generated id used for idempotent retries of one commit request. |
| **Maintenance run** | One invocation of a named maintenance job for one namespace, whether triggered through the maintenance API or selected by a scheduler. |
| **Writer session** | One node's open claim to publish for one namespace under a writer epoch. A node opens it on first write or explicitly, closes it by draining admitted work, and holds at most `max_writer_sessions` of them. |
| **Operation id** | Optional commit metadata used to correlate multiple commits that belong to one higher-level workflow. |
| **Control object** | A server-side control-plane object used to preserve authoritative state across multiple requests, such as a pinned read snapshot, a resumable upload, or a stable destination binding. |
| **API group** | An all-or-nothing API conformance unit (for example `filesystem/v0`). A deployment advertises an API group only when every required op in it is implemented. |
| **Feature** | A named optional capability inside an advertised API group, keyed `group.area.name` (for example `filesystem.uploads.direct_put`). |
| **Capability document** | The self-description a deployment returns from `GET /v0/capabilities` (or exposes as a constant when embedded): protocol version, advertised API groups, features, and advisory limits. |
| **Extension keyspace** | Namespace-scoped durable state owned and versioned by one derived subsystem below `namespaces/{namespace_id}/extensions/{name}/`. |
