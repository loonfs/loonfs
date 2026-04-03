# Glossary

| Term | Meaning |
| --- | --- |
| **Namespace** | The unit of ordered metadata history. Each namespace has its own head, WAL, checkpoints, and retention policy. |
| **Head** | A small mutable object that names the current visible sequence number (`seq`), the next inode id, and replay hints such as the latest checkpoint and retention floor. |
| **Seq** | The namespace-local number that gives the visible order of committed metadata changes. |
| **Inode** | The durable identity of a filesystem item within one namespace. An inode stays the same when its path changes. |
| **Direntry** | A directory binding that places one inode under one parent directory and one name. |
| **Path** | A human-friendly name built by walking visible directory bindings. Paths can change; inode identity does not. |
| **Revision** | One immutable committed version of a file's content. Revisions are ordered by `revision_no` within an inode. |
| **WAL** | The write-ahead log of immutable metadata commit objects. |
| **Checkpoint** | A verified snapshot of namespace metadata at one chosen `seq`. It lets readers avoid replaying the entire WAL history. |
| **Content block** | One immutable block of file bytes. In v1, blocks are fixed-size except for the final partial block. |
| **Content manifest** | The immutable object that describes a file's size, digest, block size, and ordered list of content blocks. |
| **NamePolicy** | The versioned rule that decides how sibling names are compared for collisions. |
| **Tombstone** | A metadata record that hides a deleted inode or subtree without erasing history. |
| **Retention floor** | The oldest sequence number from which the system still promises incremental replay. Older clients must re-bootstrap. |
| **Change feed** | The ordered stream of committed metadata changes after a chosen `seq`. |
| **Cursor** | A bookmark such as `after_seq` used to resume incremental reads. |
| **Mount** | A special inode that exposes another namespace, or a subtree of another namespace, inside the current tree. |
| **ACL** | An access-control rule granting a principal a role over a namespace or subtree. ACLs are not part of namespace metadata history. |
| **Share** | An access grant to a namespace or subtree. A share may later be presented through a mount in another tree. |
| **Precondition** | A rule that must still hold at an explicit namespace history point before a commit is accepted. |
| **Request id** | A stable client-generated id used for idempotent retries. |
| **Session / job** | A server-side control object used for long-running operations such as recursive reads, resumable uploads, and server-side copies. |
