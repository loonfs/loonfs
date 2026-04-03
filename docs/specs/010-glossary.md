# Glossary

This glossary defines the terms used across the LoonFS spec in plain language.

| Term | Meaning |
| --- | --- |
| **Namespace** | The unit of serialized metadata history. Each namespace has its own head, WAL, checkpoints, and `seq` order. |
| **Inode** | The durable identity of a filesystem item inside one namespace. An inode keeps the same identity when its path changes. |
| **Direntry** | A name binding that places one inode under one parent directory and one name. |
| **Path** | A human-friendly projection built by walking visible directory bindings. Paths can change; inodes do not. |
| **Seq** | The namespace-local sequence number that gives the visible order of committed metadata changes. |
| **Revision** | One immutable committed version of a file’s content. Revisions are ordered by `revision_no` within an inode. |
| **Content manifest** | The immutable object that describes a file’s size, digest, block size, and ordered list of content blocks. |
| **Checkpoint** | A verified snapshot of namespace metadata at one chosen `seq`. It lets readers avoid replaying an unbounded WAL history. |
| **WAL** | The write-ahead log of immutable commit objects that record namespace mutations in order. |
| **Retention floor** | The oldest `seq` from which the system still promises incremental replay. Clients older than that point must re-bootstrap. |
| **Derived index** | Rebuildable helper state that improves performance but is not part of durable truth. |
| **Fence token** | A writer generation number used to prevent stale lease holders from publishing after takeover. |
| **Mount** | A directory-like inode that exposes another namespace, or a subtree of another namespace, inside the current tree. |
| **NamePolicy** | The shared, versioned rule for how names are normalized for collision checks. |
| **Conflict artifact** | A durable record that preserves losing content when the client must keep the canonical winner path stable. |
| **Cursor** | A client bookmark, usually an `after_seq` value, used to resume reading changes incrementally. |

## Three ideas worth remembering

1. **Identity is inode-based.**  
   Paths are presentation. Mutations do not target paths.

2. **Visibility is head-based.**  
   A WAL object may exist, but the change is not visible until the head advances.

3. **Performance features are disposable.**  
   Checkpoints and indices are important, but only because they can be rebuilt from durable truth.
