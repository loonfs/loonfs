# LoonFS glossary

| Term | Meaning |
| --- | --- |
| **Namespace** | A directory tree with its own ordered metadata history, manifests, WAL, and retention policy. Forks can share stored objects across namespaces. |
| **Head** | The current logical position and state derived from the current manifest and later WAL objects; not a separate durable object. |
| **Sequence (`seq`)** | A namespace-local position assigned to one committed mutation request. |
| **Commit** | One successfully published mutation request whose operations share a sequence. |
| **Commit ID** | A caller-supplied identifier used to recognize retries while the corresponding receipt remains retained. |
| **Commit receipt** | The durable result and semantic fingerprint of a committed request. |
| **Semantic fingerprint** | A digest of the canonical logical request, used to detect conflicting reuse of a commit ID. |
| **WAL** | The ordered log of immutable, consecutively numbered WAL objects. |
| **WAL segment** | A numbered immutable object containing contiguous commits, or no commits when fencing a writer. |
| **Fence** | A zero-record WAL object used to establish a writer epoch in WAL order without creating a logical commit. |
| **Content publication** | Permanent metadata evidence that a content ID was committed; collection uses it to decide completed-upload cleanup. |
| **Flush / WAL fold** | Materializing committed WAL into metadata segments and publishing a manifest so later readers replay less history. |
| **Inode** | The identity and creation metadata of a filesystem item. Its ID remains unchanged when the item is renamed or moved within a namespace. |
| **Directory binding / direntry** | A parent inode, name, and child inode association that places an item in the tree. |
| **Binding generation** | The sequence and delta position of a particular bind. The API represents this pair as an opaque token. |
| **Path** | An absolute name resolved by following visible directory bindings from the root. |
| **Display name** | The stored spelling of a directory entry's name. |
| **Name key** | The normalized and case-folded value used for sibling-name comparison and lookup. |
| **Revision** | One committed content state of a file, ordered by a revision number scoped to that inode. |
| **Content store** | A domain of immutable file objects identified by `content_store_id`, shared by a namespace and its forks. |
| **Content object** | The complete bytes of one uploaded file, stored immutably under the original owner's prefix. |
| **Content reference** | A `blob_v1` record containing the original owner namespace, content ID, complete size, and checksum. |
| **Upload session** | A durable record for one upload, with a fixed identity and mode and an open, completed, or aborted status. Completion alone does not commit a file. |
| **Metadata segment** | An immutable, sorted set of rows in one metadata family, stored in independently readable blocks. |
| **Run** | The metadata segments produced together, identified by a manifest-allocated run number. |
| **Namespace manifest** | A numbered immutable record of namespace identity, lifecycle, authority, materialized file set, and retention floors. |
| **Hint** | A mutable starting point for forward discovery. Its numbers can lag publication and are not freshness evidence. |
| **Metadata basis** | The verified file set in the reading namespace’s current manifest. A fork’s own manifest lists its inherited runs. |
| **Checkpoint** | A durable pin retaining one numbered manifest for a user, snapshot, or fork dependency. |
| **Snapshot** | A retained read view represented by a snapshot-owned pin; reads require an unexpired record. |
| **Fork** | A new namespace initialized from a retained source view, sharing stored objects with independent subsequent metadata history. |
| **Tombstone** | A committed deletion event that hides an inode or subtree while preserving the information needed for undelete. |
| **Retention floor** | The lower bound for guaranteed incremental replay and retained metadata views. It limits superseded metadata and receipt retention but does not expire a live namespace's file revisions. |
| **Namespace retirement** | A fixed deadline added to a deleted manifest after its pin dependencies are cleared, allowing later collection of its owned content. |
| **Change feed** | Committed filesystem events ordered by namespace sequence and operation position. |
| **Cursor** | A position used to resume a paginated read or bounded index build under its consistency rules. Core and grep GC complete one pass without a cursor. |
| **Precondition** | A requirement checked against the applicable metadata state before a new mutation is accepted. |
| **Writer epoch** | A namespace-local fencing counter. A writer session cannot publish after another session acquires a newer epoch. |
| **Compare-and-swap (CAS)** | A conditional update that succeeds only if the object's compare token still matches the version previously read. |
| **Control object** | A structured durable record for discovery, retained views, content domains, or upload state. Its kind determines its update rules. |
| **Family group** | Related metadata row families that compaction processes together, such as the two bind indexes and unbinds. |
| **Compactor epoch** | A namespace-wide counter in the manifest that fences compaction publications from older runtime claims. |
| **GC pass** | One complete collection call with freshly loaded roots, an in-memory live set, and a fixed call clock. |
| **API group** | A conformance unit, such as `filesystem/v0`, advertised only when all its required operations are implemented. |
| **Feature** | An optional capability within an API group, such as `filesystem.uploads.direct_put`. |
| **Capability document** | The deployment's advertised protocol version, API groups, features, and advisory limits. |
| **Extension** | A derived subsystem with its own objects and collection rules under `namespaces/{namespace_id}/extensions/{name}/`. |

The [format](format.md) defines stored representations and lifecycle rules. The [API](api.md) defines request, response, and cursor behavior. Mounts, ACL records, and shares have no implemented durable schema in this format.
