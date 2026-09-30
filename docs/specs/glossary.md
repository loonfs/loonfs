# LoonFS glossary

| Term | Meaning |
| --- | --- |
| **Namespace** | A directory tree with its own ordered metadata history, manifests, WAL, and retention policy. Its id names one lifetime; deletion is terminal and a create or fork into that id returns `namespace_deleted` ([format section 1.1](format.md#11-namespaces-and-identity)). Forks can share stored objects across namespaces. |
| **Head** | The current logical position and state derived from the current manifest and later WAL objects; not a separate durable object. |
| **Sequence (`seq`)** | A namespace-local position assigned to one committed mutation request. |
| **Commit** | One successfully published mutation request whose operations share a sequence. |
| **Commit ID** | A caller-supplied identifier used to recognize retries while the corresponding receipt remains retained. |
| **Commit receipt** | A durable row that maps a commit ID to its committed sequence. The commit row at that sequence stores the semantic fingerprint. |
| **Semantic fingerprint** | A digest of the canonical logical request, used to detect conflicting reuse of a commit ID. |
| **WAL** | The ordered log of immutable, consecutively numbered WAL objects. |
| **Fence** | A zero-record WAL object used to establish a writer epoch in WAL order without creating a logical commit. |
| **Content publication** | Permanent metadata evidence that a content ID was committed; collection uses it to decide completed-upload cleanup. |
| **Fold** | Materializing committed WAL into metadata segments and publishing a manifest so later readers replay less history. The CLI command `loonfs maintenance flush` runs a fold, and the maintenance response reports it in `wal_flush`. |
| **Inode** | The identity and creation metadata of a filesystem item. Its ID remains unchanged when the item is renamed or moved within a namespace. |
| **Directory binding / direntry** | A parent inode, name, and child inode association that places an item in the tree. |
| **Binding version** | The sequence and delta position of a particular bind. The API represents this pair as an opaque token. |
| **Path** | An absolute name resolved by following visible directory bindings from the root. |
| **Display name** | The stored spelling of a directory entry's name. |
| **Name key** | The normalized and case-folded value used for sibling-name comparison and lookup. |
| **Revision** | One committed content state of a file, ordered by a revision number scoped to that inode. |
| **Content object** | The complete bytes of one piece of file content, stored immutably under `namespaces/{owner_namespace_id}/content/{content_id}`. |
| **Content reference** | A `blob_v1` record containing the original owner namespace, content ID, complete size, and checksum. It identifies content; it does not prove that the content object exists yet. |
| **Inline content** | File bytes carried in the WAL commit that references them. A fold writes them to a content object before the WAL object can be collected ([format section 1.5](format.md#15-file-contents-and-ownership)). |
| **Upload session** | A durable record for one upload, with a fixed identity and mode and an open, completed, or aborted status. Completion alone does not commit a file. |
| **Admission evidence** | The in-process proof or the signed content token that admits an externally supplied content reference to publication ([format section 5.5](format.md#55-admission-proofs)). It is bound to the namespace and the complete reference. It is valid while the clock reads before its expiry. |
| **Metadata segment** | An immutable, sorted set of rows in one metadata family, stored in independently readable blocks. |
| **Run** | The metadata segments produced together, identified by a manifest-allocated run number. |
| **Namespace manifest** | A numbered immutable record of namespace identity, lifecycle, authority, materialized file set, and retention floors. |
| **Hint** | A mutable starting point for forward manifest discovery. Its number can lag publication and is not freshness evidence. |
| **Metadata basis** | The verified file set in the reading namespace’s current manifest. A fork’s own manifest lists its inherited runs. |
| **Checkpoint** | A durable pin retaining one numbered manifest for a user, snapshot, or fork dependency. |
| **Snapshot** | A retained read view represented by a snapshot-owned pin; reads require an unexpired record. |
| **Fork** | A new namespace initialized from a retained source view, sharing stored objects with independent subsequent metadata history. |
| **Tombstone** | A committed deletion event that hides an inode or subtree while preserving the information needed for undelete. It is a row in a namespace's history. The manifest that a namespace deletion publishes is a different object, the namespace tombstone. |
| **Namespace tombstone** | A deleted namespace's final manifest ([format section 9.4](format.md#94-deleting-a-namespace)). It stays the current manifest forever and is the last manifest of its chain. It is not a file or subtree tombstone. |
| **Retention floor** | The lower bound for guaranteed incremental replay and retained metadata views. It limits superseded metadata and receipt retention but does not expire a live namespace's file revisions. |
| **Namespace retirement** | Eligibility to reclaim a deleted namespace's content prefix and source pin under [format section 9.5](format.md#95-retirement). |
| **Change feed** | Committed filesystem events ordered by namespace sequence and operation position. |
| **Cursor** | A position used to resume a paginated read or bounded index build under its consistency rules. Core and grep GC complete one pass without a cursor. |
| **Precondition** | A requirement checked against the applicable metadata state before a new mutation is accepted. |
| **Writer epoch** | A namespace-local fencing counter. A writer session cannot publish once another session's fence for a newer epoch has landed. |
| **Epoch claim** | A manifest publication that raises the writer or compactor epoch by one. A writer's claim is followed by a fence; until the fence lands, the previous writer can still commit ([format section 6.1](format.md#61-writer-ownership)). |
| **Compare-and-swap (CAS)** | A conditional update that succeeds only if the object's compare token still matches the version previously read. |
| **Control object** | A structured durable record for discovery, retained views, or upload state. Its kind determines its update rules. |
| **Family group** | Related metadata row families that compaction processes together, such as the two bind indexes and unbinds. |
| **Bounded compaction** | One compaction step that merges a contiguous window of runs in one family group within the step's row and byte budgets and publishes the result at once ([format section 10.2](format.md#102-compaction-windows)). A window over those budgets runs as streaming compaction instead: a background job that merges the window without holding every row in memory and publishes once at the end ([format section 10.4](format.md#104-streaming-compaction)). |
| **Compactor epoch** | A namespace-wide counter in the manifest that fences compaction publications from older runtime claims. |
| **GC pass** | One complete collection call with freshly loaded roots, an in-memory live set, and a fixed call clock. |
| **Publication budget** | The longest monotonic time from the observation a publisher planned against to the start of its numbered put. A put that returns after its budget has an unknown outcome ([format Appendix C.1](format.md#c1-publication-and-collection-timing)). |
| **Reclamation grace** | The configured age `T` that a collectable manifest or WAL object must reach before collection deletes it. It is at least `GC_MIN_GRACE_WINDOW_MS`, the longest publication budget plus the provider request bound and the clock allowance ([format section 11.3](format.md#113-candidate-and-age-rules)). |
| **Revalidation bound** | `READ_REVALIDATION_BOUND_MS`, the longest time a reader trusts an absent manifest successor after the probe it confirms ([format section 4.2](format.md#42-replaying-the-visible-wal)). |
| **API group** | A conformance unit, such as `filesystem/v0`, advertised only when all its required operations are implemented. |
| **Feature** | An optional capability within an API group, such as `filesystem.uploads.direct_put`. |
| **Capability document** | The deployment's advertised protocol version, API groups, features, and advisory limits. |
| **Extension** | A derived subsystem with its own objects and collection rules under `namespaces/{namespace_id}/extensions/{name}/`. |

The [format](format.md) defines stored representations and lifecycle rules. The [API](api.md) defines request, response, and cursor behavior.
