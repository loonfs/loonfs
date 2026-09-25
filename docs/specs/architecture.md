# LoonFS architecture

LoonFS stores file bytes and filesystem metadata in object storage. The metadata describes directory bindings, file revisions, retained views, and committed changes. A reader can recover the filesystem without the process that originally wrote it.

A namespace id names one lifetime; deletion is terminal and a create or fork into that id returns `namespace_deleted` ([format section 1.1](format.md#11-namespaces-and-identity)).

`loonfs-http` implements the representative HTTP binding. `loonfs-server` hosts it.

## Stored state

The main objects have separate roles:

| Object | Role |
| --- | --- |
| Numbered namespace manifest | Records namespace identity, lifecycle, writer authority, materialized metadata runs, and the retention floor. |
| Numbered WAL object | Publishes logical commits or a writer fence after the materialized boundary. |
| Hint | Records a starting point for forward discovery of manifests and WAL. |
| Pin record | Retains one manifest for a user, snapshot, or fork. |
| Metadata segment | Stores sorted rows in independently readable blocks. |
| Content object | Stores the complete bytes of a file revision. |

Creating the next numbered WAL object commits its records. Creating the next numbered manifest publishes a new materialized file set or control-state change. Each publication uses put-if-absent, so competing attempts at one number cannot both succeed.

The hint may lag either publication stream. Readers load the hinted objects and probe forward; they do not treat the hint as the current state. The [format specification](format.md#2-objects-and-references) defines the complete layout and stored fields.

## Writing a file

File content takes one of two paths. Uploaded content is stored and verified before the commit that references it:

```text
create upload session -> transfer bytes -> verify and complete upload
                                                   |
                                                   v
                                  validate metadata and content proof
                                                   |
                                                   v
                                    create next numbered WAL object
                                                   |
                                             file committed
```

Completing an upload does not change a file. The later WAL put commits the mutation.

A small file can carry its bytes inline in the commit. One WAL put makes the bytes durable and the file visible. A later flush writes the bytes to a content object before it publishes the manifest that lets collection delete that WAL object:

```text
validate metadata -> create next numbered WAL object with the bytes
                                        |
                                  file committed
                                        |
                                        v
                 flush writes the content object, then the manifest
```

[Format section 1.5](format.md#15-file-contents-and-ownership) defines both paths. Several requests can share one WAL object, but each accepted request has its own sequence and commit ID.

A writer acquires an epoch through a manifest publication, then writes a zero-record WAL fence. Another session can acquire a newer epoch; the earlier session must stop once fenced. Writer authority uses epochs and conditional writes, without a writer lease.

A lost response can hide a successful commit. Clients reconcile an uncertain outcome with the original commit ID and request. Re-uploading first creates a different content identity and can turn an otherwise valid retry into conflicting commit-ID reuse.

## Reading a file

A reader discovers the current manifest and WAL tip. It reads the manifest's runs and replays the required WAL objects after the folded boundary. A new namespace's manifest represents the built-in root directory; a fork's own manifest lists its inherited runs from installation.

```text
current manifest -> listed metadata runs --+
                                           +--> view at one sequence
later numbered WAL -> committed changes ---+            |
                                                        v
                                        path -> inode -> revision -> content
```

A path is resolved through directory bindings. A file revision contains the original owner namespace, content ID, size, and checksum. The owner namespace and content ID determine its object key. Inherited content can therefore be read without fetching its owner's manifest or walking the fork ancestry. Content committed inline is read from the replayed WAL until a flush writes its object.

Directory listings use committed metadata for names and file sizes. They do not download every file. Content reads verify the complete size and checksum. Missing or corrupt required recovery objects fail the read; an available earlier file set is not a substitute.

Warm readers probe the next WAL number and periodically check for a successor manifest. The default manifest interval is one second, so a cached active view can remain usable until that check observes a namespace deletion. Local caches affect read cost and freshness within the defined interval; they are not durable recovery state.

## Forks and retained views

A fork creates and verifies a source pin, then installs its own manifest 1 with the pinned run references. It shares the source's existing content and metadata objects without copying the filesystem. Forking the current head may first flush the source's outstanding WAL tail, which writes that tail's inline content to content objects. The fork's later commits belong to independent history.

The target can continue referencing ancestor-owned content and segments. A target-owned manifest does not mean those dependencies have been copied locally. The source pin remains until the target has retired and released it.

User checkpoints and snapshots use the same pin representation with different owner rules. Every creation writes a new pin with a fresh id; a name is a label, not a key. Snapshot reads require an unexpired record; a user pin remains readable while it exists, including after its expiry. Explicit deletion removes the record. Collection retains every manifest named by its complete pin listing, including expired records that have not yet been collected ([format section 8](format.md#8-pins)).

## Maintenance and deletion

| Operation | Effect |
| --- | --- |
| Flush | Materializes the WAL tail into segments and publishes the next manifest. |
| Compaction | Merges selected runs and applies eligible row-retention rules while preserving retained views. |
| Retention advance | Explicitly advances the sequence floor after verifying the materialized basis. |
| Garbage collection | Completes one pass over eligible objects using fresh roots and an in-memory live set. |

A file deletion records a recoverable subtree tombstone. A namespace deletion publishes terminal deleted status in its next manifest. Neither immediately removes shared content.

Retirement follows [format section 9.5](format.md#95-retirement). An eligible pass reclaims the namespace’s own content and releases its source pin under [section 11.8](format.md#118-sweeping-a-retired-owners-content).

Hosts choose when maintenance runs and which namespaces it covers. The storage protocols determine what each operation can publish or delete. Derived extensions such as grep have separate manifests and collection rules; core collection never sweeps their objects.
