# LoonFS architecture

LoonFS stores a filesystem directly in object storage, with no external metadata database. File bytes go into immutable content objects, and every change to the directory tree goes into a numbered write-ahead log (WAL). The [format specification](format.md) defines the exact rules, and the [glossary](glossary.md) defines each term.

LoonFS relies on seven object-store operations ([Required operations](format.md#31-required-operations)): put-if-absent, compare-and-swap on small objects, full and ranged reads, version-conditional reads, deletes, prefix listings, and child-prefix listings. Because object storage offers no multi-object transactions, every durable state change commits through a single object write.

Each "namespace" is one directory tree with its own history under `namespaces/{namespace_id}/`. Its content objects, upload records, and assembly temporary objects are stored under its [pool](glossary.md#pool) prefix (`pools/{pool_id}/`), which it shares with all of its forks.

Applications can embed LoonFS as a Rust library (`loonfs`) or talk to it over HTTP (`loonfs-http`, hosted by the standalone `loonfs-server` binary). Because writer handoff goes through object storage and takes around 200 ms, most deployments run one long-lived writer session per namespace inside a shared server.

## Stored objects

Each namespace is represented by six main object types:

| Object | What it stores |
| --- | --- |
| Manifest | Numbered, immutable snapshot of namespace identity, lifecycle status, current writer, metadata runs, and retention floor. |
| WAL object | Numbered, immutable log object containing one or more commits, a writer [fence](glossary.md#fence), or a [floor object](glossary.md#floor-object) (which advances the retention floor with no commits). |
| Manifest boundary | Small control object recording a manifest number at or below which every manifest is obsolete. Garbage collection raises it before deleting old manifests. |
| Snapshot | Record pointing to one manifest so garbage collection keeps that manifest and all of its segments. Callers create user snapshots, and forks create fork snapshots. |
| Metadata segment | Immutable sorted metadata rows organized into independently readable blocks. |
| Content object | Immutable file bytes stored under the namespace's pool (`pools/{pool_id}/content/`). A file can span multiple content objects. |

Manifests and WAL objects each use their own 1-based counter (1, 2, 3, ...), and every numbered object is written with put-if-absent so only one writer can win a given number. Writing the next WAL object commits its records, while writing the next manifest publishes new metadata segments or namespace state changes (such as a writer takeover or deletion).

Because a namespace with many snapshots or forks can keep thousands of historical manifests, readers don't list the whole manifest directory. Instead, a reader reads the [manifest boundary](glossary.md#manifest-boundary), lists only manifests numbered above it, and loads the highest one. A publisher checks the boundary again after writing a manifest so any write at or below a boundary raised by garbage collection counts as lost. See [Objects and references](format.md#2-objects-and-references) for the full layout.

## Writing a file

File writes are durable in two steps: bytes become durable first, and then a WAL commit makes the change visible. There are two paths for file content ([File contents and ownership](format.md#15-file-contents-and-ownership)).

For uploaded content, bytes are uploaded and verified before the commit references them:

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

Completing an upload does not change any file in the namespace. The file updates only when a commit referencing that upload is written to the WAL (an uncommitted upload is cleaned up later by garbage collection).

For small files ("inline" content) and appends, the bytes go directly into the WAL object so a single write makes them durable and visible. A later fold copies those bytes into a content object before publishing the manifest that lets garbage collection delete the WAL object:

```text
validate metadata -> create next numbered WAL object with the bytes
                                        |
                                  file committed
                                        |
                                        v
                 fold writes the content object, then the manifest
```

Multiple requests can share one WAL object, while each accepted request keeps its own sequence number and commit ID.

### Writer ownership and retries

Only one writer session can commit to a namespace at a time. LoonFS coordinates writers with monotonic epochs rather than leases so a new writer can take over immediately without waiting for a lease timeout:

1. The new writer publishes the next manifest with a higher writer epoch.
2. It writes an empty WAL object (a "fence") with that epoch at the next WAL number.

Commits from the old writer remain valid between steps 1 and 2. Once the fence is written, the old writer's next WAL put collides with an existing object, reloads the namespace, sees the higher epoch, and fails with `writer_fenced`.

If a commit response is lost, the client retries with the exact same commit ID and request. If the original commit succeeded, LoonFS replays its original result rather than committing twice.

Warning: when retrying a commit, reuse the original content reference instead of re-uploading the file. A new upload gets a new content ID, which changes the commit fingerprint and causes a retry of an already-committed write to fail with `commit_id_reuse_conflict`.

## Reading a file

To read a namespace, a reader locates the current manifest and the WAL tip, loads the metadata runs listed in the manifest, and replays any WAL objects written after the manifest's folded boundary:

```text
current manifest -> listed metadata runs --+
                                           +--> view at one sequence
later numbered WAL -> committed changes ---+            |
                                                        v
                                        path -> inode -> revision -> content
```

A new namespace's first manifest contains only the root directory, and a fork's first manifest inherits its source's metadata runs.

Paths resolve by walking [directory bindings](glossary.md#directory-binding-direntry) from the root. Each file revision stores a content reference (content ID, size, and checksum). Combining that content ID with the `pool_id` from the reader's manifest yields the content object keys directly, so a fork reads inherited content without loading its source's manifest or walking fork ancestry. Unfolded inline bytes are read from the WAL tail.

Directory listings read names and file sizes from committed binding metadata without downloading file content. Whole-file reads verify complete length and checksum. If any required object is missing or corrupt, the read fails rather than silently falling back to an older version.

### Caching and freshness

Local caches affect latency and freshness only and are never needed for recovery:

- **Readers without a local writer session.** Under the default `eventual` consistency ([Read consistency](api.md#read-consistency)), cached reads make no object-store requests while the cached head was confirmed within `max_staleness_ms` (1 second by default). Past that window, and on every `strong` read, the reader probes the next WAL number for new commits and checks for a newer manifest (about once per second by default).
- **Readers in the writer's process.** Reads start from the writer session's in-memory [position](glossary.md#writer-position), which already includes every locally acknowledged commit, so they skip WAL probes and only check for newer manifests.
- **Content caching.** Because content objects are immutable, readers cache recently fetched small content objects in memory without invalidation.

## Forks and retained views

A "fork" is a new namespace initialized from a point in another namespace's history. Creating one copies no file bytes or metadata segments, so forking cost is independent of namespace size. LoonFS writes the fork's initializing manifest 1 (listing the source's metadata runs), writes and verifies a fork snapshot on the source and a [member entry](glossary.md#member-entry) in the pool, folds any source WAL tail into the fork's own manifest when needed, and writes the fork's active manifest ([Forking a namespace](format.md#92-forking-a-namespace)). After creation, commits to either namespace stay isolated unless applied by a [merge](glossary.md#merge).

Every fork joins its source's [pool](glossary.md#pool), so all namespaces in the fork tree store content under the same `pools/{pool_id}/` prefix. Forks read inherited content and write new content with identical key shapes, and merges reference fork content without copying bytes. The source retains its fork snapshot until the fork is deleted and retired.

Callers can also create user snapshots to pin a point in history for reads or forks. Each create call generates a new `snapshot_id` (names are optional non-unique labels), with an optional expiry time. The maintenance snapshot inventory lists both user and fork snapshots so operators can inspect everything keeping old manifests alive.

| Snapshot type | Readable directly? | Removed when |
| --- | --- | --- |
| User snapshot | Yes, until deleted or expired | Deleted by caller, or by garbage collection after expiry |
| Fork snapshot | Fork point snapshot on the fork only | Deleted by garbage collection after the fork retires |

An expired snapshot continues protecting its manifest until garbage collection removes the snapshot record ([Snapshots](format.md#8-snapshots)).

## Maintenance and deletion

| Operation | What it does |
| --- | --- |
| Fold | Moves committed WAL tail records into new metadata segments and publishes the next manifest. |
| Compaction | Rewrites metadata runs into fewer runs and drops obsolete rows without altering any retained view. |
| Retention advance | Verifies the current manifest's segments and writes a [floor object](glossary.md#floor-object) to the WAL. The next fold copies the new floor into a manifest, allowing compaction to remove older history. |
| Garbage collection | Deletes unreferenced objects past the grace window from a fresh root scan. Namespace collections clean manifests, WAL commit objects, segments, and snapshots, while pool collections clean content objects, upload records, and temporary objects across all pool members. |

Deleting a file records a reversible subtree tombstone, and deleting a namespace publishes a final manifest with status `deleted`. Neither reclaims content immediately.

A deleted namespace becomes eligible for retirement once the grace window has elapsed and no snapshots remain on it besides its own fork point snapshot ([Retirement](format.md#95-retirement)). Retiring a namespace deletes its fork snapshots (if it is a fork), deletes its pool [member entry](glossary.md#member-entry), and raises its manifest boundary to `retired`. Later namespace collections delete everything under its prefix except the boundary object, fences, and floor objects ([Retiring an eligible tombstone](format.md#118-retiring-an-eligible-tombstone)). Pool collections delete unreferenced content objects once no pool member references them and they pass the grace window ([Content roots](format.md#119-content-roots)).

Hosts choose when and where to run maintenance, while the format defines what each operation may publish or delete. Extensions such as grep store their segment lists inside the namespace manifest and publish updates as the next manifest, so garbage collection cleans up unreferenced extension segments under the same age rule as metadata segments ([Extensions](format.md#124-extensions)).
