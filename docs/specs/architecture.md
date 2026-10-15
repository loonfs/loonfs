# LoonFS architecture

This document describes how LoonFS stores a filesystem in object storage, and how reads, writes, forks, and maintenance fit together. The [format specification](format.md) defines the exact rules, and the [glossary](glossary.md) defines each term.

LoonFS stores everything in object storage. File bytes are stored in content objects, and every change to the directory tree is recorded in a numbered write-ahead log (WAL). LoonFS uses five object-store operations: put-if-absent, compare-and-swap on small objects, reads, listings, and deletes. It doesn't use transactions across objects, so every change to a namespace becomes durable through a single object write.

Each "namespace" is one directory tree with its own history, stored under `namespaces/{namespace_id}/`.

An application can use LoonFS as an embedded Rust library (`loonfs`) or through the HTTP API, which `loonfs-http` implements and `loonfs-server` runs as a standalone server. LoonFS coordinates writers through object-storage writes, so a change of writer takes about 200 ms. Because of that, most applications are better served by one long-lived writer in a server that many clients share.

## Stored objects

Each namespace is stored as six main kinds of objects:

| Object | What it stores |
| --- | --- |
| Manifest | The namespace's identity and status, the current writer, the metadata runs that make up its files and directories, and its retention floor. Manifests are numbered. |
| WAL object | Commits made after the most recent manifest, or a writer fence. WAL objects are numbered. |
| Hint | The number of a recent manifest, where readers start looking for the current one. |
| Pin | A reference to one manifest that keeps it, and everything it lists, from being deleted. Checkpoints, snapshots, and forks use pins. |
| Metadata segment | Sorted metadata rows, in blocks that can be read independently. |
| Content object | File bytes. Content objects are immutable, and a file's bytes can span several of them. |

Manifests and WAL objects are numbered 1, 2, 3, and so on, with a separate counter for each. LoonFS creates every numbered object with put-if-absent, so when two processes try to write the same number, exactly one of them succeeds. Successfully writing the next WAL object commits every record in it. Writing the next manifest publishes a new set of metadata segments or a change to the namespace's state (for example, a new writer or a deletion).

The hint is a best-effort pointer to a recent manifest. It's a safe place to start, but not an authoritative pointer to the current manifest, so readers load the hinted manifest and then check for newer ones. The [format specification](format.md#2-objects-and-references) defines the complete layout and the stored fields.

Why have a hint at all? Without it, a reader would have to list the namespace's manifests to find a place to start, and object-storage listings are slow (LoonFS keeps them off every hot path). Why is it best effort? Updating the hint is a separate write from publishing a manifest. Keeping the two in step would need a transaction across two objects, which object storage doesn't support.

## Writing a file

A file change is committed in two parts. First the file's bytes become durable, and then a commit records the change in the WAL. File content can be published via one of two paths.

Uploaded content is written and verified before the commit that references it:

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

Completing an upload doesn't automatically update a file in a LoonFS namespace. The file changes only when the WAL object that commits the change is written. (If the upload is never committed, garbage collection deletes it later.)

A small file's bytes can be included in the commit itself, as "inline" content. One WAL write then makes the bytes durable and the file visible. Later, a fold writes the bytes to a content object before it publishes the manifest that allows garbage collection to delete that WAL object. Appends use the same path, and the WAL object includes only the appended bytes.

```text
validate metadata -> create next numbered WAL object with the bytes
                                        |
                                  file committed
                                        |
                                        v
                 fold writes the content object, then the manifest
```

[File contents and ownership](format.md#15-file-contents-and-ownership) defines both paths. Several requests can share one WAL object, but each accepted request still has its own sequence number and commit ID.

Only one writer session can commit to a namespace at a time. A new writer session takes over in two steps:

1. It publishes the next manifest with a higher writer epoch.
2. It writes an empty WAL object (a "fence") with that epoch at the next WAL number.

Between the two steps, the previous writer can still commit, and those commits are valid. Once the fence is written, the previous writer can't commit anymore. Its next WAL write collides with an object that already exists, and when it reloads the namespace, it finds the higher epoch and stops with `writer_fenced`.

A lost response can hide a successful commit. To find out what happened, a client retries with the same commit ID and the same request. If the original commit succeeded, LoonFS returns its result instead of committing again.

Warning: when a client retries a commit, it shouldn't retry the upload too. If the upload is retried, it's assigned a different content ID, so the retried commit is no longer the same request. If the original commit succeeded, the retry would fail with `commit_id_reuse_conflict` instead of returning the original result.

Why use epochs instead of a writer lease? With a lease, a new writer would have to wait for the old lease to expire. With epochs, a new writer can take over at any time, and correctness depends only on conditional writes.

## Reading a file

To read a namespace, a reader finds the current manifest and the last WAL object (the "WAL tip"). It reads the metadata runs that the manifest lists, then replays the WAL objects written after the manifest's folded boundary. The result is a view of the namespace at one sequence number.

```text
current manifest -> listed metadata runs --+
                                           +--> view at one sequence
later numbered WAL -> committed changes ---+            |
                                                        v
                                        path -> inode -> revision -> content
```

A new namespace's first manifest describes only the root directory. A fork's first manifest lists the metadata runs it inherited from its source.

LoonFS resolves a path by following [directory bindings](glossary.md#directory-binding-direntry) from the root. Each file revision has a content reference, which records the content's owner namespace, a content ID, a size, and a checksum. The owner and content ID determine the keys of the objects that store the bytes, so a fork can read inherited content without loading its source's manifest or walking its fork ancestry. Inline content is read from the WAL until a fold writes it to a content object.

Directory listings use committed metadata for names and file sizes, so listing a directory doesn't download any files. Reading a file verifies its complete size and checksum. If an object needed to recover the namespace is missing or corrupt, the read fails.

In a process without the namespace's writer session, a `latest` read (the default) from a cached view checks the next WAL number for new commits, and it checks for a newer manifest about once a second by default. A `bounded` read skips both checks while the view was confirmed less than its interval ago (one second by default), so it may miss commits made since (see [Read freshness](api.md#read-freshness)). In the writer's process, reads take the head from the writer session's [position](glossary.md#writer-position), which includes every commit that the session made, so a read sends no request to confirm the head apart from the same check for a newer manifest. Because WAL objects and content objects are immutable, a reader also keeps recently read WAL objects and small content objects in memory and doesn't need to invalidate them. Local caches affect only read cost and freshness. They aren't needed for recovery.

Why fail instead of reading an older version of the files? A fallback would return files from an earlier point in history, and the caller would have no way to tell.

## Forks and retained views

A "fork" is a new namespace that starts as a copy of another namespace at one point in its history. To create one, LoonFS writes and verifies a [pin](glossary.md#pin) on the source's manifest, and then writes the fork's manifest 1, which lists the source's metadata runs. The fork shares the source's content and metadata objects, so nothing is copied (and the cost of a fork doesn't depend on how much data the source stores). Forking the source's current head may first fold the source's WAL tail, which writes its inline content to content objects. After the fork, commits to one namespace never appear in the other.

A fork can keep reading objects owned by its source and the source's ancestors. When the fork publishes its own manifests, those objects stay where they are. The source keeps its pin for the fork until the fork is deleted and retired.

User checkpoints and snapshots are pins too, with different rules for reading and removal. Snapshots are for applications. The read endpoints accept a `snapshot_id`, and every snapshot has a required expiry time and counts against a per-namespace limit, so applications can't keep old data around indefinitely. Checkpoints, by contrast, are for operators. They're managed through the maintenance API, and a checkpoint can be kept without an expiry time. Each create call writes a new pin with a new ID, so a checkpoint's name is a label, not a key (two checkpoints can have the same name).

| Pin | Readable | Removed |
| --- | --- | --- |
| Checkpoint | While the pin exists, even after its expiry time | When it's deleted, or by garbage collection after its expiry time (if it has one) |
| Snapshot | Only before it expires | When it's deleted, or by garbage collection after it expires |
| Fork | Not read directly | By garbage collection, after the fork no longer depends on it |

An expired pin still protects its manifest until garbage collection deletes the pin (see [Pins](format.md#8-pins)).

## Maintenance and deletion

| Operation | What it does |
| --- | --- |
| Fold | Moves committed records from the WAL into new metadata segments, and publishes the next manifest. |
| Compaction | Merges metadata runs and drops rows that are no longer needed, without changing any retained view. |
| Retention advance | Moves the [retention floor](glossary.md#retention-floor) forward, after checking that the current manifest's segments exist. Compaction can then remove history at or below the floor. |
| Garbage collection | Deletes objects that nothing references anymore, once they're old enough. Each pass starts from a fresh list of what's still referenced. |

Deleting a file records a subtree tombstone, which can be undone. Deleting a namespace publishes a final manifest with a deleted status. Neither one deletes shared content right away.

A deleted namespace's own content is deleted when the namespace is retired, which happens once a grace period has passed and no pins on the namespace remain (see [Retirement](format.md#95-retirement)). Retirement also deletes a fork's pin on its source (see [Sweeping a retired owner's content](format.md#118-sweeping-a-retired-owners-content)).

Hosts decide when maintenance runs and which namespaces it covers. The format defines what each operation can publish or delete. Extensions such as grep have their own manifests and garbage collection rules, and core garbage collection never deletes their objects.
