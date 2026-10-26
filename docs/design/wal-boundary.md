# The WAL implementation

LoonFS records committed metadata changes in a write-ahead log (WAL). Each namespace has its own log, stored as numbered, immutable objects. Creating the next object with a conditional write publishes its contents, and a competing write to the same number fails.

The WAL is also part of recovery and writer fencing. Keeping these operations together in `loonfs_core::wal` makes it easier to check that they follow the same rules. This note describes how responsibilities are divided between that module and the rest of LoonFS. The [storage format specification](../specs/format.md) defines the durable format and its requirements.

## WAL numbers and commit sequences

A WAL number identifies an object, and a commit sequence identifies a committed mutation. They're separate counters, because one WAL object can contain several commits, and a fence contains none.

For example, a namespace might have this log:

| WAL number | Contents | Commit sequence after publication |
| --- | --- | --- |
| 24 | Two commits at sequences 40 and 41 | 41 |
| 25 | A fence for a new writer epoch | 41 |
| 26 | One commit at sequence 42 | 42 |

During writer acquisition, LoonFS first records the new writer epoch in a manifest, and then publishes a fence at the next available WAL number. The fence prevents a stale writer from publishing at that number. It doesn't change the filesystem, and it doesn't advance the commit sequence.

These differences matter during recovery and garbage collection. A fence is still a WAL object, even though it contains no commits, so counting WAL objects isn't the same as counting committed changes.

## Module responsibilities

The `wal/` module handles numbered WAL keys, building WAL objects, reads, and publication. Discovery and the collection decisions that are specific to the log are defined there too.

| File | Responsibility |
| --- | --- |
| `frame.rs` | WAL object and tail types, including validation errors |
| `writer.rs` | Building data and fence WAL objects, and calculating the next WAL number and the resulting head |
| `publish.rs` | Conditional writes, and classifying their outcomes as successful, conflicting, or uncertain |
| `discover.rs` | Windowed discovery that keeps the WAL tail, and incremental updates to cached views |
| `reader.rs` | Reading consecutive WAL objects, loading bounded tails, and replaying discovered tails |
| `replay.rs` | Rebuilding metadata state from committed records |
| `projected_tail.rs` | Metadata rows and inline content projected from the unfolded WAL tail |
| `reclaim.rs` | Finding the WAL objects that recovery still needs |

To publish a commit, LoonFS prepares the next WAL object from the namespace's current head and the accepted commits. A read uses a base head and the current head to load and replay the WAL objects between them. The change feed and commit replay read the `commits` metadata family and the projected tail, and they don't load retained WAL objects.

`clippy.toml` restricts the numbered-key builder, with explicit exceptions for WAL storage code and for tests that inspect physical objects. The envelope codecs are still available for format validation and tests.

## Responsibilities outside the WAL

Several parts of the storage protocol involve both the log and other namespace state:

- **Manifest and hint management.** A hint contains a manifest number, and the runtime that published a manifest raises the hint to that manifest after the publication returns. The code that raises the hint is in the control module, and the runtime's publisher calls that code. LoonFS paces compare-and-swap writes to each object to one per second (see the [provider reference](../specs/object-storage-providers.md#1-frequently-used-data-points)), so the publisher combines the raises requested while an earlier raise waits into one raise to the newest manifest, and a publication doesn't wait for the hint. WAL discovery starts at the manifest's folded position and reads forward in windows.
- **Garbage collection.** Listing objects, checking their ages, and deleting them are handled in `gc/`. For a live namespace, a WAL object is required if its number is above the folded position. An object at or below that position goes through the remaining collection checks.
- **Position tracking.** WAL numbers appear in the namespace head and in the durable manifest. The differences between them count the unfolded WAL objects, and LoonFS uses those counts to enforce maintenance and write limits.

The module boundary separates the WAL operations, but code elsewhere in LoonFS still uses the numbered-log model.

## Supporting another WAL implementation

LoonFS has one WAL implementation, and an internal module is enough to organize it. A trait for interchangeable backends would require decisions about storage behavior, and there's no second implementation to validate those decisions against.

For example, a hosted log might use offsets that can't be represented as consecutive WAL numbers, and it might handle writer fencing and history removal differently. Before a shared interface could be introduced, both implementations would need defined behavior for:

- **Positions.** The manifest stores the folded WAL number. Different offsets would require a mapping or a change to the storage format.
- **Writer fencing.** A replacement must prevent stale writers from committing after a takeover, including when a request has an uncertain outcome. The coordination between the log and the manifest's writer epoch must be explicit.
- **Discovery.** Readers need a reliable way to find the committed end of the log. In object storage, discovery probes consecutive numbers until one is missing, but a hosted log might use a different mechanism.
- **Retention.** Removing old records must preserve the history promised to readers, and the state needed to prevent stale writes or the reuse of a position.
- **Namespace binding.** The namespace must durably identify its log implementation and location. An unsupported or unavailable log must produce an error, because opening an empty replacement would lose acknowledged history.

Keeping the WAL operations together limits the code that this work would involve. A replacement might still require changes to the manifest format, writer acquisition, and the retention protocol.
