# The WAL implementation

LoonFS records committed metadata changes in a write-ahead log, or WAL. Each namespace has its own log, stored as numbered, immutable objects. Creating the next object with a conditional write publishes its contents; a competing write to the same number fails.

The WAL is also part of recovery, writer fencing, and history retention. Keeping these operations together in `loonfs_core::wal` makes it easier to check that they follow the same rules. This note describes that division of responsibility. The [storage format specification](../specs/format.md) defines the durable format and its requirements.

## WAL numbers and commit sequences

A WAL number identifies an object. A commit sequence identifies a committed mutation. They are different counters: one WAL object can contain several commits, and a fence contains none.

For example, a namespace might have this log:

| WAL number | Contents | Commit sequence after publication |
| --- | --- | --- |
| 24 | Two commits at sequences 40 and 41 | 41 |
| 25 | A fence for a new writer epoch | 41 |
| 26 | One commit at sequence 42 | 42 |

During writer acquisition, LoonFS first records the new writer epoch in a manifest, then publishes a fence at the next available WAL number. The fence prevents a stale writer from successfully publishing at that number. It does not change the filesystem or advance the commit sequence.

These distinctions matter during recovery and collection. A fence is still a WAL object, even though it contains no commits, and counting WAL objects is different from counting committed changes.

## Module responsibilities

Numbered WAL keys, segment construction, reads, and publication are handled in `wal/`. Discovery and retention decisions specific to the log are defined there as well.

| File | Responsibility |
| --- | --- |
| `frame.rs` | Segment and tail types, including validation errors |
| `writer.rs` | Construction of data and fence segments, calculation of the next WAL number and resulting head |
| `publish.rs` | Conditional writes and classification of successful, conflicting, and uncertain outcomes |
| `discover.rs` | Discovery of the latest WAL state and incremental updates to cached views |
| `reader.rs` | Loading and validation of a bounded range of segments |
| `replay.rs` | Reconstruction of metadata state from committed records |
| `reclaim.rs` | Identification of WAL objects still required by retention |

Commit publication uses the current namespace head and accepted commits to prepare the next segment. Reads use a base head and current head to load and replay the intervening WAL. For the change feed, the retained segments are loaded through the current head, then converted into filesystem events. The numbered ranges for these reads are calculated inside `wal/`.

The numbered-key builder is restricted by `clippy.toml`, with explicit exceptions for WAL storage code and tests that inspect physical objects. The envelope codecs remain available for format validation and tests.

## Responsibilities outside the WAL

Several parts of the storage protocol involve both the log and other namespace state:

- **Manifest and hint management.** A hint contains both a manifest number and a WAL number. Its updates remain in the control module, with their frequency managed by the runtime. WAL discovery uses the hinted WAL number from the loaded manifest.
- **Garbage collection.** Object enumeration, age checks, and deletion remain in `gc/`. The WAL retention rules determine which numbered objects must be preserved. For a live namespace, an object remains required if its number is above either the folded position or the retention floor. An object at or below both positions is eligible for the remaining collection checks.
- **Position tracking.** WAL numbers appear in the namespace head and durable manifest. Their differences are used to count unfolded segments and enforce maintenance and write limits.

The module boundary therefore separates WAL operations while preserving the numbered-log model used elsewhere in LoonFS.

## Supporting another WAL implementation

LoonFS currently has one WAL implementation. An internal module is sufficient to organize it; a trait for interchangeable backends would require decisions about storage behavior that have no second implementation to validate them against.

A hosted log, for example, might use offsets that cannot be represented as consecutive WAL numbers. It might also handle writer fencing and history removal differently. Before introducing a shared interface, both implementations would need defined behavior for:

- **Positions.** The manifest stores folded and retention-floor WAL numbers. Different offsets would require a mapping or a storage-format change.
- **Writer fencing.** A replacement must prevent stale writers from committing after takeover, including when a request has an uncertain outcome. Coordination between the log and the manifest's writer epoch must be explicit.
- **Discovery.** Readers need a reliable way to identify the committed end of the log. In object storage, discovery probes consecutive numbers until one is absent; a hosted log may use a different mechanism.
- **Retention.** Removing old records must preserve the history promised to readers and the state needed to prevent stale writes or position reuse.
- **Namespace binding.** The namespace must durably identify its log implementation and location. An unsupported or unavailable log must produce an error; opening an empty replacement would lose acknowledged history.

Keeping the WAL operations together limits the code involved in that work. A replacement may still require changes to the manifest format, writer acquisition, and retention protocol.
