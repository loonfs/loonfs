## 095. Operation Statefulness Matrix

This section defines when a LoonFS operation is sessionless, when it uses a server-side session, and when it uses a server-side job. It also defines the split of responsibility between the client and server for common filesystem operations.

### 095.1. Purpose

LoonFS supports both one-shot operations and long-running operations. One-shot operations are fully described by a single request and normally do not require durable control-plane state. Long-running operations may span multiple requests, may require a stable snapshot or destination binding, and may need resumability across client or server restarts.

This section standardizes:

- which classes of operations require server-side control-plane objects;
- which classes of operations are normally sessionless;
- which part of each operation is authoritative on the server; and
- which part of each operation is maintained by the client.

### 095.2. Definitions

For the purposes of this specification:

| Term | Meaning |
| --- | --- |
| **Sessionless operation** | An operation that is fully described by a single request and does not require a server-side session or job after the request completes. |
| **Session** | A server-side control-plane object used when the client continues driving an operation across multiple requests. Examples include `ReadSession`, `UploadSession`, and `PutIntent`. |
| **Job** | A server-side control-plane object used when the server continues performing work after the initiating request returns. Examples include `CopyJob` and `ImportJob`. |
| **Authoritative operation state** | The state that determines the correctness, visibility, and resumability of an operation. |
| **Transfer progress** | Non-authoritative progress information such as completed bytes, completed files, local temporary outputs, or user-interface counters. |

### 095.3. Normative rules

1. A LoonFS operation **MAY** be sessionless only when all of the following are true:
   - the operation is fully described by one request;
   - the operation completes synchronously;
   - no pinned read snapshot is required after the request returns;
   - no stable destination binding is required after the request returns; and
   - the request can be retried by replaying the full request.

2. A LoonFS operation **MUST** use a server-side session when any of the following are true:
   - the client will continue the operation across multiple requests;
   - the operation requires a pinned snapshot for consistent reads;
   - the operation requires a stable destination binding across time;
   - the operation requires resumable multi-part upload; or
   - loss of server restart state would change correctness, retention safety, or resumability guarantees.

3. A LoonFS operation **MUST** use a server-side job when the server continues executing work after the initiating request returns.

4. When a server-side session or job is used, the authoritative identity of the operation **MUST** become the server-issued session or job identifier. After that point, the original path string is entry input only and **MUST NOT** remain the sole identifier of the in-flight operation.

5. Server-side sessions and jobs are control-plane objects. They **MUST NOT** advance namespace `seq`, **MUST NOT** appear as filesystem-visible resources, and **MUST NOT** appear in the namespace change feed.

### 095.4. Operation statefulness matrix

| Operation | Typical execution shape | Server-side control-plane object | Long-lived server state required? | Server is authoritative for | Client is authoritative for |
| --- | --- | --- | --- | --- | --- |
| `get <file>` | Single-request read | none | No | path resolution, access check, selected file revision, content serving or delegated download | local download progress, temporary file, client retries |
| `get -r <dir>` | Multi-request recursive snapshot read | `ReadSession` | Yes | resolved root, pinned snapshot, traversal policy, optional retention hold | traversal order, local materialization, completed files, local resume state |
| `put <file>` (small, one-shot convenience) | Single request | none | No | destination resolution, validation, metadata commit | request payload, client retries |
| `put <file>` (large or resumable) | Begin, upload, commit | `PutIntent` and `UploadSession` | Yes | stable destination binding, expected slot or revision, upload session validity, final publish | file reading, hashing, block upload progress, retry tokens |
| `put -r <dir>` (small tree) | Upload then one batched commit | implementation-defined; often none | Usually no | final batched metadata commit | local traversal, file hashing, upload progress |
| `put -r <dir>` (general or resumable) | Long-running import | `ImportJob`, or durable set of `PutIntent`s | Yes | destination root binding, per-item publish state, restart-safe import state | local traversal, file hashing, user-visible progress |
| `cp <file>` (same server) | Single-request server-side copy | none | No | source resolution, destination resolution, metadata publication, content reference reuse | request retry |
| `cp -r <dir>` (same server) | Long-running recursive server-side copy | `CopyJob` | Yes | source snapshot, destination binding, traversal, copy progress, final publish | start, poll, cancel, progress display |
| `cp remote -> local` | Alias for `get` or `get -r` | same as `get` | same as `get` | same as `get` | same as `get` |
| `cp local -> remote` | Alias for `put` or `put -r` | same as `put` | same as `put` | same as `put` | same as `put` |
| `cp serverA -> serverB` | Cross-service transfer | source `ReadSession`; destination `PutIntent`, `UploadSession`, or `ImportJob` | Yes, but split across services | each service is authoritative for its own side only | end-to-end coordination, bridging retries, overall progress |

### 095.5. Client and server split for common commands

The following table is the normative split of responsibility for the primary filesystem commands.

| Command | Server responsibilities | Client responsibilities |
| --- | --- | --- |
| `get <file>` | resolve the requested path or handle; authorize the read; select the file revision to read; serve bytes or delegated download targets | receive bytes; write local output; maintain local retry and resume state |
| `get -r <dir>` | create the `ReadSession`; pin the authoritative snapshot; enforce traversal policy such as mount-crossing rules; serve entry listings and file content within the session | traverse the returned directory tree; schedule downloads; create local directories and files; track completed outputs |
| `put <file>` (one-shot) | resolve the destination; validate preconditions; publish the metadata change | supply bytes or content reference; retry the request if needed |
| `put <file>` (resumable) | create the `PutIntent`; bind the destination; create or validate the `UploadSession`; validate durable content and commit the final publish | read the local file; chunk and hash it; upload missing blocks; track upload progress; submit the final commit request |
| `put -r <dir>` | validate and publish the imported remote tree; if an `ImportJob` is used, maintain authoritative import state | walk the local tree; produce upload content; track local progress; optionally poll job state |
| `cp <file>` (same server) | resolve source and destination; authorize both sides; create the copied resource; publish the metadata change | submit the request; retry if appropriate |
| `cp -r <dir>` (same server) | create the `CopyJob`; pin the source snapshot; bind the destination; execute traversal and publication; report progress and final result | initiate the job; poll status; cancel if supported; display progress |

### 095.6. When raw paths cease to identify the operation

LoonFS accepts path-oriented input because user intent is naturally expressed by path. However, long-running operations require a more stable identity than a raw path string.

| Operation | Raw path is used for | Stable in-flight identity after start |
| --- | --- | --- |
| `get <file>` | the request itself | none required |
| `get -r <dir>` | opening the recursive read | `ReadSession` and session-scoped entry handles |
| `put <file>` (resumable) | `begin_put` only | `PutIntent` and `UploadSession` |
| `put -r <dir>` (resumable) | opening the import | `ImportJob`, or destination-root handle plus per-file intents |
| `cp -r <dir>` (same server) | creating the copy operation | `CopyJob` |

### 095.7. Control-plane durability guidance

1. A session or job **MUST** be durably recorded if losing it on restart would change correctness, visibility, retention safety, or promised resumability.

2. At minimum, the following control-plane objects are normally durable:
   - `PutIntent`;
   - `UploadSession`;
   - `CopyJob`; and
   - any `ReadSession` or `ImportJob` that promises restart-safe resumption or pins retention-sensitive content.

3. Sessions and jobs **MAY** be stored in object storage using the same create-if-absent, compare-and-swap, and read-after-write guarantees used elsewhere in LoonFS. A separate transactional database is not required by this specification.

4. Sessions and jobs **MUST** remain outside namespace-visible metadata. Their existence is authoritative for orchestration, not for filesystem history.

### 095.8. Recommended defaults

A conforming implementation SHOULD use the following defaults unless a stronger mode is explicitly requested:

- `get <file>` is sessionless;
- `cp <file>` on the same service is sessionless;
- `get -r <dir>` uses a `ReadSession`;
- large or resumable `put <file>` uses `PutIntent` and `UploadSession`;
- same-service `cp -r <dir>` uses a `CopyJob`; and
- recursive `put` uses an `ImportJob` once resumability, restart safety, or large-tree import is required.

These defaults preserve a simple model for single-request commands while providing stable identities for long-running operations.
