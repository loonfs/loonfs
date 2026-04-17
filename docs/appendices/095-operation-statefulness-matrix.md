## 095. Operation Statefulness Matrix

This section defines when a LoonFS operation is a single-request operation and when it uses a control object. It also defines the split of responsibility between the client and server for common filesystem operations. Recursive `put` and recursive `cp` may also be coordinated by implementation-specific helpers, but those helpers are outside the core interoperable model.

### 095.1. Purpose

LoonFS supports both one-shot operations and long-running operations. One-shot operations are fully described by a single request and normally do not require durable control-plane state. Long-running operations may span multiple requests, may require a stable snapshot or destination binding, and may need resumability across client or server restarts.

This section standardizes:

- which classes of operations require server-side control-plane objects;
- which classes of operations are normally single-request operations;
- which part of each operation is authoritative on the server; and
- which part of each operation is maintained by the client.

### 095.2. Definitions

For the purposes of this specification:

| Term | Meaning |
| --- | --- |
| **Single-request operation** | An operation that is fully described by a single request and does not require a server-side control object after the request completes. |
| **Control object** | A server-side control-plane object used when the client continues driving an operation across multiple requests. Examples include `UploadHandle`, and `PutIntent`. |
| **Implementation-specific coordinator** | A helper resource or service that correlates multiple logical commits for one higher-level workflow. Coordinators are outside the core interoperable model and do not define namespace history. |
| **Authoritative operation state** | The state that determines the correctness, visibility, and resumability of an operation. |
| **Transfer progress** | Non-authoritative progress information such as completed bytes, completed files, local temporary outputs, or user-interface counters. |

### 095.3. Normative rules

1. A LoonFS operation **MAY** be sessionless only when all of the following are true:
   - the operation is fully described by one request;
   - the operation completes synchronously;
   - no pinned read snapshot is required after the request returns;
   - no stable destination binding is required after the request returns; and
   - the request can be retried by replaying the full request.

2. A LoonFS operation **MUST** use a server-side handle when any of the following are true:
   - the client will continue the operation across multiple requests;
   - the operation requires a pinned snapshot for consistent reads;
   - the operation requires a stable destination binding across time;
   - the operation requires resumable multi-part upload; or
   - loss of server restart state would change correctness, retention safety, or promised resumability guarantees.

3. The core specification does **NOT** require a server-side job object for recursive `put` or recursive `cp`. Those workflows may be realized as one or more logical commits, optionally coordinated by implementation-specific helpers outside the core model.

4. When a server-side control-plane object is used, the authoritative identity of the in-flight interaction **MUST** become the server-issued object identifier. After that point, the original path string is entry input only and **MUST NOT** remain the sole identifier of the in-flight interaction.

5. Server-side control-plane objects **MUST NOT** advance namespace `seq`, **MUST NOT** appear as filesystem-visible resources, and **MUST NOT** appear in the namespace change feed.

6. Implementation-specific coordinators **MAY** exist, but they **MUST NOT** redefine logical commit boundaries or change-feed semantics.

### 095.4. Operation statefulness matrix

| Operation | Typical execution shape | Core server-side control-plane object | Long-lived server state required? | Server is authoritative for | Client is authoritative for |
| --- | --- | --- | --- | --- | --- |
| `get <file>` | Single-request read | none | No | path resolution, access check, selected file revision, content serving or delegated download | local download progress, temporary file, client retries |
| `put <file>` (small, one-shot convenience) | Single request | none | No | destination resolution, validation, metadata commit | request payload, client retries |
| `put <file>` (large or resumable) | Begin, upload, commit | `PutIntent` and `UploadHandle` | Yes | stable destination binding, expected slot or revision, upload handle validity, final publish | file reading, hashing, block upload progress, retry tokens |
| `cp <file>` (same server) | Single-request server-side copy | none | No | source resolution, destination resolution, metadata publication, content reference reuse | request retry |
| `cp remote -> local` | Alias for `get` or `get -r` | same as `get` | same as `get` | same as `get` | same as `get` |
| `cp local -> remote` | Alias for `put` or `put -r` | same as `put` | same as `put` | same as `put` | same as `put` |

### 095.5. Client and server split for common commands

The following table is the normative split of responsibility for the primary filesystem commands.

| Command | Server responsibilities | Client responsibilities |
| --- | --- | --- |
| `get <file>` | resolve the requested path or handle; authorize the read; select the file revision to read; serve bytes or delegated download targets | receive bytes; write local output; maintain local retry and resume state |
| `put <file>` (one-shot) | resolve the destination; validate preconditions; publish the metadata change | supply bytes or content reference; retry the request if needed |
| `put <file>` (resumable) | create the `PutIntent`; bind the destination; create or validate the `UploadHandle`; validate durable content and commit the final publish | read the local file; chunk and hash it; upload missing blocks; track upload progress; submit the final commit request |
| `cp <file>` (same server) | resolve source and destination; authorize both sides; create the copied resource; publish the metadata change | submit the request; retry if appropriate |

### 095.6. When raw paths cease to identify the operation

LoonFS accepts path-oriented input because user intent is naturally expressed by path. However, long-running operations require a more stable identity than a raw path string.

| Operation | Raw path is used for | Stable in-flight identity after start |
| --- | --- | --- |
| `get <file>` | the request itself | none required |
| `put <file>` (resumable) | `begin_put` only | `PutIntent` and `UploadHandle` |

### 095.7. Control-plane durability guidance

1. A control-object **MUST** be durably recorded if losing it on restart would change correctness, visibility, retention safety, or promised resumability.

2. At minimum, the following control-plane objects are normally durable:
   - `PutIntent`;
   - `UploadHandle`;

3. Implementation-specific coordinators **MAY** also be stored durably, but they are not required by this specification.

4. Control-objects and any implementation-specific coordinators **MUST** remain outside namespace-visible metadata. Their existence is authoritative for orchestration, not for filesystem history.

### 095.8. Recommended defaults

A conforming implementation SHOULD use the following defaults unless a stronger mode is explicitly requested:

- `get <file>` is sessionless;
- `cp <file>` on the same service is sessionless;
- large or resumable `put <file>` uses `PutIntent` and `UploadHandle`;

These defaults preserve a simple model for single-request commands while allowing multi-request correctness and resumability where they actually matter.
