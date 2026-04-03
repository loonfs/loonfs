# Interfaces and Clients

## 1. Two public operation surfaces

LoonFS has two peer public surfaces.

| Surface | Purpose | Shape |
| --- | --- | --- |
| **Filesystem operations** | User-facing filesystem actions | Path-oriented commands such as `ls`, `stat`, `get`, `put`, `mkdir`, `mv`, `cp`, and `rm`. |
| **Upload, commit, and change feed** | Explicit staging, publication, and incremental consumption | Explicit content staging, explicit commit, and incremental change consumption. |

The filesystem surface exists because user intent is naturally path-based. The upload-and-commit surface exists because long-running and stateful clients need explicit control over staging, validation, and replay.

A single client may use or expose both surfaces.

Both surfaces share the same namespace, inode, content, and visibility rules.

## 2. Representative HTTP binding

HTTP is one transport binding for these abstract operations. It is not the underlying semantics.

A representative v0 binding is shown below.

| Purpose | Representative HTTP shape |
| --- | --- |
| Stat a path | `GET /v0/namespaces/{ns}/filesystem/stat?path=/docs/report.txt` |
| List a path | `GET /v0/namespaces/{ns}/filesystem/list?path=/docs` |
| Read file content | `GET /v0/namespaces/{ns}/filesystem/content?path=/docs/report.txt` |
| Apply path-oriented operations | `POST /v0/namespaces/{ns}/filesystem/operations` |
| Begin or prepare upload | `POST /v0/namespaces/{ns}/uploads` |
| Publish an explicit commit | `POST /v0/namespaces/{ns}/commits` |
| Read committed changes | `GET /v0/namespaces/{ns}/changes?after_seq=123` |

Long-running transfers may additionally expose session or job resources. The exact endpoint set is less important than the semantic rule: once a long-running operation begins, the server-issued session or job id becomes the stable in-flight identifier of that operation.

## 3. Client profiles

These profiles are defined by the surface a client uses, not by whether the implementation is a
CLI, desktop app, web app, SDK, or service. A single client may implement more than one profile.

### 3.1 Path-oriented client

This client uses the path-oriented surface.

Typical behavior:

- `ls`, `stat`, `get`, `put`, `mv`, and `cp` use user-visible paths;
- the server remains authoritative for path resolution, canonical inode identity, and commit validation;
- small commands are often sessionless;
- large or recursive commands may use server-side sessions or jobs.

This client does not require a sync database or full local mirror to be a first-class client.
Implementations may still keep durable local state such as auth/session state, retry journals,
pinned snapshot ids, or inode context learned from prior responses when that improves usability,
restart safety, or resumability.

### 3.2 Sync client

This client maintains durable local state and consumes the change feed over time.

Typical behavior:

- maintains a durable cursor;
- projects remote state into local state;
- may upload content and publish explicit commits;
- preserves conflicts according to the client's conflict policy.

### 3.3 Explicit-commit client

This client uses the upload, commit, and change-feed surface more directly. It stages content and
publishes explicit commits, but it does not necessarily maintain a long-lived local mirror.

Typical behavior:

- content hashing and upload;
- explicit commit with preconditions and request ids;
- change-feed reads or cursors where incremental observation is needed.

### 3.4 Operator or admin tool

This client uses low-level recovery or inspection surfaces that are specific to an implementation or deployment.

## 4. Statefulness summary

The table below is intentionally short. It captures the core split without turning stateful transfers into the main story of the spec.
For more detailed command-oriented guidance, see [Appendix 095: Operation Statefulness Matrix](../appendices/095-operation-statefulness-matrix.md).

| Operation | Usual shape | Typical server-side state |
| --- | --- | --- |
| `get <file>` | One request | None after the request completes. |
| `get -r <dir>` | Multi-request snapshot read | A read session may pin a consistent snapshot. |
| `put <file>` | One request for small files; staged upload for large files | A put intent and upload session may bind the destination and upload. |
| `put -r <dir>` | Import-style operation | An import job may coordinate a large tree upload. |
| `cp <file>` on one service | One request | Usually none after the request completes. |
| `cp -r <dir>` on one service | Server-side job | A copy job may coordinate traversal and publication. |

## 5. Client and server responsibilities

| Concern | Server | Client |
| --- | --- | --- |
| Path resolution | Authoritative | Supplies user intent by path when using the filesystem surface. |
| Content hashing and upload | May accept direct bytes, proxy uploads, or issue upload capabilities, but must verify that any content referenced by a commit is already durable. | Usually responsible for reading local bytes, computing content hashes, and uploading missing content when originating new data. |
| Commit validation | Authoritative | Supplies preconditions and request ids where needed. |
| Namespace visibility | Authoritative | Observes committed results. |
| Long-running transfer progress | Authoritative for sessions or jobs that affect correctness | Responsible for local temp files, local progress, and retry behavior. |
