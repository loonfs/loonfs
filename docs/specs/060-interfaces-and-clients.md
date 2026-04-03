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

### 3.1 Filesystem CLI or app

This client uses the path-oriented surface.

Typical behavior:

- `ls`, `stat`, `get`, `put`, `mv`, and `cp` use user-visible paths;
- the server resolves those paths to canonical inodes as needed;
- small commands are often sessionless;
- large or recursive commands may use server-side sessions or jobs.

This client does not need a sync database to be a first-class client.

### 3.2 Sync client

This client maintains durable local state and consumes the change feed over time.

Typical behavior:

- maintains a durable cursor;
- projects remote state into local state;
- may upload content and publish explicit commits;
- preserves conflicts according to the client's conflict policy.

### 3.3 Service writer or batch tool

This client stages content and publishes explicit commits, but it does not necessarily maintain a long-lived local mirror.

Typical behavior:

- content hashing and upload;
- explicit commit with preconditions;
- request-id based retry.

### 3.4 Operator or admin tool

This client uses low-level recovery or inspection surfaces that are specific to an implementation or deployment.

## 4. Statefulness summary

The table below is intentionally short. It captures the core split without turning stateful transfers into the main story of the spec.

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
| Content hashing and upload | May assist or delegate | Usually responsible for reading local bytes and uploading missing content. |
| Commit validation | Authoritative | Supplies preconditions and request ids where needed. |
| Namespace visibility | Authoritative | Observes committed results. |
| Long-running transfer progress | Authoritative for sessions or jobs that affect correctness | Responsible for local temp files, local progress, and retry behavior. |
