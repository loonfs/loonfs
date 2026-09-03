# LoonFS Architecture Overview

## 1. Major parts

| Part | Role |
| --- | --- |
| **Object store** | Holds every durable object: content-store blobs, namespace WAL segments, namespace manifests, descriptors, checkpoint records, and small control objects. |
| **Authoritative runtime** | Resolves paths, validates mutations, writes logical commits into WAL segments, advances heads, serves reads, and issues capabilities for upload or download. |
| **Clients** | Use either direct filesystem operations or the lower-level upload, commit, and change-feed model. |
| **Access-control service** | Evaluates ACLs and shares, then authorizes LoonFS operations. This may be part of the authoritative runtime in a simple deployment. |
| **Background workers** | Publish namespace manifests, create checkpoint records, advance retention safely, clean up expired control objects, and reclaim unreachable content. |

The embedded runtime exposes `FsReader`, `FsWriter`, and `FsMaintenance` handles.
Snapshot listing is an `FsReader` operation; snapshot create, extend, and release are `FsWriter` operations.

Namespaces and content stores are separate durable domains. A namespace owns filesystem metadata and history; a content store owns immutable file bytes. A namespace descriptor references exactly one content store, but that reference is not lifecycle ownership. Forked namespaces share the source namespace's content store while keeping independent future metadata history. Fork provenance and GC pins may record source-owned immutable files needed by the fork.

## 2. Data plane, metadata plane, and control plane

| Plane | Purpose | Examples | Namespace-visible history? |
| --- | --- | --- | --- |
| **Data plane** | Stores and serves file bytes. | Whole-file content objects and download streams. | No, by itself. |
| **Metadata plane** | Defines the filesystem's durable truth. | WAL segments, namespace head, manifests, checkpoints, inode and direntry state. | Yes. |
| **Control plane** | Coordinates multi-request work and authorization. | Upload sessions. | No. |

Two rules follow from this split:

1. The metadata plane is authoritative for filesystem state.
2. Control-plane objects may be durable, but they do not advance namespace `seq` and do not appear in the change feed.

Control-plane state should still be durable when losing it on restart would violate correctness, restart safety, or promised resumability.

## 3. Client usage patterns

A client pattern is defined by the protocol surface a client uses, not by what the client is: a CLI, desktop app, or service may implement several. (API *planes* — `filesystem/v0`, `maintenance/v0` — are a different concept; see `api.md`.)

| Client pattern | Primary surface | Typical state |
| --- | --- | --- |
| **Path-oriented client** | Filesystem operations such as `ls`, `stat`, `get`, `put`, `mv`, and `cp` | Often little or no durable local state beyond transient request context. |
| **Batch-writing client** | Staged upload, commit ids, multi-operation commits, and change cursors | Durable retry state for in-flight uploads and requests, but not necessarily a full local projection. |
| **Sync client** | Change feed plus durable local projection, with optional writes | Durable local state, cursors, and restart-safe reconciliation state. |
| **Operator or admin client** | Recovery, inspection, repair, and low-level operations | Implementation-specific. |


## 4. Operation classes

Most core operations fall into one of two classes.

| Class | Typical examples | Server-side state |
| --- | --- | --- |
| **One-shot** | `ls`, `stat`, `get <file>`, `put <small file>`, `cp <file>` on one service | Usually none after the request completes. |
| **Client-driven long-running** | recursive `get`, resumable `put`, recursive `put`, recursive `cp` realized as several commits | Resumable puts may use an upload session. Other orchestration remains client-side. |

Implementations may additionally expose coordinator-specific helpers for recursive workflows or admin work, but those helpers are outside the interoperable core model.

Control objects and implementation-specific helpers never create a second history model.

## 5. Maintenance

A maintenance job reloads durable state and performs one run for one namespace. A
`MaintenanceRegistry` holds jobs and can execute assignments without a writer or
scheduler. Jobs never schedule work.

A `MaintenanceRunner` is an optional in-process scheduler over a registry. It
owns coalescing by `{job, namespace}`, invocation permits, retry backoff,
not-before deadlines, process-local continuations, reconciliation probes, and
shutdown. Only admitted keys are reconciled.

Writers emit best-effort maintenance hints. Observers must return without
blocking; bounded relays drop hints when full. A lost, duplicate, or late hint
only delays work because durable state, probes, and the change feed recover it.
A run may request one follow-up job for the same namespace, which the runner
coalesces like any other nudge.

### Step results

Each maintenance run returns one of these results:

| Result | Meaning | Runner action |
| --- | --- | --- |
| `progressed` | Durable state changed. | Queue another run after other waiting work. |
| `idle` | No work is currently available. | Wait for another hint or reconciliation probe. |
| `blocked` | Work exists but cannot proceed under the current policy or budget. | Wait for a hint, deadline, or reconciliation probe. |
| `superseded` | Another writer won the compare-and-swap race. | Read the new state and try again. |
| `not_enabled` | This job is not enabled for the namespace. | Stop tracking the key. |

A run may also return a continuation cursor and the earliest time its next work
can begin. Continuations are process-local, so every job can restart from
durable state.

### Jobs and admission policy

| Job | Run | Admission |
| --- | --- | --- |
| `metadata` | Flush a due WAL tail and merge one bounded reorganization unit. | After a fold or due WAL publication; reconciliation recovers missed hints. |
| `metadata-compaction` | Run one streaming metadata compaction under its two-permit limit. | Follow-up from `metadata`. |
| `gc` | Perform one bounded mark-and-sweep pass. | At reclamation deadlines. |
| `grep-index` | Build or reorganize one bounded unit of the grep index. | After publication on hosts configured to maintain the index. |
| `grep-gc` | Inspect one bounded part of a namespace's grep objects. | Explicit assignment. |
| retention (*not a job*) | Advance the retention floor. | Explicit request. |

### Hosts

| Host | Composition | Coverage |
| --- | --- | --- |
| **Server** | Writer, shared-core maintenance handle, registry, and optional local runner. | Namespaces admitted by hints or explicit nudges. |
| **Embedded process** | Writer, shared-core maintenance handle, registry, relay, and local runner. | Namespaces written by the process. |
| **`loonfs maintenance run`** | Registry execution with process-local continuations. | Namespaces passed with `--namespaces`. |
| **Worker with no writer** | Standalone maintenance handle and registry; scheduler optional. | Assigned namespaces. |
