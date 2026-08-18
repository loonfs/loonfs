# LoonFS Architecture Overview

## 1. Major parts

| Part | Role |
| --- | --- |
| **Object store** | Holds every durable object: content-store blobs, namespace WAL segments, namespace manifests, descriptors, checkpoint records, and small control objects. |
| **Authoritative runtime** | Resolves paths, validates mutations, writes logical commits into WAL segments, advances heads, serves reads, and issues capabilities for upload or download. |
| **Clients** | Use either direct filesystem operations or the lower-level upload, commit, and change-feed model. |
| **Access-control service** | Evaluates ACLs and shares, then authorizes LoonFS operations. This may be part of the authoritative runtime in a simple deployment. |
| **Background workers** | Publish namespace manifests, create checkpoint records, advance retention safely, clean up expired control objects, and reclaim unreachable content. |

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

A client pattern is defined by the protocol surface a client uses, not by what the client is: a CLI, desktop app, or service may implement several. (API *profiles* — `core/v0`, `admin/v0` — are a different concept; see `api.md`.)

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

## 5. Background maintenance

Each write-capable handle has one maintenance runner. The runner schedules
work, limits concurrency, retries failures, and coordinates shutdown. A
maintenance job performs one bounded unit of work after reading the latest
durable state. The job publishes changes through the same compare-and-swap
path as other writers. Jobs do not schedule themselves.

The runner tracks work by `{job, namespace}`. It runs at most one step for a
given key, combines duplicate notifications, and applies the shared
`max_concurrent_maintenance` limit across all jobs and namespaces. A
notification means work may exist; the job must read durable state to confirm
it. User writes do not wait for maintenance to start or finish.

### Step results

Each maintenance step returns one of these results:

| Result | Meaning | Runner action |
| --- | --- | --- |
| `progressed` | Durable state changed. | Queue another step after other waiting work. |
| `idle` | No work is currently available. | Wait for another notification or periodic check. |
| `blocked` | Work exists but cannot fit the current step policy or budget. | Wait instead of repeatedly running a step that cannot progress. |
| `superseded` | Another writer won the compare-and-swap race. | Read the new state and try again. |
| `not_enabled` | This job is not enabled for the namespace. | Stop tracking the key. |

A step may also return a continuation cursor and the earliest time its next
work can begin, such as a lease expiry. The runner stores both. Continuations
are process-local, so a job must be able to restart safely if the process
exits. Transient failures use separate exponential backoff for each key so a
provider outage does not make every namespace retry at the same time.

### Jobs and admission policy

| Job | Step | Admission |
| --- | --- | --- |
| `metadata` | Flush the WAL tail when needed, then perform one bounded reorganization step. | Automatic after publication. |
| `gc` | Perform one bounded mark-and-sweep pass. | Automatic after publication or when a lease or grace period expires. |
| `grep-index` | Build or reorganize one bounded unit of the grep index. | Automatic on hosts configured to maintain the index. |
| `grep-gc` | Inspect one bounded part of a namespace's grep objects. | Runs only when explicitly requested. |
| retention (*not a job*) | Advance the retention floor. | Runs only when explicitly requested. |

Retention is not automatic because it intentionally discards replay history.
An operator must request it through a maintenance step or
`loonfs admin retention advance`. Garbage collection may run automatically
because it removes state that is no longer reachable. Grep garbage collection
also requires an explicit request, but it uses a maintenance job so it can
resume across bounded steps and share the runner's concurrency limit.

### Coverage: touched and assigned

LoonFS has no operation that lists every namespace. A runner can therefore
maintain only namespaces the current process uses or an operator assigns to
it.

A **touched** namespace has been written, queried, or otherwise used by the
current process. An **assigned** namespace is named explicitly with
`loonfs admin maintenance run --namespaces`. Assign inactive namespaces to a
maintenance process if they must continue receiving maintenance.

### Hosts

The same runner and jobs can run in several kinds of process:

| Host | Registers | Covers |
| --- | --- | --- |
| **Server** | Runtime jobs, plus the grep index job when configured. | Namespaces the server uses while automatic maintenance is enabled. |
| **Embedded process** | Whatever the library host registers on its writer. | Namespaces it touches. |
| **`loonfs admin maintenance run`** | Runtime jobs selected by `--job`. | Namespaces passed with `--namespaces`. It runs until stopped, or completes the current assignments and exits with `--drain`. |

After a stop signal, the process calls `FsWriter::shutdown`. The writer stops
accepting new maintenance work and publications, then waits for accepted work
to finish. Maintenance publishes directly through compare-and-swap, so
shutting down publication and maintenance cannot make them wait on each
other. Extension and query services stop after the writer finishes.
