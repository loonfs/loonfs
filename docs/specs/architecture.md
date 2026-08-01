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
| **Control plane** | Coordinates multi-request work and authorization. | Upload handles, put intents, ACLs, shares, leases. | No. |

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
| **Client-driven long-running** | recursive `get`, resumable `put`, recursive `put`, recursive `cp` realized as several commits | A handle or intent may be used to pin a snapshot or destination across multiple requests. Other orchestration may remain client-side. |

Implementations may additionally expose coordinator-specific helpers for recursive workflows or admin work, but those helpers are outside the interoperable core model.

Control objects and implementation-specific helpers never create a second history model.

## 5. Background maintenance

Maintenance is split in two, and the split is the whole design. A **runner** owns scheduling: which work is eligible, how much runs at once, what happens when a step fails, and when everything stops. A **job** owns one kind of work and knows nothing about scheduling: asked for a step, it re-reads durable state, does one bounded unit, publishes the result through the same compare-and-swap protocol any writer uses, and says what it accomplished. There is one runner per write-capable handle and one implementation of retry, coalescing, concurrency, and shutdown in it. A job that wants a scheduler of its own is a design error.

The runner keys work by `{job, namespace}`. One key runs at a time, duplicate hints for a key coalesce into one, and every key shares a single permit pool sized by `max_concurrent_maintenance`, so a burst across many namespaces cannot fan out into unbounded concurrent maintenance. Hints are level-triggered and never authoritative: a step's own read of durable state decides whether there was anything to do. User writes never wait on maintenance admission or execution.

### Conclusions

A step's return value is its entire scheduling vocabulary.

| Conclusion | What it says | What the runner does |
| --- | --- | --- |
| `progressed` | Durable state advanced. | Eligible again at once, behind whatever else is waiting. |
| `idle` | Nothing to do. | Parks until a nudge or a reconciliation sweep finds work. |
| `blocked` | There is work this step's policy cannot advance — an input that does not fit the per-step budget, for one. | Parks like `idle`; requeueing zero-progress work would only spin. |
| `superseded` | Another writer won this step's race. | Eligible again at once, to take the race against what landed. Not a failure. |
| `not_enabled` | This job has nothing to maintain here at all. | Forgets the key. |

A step may also hand back an opaque continuation — where it stopped, for the next step to resume from — and the earliest time it saw work becoming eligible, such as a lease expiry. The runner holds both; jobs keep no scheduler state beside it. A continuation never crosses a process boundary, so a job that cannot safely restart its pass from the beginning must not use one. Transient errors get per-key exponential backoff with a fleet-safe ceiling, because one provider outage must not turn into every namespace retrying in lockstep.

### Jobs and admission policy

| Job | Step | Admission |
| --- | --- | --- |
| `metadata` | Flush the WAL tail past its threshold, then fold one bounded reorganization unit. | Automatic. Nudged by publication. |
| `gc` | One bounded mark-and-sweep pass. | Automatic, and clock-driven: work becomes eligible when a lease expires or a grace window passes, so the deadlines that create reclaimable state are what plant the wakeup. |
| `grep-index` | One bounded gram-index build or fold unit. | Automatic where a host maintains the index. Nudged by enable, publication, and queries that find the index behind. |
| `grep-gc` | One bounded pass over one namespace's grep keyspace. | Registered where a host maintains the index, on the same switch. Never nudged by anything the index does: like grep collection generally, somebody asks for it. |
| retention (*not a job*) | Advance the retention floor. | **Never automatic.** There is no job id to register; an operator asks for it. |

Retention is the one deliberate exception, and it is a moral distinction rather than a safety budget. Collection reclaims state that is provably dead; advancing the retention floor surrenders replay history that is still there. So collection runs on its own and retention is asked for explicitly, through the maintenance step's `retention` opt-in or the typed admin operation. Grep collection is likewise explicit and per namespace: `grep-gc` is a job so that a pass resumes where the last one stopped and shares the runner's admission and permits, but nothing schedules it on grep's behalf.

### Coverage: touched and assigned

LoonFS has no global namespace enumeration, so no local runner can promise to reach every namespace in a deployment. Coverage is stated exactly:

> Automatic maintenance covers namespaces touched by the running process and namespaces explicitly assigned to a maintenance host.

A **touched** namespace — written, queried, or nudged by this process — stays covered for the rest of the process lifetime, and the runner may forget it once a probe finds it idle. An **assigned** namespace is named by an operator and stays covered across quiet periods and restarts, because its host asserts the assignment again on an interval. Nothing claims that reconciliation eventually finds every namespace; a deployment that wants a cold namespace maintained assigns it somewhere.

### Hosts

The same runner and the same jobs run in every host; which one is running is a deployment choice.

| Host | Registers | Covers |
| --- | --- | --- |
| **Server** | The runtime's jobs, plus the grep index job when its grep mode maintains. | Namespaces it touches, while its maintenance mode is automatic. Set it manual when a dedicated process owns upkeep instead. |
| **Embedded process** | Whatever the library host registers on its writer. | Namespaces it touches. |
| **`loonfs admin run`** | The runtime's jobs and the grep index job, narrowed by `--job`. | The namespaces named by `--namespace`, and nothing else — this command never discovers namespaces. It serves nothing, hosts until a signal, or catches its assignment up and exits with `--drain`. |

Every host shuts down in the same order: close maintenance admission, close publication admission, drain admitted publications, drain the runner's in-flight steps, then stop the extension and query services. Maintenance admission closes first because draining publications is a wait, and an open runner spends that wait admitting steps the shutdown has already decided to drop — a metadata root advanced, provider objects deleted, index segments written, all after the process was asked to stop, and all of it work the drain then has to sit through. Closing first is also what makes the drain honest: nothing may register work after the registry has been drained, and a finishing step may not hand its slot to queued work once admission is shut. No order here can deadlock: a step publishes through the same compare-and-swap any writer uses rather than through the host's publication service, so a publication drain never waits on maintenance.
