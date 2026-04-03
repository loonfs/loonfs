# Architecture Overview

## 1. Major parts

| Part | Role |
| --- | --- |
| **Object store** | Holds every durable object: content blocks, content manifests, WAL entries, checkpoints, and small control objects. |
| **Authoritative service** | Resolves paths, validates mutations, writes WAL entries, advances heads, serves reads, and issues capabilities for upload or download. |
| **Clients** | Use either direct filesystem operations or the lower-level upload, commit, and change-feed model. |
| **Access-control service** | Evaluates ACLs and shares, then authorizes LoonFS operations. This may be part of the authoritative service in a simple deployment. |
| **Background workers** | Build checkpoints, advance retention safely, clean up expired control objects, and reclaim unreachable content. |

## 2. Data plane, metadata plane, and control plane

For clarity, this spec uses three terms.

| Plane | Purpose | Examples | Namespace-visible history? |
| --- | --- | --- | --- |
| **Data plane** | Stores and serves file bytes. | Content blocks, content manifests, download streams. | No, by itself. |
| **Metadata plane** | Defines the filesystem's durable truth. | WAL entries, namespace head, checkpoints, inode and direntry state. | Yes. |
| **Control plane** | Coordinates long-running work and authorization. | Upload sessions, read sessions, copy jobs, ACLs, shares, leases. | No. |

Two rules follow from this split:

1. The metadata plane is authoritative for filesystem state.
2. Control-plane objects may be durable, but they do not advance namespace `seq` and do not appear in the change feed.

## 3. Object storage remains the durable foundation

A conforming implementation may keep both metadata-plane objects and control-plane objects in object storage. The spec does not require a separate transactional database.

The important distinction is not *where* a record is stored. The distinction is *what kind of truth it represents*:

- namespace-visible filesystem truth lives in the metadata plane;
- transfer orchestration, leases, and authorization live in the control plane.

If losing a control-plane record on restart would break correctness or promised resumability, that control-plane record should be durable.

## 4. Client profiles

LoonFS supports more than one style of client.

| Client profile | Primary surface | Typical state |
| --- | --- | --- |
| **Filesystem CLI or app** | Path-oriented filesystem operations | Usually little or no durable local state. |
| **Sync client** | Change feed plus local projection, with optional writes | Durable local state and cursor management. |
| **Service writer / batch tool** | Upload plus explicit commit | Usually request ids and upload retry state, but not a full sync database. |
| **Operator or admin tool** | Recovery, inspection, and low-level operations | Implementation-specific. |

The spec does not assume that every client is a sync engine. Direct filesystem commands are a first-class use of the system.

## 5. Operation classes

Most operations fall into one of three classes.

| Class | Typical examples | Server-side state |
| --- | --- | --- |
| **One-shot** | `ls`, `stat`, `get <file>`, `put <small file>`, `cp <file>` on one service | Usually none after the request completes. |
| **Client-driven long-running** | recursive `get`, resumable `put` | A session or intent may be used to pin a snapshot or destination across multiple requests. |
| **Server-driven long-running** | recursive same-service `cp`, large import jobs | A job record may be used while the server continues the work. |

Long-running operations use sessions or jobs only to preserve stable meaning across time. They do not create a second history model.
