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

This spec uses three terms.

| Plane | Purpose | Examples | Namespace-visible history? |
| --- | --- | --- | --- |
| **Data plane** | Stores and serves file bytes. | Content blocks, content manifests, download streams. | No, by itself. |
| **Metadata plane** | Defines the filesystem's durable truth. | WAL entries, namespace head, checkpoints, inode and direntry state. | Yes. |
| **Control plane** | Coordinates long-running work and authorization. | Upload sessions, read sessions, copy jobs, ACLs, shares, leases. | No. |

Two rules follow from this split:

1. The metadata plane is authoritative for filesystem state.
2. Control-plane objects may be durable, but they do not advance namespace `seq` and do not appear in the change feed.

Control-plane state should still be durable when losing it on restart would violate correctness, restart safety, or promised resumability.

## 3. Client profiles

LoonFS supports multiple client profiles. These profiles are defined by the protocol surface they use, not by whether the implementation is a CLI, desktop app, web app, or service.

| Client profile | Primary surface | Typical state |
| --- | --- | --- |
| **Path-oriented client** | Filesystem operations such as `ls`, `stat`, `get`, `put`, `mv`, and `cp` | Often little or no durable local state beyond transient request context. |
| **Explicit-commit client** | Staged upload, request ids, explicit commit, and change cursors | Durable retry state for in-flight uploads and requests, but not necessarily a full local projection. |
| **Sync client** | Change feed plus durable local projection, with optional writes | Durable local state, cursors, and restart-safe reconciliation state. |
| **Operator or admin client** | Recovery, inspection, repair, and low-level operations | Implementation-specific. |

A CLI, desktop app, web app, SDK, or service may implement one or more of these profiles.

## 4. Operation classes

Most operations fall into one of three classes.

| Class | Typical examples | Server-side state |
| --- | --- | --- |
| **One-shot** | `ls`, `stat`, `get <file>`, `put <small file>`, `cp <file>` on one service | Usually none after the request completes. |
| **Client-driven long-running** | recursive `get`, resumable `put` | A session or intent may be used to pin a snapshot or destination across multiple requests. |
| **Server-driven long-running** | recursive same-service `cp`, large import jobs | A job record may be used while the server continues the work. |

Sessions and jobs preserve stable meaning across time. They do not create a second history model.
