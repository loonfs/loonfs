# Architecture Overview

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

This spec uses three terms.

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

LoonFS supports multiple client usage patterns. These patterns are defined by the protocol surface a client uses, not by whether the implementation is a CLI, desktop app, web app, or service. (API conformance *profiles* — `core/v0`, `admin/v0` — are a different concept, defined in `api.md`.)

| Client pattern | Primary surface | Typical state |
| --- | --- | --- |
| **Path-oriented client** | Filesystem operations such as `ls`, `stat`, `get`, `put`, `mv`, and `cp` | Often little or no durable local state beyond transient request context. |
| **Explicit-commit client** | Staged upload, commit ids, explicit commit, and change cursors | Durable retry state for in-flight uploads and requests, but not necessarily a full local projection. |
| **Sync client** | Change feed plus durable local projection, with optional writes | Durable local state, cursors, and restart-safe reconciliation state. |
| **Operator or admin client** | Recovery, inspection, repair, and low-level operations | Implementation-specific. |

A CLI, desktop app, web app, SDK, or service may implement one or more of these patterns.

## 4. Operation classes

Most core operations fall into one of two classes.

| Class | Typical examples | Server-side state |
| --- | --- | --- |
| **One-shot** | `ls`, `stat`, `get <file>`, `put <small file>`, `cp <file>` on one service | Usually none after the request completes. |
| **Client-driven long-running** | recursive `get`, resumable `put`, recursive `put`, recursive `cp` realized as several commits | A handle or intent may be used to pin a snapshot or destination across multiple requests. Other orchestration may remain client-side. |

Implementations may additionally expose coordinator-specific helpers for recursive workflows or admin work, but those helpers are outside the interoperable core model.

Control-objects and any implementation-specific helpers preserve stable meaning across time. They do not create a second history model.
