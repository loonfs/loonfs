# Object Store Contract

## 1. Purpose

LoonFS relies on object storage as its only required durable dependency. The object-store contract is therefore part of the core spec, not an implementation detail.

## 2. Required guarantees

A conforming object-store layer must provide the following behaviors.

| Guarantee | Why it matters |
| --- | --- |
| **Create-if-absent** for immutable objects | Content blocks, manifests, WAL entries, and checkpoints must never be silently overwritten. |
| **Compare-and-swap update** for small mutable objects | The namespace head and similar control objects must be advanced safely in the presence of concurrent writers. |
| **Strong visibility after write and delete** | A successful publish must become authoritative immediately after the guarded write succeeds. |
| **Prefix enumeration** | WAL discovery, checkpoint discovery, repair, and cleanup need a reliable way to enumerate objects by prefix. |
| **Deterministic key scoping** | Providers must not allow objects outside the configured namespace or tenant prefix to leak into operations. |
| **Consistent error signaling for failed preconditions** | Higher layers need one generic way to detect stale writes and retry or fail safely. |

The spec deliberately avoids relying on multi-object transactions or provider-specific behavior that is not exposed through this contract.

## 3. Durable object families

At a minimum, LoonFS stores the following durable object families in object storage.

| Family | Mutability | Purpose |
| --- | --- | --- |
| **Content blocks** | Immutable | Store file bytes. |
| **Content manifests** | Immutable | Describe file size, digest, block size, and the ordered block list. |
| **WAL entries** | Immutable | Record committed metadata changes. |
| **Checkpoints** | Immutable | Record verified snapshots of namespace metadata. |
| **Control objects** | Small and mutable or short-lived | Track heads, leases, sessions, jobs, and similar coordination state. |

## 4. Immutable content rules

The content model has four important rules.

1. Block digests are content-derived, not provider-derived.
2. A content manifest describes one complete file revision.
3. Immutable content objects are written with create-if-absent semantics.
4. A metadata commit may reference a content manifest only after that manifest and all referenced blocks are already durable.

In v1, file content is stored as fixed-size 16 MiB blocks, except for the final partial block.

## 5. Mutable control-object rules

Small mutable objects such as the namespace head or a lease must use compare-and-swap semantics. These objects must remain small enough that guarded rewrite is practical.

Large immutable file data may use multipart upload or another provider-specific optimization. Small mutable control objects should not depend on those mechanisms.

## 6. Provider conformance

The spec standardizes the required behaviors, not a brand name such as "S3 compatible." A provider is conforming only when those behaviors are verified by conformance tests.

The practical rule is simple:

- higher layers may depend on the LoonFS object-store contract;
- higher layers may not depend directly on provider headers, status codes, or SDK quirks.
