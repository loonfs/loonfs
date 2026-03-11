# Spec 020: object-store contract

## Purpose

LoonDB is built on the idea that object storage is the only durable dependency.
That makes the object-store contract a first-class part of the product.

## Required primitives

### Create-if-absent

Plainly:
write an object only if it does not already exist.

Why it exists:
immutable content blocks and manifests should never overwrite each other.

Example:
upload `blobs/sha256/aa/bb/<digest>` with `If-None-Match: *`.

Failure mode prevented:
one writer silently replacing another writer’s supposedly immutable object.

### Compare-and-swap update

Plainly:
update a small mutable control object only if it still has the expected version or ETag.

Why it exists:
this is how we safely advance `head.json`, `lease.json`, and queue shards.

Example:
rewrite `namespaces/ns-1/head.json` with `If-Match: <old-etag>`.

Failure mode prevented:
lost updates from concurrent writers.

### Strong visibility after write and delete

Plainly:
if a write succeeds, later reads and lists must see it immediately.

Why it exists:
namespace publish logic assumes the head object becomes authoritative as soon as the CAS succeeds.

Failure mode prevented:
clients reading stale state after a successful publish.

## What we deliberately avoid

- multi-object transactions
- provider ETags as canonical content identity
- correctness assumptions based only on “S3 compatible” marketing

## S3 and R2 notes

Both AWS S3 and Cloudflare R2 expose strong consistency for reads, writes, deletes, and listings, and both expose S3-style conditional operations. We still treat those behaviors as **must verify by conformance**, because LoonDB depends on the details, not only on the headlines.

## Control-plane rule

Head, lease, and queue objects must stay small enough for simple conditional writes.
Large immutable file content may use multipart upload.
Small mutable control objects should not.
