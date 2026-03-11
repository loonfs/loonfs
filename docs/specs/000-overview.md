# Spec 000: LoonDB overview

## What LoonDB is

LoonDB is a Dropbox-like sync engine with an object-storage-only durable backend.

The durable system of record is made of:

- immutable content blocks
- immutable content manifests
- immutable namespace WAL entries
- small mutable head / lease / queue control objects

Everything else is cache or rebuildable acceleration.

## Why the design looks like this

We want a system that is:

- simpler to reason about than a multi-service metadata stack
- easy to run in local-server mode and remote-server mode
- naturally portable across S3-class object stores
- compatible with deterministic testing
- scalable where object storage is naturally scalable, without adding coordination layers

## Plain-language shape of the product

There are four client buckets:

1. stateless readers
2. continuous sync clients (with on-demand or eager content fetching)
3. deterministic batch sync CLIs
4. later streamed / mounted-drive clients

The first server milestone is not “every sync feature.” It is a small, correct metadata engine with strong invariants.

## Most important invariants

- identity is `(namespace_id, inode_id)`
- paths are lookup views
- a namespace has a total order of visible metadata commits
- a revision is visible only after its content is durable
- derived indices are disposable

## Example

A file edit should look like this:

1. upload any missing blocks
2. upload the file manifest
3. validate preconditions against the current namespace head
4. write an immutable WAL commit object
5. advance the namespace head with CAS

The file becomes visible only after step 5.

## Failure mode this prevents

This prevents “dangling revisions,” where metadata points to content that is not actually durable.
