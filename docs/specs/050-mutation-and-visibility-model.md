# Mutation and Visibility Model

## 1. The commit rule

LoonFS uses an explicit publish rule:

1. make content durable
2. validate the metadata change against authoritative state
3. write one immutable WAL entry
4. advance the namespace head with compare-and-swap

A metadata change becomes visible only after step 4 succeeds.

This prevents a visible file revision from pointing at content that was never fully uploaded.

## 2. One request, one visible sequence

A successful mutation request publishes as one namespace `seq`.

A request may contain more than one operation, but:

- the operations are evaluated in request order;
- the request becomes visible as one committed step in namespace history.

This gives users and downstream consumers a simple rule for visibility and replay.

## 3. Server authority

The server is authoritative for mutation validation.

In particular, the server is responsible for:

- resolving any supplied paths against the current visible tree;
- allocating new inode ids;
- validating name collisions according to the namespace's `NamePolicy`;
- validating preconditions;
- verifying that referenced content is already durable; and
- publishing the final WAL entry and head update.

Clients may assist with planning, hashing, upload, or retry, but they are not the authority for visible state.

## 4. Preconditions

A mutation may include explicit preconditions. Preconditions are how clients say, "apply this only if the namespace still looks like the state I planned against."

The core kinds of precondition are:

| Kind of check | Example use |
| --- | --- |
| **Head-based** | "Apply this only if I planned against the current head." |
| **Name-slot based** | "Create this child only if that name slot is still empty." |
| **Revision-based** | "Replace this file only if it is still at the revision I saw." |
| **Ancestor-visibility based** | "Apply this only if no ancestor was tombstoned." |

The exact wire shape of preconditions may vary by transport binding, but the semantics must match these checks.

## 5. Change feed and replay

A namespace exposes an ordered change feed. The feed answers the question:

> What committed metadata changes happened after `seq = N`?

This feed is the basis for sync engines, replication, and other incremental consumers.

A reader reconstructs authoritative state from:

- a verified checkpoint, when one is available; and
- the WAL entries after that checkpoint.

## 6. Retention floor

A namespace may advance a retention floor to say:

> Incremental replay older than this point is no longer promised.

Clients older than the retention floor must re-bootstrap from a fresh snapshot instead of replaying from an obsolete cursor.

The retention floor may advance only after the system has enough verified material to keep replay safe at or after that point.

## 7. Long-running operations

Some operations are not well described by one request.

Examples include:

- recursive reads that need a pinned snapshot;
- large or resumable uploads that need a stable destination binding;
- same-service recursive copy jobs.

In those cases, the server may create control-plane objects such as read sessions, upload sessions, put intents, import jobs, or copy jobs.

Three rules apply:

1. these objects may be ephemeral when no durability guarantee is required; if an operation's correctness, restart safety, or promised resumability depends on them, they must be stored durably in object storage;
2. they do not advance namespace `seq`;
3. they do not appear in the namespace change feed.
