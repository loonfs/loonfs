# Spec 010: glossary

This file defines project terms in plain language.

## Seq

A seq is a namespace-local sequence number.

Why it exists:
it gives a single answer to “what order did visible metadata commits happen in?”

Example:
a rename might become seq 418.

Failure mode prevented:
ambiguous ordering between concurrent changes.

## Cursor

A cursor is a client bookmark.

Why it exists:
it lets a client ask for “everything after what I already saw.”

Example:
`changes(after_seq=418)`.

Failure mode prevented:
expensive full rescans and uncertain incremental replay.

## Derived index

A derived index is rebuildable helper state.

Why it exists:
it makes hot reads fast without becoming part of the durable truth.

Example:
a paged directory listing cache.

Failure mode prevented:
conflating convenience structures with canonical history.

## Retention floor

The retention floor is the oldest point from which the system still promises incremental replay.

Why it exists:
old WAL history cannot be kept forever.

Example:
“clients may resume incrementally from any seq at or above the retention floor.”

Failure mode prevented:
promising infinite incremental history.

## retention_floor_seq

This is the concrete sequence number for the current retention floor.

Example:
if `retention_floor_seq = 950`, a client that only has state through seq 900 must re-bootstrap.

Failure mode prevented:
vague retention behavior.

## Fencing token

A fencing token is a writer generation number.

Why it exists:
when lease ownership changes, the new writer must be able to prove it is newer than any old writer.

Example:
writer A holds token 41. It stalls. Writer B takes over with token 42. Even if A wakes up later, it must not be able to publish.

Failure mode prevented:
stale writers overwriting newer state.
