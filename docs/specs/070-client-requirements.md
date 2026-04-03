# Client requirements

This document replaces a detailed client architecture with a smaller set of rules every conforming client must preserve.

## What a client must remember durably

A client must preserve at least three durable views, or an equivalent model:

| View | Meaning |
| --- | --- |
| **Remote observed state** | What the client has observed from authoritative namespace history. |
| **Local observed state** | What the client has observed on local storage. |
| **Sync anchor** | The last state the client knows was reconciled both ways. |

The exact local database engine is not part of the core spec. What matters is that these views survive restart and allow deterministic reconciliation.

## Identity rules

Once a client knows a remote inode, it must use `(namespace_id, inode_id)` as the item’s durable identity.

The client must not fall back to path strings as canonical identity for bound items.

For local-only items that do not yet have a remote inode, the client must allocate stable temporary local identities until binding succeeds.

## Publish rules for file content

Before sending `create_file` or `replace_file`, a client must:

1. capture a stable file snapshot
2. upload any missing content blocks
3. upload the content manifest
4. persist enough local state to retry without re-deriving the request from a mutable path or a changed local file

This keeps “upload content” and “publish metadata” as two durable stages.

## Conflict rules

The default v1 client policy is `stable_paths`.

That means:

- the authoritative winner keeps the canonical path
- the losing local content is preserved as a durable conflict artifact
- conflict preservation is explicit and recoverable, not an implicit server-side merge

Conflict artifacts are immutable objects under:

```text
namespaces/{namespace_id}/conflicts/{conflict_id}.json
```

Visible suffixed conflict files may exist as a product feature, but they are not the canonical conflict-preservation model.

## Restart and retry rules

A client must be able to recover after restart without guessing what it previously intended.

That means:

- request IDs are stable across retries
- multi-step local apply flows are idempotent
- already uploaded content should be reused safely
- already created conflict artifacts should be reused rather than duplicated
- late remote observations should converge local state instead of corrupting it

## Multiple client surfaces, one model

Mirror mode, on-demand mode, and platform integrations such as File Provider should all use the same canonical inode, revision, and mutation semantics.

A platform bridge may change *when* data is downloaded or materialized, but it must not invent a second sync model.

## What this document intentionally does not freeze

This document does not require:

- SQLite specifically
- one schema version plan
- one planner loop
- one executor graph
- one platform watcher API

Those are good reference-client topics, but they are not the public LoonFS contract.
