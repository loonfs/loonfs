# HTTP API binding

This document defines a minimal HTTP binding for the mutation model. The HTTP routes are a binding of the model, not the definition of the model.

## Why this binding stays small

The core LoonFS semantics are durable and transport-neutral. The HTTP API should expose those semantics cleanly without mixing them with client implementation details.

For that reason, this binding standardizes only:

- namespace head reads
- namespace change feed reads
- namespace commit writes

Additional read projections such as path listings, path resolution, or direct file metadata lookups can be standardized later or left to product-specific APIs.

## Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/v1/namespaces/{namespace_id}/head` | Return the current visible head summary. |
| `GET` | `/v1/namespaces/{namespace_id}/changes?after_seq={seq}` | Return committed changes after a known cursor. |
| `POST` | `/v1/namespaces/{namespace_id}/commits` | Submit one logical commit request. |

## `GET /head`

The head response should contain at least:

```json
{
  "namespace_id": "ns-1",
  "seq": 418,
  "snapshot_hint_seq": 400,
  "retention_floor_seq": 350
}
```

This endpoint is the stable starting point for readers and clients.

## `GET /changes`

The change feed is the incremental read surface.

Request:

```text
GET /v1/namespaces/ns-1/changes?after_seq=418&limit=1000
```

Response:

```json
{
  "namespace_id": "ns-1",
  "from_exclusive_seq": 418,
  "through_seq": 420,
  "changes": [
    { "seq": 419, "request_id": "req_a", "ops": [ ... ] },
    { "seq": 420, "request_id": "req_b", "ops": [ ... ] }
  ]
}
```

If `after_seq` is below the retention floor, the server should return an explicit re-bootstrap response rather than silently scanning from an unspecified point.

## `POST /commits`

Request:

```json
{
  "request_id": "req_01J...",
  "expected_head_seq": 418,
  "ops": [
    {
      "create_file": {
        "parent_inode_id": 7,
        "display_name": "notes.txt",
        "content_manifest_digest": "sha256:..."
      }
    }
  ]
}
```

Success response:

```json
{
  "namespace_id": "ns-1",
  "request_id": "req_01J...",
  "committed_seq": 419,
  "results": [
    {
      "op_index": 0,
      "created_inode_id": 42,
      "revision_no": 1
    }
  ]
}
```

Failure response:

```json
{
  "namespace_id": "ns-1",
  "request_id": "req_01J...",
  "error": {
    "code": "precondition_failed",
    "detail": "InodeRevisionIs"
  }
}
```

## Binding rules

- the HTTP request must carry the same `request_id` used for retry and deduplication
- the server must not return success before the head CAS succeeds
- a stale or conflicting write should return a structured conflict response, not a silent merge
- the binding must preserve request order and operation order exactly as submitted
