# Mutation model

This document defines the transport-neutral rules for making metadata changes visible.

## Core commit rule

A metadata change becomes visible only when the namespace head advances successfully.

That gives the system one clear publish point and one clear answer to the question “what is visible now?”

## Writer requirements

These are the durable rules every authoritative writer must preserve.

| Requirement | Meaning |
| --- | --- |
| **One request, one `seq`** | One user commit request may contain multiple operations, but if it succeeds they publish as one namespace sequence number. |
| **Stable within-request order** | Operations in one request are evaluated and replayed in vector order. That order must remain durable. |
| **Inode-addressed mutation** | Mutations target inodes and parent inodes, never path strings. |
| **Authoritative basis from durable state** | Validation must be based on the latest durable head, checkpoint, and WAL state, not on caller-supplied snapshots. |
| **Durable content before visible metadata** | `create_file` and `replace_file` may reference only already-durable content manifests and blocks. |
| **Strict optimistic concurrency** | Stale preconditions fail explicitly. The authoritative service does not silently merge stale writes. |
| **Lease and fencing** | The writer must hold the active namespace lease and fence stale generations before publish. |
| **Success only after head CAS** | A request is successful only after the WAL write and head update both succeed. |
| **Idempotent retry key** | A stable `request_id` allows safe retry and deduplication. |

## Supported mutation operations

| Operation | Addressed by | Effect |
| --- | --- | --- |
| `create_dir` | `parent_inode_id`, `display_name` | Allocates one new directory inode and binds it under the parent. |
| `create_file` | `parent_inode_id`, `display_name`, `content_manifest_digest` | Allocates one new file inode, creates revision 1, and binds it under the parent. |
| `replace_file` | `inode_id`, `base_revision_no`, `content_manifest_digest` | Appends a new file revision for an existing inode. |
| `delete_file` | `inode_id` | Removes a visible file without using subtree-delete rules. |
| `rename` | `inode_id`, `new_parent_inode_id`, `new_display_name` | Changes a visible binding, possibly moving the inode, renaming it, or both. |
| `delete_subtree` | `root_inode_id` | Appends one subtree tombstone that hides a whole directory tree. |
| `restore_revision` | `inode_id`, `base_revision_no`, `restore_from_revision_no` | Appends a new revision that reuses content from an older revision. |

## Common preconditions

Different operations use different checks, but the first durable set is intentionally small:

- `HeadSeqIs(expected_seq)`
- `ChildNameAbsent(parent_inode_id, name_key)`
- `InodeRevisionIs(inode_id, revision_no)`
- `AncestorsNotSubtreeDeleted(inode_id)`

These checks make races explicit and reviewable.

## Commit flow

A conforming writer performs the following steps:

1. ensure required content blocks and content manifest are already durable
2. load and verify `head.json`
3. load or renew the active lease and fence stale generations
4. reconstruct the authoritative basis from the verified checkpoint plus WAL tail
5. validate the request against that basis
6. write one immutable WAL commit object
7. compare-and-swap `head.json` to the next visible state
8. return success only after step 7 succeeds

## Request shape

A logical commit request has this shape:

```json
{
  "namespace_id": "ns-1",
  "request_id": "req_01J...",
  "expected_head_seq": 418,
  "ops": [
    {
      "rename": {
        "inode_id": 42,
        "new_parent_inode_id": 7,
        "new_display_name": "logo.png"
      }
    }
  ]
}
```

A successful response must identify the committed namespace sequence number and any newly allocated inode ids that the caller needs to bind locally.

## Failure model

The mutation model is intentionally strict:

- stale-base writes fail
- sibling-name collisions fail
- writes under deleted ancestors fail
- directory cycles fail
- a request never becomes partially visible

Conflict preservation such as “stable paths” is a **client** policy, not an implicit server-side merge behavior.
