# Spec 100: authoritative read-only file CLI

## Purpose

`loon ops ...` is the explicit engine and recovery shell.

The first user-facing product shell is a separate authoritative file surface:

- `loon file ls <namespace:/path>`
- `loon file stat <namespace:/path>`
- `loon file get <namespace:/path> <local-path>`
- `loon file cat <namespace:/path>`

This surface is path-oriented for user intent, but it still reads canonical inode-keyed namespace
state directly from durable object storage.

## Surface split

Rules:

- `loon ops ...` remains the low-level admin/debug shell
- `loon file ...` is a product-facing authoritative read surface
- `loon file ...` must not secretly route through:
  - client SQLite state
  - mirror-root observation
  - import-remote-observations
  - sync-once
  - sync-until-idle

Why this split exists:

- normal users should not have to think in sync-engine phases
- the repo still needs an explicit operator surface for debugging and recovery

Failure modes prevented:

- product commands accidentally depending on stale local replica state
- widening `loon ops ...` until it stops being an honest engine shell

## Selector syntax

The first selector syntax is URI-style only:

```text
<namespace_id>:/absolute/path
```

Examples:

- `demo:/`
- `demo:/docs`
- `demo:/docs/report.txt`

Rules:

- the namespace prefix before `:` must be non-empty
- the path portion must begin with `/`
- `..` path traversal is invalid
- `.` components are invalid
- repeated or trailing `/` may normalize to the same canonical absolute path
- root is always represented canonically as `namespace:/`

Why these rules exist:

- the user-facing shell needs concise path selectors
- selector parsing must stay explicit and deterministic
- path selectors are a view over canonical inode identity, not a second path protocol

Failure modes prevented:

- product commands accidentally inheriting ambient cwd semantics
- directory traversal or ambiguous selector normalization becoming hidden behavior

## Authoritative path projection

All read commands project from the verified authoritative basis:

1. load verified namespace basis from durable state
2. start at canonical root inode `1`
3. walk visible child bindings only
4. hide tombstoned or superseded bindings
5. derive the canonical absolute path from the visible bindings encountered

Rules:

- path traversal uses the current `display_name == name_key` contract
- listing and stat operate on visible state only
- root remains an ordinary authoritative inode, not a synthetic CLI special case

Why these rules exist:

- the user-facing shell should read the same truth the mutation path validates against
- visible-path projection must not become a second identity model

Failure modes prevented:

- listing or stat surfacing stale superseded direntry bindings
- product commands reading through a client-local cache that disagrees with authority

## Read commands

### `loon file ls`

Rules:

- if the selector resolves to a directory, return its visible children only
- if the selector resolves to a file, return exactly one entry for that file
- entries are sorted by display name ascending
- directories render with a trailing `/`

### `loon file stat`

Rules:

- accepts root, directory, or file selectors
- returns a stable key-value block with:
  - `namespace_id`
  - `absolute_path`
  - `inode_id`
  - `inode_kind`
  - `authoritative_head_seq`
  - optional `revision_no`
  - optional `size_bytes`
  - optional `content_digest`
  - optional `content_manifest_digest`

### `loon file get`

Rules:

- file-only in this slice
- reads authoritative bytes directly from the content manifest and block objects
- if destination exists and is a directory, write `<dest>/<basename>`
- otherwise treat destination as the exact target path
- fail if the final target already exists
- fail if the target parent directory does not already exist
- do not auto-create directories
- do not overwrite existing files
- do not touch mirror-root or client SQLite state

### `loon file cat`

Rules:

- file-only in this slice
- writes raw file bytes to stdout unchanged
- emits no success wrapper text

Why these rules exist:

- the first product shell should be directly useful for inspection and download
- local side effects should stay narrow and explicit

Failure modes prevented:

- using `get` as a hidden sync/materialization path
- mixing human-readable wrapper text into binary stdout

## Content read validation

`get` and `cat` must reuse the authoritative content-manifest and block validation rules before
returning bytes.

Rules:

- manifest digest must match the manifest object bytes
- manifest namespace must match the requested namespace
- every referenced block must exist and match its recorded digest and size
- the reconstructed file size and file digest must match the manifest payload

Why these rules exist:

- product reads should not trust unchecked object-store bytes
- the read surface should exercise the same immutable content contract as authoritative writes

Failure modes prevented:

- returning silently corrupted bytes through `cat`
- downloading a file whose manifest or blocks are incomplete or mismatched
