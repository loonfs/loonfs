# Spec 100: authoritative file CLI

## Purpose

`loon ops ...` is the explicit engine and recovery shell.

The first user-facing product shell is a separate authoritative file surface:

- `loon file ls <namespace:/path>`
- `loon file stat <namespace:/path>`
- `loon file get <namespace:/path> <local-path>`
- `loon file get --recursive <namespace:/absolute/path> <local-path>`
- `loon file cat <namespace:/path>`
- `loon file put <local-file> <namespace:/absolute/path>`
- `loon file put --replace <local-file> <namespace:/absolute/path>`
- `loon file put --recursive <local-dir> <namespace:/absolute/path>`
- `loon file cp <from-namespace:/absolute/path> <to-namespace:/absolute/path>`
- `loon file cp --recursive <from-namespace:/absolute/path> <to-namespace:/absolute/path>`
- `loon file mkdir <namespace:/absolute/path>`
- `loon file rm [--recursive] <namespace:/absolute/path>`
- `loon file mv <from-namespace:/absolute/path> <to-namespace:/absolute/path>`

This surface is path-oriented for user intent, but it still reads canonical inode-keyed namespace
state directly from durable object storage.

## Surface split

Rules:

- `loon ops ...` remains the low-level admin/debug shell
- `loon file ...` is a product-facing authoritative file surface
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

- plain `get` is file-only
- `get --recursive` is directory-only
- reads authoritative bytes directly from the content manifest and block objects
- plain `get`:
  - if destination exists and is a directory, write `<dest>/<basename>`
  - otherwise treat destination as the exact target path
- `get --recursive`:
  - treats the provided local path as the exact destination root path
  - destination root must be absent
  - destination parent must already exist and be a directory
  - preflights the whole output tree before writing any files
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

## Write commands

Write commands are direct authoritative mutations. They do not route through mirror-root
observation, client SQLite state, or sync execution.

### `loon file put`

Rules:

- source must be one existing regular local file
- `put --recursive` is directory-only
- destination selector must not be root
- destination selector names the exact final remote path
- destination must be absent unless `--replace` is present
- destination parent must already exist and be a visible directory
- plain `put` is create-only
- `put --replace` is update-only:
  - destination must resolve to one visible file
  - absent destination is rejected
  - directory destination is rejected
- `put --recursive` is create-only:
  - source must be one existing local directory
  - `--recursive` and `--replace` are mutually exclusive
  - destination must resolve as one exact absent directory root create target
  - destination parent must already exist as a visible directory
  - empty local directories are valid
  - unsupported local descendants are rejected before any metadata commit:
    - symlink
    - device / fifo / socket / unknown non-file non-dir kinds
- no overwrite or parent auto-create

### `loon file cp`

Rules:

- source and destination selectors must be in the same namespace
- plain `cp` and `cp --replace` are file-only
- `cp --recursive` is directory-only and create-only
- source selector must not be root
- destination selector must not be root
- destination selector names the exact final remote path
- destination must be absent unless `--replace` is present on plain file copy
- plain `cp` is create-only
- `cp --replace` is update-only:
  - destination must resolve to one visible file
  - absent destination is rejected
  - directory destination is rejected
- `cp --recursive`:
  - source must resolve to one visible directory
  - destination must resolve as one exact absent directory root create target
  - destination parent must already exist as a visible directory
  - `--recursive` and `--replace` are mutually exclusive
  - empty source directories are valid
  - copied directories and files get new inode identities at the destination
  - unsupported visible descendants are rejected before metadata publish:
    - symlink
    - mount
    - file descendants missing a current revision head
- identical normalized source and destination paths are explicit errors
- plain `cp` creates a new inode and preserves the source binding unchanged
- `cp --replace` preserves destination inode identity and updates only its file revision/content
- `cp` reuses the source file's current manifest digest; it does not re-upload content objects

### `loon file mkdir`

Rules:

- target selector must not be root
- target selector names one exact final directory path
- destination must be absent
- destination parent must already exist and be a visible directory
- no `-p` behavior

### `loon file rm`

Rules:

- file targets delete directly
- directory targets require `--recursive`
- root is always rejected
- there is no `--force` in this slice

### `loon file mv`

Rules:

- source and destination selectors must be in the same namespace
- both selectors name exact final paths
- source must exist and must not be root
- destination must be absent
- destination parent must already exist and be a visible directory
- identical source and destination paths are explicit errors
- there is no overwrite behavior in this slice

Why these rules exist:

- the first product write surface should be immediately useful without inventing shell-like path
  heuristics
- fail-closed path rules keep the authoritative mutation contract easy to reason about

Failure modes prevented:

- interpreting a remote directory selector as an implicit basename-placement container
- accidental overwrite semantics that do not exist in the underlying authoritative protocol

## Path-addressed authoritative mutation flow

Product writes must resolve paths and commit inside one authoritative server-side mutation flow.

Rules:

1. `put` uploads durable content first
2. acquire or renew the namespace lease
3. load one verified authoritative basis
4. resolve selectors against that basis
5. translate the resolved paths to the existing inode-keyed mutation ops
6. validate, write WAL, and CAS-publish the head through the existing commit machinery

Additional rules:

- `put --replace` resolves the destination to one visible file and commits `ReplaceFile` against
  that file's current revision under the same leased basis
- `put --recursive` walks one normalized local subtree, uploads all file content first, then
  commits one ordered multi-op `CommitRequest` that creates:
  - the exact destination root directory
  - descendant directories in lexicographic relative-path order
  - descendant files in lexicographic relative-path order
- `put --recursive` publishes metadata atomically:
  - no subtree entries become visible until the one commit succeeds
  - the command does not widen the shared single-op client mutation protocol
- plain `cp` resolves source and destination under one leased basis and commits `CreateFile` at
  the destination with the source file's current manifest digest
- `cp --replace` resolves source and destination under one leased basis and commits `ReplaceFile`
  on the destination inode with the source file's current manifest digest
- `cp --recursive` resolves source and destination under one leased basis and commits one ordered
  multi-op `CommitRequest` that creates:
  - the exact destination root directory
  - descendant directories in lexicographic relative-path order
  - descendant files in lexicographic relative-path order
- `cp --recursive` is atomic at metadata publish time:
  - no copied subtree entries become visible until the one commit succeeds
  - source visibility remains unchanged throughout the operation
- `loon-ops` and `loon-cli` must not resolve a path to an inode and then call the current
  inode-addressed mutation path as two separate authoritative steps
- selector resolution and authoritative commit validation must therefore observe one coherent basis
- `put` and `put --replace` reuse the same immutable block/manifest upload contract as client sync
  uploads

Why these rules exist:

- authoritative path writes must not be vulnerable to path drift between selection and commit
- the product shell should remain a second frontend to the same semantics engine, not a second
  mutation protocol

Failure modes prevented:

- resolving `/docs/report.txt` to inode `42` in one step and committing a stale mutation after a
  later authoritative rename
- widening `loon file ...` into a bespoke path mutation implementation that diverges from the
  namespace commit protocol
- publishing a partially visible remote subtree before all uploaded content is durable

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
