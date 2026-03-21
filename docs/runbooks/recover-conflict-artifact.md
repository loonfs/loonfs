# Recover a Conflict Artifact

This runbook covers the first operator-facing recovery shell for stable-path conflict artifacts.

The commands are all `xtask`-first and run only against:

- one explicit namespace
- one existing client SQLite DB
- one existing local-fs object-store root

Recovery is out of band. It does not rebind recovered content into sync state, does not update
planner truth, and does not overwrite the canonical winner path.

## Required inputs

You need all of the following before running any command:

- `namespace_id`
- an existing client DB path passed with `--db`
- an existing local-fs object-store root passed with `--store-root`
- for restore, an explicit absent destination path passed with `--to`

The current shell does not support:

- provider credentials or non-local object stores
- acknowledge/archive/delete lifecycle
- automatic restore destinations

## List artifacts

Refresh the namespace cache from object storage, then print the cached rows:

```bash
cargo run -p xtask -- conflict-list ns-123 --db /path/to/client.sqlite3 --store-root /path/to/store
```

Output is deterministic:

- first line shows `namespace`, `discovered_count`, and `cached_count_after`
- following lines are ordered by `created_at_ms ASC, conflict_id ASC`

## Show one artifact

Load one artifact by `conflict_id`, discovering it from object storage if it is not already cached:

```bash
cargo run -p xtask -- conflict-show ns-123 conflict-abc --db /path/to/client.sqlite3 --store-root /path/to/store
```

The command prints:

- a stable summary header
- one YAML rendering of the cached artifact envelope

## Restore one artifact

Restore the preserved loser content into an explicit destination path:

```bash
cargo run -p xtask -- conflict-restore ns-123 conflict-abc --db /path/to/client.sqlite3 --store-root /path/to/store --to /path/to/recovered-output
```

Rules:

- the destination must be explicit and absent
- the destination parent must already exist
- restore never overwrites the canonical winner path
- restore never deletes or mutates the conflict artifact object

File restore prints:

- `namespace`
- `conflict_id`
- `artifact_kind=file`
- `destination`
- `file_digest_sha256`
- `content_manifest_digest`

Subtree restore prints:

- `namespace`
- `conflict_id`
- `artifact_kind=subtree`
- `destination`
- `restored_entry_count`

## Failure behavior

The commands fail closed and exit non-zero.

Common failures:

- missing DB path
- missing store root
- malformed artifact object in the namespace prefix
- absent artifact
- occupied restore destination
- missing preserved manifest or block content

There is no fallback behavior in this shell. Fix the input or artifact problem and rerun.
