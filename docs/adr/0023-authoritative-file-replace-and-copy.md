# ADR 0023: authoritative replace and copy stay path-addressed and direct

Status: accepted

`loon file ...` now grows two additional direct authoritative mutations:

- `loon file put --replace <local-file> <namespace:/absolute/path>`
- `loon file cp <from-namespace:/absolute/path> <to-namespace:/absolute/path>`

Rules:

- `put --replace` is update-only:
  - it resolves the destination selector to one visible file
  - it rejects absent or directory destinations
  - it uploads durable content first and then commits `ReplaceFile` under one leased verified basis
- `cp` is same-namespace, file-only, and create-only:
  - it resolves the source selector to one visible file
  - it resolves the destination selector as one absent exact final path
  - it commits `CreateFile` with the source file's current manifest digest under one leased
    verified basis
- `cp` creates a new inode; it does not alias, rename, or otherwise reuse source inode identity
- neither command routes through mirror-root observation, client SQLite state, or sync execution

Consequences:

- the product shell gets explicit overwrite and copy behavior without creating a second mutation
  protocol
- `cp` reuses durable content objects directly instead of re-uploading bytes
- path drift between selector resolution and authoritative commit stays prevented by construction
