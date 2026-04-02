# ADR 0024: authoritative recursive download and copy replace stay strict

Status: accepted

`loon file ...` now grows:

- `loon file cp --replace <from-namespace:/absolute/path> <to-namespace:/absolute/path>`
- `loon file get --recursive <namespace:/absolute/path> <local-path>`

Rules:

- `cp --replace` is update-only:
  - source and destination are resolved under one leased verified basis
  - source must be a visible file
  - destination must be a visible file
  - the command commits `ReplaceFile` on the destination inode using the source file's current
    manifest digest
- `cp --replace` preserves destination inode identity and leaves source visibility/content
  unchanged
- `get --recursive` is directory-only and read-only:
  - it captures one authoritative subtree snapshot first
  - it stages the full local tree under a sibling temp root
  - it renames the staged root into place only after preflight and write completion succeed
- recursive download is fail-closed:
  - exact output root path only
  - no overwrite
  - no merge into existing local directories
  - no partial success on conflicts

Consequences:

- overwrite semantics stay explicit and aligned with `put --replace`
- recursive download reuses immutable content validation without touching mirror-root or client
  state
- both commands remain frontends to the same authoritative model rather than hidden sync wrappers
