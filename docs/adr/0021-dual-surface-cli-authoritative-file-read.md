# ADR 0021: keep `loon ops` explicit and add a separate authoritative `loon file` surface

Status: accepted

We will keep two CLI surfaces:

- `loon ops ...` for explicit engine/admin/recovery work
- `loon file ...` for user-facing authoritative file reads

The first `loon file ...` slice is read-only and path-oriented, but it still reads directly from
verified authoritative namespace state and immutable content objects. It does not route through the
client mirror, observation, import, or sync paths.

Consequences:

- user-facing commands can feel like product actions without creating a second semantics engine
- `loon ops ...` remains honest about import/observe/sync mechanics
- later direct write commands can build on the same authoritative path-view layer instead of being
  wrappers over the client sync engine
