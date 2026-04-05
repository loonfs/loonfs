# ADR 0020: active v0 name policy follows the imported core spec

Status: accepted

Supersedes: ADR 0013

The imported core spec family defines the v0 namespace name policy as `nfc_casefold_v0`.

The rewrite will therefore:

- store a namespace `NamePolicy` explicitly on the head state
- use `nfc_casefold_v0` as the default active policy for v0 namespaces
- derive `name_key` from normalized, case-folded name text instead of reusing raw `display_name`

Consequences:

- sibling-name collision behavior becomes explicit and testable
- case-insensitive and normalization-equivalent lookups resolve through the same durable name key
- older branch-local ADR language that named `macos_ci_v1` is historical only
