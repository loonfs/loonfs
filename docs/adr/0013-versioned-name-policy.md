# ADR 0013: names use an explicit versioned policy

Status: accepted

Canonical sibling-name comparison will use a shared, versioned `NamePolicy` rather than ambient host filesystem rules. The first policy is `macos_ci_v1`, which preserves `display_name` and compares names by normalized, case-folded `name_key`.

Consequences:
- client and server agree on collision rules
- Unicode behavior is pinned and testable
- path semantics can evolve only through an explicit policy change
