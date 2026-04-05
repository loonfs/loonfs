# ADR 0013: names use an explicit versioned policy

Status: superseded by ADR 0020

This ADR introduced the idea that canonical sibling-name comparison must use a shared, versioned
`NamePolicy` rather than ambient host filesystem rules.

The active v0 policy selection now follows the imported core spec family instead. See ADR 0020.

Consequences:
- client and server agree on collision rules
- Unicode behavior is pinned and testable
- path semantics can evolve only through an explicit policy change
