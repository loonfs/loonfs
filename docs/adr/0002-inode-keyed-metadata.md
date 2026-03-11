# ADR 0002: metadata is inode-keyed

Status: accepted

Canonical identity is `(namespace_id, inode_id)`.
Paths are derived views.

Consequences:
- rename is not modeled as delete+add
- move semantics are simpler and more testable
- path-based mutation APIs are disallowed
