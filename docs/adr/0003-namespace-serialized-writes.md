# ADR 0003: namespace commits are serialized per namespace

Status: accepted

We serialize visible metadata commits within a namespace.
There is no atomic multi-namespace transaction.

Consequences:
- cross-namespace move is copy+delete
- seq is namespace-local
- queue and snapshot work should also be namespace-scoped where possible
