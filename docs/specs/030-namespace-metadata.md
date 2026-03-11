# Spec 030: namespace metadata model

## Namespace

A namespace is the unit of serialized metadata history.

Why it exists:
we want a clean boundary for ordering, caching, and background work.

Example:
an account may have a home namespace, a shared project namespace, and several agent namespaces.

Failure mode prevented:
forcing unrelated trees into one giant global transaction log.

## Canonical identity

The canonical identity of an item is `(namespace_id, inode_id)`.

Why it exists:
paths change; identity should not.

Example:
if `/Projects/A/logo.png` is renamed to `/Brand/logo.png`, it keeps the same inode.

Failure mode prevented:
rename being mis-modeled as delete+add.

## Inode kinds

Supported kinds:

- `FILE`
- `DIR`
- `SYMLINK`
- `MOUNT`

## Authoritative metadata records

- inode record
- direntry record
- revision record
- content manifest reference

The important split is:

- inode identity and history are durable metadata
- directory entries are name bindings
- paths are rebuilt by walking bindings

## Multiple namespaces per account

Accounts can own more than one namespace.

The home namespace exposes other namespaces through `MOUNT` inodes.

This lets us support:

- user workspaces
- shared project spaces
- agent sandboxes
- selective sharing of a subtree instead of an entire namespace

## Cross-namespace move rule

Cross-namespace moves are not atomic by default.
They are modeled as copy + delete.

Why the rule exists:
it keeps the namespace ordering model simple.
