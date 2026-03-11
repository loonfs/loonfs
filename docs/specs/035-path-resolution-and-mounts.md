# Spec 035: path resolution and mounts

## Important rule

Path lookup is a read convenience. It is not the mutation identity model.

## Resolver input

The resolver starts from:

- a namespace
- a starting inode, usually that namespace root
- a path string

It returns:

- `NOT_FOUND`, `MOUNT_LOOP`, or another lookup error
- or a resolved `(namespace_id, inode_id)`

## Mount crossing

A `MOUNT` inode contains:

- `target_namespace_id`
- `target_root_inode_id`

Most mounts point at the target namespace root.
Some mounts point at a subtree.

Example:

- namespace B root inode is `1`
- `/assets/icons` is inode `7`
- a mount may expose inode `7` as `/SharedIcons`

In that case the mount root is inode `7`, not namespace B root inode `1`.

## Why this exists

It allows selective exposure of another namespace.

Example:
an agent namespace may contain `/workspace`, `/tmp`, and `/results`, but the user-facing tree may mount only `/results`.

Failure mode prevented:
accidentally exposing the whole target namespace when only one subtree should be visible.

## Mount loop rule

The resolver must track visited mount targets and reject loops.

Failure mode prevented:
infinite traversal across mount chains.
