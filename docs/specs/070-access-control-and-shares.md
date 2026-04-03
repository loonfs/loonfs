# Access Control and Shares

## 1. Separation from namespace history

ACLs and shares are a control-plane concern, not part of namespace metadata history.

This keeps two kinds of change separate:

- filesystem history, which changes what files and directories exist;
- authorization state, which changes who may read or modify those files and directories.

ACL or share changes therefore do not advance namespace `seq`.

## 2. Identity of an access grant

An access grant targets one of two things:

- a whole namespace; or
- a subtree identified by `(namespace_id, root_inode_id)`.

The grant should not be keyed only by a path string. Path text is presentation; inode-root identity is durable.

## 3. Suggested role model

A small role model is sufficient for the core spec.

| Role | Allowed actions |
| --- | --- |
| **reader** | Read, list, and traverse the granted namespace or subtree. |
| **editor** | Reader actions plus create, replace, rename, and delete. |
| **manager** | Editor actions plus share and ACL management. |

Implementations may add finer-grained roles, but these three capture the basic model.

## 4. Shares and mounts

A share grants access to a namespace or subtree.

A mount presents that accessible subtree somewhere in a visible tree.

Example:

- a project subtree is shared with a user;
- that subtree is then mounted at `/Shared/ProjectA` inside another namespace.

This model keeps access and presentation separate:

- the share answers "who may access this subtree?";
- the mount answers "where is that subtree shown in a tree?".

## 5. Download and upload capabilities

A service may issue short-lived capabilities or signed URLs for content upload or download after authorization is checked.

Those capabilities are part of request execution. They are not a substitute for ACLs or shares, and they do not become namespace-visible metadata.
