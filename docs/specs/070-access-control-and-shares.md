# Access Control and Shares

## 1. Separation from namespace history

ACLs and shares are a control-plane concern, not part of namespace metadata history.

This separates two kinds of change:

- filesystem history, which changes what files and directories exist;
- authorization state, which changes who may read or modify those files and directories.

ACL or share changes therefore do not advance namespace `seq`.

## 2. Identity of an access grant

An access grant targets either:

- a whole namespace; or
- a subtree identified by `(namespace_id, root_inode_id)`.

The grant should not be keyed only by a path string. Path text is presentation; inode-root identity is durable.

## 3. Suggested role model

The core spec uses a small role model.

| Role | Allowed actions |
| --- | --- |
| **reader** | Read, list, and traverse the granted namespace or subtree. |
| **editor** | Reader actions plus create, replace, rename, and delete. |
| **manager** | Editor actions plus share and ACL management. |

Implementations may add finer-grained roles, but these three capture the basic model.

## 4. Shares and mounts

A share grants access to a namespace or subtree.

A share object includes at least:

- `share_id`
- `target_namespace_id`
- optional `target_root_inode_id`
- principal or share-link identity
- role
- optional presentation metadata such as display name or description

A future mount feature may present that accessible subtree somewhere in a visible tree; this is not part of the v0 namespace mutation model.

Example:

- a project subtree is shared with a user;
- a future presentation layer may mount that subtree at `/Shared/ProjectA` inside another namespace.

This separates access and presentation:

- the share answers "who may access this subtree?";
- a future mount would answer "where is that subtree shown in a tree?".

## 5. Download and upload capabilities

A service may issue short-lived capabilities or signed URLs for content upload or download after authorization is checked.

Those capabilities are part of request execution. They are not a substitute for ACLs or shares, and they do not become namespace-visible metadata.
