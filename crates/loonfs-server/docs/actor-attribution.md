# Actor attribution

This guide is for applications that submit filesystem changes on behalf of their users. It describes how LoonFS records the actor for each change, and what your backend is responsible for.

Commits, namespace creations, namespace forks, and administrator recoveries each include an actor ID in the `Loonfs-Actor` request header, such as `Loonfs-Actor: usr_8f3c`. The [identity header table](../../../docs/specs/api.md#identity-headers) in the API specification lists the header requirements for every operation.

Your backend authenticates and authorizes each request. LoonFS records the header value exactly as it was sent, and it doesn't verify or manage identities. It checks only that the ID is 1 to 256 visible ASCII characters, and it doesn't parse it. Use a stable internal ID, not an email address or a display name.

To find the actor for a commit, use its `committed_by` field. Don't infer the actor from a commit message or an error message.

## Security

The LoonFS bearer token identifies your backend, not the actor. Never expose it to browser code. The browser should call your backend, and your backend should call LoonFS after it checks access.

## Permanent history

LoonFS can discard change history below a namespace's [retention floor](../../../docs/specs/glossary.md#retention-floor). If you need to keep the full history, copy the [change feed](../../../docs/specs/glossary.md#change-feed) to your own store before the floor advances. Process changes in sequence order, and save the feed cursor only after the change is durable in your store.

Key each change by `(namespace_id, committed_seq)`. Identify files and directories by `inode_id`, because paths can change. Store `commit_id` only for correlation, because a commit ID is unique only while its [commit receipt](../../../docs/specs/glossary.md#commit-receipt) is kept in LoonFS.

For a `deleted` event, the enclosing change's `committed_seq` is the `deletion_seq` that trash and undelete use with the event's `inode_id`. The event doesn't repeat that sequence number in its payload.

If LoonFS returns `rebootstrap_required`, you can rebuild the current filesystem state from a checkpoint, but you can't recover activity history that was already discarded.
