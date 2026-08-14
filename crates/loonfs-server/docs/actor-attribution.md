# Actor attribution

Every mutation includes an actor, such as
`{ "kind": "user", "id": "usr_8f3c" }`.

Your backend authenticates and authorizes the request. LoonFS records the
actor exactly as sent; it does not verify the identity or manage profiles. Use
a stable internal ID, not an email address or display name.

- `user`: a known person caused the change. Use this even when a backend or
  worker carries out the change for that person.
- `service`: your application, integration, or background job caused the
  change without acting for a specific user.
- `system`: platform-level work changed filesystem data.

Use the actor and event fields for attribution. Do not infer the actor from a
commit message or error message.

## Security

The LoonFS bearer token identifies your backend, not the actor. Never expose it
to browser code. The browser should call your backend, and your backend should
call LoonFS after checking access.

## Permanent history

LoonFS may discard change history below a namespace's retention floor. If you
need permanent history, copy the change feed to your own store before the floor
advances. Process changes in sequence order, and save the feed cursor only
after the change is durable.

Key each change by `(namespace_id, committed_seq)`. Identify files and
directories by `inode_id`, because paths can change. Store `commit_id` for
correlation only; it is unique only while its receipt remains in LoonFS.

For a `deleted` event, the enclosing `committed_seq` is the `deletion_seq`
used with the event's `inode_id` by trash and undelete. The event does not
duplicate the sequence inside its payload.

If LoonFS returns `rebootstrap_required`, a checkpoint can rebuild the current
filesystem state, but it cannot recover activity history that was discarded.
