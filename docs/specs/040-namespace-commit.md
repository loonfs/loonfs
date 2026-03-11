# Spec 040: namespace commit protocol

## Purpose

A namespace commit is the operation that makes metadata changes visible.

## Publish rule

A metadata change becomes visible only when the namespace head advances successfully.

Why it exists:
it gives one publish point for visibility.

Failure mode prevented:
readers seeing half-applied metadata.

## Plain-language write path

1. upload missing blocks
2. upload the content manifest
3. acquire or renew the namespace lease
4. validate preconditions against the latest head
5. write an immutable WAL commit object
6. CAS-update the head object
7. return success only after step 6 succeeds

## Preconditions

Mutations are never path-addressed. They are inode-addressed and explicit.

Example preconditions:

- planned head seq still matches
- target inode is still a file
- current revision is still `12`
- target child name is absent
- ancestors are not covered by a subtree tombstone

Why they exist:
they make races observable and reviewable.

Failure mode prevented:
silent last-writer-wins corruption.

## Fencing

Lease ownership changes must change the active fencing token.

Why it exists:
an old writer may still be alive after a failover.

Example:
writer A reads head with fence token 41. writer B takes over and publishes token 42. A must not be able to publish later using its stale view.

## Restore revision rule

Restoring revision 3 while revision 7 is current creates revision 8 that points to revision 3’s content.

Why it exists:
history should be monotonic.

Failure mode prevented:
moving the head backward and rewriting history.
