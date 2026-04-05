# Proposal: align the rewrite branch to the imported LoonFS core spec family

## Type

superseding

## Affected specs

- `docs/specs/000-overview.md`
- `docs/specs/020-architecture-overview.md`
- `docs/specs/030-object-store-contract.md`
- `docs/specs/040-filesystem-and-storage-model.md`
- `docs/specs/050-write-read-protocol.md`
- `docs/specs/060-interfaces-and-clients.md`
- `docs/specs/080-background-jobs.md`
- `docs/specs/090-versioning-conformance-and-extensions.md`

## Problem

The branch was recently implemented against an older spec family that centered the project on a
server-backed CLI rewrite. `origin/main` now carries a newer LoonFS core spec family with a
broader public model: a path-oriented filesystem surface, a lower-level upload/commit/change-feed
surface, and a clearer separation between metadata plane and control plane.

Without an explicit proposal, the branch would continue to mix the imported spec family with
outdated ADRs, proposals, and top-level docs.

## Proposed change

Treat the imported LoonFS core spec family as authoritative on this branch.

Interpret the current `loon` / `loond` local-remote profile model as an implementation-layer
choice for the path-oriented client profile, not as a replacement for the full public surface
described in the imported specs.

Consequences of that interpretation:

- the current CLI remains a valid path-oriented client profile
- upload/commit/change-feed remains required future work, not optional scope drift
- mounts, ACLs, shares, and long-running sessions/jobs are not implied to exist until explicitly
  implemented
- older branch-local proposals that referenced deleted spec files become historical only

## Rewrite decision

The rewrite is following this proposal now.

## Consequences

- governance/docs must stop pointing at deleted spec files immediately
- current implementation docs must describe the branch as a partial implementation of the imported
  core spec family
- semantic mismatches such as `NamePolicy` must be fixed before new protocol surface is added
- future CLI UX decisions stay layered on top of the imported core spec family rather than
  redefining it
