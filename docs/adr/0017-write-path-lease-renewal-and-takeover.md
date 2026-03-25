# ADR 0017: write-path lease renewal and takeover

Status: accepted

## Decision

Namespace lease renewal and expired-lease takeover happen only on the authoritative mutation path.

Rules:

- `bootstrap-namespace --allow-existing` remains read-only and idempotent
- an unexpired same-holder write renews only `lease.json` and preserves the current fence token
- any reacquire after expiry, including same-holder reacquire, rotates the active head fence token
  first
- expired foreign-writer takeover is supported
- the fence rotation is a control-plane `head.json` CAS only:
  - no WAL object
  - no metadata `seq` advance
  - no `next_inode_id` change
  - no checkpoint mutation
- after fence rotation, the new lease publish sets:
  - `holder_id = requesting writer_id`
  - `fence_token = head.active_fence_token`
  - `lease_expires_at_ms = now_ms + lease_duration_ms`
- the one tolerated interrupted-takeover recovery shape is:
  - expired lease
  - `head.active_fence_token = lease.fence_token + 1`
  and the next write may finish the lease publish without another head rotation
- an unexpired foreign holder still blocks the write

## Consequences

- namespaces no longer depend on bootstrap or a standalone CLI repair command to become writable
  again after lease expiry
- same-holder writes renew automatically when the lease is still live
- expired same-holder writes are fenced into a new generation instead of silently continuing with an
  old fence token
- foreign-writer takeover after expiry is explicit and deterministic
- stale writers are fenced by the control-plane head token before ordinary commit validation begins
- basis reconstruction must verify replayed metadata state against the current head while treating
  `active_fence_token` as a control-plane field that may advance without a WAL commit
