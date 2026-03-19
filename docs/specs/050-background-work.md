# Spec 050: background work

## What background work is for

Background work exists to build derived state after canonical metadata has already been committed.

It is for:

- snapshots
- change indices
- listing indices
- revision indices
- verification
- GC planning and sweep

It is not for:

- authoritative namespace commits
- required content uploads for the current request

## Queue design

We use sharded queue objects in object storage.

Each shard owns:

- broker lease state
- a set of queued jobs
- claim / heartbeat / timeout metadata

We do **not** use one global queue file.

Why not:
a single mutable object becomes a write hotspot.

Queue shards are durable JSON control objects at:

```text
queue/shards/{shard_index:05}.json
```

The envelope shape matches other small mutable control objects:

```json
{
  "kind": "queue_shard",
  "format_version": 1,
  "writer_version": "loon-worker/0.1.0",
  "payload_checksum_sha256": "<sha256>",
  "state": {
    "work_class": "BuildSnapshot",
    "shard_id": 17,
    "broker": null,
    "jobs": []
  }
}
```

Rules:

- the object key must match `shard_id`
- queue shard updates use create-if-absent for first publish and CAS for later mutation
- broker lease state and jobs live in the same durable object so claim and repair decisions see one shard view

Failure modes prevented:

- queue writers silently disagreeing about which shard object they meant to update
- claim metadata racing in a different object from the jobs it is meant to guard
- lost updates from blind overwrite of a mutable shard

## Broker lease rule

Each shard has one active broker lease in `state.broker`.

Broker lease transitions:

- a missing broker lease may be created
- the same broker may renew its unexpired lease without changing epoch
- a different broker may take over only after the prior lease is expired
- every takeover increments `epoch`

Worker-visible shard mutations must fence on both:

- `broker_id`
- `epoch`

Why this rule exists:

- broker identity alone is not enough when a restarted broker reuses the same ID
- lease takeover must fence stale shard mutators

Failure modes prevented:

- a stale broker continuing to mutate jobs after another broker already took the shard
- the same broker ID being reused without a new generation fence

## Worker claim, heartbeat, and complete rule

Claims live inside the same durable shard object as the jobs they guard.

Claim rule:

- claiming a ready job sets `state = claimed`
- the shard records `worker_id`, `claim_token`, `heartbeat_at_ms`, and `timeout_at_ms`
- claiming an already claimed job is allowed only when `timeout_at_ms <= now_ms`
- every successful claim or steal increments `attempts`

Heartbeat rule:

- only the matching `claim_token` may extend `heartbeat_at_ms` and `timeout_at_ms`

Complete rule:

- only the matching `claim_token` may complete the job
- if a follow-up payload exists, completion promotes it into a fresh ready job state
- otherwise completion removes the job from the shard

Why this rule exists:

- claim ownership and stale-token rejection must be derived from one durable shard view
- a stolen job must not be completable by the loser

Failure modes prevented:

- one worker heartbeating or completing another worker's claim
- a stolen job being completed twice
- completion racing against a separately stored claim record

## Job rule

Every job class must be idempotent.

Example:
`BuildSnapshot(namespace=abc, through_seq=420)` may run twice and still converge to the same durable outputs.

Failure mode prevented:
duplicate execution corrupting derived state.

## Lost enqueue rule

The queue is coordination, not truth.

If a post-commit enqueue is lost, repair logic must be able to recreate it by comparing namespace head seq to derived progress objects.

Failure mode prevented:
derived work permanently stalling because one enqueue was dropped.

## Progress object contract

Every derived work class publishes one small mutable `progress.json` object at:

```text
namespaces/{namespace_id}/derived/{work_class}/progress.json
```

The durable JSON envelope uses the same versioned control-object shape as `head.json` and
`lease.json`:

```json
{
  "kind": "namespace_progress",
  "format_version": 1,
  "writer_version": "loon-worker/0.1.0",
  "payload_checksum_sha256": "<sha256>",
  "state": {
    "namespace_id": "ns-1",
    "work_class": "BuildListingIndex",
    "through_seq": 420
  }
}
```

Rules:

- the object key must match `namespace_id` and `work_class`
- `through_seq` is monotonic and must only advance with CAS
- readers may trust derived outputs only when the corresponding `progress.json` proves coverage for the requested boundary

Why this shape exists:

- small control objects stay readable
- readers can validate progress objects against durable keys instead of ambient arguments
- checksum validation catches silent payload drift

Failure modes prevented:

- trusting derived outputs whose control object does not match the namespace or work class
- mutating progress state backward and silently regressing published coverage
- treating malformed progress JSON as authoritative

## Progress publication rule

Workers publish `progress.json` only after the immutable derived outputs for that `through_seq` are already durable.

Publication must:

1. derive the progress key from `namespace_id` and `work_class`
2. read the current `progress.json` if it exists
3. validate the existing object against its durable key, kind, and payload checksum
4. skip the write when `current.through_seq >= requested_through_seq`
5. create the object if it does not exist
6. otherwise compare-and-swap it to the new higher `through_seq`
7. treat CAS/create conflicts as retry-from-fresh-read, never as permission to overwrite blindly

Why these rules exist:

- derived coverage must only move forward
- duplicate workers should converge on the same durable promise
- stale workers must not regress visibility of already-built derived state

Failure modes prevented:

- a slower worker moving progress backward after a newer worker already advanced it
- publishing progress for outputs that are not fully durable yet
- lost updates from concurrent workers racing on the same progress object

## Retention policy gate

Retention advancement also requires one durable progress object that represents the policy gate for the namespace.

The current skeleton stores that policy gate in the same `progress.json` family, under a dedicated
work class such as `RetentionPolicy`.

Rule:

- `retention_floor_seq` may advance only when the retention-policy progress object's `through_seq` is at or above the requested floor

Failure mode prevented:

- dropping incremental replay before policy has actually authorized it

## Lost enqueue repair

`BuildSnapshot` is the first repair path.

Repair uses canonical durable state, not queue state, as truth:

1. read `head.json`
2. read `namespaces/{namespace_id}/derived/BuildSnapshot/progress.json` if it exists
3. if `head.seq <= progress.through_seq`, do nothing
4. if `head.seq > progress.through_seq` or the progress object is missing, ensure one deduped queue job exists for that namespace

The dedupe key is namespace-scoped:

```text
BuildSnapshot:{namespace_id}
```

Repair behavior:

- if no matching job exists, enqueue a ready `BuildSnapshot` job through `head.seq`
- if a matching ready job exists, raise its payload `through_seq` to `max(existing, head.seq)`
- if a matching claimed job exists, attach or raise its follow-up payload to `max(existing, head.seq)`

Durable repair publish rule:

1. read `queue/shards/{shard_index:05}.json` if it exists
2. validate the shard object against key, kind, and payload checksum
3. apply the in-memory repair transform to the shard state
4. if the transform produced no shard change, skip the write
5. if the shard did not exist and repair needs a job, create the shard object
6. otherwise compare-and-swap the updated shard object
7. treat create/CAS conflicts as retry-from-fresh-read

The same read-validate-transform-CAS rule applies to broker lease renewal, worker claim,
heartbeat, and complete transitions.

Why this rule exists:

- queue shards are coordination only
- repair must reconstruct missing work from durable namespace state
- a claimed worker should finish what it already holds while still learning about newer head coverage

Failure modes prevented:

- derived work stalling forever because one post-commit enqueue was dropped
- duplicate snapshot jobs for the same namespace fighting each other
- a claimed stale job losing visibility of newer required work
- repair logic fabricating queue state without a durable CAS boundary

## Executable invariant surface for Milestone 8 slice 2

For background-work fixtures that list `expect.invariants`, each listed name is now an executable
harness check, not just a string collected from runtime output.

The first background-work executable families are:

- progress publication:
  - `progress_object_checksum_matches_payload`
  - `progress_object_key_matches_namespace_and_work_class`
  - `progress_through_seq_advances_monotonically`
- durable queue shard objects:
  - `queue_shard_checksum_matches_payload`
  - `queue_shard_key_matches_shard_id`
  - `queue_shard_cas_protects_updates`
- queue repair / broker / worker flow:
  - `lost_enqueue_repair_enqueues_when_head_outpaces_progress`
  - `snapshot_repair_dedupe_key_is_namespace_scoped`
  - `snapshot_repair_claimed_job_gets_follow_up`
  - `broker_lease_takeover_increments_epoch`
  - `active_broker_lease_required_for_shard_mutation`
  - `claim_timeout_allows_steal`
  - `worker_heartbeat_requires_matching_claim_token`
  - `stale_claim_token_cannot_complete`
  - `stolen_job_completes_once`
- verified checkpoint head publish and retention gates:
  - `checkpoint_publish_requires_verified_checkpoint`
  - `snapshot_hint_seq_advances_monotonically`
  - `retention_floor_seq_advances_monotonically`
  - `retention_floor_seq_requires_checkpoint_coverage`
  - `retention_floor_seq_requires_derived_progress`
  - `retention_floor_seq_respects_policy_gate`

Milestone 8 stays harness-first for now:

- runtime `checked_invariants` strings remain unchanged
- traces and snapshots must show structured pass/fail details
- model/core differential harnesses must agree on invariant outcomes, not only final state
