# Shared-content ownership and collection

Status: proposed protocol, with an executable bounded model in
`crates/loonfs-core/tests/content_lifecycle_model.rs`. This is step 1 of the
content-lifecycle work. It does not change the storage format, enable published
content deletion, or change the launch policy of permanent retention.

Baseline: `4bea72dae4de1478f9ff2c1c4ba39b8607162011`.

## Decision

Keep file commits local to their namespace. Serialize membership changes with
one content-store control CAS. While a collector captures the registered roots,
that CAS temporarily closes registration and membership removal. Reopen them
once the complete root set has been durably protected. Marking and sweeping can
then continue without holding the registration barrier.

A fork transfers protection, never just a content identifier. Until its target
is registered, its source checkpoint protects the inherited view. Registration
makes the target's creation basis discoverable even before its head exists.
Only then may the source-side transfer protection be released. Other metadata
protection required by a live fork remains in place.

This chooses a bounded-memory capture with potentially unbounded elapsed time
over a second protocol for registering concurrent additions during capture.
Creating namespaces and registering forks may wait for capture or its explicit
abort. File writes, uploads, local copies, and namespace head tombstones do not
wait for it. This is a real availability tradeoff, especially for large fork
families or an unavailable member. Measure capture latency before enabling the
collector; do not quietly add timeouts that bypass the barrier.

## Authorities and linearization points

These are logical records, not a new wire schema or a commitment to object-key
spellings. All records belong to the existing `content_store_id` boundary.

| Record | Authority and transition |
| --- | --- |
| Content-store control | One CAS selects a membership root and the current collection run. `open -> capturing -> open` gates membership edits. A sealed run may still be marking or sweeping while membership is open. Only one run is current. |
| Membership | A bounded, immutable, paged index selected by the control record. Entries name namespaces and their immutable creation intents, including a fork's complete initial basis. Register/remove by publishing replacement index pages and CASing the control pointer while open. Never put all members in one mutable array. |
| Namespace head | The existing create-if-absent write makes a namespace live. The existing terminal tombstone ends its public promises and prevents ID reuse. Ordinary commits still CAS this head. |
| Source checkpoint | Holds a fork's source view before registration. Cleanup must establish that the target was registered, or permanently prevent that attempt from publishing, before releasing transfer protection. An expired clock alone is insufficient. |
| Collection run and root pages | The control CAS starts a run against a fixed membership root. Capture records the full protected roots of each member. Sealing requires complete, verified, durable root pages and protection of the metadata they name. Only a sealed run may authorize content deletion. |

A registry entry is lifecycle authority, not a copy of current file state. Its
creation intent protects a registered target whose head is absent. Once a head
exists, that head determines whether the namespace is active or terminal.
Never treat a failed head read as an absent head.

Candidate index pages written before a lost control CAS are unpublished orphans.
A delayed registration must retry its CAS and observe `capturing`; a prior read
of `open` is not an admission token. Index page reclamation and root-page
protection must obey the existing immutable-metadata publication rules.

## Creation, fork, cancellation, and retirement

1. Prepare a complete creation intent. An independent namespace has a fresh
   content store and no old content. A fork first obtains a source checkpoint
   through the namespace's validated checkpoint publication path.
2. Register the intent with the content-store control CAS. While capture is in
   progress, wait or return a retryable result. Continue protecting the source
   view while waiting; cancellation must fence publication before dropping it.
3. Install the complete target head with create-if-absent. Registration already
   protects the creation basis if the creator pauses before this write.
4. Reconcile uncertain outcomes by reading authoritative records. Do not create
   a second target or silently switch to a new source basis under the same
   registered intent.

An abandoned attempt consumes its namespace ID. Cleanup races the creator's
head installation by conditionally installing a **terminal head** at the absent
head key. If cleanup wins, every delayed create-if-absent fails. If the creator
wins, cleanup must recognize the installed namespace and cannot tombstone it as
an orphan. A head belonging to a different attempt also fences this attempt;
its metadata must not be adopted as this intent's outcome. This deliberately
uses the existing active/deleted head states rather than a new `creating` state.
Only the admitted creator may install the intent; public writes cannot operate
on registration alone.

Cancellation before registration also needs this head fence. A delayed registry
CAS might still add a now-terminal intent, but it cannot make the namespace live;
a later pass removes that entry. The creation basis is not a public read promise
for a terminal target. Initial content-store publication needs its own
create-if-absent control record before installing its first head.

Namespace deletion still tombstones the head first. Remove membership only when
the head is terminal and all outgoing, not-yet-transferred fork protections have
ended, including already admitted operations that still require a view.
Membership removal uses the same open-state CAS as registration. A
terminal member may still protect an outgoing fork; capture must inspect those
protections even though public namespace reads are rejected. Deletion closes
admission; it cannot revoke an already promised operation midway through its
read. A snapshot is still not a way to reopen a deleted namespace.

Registered descendants protect the full closure of their own metadata bases,
including source-owned metadata. Parent and grandparent deletion does not remove
those dependencies. Do not replace the current conservative ancestry rule until
both metadata GC and content GC can follow this closure. In particular, a live
fork's source checkpoint cannot simply be released because registration exists:
registration transfers *content capture* responsibility, not permission for the
old namespace metadata collector to delete that checkpoint's inputs.

## Collection and the protection argument

At run start, select only objects whose preparation/publication rights have
already ended. Freeze this candidate cutoff for the entire run. Object creation
age alone is insufficient: an upload can complete much later than its first
object write. Reuse the completed-upload admission window and full publication
budget from `limits.rs`; do not replace them with a guessed grace period. New
objects and objects with remaining publication rights are not candidates.

The control CAS then fixes the membership root and closes membership edits.
Capture each member's full promise: current metadata and visible WAL, retained
history, recoverable deletions, promised replay views, snapshots/checkpoints,
and outgoing fork protections. Capture a registered but absent target from its
creation intent. Capture a terminal member's surviving outgoing protections.
Persist protected root pages incrementally; do not gather the family in memory.

Each namespace capture must be a validated cut of its head and independently
published protections. It is **not** one head GET followed by an unchecked
checkpoint listing. The implementation must validate publication/retirement
races, preserve the captured immutable metadata until traversal finishes, and
cooperate with namespace metadata GC. A source checkpoint created after its
source was captured is safe only because it derives from that captured promise
or from content outside the candidate cutoff. The existing bounded checkpoint
publication rules remain necessary while acquiring that protection.

Seal the protected root set before reopening membership. Traverse those roots
with the existing bounded mark/merge machinery; seal complete content marks
before issuing any DELETE. Missing coverage blocks sealing. Corruption fails
visibly. A capture may be explicitly aborted by CAS, reopening membership, but
an aborted or replaced run can never subsequently seal or authorize deletion.

The safety argument for an old candidate is:

- A local commit may keep, remove, or copy an already promised reference. It may
  introduce uploaded content only while its namespace-bound preparation proof
  admits publication. Candidate selection excludes that entire window. A
  stale local candidate must lose the head CAS and be revalidated; history
  expiration cannot be bypassed by replaying a prepared copy or restore.
- A fork registered before capture is in the fixed membership, including when
  its head installation is paused. If registration comes later, its source
  protection survives through capture. If that protection was created after
  source capture, its old content was already protected by the source's cut.
- Deleting a source cannot erase an outgoing transfer protection. Retiring a
  member cannot race membership capture. Once protection is transferred, the
  registered target or a subsequent protected descendant carries it.

Therefore an unmarked candidate has no route back into a promised view. This
last property matters beyond the current run: an already issued provider DELETE
may arrive after collector handoff or a later run. A run-ID check cannot retract
that request. Its safety comes from immutable, never-reused content identities
and the prohibition on resurrecting an unprotected identity. Fresh-identity
imports preserve this property. An identity-preserving import would require a
new proof and is outside this design.

Run/progress CASes still fence stale workers. A replacement worker resumes the
same durable run; starting a later run cannot adopt incomplete marks from an
aborted one. Aborting capture abandons its pages under normal protected-object
cleanup; it does not authorize sweeping. Sealed runs retain their metadata roots
until traversal and any required protection handoff have completed.

## Routes audited in the current code

| Route | Obligation for this protocol |
| --- | --- |
| Upload completion and prepared writes (`content/`, `publish/`) | Namespace-bound proof; exclude candidates until the last admitted publication can finish. An uncompleted session owns its object and transfer cleanup. |
| Raw-reference import (`fs/writes.rs`) | Preserve verified copying to a fresh destination identity, even inside one content store. |
| Copy and restore (`commit/validate/`, `commit/materialize.rs`) | Resolve a promised source in the namespace view and revalidate after a lost head CAS. |
| Fork (`namespace/fork.rs`) | Acquire source protection, register the exact creation intent, then install the target. Preserve metadata ancestry independently of public source readability. |
| Bootstrap (`namespace/bootstrap.rs`) | Publish lifecycle membership before the active head; replace the current single-write creation contract deliberately. |
| Namespace deletion (`namespace/delete.rs`, `fs/namespaces.rs`) | Preserve writer fencing and the runtime barrier; tombstone before membership removal. |
| Checkpoint creation/release and fork cleanup (`checkpoint/`, `gc/fork_checkpoints.rs`) | Account for promised views and admitted publications; never drop the last pre-registration transfer protection based only on time or a missing head. |
| Replay, flush, compaction (`metadata/`, `wal/`) | Preserve every still-promised view and captured root. Physical row removal is not itself authorization to reclaim bytes. |
| Future expiration/discard | Publish through the semantic commit path, retaining current revisions and independent promises. No policy change in this step. |
| Upload GC (`gc/`) | Define one ownership handoff before adding prefix sweep: a removed completed session must not hide eligibility evidence or allow a second collector to use incompatible roots. |

## Executable model and limits

Run `cargo test -p loonfs-core --test content_lifecycle_model`.

The dependency-free model explores all schedules of short, ordered actors. It
models membership CAS, namespace head CAS/create-if-absent, source protection,
registration, capture, seal, cancellation, and delayed physical deletion as
separate steps. It asserts after every step that every promised object exists
and every active namespace is registered. Negative controls intentionally remove
individual safeguards and must produce a counterexample.

A namespace root capture abstracts a **complete, validated, protected cut**;
the model does not implement that multi-object read protocol. Likewise it treats
candidate admission as already closed under the real upload timing bound and
models one old object and one fresh object, not provider clocks or multipart
uploads. An admitted view represents a still-promised operation independently
of the namespace head; it does not prescribe a durable write for every read.
The eventual read-admission protocol must establish that protection, or specify
and enforce a complete bounded lifetime before reclaiming its bytes. These are
explicit proof premises, not conclusions of the model.
Deterministic storage fault tests must establish those premises in the real code
before this proposal becomes a format requirement or enables deletion.

The model checks finite safety schedules and concrete quiescent cleanup through
multiple generations. It is not an unbounded model checker or a proof of
production liveness. Eventual reclamation additionally requires fair retries,
complete pagination, bounded resume progress, eventual end of protections, and
available storage. Failed creations must be fenced and retired, including
creation intents that were never installed. An active fork keeps inherited
content as long as its view promises it; an entirely deleted family must not
retain content merely because its ancestors once materialized metadata.

## Next implementation gate

Step 2 starts by implementing and fault-testing the validated namespace root cut
and the source-checkpoint-to-registration handoff against the real object-store
seams, before wiring membership into public namespace creation.
Select the concrete bounded membership index after those tests establish its
needed operations. Then specify the wire records and version gate together;
old writers and collectors must reject the new format, with no mixed-format
fallback. Do not enable deletion from this model alone.
