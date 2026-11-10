# Retrying a create or a delete

This note describes how a caller retries a request that creates a namespace, forks a namespace, or deletes a namespace. Each of these requests includes an ID that the caller generates, LoonFS stores the ID in the manifest that the request publishes, and a retry that finds its own ID succeeds, so a caller can safely repeat the request.

## The problem

This section describes which writes a caller can retry safely, and what a lost response costs for the others.

A commit can be retried safely. The caller generates a [commit ID](../specs/glossary.md#commit-id), LoonFS stores the ID with the commit, and a retry that finds the ID returns the first commit's result (see [Commit responses and safe retry](../specs/api.md#52-commit-responses-and-safe-retry)). Five other writes have no such ID: creating a namespace, forking a namespace, deleting a namespace, creating a snapshot, and creating an upload session. The API's [retry table](../specs/api.md#operations-and-retry-classes) lists all five as `not_idempotent`, and the client library sends each of these requests once. When a response is lost, the caller can't tell whether the write happened, and a retry either fails or creates a second resource:

- A retried create or fork returns `namespace_exists`, and the caller can't tell its own namespace from a namespace that another caller created. The format spec explains why the original attempt can't settle the question either. Two publishers can build the same first manifest from the same request, so identical bytes don't show which publisher wrote the manifest (see [Conflicting and unknown installations](../specs/format.md#93-conflicting-and-unknown-installations)).
- A retried delete returns `namespace_deleted`, which is also what a caller gets when someone else deleted the namespace.
- A retried snapshot creates a second pin. A user pin without an expiry stays until someone deletes the pin, so a lost response can keep a manifest and its segments from garbage collection indefinitely.
- A retried upload creates a second upload session, which the collector aborts after the session's 24-hour lease and the grace window pass.

Why can't the caller read the namespace before retrying, as the `outcome_unknown` error suggests? A read shows that the namespace exists, but not which request created the namespace. Two callers can send the same namespace ID with the same `Loonfs-Actor`, so the caller has nothing to compare.

## The design

This section describes the rule, what a retry finds, the protocols for a create, a fork, and a delete, why snapshots and upload sessions keep their current behavior, and the retry classes that follow.

### The rule

Every request that creates, forks, or deletes a namespace includes an ID that the caller generates. LoonFS stores the ID in the manifest that the request publishes. A later request with the same ID that finds that manifest succeeds. A create or a fork returns the namespace as it is now, and a delete returns the namespace ID and the final head. A retried commit uses its commit ID in the same way.

A "creation ID" (`creation_id`) is an identifier that the caller generates for one request that creates or forks a namespace, such as `job-4182-create`. LoonFS stores the creation ID in the namespace's first manifest and copies the ID into every later manifest, in the same way as `created_by`, so a retry can find the ID for as long as the namespace exists. A "deletion ID" (`deletion_id`) is the same kind of identifier for one request that deletes a namespace, and LoonFS stores the deletion ID in the [namespace tombstone](../specs/glossary.md#namespace-tombstone).

| Operation | ID in the request | Where LoonFS stores the ID | A request that finds its own ID returns |
| --- | --- | --- | --- |
| Create a namespace | Creation ID (`creation_id`) | The namespace's first manifest, copied into every later manifest | The namespace's current summary |
| Fork a namespace | Creation ID (`creation_id`) | The fork's first manifest, copied in the same way | The fork's current summary |
| Delete a namespace | Deletion ID (`deletion_id`) | The tombstone's `status`, beside `deleted_at_ms` | The namespace ID and final head, as the first delete returned |

The IDs follow the commit ID's rules. The HTTP API requires the field, and the SDKs and the embedded API generate an ID when the caller doesn't supply an ID, as `CommitOptions::commit_id_or_generate` does for a commit. Both IDs use the commit ID's grammar (1 to 128 lowercase ASCII letters, digits, dots, underscores, or hyphens, starting with a letter or a digit). The namespace summary doesn't include the creation ID.

For example, a caller creates the namespace `demo` with the creation ID `job-4182-create`. Manifest 1 is written, but the response is lost. The caller sends the same request again, and LoonFS finds manifest 1 with the creation ID `job-4182-create`, so the retry returns the namespace's summary with `head_seq` 0.

Warning: a generated ID makes a retry safe only if the caller keeps the ID between attempts. If an SDK call generates a new ID on each call, a second call is a new request, and a create returns `namespace_exists`. The client library retries with the same ID, and a caller that retries after a restart saves the ID with the rest of the request.

Why aren't these IDs called request IDs? The API already uses "request ID" for the `req_...` value that the server generates for each HTTP request and returns in the `x-request-id` header. That value is diagnostic, and each retry has a different `req_...` value.

Why copy the creation ID into every manifest? Garbage collection deletes manifest 1 once a newer manifest replaces manifest 1 and the grace window passes. A retry reads the current manifest, so the current manifest has to include the ID.

### What a request finds

A request that finds the namespace under a different ID gets an error:

- A create or fork that finds an active namespace with another creation ID returns `namespace_exists`.
- A delete that finds a tombstone with another deletion ID returns `namespace_deleted`.
- A create or fork that finds a tombstone returns `namespace_deleted`, whatever the IDs are, because a deleted namespace ID can't be used again.

A request that finds its own ID gets the namespace as it is now. LoonFS doesn't compare the rest of the request with the manifest. For example, if a `retention_window` job has since changed the retention window of the namespace `demo`, and the namespace has commits through sequence 12, a retry with `job-4182-create` returns a summary with the new window and `head_seq` 12.

Why doesn't LoonFS compare a retried create with the namespace? A namespace's settings can change after creation (the retention window, for example). A comparison would fail a retry that arrives after such a change, even though the first request succeeded.

### Creating a namespace

To create a namespace with creation ID `C`, LoonFS:

1. Builds the first manifest, with `creation_id` set to `C` (see [Publishing a manifest](../specs/format.md#72-publishing-a-manifest)).
2. Writes the hint with put-if-absent, and then manifest 1 with put-if-absent, as described in [Creating a namespace](../specs/format.md#91-creating-a-namespace).
3. If either put finds an existing namespace, loads the current manifest. A tombstone returns `namespace_deleted`. An active manifest whose creation ID is `C` returns the namespace's current summary, and an active manifest with another creation ID returns `namespace_exists`.

The embedded API's `allow_existing` option doesn't change, so with that option a create returns the existing namespace whatever its creation ID is.

`create_manifest` in `crates/loonfs-core/src/manifest/publish.rs` makes the comparison of step 3 at the point where it finds an existing namespace. A retry in a later HTTP request builds a manifest 1 with a different `created_at_ms`, so LoonFS recognizes the retry by this comparison, not by comparing bytes.

The creation ID also simplifies the rule for a manifest put whose outcome is unknown. The format spec says that identical bytes prove a publisher's own success only when the manifest refers to an object that the attempt created, such as a fork's pin or a new segment. Without a creation ID, a plain create's first manifest has no such object, so a publisher that read back identical bytes couldn't claim the create and would return `namespace_exists`. With the creation ID in the first manifest, identical bytes show that a process with the same creation ID wrote the manifest. Two processes with the same creation ID are attempts of one request, so the request succeeded, whichever process wrote the bytes. The same argument applies to a tombstone and its deletion ID.

In `publish.rs`, `references_own_object` is the function that says whether identical bytes confirm a publisher's own put. In this design, the function also returns true for every manifest 1 and for every tombstone. For every other manifest, the function still returns true when the manifest lists a segment that the attempt wrote.

Why is the hint put still the check for a new namespace ID? The creation ID doesn't change how LoonFS detects an existing namespace. The hint stays for as long as the namespace exists, and manifest 1 doesn't, so two concurrent creates still race on the hint put. The creation ID changes what LoonFS returns after the hint put finds an existing namespace.

### Forking a namespace

A fork attempt creates a [fork pin](../specs/glossary.md#snapshot) on the source before it writes the fork's first manifest. Without a check of the target, a retry whose fork already exists would write a pin and then delete the pin.

To fork a source into the target `T` with creation ID `C`, LoonFS:

1. Loads `T`. If `T` has a tombstone, returns `namespace_deleted`. If `T` is active and its creation ID is `C`, returns the fork's summary without creating a pin. If `T` is active with another creation ID, returns `namespace_exists`.
2. Creates and verifies a fork pin on the source (see [Creating and verifying a pin](../specs/format.md#82-creating-and-verifying-a-pin)).
3. Writes the hint and manifest 1 of `T`. Manifest 1 records `C` and the fork pin's ID.
4. If a put finds an existing namespace, loads the current manifest of `T` and deletes this attempt's fork pin unless the manifest's fork basis refers to that pin. The `target_retains_pin` check in `crates/loonfs-core/src/namespace/fork.rs` already returns whether the fork basis refers to the pin. LoonFS then applies the rules of step 1 to the manifest.

The runtime loads the target before it folds the source (`find_fork_target` in `crates/loonfs/src/runtime.rs`), and step 1 makes its comparison on that load.

For example, two attempts with the creation ID `job-4182-fork` overlap, because the client library retried after a timeout while the first attempt was still running. Each attempt creates a fork pin on `demo`, and the first attempt's put of manifest 1 of `demo-branch` succeeds. The second attempt's put fails. The second attempt loads manifest 1, finds that its fork basis refers to the first attempt's pin, and deletes its own pin. Manifest 1 has the creation ID `job-4182-fork`, so the second attempt returns the fork's summary. The caller receives the fork's summary from either attempt, and the source keeps one fork pin.

Why does the losing attempt delete its pin before it returns success? The fork's manifest refers to the winning attempt's pin, so the losing attempt's pin protects nothing that the fork reads. Without the delete, the pin would keep the source manifest until garbage collection collects an unused fork pin after its creation grace.

### Deleting a namespace

A delete request includes the deletion ID as the `deletion_id` query parameter, because a `DELETE` request has no body. LoonFS stores the deletion ID in the tombstone's status, beside `deleted_at_ms`, for example `{"kind":"deleted","deleted_at_ms":1752629000000,"deletion_id":"job-4182-delete"}`. The tombstone's `head_seq` is the final head, which the first delete returned.

A delete that finds a tombstone compares the tombstone's deletion ID with the request's. If the IDs are equal, the delete returns the namespace ID and the tombstone's `head_seq`. LoonFS runs this comparison wherever a delete can find the tombstone, so a retry gets the same result whether the writer session's open or the deletion's own check that the namespace is live finds the tombstone first. A retry doesn't check `expected_head_seq`. The tombstone doesn't record the value that the first request sent, so a retry with the same deletion ID and a different `expected_head_seq` also returns the tombstone's result. (A commit differs. A retried commit with a changed precondition is a different request, and it returns `commit_id_reuse_conflict`.)

For example, `DELETE /v0/namespaces/demo?deletion_id=job-4182-delete&expected_head_seq=418` publishes a tombstone with `head_seq` 418, and the response is lost. The same request sent again returns `{"namespace_id":"demo","head_seq":418}`. A request with the deletion ID `job-5000-delete` returns `namespace_deleted`.

Why does the deletion ID go in the status? The tombstone is the one record that a deletion creates, and the deleted status already records when the deletion happened. An active manifest has no deletion, so an active manifest has no deletion ID.

### Snapshots and upload sessions

Snapshots and upload sessions keep IDs that LoonFS generates, and `create_snapshot` and `create_upload` stay `not_idempotent`. A caller's ID can't be the key of a pin or an upload session, because garbage collection is safe only if a pin ID or an upload ID is never used twice:

- **Pins.** A fork of a snapshot checks that the snapshot exists, writes its fork pin, and checks again. [Creating and verifying a pin](../specs/format.md#82-creating-and-verifying-a-pin) argues that pins are never recreated, so a snapshot that exists at the second check existed the whole time, and every collection that listed pins before the fork pin was written also listed the snapshot. With a caller's ID as the key, one create would write the key again when its verification fails and `pin_after_fold` tries again, and a late retry after the snapshot is deleted would create the snapshot again under the same key.
- **Upload sessions.** Garbage collection deletes a session record before its outcome record (see [Upload-session cleanup](../specs/format.md#116-upload-session-cleanup)). A late retry with the same upload ID between those two deletes would write a new session record beside the old outcome record, so the new session would read as completed or aborted, with the old content reference (see [Upload sessions](../specs/format.md#51-upload-sessions)).

A retry after a lost response costs a second resource. A retried upload creates a second upload session, which the collector aborts after the session's 24-hour lease and the grace window pass. A retried snapshot creates a second pin, which stays until the pin expires or someone deletes the pin.

A caller supplies each snapshot's `name`, and the snapshot listing returns the name. So after a snapshot create with an unknown outcome, a caller that set a name it can recognize (for example `release-job-4182`) lists the snapshots and looks for that name before it retries. The check isn't exact, because a put whose outcome is unknown can still succeed after the listing (see [Creating and verifying a pin](../specs/format.md#82-creating-and-verifying-a-pin)), and a retry then creates a second pin with the same name. A caller that later finds two snapshots with that name deletes the extra snapshot.

### Retry classes

After this change, `create_namespace`, `fork_namespace`, and `delete_namespace` are `idempotent`, which is the class of a request that a client can safely repeat, and the client library retries these requests under its usual retry policy (three retries, with a backoff from 250 ms to 2 s). The three operations aren't `replayable`, because that class promises the original result, and a retried create returns the namespace as it is now.

`create_snapshot`, `create_upload`, `run_maintenance`, and `probe_store` stay `not_idempotent`:

- A snapshot create and an upload create use IDs that LoonFS generates, as [Snapshots and upload sessions](#snapshots-and-upload-sessions) describes.
- A maintenance job's result describes that run, such as the number of compaction units that a `metadata` job published, so a second run can't return the first run's result. A `recover_administrator` job also commits under a commit ID that the server generates, so a second run makes a second commit.
- A store probe writes and deletes its own scratch objects under a new run ID, and its report describes how the store behaved during that run. A retry is a second probe, so an operator chooses whether to probe the store again.

## Costs

This section lists what the design costs.

- Every create, fork, and delete request includes one more field, and the HTTP API rejects a request without the field. A manifest includes the creation ID (at most 128 bytes), and a tombstone also includes the deletion ID.
- A retry works while the manifest that stores the ID exists. A creation ID lasts as long as the namespace's manifests, and a deletion ID as long as the tombstone, which stays the current manifest after the namespace is deleted. A create that is retried after the namespace was deleted returns `namespace_deleted`.
- A retried fork that overlaps the first attempt writes a second fork pin and then deletes the pin. If the delete fails, the pin stays until garbage collection collects an unused fork pin after its creation grace.
- Snapshot and upload creates still have no safe retry.

## Changes to the specs and glossary

This section lists the sections of the API spec, the storage format, and the glossary that the design changes.

The API changes in these sections:

- In [Operations and retry classes](../specs/api.md#operations-and-retry-classes), the rows for `create_namespace`, `fork_namespace`, and `delete_namespace` change from `not_idempotent` to `idempotent`. The `create_namespace` and `fork_namespace` rows add the required `creation_id` body field, and the `delete_namespace` row shows the required `deletion_id` query parameter.
- In [Creating and forking namespaces](../specs/api.md#creating-and-forking-namespaces), the example request includes `creation_id`, and the last paragraph is replaced by the rules in [What a request finds](#what-a-request-finds).
- In [`DELETE /v0/namespaces/{ns}`](../specs/api.md#63-delete-v0namespacesns), the request includes `deletion_id`, and "another deletion" fails with `namespace_deleted` unless the deletion has the tombstone's deletion ID.
- In [`POST /forks`](../specs/api.md#613-post-forks), the request includes `creation_id`, and the paragraph about an active or deleted target describes step 1 of [Forking a namespace](#forking-a-namespace).
- In [Snapshots](../specs/api.md#snapshots), a paragraph says how a caller checks for a snapshot by name after an unknown outcome.

The storage format changes in these sections:

- [Creating a namespace](../specs/format.md#91-creating-a-namespace), [Forking a namespace](../specs/format.md#92-forking-a-namespace), and [Conflicting and unknown installations](../specs/format.md#93-conflicting-and-unknown-installations) change. Step 3 of the create compares creation IDs. Step 1 of the fork says that the fork loads the target first, in place of "The fork reads nothing of the target first". A plain create's first manifest and a tombstone move to the list of manifests that identical bytes confirm, and the "Why don't identical bytes always prove success?" paragraph uses the creation ID.
- [Deleting a namespace](../specs/format.md#94-deleting-a-namespace) says that the tombstone records the deletion ID, and that a delete with the same ID returns the tombstone's head.
- In [Control and manifest payloads](../specs/format.md#a4-control-and-manifest-payloads), the deleted status adds a required `deletion_id`, and the manifest table adds a required `creation_id`. The first manifest's table in [Publishing a manifest](../specs/format.md#72-publishing-a-manifest) lists the creation ID as "From the request" for a plain create and a fork.

In the golden fixtures in `crates/loonfs-types/tests/golden/`, `manifest.v1.json` and the six lifecycle variants add `creation_id`, and `manifest.deleted.v1.json` adds `deletion_id`.

In the glossary, new entries define "Creation ID" and "Deletion ID", and "Namespace tombstone" says that the tombstone records the deletion ID.

## Alternatives considered

- **An `Idempotency-Key` header.** The server stores each recent key with its response in a table, and a request with a known key returns the stored response. The table is a new durable record family with its own expiry and its own collection, and a stored response can disagree with the resource, for example after a namespace is deleted.
- **Comparing the request with the existing resource.** A create that finds a namespace whose fields match the request returns success. Two callers that send the same create can't be told apart, which is the case that the format spec already describes, and a namespace's settings can change after creation, so a retry can fail to match its own namespace.
- **Using the caller's ID as the key of a pin or an upload session.** The caller generates the snapshot ID or the upload ID, and a retry that finds the record returns the record. A fork of a snapshot depends on pins never being recreated, but a create's own retries after a failed verification, and a late retry after the snapshot is deleted, would write the same key again. Garbage collection deletes a session record before its outcome record, so a late retry between the two deletes would create a session that reads as ended, with the old content reference.
- **The creation ID in the first manifest only.** Garbage collection deletes old manifests, so a retry after a fold and a collection would find no creation ID.
