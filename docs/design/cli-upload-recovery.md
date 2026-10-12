# CLI upload recovery

A file upload can fail before all of its bytes reach the server, or after the bytes are durable but before the client receives a commit response. Those two cases need different retries. An interrupted multipart transfer should resume from the parts the server already accepted. A commit with an uncertain outcome should be retried with the original request, without uploading another copy of the file.

For file-backed PUTs, the CLI records enough local state to tell these cases apart across commands. It writes a recovery record before it sends any content, and it updates the record as a multipart transfer progresses. Before it submits the commit, the CLI persists the complete request.

This note describes local recovery. [Upload sessions](../specs/format.md#51-upload-sessions) and [Commit identity and retries](../specs/format.md#65-commit-identity-and-retries) in the storage format define the server's upload-session lifecycle and its commit-idempotency rules.

## Which uploads can be recovered

The CLI uses recovery records for embedded and remote PUTs that read from a local file. It stores them in `$XDG_STATE_HOME/loonfs/uploads`, or in `$HOME/.local/state/loonfs/uploads` if `XDG_STATE_HOME` isn't set to an absolute path.

A record's key includes the profile, the target identity, the namespace, the remote path, the canonical local path, and the explicit commit ID, if there is one. The local path's bytes are encoded without a lossy Unicode conversion. So commands against different destinations don't accidentally reuse the same recovery record.

Embedded profiles run the HTTP binding in memory, and they use the same recovery records. An upload from standard input has no record, because the input can't be read again.

## What the record contains

Before it sends any content, the CLI records the chosen commit ID, the actor, the message, the overwrite preconditions, the source file's length, and the source file's modification time at full precision.

The record has two states:

| State | Stored information | Recovery action |
| --- | --- | --- |
| `uploading` | The original options and source metadata, plus the multipart geometry and accepted parts when the transfer is multipart | Continue the existing upload. |
| `prepared` | The complete commit request, including either its original content reference and proof, or its inline bytes | Resubmit that request through the `create_commit` operation. |

A resumed command uses the recorded options. If the source metadata or the options have changed, the command stops and keeps the record. It also stops if the record can't be read or its JSON is malformed. It doesn't replace a questionable record with a new upload attempt.

To check the source, the CLI compares file metadata, not a hash of the whole file. A change to the source that keeps both the recorded length and the modification time isn't caught by that check. A prepared request still refers to the content that was originally uploaded, not to the bytes in the local file at the time of the retry.

## From upload progress to a prepared commit

During a transfer, the client's `PutFileJournal` records the multipart geometry and the accepted parts. For every transfer mode, it records the complete commit request before the request is submitted. If a journal callback fails, the operation stops before its next request, so the client doesn't continue from progress that it failed to persist.

Once a request is prepared, recovery doesn't reopen the upload session. The CLI resends the saved request directly. If a multipart session had already completed on the server before the request was prepared, the CLI reads the completed session back and commits without uploading again.

For example, suppose an upload finishes and the CLI persists a prepared request with commit ID `c_0123456789abcdef0123456789abcdef`. The server commits it, but the connection closes before the CLI receives the response. On the next run, the CLI submits the same saved request. As long as the server still keeps the commit's receipt, it returns the original commit result instead of creating another file revision.

If the request never committed, and the proof for its uploaded content has expired, the server rejects the retry. Inline content has no proof and doesn't expire. Recovery doesn't silently upload the file again or create a replacement attempt. A caller without a journal can get the same retry behavior for publication by keeping the prepared content and an explicit commit ID in its own process.

Why doesn't recovery reopen the upload session? If the server committed the request but the response was lost, the upload session might have been removed since then. The durable commit receipt can still establish the original outcome.

## Persisting a record

To write each update to a recovery record, the CLI:

1. Writes the update to a private temporary file in the journal directory.
2. Syncs that file.
3. Atomically replaces the recovery record.
4. On Unix, syncs the directory.

On Unix, the CLI also syncs the directory after it removes a record.

The CLI updates its in-memory progress after the write succeeds, and not before. If a failure happens after the atomic replacement, the local outcome is uncertain, because the new record might already be in place even though the operation reported an error. Recovery must use the persisted record, instead of assuming that the earlier version is still current.

The CLI writes these records for every file-backed PUT, including small files. For a file sent inline, the prepared record contains the file's bytes, and it keeps the commit ID the same across reruns. Writing the records durably adds local I/O, but recovery then works the same way for every file size and transfer mode.

## Concurrent commands

Each file PUT takes an exclusive lock on a sidecar file, and a recursive upload takes and releases the lock for each file. A competing command fails immediately, instead of working on the same upload session or recovery record.

The lock file stays in place after the command finishes. The operating system releases the lock when the process exits, so the lock file's existence isn't evidence that a command is still running.

Why isn't the lock file deleted? If the CLI deleted and recreated it, a file descriptor that was already open could refer to a different lock file from the one a new command uses.

## Completion and cleanup

This section describes when the CLI removes a recovery record, and when it keeps one.

An ordinary attempt removes its recovery record after the commit is acknowledged. An attempt with an explicit commit ID keeps the prepared request after a successful commit, so a later run with that ID can retry the publication without another upload.

After an error, what happens to the record depends on the error:

- If the server refuses a prepared commit because of the namespace's state, the CLI removes the record, even for an attempt with an explicit commit ID.
- After transport failures, unknown outcomes, admission pressure, and other unavailable conditions, the CLI keeps the record.
- After a `commit_id_reuse_conflict`, the CLI also keeps the record, as evidence of the attempted request.
- After errors before receipt resolution, and after unrecognized errors, the CLI keeps the record.

Kept records and sidecar files use local storage until someone deliberately removes them. Removing a record discards the local recovery information. It doesn't extend how long the server keeps receipts, and it doesn't turn a committed ID back into an unused one. The server's retention still limits how long the original commit result is available.

After a transfer error, the CLI keeps the multipart progress, so the upload can be resumed or explicitly aborted. An abandoned session stays on the server until it expires and garbage collection removes it. Its upload parts can take up storage until then, but an incomplete upload is never published as a visible file.

If local cleanup fails after a successful commit, the error reports the committed ID and sequence. That error is about the recovery record, not about whether the filesystem change committed. If cleanup fails after a refusal, the CLI returns the original server error, with the journal path added to its message.
