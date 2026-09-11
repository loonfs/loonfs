# CLI upload recovery

A file upload can fail before all of its bytes reach the server, or after the bytes are durable but before the client receives a commit response. Those cases require different retries. An interrupted transfer should resume from the accepted parts. An uncertain commit should be retried with the original commit ID and content reference, without uploading another copy of the file.

For file-backed remote PUTs, the CLI records enough local state to distinguish these cases across commands. The recovery record is written before the upload begins and updated as the transfer progresses. Before submitting the commit, the CLI persists the complete request.

This document describes local recovery. The server's upload-session lifecycle and commit-idempotency rules are defined in the [storage format](../specs/format.md).

## Which uploads can be recovered

Recovery records are used for remote PUTs that read from a local file. They are stored in `$XDG_STATE_HOME/loonfs/uploads`, or `$HOME/.local/state/loonfs/uploads` when `XDG_STATE_HOME` is not set.

The record key includes the profile, server URL, namespace, remote path, canonical local path, and any explicit commit ID. Local path bytes are encoded without lossy Unicode conversion. Commands against different destinations therefore do not accidentally reuse the same recovery record.

Embedded CLI uploads and standard-input streams do not use these records. Repeating either command uploads new content. If an explicit commit ID was already committed, that new request conflicts with the earlier request even when the bytes are identical. The embedded Rust API supports publication retries through retained prepared content, but the embedded CLI does not persist that proof between commands.

## What the record contains

Before opening an upload session, the CLI records the chosen commit ID, actor, message, overwrite preconditions, source length, and full-precision source modification time.

The record has two states:

| State | Stored information | Recovery action |
| --- | --- | --- |
| `uploading` | Original options and source metadata, with multipart geometry and accepted parts when applicable | Continue the existing upload. |
| `prepared` | The complete commit request, including its original content reference and proof | Resubmit that request through `create_commit`. |

A resumed command uses the recorded options. If the source metadata or options have changed, the command stops and preserves the record. It also stops if the record cannot be read or its JSON is malformed. It does not replace a questionable record with a new upload attempt.

Source validation compares file metadata, not a full-file hash. A source change that preserves both the recorded length and modification time is outside that check. A prepared request still refers to the content that was uploaded originally, rather than to whatever bytes the local path contains now.

## From upload progress to a prepared commit

The client's `PutFileJournal` records multipart geometry and accepted parts during a transfer. It records the complete commit request before submission for every transfer mode. A journal callback failure stops the operation before its next request, so the client does not proceed using progress that it failed to persist.

Once a request is prepared, recovery does not reopen the upload session. The CLI resends the saved request directly. This matters when the server committed the request but the response was lost: the durable commit receipt can establish the original outcome even if the upload session has since been removed.

For example, suppose an upload finishes and the CLI persists a prepared request with commit ID `c_0123456789abcdef0123456789abcdef`. The server commits it, but the connection closes before the CLI receives the response. On the next invocation, the CLI submits the same saved request. While its receipt is retained, the server returns the original commit result rather than creating another file revision.

If the request never committed and its content proof has expired, the server rejects the retry. Recovery does not silently upload the file again or create a replacement attempt. A caller without a journal can obtain the same publication-retry behavior by retaining prepared content and an explicit commit ID in its own process.

## Persisting a record

Each update is written to a private temporary file in the journal directory. The CLI syncs that file and atomically replaces the recovery record. On Unix, it also syncs the directory after replacement or removal.

In-memory progress is updated only after persistence succeeds. A failure after the atomic replacement is an uncertain local outcome: the new record may already be present even though the operation reported an error. Recovery must use the persisted record rather than assume the earlier version is still current.

These writes are performed for all file-backed remote PUTs, including small files. Local durability adds I/O, but it is necessary for the same recovery behavior across file sizes and transfer modes.

## Concurrent commands

An exclusive sidecar-file lock remains acquired for the entire command. A competing command fails immediately rather than operating on the same upload session or recovery record.

The lock file remains after completion. Deleting and recreating it could allow an already-open file descriptor to refer to a different lock file from the one used by a new command. The operating system releases the lock when the process exits; the persistent file is not evidence that a command is still running.

## Completion and cleanup

An ordinary attempt removes its recovery record after the commit is acknowledged. An attempt with an explicit commit ID retains the prepared request, so a later invocation with that ID can retry publication without another upload.

Retained records and sidecar files consume local storage until they are deliberately removed. Removing a record discards the local recovery information. It does not extend server receipt retention or convert a previously committed ID into an unused one. Server-side retention still limits the period during which the original commit result is available.

Transfer errors preserve multipart progress for resume or explicit abort. Abandoned server sessions remain until expiry and garbage collection. This can temporarily retain upload parts, but an incomplete upload is never published as a visible file.

A local cleanup error after a successful commit reports the committed ID and sequence. That error concerns the recovery record, not whether the filesystem mutation committed.
