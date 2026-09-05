# CLI upload recovery

Remote uploads of large local files keep multipart progress in
`$XDG_STATE_HOME/loonfs/uploads`, or `$HOME/.local/state/loonfs/uploads`.
The journal key includes the profile, namespace, remote path, and canonical
local path. Local path bytes are encoded without lossy Unicode conversion.
Embedded uploads and standard-input streams do not create multipart journals.

The record contains the upload ID, part geometry, checksum algorithm, accepted
parts, and source file length and full-precision modification time. Only a
missing record means a new upload. An unreadable or malformed record, unavailable
source metadata, or changed source stops the command with an error. Records are
preserved for inspection; remove the named journal to explicitly start again.
There is no reader for earlier development journal layouts.

Each update writes and syncs a temporary file in the journal directory before
atomically replacing the record. Unix hosts also sync the directory after
replacement or removal. The in-memory record advances only after persistence
succeeds. A failed update therefore leaves the preceding record recoverable,
although a failure after rename can mean that the new record already landed.
Temporary files are removed when their owners are dropped.

The client's `MultipartUploadJournal` callbacks return I/O errors. Failure to
record the session prevents part uploads; failure to record an accepted part
prevents the next wave, completion, and file commit. Accepted parts missing from
the last saved record may be uploaded again safely. Journal failures include the
server upload ID and the local record path in the error.

Transfer errors leave the multipart session open. The caller can resume it or
explicitly abort it; abandoned sessions wait for expiry and garbage collection.
This costs temporary storage compared with aborting immediately after an error,
but preserves accepted parts after a recoverable failure. No partially uploaded
file becomes visible.

A completed upload can be committed without transferring its content again.
Successful file commits remove their journal; if removal fails, the error names
the successful commit and its sequence so the outcome is clear. Journal progress
does not yet preserve the complete file-commit request or coordinate concurrent
commands targeting the same journal. Retaining that request and exclusive journal
ownership are follow-up work for exact PUT retries.
