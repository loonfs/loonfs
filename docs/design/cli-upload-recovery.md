# CLI upload recovery

File-backed remote PUTs keep recovery records in `$XDG_STATE_HOME/loonfs/uploads`,
or `$HOME/.local/state/loonfs/uploads`. The key includes the profile, server URL,
namespace, remote path, canonical local path, and any explicit commit ID. Local
path bytes are encoded without lossy Unicode conversion. Embedded uploads and
standard-input streams do not use these records. Repeating those commands uploads
new content and conflicts if the supplied commit ID already committed, even for
identical bytes. The embedded Rust API supports retries through retained prepared
content; the embedded CLI does not persist that proof across commands.

Before opening an upload, the CLI saves the chosen commit ID, actor, message,
overwrite guards, source length, and full-precision modification time. The record
has two states: uploading, with optional multipart progress; and prepared, with
the complete commit request. A resumed command uses the saved options. Changed
source metadata or options, an unreadable record, or malformed JSON stops the
command and preserves the record. Source checks compare metadata, not a full
file hash; the saved request always refers to the original uploaded content.

The client's `PutFileJournal` records multipart geometry and accepted parts, then
records the complete commit request before submission for every transport.
Callback failures stop the operation before its next request. A prepared request
includes its original content reference and proof; the CLI resends it directly
through `create_commit`, without reopening the upload or reconciling new content
against the earlier commit. Durable commit receipts can resolve a lost response
even after the upload session has disappeared. If the request never committed and
its proof expired, the server rejects it; recovery does not silently create a new
attempt. Calls without a journal can retain prepared content and an explicit
commit ID to retry publication. Repeating the upload creates a different request
and returns a conflict if the ID already committed, even for identical bytes.

Each update writes and syncs a private temporary file in the journal directory,
then atomically replaces the record. Unix hosts also sync the directory after
replacement or removal. In-memory progress changes only after persistence
succeeds. A failure after rename can mean the new record already landed.

An exclusive sidecar-file lock spans the entire command. A competing command
fails immediately. The lock file remains after completion so an already-open
file descriptor cannot bypass ownership when a record is replaced or removed.
The OS releases ownership when the process exits.

Ordinary attempts remove their record after acknowledgement. Explicit commit
IDs retain the prepared request so later invocations with the same ID can replay
it without another upload. These records and the small lock files consume local
storage until deliberately removed; receipt retention on the server still limits
how long a commit can be replayed. Removing a record abandons its local recovery
information. Cleanup errors after a successful commit name its ID and sequence.

All file-backed remote PUTs pay for durable local writes, including small files.
Transfer errors preserve multipart sessions for resume or explicit abort;
abandoned sessions wait for expiry and GC. This uses temporary storage but keeps
accepted parts after a recoverable failure. Partially uploaded files never become
visible.
