//! The typed command results the renderer turns into text or JSON.

use crate::args::CommandKind;
use crate::config::{CliConfig, ConfigSource, ProfileConfig};
use crate::error::CliError;
use crate::profiles::ProfileSummary;
use loonfs_api::v0::{
    ChangesResponse, DisableGrepIndexResponse, GrepGcResponse, GrepIndexLifecycle,
    GrepIndexStatusResponse, StoreProbeCheckOutcome, StoreProbeResponse,
};
use loonfs_api::{
    AuthoritativePathEntry, ChangeSeq, CommitId, CreateCheckpointResponse, DeleteNamespaceResponse,
    FileRevision, GcResponse, GrepMatch, InodeId, ListCheckpointsResponse, MaintenanceStepResponse,
    NamespaceId, NamespaceSummary, ReleaseCheckpointResponse,
};
use serde::Serialize;

/// One failed item inside a recursive transfer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TreeTransferFailure {
    /// The path that failed — remote for uploads and copies, whichever side
    /// failed for downloads.
    pub path: String,
    pub error: CliError,
}

/// A trash page plus the recovery command printed beside each entry.
///
/// The commands are rendered rather than carried as parts because the JSON
/// envelope already publishes every field a script would build its own
/// command from; a ready-to-paste line is a convenience for the human table,
/// so it is skipped on the wire and the response shape stays the API's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TrashListing {
    #[serde(flatten)]
    pub response: loonfs_api::ListTrashResponse,
    /// One complete `loonfs undelete` per entry, in `response.entries` order.
    #[serde(skip)]
    pub recovery_commands: Vec<String>,
}

/// One assigned `{job, namespace}` key, as a drain left it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct MaintenanceKeyReport {
    pub namespace_id: NamespaceId,
    /// The job as the runner names it in its own traces.
    pub job: String,
    /// Steps the drain ran for this key.
    pub steps: u64,
    /// What its last step concluded. Absent when the budget ran out before
    /// this key took a step.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conclusion: Option<String>,
    /// True when the key reached a conclusion with nothing left to drive.
    pub settled: bool,
}

pub(crate) struct CommandOutput {
    pub kind: CommandKind,
    pub profile: Option<String>,
    pub mode: Option<String>,
    pub data: CommandData,
}

pub(crate) struct CommandFailure {
    pub kind: CommandKind,
    pub profile: Option<String>,
    pub mode: Option<String>,
    /// Boxed to keep `Result<CommandOutput, CommandFailure>` small now that
    /// [`CliError`] carries request diagnostics (clippy `result_large_err`).
    pub error: Box<CliError>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum CommandData {
    Profile(ProfileConfig),
    ProfileSummary(ProfileSummary),
    ProfileList {
        default_profile: Option<String>,
        profiles: Vec<ProfileSummary>,
    },
    DefaultProfile {
        name: String,
    },
    DefaultNamespace {
        profile: String,
        namespace: String,
    },
    Current {
        profile: String,
        namespace: Option<String>,
    },
    NamespaceSummary(NamespaceSummary),
    NamespaceDeleted(DeleteNamespaceResponse),
    CheckpointCreated(CreateCheckpointResponse),
    CheckpointsListed(ListCheckpointsResponse),
    CheckpointReleased(ReleaseCheckpointResponse),
    MaintenanceStepped(MaintenanceStepResponse),
    GarbageCollected(GcResponse),
    GrepIndexEnabled {
        namespace_id: NamespaceId,
        /// True when the namespace already carried an enabled grep root.
        already_enabled: bool,
        /// The lifecycle last observed: what enable published with
        /// `--no-wait`, otherwise where the wait stopped.
        state: GrepIndexLifecycle,
        /// The sequence the wait drove toward. Absent with `--no-wait` and
        /// on a namespace whose index is disabled.
        #[serde(skip_serializing_if = "Option::is_none")]
        waited_for_seq: Option<ChangeSeq>,
        /// Steps the wait spent: bounded index steps in embedded mode,
        /// status checks in remote mode.
        steps: u64,
        /// True when a budget stopped the wait before the target.
        budget_exhausted: bool,
    },
    /// What an assigned-namespace maintenance host did.
    MaintenanceHosted {
        /// The assignment, sorted and deduplicated.
        namespaces: Vec<NamespaceId>,
        jobs: Vec<String>,
        /// True when the host caught the assignment up and exited instead of
        /// hosting until a signal.
        drained: bool,
        /// Where each key got to. Empty for a hosted run: the runner ran
        /// those steps, and durable state is what reports them.
        keys: Vec<MaintenanceKeyReport>,
        /// Steps the drain ran across every key.
        steps: u64,
        /// True when a budget stopped the drain before every key settled.
        budget_exhausted: bool,
    },
    /// What one store contract probe found. Failed checks are data, not an
    /// error: the probe ran and the store is what it is.
    StoreProbed(StoreProbeResponse),
    GrepIndexDisabled(DisableGrepIndexResponse),
    GrepIndexStatus(GrepIndexStatusResponse),
    GrepIndexCollected(GrepGcResponse),
    Changes(ChangesResponse),
    Trash(TrashListing),
    PathEntries {
        entries: Vec<AuthoritativePathEntry>,
        /// Where a bounded listing stopped, and how to resume it. Present
        /// only when `--limit` cut the listing short; its presence is what
        /// says the directory holds more than was printed.
        #[serde(skip_serializing_if = "Option::is_none")]
        next_cursor: Option<String>,
    },
    PathEntry(AuthoritativePathEntry),
    GrepMatches {
        pattern: String,
        namespace_id: NamespaceId,
        /// Namespace head the final page was evaluated against.
        head_seq: ChangeSeq,
        /// Index watermark: content committed at or below this sequence is
        /// searchable through the index.
        built_through_seq: ChangeSeq,
        matches: Vec<GrepMatch>,
        tail_scanned: bool,
        /// True when `--max-matches` stopped the search with matches left
        /// to find.
        truncated: bool,
    },
    FileRevisions {
        target: String,
        revisions: Vec<FileRevision>,
        next_cursor: Option<String>,
    },
    FileTransfer {
        target: String,
        destination: String,
        bytes_written: u64,
    },
    /// A recursive transfer's summary: per-item successes are counted, not
    /// listed (a tree can hold tens of thousands of entries), and every
    /// failure is listed with its own error.
    TreeTransfer {
        source: String,
        destination: String,
        files: u64,
        directories: u64,
        failures: Vec<TreeTransferFailure>,
    },
    FileMutation {
        target: String,
        committed_seq: ChangeSeq,
        commit_id: CommitId,
        /// Inode the mutation acted on, when the command resolved one —
        /// `rm` reports it so the deletion stays recoverable via
        /// `loonfs undelete`.
        #[serde(skip_serializing_if = "Option::is_none")]
        inode_id: Option<InodeId>,
        /// The `loonfs undelete` that puts this deletion back, set by `rm`
        /// alone. Human-only, for the reason [`TrashListing`] gives.
        #[serde(skip)]
        recovery_command: Option<String>,
    },
    /// A `mkdir -p` whose target was already a directory. Nothing was
    /// committed, so there is no commit to report — the directory the
    /// caller asked for is simply there.
    DirectoryAlreadyExists {
        target: String,
        inode_id: InodeId,
        /// Namespace head the existing directory was observed at.
        head_seq: ChangeSeq,
    },
    PathMove {
        from: String,
        to: String,
        committed_seq: ChangeSeq,
        commit_id: CommitId,
    },
    ConfigPath {
        path: String,
        /// Which rule chose the path.
        source: ConfigSource,
        /// Where the file belongs now, while a legacy file is in use only
        /// because the preferred location holds none yet.
        #[serde(skip_serializing_if = "Option::is_none")]
        preferred_path: Option<String>,
    },
    ConfigShow {
        config: CliConfig,
    },
    /// `config show` when the file no longer strict-decodes: the failure and
    /// the file as parsed (secrets masked), so the user can see what to fix.
    ConfigShowDegraded {
        error: String,
        config_toml: String,
    },
    Version {
        version: String,
    },
    StreamBytes(Vec<u8>),
    /// The payload already went to standard output as it arrived, so there is
    /// nothing left to render. `get -` reports this: a download that is
    /// written chunk by chunk cannot also be handed to the renderer at the
    /// end without holding all of it.
    StreamedToStdout,
}

impl CommandData {
    /// Whether this success-shaped output still reports failed items, so
    /// the process can exit nonzero without discarding the structured
    /// results a partial failure produced.
    pub(crate) fn reports_failures(&self) -> bool {
        match self {
            CommandData::TreeTransfer { failures, .. } => !failures.is_empty(),
            // A wait that ran out of budget renders where the index got to
            // — real data, not an error — and still exits nonzero, because
            // the caller asked for a target that was not reached.
            CommandData::GrepIndexEnabled {
                budget_exhausted, ..
            }
            // Same for a drain that ran out of budget: the per-key progress
            // it prints is real, and the assignment it was asked to catch
            // up is not caught up.
            | CommandData::MaintenanceHosted {
                budget_exhausted, ..
            } => *budget_exhausted,
            // A probe that found a broken store prints every check's verdict
            // and still exits nonzero, because the store it was asked about
            // cannot be trusted.
            CommandData::StoreProbed(response) => response
                .checks
                .iter()
                .any(|check| check.outcome == StoreProbeCheckOutcome::Failed),
            _ => false,
        }
    }
}
