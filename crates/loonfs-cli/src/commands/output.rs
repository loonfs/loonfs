//! The typed command results the renderer turns into text or JSON.

use crate::args::CommandKind;
use crate::config::{CliConfig, ProfileConfig};
use crate::error::CliError;
use crate::profiles::ProfileSummary;
use loonfs_api::v0::{
    ChangesResponse, DisableGrepIndexResponse, EnableGrepIndexResponse, RepairNamespaceResponse,
};
use loonfs_api::{
    AuthoritativePathEntry, ChangeSeq, CommitId, CreateCheckpointResponse, DeleteNamespaceResponse,
    FileRevision, GcResponse, GrepMatch, InodeId, MaintenanceStepResponse, NamespaceId,
    NamespaceSummary, ReleaseCheckpointResponse,
};
use serde::Serialize;

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
    CheckpointReleased(ReleaseCheckpointResponse),
    MaintenanceStepped(MaintenanceStepResponse),
    GarbageCollected(GcResponse),
    NamespaceRepaired(RepairNamespaceResponse),
    GrepIndexEnabled(EnableGrepIndexResponse),
    GrepIndexDisabled(DisableGrepIndexResponse),
    Changes(ChangesResponse),
    PathEntries {
        entries: Vec<AuthoritativePathEntry>,
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
    FileMutation {
        target: String,
        committed_seq: ChangeSeq,
        commit_id: CommitId,
        /// Inode the mutation acted on, when the command resolved one —
        /// `rm` reports it so the deletion stays recoverable via
        /// `loon undelete`.
        #[serde(skip_serializing_if = "Option::is_none")]
        inode_id: Option<InodeId>,
    },
    PathMove {
        from: String,
        to: String,
        committed_seq: ChangeSeq,
        commit_id: CommitId,
    },
    ConfigPath {
        path: String,
    },
    ConfigShow {
        config: CliConfig,
    },
    Version {
        version: String,
        /// Git commit the binary was built from ("unknown" without git).
        commit: String,
        /// Commit date of that commit ("unknown" without git).
        commit_date: String,
    },
    StreamBytes(Vec<u8>),
}
