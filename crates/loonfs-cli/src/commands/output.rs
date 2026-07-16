//! The typed command results the renderer turns into text or JSON.

use crate::args::CommandKind;
use crate::config::{CliConfig, ProfileConfig};
use crate::error::CliError;
use crate::profiles::ProfileSummary;
use loonfs_api::v0::ChangesResponse;
use loonfs_api::{
    AdvanceRetentionResponse, AuthoritativePathEntry, CreateCheckpointResponse,
    DeleteNamespaceResponse, DisableGramsIndexResponse, EnableGramsIndexResponse, FileRevision,
    FlushWalResponse, GcResponse, GrepMatch, MaintenanceTickResponse, NamespaceSummary,
    ReleaseCheckpointResponse,
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
    pub error: CliError,
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
    WalFlushed(FlushWalResponse),
    RetentionAdvanced(AdvanceRetentionResponse),
    MaintenanceTicked(MaintenanceTickResponse),
    GarbageCollected(GcResponse),
    GramsIndexEnabled(EnableGramsIndexResponse),
    GramsIndexDisabled(DisableGramsIndexResponse),
    Changes(ChangesResponse),
    PathEntries {
        entries: Vec<AuthoritativePathEntry>,
    },
    PathEntry(AuthoritativePathEntry),
    GrepMatches {
        pattern: String,
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
        committed_seq: u64,
        commit_id: String,
        /// Inode the mutation acted on, when the command resolved one —
        /// `rm` reports it so the deletion stays recoverable via
        /// `loon undelete`.
        #[serde(skip_serializing_if = "Option::is_none")]
        inode_id: Option<u64>,
    },
    PathMove {
        from: String,
        to: String,
        committed_seq: u64,
        commit_id: String,
    },
    ConfigPath {
        path: String,
    },
    ConfigShow {
        config: CliConfig,
    },
    Version {
        version: String,
    },
    StreamBytes(Vec<u8>),
}
