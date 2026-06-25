use crate::args::CommandKind;
use crate::config::{CliConfig, ProfileConfig};
use crate::error::CliError;
use crate::profiles::ProfileSummary;
use loonfs_api::{AuthoritativePathEntry, DeleteNamespaceResponse, FileRevision, NamespaceSummary};
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
#[serde(tag = "type", rename_all = "snake_case")]
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
    NamespaceList {
        namespaces: Vec<NamespaceSummary>,
    },
    PathEntries {
        entries: Vec<AuthoritativePathEntry>,
    },
    PathEntry(AuthoritativePathEntry),
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
    },
    PathMove {
        from: String,
        to: String,
        committed_seq: u64,
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
