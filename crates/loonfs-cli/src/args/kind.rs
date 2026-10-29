//! [`CommandKind`]: the name of each command, as the JSON output and the
//! error envelope report it.

use super::cli::{Cli, Command, ConfigCommand, TopLevelCommand};
use super::filesystem::AccessCommand;
use super::maintenance::{
    MaintenanceCheckpointCommand, MaintenanceCommand, MaintenanceIndexCommand,
    MaintenanceRetentionCommand, MaintenanceStoreCommand,
};
use super::namespace::{NamespaceCommand, SnapshotCommand};
use super::profile::ProfileCommand;

macro_rules! command_kinds {
    (@count) => { 0 };
    (@count $head:ident $($tail:ident)*) => { 1 + command_kinds!(@count $($tail)*) };
    ($($variant:ident => $value:literal),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub(crate) enum CommandKind {
            $($variant,)+
        }

        impl CommandKind {
            #[cfg(test)]
            pub(crate) const ALL: [Self; command_kinds!(@count $($variant)+)] =
                [$(Self::$variant,)+];

            pub(crate) fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value,)+
                }
            }
        }
    };
}

command_kinds! {
    Completion => "completion",
    Init => "init",
    ProfileCreate => "profile_create",
    ProfileList => "profile_list",
    ProfileShow => "profile_show",
    ProfileUpdate => "profile_update",
    ProfileDelete => "profile_delete",
    ProfileUse => "profile_use",
    NamespaceCreate => "namespace_create",
    NamespaceShow => "namespace_show",
    NamespaceDelete => "namespace_delete",
    NamespaceFork => "namespace_fork",
    NamespaceUse => "namespace_use",
    SnapshotCreate => "snapshot_create",
    SnapshotList => "snapshot_list",
    SnapshotExtend => "snapshot_extend",
    SnapshotDelete => "snapshot_delete",
    Current => "current",
    FilesystemLs => "filesystem_ls",
    FilesystemStat => "filesystem_stat",
    FilesystemAnnotate => "filesystem_annotate",
    AccessSet => "access_set",
    MaintenanceRecoverAdministrator => "maintenance_recover_administrator",
    FilesystemCat => "filesystem_cat",
    FilesystemGrep => "filesystem_grep",
    FilesystemGet => "filesystem_get",
    FilesystemPut => "filesystem_put",
    FilesystemAppend => "filesystem_append",
    FilesystemRevisions => "filesystem_revisions",
    FilesystemTrash => "filesystem_trash",
    FilesystemRestore => "filesystem_restore",
    FilesystemUndelete => "filesystem_undelete",
    FilesystemMkdir => "filesystem_mkdir",
    FilesystemRm => "filesystem_rm",
    FilesystemMv => "filesystem_mv",
    FilesystemCp => "filesystem_cp",
    Changes => "changes",
    Capabilities => "capabilities",
    Doctor => "doctor",
    MaintenanceCheckpointList => "maintenance_checkpoint_list",
    MaintenanceFold => "maintenance_fold",
    MaintenanceRetentionAdvance => "maintenance_retention_advance",
    MaintenanceRetentionWindow => "maintenance_retention_window",
    MaintenanceMetadata => "maintenance_metadata",
    MaintenanceGc => "maintenance_gc",
    MaintenanceStoreProbe => "maintenance_store_probe",
    MaintenanceIndexEnable => "maintenance_index_enable",
    MaintenanceIndexDisable => "maintenance_index_disable",
    MaintenanceIndexStatus => "maintenance_index_status",
    ConfigPath => "config_path",
    ConfigShow => "config_show",
    Version => "version",
}

impl CommandKind {
    pub(crate) fn supports_json(self) -> bool {
        !matches!(self, CommandKind::Completion | CommandKind::FilesystemCat)
    }
}

impl Cli {
    pub(crate) fn kind(&self) -> CommandKind {
        match &self.command {
            TopLevelCommand::Configured(command) => command.kind(),
            TopLevelCommand::Doctor(_) => CommandKind::Doctor,
            TopLevelCommand::Completion(_) => CommandKind::Completion,
        }
    }
}

impl Command {
    fn kind(&self) -> CommandKind {
        match self {
            Command::Init => CommandKind::Init,
            Command::Profile { command } => match command {
                ProfileCommand::Create { .. } => CommandKind::ProfileCreate,
                ProfileCommand::List => CommandKind::ProfileList,
                ProfileCommand::Show { .. } => CommandKind::ProfileShow,
                ProfileCommand::Update { .. } => CommandKind::ProfileUpdate,
                ProfileCommand::Delete { .. } => CommandKind::ProfileDelete,
                ProfileCommand::Use { .. } => CommandKind::ProfileUse,
            },
            Command::Namespace { command } => match command {
                NamespaceCommand::Create(_) => CommandKind::NamespaceCreate,
                NamespaceCommand::Show(_) => CommandKind::NamespaceShow,
                NamespaceCommand::Delete(_) => CommandKind::NamespaceDelete,
                NamespaceCommand::Fork(_) => CommandKind::NamespaceFork,
            },
            Command::Snapshot { command } => match command {
                SnapshotCommand::Create(_) => CommandKind::SnapshotCreate,
                SnapshotCommand::List(_) => CommandKind::SnapshotList,
                SnapshotCommand::Extend(_) => CommandKind::SnapshotExtend,
                SnapshotCommand::Delete(_) => CommandKind::SnapshotDelete,
            },
            Command::Use(_) => CommandKind::NamespaceUse,
            Command::Current => CommandKind::Current,
            Command::Ls(_) => CommandKind::FilesystemLs,
            Command::Stat(_) => CommandKind::FilesystemStat,
            Command::Annotate(_) => CommandKind::FilesystemAnnotate,
            Command::Access { command } => match command {
                AccessCommand::Set(_) => CommandKind::AccessSet,
            },
            Command::Cat(_) => CommandKind::FilesystemCat,
            Command::Grep(_) => CommandKind::FilesystemGrep,
            Command::Get(_) => CommandKind::FilesystemGet,
            Command::Put(_) => CommandKind::FilesystemPut,
            Command::Append(_) => CommandKind::FilesystemAppend,
            Command::Revisions(_) => CommandKind::FilesystemRevisions,
            Command::Trash(_) => CommandKind::FilesystemTrash,
            Command::Restore(_) => CommandKind::FilesystemRestore,
            Command::Undelete(_) => CommandKind::FilesystemUndelete,
            Command::Mkdir(_) => CommandKind::FilesystemMkdir,
            Command::Rm(_) => CommandKind::FilesystemRm,
            Command::Mv(_) => CommandKind::FilesystemMv,
            Command::Cp(_) => CommandKind::FilesystemCp,
            Command::Changes(_) => CommandKind::Changes,
            Command::Capabilities => CommandKind::Capabilities,
            Command::Maintenance { command } => match command {
                MaintenanceCommand::RecoverAdministrator(_) => {
                    CommandKind::MaintenanceRecoverAdministrator
                }
                MaintenanceCommand::Metadata(_) => CommandKind::MaintenanceMetadata,
                MaintenanceCommand::Fold => CommandKind::MaintenanceFold,
                MaintenanceCommand::Checkpoint { command } => match command {
                    MaintenanceCheckpointCommand::List(_) => CommandKind::MaintenanceCheckpointList,
                },
                MaintenanceCommand::Index { command } => match command {
                    MaintenanceIndexCommand::Enable(_) => CommandKind::MaintenanceIndexEnable,
                    MaintenanceIndexCommand::Disable => CommandKind::MaintenanceIndexDisable,
                    MaintenanceIndexCommand::Status => CommandKind::MaintenanceIndexStatus,
                },
                MaintenanceCommand::Retention { command } => match command {
                    MaintenanceRetentionCommand::Advance(_) => {
                        CommandKind::MaintenanceRetentionAdvance
                    }
                    MaintenanceRetentionCommand::Window(_) => {
                        CommandKind::MaintenanceRetentionWindow
                    }
                },
                MaintenanceCommand::Gc(_) => CommandKind::MaintenanceGc,
                MaintenanceCommand::Store { command } => match command {
                    MaintenanceStoreCommand::Probe => CommandKind::MaintenanceStoreProbe,
                },
            },
            Command::Config { command } => match command {
                ConfigCommand::Path => CommandKind::ConfigPath,
                ConfigCommand::Show => CommandKind::ConfigShow,
            },
            Command::Version => CommandKind::Version,
        }
    }
}
