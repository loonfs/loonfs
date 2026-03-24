use clap::{Args, Subcommand};
use loon_ops::OpsCommand;
use loon_types::NamespaceId;
use std::path::PathBuf;

#[derive(Debug, Args)]
pub struct OpsArgs {
    #[command(subcommand)]
    command: OpsSubcommand,
}

#[derive(Debug, Subcommand)]
enum OpsSubcommand {
    /// Bootstrap namespace control state in object storage.
    BootstrapNamespace(BootstrapNamespaceArgs),
    /// Show the authoritative namespace state reconstructed from object storage.
    ShowNamespaceState(CommonArgs),
    /// Show the local client SQLite state for one namespace.
    ShowClientState(CommonArgs),
    /// Import authoritative remote observations into the local client DB.
    ImportRemoteObservations(CommonArgs),
    /// Observe one existing local file and replan immediately.
    ObserveLocal(CommonArgsWithPath),
    /// Observe one explicit local delete and replan immediately.
    ObserveDelete(CommonArgsWithPath),
    /// Observe one explicit local move and replan immediately.
    ObserveMove(CommonArgsWithFromTo),
    /// Observe one recursive local subtree and replan atomically.
    ObserveSubtree(CommonArgsWithPath),
    /// Execute at most one real scheduler step.
    SyncOnce(CommonArgs),
    /// Repeat real scheduler steps until idle or the step cap is reached.
    SyncUntilIdle(CommonArgsWithOptionalMaxSteps),
    /// Validate local operability against the configured provider and client DB.
    Smoke(CommonArgs),
}

#[derive(Debug, Clone, Args)]
struct CommonArgs {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    namespace: String,
}

#[derive(Debug, Clone, Args)]
struct BootstrapNamespaceArgs {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long)]
    allow_existing: bool,
}

#[derive(Debug, Clone, Args)]
struct CommonArgsWithPath {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long)]
    path: PathBuf,
}

#[derive(Debug, Clone, Args)]
struct CommonArgsWithFromTo {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long)]
    from: PathBuf,
    #[arg(long)]
    to: PathBuf,
}

#[derive(Debug, Clone, Args)]
struct CommonArgsWithOptionalMaxSteps {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long)]
    max_steps: Option<u64>,
}

impl OpsArgs {
    pub(crate) fn into_command(self) -> OpsCommand {
        match self.command {
            OpsSubcommand::BootstrapNamespace(args) => OpsCommand::BootstrapNamespace {
                config_path: args.common.config,
                namespace_id: NamespaceId::from(args.common.namespace),
                allow_existing: args.allow_existing,
            },
            OpsSubcommand::ShowNamespaceState(args) => OpsCommand::ShowNamespaceState {
                config_path: args.config,
                namespace_id: NamespaceId::from(args.namespace),
            },
            OpsSubcommand::ShowClientState(args) => OpsCommand::ShowClientState {
                config_path: args.config,
                namespace_id: NamespaceId::from(args.namespace),
            },
            OpsSubcommand::ImportRemoteObservations(args) => OpsCommand::ImportRemoteObservations {
                config_path: args.config,
                namespace_id: NamespaceId::from(args.namespace),
            },
            OpsSubcommand::ObserveLocal(args) => OpsCommand::ObserveLocal {
                config_path: args.common.config,
                namespace_id: NamespaceId::from(args.common.namespace),
                path: args.path,
            },
            OpsSubcommand::ObserveDelete(args) => OpsCommand::ObserveDelete {
                config_path: args.common.config,
                namespace_id: NamespaceId::from(args.common.namespace),
                path: args.path,
            },
            OpsSubcommand::ObserveMove(args) => OpsCommand::ObserveMove {
                config_path: args.common.config,
                namespace_id: NamespaceId::from(args.common.namespace),
                from: args.from,
                to: args.to,
            },
            OpsSubcommand::ObserveSubtree(args) => OpsCommand::ObserveSubtree {
                config_path: args.common.config,
                namespace_id: NamespaceId::from(args.common.namespace),
                path: args.path,
            },
            OpsSubcommand::SyncOnce(args) => OpsCommand::SyncOnce {
                config_path: args.config,
                namespace_id: NamespaceId::from(args.namespace),
            },
            OpsSubcommand::SyncUntilIdle(args) => OpsCommand::SyncUntilIdle {
                config_path: args.common.config,
                namespace_id: NamespaceId::from(args.common.namespace),
                max_steps: args.max_steps,
            },
            OpsSubcommand::Smoke(args) => OpsCommand::Smoke {
                config_path: args.config,
                namespace_id: NamespaceId::from(args.namespace),
            },
        }
    }
}
