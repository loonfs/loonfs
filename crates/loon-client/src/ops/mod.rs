//! Shared operability command, config, and rendering contract for LoonDB.
//!
//! Implements the core operations (import, observe, sync) consumed by both `loon-cli` and
//! `xtask`. This is the integration point that composes `loon-client` and `loon-types`
//! into user-facing workflows.

pub mod file;
pub mod import;
mod observe;
mod sync;

use crate::state_db::{ClientNamespaceStateSummary, SqliteStateDb};
use anyhow::{anyhow, bail, Context, Result};
use loon_types::server::{NamespaceBootstrapParams, NamespaceStateSummary, ServerTransport};
use loon_types::{NamespaceId, ObjectStore};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub use file::{run_file_command, FileCommand, FileCommandOutput};
pub use import::{
    import_authoritative_remote_observations, AuthoritativeObservationImportError,
    AuthoritativeObservationImportReport,
};
pub use observe::{
    observe_delete_path, observe_local_path, observe_move_path, observe_subtree_path,
    ObserveDeleteError, ObserveDeleteReport, ObserveLocalError, ObserveLocalReport,
    ObserveMoveError, ObserveMoveReport, ObserveSubtreeError, ObserveSubtreeReport,
    ObservedPathKind,
};
pub use sync::{
    sync_once, sync_until_idle, SyncOnceError, SyncOnceOutcome, SyncOnceReport, SyncUntilIdleError,
    SyncUntilIdleReport,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpsConfig {
    pub object_store: OpsObjectStoreSpec,
    pub client: OpsClientConfig,
    pub server: OpsServerConfig,
    #[serde(default)]
    pub ops: OpsSection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum OpsObjectStoreSpec {
    LocalFs {
        root: PathBuf,
        #[serde(default)]
        key_prefix: Option<String>,
    },
    AwsS3 {
        bucket: String,
        region: String,
        #[serde(default)]
        endpoint_url: Option<String>,
        access_key_id: String,
        secret_access_key: String,
        #[serde(default)]
        session_token: Option<String>,
        #[serde(default)]
        key_prefix: Option<String>,
        #[serde(default)]
        force_path_style: bool,
    },
    CloudflareR2 {
        bucket: String,
        account_id: String,
        endpoint_url: String,
        access_key_id: String,
        secret_access_key: String,
        #[serde(default)]
        key_prefix: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpsClientConfig {
    pub state_db_path: PathBuf,
    pub mirror_root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpsServerConfig {
    pub writer_id: String,
    pub writer_version: String,
    pub lease_duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct OpsSection {
    #[serde(default)]
    pub now_ms: Option<u64>,
    #[serde(default)]
    pub max_steps: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpsCommand {
    BootstrapNamespace {
        config_path: PathBuf,
        namespace_id: NamespaceId,
        allow_existing: bool,
    },
    ShowNamespaceState {
        config_path: PathBuf,
        namespace_id: NamespaceId,
    },
    ShowClientState {
        config_path: PathBuf,
        namespace_id: NamespaceId,
    },
    ImportRemoteObservations {
        config_path: PathBuf,
        namespace_id: NamespaceId,
    },
    ObserveLocal {
        config_path: PathBuf,
        namespace_id: NamespaceId,
        path: PathBuf,
    },
    ObserveDelete {
        config_path: PathBuf,
        namespace_id: NamespaceId,
        path: PathBuf,
    },
    ObserveMove {
        config_path: PathBuf,
        namespace_id: NamespaceId,
        from: PathBuf,
        to: PathBuf,
    },
    ObserveSubtree {
        config_path: PathBuf,
        namespace_id: NamespaceId,
        path: PathBuf,
    },
    SyncOnce {
        config_path: PathBuf,
        namespace_id: NamespaceId,
    },
    SyncUntilIdle {
        config_path: PathBuf,
        namespace_id: NamespaceId,
        max_steps: Option<u64>,
    },
    Smoke {
        config_path: PathBuf,
        namespace_id: NamespaceId,
    },
}

pub fn run_args<T: ServerTransport, S: ObjectStore>(
    args: impl IntoIterator<Item = String>,
    transport_factory: impl Fn(&OpsConfig) -> Result<T>,
    store_factory: impl Fn(&OpsConfig) -> Result<S>,
) -> Result<String> {
    run_command(parse_args(args)?, &transport_factory, &store_factory)
}

pub fn run_command<T: ServerTransport, S: ObjectStore>(
    command: OpsCommand,
    transport_factory: &impl Fn(&OpsConfig) -> Result<T>,
    store_factory: &impl Fn(&OpsConfig) -> Result<S>,
) -> Result<String> {
    match command {
        OpsCommand::BootstrapNamespace {
            config_path,
            namespace_id,
            allow_existing,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let transport = transport_factory(&config)?;
            let bootstrapped = transport.bootstrap_namespace(
                &namespace_id,
                &NamespaceBootstrapParams {
                    holder_id: config.server.writer_id.clone(),
                    writer_version: config.server.writer_version.clone(),
                    now_ms: config.now_ms(),
                    lease_duration_ms: config.server.lease_duration_ms,
                    allow_existing,
                },
            )?;
            Ok(format!(
                "command=ops/bootstrap-namespace\nnamespace={}\ncreated={}\ncheckpoint_seq={}\n",
                bootstrapped.namespace_id.as_str(),
                bootstrapped.created,
                bootstrapped.checkpoint_seq.0
            ))
        }
        OpsCommand::ShowNamespaceState {
            config_path,
            namespace_id,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let transport = transport_factory(&config)?;
            let summary = transport.load_namespace_state_summary(&namespace_id)?;
            render_namespace_state(&summary)
        }
        OpsCommand::ShowClientState {
            config_path,
            namespace_id,
        } => {
            let config = OpsConfig::load(&config_path)?;
            require_existing_file(&config.client.state_db_path, "client state db")?;
            let db = SqliteStateDb::open(&config.client.state_db_path)?;
            let summary = db.load_namespace_state_summary(&namespace_id)?;
            render_client_state(&summary)
        }
        OpsCommand::ImportRemoteObservations {
            config_path,
            namespace_id,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let transport = transport_factory(&config)?;
            let mut db = SqliteStateDb::open(&config.client.state_db_path)?;
            let report = import_authoritative_remote_observations(
                &mut db,
                &transport,
                &namespace_id,
                config.now_ms(),
            )?;
            render_authoritative_import_report(&report)
        }
        OpsCommand::ObserveLocal {
            config_path,
            namespace_id,
            path,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let report = observe::observe_local_path(&config, &namespace_id, &path)?;
            observe::render_observe_local_report(&report)
        }
        OpsCommand::ObserveDelete {
            config_path,
            namespace_id,
            path,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let report = observe::observe_delete_path(&config, &namespace_id, &path)?;
            observe::render_observe_delete_report(&report)
        }
        OpsCommand::ObserveMove {
            config_path,
            namespace_id,
            from,
            to,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let report = observe::observe_move_path(&config, &namespace_id, &from, &to)?;
            observe::render_observe_move_report(&report)
        }
        OpsCommand::ObserveSubtree {
            config_path,
            namespace_id,
            path,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let report = observe::observe_subtree_path(&config, &namespace_id, &path)?;
            observe::render_observe_subtree_report(&report)
        }
        OpsCommand::SyncOnce {
            config_path,
            namespace_id,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let transport = transport_factory(&config)?;
            let store = store_factory(&config)?;
            let report = sync::sync_once(&config, &namespace_id, &transport, &store)?;
            sync::render_sync_once_report(&report)
        }
        OpsCommand::SyncUntilIdle {
            config_path,
            namespace_id,
            max_steps,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let transport = transport_factory(&config)?;
            let store = store_factory(&config)?;
            let report =
                sync::sync_until_idle(&config, &namespace_id, max_steps, &transport, &store)?;
            sync::render_sync_until_idle_report(&report)
        }
        OpsCommand::Smoke {
            config_path,
            namespace_id,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let transport = transport_factory(&config)?;
            let bootstrap = transport.bootstrap_namespace(
                &namespace_id,
                &NamespaceBootstrapParams {
                    holder_id: config.server.writer_id.clone(),
                    writer_version: config.server.writer_version.clone(),
                    now_ms: config.now_ms(),
                    lease_duration_ms: config.server.lease_duration_ms,
                    allow_existing: true,
                },
            )?;
            let namespace_state = transport.load_namespace_state_summary(&namespace_id)?;
            let db = SqliteStateDb::open(&config.client.state_db_path)?;
            let client_state = db.load_namespace_state_summary(&namespace_id)?;
            let store_kind = match &config.object_store {
                OpsObjectStoreSpec::LocalFs { .. } => "local-fs",
                OpsObjectStoreSpec::AwsS3 { .. } => "aws-s3",
                OpsObjectStoreSpec::CloudflareR2 { .. } => "cloudflare-r2",
            };
            Ok(format!(
                "command=ops/smoke\nnamespace={}\nstore_kind={}\nbootstrap_status={}\nhead_seq={}\nclient_remote_rows={}\nclient_local_rows={}\n",
                namespace_id.as_str(),
                store_kind,
                if bootstrap.created { "bootstrapped" } else { "existing" },
                namespace_state.head.seq.0,
                client_state.remote_state.len(),
                client_state.local_state.len()
            ))
        }
    }
}

impl OpsConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("read ops config {}", path.display()))?;
        toml::from_str(&contents).with_context(|| format!("parse ops config {}", path.display()))
    }

    pub fn now_ms(&self) -> u64 {
        self.ops.now_ms.unwrap_or_else(current_time_ms)
    }
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<OpsCommand> {
    let mut args = args.into_iter();
    match args.next().as_deref() {
        Some("bootstrap-namespace") => {
            let parsed = parse_common_args(args, true)?;
            Ok(OpsCommand::BootstrapNamespace {
                config_path: parsed.config_path,
                namespace_id: parsed.namespace_id,
                allow_existing: parsed.allow_existing,
            })
        }
        Some("show-namespace-state") => {
            let parsed = parse_common_args(args, false)?;
            Ok(OpsCommand::ShowNamespaceState {
                config_path: parsed.config_path,
                namespace_id: parsed.namespace_id,
            })
        }
        Some("show-client-state") => {
            let parsed = parse_common_args(args, false)?;
            Ok(OpsCommand::ShowClientState {
                config_path: parsed.config_path,
                namespace_id: parsed.namespace_id,
            })
        }
        Some("import-remote-observations") => {
            let parsed = parse_common_args(args, false)?;
            Ok(OpsCommand::ImportRemoteObservations {
                config_path: parsed.config_path,
                namespace_id: parsed.namespace_id,
            })
        }
        Some("observe-local") => {
            let parsed = parse_common_args_with_path(args)?;
            Ok(OpsCommand::ObserveLocal {
                config_path: parsed.config_path,
                namespace_id: parsed.namespace_id,
                path: parsed.path,
            })
        }
        Some("observe-delete") => {
            let parsed = parse_common_args_with_path(args)?;
            Ok(OpsCommand::ObserveDelete {
                config_path: parsed.config_path,
                namespace_id: parsed.namespace_id,
                path: parsed.path,
            })
        }
        Some("observe-move") => {
            let parsed = parse_common_args_with_from_to(args)?;
            Ok(OpsCommand::ObserveMove {
                config_path: parsed.config_path,
                namespace_id: parsed.namespace_id,
                from: parsed.from,
                to: parsed.to,
            })
        }
        Some("observe-subtree") => {
            let parsed = parse_common_args_with_path(args)?;
            Ok(OpsCommand::ObserveSubtree {
                config_path: parsed.config_path,
                namespace_id: parsed.namespace_id,
                path: parsed.path,
            })
        }
        Some("sync-once") => {
            let parsed = parse_common_args(args, false)?;
            Ok(OpsCommand::SyncOnce {
                config_path: parsed.config_path,
                namespace_id: parsed.namespace_id,
            })
        }
        Some("sync-until-idle") => {
            let parsed = parse_common_args_with_optional_max_steps(args)?;
            Ok(OpsCommand::SyncUntilIdle {
                config_path: parsed.config_path,
                namespace_id: parsed.namespace_id,
                max_steps: parsed.max_steps,
            })
        }
        Some("smoke") => {
            let parsed = parse_common_args(args, false)?;
            Ok(OpsCommand::Smoke {
                config_path: parsed.config_path,
                namespace_id: parsed.namespace_id,
            })
        }
        Some(other) => bail!("unknown ops subcommand: {other}"),
        None => bail!(
            "usage: ops <bootstrap-namespace|show-namespace-state|show-client-state|import-remote-observations|observe-local|observe-delete|observe-move|observe-subtree|sync-once|sync-until-idle|smoke> --config <path> --namespace <id> [--allow-existing] [--path <path>] [--from <path> --to <path>] [--max-steps <n>]"
        ),
    }
}

fn render_namespace_state(summary: &NamespaceStateSummary) -> Result<String> {
    let yaml = serde_yaml::to_string(summary).context("render namespace state yaml")?;
    Ok(format!(
        "command=ops/show-namespace-state\nnamespace={}\n---\n{}",
        summary.namespace_id.as_str(),
        yaml
    ))
}

fn render_client_state(summary: &ClientNamespaceStateSummary) -> Result<String> {
    let yaml = serde_yaml::to_string(summary).context("render client state yaml")?;
    Ok(format!(
        "command=ops/show-client-state\nnamespace={}\n---\n{}",
        summary.namespace_id.as_str(),
        yaml
    ))
}

fn render_authoritative_import_report(
    report: &AuthoritativeObservationImportReport,
) -> Result<String> {
    let yaml = serde_yaml::to_string(report).context("render authoritative import yaml")?;
    Ok(format!(
        "command=ops/import-remote-observations\nnamespace={}\nauthoritative_head_seq={}\ntranslated_observation_count={}\n---\n{}",
        report.namespace_id.as_str(),
        report.authoritative_head_seq.0,
        report.translated_observation_count,
        yaml
    ))
}

fn require_existing_file(path: &Path, label: &str) -> Result<()> {
    if path.is_file() {
        return Ok(());
    }
    Err(anyhow!("{label} is missing: {}", path.display()))
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("wall clock should be after unix epoch")
        .as_millis() as u64
}

struct CommonArgs {
    config_path: PathBuf,
    namespace_id: NamespaceId,
    allow_existing: bool,
}

struct CommonArgsWithPath {
    config_path: PathBuf,
    namespace_id: NamespaceId,
    path: PathBuf,
}

struct CommonArgsWithFromTo {
    config_path: PathBuf,
    namespace_id: NamespaceId,
    from: PathBuf,
    to: PathBuf,
}

struct CommonArgsWithOptionalMaxSteps {
    config_path: PathBuf,
    namespace_id: NamespaceId,
    max_steps: Option<u64>,
}

fn parse_common_args(
    mut args: impl Iterator<Item = String>,
    allow_allow_existing: bool,
) -> Result<CommonArgs> {
    let mut config_path = None;
    let mut namespace_id = None;
    let mut allow_existing = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("missing value for --config"))?
                        .into(),
                );
            }
            "--namespace" => {
                namespace_id = Some(NamespaceId::from(
                    args.next()
                        .ok_or_else(|| anyhow!("missing value for --namespace"))?,
                ));
            }
            "--allow-existing" if allow_allow_existing => allow_existing = true,
            "--allow-existing" => bail!("--allow-existing is only valid for bootstrap-namespace"),
            other => bail!("unexpected ops argument: {other}"),
        }
    }

    Ok(CommonArgs {
        config_path: config_path.ok_or_else(|| anyhow!("missing --config"))?,
        namespace_id: namespace_id.ok_or_else(|| anyhow!("missing --namespace"))?,
        allow_existing,
    })
}

fn parse_common_args_with_path(
    mut args: impl Iterator<Item = String>,
) -> Result<CommonArgsWithPath> {
    let mut config_path = None;
    let mut namespace_id = None;
    let mut path = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("missing value for --config"))?
                        .into(),
                );
            }
            "--namespace" => {
                namespace_id = Some(NamespaceId::from(
                    args.next()
                        .ok_or_else(|| anyhow!("missing value for --namespace"))?,
                ));
            }
            "--path" => {
                path = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("missing value for --path"))?
                        .into(),
                );
            }
            other => bail!("unexpected ops argument: {other}"),
        }
    }

    Ok(CommonArgsWithPath {
        config_path: config_path.ok_or_else(|| anyhow!("missing --config"))?,
        namespace_id: namespace_id.ok_or_else(|| anyhow!("missing --namespace"))?,
        path: path.ok_or_else(|| anyhow!("missing --path"))?,
    })
}

fn parse_common_args_with_from_to(
    mut args: impl Iterator<Item = String>,
) -> Result<CommonArgsWithFromTo> {
    let mut config_path = None;
    let mut namespace_id = None;
    let mut from = None;
    let mut to = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("missing value for --config"))?
                        .into(),
                );
            }
            "--namespace" => {
                namespace_id = Some(NamespaceId::from(
                    args.next()
                        .ok_or_else(|| anyhow!("missing value for --namespace"))?,
                ));
            }
            "--from" => {
                from = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("missing value for --from"))?
                        .into(),
                );
            }
            "--to" => {
                to = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("missing value for --to"))?
                        .into(),
                );
            }
            other => bail!("unexpected ops argument: {other}"),
        }
    }

    Ok(CommonArgsWithFromTo {
        config_path: config_path.ok_or_else(|| anyhow!("missing --config"))?,
        namespace_id: namespace_id.ok_or_else(|| anyhow!("missing --namespace"))?,
        from: from.ok_or_else(|| anyhow!("missing --from"))?,
        to: to.ok_or_else(|| anyhow!("missing --to"))?,
    })
}

fn parse_common_args_with_optional_max_steps(
    mut args: impl Iterator<Item = String>,
) -> Result<CommonArgsWithOptionalMaxSteps> {
    let mut config_path = None;
    let mut namespace_id = None;
    let mut max_steps = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("missing value for --config"))?
                        .into(),
                );
            }
            "--namespace" => {
                namespace_id = Some(NamespaceId::from(
                    args.next()
                        .ok_or_else(|| anyhow!("missing value for --namespace"))?,
                ));
            }
            "--max-steps" => {
                max_steps = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("missing value for --max-steps"))?
                        .parse()
                        .context("parse --max-steps")?,
                );
            }
            other => bail!("unexpected ops argument: {other}"),
        }
    }

    Ok(CommonArgsWithOptionalMaxSteps {
        config_path: config_path.ok_or_else(|| anyhow!("missing --config"))?,
        namespace_id: namespace_id.ok_or_else(|| anyhow!("missing --namespace"))?,
        max_steps,
    })
}
