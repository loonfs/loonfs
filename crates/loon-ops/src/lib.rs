#![forbid(unsafe_code)]

mod import;
mod observe;
mod sync;

use anyhow::{anyhow, bail, Context, Result};
use loon_client::state_db::{ClientNamespaceStateSummary, SqliteStateDb};
use loon_objectstore::r2::R2StoreConfig;
use loon_objectstore::s3::AwsS3StoreConfig;
use loon_objectstore::{ConfiguredObjectStore, ConfiguredObjectStoreKind};
use loon_server::ops::{
    bootstrap_namespace, load_namespace_state_summary, NamespaceBootstrapParams,
    NamespaceStateSummary,
};
use loon_types::NamespaceId;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

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

pub fn run_args(args: impl IntoIterator<Item = String>) -> Result<String> {
    run_command(parse_args(args)?)
}

pub fn run_command(command: OpsCommand) -> Result<String> {
    match command {
        OpsCommand::BootstrapNamespace {
            config_path,
            namespace_id,
            allow_existing,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let store = config.open_store()?;
            let bootstrapped = bootstrap_namespace(
                &store,
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
            let store = config.open_store()?;
            let summary = load_namespace_state_summary(&store, &namespace_id)?;
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
            let store = config.open_store()?;
            let mut db = SqliteStateDb::open(&config.client.state_db_path)?;
            let report = import_authoritative_remote_observations(
                &mut db,
                &store,
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
            let report = sync::sync_once(&config, &namespace_id)?;
            sync::render_sync_once_report(&report)
        }
        OpsCommand::SyncUntilIdle {
            config_path,
            namespace_id,
            max_steps,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let report = sync::sync_until_idle(&config, &namespace_id, max_steps)?;
            sync::render_sync_until_idle_report(&report)
        }
        OpsCommand::Smoke {
            config_path,
            namespace_id,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let store = config.open_store()?;
            let bootstrap = bootstrap_namespace(
                &store,
                &namespace_id,
                &NamespaceBootstrapParams {
                    holder_id: config.server.writer_id.clone(),
                    writer_version: config.server.writer_version.clone(),
                    now_ms: config.now_ms(),
                    lease_duration_ms: config.server.lease_duration_ms,
                    allow_existing: true,
                },
            )?;
            let namespace_state = load_namespace_state_summary(&store, &namespace_id)?;
            let db = SqliteStateDb::open(&config.client.state_db_path)?;
            let client_state = db.load_namespace_state_summary(&namespace_id)?;
            Ok(format!(
                "command=ops/smoke\nnamespace={}\nstore_kind={}\nbootstrap_status={}\nhead_seq={}\nclient_remote_rows={}\nclient_local_rows={}\n",
                namespace_id.as_str(),
                configured_store_kind_as_str(store.kind()),
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

    pub fn open_store(&self) -> Result<ConfiguredObjectStore> {
        match &self.object_store {
            OpsObjectStoreSpec::LocalFs { root, key_prefix } => {
                ConfiguredObjectStore::local_fs(root, key_prefix.as_deref()).map_err(Into::into)
            }
            OpsObjectStoreSpec::AwsS3 {
                bucket,
                region,
                endpoint_url,
                access_key_id,
                secret_access_key,
                session_token,
                key_prefix,
                force_path_style,
            } => ConfiguredObjectStore::aws_s3(AwsS3StoreConfig {
                bucket: bucket.clone(),
                region: region.clone(),
                endpoint_url: endpoint_url.clone(),
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                session_token: session_token.clone(),
                key_prefix: key_prefix.clone(),
                force_path_style: *force_path_style,
            })
            .map_err(Into::into),
            OpsObjectStoreSpec::CloudflareR2 {
                bucket,
                account_id,
                endpoint_url,
                access_key_id,
                secret_access_key,
                key_prefix,
            } => ConfiguredObjectStore::cloudflare_r2(R2StoreConfig {
                bucket: bucket.clone(),
                account_id: account_id.clone(),
                endpoint_url: endpoint_url.clone(),
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                key_prefix: key_prefix.clone(),
            })
            .map_err(Into::into),
        }
    }

    fn now_ms(&self) -> u64 {
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

fn configured_store_kind_as_str(kind: ConfiguredObjectStoreKind) -> &'static str {
    match kind {
        ConfiguredObjectStoreKind::LocalFs => "local-fs",
        ConfiguredObjectStoreKind::AwsS3 => "aws-s3",
        ConfiguredObjectStoreKind::CloudflareR2 => "cloudflare-r2",
    }
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

#[cfg(test)]
mod tests {
    use super::{run_args, OpsConfig};
    use loon_client::local_fs::NamespacePathIndex;
    use loon_client::state_db::{
        LocalFileStateRow, LocalOnlyFileStateRow, LocalOnlyPlannedActionRow, RemoteFileStateRow,
        SqliteStateDb, SyncAnchorRow,
    };
    use loon_client::upload::upload_small_file_from_path;
    use loon_core::checkpoint::prepare_checkpoint;
    use loon_core::metadata::{DirentryRecord, InodeRecord, MetadataState, RevisionRecord};
    use loon_objectstore::keys::{namespace_head, namespace_lease};
    use loon_objectstore::{ConfiguredObjectStore, ObjectStore};
    use loon_types::{
        sha256_digest, ChangeSeq, ControlObjectKind, FenceToken, HeadState, HeadStateEnvelope,
        InodeId, InodeKind, LeaseState, LeaseStateEnvelope, NamespaceId, RevisionNo,
    };
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn parses_all_provider_variants() {
        let local = load_config(
            r#"
[object_store]
kind = "local-fs"
root = "/tmp/loondb"
key_prefix = "tenant-a"

[client]
state_db_path = "/tmp/client.sqlite3"
mirror_root = "/tmp/mirror"

[server]
writer_id = "writer-a"
writer_version = "loon-ops-test"
lease_duration_ms = 60000
"#,
        );
        assert!(matches!(
            local.object_store,
            super::OpsObjectStoreSpec::LocalFs { .. }
        ));

        let s3 = load_config(
            r#"
[object_store]
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
endpoint_url = "http://127.0.0.1:9000"
access_key_id = "access"
secret_access_key = "secret"
key_prefix = "tenant-a"
force_path_style = true

[client]
state_db_path = "/tmp/client.sqlite3"
mirror_root = "/tmp/mirror"

[server]
writer_id = "writer-a"
writer_version = "loon-ops-test"
lease_duration_ms = 60000
"#,
        );
        assert!(matches!(
            s3.object_store,
            super::OpsObjectStoreSpec::AwsS3 { .. }
        ));

        let r2 = load_config(
            r#"
[object_store]
kind = "cloudflare-r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "https://example.r2.cloudflarestorage.com"
access_key_id = "access"
secret_access_key = "secret"
key_prefix = "tenant-a"

[client]
state_db_path = "/tmp/client.sqlite3"
mirror_root = "/tmp/mirror"

[server]
writer_id = "writer-a"
writer_version = "loon-ops-test"
lease_duration_ms = 60000
"#,
        );
        assert!(matches!(
            r2.object_store,
            super::OpsObjectStoreSpec::CloudflareR2 { .. }
        ));
    }

    #[test]
    fn bootstrap_show_and_smoke_local_fs() {
        let temp_dir = unique_temp_dir("ops-local-fs");
        let config_path = write_local_fs_config(&temp_dir);
        let namespace_id = "demo";

        let bootstrap = run_args([
            "bootstrap-namespace".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            namespace_id.to_owned(),
        ])
        .expect("run bootstrap");
        assert_eq!(
            bootstrap,
            include_str!(
                "../../../tests/snapshots/ops-bootstrap-namespace/ops_bootstrap_namespace.txt"
            )
        );

        let namespace_state = run_args([
            "show-namespace-state".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            namespace_id.to_owned(),
        ])
        .expect("run show namespace state");
        assert_eq!(
            namespace_state,
            include_str!(
                "../../../tests/snapshots/ops-show-namespace-state/ops_show_namespace_state.txt"
            )
        );

        let db_path = temp_dir.join("client.sqlite3");
        let _db = SqliteStateDb::open(&db_path).expect("open client db");
        let client_state = run_args([
            "show-client-state".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            namespace_id.to_owned(),
        ])
        .expect("run show client state");
        assert_eq!(
            client_state,
            include_str!(
                "../../../tests/snapshots/ops-show-client-state/ops_show_client_state.txt"
            )
        );

        let smoke = run_args([
            "smoke".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            namespace_id.to_owned(),
        ])
        .expect("run smoke");
        assert_eq!(
            smoke,
            include_str!("../../../tests/snapshots/ops-smoke/ops_smoke.txt")
        );
    }

    #[test]
    fn import_remote_observations_local_fs() {
        let temp_dir = unique_temp_dir("ops-import-local-fs");
        let config_path = write_local_fs_config(&temp_dir);
        let namespace_id = "demo";

        run_args([
            "bootstrap-namespace".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            namespace_id.to_owned(),
        ])
        .expect("bootstrap namespace before import");

        let rendered = run_args([
            "import-remote-observations".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            namespace_id.to_owned(),
        ])
        .expect("run import-remote-observations");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-import-remote-observations/ops_import_remote_observations.txt"
            )
        );
    }

    #[test]
    fn import_remote_observations_plans_root_materialization_for_fresh_namespace() {
        let temp_dir = unique_temp_dir("ops-import-root-materialization");
        let config_path = write_local_fs_config(&temp_dir);
        let namespace_id = "demo";

        run_args([
            "bootstrap-namespace".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            namespace_id.to_owned(),
        ])
        .expect("bootstrap namespace before import");
        run_args([
            "import-remote-observations".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            namespace_id.to_owned(),
        ])
        .expect("import remote observations");

        let rendered = run_args([
            "show-client-state".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            namespace_id.to_owned(),
        ])
        .expect("show client state after import");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-show-client-state/ops_show_client_state_after_import_plans_root_materialization.txt"
            )
        );
    }

    #[test]
    fn observe_local_rejects_path_outside_mirror_root() {
        let temp_dir = unique_temp_dir("ops-observe-outside-root");
        let config_path = write_local_fs_config(&temp_dir);
        let _db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client db");
        let outside_path = temp_dir.join("outside.txt");
        fs::write(&outside_path, b"hello from outside root\n").expect("write outside file");

        let error = run_args([
            "observe-local".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            outside_path.display().to_string(),
        ])
        .expect_err("observe-local should reject path outside mirror_root");

        assert!(
            error
                .to_string()
                .contains("observe-local path is outside mirror_root"),
            "{error:#}"
        );
    }

    #[test]
    fn observe_local_rejects_directory_path() {
        let temp_dir = unique_temp_dir("ops-observe-dir");
        let config_path = write_local_fs_config(&temp_dir);
        let _db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client db");

        let error = run_args([
            "observe-local".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror").display().to_string(),
        ])
        .expect_err("observe-local should reject directory path");

        assert!(
            error
                .to_string()
                .contains("observe-local requires an existing file path, got directory"),
            "{error:#}"
        );
    }

    #[test]
    fn observe_local_bound_file_renders_upload_local_edit_report() {
        let temp_dir = unique_temp_dir("ops-observe-bound-file");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            "hello.txt",
            "sha256:remote-hello-v1",
            "sha256:manifest-remote-hello-v1",
        );
        fs::write(
            temp_dir.join("mirror/hello.txt"),
            b"hello from local edit\n",
        )
        .expect("write bound file bytes");

        let rendered = run_args([
            "observe-local".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror/hello.txt").display().to_string(),
        ])
        .expect("run observe-local on bound file");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-observe-local/ops_observe_local_bound_file.txt"
            )
        );
    }

    #[test]
    fn observe_local_local_only_file_renders_upload_local_create_report() {
        let temp_dir = unique_temp_dir("ops-observe-local-only");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        fs::write(temp_dir.join("mirror/draft.txt"), b"draft local file\n")
            .expect("write local-only file bytes");

        let rendered = run_args([
            "observe-local".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror/draft.txt").display().to_string(),
        ])
        .expect("run observe-local on local-only file");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-observe-local/ops_observe_local_local_only_file.txt"
            )
        );
    }

    #[test]
    fn observe_delete_bound_file_renders_delete_file_report() {
        let temp_dir = unique_temp_dir("ops-observe-delete-bound-file");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            "hello.txt",
            "sha256:remote-hello-v1",
            "sha256:manifest-remote-hello-v1",
        );

        let rendered = run_args([
            "observe-delete".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror/hello.txt").display().to_string(),
        ])
        .expect("run observe-delete on bound file");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-observe-delete/ops_observe_delete_bound_file.txt"
            )
        );
    }

    #[test]
    fn observe_delete_local_only_file_renders_noop_cleanup_report() {
        let temp_dir = unique_temp_dir("ops-observe-delete-local-only-file");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_local_only_file(
            &mut db,
            "tmp:demo:00000000000000000001",
            InodeId(1),
            "draft.txt",
            "sha256:draft-v1",
        );

        let rendered = run_args([
            "observe-delete".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror/draft.txt").display().to_string(),
        ])
        .expect("run observe-delete on local-only file");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-observe-delete/ops_observe_delete_local_only_file.txt"
            )
        );
    }

    #[test]
    fn observe_move_bound_file_renders_rename_report() {
        let temp_dir = unique_temp_dir("ops-observe-move-bound-file");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_directory(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(9),
            Some(InodeId(1)),
            "archive",
        );
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            "hello.txt",
            "sha256:remote-hello-v1",
            "sha256:manifest-remote-hello-v1",
        );
        fs::create_dir_all(temp_dir.join("mirror/archive")).expect("create archive dir");
        fs::write(temp_dir.join("mirror/archive/hello.txt"), b"hello v1\n")
            .expect("write moved bound file");

        let rendered = run_args([
            "observe-move".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--from".to_owned(),
            temp_dir.join("mirror/hello.txt").display().to_string(),
            "--to".to_owned(),
            temp_dir
                .join("mirror/archive/hello.txt")
                .display()
                .to_string(),
        ])
        .expect("run observe-move on bound file");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-observe-move/ops_observe_move_bound_file.txt"
            )
        );
    }

    #[test]
    fn observe_move_local_only_directory_preserves_identity() {
        let temp_dir = unique_temp_dir("ops-observe-move-local-only-dir");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_directory(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(9),
            Some(InodeId(1)),
            "archive",
        );
        seed_local_only_directory_with_child(
            &mut db,
            "tmp:demo:00000000000000000001",
            "tmp:demo:00000000000000000002",
            InodeId(1),
            "drafts",
            "note.txt",
        );
        fs::create_dir_all(temp_dir.join("mirror/archive/drafts"))
            .expect("create moved local-only dir");

        let rendered = run_args([
            "observe-move".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--from".to_owned(),
            temp_dir.join("mirror/drafts").display().to_string(),
            "--to".to_owned(),
            temp_dir.join("mirror/archive/drafts").display().to_string(),
        ])
        .expect("run observe-move on local-only directory");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-observe-move/ops_observe_move_local_only_directory.txt"
            )
        );
    }

    #[test]
    fn observe_move_rejects_bound_destination_local_only_parent() {
        let temp_dir = unique_temp_dir("ops-observe-move-bound-into-local-only");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            "hello.txt",
            "sha256:remote-hello-v1",
            "sha256:manifest-remote-hello-v1",
        );
        seed_local_only_directory_with_child(
            &mut db,
            "tmp:demo:00000000000000000001",
            "tmp:demo:00000000000000000002",
            InodeId(1),
            "drafts",
            "note.txt",
        );
        fs::create_dir_all(temp_dir.join("mirror/drafts")).expect("create local-only target dir");
        fs::write(temp_dir.join("mirror/drafts/hello.txt"), b"hello v1\n")
            .expect("write moved file under local-only dir");

        let error = run_args([
            "observe-move".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--from".to_owned(),
            temp_dir.join("mirror/hello.txt").display().to_string(),
            "--to".to_owned(),
            temp_dir
                .join("mirror/drafts/hello.txt")
                .display()
                .to_string(),
        ])
        .expect_err("bound move into local-only parent should fail");

        assert!(
            error
                .to_string()
                .contains("bound source requires a bound destination parent"),
            "{error:#}"
        );
    }

    #[test]
    fn observe_move_rejects_occupied_target_path() {
        let temp_dir = unique_temp_dir("ops-observe-move-occupied");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_directory(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(9),
            Some(InodeId(1)),
            "archive",
        );
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            "hello.txt",
            "sha256:remote-hello-v1",
            "sha256:manifest-remote-hello-v1",
        );
        seed_local_only_file(
            &mut db,
            "tmp:demo:00000000000000000003",
            InodeId(9),
            "hello.txt",
            "sha256:other-v1",
        );
        fs::create_dir_all(temp_dir.join("mirror/archive")).expect("create archive dir");
        fs::write(temp_dir.join("mirror/archive/hello.txt"), b"hello v1\n")
            .expect("write occupied target file");

        let error = run_args([
            "observe-move".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--from".to_owned(),
            temp_dir.join("mirror/hello.txt").display().to_string(),
            "--to".to_owned(),
            temp_dir
                .join("mirror/archive/hello.txt")
                .display()
                .to_string(),
        ])
        .expect_err("occupied target should fail");

        assert!(
            error.to_string().contains("target is already occupied"),
            "{error:#}"
        );
    }

    #[test]
    fn observe_subtree_renders_bound_edit_and_nested_local_only_create_report() {
        let temp_dir = unique_temp_dir("ops-observe-subtree");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            "hello.txt",
            "sha256:remote-hello-v1",
            "sha256:manifest-remote-hello-v1",
        );

        fs::create_dir_all(temp_dir.join("mirror/drafts")).expect("create local-only dir");
        fs::write(
            temp_dir.join("mirror/hello.txt"),
            b"hello from subtree edit\n",
        )
        .expect("write bound file edit");
        fs::write(temp_dir.join("mirror/drafts/note.txt"), b"draft note\n")
            .expect("write nested local-only file");

        let rendered = run_args([
            "observe-subtree".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror").display().to_string(),
        ])
        .expect("run observe-subtree");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-observe-subtree/ops_observe_subtree_bound_edit_and_nested_local_only_create.txt"
            )
        );
    }

    #[test]
    fn observe_subtree_renders_inferred_bound_file_move_report() {
        let temp_dir = unique_temp_dir("ops-observe-subtree-bound-move");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        let file_digest = sha256_digest(b"hello v1\n");
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            "hello.txt",
            &file_digest,
            &format!("manifest:{file_digest}"),
        );

        fs::write(temp_dir.join("mirror/renamed.txt"), b"hello v1\n")
            .expect("write moved bound file");

        let rendered = run_args([
            "observe-subtree".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror").display().to_string(),
        ])
        .expect("run observe-subtree");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-observe-subtree/ops_observe_subtree_inferred_bound_file_move.txt"
            )
        );
    }

    #[test]
    fn observe_subtree_renders_inferred_local_only_file_move_report() {
        let temp_dir = unique_temp_dir("ops-observe-subtree-local-only-move");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        let file_digest = sha256_digest(b"draft v1\n");
        seed_local_only_file(
            &mut db,
            "tmp:demo:00000000000000000001",
            InodeId(1),
            "draft.txt",
            &file_digest,
        );

        fs::write(temp_dir.join("mirror/renamed.txt"), b"draft v1\n")
            .expect("write moved local-only file");

        let rendered = run_args([
            "observe-subtree".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror").display().to_string(),
        ])
        .expect("run observe-subtree");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-observe-subtree/ops_observe_subtree_inferred_local_only_file_move.txt"
            )
        );
    }

    #[test]
    fn observe_subtree_renders_inferred_bound_directory_move_report() {
        let temp_dir = unique_temp_dir("ops-observe-subtree-bound-dir-move");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_directory(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(3),
            Some(InodeId(1)),
            "archive",
        );
        seed_bound_directory(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            Some(InodeId(1)),
            "docs",
        );
        let file_digest = sha256_digest(b"note v1\n");
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(4),
            "note.txt",
            &file_digest,
            &format!("manifest:{file_digest}"),
        );
        db.planner_transaction("reparent-note-under-docs", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: NamespaceId::from("demo"),
                inode_id: InodeId(4),
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(1),
                revision_no: RevisionNo(1),
                content_digest: Some(file_digest.clone()),
                content_manifest_digest: Some(format!("manifest:{file_digest}")),
                parent_inode_id: Some(InodeId(2)),
                display_name: "note.txt".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: NamespaceId::from("demo"),
                inode_id: InodeId(4),
                inode_kind: InodeKind::File,
                content_digest: Some(file_digest.clone()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "note.txt".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: NamespaceId::from("demo"),
                inode_id: InodeId(4),
                inode_kind: InodeKind::File,
                synced_seq: ChangeSeq(1),
                revision_no: RevisionNo(1),
                content_digest: Some(file_digest.clone()),
                content_manifest_digest: Some(format!("manifest:{file_digest}")),
                parent_inode_id: Some(InodeId(2)),
                display_name: "note.txt".to_owned(),
            })?;
            Ok(())
        })
        .expect("seed note under docs");

        fs::create_dir_all(temp_dir.join("mirror/archive/docs")).expect("create moved docs dir");
        fs::write(temp_dir.join("mirror/archive/docs/note.txt"), b"note v1\n")
            .expect("write moved note file");

        let rendered = run_args([
            "observe-subtree".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror").display().to_string(),
        ])
        .expect("run observe-subtree");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-observe-subtree/ops_observe_subtree_inferred_bound_directory_move.txt"
            )
        );
    }

    #[test]
    fn observe_subtree_renders_inferred_local_only_directory_move_report() {
        let temp_dir = unique_temp_dir("ops-observe-subtree-local-only-dir-move");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_directory(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(3),
            Some(InodeId(1)),
            "archive",
        );
        let note_digest = sha256_digest(b"note v1\n");
        seed_local_only_directory_with_child_digest(
            &mut db,
            "tmp:demo:00000000000000000001",
            "tmp:demo:00000000000000000002",
            InodeId(1),
            "drafts",
            "note.txt",
            &note_digest,
        );

        fs::create_dir_all(temp_dir.join("mirror/archive/drafts"))
            .expect("create moved drafts dir");
        fs::write(
            temp_dir.join("mirror/archive/drafts/note.txt"),
            b"note v1\n",
        )
        .expect("write moved local-only note");

        let rendered = run_args([
            "observe-subtree".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror").display().to_string(),
        ])
        .expect("run observe-subtree");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-observe-subtree/ops_observe_subtree_inferred_local_only_directory_move.txt"
            )
        );
    }

    #[test]
    fn observe_subtree_renders_same_path_bound_file_replacement_report() {
        let temp_dir = unique_temp_dir("ops-observe-subtree-bound-file-replacement");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        let file_digest = sha256_digest(b"note v1\n");
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            "notes",
            &file_digest,
            &format!("manifest:{file_digest}"),
        );

        fs::create_dir_all(temp_dir.join("mirror/notes")).expect("create replacement directory");
        fs::write(temp_dir.join("mirror/notes/child.txt"), b"child v1\n")
            .expect("write replacement child file");

        let rendered = run_args([
            "observe-subtree".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror").display().to_string(),
        ])
        .expect("run observe-subtree");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-observe-subtree/ops_observe_subtree_same_path_bound_file_replacement.txt"
            )
        );
    }

    #[cfg(unix)]
    #[test]
    fn observe_subtree_renders_skipped_unsupported_entry_report() {
        let temp_dir = unique_temp_dir("ops-observe-subtree-skipped-unsupported");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));

        fs::write(temp_dir.join("mirror/alpha.txt"), b"alpha v1\n").expect("write regular file");
        let symlink_target = temp_dir.join("outside-target.txt");
        fs::write(&symlink_target, b"target\n").expect("write symlink target");
        std::os::unix::fs::symlink(&symlink_target, temp_dir.join("mirror/link.txt"))
            .expect("create symlink");

        let rendered = run_args([
            "observe-subtree".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror").display().to_string(),
        ])
        .expect("run observe-subtree");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-observe-subtree/ops_observe_subtree_skipped_unsupported_entry.txt"
            )
        );
    }

    #[test]
    fn observe_subtree_rejects_ambiguous_same_digest_file_pairing() {
        let temp_dir = unique_temp_dir("ops-observe-subtree-ambiguous-move");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        let file_digest = sha256_digest(b"hello v1\n");
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            "hello.txt",
            &file_digest,
            &format!("manifest:{file_digest}"),
        );

        fs::write(temp_dir.join("mirror/renamed-a.txt"), b"hello v1\n")
            .expect("write first ambiguous file");
        fs::write(temp_dir.join("mirror/renamed-b.txt"), b"hello v1\n")
            .expect("write second ambiguous file");

        let error = run_args([
            "observe-subtree".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror").display().to_string(),
        ])
        .expect_err("ambiguous same-digest pairing should fail");

        assert!(
            error.to_string().contains("move pairing is ambiguous"),
            "{error:#}"
        );
    }

    #[test]
    fn observe_subtree_rejects_ambiguous_exact_directory_pairing() {
        let temp_dir = unique_temp_dir("ops-observe-subtree-ambiguous-dir-move");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let mut db = SqliteStateDb::open(&db_path).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        let note_digest = sha256_digest(b"note v1\n");
        seed_local_only_directory_with_child_digest(
            &mut db,
            "tmp:demo:00000000000000000001",
            "tmp:demo:00000000000000000002",
            InodeId(1),
            "drafts",
            "note.txt",
            &note_digest,
        );

        fs::create_dir_all(temp_dir.join("mirror/archive-a/drafts"))
            .expect("create first ambiguous directory target");
        fs::create_dir_all(temp_dir.join("mirror/archive-b/drafts"))
            .expect("create second ambiguous directory target");
        fs::write(
            temp_dir.join("mirror/archive-a/drafts/note.txt"),
            b"note v1\n",
        )
        .expect("write first ambiguous note");
        fs::write(
            temp_dir.join("mirror/archive-b/drafts/note.txt"),
            b"note v1\n",
        )
        .expect("write second ambiguous note");

        let error = run_args([
            "observe-subtree".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror").display().to_string(),
        ])
        .expect_err("ambiguous directory pairing should fail");

        assert_eq!(
            format!("{}\n", error),
            include_str!(
                "../../../tests/snapshots/ops-observe-subtree/ops_observe_subtree_ambiguous_directory_pairing_error.txt"
            )
        );
    }

    #[test]
    fn sync_once_returns_no_work_for_idle_client() {
        let temp_dir = unique_temp_dir("ops-sync-once-idle");
        let config_path = write_local_fs_config(&temp_dir);
        let _db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client db");

        let rendered = run_args([
            "sync-once".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("run sync-once on idle client");

        assert_eq!(
            rendered,
            include_str!("../../../tests/snapshots/ops-sync-once/ops_sync_once_no_work.txt")
        );
    }

    #[test]
    fn sync_once_can_progress_non_dispatch_materialization_action() {
        let temp_dir = unique_temp_dir("ops-sync-once-materialize");
        let config_path = write_local_fs_config(&temp_dir);
        run_args([
            "bootstrap-namespace".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("bootstrap namespace before import");
        run_args([
            "import-remote-observations".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("import remote observations before sync-once");

        let rendered = run_args([
            "sync-once".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("run sync-once after import");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-sync-once/ops_sync_once_materialize_remote_dir.txt"
            )
        );
    }

    #[test]
    fn sync_once_can_commit_local_edit_through_real_client_server_path() {
        let temp_dir = unique_temp_dir("ops-sync-once-local-edit");
        let config_path = write_local_fs_config(&temp_dir);
        let snapshot = seed_authoritative_single_file_snapshot(
            &temp_dir.join("store"),
            &NamespaceId::from("demo"),
            "hello.txt",
            b"v1\n",
        );
        let mut db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            "hello.txt",
            &snapshot.file_digest,
            &snapshot.manifest_digest,
        );

        fs::write(
            temp_dir.join("mirror/hello.txt"),
            b"hello from local edit\n",
        )
        .expect("write local edit file");
        run_args([
            "observe-local".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror/hello.txt").display().to_string(),
        ])
        .expect("observe local edit");

        let rendered = run_args([
            "sync-once".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("run sync-once for local edit");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-sync-once/ops_sync_once_request_committed_upload_local_edit.txt"
            )
        );
    }

    #[test]
    fn sync_once_reacquires_expired_lease_for_real_local_edit_flow() {
        let temp_dir = unique_temp_dir("ops-sync-once-expired-lease");
        let config_path = write_local_fs_config_with_params(&temp_dir, 70_000, "writer-a");
        let snapshot = seed_authoritative_single_file_snapshot(
            &temp_dir.join("store"),
            &NamespaceId::from("demo"),
            "hello.txt",
            b"v1\n",
        );
        let mut db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            "hello.txt",
            &snapshot.file_digest,
            &snapshot.manifest_digest,
        );

        fs::write(
            temp_dir.join("mirror/hello.txt"),
            b"hello from local edit\n",
        )
        .expect("write local edit file");
        run_args([
            "observe-local".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror/hello.txt").display().to_string(),
        ])
        .expect("observe local edit");

        let rendered = run_args([
            "sync-once".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("run sync-once after lease expiry");
        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-sync-once/ops_sync_once_request_committed_upload_local_edit.txt"
            )
        );

        let namespace_state = run_args([
            "show-namespace-state".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("show namespace state after reacquire");
        assert!(namespace_state.contains("active_fence_token: 1"));
        assert!(namespace_state.contains("holder_id: writer-a"));
        assert!(namespace_state.contains("lease_expires_at_ms: 130000"));
    }

    #[test]
    fn sync_once_can_commit_bound_delete_through_real_client_server_path() {
        let temp_dir = unique_temp_dir("ops-sync-once-delete");
        let config_path = write_local_fs_config(&temp_dir);
        let snapshot = seed_authoritative_single_file_snapshot(
            &temp_dir.join("store"),
            &NamespaceId::from("demo"),
            "hello.txt",
            b"v1\n",
        );
        let mut db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            "hello.txt",
            &snapshot.file_digest,
            &snapshot.manifest_digest,
        );

        run_args([
            "observe-delete".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror/hello.txt").display().to_string(),
        ])
        .expect("observe delete");

        let rendered = run_args([
            "sync-once".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("run sync-once for bound delete");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-sync-once/ops_sync_once_request_committed_delete_file.txt"
            )
        );
    }

    #[test]
    fn sync_until_idle_returns_no_work_for_idle_client() {
        let temp_dir = unique_temp_dir("ops-sync-until-idle-idle");
        let config_path = write_local_fs_config(&temp_dir);
        let _db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client db");

        let rendered = run_args([
            "sync-until-idle".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("run sync-until-idle on idle client");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-sync-until-idle/ops_sync_until_idle_no_work.txt"
            )
        );
    }

    #[test]
    fn sync_until_idle_reaches_idle_after_real_local_edit_flow() {
        let temp_dir = unique_temp_dir("ops-sync-until-idle-local-edit");
        let config_path = write_local_fs_config(&temp_dir);
        let snapshot = seed_authoritative_single_file_snapshot(
            &temp_dir.join("store"),
            &NamespaceId::from("demo"),
            "hello.txt",
            b"v1\n",
        );
        let mut db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            "hello.txt",
            &snapshot.file_digest,
            &snapshot.manifest_digest,
        );

        fs::write(
            temp_dir.join("mirror/hello.txt"),
            b"hello from local edit\n",
        )
        .expect("write local edit file");
        run_args([
            "observe-local".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror/hello.txt").display().to_string(),
        ])
        .expect("observe local edit");

        let rendered = run_args([
            "sync-until-idle".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("run sync-until-idle for local edit");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-sync-until-idle/ops_sync_until_idle_request_committed_upload_local_edit.txt"
            )
        );
    }

    #[test]
    fn sync_until_idle_reaches_idle_after_inferred_bound_rename() {
        let temp_dir = unique_temp_dir("ops-sync-until-idle-bound-rename");
        let config_path = write_local_fs_config(&temp_dir);
        let snapshot = seed_authoritative_single_file_snapshot(
            &temp_dir.join("store"),
            &NamespaceId::from("demo"),
            "hello.txt",
            b"v1\n",
        );
        let mut db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            "hello.txt",
            &snapshot.file_digest,
            &snapshot.manifest_digest,
        );

        fs::write(temp_dir.join("mirror/renamed.txt"), b"v1\n").expect("write renamed bound file");
        run_args([
            "observe-subtree".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror").display().to_string(),
        ])
        .expect("observe subtree rename");

        let rendered = run_args([
            "sync-until-idle".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("run sync-until-idle for inferred bound rename");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-sync-until-idle/ops_sync_until_idle_request_committed_rename.txt"
            )
        );
    }

    #[test]
    fn sync_until_idle_reaches_idle_after_inferred_bound_directory_rename() {
        let temp_dir = unique_temp_dir("ops-sync-until-idle-bound-dir-rename");
        let config_path = write_local_fs_config(&temp_dir);
        let snapshot = seed_authoritative_directory_snapshot(
            &temp_dir.join("store"),
            &NamespaceId::from("demo"),
        );
        let mut db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_directory(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(3),
            Some(InodeId(1)),
            "archive",
        );
        seed_bound_directory(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            Some(InodeId(1)),
            "docs",
        );
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(4),
            "note.txt",
            &snapshot.file_digest,
            &snapshot.manifest_digest,
        );
        db.planner_transaction("reparent-note-under-docs", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: NamespaceId::from("demo"),
                inode_id: InodeId(4),
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(1),
                revision_no: RevisionNo(1),
                content_digest: Some(snapshot.file_digest.clone()),
                content_manifest_digest: Some(snapshot.manifest_digest.clone()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "note.txt".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: NamespaceId::from("demo"),
                inode_id: InodeId(4),
                inode_kind: InodeKind::File,
                content_digest: Some(snapshot.file_digest.clone()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "note.txt".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: NamespaceId::from("demo"),
                inode_id: InodeId(4),
                inode_kind: InodeKind::File,
                synced_seq: ChangeSeq(1),
                revision_no: RevisionNo(1),
                content_digest: Some(snapshot.file_digest.clone()),
                content_manifest_digest: Some(snapshot.manifest_digest.clone()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "note.txt".to_owned(),
            })?;
            Ok(())
        })
        .expect("seed note under docs");

        fs::create_dir_all(temp_dir.join("mirror/archive/docs")).expect("create moved docs dir");
        fs::write(temp_dir.join("mirror/archive/docs/note.txt"), b"note v1\n")
            .expect("write moved note");
        run_args([
            "observe-subtree".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror").display().to_string(),
        ])
        .expect("observe subtree directory rename");

        let rendered = run_args([
            "sync-until-idle".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("run sync-until-idle for inferred bound directory rename");

        assert_eq!(
            rendered,
            include_str!(
                "../../../tests/snapshots/ops-sync-until-idle/ops_sync_until_idle_request_committed_directory_rename.txt"
            )
        );
    }

    #[test]
    fn sync_until_idle_reaches_idle_after_same_path_bound_file_to_directory_replacement() {
        let temp_dir = unique_temp_dir("ops-sync-until-idle-bound-file-to-dir");
        let config_path = write_local_fs_config(&temp_dir);
        let snapshot = seed_authoritative_single_file_snapshot(
            &temp_dir.join("store"),
            &NamespaceId::from("demo"),
            "notes",
            b"notes v1\n",
        );
        let mut db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            "notes",
            &snapshot.file_digest,
            &snapshot.manifest_digest,
        );

        fs::create_dir_all(temp_dir.join("mirror/notes")).expect("create replacement directory");
        run_args([
            "observe-subtree".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror").display().to_string(),
        ])
        .expect("observe subtree replacement");

        let rendered = run_args([
            "sync-until-idle".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("run sync-until-idle for same-path file replacement");

        assert!(rendered.contains("final_outcome=no_work"), "{rendered}");
        assert!(rendered.contains("action_kind: delete_file"), "{rendered}");
        assert!(
            rendered.contains("action_kind: create_remote_dir"),
            "{rendered}"
        );

        let db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("reopen client db");
        let summary = db
            .load_namespace_state_summary(&NamespaceId::from("demo"))
            .expect("load namespace summary");
        let parent_links = db
            .load_local_only_parent_links_for_namespace(&NamespaceId::from("demo"))
            .expect("load parent links");
        let path_index = NamespacePathIndex::build(&summary, &parent_links);
        assert!(path_index.bound_file_matches("notes").is_empty());
        assert_eq!(path_index.bound_dir_matches("notes").len(), 1);
        assert!(summary.local_only_state.is_empty());
        assert!(summary.local_only_planned_actions.is_empty());
    }

    #[test]
    fn sync_until_idle_reaches_idle_after_same_path_bound_directory_to_file_replacement() {
        let temp_dir = unique_temp_dir("ops-sync-until-idle-bound-dir-to-file");
        let config_path = write_local_fs_config(&temp_dir);
        let snapshot = seed_authoritative_directory_snapshot(
            &temp_dir.join("store"),
            &NamespaceId::from("demo"),
        );
        let mut db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_directory(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            Some(InodeId(1)),
            "docs",
        );
        seed_bound_directory(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(3),
            Some(InodeId(1)),
            "archive",
        );
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(4),
            "note.txt",
            &snapshot.file_digest,
            &snapshot.manifest_digest,
        );
        db.planner_transaction("reparent-note-under-docs", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: NamespaceId::from("demo"),
                inode_id: InodeId(4),
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(1),
                revision_no: RevisionNo(1),
                content_digest: Some(snapshot.file_digest.clone()),
                content_manifest_digest: Some(snapshot.manifest_digest.clone()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "note.txt".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: NamespaceId::from("demo"),
                inode_id: InodeId(4),
                inode_kind: InodeKind::File,
                content_digest: Some(snapshot.file_digest.clone()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "note.txt".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: NamespaceId::from("demo"),
                inode_id: InodeId(4),
                inode_kind: InodeKind::File,
                synced_seq: ChangeSeq(1),
                revision_no: RevisionNo(1),
                content_digest: Some(snapshot.file_digest.clone()),
                content_manifest_digest: Some(snapshot.manifest_digest.clone()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "note.txt".to_owned(),
            })?;
            Ok(())
        })
        .expect("reparent note under docs");

        fs::write(temp_dir.join("mirror/docs"), b"replacement file\n")
            .expect("write replacement file");
        run_args([
            "observe-subtree".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror").display().to_string(),
        ])
        .expect("observe subtree replacement");

        let rendered = run_args([
            "sync-until-idle".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("run sync-until-idle for same-path directory replacement");

        assert!(rendered.contains("final_outcome=no_work"), "{rendered}");
        assert!(
            rendered.contains("action_kind: delete_subtree"),
            "{rendered}"
        );
        assert!(
            rendered.contains("action_kind: upload_local_create"),
            "{rendered}"
        );

        let db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("reopen client db");
        let summary = db
            .load_namespace_state_summary(&NamespaceId::from("demo"))
            .expect("load namespace summary");
        let parent_links = db
            .load_local_only_parent_links_for_namespace(&NamespaceId::from("demo"))
            .expect("load parent links");
        let path_index = NamespacePathIndex::build(&summary, &parent_links);
        assert!(path_index.bound_dir_matches("docs").is_empty());
        assert_eq!(path_index.bound_file_matches("docs").len(), 1);
        assert!(path_index.bound_file_matches("docs/note.txt").is_empty());
        assert!(summary.local_only_state.is_empty());
        assert!(summary.local_only_planned_actions.is_empty());
    }

    #[test]
    fn sync_until_idle_fails_on_max_steps() {
        let temp_dir = unique_temp_dir("ops-sync-until-idle-max-steps");
        let config_path = write_local_fs_config(&temp_dir);
        let snapshot = seed_authoritative_single_file_snapshot(
            &temp_dir.join("store"),
            &NamespaceId::from("demo"),
            "hello.txt",
            b"v1\n",
        );
        let mut db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client db");
        seed_bound_root_directory(&mut db, NamespaceId::from("demo"));
        seed_bound_file(
            &mut db,
            NamespaceId::from("demo"),
            InodeId(2),
            "hello.txt",
            &snapshot.file_digest,
            &snapshot.manifest_digest,
        );

        fs::write(
            temp_dir.join("mirror/hello.txt"),
            b"hello from local edit\n",
        )
        .expect("write local edit file");
        run_args([
            "observe-local".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--path".to_owned(),
            temp_dir.join("mirror/hello.txt").display().to_string(),
        ])
        .expect("observe local edit");

        let error = run_args([
            "sync-until-idle".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
            "--max-steps".to_owned(),
            "1".to_owned(),
        ])
        .expect_err("sync-until-idle should fail on max steps");

        assert!(
            error
                .to_string()
                .contains("reached max steps without becoming idle"),
            "{error:#}"
        );
    }

    fn load_config(contents: &str) -> OpsConfig {
        toml::from_str(contents).expect("parse config")
    }

    fn write_local_fs_config(temp_dir: &Path) -> PathBuf {
        write_local_fs_config_with_params(temp_dir, 1_000, "writer-a")
    }

    fn write_local_fs_config_with_params(temp_dir: &Path, now_ms: u64, writer_id: &str) -> PathBuf {
        let config_path = temp_dir.join("loondb-demo.toml");
        fs::write(
            &config_path,
            format!(
                r#"[object_store]
kind = "local-fs"
root = "{}"
key_prefix = "tenant-a"

[client]
state_db_path = "{}"
mirror_root = "{}"

[server]
writer_id = "{writer_id}"
writer_version = "loon-ops-test"
lease_duration_ms = 60000

[ops]
now_ms = {now_ms}
"#,
                temp_dir.join("store").display(),
                temp_dir.join("client.sqlite3").display(),
                temp_dir.join("mirror").display(),
            ),
        )
        .expect("write config");
        fs::create_dir_all(temp_dir.join("mirror")).expect("create mirror root");
        config_path
    }

    fn seed_bound_root_directory(db: &mut SqliteStateDb, namespace_id: NamespaceId) {
        db.planner_transaction("seed-bound-root-directory", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(1),
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(1),
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: None,
                display_name: String::new(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(1),
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: None,
                display_name: String::new(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id,
                inode_id: InodeId(1),
                inode_kind: InodeKind::Dir,
                synced_seq: ChangeSeq(1),
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: None,
                display_name: String::new(),
            })?;
            Ok(())
        })
        .expect("seed bound root directory");
    }

    fn seed_bound_file(
        db: &mut SqliteStateDb,
        namespace_id: NamespaceId,
        inode_id: InodeId,
        display_name: &str,
        content_digest: &str,
        content_manifest_digest: &str,
    ) {
        db.planner_transaction("seed-bound-file", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id,
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(1),
                revision_no: RevisionNo(1),
                content_digest: Some(content_digest.to_owned()),
                content_manifest_digest: Some(content_manifest_digest.to_owned()),
                parent_inode_id: Some(InodeId(1)),
                display_name: display_name.to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id,
                inode_kind: InodeKind::File,
                content_digest: Some(content_digest.to_owned()),
                parent_inode_id: Some(InodeId(1)),
                display_name: display_name.to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id,
                inode_id,
                inode_kind: InodeKind::File,
                synced_seq: ChangeSeq(1),
                revision_no: RevisionNo(1),
                content_digest: Some(content_digest.to_owned()),
                content_manifest_digest: Some(content_manifest_digest.to_owned()),
                parent_inode_id: Some(InodeId(1)),
                display_name: display_name.to_owned(),
            })?;
            Ok(())
        })
        .expect("seed bound file");
    }

    fn seed_bound_directory(
        db: &mut SqliteStateDb,
        namespace_id: NamespaceId,
        inode_id: InodeId,
        parent_inode_id: Option<InodeId>,
        display_name: &str,
    ) {
        db.planner_transaction("seed-bound-directory", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id,
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(1),
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id,
                display_name: display_name.to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id,
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id,
                display_name: display_name.to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id,
                inode_id,
                inode_kind: InodeKind::Dir,
                synced_seq: ChangeSeq(1),
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id,
                display_name: display_name.to_owned(),
            })?;
            Ok(())
        })
        .expect("seed bound directory");
    }

    fn seed_local_only_file(
        db: &mut SqliteStateDb,
        client_file_id: &str,
        parent_inode_id: InodeId,
        display_name: &str,
        content_digest: &str,
    ) {
        db.planner_transaction("seed-local-only-file", |tx| {
            tx.upsert_local_only_file(&LocalOnlyFileStateRow {
                client_file_id: client_file_id.into(),
                namespace_id: NamespaceId::from("demo"),
                inode_kind: InodeKind::File,
                parent_inode_id: Some(parent_inode_id),
                display_name: display_name.to_owned(),
                content_digest: Some(content_digest.to_owned()),
                exists_on_disk: true,
                dirty: true,
                last_local_change_ms: 1_000,
            })?;
            tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
                client_file_id: client_file_id.into(),
                namespace_id: NamespaceId::from("demo"),
                decision: "upload_local_create".to_owned(),
                reason: "local_only_file_without_remote_identity".to_owned(),
                created_at_ms: 1_000,
            })?;
            Ok(())
        })
        .expect("seed local-only file");
    }

    fn seed_local_only_directory_with_child(
        db: &mut SqliteStateDb,
        root_client_file_id: &str,
        child_client_file_id: &str,
        parent_inode_id: InodeId,
        root_display_name: &str,
        child_display_name: &str,
    ) {
        seed_local_only_directory_with_child_digest(
            db,
            root_client_file_id,
            child_client_file_id,
            parent_inode_id,
            root_display_name,
            child_display_name,
            "sha256:note-v1",
        );
    }

    fn seed_local_only_directory_with_child_digest(
        db: &mut SqliteStateDb,
        root_client_file_id: &str,
        child_client_file_id: &str,
        parent_inode_id: InodeId,
        root_display_name: &str,
        child_display_name: &str,
        child_content_digest: &str,
    ) {
        db.planner_transaction("seed-local-only-directory-with-child", |tx| {
            tx.upsert_local_only_file(&LocalOnlyFileStateRow {
                client_file_id: root_client_file_id.into(),
                namespace_id: NamespaceId::from("demo"),
                inode_kind: InodeKind::Dir,
                parent_inode_id: Some(parent_inode_id),
                display_name: root_display_name.to_owned(),
                content_digest: None,
                exists_on_disk: true,
                dirty: true,
                last_local_change_ms: 1_000,
            })?;
            tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
                client_file_id: root_client_file_id.into(),
                namespace_id: NamespaceId::from("demo"),
                decision: "create_remote_dir".to_owned(),
                reason: "local_only_directory_without_remote_identity".to_owned(),
                created_at_ms: 1_000,
            })?;
            tx.upsert_local_only_file(&LocalOnlyFileStateRow {
                client_file_id: child_client_file_id.into(),
                namespace_id: NamespaceId::from("demo"),
                inode_kind: InodeKind::File,
                parent_inode_id: None,
                display_name: child_display_name.to_owned(),
                content_digest: Some(child_content_digest.to_owned()),
                exists_on_disk: true,
                dirty: true,
                last_local_change_ms: 1_000,
            })?;
            tx.upsert_local_only_parent_link(
                &child_client_file_id.into(),
                &root_client_file_id.into(),
            )?;
            tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
                client_file_id: child_client_file_id.into(),
                namespace_id: NamespaceId::from("demo"),
                decision: "upload_local_create".to_owned(),
                reason: "local_only_file_without_remote_identity".to_owned(),
                created_at_ms: 1_000,
            })?;
            Ok(())
        })
        .expect("seed local-only directory subtree");
    }

    #[derive(Debug, Clone)]
    struct SeededAuthoritativeSnapshot {
        file_digest: String,
        manifest_digest: String,
    }

    fn seed_authoritative_single_file_snapshot(
        store_root: &Path,
        namespace_id: &NamespaceId,
        display_name: &str,
        contents: &[u8],
    ) -> SeededAuthoritativeSnapshot {
        let store = ConfiguredObjectStore::local_fs(store_root, Some("tenant-a"))
            .expect("create configured store");
        let source_path = store_root.join("seed-source.txt");
        fs::write(&source_path, contents).expect("write seed source");
        let uploaded = upload_small_file_from_path(&store, namespace_id, &source_path)
            .expect("upload seed content");

        let metadata = MetadataState {
            inodes: vec![
                InodeRecord {
                    inode_id: InodeId(1),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(0),
                },
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(1),
                },
            ],
            direntries: vec![DirentryRecord {
                parent_inode_id: InodeId(1),
                name_key: display_name.to_owned(),
                display_name: display_name.to_owned(),
                child_inode_id: InodeId(2),
                bind_seq: ChangeSeq(1),
                bind_op_index: 0,
            }],
            revisions: vec![RevisionRecord {
                inode_id: InodeId(2),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(1),
                revision_op_index: 0,
                content_manifest_digest: uploaded.content_manifest_digest.clone(),
            }],
            subtree_tombstones: Vec::new(),
        };
        let head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(1),
            active_fence_token: FenceToken(0),
            next_inode_id: InodeId(3),
            snapshot_hint_seq: Some(ChangeSeq(1)),
            retention_floor_seq: ChangeSeq(0),
        };
        let lease = LeaseState {
            namespace_id: namespace_id.clone(),
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(0),
            lease_expires_at_ms: 60_000,
        };
        let checkpoint =
            prepare_checkpoint(&head, &metadata, "loon-ops-test").expect("prepare checkpoint");
        store
            .put_overwrite(
                &checkpoint.manifest.object_key,
                &checkpoint.manifest.encoded_bytes,
            )
            .expect("write checkpoint manifest");
        for segment in &checkpoint.segments {
            store
                .put_overwrite(&segment.object_key, &segment.encoded_bytes)
                .expect("write checkpoint segment");
        }
        let head_envelope =
            HeadStateEnvelope::from_state(ControlObjectKind::NamespaceHead, "loon-ops-test", head)
                .expect("encode head envelope");
        let lease_envelope = LeaseStateEnvelope::from_state(
            ControlObjectKind::NamespaceLease,
            "loon-ops-test",
            lease,
        )
        .expect("encode lease envelope");
        store
            .put_overwrite(
                &namespace_head(namespace_id.as_str()),
                &serde_json::to_vec(&head_envelope).expect("serialize head envelope"),
            )
            .expect("write head object");
        store
            .put_overwrite(
                &namespace_lease(namespace_id.as_str()),
                &serde_json::to_vec(&lease_envelope).expect("serialize lease envelope"),
            )
            .expect("write lease object");

        SeededAuthoritativeSnapshot {
            file_digest: uploaded.file_digest_sha256,
            manifest_digest: uploaded.content_manifest_digest,
        }
    }

    fn seed_authoritative_directory_snapshot(
        store_root: &Path,
        namespace_id: &NamespaceId,
    ) -> SeededAuthoritativeSnapshot {
        let store = ConfiguredObjectStore::local_fs(store_root, Some("tenant-a"))
            .expect("create configured store");
        let source_path = store_root.join("seed-note.txt");
        fs::write(&source_path, b"note v1\n").expect("write seed note source");
        let uploaded = upload_small_file_from_path(&store, namespace_id, &source_path)
            .expect("upload seed note content");

        let metadata = MetadataState {
            inodes: vec![
                InodeRecord {
                    inode_id: InodeId(1),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(0),
                },
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: InodeId(3),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: InodeId(4),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(1),
                },
            ],
            direntries: vec![
                DirentryRecord {
                    parent_inode_id: InodeId(1),
                    name_key: "docs".to_owned(),
                    display_name: "docs".to_owned(),
                    child_inode_id: InodeId(2),
                    bind_seq: ChangeSeq(1),
                    bind_op_index: 0,
                },
                DirentryRecord {
                    parent_inode_id: InodeId(1),
                    name_key: "archive".to_owned(),
                    display_name: "archive".to_owned(),
                    child_inode_id: InodeId(3),
                    bind_seq: ChangeSeq(1),
                    bind_op_index: 0,
                },
                DirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "note.txt".to_owned(),
                    display_name: "note.txt".to_owned(),
                    child_inode_id: InodeId(4),
                    bind_seq: ChangeSeq(1),
                    bind_op_index: 0,
                },
            ],
            revisions: vec![RevisionRecord {
                inode_id: InodeId(4),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(1),
                revision_op_index: 0,
                content_manifest_digest: uploaded.content_manifest_digest.clone(),
            }],
            subtree_tombstones: Vec::new(),
        };
        let head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(1),
            active_fence_token: FenceToken(0),
            next_inode_id: InodeId(5),
            snapshot_hint_seq: Some(ChangeSeq(1)),
            retention_floor_seq: ChangeSeq(0),
        };
        let lease = LeaseState {
            namespace_id: namespace_id.clone(),
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(0),
            lease_expires_at_ms: 60_000,
        };
        let checkpoint =
            prepare_checkpoint(&head, &metadata, "loon-ops-test").expect("prepare checkpoint");
        store
            .put_overwrite(
                &checkpoint.manifest.object_key,
                &checkpoint.manifest.encoded_bytes,
            )
            .expect("write checkpoint manifest");
        for segment in &checkpoint.segments {
            store
                .put_overwrite(&segment.object_key, &segment.encoded_bytes)
                .expect("write checkpoint segment");
        }
        let head_envelope =
            HeadStateEnvelope::from_state(ControlObjectKind::NamespaceHead, "loon-ops-test", head)
                .expect("encode head envelope");
        let lease_envelope = LeaseStateEnvelope::from_state(
            ControlObjectKind::NamespaceLease,
            "loon-ops-test",
            lease,
        )
        .expect("encode lease envelope");
        store
            .put_overwrite(
                &namespace_head(namespace_id.as_str()),
                &serde_json::to_vec(&head_envelope).expect("serialize head envelope"),
            )
            .expect("write head object");
        store
            .put_overwrite(
                &namespace_lease(namespace_id.as_str()),
                &serde_json::to_vec(&lease_envelope).expect("serialize lease envelope"),
            )
            .expect("write lease object");

        SeededAuthoritativeSnapshot {
            file_digest: uploaded.file_digest_sha256,
            manifest_digest: uploaded.content_manifest_digest,
        }
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("loondb-ops-{label}-{stamp}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }
}
