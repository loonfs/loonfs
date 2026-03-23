#![forbid(unsafe_code)]

mod import;

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
        Some("smoke") => {
            let parsed = parse_common_args(args, false)?;
            Ok(OpsCommand::Smoke {
                config_path: parsed.config_path,
                namespace_id: parsed.namespace_id,
            })
        }
        Some(other) => bail!("unknown ops subcommand: {other}"),
        None => bail!(
            "usage: ops <bootstrap-namespace|show-namespace-state|show-client-state|import-remote-observations|smoke> --config <path> --namespace <id> [--allow-existing]"
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

#[cfg(test)]
mod tests {
    use super::{run_args, OpsConfig};
    use loon_client::state_db::SqliteStateDb;
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

    fn load_config(contents: &str) -> OpsConfig {
        toml::from_str(contents).expect("parse config")
    }

    fn write_local_fs_config(temp_dir: &Path) -> PathBuf {
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
writer_id = "writer-a"
writer_version = "loon-ops-test"
lease_duration_ms = 60000

[ops]
now_ms = 1000
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
