//! The CLI config file: profiles, defaults, and strict TOML loading.

use crate::error::CliError;
use loonfs_api::NamespaceId;
use loonfs_client::ClientConfig;
use loonfs_objectstore::{SecretString, StoreConfigError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub(crate) use loonfs_objectstore::StoreConfig;

pub(crate) const CONFIG_VERSION: u32 = 1;

/// Environment override for the config file, and the escape hatch a shell
/// session reaches for when the default file is one the CLI will not read.
pub(crate) const CONFIG_PATH_ENV: &str = "LOONFS_CONFIG";

const CONFIG_FILE_NAME: &str = "config.toml";
/// Directory under `$XDG_CONFIG_HOME`.
const XDG_CONFIG_SUBDIR: &str = "loonfs";
/// Directory under `$HOME`, where installs predating XDG support live.
const LEGACY_CONFIG_SUBDIR: &str = ".loonfs";

/// The way past a config file this build will not accept. Every failure to
/// load the resolved file ends with it, because strict decoding is only
/// safe while a rejected file can always be stepped around — otherwise one
/// unknown key bricks every command, `loonfs init` included. The
/// environment variable named here is [`CONFIG_PATH_ENV`].
const CONFIG_REMEDY: &str = "fix that file, or run against a different one with `--config <path>` \
     or `LOONFS_CONFIG=<path>`; `loonfs config path` prints the file in use, and \
     `loonfs init --config <path>` writes a fresh one";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CliConfig {
    pub config_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_profile: Option<String>,
    /// Profiles live in their own `[profiles.<name>]` tables so profile
    /// names can never collide with top-level settings.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub profiles: BTreeMap<String, ProfileConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum ProfileConfig {
    Embedded {
        store: StoreConfig,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default_namespace: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        writer_id: Option<String>,
    },
    Remote {
        server_url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default_namespace: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_token: Option<SecretString>,
        /// PEM bundle of extra certificate authorities to trust, for a
        /// server whose certificate a private CA issued.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ca_cert_path: Option<String>,
    },
}

impl CliConfig {
    pub(crate) fn new() -> Self {
        Self {
            config_version: CONFIG_VERSION,
            default_profile: None,
            profiles: BTreeMap::new(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), CliError> {
        if self.config_version != CONFIG_VERSION {
            return Err(CliError::invalid_config(format!(
                "unsupported `config_version`: expected `{CONFIG_VERSION}`, got `{}`",
                self.config_version
            )));
        }
        if let Some(default_profile) = &self.default_profile {
            require_non_empty("default_profile", default_profile)?;
            if !self.profiles.contains_key(default_profile) {
                return Err(CliError::invalid_config(format!(
                    "`default_profile` points to missing profile `{default_profile}`"
                )));
            }
        }
        for (name, profile) in &self.profiles {
            validate_profile_name(name).map_err(|error| CliError::invalid_config(error.message))?;
            profile.validate(name)?;
        }
        Ok(())
    }

    pub(crate) fn redacted(&self) -> Self {
        CliConfig {
            config_version: self.config_version,
            default_profile: self.default_profile.clone(),
            profiles: self
                .profiles
                .iter()
                .map(|(name, profile)| (name.clone(), profile.redacted()))
                .collect(),
        }
    }
}

impl Default for CliConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl ProfileConfig {
    pub(crate) fn mode_str(&self) -> &'static str {
        match self {
            ProfileConfig::Embedded { .. } => "embedded",
            ProfileConfig::Remote { .. } => "remote",
        }
    }

    pub(crate) fn store_kind_str(&self) -> Option<&'static str> {
        match self {
            ProfileConfig::Embedded { store, .. } => Some(store.kind().as_str()),
            ProfileConfig::Remote { .. } => None,
        }
    }

    pub(crate) fn validate(&self, name: &str) -> Result<(), CliError> {
        match self {
            ProfileConfig::Embedded {
                store,
                default_namespace,
                ..
            } => {
                if let Some(namespace) = default_namespace {
                    validate_default_namespace(
                        &profile_field(name, "default_namespace"),
                        namespace,
                    )?;
                }
                store
                    .validate()
                    .map_err(|error| profile_store_error(name, &error))
            }
            ProfileConfig::Remote {
                server_url,
                default_namespace,
                auth_token,
                ca_cert_path,
            } => {
                if let Some(namespace) = default_namespace {
                    validate_default_namespace(
                        &profile_field(name, "default_namespace"),
                        namespace,
                    )?;
                }
                validate_http_url(&profile_field(name, "server_url"), server_url)?;
                if let Some(token) = auth_token {
                    require_non_empty(&profile_field(name, "auth_token"), token.expose())?;
                }
                if let Some(path) = ca_cert_path {
                    require_non_empty(&profile_field(name, "ca_cert_path"), path)?;
                }
                Ok(())
            }
        }
    }

    pub(crate) fn redacted(&self) -> Self {
        match self {
            ProfileConfig::Embedded {
                store,
                default_namespace,
                writer_id,
            } => ProfileConfig::Embedded {
                store: store.redacted(),
                default_namespace: default_namespace.clone(),
                writer_id: writer_id.clone(),
            },
            ProfileConfig::Remote {
                server_url,
                default_namespace,
                auth_token,
                ca_cert_path,
            } => ProfileConfig::Remote {
                server_url: server_url.clone(),
                default_namespace: default_namespace.clone(),
                auth_token: auth_token.as_ref().map(SecretString::masked),
                ca_cert_path: ca_cert_path.clone(),
            },
        }
    }
}

/// Prefixes a shared store-validation error (`store.<field>`-rooted) with the
/// profile name, preserving the CLI's `missing `<profile>.store.<field>``
/// message shape.
fn profile_store_error(profile_name: &str, error: &StoreConfigError) -> CliError {
    match error {
        // A missing credential reports as a missing field here, without the
        // shared error's environment hint: this loader does not consult the
        // environment. A profile stores the credentials it was created
        // with, and `profile create`/`update` is where the environment is
        // read.
        StoreConfigError::MissingField { field }
        | StoreConfigError::MissingCredential { field, .. } => {
            CliError::invalid_config(format!("missing `{profile_name}.{field}`"))
        }
        StoreConfigError::InvalidField { field, reason } => {
            CliError::invalid_config(format!("invalid `{profile_name}.{field}`: {reason}"))
        }
    }
}

/// How [`resolve_config_location`] chose the file, so `config path` answers
/// both "which file" and "why that one". The serialized spellings are the
/// `--json` surface: `flag`, `env`, `xdg`, `legacy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ConfigSource {
    /// The `--config` flag.
    Flag,
    /// The `LOONFS_CONFIG` environment variable.
    Env,
    /// `$XDG_CONFIG_HOME/loonfs/config.toml`.
    Xdg,
    /// `~/.loonfs/config.toml`.
    Legacy,
}

/// The one config file this invocation reads and writes.
pub(crate) struct ConfigLocation {
    pub path: PathBuf,
    pub source: ConfigSource,
    /// Where the file belongs now. Set only while a legacy file is in use
    /// because `XDG_CONFIG_HOME` is set and holds no config yet.
    pub preferred_path: Option<PathBuf>,
}

impl ConfigLocation {
    fn at(path: PathBuf, source: ConfigSource) -> Self {
        Self {
            path,
            source,
            preferred_path: None,
        }
    }
}

/// Resolves the config file for this invocation: `--config` beats
/// `LOONFS_CONFIG` beats the default location.
///
/// The explicit forms win by being spelled, never by the named file
/// existing — a path that is not there yet is still the path this
/// invocation uses, which is what makes `loonfs init --config <path>` a way
/// out of an unreadable default file.
pub(crate) fn resolve_config_location(flag: Option<&Path>) -> Result<ConfigLocation, CliError> {
    if let Some(path) = flag {
        return Ok(ConfigLocation::at(path.to_path_buf(), ConfigSource::Flag));
    }
    if let Some(path) = non_empty_env_path(CONFIG_PATH_ENV) {
        return Ok(ConfigLocation::at(path, ConfigSource::Env));
    }
    default_config_location()
}

/// The default location: `$XDG_CONFIG_HOME/loonfs/config.toml` when that
/// variable names an absolute directory, and `~/.loonfs/config.toml`
/// otherwise.
///
/// Existence decides exactly one thing here, and only for the migration:
/// an install that predates XDG support keeps reading its `~/.loonfs` file
/// until a config exists at the preferred path.
fn default_config_location() -> Result<ConfigLocation, CliError> {
    let legacy = legacy_config_path();
    let Some(xdg_dir) = xdg_config_home() else {
        let path = legacy.ok_or_else(|| {
            CliError::invalid_config(format!(
                "unable to determine the home directory; name the config file with \
                 `--config <path>` or `{CONFIG_PATH_ENV}=<path>`"
            ))
        })?;
        return Ok(ConfigLocation::at(path, ConfigSource::Legacy));
    };
    let xdg_path = xdg_dir.join(XDG_CONFIG_SUBDIR).join(CONFIG_FILE_NAME);
    match legacy.filter(|path| !xdg_path.exists() && path.exists()) {
        Some(legacy_path) => Ok(ConfigLocation {
            path: legacy_path,
            source: ConfigSource::Legacy,
            preferred_path: Some(xdg_path),
        }),
        None => Ok(ConfigLocation::at(xdg_path, ConfigSource::Xdg)),
    }
}

/// `XDG_CONFIG_HOME` when it names an absolute directory. The XDG spec
/// calls an empty or relative value invalid, and ignoring it keeps a stray
/// relative value from making the config file follow the working directory.
fn xdg_config_home() -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var_os("XDG_CONFIG_HOME")?);
    path.is_absolute().then_some(path)
}

fn legacy_config_path() -> Option<PathBuf> {
    let home = non_empty_env_path("HOME")?;
    Some(home.join(LEGACY_CONFIG_SUBDIR).join(CONFIG_FILE_NAME))
}

fn non_empty_env_path(name: &str) -> Option<PathBuf> {
    let value = std::env::var_os(name)?;
    (!value.is_empty()).then(|| PathBuf::from(value))
}

pub(crate) fn validate_profile_name(name: &str) -> Result<(), CliError> {
    require_non_empty("profile name", name)
}

fn profile_field(name: &str, field: &str) -> String {
    format!("{name}.{field}")
}

pub(crate) fn load_config(path: &Path) -> Result<CliConfig, CliError> {
    let source = load_config_source(path)?;
    decode_config_source(path, &source.text)
}

/// A config load for the repair commands. When the strict decode rejects the
/// file, they get the loose TOML table instead, so a typo in one profile
/// never bricks the commands that would fix it.
pub(crate) enum ConfigLoad {
    Valid(CliConfig),
    Degraded {
        table: toml::Table,
        /// The strict-decode failure, reported alongside the degraded output.
        error: Box<CliError>,
    },
}

pub(crate) fn load_config_for_repair(path: &Path) -> Result<ConfigLoad, CliError> {
    let source = load_config_source(path)?;
    match decode_config_source(path, &source.text) {
        Ok(config) => Ok(ConfigLoad::Valid(config)),
        Err(error) => Ok(ConfigLoad::Degraded {
            table: source.table,
            error: Box::new(error),
        }),
    }
}

/// A config file that got as far as being TOML of this version.
struct ConfigDocument {
    /// The file's own text, which the strict decode reads so its errors can
    /// point at a line and column in it.
    text: String,
    /// The same file as a loose table, for the version probe and for the
    /// repair commands.
    table: toml::Table,
}

/// Stage one of loading: bytes to text and a loose TOML table, plus the
/// version probe. Unknown fields and per-profile shape problems survive
/// this stage; unreadable files, TOML syntax errors, and other config
/// versions do not.
fn load_config_source(path: &Path) -> Result<ConfigDocument, CliError> {
    let bytes = fs::read(path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            CliError::invalid_config(format!(
                "config file does not exist: {}; run `loonfs init` to create it",
                path.display()
            ))
        } else {
            unusable_config(format!("failed to read config {}: {err}", path.display()))
        }
    })?;
    let text = String::from_utf8(bytes).map_err(|err| {
        unusable_config(format!("failed to decode config {}: {err}", path.display()))
    })?;
    let table: toml::Table = toml::from_str(&text).map_err(|err| {
        unusable_config(format!("failed to decode config {}: {err}", path.display()))
    })?;
    // Version before shape: a config written for another version should say
    // so directly, not surface as unknown-field noise from the strict decode.
    match table.get("config_version") {
        None => {
            return Err(unusable_config(format!(
                "config {} is missing `config_version`",
                path.display()
            )));
        }
        Some(value) => match value.as_integer() {
            Some(version) if version == i64::from(CONFIG_VERSION) => {}
            Some(version) => {
                return Err(unusable_config(format!(
                    "config {} declares `config_version = {version}`; this build supports \
                     `{CONFIG_VERSION}`",
                    path.display()
                )));
            }
            None => {
                return Err(unusable_config(format!(
                    "config {}: `config_version` must be an integer",
                    path.display()
                )));
            }
        },
    }
    Ok(ConfigDocument { text, table })
}

/// Stage two of loading: the strict typed decode (`deny_unknown_fields`)
/// plus semantic validation.
///
/// The decode reads the file's own text rather than the table stage one
/// already built. That costs a second parse of a small file and buys the
/// line, column, and quoted source line a TOML error carries only when it
/// knows where in the text it is.
fn decode_config_source(path: &Path, text: &str) -> Result<CliConfig, CliError> {
    let config: CliConfig = toml::from_str(text).map_err(|err| {
        unusable_config(format!("failed to decode config {}: {err}", path.display()))
    })?;
    // Validation messages are field-rooted (`<profile>.store.<field>`), so
    // the file they belong to is named here rather than in every check.
    config.validate().map_err(|error| {
        unusable_config(format!(
            "invalid config {}: {}",
            path.display(),
            error.message
        ))
    })?;
    Ok(config)
}

/// Marks a config file this build will not read, so the failure carries the
/// way past it. Every load failure but "no file yet" goes through here.
fn unusable_config(problem: String) -> CliError {
    CliError::invalid_config(format!("{problem}\n{CONFIG_REMEDY}"))
}

pub(crate) fn load_config_if_exists(path: &Path) -> Result<Option<CliConfig>, CliError> {
    if !path.exists() {
        return Ok(None);
    }
    load_config(path).map(Some)
}

pub(crate) fn load_or_default_config(path: &Path) -> Result<CliConfig, CliError> {
    Ok(load_config_if_exists(path)?.unwrap_or_default())
}

pub(crate) fn save_config(path: &Path, config: &CliConfig) -> Result<(), CliError> {
    config.validate()?;
    let contents = toml::to_string_pretty(config)
        .map_err(|err| CliError::invalid_config(format!("failed to encode config: {err}")))?;
    persist_config_contents(path, &contents)
}

/// Persists a loose config table as-is (atomic, owner-only), for the repair
/// commands when the file no longer strict-decodes: round-tripping through
/// [`CliConfig`] would drop the content the user still needs to fix.
pub(crate) fn save_config_table(path: &Path, table: &toml::Table) -> Result<(), CliError> {
    let contents = toml::to_string_pretty(table)
        .map_err(|err| CliError::invalid_config(format!("failed to encode config: {err}")))?;
    persist_config_contents(path, &contents)
}

fn persist_config_contents(path: &Path, contents: &str) -> Result<(), CliError> {
    let parent = path.parent().ok_or_else(|| {
        CliError::invalid_config(format!(
            "config path has no parent directory: {}",
            path.display()
        ))
    })?;
    fs::create_dir_all(parent).map_err(|err| {
        CliError::invalid_config(format!("failed to create config directory: {err}"))
    })?;
    let tmp_path = path.with_extension("tmp");
    write_owner_only(&tmp_path, contents.as_bytes())?;
    fs::rename(&tmp_path, path).map_err(|err| {
        CliError::invalid_config(format!(
            "failed to persist config {}: {err}",
            path.display()
        ))
    })?;
    Ok(())
}

/// Key names whose values never leave the file unmasked, whatever shape the
/// config is in. Mirrors the typed redaction: the `SecretString` store
/// fields plus the remote profile's `auth_token`.
const SECRET_CONFIG_KEYS: &[&str] = &[
    "access_key",
    "access_key_id",
    "auth_token",
    "secret_access_key",
    "session_token",
];

/// Deep-masks secret-named keys in a loose config table so degraded
/// `config show` output stays as safe to paste as the typed redaction.
pub(crate) fn redacted_config_table(table: &toml::Table) -> toml::Table {
    table
        .iter()
        .map(|(key, value)| (key.clone(), redacted_config_value(Some(key), value)))
        .collect()
}

fn redacted_config_value(key: Option<&str>, value: &toml::Value) -> toml::Value {
    if key.is_some_and(|key| SECRET_CONFIG_KEYS.contains(&key)) {
        return toml::Value::String("<redacted>".to_owned());
    }
    match value {
        toml::Value::Table(table) => toml::Value::Table(redacted_config_table(table)),
        toml::Value::Array(items) => toml::Value::Array(
            items
                .iter()
                .map(|item| redacted_config_value(None, item))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn write_owner_only(path: &Path, bytes: &[u8]) -> Result<(), CliError> {
    #[cfg(unix)]
    let mut file = {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(path)
            .map_err(|err| {
                CliError::invalid_config(format!(
                    "failed to create config file {}: {err}",
                    path.display()
                ))
            })?
    };
    #[cfg(not(unix))]
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .map_err(|err| {
            CliError::invalid_config(format!(
                "failed to create config file {}: {err}",
                path.display()
            ))
        })?;

    file.write_all(bytes)
        .map_err(|err| CliError::invalid_config(format!("failed to write config: {err}")))?;
    file.sync_all()
        .map_err(|err| CliError::invalid_config(format!("failed to flush config: {err}")))?;
    Ok(())
}

fn require_non_empty(field: &str, value: &str) -> Result<(), CliError> {
    if value.trim().is_empty() {
        return Err(CliError::invalid_config(format!("missing `{field}`")));
    }
    Ok(())
}

fn validate_default_namespace(field: &str, value: &str) -> Result<(), CliError> {
    NamespaceId::parse(value)
        .map(|_| ())
        .map_err(|err| CliError::invalid_config(format!("invalid `{field}`: {err}")))
}

fn validate_http_url(field: &str, value: &str) -> Result<(), CliError> {
    // One URL grammar authority: the client's own config validation.
    ClientConfig {
        server_url: value.to_owned(),
        auth_token: None,
        request_timeout_ms: None,
        disable_transient_retry: false,
        ca_cert_path: None,
    }
    .validate()
    .map_err(|error| CliError::invalid_config(format!("invalid `{field}`: {error}")))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic)]
    // Config tests use panic in unexpected match arms for precise diagnostics.

    use super::{CliConfig, ProfileConfig, StoreConfig};

    fn parse(contents: &str) -> Result<CliConfig, toml::de::Error> {
        toml::from_str(contents)
    }

    #[test]
    fn cli_config_debug_redacts_secrets() {
        let config = parse(
            r#"
config_version = 1
default_profile = "cloud"

[profiles.cloud]
mode = "embedded"

[profiles.cloud.store]
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
access_key_id = "debug-access-key-id"
secret_access_key = "debug-secret-access-key"
session_token = "debug-session-token"

[profiles.prod]
mode = "remote"
server_url = "https://loonfs.example.com"
auth_token = "debug-auth-token"
"#,
        )
        .expect("parse config");
        config.validate().expect("valid config");

        let rendered = format!("{config:?}");

        assert!(!rendered.contains("debug-access-key-id"));
        assert!(!rendered.contains("debug-secret-access-key"));
        assert!(!rendered.contains("debug-session-token"));
        assert!(!rendered.contains("debug-auth-token"));
        assert!(rendered.contains("bucket"));
    }

    #[test]
    fn redacted_config_serializes_without_secrets() {
        let config = parse(
            r#"
config_version = 1

[profiles.cloud]
mode = "embedded"

[profiles.cloud.store]
kind = "cloudflare-r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "https://account.r2.cloudflarestorage.com"
access_key_id = "plain-access-key-id"
secret_access_key = "plain-secret-access-key"

[profiles.prod]
mode = "remote"
server_url = "https://loonfs.example.com"
auth_token = "plain-auth-token"
"#,
        )
        .expect("parse config");

        let rendered =
            toml::to_string_pretty(&config.redacted()).expect("serialize redacted config");

        assert!(!rendered.contains("plain-access-key-id"));
        assert!(!rendered.contains("plain-secret-access-key"));
        assert!(!rendered.contains("plain-auth-token"));
        assert!(rendered.contains("<redacted>"));
    }

    #[test]
    fn unknown_keys_are_rejected_at_every_level() {
        let top_level = parse(
            r#"
config_version = 1
default_profil = "typo"
"#,
        )
        .expect_err("typo'd top-level key");
        assert!(top_level.to_string().contains("default_profil"));

        let profile_level = parse(
            r#"
config_version = 1

[profiles.local]
mode = "embedded"
default_namespac = "typo"

[profiles.local.store]
kind = "local-fs"
root = "/tmp/store"
"#,
        )
        .expect_err("typo'd profile key");
        assert!(profile_level.to_string().contains("default_namespac"));

        let store_level = parse(
            r#"
config_version = 1

[profiles.local]
mode = "embedded"

[profiles.local.store]
kind = "local-fs"
root = "/tmp/store"
key_prefiks = "typo"
"#,
        )
        .expect_err("typo'd store key");
        assert!(store_level.to_string().contains("key_prefiks"));
    }

    #[test]
    fn validation_reports_profile_prefixed_store_fields() {
        let config = parse(
            r#"
config_version = 1

[profiles.cloud]
mode = "embedded"

[profiles.cloud.store]
kind = "aws-s3"
bucket = " "
region = "us-east-1"
access_key_id = "access"
secret_access_key = "secret"
"#,
        )
        .expect("parse config");

        let error = config.validate().expect_err("blank bucket");
        assert_eq!(error.message, "missing `cloud.store.bucket`");
    }

    /// Every CLI example config must keep parsing into [`CliConfig`]
    /// (including under `deny_unknown_fields`) and passing validation.
    #[test]
    fn cli_example_configs_parse_and_validate() {
        let configs_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs");
        let mut examples = 0usize;
        for entry in std::fs::read_dir(configs_dir).expect("read configs directory") {
            let path = entry.expect("read configs entry").path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !name.starts_with("loonfs.") || !name.ends_with(".example.toml") {
                continue;
            }
            let contents = std::fs::read_to_string(&path).expect("read example config");
            let config: CliConfig =
                toml::from_str(&contents).unwrap_or_else(|err| panic!("{name} must parse: {err}"));
            config
                .validate()
                .unwrap_or_else(|err| panic!("{name} must validate: {}", err.message));
            examples += 1;
        }
        assert!(
            examples >= 2,
            "expected at least 2 CLI example configs, found {examples}"
        );
    }

    #[test]
    fn load_errors_name_the_file_and_probe_the_version_first() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let write = |contents: &str| std::fs::write(&path, contents).expect("write config");
        let shown_path = path.display().to_string();

        let missing = super::load_config(&path).expect_err("missing file");
        assert!(missing.message.contains(&shown_path), "{}", missing.message);

        write("config_version = ");
        let syntax = super::load_config(&path).expect_err("syntax error");
        assert!(syntax.message.contains(&shown_path), "{}", syntax.message);

        // The version verdict beats unknown-field noise: a config written
        // for another version says so directly.
        write("config_version = 2\nfuture_setting = true\n");
        let version = super::load_config(&path).expect_err("future version");
        assert!(
            version.message.contains("`config_version = 2`"),
            "{}",
            version.message
        );
        assert!(
            !version.message.contains("future_setting"),
            "{}",
            version.message
        );

        write("default_profile = \"x\"\n");
        let unversioned = super::load_config(&path).expect_err("missing version");
        assert!(
            unversioned.message.contains("missing `config_version`"),
            "{}",
            unversioned.message
        );

        write("config_version = 1\ndefault_profil = \"typo\"\n");
        let unknown = super::load_config(&path).expect_err("unknown field");
        assert!(unknown.message.contains(&shown_path), "{}", unknown.message);
        assert!(
            unknown.message.contains("default_profil"),
            "{}",
            unknown.message
        );
    }

    /// Strict decoding is only safe while a rejected file can be stepped
    /// around, so every way of failing to load one offers both hatches.
    #[test]
    fn every_unreadable_config_offers_both_escape_hatches() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let write = |contents: &str| std::fs::write(&path, contents).expect("write config");

        for contents in [
            "config_version = ",
            "config_version = 2\n",
            "default_profile = \"x\"\n",
            "config_version = 1\ndefault_profil = \"typo\"\n",
            // Semantic failures are as much a wall as shape failures.
            "config_version = 1\ndefault_profile = \"missing\"\n",
        ] {
            write(contents);
            let error = super::load_config(&path).expect_err("unreadable config");
            assert!(error.message.contains("--config"), "{}", error.message);
            assert!(
                error.message.contains(super::CONFIG_PATH_ENV),
                "{}",
                error.message
            );
        }
    }

    #[test]
    fn repair_load_keeps_the_loose_table_and_masks_secrets() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
config_version = 1
default_profile = "broken"

[profiles.broken]
mode = "remote"
server_url = "https://loonfs.example.com"
auth_token = "degraded-auth-token"
unknown_knob = true

[profiles.ok]
mode = "embedded"

[profiles.ok.store]
kind = "local-fs"
root = "/tmp/store"
"#,
        )
        .expect("write config");

        let super::ConfigLoad::Degraded { mut table, error } =
            super::load_config_for_repair(&path).expect("stage one holds")
        else {
            panic!("unknown_knob must fail the strict decode");
        };
        assert!(error.message.contains("unknown_knob"), "{}", error.message);

        let redacted = toml::to_string_pretty(&super::redacted_config_table(&table))
            .expect("render redacted table");
        assert!(!redacted.contains("degraded-auth-token"), "{redacted}");
        assert!(redacted.contains("<redacted>"), "{redacted}");
        assert!(redacted.contains("unknown_knob"), "{redacted}");

        // Deleting the broken profile through the table heals the file: the
        // strict decoder accepts what remains.
        let removed =
            crate::profiles::delete_profile_in_table(&mut table, "broken").expect("delete broken");
        assert_eq!(removed.mode, "remote");
        crate::profiles::make_default_profile_in_table(&mut table, "ok").expect("switch default");
        assert!(
            crate::profiles::make_default_profile_in_table(&mut table, "gone").is_err(),
            "unknown profile must not become the default"
        );
        super::save_config_table(&path, &table).expect("save repaired table");

        match super::load_config_for_repair(&path).expect("reload") {
            super::ConfigLoad::Valid(config) => {
                assert_eq!(config.default_profile.as_deref(), Some("ok"));
                assert!(!config.profiles.contains_key("broken"));
            }
            super::ConfigLoad::Degraded { error, .. } => {
                panic!("repaired config must strict-decode: {}", error.message)
            }
        }
    }

    #[test]
    fn store_kind_str_matches_config_tags() {
        let profile = ProfileConfig::Embedded {
            store: StoreConfig::LocalFs {
                root: "/tmp/store".to_owned(),
                key_prefix: None,
            },
            default_namespace: None,
            writer_id: None,
        };
        assert_eq!(profile.store_kind_str(), Some("local-fs"));
    }
}
