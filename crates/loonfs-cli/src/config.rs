//! The CLI config file's contents: profiles, defaults, and their validation.

use crate::args::TransientRetry;
use crate::config_file::{profile_field, validate_profile_name};
use crate::error::{CliError, Result};
use loonfs_client::ClientConfig;
use loonfs_objectstore::local_fs_store::LocalFsStoreConfig;
use loonfs_types::config::{validate_non_empty, ConfigFieldError};
use loonfs_types::env::AUTH_TOKEN_ENV;
use loonfs_types::{
    ActorId, NamespaceId, PrincipalId, PrincipalScope, SecretString, SubjectId, SubjectPart,
    SubjectPartsError, REDACTED,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub(crate) use loonfs_objectstore::StoreConfig;

pub(crate) const CONFIG_VERSION: u32 = 1;

/// Environment override for the config file, and the escape hatch a shell
/// session reaches for when the default file is one the CLI will not read.
pub(crate) const CONFIG_PATH_ENV: &str = "LOONFS_CONFIG";
pub(crate) const PROFILE_ENV: &str = "LOONFS_PROFILE";
pub(crate) const NAMESPACE_ENV: &str = "LOONFS_NAMESPACE";
pub(crate) const ACTOR_ID_ENV: &str = "LOONFS_ACTOR_ID";
pub(crate) const SUBJECT_ID_ENV: &str = "LOONFS_SUBJECT_ID";
pub(crate) const PRINCIPAL_SCOPE_ENV: &str = "LOONFS_PRINCIPAL_SCOPE";
pub(crate) const PRINCIPALS_ENV: &str = "LOONFS_PRINCIPALS";

/// A blank environment value has no usable setting, so treat it as unset
/// rather than passing it on to validation.
pub(crate) fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

pub(crate) fn absolute_env_path(name: &str) -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var_os(name)?);
    path.is_absolute().then_some(path)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CliConfig {
    pub config_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_profile: Option<String>,
    /// Profiles are in their own `[profiles.<name>]` tables so profile
    /// names can never collide with top-level settings.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub profiles: BTreeMap<String, ProfileConfig>,
}

/// One configured profile: who it acts as, its default namespace, and how it
/// reaches LoonFS.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "ProfileConfigFile", into = "ProfileConfigFile")]
pub(crate) struct ProfileConfig {
    pub identity: ProfileIdentity,
    pub default_namespace: Option<String>,
    pub connection: ProfileConnection,
}

/// The actor and the subject a profile acts as.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ProfileIdentity {
    pub actor_id: Option<ActorId>,
    pub subject_id: Option<SubjectId>,
    pub principal_scope: Option<PrincipalScope>,
    pub principals: Option<Vec<PrincipalId>>,
}

/// How a profile reaches LoonFS: a runtime in this process over a store, or
/// a server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProfileConnection {
    Embedded {
        store: StoreConfig,
        writer_id: Option<String>,
    },
    Remote {
        server_url: String,
        auth_token: Option<SecretString>,
        /// PEM bundle of extra certificate authorities to trust, for a
        /// server whose certificate a private CA issued.
        ca_cert_path: Option<String>,
    },
}

/// Whether a profile runs the runtime in this process or talks to a server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProfileMode {
    Embedded,
    Remote,
}

impl ProfileMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Embedded => "embedded",
            Self::Remote => "remote",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "embedded" => Some(Self::Embedded),
            "remote" => Some(Self::Remote),
            _ => None,
        }
    }
}

/// A profile as a config file stores it.
#[derive(Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum ProfileConfigFile {
    Embedded {
        store: StoreConfig,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        actor_id: Option<ActorId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subject_id: Option<SubjectId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        principal_scope: Option<PrincipalScope>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        principals: Option<Vec<PrincipalId>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default_namespace: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        writer_id: Option<String>,
    },
    Remote {
        server_url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        actor_id: Option<ActorId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subject_id: Option<SubjectId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        principal_scope: Option<PrincipalScope>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        principals: Option<Vec<PrincipalId>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default_namespace: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_token: Option<SecretString>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ca_cert_path: Option<String>,
    },
}

impl From<ProfileConfigFile> for ProfileConfig {
    fn from(file: ProfileConfigFile) -> Self {
        match file {
            ProfileConfigFile::Embedded {
                store,
                actor_id,
                subject_id,
                principal_scope,
                principals,
                default_namespace,
                writer_id,
            } => Self {
                identity: ProfileIdentity {
                    actor_id,
                    subject_id,
                    principal_scope,
                    principals,
                },
                default_namespace,
                connection: ProfileConnection::Embedded { store, writer_id },
            },
            ProfileConfigFile::Remote {
                server_url,
                actor_id,
                subject_id,
                principal_scope,
                principals,
                default_namespace,
                auth_token,
                ca_cert_path,
            } => Self {
                identity: ProfileIdentity {
                    actor_id,
                    subject_id,
                    principal_scope,
                    principals,
                },
                default_namespace,
                connection: ProfileConnection::Remote {
                    server_url,
                    auth_token,
                    ca_cert_path,
                },
            },
        }
    }
}

impl From<ProfileConfig> for ProfileConfigFile {
    fn from(profile: ProfileConfig) -> Self {
        let ProfileIdentity {
            actor_id,
            subject_id,
            principal_scope,
            principals,
        } = profile.identity;
        let default_namespace = profile.default_namespace;
        match profile.connection {
            ProfileConnection::Embedded { store, writer_id } => Self::Embedded {
                store,
                actor_id,
                subject_id,
                principal_scope,
                principals,
                default_namespace,
                writer_id,
            },
            ProfileConnection::Remote {
                server_url,
                auth_token,
                ca_cert_path,
            } => Self::Remote {
                server_url,
                actor_id,
                subject_id,
                principal_scope,
                principals,
                default_namespace,
                auth_token,
                ca_cert_path,
            },
        }
    }
}

impl CliConfig {
    pub(crate) fn new() -> Self {
        Self {
            config_version: CONFIG_VERSION,
            default_profile: None,
            profiles: BTreeMap::new(),
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.config_version != CONFIG_VERSION {
            return Err(CliError::invalid_config(format!(
                "unsupported `config_version`: expected `{CONFIG_VERSION}`, got `{}`",
                self.config_version
            )));
        }
        if let Some(default_profile) = &self.default_profile {
            validate_non_empty("default_profile", default_profile)?;
            if !self.profiles.contains_key(default_profile) {
                return Err(CliError::invalid_config(format!(
                    "`default_profile` points to missing profile `{default_profile}`"
                )));
            }
        }
        for (name, profile) in &self.profiles {
            validate_profile_name(name)
                .map_err(|error| CliError::invalid_config(error.message.clone()))?;
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
    pub(crate) fn mode(&self) -> ProfileMode {
        match self.connection {
            ProfileConnection::Embedded { .. } => ProfileMode::Embedded,
            ProfileConnection::Remote { .. } => ProfileMode::Remote,
        }
    }

    pub(crate) fn store_kind_str(&self) -> Option<&'static str> {
        match &self.connection {
            ProfileConnection::Embedded { store, .. } => Some(store.kind().as_str()),
            ProfileConnection::Remote { .. } => None,
        }
    }

    pub(crate) fn validate(&self, name: &str) -> Result<()> {
        if let Some(namespace) = self.default_namespace.as_deref() {
            validate_default_namespace(&profile_field(name, "default_namespace"), namespace)?;
        }
        validate_profile_subject(
            name,
            self.identity.principal_scope.as_ref(),
            self.identity.principals.as_deref(),
        )?;
        match &self.connection {
            ProfileConnection::Embedded { store, .. } => store
                .validate()
                .map_err(|error| profile_field_error(name, &error)),
            ProfileConnection::Remote {
                server_url,
                auth_token,
                ca_cert_path,
            } => validate_stored_remote_client_config(
                name,
                server_url,
                auth_token.as_ref(),
                ca_cert_path.as_deref(),
            ),
        }
    }

    pub(crate) fn ensure_store_usable(&self, name: &str) -> Result<()> {
        if let ProfileConnection::Embedded {
            store: StoreConfig::LocalFs(LocalFsStoreConfig { root, .. }),
            ..
        } = &self.connection
        {
            if !Path::new(root).is_absolute() {
                return Err(CliError::invalid_config(format!(
                    "profile `{name}` has a relative local_fs root `{root}`; \
                     set an absolute path with `loonfs profile update local {name} --root <path>`"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn redacted(&self) -> Self {
        let connection = match &self.connection {
            ProfileConnection::Embedded { store, writer_id } => ProfileConnection::Embedded {
                store: store.redacted(),
                writer_id: writer_id.clone(),
            },
            ProfileConnection::Remote {
                server_url,
                auth_token,
                ca_cert_path,
            } => ProfileConnection::Remote {
                server_url: server_url.clone(),
                auth_token: auth_token.as_ref().map(SecretString::masked),
                ca_cert_path: ca_cert_path.clone(),
            },
        };
        Self {
            identity: self.identity.clone(),
            default_namespace: self.default_namespace.clone(),
            connection,
        }
    }
}

/// Refuses a stored principal scope without principals, or principals
/// without a scope.
///
/// This is the pairing half of what `Subject::from_parts` checks for a
/// request. A profile may also store a subject id alone, because the subject
/// is used only with principals, so the rest of that rule does not apply here.
pub(crate) fn ensure_subject_pairing(
    principal_scope: Option<&PrincipalScope>,
    principals: Option<&[PrincipalId]>,
) -> Result<(), SubjectPartsError> {
    let (part, required_by) = match (principal_scope, principals) {
        (Some(_), None) => (SubjectPart::Principals, SubjectPart::PrincipalScope),
        (None, Some(_)) => (SubjectPart::PrincipalScope, SubjectPart::Principals),
        (Some(_), Some(_)) | (None, None) => return Ok(()),
    };
    Err(SubjectPartsError::Missing { part, required_by })
}

/// Reports a stored subject's missing part under the profile's field names.
fn validate_profile_subject(
    profile_name: &str,
    principal_scope: Option<&PrincipalScope>,
    principals: Option<&[PrincipalId]>,
) -> Result<()> {
    ensure_subject_pairing(principal_scope, principals).map_err(|error| match error {
        SubjectPartsError::Missing { part, required_by } => CliError::invalid_config(format!(
            "`profiles.{profile_name}.{part}` is required with `profiles.{profile_name}.{required_by}`"
        )),
        other => CliError::invalid_config(other.to_string()),
    })
}

/// Reports a store or client config field failure under the profile's path.
fn profile_field_error(profile_name: &str, error: &ConfigFieldError) -> CliError {
    let field = profile_field(profile_name, error.field());
    match error {
        ConfigFieldError::Missing { .. } => CliError::invalid_config(format!("missing `{field}`")),
        ConfigFieldError::Invalid { reason, .. } => {
            CliError::invalid_config(format!("invalid `{field}`: {reason}"))
        }
        error => CliError::invalid_config(format!("invalid `{field}`: {error}")),
    }
}

/// Secret fields that are always redacted, even when the config cannot be
/// decoded into the current typed format.
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
        return toml::Value::String(REDACTED.to_owned());
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

fn validate_default_namespace(field: &str, value: &str) -> Result<()> {
    NamespaceId::parse(value)
        .map(|_| ())
        .map_err(|err| CliError::invalid_config(format!("invalid `{field}`: {err}")))
}

/// Builds the client configuration used by a remote profile.
pub(crate) fn remote_client_config(
    server_url: &str,
    auth_token: Option<&SecretString>,
    ca_cert_path: Option<&str>,
    transient_retry: TransientRetry,
) -> ClientConfig {
    remote_client_config_from(
        server_url,
        auth_token,
        ca_cert_path,
        transient_retry,
        non_empty_env,
    )
}

fn remote_client_config_from(
    server_url: &str,
    auth_token: Option<&SecretString>,
    ca_cert_path: Option<&str>,
    transient_retry: TransientRetry,
    lookup: impl Fn(&str) -> Option<String>,
) -> ClientConfig {
    ClientConfig {
        server_url: server_url.to_owned(),
        auth_token: auth_token.cloned().or_else(|| {
            lookup(AUTH_TOKEN_ENV)
                .filter(|token| !token.trim().is_empty())
                .map(SecretString::from)
        }),
        request_timeout_ms: None,
        disable_transient_retry: transient_retry == TransientRetry::NoRetry,
        ca_cert_path: ca_cert_path.map(ToOwned::to_owned),
    }
}

pub(crate) fn validate_remote_client_config(
    profile_name: &str,
    server_url: &str,
    auth_token: Option<&SecretString>,
    ca_cert_path: Option<&str>,
) -> Result<()> {
    validate_client_config(
        profile_name,
        remote_client_config(server_url, auth_token, ca_cert_path, TransientRetry::Retry),
    )
}

fn validate_stored_remote_client_config(
    profile_name: &str,
    server_url: &str,
    auth_token: Option<&SecretString>,
    ca_cert_path: Option<&str>,
) -> Result<()> {
    validate_client_config(
        profile_name,
        remote_client_config_from(
            server_url,
            auth_token,
            ca_cert_path,
            TransientRetry::Retry,
            |_| None,
        ),
    )
}

fn validate_client_config(profile_name: &str, config: ClientConfig) -> Result<()> {
    config
        .validate()
        .map_err(|error| profile_field_error(profile_name, &error))
}

#[cfg(test)]
mod tests {

    use super::{
        remote_client_config_from, CliConfig, ProfileConfig, ProfileConnection, ProfileIdentity,
    };
    use crate::args::TransientRetry;
    use loonfs_test_support::EnvGuard;
    use loonfs_types::env::AUTH_TOKEN_ENV;
    use loonfs_types::SecretString;

    #[test]
    fn a_remote_profile_takes_its_token_from_the_config_then_the_environment() {
        let stored = SecretString::from("stored-token");
        let from_config = remote_client_config_from(
            "https://loonfs.example.com",
            Some(&stored),
            None,
            TransientRetry::Retry,
            |_| Some("env-token".to_owned()),
        );
        assert_eq!(
            from_config.auth_token.as_ref().map(SecretString::expose),
            Some("stored-token")
        );
        assert!(!from_config.disable_transient_retry);

        let from_environment = remote_client_config_from(
            "https://loonfs.example.com",
            None,
            None,
            TransientRetry::NoRetry,
            |name| {
                assert_eq!(name, AUTH_TOKEN_ENV);
                Some("env-token".to_owned())
            },
        );
        assert_eq!(
            from_environment
                .auth_token
                .as_ref()
                .map(SecretString::expose),
            Some("env-token")
        );
        assert!(
            from_environment.disable_transient_retry,
            "--no-retry reaches the client"
        );

        let blank = remote_client_config_from(
            "https://loonfs.example.com",
            None,
            None,
            TransientRetry::NoRetry,
            |_| Some("  ".to_owned()),
        );
        assert!(blank.auth_token.is_none());
    }

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
kind = "aws_s3"
bucket = "bucket"
region = "us-east-1"

[profiles.cloud.store.credentials]
kind = "static"
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
kind = "cloudflare_r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "https://account.r2.cloudflarestorage.com"

[profiles.cloud.store.credentials]
kind = "static"
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
kind = "local_fs"
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
kind = "local_fs"
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
kind = "aws_s3"
bucket = " "
region = "us-east-1"

[profiles.cloud.store.credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
"#,
        )
        .expect("parse config");

        let error = config.validate().expect_err("blank bucket");
        assert_eq!(error.message, "missing `cloud.store.bucket`");
    }

    #[test]
    fn cli_example_configs_parse_and_validate() {
        let configs_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config");
        let mut examples = 0usize;
        for entry in std::fs::read_dir(configs_dir).expect("read config directory") {
            let path = entry.expect("read config entry").path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !name.ends_with(".example.toml") {
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
    fn the_loader_preserves_the_serialized_credential_source() {
        let store = loonfs_objectstore::StoreConfig::AwsS3(
            loonfs_objectstore::s3_compatible::AwsS3StoreConfig {
                bucket: "bucket".to_owned(),
                region: "us-east-1".to_owned(),
                endpoint_url: None,
                credentials: loonfs_objectstore::AwsS3Credentials::Ambient {},
                key_prefix: None,
                force_path_style: false,
            },
        );
        let serialized = toml::to_string_pretty(&store).expect("serialize shared store");
        let qualified = format!(
            "[profiles.parity.store]\n{}",
            serialized.replace("[credentials]", "[profiles.parity.store.credentials]")
        );

        let dir = tempfile::tempdir().expect("tempdir");
        let cli_path = dir.path().join("cli.toml");
        std::fs::write(
            &cli_path,
            format!(
                "config_version = 1\ndefault_profile = \"parity\"\n\n\
                 [profiles.parity]\nmode = \"embedded\"\n\n{qualified}"
            ),
        )
        .expect("write cli config");

        let access_key = EnvGuard::set("AWS_ACCESS_KEY_ID", "parity-access");
        let secret_key = EnvGuard::set("AWS_SECRET_ACCESS_KEY", "parity-secret");
        let cli = crate::config_file::load_config(&cli_path).expect("load cli config");
        drop((access_key, secret_key));
        let ProfileConnection::Embedded {
            store: cli_store, ..
        } = &cli
            .profiles
            .get("parity")
            .expect("parity profile")
            .connection
        else {
            panic!("expected embedded profile")
        };
        assert_eq!(cli_store.credentials_kind(), Some("ambient"));
    }

    #[test]
    fn load_errors_name_the_file_and_probe_the_version_first() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let write = |contents: &str| std::fs::write(&path, contents).expect("write config");
        let shown_path = path.display().to_string();

        let missing = crate::config_file::load_config(&path).expect_err("missing file");
        assert!(missing.message.contains(&shown_path), "{}", missing.message);

        write("config_version = ");
        let syntax = crate::config_file::load_config(&path).expect_err("syntax error");
        assert!(syntax.message.contains(&shown_path), "{}", syntax.message);

        // The version verdict beats unknown-field noise: a config written
        // for another version says so directly.
        write("config_version = 2\nfuture_setting = true\n");
        let version = crate::config_file::load_config(&path).expect_err("future version");
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
        let unversioned = crate::config_file::load_config(&path).expect_err("missing version");
        assert!(
            unversioned.message.contains("missing `config_version`"),
            "{}",
            unversioned.message
        );

        write("config_version = 1\ndefault_profil = \"typo\"\n");
        let unknown = crate::config_file::load_config(&path).expect_err("unknown field");
        assert!(unknown.message.contains(&shown_path), "{}", unknown.message);
        assert!(
            unknown.message.contains("default_profil"),
            "{}",
            unknown.message
        );
    }

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
            let error = crate::config_file::load_config(&path).expect_err("unreadable config");
            assert!(error.message.contains("--config"), "{}", error.message);
            assert!(
                error.message.contains(super::CONFIG_PATH_ENV),
                "{}",
                error.message
            );
        }
    }

    #[test]
    fn concurrent_mutations_preserve_every_profile_and_remove_temp_files() {
        const THREADS: usize = 16;
        const MUTATIONS_PER_THREAD: usize = 8;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let barrier = std::sync::Barrier::new(THREADS);

        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(THREADS);
            for thread_index in 0..THREADS {
                let barrier = &barrier;
                let path = &path;
                handles.push(scope.spawn(move || {
                    let profile_name = format!("agent-{thread_index}");
                    barrier.wait();
                    for mutation_index in 0..MUTATIONS_PER_THREAD {
                        let namespace = format!("namespace-{thread_index}-{mutation_index}");
                        crate::config_file::mutate_config(path, |config| {
                            config.profiles.insert(
                                profile_name.clone(),
                                ProfileConfig {
                                    identity: ProfileIdentity::default(),
                                    default_namespace: Some(namespace),
                                    connection: ProfileConnection::Remote {
                                        server_url: format!(
                                            "https://agent-{thread_index}-{mutation_index}.example.com"
                                        ),
                                        auth_token: None,
                                        ca_cert_path: None,
                                    },
                                },
                            );
                            Ok(())
                        })
                        .expect("mutate config");
                    }
                }));
            }
            for handle in handles {
                handle.join().expect("mutation thread");
            }
        });

        let contents = std::fs::read_to_string(&path).expect("read final config");
        let config: CliConfig = toml::from_str(&contents).expect("parse final config");
        config.validate().expect("validate final config");
        for thread_index in 0..THREADS {
            let profile_name = format!("agent-{thread_index}");
            let expected_namespace =
                format!("namespace-{thread_index}-{}", MUTATIONS_PER_THREAD - 1);
            let Some(ProfileConfig {
                default_namespace,
                connection: ProfileConnection::Remote { .. },
                ..
            }) = config.profiles.get(&profile_name)
            else {
                panic!("missing remote profile {profile_name}");
            };
            assert_eq!(
                default_namespace.as_deref(),
                Some(expected_namespace.as_str())
            );
        }

        let temp_files: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read config directory")
            .map(|entry| entry.expect("read directory entry").path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "tmp"))
            .collect();
        assert!(
            temp_files.is_empty(),
            "temporary files remain: {temp_files:?}"
        );
    }

    #[test]
    #[cfg(unix)]
    fn unreadable_config_parent_is_not_treated_as_absence() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("config.toml");
        let contents = "config_version = 1\n";
        std::fs::write(&path, contents).expect("write config");
        let permissions = std::fs::metadata(directory.path())
            .expect("directory metadata")
            .permissions();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o000))
            .expect("make parent unreadable");
        let result = crate::config_file::load_or_default_config(&path);
        std::fs::set_permissions(directory.path(), permissions).expect("restore permissions");

        let error = result.expect_err("unreadable parent must fail");
        assert_eq!(
            error.code,
            crate::error::CliErrorCode::InvalidConfig.as_str()
        );
        assert!(error.message.contains(&path.display().to_string()));
        assert_eq!(
            std::fs::read_to_string(path).expect("read config"),
            contents
        );
    }

    #[test]
    fn mutate_config_creates_an_owner_only_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nested").join("config.toml");

        crate::config_file::mutate_config(&path, |_| Ok(())).expect("create config");

        assert!(path.is_file());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = std::fs::metadata(&path)
                .expect("config metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_broken_config_symlink_is_not_absence() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::os::unix::fs::symlink(dir.path().join("missing.toml"), &path).expect("symlink");

        assert_eq!(
            crate::config_file::load_config_if_exists(&path)
                .expect_err("a broken config symlink should not read as an absent config")
                .code,
            crate::error::CliErrorCode::InvalidConfig.as_str()
        );
        assert_eq!(
            crate::config_file::mutate_config(&path, |_| Ok(()))
                .expect_err("a broken config symlink should not be replaced")
                .code,
            crate::error::CliErrorCode::InvalidConfig.as_str()
        );
        assert!(!dir.path().join("missing.toml").exists());
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
mode = "embedded"
unknown_knob = true

[profiles.broken.store]
kind = "aws_s3"
bucket = "bucket"
region = "us-east-1"

[profiles.broken.store.credentials]
kind = "static"
access_key_id = "degraded-access-key-id"
secret_access_key = "degraded-secret-access-key"
session_token = "degraded-session-token"

[profiles.ok]
mode = "embedded"

[profiles.ok.store]
kind = "local_fs"
root = "/tmp/store"
"#,
        )
        .expect("write config");

        let crate::config_file::ConfigLoad::Degraded { mut table, error } =
            crate::config_file::load_config_for_repair(&path).expect("stage one holds")
        else {
            panic!("unknown_knob must fail the strict decode");
        };
        assert!(error.message.contains("unknown_knob"), "{}", error.message);

        let redacted = toml::to_string_pretty(&super::redacted_config_table(&table))
            .expect("render redacted table");
        assert!(!redacted.contains("degraded-access-key-id"), "{redacted}");
        assert!(
            !redacted.contains("degraded-secret-access-key"),
            "{redacted}"
        );
        assert!(!redacted.contains("degraded-session-token"), "{redacted}");
        assert!(redacted.contains("<redacted>"), "{redacted}");
        assert!(redacted.contains("unknown_knob"), "{redacted}");

        // Deleting the broken profile through the table heals the file: the
        // strict decoder accepts what remains.
        let removed =
            crate::profiles::delete_profile_in_table(&mut table, "broken").expect("delete broken");
        assert_eq!(removed.mode, "embedded");
        crate::profiles::set_default_profile_in_table(&mut table, "ok").expect("switch default");
        assert_eq!(
            crate::profiles::set_default_profile_in_table(&mut table, "gone")
                .expect_err("unknown profile must not become the default")
                .code,
            crate::error::CliErrorCode::ProfileNotFound.as_str()
        );
        crate::config_file::save_config_table(&path, &table).expect("save repaired table");

        match crate::config_file::load_config_for_repair(&path).expect("reload") {
            crate::config_file::ConfigLoad::Valid(config) => {
                assert_eq!(config.default_profile.as_deref(), Some("ok"));
                assert!(!config.profiles.contains_key("broken"));
            }
            crate::config_file::ConfigLoad::Degraded { error, .. } => {
                panic!("repaired config must strict-decode: {}", error.message)
            }
        }
    }
}
