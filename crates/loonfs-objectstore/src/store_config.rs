//! The shared serde-facing object-store provider configuration.
//!
//! Both the server config file (`[store]`) and the CLI profile config
//! (`[profiles.<name>.store]`) deserialize into [`StoreConfig`], validate it
//! with [`StoreConfig::validate`], and construct the runtime store with
//! [`StoreConfig::configured_object_store`]. The TOML shape is kind-tagged
//! and snake_case. Each provider variant wraps the settings struct that the
//! provider's constructor takes. Each binary ships the examples that document
//! it, in the `config/` directory of its own crate.

use crate::configured::{
    endpoint_host_is_proven, AWS_S3_PROVEN_DOMAINS, CLOUDFLARE_R2_PROVEN_DOMAINS,
};
use crate::gcs::GcpGcsStoreConfig;
use crate::s3_compatible::{AwsS3StoreConfig, CloudflareR2StoreConfig};
use crate::{ConfiguredObjectStore, ConfiguredObjectStoreKind, ObjectStoreError};
use http::Uri;
use loonfs_types::config::{validate_absolute_http_url, validate_non_empty, ConfigFieldError};
use loonfs_types::SecretString;
use serde::{Deserialize, Serialize};

/// Standard AWS environment credential name for the access-key id.
pub const ACCESS_KEY_ID_ENV: &str = "AWS_ACCESS_KEY_ID";
/// Standard AWS environment credential name for the secret access key.
pub const SECRET_ACCESS_KEY_ENV: &str = "AWS_SECRET_ACCESS_KEY";

/// AWS S3 credential source, stored separately from provider settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AwsS3Credentials {
    /// Resolves refreshable credentials through the standard AWS SDK chain.
    Ambient {},
    /// Uses the complete credential set stored in the configuration file.
    Static {
        /// Access-key id used for provider requests and direct-upload signing.
        access_key_id: SecretString,
        /// Secret access key used for provider requests and direct-upload signing.
        secret_access_key: SecretString,
        /// Temporary credential token, or `None` for long-lived credentials.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_token: Option<SecretString>,
    },
}

/// Cloudflare R2 credential source, stored separately from provider settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CloudflareR2Credentials {
    /// Resolves S3-compatible credentials from the process environment at construction.
    Ambient {},
    /// Uses the complete credential set stored in the configuration file.
    Static {
        /// S3-compatible access-key id used for requests and direct-upload signing.
        access_key_id: SecretString,
        /// S3-compatible secret used for requests and direct-upload signing.
        secret_access_key: SecretString,
    },
}

/// Google Cloud Storage credential source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum GcpGcsCredentials {
    /// Reads one service-account JSON file for requests and direct-transfer signing.
    ///
    /// Store construction fails if the key cannot sign direct-transfer URLs.
    ServiceAccountFile {
        /// Filesystem path to the service-account JSON.
        path: String,
    },
}

/// Provider selection, settings, and an explicit credential source, as written in config files.
///
/// Serialization is transparent for secret fields and therefore writes the
/// real credentials; only serialize a [`StoreConfig::redacted`] copy into
/// display output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    deny_unknown_fields,
    remote = "Self"
)]
pub enum StoreConfig {
    /// Stores objects beneath a Unix-family directory using atomic filesystem replacement.
    LocalFs {
        /// Directory created or opened as the physical store root.
        root: String,
        /// Logical prefix applied inside `root`, or `None` to expose it directly.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key_prefix: Option<String>,
    },
    /// Connects to AWS S3 or an explicitly configured S3-compatible endpoint.
    AwsS3(AwsS3StoreConfig),
    /// Connects to Cloudflare R2 through its S3-compatible endpoint.
    CloudflareR2(CloudflareR2StoreConfig),
    /// Connects to Google Cloud Storage through its native generation-aware API.
    GcpGcs(GcpGcsStoreConfig),
}

impl serde::Serialize for StoreConfig {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Self::serialize(self, serializer)
    }
}

impl<'de> serde::Deserialize<'de> for StoreConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value.get("kind").and_then(serde_json::Value::as_str) == Some("azure-abs") {
            return Err(serde::de::Error::custom(
                "Azure Blob Storage is not supported yet: it stores no full-object checksum",
            ));
        }
        Self::deserialize(value).map_err(serde::de::Error::custom)
    }
}

impl StoreConfig {
    /// Returns the provider kind this configuration selects.
    pub fn kind(&self) -> ConfiguredObjectStoreKind {
        match self {
            StoreConfig::LocalFs { .. } => ConfiguredObjectStoreKind::LocalFs,
            StoreConfig::AwsS3 { .. } => ConfiguredObjectStoreKind::AwsS3,
            StoreConfig::CloudflareR2 { .. } => ConfiguredObjectStoreKind::CloudflareR2,
            StoreConfig::GcpGcs { .. } => ConfiguredObjectStoreKind::GcpGcs,
        }
    }

    /// Returns the configured credential-source kind, or `None` for local
    /// filesystem stores.
    pub fn credentials_kind(&self) -> Option<&'static str> {
        match self {
            StoreConfig::LocalFs { .. } => None,
            StoreConfig::AwsS3(config) => Some(match config.credentials {
                AwsS3Credentials::Ambient {} => "ambient",
                AwsS3Credentials::Static { .. } => "static",
            }),
            StoreConfig::CloudflareR2(config) => Some(match config.credentials {
                CloudflareR2Credentials::Ambient {} => "ambient",
                CloudflareR2Credentials::Static { .. } => "static",
            }),
            StoreConfig::GcpGcs { .. } => Some("service_account_file"),
        }
    }

    /// Builds the configured runtime object store for this provider.
    ///
    /// This also determines whether the store supports direct transfers. Read
    /// [`ConfiguredObjectStore::direct_transfers`](crate::ConfiguredObjectStore::direct_transfers)
    /// from the constructed store instead of checking this config again.
    pub fn configured_object_store(&self) -> crate::object_store::Result<ConfiguredObjectStore> {
        self.validate()
            .map_err(|error| ObjectStoreError::Configuration(error.to_string()))?;
        match self {
            StoreConfig::LocalFs { root, key_prefix } => {
                ConfiguredObjectStore::local_fs(root, key_prefix.as_deref())
            }
            StoreConfig::AwsS3(config) => ConfiguredObjectStore::aws_s3(config.clone()),
            StoreConfig::CloudflareR2(config) => {
                ConfiguredObjectStore::cloudflare_r2(config.clone())
            }
            StoreConfig::GcpGcs(config) => ConfiguredObjectStore::gcp_gcs(config.clone()),
        }
    }

    /// Checks required fields and URL shapes, reporting `store.`-rooted field
    /// paths.
    pub fn validate(&self) -> Result<(), ConfigFieldError> {
        match self {
            StoreConfig::LocalFs { root, .. } => {
                validate_non_empty("store.root", root)?;
            }
            StoreConfig::AwsS3(AwsS3StoreConfig {
                bucket,
                region,
                endpoint_url,
                credentials,
                ..
            }) => {
                validate_non_empty("store.bucket", bucket)?;
                validate_non_empty("store.region", region)?;
                validate_aws_s3_credentials(credentials)?;
                if let Some(url) = endpoint_url {
                    validate_absolute_http_url("store.endpoint_url", url)?;
                    validate_tls_on_proven_domains(
                        "store.endpoint_url",
                        url,
                        AWS_S3_PROVEN_DOMAINS,
                    )?;
                }
            }
            StoreConfig::CloudflareR2(CloudflareR2StoreConfig {
                bucket,
                account_id,
                endpoint_url,
                credentials,
                ..
            }) => {
                validate_non_empty("store.bucket", bucket)?;
                validate_non_empty("store.account_id", account_id)?;
                validate_r2_credentials(credentials)?;
                validate_absolute_http_url("store.endpoint_url", endpoint_url)?;
                validate_tls_on_proven_domains(
                    "store.endpoint_url",
                    endpoint_url,
                    CLOUDFLARE_R2_PROVEN_DOMAINS,
                )?;
            }
            StoreConfig::GcpGcs(GcpGcsStoreConfig {
                bucket,
                credentials: GcpGcsCredentials::ServiceAccountFile { path },
                ..
            }) => {
                validate_non_empty("store.bucket", bucket)?;
                validate_non_empty("store.credentials.path", path)?;
            }
        }
        Ok(())
    }

    /// Returns a copy whose secret fields hold the redaction placeholder, for
    /// serialization into `show`-style display output.
    pub fn redacted(&self) -> Self {
        let mut redacted = self.clone();
        match &mut redacted {
            StoreConfig::LocalFs { .. } | StoreConfig::GcpGcs { .. } => {}
            StoreConfig::AwsS3(AwsS3StoreConfig { credentials, .. }) => {
                if let AwsS3Credentials::Static {
                    access_key_id,
                    secret_access_key,
                    session_token,
                } = credentials
                {
                    *access_key_id = access_key_id.masked();
                    *secret_access_key = secret_access_key.masked();
                    *session_token = session_token.as_ref().map(SecretString::masked);
                }
            }
            StoreConfig::CloudflareR2(CloudflareR2StoreConfig { credentials, .. }) => {
                if let CloudflareR2Credentials::Static {
                    access_key_id,
                    secret_access_key,
                } = credentials
                {
                    *access_key_id = access_key_id.masked();
                    *secret_access_key = secret_access_key.masked();
                }
            }
        }
        redacted
    }
}

fn validate_aws_s3_credentials(credentials: &AwsS3Credentials) -> Result<(), ConfigFieldError> {
    if let AwsS3Credentials::Static {
        access_key_id,
        secret_access_key,
        session_token,
    } = credentials
    {
        validate_non_empty("store.credentials.access_key_id", access_key_id.expose())?;
        validate_non_empty(
            "store.credentials.secret_access_key",
            secret_access_key.expose(),
        )?;
        if let Some(session_token) = session_token {
            validate_non_empty("store.credentials.session_token", session_token.expose())?;
        }
    }
    Ok(())
}

fn validate_r2_credentials(credentials: &CloudflareR2Credentials) -> Result<(), ConfigFieldError> {
    if let CloudflareR2Credentials::Static {
        access_key_id,
        secret_access_key,
    } = credentials
    {
        validate_non_empty("store.credentials.access_key_id", access_key_id.expose())?;
        validate_non_empty(
            "store.credentials.secret_access_key",
            secret_access_key.expose(),
        )?;
    }
    Ok(())
}

/// Refuses a provider's own domain addressed over plain `http`.
///
/// Reaching one of these endpoints without TLS is a misconfiguration, not a
/// choice: they all serve TLS, and the deployment would otherwise mint
/// presigned URLs — bearer capabilities to read or write one object — into
/// cleartext. Any other host is left alone: a private gateway on `http` is a
/// legitimate setup, and it simply earns no direct transfers.
fn validate_tls_on_proven_domains(
    field: &'static str,
    value: &str,
    domain_families: &[&str],
) -> Result<(), ConfigFieldError> {
    let Ok(uri) = value.trim().parse::<Uri>() else {
        // Shape was already reported by the caller's URL validation.
        return Ok(());
    };
    if uri.scheme_str() == Some("https") || !endpoint_host_is_proven(&uri, domain_families) {
        return Ok(());
    }
    Err(ConfigFieldError::Invalid {
        field,
        reason: format!(
            "`{}` is a provider endpoint and must be reached over https: a presigned URL is a \
             bearer capability, and http would put it on the wire in cleartext",
            uri.host().unwrap_or_default()
        ),
    })
}

#[cfg(test)]
mod tests {

    use super::{CloudflareR2Credentials, ConfigFieldError, StoreConfig};
    use crate::aws_credentials::resolve_r2_credentials;
    use crate::ConfiguredObjectStoreKind;

    fn parse(contents: &str) -> StoreConfig {
        toml::from_str(contents).expect("parse store config")
    }

    #[test]
    fn parses_all_provider_kinds_and_reports_their_kind() {
        let cases: [(&str, ConfiguredObjectStoreKind); 4] = [
            (
                "kind = \"local_fs\"\nroot = \"/tmp/store\"",
                ConfiguredObjectStoreKind::LocalFs,
            ),
            (
                r#"
kind = "aws_s3"
bucket = "bucket"
region = "us-east-1"

[credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
"#,
                ConfiguredObjectStoreKind::AwsS3,
            ),
            (
                r#"
kind = "cloudflare_r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "https://account.r2.cloudflarestorage.com"

[credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
"#,
                ConfiguredObjectStoreKind::CloudflareR2,
            ),
            (
                r#"
kind = "gcp_gcs"
bucket = "bucket"

[credentials]
kind = "service_account_file"
path = "/tmp/service-account.json"
"#,
                ConfiguredObjectStoreKind::GcpGcs,
            ),
        ];

        for (contents, kind) in cases {
            let config = parse(contents);
            assert_eq!(config.kind(), kind);
            assert_eq!(
                toml::from_str::<toml::Value>(contents).expect("toml")["kind"].as_str(),
                Some(kind.as_str()),
                "the label matches the config spelling"
            );
            config.validate().expect("valid config");
        }
    }

    #[test]
    fn a_hyphenated_store_kind_does_not_load() {
        for kind in ["local-fs", "aws-s3", "cloudflare-r2", "gcp-gcs"] {
            let contents = format!("kind = \"{kind}\"\nroot = \"/tmp/store\"");
            toml::from_str::<StoreConfig>(&contents).expect_err(&format!("`{kind}` must not load"));
        }
    }

    #[test]
    fn a_provider_endpoint_without_tls_is_rejected_by_name() {
        for (contents, host) in [
            (
                r#"
kind = "aws_s3"
bucket = "bucket"
region = "us-east-1"
endpoint_url = "http://bucket.s3.amazonaws.com"

[credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
"#,
                "bucket.s3.amazonaws.com",
            ),
            (
                r#"
kind = "cloudflare_r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "http://account.r2.cloudflarestorage.com"

[credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
"#,
                "account.r2.cloudflarestorage.com",
            ),
        ] {
            match parse(contents).validate() {
                Err(ConfigFieldError::Invalid { field, reason }) => {
                    assert_eq!(field, "store.endpoint_url");
                    assert!(reason.contains(host), "{reason}");
                    assert!(reason.contains("https"), "{reason}");
                }
                other => panic!("expected an https requirement, got {other:?}"),
            }
        }

        // A gateway that is nobody's provider domain keeps working on http.
        parse(
            r#"
kind = "aws_s3"
bucket = "bucket"
region = "us-east-1"
endpoint_url = "http://127.0.0.1:9000"

[credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
"#,
        )
        .validate()
        .expect("a private http gateway is a valid configuration");
    }

    #[test]
    fn ambient_credential_sources_serialize_without_resolved_secrets() {
        let environment = |name: &str| match name {
            "AWS_ACCESS_KEY_ID" => Some("env-access".to_owned()),
            "AWS_SECRET_ACCESS_KEY" => Some("env-secret".to_owned()),
            "AWS_SESSION_TOKEN" => Some("env-session".to_owned()),
            _ => None,
        };

        let aws = parse(
            r#"
kind = "aws_s3"
bucket = "bucket"
region = "us-east-1"

[credentials]
kind = "ambient"
"#,
        );
        aws.validate()
            .expect("ambient is a valid credential source");
        let StoreConfig::AwsS3 { .. } = &aws else {
            panic!("expected aws-s3")
        };
        let rendered = toml::to_string_pretty(&aws).expect("serialize ambient aws");
        assert!(rendered.contains("kind = \"ambient\""));
        assert!(!rendered.contains("env-access"));
        assert!(!rendered.contains("env-secret"));
        assert!(!rendered.contains("env-session"));

        let r2 = parse(
            r#"
kind = "cloudflare_r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "https://account.r2.cloudflarestorage.com"

[credentials]
kind = "ambient"
"#,
        );
        let StoreConfig::CloudflareR2(config) = &r2 else {
            panic!("expected cloudflare-r2")
        };
        let credentials =
            resolve_r2_credentials(&config.credentials, environment).expect("resolve ambient r2");
        assert_eq!(credentials.access_key_id.expose(), "env-access");
        assert_eq!(credentials.secret_access_key.expose(), "env-secret");
    }

    #[test]
    fn missing_r2_ambient_credentials_are_actionable() {
        let missing = |_: &str| None;
        let r2_error = resolve_r2_credentials(&CloudflareR2Credentials::Ambient {}, missing)
            .expect_err("missing r2 ambient credentials");
        let r2_message = r2_error.to_string();
        assert!(
            r2_message.contains("cloudflare_r2 ambient credentials"),
            "{r2_message}"
        );
        assert!(r2_message.contains("AWS_ACCESS_KEY_ID"), "{r2_message}");
        assert!(r2_message.contains("AWS_SECRET_ACCESS_KEY"), "{r2_message}");
    }

    #[test]
    fn validate_reports_store_rooted_field_paths() {
        let blank_bucket = parse(
            r#"
kind = "cloudflare_r2"
bucket = " "
account_id = "account"
endpoint_url = "https://example.com"

[credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
"#,
        );
        assert_eq!(
            blank_bucket.validate(),
            Err(ConfigFieldError::Missing {
                field: "store.bucket"
            })
        );

        let bad_scheme = parse(
            r#"
kind = "aws_s3"
bucket = "bucket"
region = "us-east-1"
endpoint_url = "ftp://example.com"

[credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
"#,
        );
        match bad_scheme.validate() {
            Err(ConfigFieldError::Invalid { field, reason }) => {
                assert_eq!(field, "store.endpoint_url");
                assert!(reason.contains("ftp"));
            }
            other => panic!("expected invalid endpoint_url, got {other:?}"),
        }
    }

    #[test]
    fn azure_configuration_is_rejected_with_the_missing_checksum_reason() {
        let error = toml::from_str::<StoreConfig>(r#"kind = "azure-abs""#)
            .expect_err("unsupported provider");
        assert!(error.to_string().contains(
            "Azure Blob Storage is not supported yet: it stores no full-object checksum"
        ));
    }

    #[test]
    fn unknown_keys_are_rejected_and_named() {
        // `deny_unknown_fields` must keep working through the internally
        // tagged (`kind = ...`) enum representation: a typo'd key in the
        // store table has to fail the parse and name the offending key.
        let error = toml::from_str::<StoreConfig>(
            r#"
kind = "aws_s3"
bucket = "bucket"
region = "us-east-1"
buckt = "typo"

[credentials]
kind = "ambient"
"#,
        )
        .expect_err("typo'd key must be rejected");

        let message = error.to_string();
        assert!(
            message.contains("buckt"),
            "error must name the unknown key, got: {message}"
        );

        let nested = toml::from_str::<StoreConfig>(
            r#"
kind = "aws_s3"
bucket = "bucket"
region = "us-east-1"

[credentials]
kind = "ambient"
access_key_id = "not-allowed"
"#,
        )
        .expect_err("ambient credentials cannot contain static fields")
        .to_string();
        assert!(nested.contains("access_key_id"), "{nested}");
    }

    #[test]
    fn debug_output_redacts_credentials() {
        let config = parse(
            r#"
kind = "aws_s3"
bucket = "bucket"
region = "us-east-1"

[credentials]
kind = "static"
access_key_id = "debug-access-key-id"
secret_access_key = "debug-secret-access-key"
session_token = "debug-session-token"
"#,
        );

        let rendered = format!("{config:?}");

        assert!(!rendered.contains("debug-access-key-id"));
        assert!(!rendered.contains("debug-secret-access-key"));
        assert!(!rendered.contains("debug-session-token"));
        assert!(rendered.contains("bucket"));
    }

    #[test]
    fn redacted_copy_serializes_without_credentials() {
        let config = parse(
            r#"
kind = "cloudflare_r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "https://account.r2.cloudflarestorage.com"

[credentials]
kind = "static"
access_key_id = "plain-access-key-id"
secret_access_key = "plain-secret-access-key"
"#,
        );

        let rendered = toml::to_string_pretty(&config.redacted()).expect("serialize redacted");

        assert!(!rendered.contains("plain-access-key-id"));
        assert!(!rendered.contains("plain-secret-access-key"));
        assert!(rendered.contains("<redacted>"));
        assert!(rendered.contains("account"));
    }

    /// Checks that each provider's store table, with every key set, serializes
    /// back to the same keys and values and parses to the same config, and
    /// that an unknown key in it fails the parse.
    #[test]
    fn every_store_table_round_trips_with_unchanged_keys() {
        let tables = [
            r#"
kind = "local_fs"
root = "/tmp/store"
key_prefix = "demo"
"#,
            r#"
kind = "aws_s3"
bucket = "bucket"
region = "us-east-1"
endpoint_url = "https://bucket.s3.amazonaws.com"
key_prefix = "demo"
force_path_style = true

[credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
session_token = "session"
"#,
            r#"
kind = "aws_s3"
bucket = "bucket"
region = "us-east-1"
force_path_style = false

[credentials]
kind = "ambient"
"#,
            r#"
kind = "cloudflare_r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "https://account.r2.cloudflarestorage.com"
key_prefix = "demo"

[credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
"#,
            r#"
kind = "cloudflare_r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "https://account.r2.cloudflarestorage.com"

[credentials]
kind = "ambient"
"#,
            r#"
kind = "gcp_gcs"
bucket = "bucket"
key_prefix = "demo"

[credentials]
kind = "service_account_file"
path = "/tmp/service-account.json"
"#,
        ];

        for table in tables {
            let config = parse(table);
            let rendered = toml::to_string_pretty(&config).expect("serialize store config");
            assert_eq!(
                toml::from_str::<toml::Value>(&rendered).expect("rendered toml"),
                toml::from_str::<toml::Value>(table).expect("fixture toml"),
                "the serialized table has the fixture's keys:\n{rendered}"
            );
            assert_eq!(parse(&rendered), config);

            let typo = format!("buckt = \"typo\"\n{table}");
            let error = toml::from_str::<StoreConfig>(&typo).expect_err("typo'd key must fail");
            assert!(error.to_string().contains("buckt"), "{error}");
        }
    }

    #[test]
    fn serialization_round_trips_the_store_table() {
        let config = parse(
            r#"
kind = "aws_s3"
bucket = "bucket"
region = "us-east-1"
key_prefix = "demo"

[credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
"#,
        );

        let rendered = toml::to_string_pretty(&config).expect("serialize store config");
        assert!(rendered.contains("kind = \"aws_s3\""));
        assert!(!rendered.contains("session_token"));
        assert!(!rendered.contains("endpoint_url"));

        let reparsed: StoreConfig = toml::from_str(&rendered).expect("reparse store config");
        assert_eq!(reparsed, config);
    }
}
