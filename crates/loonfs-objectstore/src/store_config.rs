//! The shared serde-facing object-store provider configuration.
//!
//! Both the server config file (`[store]`) and the CLI profile config
//! (`[profiles.<name>.store]`) deserialize into [`StoreConfig`], validate it
//! with [`StoreConfig::validate`], and construct the runtime store with
//! [`StoreConfig::configured_object_store`]. The TOML shape is kind-tagged
//! and kebab-case, documented by the examples in `configs/`.

use crate::abs::AzureAbsStoreConfig;
use crate::gcs::GcpGcsStoreConfig;
use crate::s3_compatible::{AwsS3StoreConfig, CloudflareR2StoreConfig};
use crate::secret::SecretString;
use crate::{ConfiguredObjectStore, ConfiguredObjectStoreKind};
use http::Uri;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Provider selection plus credentials, as written in config files.
///
/// Serialization is transparent for secret fields and therefore writes the
/// real credentials; only serialize a [`StoreConfig::redacted`] copy into
/// display output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
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
    AwsS3 {
        /// Bucket that acts as the physical store root.
        bucket: String,
        /// SigV4 signing region.
        region: String,
        /// Service endpoint override, or `None` to derive the regional AWS endpoint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        endpoint_url: Option<String>,
        /// Access-key id used for provider requests and direct-upload signing.
        access_key_id: SecretString,
        /// Secret access key used for provider requests and direct-upload signing.
        secret_access_key: SecretString,
        /// Temporary credential token, or `None` for long-lived credentials.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_token: Option<SecretString>,
        /// Logical prefix applied inside the bucket, or `None` to expose its root.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key_prefix: Option<String>,
        /// Uses path-style bucket addressing when `true`; defaults to virtual-hosted style.
        #[serde(default)]
        force_path_style: bool,
    },
    /// Connects to Cloudflare R2 through its S3-compatible endpoint.
    CloudflareR2 {
        /// R2 bucket that acts as the physical store root.
        bucket: String,
        /// Cloudflare account identity used to validate provider configuration.
        account_id: String,
        /// Account-level R2 S3 endpoint used with path-style bucket addressing.
        endpoint_url: String,
        /// S3-compatible access-key id used for requests and direct-upload signing.
        access_key_id: SecretString,
        /// S3-compatible secret used for requests and direct-upload signing.
        secret_access_key: SecretString,
        /// Logical prefix applied inside the bucket, or `None` to expose its root.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key_prefix: Option<String>,
    },
    /// Connects to Google Cloud Storage through its native generation-aware API.
    GcpGcs {
        /// GCS bucket that acts as the physical store root.
        bucket: String,
        /// Filesystem path to the service-account JSON used for authentication.
        service_account_key_path: String,
        /// Logical prefix applied inside the bucket, or `None` to expose its root.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key_prefix: Option<String>,
    },
    /// Connects to Azure Blob Storage through its native shared-key API.
    AzureAbs {
        /// Azure storage account used for addressing and signing.
        account_name: String,
        /// Blob container that acts as the physical store root.
        container_name: String,
        /// Shared account key used for request authentication.
        access_key: SecretString,
        /// Azure-compatible endpoint override, or `None` for the public service.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        endpoint_url: Option<String>,
        /// Logical prefix applied inside the container, or `None` to expose its root.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key_prefix: Option<String>,
    },
}

/// Validation failure for a [`StoreConfig`].
///
/// Field paths are rooted at the store table (`store.bucket`, ...) so callers
/// can report them directly or prefix them with their own config path.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StoreConfigError {
    /// Reports a required field whose value is absent or blank.
    #[error("missing `{field}`")]
    MissingField {
        /// `store.`-rooted path suitable for direct configuration diagnostics.
        field: &'static str,
    },
    /// Reports a present field whose value violates its provider-specific contract.
    #[error("invalid `{field}`: {reason}")]
    InvalidField {
        /// `store.`-rooted path suitable for direct configuration diagnostics.
        field: &'static str,
        /// Specific validation failure without repeating the field path.
        reason: String,
    },
}

impl StoreConfigError {
    /// The `store.`-rooted path of the offending field.
    pub fn field(&self) -> &'static str {
        match self {
            StoreConfigError::MissingField { field }
            | StoreConfigError::InvalidField { field, .. } => field,
        }
    }
}

impl StoreConfig {
    /// The provider kind this configuration selects.
    pub fn kind(&self) -> ConfiguredObjectStoreKind {
        match self {
            StoreConfig::LocalFs { .. } => ConfiguredObjectStoreKind::LocalFs,
            StoreConfig::AwsS3 { .. } => ConfiguredObjectStoreKind::AwsS3,
            StoreConfig::CloudflareR2 { .. } => ConfiguredObjectStoreKind::CloudflareR2,
            StoreConfig::GcpGcs { .. } => ConfiguredObjectStoreKind::GcpGcs,
            StoreConfig::AzureAbs { .. } => ConfiguredObjectStoreKind::AzureAbs,
        }
    }

    /// Whether this endpoint is one whose direct-put preconditions the live
    /// conformance suite has actually proven.
    ///
    /// `direct_put` hands a client a presigned URL and then trusts the
    /// provider to have enforced the signed checksum and create-only
    /// preconditions — completion never reads the bytes back. Only
    /// first-party AWS S3 and Cloudflare R2 endpoints have been run against
    /// that suite, so only they earn the trust. An endpoint override outside
    /// the provider's own domain family is some other implementation of the
    /// S3 API and is not covered by those runs.
    ///
    /// Providers without a presigner at all (local filesystem, GCS, Azure)
    /// never reach this question: `direct_put` is unavailable for them
    /// because [`ConfiguredObjectStore::transfer_issuer`] returns `None`.
    pub fn direct_put_is_proven(&self) -> bool {
        match self {
            StoreConfig::AwsS3 { endpoint_url, .. } => match endpoint_url {
                None => true,
                Some(endpoint_url) => endpoint_in_domain_families(
                    endpoint_url,
                    &["amazonaws.com", "amazonaws.com.cn"],
                ),
            },
            StoreConfig::CloudflareR2 { endpoint_url, .. } => {
                endpoint_in_domain_families(endpoint_url, &["r2.cloudflarestorage.com"])
            }
            StoreConfig::LocalFs { .. }
            | StoreConfig::GcpGcs { .. }
            | StoreConfig::AzureAbs { .. } => false,
        }
    }

    /// Builds the configured runtime object store for this provider.
    pub fn configured_object_store(&self) -> crate::object_store::Result<ConfiguredObjectStore> {
        match self {
            StoreConfig::LocalFs { root, key_prefix } => {
                ConfiguredObjectStore::local_fs(root, key_prefix.as_deref())
            }
            StoreConfig::AwsS3 {
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
            }),
            StoreConfig::CloudflareR2 {
                bucket,
                account_id,
                endpoint_url,
                access_key_id,
                secret_access_key,
                key_prefix,
            } => ConfiguredObjectStore::cloudflare_r2(CloudflareR2StoreConfig {
                bucket: bucket.clone(),
                account_id: account_id.clone(),
                endpoint_url: endpoint_url.clone(),
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                key_prefix: key_prefix.clone(),
            }),
            StoreConfig::GcpGcs {
                bucket,
                service_account_key_path,
                key_prefix,
            } => ConfiguredObjectStore::gcp_gcs(GcpGcsStoreConfig {
                bucket: bucket.clone(),
                service_account_key_path: service_account_key_path.clone(),
                key_prefix: key_prefix.clone(),
            }),
            StoreConfig::AzureAbs {
                account_name,
                container_name,
                access_key,
                endpoint_url,
                key_prefix,
            } => ConfiguredObjectStore::azure_abs(AzureAbsStoreConfig {
                account_name: account_name.clone(),
                container_name: container_name.clone(),
                access_key: access_key.clone(),
                endpoint_url: endpoint_url.clone(),
                key_prefix: key_prefix.clone(),
            }),
        }
    }

    /// Checks required fields and URL shapes, reporting `store.`-rooted field
    /// paths.
    pub fn validate(&self) -> Result<(), StoreConfigError> {
        match self {
            StoreConfig::LocalFs { root, .. } => {
                require_non_empty("store.root", root)?;
            }
            StoreConfig::AwsS3 {
                bucket,
                region,
                endpoint_url,
                access_key_id,
                secret_access_key,
                ..
            } => {
                require_non_empty("store.bucket", bucket)?;
                require_non_empty("store.region", region)?;
                require_non_empty("store.access_key_id", access_key_id.expose())?;
                require_non_empty("store.secret_access_key", secret_access_key.expose())?;
                if let Some(url) = endpoint_url {
                    validate_absolute_http_url("store.endpoint_url", url)?;
                }
            }
            StoreConfig::CloudflareR2 {
                bucket,
                account_id,
                endpoint_url,
                access_key_id,
                secret_access_key,
                ..
            } => {
                require_non_empty("store.bucket", bucket)?;
                require_non_empty("store.account_id", account_id)?;
                require_non_empty("store.access_key_id", access_key_id.expose())?;
                require_non_empty("store.secret_access_key", secret_access_key.expose())?;
                validate_absolute_http_url("store.endpoint_url", endpoint_url)?;
            }
            StoreConfig::GcpGcs {
                bucket,
                service_account_key_path,
                ..
            } => {
                require_non_empty("store.bucket", bucket)?;
                require_non_empty("store.service_account_key_path", service_account_key_path)?;
            }
            StoreConfig::AzureAbs {
                account_name,
                container_name,
                access_key,
                endpoint_url,
                ..
            } => {
                require_non_empty("store.account_name", account_name)?;
                require_non_empty("store.container_name", container_name)?;
                require_non_empty("store.access_key", access_key.expose())?;
                if let Some(url) = endpoint_url {
                    validate_absolute_http_url("store.endpoint_url", url)?;
                }
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
            StoreConfig::AwsS3 {
                access_key_id,
                secret_access_key,
                session_token,
                ..
            } => {
                *access_key_id = access_key_id.masked();
                *secret_access_key = secret_access_key.masked();
                *session_token = session_token.as_ref().map(SecretString::masked);
            }
            StoreConfig::CloudflareR2 {
                access_key_id,
                secret_access_key,
                ..
            } => {
                *access_key_id = access_key_id.masked();
                *secret_access_key = secret_access_key.masked();
            }
            StoreConfig::AzureAbs { access_key, .. } => {
                *access_key = access_key.masked();
            }
        }
        redacted
    }
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), StoreConfigError> {
    if value.trim().is_empty() {
        Err(StoreConfigError::MissingField { field })
    } else {
        Ok(())
    }
}

fn validate_absolute_http_url(field: &'static str, value: &str) -> Result<(), StoreConfigError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(StoreConfigError::MissingField { field });
    }

    let uri: Uri =
        trimmed.parse().map_err(
            |err: http::uri::InvalidUri| StoreConfigError::InvalidField {
                field,
                reason: err.to_string(),
            },
        )?;

    match uri.scheme_str() {
        Some("http" | "https") => {}
        Some(other) => {
            return Err(StoreConfigError::InvalidField {
                field,
                reason: format!("scheme must be http or https, got `{other}`"),
            });
        }
        None => {
            return Err(StoreConfigError::InvalidField {
                field,
                reason: "must be an absolute http or https URL".to_owned(),
            });
        }
    }

    if uri.authority().is_none() {
        return Err(StoreConfigError::InvalidField {
            field,
            reason: "must be an absolute http or https URL".to_owned(),
        });
    }

    Ok(())
}

fn endpoint_in_domain_families(endpoint_url: &str, domain_families: &[&str]) -> bool {
    let Ok(uri) = endpoint_url.parse::<Uri>() else {
        return false;
    };
    let Some(host) = uri.host() else {
        return false;
    };
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    domain_families.iter().any(|domain| {
        host == *domain
            || host
                .strip_suffix(domain)
                .is_some_and(|prefix| prefix.ends_with('.'))
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic)]
    // Config tests use panic in unexpected match arms for precise diagnostics.

    use super::{StoreConfig, StoreConfigError};
    use crate::ConfiguredObjectStoreKind;
    use std::path::{Path, PathBuf};

    fn parse(contents: &str) -> StoreConfig {
        toml::from_str(contents).expect("parse store config")
    }

    #[test]
    fn parses_all_provider_kinds_and_reports_their_kind() {
        let cases: [(&str, ConfiguredObjectStoreKind); 5] = [
            (
                "kind = \"local-fs\"\nroot = \"/tmp/store\"",
                ConfiguredObjectStoreKind::LocalFs,
            ),
            (
                r#"
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
access_key_id = "access"
secret_access_key = "secret"
"#,
                ConfiguredObjectStoreKind::AwsS3,
            ),
            (
                r#"
kind = "cloudflare-r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "https://account.r2.cloudflarestorage.com"
access_key_id = "access"
secret_access_key = "secret"
"#,
                ConfiguredObjectStoreKind::CloudflareR2,
            ),
            (
                r#"
kind = "gcp-gcs"
bucket = "bucket"
service_account_key_path = "/tmp/service-account.json"
"#,
                ConfiguredObjectStoreKind::GcpGcs,
            ),
            (
                r#"
kind = "azure-abs"
account_name = "account"
container_name = "container"
access_key = "key"
"#,
                ConfiguredObjectStoreKind::AzureAbs,
            ),
        ];

        for (contents, kind) in cases {
            let config = parse(contents);
            assert_eq!(config.kind(), kind);
            config.validate().expect("valid config");
        }
    }

    #[test]
    fn direct_put_is_proven_only_for_first_party_s3_and_r2_endpoints() {
        let aws_default = parse(
            r#"
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
access_key_id = "access"
secret_access_key = "secret"
"#,
        );
        let aws_first_party = parse(
            r#"
kind = "aws-s3"
bucket = "bucket"
region = "cn-north-1"
endpoint_url = "https://bucket.s3.cn-north-1.amazonaws.com.cn"
access_key_id = "access"
secret_access_key = "secret"
"#,
        );
        let aws_custom = parse(
            r#"
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
endpoint_url = "https://gateway.example"
access_key_id = "access"
secret_access_key = "secret"
"#,
        );
        let r2_first_party = parse(
            r#"
kind = "cloudflare-r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "https://account.r2.cloudflarestorage.com"
access_key_id = "access"
secret_access_key = "secret"
"#,
        );
        let r2_custom = parse(
            r#"
kind = "cloudflare-r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "https://gateway.example"
access_key_id = "access"
secret_access_key = "secret"
"#,
        );
        let gcs = parse(
            r#"
kind = "gcp-gcs"
bucket = "bucket"
service_account_key_path = "/tmp/service-account.json"
"#,
        );

        assert!(aws_default.direct_put_is_proven());
        assert!(aws_first_party.direct_put_is_proven());
        assert!(!aws_custom.direct_put_is_proven());
        assert!(r2_first_party.direct_put_is_proven());
        assert!(!r2_custom.direct_put_is_proven());
        // GCS has no presigner at all, so it never reaches the question.
        assert!(!gcs.direct_put_is_proven());
    }

    #[test]
    fn validate_reports_store_rooted_field_paths() {
        let blank_bucket = parse(
            r#"
kind = "cloudflare-r2"
bucket = " "
account_id = "account"
endpoint_url = "https://example.com"
access_key_id = "access"
secret_access_key = "secret"
"#,
        );
        assert_eq!(
            blank_bucket.validate(),
            Err(StoreConfigError::MissingField {
                field: "store.bucket"
            })
        );

        let bad_scheme = parse(
            r#"
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
endpoint_url = "ftp://example.com"
access_key_id = "access"
secret_access_key = "secret"
"#,
        );
        match bad_scheme.validate() {
            Err(StoreConfigError::InvalidField { field, reason }) => {
                assert_eq!(field, "store.endpoint_url");
                assert!(reason.contains("ftp"));
            }
            other => panic!("expected invalid endpoint_url, got {other:?}"),
        }

        let blank_azure_account = parse(
            r#"
kind = "azure-abs"
account_name = " "
container_name = "container"
access_key = "key"
"#,
        );
        assert_eq!(
            blank_azure_account.validate(),
            Err(StoreConfigError::MissingField {
                field: "store.account_name"
            })
        );
    }

    #[test]
    fn unknown_keys_are_rejected_and_named() {
        // `deny_unknown_fields` must keep working through the internally
        // tagged (`kind = ...`) enum representation: a typo'd key in the
        // store table has to fail the parse and name the offending key.
        let error = toml::from_str::<StoreConfig>(
            r#"
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
access_key_id = "access"
secret_access_key = "secret"
buckt = "typo"
"#,
        )
        .expect_err("typo'd key must be rejected");

        let message = error.to_string();
        assert!(
            message.contains("buckt"),
            "error must name the unknown key, got: {message}"
        );
    }

    #[test]
    fn debug_output_redacts_credentials() {
        let config = parse(
            r#"
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
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
kind = "cloudflare-r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "https://account.r2.cloudflarestorage.com"
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

    #[test]
    fn serialization_round_trips_the_store_table() {
        let config = parse(
            r#"
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
access_key_id = "access"
secret_access_key = "secret"
key_prefix = "demo"
"#,
        );

        let rendered = toml::to_string_pretty(&config).expect("serialize store config");
        assert!(rendered.contains("kind = \"aws-s3\""));
        assert!(!rendered.contains("session_token"));
        assert!(!rendered.contains("endpoint_url"));

        let reparsed: StoreConfig = toml::from_str(&rendered).expect("reparse store config");
        assert_eq!(reparsed, config);
    }

    /// Every example config in `configs/*.example.toml` must keep parsing
    /// into the shared [`StoreConfig`]: the examples document the frozen TOML
    /// shape.
    #[test]
    fn example_configs_store_sections_parse() {
        let configs_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs");
        let mut store_sections = 0usize;

        for path in example_config_paths(&configs_dir) {
            let contents = std::fs::read_to_string(&path)
                .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
            let value: toml::Value = toml::from_str(&contents)
                .unwrap_or_else(|err| panic!("parse {}: {err}", path.display()));

            for store in store_tables(&value) {
                let config: StoreConfig = store.clone().try_into().unwrap_or_else(|err| {
                    panic!("store section in {} must parse: {err}", path.display())
                });
                config.validate().unwrap_or_else(|err| {
                    panic!("store section in {} must validate: {err}", path.display())
                });
                store_sections += 1;
            }
        }

        // Five server examples plus the embedded CLI example.
        assert!(
            store_sections >= 6,
            "expected at least 6 store sections across configs/*.example.toml, found {store_sections}"
        );
    }

    fn example_config_paths(configs_dir: &Path) -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = std::fs::read_dir(configs_dir)
            .expect("read configs directory")
            .map(|entry| entry.expect("read configs entry").path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(".example.toml"))
            })
            .collect();
        paths.sort();
        assert!(!paths.is_empty(), "no example configs found");
        paths
    }

    /// Collects `[store]` tables from server configs and
    /// `[profiles.<name>.store]` tables from CLI configs.
    fn store_tables(value: &toml::Value) -> Vec<&toml::Value> {
        let mut sections = Vec::new();
        if let Some(store) = value.get("store") {
            sections.push(store);
        }
        if let Some(profiles) = value.get("profiles").and_then(toml::Value::as_table) {
            for profile in profiles.values() {
                if let Some(store) = profile.get("store") {
                    sections.push(store);
                }
            }
        }
        sections
    }
}
