//! The shared serde-facing object-store provider configuration.
//!
//! Both the server config file (`[store]`) and the CLI profile config
//! (`[profiles.<name>.store]`) deserialize into [`StoreConfig`], validate it
//! with [`StoreConfig::validate`], and construct the runtime store with
//! [`StoreConfig::configured_object_store`]. The TOML shape is kind-tagged
//! and kebab-case, documented by the examples in `configs/`.

use crate::abs::AzureAbsStoreConfig;
use crate::configured::{
    endpoint_host_is_proven, AWS_S3_PROVEN_DOMAINS, CLOUDFLARE_R2_PROVEN_DOMAINS,
};
use crate::gcs::GcpGcsStoreConfig;
use crate::s3_compatible::{AwsS3StoreConfig, CloudflareR2StoreConfig};
use crate::secret::SecretString;
use crate::{ConfiguredObjectStore, ConfiguredObjectStoreKind};
use http::Uri;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Environment variable the S3-compatible providers read their access-key id
/// from when the config file leaves it out.
pub const ACCESS_KEY_ID_ENV: &str = "AWS_ACCESS_KEY_ID";
/// Environment variable the S3-compatible providers read their secret access
/// key from when the config file leaves it out.
pub const SECRET_ACCESS_KEY_ENV: &str = "AWS_SECRET_ACCESS_KEY";
/// Environment variable a temporary AWS credential's session token comes
/// from when the config file leaves it out.
pub const SESSION_TOKEN_ENV: &str = "AWS_SESSION_TOKEN";

/// Provider selection plus credentials, as written in config files.
///
/// Serialization is transparent for secret fields and therefore writes the
/// real credentials; only serialize a [`StoreConfig::redacted`] copy into
/// display output.
///
/// # Credential precedence
///
/// The S3-compatible credential fields may be left out of the file and
/// supplied through the standard environment instead — see
/// [`StoreConfig::apply_env_credentials`]. A non-blank value in the file
/// always wins; blank environment values are ignored. Loading a config is
/// what decides whether that fallback applies, so each caller says for
/// itself: the server's loader applies it, the CLI's profile loader does not
/// (a profile stores what it was created with).
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
        /// Absent here means `AWS_ACCESS_KEY_ID`, for loaders that apply the
        /// environment fallback.
        #[serde(default)]
        access_key_id: SecretString,
        /// Secret access key used for provider requests and direct-upload signing.
        /// Absent here means `AWS_SECRET_ACCESS_KEY`, for loaders that apply
        /// the environment fallback.
        #[serde(default)]
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
        /// S3-compatible access-key id used for requests and direct-upload
        /// signing. R2 speaks the S3 API, so an absent value here means
        /// `AWS_ACCESS_KEY_ID` too.
        #[serde(default)]
        access_key_id: SecretString,
        /// S3-compatible secret used for requests and direct-upload signing.
        /// Absent here means `AWS_SECRET_ACCESS_KEY`.
        #[serde(default)]
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
    /// Reports a required credential absent from the file and from the
    /// environment variable that can stand in for it.
    #[error("missing `{field}`; set it in the config or export `{env}`")]
    MissingCredential {
        /// `store.`-rooted path suitable for direct configuration diagnostics.
        field: &'static str,
        /// Standard environment variable this credential also reads from.
        env: &'static str,
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
            | StoreConfigError::MissingCredential { field, .. }
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

    /// Builds the configured runtime object store for this provider.
    ///
    /// Whether the result can authorize direct transfers is settled here, by
    /// construction: read
    /// [`ConfiguredObjectStore::direct_transfers`](crate::ConfiguredObjectStore::direct_transfers)
    /// rather than asking this configuration a second time.
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

    /// Fills blank S3-compatible credential fields from the standard
    /// environment variables, so a config file need not carry secrets at
    /// all.
    ///
    /// A non-blank value in the file always wins, and a blank environment
    /// value is ignored rather than stored and later rejected. Providers
    /// with no standard variable of their own — local filesystem, GCS,
    /// Azure — are untouched: inventing a name for them would be a guess.
    ///
    /// Call this after decoding and before [`Self::validate`], so a config
    /// that relies on the environment passes validation and one that does
    /// not is reported against the field it actually lacks.
    pub fn apply_env_credentials(&mut self) {
        self.apply_env_credentials_from(|name| std::env::var(name).ok());
    }

    fn apply_env_credentials_from(&mut self, lookup: impl Fn(&str) -> Option<String>) {
        match self {
            StoreConfig::AwsS3 {
                access_key_id,
                secret_access_key,
                session_token,
                ..
            } => {
                fill_secret(access_key_id, &lookup, ACCESS_KEY_ID_ENV);
                fill_secret(secret_access_key, &lookup, SECRET_ACCESS_KEY_ENV);
                if session_token.is_none() {
                    *session_token = non_blank(lookup(SESSION_TOKEN_ENV)).map(SecretString::new);
                }
            }
            StoreConfig::CloudflareR2 {
                access_key_id,
                secret_access_key,
                ..
            } => {
                fill_secret(access_key_id, &lookup, ACCESS_KEY_ID_ENV);
                fill_secret(secret_access_key, &lookup, SECRET_ACCESS_KEY_ENV);
            }
            StoreConfig::LocalFs { .. }
            | StoreConfig::GcpGcs { .. }
            | StoreConfig::AzureAbs { .. } => {}
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
                require_credential(
                    "store.access_key_id",
                    ACCESS_KEY_ID_ENV,
                    access_key_id.expose(),
                )?;
                require_credential(
                    "store.secret_access_key",
                    SECRET_ACCESS_KEY_ENV,
                    secret_access_key.expose(),
                )?;
                if let Some(url) = endpoint_url {
                    validate_absolute_http_url("store.endpoint_url", url)?;
                    require_tls_on_proven_domains(
                        "store.endpoint_url",
                        url,
                        AWS_S3_PROVEN_DOMAINS,
                    )?;
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
                require_credential(
                    "store.access_key_id",
                    ACCESS_KEY_ID_ENV,
                    access_key_id.expose(),
                )?;
                require_credential(
                    "store.secret_access_key",
                    SECRET_ACCESS_KEY_ENV,
                    secret_access_key.expose(),
                )?;
                validate_absolute_http_url("store.endpoint_url", endpoint_url)?;
                require_tls_on_proven_domains(
                    "store.endpoint_url",
                    endpoint_url,
                    CLOUDFLARE_R2_PROVEN_DOMAINS,
                )?;
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

/// Like [`require_non_empty`], for a field the standard environment can also
/// supply; the failure names both places.
fn require_credential(
    field: &'static str,
    env: &'static str,
    value: &str,
) -> Result<(), StoreConfigError> {
    if value.trim().is_empty() {
        Err(StoreConfigError::MissingCredential { field, env })
    } else {
        Ok(())
    }
}

/// Replaces a blank secret with the environment's value, if it has one.
fn fill_secret(
    secret: &mut SecretString,
    lookup: &impl Fn(&str) -> Option<String>,
    env: &'static str,
) {
    if !secret.expose().trim().is_empty() {
        return;
    }
    if let Some(value) = non_blank(lookup(env)) {
        *secret = SecretString::new(value);
    }
}

fn non_blank(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

/// Refuses a provider's own domain addressed over plain `http`.
///
/// Reaching one of these endpoints without TLS is a misconfiguration, not a
/// choice: they all serve TLS, and the deployment would otherwise mint
/// presigned URLs — bearer capabilities to read or write one object — into
/// cleartext. Any other host is left alone: a private gateway on `http` is a
/// legitimate setup, and it simply earns no direct transfers.
fn require_tls_on_proven_domains(
    field: &'static str,
    value: &str,
    domain_families: &[&str],
) -> Result<(), StoreConfigError> {
    let Ok(uri) = value.trim().parse::<Uri>() else {
        // Shape was already reported by the caller's URL validation.
        return Ok(());
    };
    if uri.scheme_str() == Some("https") || !endpoint_host_is_proven(&uri, domain_families) {
        return Ok(());
    }
    Err(StoreConfigError::InvalidField {
        field,
        reason: format!(
            "`{}` is a provider endpoint and must be reached over https: a presigned URL is a \
             bearer capability, and http would put it on the wire in cleartext",
            uri.host().unwrap_or_default()
        ),
    })
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

#[cfg(test)]
mod tests {
    #![allow(clippy::panic)]
    // Config tests use panic in unexpected match arms for precise diagnostics.

    use super::{StoreConfig, StoreConfigError};
    use crate::secret::SecretString;
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

    /// A provider's own endpoint on plain `http` is a misconfiguration, and
    /// the message says what to do about it. Any other host is left alone —
    /// a private gateway on `http` is a legitimate setup that simply earns
    /// no direct transfers.
    #[test]
    fn a_provider_endpoint_without_tls_is_rejected_by_name() {
        for (contents, host) in [
            (
                r#"
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
endpoint_url = "http://bucket.s3.amazonaws.com"
access_key_id = "access"
secret_access_key = "secret"
"#,
                "bucket.s3.amazonaws.com",
            ),
            (
                r#"
kind = "cloudflare-r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "http://account.r2.cloudflarestorage.com"
access_key_id = "access"
secret_access_key = "secret"
"#,
                "account.r2.cloudflarestorage.com",
            ),
        ] {
            match parse(contents).validate() {
                Err(StoreConfigError::InvalidField { field, reason }) => {
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
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
endpoint_url = "http://127.0.0.1:9000"
access_key_id = "access"
secret_access_key = "secret"
"#,
        )
        .validate()
        .expect("a private http gateway is a valid configuration");
    }

    #[test]
    fn credentials_left_out_of_the_file_come_from_the_environment() {
        let environment = |name: &str| match name {
            "AWS_ACCESS_KEY_ID" => Some("env-access".to_owned()),
            "AWS_SECRET_ACCESS_KEY" => Some("env-secret".to_owned()),
            "AWS_SESSION_TOKEN" => Some("env-session".to_owned()),
            _ => None,
        };

        // An S3 store may name no credentials at all.
        let mut aws = parse(
            r#"
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
"#,
        );
        aws.apply_env_credentials_from(environment);
        aws.validate().expect("the environment completed the store");
        match &aws {
            StoreConfig::AwsS3 {
                access_key_id,
                secret_access_key,
                session_token,
                ..
            } => {
                assert_eq!(access_key_id.expose(), "env-access");
                assert_eq!(secret_access_key.expose(), "env-secret");
                assert_eq!(
                    session_token.as_ref().map(SecretString::expose),
                    Some("env-session")
                );
            }
            other => panic!("expected an aws-s3 store, got {other:?}"),
        }

        // R2 reads the same S3-compatible variables.
        let mut r2 = parse(
            r#"
kind = "cloudflare-r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "https://account.r2.cloudflarestorage.com"
"#,
        );
        r2.apply_env_credentials_from(environment);
        r2.validate().expect("the environment completed the store");

        // A value in the file wins, and a blank environment value is ignored.
        let mut explicit = parse(
            r#"
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
access_key_id = "file-access"
secret_access_key = "file-secret"
"#,
        );
        explicit.apply_env_credentials_from(environment);
        let blank = |_: &str| Some("   ".to_owned());
        let mut unfilled = parse(
            r#"
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
"#,
        );
        unfilled.apply_env_credentials_from(blank);
        match (&explicit, &unfilled) {
            (
                StoreConfig::AwsS3 {
                    access_key_id,
                    session_token,
                    ..
                },
                StoreConfig::AwsS3 {
                    access_key_id: unfilled_key,
                    session_token: unfilled_token,
                    ..
                },
            ) => {
                assert_eq!(access_key_id.expose(), "file-access");
                assert_eq!(
                    session_token.as_ref().map(SecretString::expose),
                    Some("env-session"),
                    "an absent optional field still takes the environment"
                );
                assert!(unfilled_key.expose().is_empty());
                assert!(unfilled_token.is_none());
            }
            other => panic!("expected two aws-s3 stores, got {other:?}"),
        }

        // Providers with no standard variable are untouched.
        let mut gcs = parse(
            r#"
kind = "gcp-gcs"
bucket = "bucket"
service_account_key_path = "/tmp/service-account.json"
"#,
        );
        let before = gcs.clone();
        gcs.apply_env_credentials_from(environment);
        assert_eq!(gcs, before);
    }

    #[test]
    fn a_credential_missing_everywhere_names_its_environment_variable() {
        let store = parse(
            r#"
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
secret_access_key = "secret"
"#,
        );

        let error = store.validate().expect_err("no access key anywhere");
        assert_eq!(
            error,
            StoreConfigError::MissingCredential {
                field: "store.access_key_id",
                env: "AWS_ACCESS_KEY_ID",
            }
        );
        assert_eq!(
            error.to_string(),
            "missing `store.access_key_id`; set it in the config or export `AWS_ACCESS_KEY_ID`"
        );
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
