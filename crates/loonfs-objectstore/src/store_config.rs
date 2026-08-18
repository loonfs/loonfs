//! The shared serde-facing object-store provider configuration.
//!
//! Both the server config file (`[store]`) and the CLI profile config
//! (`[profiles.<name>.store]`) deserialize into [`StoreConfig`], validate it
//! with [`StoreConfig::validate`], and construct the runtime store with
//! [`StoreConfig::configured_object_store`]. The TOML shape is kind-tagged
//! and kebab-case. Each binary ships the examples that document it, in the
//! `config/` directory of its own crate.

use crate::abs::AzureAbsStoreConfig;
use crate::configured::{
    endpoint_host_is_proven, AWS_S3_PROVEN_DOMAINS, CLOUDFLARE_R2_PROVEN_DOMAINS,
};
use crate::gcs::GcpGcsStoreConfig;
use crate::s3_compatible::{AwsS3StoreConfig, CloudflareR2StoreConfig};
use crate::{ConfiguredObjectStore, ConfiguredObjectStoreKind, ObjectStoreError};
use http::Uri;
use loonfs_api::SecretString;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Environment variable ambient S3-compatible credentials read their access-key id from.
pub const ACCESS_KEY_ID_ENV: &str = "AWS_ACCESS_KEY_ID";
/// Environment variable ambient S3-compatible credentials read their secret access key from.
pub const SECRET_ACCESS_KEY_ENV: &str = "AWS_SECRET_ACCESS_KEY";
/// Environment variable ambient AWS credentials read an optional session token from.
pub const SESSION_TOKEN_ENV: &str = "AWS_SESSION_TOKEN";

/// AWS S3 credential source, stored separately from provider settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum AwsS3Credentials {
    /// Resolves credentials from the process environment when the store is constructed.
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
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
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
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum GcpGcsCredentials {
    /// Reads one service-account JSON file for requests and direct-transfer signing.
    ///
    /// Store construction fails if the key cannot sign direct-transfer URLs.
    ServiceAccountFile {
        /// Filesystem path to the service-account JSON.
        path: String,
    },
}

/// Azure Blob Storage credential source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum AzureAbsCredentials {
    /// Uses a stored shared account key.
    AccessKey {
        /// Shared account key used for request authentication.
        access_key: SecretString,
    },
}

/// Provider selection, settings, and an explicit credential source, as written in config files.
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
        /// Credential source resolved when the store is constructed.
        credentials: AwsS3Credentials,
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
        /// Credential source resolved when the store is constructed.
        credentials: CloudflareR2Credentials,
        /// Logical prefix applied inside the bucket, or `None` to expose its root.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key_prefix: Option<String>,
    },
    /// Connects to Google Cloud Storage through its native generation-aware API.
    GcpGcs {
        /// GCS bucket that acts as the physical store root.
        bucket: String,
        /// Credential source used for requests and direct-transfer signing.
        credentials: GcpGcsCredentials,
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
        /// Credential source used for request authentication.
        credentials: AzureAbsCredentials,
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
#[non_exhaustive]
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

    /// The configured credential-source kind, or `None` for local filesystem stores.
    pub fn credentials_kind(&self) -> Option<&'static str> {
        match self {
            StoreConfig::LocalFs { .. } => None,
            StoreConfig::AwsS3 { credentials, .. } => Some(match credentials {
                AwsS3Credentials::Ambient {} => "ambient",
                AwsS3Credentials::Static { .. } => "static",
            }),
            StoreConfig::CloudflareR2 { credentials, .. } => Some(match credentials {
                CloudflareR2Credentials::Ambient {} => "ambient",
                CloudflareR2Credentials::Static { .. } => "static",
            }),
            StoreConfig::GcpGcs { .. } => Some("service-account-file"),
            StoreConfig::AzureAbs { .. } => Some("access-key"),
        }
    }

    /// Builds the configured runtime object store for this provider.
    ///
    /// This also determines whether the store supports direct transfers. Read
    /// [`ConfiguredObjectStore::direct_transfers`](crate::ConfiguredObjectStore::direct_transfers)
    /// from the constructed store instead of checking this config again.
    pub fn configured_object_store(&self) -> crate::object_store::Result<ConfiguredObjectStore> {
        match self {
            StoreConfig::LocalFs { root, key_prefix } => {
                ConfiguredObjectStore::local_fs(root, key_prefix.as_deref())
            }
            StoreConfig::AwsS3 {
                bucket,
                region,
                endpoint_url,
                credentials,
                key_prefix,
                force_path_style,
            } => {
                let (access_key_id, secret_access_key, session_token) =
                    resolve_aws_s3_credentials(credentials, |name| std::env::var(name).ok())?;
                ConfiguredObjectStore::aws_s3(AwsS3StoreConfig {
                    bucket: bucket.clone(),
                    region: region.clone(),
                    endpoint_url: endpoint_url.clone(),
                    access_key_id,
                    secret_access_key,
                    session_token,
                    key_prefix: key_prefix.clone(),
                    force_path_style: *force_path_style,
                })
            }
            StoreConfig::CloudflareR2 {
                bucket,
                account_id,
                endpoint_url,
                credentials,
                key_prefix,
            } => {
                let (access_key_id, secret_access_key) =
                    resolve_r2_credentials(credentials, |name| std::env::var(name).ok())?;
                ConfiguredObjectStore::cloudflare_r2(CloudflareR2StoreConfig {
                    bucket: bucket.clone(),
                    account_id: account_id.clone(),
                    endpoint_url: endpoint_url.clone(),
                    access_key_id,
                    secret_access_key,
                    key_prefix: key_prefix.clone(),
                })
            }
            StoreConfig::GcpGcs {
                bucket,
                credentials,
                key_prefix,
            } => match credentials {
                GcpGcsCredentials::ServiceAccountFile { path } => {
                    ConfiguredObjectStore::gcp_gcs(GcpGcsStoreConfig {
                        bucket: bucket.clone(),
                        service_account_key_path: path.clone(),
                        key_prefix: key_prefix.clone(),
                    })
                }
            },
            StoreConfig::AzureAbs {
                account_name,
                container_name,
                credentials,
                endpoint_url,
                key_prefix,
            } => match credentials {
                AzureAbsCredentials::AccessKey { access_key } => {
                    ConfiguredObjectStore::azure_abs(AzureAbsStoreConfig {
                        account_name: account_name.clone(),
                        container_name: container_name.clone(),
                        access_key: access_key.clone(),
                        endpoint_url: endpoint_url.clone(),
                        key_prefix: key_prefix.clone(),
                    })
                }
            },
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
                credentials,
                ..
            } => {
                require_non_empty("store.bucket", bucket)?;
                require_non_empty("store.region", region)?;
                validate_aws_s3_credentials(credentials)?;
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
                credentials,
                ..
            } => {
                require_non_empty("store.bucket", bucket)?;
                require_non_empty("store.account_id", account_id)?;
                validate_r2_credentials(credentials)?;
                validate_absolute_http_url("store.endpoint_url", endpoint_url)?;
                require_tls_on_proven_domains(
                    "store.endpoint_url",
                    endpoint_url,
                    CLOUDFLARE_R2_PROVEN_DOMAINS,
                )?;
            }
            StoreConfig::GcpGcs {
                bucket,
                credentials,
                ..
            } => {
                require_non_empty("store.bucket", bucket)?;
                match credentials {
                    GcpGcsCredentials::ServiceAccountFile { path } => {
                        require_non_empty("store.credentials.path", path)?;
                    }
                }
            }
            StoreConfig::AzureAbs {
                account_name,
                container_name,
                credentials,
                endpoint_url,
                ..
            } => {
                require_non_empty("store.account_name", account_name)?;
                require_non_empty("store.container_name", container_name)?;
                match credentials {
                    AzureAbsCredentials::AccessKey { access_key } => {
                        require_non_empty("store.credentials.access_key", access_key.expose())?;
                    }
                }
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
            StoreConfig::AwsS3 { credentials, .. } => {
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
            StoreConfig::CloudflareR2 { credentials, .. } => {
                if let CloudflareR2Credentials::Static {
                    access_key_id,
                    secret_access_key,
                } = credentials
                {
                    *access_key_id = access_key_id.masked();
                    *secret_access_key = secret_access_key.masked();
                }
            }
            StoreConfig::AzureAbs { credentials, .. } => match credentials {
                AzureAbsCredentials::AccessKey { access_key } => {
                    *access_key = access_key.masked();
                }
            },
        }
        redacted
    }
}

fn validate_aws_s3_credentials(credentials: &AwsS3Credentials) -> Result<(), StoreConfigError> {
    if let AwsS3Credentials::Static {
        access_key_id,
        secret_access_key,
        session_token,
    } = credentials
    {
        require_non_empty("store.credentials.access_key_id", access_key_id.expose())?;
        require_non_empty(
            "store.credentials.secret_access_key",
            secret_access_key.expose(),
        )?;
        if let Some(session_token) = session_token {
            require_non_empty("store.credentials.session_token", session_token.expose())?;
        }
    }
    Ok(())
}

fn validate_r2_credentials(credentials: &CloudflareR2Credentials) -> Result<(), StoreConfigError> {
    if let CloudflareR2Credentials::Static {
        access_key_id,
        secret_access_key,
    } = credentials
    {
        require_non_empty("store.credentials.access_key_id", access_key_id.expose())?;
        require_non_empty(
            "store.credentials.secret_access_key",
            secret_access_key.expose(),
        )?;
    }
    Ok(())
}

fn resolve_aws_s3_credentials(
    credentials: &AwsS3Credentials,
    lookup: impl Fn(&str) -> Option<String>,
) -> crate::object_store::Result<(SecretString, SecretString, Option<SecretString>)> {
    match credentials {
        AwsS3Credentials::Static {
            access_key_id,
            secret_access_key,
            session_token,
        } => Ok((
            access_key_id.clone(),
            secret_access_key.clone(),
            session_token.clone(),
        )),
        AwsS3Credentials::Ambient {} => {
            let access_key_id = non_blank(lookup(ACCESS_KEY_ID_ENV));
            let secret_access_key = non_blank(lookup(SECRET_ACCESS_KEY_ENV));
            match (access_key_id, secret_access_key) {
                (Some(access_key_id), Some(secret_access_key)) => Ok((
                    SecretString::new(access_key_id),
                    SecretString::new(secret_access_key),
                    non_blank(lookup(SESSION_TOKEN_ENV)).map(SecretString::new),
                )),
                _ => Err(missing_ambient_credentials("aws-s3", true)),
            }
        }
    }
}

fn resolve_r2_credentials(
    credentials: &CloudflareR2Credentials,
    lookup: impl Fn(&str) -> Option<String>,
) -> crate::object_store::Result<(SecretString, SecretString)> {
    match credentials {
        CloudflareR2Credentials::Static {
            access_key_id,
            secret_access_key,
        } => Ok((access_key_id.clone(), secret_access_key.clone())),
        CloudflareR2Credentials::Ambient {} => {
            let access_key_id = non_blank(lookup(ACCESS_KEY_ID_ENV));
            let secret_access_key = non_blank(lookup(SECRET_ACCESS_KEY_ENV));
            match (access_key_id, secret_access_key) {
                (Some(access_key_id), Some(secret_access_key)) => Ok((
                    SecretString::new(access_key_id),
                    SecretString::new(secret_access_key),
                )),
                _ => Err(missing_ambient_credentials("cloudflare-r2", false)),
            }
        }
    }
}

fn missing_ambient_credentials(provider: &str, session_token: bool) -> ObjectStoreError {
    let optional_session = if session_token {
        format!("; `{SESSION_TOKEN_ENV}` is optional")
    } else {
        String::new()
    };
    ObjectStoreError::Configuration(format!(
        "{provider} ambient credentials require non-blank `{ACCESS_KEY_ID_ENV}` and \
         `{SECRET_ACCESS_KEY_ENV}`{optional_session}; set them in the environment or configure \
         `store.credentials.kind = \"static\"`"
    ))
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), StoreConfigError> {
    if value.trim().is_empty() {
        Err(StoreConfigError::MissingField { field })
    } else {
        Ok(())
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

    use super::{
        resolve_aws_s3_credentials, resolve_r2_credentials, AwsS3Credentials,
        CloudflareR2Credentials, StoreConfig, StoreConfigError,
    };
    use crate::ConfiguredObjectStoreKind;
    use loonfs_api::SecretString;

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

[credentials]
kind = "static"
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

[credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
"#,
                ConfiguredObjectStoreKind::CloudflareR2,
            ),
            (
                r#"
kind = "gcp-gcs"
bucket = "bucket"

[credentials]
kind = "service-account-file"
path = "/tmp/service-account.json"
"#,
                ConfiguredObjectStoreKind::GcpGcs,
            ),
            (
                r#"
kind = "azure-abs"
account_name = "account"
container_name = "container"

[credentials]
kind = "access-key"
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

[credentials]
kind = "static"
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

[credentials]
kind = "static"
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
    fn ambient_credentials_resolve_without_mutating_the_serialized_source() {
        let environment = |name: &str| match name {
            "AWS_ACCESS_KEY_ID" => Some("env-access".to_owned()),
            "AWS_SECRET_ACCESS_KEY" => Some("env-secret".to_owned()),
            "AWS_SESSION_TOKEN" => Some("env-session".to_owned()),
            _ => None,
        };

        let aws = parse(
            r#"
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"

[credentials]
kind = "ambient"
"#,
        );
        aws.validate()
            .expect("ambient is a valid credential source");
        let StoreConfig::AwsS3 { credentials, .. } = &aws else {
            panic!("expected aws-s3")
        };
        let (access_key_id, secret_access_key, session_token) =
            resolve_aws_s3_credentials(credentials, environment).expect("resolve ambient aws");
        assert_eq!(access_key_id.expose(), "env-access");
        assert_eq!(secret_access_key.expose(), "env-secret");
        assert_eq!(
            session_token.as_ref().map(SecretString::expose),
            Some("env-session")
        );
        let rendered = toml::to_string_pretty(&aws).expect("serialize ambient aws");
        assert!(rendered.contains("kind = \"ambient\""));
        assert!(!rendered.contains("env-access"));
        assert!(!rendered.contains("env-secret"));
        assert!(!rendered.contains("env-session"));

        let r2 = parse(
            r#"
kind = "cloudflare-r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "https://account.r2.cloudflarestorage.com"

[credentials]
kind = "ambient"
"#,
        );
        let StoreConfig::CloudflareR2 { credentials, .. } = &r2 else {
            panic!("expected cloudflare-r2")
        };
        let (access_key_id, secret_access_key) =
            resolve_r2_credentials(credentials, environment).expect("resolve ambient r2");
        assert_eq!(access_key_id.expose(), "env-access");
        assert_eq!(secret_access_key.expose(), "env-secret");
    }

    #[test]
    fn missing_runtime_ambient_credentials_are_provider_specific_and_actionable() {
        let missing = |_: &str| None;
        let aws_error = resolve_aws_s3_credentials(&AwsS3Credentials::Ambient {}, missing)
            .expect_err("missing aws ambient credentials");
        let aws_message = aws_error.to_string();
        assert!(
            aws_message.contains("aws-s3 ambient credentials"),
            "{aws_message}"
        );
        assert!(aws_message.contains("AWS_ACCESS_KEY_ID"), "{aws_message}");
        assert!(
            aws_message.contains("AWS_SECRET_ACCESS_KEY"),
            "{aws_message}"
        );
        assert!(aws_message.contains("AWS_SESSION_TOKEN"), "{aws_message}");
        assert!(aws_message.contains("kind = \"static\""), "{aws_message}");

        let r2_error = resolve_r2_credentials(&CloudflareR2Credentials::Ambient {}, missing)
            .expect_err("missing r2 ambient credentials");
        let r2_message = r2_error.to_string();
        assert!(
            r2_message.contains("cloudflare-r2 ambient credentials"),
            "{r2_message}"
        );
        assert!(r2_message.contains("AWS_ACCESS_KEY_ID"), "{r2_message}");
        assert!(r2_message.contains("AWS_SECRET_ACCESS_KEY"), "{r2_message}");
    }

    #[test]
    fn validate_reports_store_rooted_field_paths() {
        let blank_bucket = parse(
            r#"
kind = "cloudflare-r2"
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

[credentials]
kind = "static"
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

[credentials]
kind = "access-key"
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
kind = "aws-s3"
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

        let legacy_flat = toml::from_str::<StoreConfig>(
            r#"
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
access_key_id = "legacy-access"
secret_access_key = "legacy-secret"
"#,
        )
        .expect_err("version-1 flat credentials must not decode")
        .to_string();
        assert!(legacy_flat.contains("access_key_id"), "{legacy_flat}");
    }

    #[test]
    fn debug_output_redacts_credentials() {
        let config = parse(
            r#"
kind = "aws-s3"
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
kind = "cloudflare-r2"
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

    #[test]
    fn serialization_round_trips_the_store_table() {
        let config = parse(
            r#"
kind = "aws-s3"
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
        assert!(rendered.contains("kind = \"aws-s3\""));
        assert!(!rendered.contains("session_token"));
        assert!(!rendered.contains("endpoint_url"));

        let reparsed: StoreConfig = toml::from_str(&rendered).expect("reparse store config");
        assert_eq!(reparsed, config);
    }
}
