//! Builds each provider's store configuration and credentials, and a remote
//! profile's connection, from flags or prompts.

use crate::args::{
    BucketAddressing, CredentialSource, GcsStoreArgs, LocalStoreArgs, R2StoreArgs,
    RemoteServerArgs, S3StoreArgs,
};
use crate::commands::profile_config::FieldSource;
use crate::commands::profile_update::{prompt_aws_credentials, prompt_r2_credentials};
use crate::config::{validate_remote_client_config, ProfileConnection, StoreConfig};
use crate::error::{CliError, Result};
use crate::prompt;
use loonfs_objectstore::gcs::GcpGcsStoreConfig;
use loonfs_objectstore::local_fs_store::LocalFsStoreConfig;
use loonfs_objectstore::s3_compatible::{AwsS3StoreConfig, CloudflareR2StoreConfig};
use loonfs_objectstore::{AwsS3Credentials, CloudflareR2Credentials, GcpGcsCredentials};
use loonfs_types::config::non_blank;
use loonfs_types::{PrincipalId, PrincipalScope, PrincipalSet, SecretString};
use std::path::Path;

const AWS_REGIONS: &[&str] = &[
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "eu-central-1",
    "eu-central-2",
    "eu-north-1",
    "eu-south-1",
    "eu-south-2",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-southeast-3",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-northeast-3",
    "ap-south-1",
    "ap-south-2",
    "ap-east-1",
    "ca-central-1",
    "ca-west-1",
    "sa-east-1",
    "me-south-1",
    "me-central-1",
    "af-south-1",
    "il-central-1",
];

fn selected_credential_source(
    source: Option<CredentialSource>,
    has_static_flags: bool,
) -> Result<CredentialSource> {
    match source {
        Some(CredentialSource::Ambient) if has_static_flags => Err(CliError::invalid_request(
            "`--credential-source ambient` cannot be combined with static credential flags",
        )
        .with_param("--credential-source")),
        Some(source) => Ok(source),
        None if has_static_flags => Ok(CredentialSource::Static),
        None => Ok(CredentialSource::Ambient),
    }
}

/// Returns an S3 store's credentials from the flags, or `current` when no
/// credential flag is given (ambient for a new profile).
fn aws_credentials(
    current: Option<AwsS3Credentials>,
    args: &S3StoreArgs,
    source: FieldSource,
) -> Result<AwsS3Credentials> {
    let has_static_flags = args.access_key_id.is_some()
        || args.secret_access_key.is_some()
        || args.session_token.is_some();
    if args.credential_source.is_none() && !has_static_flags {
        return Ok(current.unwrap_or(AwsS3Credentials::Ambient {}));
    }
    match selected_credential_source(args.credential_source, has_static_flags)? {
        CredentialSource::Ambient => Ok(AwsS3Credentials::Ambient {}),
        CredentialSource::Static => Ok(AwsS3Credentials::Static {
            access_key_id: static_secret(args.access_key_id.as_ref(), "access-key-id", source)?,
            secret_access_key: static_secret(
                args.secret_access_key.as_ref(),
                "secret-access-key",
                source,
            )?,
            session_token: non_blank(args.session_token.clone()).map(SecretString::from),
        }),
    }
}

/// Returns an R2 store's credentials from the flags, or `current` when no
/// credential flag is given (ambient for a new profile).
fn r2_credentials(
    current: Option<CloudflareR2Credentials>,
    args: &R2StoreArgs,
    source: FieldSource,
) -> Result<CloudflareR2Credentials> {
    let has_static_flags = args.access_key_id.is_some() || args.secret_access_key.is_some();
    if args.credential_source.is_none() && !has_static_flags {
        return Ok(current.unwrap_or(CloudflareR2Credentials::Ambient {}));
    }
    match selected_credential_source(args.credential_source, has_static_flags)? {
        CredentialSource::Ambient => Ok(CloudflareR2Credentials::Ambient {}),
        CredentialSource::Static => Ok(CloudflareR2Credentials::Static {
            access_key_id: static_secret(args.access_key_id.as_ref(), "access-key-id", source)?,
            secret_access_key: static_secret(
                args.secret_access_key.as_ref(),
                "secret-access-key",
                source,
            )?,
        }),
    }
}

/// Returns one half of a static key pair from its flag, or from a prompt.
fn static_secret(
    value: Option<&String>,
    field: &'static str,
    source: FieldSource,
) -> Result<SecretString> {
    if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
        return Ok(value.clone().into());
    }
    match source {
        FieldSource::Prompt => prompt::prompt_secret(field).map(SecretString::from),
        FieldSource::Fail => Err(CliError::invalid_request(format!(
            "static credentials require `--access-key-id` and `--secret-access-key`; \
             missing `--{field}`"
        ))
        .with_param(format!("--{field}"))),
    }
}

fn required_field(
    value: Option<&String>,
    current: Option<&String>,
    field: &str,
    source: FieldSource,
) -> Result<String> {
    if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
        return Ok(value.clone());
    }
    match (current, source) {
        (Some(current), FieldSource::Fail) => Ok(current.clone()),
        (Some(current), FieldSource::Prompt) => prompt::prompt_line_default(field, current),
        (None, FieldSource::Prompt) => prompt::prompt_line(field),
        (None, FieldSource::Fail) => Err(CliError::non_interactive_field_required(field)),
    }
}

fn optional_field(
    value: Option<&String>,
    current: Option<&String>,
    field: &str,
    source: FieldSource,
) -> Result<Option<String>> {
    if value.is_some() {
        // An explicitly-blank flag value clears the field rather than storing
        // the empty string, which would then fail profile validation.
        return Ok(non_blank(value.cloned()));
    }
    match source {
        FieldSource::Fail => Ok(current.cloned()),
        FieldSource::Prompt => prompt::prompt_optional(field, current.map(String::as_str)),
    }
}

pub(crate) fn absolute_local_root(root: &Path) -> Result<String> {
    std::path::absolute(root)
        .map_err(CliError::io)?
        .into_os_string()
        .into_string()
        .map_err(|_| CliError::invalid_config("local_fs root is not valid UTF-8"))
}

#[derive(Clone, Copy)]
pub(crate) struct RemoteFields<'a> {
    pub(crate) server_url: &'a String,
    pub(crate) auth_token: Option<&'a SecretString>,
    pub(crate) ca_cert_path: Option<&'a String>,
}

pub(crate) fn local_store(
    current: Option<&LocalFsStoreConfig>,
    args: &LocalStoreArgs,
    source: FieldSource,
) -> Result<StoreConfig> {
    let root = required_field(
        args.root.as_ref(),
        current.map(|value| &value.root),
        "root",
        source,
    )?;
    let root = if args.root.is_some() || current.is_none() || matches!(source, FieldSource::Prompt)
    {
        absolute_local_root(Path::new(&root))?
    } else {
        root
    };
    Ok(StoreConfig::LocalFs(LocalFsStoreConfig {
        root,
        key_prefix: optional_field(
            args.key_prefix.as_ref(),
            current.and_then(|value| value.key_prefix.as_ref()),
            "key prefix",
            source,
        )?,
    }))
}

pub(crate) fn s3_store(
    current: Option<&AwsS3StoreConfig>,
    args: &S3StoreArgs,
    source: FieldSource,
) -> Result<StoreConfig> {
    let current_credentials = current.map(|value| value.credentials.clone());
    let credentials = match (current_credentials, source) {
        (Some(current), FieldSource::Prompt) => prompt_aws_credentials(current)?,
        (current, source) => aws_credentials(current, args, source)?,
    };
    let region = match (
        args.region.as_ref(),
        current.map(|value| &value.region),
        source,
    ) {
        (Some(region), _, _) if !region.trim().is_empty() => region.clone(),
        (_, Some(region), FieldSource::Fail) => region.clone(),
        (_, Some(region), FieldSource::Prompt) => {
            let index = AWS_REGIONS
                .iter()
                .position(|candidate| *candidate == region)
                .unwrap_or(0);
            prompt::prompt_fuzzy_choice("region", AWS_REGIONS, index)?
        }
        (_, None, FieldSource::Prompt) => prompt::prompt_fuzzy_choice("region", AWS_REGIONS, 0)?,
        (_, None, FieldSource::Fail) => {
            return Err(CliError::non_interactive_field_required("region"))
        }
    };
    Ok(StoreConfig::AwsS3(AwsS3StoreConfig {
        bucket: required_field(
            args.bucket.as_ref(),
            current.map(|value| &value.bucket),
            "bucket",
            source,
        )?,
        region,
        endpoint_url: optional_field(
            args.endpoint_url.as_ref(),
            current.and_then(|value| value.endpoint_url.as_ref()),
            "endpoint url",
            source,
        )?,
        credentials,
        key_prefix: optional_field(
            args.key_prefix.as_ref(),
            current.and_then(|value| value.key_prefix.as_ref()),
            "key prefix",
            source,
        )?,
        force_path_style: match args.force_path_style {
            Some(BucketAddressing::Path) => true,
            Some(BucketAddressing::VirtualHosted) => false,
            None => current.is_some_and(|value| value.force_path_style),
        },
    }))
}

pub(crate) fn r2_store(
    current: Option<&CloudflareR2StoreConfig>,
    args: &R2StoreArgs,
    source: FieldSource,
) -> Result<StoreConfig> {
    let current_credentials = current.map(|value| value.credentials.clone());
    let credentials = match (current_credentials, source) {
        (Some(current), FieldSource::Prompt) => prompt_r2_credentials(current)?,
        (current, source) => r2_credentials(current, args, source)?,
    };
    Ok(StoreConfig::CloudflareR2(CloudflareR2StoreConfig {
        bucket: required_field(
            args.bucket.as_ref(),
            current.map(|value| &value.bucket),
            "bucket",
            source,
        )?,
        account_id: required_field(
            args.account_id.as_ref(),
            current.map(|value| &value.account_id),
            "account-id",
            source,
        )?,
        endpoint_url: required_field(
            args.endpoint_url.as_ref(),
            current.map(|value| &value.endpoint_url),
            "endpoint-url",
            source,
        )?,
        credentials,
        key_prefix: optional_field(
            args.key_prefix.as_ref(),
            current.and_then(|value| value.key_prefix.as_ref()),
            "key prefix",
            source,
        )?,
    }))
}

pub(crate) fn gcs_store(
    current: Option<&GcpGcsStoreConfig>,
    args: &GcsStoreArgs,
    source: FieldSource,
) -> Result<StoreConfig> {
    let current_key_path = current.map(|value| {
        let GcpGcsCredentials::ServiceAccountFile { path } = &value.credentials;
        path
    });
    Ok(StoreConfig::GcpGcs(GcpGcsStoreConfig {
        bucket: required_field(
            args.bucket.as_ref(),
            current.map(|value| &value.bucket),
            "bucket",
            source,
        )?,
        credentials: GcpGcsCredentials::ServiceAccountFile {
            path: required_field(
                args.service_account_key_path.as_ref(),
                current_key_path,
                "service-account-key-path",
                source,
            )?,
        },
        key_prefix: optional_field(
            args.key_prefix.as_ref(),
            current.and_then(|value| value.key_prefix.as_ref()),
            "key prefix",
            source,
        )?,
    }))
}

pub(crate) fn remote_connection(
    name: &str,
    current: Option<RemoteFields<'_>>,
    args: &RemoteServerArgs,
    source: FieldSource,
) -> Result<ProfileConnection> {
    let server_url = required_field(
        args.server_url.as_ref(),
        current.map(|value| value.server_url),
        "server-url",
        source,
    )?;
    let auth_token = if args.auth_token.is_some() {
        non_blank(args.auth_token.clone()).map(SecretString::from)
    } else {
        match source {
            FieldSource::Fail => current.and_then(|value| value.auth_token).cloned(),
            FieldSource::Prompt => prompt::prompt_secret_optional(
                "auth token",
                current
                    .and_then(|value| value.auth_token)
                    .map(SecretString::expose),
            )?
            .map(SecretString::from),
        }
    };
    let ca_cert_path = optional_field(
        args.ca_cert_path.as_ref(),
        current.and_then(|value| value.ca_cert_path),
        "ca cert path",
        source,
    )?;
    validate_remote_client_config(
        name,
        &server_url,
        auth_token.as_ref(),
        ca_cert_path.as_deref(),
    )?;
    Ok(ProfileConnection::Remote {
        server_url,
        auth_token,
        ca_cert_path,
    })
}

pub(crate) fn parse_profile_principal_scope(
    principal_scope: Option<&str>,
) -> Result<Option<PrincipalScope>> {
    principal_scope
        .map(|principal_scope| {
            PrincipalScope::parse(principal_scope).map_err(|error| {
                CliError::invalid_request(format!("invalid `--principal-scope`: {error}"))
                    .with_param("--principal-scope")
            })
        })
        .transpose()
}

pub(crate) fn parse_profile_principals(
    principals: Option<&str>,
) -> Result<Option<Vec<PrincipalId>>> {
    principals
        .map(|principals| {
            principals
                .parse::<PrincipalSet>()
                .map(|principals| principals.iter().cloned().collect())
                .map_err(|error| {
                    CliError::invalid_request(format!("invalid `--principals`: {error}"))
                        .with_param("--principals")
                })
        })
        .transpose()
}
