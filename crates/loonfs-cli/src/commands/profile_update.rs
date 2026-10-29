//! Applies `profile update` flags or prompts to a stored profile, keeping the
//! fields that the update leaves out.

use crate::args::{
    GcsStoreArgs, LocalStoreArgs, ProfileProviderCommand, R2StoreArgs, RemoteServerArgs,
    S3StoreArgs, SubjectSelectors,
};
use crate::commands::profile_config::{
    profile_provider, provider_name, FieldSource, ProfileProvider,
};
use crate::commands::store_builders::{
    gcs_store, local_store, r2_store, remote_connection, s3_store, selected_credential_source,
    CredentialSource, LocalFsFields, RemoteFields,
};
use crate::config::{ProfileConfig, ProfileConnection, StoreConfig};
use crate::error::{CliError, Result};
use crate::prompt;
use loonfs_objectstore::local_fs_store::LocalFsStoreConfig;
use loonfs_objectstore::{AwsS3Credentials, CloudflareR2Credentials};
use loonfs_types::config::non_blank;
use loonfs_types::SecretString;

/// Applies an update's flags to a stored profile, keeping every field that
/// the update doesn't set.
pub(super) fn apply_update_flags(
    existing: ProfileConfig,
    command: &ProfileProviderCommand,
    subject: &SubjectSelectors,
) -> Result<ProfileConfig> {
    let name = command.name();
    let stored_provider = profile_provider(&existing);
    let mismatch = || provider_mismatch(name, stored_provider, command.provider());
    let identity = existing.identity.merge_flags(command.actor(), subject)?;
    let connection = match (&existing.connection, command) {
        (ProfileConnection::Embedded { store, writer_id }, provider) => {
            let store = match (store, provider) {
                (
                    StoreConfig::LocalFs(LocalFsStoreConfig { root, key_prefix }),
                    ProfileProviderCommand::Local(args),
                ) => local_store(
                    Some(LocalFsFields {
                        root,
                        key_prefix: key_prefix.as_ref(),
                    }),
                    &args.store,
                    FieldSource::Fail,
                )?,
                (StoreConfig::AwsS3(current), ProfileProviderCommand::S3(args)) => {
                    s3_store(Some(current), &args.store, FieldSource::Fail)?
                }
                (StoreConfig::CloudflareR2(current), ProfileProviderCommand::R2(args)) => {
                    r2_store(Some(current), &args.store, FieldSource::Fail)?
                }
                (StoreConfig::GcpGcs(current), ProfileProviderCommand::Gcs(args)) => {
                    gcs_store(Some(current), &args.store, FieldSource::Fail)?
                }
                _ => return Err(mismatch()),
            };
            ProfileConnection::Embedded {
                store,
                writer_id: writer_id.clone(),
            }
        }
        (
            ProfileConnection::Remote {
                server_url,
                auth_token,
                ca_cert_path,
            },
            ProfileProviderCommand::Remote(args),
        ) => remote_connection(
            name,
            Some(RemoteFields {
                server_url,
                auth_token: auth_token.as_ref(),
                ca_cert_path: ca_cert_path.as_ref(),
            }),
            &args.server,
            FieldSource::Fail,
        )?,
        _ => return Err(mismatch()),
    };
    Ok(ProfileConfig {
        identity,
        default_namespace: existing.default_namespace,
        connection,
    })
}

pub(super) fn validate_update_provider(
    existing: &ProfileConfig,
    command: &ProfileProviderCommand,
) -> Result<()> {
    let stored = profile_provider(existing);
    let requested = command.provider();
    if stored == requested {
        Ok(())
    } else {
        Err(provider_mismatch(command.name(), stored, requested))
    }
}

fn provider_mismatch(name: &str, stored: ProfileProvider, requested: ProfileProvider) -> CliError {
    CliError::invalid_request(format!(
        "profile `{name}` uses provider `{}` not `{}`",
        provider_name(stored),
        provider_name(requested),
    ))
    .with_param("provider")
}

pub(crate) fn updated_aws_credentials(
    current: AwsS3Credentials,
    args: &S3StoreArgs,
) -> Result<AwsS3Credentials> {
    let has_static_flags = args.access_key_id.is_some()
        || args.secret_access_key.is_some()
        || args.session_token.is_some();
    if args.credential_source.is_none() && !has_static_flags {
        return Ok(current);
    }
    match selected_credential_source(args.credential_source.as_deref(), has_static_flags)? {
        CredentialSource::Ambient => Ok(AwsS3Credentials::Ambient {}),
        CredentialSource::Static => Ok(AwsS3Credentials::Static {
            access_key_id: require_update_static_secret(
                args.access_key_id.as_ref(),
                "access-key-id",
            )?,
            secret_access_key: require_update_static_secret(
                args.secret_access_key.as_ref(),
                "secret-access-key",
            )?,
            session_token: non_blank(args.session_token.clone()).map(SecretString::from),
        }),
    }
}

pub(crate) fn updated_r2_credentials(
    current: CloudflareR2Credentials,
    args: &R2StoreArgs,
) -> Result<CloudflareR2Credentials> {
    let has_static_flags = args.access_key_id.is_some() || args.secret_access_key.is_some();
    if args.credential_source.is_none() && !has_static_flags {
        return Ok(current);
    }
    match selected_credential_source(args.credential_source.as_deref(), has_static_flags)? {
        CredentialSource::Ambient => Ok(CloudflareR2Credentials::Ambient {}),
        CredentialSource::Static => Ok(CloudflareR2Credentials::Static {
            access_key_id: require_update_static_secret(
                args.access_key_id.as_ref(),
                "access-key-id",
            )?,
            secret_access_key: require_update_static_secret(
                args.secret_access_key.as_ref(),
                "secret-access-key",
            )?,
        }),
    }
}

fn require_update_static_secret(
    value: Option<&String>,
    field: &'static str,
) -> Result<SecretString> {
    value
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .map(SecretString::from)
        .ok_or_else(|| {
            CliError::invalid_request(format!(
                "switching to static credentials requires `--access-key-id` and \
                 `--secret-access-key`; missing `--{field}`"
            ))
            .with_param(format!("--{field}"))
        })
}

/// Edits a stored profile's connection at prompts, keeping its identity and
/// default namespace.
pub(super) fn apply_update_interactive(
    name: &str,
    existing: ProfileConfig,
) -> Result<ProfileConfig> {
    let connection = match &existing.connection {
        ProfileConnection::Embedded { store, writer_id } => {
            let store = match store {
                StoreConfig::LocalFs(LocalFsStoreConfig { root, key_prefix }) => local_store(
                    Some(LocalFsFields {
                        root,
                        key_prefix: key_prefix.as_ref(),
                    }),
                    &LocalStoreArgs::default(),
                    FieldSource::Prompt,
                )?,
                StoreConfig::AwsS3(current) => {
                    s3_store(Some(current), &S3StoreArgs::default(), FieldSource::Prompt)?
                }
                StoreConfig::CloudflareR2(current) => {
                    r2_store(Some(current), &R2StoreArgs::default(), FieldSource::Prompt)?
                }
                StoreConfig::GcpGcs(current) => {
                    gcs_store(Some(current), &GcsStoreArgs::default(), FieldSource::Prompt)?
                }
            };
            ProfileConnection::Embedded {
                store,
                writer_id: writer_id.clone(),
            }
        }
        ProfileConnection::Remote {
            server_url,
            auth_token,
            ca_cert_path,
        } => remote_connection(
            name,
            Some(RemoteFields {
                server_url,
                auth_token: auth_token.as_ref(),
                ca_cert_path: ca_cert_path.as_ref(),
            }),
            &RemoteServerArgs::default(),
            FieldSource::Prompt,
        )?,
    };
    Ok(ProfileConfig {
        connection,
        ..existing
    })
}

pub(crate) fn prompt_aws_credentials(current: AwsS3Credentials) -> Result<AwsS3Credentials> {
    let default = usize::from(matches!(&current, AwsS3Credentials::Static { .. }));
    match prompt::prompt_choice_default("credential source", &["ambient", "static"], default)?
        .as_str()
    {
        "ambient" => Ok(AwsS3Credentials::Ambient {}),
        _ => match current {
            AwsS3Credentials::Ambient {} => Ok(AwsS3Credentials::Static {
                access_key_id: prompt::prompt_secret("access-key-id")?.into(),
                secret_access_key: prompt::prompt_secret("secret-access-key")?.into(),
                session_token: prompt::prompt_secret_optional("session token", None)?
                    .map(SecretString::from),
            }),
            AwsS3Credentials::Static {
                access_key_id,
                secret_access_key,
                session_token,
            } => Ok(AwsS3Credentials::Static {
                access_key_id: prompt::prompt_secret_keep_current(
                    "access-key-id",
                    access_key_id.expose(),
                )?
                .into(),
                secret_access_key: prompt::prompt_secret_keep_current(
                    "secret-access-key",
                    secret_access_key.expose(),
                )?
                .into(),
                session_token: prompt::prompt_secret_optional(
                    "session token",
                    session_token.as_ref().map(SecretString::expose),
                )?
                .map(SecretString::from),
            }),
        },
    }
}

pub(crate) fn prompt_r2_credentials(
    current: CloudflareR2Credentials,
) -> Result<CloudflareR2Credentials> {
    let default = usize::from(matches!(&current, CloudflareR2Credentials::Static { .. }));
    match prompt::prompt_choice_default("credential source", &["ambient", "static"], default)?
        .as_str()
    {
        "ambient" => Ok(CloudflareR2Credentials::Ambient {}),
        _ => match current {
            CloudflareR2Credentials::Ambient {} => Ok(CloudflareR2Credentials::Static {
                access_key_id: prompt::prompt_secret("access-key-id")?.into(),
                secret_access_key: prompt::prompt_secret("secret-access-key")?.into(),
            }),
            CloudflareR2Credentials::Static {
                access_key_id,
                secret_access_key,
            } => Ok(CloudflareR2Credentials::Static {
                access_key_id: prompt::prompt_secret_keep_current(
                    "access-key-id",
                    access_key_id.expose(),
                )?
                .into(),
                secret_access_key: prompt::prompt_secret_keep_current(
                    "secret-access-key",
                    secret_access_key.expose(),
                )?
                .into(),
            }),
        },
    }
}
