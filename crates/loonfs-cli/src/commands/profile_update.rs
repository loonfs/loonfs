//! Applies `profile update` flags or prompts to a stored profile, keeping the
//! fields that the update leaves out.

use crate::args::{ProfileProviderCommand, SubjectSelectors};
use crate::commands::profile_config::{
    profile_provider, provider_name, FieldSource, ProfileProvider,
};
use crate::commands::store_builders::{
    gcs_store, local_store, r2_store, remote_connection, s3_store, RemoteFields,
};
use crate::config::{ProfileConfig, ProfileConnection, StoreConfig};
use crate::error::{CliError, Result};
use crate::prompt;
use loonfs_objectstore::{AwsS3Credentials, CloudflareR2Credentials};
use loonfs_types::SecretString;

/// Applies an update's flags, or prompts when `source` is `Prompt`, to a
/// stored profile, keeping every field that the update doesn't set.
pub(super) fn apply_update(
    existing: ProfileConfig,
    command: &ProfileProviderCommand,
    subject: &SubjectSelectors,
    source: FieldSource,
) -> Result<ProfileConfig> {
    let name = command.name();
    let stored_provider = profile_provider(&existing);
    let mismatch = || provider_mismatch(name, stored_provider, command.provider());
    let identity = existing.identity.merge_flags(command.actor(), subject)?;
    let connection = match (&existing.connection, command) {
        (ProfileConnection::Embedded { store, writer_id }, provider) => {
            let store = match (store, provider) {
                (StoreConfig::LocalFs(current), ProfileProviderCommand::Local(args)) => {
                    local_store(Some(current), &args.store, source)?
                }
                (StoreConfig::AwsS3(current), ProfileProviderCommand::S3(args)) => {
                    s3_store(Some(current), &args.store, source)?
                }
                (StoreConfig::CloudflareR2(current), ProfileProviderCommand::R2(args)) => {
                    r2_store(Some(current), &args.store, source)?
                }
                (StoreConfig::GcpGcs(current), ProfileProviderCommand::Gcs(args)) => {
                    gcs_store(Some(current), &args.store, source)?
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
            source,
        )?,
        _ => return Err(mismatch()),
    };
    Ok(ProfileConfig {
        identity,
        default_namespace: existing.default_namespace,
        connection,
    })
}

fn provider_mismatch(name: &str, stored: ProfileProvider, requested: ProfileProvider) -> CliError {
    CliError::invalid_request(format!(
        "profile `{name}` uses provider `{}` not `{}`",
        provider_name(stored),
        provider_name(requested),
    ))
    .with_param("provider")
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
