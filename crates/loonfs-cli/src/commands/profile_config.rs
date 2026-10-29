//! Builds provider-specific profile configurations and applies validated updates.

use crate::args::{
    BucketAddressing, GcsStoreArgs, LocalStoreArgs, ProfileActorArgs, ProfileProviderCommand,
    R2StoreArgs, RemoteServerArgs, RuntimeBehavior, S3StoreArgs, SubjectSelectors,
};
use crate::config::{
    absolute_env_path, ensure_subject_pairing, validate_remote_client_config, ProfileConfig,
    ProfileConnection, ProfileIdentity, StoreConfig,
};
use crate::error::{CliError, Result};
use crate::prompt;
use crate::resolve::subject_flag_error;
use loonfs_objectstore::gcs::GcpGcsStoreConfig;
use loonfs_objectstore::local_fs_store::LocalFsStoreConfig;
use loonfs_objectstore::s3_compatible::{AwsS3StoreConfig, CloudflareR2StoreConfig};
use loonfs_objectstore::{AwsS3Credentials, CloudflareR2Credentials, GcpGcsCredentials};
use loonfs_types::config::non_blank;
use loonfs_types::{PrincipalId, PrincipalScope, PrincipalSet, SecretString};
use std::path::{Path, PathBuf};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProfileProvider {
    S3,
    R2,
    Gcs,
    Local,
    Remote,
}

impl ProfileProviderCommand {
    /// Returns the name of the profile that the command creates or updates.
    pub(super) fn name(&self) -> &str {
        match self {
            Self::S3(args) => &args.name,
            Self::R2(args) => &args.name,
            Self::Gcs(args) => &args.name,
            Self::Local(args) => &args.name,
            Self::Remote(args) => &args.name,
        }
    }

    fn actor(&self) -> &ProfileActorArgs {
        match self {
            Self::S3(args) => &args.actor,
            Self::R2(args) => &args.actor,
            Self::Gcs(args) => &args.actor,
            Self::Local(args) => &args.actor,
            Self::Remote(args) => &args.actor,
        }
    }

    fn provider(&self) -> ProfileProvider {
        match self {
            Self::S3(_) => ProfileProvider::S3,
            Self::R2(_) => ProfileProvider::R2,
            Self::Gcs(_) => ProfileProvider::Gcs,
            Self::Local(_) => ProfileProvider::Local,
            Self::Remote(_) => ProfileProvider::Remote,
        }
    }

    /// Returns whether an update sets any field, with the selector flags that
    /// an update saves in the profile. An update without one edits the
    /// profile at prompts.
    pub(super) fn has_update_flags(&self, subject: &SubjectSelectors) -> bool {
        self.actor().actor_id.is_some()
            || subject.subject_id.is_some()
            || subject.principal_scope.is_some()
            || subject.principals.is_some()
            || match self {
                Self::S3(args) => args.store.has_update_flags(),
                Self::R2(args) => args.store.has_update_flags(),
                Self::Gcs(args) => args.store.has_update_flags(),
                Self::Local(args) => args.store.has_update_flags(),
                Self::Remote(args) => args.server.has_update_flags(),
            }
    }
}

impl ProfileIdentity {
    /// Returns this identity with the fields that the actor flag and the
    /// subject selectors set, and refuses a principal scope without
    /// principals or principals without a scope.
    pub(super) fn merge_flags(
        self,
        actor: &ProfileActorArgs,
        subject: &SubjectSelectors,
    ) -> Result<Self> {
        let principal_scope = parse_profile_principal_scope(subject.principal_scope.as_deref())?
            .or(self.principal_scope);
        let principals =
            parse_profile_principals(subject.principals.as_deref())?.or(self.principals);
        ensure_subject_pairing(principal_scope.as_ref(), principals.as_deref())
            .map_err(subject_flag_error)?;
        Ok(Self {
            actor_id: actor.actor_id.clone().or(self.actor_id),
            subject_id: subject.subject_id.clone().or(self.subject_id),
            principal_scope,
            principals,
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum FieldSource {
    Fail,
    Prompt,
}

impl FieldSource {
    fn from_runtime(runtime: RuntimeBehavior) -> Self {
        if runtime.interactive {
            Self::Prompt
        } else {
            Self::Fail
        }
    }
}

pub(super) fn build_profile_interactive(
    name: &str,
    runtime: RuntimeBehavior,
) -> Result<ProfileConfig> {
    if !runtime.interactive {
        return Err(CliError::non_interactive_input_required(
            "`loonfs init` is interactive; use `loonfs profile create <provider>` for scripted setup",
        ));
    }
    let source = FieldSource::from_runtime(runtime);
    let connection = match prompt::prompt_choice(
        "provider",
        &["s3", "r2", "gcs", "local", "remote"],
    )?
    .as_str()
    {
        "s3" => embedded(s3_store(None, &S3StoreArgs::default(), source)?),
        "r2" => embedded(r2_store(None, &R2StoreArgs::default(), source)?),
        "gcs" => embedded(gcs_store(None, &GcsStoreArgs::default(), source)?),
        "local" => {
            let root = prompt::prompt_line("root")?;
            let path = match root.strip_prefix("~/") {
                Some(relative) => absolute_env_path("HOME")
                    .ok_or_else(|| {
                        CliError::invalid_config(
                            "unable to determine the home directory; enter an absolute root path",
                        )
                    })?
                    .join(relative.trim_start_matches('/')),
                None => PathBuf::from(root),
            };
            let args = LocalStoreArgs {
                root: Some(absolute_local_root(&path)?),
                key_prefix: None,
            };
            embedded(local_store(None, &args, source)?)
        }
        _ => remote_connection(name, None, &RemoteServerArgs::default(), source)?,
    };
    Ok(ProfileConfig {
        identity: ProfileIdentity::default(),
        default_namespace: None,
        connection,
    })
}

/// Builds the profile that `profile create` creates, asking for missing
/// fields when the run is interactive.
pub(super) fn build_profile_from_command(
    command: &ProfileProviderCommand,
    subject: &SubjectSelectors,
    runtime: RuntimeBehavior,
) -> Result<ProfileConfig> {
    let identity = ProfileIdentity::default().merge_flags(command.actor(), subject)?;
    let source = FieldSource::from_runtime(runtime);
    let connection = match command {
        ProfileProviderCommand::Local(args) => embedded(local_store(None, &args.store, source)?),
        ProfileProviderCommand::S3(args) => embedded(s3_store(None, &args.store, source)?),
        ProfileProviderCommand::R2(args) => embedded(r2_store(None, &args.store, source)?),
        ProfileProviderCommand::Gcs(args) => embedded(gcs_store(None, &args.store, source)?),
        ProfileProviderCommand::Remote(args) => {
            remote_connection(&args.name, None, &args.server, source)?
        }
    };
    Ok(ProfileConfig {
        identity,
        default_namespace: None,
        connection,
    })
}

fn embedded(store: StoreConfig) -> ProfileConnection {
    ProfileConnection::Embedded {
        store,
        writer_id: None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CredentialSource {
    Ambient,
    Static,
}

fn selected_credential_source(
    source: Option<&str>,
    has_static_flags: bool,
) -> Result<CredentialSource> {
    match source {
        Some("ambient") if has_static_flags => Err(CliError::invalid_request(
            "`--credential-source ambient` cannot be combined with static credential flags",
        )
        .with_param("--credential-source")),
        Some("ambient") => Ok(CredentialSource::Ambient),
        Some("static") => Ok(CredentialSource::Static),
        Some(other) => Err(CliError::invalid_request(format!(
            "unknown credential source: `{other}` (expected ambient or static)"
        ))
        .with_param("--credential-source")),
        None if has_static_flags => Ok(CredentialSource::Static),
        None => Ok(CredentialSource::Ambient),
    }
}

fn build_aws_credentials(
    credential_source: Option<&str>,
    access_key_id: Option<&String>,
    secret_access_key: Option<&String>,
    session_token: Option<&String>,
    source: FieldSource,
) -> Result<AwsS3Credentials> {
    let has_static_flags =
        access_key_id.is_some() || secret_access_key.is_some() || session_token.is_some();
    match selected_credential_source(credential_source, has_static_flags)? {
        CredentialSource::Ambient => Ok(AwsS3Credentials::Ambient {}),
        CredentialSource::Static => Ok(AwsS3Credentials::Static {
            access_key_id: required_secret(access_key_id, None, "access-key-id", source)?,
            secret_access_key: required_secret(
                secret_access_key,
                None,
                "secret-access-key",
                source,
            )?,
            session_token: non_blank(session_token.cloned()).map(SecretString::from),
        }),
    }
}

fn build_r2_credentials(
    credential_source: Option<&str>,
    access_key_id: Option<&String>,
    secret_access_key: Option<&String>,
    source: FieldSource,
) -> Result<CloudflareR2Credentials> {
    let has_static_flags = access_key_id.is_some() || secret_access_key.is_some();
    match selected_credential_source(credential_source, has_static_flags)? {
        CredentialSource::Ambient => Ok(CloudflareR2Credentials::Ambient {}),
        CredentialSource::Static => Ok(CloudflareR2Credentials::Static {
            access_key_id: required_secret(access_key_id, None, "access-key-id", source)?,
            secret_access_key: required_secret(
                secret_access_key,
                None,
                "secret-access-key",
                source,
            )?,
        }),
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

fn required_secret(
    value: Option<&String>,
    current: Option<&SecretString>,
    field: &str,
    source: FieldSource,
) -> Result<SecretString> {
    if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
        return Ok(value.clone().into());
    }
    match (current, source) {
        (Some(current), FieldSource::Fail) => Ok(current.clone()),
        (Some(current), FieldSource::Prompt) => {
            prompt::prompt_secret_keep_current(field, current.expose()).map(SecretString::from)
        }
        (None, FieldSource::Prompt) => prompt::prompt_secret(field).map(SecretString::from),
        (None, FieldSource::Fail) => Err(CliError::non_interactive_field_required(field)),
    }
}

fn absolute_local_root(root: &Path) -> Result<String> {
    std::path::absolute(root)
        .map_err(CliError::io)?
        .into_os_string()
        .into_string()
        .map_err(|_| CliError::invalid_config("local_fs root is not valid UTF-8"))
}

#[derive(Clone, Copy)]
struct LocalFsFields<'a> {
    root: &'a String,
    key_prefix: Option<&'a String>,
}

#[derive(Clone, Copy)]
struct RemoteFields<'a> {
    server_url: &'a String,
    auth_token: Option<&'a SecretString>,
    ca_cert_path: Option<&'a String>,
}

fn local_store(
    current: Option<LocalFsFields<'_>>,
    args: &LocalStoreArgs,
    source: FieldSource,
) -> Result<StoreConfig> {
    let root = required_field(
        args.root.as_ref(),
        current.map(|value| value.root),
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
            current.and_then(|value| value.key_prefix),
            "key prefix",
            source,
        )?,
    }))
}

fn s3_store(
    current: Option<&AwsS3StoreConfig>,
    args: &S3StoreArgs,
    source: FieldSource,
) -> Result<StoreConfig> {
    let credentials = match (current.map(|value| &value.credentials), source) {
        (Some(current), FieldSource::Prompt) => prompt_aws_credentials(current.clone())?,
        (Some(current), FieldSource::Fail) => updated_aws_credentials(current.clone(), args)?,
        (None, _) => build_aws_credentials(
            args.credential_source.as_deref(),
            args.access_key_id.as_ref(),
            args.secret_access_key.as_ref(),
            args.session_token.as_ref(),
            source,
        )?,
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

fn r2_store(
    current: Option<&CloudflareR2StoreConfig>,
    args: &R2StoreArgs,
    source: FieldSource,
) -> Result<StoreConfig> {
    let credentials = match (current.map(|value| &value.credentials), source) {
        (Some(current), FieldSource::Prompt) => prompt_r2_credentials(current.clone())?,
        (Some(current), FieldSource::Fail) => updated_r2_credentials(current.clone(), args)?,
        (None, _) => build_r2_credentials(
            args.credential_source.as_deref(),
            args.access_key_id.as_ref(),
            args.secret_access_key.as_ref(),
            source,
        )?,
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

fn gcs_store(
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

fn remote_connection(
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

fn parse_profile_principal_scope(principal_scope: Option<&str>) -> Result<Option<PrincipalScope>> {
    principal_scope
        .map(|principal_scope| {
            PrincipalScope::parse(principal_scope).map_err(|error| {
                CliError::invalid_request(format!("invalid `--principal-scope`: {error}"))
                    .with_param("--principal-scope")
            })
        })
        .transpose()
}

fn parse_profile_principals(principals: Option<&str>) -> Result<Option<Vec<PrincipalId>>> {
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

fn profile_provider(profile: &ProfileConfig) -> ProfileProvider {
    match &profile.connection {
        ProfileConnection::Embedded {
            store: StoreConfig::AwsS3 { .. },
            ..
        } => ProfileProvider::S3,
        ProfileConnection::Embedded {
            store: StoreConfig::CloudflareR2 { .. },
            ..
        } => ProfileProvider::R2,
        ProfileConnection::Embedded {
            store: StoreConfig::GcpGcs { .. },
            ..
        } => ProfileProvider::Gcs,
        ProfileConnection::Embedded {
            store: StoreConfig::LocalFs(_),
            ..
        } => ProfileProvider::Local,
        ProfileConnection::Remote { .. } => ProfileProvider::Remote,
    }
}

fn provider_name(provider: ProfileProvider) -> &'static str {
    match provider {
        ProfileProvider::S3 => "s3",
        ProfileProvider::R2 => "r2",
        ProfileProvider::Gcs => "gcs",
        ProfileProvider::Local => "local",
        ProfileProvider::Remote => "remote",
    }
}

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

fn updated_aws_credentials(
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

fn updated_r2_credentials(
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

fn prompt_aws_credentials(current: AwsS3Credentials) -> Result<AwsS3Credentials> {
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

fn prompt_r2_credentials(current: CloudflareR2Credentials) -> Result<CloudflareR2Credentials> {
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

#[cfg(test)]
mod tests {

    use super::{apply_update_flags, build_profile_from_command};
    use crate::args::{
        Cli, Command, LocalStoreArgs, ProfileActorArgs, ProfileCommand, ProfileLocalArgs,
        ProfileProviderCommand, ProfileR2Args, ProfileRemoteArgs, ProfileS3Args, R2StoreArgs,
        RemoteServerArgs, RuntimeBehavior, S3StoreArgs, SubjectSelectors, TopLevelCommand,
    };
    use crate::config::{ProfileConfig, ProfileConnection, ProfileIdentity, StoreConfig};
    use crate::error::Result;
    use clap::Parser;
    use loonfs_objectstore::local_fs_store::LocalFsStoreConfig;
    use loonfs_objectstore::s3_compatible::{AwsS3StoreConfig, CloudflareR2StoreConfig};
    use loonfs_objectstore::{AwsS3Credentials, CloudflareR2Credentials};
    use std::assert_matches;

    #[cfg(unix)]
    #[test]
    fn relative_local_roots_remove_dots_and_keep_parents_without_creating_paths() {
        let current_directory = std::env::current_dir().expect("current directory");
        let name = loonfs_types::generated_id("local-root");
        let missing_root = current_directory.join(&name);
        let input = std::path::Path::new(&name).join("./child//../store");
        assert!(!missing_root.exists());

        let absolute = super::absolute_local_root(&input).expect("absolute root");
        assert!(std::path::Path::new(&absolute).is_absolute());
        assert_eq!(
            absolute,
            missing_root
                .join("child/../store")
                .to_str()
                .expect("utf-8 root"),
        );
        assert!(!missing_root.exists());
    }

    #[test]
    fn create_profile_supports_cloudflare_r2() {
        let profile = build(&r2(R2StoreArgs {
            bucket: Some("documents".to_owned()),
            account_id: Some("account".to_owned()),
            endpoint_url: Some("https://account.r2.cloudflarestorage.com".to_owned()),
            access_key_id: Some("access".to_owned()),
            secret_access_key: Some("secret".to_owned()),
            key_prefix: Some("tenant-a".to_owned()),
            ..R2StoreArgs::default()
        }))
        .expect("build r2 profile");
        assert_eq!(
            store(&profile),
            &StoreConfig::CloudflareR2(CloudflareR2StoreConfig {
                bucket: "documents".to_owned(),
                account_id: "account".to_owned(),
                endpoint_url: "https://account.r2.cloudflarestorage.com".to_owned(),
                credentials: CloudflareR2Credentials::Static {
                    access_key_id: "access".into(),
                    secret_access_key: "secret".into(),
                },
                key_prefix: Some("tenant-a".to_owned()),
            })
        );
    }

    #[test]
    fn update_r2_flags_round_trip_credentials_and_preserve_unspecified_fields() {
        let profile = build(&r2(R2StoreArgs {
            bucket: Some("documents".to_owned()),
            account_id: Some("account".to_owned()),
            endpoint_url: Some("https://account.r2.cloudflarestorage.com".to_owned()),
            ..R2StoreArgs::default()
        }))
        .expect("build ambient r2 profile");
        let command = r2(R2StoreArgs {
            bucket: Some("archive".to_owned()),
            credential_source: Some("static".to_owned()),
            access_key_id: Some("access".to_owned()),
            secret_access_key: Some("secret".to_owned()),
            key_prefix: Some("tenant-b".to_owned()),
            ..R2StoreArgs::default()
        });
        let updated = update(profile.clone(), &command).expect("update r2");
        assert_eq!(
            store(&updated),
            &StoreConfig::CloudflareR2(CloudflareR2StoreConfig {
                bucket: "archive".to_owned(),
                account_id: "account".to_owned(),
                endpoint_url: "https://account.r2.cloudflarestorage.com".to_owned(),
                credentials: CloudflareR2Credentials::Static {
                    access_key_id: "access".into(),
                    secret_access_key: "secret".into(),
                },
                key_prefix: Some("tenant-b".to_owned()),
            })
        );
        let reset = r2(R2StoreArgs {
            bucket: Some("documents".to_owned()),
            credential_source: Some("ambient".to_owned()),
            key_prefix: Some(String::new()),
            ..R2StoreArgs::default()
        });
        assert_eq!(
            update(updated, &reset).expect("restore r2 profile"),
            profile
        );
    }

    /// Checks the bare flag, both values, and an omitted flag, which keeps
    /// the stored addressing style.
    #[test]
    fn update_s3_flags_set_path_style_and_preserve_it_when_omitted() {
        let mut profile = build(&s3(S3StoreArgs {
            bucket: Some("documents".to_owned()),
            region: Some("us-east-1".to_owned()),
            ..S3StoreArgs::default()
        }))
        .expect("build ambient s3 profile");

        for (flags, expected_value) in [
            (&["--force-path-style"][..], true),
            (&[][..], true),
            (&["--force-path-style=virtual-hosted"][..], false),
            (&[][..], false),
            (&["--force-path-style=path"][..], true),
        ] {
            let cli = Cli::try_parse_from(
                ["loonfs", "profile", "update", "s3", "default"]
                    .into_iter()
                    .chain(flags.iter().copied()),
            )
            .expect("parse s3 update flags");
            let subject = cli.selectors().subject;
            let TopLevelCommand::Configured(Command::Profile {
                command: ProfileCommand::Update { provider },
            }) = cli.command
            else {
                panic!("expected profile update, got {:?}", cli.command);
            };
            assert_eq!(provider.has_update_flags(&subject), !flags.is_empty());

            let mut expected = profile.clone();
            let ProfileConnection::Embedded {
                store:
                    StoreConfig::AwsS3(AwsS3StoreConfig {
                        force_path_style, ..
                    }),
                ..
            } = &mut expected.connection
            else {
                panic!("expected s3 profile, got {expected:?}");
            };
            *force_path_style = expected_value;
            profile = update(profile, &provider).expect("update s3");
            assert_eq!(profile, expected);
        }
    }

    #[test]
    fn no_static_flags_select_ambient_without_storing_secrets() {
        let s3 = build(&s3(S3StoreArgs {
            bucket: Some("bucket".to_owned()),
            region: Some("us-east-1".to_owned()),
            ..S3StoreArgs::default()
        }))
        .expect("build ambient s3 profile");
        let StoreConfig::AwsS3(AwsS3StoreConfig { credentials, .. }) = store(&s3) else {
            panic!("expected aws-s3 profile")
        };
        assert_eq!(credentials, &AwsS3Credentials::Ambient {});
    }

    #[test]
    fn static_flags_imply_static_and_require_the_complete_set() {
        let incomplete = build(&s3(S3StoreArgs {
            bucket: Some("bucket".to_owned()),
            region: Some("us-east-1".to_owned()),
            access_key_id: Some("access".to_owned()),
            ..S3StoreArgs::default()
        }))
        .expect_err("a partial static set must fail");
        assert!(incomplete.message.contains("secret-access-key"));

        let complete = build(&s3(S3StoreArgs {
            bucket: Some("bucket".to_owned()),
            region: Some("us-east-1".to_owned()),
            access_key_id: Some("access".to_owned()),
            secret_access_key: Some("secret".to_owned()),
            session_token: Some("session".to_owned()),
            ..S3StoreArgs::default()
        }))
        .expect("complete static credentials");
        assert_matches!(
            store(&complete),
            StoreConfig::AwsS3(AwsS3StoreConfig {
                credentials: AwsS3Credentials::Static { .. },
                ..
            })
        );
    }

    #[test]
    fn update_rejects_a_provider_that_does_not_match_the_profile() {
        let error = update(local_fs_profile(), &s3(S3StoreArgs::default()))
            .expect_err("s3 must not apply to local-fs");
        assert_eq!(
            error.message,
            "profile `default` uses provider `local` not `s3`"
        );
    }

    #[test]
    fn update_applies_flags_to_matching_provider() {
        let updated = update(
            remote_profile("http://127.0.0.1:9400"),
            &remote(RemoteServerArgs {
                auth_token: Some("new-token".to_owned()),
                ..RemoteServerArgs::default()
            }),
        )
        .expect("update remote auth token");

        match updated.connection {
            ProfileConnection::Remote { auth_token, .. } => {
                assert_eq!(
                    auth_token.as_ref().map(|token| token.expose()),
                    Some("new-token")
                );
            }
            other => panic!("expected remote profile, got {other:?}"),
        }

        let updated = update(
            local_fs_profile(),
            &local(LocalStoreArgs {
                root: Some("/tmp/moved".to_owned()),
                ..LocalStoreArgs::default()
            }),
        )
        .expect("update local-fs root");

        match store(&updated) {
            StoreConfig::LocalFs(LocalFsStoreConfig { root, .. }) => {
                assert_eq!(root, "/tmp/moved")
            }
            other => panic!("expected local-fs profile, got {other:?}"),
        }
    }

    #[test]
    fn remote_update_rejects_a_token_over_non_loopback_plaintext_http() {
        let error = update(
            remote_profile("https://example.internal"),
            &remote(RemoteServerArgs {
                server_url: Some("http://example.internal".to_owned()),
                auth_token: Some("new-token".to_owned()),
                ca_cert_path: None,
            }),
        )
        .expect_err("unsafe effective remote profile should be rejected");

        assert_eq!(error.code, "invalid_config");
        assert!(
            error
                .message
                .contains("bearer tokens require https except for loopback http URLs"),
            "{}",
            error.message
        );
    }

    #[test]
    fn update_switches_credential_source_only_with_a_complete_static_set() {
        let profile = embedded_profile(StoreConfig::AwsS3(AwsS3StoreConfig {
            bucket: "bucket".to_owned(),
            region: "us-east-1".to_owned(),
            endpoint_url: None,
            credentials: AwsS3Credentials::Ambient {},
            key_prefix: None,
            force_path_style: false,
        }));

        let incomplete = s3(S3StoreArgs {
            credential_source: Some("static".to_owned()),
            access_key_id: Some("access".to_owned()),
            ..S3StoreArgs::default()
        });
        let error = update(profile.clone(), &incomplete).expect_err("failed switch");
        assert!(error.message.contains("secret-access-key"));
        assert_matches!(
            store(&profile),
            StoreConfig::AwsS3(AwsS3StoreConfig {
                credentials: AwsS3Credentials::Ambient {},
                ..
            })
        );

        let complete = s3(S3StoreArgs {
            credential_source: Some("static".to_owned()),
            access_key_id: Some("access".to_owned()),
            secret_access_key: Some("secret".to_owned()),
            ..S3StoreArgs::default()
        });
        let updated = update(profile, &complete).expect("complete switch");
        assert_matches!(
            store(&updated),
            StoreConfig::AwsS3(AwsS3StoreConfig {
                credentials: AwsS3Credentials::Static { .. },
                ..
            })
        );
    }

    #[test]
    fn a_profile_with_more_principals_than_a_request_accepts_is_refused() {
        let principals = (0..=loonfs_types::MAX_SUBJECT_PRINCIPALS)
            .map(|index| format!("prn_{index}"))
            .collect::<Vec<_>>()
            .join(",");
        let error = build_profile_from_command(
            &local(LocalStoreArgs {
                root: Some("/tmp/store".to_owned()),
                key_prefix: None,
            }),
            &SubjectSelectors {
                principal_scope: Some("org".to_owned()),
                principals: Some(principals),
                ..SubjectSelectors::default()
            },
            non_interactive_runtime(),
        )
        .expect_err("a profile no request could use is refused");
        assert_eq!(error.code, "invalid_request");
        assert_eq!(error.param.as_deref(), Some("--principals"));
    }

    fn s3(store: S3StoreArgs) -> ProfileProviderCommand {
        ProfileProviderCommand::S3(ProfileS3Args {
            name: "default".to_owned(),
            store,
            actor: ProfileActorArgs::default(),
        })
    }

    fn r2(store: R2StoreArgs) -> ProfileProviderCommand {
        ProfileProviderCommand::R2(ProfileR2Args {
            name: "default".to_owned(),
            store,
            actor: ProfileActorArgs::default(),
        })
    }

    fn local(store: LocalStoreArgs) -> ProfileProviderCommand {
        ProfileProviderCommand::Local(ProfileLocalArgs {
            name: "default".to_owned(),
            store,
            actor: ProfileActorArgs::default(),
        })
    }

    fn remote(server: RemoteServerArgs) -> ProfileProviderCommand {
        ProfileProviderCommand::Remote(ProfileRemoteArgs {
            name: "default".to_owned(),
            server,
            actor: ProfileActorArgs::default(),
        })
    }

    fn build(command: &ProfileProviderCommand) -> Result<ProfileConfig> {
        build_profile_from_command(
            command,
            &SubjectSelectors::default(),
            non_interactive_runtime(),
        )
    }

    fn update(profile: ProfileConfig, command: &ProfileProviderCommand) -> Result<ProfileConfig> {
        apply_update_flags(profile, command, &SubjectSelectors::default())
    }

    fn store(profile: &ProfileConfig) -> &StoreConfig {
        let ProfileConnection::Embedded { store, .. } = &profile.connection else {
            panic!("expected embedded profile, got {profile:?}");
        };
        store
    }

    fn embedded_profile(store: StoreConfig) -> ProfileConfig {
        ProfileConfig {
            identity: ProfileIdentity::default(),
            default_namespace: None,
            connection: ProfileConnection::Embedded {
                store,
                writer_id: None,
            },
        }
    }

    fn local_fs_profile() -> ProfileConfig {
        embedded_profile(StoreConfig::LocalFs(LocalFsStoreConfig {
            root: "/tmp/store".to_owned(),
            key_prefix: None,
        }))
    }

    fn remote_profile(server_url: &str) -> ProfileConfig {
        ProfileConfig {
            identity: ProfileIdentity::default(),
            default_namespace: None,
            connection: ProfileConnection::Remote {
                server_url: server_url.to_owned(),
                auth_token: None,
                ca_cert_path: None,
            },
        }
    }

    fn non_interactive_runtime() -> RuntimeBehavior {
        RuntimeBehavior {
            output: crate::render::OutputFormat::Human,
            interactive: false,
            progress: crate::progress::ProgressMode::Off,
            transient_retry: crate::args::TransientRetry::Retry,
        }
    }
}
