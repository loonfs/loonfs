//! Builds provider-specific profile configurations and applies validated updates.

use crate::args::{
    ActorKindArg, ProfileCreateActorArgs, ProfileCreateAzureArgs, ProfileCreateCommand,
    ProfileCreateGcsArgs, ProfileCreateLocalArgs, ProfileCreateR2Args, ProfileCreateRemoteArgs,
    ProfileCreateS3Args, ProfileUpdateArgs, RuntimeBehavior,
};
use crate::config::{
    validate_remote_client_config, ProfileActorConfig, ProfileConfig, StoreConfig,
};
use crate::error::CliError;
use crate::prompt;
use loonfs_api::{ActorId, ActorKind, SecretString};
use loonfs_objectstore::{
    AwsS3Credentials, AzureAbsCredentials, CloudflareR2Credentials, GcpGcsCredentials,
};

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

pub(super) fn has_update_flags(args: &ProfileUpdateArgs) -> bool {
    args.actor_kind.is_some()
        || args.actor_id.is_some()
        || args.root.is_some()
        || args.key_prefix.is_some()
        || args.bucket.is_some()
        || args.region.is_some()
        || args.credential_source.is_some()
        || args.access_key_id.is_some()
        || args.secret_access_key.is_some()
        || args.endpoint_url.is_some()
        || args.session_token.is_some()
        || args.account_id.is_some()
        || args.account_name.is_some()
        || args.container_name.is_some()
        || args.access_key.is_some()
        || args.service_account_key_path.is_some()
        || args.server_url.is_some()
        || args.auth_token.is_some()
        || args.ca_cert_path.is_some()
}

// --- create/update helpers ---

#[derive(Debug, Clone)]
pub(super) struct CreateProfileSpec {
    provider: CreateProviderSpec,
    actor: CreateActorSpec,
}

#[derive(Debug, Clone)]
struct CreateActorSpec {
    kind: Option<ActorKindArg>,
    id: Option<String>,
}

impl From<ProfileCreateActorArgs> for CreateActorSpec {
    fn from(value: ProfileCreateActorArgs) -> Self {
        Self {
            kind: value.actor_kind,
            id: value.actor_id,
        }
    }
}

#[derive(Debug, Clone)]
enum CreateProviderSpec {
    S3(ProfileCreateS3Spec),
    R2(ProfileCreateR2Spec),
    Gcs(ProfileCreateGcsSpec),
    Azure(ProfileCreateAzureSpec),
    Local(ProfileCreateLocalSpec),
    Remote(ProfileCreateRemoteSpec),
}

#[derive(Debug, Clone, Default)]
struct ProfileCreateS3Spec {
    bucket: Option<String>,
    region: Option<String>,
    credential_source: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    endpoint_url: Option<String>,
    session_token: Option<String>,
    force_path_style: bool,
    key_prefix: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct ProfileCreateR2Spec {
    bucket: Option<String>,
    account_id: Option<String>,
    endpoint_url: Option<String>,
    credential_source: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    key_prefix: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct ProfileCreateGcsSpec {
    bucket: Option<String>,
    service_account_key_path: Option<String>,
    key_prefix: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct ProfileCreateAzureSpec {
    account_name: Option<String>,
    container_name: Option<String>,
    access_key: Option<String>,
    endpoint_url: Option<String>,
    key_prefix: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct ProfileCreateLocalSpec {
    root: Option<String>,
    key_prefix: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct ProfileCreateRemoteSpec {
    server_url: Option<String>,
    auth_token: Option<String>,
    ca_cert_path: Option<String>,
}

pub(super) fn create_profile_spec_from_create(
    command: ProfileCreateCommand,
) -> (String, CreateProfileSpec) {
    match command {
        ProfileCreateCommand::S3(args) => create_s3_spec(args),
        ProfileCreateCommand::R2(args) => create_r2_spec(args),
        ProfileCreateCommand::Gcs(args) => create_gcs_spec(args),
        ProfileCreateCommand::Azure(args) => create_azure_spec(args),
        ProfileCreateCommand::Local(args) => create_local_spec(args),
        ProfileCreateCommand::Remote(args) => create_remote_spec(args),
    }
}

fn create_s3_spec(args: ProfileCreateS3Args) -> (String, CreateProfileSpec) {
    let spec = ProfileCreateS3Spec {
        bucket: args.bucket,
        region: args.region,
        credential_source: args.credential_source,
        access_key_id: args.access_key_id,
        secret_access_key: args.secret_access_key,
        endpoint_url: args.endpoint_url,
        session_token: args.session_token,
        force_path_style: args.force_path_style,
        key_prefix: args.key_prefix,
    };
    (
        args.name,
        CreateProfileSpec {
            provider: CreateProviderSpec::S3(spec),
            actor: args.actor.into(),
        },
    )
}

fn create_r2_spec(args: ProfileCreateR2Args) -> (String, CreateProfileSpec) {
    let spec = ProfileCreateR2Spec {
        bucket: args.bucket,
        account_id: args.account_id,
        endpoint_url: args.endpoint_url,
        credential_source: args.credential_source,
        access_key_id: args.access_key_id,
        secret_access_key: args.secret_access_key,
        key_prefix: args.key_prefix,
    };
    (
        args.name,
        CreateProfileSpec {
            provider: CreateProviderSpec::R2(spec),
            actor: args.actor.into(),
        },
    )
}

fn create_gcs_spec(args: ProfileCreateGcsArgs) -> (String, CreateProfileSpec) {
    let spec = ProfileCreateGcsSpec {
        bucket: args.bucket,
        service_account_key_path: args.service_account_key_path,
        key_prefix: args.key_prefix,
    };
    (
        args.name,
        CreateProfileSpec {
            provider: CreateProviderSpec::Gcs(spec),
            actor: args.actor.into(),
        },
    )
}

fn create_azure_spec(args: ProfileCreateAzureArgs) -> (String, CreateProfileSpec) {
    let spec = ProfileCreateAzureSpec {
        account_name: args.account_name,
        container_name: args.container_name,
        access_key: args.access_key,
        endpoint_url: args.endpoint_url,
        key_prefix: args.key_prefix,
    };
    (
        args.name,
        CreateProfileSpec {
            provider: CreateProviderSpec::Azure(spec),
            actor: args.actor.into(),
        },
    )
}

fn create_local_spec(args: ProfileCreateLocalArgs) -> (String, CreateProfileSpec) {
    let spec = ProfileCreateLocalSpec {
        root: args.root,
        key_prefix: args.key_prefix,
    };
    (
        args.name,
        CreateProfileSpec {
            provider: CreateProviderSpec::Local(spec),
            actor: args.actor.into(),
        },
    )
}

fn create_remote_spec(args: ProfileCreateRemoteArgs) -> (String, CreateProfileSpec) {
    let spec = ProfileCreateRemoteSpec {
        server_url: args.server_url,
        auth_token: args.auth_token,
        ca_cert_path: args.ca_cert_path,
    };
    (
        args.name,
        CreateProfileSpec {
            provider: CreateProviderSpec::Remote(spec),
            actor: args.actor.into(),
        },
    )
}

pub(super) fn build_profile_interactive(
    name: &str,
    runtime: RuntimeBehavior,
) -> Result<ProfileConfig, CliError> {
    if !runtime.interactive {
        return Err(CliError::non_interactive_input_required(
            "`loonfs init` is interactive; use `loonfs profile create <provider>` for scripted setup",
        ));
    }
    let provider =
        match prompt::prompt_choice("provider", &["s3", "r2", "gcs", "azure", "local", "remote"])?
            .as_str()
        {
            "s3" => CreateProviderSpec::S3(ProfileCreateS3Spec::default()),
            "r2" => CreateProviderSpec::R2(ProfileCreateR2Spec::default()),
            "gcs" => CreateProviderSpec::Gcs(ProfileCreateGcsSpec::default()),
            "azure" => CreateProviderSpec::Azure(ProfileCreateAzureSpec::default()),
            "local" => CreateProviderSpec::Local(ProfileCreateLocalSpec::default()),
            _ => CreateProviderSpec::Remote(ProfileCreateRemoteSpec::default()),
        };
    build_profile_from_create_spec(
        name,
        CreateProfileSpec {
            provider,
            actor: CreateActorSpec {
                kind: None,
                id: None,
            },
        },
        runtime,
    )
}

pub(super) fn build_profile_from_create_spec(
    name: &str,
    spec: CreateProfileSpec,
    runtime: RuntimeBehavior,
) -> Result<ProfileConfig, CliError> {
    let actor = profile_actor_config(spec.actor.kind, spec.actor.id.as_deref())?;
    match spec.provider {
        CreateProviderSpec::Local(spec) => embedded_profile(
            StoreConfig::LocalFs {
                root: require_or_prompt(spec.root.as_ref(), "root", runtime)?,
                key_prefix: spec.key_prefix,
            },
            actor,
        ),
        CreateProviderSpec::S3(spec) => {
            let credentials = build_aws_credentials(
                spec.credential_source.as_deref(),
                spec.access_key_id.as_ref(),
                spec.secret_access_key.as_ref(),
                spec.session_token.as_ref(),
                runtime,
            )?;
            embedded_profile(
                StoreConfig::AwsS3 {
                    bucket: require_or_prompt(spec.bucket.as_ref(), "bucket", runtime)?,
                    region: require_or_prompt_region(spec.region.as_ref(), runtime)?,
                    endpoint_url: spec.endpoint_url,
                    credentials,
                    key_prefix: spec.key_prefix,
                    force_path_style: spec.force_path_style,
                },
                actor,
            )
        }
        CreateProviderSpec::R2(spec) => {
            let credentials = build_r2_credentials(
                spec.credential_source.as_deref(),
                spec.access_key_id.as_ref(),
                spec.secret_access_key.as_ref(),
                runtime,
            )?;
            embedded_profile(
                StoreConfig::CloudflareR2 {
                    bucket: require_or_prompt(spec.bucket.as_ref(), "bucket", runtime)?,
                    account_id: require_or_prompt(spec.account_id.as_ref(), "account-id", runtime)?,
                    endpoint_url: require_or_prompt(
                        spec.endpoint_url.as_ref(),
                        "endpoint-url",
                        runtime,
                    )?,
                    credentials,
                    key_prefix: spec.key_prefix,
                },
                actor,
            )
        }
        CreateProviderSpec::Gcs(spec) => embedded_profile(
            StoreConfig::GcpGcs {
                bucket: require_or_prompt(spec.bucket.as_ref(), "bucket", runtime)?,
                credentials: GcpGcsCredentials::ServiceAccountFile {
                    path: require_or_prompt(
                        spec.service_account_key_path.as_ref(),
                        "service-account-key-path",
                        runtime,
                    )?,
                },
                key_prefix: spec.key_prefix,
            },
            actor,
        ),
        CreateProviderSpec::Azure(spec) => embedded_profile(
            StoreConfig::AzureAbs {
                account_name: require_or_prompt(
                    spec.account_name.as_ref(),
                    "account-name",
                    runtime,
                )?,
                container_name: require_or_prompt(
                    spec.container_name.as_ref(),
                    "container-name",
                    runtime,
                )?,
                credentials: AzureAbsCredentials::AccessKey {
                    access_key: require_or_prompt_secret(
                        spec.access_key.as_ref(),
                        "access-key",
                        runtime,
                    )?,
                },
                endpoint_url: spec.endpoint_url,
                key_prefix: spec.key_prefix,
            },
            actor,
        ),
        CreateProviderSpec::Remote(spec) => {
            let auth_token = match spec.auth_token {
                Some(token) => blank_to_none(Some(token)),
                None if runtime.interactive => prompt::prompt_secret_optional("auth token", None)?,
                None => None,
            };
            let server_url = require_or_prompt(spec.server_url.as_ref(), "server-url", runtime)?;
            let auth_token = auth_token.map(SecretString::from);
            let ca_cert_path = blank_to_none(spec.ca_cert_path);
            validate_remote_client_config(
                name,
                &server_url,
                auth_token.as_ref(),
                ca_cert_path.as_deref(),
            )?;
            Ok(ProfileConfig::Remote {
                server_url,
                actor,
                default_namespace: None,
                auth_token,
                ca_cert_path,
            })
        }
    }
}

fn embedded_profile(
    store: StoreConfig,
    actor: ProfileActorConfig,
) -> Result<ProfileConfig, CliError> {
    Ok(ProfileConfig::Embedded {
        store,
        actor,
        default_namespace: None,
        writer_id: None,
    })
}

/// An explicitly-blank flag value clears the field rather than storing the
/// empty string, which would then fail profile validation.
fn blank_to_none(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CredentialSource {
    Ambient,
    Static,
}

fn selected_credential_source(
    source: Option<&str>,
    has_static_flags: bool,
) -> Result<CredentialSource, CliError> {
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
    runtime: RuntimeBehavior,
) -> Result<AwsS3Credentials, CliError> {
    let has_static_flags =
        access_key_id.is_some() || secret_access_key.is_some() || session_token.is_some();
    match selected_credential_source(credential_source, has_static_flags)? {
        CredentialSource::Ambient => Ok(AwsS3Credentials::Ambient {}),
        CredentialSource::Static => Ok(AwsS3Credentials::Static {
            access_key_id: require_or_prompt_secret(access_key_id, "access-key-id", runtime)?,
            secret_access_key: require_or_prompt_secret(
                secret_access_key,
                "secret-access-key",
                runtime,
            )?,
            session_token: blank_to_none(session_token.cloned()).map(SecretString::from),
        }),
    }
}

fn build_r2_credentials(
    credential_source: Option<&str>,
    access_key_id: Option<&String>,
    secret_access_key: Option<&String>,
    runtime: RuntimeBehavior,
) -> Result<CloudflareR2Credentials, CliError> {
    let has_static_flags = access_key_id.is_some() || secret_access_key.is_some();
    match selected_credential_source(credential_source, has_static_flags)? {
        CredentialSource::Ambient => Ok(CloudflareR2Credentials::Ambient {}),
        CredentialSource::Static => Ok(CloudflareR2Credentials::Static {
            access_key_id: require_or_prompt_secret(access_key_id, "access-key-id", runtime)?,
            secret_access_key: require_or_prompt_secret(
                secret_access_key,
                "secret-access-key",
                runtime,
            )?,
        }),
    }
}

fn profile_actor_config(
    kind: Option<ActorKindArg>,
    id: Option<&str>,
) -> Result<ProfileActorConfig, CliError> {
    match (kind, id) {
        (None, None) => Ok(ProfileActorConfig::default()),
        (Some(kind), Some(id)) => Ok(ProfileActorConfig {
            actor_kind: Some(ActorKind::from(kind)),
            actor_id: Some(ActorId::parse(id).map_err(|error| {
                CliError::invalid_request(format!("invalid --actor-id: {error}"))
                    .with_param("--actor-id")
            })?),
        }),
        (None, Some(_)) => Err(
            CliError::invalid_request("--actor-id requires --actor-kind").with_param("--actor-id"),
        ),
        (Some(_), None) => Err(
            CliError::invalid_request("--actor-kind requires --actor-id")
                .with_param("--actor-kind"),
        ),
    }
}

fn updated_actor(
    current: ProfileActorConfig,
    args: &ProfileUpdateArgs,
) -> Result<ProfileActorConfig, CliError> {
    if args.actor_kind.is_none() && args.actor_id.is_none() {
        Ok(current)
    } else {
        profile_actor_config(args.actor_kind, args.actor_id.as_deref())
    }
}

fn require_or_prompt(
    value: Option<&String>,
    field: &str,
    runtime: RuntimeBehavior,
) -> Result<String, CliError> {
    match value {
        Some(v) if !v.trim().is_empty() => Ok(v.clone()),
        _ if runtime.interactive => prompt::prompt_line(field),
        _ => Err(CliError::non_interactive_field_required(field)),
    }
}

/// Like [`require_or_prompt`] but prompts with hidden input and returns the
/// value wrapped as a secret.
///
fn require_or_prompt_secret(
    value: Option<&String>,
    field: &str,
    runtime: RuntimeBehavior,
) -> Result<SecretString, CliError> {
    match value {
        Some(v) if !v.trim().is_empty() => Ok(SecretString::from(v.clone())),
        _ if runtime.interactive => prompt::prompt_secret(field).map(SecretString::from),
        _ => Err(CliError::non_interactive_field_required(field)),
    }
}

fn require_or_prompt_region(
    value: Option<&String>,
    runtime: RuntimeBehavior,
) -> Result<String, CliError> {
    match value {
        Some(v) if !v.trim().is_empty() => Ok(v.clone()),
        _ if runtime.interactive => prompt::prompt_fuzzy_choice("region", AWS_REGIONS, 0),
        _ => Err(CliError::non_interactive_field_required("region")),
    }
}

fn reject_inapplicable_update_flag(
    is_set: bool,
    flag: &str,
    profile_label: &str,
) -> Result<(), CliError> {
    if is_set {
        return Err(CliError::invalid_request(format!(
            "`--{flag}` does not apply to {profile_label} profiles"
        ))
        .with_param(format!("--{flag}")));
    }
    Ok(())
}

fn reject_remote_flags_for_embedded(
    args: &ProfileUpdateArgs,
    profile_label: &str,
) -> Result<(), CliError> {
    reject_inapplicable_update_flag(args.server_url.is_some(), "server-url", profile_label)?;
    reject_inapplicable_update_flag(args.auth_token.is_some(), "auth-token", profile_label)?;
    reject_inapplicable_update_flag(args.ca_cert_path.is_some(), "ca-cert-path", profile_label)
}

fn validate_local_update_flags(args: &ProfileUpdateArgs) -> Result<(), CliError> {
    let label = "local-fs";
    reject_remote_flags_for_embedded(args, label)?;
    reject_inapplicable_update_flag(args.bucket.is_some(), "bucket", label)?;
    reject_inapplicable_update_flag(args.region.is_some(), "region", label)?;
    reject_inapplicable_update_flag(args.credential_source.is_some(), "credential-source", label)?;
    reject_inapplicable_update_flag(args.access_key_id.is_some(), "access-key-id", label)?;
    reject_inapplicable_update_flag(args.secret_access_key.is_some(), "secret-access-key", label)?;
    reject_inapplicable_update_flag(args.endpoint_url.is_some(), "endpoint-url", label)?;
    reject_inapplicable_update_flag(args.session_token.is_some(), "session-token", label)?;
    reject_inapplicable_update_flag(args.account_id.is_some(), "account-id", label)?;
    reject_inapplicable_update_flag(args.account_name.is_some(), "account-name", label)?;
    reject_inapplicable_update_flag(args.container_name.is_some(), "container-name", label)?;
    reject_inapplicable_update_flag(args.access_key.is_some(), "access-key", label)?;
    reject_inapplicable_update_flag(
        args.service_account_key_path.is_some(),
        "service-account-key-path",
        label,
    )
}

fn validate_s3_update_flags(args: &ProfileUpdateArgs) -> Result<(), CliError> {
    let label = "aws-s3";
    reject_remote_flags_for_embedded(args, label)?;
    reject_inapplicable_update_flag(args.root.is_some(), "root", label)?;
    reject_inapplicable_update_flag(args.account_id.is_some(), "account-id", label)?;
    reject_inapplicable_update_flag(args.account_name.is_some(), "account-name", label)?;
    reject_inapplicable_update_flag(args.container_name.is_some(), "container-name", label)?;
    reject_inapplicable_update_flag(args.access_key.is_some(), "access-key", label)?;
    reject_inapplicable_update_flag(
        args.service_account_key_path.is_some(),
        "service-account-key-path",
        label,
    )
}

fn validate_r2_update_flags(args: &ProfileUpdateArgs) -> Result<(), CliError> {
    let label = "cloudflare-r2";
    reject_remote_flags_for_embedded(args, label)?;
    reject_inapplicable_update_flag(args.root.is_some(), "root", label)?;
    reject_inapplicable_update_flag(args.region.is_some(), "region", label)?;
    reject_inapplicable_update_flag(args.session_token.is_some(), "session-token", label)?;
    reject_inapplicable_update_flag(args.account_name.is_some(), "account-name", label)?;
    reject_inapplicable_update_flag(args.container_name.is_some(), "container-name", label)?;
    reject_inapplicable_update_flag(args.access_key.is_some(), "access-key", label)?;
    reject_inapplicable_update_flag(
        args.service_account_key_path.is_some(),
        "service-account-key-path",
        label,
    )
}

fn validate_gcs_update_flags(args: &ProfileUpdateArgs) -> Result<(), CliError> {
    let label = "gcp-gcs";
    reject_remote_flags_for_embedded(args, label)?;
    reject_inapplicable_update_flag(args.root.is_some(), "root", label)?;
    reject_inapplicable_update_flag(args.region.is_some(), "region", label)?;
    reject_inapplicable_update_flag(args.credential_source.is_some(), "credential-source", label)?;
    reject_inapplicable_update_flag(args.access_key_id.is_some(), "access-key-id", label)?;
    reject_inapplicable_update_flag(args.secret_access_key.is_some(), "secret-access-key", label)?;
    reject_inapplicable_update_flag(args.endpoint_url.is_some(), "endpoint-url", label)?;
    reject_inapplicable_update_flag(args.session_token.is_some(), "session-token", label)?;
    reject_inapplicable_update_flag(args.account_id.is_some(), "account-id", label)?;
    reject_inapplicable_update_flag(args.account_name.is_some(), "account-name", label)?;
    reject_inapplicable_update_flag(args.container_name.is_some(), "container-name", label)?;
    reject_inapplicable_update_flag(args.access_key.is_some(), "access-key", label)
}

fn validate_azure_update_flags(args: &ProfileUpdateArgs) -> Result<(), CliError> {
    let label = "azure-abs";
    reject_remote_flags_for_embedded(args, label)?;
    reject_inapplicable_update_flag(args.root.is_some(), "root", label)?;
    reject_inapplicable_update_flag(args.bucket.is_some(), "bucket", label)?;
    reject_inapplicable_update_flag(args.region.is_some(), "region", label)?;
    reject_inapplicable_update_flag(args.credential_source.is_some(), "credential-source", label)?;
    reject_inapplicable_update_flag(args.access_key_id.is_some(), "access-key-id", label)?;
    reject_inapplicable_update_flag(args.secret_access_key.is_some(), "secret-access-key", label)?;
    reject_inapplicable_update_flag(args.session_token.is_some(), "session-token", label)?;
    reject_inapplicable_update_flag(args.account_id.is_some(), "account-id", label)?;
    reject_inapplicable_update_flag(
        args.service_account_key_path.is_some(),
        "service-account-key-path",
        label,
    )
}

fn validate_remote_update_flags(args: &ProfileUpdateArgs) -> Result<(), CliError> {
    let label = "remote";
    reject_inapplicable_update_flag(args.root.is_some(), "root", label)?;
    reject_inapplicable_update_flag(args.key_prefix.is_some(), "key-prefix", label)?;
    reject_inapplicable_update_flag(args.bucket.is_some(), "bucket", label)?;
    reject_inapplicable_update_flag(args.region.is_some(), "region", label)?;
    reject_inapplicable_update_flag(args.credential_source.is_some(), "credential-source", label)?;
    reject_inapplicable_update_flag(args.access_key_id.is_some(), "access-key-id", label)?;
    reject_inapplicable_update_flag(args.secret_access_key.is_some(), "secret-access-key", label)?;
    reject_inapplicable_update_flag(args.endpoint_url.is_some(), "endpoint-url", label)?;
    reject_inapplicable_update_flag(args.session_token.is_some(), "session-token", label)?;
    reject_inapplicable_update_flag(args.account_id.is_some(), "account-id", label)?;
    reject_inapplicable_update_flag(args.account_name.is_some(), "account-name", label)?;
    reject_inapplicable_update_flag(args.container_name.is_some(), "container-name", label)?;
    reject_inapplicable_update_flag(args.access_key.is_some(), "access-key", label)?;
    reject_inapplicable_update_flag(
        args.service_account_key_path.is_some(),
        "service-account-key-path",
        label,
    )
}

pub(super) fn apply_update_flags(
    name: &str,
    existing: ProfileConfig,
    args: &ProfileUpdateArgs,
) -> Result<ProfileConfig, CliError> {
    match &existing {
        ProfileConfig::Embedded {
            store: StoreConfig::LocalFs { .. },
            ..
        } => validate_local_update_flags(args)?,
        ProfileConfig::Embedded {
            store: StoreConfig::AwsS3 { .. },
            ..
        } => validate_s3_update_flags(args)?,
        ProfileConfig::Embedded {
            store: StoreConfig::CloudflareR2 { .. },
            ..
        } => validate_r2_update_flags(args)?,
        ProfileConfig::Embedded {
            store: StoreConfig::GcpGcs { .. },
            ..
        } => validate_gcs_update_flags(args)?,
        ProfileConfig::Embedded {
            store: StoreConfig::AzureAbs { .. },
            ..
        } => validate_azure_update_flags(args)?,
        ProfileConfig::Remote { .. } => validate_remote_update_flags(args)?,
    }

    match existing {
        ProfileConfig::Embedded {
            store,
            actor,
            default_namespace,
            writer_id,
        } => {
            let store = match store {
                StoreConfig::LocalFs { root, key_prefix } => StoreConfig::LocalFs {
                    root: args.root.clone().unwrap_or(root),
                    key_prefix: args.key_prefix.clone().or(key_prefix),
                },
                StoreConfig::AwsS3 {
                    bucket,
                    region,
                    endpoint_url,
                    credentials,
                    key_prefix,
                    force_path_style,
                } => StoreConfig::AwsS3 {
                    bucket: args.bucket.clone().unwrap_or(bucket),
                    region: args.region.clone().unwrap_or(region),
                    endpoint_url: args.endpoint_url.clone().or(endpoint_url),
                    credentials: updated_aws_credentials(credentials, args)?,
                    key_prefix: args.key_prefix.clone().or(key_prefix),
                    force_path_style,
                },
                StoreConfig::CloudflareR2 {
                    bucket,
                    account_id,
                    endpoint_url,
                    credentials,
                    key_prefix,
                } => StoreConfig::CloudflareR2 {
                    bucket: args.bucket.clone().unwrap_or(bucket),
                    account_id: args.account_id.clone().unwrap_or(account_id),
                    endpoint_url: args.endpoint_url.clone().unwrap_or(endpoint_url),
                    credentials: updated_r2_credentials(credentials, args)?,
                    key_prefix: args.key_prefix.clone().or(key_prefix),
                },
                StoreConfig::GcpGcs {
                    bucket,
                    credentials,
                    key_prefix,
                } => StoreConfig::GcpGcs {
                    bucket: args.bucket.clone().unwrap_or(bucket),
                    credentials: match credentials {
                        GcpGcsCredentials::ServiceAccountFile { path } => {
                            GcpGcsCredentials::ServiceAccountFile {
                                path: args.service_account_key_path.clone().unwrap_or(path),
                            }
                        }
                    },
                    key_prefix: args.key_prefix.clone().or(key_prefix),
                },
                StoreConfig::AzureAbs {
                    account_name,
                    container_name,
                    credentials,
                    endpoint_url,
                    key_prefix,
                } => StoreConfig::AzureAbs {
                    account_name: args.account_name.clone().unwrap_or(account_name),
                    container_name: args.container_name.clone().unwrap_or(container_name),
                    credentials: match credentials {
                        AzureAbsCredentials::AccessKey { access_key } => {
                            AzureAbsCredentials::AccessKey {
                                access_key: args
                                    .access_key
                                    .clone()
                                    .map(SecretString::from)
                                    .unwrap_or(access_key),
                            }
                        }
                    },
                    endpoint_url: args.endpoint_url.clone().or(endpoint_url),
                    key_prefix: args.key_prefix.clone().or(key_prefix),
                },
            };
            Ok(ProfileConfig::Embedded {
                store,
                actor: updated_actor(actor, args)?,
                default_namespace,
                writer_id,
            })
        }
        ProfileConfig::Remote {
            server_url,
            actor,
            default_namespace,
            auth_token,
            ca_cert_path,
        } => {
            let server_url = args.server_url.clone().unwrap_or(server_url);
            let auth_token = match args.auth_token.clone() {
                Some(token) => blank_to_none(Some(token)).map(SecretString::from),
                None => auth_token,
            };
            let ca_cert_path = blank_to_none(args.ca_cert_path.clone()).or(ca_cert_path);
            validate_remote_client_config(
                name,
                &server_url,
                auth_token.as_ref(),
                ca_cert_path.as_deref(),
            )?;
            Ok(ProfileConfig::Remote {
                server_url,
                actor: updated_actor(actor, args)?,
                default_namespace,
                auth_token,
                ca_cert_path,
            })
        }
    }
}

fn updated_aws_credentials(
    current: AwsS3Credentials,
    args: &ProfileUpdateArgs,
) -> Result<AwsS3Credentials, CliError> {
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
            session_token: blank_to_none(args.session_token.clone()).map(SecretString::from),
        }),
    }
}

fn updated_r2_credentials(
    current: CloudflareR2Credentials,
    args: &ProfileUpdateArgs,
) -> Result<CloudflareR2Credentials, CliError> {
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
) -> Result<SecretString, CliError> {
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

pub(super) fn apply_update_interactive(existing: ProfileConfig) -> Result<ProfileConfig, CliError> {
    match existing {
        ProfileConfig::Embedded {
            store,
            actor,
            default_namespace,
            writer_id,
        } => {
            let store = match store {
                StoreConfig::LocalFs { root, key_prefix } => StoreConfig::LocalFs {
                    root: prompt::prompt_line_default("root", &root)?,
                    key_prefix: prompt::prompt_optional("key prefix", key_prefix.as_deref())?,
                },
                StoreConfig::AwsS3 {
                    bucket,
                    region,
                    endpoint_url,
                    credentials,
                    key_prefix,
                    force_path_style,
                } => StoreConfig::AwsS3 {
                    bucket: prompt::prompt_line_default("bucket", &bucket)?,
                    region: {
                        let default_idx =
                            AWS_REGIONS.iter().position(|r| *r == region).unwrap_or(0);
                        prompt::prompt_fuzzy_choice("region", AWS_REGIONS, default_idx)?
                    },
                    credentials: prompt_aws_credentials(credentials)?,
                    endpoint_url: prompt::prompt_optional("endpoint url", endpoint_url.as_deref())?,
                    key_prefix: prompt::prompt_optional("key prefix", key_prefix.as_deref())?,
                    force_path_style,
                },
                StoreConfig::CloudflareR2 {
                    bucket,
                    account_id,
                    endpoint_url,
                    credentials,
                    key_prefix,
                } => StoreConfig::CloudflareR2 {
                    bucket: prompt::prompt_line_default("bucket", &bucket)?,
                    account_id: prompt::prompt_line_default("account id", &account_id)?,
                    endpoint_url: prompt::prompt_line_default("endpoint url", &endpoint_url)?,
                    credentials: prompt_r2_credentials(credentials)?,
                    key_prefix: prompt::prompt_optional("key prefix", key_prefix.as_deref())?,
                },
                StoreConfig::GcpGcs {
                    bucket,
                    credentials,
                    key_prefix,
                } => StoreConfig::GcpGcs {
                    bucket: prompt::prompt_line_default("bucket", &bucket)?,
                    credentials: match credentials {
                        GcpGcsCredentials::ServiceAccountFile { path } => {
                            GcpGcsCredentials::ServiceAccountFile {
                                path: prompt::prompt_line_default(
                                    "service account key path",
                                    &path,
                                )?,
                            }
                        }
                    },
                    key_prefix: prompt::prompt_optional("key prefix", key_prefix.as_deref())?,
                },
                StoreConfig::AzureAbs {
                    account_name,
                    container_name,
                    credentials,
                    endpoint_url,
                    key_prefix,
                } => StoreConfig::AzureAbs {
                    account_name: prompt::prompt_line_default("account-name", &account_name)?,
                    container_name: prompt::prompt_line_default("container-name", &container_name)?,
                    credentials: match credentials {
                        AzureAbsCredentials::AccessKey { access_key } => {
                            AzureAbsCredentials::AccessKey {
                                access_key: prompt::prompt_secret_keep_current(
                                    "access-key",
                                    access_key.expose(),
                                )?
                                .into(),
                            }
                        }
                    },
                    endpoint_url: prompt::prompt_optional("endpoint url", endpoint_url.as_deref())?,
                    key_prefix: prompt::prompt_optional("key prefix", key_prefix.as_deref())?,
                },
            };
            Ok(ProfileConfig::Embedded {
                store,
                actor,
                default_namespace,
                writer_id,
            })
        }
        ProfileConfig::Remote {
            server_url,
            actor,
            default_namespace,
            auth_token,
            ca_cert_path,
        } => Ok(ProfileConfig::Remote {
            server_url: prompt::prompt_line_default("server-url", &server_url)?,
            actor,
            default_namespace,
            auth_token: prompt::prompt_secret_optional(
                "auth token",
                auth_token.as_ref().map(SecretString::expose),
            )?
            .map(SecretString::from),
            ca_cert_path: prompt::prompt_optional("ca cert path", ca_cert_path.as_deref())?,
        }),
    }
}

fn prompt_aws_credentials(current: AwsS3Credentials) -> Result<AwsS3Credentials, CliError> {
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

fn prompt_r2_credentials(
    current: CloudflareR2Credentials,
) -> Result<CloudflareR2Credentials, CliError> {
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
    #![allow(clippy::panic)]
    // Profile tests use panic in unexpected match arms for precise diagnostics.

    use super::{
        apply_update_flags, build_profile_from_create_spec, CreateActorSpec, CreateProfileSpec,
        CreateProviderSpec, ProfileCreateAzureSpec, ProfileCreateS3Spec,
    };
    use crate::args::{ProfileUpdateArgs, RuntimeBehavior};
    use crate::config::{ProfileConfig, StoreConfig};
    use loonfs_objectstore::{AwsS3Credentials, AzureAbsCredentials};

    #[test]
    fn create_profile_supports_azure_abs() {
        let profile = build_profile_from_create_spec(
            "default",
            CreateProfileSpec {
                provider: CreateProviderSpec::Azure(ProfileCreateAzureSpec {
                    account_name: Some("devstoreaccount1".to_owned()),
                    container_name: Some("container".to_owned()),
                    access_key: Some("account-key".to_owned()),
                    endpoint_url: Some("https://devstoreaccount1.blob.core.windows.net".to_owned()),
                    key_prefix: Some("tenant-a".to_owned()),
                }),
                actor: empty_actor(),
            },
            non_interactive_runtime(),
        )
        .expect("build azure profile");

        assert!(matches!(
            profile,
            ProfileConfig::Embedded {
                store: StoreConfig::AzureAbs { .. },
                ..
            }
        ));
        if let ProfileConfig::Embedded {
            store:
                StoreConfig::AzureAbs {
                    account_name,
                    container_name,
                    credentials,
                    endpoint_url,
                    key_prefix,
                },
            ..
        } = profile
        {
            assert_eq!(account_name, "devstoreaccount1");
            assert_eq!(container_name, "container");
            let AzureAbsCredentials::AccessKey { access_key } = credentials;
            assert_eq!(access_key.expose(), "account-key");
            assert_eq!(
                endpoint_url.as_deref(),
                Some("https://devstoreaccount1.blob.core.windows.net")
            );
            assert_eq!(key_prefix.as_deref(), Some("tenant-a"));
        }
    }

    #[test]
    fn no_static_flags_select_ambient_without_storing_secrets() {
        let s3 = build_profile_from_create_spec(
            "default",
            CreateProfileSpec {
                provider: CreateProviderSpec::S3(ProfileCreateS3Spec {
                    bucket: Some("bucket".to_owned()),
                    region: Some("us-east-1".to_owned()),
                    ..ProfileCreateS3Spec::default()
                }),
                actor: empty_actor(),
            },
            non_interactive_runtime(),
        )
        .expect("build ambient s3 profile");
        let ProfileConfig::Embedded {
            store: StoreConfig::AwsS3 { credentials, .. },
            ..
        } = s3
        else {
            panic!("expected aws-s3 profile")
        };
        assert_eq!(credentials, AwsS3Credentials::Ambient {});
    }

    #[test]
    fn static_flags_imply_static_and_require_the_complete_set() {
        let incomplete = build_profile_from_create_spec(
            "default",
            CreateProfileSpec {
                provider: CreateProviderSpec::S3(ProfileCreateS3Spec {
                    bucket: Some("bucket".to_owned()),
                    region: Some("us-east-1".to_owned()),
                    access_key_id: Some("access".to_owned()),
                    ..ProfileCreateS3Spec::default()
                }),
                actor: empty_actor(),
            },
            non_interactive_runtime(),
        )
        .expect_err("a partial static set must fail");
        assert!(incomplete.message.contains("secret-access-key"));

        let complete = build_profile_from_create_spec(
            "default",
            CreateProfileSpec {
                provider: CreateProviderSpec::S3(ProfileCreateS3Spec {
                    bucket: Some("bucket".to_owned()),
                    region: Some("us-east-1".to_owned()),
                    access_key_id: Some("access".to_owned()),
                    secret_access_key: Some("secret".to_owned()),
                    session_token: Some("session".to_owned()),
                    ..ProfileCreateS3Spec::default()
                }),
                actor: empty_actor(),
            },
            non_interactive_runtime(),
        )
        .expect("complete static credentials");
        assert!(matches!(
            complete,
            ProfileConfig::Embedded {
                store: StoreConfig::AwsS3 {
                    credentials: AwsS3Credentials::Static { .. },
                    ..
                },
                ..
            }
        ));
    }

    #[test]
    fn update_rejects_flags_outside_their_provider() {
        let local_fs = ProfileConfig::Embedded {
            store: StoreConfig::LocalFs {
                root: "/tmp/store".to_owned(),
                key_prefix: None,
            },
            actor: crate::config::ProfileActorConfig::default(),
            default_namespace: None,
            writer_id: None,
        };

        let error = apply_update_flags(
            "default",
            local_fs.clone(),
            &ProfileUpdateArgs {
                bucket: Some("bucket".to_owned()),
                ..empty_update_args()
            },
        )
        .expect_err("bucket must not apply to local-fs");
        assert_eq!(
            error.message,
            "`--bucket` does not apply to local-fs profiles"
        );

        let error = apply_update_flags(
            "default",
            local_fs,
            &ProfileUpdateArgs {
                auth_token: Some("token".to_owned()),
                ..empty_update_args()
            },
        )
        .expect_err("auth-token must not apply to embedded");
        assert_eq!(
            error.message,
            "`--auth-token` does not apply to local-fs profiles"
        );
    }

    #[test]
    fn update_applies_flags_to_matching_provider() {
        let remote = ProfileConfig::Remote {
            server_url: "http://127.0.0.1:9400".to_owned(),
            actor: crate::config::ProfileActorConfig::default(),
            default_namespace: None,
            auth_token: None,
            ca_cert_path: None,
        };

        let updated = apply_update_flags(
            "default",
            remote,
            &ProfileUpdateArgs {
                auth_token: Some("new-token".to_owned()),
                ..empty_update_args()
            },
        )
        .expect("update remote auth token");

        match updated {
            ProfileConfig::Remote { auth_token, .. } => {
                assert_eq!(
                    auth_token.as_ref().map(|token| token.expose()),
                    Some("new-token")
                );
            }
            other => panic!("expected remote profile, got {other:?}"),
        }

        let local_fs = ProfileConfig::Embedded {
            store: StoreConfig::LocalFs {
                root: "/tmp/store".to_owned(),
                key_prefix: None,
            },
            actor: crate::config::ProfileActorConfig::default(),
            default_namespace: None,
            writer_id: None,
        };

        let updated = apply_update_flags(
            "default",
            local_fs,
            &ProfileUpdateArgs {
                root: Some("/tmp/moved".to_owned()),
                ..empty_update_args()
            },
        )
        .expect("update local-fs root");

        match updated {
            ProfileConfig::Embedded {
                store: StoreConfig::LocalFs { root, .. },
                ..
            } => assert_eq!(root, "/tmp/moved"),
            other => panic!("expected local-fs profile, got {other:?}"),
        }
    }

    #[test]
    fn remote_update_rejects_a_token_over_non_loopback_plaintext_http() {
        let remote = ProfileConfig::Remote {
            server_url: "https://example.internal".to_owned(),
            actor: crate::config::ProfileActorConfig::default(),
            default_namespace: None,
            auth_token: None,
            ca_cert_path: None,
        };

        let error = apply_update_flags(
            "default",
            remote,
            &ProfileUpdateArgs {
                server_url: Some("http://example.internal".to_owned()),
                auth_token: Some("new-token".to_owned()),
                ..empty_update_args()
            },
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
        let profile = ProfileConfig::Embedded {
            store: StoreConfig::AwsS3 {
                bucket: "bucket".to_owned(),
                region: "us-east-1".to_owned(),
                endpoint_url: None,
                credentials: AwsS3Credentials::Ambient {},
                key_prefix: None,
                force_path_style: false,
            },
            actor: crate::config::ProfileActorConfig::default(),
            default_namespace: None,
            writer_id: None,
        };

        let error = apply_update_flags(
            "default",
            profile.clone(),
            &ProfileUpdateArgs {
                credential_source: Some("static".to_owned()),
                access_key_id: Some("access".to_owned()),
                ..empty_update_args()
            },
        )
        .expect_err("failed switch");
        assert!(error.message.contains("secret-access-key"));
        assert!(matches!(
            profile,
            ProfileConfig::Embedded {
                store: StoreConfig::AwsS3 {
                    credentials: AwsS3Credentials::Ambient {},
                    ..
                },
                ..
            }
        ));

        let updated = apply_update_flags(
            "default",
            profile,
            &ProfileUpdateArgs {
                credential_source: Some("static".to_owned()),
                access_key_id: Some("access".to_owned()),
                secret_access_key: Some("secret".to_owned()),
                ..empty_update_args()
            },
        )
        .expect("complete switch");
        assert!(matches!(
            updated,
            ProfileConfig::Embedded {
                store: StoreConfig::AwsS3 {
                    credentials: AwsS3Credentials::Static { .. },
                    ..
                },
                ..
            }
        ));
    }

    fn empty_actor() -> CreateActorSpec {
        CreateActorSpec {
            kind: None,
            id: None,
        }
    }

    fn empty_update_args() -> ProfileUpdateArgs {
        ProfileUpdateArgs {
            name: "profile".to_owned(),
            root: None,
            key_prefix: None,
            bucket: None,
            region: None,
            credential_source: None,
            access_key_id: None,
            secret_access_key: None,
            endpoint_url: None,
            session_token: None,
            account_id: None,
            account_name: None,
            container_name: None,
            access_key: None,
            service_account_key_path: None,
            server_url: None,
            auth_token: None,
            ca_cert_path: None,
            actor_kind: None,
            actor_id: None,
        }
    }

    fn non_interactive_runtime() -> RuntimeBehavior {
        RuntimeBehavior {
            json: false,
            no_input: true,
            interactive: false,
            progress: crate::progress::ProgressMode::Off,
        }
    }
}
