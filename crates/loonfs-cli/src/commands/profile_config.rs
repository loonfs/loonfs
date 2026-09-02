//! Builds provider-specific profile configurations and applies validated updates.

use crate::args::{
    ActorKindArg, ProfileCreateActorArgs, ProfileCreateAzureArgs, ProfileCreateCommand,
    ProfileCreateGcsArgs, ProfileCreateLocalArgs, ProfileCreateR2Args, ProfileCreateRemoteArgs,
    ProfileCreateS3Args, ProfileUpdateActorArgs, ProfileUpdateAzureArgs, ProfileUpdateCommand,
    ProfileUpdateGcsArgs, ProfileUpdateLocalArgs, ProfileUpdateR2Args, ProfileUpdateRemoteArgs,
    ProfileUpdateS3Args, RuntimeBehavior,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProfileProvider {
    S3,
    R2,
    Gcs,
    Azure,
    Local,
    Remote,
}

pub(super) struct ProfileUpdateSpec {
    pub(super) name: String,
    provider: CreateProviderSpec,
    actor: CreateActorSpec,
}

impl ProfileUpdateSpec {
    fn provider(&self) -> ProfileProvider {
        self.provider.provider()
    }
}

pub(super) fn profile_update_spec(command: ProfileUpdateCommand) -> ProfileUpdateSpec {
    match command {
        ProfileUpdateCommand::S3(args) => update_s3_spec(args),
        ProfileUpdateCommand::R2(args) => update_r2_spec(args),
        ProfileUpdateCommand::Gcs(args) => update_gcs_spec(args),
        ProfileUpdateCommand::Azure(args) => update_azure_spec(args),
        ProfileUpdateCommand::Local(args) => update_local_spec(args),
        ProfileUpdateCommand::Remote(args) => update_remote_spec(args),
    }
}

fn update_s3_spec(args: ProfileUpdateS3Args) -> ProfileUpdateSpec {
    ProfileUpdateSpec {
        name: args.name,
        provider: CreateProviderSpec::S3(ProfileCreateS3Spec {
            bucket: args.bucket,
            region: args.region,
            credential_source: args.credential_source,
            access_key_id: args.access_key_id,
            secret_access_key: args.secret_access_key,
            endpoint_url: args.endpoint_url,
            session_token: args.session_token,
            force_path_style: false,
            key_prefix: args.key_prefix,
        }),
        actor: update_actor_spec(args.actor),
    }
}

fn update_r2_spec(args: ProfileUpdateR2Args) -> ProfileUpdateSpec {
    ProfileUpdateSpec {
        name: args.name,
        provider: CreateProviderSpec::R2(ProfileCreateR2Spec {
            bucket: args.bucket,
            account_id: args.account_id,
            endpoint_url: args.endpoint_url,
            credential_source: args.credential_source,
            access_key_id: args.access_key_id,
            secret_access_key: args.secret_access_key,
            key_prefix: args.key_prefix,
        }),
        actor: update_actor_spec(args.actor),
    }
}

fn update_gcs_spec(args: ProfileUpdateGcsArgs) -> ProfileUpdateSpec {
    ProfileUpdateSpec {
        name: args.name,
        provider: CreateProviderSpec::Gcs(ProfileCreateGcsSpec {
            bucket: args.bucket,
            service_account_key_path: args.service_account_key_path,
            key_prefix: args.key_prefix,
        }),
        actor: update_actor_spec(args.actor),
    }
}

fn update_azure_spec(args: ProfileUpdateAzureArgs) -> ProfileUpdateSpec {
    ProfileUpdateSpec {
        name: args.name,
        provider: CreateProviderSpec::Azure(ProfileCreateAzureSpec {
            account_name: args.account_name,
            container_name: args.container_name,
            access_key: args.access_key,
            endpoint_url: args.endpoint_url,
            key_prefix: args.key_prefix,
        }),
        actor: update_actor_spec(args.actor),
    }
}

fn update_local_spec(args: ProfileUpdateLocalArgs) -> ProfileUpdateSpec {
    ProfileUpdateSpec {
        name: args.name,
        provider: CreateProviderSpec::Local(ProfileCreateLocalSpec {
            root: args.root,
            key_prefix: args.key_prefix,
        }),
        actor: update_actor_spec(args.actor),
    }
}

fn update_remote_spec(args: ProfileUpdateRemoteArgs) -> ProfileUpdateSpec {
    ProfileUpdateSpec {
        name: args.name,
        provider: CreateProviderSpec::Remote(ProfileCreateRemoteSpec {
            server_url: args.server_url,
            auth_token: args.auth_token,
            ca_cert_path: args.ca_cert_path,
        }),
        actor: update_actor_spec(args.actor),
    }
}

fn update_actor_spec(actor: ProfileUpdateActorArgs) -> CreateActorSpec {
    CreateActorSpec {
        kind: actor.actor_kind,
        id: actor.actor_id,
    }
}

pub(super) fn has_update_flags(spec: &ProfileUpdateSpec) -> bool {
    spec.actor.kind.is_some()
        || spec.actor.id.is_some()
        || match &spec.provider {
            CreateProviderSpec::S3(args) => {
                args.bucket.is_some()
                    || args.region.is_some()
                    || args.credential_source.is_some()
                    || args.access_key_id.is_some()
                    || args.secret_access_key.is_some()
                    || args.endpoint_url.is_some()
                    || args.session_token.is_some()
                    || args.key_prefix.is_some()
            }
            CreateProviderSpec::R2(args) => {
                args.bucket.is_some()
                    || args.account_id.is_some()
                    || args.endpoint_url.is_some()
                    || args.credential_source.is_some()
                    || args.access_key_id.is_some()
                    || args.secret_access_key.is_some()
                    || args.key_prefix.is_some()
            }
            CreateProviderSpec::Gcs(args) => {
                args.bucket.is_some()
                    || args.service_account_key_path.is_some()
                    || args.key_prefix.is_some()
            }
            CreateProviderSpec::Azure(args) => {
                args.account_name.is_some()
                    || args.container_name.is_some()
                    || args.access_key.is_some()
                    || args.endpoint_url.is_some()
                    || args.key_prefix.is_some()
            }
            CreateProviderSpec::Local(args) => args.root.is_some() || args.key_prefix.is_some(),
            CreateProviderSpec::Remote(args) => {
                args.server_url.is_some()
                    || args.auth_token.is_some()
                    || args.ca_cert_path.is_some()
            }
        }
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

impl CreateProviderSpec {
    fn provider(&self) -> ProfileProvider {
        match self {
            Self::S3(_) => ProfileProvider::S3,
            Self::R2(_) => ProfileProvider::R2,
            Self::Gcs(_) => ProfileProvider::Gcs,
            Self::Azure(_) => ProfileProvider::Azure,
            Self::Local(_) => ProfileProvider::Local,
            Self::Remote(_) => ProfileProvider::Remote,
        }
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
    let source = FieldSource::from_runtime(runtime);
    match spec.provider {
        CreateProviderSpec::Local(spec) => {
            embedded_profile(local_store(None, &spec, source)?, actor)
        }
        CreateProviderSpec::S3(spec) => embedded_profile(s3_store(None, &spec, source)?, actor),
        CreateProviderSpec::R2(spec) => embedded_profile(r2_store(None, &spec, source)?, actor),
        CreateProviderSpec::Gcs(spec) => embedded_profile(gcs_store(None, &spec, source)?, actor),
        CreateProviderSpec::Azure(spec) => {
            embedded_profile(azure_store(None, &spec, source)?, actor)
        }
        CreateProviderSpec::Remote(spec) => remote_profile(name, None, &spec, actor, source),
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
    source: FieldSource,
) -> Result<AwsS3Credentials, CliError> {
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
            session_token: blank_to_none(session_token.cloned()).map(SecretString::from),
        }),
    }
}

fn build_r2_credentials(
    credential_source: Option<&str>,
    access_key_id: Option<&String>,
    secret_access_key: Option<&String>,
    source: FieldSource,
) -> Result<CloudflareR2Credentials, CliError> {
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
) -> Result<String, CliError> {
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
) -> Result<Option<String>, CliError> {
    if value.is_some() {
        return Ok(blank_to_none(value.cloned()));
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
) -> Result<SecretString, CliError> {
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

fn local_store(
    current: Option<&StoreConfig>,
    args: &ProfileCreateLocalSpec,
    source: FieldSource,
) -> Result<StoreConfig, CliError> {
    let current = current
        .map(|store| {
            let StoreConfig::LocalFs { root, key_prefix } = store else {
                return None;
            };
            Some((root, key_prefix.as_ref()))
        })
        .map(|current| current.expect("local store builder should receive a local current store"));
    Ok(StoreConfig::LocalFs {
        root: required_field(
            args.root.as_ref(),
            current.map(|value| value.0),
            "root",
            source,
        )?,
        key_prefix: optional_field(
            args.key_prefix.as_ref(),
            current.and_then(|value| value.1),
            "key prefix",
            source,
        )?,
    })
}

fn s3_store(
    current: Option<&StoreConfig>,
    args: &ProfileCreateS3Spec,
    source: FieldSource,
) -> Result<StoreConfig, CliError> {
    let current = current
        .map(|store| {
            let StoreConfig::AwsS3 {
                bucket,
                region,
                endpoint_url,
                credentials,
                key_prefix,
                force_path_style,
            } = store
            else {
                return None;
            };
            Some((
                bucket,
                region,
                endpoint_url.as_ref(),
                credentials,
                key_prefix.as_ref(),
                *force_path_style,
            ))
        })
        .map(|current| current.expect("s3 store builder should receive an s3 current store"));
    let credentials = match (current.map(|value| value.3), source) {
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
    let region = match (args.region.as_ref(), current.map(|value| value.1), source) {
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
    Ok(StoreConfig::AwsS3 {
        bucket: required_field(
            args.bucket.as_ref(),
            current.map(|value| value.0),
            "bucket",
            source,
        )?,
        region,
        endpoint_url: optional_field(
            args.endpoint_url.as_ref(),
            current.and_then(|value| value.2),
            "endpoint url",
            source,
        )?,
        credentials,
        key_prefix: optional_field(
            args.key_prefix.as_ref(),
            current.and_then(|value| value.4),
            "key prefix",
            source,
        )?,
        force_path_style: current.map_or(args.force_path_style, |value| value.5),
    })
}

fn r2_store(
    current: Option<&StoreConfig>,
    args: &ProfileCreateR2Spec,
    source: FieldSource,
) -> Result<StoreConfig, CliError> {
    let current = current
        .map(|store| {
            let StoreConfig::CloudflareR2 {
                bucket,
                account_id,
                endpoint_url,
                credentials,
                key_prefix,
            } = store
            else {
                return None;
            };
            Some((
                bucket,
                account_id,
                endpoint_url,
                credentials,
                key_prefix.as_ref(),
            ))
        })
        .map(|current| current.expect("r2 store builder should receive an r2 current store"));
    let credentials = match (current.map(|value| value.3), source) {
        (Some(current), FieldSource::Prompt) => prompt_r2_credentials(current.clone())?,
        (Some(current), FieldSource::Fail) => updated_r2_credentials(current.clone(), args)?,
        (None, _) => build_r2_credentials(
            args.credential_source.as_deref(),
            args.access_key_id.as_ref(),
            args.secret_access_key.as_ref(),
            source,
        )?,
    };
    Ok(StoreConfig::CloudflareR2 {
        bucket: required_field(
            args.bucket.as_ref(),
            current.map(|value| value.0),
            "bucket",
            source,
        )?,
        account_id: required_field(
            args.account_id.as_ref(),
            current.map(|value| value.1),
            "account-id",
            source,
        )?,
        endpoint_url: required_field(
            args.endpoint_url.as_ref(),
            current.map(|value| value.2),
            "endpoint-url",
            source,
        )?,
        credentials,
        key_prefix: optional_field(
            args.key_prefix.as_ref(),
            current.and_then(|value| value.4),
            "key prefix",
            source,
        )?,
    })
}

fn gcs_store(
    current: Option<&StoreConfig>,
    args: &ProfileCreateGcsSpec,
    source: FieldSource,
) -> Result<StoreConfig, CliError> {
    let current = current
        .map(|store| {
            let StoreConfig::GcpGcs {
                bucket,
                credentials: GcpGcsCredentials::ServiceAccountFile { path },
                key_prefix,
            } = store
            else {
                return None;
            };
            Some((bucket, path, key_prefix.as_ref()))
        })
        .map(|current| current.expect("gcs store builder should receive a gcs current store"));
    Ok(StoreConfig::GcpGcs {
        bucket: required_field(
            args.bucket.as_ref(),
            current.map(|value| value.0),
            "bucket",
            source,
        )?,
        credentials: GcpGcsCredentials::ServiceAccountFile {
            path: required_field(
                args.service_account_key_path.as_ref(),
                current.map(|value| value.1),
                "service-account-key-path",
                source,
            )?,
        },
        key_prefix: optional_field(
            args.key_prefix.as_ref(),
            current.and_then(|value| value.2),
            "key prefix",
            source,
        )?,
    })
}

fn azure_store(
    current: Option<&StoreConfig>,
    args: &ProfileCreateAzureSpec,
    source: FieldSource,
) -> Result<StoreConfig, CliError> {
    let current = current
        .map(|store| {
            let StoreConfig::AzureAbs {
                account_name,
                container_name,
                credentials: AzureAbsCredentials::AccessKey { access_key },
                endpoint_url,
                key_prefix,
            } = store
            else {
                return None;
            };
            Some((
                account_name,
                container_name,
                access_key,
                endpoint_url.as_ref(),
                key_prefix.as_ref(),
            ))
        })
        .map(|current| current.expect("azure store builder should receive an azure current store"));
    Ok(StoreConfig::AzureAbs {
        account_name: required_field(
            args.account_name.as_ref(),
            current.map(|value| value.0),
            "account-name",
            source,
        )?,
        container_name: required_field(
            args.container_name.as_ref(),
            current.map(|value| value.1),
            "container-name",
            source,
        )?,
        credentials: AzureAbsCredentials::AccessKey {
            access_key: required_secret(
                args.access_key.as_ref(),
                current.map(|value| value.2),
                "access-key",
                source,
            )?,
        },
        endpoint_url: optional_field(
            args.endpoint_url.as_ref(),
            current.and_then(|value| value.3),
            "endpoint url",
            source,
        )?,
        key_prefix: optional_field(
            args.key_prefix.as_ref(),
            current.and_then(|value| value.4),
            "key prefix",
            source,
        )?,
    })
}

fn remote_profile(
    name: &str,
    current: Option<&ProfileConfig>,
    args: &ProfileCreateRemoteSpec,
    actor: ProfileActorConfig,
    source: FieldSource,
) -> Result<ProfileConfig, CliError> {
    let current = current
        .map(|profile| {
            let ProfileConfig::Remote {
                server_url,
                default_namespace,
                auth_token,
                ca_cert_path,
                ..
            } = profile
            else {
                return None;
            };
            Some((
                server_url,
                default_namespace,
                auth_token.as_ref(),
                ca_cert_path.as_ref(),
            ))
        })
        .map(|current| current.expect("remote profile builder should receive a remote profile"));
    let server_url = required_field(
        args.server_url.as_ref(),
        current.map(|value| value.0),
        "server-url",
        source,
    )?;
    let auth_token = if args.auth_token.is_some() {
        blank_to_none(args.auth_token.clone()).map(SecretString::from)
    } else {
        match source {
            FieldSource::Fail => current.and_then(|value| value.2).cloned(),
            FieldSource::Prompt => prompt::prompt_secret_optional(
                "auth token",
                current.and_then(|value| value.2).map(SecretString::expose),
            )?
            .map(SecretString::from),
        }
    };
    let ca_cert_path = optional_field(
        args.ca_cert_path.as_ref(),
        current.and_then(|value| value.3),
        "ca cert path",
        source,
    )?;
    validate_remote_client_config(
        name,
        &server_url,
        auth_token.as_ref(),
        ca_cert_path.as_deref(),
    )?;
    Ok(ProfileConfig::Remote {
        server_url,
        actor,
        default_namespace: current.and_then(|value| value.1.as_ref()).cloned(),
        auth_token,
        ca_cert_path,
    })
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
    args: &CreateActorSpec,
) -> Result<ProfileActorConfig, CliError> {
    if args.kind.is_none() && args.id.is_none() {
        Ok(current)
    } else {
        profile_actor_config(args.kind, args.id.as_deref())
    }
}

fn profile_provider(profile: &ProfileConfig) -> ProfileProvider {
    match profile {
        ProfileConfig::Embedded {
            store: StoreConfig::AwsS3 { .. },
            ..
        } => ProfileProvider::S3,
        ProfileConfig::Embedded {
            store: StoreConfig::CloudflareR2 { .. },
            ..
        } => ProfileProvider::R2,
        ProfileConfig::Embedded {
            store: StoreConfig::GcpGcs { .. },
            ..
        } => ProfileProvider::Gcs,
        ProfileConfig::Embedded {
            store: StoreConfig::AzureAbs { .. },
            ..
        } => ProfileProvider::Azure,
        ProfileConfig::Embedded {
            store: StoreConfig::LocalFs { .. },
            ..
        } => ProfileProvider::Local,
        ProfileConfig::Remote { .. } => ProfileProvider::Remote,
    }
}

fn provider_name(provider: ProfileProvider) -> &'static str {
    match provider {
        ProfileProvider::S3 => "s3",
        ProfileProvider::R2 => "r2",
        ProfileProvider::Gcs => "gcs",
        ProfileProvider::Azure => "azure",
        ProfileProvider::Local => "local",
        ProfileProvider::Remote => "remote",
    }
}

pub(super) fn apply_update_flags(
    name: &str,
    existing: ProfileConfig,
    spec: &ProfileUpdateSpec,
) -> Result<ProfileConfig, CliError> {
    let stored_provider = profile_provider(&existing);
    let requested_provider = spec.provider();
    match (existing, &spec.provider) {
        (
            ProfileConfig::Embedded {
                store,
                actor,
                default_namespace,
                writer_id,
            },
            provider,
        ) => {
            let store = match (&store, provider) {
                (StoreConfig::LocalFs { .. }, CreateProviderSpec::Local(args)) => {
                    local_store(Some(&store), args, FieldSource::Fail)?
                }
                (StoreConfig::AwsS3 { .. }, CreateProviderSpec::S3(args)) => {
                    s3_store(Some(&store), args, FieldSource::Fail)?
                }
                (StoreConfig::CloudflareR2 { .. }, CreateProviderSpec::R2(args)) => {
                    r2_store(Some(&store), args, FieldSource::Fail)?
                }
                (StoreConfig::GcpGcs { .. }, CreateProviderSpec::Gcs(args)) => {
                    gcs_store(Some(&store), args, FieldSource::Fail)?
                }
                (StoreConfig::AzureAbs { .. }, CreateProviderSpec::Azure(args)) => {
                    azure_store(Some(&store), args, FieldSource::Fail)?
                }
                _ => return Err(provider_mismatch(name, stored_provider, requested_provider)),
            };
            Ok(ProfileConfig::Embedded {
                store,
                actor: updated_actor(actor, &spec.actor)?,
                default_namespace,
                writer_id,
            })
        }
        (
            ProfileConfig::Remote {
                server_url,
                actor,
                default_namespace,
                auth_token,
                ca_cert_path,
            },
            CreateProviderSpec::Remote(args),
        ) => {
            let updated_actor = updated_actor(actor.clone(), &spec.actor)?;
            let current = ProfileConfig::Remote {
                server_url,
                actor,
                default_namespace,
                auth_token,
                ca_cert_path,
            };
            remote_profile(name, Some(&current), args, updated_actor, FieldSource::Fail)
        }
        _ => Err(provider_mismatch(name, stored_provider, requested_provider)),
    }
}

pub(super) fn validate_update_provider(
    name: &str,
    existing: &ProfileConfig,
    spec: &ProfileUpdateSpec,
) -> Result<(), CliError> {
    let stored = profile_provider(existing);
    let requested = spec.provider();
    if stored == requested {
        Ok(())
    } else {
        Err(provider_mismatch(name, stored, requested))
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
    args: &ProfileCreateS3Spec,
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
    args: &ProfileCreateR2Spec,
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

pub(super) fn apply_update_interactive(
    name: &str,
    existing: ProfileConfig,
) -> Result<ProfileConfig, CliError> {
    match existing {
        ProfileConfig::Embedded {
            store,
            actor,
            default_namespace,
            writer_id,
        } => {
            let store = match &store {
                StoreConfig::LocalFs { .. } => local_store(
                    Some(&store),
                    &ProfileCreateLocalSpec::default(),
                    FieldSource::Prompt,
                )?,
                StoreConfig::AwsS3 { .. } => s3_store(
                    Some(&store),
                    &ProfileCreateS3Spec::default(),
                    FieldSource::Prompt,
                )?,
                StoreConfig::CloudflareR2 { .. } => r2_store(
                    Some(&store),
                    &ProfileCreateR2Spec::default(),
                    FieldSource::Prompt,
                )?,
                StoreConfig::GcpGcs { .. } => gcs_store(
                    Some(&store),
                    &ProfileCreateGcsSpec::default(),
                    FieldSource::Prompt,
                )?,
                StoreConfig::AzureAbs { .. } => azure_store(
                    Some(&store),
                    &ProfileCreateAzureSpec::default(),
                    FieldSource::Prompt,
                )?,
            };
            Ok(ProfileConfig::Embedded {
                store,
                actor,
                default_namespace,
                writer_id,
            })
        }
        ref current @ ProfileConfig::Remote { ref actor, .. } => remote_profile(
            name,
            Some(current),
            &ProfileCreateRemoteSpec::default(),
            actor.clone(),
            FieldSource::Prompt,
        ),
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
        CreateProviderSpec, ProfileCreateAzureSpec, ProfileCreateLocalSpec,
        ProfileCreateRemoteSpec, ProfileCreateS3Spec, ProfileUpdateSpec,
    };
    use crate::args::RuntimeBehavior;
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
    fn update_rejects_a_provider_that_does_not_match_the_profile() {
        let local_fs = ProfileConfig::Embedded {
            store: StoreConfig::LocalFs {
                root: "/tmp/store".to_owned(),
                key_prefix: None,
            },
            actor: crate::config::ProfileActorConfig::default(),
            default_namespace: None,
            writer_id: None,
        };

        let spec = update_spec(CreateProviderSpec::S3(ProfileCreateS3Spec::default()));
        let error = apply_update_flags("default", local_fs, &spec)
            .expect_err("s3 must not apply to local-fs");
        assert_eq!(
            error.message,
            "profile `default` uses provider `local` not `s3`"
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

        let spec = update_spec(CreateProviderSpec::Remote(ProfileCreateRemoteSpec {
            auth_token: Some("new-token".to_owned()),
            ..ProfileCreateRemoteSpec::default()
        }));
        let updated =
            apply_update_flags("default", remote, &spec).expect("update remote auth token");

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

        let spec = update_spec(CreateProviderSpec::Local(ProfileCreateLocalSpec {
            root: Some("/tmp/moved".to_owned()),
            ..ProfileCreateLocalSpec::default()
        }));
        let updated = apply_update_flags("default", local_fs, &spec).expect("update local-fs root");

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

        let spec = update_spec(CreateProviderSpec::Remote(ProfileCreateRemoteSpec {
            server_url: Some("http://example.internal".to_owned()),
            auth_token: Some("new-token".to_owned()),
            ..ProfileCreateRemoteSpec::default()
        }));
        let error = apply_update_flags("default", remote, &spec)
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

        let incomplete = update_spec(CreateProviderSpec::S3(ProfileCreateS3Spec {
            credential_source: Some("static".to_owned()),
            access_key_id: Some("access".to_owned()),
            ..ProfileCreateS3Spec::default()
        }));
        let error =
            apply_update_flags("default", profile.clone(), &incomplete).expect_err("failed switch");
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

        let complete = update_spec(CreateProviderSpec::S3(ProfileCreateS3Spec {
            credential_source: Some("static".to_owned()),
            access_key_id: Some("access".to_owned()),
            secret_access_key: Some("secret".to_owned()),
            ..ProfileCreateS3Spec::default()
        }));
        let updated = apply_update_flags("default", profile, &complete).expect("complete switch");
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

    fn update_spec(provider: CreateProviderSpec) -> ProfileUpdateSpec {
        ProfileUpdateSpec {
            name: "default".to_owned(),
            provider,
            actor: empty_actor(),
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
