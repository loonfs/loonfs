//! Builds provider-specific profile configurations and applies validated updates.

use crate::args::{
    ProfileCreateActorArgs, ProfileCreateCommand, ProfileGcsArgs, ProfileLocalArgs, ProfileR2Args,
    ProfileRemoteArgs, ProfileS3Args, ProfileUpdateActorArgs, ProfileUpdateCommand,
    RuntimeBehavior,
};
use crate::config::{
    absolute_env_path, ensure_subject_pairing, validate_remote_client_config, ProfileConfig,
    StoreConfig,
};
use crate::error::CliError;
use crate::prompt;
use crate::resolve::subject_flag_error;
use loonfs_objectstore::{AwsS3Credentials, CloudflareR2Credentials, GcpGcsCredentials};
use loonfs_types::config::non_blank;
use loonfs_types::{ActorId, PrincipalId, PrincipalScope, PrincipalSet, SecretString, SubjectId};
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

pub(super) struct ProfileUpdateSpec {
    pub(super) name: String,
    provider: CreateProviderSpec,
    actor: ProfileUpdateActorArgs,
}

impl ProfileUpdateSpec {
    fn provider(&self) -> ProfileProvider {
        self.provider.provider()
    }
}

pub(super) fn profile_update_spec(command: ProfileUpdateCommand) -> ProfileUpdateSpec {
    let (name, provider, actor) = match command {
        ProfileUpdateCommand::S3(args) => (
            args.name,
            CreateProviderSpec::S3 {
                store: args.store,
                force_path_style: args.force_path_style,
            },
            args.actor,
        ),
        ProfileUpdateCommand::R2(args) => {
            (args.name, CreateProviderSpec::R2(args.store), args.actor)
        }
        ProfileUpdateCommand::Gcs(args) => {
            (args.name, CreateProviderSpec::Gcs(args.store), args.actor)
        }
        ProfileUpdateCommand::Local(args) => {
            (args.name, CreateProviderSpec::Local(args.store), args.actor)
        }
        ProfileUpdateCommand::Remote(args) => (
            args.name,
            CreateProviderSpec::Remote {
                server: args.server,
                auth_token: args.auth_token,
                ca_cert_path: args.ca_cert_path,
            },
            args.actor,
        ),
    };
    ProfileUpdateSpec {
        name,
        provider,
        actor,
    }
}

pub(super) fn has_update_flags(spec: &ProfileUpdateSpec) -> bool {
    spec.actor.actor_id.is_some()
        || spec.actor.subject_id.is_some()
        || spec.actor.principal_scope.is_some()
        || spec.actor.principals.is_some()
        || match &spec.provider {
            CreateProviderSpec::S3 {
                store,
                force_path_style,
            } => {
                store.bucket.is_some()
                    || store.region.is_some()
                    || store.credential_source.is_some()
                    || store.access_key_id.is_some()
                    || store.secret_access_key.is_some()
                    || store.endpoint_url.is_some()
                    || store.session_token.is_some()
                    || force_path_style.is_some()
                    || store.key_prefix.is_some()
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
            CreateProviderSpec::Local(args) => args.root.is_some() || args.key_prefix.is_some(),
            CreateProviderSpec::Remote {
                server,
                auth_token,
                ca_cert_path,
            } => server.server_url.is_some() || auth_token.is_some() || ca_cert_path.is_some(),
        }
}

// --- create/update helpers ---

#[derive(Debug)]
pub(super) struct CreateProfileSpec {
    provider: CreateProviderSpec,
    actor: ProfileCreateActorArgs,
}

struct ProfileActor {
    actor_id: Option<ActorId>,
    subject_id: Option<SubjectId>,
    principal_scope: Option<PrincipalScope>,
    principals: Option<Vec<PrincipalId>>,
}

#[derive(Debug, Clone)]
enum CreateProviderSpec {
    S3 {
        store: ProfileS3Args,
        force_path_style: Option<bool>,
    },
    R2(ProfileR2Args),
    Gcs(ProfileGcsArgs),
    Local(ProfileLocalArgs),
    Remote {
        server: ProfileRemoteArgs,
        auth_token: Option<String>,
        ca_cert_path: Option<String>,
    },
}

impl CreateProviderSpec {
    fn provider(&self) -> ProfileProvider {
        match self {
            Self::S3 { .. } => ProfileProvider::S3,
            Self::R2(_) => ProfileProvider::R2,
            Self::Gcs(_) => ProfileProvider::Gcs,
            Self::Local(_) => ProfileProvider::Local,
            Self::Remote { .. } => ProfileProvider::Remote,
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

pub(super) fn create_profile_spec_from_create(
    command: ProfileCreateCommand,
) -> (String, CreateProfileSpec) {
    let (name, provider, actor) = match command {
        ProfileCreateCommand::S3(args) => (
            args.name,
            CreateProviderSpec::S3 {
                store: args.store,
                force_path_style: Some(args.force_path_style),
            },
            args.actor,
        ),
        ProfileCreateCommand::R2(args) => {
            (args.name, CreateProviderSpec::R2(args.store), args.actor)
        }
        ProfileCreateCommand::Gcs(args) => {
            (args.name, CreateProviderSpec::Gcs(args.store), args.actor)
        }
        ProfileCreateCommand::Local(args) => {
            (args.name, CreateProviderSpec::Local(args.store), args.actor)
        }
        ProfileCreateCommand::Remote(args) => (
            args.name,
            CreateProviderSpec::Remote {
                server: args.server,
                auth_token: args.auth_token,
                ca_cert_path: args.ca_cert_path,
            },
            args.actor,
        ),
    };
    (name, CreateProfileSpec { provider, actor })
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
    let provider = match prompt::prompt_choice("provider", &["s3", "r2", "gcs", "local", "remote"])?
        .as_str()
    {
        "s3" => CreateProviderSpec::S3 {
            store: ProfileS3Args::default(),
            force_path_style: None,
        },
        "r2" => CreateProviderSpec::R2(ProfileR2Args::default()),
        "gcs" => CreateProviderSpec::Gcs(ProfileGcsArgs::default()),
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
            CreateProviderSpec::Local(ProfileLocalArgs {
                root: Some(absolute_local_root(&path)?),
                key_prefix: None,
            })
        }
        _ => CreateProviderSpec::Remote {
            server: ProfileRemoteArgs::default(),
            auth_token: None,
            ca_cert_path: None,
        },
    };
    build_profile_from_create_spec(
        name,
        CreateProfileSpec {
            provider,
            actor: ProfileCreateActorArgs::default(),
        },
        runtime,
    )
}

pub(super) fn build_profile_from_create_spec(
    name: &str,
    spec: CreateProfileSpec,
    runtime: RuntimeBehavior,
) -> Result<ProfileConfig, CliError> {
    let actor = ProfileActor {
        actor_id: spec.actor.actor_id,
        subject_id: spec.actor.subject_id,
        principal_scope: parse_profile_principal_scope(spec.actor.principal_scope.as_deref())?,
        principals: parse_profile_principals(spec.actor.principals.as_deref())?,
    };
    ensure_subject_pairing(actor.principal_scope.as_ref(), actor.principals.as_deref())
        .map_err(subject_flag_error)?;
    let source = FieldSource::from_runtime(runtime);
    match spec.provider {
        CreateProviderSpec::Local(spec) => {
            embedded_profile(local_store(None, &spec, source)?, actor)
        }
        CreateProviderSpec::S3 {
            store,
            force_path_style,
        } => embedded_profile(s3_store(None, &store, force_path_style, source)?, actor),
        CreateProviderSpec::R2(spec) => embedded_profile(r2_store(None, &spec, source)?, actor),
        CreateProviderSpec::Gcs(spec) => embedded_profile(gcs_store(None, &spec, source)?, actor),
        CreateProviderSpec::Remote {
            server,
            auth_token,
            ca_cert_path,
        } => remote_profile(
            name,
            None,
            &server,
            auth_token.as_ref(),
            ca_cert_path.as_ref(),
            actor,
            source,
        ),
    }
}

fn embedded_profile(store: StoreConfig, actor: ProfileActor) -> Result<ProfileConfig, CliError> {
    Ok(ProfileConfig::Embedded {
        store,
        actor_id: actor.actor_id,
        subject_id: actor.subject_id,
        principal_scope: actor.principal_scope,
        principals: actor.principals,
        default_namespace: None,
        writer_id: None,
    })
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
            session_token: non_blank(session_token.cloned()).map(SecretString::from),
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

fn absolute_local_root(root: &Path) -> Result<String, CliError> {
    std::path::absolute(root)
        .map_err(CliError::io)?
        .into_os_string()
        .into_string()
        .map_err(|_| CliError::invalid_config("local-fs root is not valid UTF-8"))
}

fn local_store(
    current: Option<&StoreConfig>,
    args: &ProfileLocalArgs,
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
    let root = required_field(
        args.root.as_ref(),
        current.map(|value| value.0),
        "root",
        source,
    )?;
    let root = if args.root.is_some() || current.is_none() || matches!(source, FieldSource::Prompt)
    {
        absolute_local_root(Path::new(&root))?
    } else {
        root
    };
    Ok(StoreConfig::LocalFs {
        root,
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
    args: &ProfileS3Args,
    force_path_style: Option<bool>,
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
        force_path_style: force_path_style
            .or(current.map(|value| value.5))
            .unwrap_or_default(),
    })
}

fn r2_store(
    current: Option<&StoreConfig>,
    args: &ProfileR2Args,
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
    args: &ProfileGcsArgs,
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

fn remote_profile(
    name: &str,
    current: Option<&ProfileConfig>,
    server: &ProfileRemoteArgs,
    auth_token: Option<&String>,
    ca_cert_path: Option<&String>,
    actor: ProfileActor,
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
        server.server_url.as_ref(),
        current.map(|value| value.0),
        "server-url",
        source,
    )?;
    let auth_token = if auth_token.is_some() {
        non_blank(auth_token.cloned()).map(SecretString::from)
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
        ca_cert_path,
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
        actor_id: actor.actor_id,
        subject_id: actor.subject_id,
        principal_scope: actor.principal_scope,
        principals: actor.principals,
        default_namespace: current.and_then(|value| value.1.as_ref()).cloned(),
        auth_token,
        ca_cert_path,
    })
}

fn parse_profile_principal_scope(
    principal_scope: Option<&str>,
) -> Result<Option<PrincipalScope>, CliError> {
    principal_scope
        .map(|principal_scope| {
            PrincipalScope::parse(principal_scope).map_err(|error| {
                CliError::invalid_request(format!("invalid `--principal-scope`: {error}"))
                    .with_param("--principal-scope")
            })
        })
        .transpose()
}

fn parse_profile_principals(
    principals: Option<&str>,
) -> Result<Option<Vec<PrincipalId>>, CliError> {
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
                actor_id: actor,
                subject_id,
                principal_scope,
                principals,
                default_namespace,
                writer_id,
            },
            provider,
        ) => {
            let store = match (&store, provider) {
                (StoreConfig::LocalFs { .. }, CreateProviderSpec::Local(args)) => {
                    local_store(Some(&store), args, FieldSource::Fail)?
                }
                (
                    StoreConfig::AwsS3 { .. },
                    CreateProviderSpec::S3 {
                        store: args,
                        force_path_style,
                    },
                ) => s3_store(Some(&store), args, *force_path_style, FieldSource::Fail)?,
                (StoreConfig::CloudflareR2 { .. }, CreateProviderSpec::R2(args)) => {
                    r2_store(Some(&store), args, FieldSource::Fail)?
                }
                (StoreConfig::GcpGcs { .. }, CreateProviderSpec::Gcs(args)) => {
                    gcs_store(Some(&store), args, FieldSource::Fail)?
                }
                _ => return Err(provider_mismatch(name, stored_provider, requested_provider)),
            };
            let updated_principal_scope =
                parse_profile_principal_scope(spec.actor.principal_scope.as_deref())?
                    .or(principal_scope);
            let updated_principals =
                parse_profile_principals(spec.actor.principals.as_deref())?.or(principals);
            ensure_subject_pairing(
                updated_principal_scope.as_ref(),
                updated_principals.as_deref(),
            )
            .map_err(subject_flag_error)?;
            Ok(ProfileConfig::Embedded {
                store,
                actor_id: spec.actor.actor_id.clone().or(actor),
                subject_id: spec.actor.subject_id.clone().or(subject_id),
                principal_scope: updated_principal_scope,
                principals: updated_principals,
                default_namespace,
                writer_id,
            })
        }
        (
            ProfileConfig::Remote {
                server_url,
                actor_id: actor,
                subject_id,
                principal_scope,
                principals,
                default_namespace,
                auth_token,
                ca_cert_path,
            },
            CreateProviderSpec::Remote {
                server: server_args,
                auth_token: auth_token_flag,
                ca_cert_path: ca_cert_path_flag,
            },
        ) => {
            let updated_actor = spec.actor.actor_id.clone().or_else(|| actor.clone());
            let updated_subject_id = spec.actor.subject_id.clone().or_else(|| subject_id.clone());
            let updated_principal_scope =
                parse_profile_principal_scope(spec.actor.principal_scope.as_deref())?
                    .or_else(|| principal_scope.clone());
            let updated_principals = parse_profile_principals(spec.actor.principals.as_deref())?
                .or_else(|| principals.clone());
            ensure_subject_pairing(
                updated_principal_scope.as_ref(),
                updated_principals.as_deref(),
            )
            .map_err(subject_flag_error)?;
            let current = ProfileConfig::Remote {
                server_url,
                actor_id: actor,
                subject_id,
                principal_scope,
                principals,
                default_namespace,
                auth_token,
                ca_cert_path,
            };
            remote_profile(
                name,
                Some(&current),
                server_args,
                auth_token_flag.as_ref(),
                ca_cert_path_flag.as_ref(),
                ProfileActor {
                    actor_id: updated_actor,
                    subject_id: updated_subject_id,
                    principal_scope: updated_principal_scope,
                    principals: updated_principals,
                },
                FieldSource::Fail,
            )
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
    args: &ProfileS3Args,
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
            session_token: non_blank(args.session_token.clone()).map(SecretString::from),
        }),
    }
}

fn updated_r2_credentials(
    current: CloudflareR2Credentials,
    args: &ProfileR2Args,
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
            actor_id: actor,
            subject_id,
            principal_scope,
            principals,
            default_namespace,
            writer_id,
        } => {
            let store = match &store {
                StoreConfig::LocalFs { .. } => local_store(
                    Some(&store),
                    &ProfileLocalArgs::default(),
                    FieldSource::Prompt,
                )?,
                StoreConfig::AwsS3 { .. } => s3_store(
                    Some(&store),
                    &ProfileS3Args::default(),
                    None,
                    FieldSource::Prompt,
                )?,
                StoreConfig::CloudflareR2 { .. } => {
                    r2_store(Some(&store), &ProfileR2Args::default(), FieldSource::Prompt)?
                }
                StoreConfig::GcpGcs { .. } => gcs_store(
                    Some(&store),
                    &ProfileGcsArgs::default(),
                    FieldSource::Prompt,
                )?,
            };
            Ok(ProfileConfig::Embedded {
                store,
                actor_id: actor,
                subject_id,
                principal_scope,
                principals,
                default_namespace,
                writer_id,
            })
        }
        ref current @ ProfileConfig::Remote {
            actor_id: ref actor,
            ..
        } => remote_profile(
            name,
            Some(current),
            &ProfileRemoteArgs::default(),
            None,
            None,
            ProfileActor {
                actor_id: actor.clone(),
                subject_id: current.subject_id(),
                principal_scope: current.principal_scope(),
                principals: current.principals(),
            },
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
        apply_update_flags, build_profile_from_create_spec, has_update_flags, profile_update_spec,
        CreateProfileSpec, CreateProviderSpec, ProfileUpdateSpec,
    };
    use crate::args::{
        Cli, Command, ProfileCommand, ProfileCreateActorArgs, ProfileLocalArgs, ProfileR2Args,
        ProfileRemoteArgs, ProfileS3Args, ProfileUpdateActorArgs, RuntimeBehavior, TopLevelCommand,
    };
    use crate::config::{ProfileConfig, StoreConfig};
    use clap::Parser;
    use loonfs_objectstore::{AwsS3Credentials, CloudflareR2Credentials};

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
        let profile = build_profile_from_create_spec(
            "default",
            CreateProfileSpec {
                provider: CreateProviderSpec::R2(ProfileR2Args {
                    bucket: Some("documents".to_owned()),
                    account_id: Some("account".to_owned()),
                    endpoint_url: Some("https://account.r2.cloudflarestorage.com".to_owned()),
                    access_key_id: Some("access".to_owned()),
                    secret_access_key: Some("secret".to_owned()),
                    key_prefix: Some("tenant-a".to_owned()),
                    ..ProfileR2Args::default()
                }),
                actor: ProfileCreateActorArgs::default(),
            },
            non_interactive_runtime(),
        )
        .expect("build r2 profile");
        let ProfileConfig::Embedded { store, .. } = profile else {
            panic!("expected embedded profile, got {profile:?}");
        };
        assert_eq!(
            store,
            StoreConfig::CloudflareR2 {
                bucket: "documents".to_owned(),
                account_id: "account".to_owned(),
                endpoint_url: "https://account.r2.cloudflarestorage.com".to_owned(),
                credentials: CloudflareR2Credentials::Static {
                    access_key_id: "access".into(),
                    secret_access_key: "secret".into(),
                },
                key_prefix: Some("tenant-a".to_owned()),
            }
        );
    }

    #[test]
    fn update_r2_flags_round_trip_credentials_and_preserve_unspecified_fields() {
        let profile = build_profile_from_create_spec(
            "default",
            CreateProfileSpec {
                provider: CreateProviderSpec::R2(ProfileR2Args {
                    bucket: Some("documents".to_owned()),
                    account_id: Some("account".to_owned()),
                    endpoint_url: Some("https://account.r2.cloudflarestorage.com".to_owned()),
                    ..ProfileR2Args::default()
                }),
                actor: ProfileCreateActorArgs::default(),
            },
            non_interactive_runtime(),
        )
        .expect("build ambient r2 profile");
        let spec = update_spec(CreateProviderSpec::R2(ProfileR2Args {
            bucket: Some("archive".to_owned()),
            credential_source: Some("static".to_owned()),
            access_key_id: Some("access".to_owned()),
            secret_access_key: Some("secret".to_owned()),
            key_prefix: Some("tenant-b".to_owned()),
            ..ProfileR2Args::default()
        }));
        let updated = apply_update_flags("default", profile.clone(), &spec).expect("update r2");
        let ProfileConfig::Embedded { store, .. } = &updated else {
            panic!("expected embedded profile, got {updated:?}");
        };
        assert_eq!(
            *store,
            StoreConfig::CloudflareR2 {
                bucket: "archive".to_owned(),
                account_id: "account".to_owned(),
                endpoint_url: "https://account.r2.cloudflarestorage.com".to_owned(),
                credentials: CloudflareR2Credentials::Static {
                    access_key_id: "access".into(),
                    secret_access_key: "secret".into(),
                },
                key_prefix: Some("tenant-b".to_owned()),
            }
        );
        let reset = update_spec(CreateProviderSpec::R2(ProfileR2Args {
            bucket: Some("documents".to_owned()),
            credential_source: Some("ambient".to_owned()),
            key_prefix: Some(String::new()),
            ..ProfileR2Args::default()
        }));
        assert_eq!(
            apply_update_flags("default", updated, &reset).expect("restore r2 profile"),
            profile
        );
    }

    #[test]
    fn update_s3_flags_set_path_style_and_preserve_it_when_omitted() {
        let mut profile = build_profile_from_create_spec(
            "default",
            CreateProfileSpec {
                provider: CreateProviderSpec::S3 {
                    store: ProfileS3Args {
                        bucket: Some("documents".to_owned()),
                        region: Some("us-east-1".to_owned()),
                        ..ProfileS3Args::default()
                    },
                    force_path_style: None,
                },
                actor: ProfileCreateActorArgs::default(),
            },
            non_interactive_runtime(),
        )
        .expect("build ambient s3 profile");

        for (flags, expected_value) in [
            (&["--force-path-style", "true"][..], true),
            (&[][..], true),
            (&["--force-path-style", "false"][..], false),
            (&[][..], false),
        ] {
            let cli = Cli::try_parse_from(
                ["loonfs", "profile", "update", "s3", "default"]
                    .into_iter()
                    .chain(flags.iter().copied()),
            )
            .expect("parse s3 update flags");
            let TopLevelCommand::Configured(Command::Profile {
                command: ProfileCommand::Update { provider },
            }) = cli.command
            else {
                panic!("expected profile update, got {:?}", cli.command);
            };
            let spec = profile_update_spec(*provider);
            assert_eq!(has_update_flags(&spec), !flags.is_empty());

            let mut expected = profile.clone();
            let ProfileConfig::Embedded {
                store:
                    StoreConfig::AwsS3 {
                        force_path_style, ..
                    },
                ..
            } = &mut expected
            else {
                panic!("expected s3 profile, got {expected:?}");
            };
            *force_path_style = expected_value;
            profile = apply_update_flags("default", profile, &spec).expect("update s3");
            assert_eq!(profile, expected);
        }
    }

    #[test]
    fn no_static_flags_select_ambient_without_storing_secrets() {
        let s3 = build_profile_from_create_spec(
            "default",
            CreateProfileSpec {
                provider: CreateProviderSpec::S3 {
                    store: ProfileS3Args {
                        bucket: Some("bucket".to_owned()),
                        region: Some("us-east-1".to_owned()),
                        ..ProfileS3Args::default()
                    },
                    force_path_style: None,
                },
                actor: ProfileCreateActorArgs::default(),
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
                provider: CreateProviderSpec::S3 {
                    store: ProfileS3Args {
                        bucket: Some("bucket".to_owned()),
                        region: Some("us-east-1".to_owned()),
                        access_key_id: Some("access".to_owned()),
                        ..ProfileS3Args::default()
                    },
                    force_path_style: None,
                },
                actor: ProfileCreateActorArgs::default(),
            },
            non_interactive_runtime(),
        )
        .expect_err("a partial static set must fail");
        assert!(incomplete.message.contains("secret-access-key"));

        let complete = build_profile_from_create_spec(
            "default",
            CreateProfileSpec {
                provider: CreateProviderSpec::S3 {
                    store: ProfileS3Args {
                        bucket: Some("bucket".to_owned()),
                        region: Some("us-east-1".to_owned()),
                        access_key_id: Some("access".to_owned()),
                        secret_access_key: Some("secret".to_owned()),
                        session_token: Some("session".to_owned()),
                        ..ProfileS3Args::default()
                    },
                    force_path_style: None,
                },
                actor: ProfileCreateActorArgs::default(),
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
            actor_id: None,
            subject_id: None,
            principal_scope: None,
            principals: None,
            default_namespace: None,
            writer_id: None,
        };

        let spec = update_spec(CreateProviderSpec::S3 {
            store: ProfileS3Args::default(),
            force_path_style: None,
        });
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
            actor_id: None,
            subject_id: None,
            principal_scope: None,
            principals: None,
            default_namespace: None,
            auth_token: None,
            ca_cert_path: None,
        };

        let spec = update_spec(CreateProviderSpec::Remote {
            server: ProfileRemoteArgs::default(),
            auth_token: Some("new-token".to_owned()),
            ca_cert_path: None,
        });
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
            actor_id: None,
            subject_id: None,
            principal_scope: None,
            principals: None,
            default_namespace: None,
            writer_id: None,
        };

        let spec = update_spec(CreateProviderSpec::Local(ProfileLocalArgs {
            root: Some("/tmp/moved".to_owned()),
            ..ProfileLocalArgs::default()
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
            actor_id: None,
            subject_id: None,
            principal_scope: None,
            principals: None,
            default_namespace: None,
            auth_token: None,
            ca_cert_path: None,
        };

        let spec = update_spec(CreateProviderSpec::Remote {
            server: ProfileRemoteArgs {
                server_url: Some("http://example.internal".to_owned()),
            },
            auth_token: Some("new-token".to_owned()),
            ca_cert_path: None,
        });
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
            actor_id: None,
            subject_id: None,
            principal_scope: None,
            principals: None,
            default_namespace: None,
            writer_id: None,
        };

        let incomplete = update_spec(CreateProviderSpec::S3 {
            store: ProfileS3Args {
                credential_source: Some("static".to_owned()),
                access_key_id: Some("access".to_owned()),
                ..ProfileS3Args::default()
            },
            force_path_style: None,
        });
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

        let complete = update_spec(CreateProviderSpec::S3 {
            store: ProfileS3Args {
                credential_source: Some("static".to_owned()),
                access_key_id: Some("access".to_owned()),
                secret_access_key: Some("secret".to_owned()),
                ..ProfileS3Args::default()
            },
            force_path_style: None,
        });
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

    #[test]
    fn a_profile_with_more_principals_than_a_request_accepts_is_refused() {
        let principals = (0..=loonfs_types::MAX_SUBJECT_PRINCIPALS)
            .map(|index| format!("prn_{index}"))
            .collect::<Vec<_>>()
            .join(",");
        let error = build_profile_from_create_spec(
            "default",
            CreateProfileSpec {
                provider: CreateProviderSpec::Local(ProfileLocalArgs {
                    root: Some("/tmp/store".to_owned()),
                    key_prefix: None,
                }),
                actor: ProfileCreateActorArgs {
                    principal_scope: Some("org".to_owned()),
                    principals: Some(principals),
                    ..ProfileCreateActorArgs::default()
                },
            },
            non_interactive_runtime(),
        )
        .expect_err("a profile no request could use is refused");
        assert_eq!(error.code, "invalid_request");
        assert_eq!(error.param.as_deref(), Some("--principals"));
    }

    fn update_spec(provider: CreateProviderSpec) -> ProfileUpdateSpec {
        ProfileUpdateSpec {
            name: "default".to_owned(),
            provider,
            actor: ProfileUpdateActorArgs::default(),
        }
    }

    fn non_interactive_runtime() -> RuntimeBehavior {
        RuntimeBehavior {
            output: crate::render::OutputFormat::Human,
            interactive: false,
            progress: crate::progress::ProgressMode::Off,
        }
    }
}
