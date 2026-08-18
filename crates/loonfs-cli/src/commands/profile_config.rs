//! Builds and updates profile configurations from provider flags, with a
//! table-driven check that each flag applies to the chosen store kind.

use crate::args::{ActorKindArg, InitArgs, ProfileCreateArgs, ProfileUpdateArgs, RuntimeBehavior};
use crate::config::{ProfileActorConfig, ProfileConfig, StoreConfig};
use crate::error::CliError;
use crate::prompt;
use loonfs_api::{ActorId, ActorKind, SecretString};
use loonfs_objectstore::{
    AwsS3Credentials, AzureAbsCredentials, CloudflareR2Credentials, ConfiguredObjectStoreKind,
    GcpGcsCredentials,
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

// --- provider flag matrix ---

/// A target a provider flag can apply to: one of the embedded store kinds or
/// a remote profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlagTarget {
    LocalFs,
    AwsS3,
    CloudflareR2,
    GcpGcs,
    AzureAbs,
    Remote,
}

impl From<ConfiguredObjectStoreKind> for FlagTarget {
    fn from(kind: ConfiguredObjectStoreKind) -> Self {
        match kind {
            ConfiguredObjectStoreKind::LocalFs => Self::LocalFs,
            ConfiguredObjectStoreKind::AwsS3 => Self::AwsS3,
            ConfiguredObjectStoreKind::CloudflareR2 => Self::CloudflareR2,
            ConfiguredObjectStoreKind::GcpGcs => Self::GcpGcs,
            ConfiguredObjectStoreKind::AzureAbs => Self::AzureAbs,
        }
    }
}

/// Every embedded store kind; used to reject remote-only flags before the
/// store kind is known.
const EMBEDDED_TARGETS: &[FlagTarget] = &[
    FlagTarget::LocalFs,
    FlagTarget::AwsS3,
    FlagTarget::CloudflareR2,
    FlagTarget::GcpGcs,
    FlagTarget::AzureAbs,
];

/// One provider flag and the targets it applies to.
///
/// Adding a provider flag means adding one row here (plus its arg field and
/// the point where the value is consumed); both the create and update paths
/// walk this table to reject flags that do not apply.
struct ProviderFlag {
    /// The user-facing `--flag` name.
    flag: &'static str,
    /// Targets where the flag is accepted.
    allowed: &'static [FlagTarget],
    /// Whether the flag is set on the create/init spec.
    create_set: fn(&CreateProfileSpec) -> bool,
    /// Whether the flag is set on the update args; `None` when `profile
    /// update` has no such flag.
    update_set: Option<fn(&ProfileUpdateArgs) -> bool>,
}

use FlagTarget::{AwsS3, AzureAbs, CloudflareR2, GcpGcs, LocalFs, Remote};

const PROVIDER_FLAGS: &[ProviderFlag] = &[
    ProviderFlag {
        flag: "server-url",
        allowed: &[Remote],
        create_set: |spec| spec.server_url.is_some(),
        update_set: Some(|args| args.server_url.is_some()),
    },
    ProviderFlag {
        flag: "auth-token",
        allowed: &[Remote],
        create_set: |spec| spec.auth_token.is_some(),
        update_set: Some(|args| args.auth_token.is_some()),
    },
    ProviderFlag {
        flag: "ca-cert-path",
        allowed: &[Remote],
        create_set: |spec| spec.ca_cert_path.is_some(),
        update_set: Some(|args| args.ca_cert_path.is_some()),
    },
    ProviderFlag {
        flag: "store-kind",
        allowed: EMBEDDED_TARGETS,
        create_set: |spec| spec.store_kind.is_some(),
        update_set: None,
    },
    ProviderFlag {
        flag: "root",
        allowed: &[LocalFs],
        create_set: |spec| spec.root.is_some(),
        update_set: Some(|args| args.root.is_some()),
    },
    ProviderFlag {
        flag: "key-prefix",
        allowed: EMBEDDED_TARGETS,
        create_set: |spec| spec.key_prefix.is_some(),
        update_set: Some(|args| args.key_prefix.is_some()),
    },
    ProviderFlag {
        flag: "bucket",
        allowed: &[AwsS3, CloudflareR2, GcpGcs],
        create_set: |spec| spec.bucket.is_some(),
        update_set: Some(|args| args.bucket.is_some()),
    },
    ProviderFlag {
        flag: "region",
        allowed: &[AwsS3],
        create_set: |spec| spec.region.is_some(),
        update_set: Some(|args| args.region.is_some()),
    },
    ProviderFlag {
        flag: "credential-source",
        allowed: &[AwsS3, CloudflareR2],
        create_set: |spec| spec.credential_source.is_some(),
        update_set: Some(|args| args.credential_source.is_some()),
    },
    ProviderFlag {
        flag: "access-key-id",
        allowed: &[AwsS3, CloudflareR2],
        create_set: |spec| spec.access_key_id.is_some(),
        update_set: Some(|args| args.access_key_id.is_some()),
    },
    ProviderFlag {
        flag: "secret-access-key",
        allowed: &[AwsS3, CloudflareR2],
        create_set: |spec| spec.secret_access_key.is_some(),
        update_set: Some(|args| args.secret_access_key.is_some()),
    },
    ProviderFlag {
        flag: "endpoint-url",
        allowed: &[AwsS3, CloudflareR2, AzureAbs],
        create_set: |spec| spec.endpoint_url.is_some(),
        update_set: Some(|args| args.endpoint_url.is_some()),
    },
    ProviderFlag {
        flag: "session-token",
        allowed: &[AwsS3],
        create_set: |spec| spec.session_token.is_some(),
        update_set: Some(|args| args.session_token.is_some()),
    },
    ProviderFlag {
        flag: "force-path-style",
        allowed: &[AwsS3],
        create_set: |spec| spec.force_path_style,
        update_set: None,
    },
    ProviderFlag {
        flag: "account-id",
        allowed: &[CloudflareR2],
        create_set: |spec| spec.account_id.is_some(),
        update_set: Some(|args| args.account_id.is_some()),
    },
    ProviderFlag {
        flag: "account-name",
        allowed: &[AzureAbs],
        create_set: |spec| spec.account_name.is_some(),
        update_set: Some(|args| args.account_name.is_some()),
    },
    ProviderFlag {
        flag: "container-name",
        allowed: &[AzureAbs],
        create_set: |spec| spec.container_name.is_some(),
        update_set: Some(|args| args.container_name.is_some()),
    },
    ProviderFlag {
        flag: "access-key",
        allowed: &[AzureAbs],
        create_set: |spec| spec.access_key.is_some(),
        update_set: Some(|args| args.access_key.is_some()),
    },
    ProviderFlag {
        flag: "service-account-key-path",
        allowed: &[GcpGcs],
        create_set: |spec| spec.service_account_key_path.is_some(),
        update_set: Some(|args| args.service_account_key_path.is_some()),
    },
];

pub(super) fn has_update_flags(args: &ProfileUpdateArgs) -> bool {
    args.actor_kind.is_some()
        || args.actor_id.is_some()
        || PROVIDER_FLAGS
            .iter()
            .any(|row| row.update_set.is_some_and(|is_set| is_set(args)))
}

/// Rejects every set create flag whose allowed targets do not intersect
/// `targets`, reporting `profile_label` in the error.
fn reject_inapplicable_create_flags(
    spec: &CreateProfileSpec,
    targets: &[FlagTarget],
    profile_label: &str,
) -> Result<(), CliError> {
    for row in PROVIDER_FLAGS {
        if (row.create_set)(spec) && !row.allowed.iter().any(|target| targets.contains(target)) {
            return Err(inapplicable_flag(row.flag, profile_label));
        }
    }
    Ok(())
}

/// Rejects every set update flag whose allowed targets do not intersect
/// `targets`, reporting `profile_label` in the error.
fn reject_inapplicable_update_flags(
    args: &ProfileUpdateArgs,
    targets: &[FlagTarget],
    profile_label: &str,
) -> Result<(), CliError> {
    for row in PROVIDER_FLAGS {
        let Some(update_set) = row.update_set else {
            continue;
        };
        if update_set(args) && !row.allowed.iter().any(|target| targets.contains(target)) {
            return Err(inapplicable_flag(row.flag, profile_label));
        }
    }
    Ok(())
}

fn inapplicable_flag(flag: &str, profile_label: &str) -> CliError {
    CliError::invalid_input(format!(
        "`--{flag}` does not apply to {profile_label} profiles"
    ))
    .with_param(format!("--{flag}"))
}

// --- create/update helpers ---

#[derive(Debug, Clone)]
pub(super) struct CreateProfileSpec {
    mode: Option<String>,
    store_kind: Option<String>,
    root: Option<String>,
    key_prefix: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    credential_source: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    endpoint_url: Option<String>,
    session_token: Option<String>,
    force_path_style: bool,
    account_id: Option<String>,
    account_name: Option<String>,
    container_name: Option<String>,
    access_key: Option<String>,
    service_account_key_path: Option<String>,
    server_url: Option<String>,
    auth_token: Option<String>,
    ca_cert_path: Option<String>,
    actor_kind: Option<ActorKindArg>,
    actor_id: Option<String>,
}

pub(super) fn create_profile_spec_from_init(args: InitArgs) -> CreateProfileSpec {
    CreateProfileSpec {
        mode: args.mode,
        store_kind: args.store_kind,
        root: args.root,
        key_prefix: args.key_prefix,
        bucket: args.bucket,
        region: args.region,
        credential_source: args.credential_source,
        access_key_id: args.access_key_id,
        secret_access_key: args.secret_access_key,
        endpoint_url: args.endpoint_url,
        session_token: args.session_token,
        force_path_style: args.force_path_style,
        account_id: args.account_id,
        account_name: args.account_name,
        container_name: args.container_name,
        access_key: args.access_key,
        service_account_key_path: args.service_account_key_path,
        server_url: args.server_url,
        auth_token: args.auth_token,
        ca_cert_path: args.ca_cert_path,
        actor_kind: args.actor_kind,
        actor_id: args.actor_id,
    }
}

pub(super) fn create_profile_spec_from_create(args: ProfileCreateArgs) -> CreateProfileSpec {
    CreateProfileSpec {
        mode: args.mode,
        store_kind: args.store_kind,
        root: args.root,
        key_prefix: args.key_prefix,
        bucket: args.bucket,
        region: args.region,
        credential_source: args.credential_source,
        access_key_id: args.access_key_id,
        secret_access_key: args.secret_access_key,
        endpoint_url: args.endpoint_url,
        session_token: args.session_token,
        force_path_style: args.force_path_style,
        account_id: args.account_id,
        account_name: args.account_name,
        container_name: args.container_name,
        access_key: args.access_key,
        service_account_key_path: args.service_account_key_path,
        server_url: args.server_url,
        auth_token: args.auth_token,
        ca_cert_path: args.ca_cert_path,
        actor_kind: args.actor_kind,
        actor_id: args.actor_id,
    }
}

pub(super) fn build_profile_from_create_spec(
    spec: CreateProfileSpec,
    runtime: RuntimeBehavior,
) -> Result<ProfileConfig, CliError> {
    enum ProfileMode {
        Embedded,
        Remote,
    }

    let mode = match spec.mode.as_deref() {
        Some("embedded") => ProfileMode::Embedded,
        Some("remote") => ProfileMode::Remote,
        Some(other) => {
            return Err(CliError::invalid_input(format!(
                "unknown mode: `{other}` (expected embedded or remote)"
            ))
            .with_param("--mode"))
        }
        None if runtime.interactive => {
            match prompt::prompt_choice("mode", &["embedded", "remote"])?.as_str() {
                "embedded" => ProfileMode::Embedded,
                // `prompt_choice` returns one of the two supplied values.
                _ => ProfileMode::Remote,
            }
        }
        None => {
            return Err(CliError::non_interactive_field_required("mode"));
        }
    };

    match mode {
        ProfileMode::Embedded => build_embedded_profile(spec, runtime),
        ProfileMode::Remote => build_remote_profile(spec, runtime),
    }
}

fn build_embedded_profile(
    spec: CreateProfileSpec,
    runtime: RuntimeBehavior,
) -> Result<ProfileConfig, CliError> {
    reject_inapplicable_create_flags(&spec, EMBEDDED_TARGETS, "embedded")?;

    let store_kind = match spec.store_kind.as_deref() {
        Some("local-fs") => ConfiguredObjectStoreKind::LocalFs,
        Some("aws-s3") => ConfiguredObjectStoreKind::AwsS3,
        Some("cloudflare-r2") => ConfiguredObjectStoreKind::CloudflareR2,
        Some("gcp-gcs") => ConfiguredObjectStoreKind::GcpGcs,
        Some("azure-abs") => ConfiguredObjectStoreKind::AzureAbs,
        Some(other) => {
            return Err(CliError::invalid_input(format!(
            "unknown store kind: `{other}` (expected local-fs, aws-s3, cloudflare-r2, gcp-gcs, or azure-abs)"
        ))
            .with_param("--store-kind"))
        }
        None if runtime.interactive => {
            return prompt::prompt_choice(
                "store kind",
                &["aws-s3", "cloudflare-r2", "gcp-gcs", "azure-abs", "local-fs"],
            )
            .and_then(|choice| {
                build_embedded_profile(
                    CreateProfileSpec {
                        store_kind: Some(choice),
                        ..spec
                    },
                    runtime,
                )
            });
        }
        None => return Err(CliError::non_interactive_field_required("store-kind")),
    };

    reject_inapplicable_create_flags(&spec, &[store_kind.into()], store_kind.as_str())?;

    let aws_credentials = (store_kind == ConfiguredObjectStoreKind::AwsS3)
        .then(|| build_aws_credentials(&spec, runtime))
        .transpose()?;
    let r2_credentials = (store_kind == ConfiguredObjectStoreKind::CloudflareR2)
        .then(|| build_r2_credentials(&spec, runtime))
        .transpose()?;

    let store = match store_kind {
        ConfiguredObjectStoreKind::LocalFs => StoreConfig::LocalFs {
            root: require_or_prompt(spec.root.as_ref(), "root", runtime)?,
            key_prefix: spec.key_prefix,
        },
        ConfiguredObjectStoreKind::AwsS3 => StoreConfig::AwsS3 {
            bucket: require_or_prompt(spec.bucket.as_ref(), "bucket", runtime)?,
            region: require_or_prompt_region(spec.region.as_ref(), runtime)?,
            endpoint_url: spec.endpoint_url,
            credentials: aws_credentials.expect("aws credentials were built for aws-s3"),
            key_prefix: spec.key_prefix,
            force_path_style: spec.force_path_style,
        },
        ConfiguredObjectStoreKind::CloudflareR2 => StoreConfig::CloudflareR2 {
            bucket: require_or_prompt(spec.bucket.as_ref(), "bucket", runtime)?,
            account_id: require_or_prompt(spec.account_id.as_ref(), "account-id", runtime)?,
            endpoint_url: require_or_prompt(spec.endpoint_url.as_ref(), "endpoint-url", runtime)?,
            credentials: r2_credentials.expect("r2 credentials were built for cloudflare-r2"),
            key_prefix: spec.key_prefix,
        },
        ConfiguredObjectStoreKind::GcpGcs => StoreConfig::GcpGcs {
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
        ConfiguredObjectStoreKind::AzureAbs => StoreConfig::AzureAbs {
            account_name: require_or_prompt(spec.account_name.as_ref(), "account-name", runtime)?,
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
    };

    let actor = profile_actor_config(spec.actor_kind, spec.actor_id.as_deref())?;
    Ok(ProfileConfig::Embedded {
        store,
        actor,
        default_namespace: None,
        writer_id: None,
    })
}

fn build_remote_profile(
    spec: CreateProfileSpec,
    runtime: RuntimeBehavior,
) -> Result<ProfileConfig, CliError> {
    reject_inapplicable_create_flags(&spec, &[FlagTarget::Remote], "remote")?;

    let actor = profile_actor_config(spec.actor_kind, spec.actor_id.as_deref())?;
    let auth_token = match spec.auth_token {
        Some(token) => blank_to_none(Some(token)),
        None if runtime.interactive => prompt::prompt_secret_optional("auth token", None)?,
        None => None,
    };
    Ok(ProfileConfig::Remote {
        server_url: require_or_prompt(spec.server_url.as_ref(), "server-url", runtime)?,
        actor,
        default_namespace: None,
        auth_token: auth_token.map(SecretString::from),
        ca_cert_path: blank_to_none(spec.ca_cert_path),
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
        Some("ambient") if has_static_flags => Err(CliError::invalid_input(
            "`--credential-source ambient` cannot be combined with static credential flags",
        )
        .with_param("--credential-source")),
        Some("ambient") => Ok(CredentialSource::Ambient),
        Some("static") => Ok(CredentialSource::Static),
        Some(other) => Err(CliError::invalid_input(format!(
            "unknown credential source: `{other}` (expected ambient or static)"
        ))
        .with_param("--credential-source")),
        None if has_static_flags => Ok(CredentialSource::Static),
        None => Ok(CredentialSource::Ambient),
    }
}

fn build_aws_credentials(
    spec: &CreateProfileSpec,
    runtime: RuntimeBehavior,
) -> Result<AwsS3Credentials, CliError> {
    let has_static_flags = spec.access_key_id.is_some()
        || spec.secret_access_key.is_some()
        || spec.session_token.is_some();
    match selected_credential_source(spec.credential_source.as_deref(), has_static_flags)? {
        CredentialSource::Ambient => Ok(AwsS3Credentials::Ambient {}),
        CredentialSource::Static => Ok(AwsS3Credentials::Static {
            access_key_id: require_or_prompt_secret(
                spec.access_key_id.as_ref(),
                "access-key-id",
                runtime,
            )?,
            secret_access_key: require_or_prompt_secret(
                spec.secret_access_key.as_ref(),
                "secret-access-key",
                runtime,
            )?,
            session_token: blank_to_none(spec.session_token.clone()).map(SecretString::from),
        }),
    }
}

fn build_r2_credentials(
    spec: &CreateProfileSpec,
    runtime: RuntimeBehavior,
) -> Result<CloudflareR2Credentials, CliError> {
    let has_static_flags = spec.access_key_id.is_some() || spec.secret_access_key.is_some();
    match selected_credential_source(spec.credential_source.as_deref(), has_static_flags)? {
        CredentialSource::Ambient => Ok(CloudflareR2Credentials::Ambient {}),
        CredentialSource::Static => Ok(CloudflareR2Credentials::Static {
            access_key_id: require_or_prompt_secret(
                spec.access_key_id.as_ref(),
                "access-key-id",
                runtime,
            )?,
            secret_access_key: require_or_prompt_secret(
                spec.secret_access_key.as_ref(),
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
                CliError::invalid_input(format!("invalid --actor-id: {error}"))
                    .with_param("--actor-id")
            })?),
        }),
        (None, Some(_)) => {
            Err(CliError::invalid_input("--actor-id requires --actor-kind")
                .with_param("--actor-id"))
        }
        (Some(_), None) => {
            Err(CliError::invalid_input("--actor-kind requires --actor-id")
                .with_param("--actor-kind"))
        }
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

pub(super) fn apply_update_flags(
    existing: ProfileConfig,
    args: &ProfileUpdateArgs,
) -> Result<ProfileConfig, CliError> {
    match &existing {
        ProfileConfig::Embedded { store, .. } => {
            reject_inapplicable_update_flags(args, EMBEDDED_TARGETS, "embedded")?;
            let store_kind = store.kind();
            reject_inapplicable_update_flags(args, &[store_kind.into()], store_kind.as_str())?;
        }
        ProfileConfig::Remote { .. } => {
            reject_inapplicable_update_flags(args, &[FlagTarget::Remote], "remote")?;
        }
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
        } => Ok(ProfileConfig::Remote {
            server_url: args.server_url.clone().unwrap_or(server_url),
            actor: updated_actor(actor, args)?,
            default_namespace,
            auth_token: match args.auth_token.clone() {
                Some(token) => blank_to_none(Some(token)).map(SecretString::from),
                None => auth_token,
            },
            ca_cert_path: blank_to_none(args.ca_cert_path.clone()).or(ca_cert_path),
        }),
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
            CliError::invalid_input(format!(
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

    use super::{apply_update_flags, build_profile_from_create_spec, CreateProfileSpec};
    use crate::args::{ProfileUpdateArgs, RuntimeBehavior};
    use crate::config::{ProfileConfig, StoreConfig};
    use loonfs_objectstore::{AwsS3Credentials, AzureAbsCredentials};

    #[test]
    fn create_profile_supports_azure_abs() {
        let profile = build_profile_from_create_spec(
            CreateProfileSpec {
                mode: Some("embedded".to_owned()),
                store_kind: Some("azure-abs".to_owned()),
                account_name: Some("devstoreaccount1".to_owned()),
                container_name: Some("container".to_owned()),
                access_key: Some("account-key".to_owned()),
                endpoint_url: Some("https://devstoreaccount1.blob.core.windows.net".to_owned()),
                key_prefix: Some("tenant-a".to_owned()),
                ..empty_spec()
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
    fn create_rejects_flags_outside_their_provider() {
        let remote_with_bucket = build_profile_from_create_spec(
            CreateProfileSpec {
                mode: Some("remote".to_owned()),
                server_url: Some("http://127.0.0.1:9400".to_owned()),
                bucket: Some("bucket".to_owned()),
                ..empty_spec()
            },
            non_interactive_runtime(),
        )
        .expect_err("bucket must not apply to remote");
        assert_eq!(
            remote_with_bucket.message,
            "`--bucket` does not apply to remote profiles"
        );

        let embedded_with_server_url = build_profile_from_create_spec(
            CreateProfileSpec {
                mode: Some("embedded".to_owned()),
                store_kind: Some("local-fs".to_owned()),
                root: Some("/tmp/store".to_owned()),
                server_url: Some("http://127.0.0.1:9400".to_owned()),
                ..empty_spec()
            },
            non_interactive_runtime(),
        )
        .expect_err("server-url must not apply to embedded");
        assert_eq!(
            embedded_with_server_url.message,
            "`--server-url` does not apply to embedded profiles"
        );

        let local_fs_with_bucket = build_profile_from_create_spec(
            CreateProfileSpec {
                mode: Some("embedded".to_owned()),
                store_kind: Some("local-fs".to_owned()),
                root: Some("/tmp/store".to_owned()),
                bucket: Some("bucket".to_owned()),
                ..empty_spec()
            },
            non_interactive_runtime(),
        )
        .expect_err("bucket must not apply to local-fs");
        assert_eq!(
            local_fs_with_bucket.message,
            "`--bucket` does not apply to local-fs profiles"
        );
    }

    #[test]
    fn credential_source_is_rejected_outside_its_provider() {
        let gcs = build_profile_from_create_spec(
            CreateProfileSpec {
                mode: Some("embedded".to_owned()),
                store_kind: Some("gcp-gcs".to_owned()),
                bucket: Some("bucket".to_owned()),
                service_account_key_path: Some("/tmp/service-account.json".to_owned()),
                credential_source: Some("ambient".to_owned()),
                ..empty_spec()
            },
            non_interactive_runtime(),
        )
        .expect_err("credential source must not apply to gcs");
        assert_eq!(
            gcs.message,
            "`--credential-source` does not apply to gcp-gcs profiles"
        );
    }

    #[test]
    fn no_static_flags_select_ambient_without_storing_secrets() {
        let s3 = build_profile_from_create_spec(
            CreateProfileSpec {
                mode: Some("embedded".to_owned()),
                store_kind: Some("aws-s3".to_owned()),
                bucket: Some("bucket".to_owned()),
                region: Some("us-east-1".to_owned()),
                ..empty_spec()
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
    fn remote_creation_never_captures_an_environment_token() {
        let remote = build_profile_from_create_spec(
            CreateProfileSpec {
                mode: Some("remote".to_owned()),
                server_url: Some("http://127.0.0.1:9400".to_owned()),
                ..empty_spec()
            },
            non_interactive_runtime(),
        )
        .expect("build remote profile");
        match remote {
            ProfileConfig::Remote { auth_token, .. } => assert!(auth_token.is_none()),
            other => panic!("expected a remote profile, got {other:?}"),
        }
    }

    #[test]
    fn static_flags_imply_static_and_require_the_complete_set() {
        let incomplete = build_profile_from_create_spec(
            CreateProfileSpec {
                mode: Some("embedded".to_owned()),
                store_kind: Some("aws-s3".to_owned()),
                bucket: Some("bucket".to_owned()),
                region: Some("us-east-1".to_owned()),
                access_key_id: Some("access".to_owned()),
                ..empty_spec()
            },
            non_interactive_runtime(),
        )
        .expect_err("a partial static set must fail");
        assert!(incomplete.message.contains("secret-access-key"));

        let complete = build_profile_from_create_spec(
            CreateProfileSpec {
                mode: Some("embedded".to_owned()),
                store_kind: Some("aws-s3".to_owned()),
                bucket: Some("bucket".to_owned()),
                region: Some("us-east-1".to_owned()),
                access_key_id: Some("access".to_owned()),
                secret_access_key: Some("secret".to_owned()),
                session_token: Some("session".to_owned()),
                ..empty_spec()
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
            local_fs,
            &ProfileUpdateArgs {
                auth_token: Some("token".to_owned()),
                ..empty_update_args()
            },
        )
        .expect_err("auth-token must not apply to embedded");
        assert_eq!(
            error.message,
            "`--auth-token` does not apply to embedded profiles"
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

    fn empty_spec() -> CreateProfileSpec {
        CreateProfileSpec {
            mode: None,
            store_kind: None,
            root: None,
            key_prefix: None,
            bucket: None,
            region: None,
            credential_source: None,
            access_key_id: None,
            secret_access_key: None,
            endpoint_url: None,
            session_token: None,
            force_path_style: false,
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
