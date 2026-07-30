//! Builds and updates profile configurations from provider flags, with a
//! table-driven check that each flag applies to the chosen store kind.

use crate::args::{InitArgs, ProfileCreateArgs, ProfileUpdateArgs, RuntimeBehavior};
use crate::config::{ProfileConfig, StoreConfig};
use crate::error::CliError;
use crate::prompt;
use loonfs_objectstore::{ConfiguredObjectStoreKind, SecretString};

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
    PROVIDER_FLAGS
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
}

pub(super) fn create_profile_spec_from_init(args: InitArgs) -> CreateProfileSpec {
    CreateProfileSpec {
        mode: args.mode,
        store_kind: args.store_kind,
        root: args.root,
        key_prefix: args.key_prefix,
        bucket: args.bucket,
        region: args.region,
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
    }
}

pub(super) fn build_profile_from_create_spec(
    spec: CreateProfileSpec,
    runtime: RuntimeBehavior,
) -> Result<ProfileConfig, CliError> {
    let mode = match spec.mode.as_deref() {
        Some("embedded") => "embedded".to_owned(),
        Some("remote") => "remote".to_owned(),
        Some(other) => {
            return Err(CliError::invalid_input(format!(
                "unknown mode: `{other}` (expected embedded or remote)"
            )))
        }
        None if runtime.interactive => prompt::prompt_choice("mode", &["embedded", "remote"])?,
        None => {
            return Err(CliError::non_interactive_field_required("mode"));
        }
    };

    match mode.as_str() {
        "embedded" => build_embedded_profile(spec, runtime),
        "remote" => build_remote_profile(spec, runtime),
        _ => unreachable!("mode is validated to `embedded` or `remote` above"),
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
        )))
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

    let store = match store_kind {
        ConfiguredObjectStoreKind::LocalFs => StoreConfig::LocalFs {
            root: require_or_prompt(spec.root.as_ref(), "root", runtime)?,
            key_prefix: spec.key_prefix,
        },
        ConfiguredObjectStoreKind::AwsS3 => StoreConfig::AwsS3 {
            bucket: require_or_prompt(spec.bucket.as_ref(), "bucket", runtime)?,
            region: require_or_prompt_region(spec.region.as_ref(), runtime)?,
            endpoint_url: spec.endpoint_url,
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
            session_token: spec.session_token.map(SecretString::from),
            key_prefix: spec.key_prefix,
            force_path_style: spec.force_path_style,
        },
        ConfiguredObjectStoreKind::CloudflareR2 => StoreConfig::CloudflareR2 {
            bucket: require_or_prompt(spec.bucket.as_ref(), "bucket", runtime)?,
            account_id: require_or_prompt(spec.account_id.as_ref(), "account-id", runtime)?,
            endpoint_url: require_or_prompt(spec.endpoint_url.as_ref(), "endpoint-url", runtime)?,
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
            key_prefix: spec.key_prefix,
        },
        ConfiguredObjectStoreKind::GcpGcs => StoreConfig::GcpGcs {
            bucket: require_or_prompt(spec.bucket.as_ref(), "bucket", runtime)?,
            service_account_key_path: require_or_prompt(
                spec.service_account_key_path.as_ref(),
                "service-account-key-path",
                runtime,
            )?,
            key_prefix: spec.key_prefix,
        },
        ConfiguredObjectStoreKind::AzureAbs => StoreConfig::AzureAbs {
            account_name: require_or_prompt(spec.account_name.as_ref(), "account-name", runtime)?,
            container_name: require_or_prompt(
                spec.container_name.as_ref(),
                "container-name",
                runtime,
            )?,
            access_key: require_or_prompt_secret(spec.access_key.as_ref(), "access-key", runtime)?,
            endpoint_url: spec.endpoint_url,
            key_prefix: spec.key_prefix,
        },
    };

    Ok(ProfileConfig::Embedded {
        store,
        default_namespace: None,
        writer_id: None,
    })
}

fn build_remote_profile(
    spec: CreateProfileSpec,
    runtime: RuntimeBehavior,
) -> Result<ProfileConfig, CliError> {
    reject_inapplicable_create_flags(&spec, &[FlagTarget::Remote], "remote")?;

    Ok(ProfileConfig::Remote {
        server_url: require_or_prompt(spec.server_url.as_ref(), "server-url", runtime)?,
        default_namespace: None,
        auth_token: match spec.auth_token {
            Some(token) if token.trim().is_empty() => None,
            other => other.map(SecretString::from),
        },
    })
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
                    access_key_id,
                    secret_access_key,
                    session_token,
                    key_prefix,
                    force_path_style,
                } => StoreConfig::AwsS3 {
                    bucket: args.bucket.clone().unwrap_or(bucket),
                    region: args.region.clone().unwrap_or(region),
                    endpoint_url: args.endpoint_url.clone().or(endpoint_url),
                    access_key_id: args
                        .access_key_id
                        .clone()
                        .map(SecretString::from)
                        .unwrap_or(access_key_id),
                    secret_access_key: args
                        .secret_access_key
                        .clone()
                        .map(SecretString::from)
                        .unwrap_or(secret_access_key),
                    session_token: args
                        .session_token
                        .clone()
                        .map(SecretString::from)
                        .or(session_token),
                    key_prefix: args.key_prefix.clone().or(key_prefix),
                    force_path_style,
                },
                StoreConfig::CloudflareR2 {
                    bucket,
                    account_id,
                    endpoint_url,
                    access_key_id,
                    secret_access_key,
                    key_prefix,
                } => StoreConfig::CloudflareR2 {
                    bucket: args.bucket.clone().unwrap_or(bucket),
                    account_id: args.account_id.clone().unwrap_or(account_id),
                    endpoint_url: args.endpoint_url.clone().unwrap_or(endpoint_url),
                    access_key_id: args
                        .access_key_id
                        .clone()
                        .map(SecretString::from)
                        .unwrap_or(access_key_id),
                    secret_access_key: args
                        .secret_access_key
                        .clone()
                        .map(SecretString::from)
                        .unwrap_or(secret_access_key),
                    key_prefix: args.key_prefix.clone().or(key_prefix),
                },
                StoreConfig::GcpGcs {
                    bucket,
                    service_account_key_path,
                    key_prefix,
                } => StoreConfig::GcpGcs {
                    bucket: args.bucket.clone().unwrap_or(bucket),
                    service_account_key_path: args
                        .service_account_key_path
                        .clone()
                        .unwrap_or(service_account_key_path),
                    key_prefix: args.key_prefix.clone().or(key_prefix),
                },
                StoreConfig::AzureAbs {
                    account_name,
                    container_name,
                    access_key,
                    endpoint_url,
                    key_prefix,
                } => StoreConfig::AzureAbs {
                    account_name: args.account_name.clone().unwrap_or(account_name),
                    container_name: args.container_name.clone().unwrap_or(container_name),
                    access_key: args
                        .access_key
                        .clone()
                        .map(SecretString::from)
                        .unwrap_or(access_key),
                    endpoint_url: args.endpoint_url.clone().or(endpoint_url),
                    key_prefix: args.key_prefix.clone().or(key_prefix),
                },
            };
            Ok(ProfileConfig::Embedded {
                store,
                default_namespace,
                writer_id,
            })
        }
        ProfileConfig::Remote {
            server_url,
            default_namespace,
            auth_token,
        } => Ok(ProfileConfig::Remote {
            server_url: args.server_url.clone().unwrap_or(server_url),
            default_namespace,
            auth_token: args
                .auth_token
                .clone()
                .map(SecretString::from)
                .or(auth_token),
        }),
    }
}

pub(super) fn apply_update_interactive(existing: ProfileConfig) -> Result<ProfileConfig, CliError> {
    match existing {
        ProfileConfig::Embedded {
            store,
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
                    access_key_id,
                    secret_access_key,
                    session_token,
                    key_prefix,
                    force_path_style,
                } => StoreConfig::AwsS3 {
                    bucket: prompt::prompt_line_default("bucket", &bucket)?,
                    region: {
                        let default_idx =
                            AWS_REGIONS.iter().position(|r| *r == region).unwrap_or(0);
                        prompt::prompt_fuzzy_choice("region", AWS_REGIONS, default_idx)?
                    },
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
                    endpoint_url: prompt::prompt_optional("endpoint url", endpoint_url.as_deref())?,
                    session_token: prompt::prompt_secret_optional(
                        "session token",
                        session_token.as_ref().map(SecretString::expose),
                    )?
                    .map(SecretString::from),
                    key_prefix: prompt::prompt_optional("key prefix", key_prefix.as_deref())?,
                    force_path_style,
                },
                StoreConfig::CloudflareR2 {
                    bucket,
                    account_id,
                    endpoint_url,
                    access_key_id,
                    secret_access_key,
                    key_prefix,
                } => StoreConfig::CloudflareR2 {
                    bucket: prompt::prompt_line_default("bucket", &bucket)?,
                    account_id: prompt::prompt_line_default("account id", &account_id)?,
                    endpoint_url: prompt::prompt_line_default("endpoint url", &endpoint_url)?,
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
                    key_prefix: prompt::prompt_optional("key prefix", key_prefix.as_deref())?,
                },
                StoreConfig::GcpGcs {
                    bucket,
                    service_account_key_path,
                    key_prefix,
                } => StoreConfig::GcpGcs {
                    bucket: prompt::prompt_line_default("bucket", &bucket)?,
                    service_account_key_path: prompt::prompt_line_default(
                        "service account key path",
                        &service_account_key_path,
                    )?,
                    key_prefix: prompt::prompt_optional("key prefix", key_prefix.as_deref())?,
                },
                StoreConfig::AzureAbs {
                    account_name,
                    container_name,
                    access_key,
                    endpoint_url,
                    key_prefix,
                } => StoreConfig::AzureAbs {
                    account_name: prompt::prompt_line_default("account-name", &account_name)?,
                    container_name: prompt::prompt_line_default("container-name", &container_name)?,
                    access_key: prompt::prompt_secret_keep_current(
                        "access-key",
                        access_key.expose(),
                    )?
                    .into(),
                    endpoint_url: prompt::prompt_optional("endpoint url", endpoint_url.as_deref())?,
                    key_prefix: prompt::prompt_optional("key prefix", key_prefix.as_deref())?,
                },
            };
            Ok(ProfileConfig::Embedded {
                store,
                default_namespace,
                writer_id,
            })
        }
        ProfileConfig::Remote {
            server_url,
            default_namespace,
            auth_token,
        } => Ok(ProfileConfig::Remote {
            server_url: prompt::prompt_line_default("server-url", &server_url)?,
            default_namespace,
            auth_token: prompt::prompt_secret_optional(
                "auth token",
                auth_token.as_ref().map(SecretString::expose),
            )?
            .map(SecretString::from),
        }),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic)]
    // Profile tests use panic in unexpected match arms for precise diagnostics.

    use super::{apply_update_flags, build_profile_from_create_spec, CreateProfileSpec};
    use crate::args::{ProfileUpdateArgs, RuntimeBehavior};
    use crate::config::{ProfileConfig, StoreConfig};

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
                    access_key,
                    endpoint_url,
                    key_prefix,
                },
            ..
        } = profile
        {
            assert_eq!(account_name, "devstoreaccount1");
            assert_eq!(container_name, "container");
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
    fn update_rejects_flags_outside_their_provider() {
        let local_fs = ProfileConfig::Embedded {
            store: StoreConfig::LocalFs {
                root: "/tmp/store".to_owned(),
                key_prefix: None,
            },
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
            default_namespace: None,
            auth_token: None,
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

    fn empty_spec() -> CreateProfileSpec {
        CreateProfileSpec {
            mode: None,
            store_kind: None,
            root: None,
            key_prefix: None,
            bucket: None,
            region: None,
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
        }
    }

    fn empty_update_args() -> ProfileUpdateArgs {
        ProfileUpdateArgs {
            name: "profile".to_owned(),
            root: None,
            key_prefix: None,
            bucket: None,
            region: None,
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
        }
    }

    fn non_interactive_runtime() -> RuntimeBehavior {
        RuntimeBehavior {
            json: false,
            no_input: true,
            interactive: false,
        }
    }
}
