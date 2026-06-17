use crate::args::{InitArgs, ProfileCreateArgs, ProfileUpdateArgs, RuntimeBehavior};
use crate::config::{ProfileConfig, StoreConfig};
use crate::error::CliError;
use crate::prompt;

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
            return Err(CliError::non_interactive_input_required("mode"));
        }
    };

    match mode.as_str() {
        "embedded" => build_embedded_profile(spec, runtime),
        "remote" => build_remote_profile(spec, runtime),
        _ => unreachable!(),
    }
}

fn build_embedded_profile(
    spec: CreateProfileSpec,
    runtime: RuntimeBehavior,
) -> Result<ProfileConfig, CliError> {
    reject_create_flag("server-url", spec.server_url.is_some(), "embedded")?;
    reject_create_flag("auth-token", spec.auth_token.is_some(), "embedded")?;

    let store_kind = match spec.store_kind.as_deref() {
        Some("local-fs") => "local-fs",
        Some("aws-s3") => "aws-s3",
        Some("cloudflare-r2") => "cloudflare-r2",
        Some("gcp-gcs") => "gcp-gcs",
        Some(other) => {
            return Err(CliError::invalid_input(format!(
            "unknown store kind: `{other}` (expected local-fs, aws-s3, cloudflare-r2, or gcp-gcs)"
        )))
        }
        None if runtime.interactive => {
            return prompt::prompt_choice(
                "store kind",
                &["aws-s3", "cloudflare-r2", "gcp-gcs", "local-fs"],
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
        None => return Err(CliError::non_interactive_input_required("store-kind")),
    };

    let store = match store_kind {
        "local-fs" => {
            reject_create_flag("bucket", spec.bucket.is_some(), "local-fs")?;
            reject_create_flag("region", spec.region.is_some(), "local-fs")?;
            reject_create_flag("access-key-id", spec.access_key_id.is_some(), "local-fs")?;
            reject_create_flag(
                "secret-access-key",
                spec.secret_access_key.is_some(),
                "local-fs",
            )?;
            reject_create_flag("endpoint-url", spec.endpoint_url.is_some(), "local-fs")?;
            reject_create_flag("session-token", spec.session_token.is_some(), "local-fs")?;
            reject_create_flag("account-id", spec.account_id.is_some(), "local-fs")?;
            reject_create_flag(
                "service-account-key-path",
                spec.service_account_key_path.is_some(),
                "local-fs",
            )?;
            reject_create_flag("force-path-style", spec.force_path_style, "local-fs")?;
            StoreConfig::LocalFs {
                root: require_or_prompt(spec.root.as_ref(), "root", runtime)?,
                key_prefix: spec.key_prefix,
            }
        }
        "aws-s3" => {
            reject_create_flag("root", spec.root.is_some(), "aws-s3")?;
            reject_create_flag("account-id", spec.account_id.is_some(), "aws-s3")?;
            reject_create_flag(
                "service-account-key-path",
                spec.service_account_key_path.is_some(),
                "aws-s3",
            )?;
            StoreConfig::AwsS3 {
                bucket: require_or_prompt(spec.bucket.as_ref(), "bucket name", runtime)?,
                region: require_or_prompt_region(spec.region.as_ref(), runtime)?,
                endpoint_url: spec.endpoint_url,
                access_key_id: require_or_prompt(
                    spec.access_key_id.as_ref(),
                    "access-key-id",
                    runtime,
                )?,
                secret_access_key: require_or_prompt(
                    spec.secret_access_key.as_ref(),
                    "secret-access-key",
                    runtime,
                )?,
                session_token: spec.session_token,
                key_prefix: spec.key_prefix,
                force_path_style: if spec.force_path_style {
                    Some(true)
                } else {
                    None
                },
            }
        }
        "cloudflare-r2" => {
            reject_create_flag("root", spec.root.is_some(), "cloudflare-r2")?;
            reject_create_flag("region", spec.region.is_some(), "cloudflare-r2")?;
            reject_create_flag(
                "session-token",
                spec.session_token.is_some(),
                "cloudflare-r2",
            )?;
            reject_create_flag("force-path-style", spec.force_path_style, "cloudflare-r2")?;
            reject_create_flag(
                "service-account-key-path",
                spec.service_account_key_path.is_some(),
                "cloudflare-r2",
            )?;
            StoreConfig::CloudflareR2 {
                bucket: require_or_prompt(spec.bucket.as_ref(), "bucket name", runtime)?,
                account_id: require_or_prompt(spec.account_id.as_ref(), "account-id", runtime)?,
                endpoint_url: require_or_prompt(
                    spec.endpoint_url.as_ref(),
                    "endpoint-url",
                    runtime,
                )?,
                access_key_id: require_or_prompt(
                    spec.access_key_id.as_ref(),
                    "access-key-id",
                    runtime,
                )?,
                secret_access_key: require_or_prompt(
                    spec.secret_access_key.as_ref(),
                    "secret-access-key",
                    runtime,
                )?,
                key_prefix: spec.key_prefix,
            }
        }
        "gcp-gcs" => {
            reject_create_flag("root", spec.root.is_some(), "gcp-gcs")?;
            reject_create_flag("region", spec.region.is_some(), "gcp-gcs")?;
            reject_create_flag("access-key-id", spec.access_key_id.is_some(), "gcp-gcs")?;
            reject_create_flag(
                "secret-access-key",
                spec.secret_access_key.is_some(),
                "gcp-gcs",
            )?;
            reject_create_flag("endpoint-url", spec.endpoint_url.is_some(), "gcp-gcs")?;
            reject_create_flag("session-token", spec.session_token.is_some(), "gcp-gcs")?;
            reject_create_flag("account-id", spec.account_id.is_some(), "gcp-gcs")?;
            reject_create_flag("force-path-style", spec.force_path_style, "gcp-gcs")?;
            StoreConfig::GcpGcs {
                bucket: require_or_prompt(spec.bucket.as_ref(), "bucket name", runtime)?,
                service_account_key_path: require_or_prompt(
                    spec.service_account_key_path.as_ref(),
                    "service-account-key-path",
                    runtime,
                )?,
                key_prefix: spec.key_prefix,
            }
        }
        _ => unreachable!(),
    };

    Ok(ProfileConfig::Embedded {
        store,
        default_namespace: None,
        writer_id: None,
        writer_version: None,
        lease_duration_ms: None,
    })
}

fn build_remote_profile(
    spec: CreateProfileSpec,
    runtime: RuntimeBehavior,
) -> Result<ProfileConfig, CliError> {
    reject_create_flag("store-kind", spec.store_kind.is_some(), "remote")?;
    reject_create_flag("root", spec.root.is_some(), "remote")?;
    reject_create_flag("key-prefix", spec.key_prefix.is_some(), "remote")?;
    reject_create_flag("bucket", spec.bucket.is_some(), "remote")?;
    reject_create_flag("region", spec.region.is_some(), "remote")?;
    reject_create_flag("access-key-id", spec.access_key_id.is_some(), "remote")?;
    reject_create_flag(
        "secret-access-key",
        spec.secret_access_key.is_some(),
        "remote",
    )?;
    reject_create_flag("endpoint-url", spec.endpoint_url.is_some(), "remote")?;
    reject_create_flag("session-token", spec.session_token.is_some(), "remote")?;
    reject_create_flag("force-path-style", spec.force_path_style, "remote")?;
    reject_create_flag("account-id", spec.account_id.is_some(), "remote")?;
    reject_create_flag(
        "service-account-key-path",
        spec.service_account_key_path.is_some(),
        "remote",
    )?;

    Ok(ProfileConfig::Remote {
        server_url: require_or_prompt(spec.server_url.as_ref(), "server url", runtime)?,
        default_namespace: None,
        auth_token: match spec.auth_token {
            Some(token) if token.trim().is_empty() => None,
            other => other,
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
        _ => Err(CliError::non_interactive_input_required(field)),
    }
}

fn require_or_prompt_region(
    value: Option<&String>,
    runtime: RuntimeBehavior,
) -> Result<String, CliError> {
    match value {
        Some(v) if !v.trim().is_empty() => Ok(v.clone()),
        _ if runtime.interactive => prompt::prompt_fuzzy_choice("region", AWS_REGIONS, 0),
        _ => Err(CliError::non_interactive_input_required("region")),
    }
}

fn reject_create_flag(flag: &str, present: bool, profile_kind: &str) -> Result<(), CliError> {
    if present {
        return Err(CliError::invalid_input(format!(
            "`--{flag}` does not apply to {profile_kind} profiles"
        )));
    }
    Ok(())
}

pub(super) fn apply_update_flags(
    existing: ProfileConfig,
    args: &ProfileUpdateArgs,
) -> Result<ProfileConfig, CliError> {
    match &existing {
        ProfileConfig::Embedded { store, .. } => {
            reject_flag("server-url", &args.server_url, "embedded")?;
            reject_flag("auth-token", &args.auth_token, "embedded")?;
            match store {
                StoreConfig::LocalFs { .. } => {
                    reject_flag("bucket", &args.bucket, "local-fs")?;
                    reject_flag("region", &args.region, "local-fs")?;
                    reject_flag("access-key-id", &args.access_key_id, "local-fs")?;
                    reject_flag("secret-access-key", &args.secret_access_key, "local-fs")?;
                    reject_flag("endpoint-url", &args.endpoint_url, "local-fs")?;
                    reject_flag("session-token", &args.session_token, "local-fs")?;
                    reject_flag("account-id", &args.account_id, "local-fs")?;
                    reject_flag(
                        "service-account-key-path",
                        &args.service_account_key_path,
                        "local-fs",
                    )?;
                }
                StoreConfig::AwsS3 { .. } => {
                    reject_flag("root", &args.root, "aws-s3")?;
                    reject_flag("account-id", &args.account_id, "aws-s3")?;
                    reject_flag(
                        "service-account-key-path",
                        &args.service_account_key_path,
                        "aws-s3",
                    )?;
                }
                StoreConfig::CloudflareR2 { .. } => {
                    reject_flag("root", &args.root, "cloudflare-r2")?;
                    reject_flag("region", &args.region, "cloudflare-r2")?;
                    reject_flag("session-token", &args.session_token, "cloudflare-r2")?;
                    reject_flag(
                        "service-account-key-path",
                        &args.service_account_key_path,
                        "cloudflare-r2",
                    )?;
                }
                StoreConfig::GcpGcs { .. } => {
                    reject_flag("root", &args.root, "gcp-gcs")?;
                    reject_flag("region", &args.region, "gcp-gcs")?;
                    reject_flag("access-key-id", &args.access_key_id, "gcp-gcs")?;
                    reject_flag("secret-access-key", &args.secret_access_key, "gcp-gcs")?;
                    reject_flag("endpoint-url", &args.endpoint_url, "gcp-gcs")?;
                    reject_flag("session-token", &args.session_token, "gcp-gcs")?;
                    reject_flag("account-id", &args.account_id, "gcp-gcs")?;
                }
            }
        }
        ProfileConfig::Remote { .. } => {
            reject_flag("root", &args.root, "remote")?;
            reject_flag("bucket", &args.bucket, "remote")?;
            reject_flag("region", &args.region, "remote")?;
            reject_flag("access-key-id", &args.access_key_id, "remote")?;
            reject_flag("secret-access-key", &args.secret_access_key, "remote")?;
            reject_flag("endpoint-url", &args.endpoint_url, "remote")?;
            reject_flag("session-token", &args.session_token, "remote")?;
            reject_flag("account-id", &args.account_id, "remote")?;
            reject_flag("key-prefix", &args.key_prefix, "remote")?;
            reject_flag(
                "service-account-key-path",
                &args.service_account_key_path,
                "remote",
            )?;
        }
    }

    match existing {
        ProfileConfig::Embedded {
            store,
            default_namespace,
            writer_id,
            writer_version,
            lease_duration_ms,
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
                    access_key_id: args.access_key_id.clone().unwrap_or(access_key_id),
                    secret_access_key: args.secret_access_key.clone().unwrap_or(secret_access_key),
                    session_token: args.session_token.clone().or(session_token),
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
                    access_key_id: args.access_key_id.clone().unwrap_or(access_key_id),
                    secret_access_key: args.secret_access_key.clone().unwrap_or(secret_access_key),
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
            };
            Ok(ProfileConfig::Embedded {
                store,
                default_namespace,
                writer_id,
                writer_version,
                lease_duration_ms,
            })
        }
        ProfileConfig::Remote {
            server_url,
            default_namespace,
            auth_token,
        } => Ok(ProfileConfig::Remote {
            server_url: args.server_url.clone().unwrap_or(server_url),
            default_namespace,
            auth_token: args.auth_token.clone().or(auth_token),
        }),
    }
}

fn reject_flag(flag: &str, value: &Option<String>, profile_kind: &str) -> Result<(), CliError> {
    if value.is_some() {
        return Err(CliError::invalid_input(format!(
            "`--{flag}` does not apply to {profile_kind} profiles"
        )));
    }
    Ok(())
}

pub(super) fn apply_update_interactive(existing: ProfileConfig) -> Result<ProfileConfig, CliError> {
    match existing {
        ProfileConfig::Embedded {
            store,
            default_namespace,
            writer_id,
            writer_version,
            lease_duration_ms,
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
                    bucket: prompt::prompt_line_default("bucket name", &bucket)?,
                    region: {
                        let default_idx =
                            AWS_REGIONS.iter().position(|r| *r == region).unwrap_or(0);
                        prompt::prompt_fuzzy_choice("region", AWS_REGIONS, default_idx)?
                    },
                    access_key_id: prompt::prompt_line_default("access key id", &access_key_id)?,
                    secret_access_key: prompt::prompt_line_default(
                        "secret access key",
                        &secret_access_key,
                    )?,
                    endpoint_url: prompt::prompt_optional("endpoint url", endpoint_url.as_deref())?,
                    session_token: prompt::prompt_optional(
                        "session token",
                        session_token.as_deref(),
                    )?,
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
                    bucket: prompt::prompt_line_default("bucket name", &bucket)?,
                    account_id: prompt::prompt_line_default("account id", &account_id)?,
                    endpoint_url: prompt::prompt_line_default("endpoint url", &endpoint_url)?,
                    access_key_id: prompt::prompt_line_default("access key id", &access_key_id)?,
                    secret_access_key: prompt::prompt_line_default(
                        "secret access key",
                        &secret_access_key,
                    )?,
                    key_prefix: prompt::prompt_optional("key prefix", key_prefix.as_deref())?,
                },
                StoreConfig::GcpGcs {
                    bucket,
                    service_account_key_path,
                    key_prefix,
                } => StoreConfig::GcpGcs {
                    bucket: prompt::prompt_line_default("bucket name", &bucket)?,
                    service_account_key_path: prompt::prompt_line_default(
                        "service account key path",
                        &service_account_key_path,
                    )?,
                    key_prefix: prompt::prompt_optional("key prefix", key_prefix.as_deref())?,
                },
            };
            Ok(ProfileConfig::Embedded {
                store,
                default_namespace,
                writer_id,
                writer_version,
                lease_duration_ms,
            })
        }
        ProfileConfig::Remote {
            server_url,
            default_namespace,
            auth_token,
        } => Ok(ProfileConfig::Remote {
            server_url: prompt::prompt_line_default("server url", &server_url)?,
            default_namespace,
            auth_token: prompt::prompt_optional("auth token", auth_token.as_deref())?,
        }),
    }
}
