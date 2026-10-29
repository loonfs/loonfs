//! Builds provider-specific profile configurations from command flags or
//! prompts.

use crate::args::{
    GcsStoreArgs, LocalStoreArgs, ProfileActorArgs, ProfileProviderCommand, R2StoreArgs,
    RemoteServerArgs, RuntimeBehavior, S3StoreArgs, SubjectSelectors,
};
use crate::commands::store_builders::{
    absolute_local_root, gcs_store, local_store, parse_profile_principal_scope,
    parse_profile_principals, r2_store, remote_connection, s3_store,
};
use crate::config::{
    absolute_env_path, ensure_subject_pairing, ProfileConfig, ProfileConnection, ProfileIdentity,
    StoreConfig,
};
use crate::error::{CliError, Result};
use crate::prompt;
use crate::resolve::subject_flag_error;
use std::path::PathBuf;

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

    pub(crate) fn actor(&self) -> &ProfileActorArgs {
        match self {
            Self::S3(args) => &args.actor,
            Self::R2(args) => &args.actor,
            Self::Gcs(args) => &args.actor,
            Self::Local(args) => &args.actor,
            Self::Remote(args) => &args.actor,
        }
    }

    pub(super) fn provider(&self) -> ProfileProvider {
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
pub(crate) enum FieldSource {
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

pub(crate) fn profile_provider(profile: &ProfileConfig) -> ProfileProvider {
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

pub(crate) fn provider_name(provider: ProfileProvider) -> &'static str {
    match provider {
        ProfileProvider::S3 => "s3",
        ProfileProvider::R2 => "r2",
        ProfileProvider::Gcs => "gcs",
        ProfileProvider::Local => "local",
        ProfileProvider::Remote => "remote",
    }
}

#[cfg(test)]
mod tests {
    use super::build_profile_from_command;
    use crate::args::{
        Cli, Command, LocalStoreArgs, ProfileActorArgs, ProfileCommand, ProfileLocalArgs,
        ProfileProviderCommand, ProfileR2Args, ProfileRemoteArgs, ProfileS3Args, R2StoreArgs,
        RemoteServerArgs, RuntimeBehavior, S3StoreArgs, SubjectSelectors, TopLevelCommand,
    };
    use crate::commands::profile_update::apply_update_flags;

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
