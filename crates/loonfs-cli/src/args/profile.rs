//! The `profile` commands and the store flags of each provider.

use clap::{Args, Subcommand, ValueEnum, ValueHint};
use loonfs_types::ActorId;

#[derive(Debug, Subcommand)]
pub(crate) enum ProfileCommand {
    /// Add a profile to the config file.
    Create {
        #[command(subcommand)]
        provider: Box<ProfileProviderCommand>,
    },
    /// List configured profiles.
    List,
    /// Show one profile (secrets redacted).
    Show {
        #[arg(value_hint = ValueHint::Other)]
        name: Option<String>,
    },
    /// Update fields of an existing profile.
    Update {
        #[command(subcommand)]
        provider: Box<ProfileProviderCommand>,
    },
    /// Delete a profile from the config file.
    Delete {
        #[arg(value_hint = ValueHint::Other)]
        name: String,
    },
    /// Make a profile the default.
    Use {
        #[arg(value_hint = ValueHint::Other)]
        name: String,
    },
}

/// The provider of a profile that `profile create` creates or `profile
/// update` updates, with the provider's own flags.
#[derive(Debug, Subcommand)]
pub(crate) enum ProfileProviderCommand {
    /// An AWS S3-backed embedded profile.
    S3(ProfileS3Args),
    /// A Cloudflare R2-backed embedded profile.
    R2(ProfileR2Args),
    /// A Google Cloud Storage-backed embedded profile.
    Gcs(ProfileGcsArgs),
    /// A local-filesystem-backed embedded profile.
    Local(ProfileLocalArgs),
    /// A remote-server profile.
    Remote(ProfileRemoteArgs),
}

#[derive(Debug, Default, Args)]
pub(crate) struct ProfileActorArgs {
    /// Actor ID to save in the profile. A profile without one acts as
    /// loonfs-cli.
    #[arg(long, value_hint = ValueHint::Other)]
    pub actor_id: Option<ActorId>,
}

/// How an S3 client addresses a bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum BucketAddressing {
    /// The bucket is part of the host name, as in `bucket.s3.amazonaws.com`.
    VirtualHosted,
    /// The bucket is the first path segment, as in `s3.amazonaws.com/bucket`.
    Path,
}

#[derive(Debug, Clone, Default, Args)]
pub(crate) struct S3StoreArgs {
    /// S3 bucket name.
    #[arg(long, value_hint = ValueHint::Other)]
    pub bucket: Option<String>,
    /// AWS region.
    #[arg(long, value_hint = ValueHint::Other)]
    pub region: Option<String>,
    /// AWS credential source.
    #[arg(long, value_name = "ambient|static", value_hint = ValueHint::Other)]
    pub credential_source: Option<String>,
    /// Static access key ID.
    #[arg(long, value_hint = ValueHint::Other)]
    pub access_key_id: Option<String>,
    /// Static secret access key.
    #[arg(long, value_hint = ValueHint::Other)]
    pub secret_access_key: Option<String>,
    /// Custom S3 endpoint URL.
    #[arg(long, value_hint = ValueHint::Url)]
    pub endpoint_url: Option<String>,
    /// Optional static session token.
    #[arg(long, value_hint = ValueHint::Other)]
    pub session_token: Option<String>,
    /// Optional object-key prefix.
    #[arg(long, value_hint = ValueHint::Other)]
    pub key_prefix: Option<String>,
    /// Bucket addressing style. The bare flag selects path-style addressing.
    /// Omit it to keep the stored style, which is virtual-hosted for a new
    /// profile.
    #[arg(
        long,
        value_enum,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "path"
    )]
    pub force_path_style: Option<BucketAddressing>,
}

impl S3StoreArgs {
    pub(crate) fn has_update_flags(&self) -> bool {
        self.bucket.is_some()
            || self.region.is_some()
            || self.credential_source.is_some()
            || self.access_key_id.is_some()
            || self.secret_access_key.is_some()
            || self.endpoint_url.is_some()
            || self.session_token.is_some()
            || self.key_prefix.is_some()
            || self.force_path_style.is_some()
    }
}

#[derive(Debug, Clone, Default, Args)]
pub(crate) struct R2StoreArgs {
    /// R2 bucket name.
    #[arg(long, value_hint = ValueHint::Other)]
    pub bucket: Option<String>,
    /// Cloudflare account ID.
    #[arg(long, value_hint = ValueHint::Other)]
    pub account_id: Option<String>,
    /// R2 endpoint URL.
    #[arg(long, value_hint = ValueHint::Url)]
    pub endpoint_url: Option<String>,
    /// R2 credential source.
    #[arg(long, value_name = "ambient|static", value_hint = ValueHint::Other)]
    pub credential_source: Option<String>,
    /// Static access key ID.
    #[arg(long, value_hint = ValueHint::Other)]
    pub access_key_id: Option<String>,
    /// Static secret access key.
    #[arg(long, value_hint = ValueHint::Other)]
    pub secret_access_key: Option<String>,
    /// Optional object-key prefix.
    #[arg(long, value_hint = ValueHint::Other)]
    pub key_prefix: Option<String>,
}

impl R2StoreArgs {
    pub(crate) fn has_update_flags(&self) -> bool {
        self.bucket.is_some()
            || self.account_id.is_some()
            || self.endpoint_url.is_some()
            || self.credential_source.is_some()
            || self.access_key_id.is_some()
            || self.secret_access_key.is_some()
            || self.key_prefix.is_some()
    }
}

#[derive(Debug, Clone, Default, Args)]
pub(crate) struct GcsStoreArgs {
    /// GCS bucket name.
    #[arg(long, value_hint = ValueHint::Other)]
    pub bucket: Option<String>,
    /// Path to a GCP service-account key.
    #[arg(long, value_hint = ValueHint::FilePath)]
    pub service_account_key_path: Option<String>,
    /// Optional object-key prefix.
    #[arg(long, value_hint = ValueHint::Other)]
    pub key_prefix: Option<String>,
}

impl GcsStoreArgs {
    pub(crate) fn has_update_flags(&self) -> bool {
        self.bucket.is_some()
            || self.service_account_key_path.is_some()
            || self.key_prefix.is_some()
    }
}

#[derive(Debug, Clone, Default, Args)]
pub(crate) struct LocalStoreArgs {
    /// Local filesystem store root.
    #[arg(long, value_hint = ValueHint::DirPath)]
    pub root: Option<String>,
    /// Optional object-key prefix.
    #[arg(long, value_hint = ValueHint::Other)]
    pub key_prefix: Option<String>,
}

impl LocalStoreArgs {
    pub(crate) fn has_update_flags(&self) -> bool {
        self.root.is_some() || self.key_prefix.is_some()
    }
}

#[derive(Debug, Clone, Default, Args)]
pub(crate) struct RemoteServerArgs {
    /// Remote LoonFS server URL.
    #[arg(long, value_hint = ValueHint::Url)]
    pub server_url: Option<String>,
    /// Remote bearer token to store. An empty value clears the stored token,
    /// and a profile without one uses LOONFS_AUTH_TOKEN at request time.
    #[arg(long, value_hint = ValueHint::Other)]
    pub auth_token: Option<String>,
    /// PEM bundle of extra certificate authorities to trust for an
    /// https server URL, when a private CA issued the certificate.
    #[arg(long, value_hint = ValueHint::FilePath)]
    pub ca_cert_path: Option<String>,
}

impl RemoteServerArgs {
    pub(crate) fn has_update_flags(&self) -> bool {
        self.server_url.is_some() || self.auth_token.is_some() || self.ca_cert_path.is_some()
    }
}

#[derive(Debug, Args)]
pub(crate) struct ProfileS3Args {
    #[arg(value_hint = ValueHint::Other)]
    pub name: String,
    #[command(flatten)]
    pub store: S3StoreArgs,
    #[command(flatten)]
    pub actor: ProfileActorArgs,
}

#[derive(Debug, Args)]
pub(crate) struct ProfileR2Args {
    #[arg(value_hint = ValueHint::Other)]
    pub name: String,
    #[command(flatten)]
    pub store: R2StoreArgs,
    #[command(flatten)]
    pub actor: ProfileActorArgs,
}

#[derive(Debug, Args)]
pub(crate) struct ProfileGcsArgs {
    #[arg(value_hint = ValueHint::Other)]
    pub name: String,
    #[command(flatten)]
    pub store: GcsStoreArgs,
    #[command(flatten)]
    pub actor: ProfileActorArgs,
}

#[derive(Debug, Args)]
pub(crate) struct ProfileLocalArgs {
    #[arg(value_hint = ValueHint::Other)]
    pub name: String,
    #[command(flatten)]
    pub store: LocalStoreArgs,
    #[command(flatten)]
    pub actor: ProfileActorArgs,
}

#[derive(Debug, Args)]
pub(crate) struct ProfileRemoteArgs {
    #[arg(value_hint = ValueHint::Other)]
    pub name: String,
    #[command(flatten)]
    pub server: RemoteServerArgs,
    #[command(flatten)]
    pub actor: ProfileActorArgs,
}
