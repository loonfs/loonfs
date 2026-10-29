//! Resolves the active profile and namespace from flags, config defaults,
//! and the environment.

use crate::args::{SubjectSelectors, TransientRetry};
use crate::backend::MaintenanceHost;
use crate::config::{
    non_empty_env, remote_client_config, ProfileConfig, ProfileConnection, ProfileMode,
    StoreConfig, ACTOR_ID_ENV, NAMESPACE_ENV, PRINCIPALS_ENV, PRINCIPAL_SCOPE_ENV, SUBJECT_ID_ENV,
};
use crate::error::{CliError, Result};
use loonfs::engine::DecodedBlockCacheConfig;
use loonfs::{LoonFs, SharedObjectStore, WriterId};
use loonfs_client::Client;
use loonfs_grep::{
    GrepBlockCache, GrepPassBudget, GrepService, GrepWorker, DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
};
use loonfs_types::{
    ActorId, NamespaceId, PrincipalId, PrincipalScope, SecretString, Subject, SubjectId,
    SubjectPart, SubjectPartsError,
};
use std::sync::Arc;

pub(crate) fn resolve_namespace(
    profile_name: &str,
    profile: &ProfileConfig,
    explicit_namespace: Option<&NamespaceId>,
) -> Result<NamespaceId> {
    if let Some(namespace) = explicit_namespace {
        return Ok(namespace.clone());
    }
    if let Some(namespace) = non_empty_env(NAMESPACE_ENV) {
        return parse_namespace_id(&namespace);
    }
    let namespace = profile
        .default_namespace
        .as_deref()
        .ok_or_else(|| CliError::no_default_namespace(profile_name))?;
    parse_namespace_id(namespace)
}

pub(crate) fn resolve_actor(
    profile: &ProfileConfig,
    explicit_id: Option<&ActorId>,
) -> Result<ActorId> {
    if let Some(id) = explicit_id {
        return Ok(id.clone());
    }
    if let Some(id) = non_empty_env(ACTOR_ID_ENV) {
        return ActorId::parse(&id).map_err(|error| {
            named_cli_input_error(ACTOR_ID_ENV, format!("invalid `{ACTOR_ID_ENV}`: {error}"))
        });
    }
    Ok(profile
        .identity
        .actor_id
        .clone()
        .unwrap_or_else(|| ActorId::parse("loonfs-cli").expect("the CLI actor id should be valid")))
}

/// The subject the command acts as. Principals come from the flag, the
/// environment, or the profile; the subject id defaults to the actor id
/// the same way the server's header does. No principals means the token
/// holder.
pub(crate) fn resolve_subject(
    profile: &ProfileConfig,
    actor_id: &ActorId,
    selectors: &SubjectSelectors,
) -> Result<Option<Subject>> {
    let environment_principals = non_empty_env(PRINCIPALS_ENV);
    let profile_principals = profile
        .identity
        .principals
        .as_ref()
        .filter(|principals| !principals.is_empty())
        .map(|principals| {
            principals
                .iter()
                .map(PrincipalId::as_str)
                .collect::<Vec<_>>()
                .join(",")
        });
    let (principals_source, principals) = first_subject_part(
        selectors.principals.as_deref(),
        environment_principals.as_deref(),
        profile_principals.as_deref(),
    );
    let environment_principal_scope = non_empty_env(PRINCIPAL_SCOPE_ENV);
    let profile_principal_scope = profile.identity.principal_scope.as_ref();
    let (principal_scope_source, principal_scope) = first_subject_part(
        selectors.principal_scope.as_deref(),
        environment_principal_scope.as_deref(),
        profile_principal_scope.map(PrincipalScope::as_str),
    );
    let environment_subject_id = non_empty_env(SUBJECT_ID_ENV);
    let profile_subject_id_with_principals = profile
        .identity
        .subject_id
        .as_ref()
        .filter(|_| principals.is_some());
    let (subject_id_source, subject_id) = first_subject_part(
        selectors.subject_id.as_ref().map(SubjectId::as_str),
        environment_subject_id.as_deref(),
        profile_subject_id_with_principals.map(SubjectId::as_str),
    );
    let source = |part| match part {
        SubjectPart::Principals => principals_source,
        SubjectPart::PrincipalScope => principal_scope_source,
        SubjectPart::SubjectId => subject_id_source,
        SubjectPart::ActorId => SubjectSource::Profile,
    };
    Subject::from_parts(
        principal_scope,
        subject_id,
        Some(actor_id.as_str()),
        principals,
    )
    .map_err(|error| subject_parts_error(error, source))
}

/// Reports a subject that its flags alone name, as the profile commands'
/// flags do.
pub(crate) fn subject_flag_error(error: SubjectPartsError) -> CliError {
    subject_parts_error(error, |_| SubjectSource::Flag)
}

/// Reports `error` under the name each part had where it came from.
fn subject_parts_error(
    error: SubjectPartsError,
    source: impl Fn(SubjectPart) -> SubjectSource,
) -> CliError {
    match error {
        SubjectPartsError::Missing { part, required_by } => {
            let required = subject_part_name(part, source(required_by));
            let present = subject_part_name(required_by, source(required_by));
            named_cli_input_error(
                required,
                format!("`{required}` is required with `{present}`"),
            )
        }
        SubjectPartsError::Invalid { part, message } => {
            let name = subject_part_name(part, source(part));
            named_cli_input_error(name, format!("invalid `{name}`: {message}"))
        }
        other => CliError::invalid_request(other.to_string()),
    }
}

/// Where one part of the subject came from, which decides the name an
/// error gives it.
#[derive(Debug, Clone, Copy)]
enum SubjectSource {
    Flag,
    Environment,
    Profile,
}

/// Returns the first present value among the flag, the environment, and the
/// profile, with where it came from.
fn first_subject_part<'a>(
    flag: Option<&'a str>,
    environment: Option<&'a str>,
    profile: Option<&'a str>,
) -> (SubjectSource, Option<&'a str>) {
    match (flag, environment) {
        (Some(value), _) => (SubjectSource::Flag, Some(value)),
        (None, Some(value)) => (SubjectSource::Environment, Some(value)),
        (None, None) => (SubjectSource::Profile, profile),
    }
}

fn subject_part_name(part: SubjectPart, source: SubjectSource) -> &'static str {
    match (part, source) {
        (SubjectPart::Principals, SubjectSource::Flag) => "--principals",
        (SubjectPart::Principals, SubjectSource::Environment) => PRINCIPALS_ENV,
        (SubjectPart::PrincipalScope, SubjectSource::Flag) => "--principal-scope",
        (SubjectPart::PrincipalScope, SubjectSource::Environment) => PRINCIPAL_SCOPE_ENV,
        (SubjectPart::SubjectId, SubjectSource::Flag) => "--subject-id",
        (SubjectPart::SubjectId, SubjectSource::Environment) => SUBJECT_ID_ENV,
        (SubjectPart::ActorId, _) => SubjectPart::SubjectId.as_str(),
        (_, SubjectSource::Profile) => part.as_str(),
    }
}

fn named_cli_input_error(name: &str, message: String) -> CliError {
    let error = CliError::invalid_request(message);
    if name.starts_with('-') {
        error.with_param(name)
    } else {
        error
    }
}

/// Parses a namespace ID after argument parsing. Invalid IDs use the shared
/// `invalid_request` code in both embedded and remote modes.
pub(crate) fn parse_namespace_id(namespace: &str) -> Result<NamespaceId> {
    NamespaceId::parse(namespace).map_err(|error| CliError::invalid_request(error.to_string()))
}

fn default_writer_id() -> String {
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "loonfs-cli".to_owned())
}

/// The client of a resolved profile, and the backend that serves it.
pub(crate) struct ResolvedTarget {
    pub(crate) client: Client,
    pub(crate) backend: Backend,
}

/// What serves a resolved profile's client.
pub(crate) enum Backend {
    /// The runtime this process opened over the profile's store.
    Embedded {
        host: Box<MaintenanceHost>,
        /// The digest of the store config, which upload journals key on.
        journal_identity: String,
    },
    /// A LoonFS server.
    Remote { server_url: String },
}

impl ResolvedTarget {
    pub(crate) async fn remote_connectivity(&self) -> Result<()> {
        if let Backend::Remote { .. } = self.backend {
            match self.client.get_health().await {
                Ok(()) | Err(loonfs_client::ClientError::Api { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    pub(crate) async fn remote_health(&self) -> Result<()> {
        if let Backend::Remote { .. } = self.backend {
            self.client.get_health().await?;
        }
        Ok(())
    }

    pub(crate) async fn resolve(
        profile_name: &str,
        profile: &ProfileConfig,
        transient_retry: TransientRetry,
    ) -> Result<Self> {
        profile.ensure_store_usable(profile_name)?;
        match &profile.connection {
            ProfileConnection::Embedded { store, writer_id } => {
                Self::embedded(store, writer_id.as_deref(), transient_retry).await
            }
            ProfileConnection::Remote {
                server_url,
                auth_token,
                ca_cert_path,
            } => Self::remote(
                server_url,
                auth_token.as_ref(),
                ca_cert_path.as_deref(),
                transient_retry,
            ),
        }
    }

    pub(crate) fn mode(&self) -> ProfileMode {
        match self.backend {
            Backend::Embedded { .. } => ProfileMode::Embedded,
            Backend::Remote { .. } => ProfileMode::Remote,
        }
    }

    pub(crate) fn journal_identity(&self) -> &str {
        match &self.backend {
            Backend::Embedded {
                journal_identity, ..
            } => journal_identity,
            Backend::Remote { server_url } => server_url,
        }
    }

    pub(crate) fn scope_to_subject(&mut self, subject: &Subject) {
        self.client = self.client.with_subject(subject.clone());
    }
}

impl ResolvedTarget {
    pub(crate) async fn embedded(
        store_config: &StoreConfig,
        writer_id: Option<&str>,
        transient_retry: TransientRetry,
    ) -> Result<Self> {
        // One command drives the runtime, its maintenance, and grep from one
        // Tokio runtime, so they deliberately share one provider client.
        let store: SharedObjectStore = store_config
            .configured_object_store()
            .map_err(|err| CliError::invalid_config(err.public_message().into_owned()))?
            .into_shared();
        let journal_identity = loonfs_types::sha256_hex(
            &serde_json::to_vec(store_config)
                .map_err(|error| CliError::invalid_config(error.to_string()))?,
        );
        Self::over_store(
            store,
            writer_id,
            store_config.kind(),
            loonfs::InlineContentPolicy::default(),
            transient_retry,
            journal_identity,
        )
        .await
    }

    /// Opens the runtime over a store the caller already holds.
    ///
    /// [`Self::embedded`] opens the profile's configured store and passes it here.
    /// Callers that need to observe store operations may provide a wrapped
    /// store directly.
    pub(crate) async fn over_store(
        store: SharedObjectStore,
        writer_id: Option<&str>,
        store_kind: loonfs_objectstore::ConfiguredObjectStoreKind,
        inline_content: loonfs::InlineContentPolicy,
        transient_retry: TransientRetry,
        journal_identity: String,
    ) -> Result<Self> {
        let writer_id = writer_id
            .map(ToOwned::to_owned)
            .unwrap_or_else(default_writer_id);
        let writer = WriterId::parse(writer_id.as_str())
            .map_err(|error| CliError::invalid_config(error.to_string()))?;
        let runtime = LoonFs::builder_with_store(store.clone())
            .writer_id(writer)
            .inline_content(inline_content.clone())
            // A CLI invocation is one solo mutation: holding the commit
            // window open would only add its full delay to every command.
            .min_publish_interval_ms(0)
            .store_kind(store_kind)
            .build()
            .await
            .map_err(CliError::from)?;
        let maintenance = runtime.maintenance(
            WriterId::parse(format!("{writer_id}-maintenance"))
                .map_err(|error| CliError::invalid_config(error.to_string()))?,
        );
        let grep_block_cache = Arc::new(GrepBlockCache::new(DecodedBlockCacheConfig::new(
            DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
        )));
        let grep_worker = GrepWorker::new(
            runtime.object_store(),
            runtime.read_only(),
            runtime.clone(),
            GrepPassBudget::default(),
        );
        let host = MaintenanceHost {
            runtime,
            maintenance,
            grep_worker,
        };
        let client = crate::backend::client(
            &host,
            GrepService::new(grep_block_cache, host.runtime.monotonic_timer()),
            transient_retry,
        )?;
        Ok(Self {
            client,
            backend: Backend::Embedded {
                host: Box::new(host),
                journal_identity,
            },
        })
    }
}

impl ResolvedTarget {
    fn remote(
        server_url: &str,
        auth_token: Option<&SecretString>,
        ca_cert_path: Option<&str>,
        transient_retry: TransientRetry,
    ) -> Result<Self> {
        let client = Client::new(remote_client_config(
            server_url,
            auth_token,
            ca_cert_path,
            transient_retry,
        ))
        .map_err(|error| CliError::invalid_config(error.to_string()))?;
        Ok(Self {
            backend: Backend::Remote {
                server_url: client.server_url().to_owned(),
            },
            client,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::ResolvedTarget;
    use loonfs_types::SecretString;

    #[test]
    fn a_stored_token_is_rejected_for_non_loopback_http() {
        let stored = SecretString::from("stored-token");
        let error = ResolvedTarget::remote(
            "http://example.internal",
            Some(&stored),
            None,
            crate::args::TransientRetry::Retry,
        )
        .err()
        .expect("a token over non-loopback plaintext HTTP should be rejected");

        assert_eq!(error.code, "invalid_config");
        assert!(
            error
                .message
                .contains("bearer tokens require https except for loopback http URLs"),
            "{}",
            error.message
        );
    }
}
