use anyhow::Result;
use loon_client::ops::{OpsConfig, OpsObjectStoreSpec};
use loon_server::mutation::{execute_client_mutation, ClientMutationExecutionParams};
use loon_server::objectstore::r2::R2StoreConfig;
use loon_server::objectstore::s3::AwsS3StoreConfig;
use loon_server::objectstore::ConfiguredObjectStore;
use loon_server::ops::{
    bootstrap_namespace as server_bootstrap_namespace,
    list_authoritative_path as server_list_path,
    load_namespace_state_summary as server_load_namespace_state_summary,
    read_authoritative_file_bytes as server_read_file_bytes,
    resolve_authoritative_path as server_resolve_path,
    translate_authoritative_state_to_remote_observations as server_translate,
    NamespaceBootstrapError, NamespaceStateSummaryError, RemoteObservationTranslationError,
};
use loon_types::server::{
    AuthoritativeFileBytes, AuthoritativePathEntry, BootstrappedNamespace,
    NamespaceBootstrapParams, NamespaceStateSummary, ServerTransport,
};
use loon_types::{
    ChangeSeq, ClientMutationRequest, ClientMutationResponse, NamespaceId, ObservedRemoteInode,
};
use thiserror::Error;

/// In-process transport that calls `loon-server` functions directly against a
/// `ConfiguredObjectStore`.
pub struct LocalTransport {
    store: ConfiguredObjectStore,
    writer_id: String,
    writer_version: String,
    now_ms: u64,
    lease_duration_ms: u64,
}

#[derive(Debug, Error)]
pub enum LocalTransportError {
    #[error(transparent)]
    Bootstrap(#[from] NamespaceBootstrapError),
    #[error(transparent)]
    StateSummary(#[from] NamespaceStateSummaryError),
    #[error(transparent)]
    RemoteObservations(#[from] RemoteObservationTranslationError),
    #[error("mutation execution failed: {0}")]
    Mutation(String),
    #[error("file read failed: {0}")]
    FileRead(String),
}

impl ServerTransport for LocalTransport {
    type Error = LocalTransportError;

    fn execute_mutation(
        &self,
        request: &ClientMutationRequest,
    ) -> std::result::Result<ClientMutationResponse, Self::Error> {
        execute_client_mutation(
            &self.store,
            request,
            &ClientMutationExecutionParams {
                writer_id: self.writer_id.clone(),
                writer_version: self.writer_version.clone(),
                now_ms: self.now_ms,
                lease_duration_ms: self.lease_duration_ms,
            },
        )
        .map(|executed| executed.response)
        .map_err(|err| LocalTransportError::Mutation(err.to_string()))
    }

    fn load_namespace_state_summary(
        &self,
        namespace_id: &NamespaceId,
    ) -> std::result::Result<NamespaceStateSummary, Self::Error> {
        Ok(server_load_namespace_state_summary(&self.store, namespace_id)?)
    }

    fn load_remote_observations(
        &self,
        namespace_id: &NamespaceId,
    ) -> std::result::Result<(ChangeSeq, Vec<ObservedRemoteInode>), Self::Error> {
        let summary = server_load_namespace_state_summary(&self.store, namespace_id)?;
        let observations = server_translate(&self.store, namespace_id)?;
        Ok((summary.head.seq, observations))
    }

    fn bootstrap_namespace(
        &self,
        namespace_id: &NamespaceId,
        params: &NamespaceBootstrapParams,
    ) -> std::result::Result<BootstrappedNamespace, Self::Error> {
        Ok(server_bootstrap_namespace(&self.store, namespace_id, params)?)
    }

    fn list_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> std::result::Result<Vec<AuthoritativePathEntry>, Self::Error> {
        server_list_path(&self.store, namespace_id, absolute_path)
            .map_err(|e| LocalTransportError::FileRead(e.to_string()))
    }

    fn resolve_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> std::result::Result<AuthoritativePathEntry, Self::Error> {
        server_resolve_path(&self.store, namespace_id, absolute_path)
            .map_err(|e| LocalTransportError::FileRead(e.to_string()))
    }

    fn read_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> std::result::Result<AuthoritativeFileBytes, Self::Error> {
        server_read_file_bytes(&self.store, namespace_id, absolute_path)
            .map_err(|e| LocalTransportError::FileRead(e.to_string()))
    }
}

pub fn make_transport(config: &OpsConfig) -> Result<LocalTransport> {
    let store = open_store(config)?;
    Ok(LocalTransport {
        store,
        writer_id: config.server.writer_id.clone(),
        writer_version: config.server.writer_version.clone(),
        now_ms: config.now_ms(),
        lease_duration_ms: config.server.lease_duration_ms,
    })
}

pub fn open_store(config: &OpsConfig) -> Result<ConfiguredObjectStore> {
    match &config.object_store {
        OpsObjectStoreSpec::LocalFs { root, key_prefix } => {
            ConfiguredObjectStore::local_fs(root, key_prefix.as_deref()).map_err(Into::into)
        }
        OpsObjectStoreSpec::AwsS3 {
            bucket,
            region,
            endpoint_url,
            access_key_id,
            secret_access_key,
            session_token,
            key_prefix,
            force_path_style,
        } => ConfiguredObjectStore::aws_s3(AwsS3StoreConfig {
            bucket: bucket.clone(),
            region: region.clone(),
            endpoint_url: endpoint_url.clone(),
            access_key_id: access_key_id.clone(),
            secret_access_key: secret_access_key.clone(),
            session_token: session_token.clone(),
            key_prefix: key_prefix.clone(),
            force_path_style: *force_path_style,
        })
        .map_err(Into::into),
        OpsObjectStoreSpec::CloudflareR2 {
            bucket,
            account_id,
            endpoint_url,
            access_key_id,
            secret_access_key,
            key_prefix,
        } => ConfiguredObjectStore::cloudflare_r2(R2StoreConfig {
            bucket: bucket.clone(),
            account_id: account_id.clone(),
            endpoint_url: endpoint_url.clone(),
            access_key_id: access_key_id.clone(),
            secret_access_key: secret_access_key.clone(),
            key_prefix: key_prefix.clone(),
        })
        .map_err(Into::into),
    }
}
