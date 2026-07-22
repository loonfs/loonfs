//! Explicit per-namespace repair for incomplete namespace installations.

use super::bootstrap::{
    complete_post_head_bootstrap, recover_pre_head_debris, BootstrapNamespaceError, PreHeadRecovery,
};
use super::catalog::{
    map_namespace_initialization_error_to_core, namespace_initialization_state,
    NamespaceInitializationState,
};
use super::control::{read_metadata_root_object, ControlObjectLoadError};
use super::fork::complete_post_head_fork;
use crate::checkpoint::{load_namespace_manifest_envelope, ManifestLoadFailureClass};
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError};
use crate::gc::{reap_abandoned_bootstrap, AbandonedBootstrapReap};
use loonfs_api::{NamespaceId, NamespaceSummary, RepairNamespaceOutcome, RepairNamespaceResponse};
use loonfs_objectstore::keys::namespace_config;
use loonfs_objectstore::ObjectStore;

/// Explicitly completes or reaps one incomplete namespace installation.
pub async fn repair_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<RepairNamespaceResponse, CoreError> {
    let outcome = match namespace_initialization_state(store, namespace_id)
        .await
        .map_err(map_namespace_initialization_error_to_core)?
    {
        NamespaceInitializationState::Complete => RepairNamespaceOutcome::AlreadyComplete,
        NamespaceInitializationState::Absent => return Err(namespace_not_found(namespace_id)),
        NamespaceInitializationState::PreHeadDebris => {
            match recover_pre_head_debris(store, namespace_id, context.now_ms).await? {
                PreHeadRecovery::Cleaned => RepairNamespaceOutcome::Reaped,
                PreHeadRecovery::InFlight => RepairNamespaceOutcome::InFlight,
            }
        }
        NamespaceInitializationState::Partial => {
            if try_complete_post_head_install(store, namespace_id, context).await? {
                RepairNamespaceOutcome::Completed
            } else {
                match reap_abandoned_bootstrap(store, namespace_id, context).await? {
                    AbandonedBootstrapReap::Reaped => RepairNamespaceOutcome::Reaped,
                    AbandonedBootstrapReap::InFlight => RepairNamespaceOutcome::InFlight,
                    AbandonedBootstrapReap::AlreadyComplete => {
                        RepairNamespaceOutcome::AlreadyComplete
                    }
                }
            }
        }
    };

    Ok(RepairNamespaceResponse {
        namespace_id: namespace_id.clone(),
        outcome,
    })
}

async fn try_complete_post_head_install<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<bool, CoreError> {
    match complete_post_head_bootstrap(store, namespace_id, context).await {
        Ok(NamespaceSummary { .. }) => return Ok(true),
        Err(BootstrapNamespaceError::NamespacePartiallyInitialized { .. }) => {}
        Err(BootstrapNamespaceError::Core(error)) => return Err(error),
        Err(error) => return Err(CoreError::Internal(error.to_string())),
    }

    let root = match read_metadata_root_object(store, namespace_id).await {
        Ok(root) => root.envelope.state,
        Err(error @ ControlObjectLoadError::Store { .. }) => {
            return Err(CoreError::MetadataProjection(
                MetadataProjectionLoadError::LoadHead(error),
            ));
        }
        Err(_) => return Ok(false),
    };
    let manifest =
        match load_namespace_manifest_envelope(store, namespace_id, &root.manifest_object_id).await
        {
            Ok(manifest) => manifest,
            Err(error) if error.failure_class() == ManifestLoadFailureClass::Store => {
                return Err(CoreError::MetadataProjection(
                    MetadataProjectionLoadError::ManifestLoad(error),
                ));
            }
            Err(_) => return Ok(false),
        };
    let Some(fork) = manifest.payload.fork else {
        return Ok(false);
    };

    match complete_post_head_fork(store, &fork.source_namespace_id, namespace_id, context).await {
        Ok(NamespaceSummary { .. }) => Ok(true),
        Err(CoreError::NamespacePartiallyInitialized { .. }) => Ok(false),
        Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::LoadNamespaceDescriptor(error),
        )) if !matches!(error, ControlObjectLoadError::Store { .. }) => Ok(false),
        Err(error) => Err(error),
    }
}

fn namespace_not_found(namespace_id: &NamespaceId) -> CoreError {
    CoreError::MetadataProjection(MetadataProjectionLoadError::LoadNamespaceDescriptor(
        ControlObjectLoadError::MissingObject {
            object_key: namespace_config(namespace_id.as_str()),
        },
    ))
}
