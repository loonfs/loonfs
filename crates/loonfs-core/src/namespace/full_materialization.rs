use crate::checkpoint::{
    head_from_manifest, load_namespace_manifest_envelope, load_verified_manifest_materialization,
    ManifestLoadError,
};
use crate::error::CoreError;
use crate::metadata::MetadataState;
use crate::namespace::catalog::{
    load_namespace_catalog_entry, namespace_initialization_state, NamespaceCatalogLoadError,
    NamespaceInitializationError, NamespaceInitializationState, VerifiedNamespaceCatalogEntry,
};
use crate::namespace::control::{read_head_object, read_lease_object, ControlObjectLoadError};
use crate::wal::{
    load_validated_wal_chain, replay_validated_wal_tail_with_metadata, WalChainLoadError,
    WalChainLoadRequest, WalReplayError,
};
use loonfs_api::wire::control::{HeadState, LeaseState, NamespaceDescriptorState, NamespaceState};
use loonfs_api::{wal_segment_id_start_seq, ChangeSeq, ContentStoreId, ManifestId, NamespaceId};
use loonfs_objectstore::{
    keys::{namespace_descriptor, namespace_head, wal_segment_id_from_key, wal_segment_prefix},
    ObjectStore,
};
use serde::{Deserialize, Serialize};
use std::mem::size_of;
use thiserror::Error;

/// Why a caller is allowed to reconstruct a full namespace materialization.
///
/// Normal current-head reads and writes deliberately have no purpose here:
/// they should use manifest-plus-tail views instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FullMaterializationPurpose {
    ForkInitializationTemporary,
    ChangeFeedTemporary,
    InspectionDebug,
    TestOracle,
}

impl FullMaterializationPurpose {
    pub const ALL: [Self; 4] = [
        Self::ForkInitializationTemporary,
        Self::ChangeFeedTemporary,
        Self::InspectionDebug,
        Self::TestOracle,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ForkInitializationTemporary => "fork_initialization_temporary",
            Self::ChangeFeedTemporary => "change_feed_temporary",
            Self::InspectionDebug => "inspection_debug",
            Self::TestOracle => "test_oracle",
        }
    }
}

impl std::fmt::Display for FullMaterializationPurpose {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Permissive transition guardrails for full namespace materialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullMaterializationLimits {
    pub max_rows: usize,
    pub max_decoded_bytes: usize,
    pub max_wal_tail_segments: u64,
}

impl Default for FullMaterializationLimits {
    fn default() -> Self {
        Self {
            max_rows: 10_000_000,
            max_decoded_bytes: 4 * 1024 * 1024 * 1024,
            max_wal_tail_segments: 10_000,
        }
    }
}

/// Fully materialized namespace view at one head.
///
/// This is intentionally expensive. Production read/write paths should prefer
/// manifest-plus-tail views and use this only through an explicit purpose.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullNamespaceMaterialization {
    pub namespace_descriptor: NamespaceDescriptorState,
    pub content_store_id: ContentStoreId,
    /// Oldest namespace sequence this materialization can answer snapshot reads for.
    pub snapshot_floor_seq: ChangeSeq,
    pub head: HeadState,
    pub head_etag: String,
    pub lease: LeaseState,
    pub metadata_state: MetadataState,
}

/// Approximate size of a full materialization.
///
/// Runtime caches use this as an eviction weight, not exact heap accounting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullNamespaceMaterializationWeight {
    pub rows: usize,
    pub decoded_bytes: usize,
}

impl FullNamespaceMaterialization {
    /// Returns the approximate cache weight for this materialization.
    pub fn weight(&self) -> FullNamespaceMaterializationWeight {
        FullNamespaceMaterializationWeight {
            rows: self.row_count(),
            decoded_bytes: self.decoded_bytes(),
        }
    }

    /// Returns the number of metadata rows in this materialization.
    pub fn row_count(&self) -> usize {
        self.metadata_state.row_count()
    }

    /// Returns an approximate decoded byte size for this materialization.
    pub fn decoded_bytes(&self) -> usize {
        size_of::<Self>()
            + self.namespace_descriptor.namespace_id.as_str().len()
            + self.namespace_descriptor.content_store_id.as_str().len()
            + self.content_store_id.as_str().len()
            + size_of::<ChangeSeq>()
            + self.head.namespace_id.as_str().len()
            + self.head_etag.len()
            + wal_tip_decoded_bytes(self.head.visible_wal_tip.as_ref())
            + self.lease.namespace_id.as_str().len()
            + self.lease.holder_id.len()
            + self.metadata_state.decoded_bytes()
    }
}

fn wal_tip_decoded_bytes(pointer: Option<&loonfs_api::wire::control::WalSegmentPointer>) -> usize {
    pointer
        .map(|pointer| {
            size_of::<loonfs_api::wire::control::WalSegmentPointer>()
                + pointer.object_key.len()
                + pointer.segment_id.len()
                + pointer.payload_checksum.len()
        })
        .unwrap_or(0)
}

/// Lightweight namespace head status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceHeadSummary {
    pub namespace_id: NamespaceId,
    pub head_seq: ChangeSeq,
    pub current_manifest_id: Option<ManifestId>,
    pub latest_checkpoint_id: Option<String>,
    /// WAL segment objects positioned past the loaded manifest.
    ///
    /// Derived from position-ordered object names, not from walking the
    /// chain: an inspection count for maintenance gating and operators, not
    /// a validated chain length.
    pub wal_tail_segments: u64,
    pub retention_floor_seq: ChangeSeq,
}

/// Opaque ETag probe for the namespace head object.
///
/// This only proves that the durable head object identity still matches a
/// previously reconstructed materialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceHeadEtagProbe {
    pub head_etag: String,
}

/// Error while reconstructing a full namespace materialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum FullMaterializationLoadError {
    #[error("failed to load namespace descriptor: {0}")]
    LoadNamespaceDescriptor(ControlObjectLoadError),
    #[error("failed to load content store descriptor: {0}")]
    LoadContentStoreDescriptor(ControlObjectLoadError),
    #[error(transparent)]
    LoadHead(#[from] ControlObjectLoadError),
    #[error("failed to load lease object: {0}")]
    LoadLease(ControlObjectLoadError),
    #[error("missing head etag for `{object_key}`")]
    MissingHeadEtag { object_key: String },
    #[error("namespace `{namespace_id}` is deleted")]
    NamespaceDeleted { namespace_id: NamespaceId },
    #[error("namespace `{namespace_id}` head has no current manifest")]
    MissingCurrentManifest { namespace_id: NamespaceId },
    #[error(
        "namespace head changed during materialization load for `{object_key}`: loaded `{loaded_head_etag}`, current `{current_head_etag}`"
    )]
    HeadChangedDuringLoad {
        object_key: String,
        loaded_head_etag: String,
        current_head_etag: String,
    },
    #[error(transparent)]
    WalChainLoad(#[from] WalChainLoadError),
    #[error(transparent)]
    ManifestLoad(#[from] ManifestLoadError),
    #[error("wal replay failed: {0:?}")]
    WalReplay(WalReplayError),
    #[error(
        "full materialization mismatch: expected current head `{expected:?}`, reconstructed `{actual:?}`"
    )]
    ReconstructedHeadMismatch {
        expected: Box<HeadState>,
        actual: Box<HeadState>,
    },
    #[error(
        "full namespace materialization for `{purpose}` exceeded guardrails: rows {rows}/{max_rows}, decoded bytes {decoded_bytes}/{max_decoded_bytes}, wal tail segments {wal_tail_segments}/{max_wal_tail_segments}"
    )]
    LimitExceeded {
        purpose: FullMaterializationPurpose,
        rows: usize,
        max_rows: usize,
        decoded_bytes: usize,
        max_decoded_bytes: usize,
        wal_tail_segments: u64,
        max_wal_tail_segments: u64,
    },
}

impl From<NamespaceCatalogLoadError> for FullMaterializationLoadError {
    fn from(value: NamespaceCatalogLoadError) -> Self {
        match value {
            NamespaceCatalogLoadError::LoadNamespaceDescriptor(error) => {
                Self::LoadNamespaceDescriptor(error)
            }
            NamespaceCatalogLoadError::LoadContentStoreDescriptor(error) => {
                Self::LoadContentStoreDescriptor(error)
            }
        }
    }
}

/// Reconstructs and verifies the current namespace materialization.
pub async fn load_full_namespace_materialization<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
    purpose: FullMaterializationPurpose,
) -> Result<FullNamespaceMaterialization, FullMaterializationLoadError> {
    load_full_namespace_materialization_with_limits(
        store,
        expected_namespace,
        purpose,
        FullMaterializationLimits::default(),
    )
    .await
}

pub(crate) async fn load_full_namespace_materialization_with_limits<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
    purpose: FullMaterializationPurpose,
    limits: FullMaterializationLimits,
) -> Result<FullNamespaceMaterialization, FullMaterializationLoadError> {
    let catalog_entry = load_namespace_catalog_entry(store, expected_namespace).await?;
    let loaded_head = read_head_object(store, expected_namespace).await?;
    let head_etag = loaded_head.metadata.etag.clone().ok_or_else(|| {
        FullMaterializationLoadError::MissingHeadEtag {
            object_key: loaded_head.object_key.clone(),
        }
    })?;
    load_full_namespace_materialization_at_head_with_catalog(
        store,
        expected_namespace,
        catalog_entry,
        loaded_head.envelope.state,
        head_etag,
        purpose,
        limits,
    )
    .await
}

/// Reconstructs and verifies a namespace materialization for an already-loaded head.
pub async fn load_full_namespace_materialization_at_head<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
    head: HeadState,
    head_etag: String,
    purpose: FullMaterializationPurpose,
) -> Result<FullNamespaceMaterialization, FullMaterializationLoadError> {
    load_full_namespace_materialization_at_head_with_limits(
        store,
        expected_namespace,
        head,
        head_etag,
        purpose,
        FullMaterializationLimits::default(),
    )
    .await
}

pub(crate) async fn load_full_namespace_materialization_at_head_with_limits<
    S: ObjectStore + ?Sized,
>(
    store: &S,
    expected_namespace: &NamespaceId,
    head: HeadState,
    head_etag: String,
    purpose: FullMaterializationPurpose,
    limits: FullMaterializationLimits,
) -> Result<FullNamespaceMaterialization, FullMaterializationLoadError> {
    if &head.namespace_id != expected_namespace {
        return Err(FullMaterializationLoadError::LoadHead(
            ControlObjectLoadError::NamespaceMismatch {
                object_key: namespace_head(expected_namespace.as_str()),
                expected: expected_namespace.clone(),
                actual: head.namespace_id.clone(),
            },
        ));
    }
    let catalog_entry = load_namespace_catalog_entry(store, expected_namespace).await?;
    load_full_namespace_materialization_at_head_with_catalog(
        store,
        expected_namespace,
        catalog_entry,
        head,
        head_etag,
        purpose,
        limits,
    )
    .await
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "full_namespace_materialization")
)]
async fn load_full_namespace_materialization_at_head_with_catalog<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
    catalog_entry: VerifiedNamespaceCatalogEntry,
    head: HeadState,
    head_etag: String,
    purpose: FullMaterializationPurpose,
    limits: FullMaterializationLimits,
) -> Result<FullNamespaceMaterialization, FullMaterializationLoadError> {
    // The single lifecycle gate: every read, publish, status, fork, and
    // checkpoint reconstructs through here, so a deleted head refuses them
    // all in one place.
    if head.state == NamespaceState::Deleted {
        return Err(FullMaterializationLoadError::NamespaceDeleted {
            namespace_id: expected_namespace.clone(),
        });
    }
    let loaded_lease = read_lease_object(store, expected_namespace)
        .await
        .map_err(FullMaterializationLoadError::LoadLease)?;

    let manifest_id = head.current_manifest_id.ok_or_else(|| {
        FullMaterializationLoadError::MissingCurrentManifest {
            namespace_id: expected_namespace.clone(),
        }
    })?;
    let materialized =
        load_verified_manifest_materialization(store, expected_namespace, manifest_id).await?;
    let (initial_head, initial_metadata_state) = (
        head_from_manifest(&head, &materialized.manifest),
        materialized.metadata_state,
    );
    let wal_chain = load_validated_wal_chain(
        store,
        WalChainLoadRequest {
            namespace_id: expected_namespace,
            chain_base_seq: initial_head.seq,
            head_seq: head.seq,
            visible_tip: head.visible_wal_tip.clone(),
            stop_after_seq: None,
        },
    )
    .await?;
    let wal_tail_segments = u64::try_from(wal_chain.segments().len()).unwrap_or(u64::MAX);
    let replayed = {
        let _span = tracing::info_span!("loon.phase", phase = "project_metadata_state").entered();
        replay_validated_wal_tail_with_metadata(
            &initial_head,
            &initial_metadata_state,
            wal_chain.segments(),
        )
        .map_err(FullMaterializationLoadError::WalReplay)
    }?;
    ensure_reconstructed_head_matches(&head, &replayed.resulting_head)?;
    ensure_head_etag_still_current(store, expected_namespace, &head_etag).await?;

    let materialization = FullNamespaceMaterialization {
        namespace_descriptor: catalog_entry.namespace_descriptor,
        content_store_id: catalog_entry.content_store_id,
        snapshot_floor_seq: initial_head.seq,
        head,
        head_etag,
        lease: loaded_lease.envelope.state,
        metadata_state: replayed.resulting_metadata_state,
    };
    enforce_full_materialization_limits(purpose, limits, &materialization, wal_tail_segments)?;
    let rows = materialization.row_count();
    let decoded_bytes = materialization.decoded_bytes();
    tracing::info!(
        phase = "full_namespace_materialization",
        purpose = purpose.as_str(),
        rows,
        decoded_bytes,
        wal_tail_segments,
        "full namespace materialized"
    );
    Ok(materialization)
}

fn enforce_full_materialization_limits(
    purpose: FullMaterializationPurpose,
    limits: FullMaterializationLimits,
    materialization: &FullNamespaceMaterialization,
    wal_tail_segments: u64,
) -> Result<(), FullMaterializationLoadError> {
    let rows = materialization.row_count();
    let decoded_bytes = materialization.decoded_bytes();
    if rows > limits.max_rows
        || decoded_bytes > limits.max_decoded_bytes
        || wal_tail_segments > limits.max_wal_tail_segments
    {
        return Err(FullMaterializationLoadError::LimitExceeded {
            purpose,
            rows,
            max_rows: limits.max_rows,
            decoded_bytes,
            max_decoded_bytes: limits.max_decoded_bytes,
            wal_tail_segments,
            max_wal_tail_segments: limits.max_wal_tail_segments,
        });
    }
    Ok(())
}

async fn ensure_head_etag_still_current<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
    loaded_head_etag: &str,
) -> Result<(), FullMaterializationLoadError> {
    let object_key = namespace_head(expected_namespace.as_str());
    let metadata = store
        .head(&object_key)
        .await
        .map_err(|error| {
            FullMaterializationLoadError::LoadHead(ControlObjectLoadError::Store(error.to_string()))
        })?
        .ok_or_else(|| {
            FullMaterializationLoadError::LoadHead(ControlObjectLoadError::MissingObject {
                object_key: object_key.clone(),
            })
        })?;
    let current_head_etag =
        metadata
            .etag
            .ok_or_else(|| FullMaterializationLoadError::MissingHeadEtag {
                object_key: object_key.clone(),
            })?;
    if current_head_etag != loaded_head_etag {
        return Err(FullMaterializationLoadError::HeadChangedDuringLoad {
            object_key,
            loaded_head_etag: loaded_head_etag.to_owned(),
            current_head_etag,
        });
    }
    Ok(())
}

pub async fn load_namespace_head_summary<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<NamespaceHeadSummary, CoreError> {
    match namespace_initialization_state(store, expected_namespace).await {
        Ok(NamespaceInitializationState::Complete) => {}
        Ok(NamespaceInitializationState::Absent) => {
            return Err(CoreError::FullMaterialization(
                FullMaterializationLoadError::LoadNamespaceDescriptor(
                    ControlObjectLoadError::MissingObject {
                        object_key: namespace_descriptor(expected_namespace.as_str()),
                    },
                ),
            ));
        }
        Ok(NamespaceInitializationState::Partial) => {
            return Err(CoreError::NamespacePartiallyInitialized {
                namespace_id: expected_namespace.clone(),
            });
        }
        Err(error) => return Err(map_namespace_initialization_error_to_core(error)),
    }

    let loaded_head = read_head_object(store, expected_namespace)
        .await
        .map_err(|error| {
            CoreError::FullMaterialization(FullMaterializationLoadError::LoadHead(error))
        })?;
    if loaded_head.envelope.state.state == NamespaceState::Deleted {
        return Err(CoreError::NamespaceDeleted {
            namespace_id: expected_namespace.clone(),
        });
    }
    let head = loaded_head.envelope.state;
    let manifest_id = head.current_manifest_id.ok_or_else(|| {
        CoreError::FullMaterialization(FullMaterializationLoadError::MissingCurrentManifest {
            namespace_id: expected_namespace.clone(),
        })
    })?;
    let manifest_materialization_seq =
        load_namespace_manifest_envelope(store, expected_namespace, manifest_id)
            .await
            .map_err(|error| {
                CoreError::FullMaterialization(FullMaterializationLoadError::ManifestLoad(error))
            })?
            .payload
            .head_seq;
    let wal_tail_segments = if head.visible_wal_tip.is_some() {
        count_wal_tail_segments_by_position(store, expected_namespace, manifest_materialization_seq)
            .await?
    } else {
        0
    };
    Ok(NamespaceHeadSummary {
        namespace_id: head.namespace_id,
        head_seq: head.seq,
        current_manifest_id: head.current_manifest_id,
        latest_checkpoint_id: head.latest_checkpoint_id,
        wal_tail_segments,
        retention_floor_seq: head.retention_floor_seq,
    })
}

/// Counts WAL tail segments from their position-ordered object names.
///
/// Status is an inspection surface, so the count comes from one listing
/// instead of loading and validating segment bodies: segment file names
/// carry their `start_seq`, and every chain segment past the manifest starts
/// above it. Objects that lost a head race are counted until reclamation
/// removes them, which can only over-trigger maintenance, never starve it.
/// Recovery authority stays with the head and chain.
async fn count_wal_tail_segments_by_position<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_materialization_seq: ChangeSeq,
) -> Result<u64, CoreError> {
    let keys = store
        .list_prefix(&wal_segment_prefix(namespace_id.as_str()))
        .await
        .map_err(|error| CoreError::Store(format!("list WAL tail segments: {error}")))?;
    let tail_segments = keys
        .iter()
        .filter_map(|key| wal_segment_id_from_key(key))
        .filter_map(wal_segment_id_start_seq)
        .filter(|start_seq| *start_seq > manifest_materialization_seq)
        .count();
    u64::try_from(tail_segments)
        .map_err(|_| CoreError::Store("WAL tail segment count overflow".to_owned()))
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "probe_namespace_head_etag", key_class = "namespace_head")
)]
pub async fn probe_namespace_head_etag<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<NamespaceHeadEtagProbe, CoreError> {
    NamespaceId::parse(expected_namespace.as_str())?;
    let object_key = namespace_head(expected_namespace.as_str());
    let metadata = store
        .head(&object_key)
        .await
        .map_err(|err| {
            CoreError::FullMaterialization(FullMaterializationLoadError::LoadHead(
                ControlObjectLoadError::Store(err.to_string()),
            ))
        })?
        .ok_or_else(|| {
            CoreError::FullMaterialization(FullMaterializationLoadError::LoadHead(
                ControlObjectLoadError::MissingObject {
                    object_key: object_key.clone(),
                },
            ))
        })?;
    let head_etag = metadata.etag.ok_or(CoreError::FullMaterialization(
        FullMaterializationLoadError::MissingHeadEtag { object_key },
    ))?;
    Ok(NamespaceHeadEtagProbe { head_etag })
}

fn map_namespace_initialization_error_to_core(error: NamespaceInitializationError) -> CoreError {
    match error {
        NamespaceInitializationError::InvalidNamespaceId(error) => {
            CoreError::InvalidNamespaceId(error)
        }
        NamespaceInitializationError::LoadNamespaceDescriptor(error) => {
            CoreError::FullMaterialization(FullMaterializationLoadError::LoadNamespaceDescriptor(
                error,
            ))
        }
        NamespaceInitializationError::LoadContentStoreDescriptor(error) => {
            CoreError::FullMaterialization(
                FullMaterializationLoadError::LoadContentStoreDescriptor(error),
            )
        }
        NamespaceInitializationError::InspectNamespaceDescriptor(_)
        | NamespaceInitializationError::InspectNamespaceHead(_)
        | NamespaceInitializationError::InspectNamespaceLease(_) => {
            CoreError::Store(error.to_string())
        }
    }
}

fn ensure_reconstructed_head_matches(
    current_head: &HeadState,
    reconstructed: &HeadState,
) -> Result<(), FullMaterializationLoadError> {
    // `active_fence_token` is intentionally excluded. Lease takeover can bump
    // the fence token in the control plane without any WAL replay.
    if current_head.namespace_id != reconstructed.namespace_id
        || current_head.seq != reconstructed.seq
        || current_head.head_commit_id != reconstructed.head_commit_id
        || current_head.next_inode_id != reconstructed.next_inode_id
        || current_head.name_policy != reconstructed.name_policy
        || current_head.current_manifest_id != reconstructed.current_manifest_id
        || current_head.latest_checkpoint_id != reconstructed.latest_checkpoint_id
        || current_head.retention_floor_seq != reconstructed.retention_floor_seq
        || (reconstructed.visible_wal_tip.is_some()
            && current_head.visible_wal_tip != reconstructed.visible_wal_tip)
    {
        return Err(FullMaterializationLoadError::ReconstructedHeadMismatch {
            expected: Box::new(current_head.clone()),
            actual: Box::new(reconstructed.clone()),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::load_verified_manifest_materialization;
    use crate::{BootstrapOptions, NamespaceEngine, ReadOptions, WriteOptions};
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use loonfs_api::wire::control::{encode_control_object, ControlObjectKind, HeadStateEnvelope};
    use loonfs_api::{ErrorCode, InodeId, InodeKind, ManifestId};
    use loonfs_objectstore::fs::LocalFsStore;
    use loonfs_objectstore::{ByteRange, ObjectBody, ObjectMetadata, ObjectStoreError, PutMode};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[derive(Debug)]
    struct StaleHeadGetStore {
        inner: Arc<LocalFsStore>,
        head_key: String,
        stale_head: ObjectBody,
    }

    #[async_trait]
    impl ObjectStore for StaleHeadGetStore {
        async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
            self.inner.head(key).await
        }

        async fn head_with_checksum(
            &self,
            key: &str,
        ) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
            self.inner.head_with_checksum(key).await
        }

        async fn get_with_metadata(
            &self,
            key: &str,
        ) -> Result<Option<ObjectBody>, ObjectStoreError> {
            if key == self.head_key {
                return Ok(Some(self.stale_head.clone()));
            }
            self.inner.get_with_metadata(key).await
        }

        async fn get(
            &self,
            key: &str,
            range: Option<ByteRange>,
        ) -> Result<Option<Bytes>, ObjectStoreError> {
            self.inner.get(key, range).await
        }

        async fn put(
            &self,
            key: &str,
            bytes: Bytes,
            mode: PutMode,
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            self.inner.put(key, bytes, mode).await
        }

        async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
            self.inner.delete(key).await
        }

        fn list_prefix_stream(
            &self,
            prefix: &str,
        ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
            self.inner.list_prefix_stream(prefix)
        }
    }

    #[tokio::test]
    async fn bootstrap_publishes_initial_manifest_for_root_metadata() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
        let namespace_id = NamespaceId::parse("primary").expect("valid namespace id");
        let engine = NamespaceEngine::builder(Arc::clone(&store))
            .namespace(namespace_id.clone())
            .writer("writer-a")
            .build()
            .expect("engine");
        engine
            .bootstrap_namespace(BootstrapOptions::default())
            .await
            .expect("bootstrap");

        let materialization = load_full_namespace_materialization(
            store.as_ref(),
            &namespace_id,
            FullMaterializationPurpose::TestOracle,
        )
        .await
        .expect("materialization");
        assert_eq!(
            materialization.head.current_manifest_id,
            Some(ManifestId(0))
        );
        assert_eq!(materialization.head.latest_checkpoint_id, None);

        let materialized =
            load_verified_manifest_materialization(store.as_ref(), &namespace_id, ManifestId(0))
                .await
                .expect("initial manifest materializes");
        assert_eq!(materialized.manifest.payload.head_seq, ChangeSeq(0));
        assert_eq!(materialized.manifest.payload.base_seq, ChangeSeq(0));
        assert!(materialized.manifest.payload.checkpoints.is_empty());
        assert_eq!(materialized.metadata_state.inodes().len(), 1);
        assert_eq!(materialized.metadata_state.inodes()[0].inode_id, InodeId(1));
        assert_eq!(
            materialized.metadata_state.inodes()[0].inode_kind,
            InodeKind::Dir
        );
    }

    #[tokio::test]
    async fn completed_namespace_without_current_manifest_is_corrupt() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
        let namespace_id = NamespaceId::parse("primary").expect("valid namespace id");
        let engine = NamespaceEngine::builder(Arc::clone(&store))
            .namespace(namespace_id.clone())
            .writer("writer-a")
            .build()
            .expect("engine");
        engine
            .bootstrap_namespace(BootstrapOptions::default())
            .await
            .expect("bootstrap");

        let loaded_head = read_head_object(store.as_ref(), &namespace_id)
            .await
            .expect("head");
        let mut head = loaded_head.envelope.state;
        head.current_manifest_id = None;
        let envelope =
            HeadStateEnvelope::from_state(ControlObjectKind::NamespaceHead, "test-writer", head)
                .expect("head envelope");
        let encoded = encode_control_object(&envelope).expect("encode head");
        store
            .put_overwrite(&loaded_head.object_key, Bytes::from(encoded))
            .await
            .expect("overwrite head");

        let materialization_error = load_full_namespace_materialization(
            store.as_ref(),
            &namespace_id,
            FullMaterializationPurpose::TestOracle,
        )
        .await
        .expect_err("materialization rejects missing manifest");
        assert!(matches!(
            materialization_error,
            FullMaterializationLoadError::MissingCurrentManifest { namespace_id: found } if found == namespace_id
        ));

        let summary_error = load_namespace_head_summary(store.as_ref(), &namespace_id)
            .await
            .expect_err("status rejects missing manifest");
        assert_eq!(summary_error.code(), ErrorCode::NamespaceCorrupt);

        let read_error = engine
            .resolve_path("/", ReadOptions::default())
            .await
            .expect_err("current read rejects missing manifest");
        assert_eq!(read_error.code(), ErrorCode::NamespaceCorrupt);
    }

    #[test]
    fn full_materialization_purposes_exclude_normal_runtime_paths() {
        let names = FullMaterializationPurpose::ALL
            .iter()
            .map(|purpose| purpose.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(names.len(), FullMaterializationPurpose::ALL.len());
        for forbidden in ["current_head_read", "publish_validation", "normal_write"] {
            assert!(!names.contains(forbidden));
        }
        for name in names {
            assert!(name.chars().all(|ch| ch.is_ascii_lowercase() || ch == '_'));
        }
    }

    #[test]
    fn production_read_write_entrypoints_do_not_load_full_materializations() {
        for (label, source) in [
            ("engine", include_str!("../engine.rs")),
            ("options", include_str!("../options.rs")),
            ("publisher", include_str!("../publisher.rs")),
        ] {
            for forbidden in [
                concat!("ReadOptions::", "full_materialization"),
                concat!("ReadSource::", "FullMaterialization"),
                "load_full_namespace_materialization(",
                concat!(
                    "FullMaterializationPurpose::",
                    "DirectPathPlanning",
                    "Temporary"
                ),
            ] {
                assert!(
                    !source.contains(forbidden),
                    "{label} must not reference `{forbidden}`"
                );
            }
        }
    }

    #[tokio::test]
    async fn full_materialization_limit_exceeded_is_structured() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
        let namespace_id = NamespaceId::parse("primary").expect("valid namespace id");
        let engine = NamespaceEngine::builder(Arc::clone(&store))
            .namespace(namespace_id.clone())
            .writer("writer-a")
            .build()
            .expect("engine");
        engine
            .bootstrap_namespace(BootstrapOptions::default())
            .await
            .expect("bootstrap");

        let error = load_full_namespace_materialization_with_limits(
            store.as_ref(),
            &namespace_id,
            FullMaterializationPurpose::TestOracle,
            FullMaterializationLimits {
                max_rows: 0,
                max_decoded_bytes: usize::MAX,
                max_wal_tail_segments: u64::MAX,
            },
        )
        .await
        .expect_err("row limit rejects bootstrap materialization");

        assert!(matches!(
            error,
            FullMaterializationLoadError::LimitExceeded {
                purpose: FullMaterializationPurpose::TestOracle,
                rows: 1,
                max_rows: 0,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn full_materialization_rejects_stale_head_body_when_current_etag_changed() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
        let namespace_id = NamespaceId::parse("primary").expect("valid namespace id");
        let engine = NamespaceEngine::builder(Arc::clone(&store))
            .namespace(namespace_id.clone())
            .writer("writer-a")
            .build()
            .expect("engine");
        engine
            .bootstrap_namespace(BootstrapOptions::default())
            .await
            .expect("bootstrap");

        let head_key = namespace_head(namespace_id.as_str());
        let stale_head = store
            .get_with_metadata(&head_key)
            .await
            .expect("read initial head")
            .expect("initial head exists");
        engine
            .create_dir("/docs", WriteOptions::default())
            .await
            .expect("create dir");

        let stale_store = StaleHeadGetStore {
            inner: store,
            head_key: head_key.clone(),
            stale_head,
        };
        let error = load_full_namespace_materialization(
            &stale_store,
            &namespace_id,
            FullMaterializationPurpose::TestOracle,
        )
        .await
        .expect_err("stale head is rejected");

        assert!(matches!(
            error,
            FullMaterializationLoadError::HeadChangedDuringLoad { object_key, .. } if object_key == head_key
        ));
    }

    #[tokio::test]
    async fn head_summary_counts_only_position_named_wal_segments() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
        let namespace_id = NamespaceId::parse("primary").expect("valid namespace id");
        let engine = NamespaceEngine::builder(Arc::clone(&store))
            .namespace(namespace_id.clone())
            .writer("writer-a")
            .build()
            .expect("engine");
        engine
            .bootstrap_namespace(BootstrapOptions::default())
            .await
            .expect("bootstrap");
        engine
            .create_dir("/docs", WriteOptions::default())
            .await
            .expect("first commit");
        engine
            .create_dir("/more", WriteOptions::default())
            .await
            .expect("second commit");

        // Foreign objects in the WAL prefix are skipped: a non-segment
        // suffix, and a current-suffix name without a position prefix.
        let prefix = loonfs_objectstore::keys::wal_segment_prefix(namespace_id.as_str());
        for stray in ["random.tmp", "seg_legacy.wal.zst"] {
            store
                .put_if_absent(&format!("{prefix}{stray}"), Bytes::from_static(b"x"))
                .await
                .expect("plant stray object");
        }

        let summary = load_namespace_head_summary(store.as_ref(), &namespace_id)
            .await
            .expect("summary with strays");
        assert_eq!(summary.wal_tail_segments, 2);

        // A position-named object that lost a head race still counts:
        // status is an inspection surface and may only over-trigger
        // maintenance, never starve it.
        let orphan_key = loonfs_objectstore::keys::wal_segment(
            namespace_id.as_str(),
            &loonfs_api::generate_wal_segment_id(ChangeSeq(9)),
        );
        store
            .put_if_absent(&orphan_key, Bytes::from_static(b"x"))
            .await
            .expect("plant orphan segment");

        let summary = load_namespace_head_summary(store.as_ref(), &namespace_id)
            .await
            .expect("summary with orphan");
        assert_eq!(summary.wal_tail_segments, 3);
    }
}
