//! Publish-time metadata view: the current head plus the WAL tail replayed
//! over the manifest, with head-etag freshness checks against concurrent
//! publishers.

use crate::checkpoint::{
    head_from_manifest, load_verified_manifest_tables_with_cache, MetadataTableCache,
    VerifiedMetadataTables,
};
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::metadata::{CommitReceiptRecord, MetadataState, MetadataView};
use crate::namespace::catalog::load_namespace_catalog_entry;
use crate::namespace::control::{read_head_and_metadata_root, ControlObjectLoadError};
use crate::wal::{load_validated_wal_chain, project_validated_wal_tail, WalChainLoadRequest};
use loonfs_api::wire::control::{AcquiredWriter, HeadState, NamespaceState};
use loonfs_api::{ChangeSeq, CommitId, ContentStoreId, ManifestId, NamePolicy, NamespaceId};
use loonfs_objectstore::keys::wal_head;
use loonfs_objectstore::ObjectStore;

pub(crate) struct PublishMetadataView<'a, S: ObjectStore + ?Sized> {
    pub(super) content_store_id: ContentStoreId,
    name_policy: NamePolicy,
    pub(super) head: HeadState,
    pub(super) head_etag: String,
    pub(super) acquired_writer: Option<AcquiredWriter>,
    manifest_tables: VerifiedMetadataTables<'a, S>,
    tail_state: MetadataState,
}

impl<S: ObjectStore + ?Sized> PublishMetadataView<'_, S> {
    pub(crate) fn head(&self) -> &HeadState {
        &self.head
    }

    pub(crate) fn metadata_view(&self) -> MetadataView<'_, '_, S> {
        MetadataView::from_loaded_head(
            &self.head,
            self.name_policy,
            &self.manifest_tables,
            &self.tail_state,
        )
    }

    pub(super) async fn find_commit_receipt(
        &self,
        commit_id: &CommitId,
    ) -> Result<Option<CommitReceiptRecord>> {
        self.metadata_view().find_commit_receipt(commit_id).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishTailOptions {
    pub(crate) max_tail_rows: usize,
    pub(crate) max_tail_decoded_bytes: Option<usize>,
}

impl Default for PublishTailOptions {
    fn default() -> Self {
        Self {
            max_tail_rows: 1_000_000,
            max_tail_decoded_bytes: Some(256 * 1024 * 1024),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishTailProjection {
    pub(crate) namespace_id: NamespaceId,
    pub(crate) head_etag: String,
    pub(crate) head_seq: ChangeSeq,
    pub(crate) manifest_id: ManifestId,
    pub(crate) manifest_head_seq: ChangeSeq,
    pub(crate) manifest_payload_checksum: String,
    pub(crate) wal_tail_segments: u64,
    pub(crate) tail_state: MetadataState,
}

impl PublishTailProjection {
    fn matches(
        &self,
        namespace_id: &NamespaceId,
        head: &HeadState,
        head_etag: &str,
        manifest_id: ManifestId,
        manifest_head_seq: ChangeSeq,
        manifest_payload_checksum: &str,
    ) -> bool {
        self.namespace_id == *namespace_id
            && self.head_etag == head_etag
            && self.head_seq == head.seq
            && self.manifest_id == manifest_id
            && self.manifest_head_seq == manifest_head_seq
            && self.manifest_payload_checksum == manifest_payload_checksum
    }

    pub(crate) fn within_limits(&self, options: &PublishTailOptions) -> bool {
        self.tail_state.row_count() <= options.max_tail_rows
            && options
                .max_tail_decoded_bytes
                .map(|max| self.tail_state.decoded_bytes() <= max)
                .unwrap_or(true)
    }
}

pub(crate) async fn load_publish_metadata_view<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    table_cache: Option<&'a MetadataTableCache>,
    namespace_id: &NamespaceId,
    acquired_writer: Option<AcquiredWriter>,
    cached_projection: Option<&PublishTailProjection>,
    options: &PublishTailOptions,
) -> Result<(PublishMetadataView<'a, S>, PublishTailProjection)> {
    let catalog_entry = load_namespace_catalog_entry(store, namespace_id)
        .await
        .map_err(|error| CoreError::MetadataProjection(MetadataProjectionLoadError::from(error)))?;
    let (loaded_head, loaded_root) = read_head_and_metadata_root(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    let head_etag = loaded_head.metadata.etag.clone().ok_or_else(|| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::MissingHeadEtag {
            object_key: loaded_head.object_key.clone(),
        })
    })?;
    let head = loaded_head.envelope.state;
    let root = loaded_root.envelope.state;
    if head.state == NamespaceState::Deleted {
        return Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            },
        ));
    }
    if let Some(acquired_writer) = &acquired_writer {
        ensure_publish_head_matches_acquired_writer(&head, acquired_writer)?;
    }
    let manifest_tables = load_verified_manifest_tables_with_cache(
        store,
        table_cache,
        namespace_id,
        &root.manifest_object_id,
    )
    .await
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
    })?;
    if manifest_tables.manifest().payload_checksum != root.manifest_payload_checksum {
        return Err(CoreError::NamespaceCorrupt(format!(
            "metadata root for `{}` references manifest {:?} with checksum {} but the manifest carries {}",
            namespace_id.as_str(),
            root.manifest_id,
            root.manifest_payload_checksum,
            manifest_tables.manifest().payload_checksum,
        )));
    }
    let manifest_id = root.manifest_id;
    let manifest_head = head_from_manifest(&head, manifest_tables.manifest());
    let manifest_payload_checksum = manifest_tables.manifest().payload_checksum.clone();
    let projection = if let Some(cached) = cached_projection.filter(|cached| {
        cached.matches(
            namespace_id,
            &head,
            &head_etag,
            manifest_id,
            manifest_head.seq,
            &manifest_payload_checksum,
        ) && cached.within_limits(options)
    }) {
        cached.clone()
    } else {
        load_publish_tail_projection(
            store,
            namespace_id,
            &head,
            &head_etag,
            manifest_id,
            &manifest_head,
            manifest_payload_checksum,
        )
        .await?
    };

    let tail_state = projection.tail_state.clone();
    ensure_publish_head_etag_still_current(store, namespace_id, &head_etag).await?;

    Ok((
        PublishMetadataView {
            content_store_id: catalog_entry.content_store_id,
            name_policy: catalog_entry.namespace_config.name_policy,
            head,
            head_etag,
            acquired_writer,
            manifest_tables,
            tail_state,
        },
        projection,
    ))
}

fn ensure_publish_head_matches_acquired_writer(
    head: &HeadState,
    acquired_writer: &AcquiredWriter,
) -> Result<()> {
    if head.writer_epoch != acquired_writer.writer_epoch {
        let winner = head
            .writer
            .as_ref()
            .map(|writer| writer.writer_id.as_str())
            .unwrap_or("unknown");
        return Err(CoreError::WriterFenced(format!(
            "writer epoch {} was fenced by epoch {} (writer `{winner}`)",
            acquired_writer.writer_epoch.0, head.writer_epoch.0
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn load_publish_tail_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    head: &HeadState,
    head_etag: &str,
    manifest_id: ManifestId,
    manifest_head: &HeadState,
    manifest_payload_checksum: String,
) -> Result<PublishTailProjection> {
    let wal_chain = load_validated_wal_chain(
        store,
        WalChainLoadRequest {
            namespace_id,
            chain_base_seq: manifest_head.seq,
            head_seq: head.seq,
            visible_tip: head.visible_wal_tip.clone(),
            stop_after_seq: None,
            recent_segments: &head.recent_segments,
        },
    )
    .await
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::WalChainLoad(error))
    })?;
    let replayed = project_validated_wal_tail(
        manifest_head,
        &MetadataState::default(),
        Some(head.writer_epoch),
        &wal_chain,
    )
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::WalReplay(error))
    })?;
    ensure_publish_reconstructed_head_matches(head, &replayed.resulting_head)?;
    let wal_tail_segments = u64::try_from(wal_chain.segments().len()).unwrap_or(u64::MAX);
    let projection = PublishTailProjection {
        namespace_id: namespace_id.clone(),
        head_etag: head_etag.to_owned(),
        head_seq: head.seq,
        manifest_id,
        manifest_head_seq: manifest_head.seq,
        manifest_payload_checksum,
        wal_tail_segments,
        tail_state: replayed.resulting_metadata_state,
    };
    Ok(projection)
}

fn ensure_publish_reconstructed_head_matches(
    current_head: &HeadState,
    reconstructed: &HeadState,
) -> Result<()> {
    if current_head.namespace_id != reconstructed.namespace_id
        || current_head.seq != reconstructed.seq
        || current_head.head_commit_id != reconstructed.head_commit_id
        || current_head.next_inode_id != reconstructed.next_inode_id
        || (reconstructed.visible_wal_tip.is_some()
            && current_head.visible_wal_tip != reconstructed.visible_wal_tip)
    {
        return Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::ReplayedHeadMismatch {
                expected: Box::new(current_head.clone()),
                actual: Box::new(reconstructed.clone()),
            },
        ));
    }
    Ok(())
}

async fn ensure_publish_head_etag_still_current<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    loaded_head_etag: &str,
) -> Result<()> {
    let object_key = wal_head(namespace_id.as_str());
    let metadata = store
        .head(&object_key)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(
                ControlObjectLoadError::Store {
                    object_key: object_key.clone(),
                    message: error.message(),
                },
            ))
        })?
        .ok_or_else(|| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(
                ControlObjectLoadError::MissingObject {
                    object_key: object_key.clone(),
                },
            ))
        })?;
    let current_head_etag = metadata.etag.ok_or_else(|| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::MissingHeadEtag {
            object_key: object_key.clone(),
        })
    })?;
    if current_head_etag != loaded_head_etag {
        return Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::HeadChangedDuringLoad {
                object_key,
                loaded_head_etag: loaded_head_etag.to_owned(),
                current_head_etag,
            },
        ));
    }
    Ok(())
}
