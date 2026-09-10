//! Publish-time metadata and the numbered WAL tip retained between batches.

use crate::checkpoint::VerifiedMetadataSegments;
use crate::checkpoint::{load_basis_metadata_segments, LoadedMetadataBasis, MetadataSegmentCache};
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::limits::MAX_UNFLUSHED_WAL_SEGMENTS;
use crate::metadata::{CommitReceiptRecord, MetadataState, MetadataView};
use crate::namespace::basis::{MetadataBasis, MetadataBasisIdentity};
use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
use crate::namespace::control_snapshot::load_head_and_metadata_basis;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::writer_epoch::ensure_writer_not_fenced;
use crate::wal::{
    ensure_replayed_head_matches, load_wal_chain, project_validated_wal_tail, WalChainLoadRequest,
};
use loonfs_api::v0::CommittedChange;
use loonfs_api::wire::control::AcquiredWriter;
use loonfs_api::{ChangeSeq, CommitId, ContentStoreId, NamespaceId};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;

pub(crate) struct PublishMetadataView<'a, S: ObjectStore + ?Sized> {
    content_store_id: ContentStoreId,
    pub(super) head: NamespaceReadState,
    pub(super) acquired_writer: AcquiredWriter,
    manifest_segments: VerifiedMetadataSegments<'a, S>,
    tail_state: Arc<MetadataState>,
    /// The WAL tail length when it has reached the write-stop bound, so the
    /// tail this publish would extend stays inside it. New commits are refused
    /// with that count; a commit id the namespace already knows is still
    /// answered from its receipt.
    write_stop: Option<u64>,
}

impl<S: ObjectStore + ?Sized> PublishMetadataView<'_, S> {
    pub(crate) fn metadata_view(&self) -> MetadataView<'_, '_, S> {
        MetadataView::from_loaded_head(&self.head, &self.manifest_segments, &self.tail_state)
    }

    pub(crate) fn content_store_id(&self) -> &ContentStoreId {
        &self.content_store_id
    }

    pub(super) fn write_stop(&self) -> Option<u64> {
        self.write_stop
    }

    pub(super) async fn find_commit_receipt(
        &self,
        commit_id: &CommitId,
    ) -> Result<Option<CommitReceiptRecord>> {
        self.metadata_view().find_commit_receipt(commit_id).await
    }

    /// Reads the retained change for a commit receipt.
    pub(super) async fn find_committed_change_at(
        &self,
        committed_seq: ChangeSeq,
    ) -> Result<Option<CommittedChange>> {
        super::changes::find_committed_change_at(
            self.manifest_segments.store(),
            &self.head.namespace_id,
            committed_seq,
        )
        .await
    }
}

/// Size bounds on the publish-time WAL-tail projection a view load will
/// accept for reuse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishTailOptions {
    pub max_tail_rows: usize,
    pub max_tail_decoded_bytes: usize,
}

impl Default for PublishTailOptions {
    fn default() -> Self {
        Self {
            max_tail_rows: crate::checkpoint::DEFAULT_WAL_TAIL_PROJECTION_ROWS,
            max_tail_decoded_bytes: crate::checkpoint::DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES,
        }
    }
}

/// The row and decoded-byte cost of a retained publish-tail projection.
///
/// Runtimes use these values to enforce aggregate cache limits without
/// recounting the projection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PublishTailWeight {
    pub rows: usize,
    pub decoded_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublishProjectionKey {
    namespace_id: NamespaceId,
    head_seq: ChangeSeq,
    basis: MetadataBasisIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishTailProjection {
    key: PublishProjectionKey,
    pub(crate) head: NamespaceReadState,
    pub(crate) retention_floor_seq: Option<ChangeSeq>,
    pub(crate) wal_tail_segments: u64,
    pub(crate) tail_state: Arc<MetadataState>,
}

impl PublishTailProjection {
    fn is_reusable_for(&self, key: &PublishProjectionKey, options: &PublishTailOptions) -> bool {
        self.key == *key && self.within_limits(options)
    }

    pub(crate) fn weight(&self) -> PublishTailWeight {
        PublishTailWeight {
            rows: self.tail_state.row_count(),
            decoded_bytes: self.tail_state.decoded_bytes(),
        }
    }

    pub(crate) fn within_limits(&self, options: &PublishTailOptions) -> bool {
        let weight = self.weight();
        weight.rows <= options.max_tail_rows
            && weight.decoded_bytes <= options.max_tail_decoded_bytes
    }

    pub(crate) fn basis(&self) -> &MetadataBasis {
        self.key.basis.basis()
    }

    pub(crate) fn manifest_head_seq(&self) -> ChangeSeq {
        self.key.basis.manifest_head_seq()
    }

    pub(crate) fn reanchor(&mut self, head: NamespaceReadState) {
        self.key.head_seq = head.seq;
        self.head = head;
    }
}

pub(crate) async fn load_publish_metadata_view<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    segment_cache: Option<&'a MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    acquired_writer: AcquiredWriter,
    cached_projection: Option<&PublishTailProjection>,
    options: &PublishTailOptions,
) -> Result<(PublishMetadataView<'a, S>, PublishTailProjection)> {
    let loaded = if let Some(cached) = cached_projection {
        crate::namespace::control_snapshot::LoadedNamespaceBasis {
            head: cached.head.clone(),
            basis: cached.basis().clone(),
            retention_floor_seq: cached.retention_floor_seq,
        }
    } else {
        load_head_and_metadata_basis(store, namespace_id)
            .await
            .map_err(CoreError::ControlObjectLoad)?
    };
    let retention_floor_seq = loaded.retention_floor_seq;
    let head = loaded.head;
    if head.status.is_deleted() {
        return Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            },
        ));
    }
    ensure_writer_not_fenced(&head, &acquired_writer)?;
    let catalog_entry = VerifiedNamespaceCatalogEntry::from_head(&head);
    let loaded_basis = load_basis_metadata_segments(store, segment_cache, &loaded.basis).await?;
    let key = PublishProjectionKey {
        namespace_id: namespace_id.clone(),
        head_seq: head.seq,
        basis: loaded_basis.identity.clone(),
    };
    let projection = if let Some(cached) =
        cached_projection.filter(|cached| cached.is_reusable_for(&key, options))
    {
        cached.clone()
    } else {
        load_publish_tail_projection(store, &head, retention_floor_seq, key, &loaded_basis).await?
    };

    let manifest_segments = loaded_basis.segments;
    let tail_state = Arc::clone(&projection.tail_state);

    Ok((
        PublishMetadataView {
            content_store_id: catalog_entry.content_store_id().clone(),
            head,
            acquired_writer,
            manifest_segments,
            tail_state,
            write_stop: (projection.wal_tail_segments >= MAX_UNFLUSHED_WAL_SEGMENTS)
                .then_some(projection.wal_tail_segments),
        },
        projection,
    ))
}

async fn load_publish_tail_projection<S: ObjectStore + ?Sized>(
    store: &S,
    head: &NamespaceReadState,
    retention_floor_seq: Option<ChangeSeq>,
    key: PublishProjectionKey,
    loaded_basis: &LoadedMetadataBasis<'_, S>,
) -> Result<PublishTailProjection> {
    let manifest_head = loaded_basis.replay_head(head);
    let wal_chain = load_wal_chain(
        store,
        WalChainLoadRequest {
            namespace_id: &key.namespace_id,
            chain_base_seq: manifest_head.seq,
            head_seq: head.seq,
            base_wal_no: head.last_folded_wal_no,
            tip_wal_no: head.wal_no,
            writer_epoch: head.writer_epoch,
        },
    )
    .await
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::WalChainLoad(error))
    })?;
    let replayed = project_validated_wal_tail(
        &manifest_head,
        &loaded_basis.base_state,
        Some(head.writer_epoch),
        &wal_chain,
    )
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::WalReplay(error))
    })?;
    ensure_replayed_head_matches(head, &replayed.resulting_head)?;
    let wal_tail_segments = u64::try_from(wal_chain.segments().len()).unwrap_or(u64::MAX);
    let projection = PublishTailProjection {
        key,
        head: head.clone(),
        retention_floor_seq,
        wal_tail_segments,
        tail_state: Arc::new(replayed.resulting_metadata_state),
    };
    Ok(projection)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::namespace::bootstrap::bootstrap_metadata_state;
    use loonfs_api::wire::control::ManifestRef;
    use loonfs_test_support::ids::namespace_id;

    fn manifest_basis(owner: &str, manifest_no: u64, checksum: &str) -> MetadataBasis {
        MetadataBasis(ManifestRef {
            owner_namespace_id: namespace_id(owner),
            manifest_no: loonfs_api::ManifestNo(manifest_no),

            manifest_head_seq: ChangeSeq(manifest_no),
            manifest_payload_checksum: checksum.to_owned(),
        })
    }

    fn projection_key(
        namespace: &str,
        head_seq: u64,
        basis: MetadataBasis,
        manifest_head_seq: u64,
    ) -> PublishProjectionKey {
        PublishProjectionKey {
            namespace_id: namespace_id(namespace),
            head_seq: ChangeSeq(head_seq),
            basis: MetadataBasisIdentity::from_verified_basis(basis, ChangeSeq(manifest_head_seq)),
        }
    }

    fn projection(key: PublishProjectionKey) -> PublishTailProjection {
        let mut head = NamespaceReadState::initial(
            key.namespace_id.clone(),
            ContentStoreId::parse("cs_0123456789abcdef0123456789abcdef")
                .expect("valid content store id"),
            1_000,
        );
        head.seq = key.head_seq;
        PublishTailProjection {
            head,
            retention_floor_seq: Some(ChangeSeq(0)),
            key,
            wal_tail_segments: 3,
            tail_state: Arc::new(bootstrap_metadata_state(1_000)),
        }
    }

    fn roomy_options() -> PublishTailOptions {
        PublishTailOptions {
            max_tail_rows: usize::MAX,
            max_tail_decoded_bytes: usize::MAX,
        }
    }

    #[test]
    fn cached_projection_misses_on_every_authoritative_key_component() {
        let key = projection_key(
            "fork-target",
            9,
            manifest_basis("fork-source", 4, "sha256:basis-a"),
            7,
        );
        let projection = projection(key.clone());
        let options = roomy_options();
        assert!(projection.is_reusable_for(&key, &options));

        let changed_keys = [
            (
                "namespace id",
                projection_key(
                    "other-target",
                    9,
                    manifest_basis("fork-source", 4, "sha256:basis-a"),
                    7,
                ),
            ),
            (
                "head sequence",
                projection_key(
                    "fork-target",
                    10,
                    manifest_basis("fork-source", 4, "sha256:basis-a"),
                    7,
                ),
            ),
            (
                "basis owner namespace",
                projection_key(
                    "fork-target",
                    9,
                    manifest_basis("other-source", 4, "sha256:basis-a"),
                    7,
                ),
            ),
            (
                "manifest logical identity",
                projection_key(
                    "fork-target",
                    9,
                    manifest_basis("fork-source", 5, "sha256:basis-a"),
                    7,
                ),
            ),
            (
                "manifest checksum",
                projection_key(
                    "fork-target",
                    9,
                    manifest_basis("fork-source", 4, "sha256:basis-b"),
                    7,
                ),
            ),
            (
                "verified manifest head sequence",
                projection_key(
                    "fork-target",
                    9,
                    manifest_basis("fork-source", 4, "sha256:basis-a"),
                    8,
                ),
            ),
        ];

        for (component, changed_key) in changed_keys {
            assert!(
                !projection.is_reusable_for(&changed_key, &options),
                "changing {component} must miss the cached projection"
            );
        }
    }

    #[test]
    fn cached_projection_hits_when_only_derived_payload_values_are_observed() {
        let key = projection_key(
            "fork-target",
            9,
            manifest_basis("fork-source", 4, "sha256:basis-a"),
            7,
        );
        let mut projection = projection(key.clone());

        // Manifest position is derived from the cohesive basis identity, and
        // the measured tail count remains cached payload rather than becoming
        // a second lookup coordinate.
        assert_eq!(projection.basis().manifest_no(), loonfs_api::ManifestNo(4));
        assert_eq!(projection.manifest_head_seq(), ChangeSeq(7));
        projection.wal_tail_segments += 1;
        assert!(projection.is_reusable_for(&key, &roomy_options()));
    }

    #[test]
    fn cached_projection_reuse_keeps_limit_checks_explicit() {
        let key = projection_key(
            "genesis-namespace",
            0,
            manifest_basis("genesis-namespace", 1, "sha256:genesis"),
            0,
        );
        let projection = projection(key.clone());

        assert!(projection.is_reusable_for(&key, &roomy_options()));
        assert!(!projection.is_reusable_for(
            &key,
            &PublishTailOptions {
                max_tail_rows: 0,
                max_tail_decoded_bytes: usize::MAX,
            },
        ));
        assert!(!projection.is_reusable_for(
            &key,
            &PublishTailOptions {
                max_tail_rows: usize::MAX,
                max_tail_decoded_bytes: 0,
            },
        ));
    }
}
