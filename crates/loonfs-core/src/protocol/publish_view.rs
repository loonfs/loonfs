//! Publish-time metadata and the numbered WAL tip retained between batches.

use crate::checkpoint::VerifiedMetadataSegments;
use crate::checkpoint::{load_basis_metadata_segments, LoadedMetadataBasis, MetadataSegmentCache};
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::limits::MAX_UNFLUSHED_WAL_SEGMENTS;
use crate::metadata::{CommitReceiptRecord, MetadataView};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::read_anchor::load_read_anchor;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::writer_epoch::ensure_writer_not_fenced;
use crate::wal::load_replayed_wal_tail;
use crate::wal::ProjectedWalTail;
use loonfs_api::v0::Commit;
use loonfs_api::wire::control::AcquiredWriter;
use loonfs_api::{ChangeSeq, CommitId, NamespaceId};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;

pub(crate) struct PublishMetadataView<'a, S: ObjectStore + ?Sized> {
    pub(super) head: NamespaceReadState,
    pub(super) acquired_writer: AcquiredWriter,
    manifest_segments: VerifiedMetadataSegments<'a, S>,
    tail_state: Arc<ProjectedWalTail>,
    /// The WAL tail length when it has reached the write-stop bound, so the
    /// tail this publish would extend stays inside it. New commits are refused
    /// with that count; a commit id the namespace already knows is still
    /// answered from its receipt.
    write_stop: Option<u64>,
}

impl<S: ObjectStore + ?Sized> PublishMetadataView<'_, S> {
    pub(crate) fn metadata_view(&self) -> MetadataView<'_, '_, S> {
        MetadataView::from_loaded_head(&self.head, &self.manifest_segments, &self.tail_state.rows)
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
    ) -> Result<Option<Commit>> {
        super::changes::find_committed_change_at(
            &self.metadata_view(),
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
pub(crate) struct PublishTailProjection {
    basis: MetadataBasis,
    pub(crate) head: NamespaceReadState,
    pub(crate) wal_tail_segments: u64,
    pub(crate) tail_state: Arc<ProjectedWalTail>,
}

impl PublishTailProjection {
    pub(crate) fn weight(&self) -> PublishTailWeight {
        PublishTailWeight {
            rows: self.tail_state.rows.row_count(),
            decoded_bytes: self.tail_state.decoded_bytes(),
        }
    }

    pub(crate) fn within_limits(&self, options: &PublishTailOptions) -> bool {
        let weight = self.weight();
        weight.rows <= options.max_tail_rows
            && weight.decoded_bytes <= options.max_tail_decoded_bytes
    }

    pub(crate) fn basis(&self) -> &MetadataBasis {
        &self.basis
    }

    pub(crate) fn reanchor(&mut self, head: NamespaceReadState) {
        self.head = head;
    }
}

pub(crate) async fn load_publish_metadata_view<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    segment_cache: Option<&'a MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    acquired_writer: AcquiredWriter,
    cached_projection: Option<&PublishTailProjection>,
) -> Result<(PublishMetadataView<'a, S>, PublishTailProjection)> {
    let (head, basis) = if let Some(cached) = cached_projection {
        (cached.head.clone(), cached.basis().clone())
    } else {
        let anchor = load_read_anchor(store, namespace_id)
            .await
            .map_err(CoreError::ControlObjectLoad)?;
        let basis = anchor.basis();
        (anchor.read_state, basis)
    };
    ensure_writer_not_fenced(&head, &acquired_writer)?;
    if head.status.is_deleted() {
        return Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            },
        ));
    }
    let loaded_basis = load_basis_metadata_segments(store, segment_cache, &basis).await?;
    let projection = match cached_projection {
        Some(cached) => cached.clone(),
        None => load_publish_tail_projection(store, &head, basis, &loaded_basis).await?,
    };

    let manifest_segments = loaded_basis.segments;
    let tail_state = Arc::clone(&projection.tail_state);

    Ok((
        PublishMetadataView {
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
    basis: MetadataBasis,
    loaded_basis: &LoadedMetadataBasis<'_, S>,
) -> Result<PublishTailProjection> {
    let manifest_head = loaded_basis.replay_head(head);
    let replayed = load_replayed_wal_tail(store, &manifest_head, head, &loaded_basis.base_state)
        .await
        .map_err(CoreError::MetadataProjection)?;
    let wal_tail_segments = head.unfolded_wal_segments();
    let projection = PublishTailProjection {
        basis,
        head: head.clone(),
        wal_tail_segments,
        tail_state: Arc::new(replayed.projected_tail),
    };
    Ok(projection)
}
