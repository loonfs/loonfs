//! Publish-time metadata and the numbered WAL tip retained between batches.

use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::limits::MAX_UNFOLDED_WAL_OBJECTS;
use crate::manifest::VerifiedMetadataSegments;
use crate::manifest::{
    load_basis_metadata_segments, metadata_basis_from_manifest, LoadedMetadataBasis,
    MetadataSegmentCache, WalTailProjectionCacheKey,
};
use crate::metadata::{CommitReceiptRecord, MetadataView};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::read_anchor::{load_read_anchor, NamespaceReadAnchor};
use crate::namespace::state::NamespaceReadState;
use crate::namespace::writer_epoch::ensure_writer_not_fenced;
use crate::storage::inline_content::InlineContent;
use crate::wal::ProjectedWalTail;
use crate::wal::{replay_discovered_tail, ValidatedWalTail, WalObjectError};
use loonfs_api::wire::control::AcquiredWriter;
use loonfs_api::wire::wal::WalCommitPayload;
use loonfs_api::{CommitId, NamespaceId};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;

pub(crate) struct PublishMetadataView<'a, S: ObjectStore + ?Sized> {
    pub(super) head: NamespaceReadState,
    pub(super) acquired_writer: AcquiredWriter,
    /// The tip observation starts with this load rather than a retained view.
    pub(crate) tail_discovered: bool,
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
}

/// What a commit engine keeps between publication units. The WAL tail
/// itself lives in the head-state cache, under the key this basis and head
/// name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishTailPosition {
    basis: MetadataBasis,
    pub(crate) head: NamespaceReadState,
    pub(crate) wal_tail_objects: u64,
    pub(crate) wal_tail_inline_bytes: usize,
    fold: Option<FoldInProgress>,
}

/// A position and the tail it names, held for one publication unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishTailProjection {
    pub(crate) position: PublishTailPosition,
    pub(crate) tail_state: Arc<ProjectedWalTail>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FoldInProgress {
    /// The head the fold was taken at: its `wal_no` is the boundary the
    /// published manifest will fold through.
    from: NamespaceReadState,
    /// Rows and inline content of every commit published since `from`.
    since: Arc<ProjectedWalTail>,
    wal_objects_since: u64,
}

impl PublishTailPosition {
    pub(crate) fn tail_key(&self, namespace_id: &NamespaceId) -> WalTailProjectionCacheKey {
        WalTailProjectionCacheKey {
            namespace_id: namespace_id.clone(),
            manifest_no: self.basis.manifest_no(),
            head_seq: self.head.seq,
        }
    }

    pub(crate) fn basis(&self) -> &MetadataBasis {
        &self.basis
    }

    pub(crate) fn begin_fold(&mut self) {
        self.fold = Some(FoldInProgress {
            from: self.head.clone(),
            since: Arc::new(ProjectedWalTail::default()),
            wal_objects_since: 0,
        });
    }

    /// Moves the position onto the manifest a fold published, and returns
    /// the commits published since the fold began: the tail that position
    /// now names.
    pub(crate) fn reanchor_after_fold(
        &mut self,
        basis: MetadataBasis,
    ) -> Option<Arc<ProjectedWalTail>> {
        let fold = self.fold.take()?;
        if basis.0.head_seq != fold.from.seq {
            return None;
        }
        self.basis = basis;
        self.wal_tail_objects = fold.wal_objects_since;
        self.wal_tail_inline_bytes = fold.since.inline_bytes();
        self.head.folded_wal_no = fold.from.wal_no;
        Some(fold.since)
    }
}

impl PublishTailProjection {
    pub(crate) fn apply_fold_records(
        &mut self,
        inline_content: &[InlineContent],
        records: &[WalCommitPayload],
    ) -> std::result::Result<(), WalObjectError> {
        if let Some(fold) = &mut self.position.fold {
            let since = Arc::make_mut(&mut fold.since);
            for value in inline_content {
                since.insert_inline_content(value.content_ref().clone(), value.bytes().clone());
            }
            for record in records {
                since.apply_commit(record)?;
            }
            fold.wal_objects_since += 1;
        }
        Ok(())
    }

    pub(crate) fn reanchor(&mut self, head: NamespaceReadState) {
        self.position.head = head;
    }

    /// Splits the projection into the position the engine keeps and the
    /// tail that position names.
    pub(crate) fn into_position(mut self) -> (PublishTailPosition, Arc<ProjectedWalTail>) {
        self.position.wal_tail_inline_bytes = self.tail_state.inline_bytes();
        (self.position, self.tail_state)
    }
}

enum ViewSource {
    Cached(Box<PublishTailProjection>),
    Cold(Box<NamespaceReadAnchor>),
}

pub(crate) async fn load_publish_metadata_view<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    segment_cache: Option<&'a MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    acquired_writer: AcquiredWriter,
    cached_projection: Option<PublishTailProjection>,
    acquired_anchor: Option<NamespaceReadAnchor>,
) -> Result<(PublishMetadataView<'a, S>, PublishTailProjection)> {
    let source = if let Some(anchor) = acquired_anchor {
        ViewSource::Cold(Box::new(anchor))
    } else if let Some(cached) = cached_projection {
        ViewSource::Cached(Box::new(cached))
    } else {
        ViewSource::Cold(Box::new(load_read_anchor(store, namespace_id).await?))
    };
    let (basis, loaded_basis, head) = match &source {
        ViewSource::Cached(cached) => {
            let basis = cached.position.basis().clone();
            let loaded_basis = load_basis_metadata_segments(store, segment_cache, &basis).await?;
            (basis, loaded_basis, cached.position.head.clone())
        }
        ViewSource::Cold(anchor) => (
            anchor.basis(),
            metadata_basis_from_manifest(store, segment_cache, &anchor.manifest),
            anchor.read_state.clone(),
        ),
    };
    ensure_writer_not_fenced(&head, &acquired_writer)?;
    if head.status.is_deleted() {
        return Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            },
        ));
    }
    let tail_discovered = matches!(source, ViewSource::Cold(_));
    let projection = match source {
        ViewSource::Cached(cached) => *cached,
        ViewSource::Cold(anchor) => {
            load_publish_tail_projection(&head, basis, &loaded_basis, &anchor.tail)?
        }
    };

    let manifest_segments = loaded_basis.segments;
    let tail_state = Arc::clone(&projection.tail_state);

    Ok((
        PublishMetadataView {
            head,
            acquired_writer,
            tail_discovered,
            manifest_segments,
            tail_state,
            write_stop: (projection.position.wal_tail_objects >= MAX_UNFOLDED_WAL_OBJECTS)
                .then_some(projection.position.wal_tail_objects),
        },
        projection,
    ))
}

fn load_publish_tail_projection<S: ObjectStore + ?Sized>(
    head: &NamespaceReadState,
    basis: MetadataBasis,
    loaded_basis: &LoadedMetadataBasis<'_, S>,
    tail: &ValidatedWalTail,
) -> Result<PublishTailProjection> {
    let manifest_head = loaded_basis.replay_head(head);
    let replayed = replay_discovered_tail(&manifest_head, &loaded_basis.base_state, tail)
        .map_err(CoreError::MetadataProjection)?;
    let tail_state = Arc::new(replayed.projected_tail);
    Ok(PublishTailProjection {
        position: PublishTailPosition {
            basis,
            head: head.clone(),
            wal_tail_objects: head.unfolded_wal_objects(),
            wal_tail_inline_bytes: tail_state.inline_bytes(),
            fold: None,
        },
        tail_state,
    })
}
