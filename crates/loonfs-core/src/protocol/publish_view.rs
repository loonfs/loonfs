//! Publish-time metadata and the numbered WAL tip retained between batches.

use crate::checkpoint::VerifiedMetadataSegments;
use crate::checkpoint::{
    load_basis_metadata_segments, metadata_basis_from_manifest, LoadedMetadataBasis,
    MetadataSegmentCache,
};
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::limits::MAX_UNFLUSHED_WAL_SEGMENTS;
use crate::metadata::{CommitReceiptRecord, MetadataView};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::control::LoadedManifest;
use crate::namespace::read_anchor::{load_read_anchor, NamespaceReadAnchor};
use crate::namespace::state::NamespaceReadState;
use crate::namespace::writer_epoch::ensure_writer_not_fenced;
use crate::storage::inline_content::InlineContent;
use crate::wal::ProjectedWalTail;
use crate::wal::{replay_discovered_tail, ValidatedWalTail, WalSegmentError};
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
    fold: Option<FoldInProgress>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FoldInProgress {
    /// The head the fold was taken at: its `wal_no` is the boundary the
    /// published manifest will fold through.
    from: NamespaceReadState,
    /// Rows and inline content of every commit published since `from`.
    since: Arc<ProjectedWalTail>,
    segments_since: u64,
}

impl PublishTailProjection {
    pub(crate) fn begin_fold(&mut self) {
        self.fold = Some(FoldInProgress {
            from: self.head.clone(),
            since: Arc::new(ProjectedWalTail::default()),
            segments_since: 0,
        });
    }

    pub(crate) fn reanchor_after_fold(&mut self, basis: MetadataBasis) -> bool {
        if let Some(fold) = self.fold.take() {
            if basis.0.head_seq == fold.from.seq {
                self.basis = basis;
                self.tail_state = fold.since;
                self.wal_tail_segments = fold.segments_since;
                self.head.folded_wal_no = fold.from.wal_no;
                return true;
            }
        }
        false
    }

    pub(crate) fn apply_fold_records(
        &mut self,
        inline_content: &[InlineContent],
        records: &[WalCommitPayload],
    ) -> std::result::Result<(), WalSegmentError> {
        if let Some(fold) = &mut self.fold {
            let since = Arc::make_mut(&mut fold.since);
            for value in inline_content {
                since.insert_inline_content(value.content_ref().clone(), value.bytes().clone());
            }
            for record in records {
                since.apply_commit(record)?;
            }
            fold.segments_since += 1;
        }
        Ok(())
    }

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

enum ViewSource<'p> {
    Cached(&'p PublishTailProjection),
    Cold(Box<NamespaceReadAnchor>),
    Folded(&'p LoadedManifest),
}

pub(crate) async fn load_publish_metadata_view<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    segment_cache: Option<&'a MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    acquired_writer: AcquiredWriter,
    cached_projection: Option<&PublishTailProjection>,
    acquired_anchor: Option<NamespaceReadAnchor>,
    folded_basis: Option<&LoadedManifest>,
) -> Result<(PublishMetadataView<'a, S>, PublishTailProjection)> {
    let source = if let Some(anchor) = acquired_anchor {
        ViewSource::Cold(Box::new(anchor))
    } else if let Some(basis) = folded_basis {
        ViewSource::Folded(basis)
    } else if let Some(cached) = cached_projection {
        ViewSource::Cached(cached)
    } else {
        ViewSource::Cold(Box::new(load_read_anchor(store, namespace_id).await?))
    };
    let basis = match &source {
        ViewSource::Cached(cached) => cached.basis().clone(),
        ViewSource::Cold(anchor) => anchor.basis(),
        ViewSource::Folded(manifest) => MetadataBasis(manifest.state.manifest()),
    };
    let loaded_basis = match &source {
        ViewSource::Cold(anchor) => {
            metadata_basis_from_manifest(store, segment_cache, &anchor.manifest)
        }
        ViewSource::Folded(manifest) => {
            metadata_basis_from_manifest(store, segment_cache, manifest)
        }
        ViewSource::Cached(_) => load_basis_metadata_segments(store, segment_cache, &basis).await?,
    };
    let discovered_tail = match &source {
        ViewSource::Folded(manifest)
            if cached_projection
                .is_none_or(|cached| cached.head.wal_no <= manifest.state.folded_wal_no()) =>
        {
            Some(crate::wal::discover_tail(store, namespace_id, manifest).await?)
        }
        _ => None,
    };
    let tail_discovered = matches!(source, ViewSource::Cold(_)) || discovered_tail.is_some();
    let head = match &source {
        ViewSource::Cached(cached) => cached.head.clone(),
        ViewSource::Cold(anchor) => anchor.read_state.clone(),
        ViewSource::Folded(_) => {
            let mut head = NamespaceReadState::from(loaded_basis.segments.manifest().payload());
            if let Some(discovered) = &discovered_tail {
                head = discovered.head.clone();
            } else if let Some(cached) =
                cached_projection.filter(|cached| cached.head.wal_no > head.wal_no)
            {
                head.seq = cached.head.seq;
                head.wal_no = cached.head.wal_no;
                head.next_inode_id = cached.head.next_inode_id;
            }
            head
        }
    };
    ensure_writer_not_fenced(&head, &acquired_writer)?;
    if head.status.is_deleted() {
        return Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            },
        ));
    }
    let projection = match source {
        ViewSource::Cached(cached) => cached.clone(),
        ViewSource::Cold(anchor) => {
            load_publish_tail_projection(&head, basis, &loaded_basis, &anchor.tail)?
        }
        ViewSource::Folded(_) => {
            if let Some(discovered) = discovered_tail {
                load_publish_tail_projection(&head, basis, &loaded_basis, &discovered.segments)?
            } else {
                let replayed = crate::wal::load_replayed_wal_tail(
                    store,
                    &loaded_basis.replay_head(&head),
                    &head,
                    &loaded_basis.base_state,
                )
                .await
                .map_err(CoreError::MetadataProjection)?;
                PublishTailProjection {
                    basis,
                    head: head.clone(),
                    wal_tail_segments: head.unfolded_wal_segments(),
                    tail_state: Arc::new(replayed.projected_tail),
                    fold: None,
                }
            }
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
            write_stop: (projection.wal_tail_segments >= MAX_UNFLUSHED_WAL_SEGMENTS)
                .then_some(projection.wal_tail_segments),
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
    let replayed = replay_discovered_tail(&manifest_head, head, &loaded_basis.base_state, tail)
        .map_err(CoreError::MetadataProjection)?;
    let wal_tail_segments = head.unfolded_wal_segments();
    let projection = PublishTailProjection {
        basis,
        head: head.clone(),
        wal_tail_segments,
        tail_state: Arc::new(replayed.projected_tail),
        fold: None,
    };
    Ok(projection)
}
