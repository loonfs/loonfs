//! Publish-time metadata and the numbered WAL tip retained between batches.

use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::limits::MAX_UNFOLDED_WAL_OBJECTS;
use crate::manifest::same_head;
use crate::manifest::{
    load_basis_metadata_segments, metadata_basis_from_manifest, LoadedMetadataBasis,
    MetadataSegmentCache, PublishedManifest, VerifiedMetadataSegments,
};
use crate::metadata::{CommitReceiptRecord, MetadataView};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::read_anchor::{load_read_anchor, NamespaceReadAnchor};
use crate::namespace::state::NamespaceReadState;
use crate::namespace::writer_epoch::ensure_writer_not_fenced;
use crate::namespace::writer_epoch::AcquiredWriter;
use crate::wal::ProjectedWalTail;
use crate::wal::{replay_discovered_tail, ValidatedWalTail};
use loonfs_objectstore::ObjectStore;
use loonfs_types::{ChangeSeq, CommitId, NamespaceId};
use std::sync::Arc;

pub(crate) struct PublishMetadataView<'a, S: ObjectStore + ?Sized> {
    pub(super) head: NamespaceReadState,
    pub(super) acquired_writer: AcquiredWriter,
    /// The tip observation starts with this load rather than a retained view.
    pub(crate) tail_discovered: bool,
    manifest_segments: VerifiedMetadataSegments<'a, S>,
    position: PublishTailPosition,
    /// The WAL tail length when it has reached the write-stop bound, so the
    /// tail this publish would extend stays inside it. New commits are refused
    /// with that count; a commit id the namespace already knows is still
    /// answered from its receipt.
    write_stop: Option<u64>,
}

impl<S: ObjectStore + ?Sized> PublishMetadataView<'_, S> {
    pub(crate) fn metadata_view(&self) -> MetadataView<'_, '_, S> {
        MetadataView::from_loaded_head(&self.head, &self.manifest_segments, &self.position.tail)
    }

    pub(crate) fn wal_tail(&self) -> &ProjectedWalTail {
        &self.position.tail
    }

    pub(crate) fn into_position(self) -> PublishTailPosition {
        self.position
    }

    /// The retention floor of the manifest that the view plans against.
    pub(crate) fn retention_floor_seq(&self) -> ChangeSeq {
        self.position.retention_floor_seq()
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

/// What a commit engine keeps between publication units: the manifest that
/// the session plans against, the head, and the projected WAL tail above
/// that manifest at the head.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PublishTailPosition {
    basis: MetadataBasis,
    /// The basis manifest's `retention_floor_seq`.
    retention_floor_seq: ChangeSeq,
    pub(crate) head: NamespaceReadState,
    pub(crate) tail: Arc<ProjectedWalTail>,
}

impl PublishTailPosition {
    pub(crate) fn basis(&self) -> &MetadataBasis {
        &self.basis
    }

    pub(crate) fn retention_floor_seq(&self) -> ChangeSeq {
        self.retention_floor_seq
    }

    /// Inline content bytes in the position's WAL tail.
    pub(crate) fn wal_tail_inline_bytes(&self) -> usize {
        self.tail.inline_bytes()
    }

    /// Moves the position onto `manifest`, and returns `false` when the
    /// position can't move there, so the engine drops it (format section
    /// 4.2).
    ///
    /// A manifest at or below the basis's number leaves the position where
    /// it is. A later manifest at the basis's head (see [`same_head`])
    /// keeps the tail. A later fold moves the position when it's active, has
    /// the position's writer epoch, folds a WAL number above the position's
    /// folded number and at most its head's, and its `head_seq` is the last
    /// commit of a chunk of the tail. The tail above the fold's manifest is
    /// then the chunks after its `head_seq`, because a fold folds whole WAL
    /// objects, oldest first. For example, if a fold read the WAL through
    /// the commit at sequence 10, and this session then wrote the commits at
    /// 11 and 12, the tail above the fold's manifest is those two commits. A
    /// fold's manifest lists runs, so the cut also drops the root directory
    /// that replay adds above a manifest without runs. A manifest at the
    /// basis's head keeps that directory, which is why it keeps the tail
    /// instead of cutting it.
    ///
    /// A position that moves takes the manifest's retention floor, which is
    /// how the session finds that a retention advance raised the floor (see
    /// `NamespaceCommitEngine::expiry_due`).
    ///
    /// The rule doesn't depend on whether this runtime or another process
    /// published the manifest, or on the order in which manifests arrive.
    /// Writer epochs never decrease along manifests, so every manifest
    /// between the basis and a later manifest with the position's writer
    /// epoch has that epoch too, and none of them is another writer's claim.
    /// Each of them folds the same WAL that the position's tail holds above
    /// the basis's folded number, so the later manifest's folded WAL number
    /// and `head_seq` are enough to find the tail above it, and the position
    /// can move onto it without the manifests between. A manifest between
    /// the two that arrives later is below the new basis, so it leaves the
    /// position where it is.
    pub(crate) fn move_to(&mut self, manifest: &PublishedManifest) -> bool {
        if manifest.manifest.manifest_no <= self.basis.manifest_no() {
            return true;
        }
        if !same_head(&self.basis, &self.head, manifest) {
            if manifest.status.is_deleted()
                || manifest.writer_epoch != self.head.writer_epoch
                || manifest.folded_wal_no <= self.head.folded_wal_no
                || manifest.folded_wal_no > self.head.wal_no
            {
                return false;
            }
            let Some(after) = self
                .tail
                .after_seq(manifest.manifest.head_seq, self.head.seq)
            else {
                return false;
            };
            self.head.folded_wal_no = manifest.folded_wal_no;
            self.tail = Arc::new(after);
        }
        self.basis = MetadataBasis(manifest.manifest.clone());
        self.retention_floor_seq = manifest.retention_floor_seq;
        true
    }

    pub(crate) fn reanchor(&mut self, head: NamespaceReadState) {
        self.head = head;
    }
}

enum ViewSource {
    Cached(Box<PublishTailPosition>),
    Cold(Box<NamespaceReadAnchor>),
}

pub(crate) async fn load_publish_metadata_view<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    segment_cache: Option<&'a MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    acquired_writer: AcquiredWriter,
    cached_position: Option<PublishTailPosition>,
    acquired_anchor: Option<NamespaceReadAnchor>,
    timer: std::sync::Arc<dyn crate::time::MonotonicTimer>,
) -> Result<PublishMetadataView<'a, S>> {
    let source = if let Some(anchor) = acquired_anchor {
        ViewSource::Cold(Box::new(anchor))
    } else if let Some(cached) = cached_position {
        ViewSource::Cached(Box::new(cached))
    } else {
        let observed = crate::time::Observation::now(timer);
        ViewSource::Cold(Box::new(
            load_read_anchor(store, namespace_id, observed).await?,
        ))
    };
    let (basis, loaded_basis, head) = match &source {
        ViewSource::Cached(cached) => {
            let basis = cached.basis().clone();
            let loaded_basis = load_basis_metadata_segments(store, segment_cache, &basis).await?;
            (basis, loaded_basis, cached.head.clone())
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
    let position = match source {
        ViewSource::Cached(cached) => *cached,
        ViewSource::Cold(anchor) => {
            load_publish_tail_position(&head, basis, &loaded_basis, &anchor.tail)?
        }
    };

    let manifest_segments = loaded_basis.segments;
    let wal_tail_objects = position.head.unfolded_wal_objects();
    let write_stop = (wal_tail_objects >= MAX_UNFOLDED_WAL_OBJECTS).then_some(wal_tail_objects);
    Ok(PublishMetadataView {
        head,
        acquired_writer,
        tail_discovered,
        manifest_segments,
        position,
        write_stop,
    })
}

fn load_publish_tail_position<S: ObjectStore + ?Sized>(
    head: &NamespaceReadState,
    basis: MetadataBasis,
    loaded_basis: &LoadedMetadataBasis<'_, S>,
    tail: &ValidatedWalTail,
) -> Result<PublishTailPosition> {
    let manifest_head = loaded_basis.replay_head(head);
    let replayed = replay_discovered_tail(&manifest_head, &loaded_basis.base_state, tail)
        .map_err(CoreError::MetadataProjection)?;
    Ok(PublishTailPosition {
        basis,
        retention_floor_seq: loaded_basis
            .segments
            .manifest()
            .payload()
            .retention_floor_seq,
        head: head.clone(),
        tail: Arc::new(replayed.projected_tail),
    })
}
