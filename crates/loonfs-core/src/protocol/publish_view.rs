//! Publish-time metadata and the numbered WAL tip retained between batches.

use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::limits::MAX_UNFOLDED_WAL_OBJECTS;
use crate::manifest::{
    load_basis_metadata_segments, metadata_basis_from_manifest, FoldedWalTail, LoadedMetadataBasis,
    MetadataSegmentCache, PublishedManifest, VerifiedMetadataSegments,
};
use crate::metadata::{CommitReceiptRecord, MetadataView};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::read_anchor::{load_read_anchor, NamespaceReadAnchor};
use crate::namespace::state::NamespaceReadState;
use crate::namespace::writer_epoch::ensure_writer_not_fenced;
use crate::namespace::writer_epoch::AcquiredWriter;
use crate::wal::ProjectedWalTail;
use crate::wal::{replay_discovered_tail, TailChunk, ValidatedWalTail, WalObjectError};
use loonfs_objectstore::ObjectStore;
use loonfs_types::{CommitId, NamespaceId};
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
    pub(crate) head: NamespaceReadState,
    pub(crate) tail: Arc<ProjectedWalTail>,
    pub(crate) wal_tail_objects: u64,
    fold: Option<FoldInProgress>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FoldInProgress {
    /// The head the fold was taken at: its `wal_no` is the boundary the
    /// published manifest will fold through.
    from: NamespaceReadState,
    /// Rows and content pieces of every commit published since `from`.
    since: ProjectedWalTail,
    wal_objects_since: u64,
}

/// How a position changed for a manifest that this runtime published.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReanchorOutcome {
    /// The position already uses this manifest or a newer one.
    Kept,
    /// The position moved to the manifest, with the tail above it.
    Moved,
    /// The position can't move to the manifest, so the engine drops it.
    Superseded,
}

impl PublishTailPosition {
    pub(crate) fn basis(&self) -> &MetadataBasis {
        &self.basis
    }

    /// Inline content bytes in the position's WAL tail.
    pub(crate) fn wal_tail_inline_bytes(&self) -> usize {
        self.tail.inline_bytes()
    }

    pub(crate) fn begin_fold(&mut self) {
        self.fold = Some(FoldInProgress {
            from: self.head.clone(),
            since: ProjectedWalTail::default(),
            wal_objects_since: 0,
        });
    }

    /// Ends the fold in progress without moving the position.
    pub(crate) fn end_fold(&mut self) {
        self.fold = None;
    }

    /// Moves the position onto the manifest that covers a fold's tail (the
    /// one the fold published, or the current one it found), with the
    /// commits published since the fold began as its tail, and returns
    /// whether the position moved. It doesn't move when the manifest isn't
    /// the fold of this position's tail.
    ///
    /// The manifest has to fold exactly the head the fold started from: the
    /// same sequence and the same WAL number, under the position's writer
    /// epoch. A new writer's claim changes the epoch, and its fence moves
    /// the WAL number without moving the sequence, so a fold over a claim
    /// never moves the position. A manifest that the fold skipped while it
    /// retried (a compaction, a compactor claim, or a retention advance)
    /// keeps the epoch and the head, so the position still moves past it.
    pub(crate) fn reanchor_after_fold(&mut self, folded: &FoldedWalTail) -> bool {
        let Some(fold) = self.fold.take() else {
            return false;
        };
        if folded.writer_epoch != self.head.writer_epoch
            || folded.basis.0.head_seq != fold.from.seq
            || folded.folded_wal_no != fold.from.wal_no
        {
            return false;
        }
        self.basis = folded.basis.clone();
        self.wal_tail_objects = fold.wal_objects_since;
        self.head.folded_wal_no = fold.from.wal_no;
        self.tail = Arc::new(fold.since);
        true
    }

    /// Whether a fold of this position's tail is in progress.
    pub(crate) fn is_folding(&self) -> bool {
        self.fold.is_some()
    }

    /// Moves the position to a manifest that this runtime published.
    ///
    /// The position moves only to the successor of its manifest. A later
    /// manifest supersedes the position, because a manifest between the two
    /// (for example, another writer's claim) could have changed the head
    /// that the tail starts from. The tail above the new manifest depends on
    /// the kind of manifest:
    ///
    /// - A manifest at its predecessor's head (a compaction, a compactor
    ///   claim, or a retention advance) has the same tail. It copies every
    ///   field that `NamespaceReadState::from` reads (such as `head_seq`,
    ///   `folded_wal_no`, `next_inode_id`, and `writer_epoch`), so replay
    ///   starts from the same head on either manifest. Both manifests list
    ///   runs, so replay starts from empty base rows on either manifest. (A
    ///   compaction needs runs to merge, and it keeps every live row,
    ///   including the root directory's.)
    /// - A fold's manifest has the commits after the fold as its tail. A
    ///   fold folds whole WAL objects, oldest first, so those commits are
    ///   the chunks after the manifest's `head_seq`. For example, if a fold
    ///   read the WAL through the commit at sequence 10, and this runtime
    ///   then wrote the commits at 11 and 12, the tail above the fold's
    ///   manifest is those two commits. The fold's manifest lists runs,
    ///   because the fold folded at least one commit.
    pub(crate) fn reanchor_after_publish(
        &mut self,
        published: &PublishedManifest,
    ) -> ReanchorOutcome {
        let manifest = published.manifest();
        let basis_no = self.basis.manifest_no();
        if manifest.manifest_no <= basis_no {
            return ReanchorOutcome::Kept;
        }
        assert!(
            self.fold.is_none(),
            "the engine should keep a manifest published during a fold until the fold finishes"
        );
        let succeeds = basis_no
            .successor()
            .is_ok_and(|next| next == manifest.manifest_no);
        if !succeeds {
            return ReanchorOutcome::Superseded;
        }
        let basis_head_seq = self.basis.0.head_seq;
        match published {
            PublishedManifest::SameHead(_) if manifest.head_seq == basis_head_seq => {}
            PublishedManifest::Fold { folded_wal_no, .. }
                if self.head.folded_wal_no < *folded_wal_no
                    && *folded_wal_no <= self.head.wal_no
                    && basis_head_seq < manifest.head_seq =>
            {
                let Some(after) = self.tail.after_seq(manifest.head_seq, self.head.seq) else {
                    return ReanchorOutcome::Superseded;
                };
                self.head.folded_wal_no = *folded_wal_no;
                self.wal_tail_objects = self.head.wal_no.0 - folded_wal_no.0;
                self.tail = Arc::new(after);
            }
            _ => return ReanchorOutcome::Superseded,
        }
        self.basis = MetadataBasis(manifest.clone());
        ReanchorOutcome::Moved
    }

    pub(crate) fn apply_fold_object(
        &mut self,
        chunk: Option<Arc<TailChunk>>,
    ) -> std::result::Result<(), WalObjectError> {
        if let Some(fold) = &mut self.fold {
            if let Some(chunk) = chunk {
                fold.since.push_chunk(chunk)?;
            }
            fold.wal_objects_since += 1;
        }
        Ok(())
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
    let write_stop = (position.wal_tail_objects >= MAX_UNFOLDED_WAL_OBJECTS)
        .then_some(position.wal_tail_objects);
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
        head: head.clone(),
        tail: Arc::new(replayed.projected_tail),
        wal_tail_objects: head.unfolded_wal_objects(),
        fold: None,
    })
}
