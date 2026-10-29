//! Publish-time metadata and the numbered WAL tip retained between batches.

use crate::error::{CoreError, Result};
use crate::limits::MAX_UNFOLDED_WAL_OBJECTS;
use crate::manifest::same_head;
use crate::manifest::{
    load_basis_metadata_segments, metadata_basis_from_manifest, LoadedMetadataBasis,
    PublishedManifest, VerifiedMetadataSegments,
};
use crate::metadata::{CommitReceiptRecord, MetadataView};
use crate::namespace::basis::{MetadataBasis, ReadBasis};
use crate::namespace::control::ensure_namespace_live;
use crate::namespace::read_anchor::NamespaceReadAnchor;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::namespace::writer_epoch::ensure_writer_not_fenced;
use crate::namespace::writer_epoch::AcquiredWriter;
use crate::storage::tail_content::ContentWrites;
use crate::time::Observation;
use crate::wal::replay_discovered_tail;
use crate::wal::ProjectedWalTail;
use crate::PermitPool;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{ChangeSeq, CommitId};
use std::sync::Arc;

pub(crate) struct PublishMetadataView<'a, S: ObjectStore + ?Sized> {
    pub(super) acquired_writer: AcquiredWriter,
    /// Where each content write that the batch's planning sends takes its
    /// permit.
    content_write_permits: &'a PermitPool,
    /// The tip observation starts with this load rather than a retained view.
    pub(crate) tail_discovered: bool,
    manifest_segments: VerifiedMetadataSegments<'a, S>,
    pub(crate) position: WriterPosition,
    /// The WAL tail length when it has reached the write-stop bound, so the
    /// tail this publish would extend stays inside it. New commits are refused
    /// with that count; a commit id the namespace already knows is still
    /// answered from its receipt.
    write_stop: Option<u64>,
}

impl<S: ObjectStore + ?Sized> PublishMetadataView<'_, S> {
    pub(super) fn head(&self) -> &NamespaceReadState {
        &self.position.read_basis.head
    }

    /// Returns the store the view reads from, which a publication writes to.
    pub(super) fn store(&self) -> &S {
        self.manifest_segments.store()
    }

    /// Returns where the content writes that the batch's planning sends go.
    pub(super) fn content_writes(&self) -> ContentWrites<'_, S> {
        ContentWrites::new(self.store(), self.content_write_permits)
    }

    pub(crate) fn metadata_view(&self) -> MetadataView<'_, '_, S> {
        MetadataView::from_loaded_head(self.head(), &self.manifest_segments, &self.position.tail)
    }

    pub(crate) fn wal_tail(&self) -> &ProjectedWalTail {
        &self.position.tail
    }

    pub(crate) fn into_position(self) -> WriterPosition {
        self.position
    }

    /// The retention floor of the manifest that the view plans against.
    pub(crate) fn retention_floor_seq(&self) -> ChangeSeq {
        self.position.retention_floor_seq
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

/// A writer session's position: the head that the session plans its next
/// commit against, the metadata basis that the head was read against, and
/// the projected WAL tail above the basis at the head.
///
/// The commit engine keeps the position between publication units. Reads in
/// the session's runtime start from the position, and a fold of the
/// session's tail folds the position's tail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterPosition {
    pub read_basis: ReadBasis,
    /// The basis manifest's `retention_floor_seq`.
    pub retention_floor_seq: ChangeSeq,
    /// The projected WAL tail above the basis at the head. Reads from the
    /// position start from this tail without replaying the WAL.
    pub tail: Arc<ProjectedWalTail>,
    /// When a store observation last confirmed the basis current: the
    /// discovery that loaded the basis, or a HEAD that found no successor.
    pub basis_checked: Observation,
}

impl WriterPosition {
    /// Returns the number of unfolded WAL objects at the position's head,
    /// including fences.
    pub fn wal_tail_objects(&self) -> u64 {
        self.read_basis.head.unfolded_wal_objects()
    }

    /// Returns the inline content bytes in the position's WAL tail.
    pub fn wal_tail_inline_bytes(&self) -> usize {
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
    /// A position that moves takes the manifest's retention floor, so the
    /// session validates its next commits against a floor that a retention
    /// advance raised, and its next WAL object records that floor.
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
        let ReadBasis { head, basis } = &mut self.read_basis;
        if manifest.manifest.manifest_no <= basis.manifest_no() {
            return true;
        }
        if !same_head(basis, head, manifest) {
            if manifest.status.is_deleted()
                || manifest.writer_epoch != head.writer_epoch
                || manifest.folded_wal_no <= head.folded_wal_no
                || manifest.folded_wal_no > head.wal_no
            {
                return false;
            }
            let Some(after) = self.tail.after_seq(manifest.manifest.head_seq, head.seq) else {
                return false;
            };
            head.folded_wal_no = manifest.folded_wal_no;
            self.tail = Arc::new(after);
        }
        *basis = MetadataBasis::new(manifest.manifest.clone());
        self.retention_floor_seq = manifest.retention_floor_seq;
        true
    }
}

enum ViewSource {
    Cached(Box<WriterPosition>),
    Cold(Box<NamespaceReadAnchor>),
}

impl<S: ObjectStore> NamespaceStore<S> {
    pub(crate) async fn load_publish_metadata_view(
        &self,
        acquired_writer: AcquiredWriter,
        cached_position: Option<WriterPosition>,
        acquired_anchor: Option<NamespaceReadAnchor>,
    ) -> Result<PublishMetadataView<'_, S>> {
        let source = if let Some(anchor) = acquired_anchor {
            ViewSource::Cold(Box::new(anchor))
        } else if let Some(cached) = cached_position {
            ViewSource::Cached(Box::new(cached))
        } else {
            let observed = crate::time::Observation::now(Arc::clone(&self.timer));
            ViewSource::Cold(Box::new(self.load_read_anchor(observed).await?))
        };
        let (basis, loaded_basis, head) = match &source {
            ViewSource::Cached(cached) => {
                let basis = cached.read_basis.basis.clone();
                let loaded_basis =
                    load_basis_metadata_segments(&self.store, Some(&self.segment_cache), &basis)
                        .await?;
                (basis, loaded_basis, &cached.read_basis.head)
            }
            ViewSource::Cold(anchor) => (
                anchor.basis(),
                metadata_basis_from_manifest(
                    &self.store,
                    Some(&self.segment_cache),
                    &anchor.manifest,
                ),
                &anchor.read_state,
            ),
        };
        ensure_writer_not_fenced(head, &acquired_writer)?;
        ensure_namespace_live(&head.namespace_id, head.status)?;
        let tail_discovered = matches!(source, ViewSource::Cold(_));
        let position = match source {
            ViewSource::Cached(cached) => *cached,
            ViewSource::Cold(anchor) => load_writer_position(&anchor, basis, &loaded_basis)?,
        };

        let manifest_segments = loaded_basis.segments;
        let wal_tail_objects = position.wal_tail_objects();
        let write_stop = (wal_tail_objects >= MAX_UNFOLDED_WAL_OBJECTS).then_some(wal_tail_objects);
        Ok(PublishMetadataView {
            acquired_writer,
            content_write_permits: &self.content_writes,
            tail_discovered,
            manifest_segments,
            position,
            write_stop,
        })
    }
}

/// Returns the position at `anchor`'s head above `basis`, the anchor's
/// manifest, which the anchor checked when it loaded the manifest.
fn load_writer_position<S: ObjectStore + ?Sized>(
    anchor: &NamespaceReadAnchor,
    basis: MetadataBasis,
    loaded_basis: &LoadedMetadataBasis<'_, S>,
) -> Result<WriterPosition> {
    let manifest_head = loaded_basis.replay_head(&anchor.read_state);
    let replayed = replay_discovered_tail(&manifest_head, &loaded_basis.base_state, &anchor.tail)
        .map_err(CoreError::MetadataProjection)?;
    Ok(WriterPosition {
        read_basis: ReadBasis {
            head: anchor.read_state.clone(),
            basis,
        },
        retention_floor_seq: loaded_basis
            .segments
            .manifest()
            .payload()
            .retention_floor_seq,
        tail: Arc::new(replayed.projected_tail),
        basis_checked: anchor.checked.clone(),
    })
}
