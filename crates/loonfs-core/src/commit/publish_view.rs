//! Publish-time metadata and the numbered WAL tip retained between batches.

use crate::error::Result;
use crate::limits::{MAX_UNFOLDED_WAL_BYTES, MAX_UNFOLDED_WAL_OBJECTS};
use crate::manifest::move_read_basis;
use crate::manifest::{
    load_basis_metadata_segments, metadata_basis_from_manifest, LoadedMetadataBasis,
    PublishedManifest,
};
use crate::metadata::{CommitReceiptRecord, MetadataView};
use crate::namespace::basis::{MetadataBasis, ReadBasis};
use crate::namespace::control::ensure_namespace_live;
use crate::namespace::read_anchor::NamespaceReadAnchor;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::namespace::writer_epoch::ensure_writer_not_fenced;
use crate::namespace::writer_epoch::AcquiredWriter;
use crate::segment::VerifiedMetadataSegments;
use crate::storage::tail_content::ContentWrites;
use crate::time::Observation;
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
    /// The write stop that the WAL tail has reached, if any. New commits are
    /// refused with its reason; a commit id the namespace already records is
    /// still returned from its receipt.
    write_stop: Option<WriteStop>,
}

/// The write stop that a WAL tail has reached, with the tail's size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WriteStop {
    /// The tail has at least `MAX_UNFOLDED_WAL_OBJECTS` WAL objects.
    Objects(u64),
    /// The tail has at least `MAX_UNFOLDED_WAL_BYTES` unfolded WAL bytes.
    Bytes(usize),
}

impl WriteStop {
    /// Returns the write stop that a tail of `objects` WAL objects and
    /// `wal_bytes` unfolded WAL bytes has reached, or `None` when it has
    /// reached neither.
    fn reached(objects: u64, wal_bytes: usize) -> Option<Self> {
        if objects >= MAX_UNFOLDED_WAL_OBJECTS {
            Some(Self::Objects(objects))
        } else if wal_bytes >= MAX_UNFOLDED_WAL_BYTES {
            Some(Self::Bytes(wal_bytes))
        } else {
            None
        }
    }

    /// Returns the `maintenance_required` reason of a commit refused at
    /// this write stop.
    pub(super) fn reason(self) -> String {
        match self {
            Self::Objects(objects) => format!(
                "WAL tail has {objects} objects, at the write stop of {MAX_UNFOLDED_WAL_OBJECTS} objects; publishes resume once maintenance brings it back under {MAX_UNFOLDED_WAL_OBJECTS} objects"
            ),
            Self::Bytes(wal_bytes) => format!(
                "WAL tail has {wal_bytes} bytes, at the write stop of {} MiB; publishes resume once maintenance brings it back under {} MiB",
                MAX_UNFOLDED_WAL_BYTES / (1024 * 1024),
                MAX_UNFOLDED_WAL_BYTES / (1024 * 1024),
            ),
        }
    }
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
        ContentWrites::new(self.store(), self.content_write_permits, self.head())
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

    pub(super) fn write_stop(&self) -> Option<WriteStop> {
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

    /// Returns the unfolded WAL bytes in the position's WAL tail: the stored
    /// sizes of its WAL objects with commits.
    pub fn wal_tail_bytes(&self) -> usize {
        self.tail.wal_bytes()
    }

    /// Moves the position onto `manifest`, and returns `false` when the
    /// position can't move there, so the engine drops it (format section
    /// 4.2).
    ///
    /// A manifest at or below the basis's number leaves the position where
    /// it is. A later manifest moves the position by the rule of
    /// [`move_read_basis`]. A position that moves takes the manifest's
    /// retention floor, so the session validates its next commits against a
    /// floor that a retention advance raised.
    pub(crate) fn move_to(&mut self, manifest: &PublishedManifest) -> bool {
        if manifest.manifest.manifest_no <= self.read_basis.basis.manifest_no() {
            return true;
        }
        if !move_read_basis(&mut self.read_basis, &mut self.tail, manifest) {
            return false;
        }
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
        ensure_namespace_live(&head.namespace_id, &head.status)?;
        let tail_discovered = matches!(source, ViewSource::Cold(_));
        let position = match source {
            ViewSource::Cached(cached) => *cached,
            ViewSource::Cold(anchor) => load_writer_position(&anchor, basis, &loaded_basis)?,
        };

        let manifest_segments = loaded_basis.segments;
        let write_stop = WriteStop::reached(position.wal_tail_objects(), position.wal_tail_bytes());
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
    let tail = loaded_basis.project_tail(anchor)?;
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
        tail,
        basis_checked: anchor.checked.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::WriteStop;
    use crate::limits::{MAX_UNFOLDED_WAL_BYTES, MAX_UNFOLDED_WAL_OBJECTS};

    #[test]
    fn the_write_stop_names_the_threshold_that_the_tail_reached() {
        assert_eq!(
            WriteStop::reached(MAX_UNFOLDED_WAL_OBJECTS - 1, MAX_UNFOLDED_WAL_BYTES - 1),
            None
        );
        for (objects, wal_bytes, threshold) in [
            (MAX_UNFOLDED_WAL_OBJECTS, 0, "1024 objects"),
            (
                MAX_UNFOLDED_WAL_OBJECTS + 1,
                MAX_UNFOLDED_WAL_BYTES,
                "1024 objects",
            ),
            (1, MAX_UNFOLDED_WAL_BYTES, "64 MiB"),
        ] {
            let reason = WriteStop::reached(objects, wal_bytes)
                .expect("the tail is at a write stop")
                .reason();
            assert!(
                reason.contains(&format!("at the write stop of {threshold}")),
                "{reason}"
            );
        }
    }
}
