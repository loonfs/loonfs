//! Publish-time metadata and the numbered WAL tip retained between batches.

use crate::error::Result;
use crate::limits::{MAX_UNFOLDED_WAL_BYTES, MAX_UNFOLDED_WAL_OBJECTS};
use crate::manifest::move_read_basis;
use crate::manifest::LoadedMetadataBasis;
use crate::metadata::{CommitReceiptRecord, MetadataView};
use crate::namespace::basis::ReadBasis;
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::{Checked, Held, HeldManifestForm};
use crate::namespace::read_anchor::WriterAnchor;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::namespace::writer_epoch::AcquiredWriter;
use crate::segment::{KeptBy, VerifiedMetadataSegments};
use crate::storage::tail_content::ContentWrites;
use crate::time::Observation;
use crate::wal::{
    load_tail_after_head, project_validated_wal_tail, ProjectedWalTail, TailLoadOutcome,
};
use crate::Permits;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{CommitId, CommitSeq};
use std::sync::Arc;

pub(crate) struct PublishMetadataView<'a, S: ObjectStore + ?Sized> {
    pub(super) acquired_writer: AcquiredWriter,
    /// Where each content write that the batch's planning sends takes its
    /// permit.
    content_write_permits: &'a Permits,
    /// Whether this load read the WAL tail from the store rather than
    /// starting from a retained position.
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
    /// The tail has at least `limit` inline content bytes, the namespace's
    /// inline tail limit.
    InlineBytes { inline_bytes: usize, limit: usize },
}

impl WriteStop {
    /// Returns the write stop that a tail of `objects` WAL objects,
    /// `wal_bytes` unfolded WAL bytes, and `inline_bytes` inline content
    /// bytes has reached, or `None` when it has reached none. `inline_limit`
    /// is the namespace's inline tail limit.
    fn reached(
        objects: u64,
        wal_bytes: usize,
        inline_bytes: usize,
        inline_limit: usize,
    ) -> Option<Self> {
        if objects >= MAX_UNFOLDED_WAL_OBJECTS {
            Some(Self::Objects(objects))
        } else if wal_bytes >= MAX_UNFOLDED_WAL_BYTES {
            Some(Self::Bytes(wal_bytes))
        } else if inline_bytes >= inline_limit {
            Some(Self::InlineBytes {
                inline_bytes,
                limit: inline_limit,
            })
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
            Self::InlineBytes {
                inline_bytes,
                limit,
            } => format!(
                "WAL tail has {inline_bytes} inline content bytes, at the write stop of {limit} inline content bytes; publishes resume once maintenance brings it back under {limit} inline content bytes"
            ),
        }
    }
}

impl<S: ObjectStore + ?Sized> PublishMetadataView<'_, S> {
    pub(super) fn head(&self) -> &NamespaceReadState {
        self.position.basis.head()
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

    /// Returns the retention floor of the manifest that the view replays its
    /// WAL tail above.
    pub(super) fn manifest_retention_floor_seq(&self) -> CommitSeq {
        self.manifest_segments
            .manifest()
            .payload()
            .retention_floor_seq
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
    /// The head and the manifest it was read above, with the last check of
    /// that manifest: the discovery that loaded it, a HEAD request that found
    /// no successor, or a publication of this runtime.
    pub(crate) basis: Held<ReadBasis>,
    /// The projected WAL tail above the basis at the head. Reads from the
    /// position start from this tail without replaying the WAL.
    pub(crate) tail: Arc<ProjectedWalTail>,
}

impl WriterPosition {
    /// Returns the head and the manifest it was read above, with the last
    /// check of that manifest.
    pub fn basis(&self) -> &Held<ReadBasis> {
        &self.basis
    }

    /// Returns the head and the manifest it was read above.
    pub fn read_basis(&self) -> &ReadBasis {
        self.basis.read_basis()
    }

    /// Returns the projected WAL tail above the basis at the head.
    pub fn tail(&self) -> &Arc<ProjectedWalTail> {
        &self.tail
    }

    /// Merges `check`, a holder of a manifest of the same namespace, into the
    /// position's basis (see [`Held::merge`]), and returns whether the
    /// position changed.
    pub fn merge_check<M: HeldManifestForm>(&mut self, check: &Held<M>) -> bool {
        self.basis.merge(check)
    }

    /// Returns the position's retention floor, which the checks of a restore
    /// and the merge floor rule read: the basis manifest's floor, raised by the
    /// floor objects in the position's WAL tail.
    pub fn retention_floor_seq(&self) -> CommitSeq {
        self.read_basis().head.retention_floor_seq
    }

    /// Returns the number of unfolded WAL objects at the position's head,
    /// including fences.
    pub fn wal_tail_objects(&self) -> u64 {
        self.read_basis().head.unfolded_wal_objects()
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
    /// [`move_read_basis`], which drops the position at a manifest that
    /// folds past the position's tip. Either way the position merges
    /// `manifest`'s check (see [`Held::merge`]).
    pub(crate) fn move_to(&mut self, manifest: &Held<LoadedManifest>) -> bool {
        move_read_basis(&mut self.basis, &mut self.tail, manifest)
    }

    /// Extends the position with the WAL objects from the number after its
    /// tip, which the session's put found taken, up to the first missing
    /// number (see [`load_tail_after_head`]), and returns `false` when the
    /// session's next unit loads its view by discovery instead, so the engine
    /// drops the position (format spec, "Publication and group commit").
    ///
    /// The position keeps its manifest, and that manifest still doesn't fold
    /// past the position's tip, because the tip only moves up. The read needs
    /// no check of the manifest: a put at the next number after the read is
    /// safe however old the manifest is, and an object that the read finds
    /// can't be at a number that a collection emptied, because only this
    /// session commits above its own tip until a newer writer's fence, whose
    /// epoch ends the read. A number that the read finds empty, a gap, an
    /// object of another writer epoch, and an error all return `false`, so
    /// the next unit's discovery fences the session or returns the error.
    pub(crate) async fn load_after_collision<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
    ) -> bool {
        let Ok(TailLoadOutcome::Loaded(loaded)) =
            load_tail_after_head(store, self.basis.head()).await
        else {
            return false;
        };
        if loaded.tail.objects().is_empty() {
            return false;
        }
        let tail = Arc::unwrap_or_clone(Arc::clone(&self.tail));
        let Ok(replayed) = project_validated_wal_tail(self.basis.head(), tail, &loaded.tail) else {
            return false;
        };
        self.basis.read_basis_mut().head = replayed.resulting_head;
        self.tail = Arc::new(replayed.projected_tail);
        true
    }

    /// Checks whether the position's manifest is still the newest, and
    /// returns where the position goes (format spec, "Warm reads from a
    /// local writer position").
    ///
    /// The check sends a HEAD request for the manifest after the position's.
    /// When that manifest exists, the check loads each manifest after the
    /// position's while each is an active manifest of the position's writer
    /// epoch (a fold, a compaction, or a snapshot, by this runtime or by
    /// another process), up to [`MAX_POSITION_MOVES`] of them, and the
    /// position moves onto the newest of them. Otherwise the check reads the
    /// manifest boundary. Either way, a manifest at or below the boundary
    /// drops the position, because a collection may have deleted the
    /// manifests after it, and so does a manifest of another writer epoch, a
    /// tombstone, or a walk past its cap. `observed` is an observation that
    /// the caller read before the call, and it's the check time of what the
    /// call returns.
    ///
    /// [`MAX_POSITION_MOVES`]: crate::limits::MAX_POSITION_MOVES
    pub async fn check<S: ObjectStore>(
        &self,
        namespace: &NamespaceStore<S>,
        observed: Observation,
    ) -> Result<PositionCheck> {
        let mut checked = self.basis.clone();
        let newest = match namespace
            .check_newest(&mut checked, observed.clone())
            .await?
        {
            Checked::Newest => return Ok(PositionCheck::Newest(Box::new(checked))),
            Checked::Successor => {
                namespace
                    .same_epoch_manifests_above(&checked, observed)
                    .await?
            }
            Checked::AtOrBelowBoundary => None,
        };
        Ok(newest.map_or(PositionCheck::Drop, PositionCheck::MoveTo))
    }
}

/// Where a writer position goes after its check (see
/// [`WriterPosition::check`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PositionCheck {
    /// The position's manifest is still the newest. The holder is the
    /// position's basis with this check, which the position merges (see
    /// [`WriterPosition::merge_check`]).
    Newest(Box<Held<ReadBasis>>),
    /// The position moves onto this manifest, the newest of the manifests
    /// of its writer epoch after its own, with the check as that manifest's
    /// check. When that manifest folds past the position's tip, the move
    /// drops the position instead.
    MoveTo(Held<LoadedManifest>),
    /// The position is dropped. The check returns this when the position's
    /// manifest is at or below the manifest boundary, when the manifests
    /// after it include one that the position can't move onto (a manifest
    /// of another writer epoch, a tombstone, or a manifest that is missing
    /// when the walk loads it), when the newest of them is at or below the
    /// boundary, or when the walk passes its cap. The next use of the
    /// namespace then loads it by discovery.
    Drop,
}

enum ViewSource<'a, S: ObjectStore + ?Sized> {
    Cached(Box<WriterPosition>, Box<VerifiedMetadataSegments<'a, S>>),
    Cold(Box<WriterAnchor>),
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Loads the view that a batch publishes from. The view starts from
    /// `acquired_anchor` when the caller passes one, and otherwise from
    /// `cached_position`, unless the position's manifest is missing from the
    /// store and at or below the manifest boundary (see
    /// `load_held_manifest`). Without either, the view starts from the
    /// namespace's current state in the store, found by a discovery that
    /// began after `attempt`, an observation that the caller read before the
    /// call.
    pub(crate) async fn load_publish_metadata_view(
        &self,
        acquired_writer: AcquiredWriter,
        cached_position: Option<WriterPosition>,
        acquired_anchor: Option<WriterAnchor>,
        attempt: Observation,
    ) -> Result<PublishMetadataView<'_, S>> {
        let cached = match cached_position.filter(|_| acquired_anchor.is_none()) {
            Some(cached) => self
                .load_held_manifest(
                    cached.read_basis().basis.manifest(),
                    cached.basis.boundary().clone(),
                )
                .await?
                .map(|decoded| {
                    let segments = self.segments(decoded, self.keeper(KeptBy::Current));
                    ViewSource::Cached(Box::new(cached), Box::new(segments.segments))
                }),
            None => None,
        };
        let source = match (cached, acquired_anchor) {
            (Some(cached), _) => cached,
            (None, Some(anchor)) => ViewSource::Cold(Box::new(anchor)),
            (None, None) => ViewSource::Cold(Box::new(
                self.load_writer_read_anchor(&acquired_writer, attempt)
                    .await?,
            )),
        };
        let tail_discovered = matches!(source, ViewSource::Cold(_));
        let (position, manifest_segments) = match source {
            ViewSource::Cached(cached, segments) => (*cached, *segments),
            ViewSource::Cold(anchor) => {
                let loaded_basis = self.current_segments(anchor.current_manifest());
                let position = load_writer_position(&anchor, &loaded_basis)?;
                (position, loaded_basis.segments)
            }
        };
        let write_stop = WriteStop::reached(
            position.wal_tail_objects(),
            position.wal_tail_bytes(),
            position.wal_tail_inline_bytes(),
            self.inline_tail_limit_bytes,
        );
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

/// Returns the position at `anchor`'s head above the anchor's manifest,
/// with the check that confirmed the manifest when the anchor loaded it.
fn load_writer_position<S: ObjectStore + ?Sized>(
    anchor: &WriterAnchor,
    loaded_basis: &LoadedMetadataBasis<'_, S>,
) -> Result<WriterPosition> {
    Ok(WriterPosition {
        basis: anchor.held_basis(),
        tail: loaded_basis.project_tail(anchor)?,
    })
}

#[cfg(test)]
mod tests {
    use super::WriteStop;
    use crate::limits::{MAX_UNFOLDED_WAL_BYTES, MAX_UNFOLDED_WAL_OBJECTS};

    const INLINE_LIMIT: usize = 4096;

    #[test]
    fn the_write_stop_names_the_threshold_that_the_tail_reached() {
        assert_eq!(
            WriteStop::reached(
                MAX_UNFOLDED_WAL_OBJECTS - 1,
                MAX_UNFOLDED_WAL_BYTES - 1,
                INLINE_LIMIT - 1,
                INLINE_LIMIT
            ),
            None
        );
        for (objects, wal_bytes, inline_bytes, threshold) in [
            (
                MAX_UNFOLDED_WAL_OBJECTS,
                0,
                0,
                "at the write stop of 1024 objects",
            ),
            (
                MAX_UNFOLDED_WAL_OBJECTS + 1,
                MAX_UNFOLDED_WAL_BYTES,
                INLINE_LIMIT,
                "at the write stop of 1024 objects",
            ),
            (
                1,
                MAX_UNFOLDED_WAL_BYTES,
                INLINE_LIMIT,
                "at the write stop of 64 MiB",
            ),
            (
                1,
                1,
                INLINE_LIMIT,
                "at the write stop of 4096 inline content bytes",
            ),
        ] {
            let reason = WriteStop::reached(objects, wal_bytes, inline_bytes, INLINE_LIMIT)
                .expect("the tail is at a write stop")
                .reason();
            assert!(reason.contains(threshold), "{reason}");
        }
    }
}
