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
use crate::wal::ProjectedWalTail;
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

/// A position is the position that a publication from it builds on, as a
/// [`LaneTurn`](crate::publish::LaneTurn) is, so a runtime can start a
/// publication from either.
impl AsRef<WriterPosition> for WriterPosition {
    fn as_ref(&self) -> &WriterPosition {
        self
    }
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

    /// Returns the earliest `committed_at_ms` of the commits in the position's
    /// WAL tail, or `None` when the tail has no commit, such as a tail of
    /// fences alone.
    pub fn wal_tail_oldest_commit_at_ms(&self) -> Option<u64> {
        self.tail.oldest_commit_at_ms(self.read_basis().head.seq)
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

    /// Checks whether the position's manifest is still the newest (format
    /// spec, "Warm reads from a local writer position").
    ///
    /// The check sends a HEAD request for the manifest after the position's,
    /// and when that manifest is missing, it reads the manifest boundary.
    /// The check moves nothing, because only the session's queues change its
    /// position. A manifest after the position's is the session's own
    /// publication between its put and its record, or another writer's
    /// claim, and the session settles its position after a check that finds
    /// one.
    /// `observed` is an observation that the caller read before the call,
    /// and it's the check time of a position that the check confirms.
    pub async fn check<S: ObjectStore>(
        &self,
        namespace: &NamespaceStore<S>,
        observed: Observation,
    ) -> Result<PositionCheck> {
        let mut checked = self.basis.clone();
        Ok(
            match namespace.check_newest(&mut checked, observed).await? {
                Checked::Newest => PositionCheck::Newest(Box::new(checked)),
                Checked::Successor | Checked::AtOrBelowBoundary => PositionCheck::Replaced,
            },
        )
    }
}

/// What the check of a writer position found (see
/// [`WriterPosition::check`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PositionCheck {
    /// The position's manifest is still the newest. The holder is the
    /// position's basis with this check, which the position merges (see
    /// [`WriterPosition::merge_check`]).
    Newest(Box<Held<ReadBasis>>),
    /// A newer manifest replaced the position's manifest. The manifest after
    /// it exists, or the position's manifest is at or below the manifest
    /// boundary. A runtime then asks the session to settle its position.
    Replaced,
}

/// Where the view of one publication unit starts.
#[derive(Debug)]
pub(crate) enum ViewStart {
    /// The session's position, once the store shows that its manifest is
    /// still there, and otherwise the writer's load.
    Position(Box<WriterPosition>),
    /// The discovery of the session's claim.
    Anchor(Box<WriterAnchor>),
    /// The writer's load: the namespace's current state in the store.
    Load,
}

enum ViewSource<'a, S: ObjectStore + ?Sized> {
    Cached(Box<WriterPosition>, Box<VerifiedMetadataSegments<'a, S>>),
    Cold(Box<WriterAnchor>),
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Loads the view that a batch publishes from, from `start`. A position
    /// whose manifest is missing from the store and at or below the manifest
    /// boundary (see `load_held_manifest`) starts from the writer's load
    /// instead, as a session without a position does, from the namespace's
    /// current state in the store, found by a discovery that began after
    /// `attempt`, an observation that the caller read before the call.
    pub(crate) async fn load_publish_metadata_view(
        &self,
        acquired_writer: AcquiredWriter,
        start: ViewStart,
        attempt: Observation,
    ) -> Result<PublishMetadataView<'_, S>> {
        let source = match start {
            ViewStart::Position(cached) => self
                .load_held_manifest(
                    cached.read_basis().basis.manifest(),
                    cached.basis.boundary().clone(),
                )
                .await?
                .map(|decoded| {
                    let segments = self.segments(decoded, self.keeper(KeptBy::Current));
                    ViewSource::Cached(cached, Box::new(segments.segments))
                }),
            ViewStart::Anchor(anchor) => Some(ViewSource::Cold(anchor)),
            ViewStart::Load => None,
        };
        let source = match source {
            Some(source) => source,
            None => ViewSource::Cold(Box::new(
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
pub(crate) fn load_writer_position<S: ObjectStore + ?Sized>(
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
