//! Loads a manifest and rechecks its successor around WAL tip discovery.

use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result as CoreResult, WriterFence};
use crate::manifest::{manifest_base_state, project_manifest_tail};
use crate::namespace::basis::{MetadataBasis, ReadBasis};
use crate::namespace::control::{LoadedManifest, NewestManifest};
use crate::namespace::held::{Checked, Held};
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::namespace::writer_epoch::AcquiredWriter;
use crate::time::Observation;
use crate::wal::{
    discover_tail, find_kept_tail, DiscoveredTail, ProjectedWalTail, ValidatedWalTail,
};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::NamespaceStatus;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{CommitSeq, ManifestNo};
use std::cmp::Ordering;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct NamespaceReadAnchor {
    pub(crate) read_state: NamespaceReadState,
    /// The manifest that the anchor read the WAL tail above. Its check time
    /// was read before the anchor loaded the manifest and confirmed it with a
    /// successor check, and its boundary is the one the last check read.
    pub(crate) held: Held<LoadedManifest>,
    pub(crate) tail: ValidatedWalTail,
}

impl NamespaceReadAnchor {
    /// Returns the head that replaying the WAL tail above the anchor's
    /// manifest reached.
    pub fn read_state(&self) -> &NamespaceReadState {
        &self.read_state
    }

    /// Returns the retention floor of the anchor's view: the manifest's
    /// floor, raised by the floor objects in the anchor's WAL tail.
    pub fn retention_floor_seq(&self) -> CommitSeq {
        self.read_state.retention_floor_seq
    }

    pub fn basis(&self) -> MetadataBasis {
        MetadataBasis::new(self.held.manifest().manifest())
    }

    /// Returns the current manifest that the anchor loaded before it read
    /// the WAL tail above that manifest.
    pub fn current_manifest(&self) -> &LoadedManifest {
        self.held.manifest()
    }

    /// Returns the unfolded WAL bytes above the anchor's manifest: the
    /// stored sizes of the WAL objects with commits that the anchor read.
    pub fn wal_tail_bytes(&self) -> usize {
        self.tail.wal_bytes()
    }

    /// Returns the inline content bytes above the anchor's manifest: the
    /// bytes of every piece in the WAL objects that the anchor read.
    pub fn wal_tail_inline_bytes(&self) -> usize {
        self.tail.inline_bytes()
    }

    /// Returns the anchor's manifest with its check time and the manifest
    /// boundary that the anchor's last check read, which a publication can
    /// build on.
    pub fn held(&self) -> &Held<LoadedManifest> {
        &self.held
    }

    /// Returns the anchor's head and the manifest it was read above, kept by
    /// reference with the same check, as a cached head or a writer position
    /// keeps them.
    ///
    /// The head is the one that the anchor's load replayed, and code outside
    /// this crate can't replace that head:
    ///
    /// ```compile_fail
    /// use loonfs_core::control::{NamespaceReadAnchor, NamespaceReadState};
    ///
    /// fn rewind(anchor: &mut NamespaceReadAnchor, head: NamespaceReadState) {
    ///     anchor.read_state = head;
    /// }
    /// ```
    pub fn held_basis(&self) -> Held<ReadBasis> {
        self.held.with_head(self.read_state.clone())
    }
}

/// A read anchor that a writer session loaded under its writer epoch, which
/// a writer position, a fence and a deletion start from.
///
/// Only the writer loaders build one ([`NamespaceStore::load_writer_read_anchor`]
/// and [`NamespaceStore::load_unchecked_writer_anchor`]), so the anchor's
/// manifest has the session's writer epoch, and the namespace is active.
/// Each manifest that the load finds is checked before the WAL tail above it
/// is read (see [`ensure_stored_writer_epoch`]), and the anchor's status is
/// checked after the epoch, so a session whose namespace another writer
/// deleted gets `writer_fenced`. A fence put moves the anchor's tip through
/// the inner anchor, and each retry of the put loads the next anchor with
/// [`NamespaceStore::load_writer_read_anchor`], so the put returns a writer
/// anchor too.
#[derive(Debug, Clone)]
pub(crate) struct WriterAnchor(pub(super) NamespaceReadAnchor);

impl std::ops::Deref for WriterAnchor {
    type Target = NamespaceReadAnchor;

    fn deref(&self) -> &NamespaceReadAnchor {
        &self.0
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    pub async fn load_read_anchor(
        &self,
        observed: crate::time::Observation,
    ) -> CoreResult<NamespaceReadAnchor> {
        let current = self.load_current_basis(observed).await?;
        self.load_read_anchor_from_manifest(current).await
    }

    /// Loads the read anchor that the writer session of `acquired_writer`
    /// publishes from (see [`WriterAnchor`]). A manifest with another writer
    /// epoch fails the load before the WAL tail above that manifest is read.
    pub(crate) async fn load_writer_read_anchor(
        &self,
        acquired_writer: &AcquiredWriter,
        observed: crate::time::Observation,
    ) -> CoreResult<WriterAnchor> {
        let current = self.load_current_basis(observed).await?;
        let anchor = self
            .discover_read_anchor(current, Some(acquired_writer), true)
            .await?;
        writer_anchor(anchor)
    }

    /// Loads the read anchor of a namespace that is not deleted.
    pub async fn load_live_read_anchor(
        &self,
        observed: crate::time::Observation,
    ) -> CoreResult<NamespaceReadAnchor> {
        let anchor = self.load_read_anchor(observed).await?;
        crate::namespace::control::ensure_namespace_live(
            &anchor.read_state.namespace_id,
            anchor.read_state.status,
        )?;
        Ok(anchor)
    }

    /// Returns the read basis of the namespace's newest manifest when that
    /// manifest is initializing, or `None` otherwise, for the administrator
    /// check of a deletion of a namespace that LoonFS otherwise treats as
    /// missing.
    ///
    /// An initializing namespace has no WAL objects, so the basis's head is
    /// the manifest's own.
    ///
    /// `observed` is an observation that the caller read before the call, and
    /// the returned basis is checked at that observation.
    pub async fn find_initializing_read_basis(
        &self,
        observed: Observation,
    ) -> CoreResult<Option<Held<ReadBasis>>> {
        let NewestManifest::Manifest(current) = self.find_newest_manifest(observed).await? else {
            return Ok(None);
        };
        let payload = current.manifest().payload();
        Ok(matches!(payload.status, NamespaceStatus::Initializing {})
            .then(|| current.with_head(NamespaceReadState::from(payload))))
    }

    /// Loads the read anchor at `held`'s manifest, a manifest that the caller
    /// keeps with the check that found it.
    ///
    /// After the WAL tail above the manifest, the call checks the manifest
    /// with [`Self::check_newest`]. A successor, or a manifest at or below
    /// the boundary, makes the call find the newest manifest from the
    /// boundary and continue from it, keeping the WAL objects that it read
    /// above the newest manifest's `folded_wal_no` (format spec, "Resolving
    /// the metadata basis"). A tombstone has no successor, so it isn't
    /// checked.
    pub(crate) async fn load_read_anchor_from_manifest(
        &self,
        held: Held<LoadedManifest>,
    ) -> CoreResult<NamespaceReadAnchor> {
        self.discover_read_anchor(held, None, true).await
    }

    /// Loads the writer anchor at `held`'s manifest, the claim manifest of
    /// `acquired_writer`'s acquisition, as [`Self::load_writer_read_anchor`]
    /// does, but sends no check of the manifest after the WAL tail. An
    /// acquisition reads its fence's tail this way and checks the manifest
    /// after the fence put, so the anchor is never used before that check.
    pub(crate) async fn load_unchecked_writer_anchor(
        &self,
        held: Held<LoadedManifest>,
        acquired_writer: &AcquiredWriter,
    ) -> CoreResult<WriterAnchor> {
        let anchor = self
            .discover_read_anchor(held, Some(acquired_writer), false)
            .await?;
        writer_anchor(anchor)
    }

    /// Loads the anchor at `held`'s manifest, and continues from the newest
    /// manifest when the check after the tail finds a newer one (see
    /// [`Self::load_read_anchor_from_manifest`]).
    ///
    /// A pass that continues reads the tail above the newer manifest only
    /// when the newer manifest folds past what the last pass read, and
    /// otherwise keeps that pass's objects (see [`find_kept_tail`]). A pass
    /// that can't keep them, or a pass after a codec error, reads the whole
    /// tail above its manifest. The passes stop at
    /// [`MAX_CONTENTION_ATTEMPTS`](crate::limits::MAX_CONTENTION_ATTEMPTS),
    /// which bounds a discovery that keeps finding newer manifests.
    ///
    /// With `acquired_writer`, the call checks the writer epoch of each
    /// manifest before it reads the WAL tail above that manifest (see
    /// [`ensure_stored_writer_epoch`]). Replay doesn't change the writer
    /// epoch, so the anchor's head has the epoch of its manifest.
    async fn discover_read_anchor(
        &self,
        mut held: Held<LoadedManifest>,
        acquired_writer: Option<&AcquiredWriter>,
        check_manifest: bool,
    ) -> CoreResult<NamespaceReadAnchor> {
        // The anchor of the last pass, whose check found a newer manifest. The
        // first pass, and a pass after a codec error, has none.
        let mut previous: Option<NamespaceReadAnchor> = None;
        for _attempt in 0..crate::limits::MAX_CONTENTION_ATTEMPTS {
            if let Some(acquired_writer) = acquired_writer {
                ensure_stored_writer_epoch(held.manifest().payload(), acquired_writer)?;
            }
            let kept = previous.take().and_then(|previous| {
                find_kept_tail(held.manifest(), &previous.read_state, &previous.tail)
            });
            let discovered = match kept {
                Some(kept) => Ok(kept),
                None => discover_tail(&self.store, held.manifest()).await,
            };
            match discovered {
                Ok(DiscoveredTail { head: state, tail }) => {
                    let mut anchor = NamespaceReadAnchor {
                        read_state: state,
                        held,
                        tail,
                    };
                    if !check_manifest || anchor.read_state.status.is_deleted() {
                        return Ok(anchor);
                    }
                    // The check completes the discovery that found the
                    // manifest, so it confirms the manifest at the time that
                    // discovery began.
                    let observed = anchor.held.checked().clone();
                    if self.check_newest(&mut anchor.held, observed).await? == Checked::Newest {
                        return Ok(anchor);
                    }
                    // The next pass can keep WAL objects that this pass read
                    // before the newer manifest was loaded. A later check
                    // time would say that the view includes every commit
                    // acknowledged before that time, but a commit written
                    // after the read isn't in the view, so the newer manifest
                    // keeps this check time.
                    held = self
                        .load_current_basis(anchor.held.checked().clone())
                        .await?;
                    previous = Some(anchor);
                }
                Err(error @ ControlObjectLoadError::Codec { .. }) => {
                    let current = self.load_current_basis(held.checked().renew()).await?;
                    if current.manifest().manifest().manifest_no
                        == held.manifest().manifest().manifest_no
                    {
                        return Err(error.into());
                    }
                    held = current;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(CoreError::contention_exhausted(&held.manifest().object_key))
    }

    pub(crate) async fn manifest_has_successor(
        &self,
        manifest_no: ManifestNo,
    ) -> Result<bool, ControlObjectLoadError> {
        let Ok(next) = manifest_no.successor() else {
            return Ok(false);
        };
        self.manifest_exists(next).await
    }
}

/// Returns `anchor` as a writer anchor, or `namespace_deleted` when its head
/// is a tombstone and `namespace_not_found` when it's initializing. The
/// writer loaders check the epoch first, so this check comes after it.
fn writer_anchor(anchor: NamespaceReadAnchor) -> CoreResult<WriterAnchor> {
    crate::namespace::control::ensure_namespace_live(
        &anchor.read_state.namespace_id,
        anchor.read_state.status,
    )?;
    Ok(WriterAnchor(anchor))
}

/// Fails when `manifest`, a manifest that a writer session's load read from
/// the store, doesn't have the session's writer epoch. A higher epoch is
/// another writer's claim and returns `writer_fenced` with that writer's
/// writer block, and a lower epoch returns `namespace_corrupt`.
///
/// A lower epoch means a corrupt store, because the session's epoch is in
/// a claim manifest that the session published, and no manifest after a
/// claim has a lower epoch than the claim (format spec, "Writer ownership").
/// The decoding of a manifest refuses an epoch above zero without a writer
/// block, so a higher epoch always has one.
pub(crate) fn ensure_stored_writer_epoch(
    manifest: &NamespaceManifestPayload,
    acquired_writer: &AcquiredWriter,
) -> CoreResult<()> {
    match manifest.writer_epoch.cmp(&acquired_writer.writer_epoch) {
        Ordering::Equal => Ok(()),
        Ordering::Greater => Err(CoreError::WriterFenced(WriterFence {
            fenced_epoch: acquired_writer.writer_epoch,
            active_epoch: manifest.writer_epoch,
            active_writer: manifest
                .writer
                .clone()
                .expect("a decoded manifest with a writer epoch above zero has a writer block"),
        })),
        Ordering::Less => Err(CoreError::NamespaceCorrupt(format!(
            "manifest {} records writer epoch {}, below the writer epoch {} that this \
             session acquired",
            manifest.manifest_no, manifest.writer_epoch, acquired_writer.writer_epoch
        ))),
    }
}

/// Returns the projected WAL tail of `anchor` above the anchor's manifest.
pub fn project_anchor_tail(anchor: &NamespaceReadAnchor) -> CoreResult<Arc<ProjectedWalTail>> {
    let manifest = anchor.held.manifest().envelope();
    project_manifest_tail(manifest, &manifest_base_state(manifest), anchor)
}

#[cfg(test)]
mod tests;
