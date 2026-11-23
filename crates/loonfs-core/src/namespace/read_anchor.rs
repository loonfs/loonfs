//! Loads a manifest and rechecks its successor around WAL tip discovery.

use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result as CoreResult};
use crate::limits::MAX_POSITION_MOVES;
use crate::manifest::metadata_basis_from_manifest;
use crate::namespace::basis::{MetadataBasis, ReadBasis};
use crate::namespace::control::{LoadedManifest, NewestManifest};
use crate::namespace::held::{Checked, Held};
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::namespace::writer_epoch::{ensure_writer_not_fenced, AcquiredWriter};
use crate::segment::MetadataSegmentCache;
use crate::time::Observation;
use crate::wal::{discover_tail, DiscoveredTail, ProjectedWalTail, ValidatedWalTail};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::NamespaceStatus;
use loonfs_types::{CommitSeq, ManifestNo};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct NamespaceReadAnchor {
    pub read_state: NamespaceReadState,
    /// The manifest that the anchor read the WAL tail above. Its check time
    /// was read before the anchor loaded the manifest and confirmed it with a
    /// successor check, and its boundary is the one the last check read.
    pub(crate) held: Held<LoadedManifest>,
    pub(crate) tail: ValidatedWalTail,
}

impl NamespaceReadAnchor {
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

    /// Returns the anchor's manifest with its check time and the manifest
    /// boundary that the anchor's last check read, which a publication can
    /// build on.
    pub fn held(&self) -> &Held<LoadedManifest> {
        &self.held
    }

    /// Returns the anchor's head and the manifest it was read above, kept by
    /// reference with the same check, as a cached head or a writer position
    /// keeps them.
    pub fn held_basis(&self) -> Held<ReadBasis> {
        self.held.with_head(self.read_state.clone())
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    pub async fn load_read_anchor(
        &self,
        observed: crate::time::Observation,
    ) -> CoreResult<NamespaceReadAnchor> {
        let current = self.load_current_basis(observed).await?;
        self.load_read_anchor_from_manifest(current, None).await
    }

    /// Loads the read anchor that the writer session of `acquired_writer`
    /// publishes from. Each manifest that the load finds is checked for the
    /// session's writer epoch before the WAL tail above it is read, so a
    /// fenced session fails with `writer_fenced` without reading the tail.
    pub(crate) async fn load_writer_read_anchor(
        &self,
        acquired_writer: &AcquiredWriter,
        observed: crate::time::Observation,
    ) -> CoreResult<NamespaceReadAnchor> {
        let current = self.load_current_basis(observed).await?;
        self.load_read_anchor_from_manifest(current, Some(acquired_writer))
            .await
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
    /// `observed` is an observation that the caller read before the call.
    pub async fn find_initializing_read_basis(
        &self,
        observed: Observation,
    ) -> CoreResult<Option<ReadBasis>> {
        let NewestManifest::Manifest(current) = self.find_newest_manifest(observed).await? else {
            return Ok(None);
        };
        let manifest = current.manifest();
        Ok(
            matches!(manifest.payload().status, NamespaceStatus::Initializing {}).then(|| {
                ReadBasis {
                    head: NamespaceReadState::from(manifest.payload()),
                    basis: MetadataBasis::new(manifest.manifest()),
                }
            }),
        )
    }

    /// Loads the read anchor at `held`'s manifest, a manifest that the caller
    /// keeps with the check that found it.
    ///
    /// With `acquired_writer`, the call checks the writer epoch of each
    /// manifest before it reads the WAL tail above that manifest, and fails
    /// with `writer_fenced` at a manifest of another epoch. Replay doesn't
    /// change the writer epoch, so the anchor's head has the epoch of its
    /// manifest.
    ///
    /// After the WAL tail above the manifest, the call checks the manifest
    /// with [`Self::check_newest`]. A successor, or a manifest at or
    /// below the boundary, makes the call find the newest manifest from the
    /// boundary and start again (format spec, "Replaying the visible WAL").
    /// A tombstone has no successor, so it isn't checked.
    pub(crate) async fn load_read_anchor_from_manifest(
        &self,
        held: Held<LoadedManifest>,
        acquired_writer: Option<&AcquiredWriter>,
    ) -> CoreResult<NamespaceReadAnchor> {
        self.discover_read_anchor(held, acquired_writer, true).await
    }

    /// Loads the anchor at `manifest` as
    /// [`Self::load_read_anchor_from_manifest`] does, but sends no check of
    /// the manifest after the WAL tail. A writer acquisition reads its fence's
    /// tail this way and checks the manifest after the fence put, so the
    /// anchor is never used before that check.
    pub(crate) async fn load_unchecked_read_anchor(
        &self,
        held: Held<LoadedManifest>,
        acquired_writer: Option<&AcquiredWriter>,
    ) -> CoreResult<NamespaceReadAnchor> {
        self.discover_read_anchor(held, acquired_writer, false)
            .await
    }

    async fn discover_read_anchor(
        &self,
        mut held: Held<LoadedManifest>,
        acquired_writer: Option<&AcquiredWriter>,
        check_manifest: bool,
    ) -> CoreResult<NamespaceReadAnchor> {
        for _attempt in 0..crate::limits::MAX_CONTENTION_ATTEMPTS {
            if let Some(acquired_writer) = acquired_writer {
                let payload = held.manifest().payload();
                ensure_writer_not_fenced(
                    payload.writer_epoch,
                    payload.writer.as_ref(),
                    acquired_writer,
                )?;
            }
            match discover_tail(&self.store, held.manifest()).await {
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
                    held = self
                        .load_current_basis(anchor.held.checked().renew())
                        .await?;
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

    /// Returns the newest manifest above `held`, a writer position's basis,
    /// for the position to move onto, when every manifest above `held` is an
    /// active manifest of the position's writer epoch, checked at `observed`,
    /// an observation that the caller read before the HEAD request that found
    /// a successor of `held`.
    ///
    /// Manifest numbers are consecutive, so the walk loads each successor
    /// until one is missing, and then reads the manifest boundary (see
    /// [`Self::above_boundary`]). It returns `None` when `held` has no
    /// successor, when it finds a manifest of another writer epoch (another
    /// writer's claim) or a tombstone, which settles that the position can't
    /// move, when it finds more than [`MAX_POSITION_MOVES`] manifests, and
    /// when the newest manifest is at or below the boundary, because a
    /// collection may have deleted the manifests after it.
    pub async fn same_epoch_manifests_above(
        &self,
        held: &Held<ReadBasis>,
        observed: Observation,
    ) -> CoreResult<Option<Held<LoadedManifest>>> {
        let writer_epoch = held.head().writer_epoch;
        let mut newest = None;
        let mut moves = 0;
        let mut manifest_no = held.manifest_no();
        while moves <= MAX_POSITION_MOVES {
            let Ok(next) = manifest_no.successor() else {
                break;
            };
            let Some(loaded) = self.load_manifest_by_no(next).await? else {
                let Some(newest) = newest else {
                    return Ok(None);
                };
                let (above, boundary) = self
                    .above_boundary(manifest_no, held.boundary().clone())
                    .await?;
                return Ok(above.then(|| Held::new(newest, observed, boundary)));
            };
            let payload = loaded.payload();
            if payload.writer_epoch != writer_epoch || payload.status.is_deleted() {
                break;
            }
            newest = Some(loaded);
            moves += 1;
            manifest_no = next;
        }
        Ok(None)
    }
}

pub fn project_anchor_tail<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: &MetadataSegmentCache,
    anchor: &NamespaceReadAnchor,
) -> CoreResult<Arc<ProjectedWalTail>> {
    metadata_basis_from_manifest(store, Some(segment_cache), anchor.held.manifest())
        .project_tail(anchor)
}
