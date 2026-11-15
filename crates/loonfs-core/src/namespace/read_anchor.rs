//! Loads a manifest and rechecks its successor around WAL tip discovery.

use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result as CoreResult};
use crate::limits::MAX_POSITION_MOVES;
use crate::manifest::{metadata_basis_from_manifest, PublishedManifest};
use crate::namespace::basis::{MetadataBasis, PublicationBasis, ReadBasis};
use crate::namespace::boundary::LoadedBoundary;
use crate::namespace::control::{LoadedManifest, NewestManifest};
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::namespace::writer_epoch::{ensure_writer_not_fenced, AcquiredWriter};
use crate::segment::MetadataSegmentCache;
use crate::wal::{discover_tail, DiscoveredTail, ProjectedWalTail, ValidatedWalTail};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{ManifestRef, NamespaceStatus};
use loonfs_types::{CommitSeq, ManifestNo, WriterEpoch};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct NamespaceReadAnchor {
    pub read_state: NamespaceReadState,
    pub(crate) manifest: LoadedManifest,
    pub(crate) tail: ValidatedWalTail,
    /// When the anchor's checks began, read before the anchor loaded
    /// `manifest` and confirmed it with a successor check.
    pub(crate) checked: crate::time::Observation,
    /// The manifest boundary as the anchor's last check read it.
    pub(crate) boundary: Option<LoadedBoundary>,
}

impl NamespaceReadAnchor {
    /// Returns the retention floor of the anchor's view: the manifest's
    /// floor, raised by the floor objects in the anchor's WAL tail.
    pub fn retention_floor_seq(&self) -> CommitSeq {
        self.read_state.retention_floor_seq
    }

    pub fn basis(&self) -> MetadataBasis {
        MetadataBasis::new(self.manifest.manifest().clone())
    }

    /// Returns the current manifest that the anchor loaded before it read
    /// the WAL tail above that manifest.
    pub fn current_manifest(&self) -> &LoadedManifest {
        &self.manifest
    }

    /// Returns the unfolded WAL bytes above the anchor's manifest: the
    /// stored sizes of the WAL objects with commits that the anchor read.
    pub fn wal_tail_bytes(&self) -> usize {
        self.tail.wal_bytes()
    }

    /// Returns the manifest boundary as the anchor's last check read it.
    pub fn boundary(&self) -> Option<LoadedBoundary> {
        self.boundary.clone()
    }

    /// Returns the anchor's manifest, with the manifest boundary that the
    /// anchor's last check read, as a basis that a publication can build on.
    pub fn publication_basis(&self) -> PublicationBasis {
        PublicationBasis {
            manifest: self.manifest.clone(),
            boundary: self.boundary.clone(),
        }
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    pub async fn load_read_anchor(
        &self,
        observed: crate::time::Observation,
    ) -> CoreResult<NamespaceReadAnchor> {
        let current = self.load_current_basis().await?;
        self.load_read_anchor_from_manifest(current.manifest, current.boundary, observed, None)
            .await
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
        let current = self.load_current_basis().await?;
        self.load_read_anchor_from_manifest(
            current.manifest,
            current.boundary,
            observed,
            Some(acquired_writer),
        )
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
    pub async fn find_initializing_read_basis(&self) -> CoreResult<Option<ReadBasis>> {
        let NewestManifest::Manifest(current) = self.find_newest_manifest().await? else {
            return Ok(None);
        };
        let manifest = &current.manifest;
        Ok(
            matches!(manifest.payload().status, NamespaceStatus::Initializing {}).then(|| {
                ReadBasis {
                    head: NamespaceReadState::from(manifest.payload()),
                    basis: MetadataBasis::new(manifest.manifest()),
                }
            }),
        )
    }

    /// Loads the read anchor at `manifest`, where `boundary` is the manifest
    /// boundary as the caller last read it.
    ///
    /// With `acquired_writer`, the call checks the writer epoch of each
    /// manifest before it reads the WAL tail above that manifest, and fails
    /// with `writer_fenced` at a manifest of another epoch. Replay doesn't
    /// change the writer epoch, so the anchor's head has the epoch of its
    /// manifest.
    ///
    /// After the WAL tail above the manifest, the call checks the manifest
    /// with [`Self::check_anchor_manifest`]. A successor, or a manifest at or
    /// below the boundary, makes the call find the newest manifest from the
    /// boundary and start again (format spec, "Replaying the visible WAL").
    /// A tombstone has no successor, so it isn't checked.
    pub(crate) async fn load_read_anchor_from_manifest(
        &self,
        manifest: LoadedManifest,
        boundary: Option<LoadedBoundary>,
        observed: crate::time::Observation,
        acquired_writer: Option<&AcquiredWriter>,
    ) -> CoreResult<NamespaceReadAnchor> {
        self.discover_read_anchor(manifest, boundary, observed, acquired_writer, true)
            .await
    }

    /// Loads the anchor at `manifest` as
    /// [`Self::load_read_anchor_from_manifest`] does, but sends no check of
    /// the manifest after the WAL tail. A writer acquisition reads its fence's
    /// tail this way and checks the manifest after the fence put, so the
    /// anchor is never used before that check.
    pub(crate) async fn load_unchecked_read_anchor(
        &self,
        manifest: LoadedManifest,
        boundary: Option<LoadedBoundary>,
        observed: crate::time::Observation,
        acquired_writer: Option<&AcquiredWriter>,
    ) -> CoreResult<NamespaceReadAnchor> {
        self.discover_read_anchor(manifest, boundary, observed, acquired_writer, false)
            .await
    }

    async fn discover_read_anchor(
        &self,
        mut manifest: LoadedManifest,
        mut boundary: Option<LoadedBoundary>,
        mut observed: crate::time::Observation,
        acquired_writer: Option<&AcquiredWriter>,
        check_manifest: bool,
    ) -> CoreResult<NamespaceReadAnchor> {
        for _attempt in 0..crate::limits::MAX_CONTENTION_ATTEMPTS {
            if let Some(acquired_writer) = acquired_writer {
                let payload = manifest.payload();
                ensure_writer_not_fenced(
                    payload.writer_epoch,
                    payload.writer.as_ref(),
                    acquired_writer,
                )?;
            }
            match discover_tail(&self.store, &manifest).await {
                Ok(DiscoveredTail { head: state, tail }) => {
                    let mut anchor = NamespaceReadAnchor {
                        read_state: state,
                        manifest,
                        tail,
                        checked: observed,
                        boundary,
                    };
                    if !check_manifest
                        || anchor.read_state.status.is_deleted()
                        || self.check_anchor_manifest(&mut anchor).await?
                    {
                        return Ok(anchor);
                    }
                    observed = anchor.checked.renew();
                    PublicationBasis { manifest, boundary } = self.load_current_basis().await?;
                }
                Err(error @ ControlObjectLoadError::Codec { .. }) => {
                    observed = observed.renew();
                    let current = self.load_current_basis().await?;
                    if current.manifest.manifest().manifest_no == manifest.manifest().manifest_no {
                        return Err(error.into());
                    }
                    PublicationBasis { manifest, boundary } = current;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(CoreError::contention_exhausted(&manifest.object_key))
    }

    /// Returns whether the anchor's manifest is the newest: a HEAD request
    /// for its successor, and when there is none, the manifest boundary (see
    /// [`Self::above_boundary`]). The anchor keeps the boundary that the call
    /// read.
    pub(crate) async fn check_anchor_manifest(
        &self,
        anchor: &mut NamespaceReadAnchor,
    ) -> CoreResult<bool> {
        let manifest_no = anchor.manifest.manifest().manifest_no;
        if self.manifest_has_successor(manifest_no).await? {
            return Ok(false);
        }
        let newest;
        (newest, anchor.boundary) = self
            .above_boundary(manifest_no, anchor.boundary.take())
            .await?;
        Ok(newest)
    }

    pub async fn manifest_has_successor(
        &self,
        manifest_no: ManifestNo,
    ) -> Result<bool, ControlObjectLoadError> {
        let Ok(next) = manifest_no.successor() else {
            return Ok(false);
        };
        self.manifest_exists(next).await
    }

    /// Returns the manifests above `basis`, oldest first, for a position at
    /// `basis` to move onto, while each is an active manifest of `writer_epoch`.
    ///
    /// Manifest numbers are consecutive, so the walk loads each successor until
    /// one is missing. It returns `None` when `basis` has no successor, when it
    /// finds a manifest of another writer epoch (another writer's claim) or a
    /// tombstone, which settles that the position can't move, and when it finds
    /// more than [`MAX_POSITION_MOVES`] manifests.
    pub async fn same_epoch_manifests_above(
        &self,
        basis: &ManifestRef,
        writer_epoch: WriterEpoch,
    ) -> Result<Option<Vec<PublishedManifest>>, ControlObjectLoadError> {
        let mut moves = Vec::new();
        let mut manifest_no = basis.manifest_no;
        while moves.len() <= MAX_POSITION_MOVES {
            let Ok(next) = manifest_no.successor() else {
                break;
            };
            let Some(loaded) = self.load_manifest_by_no(next).await? else {
                return Ok((!moves.is_empty()).then_some(moves));
            };
            let payload = loaded.payload();
            if payload.writer_epoch != writer_epoch || payload.status.is_deleted() {
                break;
            }
            moves.push(PublishedManifest::new(&loaded.envelope, None, None));
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
    metadata_basis_from_manifest(store, Some(segment_cache), &anchor.manifest).project_tail(anchor)
}
