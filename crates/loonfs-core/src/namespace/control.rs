//! Derives namespace read state from numbered manifests and the WAL tail.

use crate::control_object::ControlObjectLoadError;
use crate::error::CoreError;
use crate::limits::{MANIFEST_PROBE_WINDOW, MAX_CONTENTION_ATTEMPTS};
use crate::namespace::boundary::KeptBoundary;
use crate::namespace::held::Held;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::time::Observation;
use futures::{stream, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::{metadata_manifest_object, metadata_manifest_prefix};
use loonfs_objectstore::layout::manifest_no_of;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{ManifestBoundary, ManifestRef, NamespaceStatus};
use loonfs_types::format::manifest::{NamespaceManifestEnvelope, NamespaceManifestPayload};
use loonfs_types::{CompactorEpoch, ManifestNo, NamespaceId, MAX_PUBLIC_INTEGER};
use std::sync::Arc;

/// A namespace manifest loaded from the store, with its object key and stored
/// length.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedManifest {
    pub object_key: String,
    /// Stored length of the manifest object, which is the compressed document.
    pub(crate) manifest_bytes: u64,
    pub(crate) envelope: Arc<NamespaceManifestEnvelope>,
}

impl LoadedManifest {
    /// Returns the manifest's decoded envelope.
    pub fn envelope(&self) -> &Arc<NamespaceManifestEnvelope> {
        &self.envelope
    }

    /// Returns the manifest's payload.
    pub fn payload(&self) -> &NamespaceManifestPayload {
        self.envelope.payload()
    }

    /// Returns the reference that identifies this manifest.
    pub fn manifest(&self) -> ManifestRef {
        crate::manifest::publish::manifest_ref_for(&self.envelope)
    }

    pub fn retention_floor_seq(&self) -> loonfs_types::CommitSeq {
        self.payload().retention_floor_seq
    }

    pub fn folded_wal_no(&self) -> loonfs_types::WalNo {
        self.payload().folded_wal_no
    }

    pub fn compactor_epoch(&self) -> CompactorEpoch {
        self.payload().compactor_epoch
    }
}

/// What a find of a namespace's newest manifest from the manifest boundary
/// found (see [`NamespaceStore::find_newest_manifest`]).
#[derive(Debug, Clone)]
pub(crate) enum NewestManifest {
    /// No namespace has the ID.
    Absent,
    /// The namespace's boundary is retired.
    Retired,
    /// The namespace's newest manifest, with the boundary of the last read.
    Manifest(LoadedManifest, KeptBoundary),
}

pub(crate) fn ensure_head_identity(
    head: &NamespaceReadState,
    namespace_id: &NamespaceId,
) -> crate::error::Result<()> {
    if &head.namespace_id != namespace_id {
        return Err(CoreError::NamespaceCorrupt(format!(
            "head namespace `{}` does not match requested namespace `{}`",
            head.namespace_id, namespace_id
        )));
    }
    Ok(())
}

/// Returns `namespace_not_found` for a namespace that's initializing, because
/// LoonFS treats it as missing, and `namespace_deleted` for a tombstone.
pub fn ensure_namespace_live(
    namespace_id: &NamespaceId,
    status: NamespaceStatus,
) -> crate::error::Result<()> {
    let namespace_id = namespace_id.clone();
    match status {
        NamespaceStatus::Active {} => Ok(()),
        NamespaceStatus::Initializing {} => Err(CoreError::NamespaceNotFound { namespace_id }),
        NamespaceStatus::Deleted { .. } => Err(CoreError::NamespaceDeleted { namespace_id }),
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Loads the namespace's current manifest from the manifest boundary.
    pub async fn load_current_manifest(&self) -> crate::error::Result<LoadedManifest> {
        match self.find_newest_manifest().await? {
            NewestManifest::Absent => Err(self.namespace_not_found()),
            NewestManifest::Retired => Err(self.namespace_deleted()),
            NewestManifest::Manifest(manifest, _) => Ok(manifest),
        }
    }

    /// Loads the namespace's current manifest from the manifest boundary,
    /// with the boundary of the last read, as a manifest checked at
    /// `observed`, an observation that the caller read before the call.
    ///
    /// The find lists the manifests above the boundary after `observed`, so
    /// every manifest published before `observed` is at or below the one it
    /// returns. The call returns `namespace_not_found` when no namespace has
    /// the ID, and `namespace_deleted` when the namespace is retired.
    pub async fn load_current_basis(
        &self,
        observed: Observation,
    ) -> crate::error::Result<Held<LoadedManifest>> {
        match self.find_newest_manifest().await? {
            NewestManifest::Absent => Err(self.namespace_not_found()),
            NewestManifest::Retired => Err(self.namespace_deleted()),
            NewestManifest::Manifest(manifest, boundary) => {
                Ok(Held::new(manifest, observed, boundary))
            }
        }
    }

    /// Returns the error for a namespace that has no manifest above its
    /// manifest boundary and no boundary object.
    pub(crate) fn namespace_not_found(&self) -> CoreError {
        CoreError::NamespaceNotFound {
            namespace_id: self.namespace_id.clone(),
        }
    }

    /// Returns the error for a namespace whose boundary is retired.
    pub(crate) fn namespace_deleted(&self) -> CoreError {
        CoreError::NamespaceDeleted {
            namespace_id: self.namespace_id.clone(),
        }
    }

    /// Finds the namespace's newest manifest from the manifest boundary
    /// (format spec, "Resolving the metadata basis").
    ///
    /// Each attempt reads the boundary, and the call returns
    /// [`NewestManifest::Retired`] when the boundary is retired. Otherwise
    /// the attempt lists the manifests numbered above the boundary, loads the
    /// highest-numbered manifest in the listing through the segment cache,
    /// and reads the boundary again. That manifest is the newest when it's
    /// still above the boundary, so a cached manifest that a collection
    /// deleted is never returned. The attempt starts again when the manifest
    /// is gone by its load or is at or below the boundary by the second read,
    /// and when the listing is empty and the boundary changed. An empty
    /// listing under an
    /// unchanged missing boundary means that no namespace has the ID, and
    /// under an unchanged boundary object it means a corrupt store, because
    /// a collection never leaves a namespace without a manifest above its
    /// boundary. The call returns the manifest with the boundary of its last
    /// read, and returns `store_contention` after [`MAX_CONTENTION_ATTEMPTS`]
    /// attempts.
    pub(crate) async fn find_newest_manifest(&self) -> crate::error::Result<NewestManifest> {
        let prefix = metadata_manifest_prefix(&self.namespace_id);
        for _attempt in 0..MAX_CONTENTION_ATTEMPTS {
            let boundary = self.load_boundary().await?;
            let boundary_no = match boundary.as_ref().map(|loaded| loaded.state.boundary) {
                Some(ManifestBoundary::Retired {}) => return Ok(NewestManifest::Retired),
                Some(ManifestBoundary::Manifest { manifest_no }) => Some(manifest_no),
                None => None,
            };
            let start_after = boundary_no
                .map(|manifest_no| metadata_manifest_object(&self.namespace_id, &manifest_no));
            let highest = self
                .store
                .list_entries_from_stream(&prefix, start_after.as_deref())
                .map_err(|error| CoreError::store(&prefix, &error))
                .try_fold(None, |highest, entry| async move {
                    Ok::<_, CoreError>(manifest_no_of(&entry.key).max(highest))
                })
                .await?;
            let kept = KeptBoundary::from(boundary);
            let Some(manifest_no) = highest else {
                if self.reload_boundary(kept.clone()).await? != kept {
                    continue;
                }
                let (Some(boundary), Some(boundary_no)) = (kept.object(), boundary_no) else {
                    return Ok(NewestManifest::Absent);
                };
                return Err(ControlObjectLoadError::Codec {
                    message: format!("no manifest is above the manifest boundary `{boundary_no}`"),
                    object_key: boundary.object_key.clone(),
                }
                .into());
            };
            let Some(manifest) = self.load_manifest_by_no(manifest_no).await? else {
                continue;
            };
            let reloaded = self.reload_boundary(kept).await?;
            if !reloaded.covers(manifest_no) {
                return Ok(NewestManifest::Manifest(manifest, reloaded));
            }
        }
        Err(CoreError::contention_exhausted(
            &loonfs_objectstore::keys::boundary(&self.namespace_id),
        ))
    }

    /// Returns the newest manifest above `previous`, or `None` when no manifest
    /// above `previous` exists.
    ///
    /// The probe sends one HEAD request for the number after `previous`, and a
    /// missing number ends it there. Otherwise it probes the first
    /// [`MANIFEST_PROBE_WINDOW`] numbers in parallel and, if all exist,
    /// switches to exponential galloping and parallel bracket narrowing
    /// ([`Self::probe_highest_manifest_from`]). The call then loads the
    /// manifest at the highest number found through the segment cache, with
    /// one GET when the cache doesn't keep it. The call returns `None` too if
    /// that manifest is gone by the GET.
    pub(crate) async fn find_newest_manifest_above(
        &self,
        previous: &LoadedManifest,
    ) -> Result<Option<LoadedManifest>, ControlObjectLoadError> {
        let Ok(next) = previous.payload().manifest_no.successor() else {
            return Ok(None);
        };
        if !self.manifest_exists(next).await? {
            return Ok(None);
        }
        let newest = self.probe_highest_manifest_from(next).await?;
        self.load_manifest_by_no(newest).await
    }

    /// Finds the highest existing manifest number at or above `confirmed`
    /// (which the caller already observed as present).
    ///
    /// Phase 1 probes the next [`MANIFEST_PROBE_WINDOW`] consecutive numbers in
    /// one parallel round-trip so small deltas resolve immediately. If every
    /// slot in that first window exists, phase 2 gallops exponentially in
    /// parallel batches until an absent upper bound is found, and phase 3
    /// narrows the `(lo, hi)` bracket with up to `MANIFEST_PROBE_WINDOW` evenly
    /// spaced probes per round-trip. Callers still check `boundary.json`
    /// afterward, so a concurrent GC deletion mid-probe triggers a clean retry.
    async fn probe_highest_manifest_from(
        &self,
        confirmed: ManifestNo,
    ) -> Result<ManifestNo, ControlObjectLoadError> {
        // Phase 1: initial linear window covers small deltas in 1 RTT.
        let first_window: Vec<_> = (1..=MANIFEST_PROBE_WINDOW as u64)
            .map_while(|offset| {
                confirmed
                    .0
                    .checked_add(offset)
                    .filter(|&number| number <= MAX_PUBLIC_INTEGER)
                    .map(ManifestNo)
            })
            .collect();
        let Some(&window_end) = first_window.last() else {
            return Ok(confirmed);
        };
        let mut lo = confirmed;
        let mut hi: Option<ManifestNo> = None;
        self.apply_probe_batch(&first_window, &mut lo, &mut hi)
            .await?;
        if hi.is_some() || lo != window_end {
            return Ok(lo);
        }

        // Phase 2: exponential gallop until an absent upper bound `hi` is found.
        // Starting at offset 1 lets a tip sitting right at `window_end` stop in
        // one round without any narrowing step.
        let mut step = 1u64;
        while hi.is_none() {
            let mut candidates = Vec::with_capacity(MANIFEST_PROBE_WINDOW);
            for _ in 0..MANIFEST_PROBE_WINDOW {
                let Some(next) =
                    lo.0.checked_add(step)
                        .filter(|&number| number <= MAX_PUBLIC_INTEGER)
                else {
                    break;
                };
                candidates.push(ManifestNo(next));
                step = step.saturating_mul(2);
            }
            if candidates.is_empty() {
                if lo.0 < MAX_PUBLIC_INTEGER {
                    let max_no = ManifestNo(MAX_PUBLIC_INTEGER);
                    if self.manifest_exists(max_no).await? {
                        return Ok(max_no);
                    }
                    hi = Some(max_no);
                }
                break;
            }
            self.apply_probe_batch(&candidates, &mut lo, &mut hi)
                .await?;
        }

        // Phase 3: parallel (MANIFEST_PROBE_WINDOW + 1)-way bracket narrowing.
        let Some(mut hi) = hi else {
            return Ok(lo);
        };
        while hi.0 - lo.0 > 1 {
            let span = hi.0 - lo.0;
            let gap = span - 1;
            let candidates: Vec<_> = if gap <= MANIFEST_PROBE_WINDOW as u64 {
                (lo.0 + 1..hi.0).map(ManifestNo).collect()
            } else {
                let slots = MANIFEST_PROBE_WINDOW as u128;
                (1..=slots)
                    .map(|index| {
                        let offset = u64::try_from((u128::from(span) * index) / (slots + 1))
                            .expect("interior offset fits in span");
                        ManifestNo(lo.0 + offset)
                    })
                    .collect()
            };
            let mut next_hi = Some(hi);
            self.apply_probe_batch(&candidates, &mut lo, &mut next_hi)
                .await?;
            hi = next_hi.expect("upper bound stays bounded during narrowing");
        }
        Ok(lo)
    }

    /// Probes ascending `candidates` in parallel, advancing `lo` to the highest
    /// existing number and tightening `hi` to the lowest absent number above `lo`.
    async fn apply_probe_batch(
        &self,
        candidates: &[ManifestNo],
        lo: &mut ManifestNo,
        hi: &mut Option<ManifestNo>,
    ) -> Result<(), ControlObjectLoadError> {
        let results: Vec<(ManifestNo, bool)> = stream::iter(candidates.iter().copied())
            .map(|manifest_no| async move {
                Ok::<_, ControlObjectLoadError>((
                    manifest_no,
                    self.manifest_exists(manifest_no).await?,
                ))
            })
            .buffered(MANIFEST_PROBE_WINDOW)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(present) = results
            .iter()
            .rev()
            .find(|(_, exists)| *exists)
            .map(|(manifest_no, _)| *manifest_no)
        {
            if present > *lo {
                *lo = present;
            }
        }
        if let Some(absent) = results
            .iter()
            .find(|(manifest_no, exists)| !*exists && *manifest_no > *lo)
            .map(|(manifest_no, _)| *manifest_no)
        {
            *hi = Some(match *hi {
                Some(existing) => existing.min(absent),
                None => absent,
            });
        }
        Ok(())
    }

    /// Returns whether the manifest numbered `manifest_no` exists, with one HEAD
    /// request.
    pub(crate) async fn manifest_exists(
        &self,
        manifest_no: ManifestNo,
    ) -> Result<bool, ControlObjectLoadError> {
        let object_key =
            loonfs_objectstore::keys::metadata_manifest_object(&self.namespace_id, &manifest_no);
        Ok(self
            .store
            .head(&object_key)
            .await
            .map_err(|error| ControlObjectLoadError::Store {
                object_key: object_key.clone(),
                message: error.public_message().into_owned(),
                kind: error.kind(),
            })?
            .is_some())
    }

    /// Loads the manifest numbered `manifest_no` through the segment cache, or
    /// returns `None` when the store doesn't have the manifest (see
    /// [`Self::load_manifest_if_present`]).
    pub(crate) async fn load_manifest_by_no(
        &self,
        manifest_no: loonfs_types::ManifestNo,
    ) -> Result<Option<LoadedManifest>, ControlObjectLoadError> {
        self.load_manifest_if_present(manifest_no)
            .await
            .map_err(|error| match error {
                crate::manifest::ManifestLoadError::ReadManifest {
                    object_key,
                    message,
                    kind,
                } => ControlObjectLoadError::Store {
                    object_key,
                    message,
                    kind,
                },
                error => ControlObjectLoadError::Codec {
                    object_key: metadata_manifest_object(&self.namespace_id, &manifest_no),
                    message: error.to_string(),
                },
            })
    }

    pub async fn load_read_state(&self) -> crate::error::Result<NamespaceReadState> {
        let observed = crate::time::Observation::now(Arc::clone(&self.timer));
        Ok(self.load_read_anchor(observed).await?.read_state)
    }
}

const _: () = assert!(
    0 < MANIFEST_PROBE_WINDOW && MANIFEST_PROBE_WINDOW <= crate::in_flight::STORE_READS_IN_FLIGHT
);

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use loonfs_objectstore::PutMode;
    use loonfs_test_support::stores::{KeyPredicate, MemoryStore, RecordingStore};

    async fn seed_manifest_keys(
        store: &MemoryStore,
        namespace_id: &NamespaceId,
        range: std::ops::RangeInclusive<u64>,
    ) {
        for number in range {
            store
                .put(
                    &metadata_manifest_object(namespace_id, &ManifestNo(number)),
                    Bytes::from_static(b"x"),
                    PutMode::CreateIfAbsent,
                )
                .await
                .expect("put manifest placeholder");
        }
    }

    #[tokio::test]
    async fn exponential_probe_finds_exact_tip_across_scales_in_logarithmic_heads() {
        for tip in [1u64, 2, 8, 9, 10, 25, 100, 1_000, 5_000] {
            let backing = MemoryStore::new(loonfs_types::ChecksumAlgorithm::Crc64nvme);
            let namespace_id = NamespaceId::parse("exp-probe").expect("namespace id");
            seed_manifest_keys(&backing, &namespace_id, 1..=tip).await;
            let store = RecordingStore::new(backing, KeyPredicate::any());
            let ns = crate::test_support::namespace_store(&store, &namespace_id);

            let found = ns
                .probe_highest_manifest_from(ManifestNo(1))
                .await
                .expect("probe succeeds");
            assert_eq!(found, ManifestNo(tip), "tip={tip}");

            let heads = store.counts().heads;
            if tip < 1 + MANIFEST_PROBE_WINDOW as u64 {
                assert_eq!(heads, MANIFEST_PROBE_WINDOW, "small delta uses 1 window");
            } else if tip == 1 + MANIFEST_PROBE_WINDOW as u64 {
                assert_eq!(
                    heads,
                    MANIFEST_PROBE_WINDOW * 2,
                    "boundary tip stops on first gallop batch"
                );
            } else {
                // Linear windows of 8 would take `tip` HEADs; exponential +
                // 9-way narrowing stays well under 64 HEADs even at 5,000.
                assert!(
                    heads <= 64,
                    "expected O(log N) HEADs for tip={tip}, observed {heads}"
                );
            }
        }
    }
}
