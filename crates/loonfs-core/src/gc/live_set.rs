//! The manifests, segments, and WAL objects of one namespace that its current
//! manifest, its WAL tail, and its snapshots protect.

use super::charged_set::{ChargedQueue, ChargedSet, CollectionMemory};
use super::deletes::Decision;
use super::families::CandidateFamily;
use super::snapshot_manifests::{missing_root_manifest, ListedSnapshotKey, SnapshotManifest};
use crate::error::Result;
use crate::limits::{GC_GRACE_WINDOW_MS, NAMESPACE_RETIREMENT_GRACE_WINDOW_MS};
use crate::namespace::control::LoadedManifest;
use crate::namespace::store::NamespaceStore;
use crate::segment::cache::read_working_memory;
use crate::time::Observation;
use crate::time::OperationClock;
use crate::wal::{live_folded_wal_no, object_is_required};
use futures::TryStreamExt;
use loonfs_objectstore::keys::{extension_segment_object_key, metadata_segment_object_key};
use loonfs_objectstore::layout::manifest_no_of;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{GcResponse, ManifestNo, RetainedReason, WalNo};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RetirementStatus {
    Active,
    Retained { retires_at_ms: Option<u64> },
    Eligible,
}

impl RetirementStatus {
    /// Returns the [retirement](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#95-retirement)
    /// status at `now_ms` of a namespace whose current manifest is `tombstone`
    /// when the namespace is deleted, and whose complete snapshot listing returned
    /// a key when `has_snapshots`.
    pub(super) fn of(
        tombstone: Option<&NamespaceManifestPayload>,
        has_snapshots: bool,
        now_ms: u64,
    ) -> Self {
        let Some(tombstone) = tombstone else {
            return Self::Active;
        };
        let deadline_ms = retirement_deadline_ms(tombstone);
        if now_ms < deadline_ms {
            Self::Retained {
                retires_at_ms: Some(deadline_ms),
            }
        } else if has_snapshots {
            Self::Retained {
                retires_at_ms: None,
            }
        } else {
            Self::Eligible
        }
    }
}

pub(super) struct LiveSet {
    pub(super) current_tombstone: Option<NamespaceManifestPayload>,
    pub(super) discovery_start_manifest_no: ManifestNo,
    pub(super) objects: ChargedSet<String>,
    /// The collection's decision about each key that the snapshot listing
    /// returned, which the collection applies after its other deletes.
    pub(super) snapshot_decisions: ChargedQueue<Decision<GcResponse>>,
    /// The bytes of the keys that the snapshot listing returned, as the
    /// listing reported them.
    pub(super) snapshot_listed_bytes: u64,
    pub(super) memory: CollectionMemory,
    /// Whether the snapshot listing returned any key, recognized or not, which
    /// keeps a tombstone from retirement.
    has_snapshots: bool,
    pub(super) now_ms: u64,
    live_folded_wal_no: Option<WalNo>,
}

impl LiveSet {
    /// Loads the namespace's roots and the objects that they protect: the
    /// current manifest, the WAL tail above it, and the manifests that snapshots
    /// keep, with their metadata and extension segments.
    ///
    /// The object keys and snapshot decisions that the collection keeps are charged
    /// to the read working memory of the namespace's segment cache as they
    /// arrive.
    pub(super) async fn load<S: ObjectStore>(
        namespace: &NamespaceStore<S>,
        clock: &OperationClock,
    ) -> Result<Self> {
        let namespace_id = &namespace.namespace_id;
        let observed = Observation::now(Arc::clone(&namespace.timer));
        let (manifest, hint) = namespace.load_current_manifest_with_hint().await?;
        let anchor = namespace
            .load_read_anchor_from_manifest(manifest, hint, observed)
            .await?;
        let memory = CollectionMemory::new(
            namespace_id.to_string(),
            read_working_memory(Some(namespace.segment_cache.as_ref())),
        );
        let head = anchor.manifest.payload();
        let mut live = Self {
            current_tombstone: head.status.is_deleted().then(|| head.clone()),
            discovery_start_manifest_no: anchor.hint.state.manifest_no,
            objects: ChargedSet::new(&memory),
            snapshot_decisions: ChargedQueue::new(&memory),
            snapshot_listed_bytes: 0,
            memory,
            has_snapshots: false,
            now_ms: clock.request_at_ms(),
            live_folded_wal_no: live_folded_wal_no(&anchor.read_state),
        };
        let outcome = async {
            live.protect_manifest(&anchor.manifest)?;
            live.load_snapshots(namespace, head.manifest_no, clock)
                .await
        }
        .await;
        tracing::info!(namespace_id = %namespace_id,
            object_key_bytes = live.objects.reserved_bytes(),
            snapshot_decision_bytes = live.snapshot_decisions.reserved_bytes(),
            live_set_bytes = live.memory.held_bytes(),
            read_memory_limit_bytes = live.memory.limit(),
            complete = outcome.is_ok(),
            "read the live set's roots");
        outcome?;
        Ok(live)
    }

    /// Protects the manifest of each listed snapshot, and keeps the collection's
    /// decision about each listed key.
    ///
    /// A snapshot whose manifest is missing fails the collection unless snapshot cleanup
    /// would delete the snapshot.
    async fn load_snapshots<S: ObjectStore>(
        &mut self,
        namespace: &NamespaceStore<S>,
        current_manifest_no: ManifestNo,
        clock: &OperationClock,
    ) -> Result<()> {
        let mut snapshots = namespace.list_snapshot_manifests(current_manifest_no);
        while let Some(listed) = snapshots.try_next().await? {
            self.has_snapshots = true;
            let snapshot = match listed {
                ListedSnapshotKey::Unrecognized { size_bytes } => {
                    self.snapshot_listed_bytes += size_bytes;
                    self.snapshot_decisions.push_back(Decision::retain(
                        RetainedReason::UnrecognizedKey,
                        None,
                        self.now_ms,
                    ))?;
                    continue;
                }
                ListedSnapshotKey::Snapshot(snapshot) => snapshot,
            };
            self.snapshot_listed_bytes += snapshot.size_bytes;
            if let SnapshotManifest::Loaded(manifest) = &snapshot.manifest {
                self.protect_manifest(manifest)?;
            }
            let decision = match (&snapshot.manifest, &snapshot.record) {
                // A failed snapshot installation can outlive both its basis and its
                // cleanup attempt. Another collector can also remove a released
                // snapshot's basis after this collection listed it. Every snapshot for a
                // missing manifest is checked on its own, because another snapshot
                // for the number can still require retention when an earlier
                // snapshot was deletable. The snapshot's record is read again, because
                // the snapshot can have been released after the listing read it.
                (SnapshotManifest::Missing(manifest_no), _) => {
                    let decision = namespace.sweep_snapshot(&snapshot.key, self, clock).await?;
                    if let Decision::Retain { .. } = decision {
                        return Err(missing_root_manifest(
                            &namespace.namespace_id,
                            *manifest_no,
                            &snapshot.key,
                        ));
                    }
                    decision
                }
                (_, Some(record)) => {
                    namespace
                        .sweep_snapshot_record(&snapshot.key, record, self, clock)
                        .await?
                }
                (_, None) => Decision::Gone,
            };
            self.snapshot_decisions.push_back(decision)?;
        }
        Ok(())
    }

    /// Keeps `manifest` and its metadata and extension segments.
    fn protect_manifest(&mut self, manifest: &LoadedManifest) -> Result<()> {
        self.objects.insert(manifest.object_key.clone())?;
        for segment in manifest.payload().runs.iter().flat_map(|run| &run.segments) {
            self.objects.insert(metadata_segment_object_key(segment))?;
        }
        for (name, entry) in &manifest.payload().extensions {
            for segment in entry.segments() {
                self.objects
                    .insert(extension_segment_object_key(name, segment))?;
            }
        }
        Ok(())
    }

    /// Returns whether the collection keeps the object of `family` at `key`, because
    /// a live root references the object, discovery can still read it, or a
    /// live head hasn't folded it.
    pub(super) fn protects(&self, family: CandidateFamily, key: &str) -> bool {
        self.objects.contains(key)
            || match family {
                CandidateFamily::Manifests => manifest_no_of(key)
                    .is_some_and(|number| number >= self.discovery_start_manifest_no),
                CandidateFamily::WalObjects => self
                    .live_folded_wal_no
                    .is_some_and(|folded_wal_no| object_is_required(key, folded_wal_no)),
                CandidateFamily::MetadataSegments | CandidateFamily::ExtensionSegments => false,
            }
    }

    pub(super) fn deadline(&self, tombstone: &NamespaceManifestPayload) -> u64 {
        retirement_deadline_ms(tombstone)
    }

    /// Returns the retirement status of the collected namespace.
    pub(super) fn retirement_status(&self) -> RetirementStatus {
        RetirementStatus::of(
            self.current_tombstone.as_ref(),
            self.has_snapshots,
            self.now_ms,
        )
    }
}

/// Returns the earliest time at which the namespace of `tombstone` can be
/// retired: the grace window or the retirement grace after its deletion,
/// whichever is longer.
fn retirement_deadline_ms(tombstone: &NamespaceManifestPayload) -> u64 {
    tombstone
        .status
        .deleted_at_ms()
        .expect("a tombstone should carry its deletion stamp")
        .saturating_add(GC_GRACE_WINDOW_MS.max(NAMESPACE_RETIREMENT_GRACE_WINDOW_MS))
}
