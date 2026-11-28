//! The manifests, segments, and WAL objects of one namespace that its current
//! manifest and its snapshots protect.

use super::charged_set::{ChargedQueue, ChargedSet, CollectionMemory};
use super::deletes::Decision;
use super::families::CandidateFamily;
use super::roots::{
    ListedSnapshotKey, MissingManifest, NamespaceRoots, RetirementStatus, SnapshotManifest,
};
use super::snapshots::sweep_snapshot_record;
use crate::error::Result;
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::limits::GC_GRACE_WINDOW_MS;
use crate::manifest::publish::ManifestPublicationOutcome;
use crate::namespace::control::{LoadedManifest, NewestManifest};
use crate::namespace::store::NamespaceStore;
use crate::time::OperationClock;
use crate::wal::wal_no_is_unfolded;
use futures::{future, TryStreamExt};
use loonfs_objectstore::keys::{extension_segment_object_key, metadata_segment_object_key};
use loonfs_objectstore::layout::{manifest_no_of, wal_no_of};
use loonfs_objectstore::{ListedObject, ObjectStore};
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{GcResponse, ManifestNo, NamespaceId, RetainedReason, WalNo};

pub(super) struct LiveSet {
    pub(super) current_tombstone: Option<NamespaceManifestPayload>,
    pub(super) status: RetirementStatus,
    /// The number of the newest manifest that the collection found. The
    /// collection keeps that manifest and every manifest above it.
    pub(super) newest_manifest_no: ManifestNo,
    pub(super) objects: ChargedSet<String>,
    /// The collection's decision about each key that the snapshot listing
    /// returned, which the collection applies after its other deletes.
    pub(super) snapshot_decisions: ChargedQueue<Decision<GcResponse>>,
    /// The bytes of the keys that the snapshot listing returned, as the
    /// listing reported them.
    pub(super) snapshot_listed_bytes: u64,
    pub(super) memory: CollectionMemory,
    /// Whether the snapshot listing returned a key, recognized or not, that
    /// keeps a tombstone from retirement: any key other than the namespace's
    /// own fork point snapshot.
    has_snapshots: bool,
    pub(super) now_ms: u64,
    live_folded_wal_no: Option<WalNo>,
}

impl LiveSet {
    /// Loads the namespace's roots and the objects that they protect: the
    /// newest manifest that the call finds, the WAL objects that the manifest
    /// hasn't folded, and the manifests that snapshots keep, with their
    /// metadata and extension segments. Returns `None` when the namespace is
    /// retired, because a retired namespace has no roots.
    ///
    /// The call reads no WAL object and doesn't check the manifest for a
    /// successor (format spec, "One complete collection").
    ///
    /// A newest manifest that's initializing and older than the grace window
    /// belongs to an abandoned creation, so the call first publishes the
    /// namespace's tombstone (see
    /// [`NamespaceStore::tombstone_initializing_namespace`]), and continues
    /// with the manifest that's current after that publication.
    ///
    /// The object keys and snapshot decisions that the collection keeps are
    /// charged to `memory` as they arrive.
    pub(super) async fn load<S: ObjectStore>(
        namespace: &NamespaceStore<S>,
        clock: &OperationClock,
        memory: CollectionMemory,
    ) -> Result<Option<Self>> {
        let namespace_id = &namespace.namespace_id;
        let now_ms = clock.request_at_ms();
        let current = match namespace.find_newest_manifest(clock.origin()).await? {
            NewestManifest::Absent => return Err(namespace.namespace_not_found()),
            NewestManifest::Retired => return Ok(None),
            NewestManifest::Manifest(current) => current,
        };
        let found = NamespaceRoots::new(namespace, current.manifest().payload(), now_ms).status;
        let current = match found {
            RetirementStatus::Initializing { tombstone_at_ms } if now_ms >= tombstone_at_ms => {
                let created_at_ms = current.manifest().payload().created_at_ms;
                match namespace
                    .tombstone_initializing_namespace(current, clock)
                    .await?
                {
                    ManifestPublicationOutcome::Published(tombstone) => {
                        tracing::info!(
                            namespace_id = %namespace_id,
                            created_at_ms,
                            "published the tombstone of an abandoned namespace creation"
                        );
                        tombstone
                    }
                    ManifestPublicationOutcome::Lost(winner) => winner,
                }
            }
            _ => current,
        };
        let head = current.manifest().payload();
        let roots = NamespaceRoots::new(namespace, head, now_ms);
        let mut live = Self {
            current_tombstone: roots.status.is_deleted().then(|| head.clone()),
            status: roots.status,
            newest_manifest_no: head.manifest_no,
            objects: ChargedSet::new(&memory),
            snapshot_decisions: ChargedQueue::new(&memory),
            snapshot_listed_bytes: 0,
            memory,
            has_snapshots: false,
            now_ms,
            live_folded_wal_no: (!roots.status.is_deleted()).then_some(head.folded_wal_no),
        };
        let outcome = async {
            protect_manifest(&mut live.objects, current.manifest())?;
            live.load_snapshots(&roots).await
        }
        .await;
        tracing::info!(namespace_id = %namespace_id,
            object_key_bytes = live.objects.reserved_bytes(),
            snapshot_decision_bytes = live.snapshot_decisions.reserved_bytes(),
            live_set_bytes = live.memory.held_bytes(),
            live_set_limit_bytes = live.memory.limit(),
            complete = outcome.is_ok(),
            "read the live set's roots");
        outcome?;
        Ok(Some(live))
    }

    /// Protects the manifest of each listed snapshot, and keeps the collection's
    /// decision about each listed key.
    ///
    /// The collection reads the record of each snapshot whose manifest is
    /// present, and applies the snapshot cleanup rules to it. For a snapshot
    /// whose manifest is missing, the listing has already read the record.
    /// The collection applies the cleanup rules to an unverified user
    /// snapshot, and deletes it anyway once the record's provider age reaches
    /// the grace window. It keeps a deleted namespace's own fork point
    /// snapshot, which retirement deletes. Any other snapshot whose manifest
    /// is missing ends the collection with a corruption error.
    ///
    /// The collection protects each loaded manifest as the listing returns
    /// it, and drops the loaded manifest before the record read, so the
    /// listing's window of `STORE_READS_IN_FLIGHT` reads bounds the number of
    /// loaded manifests in memory.
    async fn load_snapshots<S: ObjectStore>(
        &mut self,
        roots: &NamespaceRoots<'_, S>,
    ) -> Result<()> {
        let namespace = roots.namespace;
        let objects = &mut self.objects;
        let mut snapshots = roots
            .snapshots()
            .and_then(|mut listed| {
                // A protected manifest is rooted, so `Rooted` replaces the
                // loaded manifest, which drops at the end of this closure.
                let protected = match &mut listed {
                    ListedSnapshotKey::Snapshot(snapshot) => {
                        match std::mem::replace(&mut snapshot.manifest, SnapshotManifest::Rooted) {
                            SnapshotManifest::Loaded(manifest) => {
                                protect_manifest(objects, &manifest)
                            }
                            manifest => {
                                snapshot.manifest = manifest;
                                Ok(())
                            }
                        }
                    }
                    ListedSnapshotKey::Unrecognized { .. } => Ok(()),
                };
                future::ready(protected.map(|()| listed))
            })
            .map_ok(|listed| async move {
                let record = match &listed {
                    ListedSnapshotKey::Snapshot(snapshot)
                        if !matches!(snapshot.manifest, SnapshotManifest::Missing(_)) =>
                    {
                        namespace
                            .load_listed_snapshot_record(&snapshot.entry.key)
                            .await?
                    }
                    ListedSnapshotKey::Snapshot(_) | ListedSnapshotKey::Unrecognized { .. } => None,
                };
                Ok((listed, record))
            })
            .try_buffered(STORE_READS_IN_FLIGHT);
        while let Some((listed, record)) = snapshots.try_next().await? {
            let snapshot = match listed {
                ListedSnapshotKey::Unrecognized { size_bytes } => {
                    self.has_snapshots = true;
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
            // The namespace's own fork point snapshot doesn't keep it from
            // retirement, which deletes that snapshot (format spec,
            // "Retirement").
            self.has_snapshots |= !roots.is_own_fork_point(&snapshot.snapshot_id);
            self.snapshot_listed_bytes += snapshot.entry.size_bytes;
            let (status, now_ms) = (self.status, self.now_ms);
            let decision = match snapshot.manifest {
                SnapshotManifest::Loaded(_) | SnapshotManifest::Rooted => match record {
                    Some(record) => {
                        sweep_snapshot_record(&snapshot.entry.key, &record, status, now_ms)
                    }
                    None => Decision::Gone,
                },
                SnapshotManifest::Missing(MissingManifest::RecordGone) => Decision::Gone,
                SnapshotManifest::Missing(MissingManifest::UnverifiedUser(record)) => {
                    match sweep_snapshot_record(&snapshot.entry.key, &record, status, now_ms) {
                        Decision::Retain { .. } => {
                            unverified_candidate(&namespace.namespace_id, snapshot.entry, now_ms)
                        }
                        decision => decision,
                    }
                }
                SnapshotManifest::Missing(MissingManifest::DeletedOwnForkPoint) => {
                    Decision::retain(RetainedReason::SnapshotNotDeletable, None, now_ms)
                }
            };
            self.snapshot_decisions.push_back(decision)?;
        }
        Ok(())
    }

    /// Returns whether the collection keeps the object of `family` at `key`, because
    /// a live root references the object, the object is a manifest numbered at
    /// or above the newest manifest that the collection found, or a live head
    /// hasn't folded it.
    pub(super) fn protects(&self, family: CandidateFamily, key: &str) -> bool {
        self.objects.contains(key)
            || match family {
                CandidateFamily::Manifests => {
                    manifest_no_of(key).is_some_and(|number| number >= self.newest_manifest_no)
                }
                CandidateFamily::WalObjects => {
                    self.live_folded_wal_no.is_some_and(|folded_wal_no| {
                        wal_no_of(key)
                            .is_none_or(|number| wal_no_is_unfolded(number, folded_wal_no))
                    })
                }
                CandidateFamily::MetadataSegments | CandidateFamily::ExtensionSegments => false,
            }
    }

    /// Returns whether the namespace's tombstone is eligible for retirement,
    /// which it is once the retirement deadline has passed and the complete
    /// snapshot listing returned no key other than the namespace's own fork
    /// point snapshot.
    pub(super) fn tombstone_is_eligible(&self) -> bool {
        self.status == RetirementStatus::PastDeadline && !self.has_snapshots
    }
}

/// Keeps `manifest` and its metadata and extension segments in `objects`.
fn protect_manifest(objects: &mut ChargedSet<String>, manifest: &LoadedManifest) -> Result<()> {
    objects.insert(manifest.object_key.clone())?;
    for segment in manifest.payload().runs.iter().flat_map(|run| &run.segments) {
        objects.insert(metadata_segment_object_key(segment))?;
    }
    for (name, entry) in &manifest.payload().extensions {
        for segment in entry.segments() {
            objects.insert(extension_segment_object_key(name, segment))?;
        }
    }
    Ok(())
}

/// Deletes `snapshot`, an unverified user snapshot of `namespace_id` whose
/// manifest is missing, once the record's provider age is at least the grace
/// window at `now_ms`, and keeps it otherwise. Waiting for the grace window
/// keeps the collection from deleting a candidate that its creator is still
/// verifying or deleting (format spec, "Snapshot cleanup").
fn unverified_candidate(
    namespace_id: &NamespaceId,
    snapshot: ListedObject,
    now_ms: u64,
) -> Decision<GcResponse> {
    let key = snapshot.key.clone();
    let decision = Decision::aged(
        snapshot,
        GC_GRACE_WINDOW_MS,
        now_ms,
        |report: &mut GcResponse| &mut report.deleted_snapshots_by_owner.user,
    );
    if let Decision::Delete { .. } = decision {
        tracing::warn!(
            namespace_id = %namespace_id,
            object_key = %key,
            "deleting an unverified user snapshot whose manifest is missing"
        );
    }
    decision
}
