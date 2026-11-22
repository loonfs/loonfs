//! The manifests, segments, and WAL objects of one namespace that its current
//! manifest and its snapshots protect.

use super::charged_set::{ChargedQueue, ChargedSet, CollectionMemory};
use super::deletes::Decision;
use super::families::CandidateFamily;
use super::snapshot_manifests::{
    missing_root_manifest, ListedSnapshot, ListedSnapshotKey, SnapshotManifest,
};
use super::snapshots::sweep_snapshot_record;
use crate::error::Result;
use crate::limits::GC_GRACE_WINDOW_MS;
use crate::namespace::control::{LoadedManifest, NewestManifest};
use crate::namespace::delete::TombstoneOutcome;
use crate::namespace::store::NamespaceStore;
use crate::time::OperationClock;
use crate::wal::wal_no_is_unfolded;
use futures::TryStreamExt;
use loonfs_objectstore::keys::{extension_segment_object_key, metadata_segment_object_key};
use loonfs_objectstore::layout::{manifest_no_of, wal_no_of};
use loonfs_objectstore::{ListedObject, ObjectStore};
use loonfs_types::format::control::{NamespaceStatus, SnapshotOwner};
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{GcResponse, ManifestNo, NamespaceId, RetainedReason, WalNo};

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

    /// Returns whether the layout view of the namespace's current manifest is
    /// a [content root](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#119-content-roots):
    /// while the namespace is active, and while a deleted namespace's
    /// retirement deadline is in the future.
    pub(super) fn current_view_is_root(self) -> bool {
        matches!(
            self,
            Self::Active
                | Self::Retained {
                    retires_at_ms: Some(_)
                }
        )
    }
}

pub(super) struct LiveSet {
    pub(super) current_tombstone: Option<NamespaceManifestPayload>,
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
    /// When a collection publishes the tombstone of the namespace, whose
    /// newest manifest is initializing: the grace window after its manifest
    /// 1's `created_at_ms`.
    pub(super) tombstone_at_ms: Option<u64>,
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
        let current = match namespace.find_newest_manifest().await? {
            NewestManifest::Absent => return Err(namespace.namespace_not_found()),
            NewestManifest::Retired => return Ok(None),
            NewestManifest::Manifest(current) => current,
        };
        let created_at_ms = current.manifest.payload().created_at_ms;
        let current = if matches!(
            current.manifest.payload().status,
            NamespaceStatus::Initializing {}
        ) && clock.request_at_ms().saturating_sub(created_at_ms)
            >= GC_GRACE_WINDOW_MS
        {
            let update = namespace
                .tombstone_initializing_namespace(current, clock)
                .await?;
            if let TombstoneOutcome::Published(_) = update.response {
                tracing::info!(
                    namespace_id = %namespace_id,
                    created_at_ms,
                    "published the tombstone of an abandoned namespace creation"
                );
            }
            update.current
        } else {
            current
        };
        let head = current.manifest.payload();
        let mut live = Self {
            current_tombstone: head.status.is_deleted().then(|| head.clone()),
            newest_manifest_no: head.manifest_no,
            objects: ChargedSet::new(&memory),
            snapshot_decisions: ChargedQueue::new(&memory),
            snapshot_listed_bytes: 0,
            memory,
            has_snapshots: false,
            now_ms: clock.request_at_ms(),
            live_folded_wal_no: (!head.status.is_deleted()).then_some(head.folded_wal_no),
            tombstone_at_ms: matches!(head.status, NamespaceStatus::Initializing {})
                .then(|| head.created_at_ms.saturating_add(GC_GRACE_WINDOW_MS)),
        };
        let outcome = async {
            live.protect_manifest(&current.manifest)?;
            live.load_snapshots(namespace, head.manifest_no, clock)
                .await
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
    /// A user snapshot whose manifest is missing is an unverified candidate,
    /// which the collection deletes once the record's provider age reaches the
    /// grace window. A fork snapshot whose manifest is missing fails the
    /// collection unless snapshot cleanup would delete the snapshot, or the
    /// snapshot is a deleted namespace's own fork point snapshot, which
    /// retirement deletes.
    async fn load_snapshots<S: ObjectStore>(
        &mut self,
        namespace: &NamespaceStore<S>,
        current_manifest_no: ManifestNo,
        clock: &OperationClock,
    ) -> Result<()> {
        let mut snapshots = namespace.list_snapshot_records(current_manifest_no);
        while let Some(listed) = snapshots.try_next().await? {
            let (snapshot, record) = match listed {
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
                ListedSnapshotKey::Snapshot(snapshot, record) => (snapshot, record),
            };
            let own_fork_point = record.as_ref().is_some_and(|record| {
                matches!(
                    &record.owner,
                    SnapshotOwner::Fork { target_namespace_id }
                        if *target_namespace_id == namespace.namespace_id
                )
            });
            // The namespace's own fork point snapshot doesn't keep it from
            // retirement, which deletes that snapshot (format spec,
            // "Retirement").
            self.has_snapshots |= !own_fork_point;
            self.snapshot_listed_bytes += snapshot.size_bytes;
            if let SnapshotManifest::Loaded(manifest) = &snapshot.manifest {
                self.protect_manifest(manifest)?;
            }
            let decision = match (&snapshot.manifest, &record) {
                // Another collector can remove a released snapshot's basis after
                // this collection listed it. Every snapshot for a missing manifest
                // is checked on its own, because another snapshot for the number
                // can still require retention when an earlier snapshot was
                // deletable. The snapshot's record is read again, because the
                // snapshot can have been released after the listing read it.
                (SnapshotManifest::Missing(manifest_no), _) => {
                    let decision = namespace.sweep_snapshot(&snapshot.key, self, clock).await?;
                    let user = record
                        .as_ref()
                        .is_some_and(|record| matches!(record.owner, SnapshotOwner::User { .. }));
                    if user && matches!(decision, Decision::Retain { .. }) {
                        // A verified user snapshot protects its manifest from the
                        // moment it's acknowledged, so this record is a candidate
                        // that failed verification and outlived its creator's
                        // delete, or whose put succeeded after the creator gave
                        // up. The collection skips the record when it gathers
                        // roots. Waiting for the grace window keeps the collection
                        // from deleting a candidate that its creator is still
                        // verifying or deleting (format spec, "Snapshot cleanup").
                        self.unverified_candidate(&namespace.namespace_id, &snapshot)
                    } else {
                        // A call that finishes a fork can find an initializing
                        // manifest that's already older than the grace window,
                        // and a delete and a collection can remove that manifest
                        // during the call, which then writes the fork point
                        // snapshot on the deleted manifest. Nothing reads through a
                        // deleted namespace's fork point snapshot, and the
                        // tombstone lists the same runs, so the collection loses no
                        // root by keeping the snapshot for retirement, which
                        // deletes it by ID (format spec, "Snapshot cleanup").
                        let left_to_retirement = own_fork_point && self.current_tombstone.is_some();
                        if matches!(decision, Decision::Retain { .. }) && !left_to_retirement {
                            return Err(missing_root_manifest(
                                &namespace.namespace_id,
                                *manifest_no,
                                &snapshot.key,
                            ));
                        }
                        decision
                    }
                }
                (_, Some(record)) => sweep_snapshot_record(&snapshot.key, record, self, clock),
                (_, None) => Decision::Gone,
            };
            self.snapshot_decisions.push_back(decision)?;
        }
        Ok(())
    }

    /// Deletes `snapshot`, an unverified user snapshot whose manifest is
    /// missing, once the record's provider age is at least the grace window,
    /// and keeps it otherwise.
    fn unverified_candidate(
        &self,
        namespace_id: &NamespaceId,
        snapshot: &ListedSnapshot,
    ) -> Decision<GcResponse> {
        let decision = Decision::aged(
            ListedObject {
                key: snapshot.key.clone(),
                last_modified_at_ms: snapshot.last_modified_at_ms,
                size_bytes: snapshot.size_bytes,
            },
            GC_GRACE_WINDOW_MS,
            self.now_ms,
            |report: &mut GcResponse| &mut report.deleted_snapshots_by_owner.user,
        );
        if let Decision::Delete { .. } = decision {
            tracing::warn!(
                namespace_id = %namespace_id,
                object_key = %snapshot.key,
                "deleting an unverified user snapshot whose manifest is missing"
            );
        }
        decision
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
/// retired: the grace window after its deletion.
pub(super) fn retirement_deadline_ms(tombstone: &NamespaceManifestPayload) -> u64 {
    tombstone
        .status
        .deleted_at_ms()
        .expect("a tombstone should carry its deletion stamp")
        .saturating_add(GC_GRACE_WINDOW_MS)
}
