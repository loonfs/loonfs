//! What both collections read of one namespace's roots: the namespace's
//! retirement status, and its snapshots with the manifests that they keep.

use super::deletes::AgeWindow;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::limits::GC_GRACE_WINDOW_MS;
use crate::namespace::control::LoadedManifest;
use crate::namespace::store::NamespaceStore;
use crate::snapshot::record::snapshot_record_key_ids;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use loonfs_objectstore::keys::{metadata_manifest_object, snapshot_prefix};
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::{ListedObject, ObjectStore};
use loonfs_types::format::control::{NamespaceStatus, SnapshotOwner, SnapshotPayload};
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{ManifestNo, NamespaceId, SnapshotId};
use std::collections::BTreeSet;

/// Where a namespace stands against its
/// [retirement](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#95-retirement)
/// at a collection's clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RetirementStatus {
    /// The namespace's newest manifest is initializing, because a create or
    /// fork call hasn't finished the namespace. From `tombstone_at_ms`, the
    /// grace window after the manifest's `created_at_ms`, the creation counts
    /// as abandoned, and namespace collection publishes the namespace's
    /// tombstone.
    Initializing {
        tombstone_at_ms: u64,
    },
    Active,
    /// The namespace is deleted, and its retirement deadline is after the
    /// collection's clock.
    BeforeDeadline {
        retires_at_ms: u64,
    },
    /// The namespace is deleted, and the collection's clock is at or past its
    /// retirement deadline.
    PastDeadline,
}

impl RetirementStatus {
    /// Returns the status at `now_ms` of a namespace whose newest manifest is
    /// `head`.
    fn of(head: &NamespaceManifestPayload, now_ms: u64) -> Self {
        match head.status {
            NamespaceStatus::Initializing {} => Self::Initializing {
                tombstone_at_ms: AgeWindow::after(head.created_at_ms, GC_GRACE_WINDOW_MS)
                    .ends_at_ms,
            },
            NamespaceStatus::Active {} => Self::Active,
            NamespaceStatus::Deleted { .. } => {
                let deadline = retirement_deadline(head);
                if deadline.has_passed(now_ms) {
                    Self::PastDeadline
                } else {
                    Self::BeforeDeadline {
                        retires_at_ms: deadline.ends_at_ms,
                    }
                }
            }
        }
    }

    pub(super) fn is_deleted(self) -> bool {
        matches!(self, Self::BeforeDeadline { .. } | Self::PastDeadline)
    }

    /// Returns whether the layout view of the namespace's current manifest is
    /// a [content root](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#119-content-roots):
    /// while the namespace is active, and while a deleted namespace's
    /// retirement deadline is in the future.
    pub(super) fn current_view_is_root(self) -> bool {
        matches!(self, Self::Active | Self::BeforeDeadline { .. })
    }
}

/// Returns the window after the deletion that `tombstone` records, whose end
/// is the earliest time at which the namespace can be retired.
pub(super) fn retirement_deadline(tombstone: &NamespaceManifestPayload) -> AgeWindow {
    AgeWindow::after(
        tombstone
            .status
            .deleted_at_ms()
            .expect("a tombstone should record its deletion time"),
        GC_GRACE_WINDOW_MS,
    )
}

/// The roots of one namespace at a collection's clock: the namespace's
/// retirement status, computed from the newest manifest that the collection
/// found, and the namespace's snapshots with the manifests that they keep.
pub(super) struct NamespaceRoots<'a, S: ObjectStore> {
    pub(super) namespace: &'a NamespaceStore<S>,
    head: &'a NamespaceManifestPayload,
    pub(super) status: RetirementStatus,
}

/// One key that a listing of a namespace's snapshots returned.
#[allow(
    clippy::large_enum_variant,
    reason = "almost every listed key is a snapshot, moved once: indirection would buy an allocation and save nothing"
)]
pub(super) enum ListedSnapshotKey {
    /// A key under the snapshots prefix that isn't a snapshot record, with the
    /// object's size as the listing reported it.
    Unrecognized {
        size_bytes: u64,
    },
    Snapshot(ListedSnapshot),
}

/// One listed snapshot, with the manifest that it keeps.
pub(super) struct ListedSnapshot {
    /// The snapshot record's key, size, and provider modification time, as
    /// the listing reported them.
    pub(super) entry: ListedObject,
    pub(super) snapshot_id: SnapshotId,
    pub(super) manifest: SnapshotManifest,
}

/// The manifest that one listed snapshot keeps.
pub(super) enum SnapshotManifest {
    /// The first listed snapshot of a manifest that no earlier root loaded.
    Loaded(LoadedManifest),
    /// A manifest that is already a root, so the listing didn't load it: the
    /// newest manifest, or a manifest that an earlier snapshot loaded.
    /// Namespace collection also puts this value in place of a loaded
    /// manifest once it has protected that manifest.
    Rooted,
    /// A manifest that the store doesn't have, kept by a snapshot that
    /// protects nothing.
    Missing(MissingManifest),
}

/// The reason that a snapshot whose manifest is missing protects nothing.
/// The reason comes from the snapshot's record, which the listing reads after
/// it finds the manifest missing.
pub(super) enum MissingManifest {
    /// The record is gone, because another collection, a retirement, or the
    /// creator of a candidate that failed verification deleted the snapshot
    /// after the listing.
    RecordGone,
    /// A user snapshot that was never verified, with its record.
    UnverifiedUser(SnapshotPayload),
    /// A deleted namespace's own fork point snapshot, which retirement
    /// deletes.
    DeletedOwnForkPoint,
}

impl<'a, S: ObjectStore> NamespaceRoots<'a, S> {
    /// Returns the roots of `namespace`, whose newest manifest is `head`, at
    /// `now_ms`.
    pub(super) fn new(
        namespace: &'a NamespaceStore<S>,
        head: &'a NamespaceManifestPayload,
        now_ms: u64,
    ) -> Self {
        Self {
            namespace,
            head,
            status: RetirementStatus::of(head, now_ms),
        }
    }

    /// Returns whether `snapshot_id` is the namespace's own fork point
    /// snapshot. Every manifest of a fork records that snapshot's ID in its
    /// fork basis.
    pub(super) fn is_own_fork_point(&self, snapshot_id: &SnapshotId) -> bool {
        self.head
            .fork_basis
            .as_ref()
            .is_some_and(|basis| basis.fork_point_snapshot_id == *snapshot_id)
    }

    /// Lists the namespace's snapshots and loads the manifest that each
    /// snapshot keeps, with up to `STORE_READS_IN_FLIGHT` snapshots read at
    /// once, in listing order. The manifest number is part of the snapshot
    /// key, so the listing reads no snapshot record to find it.
    ///
    /// The first listed snapshot of a manifest number other than the newest
    /// manifest's loads that manifest, and later snapshots of the same
    /// manifest number use the result. Another collector can remove a
    /// released snapshot's manifest after the listing, and another snapshot
    /// of the same manifest number can still require the manifest, so every
    /// snapshot whose manifest is missing is checked on its own (see
    /// [`Self::missing_manifest`]). Such a snapshot protects nothing, or the
    /// stream ends with a corruption error.
    pub(super) fn snapshots(&self) -> BoxStream<'_, Result<ListedSnapshotKey>> {
        let namespace = self.namespace;
        let prefix = snapshot_prefix(&namespace.namespace_id);
        let mut requested = BTreeSet::from([self.head.manifest_no]);
        let mut rooted = BTreeSet::from([self.head.manifest_no]);
        namespace
            .store
            .list_entries_from_stream(&prefix, None)
            .map_err(move |error| CoreError::store(&prefix, &error))
            .map_ok(move |entry| {
                let snapshot_id = snapshot_id_of(&entry.key);
                let first =
                    matches!(&snapshot_id, Ok(Some(id)) if requested.insert(id.manifest_no()));
                async move {
                    let Some(snapshot_id) = snapshot_id? else {
                        return Ok((entry, None));
                    };
                    let loaded = if first {
                        namespace
                            .load_root_manifest(snapshot_id.manifest_no())
                            .await?
                    } else {
                        None
                    };
                    Ok((entry, Some((snapshot_id, loaded))))
                }
            })
            // `try_buffered` keeps listing order, so a snapshot that reuses an
            // earlier snapshot's load arrives after that load's result.
            .try_buffered(STORE_READS_IN_FLIGHT)
            .and_then(move |(entry, listed)| {
                let listed = listed.map(|(snapshot_id, loaded)| {
                    let manifest_no = snapshot_id.manifest_no();
                    let manifest = match loaded {
                        Some(manifest) => {
                            rooted.insert(manifest_no);
                            Some(SnapshotManifest::Loaded(manifest))
                        }
                        None if rooted.contains(&manifest_no) => Some(SnapshotManifest::Rooted),
                        None => None,
                    };
                    (snapshot_id, manifest)
                });
                async move {
                    let Some((snapshot_id, manifest)) = listed else {
                        return Ok(ListedSnapshotKey::Unrecognized {
                            size_bytes: entry.size_bytes,
                        });
                    };
                    let manifest = match manifest {
                        Some(manifest) => manifest,
                        None => SnapshotManifest::Missing(
                            self.missing_manifest(&entry.key, &snapshot_id).await?,
                        ),
                    };
                    Ok(ListedSnapshotKey::Snapshot(ListedSnapshot {
                        entry,
                        snapshot_id,
                        manifest,
                    }))
                }
            })
            .boxed()
    }

    /// Reads the record of the snapshot `snapshot_id` at `key`, whose
    /// manifest is missing, and returns why the snapshot protects nothing, or
    /// the corruption error of a snapshot that keeps a missing manifest.
    ///
    /// The record is read after the listing found the manifest missing, so a
    /// snapshot that was released during the listing reads as gone. A
    /// snapshot's owner can't change after the snapshot is created.
    async fn missing_manifest(
        &self,
        key: &str,
        snapshot_id: &SnapshotId,
    ) -> Result<MissingManifest> {
        match self.namespace.load_listed_snapshot_record(key).await? {
            None => Ok(MissingManifest::RecordGone),
            // A verified user snapshot protects its manifest from the moment
            // it's acknowledged, so a user snapshot whose manifest is missing
            // is a candidate that failed verification and outlived its
            // creator's delete, or whose put succeeded after the creator gave
            // up (format spec, "Snapshot cleanup").
            Some(record) if matches!(record.owner, SnapshotOwner::User { .. }) => {
                Ok(MissingManifest::UnverifiedUser(record))
            }
            // A call that finishes a fork can find an initializing manifest
            // that's already older than the grace window, and a delete and a
            // collection can remove that manifest during the call, and the
            // call then writes the fork point snapshot on the deleted
            // manifest. Nothing reads through a deleted namespace's fork point
            // snapshot, and the tombstone lists the same runs (format spec,
            // "Snapshot cleanup").
            Some(_) if self.status.is_deleted() && self.is_own_fork_point(snapshot_id) => {
                Ok(MissingManifest::DeletedOwnForkPoint)
            }
            Some(_) => Err(missing_root_manifest(
                &self.namespace.namespace_id,
                snapshot_id.manifest_no(),
                key,
            )),
        }
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Loads one root manifest through the segment cache, or returns `None`
    /// when the manifest is missing.
    async fn load_root_manifest(&self, manifest_no: ManifestNo) -> Result<Option<LoadedManifest>> {
        Ok(self
            .load_manifest_if_present(manifest_no)
            .await
            .map_err(MetadataProjectionLoadError::from)?)
    }
}

/// Returns the snapshot ID in a snapshot key, or `None` for a key that isn't
/// a snapshot record.
fn snapshot_id_of(key: &str) -> Result<Option<SnapshotId>> {
    if parse_object_key(key).map(|parsed| parsed.family()) != Some(DurableObjectFamily::Snapshot) {
        return Ok(None);
    }
    let (_, snapshot_id) = snapshot_record_key_ids(key).map_err(CoreError::ControlObjectLoad)?;
    Ok(Some(snapshot_id))
}

/// Returns the corruption error of a snapshot at `root_key` whose manifest
/// `manifest_no` of `namespace_id` is missing.
fn missing_root_manifest(
    namespace_id: &NamespaceId,
    manifest_no: ManifestNo,
    root_key: &str,
) -> CoreError {
    let key = metadata_manifest_object(namespace_id, &manifest_no);
    CoreError::NamespaceCorrupt(format!("root `{root_key}` keeps missing manifest `{key}`"))
}
