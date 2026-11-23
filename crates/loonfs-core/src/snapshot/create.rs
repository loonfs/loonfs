//! Creates a verified snapshot of a manifest that a caller's fold published or
//! found current, or that a caller's read of the namespace's head found.

use super::record::{write_snapshot_record, SnapshotBasisVerification};
use crate::engine::{Administrator, NamespaceEngine, RuntimeReadContext, Writable};
use crate::error::CoreError;
use crate::error::Result;
use crate::namespace::boundary::KeptBoundary;
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::Held;
use crate::namespace::store::NamespaceStore;
use crate::time::OperationClock;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{SnapshotOwner, SnapshotPayload};
use loonfs_types::{Snapshot, SnapshotId};

/// Longest accepted snapshot name. A label bound, not a durable format
/// limit.
const MAX_SNAPSHOT_NAME_CHARS: usize = 128;

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Creates a snapshot of `basis`, the manifest that the caller's fold of
    /// the WAL tail published or found current, with the manifest boundary
    /// that the fold read.
    ///
    /// Every call generates a fresh `SnapshotId`, and `name` is an optional
    /// label, not a key. A manifest that stopped being current before the
    /// snapshot's verification returns `stale_head`, so the caller folds and
    /// snapshots again. A snapshot with `expires_at_ms` can't be read
    /// after that time, and it stays listed until collection deletes it.
    /// `None` keeps the snapshot until it's deleted. `administrator` proves
    /// the caller administers the namespace, because snapshots are only for
    /// the administrator.
    pub async fn create_snapshot(
        &self,
        administrator: &Administrator,
        name: Option<String>,
        expires_at_ms: Option<u64>,
        basis: Held<LoadedManifest>,
    ) -> Result<Snapshot> {
        administrator.assert_for(&self.namespace.namespace_id);
        self.create_verified_snapshot(name, expires_at_ms, basis)
            .await
            .map(|(created, _)| super::snapshot_summary(created.record))
    }

    /// Creates a snapshot as [`Self::create_snapshot`] does, and returns the
    /// record with the version that its write stored, beside the newest
    /// manifest and the manifest boundary that the verification read.
    ///
    /// A merge calls this function to snapshot its fork, so the function
    /// makes no administrator check. A merge isn't only for the
    /// administrator, because it needs the subject's rights on the inodes it
    /// applies, and its snapshot keeps the fork's manifest safe from garbage
    /// collection while the merge plans and commits. The
    /// record that this call writes is a user snapshot like any other, so
    /// the merge sets an expiry on it and deletes it with
    /// [`Self::delete_created_snapshot`] once the merge's commit returns.
    pub async fn create_verified_snapshot(
        &self,
        name: Option<String>,
        expires_at_ms: Option<u64>,
        basis: Held<LoadedManifest>,
    ) -> Result<(CreatedSnapshot, Held<LoadedManifest>)> {
        self.namespace
            .create_verified_snapshot_record(
                name,
                expires_at_ms,
                basis.manifest(),
                basis.boundary().clone(),
                &self.namespace.start_clock(),
            )
            .await
    }

    /// Creates an unnamed snapshot that expires at `expires_at_ms` of the
    /// manifest that `context` reads, without folding the WAL tail first
    /// (format spec, "Creating and verifying a snapshot"), and returns the
    /// record with the version that its write stored.
    ///
    /// The manifest comes from the segment cache, or from the store after a
    /// check against the manifest boundary. If the manifest stopped being
    /// current before the load or before the snapshot's verification, the call
    /// returns `stale_head`, and the caller reads the namespace's head again
    /// and takes another snapshot.
    ///
    /// A merge calls this function to snapshot a fork that needs no fold, so
    /// the function makes no administrator check, for the reason that
    /// [`Self::create_verified_snapshot`] describes.
    pub async fn create_verified_snapshot_of_view(
        &self,
        expires_at_ms: Option<u64>,
        context: &RuntimeReadContext,
    ) -> Result<CreatedSnapshot> {
        let segments = self
            .namespace
            .load_held_manifest(
                context.read_basis().basis.manifest(),
                context.boundary().clone(),
            )
            .await?
            .ok_or_else(manifest_replaced)?;
        self.namespace
            .create_verified_snapshot_record(
                None,
                expires_at_ms,
                &segments.loaded_manifest(),
                context.boundary().clone(),
                &self.namespace.start_clock(),
            )
            .await
            .map(|(created, _)| created)
    }
}

/// A snapshot record that this process wrote, with the version of the record
/// that the write stored.
#[derive(Debug, Clone)]
pub struct CreatedSnapshot {
    /// The record as the write stored it.
    pub record: SnapshotPayload,
    /// The record's version, for a compare-and-swap, or `None` when the store
    /// returned no version.
    pub etag: Option<String>,
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Creates a snapshot of `basis`'s manifest for `owner`, and returns the
    /// snapshot record with the newest manifest and the manifest boundary that
    /// the verification read (see [`Self::create_verified_snapshot_record`]).
    /// A fork snapshot is written without verification, as a fork writes
    /// its fork snapshot, and the call returns `basis`.
    #[cfg(test)]
    pub(crate) async fn create_snapshot_record(
        &self,
        owner: SnapshotOwner,
        basis: Held<LoadedManifest>,
        clock: &OperationClock,
    ) -> Result<(SnapshotPayload, Held<LoadedManifest>)> {
        let SnapshotOwner::User {
            name,
            expires_at_ms,
            deleted_at_ms,
        } = owner
        else {
            let record = self.new_snapshot_record(owner, basis.manifest(), clock);
            write_snapshot_record(&self.store, &record, clock).await?;
            return Ok((record, basis));
        };
        assert_eq!(deleted_at_ms, None, "a created snapshot isn't deleted");
        self.create_verified_snapshot_record(
            name,
            expires_at_ms,
            basis.manifest(),
            basis.boundary().clone(),
            clock,
        )
        .await
        .map(|(created, basis)| (created.record, basis))
    }

    /// Creates a verified user snapshot of `manifest`, with the label `name`
    /// and the expiry `expires_at_ms`.
    ///
    /// `manifest` is the manifest that the caller's fold published or found
    /// current, or, for a snapshot without a fold, the manifest of the
    /// namespace's head. `kept` is the manifest boundary as the caller last
    /// read it (format spec, "Creating and verifying a snapshot"). The call
    /// returns `stale_head` when the manifest stopped being current before
    /// the snapshot's verification, so the caller can read a fresh basis and
    /// take a snapshot again. Returns the snapshot record with the version
    /// that its write stored, and the newest manifest with the manifest
    /// boundary that the verification read, checked at the start of `clock`.
    /// A call that fails after it starts writing the record leaves the record
    /// deleted at its key (see `delete_failed_snapshot`).
    pub(crate) async fn create_verified_snapshot_record(
        &self,
        name: Option<String>,
        expires_at_ms: Option<u64>,
        manifest: &LoadedManifest,
        kept: KeptBoundary,
        clock: &OperationClock,
    ) -> Result<(CreatedSnapshot, Held<LoadedManifest>)> {
        validate_snapshot_request(name.as_deref(), expires_at_ms)?;
        let record = self.new_snapshot_record(
            SnapshotOwner::User {
                name,
                expires_at_ms,
                deleted_at_ms: None,
            },
            manifest,
            clock,
        );
        let verification = async {
            let written = write_snapshot_record(&self.store, &record, clock).await?;
            let verified = self
                .verify_snapshot_basis(manifest, kept, clock.origin())
                .await?;
            Ok::<_, CoreError>((written.etag, verified))
        }
        .await;
        let error = match verification {
            Ok((etag, SnapshotBasisVerification::Verified(basis))) => {
                return Ok((CreatedSnapshot { record, etag }, basis))
            }
            Ok((_, SnapshotBasisVerification::Invalid)) => manifest_replaced(),
            // Only the record's write returns this error, and only when the
            // budget was spent before its first put, so a call that returns
            // it sent no put and has no record to delete. A write that sent a
            // put and then ran out of budget returns that put's store error,
            // and the cleanup below runs.
            Err(error @ CoreError::MetadataPublicationBudgetExceeded { .. }) => return Err(error),
            Err(error) => error,
        };
        self.delete_failed_snapshot(&record, clock, &error).await;
        Err(error)
    }

    /// Returns a record for `owner` of `manifest`, under a freshly generated
    /// [`SnapshotId`].
    fn new_snapshot_record(
        &self,
        owner: SnapshotOwner,
        manifest: &LoadedManifest,
        clock: &OperationClock,
    ) -> SnapshotPayload {
        let manifest = manifest.manifest();
        SnapshotPayload {
            snapshot_id: SnapshotId::generate(manifest.manifest_no),
            namespace_id: self.namespace_id.clone(),
            head_seq: manifest.head_seq,
            payload_checksum: manifest.payload_checksum,
            created_at_ms: clock.request_at_ms(),
            owner,
        }
    }
}

fn manifest_replaced() -> CoreError {
    CoreError::HeadMoved(
        "the snapshot's manifest stopped being current before the snapshot record was verified"
            .to_owned(),
    )
}

fn validate_snapshot_request(name: Option<&str>, expires_at_ms: Option<u64>) -> Result<()> {
    if let Some(name) = name {
        validate_snapshot_name(name)?;
    }
    if expires_at_ms == Some(0) {
        return Err(CoreError::InvalidSnapshotRequest(
            "snapshot expiry must not be zero".to_owned(),
        ));
    }
    Ok(())
}

fn validate_snapshot_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(CoreError::InvalidSnapshotRequest(
            "snapshot name must not be empty".to_owned(),
        ));
    }
    if name.chars().count() > MAX_SNAPSHOT_NAME_CHARS {
        return Err(CoreError::InvalidSnapshotRequest(format!(
            "snapshot name exceeds {MAX_SNAPSHOT_NAME_CHARS} characters"
        )));
    }
    Ok(())
}
