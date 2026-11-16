//! Creates a verified snapshot of a manifest that a caller's fold published or
//! found current.

use super::record::{write_snapshot_record, SnapshotBasisVerification};
use crate::engine::{NamespaceEngine, Writable};
use crate::error::CoreError;
use crate::error::Result;
use crate::namespace::basis::PublicationBasis;
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
    /// snapshot's verification returns `snapshot_unavailable`, so the caller
    /// folds and snapshots again. A snapshot with `expires_at_ms` can't be read
    /// after that time, and it stays listed until collection deletes it.
    /// `None` keeps the snapshot until it's deleted.
    pub async fn create_snapshot(
        &self,
        name: Option<String>,
        expires_at_ms: Option<u64>,
        basis: PublicationBasis,
    ) -> Result<Snapshot> {
        self.create_verified_snapshot(name, expires_at_ms, basis)
            .await
            .map(|(created, _)| super::snapshot_summary(created.record))
    }

    /// Creates a snapshot as [`Self::create_snapshot`] does, and returns the
    /// record with the version that its write stored, beside the newest
    /// manifest and the manifest boundary that the verification read.
    pub async fn create_verified_snapshot(
        &self,
        name: Option<String>,
        expires_at_ms: Option<u64>,
        basis: PublicationBasis,
    ) -> Result<(CreatedSnapshot, PublicationBasis)> {
        self.namespace
            .create_verified_snapshot_record(
                SnapshotOwner::User {
                    name,
                    expires_at_ms,
                    deleted_at_ms: None,
                },
                basis,
                &self.namespace.start_clock(),
            )
            .await
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
    /// Creates a verified snapshot of `basis`'s manifest for `owner`, and returns the
    /// snapshot record with the newest manifest and the manifest boundary that
    /// the verification read (see [`Self::create_verified_snapshot_record`]).
    pub(crate) async fn create_snapshot_record(
        &self,
        owner: SnapshotOwner,
        basis: PublicationBasis,
        clock: &OperationClock,
    ) -> Result<(SnapshotPayload, PublicationBasis)> {
        self.create_verified_snapshot_record(owner, basis, clock)
            .await
            .map(|(created, basis)| (created.record, basis))
    }

    /// Creates a verified snapshot of `basis`'s manifest for `owner`.
    ///
    /// `basis` contains a manifest and the manifest boundary that the caller
    /// read: the manifest that the caller's fold published or found current,
    /// or the current manifest that a discovery of the namespace's head
    /// found, for a snapshot without a fold (format spec, "Creating and
    /// verifying a snapshot"). The call returns `snapshot_unavailable` when
    /// the manifest stopped being current before the snapshot's
    /// verification, so the caller can read a fresh basis and take a snapshot
    /// again. Returns the snapshot record with the version that its write
    /// stored, and the newest manifest with the manifest boundary that the
    /// verification read.
    pub(crate) async fn create_verified_snapshot_record(
        &self,
        owner: SnapshotOwner,
        basis: PublicationBasis,
        clock: &OperationClock,
    ) -> Result<(CreatedSnapshot, PublicationBasis)> {
        validate_snapshot_owner(&owner)?;
        let manifest = basis.manifest.manifest();
        let snapshot_id = SnapshotId::generate(manifest.manifest_no);
        let record = SnapshotPayload {
            snapshot_id: snapshot_id.clone(),
            namespace_id: self.namespace_id.clone(),
            head_seq: manifest.head_seq,
            payload_checksum: manifest.payload_checksum,
            created_at_ms: clock.request_at_ms(),
            owner,
        };
        let verification = async {
            let written = write_snapshot_record(&self.store, &record).await?;
            Ok::<_, CoreError>((written.etag, self.verify_snapshot_basis(basis).await?))
        }
        .await;
        let error = match verification {
            Ok((etag, SnapshotBasisVerification::Verified(basis))) => {
                return Ok((CreatedSnapshot { record, etag }, basis))
            }
            Ok((_, SnapshotBasisVerification::Invalid)) => CoreError::SnapshotUnavailable(
                "the snapshot's manifest stopped being current before the snapshot record was verified".to_owned(),
            ),
            Err(error) => error,
        };
        self.delete_failed_snapshot_record(&snapshot_id, &error)
            .await;
        Err(error)
    }
}

fn validate_snapshot_owner(owner: &SnapshotOwner) -> Result<()> {
    match owner {
        SnapshotOwner::User {
            name,
            expires_at_ms,
            ..
        } => {
            if let Some(name) = name {
                validate_snapshot_name(name)?;
            }
            if *expires_at_ms == Some(0) {
                return Err(CoreError::InvalidSnapshotRequest(
                    "snapshot expiry must not be zero".to_owned(),
                ));
            }
            Ok(())
        }
        SnapshotOwner::Fork { .. } => Ok(()),
    }
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
