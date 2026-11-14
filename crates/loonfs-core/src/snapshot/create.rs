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
use loonfs_types::format::control::{ManifestRef, SnapshotOwner, SnapshotPayload};
use loonfs_types::{Snapshot, SnapshotId};

/// Longest accepted snapshot name. A label bound, not a durable format
/// limit.
const MAX_SNAPSHOT_NAME_CHARS: usize = 128;

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Creates a snapshot of `manifest`, the manifest that the caller's fold
    /// of the WAL tail published or found current.
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
        manifest: ManifestRef,
    ) -> Result<Snapshot> {
        self.namespace
            .create_snapshot_record(
                SnapshotOwner::User {
                    name,
                    expires_at_ms,
                    deleted_at_ms: None,
                },
                manifest,
                &self.namespace.start_clock(),
            )
            .await
            .map(|(record, _)| super::snapshot_summary(record))
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Creates a verified snapshot of `manifest` for `owner`.
    ///
    /// `manifest` is the manifest that the caller's fold published or found
    /// current. The call returns `snapshot_unavailable` when the manifest
    /// stopped being current before the snapshot's verification, so the caller
    /// can fold and take a snapshot again. Returns the snapshot record with
    /// the newest manifest and the manifest boundary that the verification
    /// read.
    pub(crate) async fn create_snapshot_record(
        &self,
        owner: SnapshotOwner,
        manifest: ManifestRef,
        clock: &OperationClock,
    ) -> Result<(SnapshotPayload, PublicationBasis)> {
        validate_snapshot_owner(&owner)?;
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
            write_snapshot_record(&self.store, &record).await?;
            self.verify_snapshot_basis(&record).await
        }
        .await;
        let error = match verification {
            Ok(SnapshotBasisVerification::Verified(basis)) => return Ok((record, basis)),
            Ok(SnapshotBasisVerification::Invalid) => CoreError::SnapshotUnavailable(
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
