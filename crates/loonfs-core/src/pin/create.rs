//! Folds the WAL and creates a verified pin for the resulting manifest.

use super::record::{write_pin, PinBasisVerification};
use crate::control_update::{retry_while_contended, CasOutcome};
use crate::engine::{NamespaceEngine, Writable};
use crate::error::CoreError;
use crate::error::Result;
use crate::manifest::PublicationReport;
use crate::namespace::store::NamespaceStore;
use crate::time::OperationClock;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{PinOwner, PinPayload};
use loonfs_types::{PinId, Snapshot};

/// Longest accepted snapshot name. A label bound, not a durable format
/// limit.
const MAX_SNAPSHOT_NAME_CHARS: usize = 128;

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Creates a snapshot of the current namespace head.
    ///
    /// Every call generates a fresh `PinId`, and `name` is a label, not a
    /// key. When WAL objects follow the current manifest's `folded_wal_no`,
    /// the method first folds them into a new manifest without compacting
    /// metadata. A snapshot with `expires_at_ms` can't be read after that
    /// time, and it stays listed until collection deletes it. `None` keeps
    /// the snapshot until it's deleted. The report includes the manifest the
    /// fold published.
    pub async fn create_snapshot(
        &self,
        name: String,
        expires_at_ms: Option<u64>,
    ) -> Result<PublicationReport<Snapshot>> {
        self.namespace
            .create_pin(
                PinOwner::User {
                    name,
                    expires_at_ms,
                },
                &self.namespace.start_clock(),
            )
            .await
            .map(|report| report.map(super::snapshot_summary))
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Folds the WAL tail and creates a verified pin of the manifest that
    /// covers it, for `owner`.
    pub(crate) async fn create_pin(
        &self,
        owner: PinOwner,
        clock: &OperationClock,
    ) -> Result<PublicationReport<PinPayload>> {
        validate_pin_owner(&owner)?;
        let owner = &owner;
        // An attempt can publish its fold and then fail to write the pin, so the
        // report keeps the newest fold any attempt published.
        let published_slot = std::sync::Mutex::new(None);
        let published = &published_slot;
        let created = retry_while_contended(|| async move {
            let folded = self.fold_wal_tail(None, clock).await?;
            if let Some(manifest) = folded.published() {
                *published
                    .lock()
                    .expect("the published-fold slot should not be poisoned") = Some(manifest);
            }

            match self
                .create_pin_at_basis(owner.clone(), folded.manifest.manifest, clock)
                .await
            {
                Ok(pin) => Ok(CasOutcome::Settled(pin)),
                Err(error @ CoreError::CheckpointUnavailable(_)) => {
                    Ok(CasOutcome::Contended(error))
                }
                Err(error) => Err(error),
            }
        })
        .await?;
        Ok(PublicationReport {
            response: created?,
            published: published_slot
                .into_inner()
                .expect("the published-fold slot should not be poisoned"),
        })
    }

    pub(crate) async fn create_pin_at_basis(
        &self,
        owner: PinOwner,
        manifest: loonfs_types::format::control::ManifestRef,
        clock: &OperationClock,
    ) -> Result<PinPayload> {
        validate_pin_owner(&owner)?;
        let pin_id = PinId::generate(manifest.manifest_no);
        let record = PinPayload {
            pin_id: pin_id.clone(),
            namespace_id: self.namespace_id.clone(),
            head_seq: manifest.head_seq,
            payload_checksum: manifest.payload_checksum,
            created_at_ms: clock.request_at_ms(),
            owner,
        };
        let verification = async {
            write_pin(&self.store, &record).await?;
            self.verify_pin_basis(&record).await
        }
        .await;
        let error = match verification {
            Ok(PinBasisVerification::Verified) => return Ok(record),
            Ok(PinBasisVerification::Invalid) => CoreError::CheckpointUnavailable(
                "checkpoint publication retry exhausted".to_owned(),
            ),
            Err(error) => error,
        };
        self.delete_failed_pin(&pin_id, &error).await;
        Err(error)
    }
}

fn validate_pin_owner(owner: &PinOwner) -> Result<()> {
    match owner {
        PinOwner::User {
            name,
            expires_at_ms,
        } => {
            validate_snapshot_name(name)?;
            if *expires_at_ms == Some(0) {
                return Err(CoreError::InvalidSnapshotRequest(
                    "snapshot expiry must not be zero".to_owned(),
                ));
            }
            Ok(())
        }
        PinOwner::Fork { .. } => Ok(()),
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
