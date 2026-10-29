//! Fork installation pins a source manifest and lists its run references in
//! a new namespace's first manifest; no segment or content object is copied.

use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::manifest::{load_namespace_manifest_envelope, PublicationReport};
use crate::namespace::store::NamespaceStore;
use crate::pin::ensure_unexpired;
use crate::pin::record::write_pin;
use crate::time::OperationClock;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{ForkBasis, PinOwner, PinPayload};
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{NamespaceId, NamespaceMetadata, PinId};

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Creates a new namespace at this namespace's current head or an unexpired snapshot.
    ///
    /// The fork shares immutable file bytes but gets its own metadata history.
    /// Returns the target's status at the fork point. The report includes the
    /// manifest of this namespace that the fork's fold published, if it
    /// published one.
    pub async fn fork_namespace(
        &self,
        new_namespace_id: &NamespaceId,
        actor_id: &loonfs_types::ActorId,
        snapshot_id: Option<&PinId>,
    ) -> Result<PublicationReport<NamespaceMetadata>> {
        let clock = self.namespace.start_clock();
        let target = self.namespace.with_namespace(new_namespace_id.clone());
        target.ensure_absent().await?;
        let owner = PinOwner::Fork {
            target_namespace_id: new_namespace_id.clone(),
        };
        let PublicationReport {
            response: source_record,
            published,
        } = if let Some(snapshot_id) = snapshot_id {
            PublicationReport {
                response: self
                    .namespace
                    .create_snapshot_fork_pin(snapshot_id, owner, &clock)
                    .await?,
                published: None,
            }
        } else {
            self.namespace.create_pin(owner, &clock).await?
        };
        let source_manifest = load_namespace_manifest_envelope(
            &self.namespace.store,
            &self.namespace.namespace_id,
            &source_record.pin_id.manifest_no(),
        )
        .await?;
        crate::manifest::ensure_manifest_reference_matches(
            "fork pin",
            &source_record.manifest(),
            &source_manifest,
        )?;
        let fork_basis = ForkBasis {
            manifest: source_record.manifest(),
            source_pin_id: source_record.pin_id.clone(),
        };
        let manifest = NamespaceManifestPayload::initial_fork(
            new_namespace_id.clone(),
            clock.request_at_ms(),
            actor_id.clone(),
            fork_basis,
            source_manifest.payload(),
        );
        if let Err(error) = target.create_manifest(manifest.clone(), &clock).await {
            if matches!(
                error,
                CoreError::NamespaceExists { .. } | CoreError::NamespaceDeleted { .. }
            ) {
                match target.target_retains_pin(&source_record).await {
                    Ok(true) => {}
                    Ok(false) => {
                        self.namespace
                            .delete_failed_pin(&source_record.pin_id, &error)
                            .await;
                    }
                    Err(check_error) => tracing::warn!(
                        namespace_id = %self.namespace.namespace_id,
                        pin_id = %source_record.pin_id,
                        original_error = %error,
                        check_error = %check_error,
                        "kept a fork pin whose target could not be read after installation failed"
                    ),
                }
            }
            return Err(error);
        }
        Ok(PublicationReport {
            response: crate::namespace::status::created_namespace(&manifest),
            published,
        })
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    async fn create_snapshot_fork_pin(
        &self,
        snapshot_id: &PinId,
        owner: PinOwner,
        clock: &OperationClock,
    ) -> Result<PinPayload> {
        let snapshot = self.load_snapshot_fork_basis(snapshot_id, clock).await?;
        let record = PinPayload {
            pin_id: PinId::generate(snapshot.pin_id.manifest_no()),
            created_at_ms: clock.request_at_ms(),
            owner,
            ..snapshot
        };
        let verification = async {
            write_pin(&self.store, &record).await?;
            self.load_snapshot_fork_basis(snapshot_id, clock).await
        }
        .await;
        if let Err(error) = verification {
            self.delete_failed_pin(&record.pin_id, &error).await;
            return Err(error);
        }
        Ok(record)
    }

    /// Loads an unexpired snapshot of a live source namespace.
    async fn load_snapshot_fork_basis(
        &self,
        snapshot_id: &PinId,
        clock: &OperationClock,
    ) -> Result<PinPayload> {
        let source_head = self.load_read_anchor(clock.observe()).await?.read_state;
        crate::namespace::control::ensure_namespace_live(
            &source_head.namespace_id,
            source_head.status,
        )?;
        let snapshot = self.load_user_pin(snapshot_id).await?;
        Ok(ensure_unexpired(snapshot, clock.now_ms())?.state)
    }

    pub(crate) async fn target_retains_pin(&self, record: &PinPayload) -> Result<bool> {
        let Some(target) = self.load_current_manifest_if_present().await? else {
            return Ok(false);
        };
        let Some(basis) = target.payload().fork_basis.as_ref() else {
            return Ok(false);
        };
        if basis.source_pin_id != record.pin_id {
            return Ok(false);
        }
        if basis.manifest != record.manifest() {
            return Err(CoreError::NamespaceCorrupt(format!(
                "fork target `{}` names pin `{}` with a different manifest reference",
                self.namespace_id, record.pin_id
            )));
        }
        Ok(true)
    }
}
