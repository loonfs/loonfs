//! Pin deletion decisions, the collection rules for fork pins, and grace
//! window age checks.

use super::live_set::LiveSet;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::store::NamespaceStore;
use crate::snapshot::record::load_pin_at_key;
use crate::time::OperationClock;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{ForkBasis, PinOwner, PinPayload};
use loonfs_types::NamespaceId;

pub(super) enum PinSweep {
    DeleteFork,
    DeleteUser,
    Gone,
    /// Kept. `reclaimable_at_ms` is when a user pin becomes deletable.
    Retain {
        reclaimable_at_ms: Option<u64>,
    },
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Reads the pin record at `key` and decides what to do with the pin.
    pub(super) async fn sweep_pin(
        &self,
        key: &str,
        grace_window_ms: u64,
        live: &LiveSet,
        clock: &OperationClock,
    ) -> Result<PinSweep> {
        match self.load_listed_pin(key).await? {
            Some(record) => {
                self.sweep_pin_record(key, &record, grace_window_ms, live, clock)
                    .await
            }
            None => Ok(PinSweep::Gone),
        }
    }

    /// Decides what to do with the pin at `key`, whose record is `record`.
    pub(super) async fn sweep_pin_record(
        &self,
        key: &str,
        record: &PinPayload,
        grace_window_ms: u64,
        live: &LiveSet,
        clock: &OperationClock,
    ) -> Result<PinSweep> {
        let deletion = match &record.owner {
            PinOwner::User { .. } => PinSweep::DeleteUser,
            PinOwner::Fork {
                target_namespace_id,
            } => {
                return Ok(
                    match self
                        .fork_pin_is_retained(record, target_namespace_id, grace_window_ms, clock)
                        .await?
                    {
                        false => PinSweep::DeleteFork,
                        true => {
                            tracing::debug!(object_key = key, "retaining fork pin");
                            PinSweep::Retain {
                                reclaimable_at_ms: None,
                            }
                        }
                    },
                );
            }
        };
        let expires_at_ms = record
            .owner
            .expires_at_ms()
            .map(|expiry| expiry.saturating_add(grace_window_ms));
        let ages_out_at_ms = live
            .current_tombstone
            .is_some()
            .then(|| record.created_at_ms.saturating_add(grace_window_ms));
        let reclaimable_at_ms = expires_at_ms.into_iter().chain(ages_out_at_ms).min();
        Ok(match reclaimable_at_ms {
            Some(at_ms) if clock.request_at_ms() >= at_ms => deletion,
            _ => PinSweep::Retain { reclaimable_at_ms },
        })
    }

    /// Loads the pin record at `key`, a key from a listing, or returns `None`
    /// when the record is gone.
    pub(super) async fn load_listed_pin(&self, key: &str) -> Result<Option<PinPayload>> {
        match load_pin_at_key(&self.store, key).await {
            Ok(loaded) => Ok(Some(loaded.state)),
            Err(ControlObjectLoadError::MissingObject { .. }) => Ok(None),
            Err(error) => Err(CoreError::ControlObjectLoad(error)),
        }
    }

    async fn fork_pin_is_retained(
        &self,
        record: &PinPayload,
        target_namespace_id: &NamespaceId,
        grace_window_ms: u64,
        clock: &OperationClock,
    ) -> Result<bool> {
        if clock.request_at_ms().saturating_sub(record.created_at_ms) < grace_window_ms {
            return Ok(true);
        }
        // A target whose current manifest can't be loaded, whether the store
        // failed or the manifest is invalid, is a member whose roots the
        // pool's collection can't read. That stops only the pool's content
        // sweep (format spec, "One complete collection"), and keeping the pin
        // protects more, never less.
        match self
            .with_namespace(target_namespace_id.clone())
            .target_retains_pin(record)
            .await
        {
            Err(CoreError::ControlObjectLoad(error)) => {
                tracing::warn!(
                    namespace_id = %target_namespace_id,
                    error = %error,
                    "the fork target's current manifest couldn't be loaded; retaining its source pin"
                );
                Ok(true)
            }
            result => result,
        }
    }

    pub(super) async fn delete_source_pin(&self, basis: &ForkBasis) -> Result<bool> {
        let key =
            loonfs_objectstore::keys::pin(&basis.manifest.owner_namespace_id, &basis.source_pin_id);
        let present = self
            .store
            .get_with_metadata(&key)
            .await
            .map_err(|error| CoreError::store(&key, &error))?
            .is_some();
        self.with_namespace(basis.manifest.owner_namespace_id.clone())
            .delete_pin(&basis.source_pin_id)
            .await?;
        Ok(present)
    }
}

/// Where one unreferenced candidate stands against the grace window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GraceAge {
    /// The window has passed over the object.
    Aged,
    /// Younger than the window by its own provider timestamp.
    Young,
    /// The provider reported no last-modified time, so the object's age is
    /// unknown and it is treated as young (rule 1).
    Unknown,
}

impl GraceAge {
    /// Compares a listed modification time with the collection's grace window.
    pub(crate) fn of(last_modified_at_ms: Option<u64>, grace_window_ms: u64, now_ms: u64) -> Self {
        match last_modified_at_ms {
            None => Self::Unknown,
            Some(modified) if now_ms.saturating_sub(modified) < grace_window_ms => Self::Young,
            Some(_) => Self::Aged,
        }
    }
}
