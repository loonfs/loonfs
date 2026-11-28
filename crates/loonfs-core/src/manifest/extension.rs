//! An extension's publications: the manifest that an extension's change
//! builds on, and the next manifest, which lists the change.

use super::published::PublicationReport;
use super::validate::validate_manifest;
use crate::commit::publication_lane::LaneTurn;
use crate::engine::{NamespaceEngine, RuntimeReadContext};
use crate::error::{CoreError, Result};
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::Held;
use crate::namespace::store::NamespaceStore;
use crate::publish::WriterPosition;
use crate::time::OperationClock;
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{ExtensionEntry, NamespaceManifestPayload};
use loonfs_types::{ExtensionName, NamespaceId};

/// The manifest that an extension's change builds on, and the clock of the
/// publication that ends the change.
///
/// The clock starts before the manifest is loaded, so the publication budget
/// covers every segment that the change writes before its publication
/// (format spec, "Publishing a manifest").
#[derive(Debug, Clone)]
pub struct ExtensionBasis {
    basis: Held<LoadedManifest>,
    clock: OperationClock,
}

impl ExtensionBasis {
    /// Returns the manifest that the change builds on.
    pub fn manifest(&self) -> &NamespaceManifestPayload {
        self.basis.manifest().payload()
    }

    /// Returns the entry of extension `name` in the manifest, or `None` when
    /// the manifest has none.
    pub fn find_entry(&self, name: &ExtensionName) -> Option<&ExtensionEntry> {
        self.manifest().extensions.get(name)
    }

    /// Returns how long ago the clock of this basis's publication started.
    pub fn elapsed_ms(&self) -> u64 {
        self.clock.elapsed_ms()
    }

    /// Checks that this basis is a manifest of `namespace_id`, the namespace
    /// that the change is published in, or returns
    /// `invalid_extension_basis`, which a caller returns before it claims
    /// anything.
    pub fn ensure_for(&self, namespace_id: &NamespaceId) -> Result<()> {
        let basis_namespace_id = &self.manifest().namespace_id;
        if basis_namespace_id == namespace_id {
            return Ok(());
        }
        Err(CoreError::InvalidExtensionBasis {
            namespace_id: namespace_id.clone(),
            basis_namespace_id: basis_namespace_id.clone(),
        })
    }
}

/// How an extension's publication ended.
#[derive(Debug, Clone)]
pub enum ExtensionPublication {
    /// The change is in the manifest after the session's newest published
    /// manifest.
    Published {
        /// The basis that the next change builds on: the published manifest,
        /// with a new publication clock.
        basis: ExtensionBasis,
    },
    /// Another publication changed the extension's entry after the basis,
    /// so the change wasn't published. The next change loads the current
    /// manifest.
    Superseded,
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Loads the namespace's current manifest as the basis of an extension's
    /// change, and starts the clock of the change's publication.
    pub async fn load_extension_basis(&self) -> Result<ExtensionBasis> {
        let clock = self.namespace.start_clock();
        let basis = self.namespace.load_live_basis(clock.origin()).await?;
        Ok(ExtensionBasis { basis, clock })
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Loads the manifest of `position`, a writer session's position, as the
    /// basis of an extension's change that the session publishes, and starts
    /// the clock of the change's publication. Returns `head_moved` when the
    /// manifest is missing from the store and at or below the manifest
    /// boundary (see [`Self::load_publication_basis`]).
    pub async fn load_position_extension_basis(
        &self,
        position: &WriterPosition,
    ) -> Result<ExtensionBasis> {
        let clock = self.start_clock();
        let basis = self.load_publication_basis(position).await?;
        Ok(ExtensionBasis { basis, clock })
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Publishes a change to the entry of extension `name` as one item of a
    /// writer session's publication lane, whose turn is `turn`, in the
    /// manifest after the turn's basis, the session's newest published
    /// manifest (see [`Self::publish_manifest_in_turn`]).
    ///
    /// The change is planned from `basis`, the manifest that the extension
    /// loaded its entry from, and the manifest lists `entry` as the
    /// extension's entry, with every other field from the turn's basis. When
    /// the basis's entry of `name` isn't `basis`'s (both can be absent), a
    /// newer change replaced the entry the change started from, such as when
    /// the index was disabled through the same writer session, so the call
    /// publishes nothing and returns
    /// [`ExtensionPublication::Superseded`] (format spec, "Extensions"). The
    /// put doesn't wait for the session's commits, and it records the
    /// published manifest as the session's newest published manifest, as a
    /// fold does. The report includes the manifest that the call published.
    ///
    /// # Panics
    ///
    /// Panics if the manifest would be invalid, such as when the entry lists
    /// a segment twice.
    pub async fn publish_extension_from(
        &self,
        turn: LaneTurn<'_>,
        basis: &ExtensionBasis,
        name: &ExtensionName,
        entry: ExtensionEntry,
    ) -> Result<PublicationReport<ExtensionPublication>> {
        let mut payload = turn.basis().manifest().payload().clone();
        if payload.extensions.get(name) != basis.find_entry(name) {
            return Ok(PublicationReport {
                response: ExtensionPublication::Superseded,
                published: None,
            });
        }
        let object_key = metadata_manifest_object(&payload.namespace_id, &payload.manifest_no);
        payload.extensions.insert(name.clone(), entry);
        validate_manifest(&object_key, &payload)
            .expect("an extension's publication should list valid runs");
        let published = self
            .publish_manifest_in_turn(turn, payload, &basis.clock)
            .await?;
        Ok(PublicationReport {
            response: ExtensionPublication::Published {
                basis: ExtensionBasis {
                    basis: published.clone(),
                    clock: self.start_clock(),
                },
            },
            published: Some(published),
        })
    }
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Returns the entry of extension `name` in the manifest that
    /// `context`'s reads start from, or `None` when that manifest has none.
    pub async fn find_extension_entry(
        &self,
        context: &RuntimeReadContext,
        name: &ExtensionName,
    ) -> Result<Option<ExtensionEntry>> {
        let decoded = self.namespace.load_context_manifest(context).await?;
        Ok(decoded.manifest.payload().extensions.get(name).cloned())
    }
}
