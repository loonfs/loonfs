//! An extension's publications: the manifest that an extension's change
//! builds on, and the next manifest, which lists the change.

use super::publish::ManifestChange;
use super::published::PublicationReport;
use super::validate::validate_manifest;
use crate::engine::{NamespaceEngine, RuntimeReadContext, Writable};
use crate::error::Result;
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::Held;
use crate::time::OperationClock;
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{ExtensionEntry, NamespaceManifestPayload};
use loonfs_types::ExtensionName;

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
}

/// How an extension's publication ended.
#[derive(Debug, Clone)]
pub enum ExtensionPublication {
    /// The change is in the manifest that followed the basis, or in the
    /// manifest after one that followed it with the same entry. The basis
    /// that the next change builds on is that manifest, with a new
    /// publication clock.
    Published(ExtensionBasis),
    /// Another publication changed the extension's entry after the basis,
    /// so the change wasn't published. The next change loads the current
    /// manifest.
    Superseded,
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Loads the namespace's current manifest as the basis of an extension's
    /// change, and starts the clock of the change's publication.
    pub async fn load_extension_basis(&self) -> Result<ExtensionBasis> {
        let clock = self.namespace.start_clock();
        let basis = self.namespace.load_live_basis(clock.origin()).await?;
        Ok(ExtensionBasis { basis, clock })
    }

    /// Publishes a change to the entry of extension `name` in the manifest
    /// after `basis`, or after the manifest that followed `basis` when that
    /// manifest has the same entry.
    ///
    /// The manifest lists `entry` as the extension's entry, and every other
    /// field is the field of the manifest that the change is published over.
    /// The publication is a maintainer publication, so it copies the epochs
    /// of its predecessor and claims neither.
    ///
    /// After a lost put, the change is rebuilt over the manifest that won
    /// when that manifest's entry of `name` is the basis's (both can be
    /// absent), because a fold, a compaction, or a claim copies the entry and
    /// leaves the change valid. When the entry differs,
    /// another process planned a change from a different state, so the call
    /// returns [`ExtensionPublication::Superseded`] and publishes nothing
    /// (format spec, "Extensions"). The report includes the manifest that the
    /// call published.
    ///
    /// # Panics
    ///
    /// Panics if the manifest would be invalid, such as when the entry lists
    /// a segment twice.
    pub async fn publish_extension(
        &self,
        basis: &ExtensionBasis,
        name: &ExtensionName,
        entry: ExtensionEntry,
    ) -> Result<PublicationReport<ExtensionPublication>> {
        let built_on = basis.find_entry(name);
        let update = self
            .namespace
            .update_live_manifest(
                Some(basis.basis.clone()),
                &basis.clock,
                |payload: NamespaceManifestPayload| {
                    let change = extension_change(payload, name, built_on, &entry);
                    async move { Ok(change) }
                },
            )
            .await?;
        if !update.published {
            return Ok(PublicationReport {
                response: ExtensionPublication::Superseded,
                published: None,
            });
        }
        let published = update.current.clone();
        Ok(PublicationReport {
            response: ExtensionPublication::Published(ExtensionBasis {
                basis: update.current,
                clock: self.namespace.start_clock(),
            }),
            published: Some(published),
        })
    }
}

/// Returns the manifest after `current` with `entry` as the entry of
/// extension `name`, or `Finished` when `current`'s entry of `name` isn't
/// `built_on`.
fn extension_change(
    mut current: NamespaceManifestPayload,
    name: &ExtensionName,
    built_on: Option<&ExtensionEntry>,
    entry: &ExtensionEntry,
) -> ManifestChange<()> {
    if current.extensions.get(name) != built_on {
        return ManifestChange::Finished(());
    }
    let object_key = metadata_manifest_object(&current.namespace_id, &current.manifest_no);
    current.extensions.insert(name.clone(), entry.clone());
    validate_manifest(&object_key, &current)
        .expect("an extension's publication should list valid runs");
    ManifestChange::Next(Box::new(current), ())
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
