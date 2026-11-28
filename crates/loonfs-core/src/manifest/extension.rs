//! An extension's publications: the manifest that an extension's change
//! builds on, and the next manifest, which lists the change.

use super::publish::{ManifestChange, ManifestSender};
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
    /// The change is in the manifest that followed the session's position,
    /// or in the manifest after one that followed it with the same entry.
    Published {
        /// The basis that the next change builds on: the published manifest,
        /// with a new publication clock.
        basis: ExtensionBasis,
        /// Whether the change was built again over a manifest that won its
        /// first put, which copied the entry.
        rebuilt: bool,
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
    /// manifest after the manifest of the turn's position.
    ///
    /// The change is planned from `basis`, the manifest that the extension
    /// loaded its entry from, and the manifest lists `entry` as the
    /// extension's entry, with every other field from the manifest that the
    /// change is published over. The publication builds on the position's
    /// manifest, and returns `head_moved` when that manifest is missing from
    /// the store and at or below the manifest boundary, as every publication
    /// of a writer session does (see [`Self::load_publication_basis`]). It
    /// copies the epochs of its predecessor. When the position's manifest's
    /// entry of `name` isn't `basis`'s (both can be absent), the session's
    /// own disable or a newer change replaced the entry the change started
    /// from, so the call publishes nothing and returns
    /// [`ExtensionPublication::Superseded`] (format spec, "Extensions").
    /// After a lost put, the change is built again over the manifest that
    /// won under the same rule. A fold, a compaction, or a window change of
    /// another process copies the entry and leaves the change valid, and a
    /// manifest that won with another entry returns `head_moved` instead of
    /// `Superseded` (see `ManifestUpdate::published_or_unchanged`). The put
    /// doesn't wait for the session's commits, and the call records the
    /// published manifest as the session's newest published manifest, as a
    /// fold does (see
    /// [`NamespaceCommitEngine::published_manifest`](crate::publish::NamespaceCommitEngine::published_manifest)).
    /// The report includes the manifest that the call published.
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
        let built_on = basis.find_entry(name);
        let position_basis = self.load_publication_basis(turn.position()).await?;
        let position_manifest_no = position_basis.manifest_no();
        let mut first_built_on = None;
        let epoch = turn.epoch(self);
        let update = self
            .update_live_manifest(
                Some(position_basis),
                &basis.clock,
                ManifestSender::Session(&epoch),
                |payload: NamespaceManifestPayload| {
                    first_built_on.get_or_insert(payload.manifest_no);
                    let change = extension_change(payload, name, built_on, &entry);
                    async move { Ok(change) }
                },
            )
            .await?
            .published_or_unchanged(position_manifest_no)?;
        if !update.published {
            return Ok(PublicationReport {
                response: ExtensionPublication::Superseded,
                published: None,
            });
        }
        let published = update.current.clone();
        turn.record(&published);
        let first_built_on =
            first_built_on.expect("a published change should be built on a manifest");
        let rebuilt = first_built_on.successor().ok() != Some(published.manifest_no());
        Ok(PublicationReport {
            response: ExtensionPublication::Published {
                basis: ExtensionBasis {
                    basis: update.current,
                    clock: self.start_clock(),
                },
                rebuilt,
            },
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
