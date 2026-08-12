//! Where a namespace's materialized metadata starts.
//!
//! A namespace publishes `metadata/root.json` at its first flush, not at
//! creation, so the basis is resolved from the head plus that root when it
//! exists (format spec, "Resolving the metadata basis"):
//!
//! 1. `metadata/root.json` present: the basis is the manifest it names,
//!    under this namespace's own prefix.
//! 2. Root absent, head carries no fork basis: the basis is the built-in
//!    genesis state — one root-inode row at sequence zero. No manifest
//!    object exists, and none was ever written.
//! 3. Root absent, head carries a fork basis: the basis is the source
//!    namespace's manifest, read under the source's prefix and validated
//!    against the identity and checksum the head recorded. A mismatch is
//!    corruption, never a fallback.

use crate::control_object::ControlObjectLoadError;
use crate::error::CoreError;
use crate::namespace::control::{
    read_head_and_metadata_root_if_present, read_wal_floor_object, LoadedHeadObject,
};
use loonfs_api::wire::control::HeadState;
use loonfs_api::{manifest_object_id_manifest_id, ChangeSeq, ManifestId, ManifestObjectId};
use loonfs_api::{NamespaceId, ROOT_INODE_ID};
use loonfs_objectstore::ObjectStore;

/// The materialized starting point every read and flush builds on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataBasis {
    /// The built-in genesis state: one root-inode row at sequence zero,
    /// synthesized rather than loaded. A created namespace reads from this
    /// until its first flush publishes a manifest.
    Genesis,
    /// A manifest object, owned by this namespace or — while the head still
    /// authorizes it — by the fork source.
    Manifest(BasisManifest),
}

/// The manifest a basis resolves to, and who owns the objects it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasisManifest {
    /// Namespace under whose prefix the manifest and its tables live.
    pub owner_namespace_id: NamespaceId,
    /// Logical manifest position, which the object id encodes.
    pub manifest_id: ManifestId,
    pub manifest_object_id: ManifestObjectId,
    /// The `payload_checksum` the loaded manifest must carry.
    pub manifest_payload_checksum: String,
}

/// The semantic identity of a metadata basis after its manifest has been
/// loaded and verified.
///
/// The resolved [`MetadataBasis`] owns the manifest object identity, owner,
/// logical position, and the checksum that authorized the load. The verified
/// manifest contributes its head sequence. Keeping these coordinates together
/// gives projection caches one value to compare without duplicating manifest
/// fields beside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataBasisIdentity {
    basis: MetadataBasis,
    manifest_head_seq: ChangeSeq,
}

impl MetadataBasisIdentity {
    pub(crate) fn from_verified_basis(basis: MetadataBasis, manifest_head_seq: ChangeSeq) -> Self {
        Self {
            basis,
            manifest_head_seq,
        }
    }

    pub(crate) fn basis(&self) -> &MetadataBasis {
        &self.basis
    }

    pub(crate) fn manifest_head_seq(&self) -> ChangeSeq {
        self.manifest_head_seq
    }
}

impl MetadataBasis {
    pub fn manifest(&self) -> Option<&BasisManifest> {
        match self {
            MetadataBasis::Genesis => None,
            MetadataBasis::Manifest(manifest) => Some(manifest),
        }
    }

    /// Logical position this basis sits at. Genesis is position zero: the
    /// namespace's first published manifest is one past it.
    pub fn manifest_id(&self) -> ManifestId {
        match self {
            MetadataBasis::Genesis => ManifestId(0),
            MetadataBasis::Manifest(manifest) => manifest.manifest_id,
        }
    }

    /// Whether the basis is a manifest this namespace itself published, so
    /// its own `metadata/root.json` exists.
    pub fn is_owned_by(&self, namespace_id: &NamespaceId) -> bool {
        self.manifest()
            .is_some_and(|manifest| manifest.owner_namespace_id == *namespace_id)
    }
}

/// The head and its resolved basis, read together.
pub(crate) struct LoadedNamespaceBasis {
    pub(crate) head: LoadedHeadObject,
    pub(crate) basis: MetadataBasis,
}

/// Reads the head and resolves the basis it authorizes.
pub(crate) async fn read_head_and_metadata_basis<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<LoadedNamespaceBasis, ControlObjectLoadError> {
    let (head, root) = read_head_and_metadata_root_if_present(store, namespace_id).await?;
    let basis = match root {
        Some(root) => MetadataBasis::Manifest(BasisManifest {
            owner_namespace_id: namespace_id.clone(),
            manifest_id: root.state.manifest_id,
            manifest_object_id: root.state.manifest_object_id,
            manifest_payload_checksum: root.state.manifest_payload_checksum,
        }),
        None => metadata_basis_without_root(&head.state)?,
    };
    Ok(LoadedNamespaceBasis { head, basis })
}

/// Resolves the basis of a namespace whose `metadata/root.json` is absent:
/// the built-in genesis state, or the fork source's manifest the head
/// authorizes.
pub(crate) fn metadata_basis_without_root(
    head: &HeadState,
) -> Result<MetadataBasis, ControlObjectLoadError> {
    let Some(fork_basis) = &head.fork_basis else {
        return Ok(MetadataBasis::Genesis);
    };
    let manifest_id = manifest_object_id_manifest_id(fork_basis.source_manifest_object_id.as_str())
        .ok_or_else(|| ControlObjectLoadError::Codec {
            object_key: loonfs_objectstore::keys::wal_head(head.namespace_id.as_str()),
            message: format!(
                "fork basis manifest object id `{}` does not encode a manifest id",
                fork_basis.source_manifest_object_id
            ),
        })?;
    Ok(MetadataBasis::Manifest(BasisManifest {
        owner_namespace_id: fork_basis.source_namespace_id.clone(),
        manifest_id,
        manifest_object_id: fork_basis.source_manifest_object_id.clone(),
        manifest_payload_checksum: fork_basis.source_manifest_checksum.clone(),
    }))
}

/// The genesis head fields a synthesized basis replays from: sequence zero,
/// the genesis commit, and the root inode already reserved.
pub(crate) fn genesis_next_inode_id() -> loonfs_api::InodeId {
    loonfs_api::InodeId(ROOT_INODE_ID.0 + 1)
}

/// The sequence a namespace's own history begins at: zero for a created
/// namespace, the fork point for a fork target.
pub(crate) fn namespace_birth_seq(head: &HeadState) -> ChangeSeq {
    head.fork_basis
        .as_ref()
        .map_or(ChangeSeq(0), |fork_basis| fork_basis.fork_seq)
}

/// Reads the retention floor, treating a missing floor object as the
/// namespace's birth sequence.
///
/// A namespace has no WAL history below its birth sequence, so "retain from
/// birth" is the most conservative reading of an absent floor: create and
/// fork write no floor, and the first advance publishes one.
pub(crate) async fn resolve_retention_floor_seq<S: ObjectStore + ?Sized>(
    store: &S,
    head: &HeadState,
) -> Result<ChangeSeq, ControlObjectLoadError> {
    match read_wal_floor_object(store, &head.namespace_id).await {
        Ok(loaded) => Ok(loaded.state.floor_seq),
        Err(ControlObjectLoadError::MissingObject { .. }) => Ok(namespace_birth_seq(head)),
        Err(error) => Err(error),
    }
}

/// Reports a basis that resolved past a materialized root that is not there.
///
/// The retention invariant keeps the floor at or below the materialized
/// root, so a floor above the namespace's birth sequence with no root object
/// means the root was lost, not that the namespace is young.
pub(crate) fn advanced_floor_without_root(
    namespace_id: &NamespaceId,
    floor_seq: ChangeSeq,
) -> CoreError {
    CoreError::NamespaceCorrupt(format!(
        "namespace `{namespace_id}` has no materialized metadata root but its retention floor \
         stands at `{floor_seq}`; the root object is missing"
    ))
}
