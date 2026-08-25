//! Resolves the metadata basis for a namespace.
//!
//! The basis is the namespace's metadata root when present, its fork source
//! manifest when recorded in the head, or the built-in genesis state before
//! the first flush. A fork basis must match the identity and checksum stored
//! in the head.

use crate::control_object::{
    reload_until_consistent, ControlObjectLoadError, CONTROL_READ_RELOADS,
};
use crate::namespace::control::{
    load_head_and_metadata_root_if_present, load_head_object, load_wal_floor_object,
    LoadedHeadObject,
};
use loonfs_api::wire::control::{HeadState, ManifestRef};
use loonfs_api::NamespaceId;
use loonfs_api::{ChangeSeq, ManifestNo};
use loonfs_objectstore::ObjectStore;

/// The materialized starting point every read and flush builds on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataBasis {
    /// The built-in genesis state: one root-inode row at sequence zero,
    /// synthesized rather than loaded. A created namespace reads from this
    /// until its first flush publishes a manifest.
    Genesis,
    /// A manifest owned by this namespace or its fork source.
    Manifest(ManifestRef),
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
    pub fn manifest(&self) -> Option<&ManifestRef> {
        match self {
            MetadataBasis::Genesis => None,
            MetadataBasis::Manifest(manifest) => Some(manifest),
        }
    }

    /// Logical position this basis sits at. Genesis is position zero: the
    /// namespace's first published manifest is one past it.
    pub fn manifest_no(&self) -> ManifestNo {
        match self {
            MetadataBasis::Genesis => ManifestNo(0),
            MetadataBasis::Manifest(manifest) => manifest.manifest_no,
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
pub(crate) async fn load_head_and_metadata_basis<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<LoadedNamespaceBasis, ControlObjectLoadError> {
    let mut reloads = 0;
    loop {
        let (head, root) = load_head_and_metadata_root_if_present(store, namespace_id).await?;
        if let Some(root) = root {
            return Ok(LoadedNamespaceBasis {
                head,
                basis: MetadataBasis::Manifest(root.state.manifest),
            });
        }

        // At the birth sequence, genesis or the fork source is still a complete
        // basis. A later floor proves a root was published, so retry before
        // reporting it missing.
        let floor_seq = resolve_retention_floor_seq(store, &head.state).await?;
        if floor_seq <= namespace_birth_seq(&head.state) {
            let basis = metadata_basis_without_root(&head.state);
            return Ok(LoadedNamespaceBasis { head, basis });
        }
        if reloads == CONTROL_READ_RELOADS {
            return Err(ControlObjectLoadError::MissingRootAfterFloor {
                namespace_id: namespace_id.clone(),
                floor_seq,
            });
        }
        reloads += 1;
    }
}

/// Resolves the basis of a namespace whose `metadata/root.json` is absent:
/// the built-in genesis state, or the fork source's manifest the head
/// authorizes.
pub(crate) fn metadata_basis_without_root(head: &HeadState) -> MetadataBasis {
    match &head.fork_basis {
        None => MetadataBasis::Genesis,
        Some(fork_basis) => MetadataBasis::Manifest(fork_basis.manifest.clone()),
    }
}

/// The genesis head fields a synthesized basis replays from: sequence zero,
/// the genesis commit, and the root inode already reserved.
pub(crate) fn genesis_next_inode_id() -> loonfs_api::InodeId {
    loonfs_api::FIRST_ALLOCATABLE_INODE_ID
}

/// The sequence a namespace's own history begins at: zero for a created
/// namespace, the fork point for a fork target.
pub(crate) fn namespace_birth_seq(head: &HeadState) -> ChangeSeq {
    head.fork_basis.as_ref().map_or(ChangeSeq(0), |fork_basis| {
        fork_basis.manifest.manifest_head_seq
    })
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
    match load_wal_floor_object(store, &head.namespace_id).await {
        Ok(loaded) => Ok(loaded.state.floor_seq),
        Err(ControlObjectLoadError::MissingObject { .. }) => Ok(namespace_birth_seq(head)),
        Err(error) => Err(error),
    }
}

/// Reads a head and retention floor that could have existed together.
///
/// The objects advance independently. A floor newer than the first head read
/// can therefore be a benign cross-read race, but it must not escape as a
/// snapshot with history retained beyond the reported namespace head.
pub(crate) async fn load_head_and_retention_floor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<(LoadedHeadObject, ChangeSeq), ControlObjectLoadError> {
    let head = load_head_object(store, namespace_id).await?;
    let floor_seq = resolve_retention_floor_seq(store, &head.state).await?;
    let head = reload_until_consistent(
        head,
        || load_head_object(store, namespace_id),
        |head| floor_seq <= head.state.seq,
        |head| ControlObjectLoadError::FloorAheadOfHead {
            floor_seq,
            head_seq: head.state.seq,
        },
    )
    .await?;
    Ok((head, floor_seq))
}
