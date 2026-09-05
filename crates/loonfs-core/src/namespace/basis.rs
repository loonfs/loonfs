//! Resolves the metadata basis for a namespace.
//!
//! The basis is the namespace's metadata root when present, its fork source
//! manifest when recorded in the head, or the built-in genesis state before
//! the first flush. A fork basis must match the identity and checksum stored
//! in the head.

use loonfs_api::wire::control::{HeadState, ManifestRef};
use loonfs_api::NamespaceId;
use loonfs_api::{ChangeSeq, ManifestNo};

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

/// Resolves the basis of a namespace whose `metadata/root.json` is absent:
/// the built-in genesis state, or the fork source's manifest the head
/// authorizes.
pub(crate) fn metadata_basis_without_root(head: &HeadState) -> MetadataBasis {
    match &head.fork_basis {
        None => MetadataBasis::Genesis,
        Some(fork_basis) => MetadataBasis::Manifest(fork_basis.manifest.clone()),
    }
}

/// The sequence a namespace's own history begins at: zero for a created
/// namespace, the fork point for a fork target.
pub(crate) fn namespace_birth_seq(head: &HeadState) -> ChangeSeq {
    head.fork_basis.as_ref().map_or(ChangeSeq(0), |fork_basis| {
        fork_basis.manifest.manifest_head_seq
    })
}
