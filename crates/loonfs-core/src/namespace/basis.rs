//! The manifest and head that reads build on.

use crate::namespace::state::NamespaceReadState;
use loonfs_types::format::control::ManifestRef;
use loonfs_types::{ManifestNo, SnapshotId};

/// The manifest that a read replays the WAL tail above.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataBasis(ManifestRef);

impl MetadataBasis {
    pub(crate) fn new(manifest: ManifestRef) -> Self {
        Self(manifest)
    }

    pub fn manifest(&self) -> &ManifestRef {
        &self.0
    }

    pub fn manifest_no(&self) -> ManifestNo {
        self.0.manifest_no
    }
}

/// A namespace head and the metadata basis that the head was read against,
/// which every read at the head starts from.
///
/// Code outside this crate can't build a read basis, so it can only use one
/// that this crate returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadBasis {
    pub(crate) head: NamespaceReadState,
    pub(crate) basis: MetadataBasis,
}

impl ReadBasis {
    /// Returns the head.
    pub fn head(&self) -> &NamespaceReadState {
        &self.head
    }

    /// Returns the metadata basis, the manifest that the head was read
    /// against.
    pub fn basis(&self) -> &MetadataBasis {
        &self.basis
    }
}

/// The head and metadata basis of the state that a snapshot kept.
///
/// Only the snapshot read-basis loaders return one, and
/// [`RuntimeReadContext::at_snapshot`](crate::RuntimeReadContext::at_snapshot)
/// is the one constructor that takes it, together with the live context at
/// whose head the snapshot's reads are authorized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotReadBasis {
    pub(crate) basis: ReadBasis,
    /// The snapshot, which keeps the manifest and its segments in the store.
    pub(crate) snapshot_id: SnapshotId,
}
