//! The manifest that reads and publications build on.

use crate::namespace::boundary::LoadedBoundary;
use crate::namespace::control::LoadedManifest;
use crate::namespace::state::NamespaceReadState;
use loonfs_types::format::control::ManifestRef;
use loonfs_types::ManifestNo;

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadBasis {
    pub head: NamespaceReadState,
    pub basis: MetadataBasis,
}

/// A manifest that a publication builds on, and the manifest boundary as
/// this process last read it.
///
/// A publication puts the basis's successor without a request first, and
/// after its put it reads the boundary conditionally on `boundary` (format
/// spec, "Publishing a manifest").
#[derive(Debug, Clone)]
pub struct PublicationBasis {
    pub manifest: LoadedManifest,
    /// The manifest boundary as last read, or `None` when no boundary object
    /// existed or none has been read.
    pub boundary: Option<LoadedBoundary>,
}
