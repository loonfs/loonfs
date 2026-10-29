//! The manifest that reads and publications build on.

use crate::namespace::control::LoadedManifest;
use crate::time::Observation;
use loonfs_types::format::control::ManifestRef;
use loonfs_types::ManifestNo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataBasis(pub ManifestRef);

impl MetadataBasis {
    pub fn manifest(&self) -> &ManifestRef {
        &self.0
    }
    pub fn manifest_no(&self) -> ManifestNo {
        self.0.manifest_no
    }
}

/// A manifest that a publication builds on, and when this process last
/// confirmed that the manifest was current.
///
/// A publication whose basis was checked less than
/// [`READ_REVALIDATION_BOUND_MS`](crate::limits::READ_REVALIDATION_BOUND_MS)
/// ago puts the basis's successor without a request first, and after a lost
/// put it finds the current manifest above the basis instead of through the
/// hint (format spec, "Publishing a materialized file set").
#[derive(Debug, Clone)]
pub struct PublicationBasis {
    pub manifest: LoadedManifest,
    /// When a store observation last found no successor of `manifest`, or of
    /// a manifest that `manifest` follows.
    pub checked: Observation,
}
