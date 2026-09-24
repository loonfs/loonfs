//! The current manifest used by namespace readers.

use loonfs_api::wire::control::ManifestRef;
use loonfs_api::ManifestNo;

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
