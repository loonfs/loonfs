//! The manifest that reads and publications build on, and how a publication
//! finds the current manifest from a manifest it already has.

use crate::control_object::ControlObjectLoadError;
use crate::limits::MAX_POSITION_MOVES;
use crate::namespace::control::{load_successor_manifest, LoadedManifest};
use crate::namespace::read_anchor::manifest_has_successor;
use crate::time::Observation;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::ManifestRef;
use loonfs_types::{ManifestNo, NamespaceId};

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
/// ago finds the current manifest from the basis instead of from the hint
/// (format spec, "Publication and group commit").
#[derive(Debug, Clone)]
pub struct PublicationBasis {
    pub manifest: LoadedManifest,
    /// When a store observation last found no successor of `manifest`, or of
    /// a manifest that `manifest` follows.
    pub checked: Observation,
}

/// Returns the current manifest, found from `basis` with one HEAD of the
/// basis's successor, or by the walk in [`find_manifest_above`] when the
/// HEAD finds a successor.
///
/// Returns `None` when the basis's check is at the revalidation bound, when
/// the answer to a request arrives after the bound, or when the walk finds
/// more than [`MAX_POSITION_MOVES`] manifests. The caller then loads the
/// current manifest through the hint.
pub(crate) async fn find_current_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    basis: &PublicationBasis,
) -> Result<Option<LoadedManifest>, ControlObjectLoadError> {
    if !basis.checked.is_within_revalidation_bound() {
        return Ok(None);
    }
    let manifest_no = basis.manifest.state.envelope.payload().manifest_no;
    if manifest_has_successor(store, namespace_id, manifest_no).await? {
        return find_manifest_above(store, namespace_id, basis).await;
    }
    Ok(basis
        .checked
        .is_within_revalidation_bound()
        .then(|| basis.manifest.clone()))
}

/// Returns the current manifest for a publication whose basis has a
/// successor, found by loading each manifest after `basis` until a number is
/// missing.
///
/// Returns `None` in the same cases as [`find_current_manifest`].
///
/// Garbage collection keeps a manifest while its successor is younger than
/// the grace window. The basis had no successor when it was checked, so
/// every manifest above the basis was published after the check, and while
/// the check is within the revalidation bound, garbage collection hasn't
/// deleted any of them. A missing number therefore ends the walk at the
/// current manifest, and a put at that number can't create a deleted
/// manifest number again, beside the newer manifests.
pub(crate) async fn find_manifest_above<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    basis: &PublicationBasis,
) -> Result<Option<LoadedManifest>, ControlObjectLoadError> {
    if !basis.checked.is_within_revalidation_bound() {
        return Ok(None);
    }
    let mut current = basis.manifest.clone();
    let mut moves = 0;
    // A tombstone is the last manifest of its namespace.
    while !current.state.envelope.payload().status.is_deleted() {
        let Some(manifest) = load_successor_manifest(store, namespace_id, &current).await? else {
            break;
        };
        if moves == MAX_POSITION_MOVES {
            return Ok(None);
        }
        moves += 1;
        current = manifest;
    }
    Ok(basis
        .checked
        .is_within_revalidation_bound()
        .then_some(current))
}
