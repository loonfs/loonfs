//! A manifest that this process keeps in memory, with what the process
//! learned the last time it checked the manifest, and the check of whether
//! the manifest is still the newest.

use crate::error::Result;
use crate::namespace::basis::{MetadataBasis, ReadBasis};
use crate::namespace::boundary::KeptBoundary;
use crate::namespace::control::LoadedManifest;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::time::Observation;
use loonfs_objectstore::ObjectStore;
use loonfs_types::ManifestNo;

/// A manifest that this process keeps in memory, together with when the
/// process last found that no newer manifest existed and the manifest
/// boundary as the process last read it.
///
/// A process that keeps a manifest needs both facts to decide whether the
/// manifest is still the newest (format spec, "Checking an in-memory
/// manifest"). The check time bounds how old the process's view of the
/// namespace can be, and the kept boundary makes the next read of the
/// boundary object conditional, or lets the check skip that read.
///
/// A holder keeps its manifest in one of two forms, `M`. A publication, a
/// read anchor and a fold keep the decoded manifest ([`LoadedManifest`]),
/// because they build the next manifest from it or read its segments. The
/// namespace's cached head and a writer session's position keep a
/// [`ReadBasis`] instead: the manifest by reference, with the head replayed
/// above it. A cached head is one entry of a byte-budgeted cache, so it
/// refers to the manifest, and the decoded manifest stays in the manifest
/// cache.
///
/// Only code in `loonfs-core` builds a holder, from the discovery, the check
/// or the publication that found the manifest, so every check time comes
/// from a request that this process sent. Code outside the crate can read
/// and merge holders, but it can't put a manifest, a check time or a
/// boundary into one. (The `test-support` helper `held_manifest` builds one
/// for tests.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Held<M> {
    manifest: M,
    /// An observation read before the first request of the last check that
    /// found no manifest after `manifest`, so every manifest published before
    /// this observation is at or below `manifest`.
    checked: Observation,
    boundary: KeptBoundary,
}

/// A form in which a [`Held`] keeps its manifest: the decoded manifest, or a
/// read basis.
pub trait HeldManifestForm: sealed::Sealed {
    /// Returns the number of the kept manifest.
    fn manifest_no(&self) -> ManifestNo;
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::LoadedManifest {}
    impl Sealed for super::ReadBasis {}
}

impl HeldManifestForm for LoadedManifest {
    fn manifest_no(&self) -> ManifestNo {
        self.payload().manifest_no
    }
}

impl HeldManifestForm for ReadBasis {
    fn manifest_no(&self) -> ManifestNo {
        self.basis.manifest_no()
    }
}

/// What a check of a held manifest found (see
/// [`NamespaceStore::check_newest`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Checked {
    /// No manifest follows the held one, and the held one is above the
    /// manifest boundary.
    Newest,
    /// A manifest follows the held one.
    Successor,
    /// No manifest follows the held one, but the held one is at or below the
    /// manifest boundary, so a collection may have deleted the manifests
    /// after it.
    AtOrBelowBoundary,
}

impl<M> Held<M> {
    /// Returns a holder of `manifest`, last found the newest by a check that
    /// started at `checked`, with the manifest boundary `boundary`.
    pub(crate) fn new(manifest: M, checked: Observation, boundary: KeptBoundary) -> Self {
        Self {
            manifest,
            checked,
            boundary,
        }
    }

    /// Returns the kept manifest.
    pub fn manifest(&self) -> &M {
        &self.manifest
    }

    /// Returns when the last check that found no newer manifest started.
    pub fn checked(&self) -> &Observation {
        &self.checked
    }

    /// Returns the manifest boundary as this process last read it.
    pub fn boundary(&self) -> &KeptBoundary {
        &self.boundary
    }

    /// Returns the kept manifest, and drops the check time and the boundary.
    pub fn into_manifest(self) -> M {
        self.manifest
    }
}

impl<M: HeldManifestForm> Held<M> {
    /// Returns the number of the kept manifest.
    pub fn manifest_no(&self) -> ManifestNo {
        self.manifest.manifest_no()
    }

    /// Merges the check of `other`, a holder of a manifest of the same
    /// namespace, into this holder, and returns whether this holder changed.
    ///
    /// When `other`'s manifest is this holder's or an earlier one, this
    /// holder takes the younger of the two check times and the higher of the
    /// two boundaries. If no manifest followed an earlier manifest when its
    /// check began, none followed this one then either, because this one was
    /// published after that check began. A check of a later manifest says
    /// nothing about this one, so it changes nothing.
    pub fn merge<N: HeldManifestForm>(&mut self, other: &Held<N>) -> bool {
        if other.manifest_no() > self.manifest_no() {
            return false;
        }
        let younger = self.checked.age_at(&other.checked) > 0;
        if younger {
            self.checked = other.checked.clone();
        }
        let before = self.boundary.clone();
        self.boundary = std::mem::take(&mut self.boundary).higher(other.boundary.clone());
        younger || self.boundary != before
    }

    /// Returns whether a check that began at or after `other`'s confirms this
    /// holder: this holder's manifest is `other`'s or a later one, and its
    /// check began no earlier than `other`'s.
    pub fn is_confirmed_by<N: HeldManifestForm>(&self, other: &Held<N>) -> bool {
        self.manifest_no() >= other.manifest_no() && self.checked.age_at(&other.checked) == 0
    }
}

impl Held<LoadedManifest> {
    /// Returns the same manifest kept by reference, with `head`, the head
    /// replayed above it, and the same check.
    pub(crate) fn with_head(&self, head: NamespaceReadState) -> Held<ReadBasis> {
        Held {
            manifest: ReadBasis {
                head,
                basis: MetadataBasis::new(self.manifest.manifest()),
            },
            checked: self.checked.clone(),
            boundary: self.boundary.clone(),
        }
    }
}

impl Held<ReadBasis> {
    /// Returns the head and the manifest that it was read above.
    pub fn read_basis(&self) -> &ReadBasis {
        &self.manifest
    }

    /// Returns the head.
    pub fn head(&self) -> &NamespaceReadState {
        &self.manifest.head
    }

    /// Returns the head and the manifest that it was read above, so a WAL
    /// object can advance the head or a later manifest can move the basis.
    pub(crate) fn read_basis_mut(&mut self) -> &mut ReadBasis {
        &mut self.manifest
    }

    /// Returns the decoded form of this holder's manifest, `loaded`, which a
    /// load of the referenced manifest returned, with the same check, because
    /// a load isn't a check.
    pub(crate) fn decoded(&self, loaded: LoadedManifest) -> Held<LoadedManifest> {
        debug_assert_eq!(
            &loaded.manifest(),
            self.manifest.basis.manifest(),
            "a held reference is decoded by its own manifest"
        );
        Held {
            manifest: loaded,
            checked: self.checked.clone(),
            boundary: self.boundary.clone(),
        }
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Checks whether `held`'s manifest is still the newest: a HEAD request
    /// for its successor, and when there is none, a read of the manifest
    /// boundary conditional on the boundary that `held` keeps (format spec,
    /// "Checking an in-memory manifest").
    ///
    /// `observed` is an observation read before the HEAD request, or before
    /// an earlier request whose answer this check completes, such as a cached
    /// view's WAL probe. `held` keeps the boundary that the check read, and
    /// when the manifest is the newest, it takes `observed` as its check time
    /// if `observed` is younger than its own.
    pub async fn check_newest<M: HeldManifestForm>(
        &self,
        held: &mut Held<M>,
        observed: Observation,
    ) -> Result<Checked> {
        let manifest_no = held.manifest_no();
        if self.manifest_has_successor(manifest_no).await? {
            return Ok(Checked::Successor);
        }
        let (above, boundary) = self
            .above_boundary(manifest_no, held.boundary.clone())
            .await?;
        held.boundary = std::mem::take(&mut held.boundary).higher(boundary);
        if !above {
            return Ok(Checked::AtOrBelowBoundary);
        }
        if held.checked.age_at(&observed) > 0 {
            held.checked = observed;
        }
        Ok(Checked::Newest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_test_support::clock::ManualClock;
    use loonfs_types::format::manifest::NamespaceManifestPayload;
    use std::sync::Arc;

    /// Returns manifest `manifest_no` of namespace `demo`, checked at
    /// `checked`.
    fn manifest(manifest_no: u64, checked: &Observation) -> Held<LoadedManifest> {
        crate::test_support::held_manifest(
            NamespaceManifestPayload {
                manifest_no: ManifestNo(manifest_no),
                ..NamespaceManifestPayload::initial(
                    loonfs_test_support::ids::namespace_id("demo"),
                    loonfs_test_support::ids::numbered_pool_id(1),
                    1_000,
                    loonfs_test_support::test_actor(),
                )
            },
            checked.clone(),
        )
    }

    /// Checks that a holder takes the check of its own manifest or an
    /// earlier one only when that check is younger, and never the check of a
    /// later manifest.
    #[test]
    fn a_holder_takes_a_younger_check_of_its_own_or_an_earlier_manifest() {
        let clock = Arc::new(ManualClock::new(1_000));
        let early = Observation::now(clock.clone());
        clock.advance_ms(10);
        let own = Observation::now(clock.clone());
        clock.advance_ms(10);
        let late = Observation::now(clock.clone());
        for (other_no, other_checked, expected) in [
            (1, &late, &late),
            (1, &early, &own),
            (2, &late, &late),
            (2, &early, &own),
            (3, &late, &own),
        ] {
            let mut held = manifest(2, &own);
            let changed = held.merge(&manifest(other_no, other_checked));
            assert_eq!(held.checked(), expected, "a check of manifest {other_no}");
            assert_eq!(changed, expected != &own, "a check of manifest {other_no}");
        }
    }
}
