//! A manifest that this process keeps in memory, with what the process
//! learned the last time it checked the manifest.

use crate::namespace::boundary::KeptBoundary;
use crate::time::Observation;

/// A manifest that this process keeps in memory, together with when the
/// process last found that no newer manifest existed and the manifest
/// boundary as the process last read it.
///
/// A process that keeps a manifest needs both facts to decide whether the
/// manifest is still the newest (format spec, "Checking an in-memory
/// manifest"). The check time bounds how old the process's view of the
/// namespace can be. The kept boundary makes the next read of the boundary
/// object conditional on its ETag, or lets the check skip that read. `M` is
/// the form in which the process keeps the manifest, such as the decoded
/// [`LoadedManifest`](crate::control::LoadedManifest) that a publication
/// builds the next manifest from.
///
/// Only code in `loonfs-core` can build a holder, and each caller passes the
/// time of the check that found the manifest. Code outside the crate can
/// read a holder and raise its boundary, but it can't change its manifest or
/// its check time.
#[derive(Debug, Clone)]
pub struct Held<M> {
    manifest: M,
    /// An observation read before the first request of the last check that
    /// found no manifest after `manifest`, so every manifest published before
    /// this observation is at or below `manifest`.
    checked: Observation,
    boundary: KeptBoundary,
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

    /// Keeps `boundary`, a boundary that a later read of the manifest
    /// boundary returned, when it's higher than the kept one.
    pub fn raise_boundary(&mut self, boundary: KeptBoundary) {
        self.boundary = std::mem::take(&mut self.boundary).higher(boundary);
    }
}
