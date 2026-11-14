//! Publication through the next immutable manifest number.

use crate::error::{CoreError, Result};
use crate::limits::MAX_CONTENTION_ATTEMPTS;
use crate::manifest::published::{PublicationReport, PublishedManifest};
use crate::namespace::basis::PublicationBasis;
use crate::namespace::boundary::LoadedBoundary;
use crate::namespace::control::{ensure_namespace_live, LoadedManifest, NewestManifest};
use crate::namespace::store::NamespaceStore;
use crate::phase::Phase;
use crate::segment::runs::runs_newest_first;
use crate::time::{Observation, OperationClock};
use bytes::Bytes;
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use loonfs_types::format::control::{ManifestRef, NamespaceStatus};
use loonfs_types::format::envelope::EncodedEnvelope;
use loonfs_types::format::manifest::{
    encode_namespace_manifest_json, ExtensionEntry, NamespaceManifestEnvelope,
    NamespaceManifestPayload, MAX_NAMESPACE_MANIFEST_BYTES,
};
use loonfs_types::{ManifestNo, NamespaceId};
use std::collections::HashSet;
use std::sync::Arc;

/// The outcome of one manifest put, with the manifest that's current after
/// the put and the manifest boundary that the put's check read.
#[derive(Debug, Clone)]
pub(crate) enum ManifestPublicationOutcome {
    Published(PublicationBasis),
    CoveredByCurrent(PublicationBasis),
    PredecessorChanged(PublicationBasis),
}

enum ManifestKind {
    Installable,
    Settled(ManifestPublicationOutcome),
}

pub(crate) fn encode_manifest(
    payload: NamespaceManifestPayload,
) -> EncodedEnvelope<NamespaceManifestPayload> {
    encode_namespace_manifest_json(payload).expect("a namespace manifest should encode")
}

pub(crate) enum ManifestChange<T> {
    Next(Box<NamespaceManifestPayload>, T),
    Finished(T),
    Again,
}

/// What one call of [`NamespaceStore::update_manifest`] did: the response of
/// its last attempt, and the manifest that's current after the call.
pub(crate) struct ManifestUpdate<T> {
    pub(crate) response: T,
    /// The manifest that the call published, or that the last attempt built
    /// on when that attempt returned `Finished`, with the manifest boundary
    /// that the call read last.
    pub(crate) current: PublicationBasis,
    /// Whether the call published `current`.
    pub(crate) published: bool,
    /// When the caller started the call's clock.
    started: Observation,
}

impl<T> ManifestUpdate<T> {
    /// Returns the current manifest, with the start of the call's clock as
    /// its start observation: no manifest followed it then.
    pub(crate) fn current_manifest(&self) -> PublishedManifest {
        PublishedManifest::new(
            &self.current.manifest.envelope,
            Some(self.started.clone()),
            self.current.boundary.clone(),
        )
    }

    /// Returns the response, and the current manifest when the call
    /// published it.
    pub(crate) fn into_report(self) -> PublicationReport<T> {
        PublicationReport {
            published: self.published.then(|| self.current_manifest()),
            response: self.response,
        }
    }
}

/// A namespace that a create or a fork found at its ID.
pub(crate) enum ExistingNamespace {
    /// The namespace's newest manifest is active.
    Active(PublicationBasis),
    /// The namespace's newest manifest is initializing.
    Initializing(PublicationBasis),
}

/// Where the next attempt of [`update_manifest`] starts.
enum NextAttempt {
    /// Builds on this manifest without a request first.
    BuildOn(PublicationBasis),
    /// Builds on the newest manifest above this one, which the last attempt
    /// found to have a successor (see [`NamespaceStore::find_current_above`]).
    BuildAbove(PublicationBasis),
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Publishes the next manifest after the current one, and tries again after
    /// a lost put.
    ///
    /// Each attempt passes the manifest that it builds on to `change`. The first
    /// attempt builds on `basis` without a request, whatever its age, and
    /// without `basis` it finds the newest manifest from the manifest
    /// boundary. A `Next` payload is published at the next manifest number with
    /// put-if-absent, and the put's outcome is settled against the boundary
    /// (see [`publish_manifest_from`]). After a lost put, the next attempt
    /// builds on the manifest that won. After `Again`, the next attempt builds
    /// on the newest manifest above the one that the attempt built on. The call
    /// returns `store_contention` once [`MAX_CONTENTION_ATTEMPTS`] attempts
    /// have each lost their put or returned `Again`.
    /// The caller starts `clock` before the call, so
    /// `METADATA_PUBLICATION_BUDGET_MS` covers every attempt, from the first
    /// request to the last put.
    ///
    /// Why can an attempt put without checking its basis first? A put at a
    /// number that a collection emptied succeeds, but the boundary read after
    /// the put finds the number at or below the boundary, and the put counts
    /// as lost (format spec, "Publishing a manifest").
    pub(crate) async fn update_manifest<T, F, Fut>(
        &self,
        basis: Option<PublicationBasis>,
        clock: &OperationClock,
        mut change: F,
    ) -> Result<ManifestUpdate<T>>
    where
        F: FnMut(LoadedManifest) -> Fut,
        Fut: std::future::Future<Output = Result<ManifestChange<T>>>,
    {
        let mut next = basis.map(NextAttempt::BuildOn);
        let mut attempt = 0;
        let update = |response, current, published| ManifestUpdate {
            response,
            current,
            published,
            started: clock.origin(),
        };
        loop {
            attempt += 1;
            let current = match next.take() {
                Some(NextAttempt::BuildOn(basis)) => basis,
                Some(NextAttempt::BuildAbove(basis)) => self.find_current_above(&basis).await?,
                None => self.load_current_basis().await?,
            };
            let built_on = current.manifest.object_key.clone();
            let predecessor = current.manifest.payload().manifest_no;
            match change(current.manifest.clone()).await? {
                ManifestChange::Next(payload, result) => {
                    let mut payload = *payload;
                    payload.manifest_no = super::fold::next_manifest_no_after(predecessor)?;
                    let manifest = encode_manifest(payload);
                    let outcome = self
                        .publish_manifest_from(manifest, clock, Some(&current))
                        .await?;
                    let (lost_to, winner) = match outcome {
                        ManifestPublicationOutcome::Published(published) => {
                            return Ok(update(result, published, true));
                        }
                        ManifestPublicationOutcome::CoveredByCurrent(winner) => {
                            ("covered_by_current", winner)
                        }
                        ManifestPublicationOutcome::PredecessorChanged(winner) => {
                            ("predecessor_changed", winner)
                        }
                    };
                    tracing::debug!(
                        namespace_id = self.namespace_id.as_str(),
                        attempt,
                        attempts = MAX_CONTENTION_ATTEMPTS,
                        lost_to,
                        "a manifest publication lost its put"
                    );
                    // The lost put checked the winner against the manifest
                    // boundary.
                    next = Some(NextAttempt::BuildOn(winner));
                }
                ManifestChange::Finished(result) => {
                    return Ok(update(result, current, false));
                }
                ManifestChange::Again => next = Some(NextAttempt::BuildAbove(current)),
            }
            if attempt >= MAX_CONTENTION_ATTEMPTS {
                return Err(CoreError::contention_exhausted(&built_on));
            }
        }
    }

    /// Publishes the next manifest after the current one of a live namespace,
    /// through [`Self::update_manifest`].
    ///
    /// Each attempt returns `namespace_deleted` when the manifest that the
    /// attempt builds on is a tombstone, and otherwise passes a copy of that
    /// manifest's payload to `change`.
    pub(crate) async fn update_live_manifest<T, F, Fut>(
        &self,
        basis: Option<PublicationBasis>,
        clock: &OperationClock,
        mut change: F,
    ) -> Result<ManifestUpdate<T>>
    where
        F: FnMut(NamespaceManifestPayload) -> Fut,
        Fut: std::future::Future<Output = Result<ManifestChange<T>>>,
    {
        self.update_manifest(basis, clock, |current: LoadedManifest| {
            let payload = current.payload();
            let changed = ensure_namespace_live(&payload.namespace_id, payload.status)
                .map(|()| change(payload.clone()));
            async move { changed?.await }
        })
        .await
    }

    /// Returns the current manifest, found by probing above `basis` with
    /// [`find_newest_manifest_above`], or `basis`'s manifest when no manifest
    /// is above it, with the manifest boundary.
    ///
    /// After the probe, the call reads the boundary (see
    /// [`Self::above_boundary`]). When the manifest that the probe found, or
    /// `basis`'s manifest, is at or below the boundary, a collection may have
    /// deleted the manifests above it, so the call finds the newest manifest
    /// from the boundary instead (format spec, "Publishing a manifest").
    async fn find_current_above(&self, basis: &PublicationBasis) -> Result<PublicationBasis> {
        let manifest = self
            .find_newest_manifest_above(&basis.manifest)
            .await
            .map_err(CoreError::ControlObjectLoad)?
            .unwrap_or_else(|| basis.manifest.clone());
        let (above, boundary) = self
            .above_boundary(manifest.payload().manifest_no, basis.boundary.clone())
            .await?;
        if above {
            return Ok(PublicationBasis { manifest, boundary });
        }
        self.load_current_basis().await
    }

    /// Finds the namespace that has this ID for a create or a fork, and
    /// returns `namespace_deleted` for a tombstone or a retired namespace, so
    /// that a create or fork for a deleted ID writes nothing (format spec,
    /// "Creating a namespace").
    pub(crate) async fn find_existing_namespace(&self) -> Result<Option<ExistingNamespace>> {
        let current = match self.find_newest_manifest().await? {
            NewestManifest::Absent => return Ok(None),
            NewestManifest::Retired => return Err(self.namespace_deleted()),
            NewestManifest::Manifest(current) => current,
        };
        match current.manifest.payload().status {
            NamespaceStatus::Initializing {} => Ok(Some(ExistingNamespace::Initializing(current))),
            NamespaceStatus::Active {} => Ok(Some(ExistingNamespace::Active(current))),
            NamespaceStatus::Deleted { .. } => Err(self.namespace_deleted()),
        }
    }

    /// Publishes manifest 1 of a new namespace with put-if-absent, and
    /// returns manifest 1 with the manifest boundary that the put's check
    /// read, or `namespace_exists` or `namespace_deleted` when another
    /// creation's manifest 1 won the put, or when manifest 1 is at or below
    /// the manifest boundary.
    pub(crate) async fn publish_first_manifest(
        &self,
        payload: NamespaceManifestPayload,
        clock: &OperationClock,
    ) -> Result<PublicationBasis> {
        match self
            .publish_manifest_from(encode_manifest(payload), clock, None)
            .await?
        {
            ManifestPublicationOutcome::Published(basis) => Ok(basis),
            ManifestPublicationOutcome::CoveredByCurrent(current)
            | ManifestPublicationOutcome::PredecessorChanged(current) => Err(existing_namespace(
                self.namespace_id.clone(),
                &current.manifest,
            )),
        }
    }

    /// Publishes `manifest` once with put-if-absent, as the successor of
    /// `current`, or as manifest 1 when `current` is `None`.
    ///
    /// After a put that loses, the call finds the current manifest by probing
    /// above `current` (see [`find_current_above`]). It finds the newest
    /// manifest from the manifest boundary instead when `current` is `None`.
    ///
    /// After a put that succeeds, the call reads the manifest boundary (see
    /// [`Self::settle_against_boundary`]).
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.phase",
        err(level = "warn"),
        skip_all,
        fields(phase = Phase::PublishManifest.as_str(), key_class = "manifest")
    )]
    pub(super) async fn publish_manifest_from(
        &self,
        manifest: EncodedEnvelope<NamespaceManifestPayload>,
        clock: &OperationClock,
        current: Option<&PublicationBasis>,
    ) -> Result<ManifestPublicationOutcome> {
        let namespace_id = manifest.envelope().payload().namespace_id.clone();
        let namespace_id = &namespace_id;
        debug_assert_eq!(
            namespace_id, &self.namespace_id,
            "a manifest publishes through its own namespace's components"
        );
        // A reader refuses a larger document, so publishing one would leave the
        // namespace unreadable.
        if manifest.document_bytes() > MAX_NAMESPACE_MANIFEST_BYTES {
            return Err(CoreError::ManifestTooLarge {
                namespace_id: namespace_id.clone(),
                document_bytes: manifest.document_bytes(),
                max_bytes: MAX_NAMESPACE_MANIFEST_BYTES,
            });
        }
        let expected_predecessor = manifest
            .envelope()
            .payload()
            .manifest_no
            .0
            .checked_sub(1)
            .filter(|number| *number > 0)
            .map(ManifestNo);
        let references_own_object = references_own_object(manifest.envelope().payload(), current);
        let object_key =
            metadata_manifest_object(namespace_id, &manifest.envelope().payload().manifest_no);
        let candidate = LoadedManifest {
            object_key: object_key.clone(),
            manifest_bytes: manifest.as_bytes().len() as u64,
            envelope: std::sync::Arc::new(manifest.envelope().clone()),
        };
        if let Some(current) = current {
            let current_manifest = &current.manifest;
            // A tombstone ends its namespace; work that raced the deletion stops here.
            if current_manifest.payload().status.is_deleted()
                && current_manifest.manifest() != candidate.manifest()
            {
                return Err(CoreError::NamespaceDeleted {
                    namespace_id: namespace_id.clone(),
                });
            }
            if manifest.envelope().payload().writer_epoch < current_manifest.payload().writer_epoch
            {
                return Ok(ManifestPublicationOutcome::PredecessorChanged(
                    current.clone(),
                ));
            }
            match classify_current(current, &candidate, expected_predecessor, false) {
                ManifestKind::Installable => {}
                ManifestKind::Settled(outcome) => return Ok(outcome),
            }
        }
        assert_legal_successor(manifest.envelope().payload(), current);
        clock.ensure_metadata_publication_budget(namespace_id)?;
        let outcome = match self
            .store
            .put_if_absent(&object_key, Bytes::from(manifest.into_bytes()))
            .await
        {
            Ok(_) => {
                let outcome = self
                    .settle_against_boundary(
                        &candidate,
                        expected_predecessor,
                        current.and_then(|current| current.boundary.clone()),
                    )
                    .await?;
                if matches!(outcome, ManifestPublicationOutcome::Published(_)) {
                    // This process's next read of the namespace starts from the
                    // published manifest.
                    super::load::cache_manifest(
                        &self.segment_cache,
                        &candidate,
                        &Arc::new(runs_newest_first(candidate.payload())),
                    );
                }
                outcome
            }
            Err(ObjectStoreError::PreconditionFailed { .. }) => self
                .classify_current_manifest(current, &candidate, expected_predecessor, false)
                .await?
                .settled_after_conflict(namespace_id)?,
            Err(error @ ObjectStoreError::Transport { .. }) => {
                self.settle_unconfirmed_put(
                    &error,
                    &candidate,
                    current,
                    expected_predecessor,
                    references_own_object,
                )
                .await?
            }
            Err(error) => {
                tracing::error!(
                    namespace_id = %namespace_id,
                    object_key = %object_key,
                    error = %error,
                    "manifest publication failed"
                );
                return Err(CoreError::store(&object_key, &error));
            }
        };
        Ok(outcome)
    }

    /// Settles a manifest put whose response was lost: the put took effect
    /// only if the manifest at the candidate's number is the candidate, or a
    /// later manifest covers it.
    ///
    /// A manifest found at the candidate's number is checked against the
    /// manifest boundary first. When the number is at or below the boundary,
    /// the call classifies the candidate against the newest manifest from the
    /// boundary instead, as [`Self::settle_against_boundary`] does, so the
    /// caller's next attempt builds on a manifest above the boundary.
    async fn settle_unconfirmed_put(
        &self,
        error: &ObjectStoreError,
        candidate: &LoadedManifest,
        current: Option<&PublicationBasis>,
        expected_predecessor: Option<ManifestNo>,
        references_own_object: bool,
    ) -> Result<ManifestPublicationOutcome> {
        // Only the exact manifest at this number confirms the put;
        // a later manifest may already cover it.
        let found = self
            .load_manifest_by_no(candidate.payload().manifest_no)
            .await
            .map_err(CoreError::ControlObjectLoad)?;
        let outcome = match found {
            Some(found) => {
                let kept = current.and_then(|current| current.boundary.clone());
                let (above, boundary) = self.candidate_above_boundary(candidate, kept).await?;
                if above {
                    classify_current(
                        &PublicationBasis {
                            manifest: found,
                            boundary,
                        },
                        candidate,
                        expected_predecessor,
                        references_own_object,
                    )
                } else {
                    self.classify_current_manifest(None, candidate, expected_predecessor, false)
                        .await?
                }
            }
            None => {
                self.classify_current_manifest(
                    current,
                    candidate,
                    expected_predecessor,
                    references_own_object,
                )
                .await?
            }
        };
        let ManifestKind::Settled(outcome) = outcome else {
            let object_key = &candidate.object_key;
            tracing::error!(
                namespace_id = %self.namespace_id,
                object_key = %object_key,
                error = %error,
                "the outcome of a manifest publication is unknown"
            );
            return Err(CoreError::OutcomeUnknown {
                object_key: object_key.clone(),
                message: error.public_message().into_owned(),
            });
        };
        Ok(outcome)
    }

    /// Reads the manifest boundary after a put of `candidate` that succeeded,
    /// and returns `Published` when the candidate is above the boundary or the
    /// boundary object is missing (format spec, "Publishing a manifest").
    ///
    /// A candidate at or below the boundary is at a number that a collection
    /// emptied, so the put counts as lost. The call then finds the newest
    /// manifest from the boundary and classifies the candidate against that
    /// manifest, without a probe above the caller's basis, which is at or below
    /// the boundary too.
    ///
    /// The read is conditional on `kept`, the boundary of the publication's
    /// basis (see [`Self::candidate_above_boundary`]).
    async fn settle_against_boundary(
        &self,
        candidate: &LoadedManifest,
        expected_predecessor: Option<ManifestNo>,
        kept: Option<LoadedBoundary>,
    ) -> Result<ManifestPublicationOutcome> {
        let (above, boundary) = self.candidate_above_boundary(candidate, kept).await?;
        if above {
            return Ok(ManifestPublicationOutcome::Published(PublicationBasis {
                manifest: candidate.clone(),
                boundary,
            }));
        }
        self.classify_current_manifest(None, candidate, expected_predecessor, false)
            .await?
            .settled_after_conflict(&self.namespace_id)
    }

    /// Returns whether `candidate`, a manifest that this call put or read
    /// back at its number, is above the manifest boundary, with the boundary
    /// as it is now (see [`Self::above_boundary`]).
    ///
    /// A boundary read that fails returns `outcome_unknown`, because the call
    /// can't tell whether the manifest counts.
    async fn candidate_above_boundary(
        &self,
        candidate: &LoadedManifest,
        kept: Option<LoadedBoundary>,
    ) -> Result<(bool, Option<LoadedBoundary>)> {
        self.above_boundary(candidate.payload().manifest_no, kept)
            .await
            .map_err(|error| {
                tracing::error!(
                    namespace_id = %self.namespace_id,
                    object_key = %candidate.object_key,
                    error = %error,
                    "the outcome of a manifest publication is unknown"
                );
                CoreError::OutcomeUnknown {
                    object_key: candidate.object_key.clone(),
                    message: error.to_string(),
                }
            })
    }

    /// Classifies `candidate` against the manifest that's current after the
    /// candidate's put.
    ///
    /// The function finds that manifest by probing above `basis` (see
    /// [`find_current_above`]), or from the manifest boundary when `basis` is
    /// `None`.
    async fn classify_current_manifest(
        &self,
        basis: Option<&PublicationBasis>,
        candidate: &LoadedManifest,
        expected_predecessor: Option<ManifestNo>,
        confirm_authorship: bool,
    ) -> Result<ManifestKind> {
        let current = match basis {
            Some(basis) => self.find_current_above(basis).await?,
            None => match self.find_newest_manifest().await? {
                NewestManifest::Absent => return Ok(ManifestKind::Installable),
                NewestManifest::Retired => return Err(self.namespace_deleted()),
                NewestManifest::Manifest(current) => current,
            },
        };
        Ok(classify_current(
            &current,
            candidate,
            expected_predecessor,
            confirm_authorship,
        ))
    }
}

/// The error for a create or fork of a namespace ID that `current` holds.
fn existing_namespace(namespace_id: NamespaceId, current: &LoadedManifest) -> CoreError {
    if current.payload().status.is_deleted() {
        CoreError::NamespaceDeleted { namespace_id }
    } else {
        CoreError::NamespaceExists { namespace_id }
    }
}

/// Returns whether `payload` references an ID that this attempt generated.
///
/// Every manifest 1, which is the only manifest without a `current`, refers to
/// one (a plain create's pool ID, or the fork snapshot of a fork). A successor refers
/// to one when it lists a metadata or extension segment that `current`
/// doesn't list. Identical bytes prove authorship only for such a manifest. A
/// rival can rebuild any other manifest byte for byte.
fn references_own_object(
    payload: &NamespaceManifestPayload,
    current: Option<&PublicationBasis>,
) -> bool {
    let Some(current) = current else {
        return true;
    };
    let listed = segment_ids(current.manifest.payload()).collect::<HashSet<_>>();
    segment_ids(payload).any(|segment_id| !listed.contains(segment_id))
}

fn segment_ids(manifest: &NamespaceManifestPayload) -> impl Iterator<Item = &str> {
    manifest
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(|segment| segment.segment_id.as_str())
        .chain(
            manifest
                .extensions
                .values()
                .flat_map(ExtensionEntry::segments)
                .map(|segment| segment.segment_id.as_str()),
        )
}

/// Asserts that `payload` is a legal successor of the current manifest, or a
/// legal first manifest when there is none.
fn assert_legal_successor(payload: &NamespaceManifestPayload, current: Option<&PublicationBasis>) {
    let legal = match current {
        Some(current) => current
            .manifest
            .payload()
            .ensure_successor(payload)
            .map_err(|error| error.to_string()),
        None if payload.manifest_no == ManifestNo(1) => payload
            .ensure_first_manifest()
            .map_err(|error| error.to_string()),
        None => Err("has no predecessor".to_owned()),
    };
    legal.expect("a candidate manifest should be a legal successor of the current manifest");
}

fn classify_current(
    basis: &PublicationBasis,
    candidate: &LoadedManifest,
    expected_predecessor: Option<ManifestNo>,
    confirm_authorship: bool,
) -> ManifestKind {
    let current = &basis.manifest;
    let outcome = if current.manifest() == candidate.manifest() {
        if confirm_authorship {
            ManifestPublicationOutcome::Published(basis.clone())
        } else {
            ManifestPublicationOutcome::CoveredByCurrent(basis.clone())
        }
    } else if current.compactor_epoch() > candidate.compactor_epoch() {
        ManifestPublicationOutcome::PredecessorChanged(basis.clone())
    } else if current.folded_wal_no() >= candidate.folded_wal_no()
        && (current.manifest().head_seq, current.manifest().manifest_no)
            >= (
                candidate.manifest().head_seq,
                candidate.manifest().manifest_no,
            )
    {
        ManifestPublicationOutcome::CoveredByCurrent(basis.clone())
    } else if Some(current.manifest().manifest_no) == expected_predecessor {
        return ManifestKind::Installable;
    } else {
        ManifestPublicationOutcome::PredecessorChanged(basis.clone())
    };
    ManifestKind::Settled(outcome)
}

impl ManifestKind {
    fn settled_after_conflict(
        self,
        namespace_id: &NamespaceId,
    ) -> Result<ManifestPublicationOutcome> {
        match self {
            Self::Settled(outcome) => Ok(outcome),
            Self::Installable => Err(CoreError::NamespaceCorrupt(format!(
                "namespace `{namespace_id}` has no current manifest covering a taken manifest number"
            ))),
        }
    }
}

pub(crate) fn manifest_ref_for(manifest: &NamespaceManifestEnvelope) -> ManifestRef {
    ManifestRef {
        owner_namespace_id: manifest.payload().namespace_id.clone(),
        manifest_no: manifest.payload().manifest_no,
        head_seq: manifest.payload().head_seq,
        payload_checksum: manifest.payload_checksum().to_owned(),
    }
}
