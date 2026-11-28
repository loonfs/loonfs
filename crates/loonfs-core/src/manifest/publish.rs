//! Publication through the next immutable manifest number.

use crate::commit::publication_lane::LaneTurn;
use crate::error::{CoreError, Result};
use crate::namespace::boundary::KeptBoundary;
use crate::namespace::control::{LoadedManifest, NewestManifest};
use crate::namespace::held::Held;
use crate::namespace::read_anchor::ensure_stored_writer_epoch;
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
    encode_namespace_manifest_json, NamespaceManifestEnvelope, NamespaceManifestPayload,
    MAX_NAMESPACE_MANIFEST_BYTES,
};
use loonfs_types::{ManifestNo, NamespaceId, PublicationId};
use std::sync::Arc;

/// The outcome of one manifest put, with the manifest that's current after
/// the put and the manifest boundary that the put's check read.
#[derive(Debug, Clone)]
pub(crate) enum ManifestPublicationOutcome {
    Published(Held<LoadedManifest>),
    /// Another publication's manifest is current, so the put was lost.
    Lost(Held<LoadedManifest>),
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

/// A namespace that a create or a fork found at its ID.
pub(crate) enum ExistingNamespace {
    /// The namespace's newest manifest is active.
    Active,
    /// The namespace's newest manifest is initializing.
    Initializing(Held<LoadedManifest>),
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Publishes `payload` as a publication of a writer session's
    /// publication lane, whose turn is `turn`: puts it once, as the successor of the
    /// turn's basis, the session's newest published manifest, under the
    /// session's epoch (see
    /// [`SessionEpoch::send_put`](crate::commit::session_epoch::SessionEpoch::send_put)), and records the
    /// published manifest as the session's newest published manifest before
    /// the lane takes the next publication (format spec, "Publishing a
    /// manifest").
    ///
    /// Only the namespace's writer publishes its manifests, and the
    /// session's publications take turns in its lane, so a lost put (a
    /// collision, or a put at a number that a collection emptied, which the
    /// boundary read reports at or below the boundary) can only have lost to
    /// another writer's claim. The call then returns `writer_fenced` with the
    /// claim's writer block, read from the manifest that won, records the
    /// session as fenced, and doesn't put again. A winner with the session's
    /// own epoch returns `namespace_corrupt`, because no other process
    /// publishes under that epoch. When the read of the manifest that won
    /// fails, the call returns that read's error, and the session keeps its
    /// epoch, so the next publication of the lane puts at the same number,
    /// loses, and reads the winner.
    ///
    /// # Panics
    ///
    /// Panics if `turn` is a turn of another namespace's lane, which is a
    /// bug in the calling process.
    pub(crate) async fn publish_manifest_in_turn(
        &self,
        turn: LaneTurn<'_>,
        payload: NamespaceManifestPayload,
        clock: &OperationClock,
    ) -> Result<Held<LoadedManifest>> {
        assert_eq!(
            turn.basis().manifest().payload().namespace_id,
            self.namespace_id,
            "a publication takes a turn of its own namespace's publication lane"
        );
        let outcome = turn
            .epoch(self)
            .send_put(self.publish_manifest_from(payload, clock, Some(turn.basis())))
            .await?;
        let winner = match outcome {
            ManifestPublicationOutcome::Published(published) => {
                turn.record(&published);
                return Ok(published);
            }
            ManifestPublicationOutcome::Lost(winner) => winner,
        };
        let error = match ensure_stored_writer_epoch(winner.manifest().payload(), turn.writer()) {
            Err(error) => error,
            Ok(()) => CoreError::NamespaceCorrupt(format!(
                "manifest {} of writer epoch {} won the put of the manifest after manifest {}, \
                 which this session published under that epoch",
                winner.manifest_no(),
                turn.writer().writer_epoch,
                turn.basis().manifest_no()
            )),
        };
        turn.record_error(&error);
        Err(error)
    }

    /// Publishes the successor of `current`, the newest manifest of an
    /// initializing namespace, that `next` builds from `current`'s payload,
    /// with one put. When a fork's manifest 2 won the put, the call puts once
    /// more over manifest 2, so an activation or a tombstone of a fork
    /// builds on the fork's manifest 2. The format allows three manifests
    /// after manifest 1 of an initializing namespace (a fork's manifest 2,
    /// the activation, and the tombstone), so the second put's outcome
    /// settles the namespace's status, and the caller returns a result that
    /// depends on the manifest that won.
    pub(crate) async fn publish_over_initializing(
        &self,
        current: Held<LoadedManifest>,
        clock: &OperationClock,
        next: impl Fn(&NamespaceManifestPayload) -> NamespaceManifestPayload,
    ) -> Result<ManifestPublicationOutcome> {
        let outcome = self
            .publish_manifest_from(next(current.manifest().payload()), clock, Some(&current))
            .await?;
        match outcome {
            ManifestPublicationOutcome::Lost(winner)
                if matches!(
                    winner.manifest().payload().status,
                    NamespaceStatus::Initializing {}
                ) =>
            {
                self.publish_manifest_from(next(winner.manifest().payload()), clock, Some(&winner))
                    .await
            }
            settled => Ok(settled),
        }
    }

    /// Returns the current manifest, with the manifest boundary, for a
    /// caller that keeps `basis` and the boundary `kept`. The returned
    /// manifest is checked at `observed`, an observation that the caller
    /// read before the call.
    ///
    /// The call first sends a HEAD request for the successor of `basis`'s
    /// manifest. When the successor exists, the call probes above the
    /// successor for the newest manifest (see [`find_newest_manifest_above`]),
    /// and otherwise keeps `basis`'s manifest. The call then reads the
    /// boundary (see [`Self::above_boundary`]). When the manifest that the
    /// call kept is at or below the boundary, a collection may have deleted
    /// the manifests above it, so the call finds the newest manifest from the
    /// boundary instead (format spec, "Publishing a manifest").
    pub(crate) async fn find_current_above(
        &self,
        basis: &LoadedManifest,
        kept: KeptBoundary,
        observed: Observation,
    ) -> Result<Held<LoadedManifest>> {
        let manifest = self
            .find_newest_manifest_above(basis)
            .await
            .map_err(CoreError::ControlObjectLoad)?
            .unwrap_or_else(|| basis.clone());
        let (above, boundary) = self
            .above_boundary(manifest.payload().manifest_no, kept)
            .await?;
        if above {
            return Ok(Held::new(manifest, observed, boundary));
        }
        self.load_current_basis(observed).await
    }

    /// Finds the namespace that has this ID for a create or a fork, and
    /// returns `namespace_deleted` for a tombstone or a retired namespace, so
    /// that a create or fork for a deleted ID writes nothing (format spec,
    /// "Creating a namespace").
    ///
    /// The newest manifest is checked at `observed`, an observation that the
    /// caller read before the call.
    pub(crate) async fn find_existing_namespace(
        &self,
        observed: Observation,
    ) -> Result<Option<ExistingNamespace>> {
        let current = match self.find_newest_manifest(observed).await? {
            NewestManifest::Absent => return Ok(None),
            NewestManifest::Retired => return Err(self.namespace_deleted()),
            NewestManifest::Manifest(current) => current,
        };
        match current.manifest().payload().status {
            NamespaceStatus::Initializing {} => Ok(Some(ExistingNamespace::Initializing(current))),
            NamespaceStatus::Active {} => Ok(Some(ExistingNamespace::Active)),
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
    ) -> Result<Held<LoadedManifest>> {
        match self.publish_manifest_from(payload, clock, None).await? {
            ManifestPublicationOutcome::Published(basis) => Ok(basis),
            ManifestPublicationOutcome::Lost(current) => Err(existing_namespace(
                self.namespace_id.clone(),
                current.manifest(),
            )),
        }
    }

    /// Publishes `payload` once with put-if-absent, as the successor of
    /// `current`, or as manifest 1 when `current` is `None`.
    ///
    /// Every manifest put goes through this call, which assigns the
    /// payload's manifest number and replaces its `publication_id` with a
    /// new ID before it encodes the payload, so a manifest with the same
    /// reference is this call's own put (format spec, "Conflicting and
    /// unknown installations"). A writer session's put comes through
    /// [`Self::publish_manifest_in_turn`], and a claim's and an initializing
    /// publication's put come here directly.
    ///
    /// After a put that loses, the call finds the current manifest by probing
    /// above `current` (see [`find_current_above`]). It finds the newest
    /// manifest from the manifest boundary instead when `current` is `None`.
    ///
    /// After a put that succeeds, the call reads the manifest boundary (see
    /// [`Self::settle_against_boundary`]). The call returns
    /// `outcome_unknown` when the put, or a check after it, failed in a way
    /// that leaves the put's outcome unknown, and every other result shows
    /// the outcome.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.phase",
        err(level = "warn"),
        skip_all,
        fields(phase = Phase::PublishManifest.as_str(), key_class = "manifest")
    )]
    pub(crate) async fn publish_manifest_from(
        &self,
        mut payload: NamespaceManifestPayload,
        clock: &OperationClock,
        current: Option<&Held<LoadedManifest>>,
    ) -> Result<ManifestPublicationOutcome> {
        payload.manifest_no = match current {
            Some(current) => super::fold::next_manifest_no_after(current.manifest_no())?,
            None => ManifestNo(1),
        };
        payload.publication_id = PublicationId::generate();
        let manifest = encode_manifest(payload);
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
        let object_key =
            metadata_manifest_object(namespace_id, &manifest.envelope().payload().manifest_no);
        let candidate = LoadedManifest {
            object_key,
            manifest_bytes: manifest.as_bytes().len() as u64,
            envelope: std::sync::Arc::new(manifest.envelope().clone()),
        };
        if let Some(current) = current {
            match classify_current(current, &candidate, expected_predecessor) {
                ManifestKind::Installable => {}
                ManifestKind::Settled(outcome) => return Ok(outcome),
            }
        }
        assert_legal_successor(manifest.envelope().payload(), current);
        clock.ensure_metadata_publication_budget(namespace_id)?;
        self.put_manifest(manifest, &candidate, current, expected_predecessor, clock)
            .await
    }

    /// Puts `manifest`, the encoding of `candidate`, with put-if-absent, and
    /// settles the put's outcome (see [`Self::publish_manifest_from`]).
    async fn put_manifest(
        &self,
        manifest: EncodedEnvelope<NamespaceManifestPayload>,
        candidate: &LoadedManifest,
        current: Option<&Held<LoadedManifest>>,
        expected_predecessor: Option<ManifestNo>,
        clock: &OperationClock,
    ) -> Result<ManifestPublicationOutcome> {
        let namespace_id = &self.namespace_id;
        let object_key = candidate.object_key.as_str();
        let outcome = match self
            .store
            .put_if_absent(object_key, Bytes::from(manifest.into_bytes()))
            .await
        {
            Ok(_) => {
                let outcome = self
                    .settle_against_boundary(
                        candidate,
                        expected_predecessor,
                        current
                            .map(|current| current.boundary().clone())
                            .unwrap_or_default(),
                        clock,
                    )
                    .await?;
                if matches!(outcome, ManifestPublicationOutcome::Published(_)) {
                    // This process's next read of the namespace starts from the
                    // published manifest.
                    super::load::cache_manifest(
                        &self.segment_cache,
                        candidate,
                        &Arc::new(runs_newest_first(candidate.payload())),
                    );
                }
                outcome
            }
            Err(ObjectStoreError::PreconditionFailed { .. }) => self
                .classify_current_manifest(current, candidate, expected_predecessor, clock)
                .await?
                .settled_after_conflict(namespace_id)?,
            Err(error @ ObjectStoreError::Transport { .. }) => {
                self.settle_unconfirmed_put(&error, candidate, current, expected_predecessor, clock)
                    .await?
            }
            Err(error) => {
                tracing::error!(
                    namespace_id = %namespace_id,
                    object_key = %object_key,
                    error = %error,
                    "manifest publication failed"
                );
                return Err(CoreError::store(object_key, &error));
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
    /// caller's next attempt builds on a manifest above the boundary. A read
    /// that fails leaves the outcome unknown, as a classification that can't
    /// tell does, so the call returns `outcome_unknown` for both.
    async fn settle_unconfirmed_put(
        &self,
        error: &ObjectStoreError,
        candidate: &LoadedManifest,
        current: Option<&Held<LoadedManifest>>,
        expected_predecessor: Option<ManifestNo>,
        clock: &OperationClock,
    ) -> Result<ManifestPublicationOutcome> {
        let message = match self
            .classify_unconfirmed_put(candidate, current, expected_predecessor, clock)
            .await
        {
            Ok(ManifestKind::Settled(outcome)) => return Ok(outcome),
            Ok(ManifestKind::Installable) => error.public_message().into_owned(),
            Err(
                settled @ (CoreError::OutcomeUnknown { .. }
                | CoreError::NamespaceExists { .. }
                | CoreError::NamespaceDeleted { .. }),
            ) => return Err(settled),
            Err(read) => read.to_string(),
        };
        let object_key = &candidate.object_key;
        tracing::error!(
            namespace_id = %self.namespace_id,
            object_key = %object_key,
            error = %error,
            "the outcome of a manifest publication is unknown"
        );
        Err(CoreError::OutcomeUnknown {
            object_key: object_key.clone(),
            message,
        })
    }

    /// Classifies `candidate`, whose put's response was lost, against the
    /// manifest at its number or the manifest that's current (see
    /// [`Self::settle_unconfirmed_put`]).
    async fn classify_unconfirmed_put(
        &self,
        candidate: &LoadedManifest,
        current: Option<&Held<LoadedManifest>>,
        expected_predecessor: Option<ManifestNo>,
        clock: &OperationClock,
    ) -> Result<ManifestKind> {
        // Only the exact manifest at this number confirms the put;
        // a later manifest may already cover it.
        let found = self
            .load_manifest_by_no(candidate.payload().manifest_no)
            .await
            .map_err(CoreError::ControlObjectLoad)?;
        let Some(found) = found else {
            return self
                .classify_current_manifest(current, candidate, expected_predecessor, clock)
                .await;
        };
        let kept = current
            .map(|current| current.boundary().clone())
            .unwrap_or_default();
        let (above, boundary) = self.candidate_above_boundary(candidate, kept).await?;
        if !above {
            return self
                .classify_current_manifest(None, candidate, expected_predecessor, clock)
                .await;
        }
        let checked = if found.manifest() == candidate.manifest() {
            // This call's own put, after the clock started.
            clock.origin()
        } else if let Some(current) = current {
            // Another publication's manifest after `current`: the check that
            // found `current` found no manifest after it, so none after this
            // later manifest either.
            current.checked().clone()
        } else {
            // Another creation's manifest 1, which a create reports as
            // `namespace_exists` or `namespace_deleted`.
            return Err(existing_namespace(self.namespace_id.clone(), &found));
        };
        Ok(classify_current(
            &Held::new(found, checked, boundary),
            candidate,
            expected_predecessor,
        ))
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
        kept: KeptBoundary,
        clock: &OperationClock,
    ) -> Result<ManifestPublicationOutcome> {
        let (above, boundary) = self.candidate_above_boundary(candidate, kept).await?;
        if above {
            return Ok(ManifestPublicationOutcome::Published(Held::new(
                candidate.clone(),
                clock.origin(),
                boundary,
            )));
        }
        self.classify_current_manifest(None, candidate, expected_predecessor, clock)
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
        kept: KeptBoundary,
    ) -> Result<(bool, KeptBoundary)> {
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
    /// `None`. That manifest is checked at the start of `clock`.
    async fn classify_current_manifest(
        &self,
        basis: Option<&Held<LoadedManifest>>,
        candidate: &LoadedManifest,
        expected_predecessor: Option<ManifestNo>,
        clock: &OperationClock,
    ) -> Result<ManifestKind> {
        let current = match basis {
            Some(basis) => {
                self.find_current_above(basis.manifest(), basis.boundary().clone(), clock.origin())
                    .await?
            }
            None => match self.find_newest_manifest(clock.origin()).await? {
                NewestManifest::Absent => return Ok(ManifestKind::Installable),
                NewestManifest::Retired => return Err(self.namespace_deleted()),
                NewestManifest::Manifest(current) => current,
            },
        };
        Ok(classify_current(&current, candidate, expected_predecessor))
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

/// Asserts that `payload` is a legal successor of the current manifest, or a
/// legal first manifest when there is none.
fn assert_legal_successor(
    payload: &NamespaceManifestPayload,
    current: Option<&Held<LoadedManifest>>,
) {
    let legal = match current {
        Some(current) => current
            .manifest()
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
    basis: &Held<LoadedManifest>,
    candidate: &LoadedManifest,
    expected_predecessor: Option<ManifestNo>,
) -> ManifestKind {
    let current = basis.manifest();
    // Only this call's put stores the candidate's publication ID.
    let outcome = if current.manifest() == candidate.manifest() {
        ManifestPublicationOutcome::Published(basis.clone())
    } else if Some(current.manifest().manifest_no) == expected_predecessor {
        return ManifestKind::Installable;
    } else {
        ManifestPublicationOutcome::Lost(basis.clone())
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
