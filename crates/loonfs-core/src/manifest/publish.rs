//! Publication through the next immutable manifest number.

use crate::error::{CoreError, Result};
use crate::limits::MAX_CONTENTION_ATTEMPTS;
use crate::namespace::basis::PublicationBasis;
use crate::namespace::control::{ensure_namespace_live, LoadedHint, LoadedManifest};
use crate::namespace::store::NamespaceStore;
use crate::phase::Phase;
use crate::time::OperationClock;
use bytes::Bytes;
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use loonfs_types::format::control::{
    encode_control_state, ControlObjectKind, HintPayload, ManifestRef,
};
use loonfs_types::format::envelope::EncodedEnvelope;
use loonfs_types::format::manifest::{
    encode_namespace_manifest_json, NamespaceManifestEnvelope, NamespaceManifestPayload,
    MAX_NAMESPACE_MANIFEST_BYTES,
};
use loonfs_types::{ManifestNo, NamespaceId};
use std::collections::HashSet;

/// The outcome of one manifest put, with the manifest that's current after
/// the put.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ManifestPublicationOutcome {
    Published(LoadedManifest),
    CoveredByCurrent(LoadedManifest),
    PredecessorChanged(LoadedManifest),
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

/// Where the next attempt of [`update_manifest`] starts.
enum NextAttempt {
    /// Builds on this manifest without a request first.
    BuildOn(PublicationBasis),
    /// Builds on the newest manifest above this one, which the last attempt
    /// found to have a successor.
    BuildAbove(PublicationBasis),
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Publishes the next manifest after the current one, and tries again after
    /// a lost put.
    ///
    /// Each attempt passes the manifest that it builds on to `change`. The first
    /// attempt builds on `basis` without a request while `basis` was checked
    /// less than the revalidation bound ago, and otherwise loads the current
    /// manifest through the hint. A `Next` payload is published at the next
    /// manifest number with put-if-absent. After a lost put, the attempt finds
    /// the manifest that won by probing above the manifest that the attempt
    /// built on (see [`publish_manifest_from`]), and the next attempt builds on
    /// the winner with the lost attempt's check. After `Again`, the next attempt
    /// builds on the newest manifest above the one that the attempt built on.
    /// An attempt whose check is at the revalidation bound loads the current
    /// manifest through the hint instead. The call returns `store_contention`
    /// once `attempts` attempts have each lost their put or returned `Again`.
    /// The caller starts `clock` before the call, so
    /// `METADATA_PUBLICATION_BUDGET_MS` covers every attempt, from the first
    /// request to the last put.
    ///
    /// Why can an attempt put without checking its basis first? Garbage
    /// collection keeps every manifest above a manifest for at least the
    /// revalidation bound after a check that found no successor of it, so
    /// within the bound a put that succeeds proves that the basis had no
    /// successor, and a put that loses proves that it had one (format spec,
    /// "Publishing a materialized file set").
    ///
    /// If any attempt loaded the current manifest through the hint, the call
    /// returns the hint that the last such attempt read, and otherwise `None`.
    /// The call doesn't raise the hint. The runtime that published the manifest
    /// raises the hint afterward with
    /// [`NamespaceStore::raise_hint`].
    pub(crate) async fn update_manifest<T, F, Fut>(
        &self,
        basis: Option<PublicationBasis>,
        clock: &OperationClock,
        attempts: usize,
        mut change: F,
    ) -> Result<(T, LoadedManifest, Option<LoadedHint>)>
    where
        F: FnMut(LoadedManifest) -> Fut,
        Fut: std::future::Future<Output = Result<ManifestChange<T>>>,
    {
        let mut next = basis.map(NextAttempt::BuildOn);
        let mut hint = None;
        let mut attempt = 0;
        loop {
            attempt += 1;
            let found = match next.take() {
                Some(NextAttempt::BuildOn(basis)) => basis
                    .checked
                    .is_within_revalidation_bound()
                    .then_some(basis),
                Some(NextAttempt::BuildAbove(basis)) => {
                    self.find_current_above(&basis)
                        .await?
                        .map(|manifest| PublicationBasis {
                            manifest,
                            checked: basis.checked,
                        })
                }
                None => None,
            };
            let current = match found {
                Some(current) => current,
                None => {
                    let checked = clock.observe();
                    let (manifest, loaded) = self.load_current_manifest_with_hint().await?;
                    hint = Some(loaded);
                    PublicationBasis { manifest, checked }
                }
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
                            return Ok((result, published, hint));
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
                        attempts,
                        lost_to,
                        "a manifest publication lost its put"
                    );
                    // The winner is the attempt's manifest or a manifest
                    // published after the attempt's check, so every manifest
                    // above the winner was published after that check too.
                    next = Some(NextAttempt::BuildOn(PublicationBasis {
                        manifest: winner,
                        checked: current.checked,
                    }));
                }
                ManifestChange::Finished(result) => return Ok((result, current.manifest, hint)),
                ManifestChange::Again => next = Some(NextAttempt::BuildAbove(current)),
            }
            if attempt >= attempts {
                return Err(CoreError::contention_exhausted(&built_on));
            }
        }
    }

    /// Publishes the next manifest after the current one of a live namespace,
    /// through [`Self::update_manifest`] with [`MAX_CONTENTION_ATTEMPTS`]
    /// attempts.
    ///
    /// Each attempt returns `namespace_deleted` when the manifest that the
    /// attempt builds on is a tombstone, and otherwise passes a copy of that
    /// manifest's payload to `change`.
    pub(crate) async fn update_live_manifest<T, F, Fut>(
        &self,
        basis: Option<PublicationBasis>,
        clock: &OperationClock,
        mut change: F,
    ) -> Result<(T, LoadedManifest, Option<LoadedHint>)>
    where
        F: FnMut(NamespaceManifestPayload) -> Fut,
        Fut: std::future::Future<Output = Result<ManifestChange<T>>>,
    {
        self.update_manifest(
            basis,
            clock,
            MAX_CONTENTION_ATTEMPTS,
            |current: LoadedManifest| {
                let payload = current.payload();
                let changed = ensure_namespace_live(&payload.namespace_id, payload.status)
                    .map(|()| change(payload.clone()));
                async move { changed?.await }
            },
        )
        .await
    }

    /// Returns the current manifest, found by probing above `basis` with
    /// [`find_newest_manifest_above`], or `basis`'s manifest when no manifest
    /// is above it.
    ///
    /// Returns `None` when `basis` was checked the revalidation bound ago or
    /// longer, before the probe or when its response arrives, and the caller then
    /// loads the current manifest through the hint. Garbage collection keeps a
    /// manifest while its successor is younger than the grace window. The basis
    /// had no successor when it was checked, so every manifest above it was
    /// published after the check, and within the bound, garbage collection
    /// hasn't deleted any of them. The probe therefore finds the newest
    /// manifest, and a put after it can't create a deleted number again, beside
    /// the newer manifests.
    async fn find_current_above(&self, basis: &PublicationBasis) -> Result<Option<LoadedManifest>> {
        if !basis.checked.is_within_revalidation_bound() {
            return Ok(None);
        }
        let newest = self
            .find_newest_manifest_above(&basis.manifest)
            .await
            .map_err(CoreError::ControlObjectLoad)?;
        Ok(basis
            .checked
            .is_within_revalidation_bound()
            .then(|| newest.unwrap_or_else(|| basis.manifest.clone())))
    }

    /// Publishes manifest 1 of a new namespace, after the hint that points to it.
    ///
    /// Both objects are created with put-if-absent, so an existing namespace
    /// keeps its objects. The caller checks that the namespace is absent first
    /// (format spec, "Creating a namespace"). If manifest 1 exists, the call
    /// returns `namespace_exists`, or `namespace_deleted` when the current
    /// manifest is a tombstone.
    pub(crate) async fn create_manifest(
        &self,
        payload: NamespaceManifestPayload,
        clock: &OperationClock,
    ) -> Result<()> {
        let namespace_id = payload.namespace_id.clone();
        let first = HintPayload {
            namespace_id: namespace_id.clone(),
            manifest_no: ManifestNo(1),
        };
        let object_key = loonfs_objectstore::keys::hint(&namespace_id);
        let bytes = encode_control_state(ControlObjectKind::Hint, &first)
            .expect("a namespace hint should encode");
        match self
            .store
            .put_if_absent(&object_key, Bytes::from(bytes))
            .await
        {
            Ok(_) | Err(ObjectStoreError::PreconditionFailed { .. }) => {}
            Err(error) => return Err(CoreError::store(&object_key, &error)),
        }
        match self
            .publish_manifest_from(encode_manifest(payload), clock, None)
            .await?
        {
            ManifestPublicationOutcome::Published(_) => Ok(()),
            ManifestPublicationOutcome::CoveredByCurrent(current)
            | ManifestPublicationOutcome::PredecessorChanged(current) => {
                Err(if current.payload().status.is_deleted() {
                    CoreError::NamespaceDeleted { namespace_id }
                } else {
                    CoreError::NamespaceExists { namespace_id }
                })
            }
        }
    }

    /// Publishes `manifest` once with put-if-absent, as the successor of
    /// `current`, or as manifest 1 when `current` is `None`.
    ///
    /// After a put that loses, the call finds the current manifest by probing
    /// above `current` (see [`find_current_above`]). It loads the current
    /// manifest through the hint instead when `current` is `None` or was checked
    /// the revalidation bound ago or longer.
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
        if let Some(current) = current.map(|current| &current.manifest) {
            // A tombstone ends its namespace; work that raced the deletion stops here.
            if current.payload().status.is_deleted() && current.manifest() != candidate.manifest() {
                return Err(CoreError::NamespaceDeleted {
                    namespace_id: namespace_id.clone(),
                });
            }
            if manifest.envelope().payload().writer_epoch < current.payload().writer_epoch {
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
                ensure_publication_in_budget(&object_key, clock)?;
                ManifestPublicationOutcome::Published(candidate.clone())
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
                    clock,
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
    async fn settle_unconfirmed_put(
        &self,
        error: &ObjectStoreError,
        candidate: &LoadedManifest,
        current: Option<&PublicationBasis>,
        expected_predecessor: Option<ManifestNo>,
        references_own_object: bool,
        clock: &OperationClock,
    ) -> Result<ManifestPublicationOutcome> {
        // Only the exact manifest at this number confirms the put;
        // a later manifest may already cover it.
        let found = self
            .load_manifest_by_no(candidate.payload().manifest_no)
            .await
            .map_err(CoreError::ControlObjectLoad)?;
        let outcome = match found {
            Some(found) => classify_current(
                &found,
                candidate,
                expected_predecessor,
                references_own_object,
            ),
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
        let object_key = &candidate.object_key;
        let ManifestKind::Settled(outcome) = outcome else {
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
        if matches!(
            outcome,
            ManifestPublicationOutcome::Published(_)
                | ManifestPublicationOutcome::CoveredByCurrent(_)
        ) {
            ensure_publication_in_budget(object_key, clock)?;
        }
        Ok(outcome)
    }

    /// Classifies `candidate` against the manifest that's current after the
    /// candidate's put.
    ///
    /// The function finds that manifest by probing above `basis` (see
    /// [`find_current_above`]), or loads it through the hint when `basis` is
    /// `None` or the probe returns `None`.
    async fn classify_current_manifest(
        &self,
        basis: Option<&PublicationBasis>,
        candidate: &LoadedManifest,
        expected_predecessor: Option<ManifestNo>,
        confirm_authorship: bool,
    ) -> Result<ManifestKind> {
        let found = match basis {
            Some(basis) => self.find_current_above(basis).await?,
            None => None,
        };
        let current = match found {
            Some(found) => Some(found),
            None => self
                .load_current_manifest_if_present()
                .await
                .map_err(CoreError::ControlObjectLoad)?,
        };
        Ok(current.map_or(ManifestKind::Installable, |current| {
            classify_current(
                &current,
                candidate,
                expected_predecessor,
                confirm_authorship,
            )
        }))
    }
}

/// Returns whether `payload` references an object that this attempt created:
/// a fork's source pin, or a segment that its predecessor doesn't list.
///
/// Identical bytes prove authorship only for such a manifest. A rival can
/// rebuild any other manifest byte for byte.
fn references_own_object(
    payload: &NamespaceManifestPayload,
    current: Option<&PublicationBasis>,
) -> bool {
    let listed = current
        .iter()
        .flat_map(|current| &current.manifest.payload().runs)
        .flat_map(|run| &run.segments)
        .map(|segment| &segment.segment_id)
        .collect::<HashSet<_>>();
    (payload.manifest_no == ManifestNo(1) && payload.fork_basis.is_some())
        || payload
            .runs
            .iter()
            .flat_map(|run| &run.segments)
            .any(|segment| !listed.contains(&segment.segment_id))
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

fn ensure_publication_in_budget(object_key: &str, clock: &OperationClock) -> Result<()> {
    let elapsed_ms = clock.elapsed_ms();
    // A put that is written after the budget may have been written at a reclaimed number.
    if elapsed_ms > crate::limits::METADATA_PUBLICATION_BUDGET_MS {
        return Err(CoreError::OutcomeUnknown {
            object_key: object_key.to_owned(),
            message: format!(
                "the publication took {elapsed_ms}ms, over its {}ms budget",
                crate::limits::METADATA_PUBLICATION_BUDGET_MS,
            ),
        });
    }
    Ok(())
}

fn classify_current(
    current: &LoadedManifest,
    candidate: &LoadedManifest,
    expected_predecessor: Option<ManifestNo>,
    confirm_authorship: bool,
) -> ManifestKind {
    let outcome = if current.manifest() == candidate.manifest() {
        if confirm_authorship {
            ManifestPublicationOutcome::Published(current.clone())
        } else {
            ManifestPublicationOutcome::CoveredByCurrent(current.clone())
        }
    } else if current.compactor_epoch() > candidate.compactor_epoch() {
        ManifestPublicationOutcome::PredecessorChanged(current.clone())
    } else if current.folded_wal_no() >= candidate.folded_wal_no()
        && (current.manifest().head_seq, current.manifest().manifest_no)
            >= (
                candidate.manifest().head_seq,
                candidate.manifest().manifest_no,
            )
    {
        ManifestPublicationOutcome::CoveredByCurrent(current.clone())
    } else if Some(current.manifest().manifest_no) == expected_predecessor {
        return ManifestKind::Installable;
    } else {
        ManifestPublicationOutcome::PredecessorChanged(current.clone())
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
