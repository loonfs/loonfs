//! Publication through the next immutable manifest number.

use crate::error::{CoreError, Result};
use crate::namespace::basis::PublicationBasis;
use crate::namespace::control::{
    find_newest_manifest_above, load_current_manifest_if_present, load_current_manifest_with_hint,
    load_manifest_by_no, CurrentManifest, LoadedHint, LoadedManifest,
};
use crate::time::Deadline;
use bytes::Bytes;
use loonfs_objectstore::keys::metadata_manifest_object;
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
/// The caller starts `deadline` before the call, so
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
/// [`raise_namespace_hint`](crate::namespace::control::raise_namespace_hint).
pub(crate) async fn update_manifest<S, T, F, Fut>(
    store: &S,
    namespace_id: &NamespaceId,
    basis: Option<PublicationBasis>,
    deadline: &Deadline,
    attempts: usize,
    mut change: F,
) -> Result<(T, LoadedManifest, Option<LoadedHint>)>
where
    S: ObjectStore + ?Sized,
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
            Some(NextAttempt::BuildAbove(basis)) => find_current_above(store, namespace_id, &basis)
                .await?
                .map(|manifest| PublicationBasis {
                    manifest,
                    checked: basis.checked,
                }),
            None => None,
        };
        let current = match found {
            Some(current) => current,
            None => {
                let checked = deadline.observe();
                let (manifest, loaded) =
                    load_current_manifest_with_hint(store, namespace_id).await?;
                hint = Some(loaded);
                PublicationBasis { manifest, checked }
            }
        };
        let built_on = current.manifest.object_key.clone();
        let predecessor = current.manifest.state.envelope.payload().manifest_no;
        match change(current.manifest.clone()).await? {
            ManifestChange::Next(payload, result) => {
                let mut payload = *payload;
                payload.manifest_no = super::fold::next_manifest_no_after(predecessor)?;
                let manifest = encode_manifest(payload);
                let outcome =
                    publish_manifest_from(store, manifest, deadline, Some(&current)).await?;
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
                    namespace_id = namespace_id.as_str(),
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

/// Returns the current manifest, found by probing above `basis` with
/// [`find_newest_manifest_above`], or `basis`'s manifest when no manifest
/// is above it.
///
/// Returns `None` when `basis` was checked the revalidation bound ago or
/// longer, before the probe or when its answer arrives, and the caller then
/// loads the current manifest through the hint. Garbage collection keeps a
/// manifest while its successor is younger than the grace window. The basis
/// had no successor when it was checked, so every manifest above it was
/// published after the check, and within the bound, garbage collection
/// hasn't deleted any of them. The probe therefore finds the newest
/// manifest, and a put after it can't create a deleted number again, beside
/// the newer manifests.
async fn find_current_above<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    basis: &PublicationBasis,
) -> Result<Option<LoadedManifest>> {
    if !basis.checked.is_within_revalidation_bound() {
        return Ok(None);
    }
    let newest = find_newest_manifest_above(store, namespace_id, &basis.manifest)
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
pub(crate) async fn create_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    payload: NamespaceManifestPayload,
    deadline: &Deadline,
) -> Result<()> {
    let namespace_id = payload.namespace_id.clone();
    let first = HintPayload {
        namespace_id: namespace_id.clone(),
        manifest_no: ManifestNo(1),
    };
    let object_key = loonfs_objectstore::keys::hint(&namespace_id);
    let bytes = encode_control_state(ControlObjectKind::Hint, &first)
        .expect("a namespace hint should encode");
    match store.put_if_absent(&object_key, Bytes::from(bytes)).await {
        Ok(_) | Err(ObjectStoreError::PreconditionFailed { .. }) => {}
        Err(error) => return Err(CoreError::store(&object_key, &error)),
    }
    match publish_manifest_from(store, encode_manifest(payload), deadline, None).await? {
        ManifestPublicationOutcome::Published(_) => Ok(()),
        ManifestPublicationOutcome::CoveredByCurrent(current)
        | ManifestPublicationOutcome::PredecessorChanged(current) => {
            Err(if current.state.envelope.payload().status.is_deleted() {
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
    fields(phase = "publish_manifest", key_class = "manifest")
)]
pub(super) async fn publish_manifest_from<S: ObjectStore + ?Sized>(
    store: &S,
    manifest: EncodedEnvelope<NamespaceManifestPayload>,
    deadline: &Deadline,
    current: Option<&PublicationBasis>,
) -> Result<ManifestPublicationOutcome> {
    let namespace_id = manifest.envelope().payload().namespace_id.clone();
    let namespace_id = &namespace_id;
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
    // Identical bytes prove authorship only when the manifest names an object
    // this attempt created: a fork's source pin, or a segment its predecessor
    // does not list. A rival can rebuild any other manifest byte for byte.
    let names_own_object = {
        let payload = manifest.envelope().payload();
        let listed = current
            .iter()
            .flat_map(|current| &current.manifest.state.envelope.payload().runs)
            .flat_map(|run| &run.segments)
            .map(|segment| &segment.segment_id)
            .collect::<HashSet<_>>();
        (payload.manifest_no == ManifestNo(1) && payload.fork_basis.is_some())
            || payload
                .runs
                .iter()
                .flat_map(|run| &run.segments)
                .any(|segment| !listed.contains(&segment.segment_id))
    };
    let object_key =
        metadata_manifest_object(namespace_id, &manifest.envelope().payload().manifest_no);
    let candidate = LoadedManifest {
        object_key: object_key.clone(),
        manifest_bytes: manifest.as_bytes().len() as u64,
        state: CurrentManifest {
            envelope: std::sync::Arc::new(manifest.envelope().clone()),
        },
    };
    if let Some(current) = current.map(|current| &current.manifest) {
        // A tombstone ends its namespace; work that raced the deletion stops here.
        if current.state.envelope.payload().status.is_deleted()
            && current.state.manifest() != candidate.state.manifest()
        {
            return Err(CoreError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            });
        }
        if manifest.envelope().payload().writer_epoch
            < current.state.envelope.payload().writer_epoch
        {
            return Ok(ManifestPublicationOutcome::PredecessorChanged(
                current.clone(),
            ));
        }
        match classify_current(current, &candidate.state, expected_predecessor, false) {
            ManifestKind::Installable => {}
            ManifestKind::Settled(outcome) => return Ok(outcome),
        }
    }
    let payload = manifest.envelope().payload();
    let legal = match current {
        Some(current) => current
            .manifest
            .state
            .envelope
            .payload()
            .ensure_successor(payload)
            .map_err(|error| error.to_string()),
        None if payload.manifest_no == ManifestNo(1) => payload
            .ensure_first_manifest()
            .map_err(|error| error.to_string()),
        None => Err("has no predecessor".to_owned()),
    };
    legal.expect("a candidate manifest should be a legal successor of the current manifest");
    deadline.ensure_metadata_publication_budget(namespace_id)?;
    let outcome = match store
        .put_if_absent(&object_key, Bytes::from(manifest.into_bytes()))
        .await
    {
        Ok(_) => {
            ensure_publication_in_budget(&object_key, deadline)?;
            ManifestPublicationOutcome::Published(candidate.clone())
        }
        Err(ObjectStoreError::PreconditionFailed { .. }) => classify_current_manifest(
            store,
            namespace_id,
            current,
            &candidate.state,
            expected_predecessor,
            false,
        )
        .await?
        .settled_after_conflict(namespace_id)?,
        Err(error @ ObjectStoreError::Transport { .. }) => {
            // Only the exact manifest at this number confirms the put;
            // a later manifest may already cover it.
            let landed = load_manifest_by_no(
                store,
                namespace_id,
                candidate.state.envelope.payload().manifest_no,
            )
            .await
            .map_err(CoreError::ControlObjectLoad)?;
            let outcome = match landed {
                Some(landed) => classify_current(
                    &landed,
                    &candidate.state,
                    expected_predecessor,
                    names_own_object,
                ),
                None => {
                    classify_current_manifest(
                        store,
                        namespace_id,
                        current,
                        &candidate.state,
                        expected_predecessor,
                        names_own_object,
                    )
                    .await?
                }
            };
            let ManifestKind::Settled(outcome) = outcome else {
                tracing::error!(
                    namespace_id = %namespace_id,
                    object_key = %object_key,
                    error = %error,
                    "the outcome of a manifest publication is unknown"
                );
                return Err(CoreError::OutcomeUnknown {
                    object_key,
                    message: error.public_message().into_owned(),
                });
            };
            if matches!(
                outcome,
                ManifestPublicationOutcome::Published(_)
                    | ManifestPublicationOutcome::CoveredByCurrent(_)
            ) {
                ensure_publication_in_budget(&object_key, deadline)?;
            }
            outcome
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

fn ensure_publication_in_budget(object_key: &str, deadline: &Deadline) -> Result<()> {
    let elapsed_ms = deadline.elapsed_ms();
    // A put that lands after the budget may have landed on a reclaimed number.
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
    loaded: &LoadedManifest,
    candidate: &CurrentManifest,
    expected_predecessor: Option<ManifestNo>,
    confirm_authorship: bool,
) -> ManifestKind {
    let current = &loaded.state;
    let outcome = if current.manifest() == candidate.manifest() {
        if confirm_authorship {
            ManifestPublicationOutcome::Published(loaded.clone())
        } else {
            ManifestPublicationOutcome::CoveredByCurrent(loaded.clone())
        }
    } else if current.compactor_epoch() > candidate.compactor_epoch() {
        ManifestPublicationOutcome::PredecessorChanged(loaded.clone())
    } else if current.folded_wal_no() >= candidate.folded_wal_no()
        && (current.manifest().head_seq, current.manifest().manifest_no)
            >= (
                candidate.manifest().head_seq,
                candidate.manifest().manifest_no,
            )
    {
        ManifestPublicationOutcome::CoveredByCurrent(loaded.clone())
    } else if Some(current.manifest().manifest_no) == expected_predecessor {
        return ManifestKind::Installable;
    } else {
        ManifestPublicationOutcome::PredecessorChanged(loaded.clone())
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

/// Classifies `candidate` against the manifest that's current after the
/// candidate's put.
///
/// The function finds that manifest by probing above `basis` (see
/// [`find_current_above`]), or loads it through the hint when `basis` is
/// `None` or the probe returns `None`.
async fn classify_current_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    basis: Option<&PublicationBasis>,
    candidate: &CurrentManifest,
    expected_predecessor: Option<ManifestNo>,
    confirm_authorship: bool,
) -> Result<ManifestKind> {
    let found = match basis {
        Some(basis) => find_current_above(store, namespace_id, basis).await?,
        None => None,
    };
    let current = match found {
        Some(found) => Some(found),
        None => load_current_manifest_if_present(store, namespace_id)
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

pub(crate) fn manifest_ref_for(
    namespace_id: &NamespaceId,
    manifest: &NamespaceManifestEnvelope,
) -> ManifestRef {
    ManifestRef {
        owner_namespace_id: namespace_id.clone(),
        manifest_no: manifest.payload().manifest_no,
        head_seq: manifest.payload().head_seq,
        payload_checksum: manifest.payload_checksum().to_owned(),
    }
}
