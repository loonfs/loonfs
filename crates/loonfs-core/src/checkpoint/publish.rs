//! Durable manifest publication: write manifest objects idempotently and
//! advance `metadata/root.json` by monotonic compare-and-swap.

use super::error::ManifestLoadError;
use crate::commit::CommitHeadPublishError;
use crate::control_update::{retry_while_contended, CasAttempt};
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::control::load_metadata_root_object_if_present;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_state, ControlObjectKind, ManifestRef, MetadataRootState,
};
use loonfs_api::wire::manifest::{encode_namespace_manifest_json, NamespaceManifestEnvelope};
use loonfs_api::{ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::{metadata_manifest_object, metadata_root};
use loonfs_objectstore::{ImmutableWriteError, ObjectStore, ObjectStoreError};

/// What a publication attempt did to `metadata/root.json`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ManifestPublicationOutcome {
    /// This call installed the candidate.
    Installed(MetadataRootState),
    /// The root already names this exact candidate, which is how an unknown
    /// write outcome resolves when the swap did land.
    AlreadyCurrent(MetadataRootState),
    /// The current root dominates the candidate, so it already covers the
    /// candidate's work. The candidate manifest stays durable and unreferenced.
    CoveredByCurrent(MetadataRootState),
    /// The expected predecessor is no longer current and the root that
    /// replaced it does not cover the candidate. The caller must rebuild
    /// against this root.
    PredecessorChanged(MetadataRootState),
}

#[tracing::instrument(
    level = "debug",
    name = "loonfs.phase",
    err(level = "warn"),
    skip_all,
    fields(phase = "write_namespace_manifest", key_class = "namespace_manifest")
)]
pub(crate) async fn write_namespace_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    manifest: &NamespaceManifestEnvelope,
) -> std::result::Result<(), MetadataProjectionLoadError> {
    let manifest_key = metadata_manifest_object(
        &manifest.payload.namespace_id,
        &manifest.payload.manifest_object_id,
    );
    let manifest_bytes = Bytes::from(encode_namespace_manifest_json(manifest).map_err(|err| {
        MetadataProjectionLoadError::ManifestLoad(ManifestLoadError::ManifestCodec {
            object_key: manifest_key.clone(),
            message: err.to_string(),
        })
    })?);
    // Immutable format objects use verified writes so identical bytes are an idempotent success
    // and different bytes are corruption.
    match store
        .put_immutable_verified(&manifest_key, manifest_bytes)
        .await
    {
        Ok(()) => Ok(()),
        Err(ImmutableWriteError::DifferentObject { .. }) => Err(
            MetadataProjectionLoadError::ManifestLoad(ManifestLoadError::ManifestObjectConflict {
                object_key: manifest_key,
                manifest_no: manifest.payload.manifest_no,
            }),
        ),
        Err(ImmutableWriteError::Transport { source, .. }) => Err(
            MetadataProjectionLoadError::ManifestLoad(ManifestLoadError::ReadManifest {
                object_key: manifest_key,
                message: source.public_message().into_owned(),
            }),
        ),
        Err(_) => Err(MetadataProjectionLoadError::ManifestLoad(
            ManifestLoadError::ReadManifest {
                object_key: manifest_key,
                message: loonfs_objectstore::ObjectStoreErrorClass::Other
                    .public_message()
                    .into_owned(),
            },
        )),
    }
}

/// Maps a manifest write failure onto a core error.
///
/// Callers write each manifest under a freshly generated object id, and
/// every generated id ends in 16 random hex characters, so no other writer
/// proposes the same key. [`write_namespace_manifest`] already accepts a
/// byte-identical rewrite of a key it wrote, which covers a retried request.
/// A different payload under the key is therefore corruption rather than
/// contention, and it is reported as such.
pub(super) fn manifest_write_failure(error: MetadataProjectionLoadError) -> CoreError {
    match error {
        MetadataProjectionLoadError::ManifestLoad(
            ManifestLoadError::ManifestObjectConflict { object_key, .. }
            | ManifestLoadError::ManifestConflict { object_key, .. },
        ) => CoreError::NamespaceCorrupt(format!(
            "namespace manifest `{object_key}` already exists with a different payload"
        )),
        error => CoreError::MetadataProjection(error),
    }
}

#[tracing::instrument(
    level = "debug",
    name = "loonfs.phase",
    err(level = "warn"),
    skip_all,
    fields(phase = "publish_metadata_root", key_class = "namespace_manifest")
)]
pub(super) async fn publish_metadata_root<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest: &NamespaceManifestEnvelope,
    expected_predecessor: Option<ManifestObjectId>,
    updated_at_ms: u64,
) -> Result<ManifestPublicationOutcome> {
    // Manifest publication CASes metadata/root.json, never the WAL head:
    // head watchers see only commits. Updates are monotonic in
    // manifest_head_seq; a same-seq replacement may reference a different
    // manifest (pure compaction), and a lower-seq attempt no-ops in favor of
    // whatever newer root someone else already published.
    //
    // A swap that loses while the expected predecessor is still current is
    // contention on this one object, and it is retried here. Every semantic
    // outcome goes back to the caller, because rebuilding costs vary by
    // operation.
    let candidate = manifest_ref_for(namespace_id, manifest);
    let candidate = &candidate;
    let expected_predecessor = expected_predecessor.as_ref();
    let published = retry_while_contended(|| async move {
        try_publish_metadata_root(
            store,
            namespace_id,
            candidate,
            expected_predecessor,
            updated_at_ms,
        )
        .await
    })
    .await?;
    // Losing every attempt is the same signal a flush reports when it loses
    // every rebuild, so callers keep one retry classification.
    published.ok_or(CoreError::HeadPublish(CommitHeadPublishError::StaleHead))
}

/// One read, decision, and swap against the root loaded with its ETag.
async fn try_publish_metadata_root<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidate: &ManifestRef,
    expected_predecessor: Option<&ManifestObjectId>,
    updated_at_ms: u64,
) -> Result<CasAttempt<ManifestPublicationOutcome>> {
    let Some(loaded) = load_metadata_root_object_if_present(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?
    else {
        return match create_first_metadata_root(store, namespace_id, candidate, updated_at_ms)
            .await?
        {
            Some(installed) => Ok(CasAttempt::Settled(ManifestPublicationOutcome::Installed(
                installed,
            ))),
            None => {
                classify_current_root(store, namespace_id, candidate, expected_predecessor).await
            }
        };
    };
    match root_transition(&loaded.state, candidate, expected_predecessor) {
        RootTransition::InstallAgainstCurrent => {}
        decided => return Ok(attempt_from(loaded.state, decided)),
    }
    ensure_legal_successor(namespace_id, candidate, &loaded.state.manifest)?;
    let next = MetadataRootState {
        namespace_id: namespace_id.clone(),
        manifest: candidate.clone(),
        updated_at_ms,
    };
    let encoded =
        encode_control_state(ControlObjectKind::MetadataRoot, &next).map_err(|error| {
            CoreError::Codec {
                object_key: loaded.object_key.clone(),
                message: error.to_string(),
            }
        })?;
    match store
        .compare_and_swap(&loaded.object_key, &loaded.etag, Bytes::from(encoded))
        .await
    {
        Ok(_) => Ok(CasAttempt::Settled(ManifestPublicationOutcome::Installed(
            next,
        ))),
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
            classify_current_root(store, namespace_id, candidate, expected_predecessor).await
        }
        // An unknown outcome is resolved by reading the root, never by
        // repeating the swap. The store error stands only while the root
        // still shows the untouched predecessor.
        Err(error) => {
            match classify_current_root(store, namespace_id, candidate, expected_predecessor)
                .await?
            {
                CasAttempt::Contended => Err(CoreError::store(&loaded.object_key, &error)),
                settled => Ok(settled),
            }
        }
    }
}

/// The four states a candidate can be in against the current root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootTransition {
    InstallAgainstCurrent,
    AlreadyCurrent,
    CoveredByCurrent,
    PredecessorChanged,
}

/// Classifies the current root against the candidate and its expected
/// predecessor.
///
/// A candidate may replace only its expected predecessor. This prevents stale
/// work from overwriting a sibling publication, even at a higher head sequence.
fn root_transition(
    root: &MetadataRootState,
    candidate: &ManifestRef,
    expected_predecessor: Option<&ManifestObjectId>,
) -> RootTransition {
    if root.manifest == *candidate {
        return RootTransition::AlreadyCurrent;
    }
    if root_supersedes_candidate(root, candidate) {
        return RootTransition::CoveredByCurrent;
    }
    match expected_predecessor {
        Some(predecessor) if root.manifest.manifest_object_id == *predecessor => {
            RootTransition::InstallAgainstCurrent
        }
        _ => RootTransition::PredecessorChanged,
    }
}

/// Maps a decided transition onto one attempt's result. An expected
/// predecessor that is still current after a swap attempt is contention on
/// this one object, so the attempt repeats instead of reporting a race.
fn attempt_from(
    root: MetadataRootState,
    transition: RootTransition,
) -> CasAttempt<ManifestPublicationOutcome> {
    match transition {
        RootTransition::InstallAgainstCurrent => CasAttempt::Contended,
        RootTransition::AlreadyCurrent => {
            CasAttempt::Settled(ManifestPublicationOutcome::AlreadyCurrent(root))
        }
        RootTransition::CoveredByCurrent => {
            CasAttempt::Settled(ManifestPublicationOutcome::CoveredByCurrent(root))
        }
        RootTransition::PredecessorChanged => {
            CasAttempt::Settled(ManifestPublicationOutcome::PredecessorChanged(root))
        }
    }
}

/// Re-reads the root after a lost or unconfirmed swap and classifies it.
async fn classify_current_root<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidate: &ManifestRef,
    expected_predecessor: Option<&ManifestObjectId>,
) -> Result<CasAttempt<ManifestPublicationOutcome>> {
    let Some(loaded) = load_metadata_root_object_if_present(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?
    else {
        return Ok(CasAttempt::Contended);
    };
    let transition = root_transition(&loaded.state, candidate, expected_predecessor);
    Ok(attempt_from(loaded.state, transition))
}

/// Rejects a candidate that may not replace the predecessor it names.
///
/// Every caller derives its candidate from the predecessor's own manifest, so
/// a violation is a construction bug rather than a race.
fn ensure_legal_successor(
    namespace_id: &NamespaceId,
    candidate: &ManifestRef,
    predecessor: &ManifestRef,
) -> Result<()> {
    if candidate.owner_namespace_id == *namespace_id
        && candidate.manifest_no > predecessor.manifest_no
        && candidate.manifest_head_seq >= predecessor.manifest_head_seq
    {
        return Ok(());
    }
    Err(CoreError::Internal(format!(
        "manifest `{}` is not a legal successor of `{}` in namespace `{namespace_id}`",
        candidate.manifest_object_id, predecessor.manifest_object_id
    )))
}

/// Publishes the namespace's first `metadata/root.json`.
///
/// Returns `None` when another publisher wins the conditional create.
async fn create_first_metadata_root<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidate: &ManifestRef,
    updated_at_ms: u64,
) -> Result<Option<MetadataRootState>> {
    let object_key = metadata_root(namespace_id);
    let next = MetadataRootState {
        namespace_id: namespace_id.clone(),
        manifest: candidate.clone(),
        updated_at_ms,
    };
    let encoded =
        encode_control_state(ControlObjectKind::MetadataRoot, &next).map_err(|error| {
            CoreError::Codec {
                object_key: object_key.clone(),
                message: error.to_string(),
            }
        })?;
    match store.put_if_absent(&object_key, Bytes::from(encoded)).await {
        Ok(_) => Ok(Some(next)),
        Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(None),
        Err(error) => Err(CoreError::store(&object_key, &error)),
    }
}

/// Builds a manifest reference owned by `namespace_id`.
pub(super) fn manifest_ref_for(
    namespace_id: &NamespaceId,
    manifest: &NamespaceManifestEnvelope,
) -> ManifestRef {
    ManifestRef {
        owner_namespace_id: namespace_id.clone(),
        manifest_no: manifest.payload.manifest_no,
        manifest_object_id: manifest.payload.manifest_object_id.clone(),
        manifest_head_seq: manifest.payload.head_seq,
        manifest_payload_checksum: manifest.payload_checksum.clone(),
    }
}

fn root_supersedes_candidate(current: &MetadataRootState, candidate: &ManifestRef) -> bool {
    current.manifest.manifest_head_seq > candidate.manifest_head_seq
        || (current.manifest.manifest_head_seq == candidate.manifest_head_seq
            && current.manifest.manifest_no >= candidate.manifest_no)
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::{ChangeSeq, ManifestNo};

    /// One root shape the candidate can meet. A reread after a lost or
    /// unconfirmed swap lands on these same rows; `tests/cas_recovery.rs`
    /// drives the store plumbing that reaches them.
    struct TransitionCase {
        name: &'static str,
        root: ManifestRef,
        expected_predecessor: Option<ManifestObjectId>,
        expected: RootTransition,
    }

    fn namespace_id() -> NamespaceId {
        NamespaceId::parse("demo").expect("valid namespace id")
    }

    fn manifest_ref(manifest_no: u64, head_seq: u64, distinguisher: u64) -> ManifestRef {
        ManifestRef {
            owner_namespace_id: namespace_id(),
            manifest_no: ManifestNo(manifest_no),
            manifest_object_id: ManifestObjectId::parse(format!(
                "man_{manifest_no:020}-{distinguisher:016x}"
            ))
            .expect("valid manifest object id"),
            manifest_head_seq: ChangeSeq(head_seq),
            manifest_payload_checksum: "checksum".to_owned(),
        }
    }

    #[test]
    fn each_current_root_shape_classifies_the_candidate_it_faces() {
        let candidate = manifest_ref(2, 20, 1);
        let predecessor = manifest_ref(1, 10, 2);
        let after_predecessor = Some(predecessor.manifest_object_id.clone());
        let cases = [
            TransitionCase {
                name: "the root already names the candidate",
                root: candidate.clone(),
                expected_predecessor: after_predecessor.clone(),
                expected: RootTransition::AlreadyCurrent,
            },
            TransitionCase {
                name: "the root is at a higher head sequence",
                root: manifest_ref(1, 30, 3),
                expected_predecessor: after_predecessor.clone(),
                expected: RootTransition::CoveredByCurrent,
            },
            TransitionCase {
                name: "the root is at the same head sequence with a higher manifest number",
                root: manifest_ref(3, 20, 4),
                expected_predecessor: after_predecessor.clone(),
                expected: RootTransition::CoveredByCurrent,
            },
            TransitionCase {
                name: "the root is a same-generation sibling of the candidate",
                root: manifest_ref(2, 20, 5),
                expected_predecessor: after_predecessor.clone(),
                expected: RootTransition::CoveredByCurrent,
            },
            TransitionCase {
                name: "the expected predecessor is still current",
                root: predecessor.clone(),
                expected_predecessor: after_predecessor.clone(),
                expected: RootTransition::InstallAgainstCurrent,
            },
            TransitionCase {
                name: "the predecessor was replaced by a root behind the candidate",
                root: manifest_ref(2, 10, 6),
                expected_predecessor: after_predecessor,
                expected: RootTransition::PredecessorChanged,
            },
            TransitionCase {
                name: "another writer won the first root and it is behind the candidate",
                root: manifest_ref(1, 10, 7),
                expected_predecessor: None,
                expected: RootTransition::PredecessorChanged,
            },
        ];

        for case in cases {
            let root = MetadataRootState {
                namespace_id: namespace_id(),
                manifest: case.root,
                updated_at_ms: 1,
            };
            assert_eq!(
                root_transition(&root, &candidate, case.expected_predecessor.as_ref()),
                case.expected,
                "{}",
                case.name
            );
        }
    }
}
