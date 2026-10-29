//! The two durable records of an upload session: loading them together,
//! creating the outcome record, and deleting both.
//!
//! Every upload session has a session record, written once when the session
//! starts, and at most one outcome record, created once when the session
//! completes or is aborted (see [upload sessions]). Neither record is ever
//! rewritten, so no upload writes the same object twice.
//!
//! [upload sessions]: https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#51-upload-sessions

use crate::control_object::{expect_identity_field, expect_namespace, load_control_object};
use crate::control_object::{ControlObjectLoadError, LoadedControl};
use crate::control_update::{retry_while_contended, CasOutcome};
use crate::error::{CoreError, Result};
use bytes::Bytes;
use loonfs_objectstore::keys::{upload_outcome, upload_session};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use loonfs_types::format::control::{
    encode_control_state, ControlObjectKind, UploadOutcome, UploadOutcomePayload,
    UploadSessionPayload,
};
use loonfs_types::{NamespaceId, UploadId};

/// An upload session's record, together with its outcome if the session ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UploadSessionState {
    pub(crate) session: UploadSessionPayload,
    /// `None` while the session is open.
    pub(crate) outcome: Option<UploadOutcome>,
}

/// Loads an upload session's records.
///
/// The outcome record is read before the session record. Garbage collection
/// deletes the session record first, so if the session record is still
/// there after the outcome was found missing, the session was open when the
/// outcome was read, and not deleted. A missing session record returns
/// `UploadNotFound`, whatever the outcome read found.
pub(crate) async fn load_upload_session_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
) -> Result<UploadSessionState> {
    let outcome = load_upload_outcome(store, namespace_id, upload_id).await?;
    let session = load_upload_session_record(store, namespace_id, upload_id)
        .await?
        .ok_or_else(|| CoreError::UploadNotFound {
            upload_id: upload_id.clone(),
        })?;
    if let Some(outcome) = &outcome {
        ensure_outcome_matches_session(&session, outcome)?;
    }
    Ok(UploadSessionState { session, outcome })
}

/// Loads the record that started an upload session, or `None` if it's gone.
pub(crate) async fn load_upload_session_record<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
) -> Result<Option<UploadSessionPayload>> {
    let loaded = load_control_object(
        store,
        upload_session(namespace_id, upload_id),
        ControlObjectKind::UploadSession,
        |session: &UploadSessionPayload| {
            expect_namespace(namespace_id, &session.namespace_id)?;
            expect_identity_field("upload id", upload_id.as_str(), session.upload_id.as_str())
        },
    )
    .await;
    present(loaded).map(|loaded| loaded.map(|loaded| loaded.state))
}

/// Loads how an upload session ended, or `None` if it has no outcome record.
pub(crate) async fn load_upload_outcome<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
) -> Result<Option<UploadOutcome>> {
    let loaded = load_control_object(
        store,
        upload_outcome(namespace_id, upload_id),
        ControlObjectKind::UploadOutcome,
        |record: &UploadOutcomePayload| {
            expect_namespace(namespace_id, &record.namespace_id)?;
            expect_identity_field("upload id", upload_id.as_str(), record.upload_id.as_str())
        },
    )
    .await;
    present(loaded).map(|loaded| loaded.map(|loaded| loaded.state.outcome))
}

fn present<T>(
    loaded: std::result::Result<LoadedControl<T>, ControlObjectLoadError>,
) -> Result<Option<LoadedControl<T>>> {
    match loaded {
        Ok(loaded) => Ok(Some(loaded)),
        Err(ControlObjectLoadError::MissingObject { .. }) => Ok(None),
        Err(error) => Err(CoreError::ControlObjectLoad(error)),
    }
}

/// Refuses an outcome that contradicts its session record.
///
/// Each record is valid on its own, so only the pair can disagree: a
/// completed reference to another content object, or one in a checksum
/// algorithm that the session's direct mode didn't select.
fn ensure_outcome_matches_session(
    session: &UploadSessionPayload,
    outcome: &UploadOutcome,
) -> Result<()> {
    let UploadOutcome::Completed { content_ref, .. } = outcome else {
        return Ok(());
    };
    if content_ref.content_id != session.content_id {
        return Err(CoreError::NamespaceCorrupt(format!(
            "upload session `{}` owns content `{}` but its outcome refers to `{}`",
            session.upload_id, session.content_id, content_ref.content_id
        )));
    }
    if let Some(algorithm) = session.mode.checksum_algorithm() {
        if content_ref.checksum.algorithm() != algorithm {
            return Err(CoreError::NamespaceCorrupt(format!(
                "upload session `{}` requires `{algorithm}` but its completed content uses `{}`",
                session.upload_id,
                content_ref.checksum.algorithm()
            )));
        }
    }
    Ok(())
}

/// What [`create_upload_outcome`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OutcomeCreation {
    /// The caller's outcome is the session's outcome.
    Created,
    /// The session had already ended with this other outcome, and nothing
    /// was written.
    Existing(UploadOutcome),
}

/// Records how an upload session ended, then confirms that the session
/// record still exists.
///
/// The outcome record is created with put-if-absent, so a completion, a
/// client's abort, and garbage collection's abort of an expired session race
/// on one create, and the session ends exactly once. After a put whose result
/// is unknown, or that fails its precondition, this function reads the record
/// back. An equal outcome is this call's own write, and a different one is
/// returned as [`OutcomeCreation::Existing`].
///
/// An outcome record counts only while its session record exists, so after
/// the put this function sends a HEAD request for the session record, and
/// returns `UploadNotFound` if it's gone. The caller returns a token or
/// deletes content only after this check passes. Without the check, a
/// completion or abort that read the session as open, and then stalled while
/// garbage collection deleted the ended session's records, would create a
/// new outcome for a session that no longer exists. A completion would then
/// issue a token for content that the earlier abort deleted, and an abort
/// would delete content that a commit might reference. The check finds the
/// session record gone in exactly those cases, because the outcome key is
/// free again only after [`delete_upload_session_records`] has deleted the
/// session record.
pub(crate) async fn create_upload_outcome<S: ObjectStore + ?Sized>(
    store: &S,
    session: &UploadSessionPayload,
    outcome: &UploadOutcome,
) -> Result<OutcomeCreation> {
    let namespace_id = &session.namespace_id;
    let upload_id = &session.upload_id;
    let object_key = upload_outcome(namespace_id, upload_id);
    let record = UploadOutcomePayload {
        namespace_id: namespace_id.clone(),
        upload_id: upload_id.clone(),
        outcome: outcome.clone(),
    };
    let encoded = Bytes::from(
        encode_control_state(ControlObjectKind::UploadOutcome, &record)
            .expect("an upload outcome record should encode"),
    );
    let creation = retry_while_contended(|| async {
        let error = match store.put_if_absent(&object_key, encoded.clone()).await {
            Ok(_) => return Ok(CasOutcome::Settled(OutcomeCreation::Created)),
            Err(
                error @ (ObjectStoreError::PreconditionFailed { .. }
                | ObjectStoreError::Transport { .. }),
            ) => error,
            Err(error) => return Err(CoreError::store(&object_key, &error)),
        };
        match load_upload_outcome(store, namespace_id, upload_id).await? {
            Some(stored) if stored == *outcome => Ok(CasOutcome::Settled(OutcomeCreation::Created)),
            Some(stored) => Ok(CasOutcome::Settled(OutcomeCreation::Existing(stored))),
            // An earlier attempt's put can still be applied after this read,
            // so a write whose result is unknown is sent again.
            None if matches!(error, ObjectStoreError::Transport { .. }) => {
                Ok(CasOutcome::Contended(CoreError::store(&object_key, &error)))
            }
            // The key was occupied and is empty now, which happens only when
            // garbage collection deleted the ended session's records.
            None => Err(CoreError::UploadNotFound {
                upload_id: upload_id.clone(),
            }),
        }
    })
    .await??;
    let session_key = upload_session(namespace_id, upload_id);
    match store.head(&session_key).await {
        Ok(Some(_)) => Ok(creation),
        Ok(None) => Err(CoreError::UploadNotFound {
            upload_id: upload_id.clone(),
        }),
        Err(error) => Err(CoreError::store(&session_key, &error)),
    }
}

/// Deletes an ended upload session's records, the session record first.
///
/// An outcome record counts only while its session record exists, so the
/// session record goes first. If this call stops between the two deletes,
/// the outcome record is left without its session record, and a later pass
/// deletes it without acting on the session's content. The other order would
/// be unsafe: a session record left without its outcome reads as an open
/// session, and once its lease had passed, garbage collection would abort it
/// and delete its content, which a commit might reference. This order is
/// also what lets [`create_upload_outcome`] detect a session that was
/// deleted while the caller stalled.
pub(crate) async fn delete_upload_session_records<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
) -> Result<()> {
    for object_key in [
        upload_session(namespace_id, upload_id),
        upload_outcome(namespace_id, upload_id),
    ] {
        store
            .delete(&object_key)
            .await
            .map_err(|error| CoreError::store(&object_key, &error))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{proxied_upload_session, write_upload_records};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_types::format::control::UploadSessionMode;
    use loonfs_types::{Checksum, ChecksumAlgorithm, ContentId, ContentRef, ContentRefKind};
    use std::assert_matches;
    use tempfile::tempdir;

    /// Checks that a completed outcome must refer to its session's content,
    /// in the algorithm that a direct session selected.
    #[tokio::test]
    async fn an_outcome_that_contradicts_its_session_is_corrupt() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let direct_put = UploadSessionMode::DirectPut {
            checksum_algorithm: ChecksumAlgorithm::Crc32c,
        };
        let cases = [
            (UploadSessionMode::ServiceProxied {}, false, None),
            (
                UploadSessionMode::ServiceProxied {},
                true,
                Some("but its outcome refers to"),
            ),
            (
                direct_put,
                false,
                Some("requires `crc32c` but its completed content uses `crc64nvme`"),
            ),
        ];
        for (mode, other_content, refusal) in cases {
            let session = UploadSessionPayload {
                mode,
                ..proxied_upload_session(&namespace_id, 2_000)
            };
            let content_ref = ContentRef {
                kind: ContentRefKind::BlobV1,
                owner_namespace_id: namespace_id.clone(),
                content_id: if other_content {
                    ContentId::generate()
                } else {
                    session.content_id.clone()
                },
                size_bytes: 5,
                checksum: Checksum::crc64nvme(b"bytes"),
            };
            let outcome = UploadOutcome::Completed {
                completed_at_ms: 1_500,
                content_ref,
            };
            write_upload_records(&store, &session, Some(outcome.clone())).await;
            let loaded = load_upload_session_state(&store, &namespace_id, &session.upload_id).await;
            match refusal {
                None => assert_eq!(loaded.expect("consistent records").outcome, Some(outcome)),
                Some(refusal) => {
                    let error = loaded.expect_err("contradicting records are corrupt");
                    assert_matches!(&error, CoreError::NamespaceCorrupt(message) if message.contains(refusal));
                }
            }
        }
    }
}
