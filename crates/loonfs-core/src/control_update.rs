//! Read-modify-write loops for control objects: load, edit, and
//! compare-and-swap the head or an upload session on its etag.

use crate::error::{CoreError, StoreFailureClass};
use crate::namespace::control::{read_head_object, ControlObjectLoadError, LoadedHeadObject};
use bytes::Bytes;
use loonfs_api::wire::control::{
    decode_control_object, encode_control_object, ControlObjectKind, HeadState, HeadStateEnvelope,
    UploadSessionEnvelope, UploadSessionState,
};
use loonfs_api::{NamespaceId, UploadId};
use loonfs_objectstore::keys::upload_session;
use loonfs_objectstore::{ObjectMetadata, ObjectStore, ObjectStoreError};
use std::future::Future;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HeadUpdate<T> {
    Noop(T),
    Replace { next: Box<HeadState>, outcome: T },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UploadSessionUpdate<T> {
    Noop(T),
    Replace {
        next: Box<UploadSessionState>,
        outcome: T,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UploadSessionCas<T> {
    Applied(T),
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum ControlUpdateError {
    #[error(transparent)]
    LoadHead(ControlObjectLoadError),
    #[error("missing etag for `{object_key}`")]
    MissingEtag { object_key: String },
    #[error("control object codec error for `{object_key}`: {message}")]
    Codec { object_key: String, message: String },
    #[error("control object store error for `{object_key}`: {message}")]
    Store { object_key: String, message: String },
    #[error("control object update retries exhausted after {attempts} attempts")]
    RetryExhausted { attempts: usize },
}

/// Reads the head, lets `update` decide `Noop` or `Replace`, and publishes a
/// replacement by compare-and-swap on the loaded etag, retrying the whole
/// read-decide-swap cycle on CAS conflict. Closure errors propagate
/// immediately without retrying — that is the fencing hook: a closure that
/// observes a disqualifying head (newer writer, changed manifest) must error,
/// never clobber.
pub(crate) async fn update_head<S, T, E, F>(
    store: &S,
    namespace_id: &NamespaceId,
    max_attempts: usize,
    mut update: F,
) -> Result<T, E>
where
    S: ObjectStore + ?Sized,
    E: From<ControlUpdateError>,
    F: FnMut(&LoadedHeadObject) -> Result<HeadUpdate<T>, E>,
{
    for _attempt in 0..max_attempts {
        let loaded = read_head_object(store, namespace_id)
            .await
            .map_err(|error| E::from(ControlUpdateError::LoadHead(error)))?;
        let expected_etag = required_etag(&loaded.metadata, &loaded.object_key).map_err(E::from)?;

        match update(&loaded)? {
            HeadUpdate::Noop(outcome) => return Ok(outcome),
            HeadUpdate::Replace { next, outcome } => {
                let encoded = encode_head(*next, &loaded.object_key).map_err(E::from)?;
                match store
                    .compare_and_swap(&loaded.object_key, expected_etag, Bytes::from(encoded))
                    .await
                {
                    Ok(_) => return Ok(outcome),
                    Err(ObjectStoreError::PreconditionFailed { .. }) => {
                        continue;
                    }
                    Err(error) => {
                        return Err(E::from(ControlUpdateError::Store {
                            object_key: loaded.object_key,
                            message: error.to_string(),
                        }))
                    }
                }
            }
        }
    }

    Err(E::from(ControlUpdateError::RetryExhausted {
        attempts: max_attempts,
    }))
}

pub(crate) async fn update_upload_session<S, T, F, Fut>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    max_attempts: usize,
    mut update: F,
) -> crate::error::Result<T>
where
    S: ObjectStore + ?Sized,
    F: FnMut(UploadSessionState) -> Fut,
    Fut: Future<Output = crate::error::Result<UploadSessionUpdate<T>>>,
{
    for _attempt in 0..max_attempts {
        match try_update_upload_session(store, namespace_id, upload_id, |state, _metadata| {
            update(state)
        })
        .await?
        {
            UploadSessionCas::Applied(outcome) => return Ok(outcome),
            UploadSessionCas::Conflict => continue,
        }
    }

    Err(CoreError::Internal(
        "upload session compare-and-swap retry exhausted".to_owned(),
    ))
}

/// Applies at most one upload-session compare-and-swap against the state and
/// metadata loaded together. Callers that must act only on one inspection
/// (garbage collection) retain on [`UploadSessionCas::Conflict`]; ordinary
/// upload operations wrap this helper in their bounded retry loop above.
pub(crate) async fn try_update_upload_session<S, T, F, Fut>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    update: F,
) -> crate::error::Result<UploadSessionCas<T>>
where
    S: ObjectStore + ?Sized,
    F: FnOnce(UploadSessionState, ObjectMetadata) -> Fut,
    Fut: Future<Output = crate::error::Result<UploadSessionUpdate<T>>>,
{
    let loaded = read_upload_session_object(store, namespace_id, upload_id).await?;
    let expected_etag = required_etag_core(&loaded.metadata, &loaded.object_key)?.to_owned();
    match update(loaded.envelope.state, loaded.metadata).await? {
        UploadSessionUpdate::Noop(outcome) => Ok(UploadSessionCas::Applied(outcome)),
        UploadSessionUpdate::Replace { next, outcome } => {
            let envelope =
                UploadSessionEnvelope::from_state(ControlObjectKind::UploadSession, *next)
                    .map_err(|err| {
                        CoreError::Internal(format!(
                            "failed to build upload session envelope: {err}"
                        ))
                    })?;
            let encoded = encode_control_object(&envelope).map_err(|err| {
                CoreError::Internal(format!("failed to encode upload session envelope: {err}"))
            })?;
            match store
                .compare_and_swap(&loaded.object_key, &expected_etag, Bytes::from(encoded))
                .await
            {
                Ok(_) => Ok(UploadSessionCas::Applied(outcome)),
                Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(UploadSessionCas::Conflict),
                Err(error) => Err(CoreError::store(&loaded.object_key, &error)),
            }
        }
    }
}

fn encode_head(next: HeadState, object_key: &str) -> Result<Vec<u8>, ControlUpdateError> {
    let envelope =
        HeadStateEnvelope::from_state(ControlObjectKind::WalHead, next).map_err(|err| {
            ControlUpdateError::Codec {
                object_key: object_key.to_owned(),
                message: err.to_string(),
            }
        })?;
    encode_control_object(&envelope).map_err(|err| ControlUpdateError::Codec {
        object_key: object_key.to_owned(),
        message: err.to_string(),
    })
}

fn required_etag<'a>(
    metadata: &'a ObjectMetadata,
    object_key: &str,
) -> Result<&'a str, ControlUpdateError> {
    metadata
        .etag
        .as_deref()
        .ok_or_else(|| ControlUpdateError::MissingEtag {
            object_key: object_key.to_owned(),
        })
}

fn required_etag_core<'a>(
    metadata: &'a ObjectMetadata,
    object_key: &str,
) -> crate::error::Result<&'a str> {
    metadata.etag.as_deref().ok_or_else(|| CoreError::Store {
        object_key: object_key.to_owned(),
        message: "missing control object etag".to_owned(),
        class: StoreFailureClass::Other,
    })
}

#[derive(Debug, Clone)]
struct LoadedUploadSessionObject {
    object_key: String,
    metadata: ObjectMetadata,
    envelope: UploadSessionEnvelope,
}

async fn read_upload_session_object<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
) -> crate::error::Result<LoadedUploadSessionObject> {
    let object_key = upload_session(namespace_id.as_str(), upload_id.as_str());
    let body = store
        .get_with_metadata(&object_key)
        .await
        .map_err(|err| CoreError::store(&object_key, &err))?
        .ok_or_else(|| CoreError::UploadNotFound {
            upload_id: upload_id.clone(),
        })?;
    let envelope: UploadSessionEnvelope =
        decode_control_object(&body.bytes, ControlObjectKind::UploadSession).map_err(|err| {
            CoreError::Internal(format!("invalid upload session `{object_key}`: {err}"))
        })?;
    if envelope.state.namespace_id != *namespace_id {
        return Err(CoreError::Internal(format!(
            "upload session namespace mismatch for `{object_key}`"
        )));
    }
    if envelope.state.upload_id != *upload_id {
        return Err(CoreError::Internal(format!(
            "upload session id mismatch for `{object_key}`"
        )));
    }

    Ok(LoadedUploadSessionObject {
        object_key,
        metadata: body.metadata,
        envelope,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::wire::control::{ControlObjectKind, HeadStateEnvelope};
    use loonfs_api::NamespaceId;
    use loonfs_objectstore::keys::wal_head;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_test_support::stores::{
        FailStore, InjectedError, KeyPredicate, MetadataMapStore, OperationClass,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use tempfile::tempdir;

    async fn write_initial_head(store: &LocalFsStore, namespace_id: &NamespaceId) {
        let envelope = HeadStateEnvelope::from_state(
            ControlObjectKind::WalHead,
            HeadState::initial(namespace_id.clone(), loonfs_api::ContentStoreId::generate()),
        )
        .expect("head envelope");
        let bytes = encode_control_object(&envelope).expect("head bytes");
        store
            .put_if_absent(&wal_head(namespace_id.as_str()), Bytes::from(bytes))
            .await
            .expect("write head");
    }

    #[tokio::test]
    async fn update_head_noop_returns_without_writing() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        write_initial_head(&store, &namespace_id).await;
        let before = store
            .head(&wal_head(namespace_id.as_str()))
            .await
            .expect("head")
            .expect("head exists")
            .etag;

        let outcome = update_head(&store, &namespace_id, 3, |_loaded| {
            Ok::<_, ControlUpdateError>(HeadUpdate::Noop("unchanged"))
        })
        .await
        .expect("noop update");

        let after = store
            .head(&wal_head(namespace_id.as_str()))
            .await
            .expect("head")
            .expect("head exists")
            .etag;
        assert_eq!(outcome, "unchanged");
        assert_eq!(after, before);
    }

    #[tokio::test]
    async fn update_head_retries_cas_conflict_and_succeeds() {
        let temp_dir = tempdir().expect("tempdir");
        let inner = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        write_initial_head(&inner, &namespace_id).await;
        let store = FailStore::new(
            inner,
            KeyPredicate::any(),
            OperationClass::CompareAndSwap,
            InjectedError::PreconditionFailed,
        );
        store.fail_next(1);

        let outcome = update_head(&store, &namespace_id, 3, |loaded| {
            let mut next = loaded.envelope.state.clone();
            next.seq.0 += 1;
            Ok::<_, ControlUpdateError>(HeadUpdate::Replace {
                next: Box::new(next),
                outcome: "updated",
            })
        })
        .await
        .expect("retry update");

        assert_eq!(outcome, "updated");
        assert_eq!(store.remaining(), 0);
    }

    #[tokio::test]
    async fn update_head_missing_etag_fails_without_retry() {
        let temp_dir = tempdir().expect("tempdir");
        let inner = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        write_initial_head(&inner, &namespace_id).await;
        let store = MetadataMapStore::without_etag(inner, KeyPredicate::any());

        let closure_called = AtomicBool::new(false);
        let error = update_head(&store, &namespace_id, 3, |_loaded| {
            closure_called.store(true, Ordering::SeqCst);
            Ok::<_, ControlUpdateError>(HeadUpdate::Noop(()))
        })
        .await
        .expect_err("missing etag should fail");

        assert!(matches!(error, ControlUpdateError::MissingEtag { .. }));
        assert!(!closure_called.load(Ordering::SeqCst));
    }
}
