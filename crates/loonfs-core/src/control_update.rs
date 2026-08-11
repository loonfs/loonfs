//! Read-modify-write loops for control objects: load, edit, and
//! compare-and-swap the head or an upload session on its etag.

use crate::control_load::{
    core_control_load_error, expect_identity_field, expect_namespace, load_control_object,
    LoadedControl,
};
use crate::error::CoreError;
use crate::namespace::control::{read_head_object, ControlObjectLoadError, LoadedHeadObject};
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_object, ControlObjectKind, HeadState, HeadStateEnvelope, UploadSessionEnvelope,
    UploadSessionState,
};
use loonfs_api::{NamespaceId, UploadId};
use loonfs_objectstore::keys::upload_session;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use std::future::Future;
use thiserror::Error;

/// Replacement head state and the value returned after its CAS succeeds.
/// An update that should not write must return an error instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HeadReplacement<T> {
    pub(crate) next: Box<HeadState>,
    pub(crate) outcome: T,
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
    #[error("control object codec error for `{object_key}`: {message}")]
    Codec { object_key: String, message: String },
    #[error("control object store error for `{object_key}`: {message}")]
    Store { object_key: String, message: String },
    #[error("control object update retries exhausted after {attempts} attempts")]
    RetryExhausted { attempts: usize },
}

/// Reads the head, asks `update` to build a replacement, and applies it with
/// a CAS against the loaded ETag. CAS conflicts retry the complete
/// read-update-CAS cycle. Errors returned by `update` are returned immediately,
/// which prevents an invalid writer or stale manifest from being overwritten.
pub(crate) async fn update_head<S, T, E, F>(
    store: &S,
    namespace_id: &NamespaceId,
    max_attempts: usize,
    mut update: F,
) -> Result<T, E>
where
    S: ObjectStore + ?Sized,
    E: From<ControlUpdateError>,
    F: FnMut(&LoadedHeadObject) -> Result<HeadReplacement<T>, E>,
{
    for _attempt in 0..max_attempts {
        let loaded = read_head_object(store, namespace_id)
            .await
            .map_err(|error| E::from(ControlUpdateError::LoadHead(error)))?;
        let HeadReplacement { next, outcome } = update(&loaded)?;
        let encoded = encode_head(*next, &loaded.object_key).map_err(E::from)?;
        match store
            .compare_and_swap(&loaded.object_key, &loaded.etag, Bytes::from(encoded))
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
        match try_update_upload_session(store, namespace_id, upload_id, &mut update).await? {
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
    F: FnOnce(UploadSessionState) -> Fut,
    Fut: Future<Output = crate::error::Result<UploadSessionUpdate<T>>>,
{
    let loaded = read_upload_session_object(store, namespace_id, upload_id).await?;
    match update(loaded.state).await? {
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
                .compare_and_swap(&loaded.object_key, &loaded.etag, Bytes::from(encoded))
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

/// Reads an upload session without retaining its ETag. Use this for status
/// checks and other operations that do not need to update the session.
pub(crate) async fn read_upload_session_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
) -> crate::error::Result<UploadSessionState> {
    Ok(read_upload_session_object(store, namespace_id, upload_id)
        .await?
        .state)
}

type LoadedUploadSessionObject = LoadedControl<UploadSessionState>;

async fn read_upload_session_object<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
) -> crate::error::Result<LoadedUploadSessionObject> {
    let object_key = upload_session(namespace_id.as_str(), upload_id.as_str());
    let loaded = load_control_object(
        store,
        object_key,
        ControlObjectKind::UploadSession,
        |state: &UploadSessionState| {
            expect_namespace(namespace_id, &state.namespace_id)?;
            expect_identity_field("upload id", upload_id.as_str(), state.upload_id.as_str())
        },
    )
    .await;
    match loaded {
        Ok(loaded) => Ok(loaded),
        Err(ControlObjectLoadError::MissingObject { .. }) => Err(CoreError::UploadNotFound {
            upload_id: upload_id.clone(),
        }),
        Err(error) => Err(core_control_load_error(error)),
    }
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
            let mut next = loaded.state.clone();
            next.seq.0 += 1;
            Ok::<_, ControlUpdateError>(HeadReplacement {
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
        let error = update_head(&store, &namespace_id, 3, |loaded| {
            closure_called.store(true, Ordering::SeqCst);
            Ok::<_, ControlUpdateError>(HeadReplacement {
                next: Box::new(loaded.state.clone()),
                outcome: (),
            })
        })
        .await
        .expect_err("missing etag should fail");

        assert!(matches!(
            error,
            ControlUpdateError::LoadHead(ControlObjectLoadError::Store { message, .. })
                if message.contains("required control-object etag")
        ));
        assert!(!closure_called.load(Ordering::SeqCst));
    }
}
