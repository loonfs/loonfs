//! Shared writes and compare-and-swap loops for mutable control objects.

use crate::control_object::{
    expect_identity_field, expect_namespace, load_control_object, ControlObjectLoadError,
    LoadedControl,
};
use crate::error::{CoreError, StoreFailureClass};
use crate::limits::CONTENTION_RETRY_LIMIT;
use crate::namespace::control::{load_head_object, LoadedHeadObject};
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_state, ControlObjectKind, HeadState, UploadSessionState,
};
use loonfs_api::{NamespaceId, UploadId};
use loonfs_objectstore::keys::{upload_session, wal_head};
use loonfs_objectstore::{ImmutableWriteError, ObjectMetadata, ObjectStore, ObjectStoreError};
use std::future::Future;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CasAttempt<T> {
    Settled(T),
    Contended,
}

/// Runs up to [`CONTENTION_RETRY_LIMIT`] attempts.
///
/// Returns `None` if every attempt encounters contention.
pub(crate) async fn retry_while_contended<T, E, F, Fut>(
    mut attempt: F,
) -> std::result::Result<Option<T>, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<CasAttempt<T>, E>>,
{
    for _attempt in 0..CONTENTION_RETRY_LIMIT {
        if let CasAttempt::Settled(outcome) = attempt().await? {
            return Ok(Some(outcome));
        }
    }
    Ok(None)
}

/// Creates a control object and reports a generated-ID collision as an internal error.
///
/// A transport failure from the first conditional write has an unknown
/// outcome. Retry it through the immutable-write path, which accepts success
/// only when the generated key contains the exact intended bytes. An immediate
/// precondition failure remains a collision rather than adopting a record from
/// another generated-ID owner.
pub(crate) async fn create_control_object_under_generated_id<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    encoded: Bytes,
) -> crate::error::Result<ObjectMetadata> {
    let collision = || {
        CoreError::Internal(format!(
            "a generated id collided with the existing control object `{object_key}`"
        ))
    };
    match store.put_if_absent(object_key, encoded.clone()).await {
        Ok(metadata) => Ok(metadata),
        Err(ObjectStoreError::PreconditionFailed { .. }) => Err(collision()),
        Err(ObjectStoreError::Transport { .. }) => {
            match store.put_immutable_verified(object_key, encoded).await {
                Ok(metadata) => Ok(metadata),
                Err(ImmutableWriteError::DifferentObject { .. }) => Err(collision()),
                Err(ImmutableWriteError::Transport { source, .. }) => {
                    Err(CoreError::store(object_key, &source))
                }
                Err(error) => Err(CoreError::Internal(format!(
                    "generated-id write reconciliation failed for `{object_key}`: {error}"
                ))),
            }
        }
        Err(error) => Err(CoreError::store(object_key, &error)),
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum ControlUpdateError {
    #[error(transparent)]
    LoadHead(ControlObjectLoadError),
    #[error("control object codec error for `{object_key}`: {message}")]
    Codec { object_key: String, message: String },
    #[error("control object store error for `{object_key}`: {message}")]
    Store {
        object_key: String,
        message: String,
        class: StoreFailureClass,
    },
    #[error("{}", crate::error::contention_message(object_key))]
    RetryExhausted { object_key: String },
}

/// Reads the head, asks `update` to build a replacement, and applies it with
/// a CAS against the loaded ETag. CAS conflicts retry the complete
/// read-update-CAS cycle. Errors returned by `update` are returned immediately,
/// which prevents an invalid writer or stale manifest from being overwritten.
pub(crate) async fn update_head<S, T, E, F>(
    store: &S,
    namespace_id: &NamespaceId,
    update: F,
) -> Result<T, E>
where
    S: ObjectStore + ?Sized,
    E: From<ControlUpdateError>,
    F: Fn(&LoadedHeadObject) -> Result<HeadReplacement<T>, E>,
{
    let update = &update;
    let updated = retry_while_contended(|| async move {
        let loaded = load_head_object(store, namespace_id)
            .await
            .map_err(|error| E::from(ControlUpdateError::LoadHead(error)))?;
        let HeadReplacement { next, outcome } = update(&loaded)?;
        let encoded = encode_head(*next, &loaded.object_key).map_err(E::from)?;
        match store
            .compare_and_swap(&loaded.object_key, &loaded.etag, Bytes::from(encoded))
            .await
        {
            Ok(_) => Ok(CasAttempt::Settled(outcome)),
            Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(CasAttempt::Contended),
            // Do not retry an unknown outcome because each attempt allocates a new epoch.
            Err(error) => Err(E::from(ControlUpdateError::Store {
                object_key: loaded.object_key,
                message: error.public_message().into_owned(),
                class: StoreFailureClass::of(&error),
            })),
        }
    })
    .await?;
    updated.ok_or_else(|| {
        E::from(ControlUpdateError::RetryExhausted {
            object_key: wal_head(namespace_id),
        })
    })
}

pub(crate) async fn update_upload_session<S, T, F, Fut>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    update: F,
) -> crate::error::Result<T>
where
    S: ObjectStore + ?Sized,
    F: Fn(UploadSessionState) -> Fut,
    Fut: Future<Output = crate::error::Result<UploadSessionUpdate<T>>>,
{
    let update = &update;
    let updated = retry_while_contended(|| async move {
        try_update_upload_session(store, namespace_id, upload_id, update).await
    })
    .await?;
    updated.ok_or_else(|| CoreError::contention_exhausted(&upload_session(namespace_id, upload_id)))
}

/// Tries one upload-session update against the state and ETag loaded together.
pub(crate) async fn try_update_upload_session<S, T, F, Fut>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    update: F,
) -> crate::error::Result<CasAttempt<T>>
where
    S: ObjectStore + ?Sized,
    F: FnOnce(UploadSessionState) -> Fut,
    Fut: Future<Output = crate::error::Result<UploadSessionUpdate<T>>>,
{
    let loaded = load_upload_session_object(store, namespace_id, upload_id).await?;
    match update(loaded.state).await? {
        UploadSessionUpdate::Noop(outcome) => Ok(CasAttempt::Settled(outcome)),
        UploadSessionUpdate::Replace { next, outcome } => {
            let encoded = encode_control_state(ControlObjectKind::UploadSession, next.as_ref())
                .map_err(|error| CoreError::Codec {
                    object_key: loaded.object_key.clone(),
                    message: error.to_string(),
                })?;
            match store
                .compare_and_swap(&loaded.object_key, &loaded.etag, Bytes::from(encoded))
                .await
            {
                Ok(_) => Ok(CasAttempt::Settled(outcome)),
                Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(CasAttempt::Contended),
                Err(error) => Err(CoreError::store(&loaded.object_key, &error)),
            }
        }
    }
}

fn encode_head(next: HeadState, object_key: &str) -> Result<Vec<u8>, ControlUpdateError> {
    encode_control_state(ControlObjectKind::WalHead, &next).map_err(|err| {
        ControlUpdateError::Codec {
            object_key: object_key.to_owned(),
            message: err.to_string(),
        }
    })
}

/// Reads an upload session without retaining its ETag. Use this for status
/// checks and other operations that do not need to update the session.
pub(crate) async fn load_upload_session_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
) -> crate::error::Result<UploadSessionState> {
    Ok(load_upload_session_object(store, namespace_id, upload_id)
        .await?
        .state)
}

type LoadedUploadSessionObject = LoadedControl<UploadSessionState>;

async fn load_upload_session_object<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
) -> crate::error::Result<LoadedUploadSessionObject> {
    let object_key = upload_session(namespace_id, upload_id);
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
        Err(error) => Err(CoreError::ControlObjectLoad(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::wire::control::{encode_control_object, ControlObjectKind, HeadStateEnvelope};
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
            HeadState::initial(
                namespace_id.clone(),
                loonfs_api::ContentStoreId::generate(),
                1_000,
            ),
        )
        .expect("head envelope");
        let bytes = encode_control_object(&envelope).expect("head bytes");
        store
            .put_if_absent(&wal_head(namespace_id), Bytes::from(bytes))
            .await
            .expect("write head");
    }

    #[tokio::test]
    async fn generated_id_create_recovers_when_the_first_write_lands_ambiguously() {
        let temp_dir = tempdir().expect("tempdir");
        let inner = LocalFsStore::new(temp_dir.path()).expect("store");
        let object_key = "namespaces/demo/checkpoints/chk_00000000000000000000000000000001.json";
        let payload = Bytes::from_static(b"generated control record");
        let store = FailStore::new(
            inner,
            KeyPredicate::exact(object_key),
            OperationClass::PutCreateIfAbsent,
            InjectedError::Transport("lost write acknowledgement".to_owned()),
        )
        .apply_then_fail();
        store.fail_next(1);

        let metadata =
            create_control_object_under_generated_id(&store, object_key, payload.clone())
                .await
                .expect("exact read-back reconciles the landed write");

        assert_eq!(store.attempts(), 2);
        assert!(metadata.etag.is_some());
        assert_eq!(
            store.get(object_key, None).await.expect("read record"),
            Some(payload)
        );
    }

    #[tokio::test]
    async fn generated_id_create_does_not_adopt_an_existing_identical_record() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let object_key = "namespaces/demo/checkpoints/chk_00000000000000000000000000000001.json";
        let payload = Bytes::from_static(b"generated control record");
        store
            .put_if_absent(object_key, payload.clone())
            .await
            .expect("seed colliding record");

        let error = create_control_object_under_generated_id(&store, object_key, payload)
            .await
            .expect_err("an immediate precondition failure remains a collision");

        assert!(matches!(
            error,
            CoreError::Internal(message) if message.contains("generated id collided")
        ));
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

        let outcome = update_head(&store, &namespace_id, |loaded| {
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
        let error = update_head(&store, &namespace_id, |loaded| {
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
