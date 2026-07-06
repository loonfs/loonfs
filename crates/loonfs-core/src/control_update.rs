use crate::error::CoreError;
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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum ControlUpdateError {
    #[error(transparent)]
    LoadHead(ControlObjectLoadError),
    #[error("missing etag for `{object_key}`")]
    MissingEtag { object_key: String },
    #[error("control object codec error: {0}")]
    Codec(String),
    #[error("control object store error: {0}")]
    Store(String),
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
    writer_version: &str,
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
                let encoded = encode_head(writer_version, *next).map_err(E::from)?;
                match store
                    .compare_and_swap(&loaded.object_key, expected_etag, Bytes::from(encoded))
                    .await
                {
                    Ok(_) => return Ok(outcome),
                    Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => {
                        continue;
                    }
                    Err(error) => {
                        return Err(E::from(ControlUpdateError::Store(error.to_string())))
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
    writer_version: &str,
    max_attempts: usize,
    mut update: F,
) -> Result<T, CoreError>
where
    S: ObjectStore + ?Sized,
    F: FnMut(UploadSessionState) -> Fut,
    Fut: Future<Output = Result<UploadSessionUpdate<T>, CoreError>>,
{
    for _attempt in 0..max_attempts {
        let loaded = read_upload_session_object(store, namespace_id, upload_id).await?;
        let expected_etag = required_etag_core(&loaded.metadata, &loaded.object_key)?;

        match update(loaded.envelope.state).await? {
            UploadSessionUpdate::Noop(outcome) => return Ok(outcome),
            UploadSessionUpdate::Replace { next, outcome } => {
                let envelope = UploadSessionEnvelope::from_state(
                    ControlObjectKind::UploadSession,
                    writer_version,
                    *next,
                )
                .map_err(|err| CoreError::Store(err.to_string()))?;
                let encoded = encode_control_object(&envelope)
                    .map_err(|err| CoreError::Store(err.to_string()))?;
                match store
                    .compare_and_swap(&loaded.object_key, expected_etag, Bytes::from(encoded))
                    .await
                {
                    Ok(_) => return Ok(outcome),
                    Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => {
                        continue;
                    }
                    Err(error) => return Err(CoreError::Store(error.to_string())),
                }
            }
        }
    }

    Err(CoreError::Store(
        "upload session compare-and-swap retry exhausted".to_owned(),
    ))
}

fn encode_head(writer_version: &str, next: HeadState) -> Result<Vec<u8>, ControlUpdateError> {
    let envelope = HeadStateEnvelope::from_state(ControlObjectKind::WalHead, writer_version, next)
        .map_err(|err| ControlUpdateError::Codec(err.to_string()))?;
    encode_control_object(&envelope).map_err(|err| ControlUpdateError::Codec(err.to_string()))
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
) -> Result<&'a str, CoreError> {
    metadata
        .etag
        .as_deref()
        .ok_or_else(|| CoreError::Store(format!("missing control object etag for `{object_key}`")))
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
) -> Result<LoadedUploadSessionObject, CoreError> {
    let object_key = upload_session(namespace_id.as_str(), upload_id.as_str());
    let body = store
        .get_with_metadata(&object_key)
        .await
        .map_err(|err| CoreError::Store(err.to_string()))?
        .ok_or_else(|| CoreError::UploadNotFound {
            upload_id: upload_id.clone(),
        })?;
    let envelope: UploadSessionEnvelope =
        decode_control_object(&body.bytes, ControlObjectKind::UploadSession).map_err(|err| {
            CoreError::Store(format!("invalid upload session `{object_key}`: {err}"))
        })?;
    if envelope.state.namespace_id != *namespace_id {
        return Err(CoreError::Store(format!(
            "upload session namespace mismatch for `{object_key}`"
        )));
    }
    if envelope.state.upload_id != *upload_id {
        return Err(CoreError::Store(format!(
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
    use async_trait::async_trait;
    use futures::stream::{self, BoxStream};
    use loonfs_api::wire::control::{ControlObjectKind, HeadStateEnvelope};
    use loonfs_api::NamespaceId;
    use loonfs_objectstore::fs::LocalFsStore;
    use loonfs_objectstore::keys::wal_head;
    use loonfs_objectstore::{ByteRange, ObjectBody, PutMode};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tempfile::tempdir;

    const WRITER_VERSION: &str = "writer/0.1.0";

    async fn write_initial_head(store: &LocalFsStore, namespace_id: &NamespaceId) {
        let envelope = HeadStateEnvelope::from_state(
            ControlObjectKind::WalHead,
            WRITER_VERSION,
            HeadState::initial(namespace_id.clone()),
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

        let outcome = update_head(&store, &namespace_id, WRITER_VERSION, 3, |_loaded| {
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
        let store = ConflictOnceStore {
            inner,
            remaining_conflicts: AtomicUsize::new(1),
        };

        let outcome = update_head(&store, &namespace_id, WRITER_VERSION, 3, |loaded| {
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
        assert_eq!(store.remaining_conflicts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn update_head_missing_etag_fails_without_retry() {
        let temp_dir = tempdir().expect("tempdir");
        let inner = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        write_initial_head(&inner, &namespace_id).await;
        let store = MissingEtagStore { inner };

        let closure_called = AtomicBool::new(false);
        let error = update_head(&store, &namespace_id, WRITER_VERSION, 3, |_loaded| {
            closure_called.store(true, Ordering::SeqCst);
            Ok::<_, ControlUpdateError>(HeadUpdate::Noop(()))
        })
        .await
        .expect_err("missing etag should fail");

        assert!(matches!(error, ControlUpdateError::MissingEtag { .. }));
        assert!(!closure_called.load(Ordering::SeqCst));
    }

    #[derive(Debug)]
    struct ConflictOnceStore {
        inner: LocalFsStore,
        remaining_conflicts: AtomicUsize,
    }

    #[async_trait]
    impl ObjectStore for ConflictOnceStore {
        async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
            self.inner.head(key).await
        }

        async fn get_with_metadata(
            &self,
            key: &str,
        ) -> Result<Option<ObjectBody>, ObjectStoreError> {
            self.inner.get_with_metadata(key).await
        }

        async fn get(
            &self,
            key: &str,
            range: Option<ByteRange>,
        ) -> Result<Option<Bytes>, ObjectStoreError> {
            self.inner.get(key, range).await
        }

        async fn put(
            &self,
            key: &str,
            bytes: Bytes,
            mode: PutMode,
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            self.inner.put(key, bytes, mode).await
        }

        async fn compare_and_swap(
            &self,
            key: &str,
            expected_etag: &str,
            bytes: Bytes,
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            if self.remaining_conflicts.load(Ordering::SeqCst) > 0 {
                self.remaining_conflicts.fetch_sub(1, Ordering::SeqCst);
                return Err(ObjectStoreError::PreconditionFailed);
            }
            self.inner.compare_and_swap(key, expected_etag, bytes).await
        }

        async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
            self.inner.delete(key).await
        }

        fn list_prefix_stream(
            &self,
            prefix: &str,
        ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
            self.inner.list_prefix_stream(prefix)
        }
    }

    #[derive(Debug)]
    struct MissingEtagStore {
        inner: LocalFsStore,
    }

    #[async_trait]
    impl ObjectStore for MissingEtagStore {
        async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
            self.inner.head(key).await
        }

        async fn get_with_metadata(
            &self,
            key: &str,
        ) -> Result<Option<ObjectBody>, ObjectStoreError> {
            let Some(mut body) = self.inner.get_with_metadata(key).await? else {
                return Ok(None);
            };
            body.metadata.etag = None;
            Ok(Some(body))
        }

        async fn get(
            &self,
            key: &str,
            range: Option<ByteRange>,
        ) -> Result<Option<Bytes>, ObjectStoreError> {
            self.inner.get(key, range).await
        }

        async fn put(
            &self,
            key: &str,
            bytes: Bytes,
            mode: PutMode,
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            self.inner.put(key, bytes, mode).await
        }

        async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
            self.inner.delete(key).await
        }

        fn list_prefix_stream(
            &self,
            _prefix: &str,
        ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
            Box::pin(stream::empty())
        }
    }
}
