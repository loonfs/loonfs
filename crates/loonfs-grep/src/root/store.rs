//! Numbered manifest discovery and publication through a monotonic hint.

use super::codec::{
    decode_grep_hint, decode_grep_manifest, encode_grep_hint, encode_grep_manifest,
    GrepManifestEnvelope,
};
use super::error::{GrepRootError, Result};
use super::state::{GrepHint, GrepManifestState};
use crate::keyspace::{hint_key, manifest_key};
use bytes::Bytes;
use loonfs::{StoreFailureClass, METADATA_PUBLICATION_BUDGET_MS};
use loonfs_api::{ManifestNo, NamespaceId};
use loonfs_objectstore::timing::MonotonicTimer;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedGrepHint {
    pub state: GrepHint,
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedGrepManifest {
    pub hint: LoadedGrepHint,
    manifest: GrepManifestEnvelope,
}

impl LoadedGrepManifest {
    pub fn manifest_envelope(&self) -> &GrepManifestEnvelope {
        &self.manifest
    }

    pub fn manifest_no(&self) -> ManifestNo {
        self.manifest_state().manifest_no()
    }

    pub fn manifest_state(&self) -> &GrepManifestState {
        self.manifest.payload()
    }
}

pub async fn load_grep_hint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<Option<LoadedGrepHint>> {
    let object_key = hint_key(namespace_id);
    let Some(body) = store
        .get_with_metadata(&object_key)
        .await
        .map_err(|error| store_error(&object_key, &error))?
    else {
        return Ok(None);
    };
    let envelope = decode_grep_hint(&body.bytes).map_err(|error| corrupt(&object_key, error))?;
    let state = envelope.payload();
    if &state.namespace_id != namespace_id {
        return Err(GrepRootError::IdentityMismatch {
            object_key,
            expected_namespace_id: namespace_id.clone(),
            actual_namespace_id: state.namespace_id.clone(),
        });
    }
    if state.manifest_no == ManifestNo(0) {
        return Err(corrupt(&object_key, "manifest hint must be at least one"));
    }
    Ok(Some(LoadedGrepHint {
        state: state.clone(),
        etag: body.metadata.etag.unwrap_or_default(),
    }))
}

pub async fn load_grep_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_no: ManifestNo,
) -> Result<Option<GrepManifestEnvelope>> {
    let object_key = manifest_key(namespace_id, &manifest_no);
    let Some(bytes) = store
        .get(&object_key, None)
        .await
        .map_err(|error| store_error(&object_key, &error))?
    else {
        return Ok(None);
    };
    let envelope = decode_grep_manifest(&bytes).map_err(|error| corrupt(&object_key, error))?;
    if envelope.payload().namespace_id() != namespace_id {
        return Err(GrepRootError::IdentityMismatch {
            object_key,
            expected_namespace_id: namespace_id.clone(),
            actual_namespace_id: envelope.payload().namespace_id().clone(),
        });
    }
    if envelope.payload().manifest_no() != manifest_no {
        return Err(corrupt(
            &object_key,
            format!(
                "expected manifest number `{manifest_no}`, actual `{}`",
                envelope.payload().manifest_no()
            ),
        ));
    }
    Ok(Some(envelope))
}

pub async fn load_current_grep_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<Option<LoadedGrepManifest>> {
    let Some(hint) = load_grep_hint(store, namespace_id).await? else {
        return Ok(None);
    };
    let Some(mut manifest) =
        load_grep_manifest(store, namespace_id, hint.state.manifest_no).await?
    else {
        if hint.state.manifest_no == ManifestNo(1) {
            return Ok(None);
        }
        return Err(corrupt(
            &hint_key(namespace_id),
            format!("hinted manifest `{}` is missing", hint.state.manifest_no),
        ));
    };
    while let Ok(next) = manifest.payload().manifest_no().successor() {
        let Some(successor) = load_grep_manifest(store, namespace_id, next).await? else {
            break;
        };
        manifest = successor;
    }
    Ok(Some(LoadedGrepManifest { hint, manifest }))
}

pub async fn publish_grep_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    current: Option<&LoadedGrepManifest>,
    next: &GrepManifestState,
    timer: &impl MonotonicTimer,
    started_ms: u64,
) -> crate::Result<LoadedGrepManifest> {
    let namespace_id = next.namespace_id();
    let object_key = manifest_key(namespace_id, &next.manifest_no());
    let expected_manifest_no = match current {
        Some(current) => {
            if current.manifest_state().namespace_id() != namespace_id {
                return Err(GrepRootError::IdentityMismatch {
                    object_key,
                    expected_namespace_id: current.manifest_state().namespace_id().clone(),
                    actual_namespace_id: namespace_id.clone(),
                }
                .into());
            }
            current
                .manifest_no()
                .successor()
                .map_err(|error| corrupt(&object_key, error))?
        }
        None => ManifestNo(1),
    };
    if next.manifest_no() != expected_manifest_no {
        return Err(corrupt(
            &object_key,
            format!(
                "expected manifest number `{expected_manifest_no}`, actual `{}`",
                next.manifest_no()
            ),
        )
        .into());
    }
    let (manifest, bytes) = encode_grep_manifest(next.clone())
        .map_err(|error| corrupt(&object_key, error))?
        .into_parts();
    loonfs::ensure_metadata_publication_budget(timer, started_ms, namespace_id)?;
    let hint = match current {
        Some(current) => current.hint.clone(),
        None => create_grep_hint(store, namespace_id).await?,
    };
    loonfs::ensure_metadata_publication_budget(timer, started_ms, namespace_id)?;
    match store.put_if_absent(&object_key, Bytes::from(bytes)).await {
        Ok(_) => {}
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
            return Err(GrepRootError::Conflict { object_key }.into())
        }
        Err(error) => return Err(store_error(&object_key, &error).into()),
    }
    let mut loaded = LoadedGrepManifest { hint, manifest };
    match raise_grep_hint(
        store,
        namespace_id,
        next.manifest_no(),
        loaded.hint.clone(),
        timer,
        started_ms,
    )
    .await
    {
        Ok(hint) => loaded.hint = hint,
        Err(error) => {
            tracing::warn!(%namespace_id, %error, "grep hint raise failed after publication")
        }
    }
    Ok(loaded)
}

async fn create_grep_hint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<LoadedGrepHint> {
    let object_key = hint_key(namespace_id);
    let state = GrepHint {
        namespace_id: namespace_id.clone(),
        manifest_no: ManifestNo(1),
    };
    let bytes = encode_grep_hint(state.clone())
        .map_err(|error| corrupt(&object_key, error))?
        .into_parts()
        .1;
    match store.put_if_absent(&object_key, Bytes::from(bytes)).await {
        Ok(metadata) => Ok(LoadedGrepHint {
            state,
            etag: metadata.etag.unwrap_or_default(),
        }),
        Err(ObjectStoreError::PreconditionFailed { .. }) => load_grep_hint(store, namespace_id)
            .await?
            .ok_or_else(|| corrupt(&object_key, "grep hint disappeared during creation")),
        Err(error) => Err(store_error(&object_key, &error)),
    }
}

pub async fn raise_grep_hint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_no: ManifestNo,
    mut current: LoadedGrepHint,
    timer: &impl MonotonicTimer,
    started_ms: u64,
) -> Result<LoadedGrepHint> {
    let object_key = hint_key(namespace_id);
    while timer.monotonic_now_ms().saturating_sub(started_ms) <= METADATA_PUBLICATION_BUDGET_MS {
        let raised = GrepHint {
            namespace_id: namespace_id.clone(),
            manifest_no: current.state.manifest_no.max(manifest_no),
        };
        if raised == current.state {
            break;
        }
        let bytes = encode_grep_hint(raised.clone())
            .map_err(|error| corrupt(&object_key, error))?
            .into_parts()
            .1;
        match store
            .compare_and_swap(&object_key, &current.etag, Bytes::from(bytes))
            .await
        {
            Ok(metadata) => {
                return Ok(LoadedGrepHint {
                    state: raised,
                    etag: metadata.etag.unwrap_or_default(),
                })
            }
            Err(ObjectStoreError::PreconditionFailed { .. }) => {
                current = load_grep_hint(store, namespace_id).await?.ok_or_else(|| {
                    corrupt(&object_key, "grep hint disappeared during publication")
                })?;
            }
            Err(error) => return Err(store_error(&object_key, &error)),
        }
    }
    Ok(current)
}

fn corrupt(object_key: &str, error: impl ToString) -> GrepRootError {
    GrepRootError::Corrupt {
        object_key: object_key.to_owned(),
        message: error.to_string(),
    }
}

fn store_error(object_key: &str, error: &ObjectStoreError) -> GrepRootError {
    GrepRootError::Store {
        object_key: object_key.to_owned(),
        message: error.public_message().into_owned(),
        class: StoreFailureClass::of(error),
    }
}
