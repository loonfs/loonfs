//! Two-step grep root loading and manifest-first pointer publication.
//!
//! Publication intentionally does not retry: a pointer compare-and-swap
//! loser gets [`GrepRootError::Conflict`](super::GrepRootError), reloads the
//! current root, and rebuilds its candidate. The losing immutable manifest
//! remains unreachable derived garbage for grep-owned collection.

use super::codec::{
    decode_grep_manifest, decode_grep_root, encode_grep_manifest, encode_grep_root,
    GrepManifestEnvelope, GrepRootEnvelope,
};
use super::error::{GrepRootError, Result};
use super::state::{GrepManifestId, GrepRootPointer, GrepRootState};
use crate::keyspace::{manifest_key, root_key};
use bytes::Bytes;
use loonfs_api::NamespaceId;
use loonfs_core::StoreFailureClass;
use loonfs_objectstore::{
    ImmutableWriteError, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};

/// A verified grep root pointer and metadata from the same store read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedGrepRootPointer {
    object_key: String,
    envelope: GrepRootEnvelope,
    metadata: ObjectMetadata,
}

impl LoadedGrepRootPointer {
    pub fn object_key(&self) -> &str {
        &self.object_key
    }

    pub fn envelope(&self) -> &GrepRootEnvelope {
        &self.envelope
    }

    pub fn pointer(&self) -> &GrepRootPointer {
        self.envelope.pointer()
    }

    pub fn metadata(&self) -> &ObjectMetadata {
        &self.metadata
    }
}

/// A verified pointer and the immutable manifest it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedGrepRoot {
    pointer: LoadedGrepRootPointer,
    manifest: GrepManifestEnvelope,
}

impl LoadedGrepRoot {
    pub fn object_key(&self) -> &str {
        self.pointer.object_key()
    }

    pub fn envelope(&self) -> &GrepRootEnvelope {
        self.pointer.envelope()
    }

    pub fn manifest_envelope(&self) -> &GrepManifestEnvelope {
        &self.manifest
    }

    pub fn state(&self) -> &GrepRootState {
        self.manifest.state()
    }

    pub fn metadata(&self) -> &ObjectMetadata {
        self.pointer.metadata()
    }
}

/// Loads and verifies one namespace's mutable grep root pointer.
pub async fn load_grep_root_pointer<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<Option<LoadedGrepRootPointer>> {
    let object_key = root_key(namespace_id);
    let Some(body) = store
        .get_with_metadata(&object_key)
        .await
        .map_err(|error| store_error(&object_key, &error))?
    else {
        return Ok(None);
    };
    let envelope = decode_grep_root(&body.bytes).map_err(|error| GrepRootError::Corrupt {
        object_key: object_key.clone(),
        message: error.to_string(),
    })?;
    if envelope.pointer().namespace_id() != namespace_id {
        return Err(GrepRootError::IdentityMismatch {
            object_key,
            expected: namespace_id.clone(),
            actual: envelope.pointer().namespace_id().clone(),
        });
    }
    Ok(Some(LoadedGrepRootPointer {
        object_key,
        envelope,
        metadata: body.metadata,
    }))
}

/// Loads and verifies one immutable manifest when it is present.
pub async fn load_grep_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_id: &GrepManifestId,
) -> Result<Option<GrepManifestEnvelope>> {
    let object_key = manifest_key(namespace_id, manifest_id);
    let Some(bytes) = store
        .get(&object_key, None)
        .await
        .map_err(|error| store_error(&object_key, &error))?
    else {
        return Ok(None);
    };
    let envelope = decode_grep_manifest(&bytes).map_err(|error| GrepRootError::Corrupt {
        object_key: object_key.clone(),
        message: error.to_string(),
    })?;
    if envelope.manifest_id() != manifest_id {
        return Err(GrepRootError::Corrupt {
            object_key,
            message: format!(
                "manifest id mismatch: key names `{manifest_id}`, payload derives `{}`",
                envelope.manifest_id()
            ),
        });
    }
    if envelope.state().namespace_id() != namespace_id {
        return Err(GrepRootError::IdentityMismatch {
            object_key,
            expected: namespace_id.clone(),
            actual: envelope.state().namespace_id().clone(),
        });
    }
    Ok(Some(envelope))
}

/// Loads a fresh pointer and then its immutable manifest.
pub async fn load_grep_root<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<Option<LoadedGrepRoot>> {
    let Some(pointer) = load_grep_root_pointer(store, namespace_id).await? else {
        return Ok(None);
    };
    let manifest_id = pointer.pointer().manifest_id();
    let Some(manifest) = load_grep_manifest(store, namespace_id, manifest_id).await? else {
        return Err(GrepRootError::MissingManifest {
            root_key: pointer.object_key.clone(),
            manifest_key: manifest_key(namespace_id, manifest_id),
        });
    };
    Ok(Some(LoadedGrepRoot { pointer, manifest }))
}

/// Seeds grep by writing an immutable manifest, then creating its pointer.
///
/// An existing pointer is a typed conflict even when it carries identical
/// bytes. Its candidate manifest remains valid derived garbage for GC.
pub async fn seed_grep_root<S: ObjectStore + ?Sized>(
    store: &S,
    state: &GrepRootState,
    writer_version: &str,
) -> Result<LoadedGrepRoot> {
    let written = write_grep_manifest(store, state, writer_version).await?;
    let manifest = written.envelope;
    let object_key = root_key(state.namespace_id());
    let envelope = GrepRootEnvelope::from_pointer(
        writer_version,
        GrepRootPointer::new(state.namespace_id().clone(), manifest.manifest_id().clone()),
    )
    .map_err(|error| corrupt(&object_key, error))?;
    let bytes = encode_grep_root(&envelope).map_err(|error| corrupt(&object_key, error))?;
    let metadata = match store
        .put(&object_key, Bytes::from(bytes), PutMode::CreateIfAbsent)
        .await
    {
        Ok(metadata) => metadata,
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
            return Err(GrepRootError::Conflict { object_key });
        }
        Err(error) => return Err(store_error(&object_key, &error)),
    };
    Ok(LoadedGrepRoot {
        pointer: LoadedGrepRootPointer {
            object_key,
            envelope,
            metadata,
        },
        manifest,
    })
}

/// Writes a successor manifest, then advances the pointer in one etag CAS.
///
/// A conflict is never retried against the stale candidate: the caller must
/// reload and recompute the whole atomic manifest state.
pub async fn advance_grep_root<S: ObjectStore + ?Sized>(
    store: &S,
    current: &LoadedGrepRoot,
    next: &GrepRootState,
    writer_version: &str,
) -> Result<LoadedGrepRoot> {
    let expected_namespace_id = current.pointer.pointer().namespace_id();
    if next.namespace_id() != expected_namespace_id {
        return Err(GrepRootError::AdvanceIdentityMismatch {
            expected: expected_namespace_id.clone(),
            actual: next.namespace_id().clone(),
        });
    }
    let expected_key = root_key(expected_namespace_id);
    if current.pointer.object_key != expected_key {
        return Err(GrepRootError::Corrupt {
            object_key: current.pointer.object_key.clone(),
            message: format!("loaded root key does not match `{expected_key}`"),
        });
    }
    let expected_etag =
        current
            .pointer
            .metadata
            .etag
            .as_deref()
            .ok_or_else(|| GrepRootError::MissingEtag {
                object_key: current.pointer.object_key.clone(),
            })?;

    let written = write_grep_manifest(store, next, writer_version).await?;
    let envelope = GrepRootEnvelope::from_pointer(
        writer_version,
        GrepRootPointer::new(
            next.namespace_id().clone(),
            written.envelope.manifest_id().clone(),
        ),
    )
    .map_err(|error| corrupt(&current.pointer.object_key, error))?;
    let bytes =
        encode_grep_root(&envelope).map_err(|error| corrupt(&current.pointer.object_key, error))?;
    let metadata = match store
        .put(
            &current.pointer.object_key,
            Bytes::from(bytes),
            PutMode::CompareAndSwap {
                expected_etag: expected_etag.to_owned(),
            },
        )
        .await
    {
        Ok(metadata) => metadata,
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
            return Err(GrepRootError::Conflict {
                object_key: current.pointer.object_key.clone(),
            });
        }
        Err(error) => return Err(store_error(&current.pointer.object_key, &error)),
    };
    // A content-addressed manifest observed through AlreadyExists may be
    // deleted by grep GC before this pointer CAS lands. Verify after the CAS;
    // if GC won, the candidate bytes are still in memory and recreate the
    // same manifest id before the successful advance is returned.
    verify_and_heal_advanced_manifest(store, next.namespace_id(), &written).await?;
    Ok(LoadedGrepRoot {
        pointer: LoadedGrepRootPointer {
            object_key: current.pointer.object_key.clone(),
            envelope,
            metadata,
        },
        manifest: written.envelope,
    })
}

struct WrittenGrepManifest {
    envelope: GrepManifestEnvelope,
    bytes: Bytes,
}

async fn write_grep_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    state: &GrepRootState,
    writer_version: &str,
) -> Result<WrittenGrepManifest> {
    let envelope = GrepManifestEnvelope::from_state(writer_version, state.clone())
        .map_err(|error| corrupt(&root_key(state.namespace_id()), error))?;
    let object_key = manifest_key(state.namespace_id(), envelope.manifest_id());
    let bytes =
        Bytes::from(encode_grep_manifest(&envelope).map_err(|error| corrupt(&object_key, error))?);
    store
        .put_immutable_verified(&object_key, bytes.clone())
        .await
        .map_err(immutable_write_error)?;
    Ok(WrittenGrepManifest { envelope, bytes })
}

async fn verify_and_heal_advanced_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    written: &WrittenGrepManifest,
) -> Result<()> {
    let object_key = manifest_key(namespace_id, written.envelope.manifest_id());
    if store
        .head(&object_key)
        .await
        .map_err(|error| store_error(&object_key, &error))?
        .is_some()
    {
        return Ok(());
    }
    store
        .put_immutable_verified(&object_key, written.bytes.clone())
        .await
        .map_err(immutable_write_error)
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
        message: error.message(),
        class: StoreFailureClass::of(error),
    }
}

fn immutable_write_error(error: ImmutableWriteError) -> GrepRootError {
    let fallback_object_key = error.object_key().to_owned();
    match error {
        ImmutableWriteError::DifferentObject { object_key } => GrepRootError::Corrupt {
            object_key,
            message: "immutable object contains different bytes".to_owned(),
        },
        ImmutableWriteError::Transport { object_key, source } => store_error(&object_key, &source),
        error => GrepRootError::Corrupt {
            object_key: fallback_object_key,
            message: error.to_string(),
        },
    }
}
