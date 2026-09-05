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
use super::state::{GrepManifestObjectId, GrepManifestState, GrepRootPointer};
use crate::keyspace::{manifest_key, root_key};
use bytes::Bytes;
use loonfs::StoreFailureClass;
use loonfs_api::NamespaceId;
use loonfs_objectstore::{ImmutableWriteError, ObjectStore, ObjectStoreError, PutMode};

/// A verified grep root pointer and metadata from the same store read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedGrepRootPointer {
    object_key: String,
    envelope: GrepRootEnvelope,
    etag: String,
}

impl LoadedGrepRootPointer {
    pub fn object_key(&self) -> &str {
        &self.object_key
    }

    pub fn envelope(&self) -> &GrepRootEnvelope {
        &self.envelope
    }

    pub fn pointer(&self) -> &GrepRootPointer {
        self.envelope.payload()
    }

    pub fn etag(&self) -> &str {
        &self.etag
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

    /// Object the pointer names, which is the manifest's whole identity.
    pub fn manifest_object_id(&self) -> &GrepManifestObjectId {
        self.pointer.pointer().manifest_object_id()
    }

    pub fn manifest_state(&self) -> &GrepManifestState {
        self.manifest.payload()
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
    let etag = required_etag(&object_key, body.metadata.etag)?;
    let envelope = decode_grep_root(&body.bytes).map_err(|error| corrupt(&object_key, error))?;
    if envelope.payload().namespace_id() != namespace_id {
        return Err(GrepRootError::IdentityMismatch {
            object_key,
            expected: namespace_id.clone(),
            actual: envelope.payload().namespace_id().clone(),
        });
    }
    Ok(Some(LoadedGrepRootPointer {
        object_key,
        envelope,
        etag,
    }))
}

/// Loads the immutable manifest a verified pointer names, when it is present.
///
/// The pointer carries the digest of the bytes its publisher installed, so
/// the object the key resolves to is checked against what the pointer
/// promised — the same binding a metadata root holds over its namespace
/// manifest.
pub async fn load_grep_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    pointer: &GrepRootPointer,
) -> Result<Option<GrepManifestEnvelope>> {
    let object_key = manifest_key(namespace_id, pointer.manifest_object_id());
    let Some(bytes) = store
        .get(&object_key, None)
        .await
        .map_err(|error| store_error(&object_key, &error))?
    else {
        return Ok(None);
    };
    let envelope = decode_grep_manifest(&bytes).map_err(|error| corrupt(&object_key, error))?;
    if envelope.payload_checksum() != pointer.manifest_payload_checksum() {
        return Err(GrepRootError::Corrupt {
            object_key,
            message: format!(
                "manifest payload checksum mismatch: pointer names `{}`, object carries `{}`",
                pointer.manifest_payload_checksum(),
                envelope.payload_checksum()
            ),
        });
    }
    if envelope.payload().namespace_id() != namespace_id {
        return Err(GrepRootError::IdentityMismatch {
            object_key,
            expected: namespace_id.clone(),
            actual: envelope.payload().namespace_id().clone(),
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
    let Some(manifest) = load_grep_manifest(store, namespace_id, pointer.pointer()).await? else {
        return Err(GrepRootError::MissingManifest {
            root_key: pointer.object_key.clone(),
            manifest_key: manifest_key(namespace_id, pointer.pointer().manifest_object_id()),
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
    state: &GrepManifestState,
) -> Result<LoadedGrepRoot> {
    let written = write_grep_manifest(store, state).await?;
    let manifest = written.envelope;
    let object_key = root_key(state.namespace_id());
    let (envelope, bytes) = encode_grep_root(GrepRootPointer::new(
        state.namespace_id().clone(),
        written.manifest_object_id,
        manifest.payload_checksum().to_owned(),
    ))
    .map_err(|error| corrupt(&object_key, error))?
    .into_parts();
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
    let etag = required_etag(&object_key, metadata.etag)?;
    Ok(LoadedGrepRoot {
        pointer: LoadedGrepRootPointer {
            object_key,
            envelope,
            etag,
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
    next: &GrepManifestState,
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
    let written = write_grep_manifest(store, next).await?;
    let (envelope, bytes) = encode_grep_root(GrepRootPointer::new(
        next.namespace_id().clone(),
        written.manifest_object_id,
        written.envelope.payload_checksum().to_owned(),
    ))
    .map_err(|error| corrupt(&current.pointer.object_key, error))?
    .into_parts();
    let metadata = match store
        .put(
            &current.pointer.object_key,
            Bytes::from(bytes),
            PutMode::CompareAndSwap {
                expected_etag: current.pointer.etag.clone(),
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
    let etag = required_etag(&current.pointer.object_key, metadata.etag)?;
    Ok(LoadedGrepRoot {
        pointer: LoadedGrepRootPointer {
            object_key: current.pointer.object_key.clone(),
            envelope,
            etag,
        },
        manifest: written.envelope,
    })
}

struct WrittenGrepManifest {
    manifest_object_id: GrepManifestObjectId,
    envelope: GrepManifestEnvelope,
}

/// Writes one candidate manifest under an identity nothing has used before.
///
/// The fresh id is what makes the write safe to leave alone until the
/// pointer moves: no earlier publication's object stands at this key, so
/// grep collection cannot be looking at it, and the grace window covers the
/// rest of the publication budget.
async fn write_grep_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    state: &GrepManifestState,
) -> Result<WrittenGrepManifest> {
    let manifest_object_id = GrepManifestObjectId::generate();
    let object_key = manifest_key(state.namespace_id(), &manifest_object_id);
    let (envelope, bytes) = encode_grep_manifest(state.clone())
        .map_err(|error| corrupt(&object_key, error))?
        .into_parts();
    let bytes = Bytes::from(bytes);
    // Create-only plus the byte check stay on this write even though a
    // random id cannot collide: an occupied key here is corruption, and it
    // must fail loudly rather than be overwritten.
    store
        .put_immutable_verified(&object_key, bytes)
        .await
        .map_err(immutable_write_error)?;
    Ok(WrittenGrepManifest {
        manifest_object_id,
        envelope,
    })
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

fn required_etag(object_key: &str, etag: Option<String>) -> Result<String> {
    etag.ok_or_else(|| GrepRootError::Store {
        object_key: object_key.to_owned(),
        message: "object store omitted the required grep-root etag".to_owned(),
        class: StoreFailureClass::Other,
    })
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
