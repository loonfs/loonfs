//! Loading and single-attempt CAS publication for grep roots.
//!
//! Publication intentionally does not retry: a compare-and-swap loser gets
//! [`GrepRootError::Conflict`](super::GrepRootError), reloads the now-current
//! root, and rebuilds its candidate. Any immutable segments written for the
//! losing candidate remain unreachable garbage for grep-owned collection.

use super::codec::{decode_grep_root, encode_grep_root, GrepRootEnvelope};
use super::error::{GrepRootError, Result};
use super::state::GrepRootState;
use crate::keyspace::root_key;
use bytes::Bytes;
use loonfs_api::NamespaceId;
use loonfs_objectstore::{ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};

/// A verified grep root and the metadata from the same object-store read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedGrepRoot {
    object_key: String,
    envelope: GrepRootEnvelope,
    metadata: ObjectMetadata,
}

impl LoadedGrepRoot {
    pub fn object_key(&self) -> &str {
        &self.object_key
    }

    pub fn envelope(&self) -> &GrepRootEnvelope {
        &self.envelope
    }

    pub fn state(&self) -> &GrepRootState {
        self.envelope.state()
    }

    pub fn metadata(&self) -> &ObjectMetadata {
        &self.metadata
    }
}

/// Loads and verifies one namespace's grep root, returning `None` when the
/// independent grep subsystem has not seeded it.
pub async fn load_grep_root<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<Option<LoadedGrepRoot>> {
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
    if envelope.state().namespace_id() != namespace_id {
        return Err(GrepRootError::IdentityMismatch {
            object_key,
            expected: namespace_id.clone(),
            actual: envelope.state().namespace_id().clone(),
        });
    }
    Ok(Some(LoadedGrepRoot {
        object_key,
        envelope,
        metadata: body.metadata,
    }))
}

/// Seeds a grep root with create-if-absent semantics.
///
/// An existing root is a typed conflict, even when it carries identical
/// bytes; callers seed exactly once and load thereafter.
pub async fn seed_grep_root<S: ObjectStore + ?Sized>(
    store: &S,
    state: &GrepRootState,
    writer_version: &str,
) -> Result<LoadedGrepRoot> {
    let object_key = root_key(state.namespace_id());
    let envelope =
        GrepRootEnvelope::from_state(writer_version, state.clone()).map_err(|error| {
            GrepRootError::Corrupt {
                object_key: object_key.clone(),
                message: error.to_string(),
            }
        })?;
    let bytes = encode_grep_root(&envelope).map_err(|error| GrepRootError::Corrupt {
        object_key: object_key.clone(),
        message: error.to_string(),
    })?;
    let metadata = match store
        .put(&object_key, Bytes::from(bytes), PutMode::CreateIfAbsent)
        .await
    {
        Ok(metadata) => metadata,
        Err(ObjectStoreError::PreconditionFailed { .. } | ObjectStoreError::Conflict { .. }) => {
            return Err(GrepRootError::Conflict { object_key });
        }
        Err(error) => return Err(store_error(&object_key, &error)),
    };
    Ok(LoadedGrepRoot {
        object_key,
        envelope,
        metadata,
    })
}

/// Advances a loaded grep root in one etag compare-and-swap attempt.
///
/// A conflict is never retried against the stale candidate: the caller must
/// reload and recompute the whole atomic root state.
pub async fn advance_grep_root<S: ObjectStore + ?Sized>(
    store: &S,
    current: &LoadedGrepRoot,
    next: &GrepRootState,
    writer_version: &str,
) -> Result<LoadedGrepRoot> {
    let expected_namespace_id = current.envelope.state().namespace_id();
    if next.namespace_id() != expected_namespace_id {
        return Err(GrepRootError::AdvanceIdentityMismatch {
            expected: expected_namespace_id.clone(),
            actual: next.namespace_id().clone(),
        });
    }
    let expected_key = root_key(expected_namespace_id);
    if current.object_key != expected_key {
        return Err(GrepRootError::Corrupt {
            object_key: current.object_key.clone(),
            message: format!("loaded root key does not match `{expected_key}`"),
        });
    }
    let expected_etag =
        current
            .metadata
            .etag
            .as_deref()
            .ok_or_else(|| GrepRootError::MissingEtag {
                object_key: current.object_key.clone(),
            })?;
    let envelope = GrepRootEnvelope::from_state(writer_version, next.clone()).map_err(|error| {
        GrepRootError::Corrupt {
            object_key: current.object_key.clone(),
            message: error.to_string(),
        }
    })?;
    let bytes = encode_grep_root(&envelope).map_err(|error| GrepRootError::Corrupt {
        object_key: current.object_key.clone(),
        message: error.to_string(),
    })?;
    let metadata = match store
        .put(
            &current.object_key,
            Bytes::from(bytes),
            PutMode::CompareAndSwap {
                expected_etag: expected_etag.to_owned(),
            },
        )
        .await
    {
        Ok(metadata) => metadata,
        Err(ObjectStoreError::PreconditionFailed { .. } | ObjectStoreError::Conflict { .. }) => {
            return Err(GrepRootError::Conflict {
                object_key: current.object_key.clone(),
            });
        }
        Err(error) => return Err(store_error(&current.object_key, &error)),
    };
    Ok(LoadedGrepRoot {
        object_key: current.object_key.clone(),
        envelope,
        metadata,
    })
}

fn store_error(object_key: &str, error: &ObjectStoreError) -> GrepRootError {
    GrepRootError::Store {
        object_key: object_key.to_owned(),
        message: error.message(),
    }
}
