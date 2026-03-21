use crate::upload::{upload_small_file_from_path, UploadError, UploadedContent};
use loon_objectstore::error::ObjectStoreError;
use loon_objectstore::keys::conflict_artifact;
use loon_objectstore::ObjectStore;
use loon_types::{
    deterministic_conflict_id, deterministic_subtree_conflict_id, ChangeSeq,
    ConflictArtifactEnvelope, ConflictArtifactLoserSummary, ConflictArtifactWinnerSummary,
    ConflictClass, ConflictPolicy, NamespaceId, SubtreeConflictArtifactEntry,
    SubtreeConflictArtifactEnvelope, SubtreeConflictArtifactRootSummary,
};
use serde_json::Error as JsonError;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConflictArtifactError {
    #[error(transparent)]
    Upload(#[from] UploadError),
    #[error("failed to encode conflict artifact `{conflict_id}`: {source}")]
    Encode {
        conflict_id: String,
        #[source]
        source: JsonError,
    },
    #[error("failed to decode conflict artifact `{conflict_id}`: {source}")]
    Decode {
        conflict_id: String,
        #[source]
        source: JsonError,
    },
    #[error("failed to write conflict artifact `{object_key}`: {source}")]
    StoreWrite {
        object_key: String,
        #[source]
        source: ObjectStoreError,
    },
    #[error("failed to read conflict artifact `{object_key}`: {source}")]
    StoreRead {
        object_key: String,
        #[source]
        source: ObjectStoreError,
    },
    #[error("existing conflict artifact `{object_key}` does not match expected bytes")]
    ExistingArtifactMismatch { object_key: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedConflictArtifact {
    pub object_key: String,
    pub envelope: ConflictArtifactEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedSubtreeConflictArtifact {
    pub object_key: String,
    pub envelope: SubtreeConflictArtifactEnvelope,
}

pub fn upload_loser_content_from_path<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    source_path: &Path,
) -> Result<UploadedContent, ConflictArtifactError> {
    upload_small_file_from_path(store, namespace_id, source_path)
        .map_err(ConflictArtifactError::from)
}

pub fn prepare_conflict_artifact(
    namespace_id: &NamespaceId,
    conflict_class: ConflictClass,
    detected_seq: ChangeSeq,
    winner: ConflictArtifactWinnerSummary,
    loser: ConflictArtifactLoserSummary,
    created_at_ms: u64,
) -> PreparedConflictArtifact {
    let conflict_id =
        deterministic_conflict_id(namespace_id, conflict_class, &winner, &loser, detected_seq);
    PreparedConflictArtifact {
        object_key: conflict_artifact(namespace_id.as_str(), &conflict_id),
        envelope: ConflictArtifactEnvelope {
            conflict_id,
            namespace_id: namespace_id.clone(),
            conflict_class,
            policy_applied: ConflictPolicy::StablePaths,
            detected_seq,
            winner,
            loser,
            created_at_ms,
        },
    }
}

pub fn prepare_subtree_conflict_artifact(
    namespace_id: &NamespaceId,
    conflict_class: ConflictClass,
    detected_seq: ChangeSeq,
    winner: ConflictArtifactWinnerSummary,
    loser_root: SubtreeConflictArtifactRootSummary,
    entries: Vec<SubtreeConflictArtifactEntry>,
    created_at_ms: u64,
) -> PreparedSubtreeConflictArtifact {
    let conflict_id = deterministic_subtree_conflict_id(
        namespace_id,
        conflict_class,
        &winner,
        &loser_root,
        &entries,
        detected_seq,
    );
    PreparedSubtreeConflictArtifact {
        object_key: conflict_artifact(namespace_id.as_str(), &conflict_id),
        envelope: SubtreeConflictArtifactEnvelope {
            conflict_id,
            namespace_id: namespace_id.clone(),
            conflict_class,
            policy_applied: ConflictPolicy::StablePaths,
            detected_seq,
            winner,
            loser_root,
            entries,
            created_at_ms,
        },
    }
}

pub fn load_conflict_artifact<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    conflict_id: &str,
) -> Result<Option<ConflictArtifactEnvelope>, ConflictArtifactError> {
    let object_key = conflict_artifact(namespace_id.as_str(), conflict_id);
    let Some(bytes) =
        store
            .get(&object_key, None)
            .map_err(|source| ConflictArtifactError::StoreRead {
                object_key: object_key.clone(),
                source,
            })?
    else {
        return Ok(None);
    };
    let envelope =
        serde_json::from_slice(&bytes).map_err(|source| ConflictArtifactError::Decode {
            conflict_id: conflict_id.to_owned(),
            source,
        })?;
    Ok(Some(envelope))
}

pub fn load_subtree_conflict_artifact<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    conflict_id: &str,
) -> Result<Option<SubtreeConflictArtifactEnvelope>, ConflictArtifactError> {
    let object_key = conflict_artifact(namespace_id.as_str(), conflict_id);
    let Some(bytes) =
        store
            .get(&object_key, None)
            .map_err(|source| ConflictArtifactError::StoreRead {
                object_key: object_key.clone(),
                source,
            })?
    else {
        return Ok(None);
    };
    let envelope =
        serde_json::from_slice(&bytes).map_err(|source| ConflictArtifactError::Decode {
            conflict_id: conflict_id.to_owned(),
            source,
        })?;
    Ok(Some(envelope))
}

pub fn write_conflict_artifact_if_absent<S: ObjectStore>(
    store: &S,
    artifact: &PreparedConflictArtifact,
) -> Result<(), ConflictArtifactError> {
    let bytes =
        serde_json::to_vec(&artifact.envelope).map_err(|source| ConflictArtifactError::Encode {
            conflict_id: artifact.envelope.conflict_id.clone(),
            source,
        })?;
    write_artifact_bytes_if_absent(store, &artifact.object_key, &bytes)
}

pub fn write_subtree_conflict_artifact_if_absent<S: ObjectStore>(
    store: &S,
    artifact: &PreparedSubtreeConflictArtifact,
) -> Result<(), ConflictArtifactError> {
    let bytes =
        serde_json::to_vec(&artifact.envelope).map_err(|source| ConflictArtifactError::Encode {
            conflict_id: artifact.envelope.conflict_id.clone(),
            source,
        })?;
    write_artifact_bytes_if_absent(store, &artifact.object_key, &bytes)
}

fn write_artifact_bytes_if_absent<S: ObjectStore>(
    store: &S,
    object_key: &str,
    bytes: &[u8],
) -> Result<(), ConflictArtifactError> {
    match store.put_if_absent(object_key, bytes) {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::PreconditionFailed) => {
            let existing =
                store
                    .get(object_key, None)
                    .map_err(|source| ConflictArtifactError::StoreRead {
                        object_key: object_key.to_owned(),
                        source,
                    })?;
            match existing {
                Some(existing) if existing == bytes => Ok(()),
                Some(_) => Err(ConflictArtifactError::ExistingArtifactMismatch {
                    object_key: object_key.to_owned(),
                }),
                None => Err(ConflictArtifactError::StoreRead {
                    object_key: object_key.to_owned(),
                    source: ObjectStoreError::NotFound,
                }),
            }
        }
        Err(source) => Err(ConflictArtifactError::StoreWrite {
            object_key: object_key.to_owned(),
            source,
        }),
    }
}
