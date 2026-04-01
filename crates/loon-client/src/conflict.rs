use crate::download::{download_file_to_bytes, DownloadError};
use crate::local_apply::{
    apply_bytes_atomically_with_hooks, create_directory_durably_with_hooks, remove_path_durably,
    remove_tree_durably, rename_path_durably_with_hooks, LocalApplyError,
};
use crate::state_db::{
    ConflictArtifactArchiveRow, ConflictArtifactEnvelopeRecord, ConflictArtifactRow, SqliteStateDb,
    StateDbError,
};
use crate::testing::{ClientExecutionHooks, NoopClientExecutionHooks};
use crate::upload::{upload_small_file_from_path, UploadError, UploadedContent};
use loon_server::objectstore::ObjectStoreError;
use loon_server::objectstore::keys::{
    conflict_artifact, conflict_artifact_archive, conflict_artifact_archive_prefix,
    conflict_artifact_prefix,
};
use loon_server::objectstore::ObjectStore;
use loon_types::{
    deterministic_conflict_id, deterministic_subtree_conflict_id, ChangeSeq,
    ConflictArtifactArchiveEnvelope, ConflictArtifactEnvelope, ConflictArtifactKind,
    ConflictArtifactLoserSummary, ConflictArtifactWinnerSummary, ConflictClass, ConflictPolicy,
    InodeKind, NamespaceId, SubtreeConflictArtifactEntry, SubtreeConflictArtifactEnvelope,
    SubtreeConflictArtifactRootSummary,
};
use serde_json::{Error as JsonError, Value};
use std::cmp::Ordering;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConflictArtifactError {
    #[error(transparent)]
    Upload(#[from] UploadError),
    #[error(transparent)]
    Download(#[from] DownloadError),
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    LocalApply(#[from] LocalApplyError),
    #[error("failed to encode conflict artifact `{conflict_id}`: {source}")]
    Encode {
        conflict_id: String,
        #[source]
        source: JsonError,
    },
    #[error("failed to encode conflict artifact archive `{conflict_id}`: {source}")]
    EncodeArchive {
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
    #[error("failed to decode conflict artifact object `{object_key}`: {source}")]
    DecodeObject {
        object_key: String,
        #[source]
        source: JsonError,
    },
    #[error("failed to decode conflict artifact archive object `{object_key}`: {source}")]
    DecodeArchiveObject {
        object_key: String,
        #[source]
        source: JsonError,
    },
    #[error("failed to write conflict artifact `{object_key}`: {source}")]
    StoreWrite {
        object_key: String,
        #[source]
        source: ObjectStoreError,
    },
    #[error("failed to delete conflict artifact `{object_key}`: {source}")]
    StoreDelete {
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
    #[error("failed to list conflict artifacts under `{prefix}`: {source}")]
    StoreList {
        prefix: String,
        #[source]
        source: ObjectStoreError,
    },
    #[error("conflict artifact `{conflict_id}` was not found at `{object_key}`")]
    ArtifactNotFound {
        conflict_id: String,
        object_key: String,
    },
    #[error(
        "conflict artifact `{conflict_id}` kind mismatch: expected `{expected}`, actual `{actual}`"
    )]
    ArtifactKindMismatch {
        conflict_id: String,
        expected: &'static str,
        actual: &'static str,
    },
    #[error("conflict artifact `{object_key}` failed validation: {detail}")]
    InvalidArtifact { object_key: String, detail: String },
    #[error("conflict artifact archive `{object_key}` failed validation: {detail}")]
    InvalidArchive { object_key: String, detail: String },
    #[error("restore destination already exists: `{path}`")]
    DestinationExists { path: String },
    #[error("restore destination parent does not exist: `{path}`")]
    DestinationParentMissing { path: String },
    #[error("restore destination parent is not a directory: `{path}`")]
    DestinationParentNotDirectory { path: String },
    #[error("restore destination must include a file or directory name: `{path}`")]
    DestinationPathMissingLeaf { path: String },
    #[error("existing conflict artifact `{object_key}` does not match expected bytes")]
    ExistingArtifactMismatch { object_key: String },
    #[error("existing conflict artifact archive `{object_key}` does not match expected bytes")]
    ExistingArchiveMismatch { object_key: String },
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoredFileConflictArtifact {
    pub namespace_id: NamespaceId,
    pub conflict_id: String,
    pub destination_path: PathBuf,
    pub file_digest_sha256: String,
    pub content_manifest_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoredSubtreeConflictArtifact {
    pub namespace_id: NamespaceId,
    pub conflict_id: String,
    pub destination_root: PathBuf,
    pub restored_entry_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoredConflictArtifact {
    File(RestoredFileConflictArtifact),
    Subtree(RestoredSubtreeConflictArtifact),
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
    let Some(row) = load_conflict_artifact_row_from_store(store, namespace_id, &object_key)? else {
        return Ok(None);
    };
    let actual = row.artifact_kind.as_str();
    let Some(envelope) = row.file_envelope().cloned() else {
        return Err(ConflictArtifactError::ArtifactKindMismatch {
            conflict_id: conflict_id.to_owned(),
            expected: ConflictArtifactKind::File.as_str(),
            actual,
        });
    };
    Ok(Some(envelope))
}

pub fn load_subtree_conflict_artifact<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    conflict_id: &str,
) -> Result<Option<SubtreeConflictArtifactEnvelope>, ConflictArtifactError> {
    let object_key = conflict_artifact(namespace_id.as_str(), conflict_id);
    let Some(row) = load_conflict_artifact_row_from_store(store, namespace_id, &object_key)? else {
        return Ok(None);
    };
    let actual = row.artifact_kind.as_str();
    let Some(envelope) = row.subtree_envelope().cloned() else {
        return Err(ConflictArtifactError::ArtifactKindMismatch {
            conflict_id: conflict_id.to_owned(),
            expected: ConflictArtifactKind::Subtree.as_str(),
            actual,
        });
    };
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

pub fn discover_conflict_artifacts_for_namespace<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<Vec<ConflictArtifactRow>, ConflictArtifactError> {
    discover_conflict_artifacts_for_namespace_with_hooks(
        db,
        store,
        namespace_id,
        &NoopClientExecutionHooks,
    )
}

pub(crate) fn discover_conflict_artifacts_for_namespace_with_hooks<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    hooks: &dyn ClientExecutionHooks,
) -> Result<Vec<ConflictArtifactRow>, ConflictArtifactError> {
    let prefix = conflict_artifact_prefix(namespace_id.as_str());
    let mut object_keys =
        store
            .list_prefix(&prefix)
            .map_err(|source| ConflictArtifactError::StoreList {
                prefix: prefix.clone(),
                source,
            })?;
    object_keys.sort();

    let mut rows = Vec::with_capacity(object_keys.len());
    for object_key in object_keys {
        let Some(row) = load_conflict_artifact_row_from_store(store, namespace_id, &object_key)?
        else {
            return Err(ConflictArtifactError::ArtifactNotFound {
                conflict_id: object_key.clone(),
                object_key,
            });
        };
        rows.push(row);
    }
    rows.sort_by(|left, right| {
        left.created_at_ms
            .cmp(&right.created_at_ms)
            .then_with(|| left.conflict_id.cmp(&right.conflict_id))
    });

    db.planner_transaction("cache_discovered_conflict_artifacts", |tx| {
        for row in &rows {
            tx.upsert_conflict_artifact(row)?;
        }
        Ok(())
    })?;
    hooks.checkpoint("conflict.discover_artifacts.after_cache");
    Ok(rows)
}

pub fn load_or_discover_conflict_artifact<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    conflict_id: &str,
) -> Result<Option<ConflictArtifactRow>, ConflictArtifactError> {
    if let Some(row) = db.load_conflict_artifact(namespace_id, conflict_id)? {
        return Ok(Some(row));
    }

    let object_key = conflict_artifact(namespace_id.as_str(), conflict_id);
    let Some(row) = load_conflict_artifact_row_from_store(store, namespace_id, &object_key)? else {
        return Ok(None);
    };
    db.planner_transaction("cache_discovered_conflict_artifact", |tx| {
        tx.upsert_conflict_artifact(&row)?;
        Ok(())
    })?;
    Ok(Some(row))
}

pub fn discover_conflict_artifact_archives_for_namespace<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<Vec<ConflictArtifactArchiveRow>, ConflictArtifactError> {
    discover_conflict_artifact_archives_for_namespace_with_hooks(
        db,
        store,
        namespace_id,
        &NoopClientExecutionHooks,
    )
}

pub(crate) fn discover_conflict_artifact_archives_for_namespace_with_hooks<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    hooks: &dyn ClientExecutionHooks,
) -> Result<Vec<ConflictArtifactArchiveRow>, ConflictArtifactError> {
    let prefix = conflict_artifact_archive_prefix(namespace_id.as_str());
    let mut object_keys =
        store
            .list_prefix(&prefix)
            .map_err(|source| ConflictArtifactError::StoreList {
                prefix: prefix.clone(),
                source,
            })?;
    object_keys.sort();

    let cached_artifacts = db.load_conflict_artifacts_for_namespace(namespace_id)?;
    let cached_conflict_ids: std::collections::BTreeSet<&str> = cached_artifacts
        .iter()
        .map(|row| row.conflict_id.as_str())
        .collect();

    let mut rows = Vec::with_capacity(object_keys.len());
    for object_key in object_keys {
        let Some(row) =
            load_conflict_artifact_archive_row_from_store(store, namespace_id, &object_key)?
        else {
            return Err(ConflictArtifactError::ArtifactNotFound {
                conflict_id: object_key.clone(),
                object_key,
            });
        };
        if !cached_conflict_ids.contains(row.conflict_id.as_str()) {
            return Err(ConflictArtifactError::InvalidArchive {
                object_key: row.object_key.clone(),
                detail: format!(
                    "archive sidecar references missing cached artifact `{}`",
                    row.conflict_id
                ),
            });
        }
        rows.push(row);
    }
    rows.sort_by(|left, right| {
        left.archived_at_ms
            .cmp(&right.archived_at_ms)
            .then_with(|| left.conflict_id.cmp(&right.conflict_id))
    });

    db.planner_transaction("cache_discovered_conflict_artifact_archives", |tx| {
        tx.delete_conflict_artifact_archives_for_namespace(namespace_id)?;
        for row in &rows {
            tx.upsert_conflict_artifact_archive(row)?;
        }
        Ok(())
    })?;
    hooks.checkpoint("conflict.discover_archives.after_cache");
    Ok(rows)
}

pub fn load_or_discover_conflict_artifact_archive<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    conflict_id: &str,
) -> Result<Option<ConflictArtifactArchiveRow>, ConflictArtifactError> {
    let Some(_artifact_row) =
        load_or_discover_conflict_artifact(db, store, namespace_id, conflict_id)?
    else {
        return Err(ConflictArtifactError::ArtifactNotFound {
            conflict_id: conflict_id.to_owned(),
            object_key: conflict_artifact(namespace_id.as_str(), conflict_id),
        });
    };

    if let Some(row) = db.load_conflict_artifact_archive(namespace_id, conflict_id)? {
        return Ok(Some(row));
    }

    let object_key = conflict_artifact_archive(namespace_id.as_str(), conflict_id);
    let Some(row) =
        load_conflict_artifact_archive_row_from_store(store, namespace_id, &object_key)?
    else {
        return Ok(None);
    };
    db.planner_transaction("cache_discovered_conflict_artifact_archive", |tx| {
        tx.upsert_conflict_artifact_archive(&row)?;
        Ok(())
    })?;
    Ok(Some(row))
}

pub fn archive_conflict_artifact<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    conflict_id: &str,
    archived_at_ms: u64,
) -> Result<ConflictArtifactArchiveRow, ConflictArtifactError> {
    archive_conflict_artifact_with_hooks(
        db,
        store,
        namespace_id,
        conflict_id,
        archived_at_ms,
        &NoopClientExecutionHooks,
    )
}

pub(crate) fn archive_conflict_artifact_with_hooks<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    conflict_id: &str,
    archived_at_ms: u64,
    hooks: &dyn ClientExecutionHooks,
) -> Result<ConflictArtifactArchiveRow, ConflictArtifactError> {
    let Some(_artifact_row) =
        load_or_discover_conflict_artifact(db, store, namespace_id, conflict_id)?
    else {
        return Err(ConflictArtifactError::ArtifactNotFound {
            conflict_id: conflict_id.to_owned(),
            object_key: conflict_artifact(namespace_id.as_str(), conflict_id),
        });
    };

    if let Some(row) = db.load_conflict_artifact_archive(namespace_id, conflict_id)? {
        return Ok(row);
    }

    let object_key = conflict_artifact_archive(namespace_id.as_str(), conflict_id);
    if let Some(row) =
        load_conflict_artifact_archive_row_from_store(store, namespace_id, &object_key)?
    {
        db.planner_transaction("cache_existing_conflict_artifact_archive", |tx| {
            tx.upsert_conflict_artifact_archive(&row)?;
            Ok(())
        })?;
        return Ok(row);
    }

    let envelope = ConflictArtifactArchiveEnvelope {
        namespace_id: namespace_id.clone(),
        conflict_id: conflict_id.to_owned(),
        archived_at_ms,
    };
    let bytes =
        serde_json::to_vec(&envelope).map_err(|source| ConflictArtifactError::EncodeArchive {
            conflict_id: conflict_id.to_owned(),
            source,
        })?;
    write_archive_bytes_if_absent(store, &object_key, &bytes)?;
    hooks.checkpoint("conflict.archive.after_store_write");

    let row = load_conflict_artifact_archive_row_from_store(store, namespace_id, &object_key)?
        .ok_or_else(|| ConflictArtifactError::ArtifactNotFound {
            conflict_id: conflict_id.to_owned(),
            object_key: object_key.clone(),
        })?;
    db.planner_transaction("cache_archived_conflict_artifact", |tx| {
        tx.upsert_conflict_artifact_archive(&row)?;
        Ok(())
    })?;
    hooks.checkpoint("conflict.archive.after_cache");
    Ok(row)
}

pub fn unarchive_conflict_artifact<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    conflict_id: &str,
) -> Result<(), ConflictArtifactError> {
    unarchive_conflict_artifact_with_hooks(
        db,
        store,
        namespace_id,
        conflict_id,
        &NoopClientExecutionHooks,
    )
}

pub(crate) fn unarchive_conflict_artifact_with_hooks<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    conflict_id: &str,
    hooks: &dyn ClientExecutionHooks,
) -> Result<(), ConflictArtifactError> {
    let Some(_artifact_row) =
        load_or_discover_conflict_artifact(db, store, namespace_id, conflict_id)?
    else {
        return Err(ConflictArtifactError::ArtifactNotFound {
            conflict_id: conflict_id.to_owned(),
            object_key: conflict_artifact(namespace_id.as_str(), conflict_id),
        });
    };

    let object_key = conflict_artifact_archive(namespace_id.as_str(), conflict_id);
    store
        .delete(&object_key)
        .map_err(|source| ConflictArtifactError::StoreDelete {
            object_key: object_key.clone(),
            source,
        })?;
    hooks.checkpoint("conflict.unarchive.after_store_delete");
    db.planner_transaction("uncache_conflict_artifact_archive", |tx| {
        tx.delete_conflict_artifact_archive(namespace_id, conflict_id)?;
        Ok(())
    })?;
    hooks.checkpoint("conflict.unarchive.after_cache");
    Ok(())
}

pub fn restore_conflict_artifact_to_path<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    conflict_id: &str,
    destination_path: &Path,
) -> Result<RestoredConflictArtifact, ConflictArtifactError> {
    restore_conflict_artifact_to_path_with_hooks(
        db,
        store,
        namespace_id,
        conflict_id,
        destination_path,
        &NoopClientExecutionHooks,
    )
}

pub(crate) fn restore_conflict_artifact_to_path_with_hooks<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    conflict_id: &str,
    destination_path: &Path,
    hooks: &dyn ClientExecutionHooks,
) -> Result<RestoredConflictArtifact, ConflictArtifactError> {
    let Some(row) = load_or_discover_conflict_artifact(db, store, namespace_id, conflict_id)?
    else {
        return Err(ConflictArtifactError::ArtifactNotFound {
            conflict_id: conflict_id.to_owned(),
            object_key: conflict_artifact(namespace_id.as_str(), conflict_id),
        });
    };

    match row.artifact_kind {
        ConflictArtifactKind::File => restore_file_conflict_artifact_row_to_path_with_hooks(
            store,
            namespace_id,
            &row,
            destination_path,
            hooks,
        )
        .map(RestoredConflictArtifact::File),
        ConflictArtifactKind::Subtree => restore_subtree_conflict_artifact_row_to_path_with_hooks(
            store,
            namespace_id,
            &row,
            destination_path,
            hooks,
        )
        .map(RestoredConflictArtifact::Subtree),
    }
}

pub fn restore_file_conflict_artifact_to_path<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    conflict_id: &str,
    destination_path: &Path,
) -> Result<RestoredFileConflictArtifact, ConflictArtifactError> {
    restore_file_conflict_artifact_to_path_with_hooks(
        db,
        store,
        namespace_id,
        conflict_id,
        destination_path,
        &NoopClientExecutionHooks,
    )
}

pub(crate) fn restore_file_conflict_artifact_to_path_with_hooks<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    conflict_id: &str,
    destination_path: &Path,
    hooks: &dyn ClientExecutionHooks,
) -> Result<RestoredFileConflictArtifact, ConflictArtifactError> {
    let Some(row) = load_or_discover_conflict_artifact(db, store, namespace_id, conflict_id)?
    else {
        return Err(ConflictArtifactError::ArtifactNotFound {
            conflict_id: conflict_id.to_owned(),
            object_key: conflict_artifact(namespace_id.as_str(), conflict_id),
        });
    };
    restore_file_conflict_artifact_row_to_path_with_hooks(
        store,
        namespace_id,
        &row,
        destination_path,
        hooks,
    )
}

pub fn restore_subtree_conflict_artifact_to_path<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    conflict_id: &str,
    destination_root: &Path,
) -> Result<RestoredSubtreeConflictArtifact, ConflictArtifactError> {
    restore_subtree_conflict_artifact_to_path_with_hooks(
        db,
        store,
        namespace_id,
        conflict_id,
        destination_root,
        &NoopClientExecutionHooks,
    )
}

pub(crate) fn restore_subtree_conflict_artifact_to_path_with_hooks<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    conflict_id: &str,
    destination_root: &Path,
    hooks: &dyn ClientExecutionHooks,
) -> Result<RestoredSubtreeConflictArtifact, ConflictArtifactError> {
    let Some(row) = load_or_discover_conflict_artifact(db, store, namespace_id, conflict_id)?
    else {
        return Err(ConflictArtifactError::ArtifactNotFound {
            conflict_id: conflict_id.to_owned(),
            object_key: conflict_artifact(namespace_id.as_str(), conflict_id),
        });
    };
    restore_subtree_conflict_artifact_row_to_path_with_hooks(
        store,
        namespace_id,
        &row,
        destination_root,
        hooks,
    )
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

fn write_archive_bytes_if_absent<S: ObjectStore>(
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
                Some(_) => Err(ConflictArtifactError::ExistingArchiveMismatch {
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

fn load_conflict_artifact_row_from_store<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    object_key: &str,
) -> Result<Option<ConflictArtifactRow>, ConflictArtifactError> {
    let Some(bytes) =
        store
            .get(object_key, None)
            .map_err(|source| ConflictArtifactError::StoreRead {
                object_key: object_key.to_owned(),
                source,
            })?
    else {
        return Ok(None);
    };
    decode_conflict_artifact_row(namespace_id, object_key, &bytes).map(Some)
}

fn load_conflict_artifact_archive_row_from_store<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    object_key: &str,
) -> Result<Option<ConflictArtifactArchiveRow>, ConflictArtifactError> {
    let Some(bytes) =
        store
            .get(object_key, None)
            .map_err(|source| ConflictArtifactError::StoreRead {
                object_key: object_key.to_owned(),
                source,
            })?
    else {
        return Ok(None);
    };
    decode_conflict_artifact_archive_row(namespace_id, object_key, &bytes).map(Some)
}

fn decode_conflict_artifact_row(
    namespace_id: &NamespaceId,
    object_key: &str,
    bytes: &[u8],
) -> Result<ConflictArtifactRow, ConflictArtifactError> {
    let json_value: Value =
        serde_json::from_slice(bytes).map_err(|source| ConflictArtifactError::DecodeObject {
            object_key: object_key.to_owned(),
            source,
        })?;

    let row = if json_value.get("loser").is_some()
        && json_value.get("entries").is_none()
        && json_value.get("loser_root").is_none()
    {
        let envelope: ConflictArtifactEnvelope =
            serde_json::from_value(json_value).map_err(|source| {
                ConflictArtifactError::DecodeObject {
                    object_key: object_key.to_owned(),
                    source,
                }
            })?;
        validate_file_conflict_artifact(namespace_id, object_key, &envelope)?;
        ConflictArtifactRow {
            namespace_id: namespace_id.clone(),
            conflict_id: envelope.conflict_id.clone(),
            object_key: object_key.to_owned(),
            artifact_kind: ConflictArtifactKind::File,
            conflict_class: envelope.conflict_class,
            created_at_ms: envelope.created_at_ms,
            envelope: ConflictArtifactEnvelopeRecord::File(envelope),
        }
    } else if json_value.get("entries").is_some()
        && json_value.get("loser_root").is_some()
        && json_value.get("loser").is_none()
    {
        let envelope: SubtreeConflictArtifactEnvelope = serde_json::from_value(json_value)
            .map_err(|source| ConflictArtifactError::DecodeObject {
                object_key: object_key.to_owned(),
                source,
            })?;
        validate_subtree_conflict_artifact(namespace_id, object_key, &envelope)?;
        ConflictArtifactRow {
            namespace_id: namespace_id.clone(),
            conflict_id: envelope.conflict_id.clone(),
            object_key: object_key.to_owned(),
            artifact_kind: ConflictArtifactKind::Subtree,
            conflict_class: envelope.conflict_class,
            created_at_ms: envelope.created_at_ms,
            envelope: ConflictArtifactEnvelopeRecord::Subtree(envelope),
        }
    } else {
        return Err(ConflictArtifactError::InvalidArtifact {
            object_key: object_key.to_owned(),
            detail: "artifact JSON must encode exactly one of file or subtree envelope shapes"
                .to_owned(),
        });
    };

    Ok(row)
}

fn decode_conflict_artifact_archive_row(
    namespace_id: &NamespaceId,
    object_key: &str,
    bytes: &[u8],
) -> Result<ConflictArtifactArchiveRow, ConflictArtifactError> {
    let envelope: ConflictArtifactArchiveEnvelope =
        serde_json::from_slice(bytes).map_err(|source| {
            ConflictArtifactError::DecodeArchiveObject {
                object_key: object_key.to_owned(),
                source,
            }
        })?;
    validate_conflict_artifact_archive(namespace_id, object_key, &envelope)?;
    Ok(ConflictArtifactArchiveRow {
        namespace_id: namespace_id.clone(),
        conflict_id: envelope.conflict_id,
        object_key: object_key.to_owned(),
        archived_at_ms: envelope.archived_at_ms,
    })
}

fn validate_file_conflict_artifact(
    namespace_id: &NamespaceId,
    object_key: &str,
    envelope: &ConflictArtifactEnvelope,
) -> Result<(), ConflictArtifactError> {
    validate_common_conflict_artifact(
        namespace_id,
        object_key,
        envelope.conflict_id.as_str(),
        envelope.namespace_id.clone(),
        envelope.conflict_class,
        ConflictArtifactKind::File,
        envelope.policy_applied,
    )?;
    if envelope.loser.content_manifest_digest.is_empty() {
        return Err(ConflictArtifactError::InvalidArtifact {
            object_key: object_key.to_owned(),
            detail: "file conflict artifact loser content_manifest_digest must be non-empty"
                .to_owned(),
        });
    }
    Ok(())
}

fn validate_subtree_conflict_artifact(
    namespace_id: &NamespaceId,
    object_key: &str,
    envelope: &SubtreeConflictArtifactEnvelope,
) -> Result<(), ConflictArtifactError> {
    validate_common_conflict_artifact(
        namespace_id,
        object_key,
        envelope.conflict_id.as_str(),
        envelope.namespace_id.clone(),
        envelope.conflict_class,
        ConflictArtifactKind::Subtree,
        envelope.policy_applied,
    )?;

    for entry in &envelope.entries {
        if entry.relative_path.is_empty() {
            return Err(ConflictArtifactError::InvalidArtifact {
                object_key: object_key.to_owned(),
                detail: "subtree conflict artifact entries must not include the root path"
                    .to_owned(),
            });
        }
        if matches!(entry.inode_kind, InodeKind::File) && entry.content_manifest_digest.is_none() {
            return Err(ConflictArtifactError::InvalidArtifact {
                object_key: object_key.to_owned(),
                detail: format!(
                    "subtree file entry `{}` must include content_manifest_digest",
                    entry.relative_path
                ),
            });
        }
    }

    if envelope
        .entries
        .windows(2)
        .any(|pair| compare_subtree_entries(&pair[0], &pair[1]) != Ordering::Less)
    {
        return Err(ConflictArtifactError::InvalidArtifact {
            object_key: object_key.to_owned(),
            detail: "subtree conflict artifact entries must be strictly ordered by durable relative path".to_owned(),
        });
    }

    Ok(())
}

fn validate_common_conflict_artifact(
    namespace_id: &NamespaceId,
    object_key: &str,
    conflict_id: &str,
    actual_namespace_id: NamespaceId,
    conflict_class: ConflictClass,
    artifact_kind: ConflictArtifactKind,
    policy_applied: ConflictPolicy,
) -> Result<(), ConflictArtifactError> {
    if actual_namespace_id != *namespace_id {
        return Err(ConflictArtifactError::InvalidArtifact {
            object_key: object_key.to_owned(),
            detail: format!(
                "artifact namespace mismatch: expected `{}`, actual `{}`",
                namespace_id.as_str(),
                actual_namespace_id.as_str()
            ),
        });
    }

    let expected_key = conflict_artifact(namespace_id.as_str(), conflict_id);
    if object_key != expected_key {
        return Err(ConflictArtifactError::InvalidArtifact {
            object_key: object_key.to_owned(),
            detail: format!("artifact key mismatch: expected `{expected_key}`"),
        });
    }

    if conflict_class.artifact_kind() != artifact_kind {
        return Err(ConflictArtifactError::InvalidArtifact {
            object_key: object_key.to_owned(),
            detail: format!(
                "artifact class `{}` does not match artifact kind `{}`",
                conflict_class.as_str(),
                artifact_kind.as_str()
            ),
        });
    }

    if policy_applied != ConflictPolicy::StablePaths {
        return Err(ConflictArtifactError::InvalidArtifact {
            object_key: object_key.to_owned(),
            detail: format!("unsupported conflict policy `{}`", policy_applied.as_str()),
        });
    }

    Ok(())
}

fn validate_conflict_artifact_archive(
    namespace_id: &NamespaceId,
    object_key: &str,
    envelope: &ConflictArtifactArchiveEnvelope,
) -> Result<(), ConflictArtifactError> {
    if envelope.namespace_id != *namespace_id {
        return Err(ConflictArtifactError::InvalidArchive {
            object_key: object_key.to_owned(),
            detail: format!(
                "archive namespace mismatch: expected `{}`, actual `{}`",
                namespace_id.as_str(),
                envelope.namespace_id.as_str()
            ),
        });
    }

    let expected_key = conflict_artifact_archive(namespace_id.as_str(), &envelope.conflict_id);
    if object_key != expected_key {
        return Err(ConflictArtifactError::InvalidArchive {
            object_key: object_key.to_owned(),
            detail: format!("archive key mismatch: expected `{expected_key}`"),
        });
    }

    Ok(())
}

fn restore_file_conflict_artifact_row_to_path_with_hooks<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    row: &ConflictArtifactRow,
    destination_path: &Path,
    hooks: &dyn ClientExecutionHooks,
) -> Result<RestoredFileConflictArtifact, ConflictArtifactError> {
    let actual = row.artifact_kind.as_str();
    let Some(envelope) = row.file_envelope() else {
        return Err(ConflictArtifactError::ArtifactKindMismatch {
            conflict_id: row.conflict_id.clone(),
            expected: ConflictArtifactKind::File.as_str(),
            actual,
        });
    };

    validate_restore_destination(destination_path)?;

    let downloaded =
        download_file_to_bytes(store, namespace_id, &envelope.loser.content_manifest_digest)?;
    if let Some(expected_digest) = envelope.loser.content_digest.as_deref() {
        if downloaded.file_digest_sha256 != expected_digest {
            return Err(ConflictArtifactError::InvalidArtifact {
                object_key: row.object_key.clone(),
                detail: format!(
                    "restored loser content digest mismatch: expected `{expected_digest}`, actual `{}`",
                    downloaded.file_digest_sha256
                ),
            });
        }
    }
    apply_bytes_atomically_with_hooks(destination_path, &downloaded.bytes, hooks)?;
    hooks.checkpoint("conflict.restore_file.after_local_apply");

    Ok(RestoredFileConflictArtifact {
        namespace_id: namespace_id.clone(),
        conflict_id: row.conflict_id.clone(),
        destination_path: destination_path.to_path_buf(),
        file_digest_sha256: downloaded.file_digest_sha256,
        content_manifest_digest: downloaded.content_manifest_digest,
    })
}

fn restore_subtree_conflict_artifact_row_to_path_with_hooks<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    row: &ConflictArtifactRow,
    destination_root: &Path,
    hooks: &dyn ClientExecutionHooks,
) -> Result<RestoredSubtreeConflictArtifact, ConflictArtifactError> {
    let actual = row.artifact_kind.as_str();
    let Some(envelope) = row.subtree_envelope() else {
        return Err(ConflictArtifactError::ArtifactKindMismatch {
            conflict_id: row.conflict_id.clone(),
            expected: ConflictArtifactKind::Subtree.as_str(),
            actual,
        });
    };

    validate_restore_destination(destination_root)?;
    let stage_root = restore_stage_path(destination_root, row.conflict_id.as_str())?;
    cleanup_restore_stage_path(&stage_root)?;

    let restore_result = (|| {
        create_directory_durably_with_hooks(&stage_root, hooks)?;
        for entry in &envelope.entries {
            let entry_path = stage_root.join(&entry.relative_path);
            let parent_dir =
                entry_path
                    .parent()
                    .ok_or_else(|| ConflictArtifactError::InvalidArtifact {
                        object_key: row.object_key.clone(),
                        detail: format!(
                        "subtree entry `{}` must have a parent directory inside the staged tree",
                        entry.relative_path
                    ),
                    })?;
            if !parent_dir.exists() {
                return Err(ConflictArtifactError::InvalidArtifact {
                    object_key: row.object_key.clone(),
                    detail: format!(
                        "subtree entry `{}` depends on a missing parent directory artifact entry",
                        entry.relative_path
                    ),
                });
            }

            match &entry.inode_kind {
                InodeKind::Dir => create_directory_durably_with_hooks(&entry_path, hooks)?,
                InodeKind::File => {
                    let manifest_digest =
                        entry.content_manifest_digest.as_deref().ok_or_else(|| {
                            ConflictArtifactError::InvalidArtifact {
                                object_key: row.object_key.clone(),
                                detail: format!(
                                    "subtree file entry `{}` is missing content_manifest_digest",
                                    entry.relative_path
                                ),
                            }
                        })?;
                    let downloaded = download_file_to_bytes(store, namespace_id, manifest_digest)?;
                    if let Some(expected_digest) = entry.content_digest.as_deref() {
                        if downloaded.file_digest_sha256 != expected_digest {
                            return Err(ConflictArtifactError::InvalidArtifact {
                                object_key: row.object_key.clone(),
                                detail: format!(
                                    "subtree file entry `{}` digest mismatch: expected `{expected_digest}`, actual `{}`",
                                    entry.relative_path,
                                    downloaded.file_digest_sha256
                                ),
                            });
                        }
                    }
                    apply_bytes_atomically_with_hooks(&entry_path, &downloaded.bytes, hooks)?;
                }
                kind => {
                    return Err(ConflictArtifactError::InvalidArtifact {
                        object_key: row.object_key.clone(),
                        detail: format!(
                            "subtree entry `{}` has unsupported inode kind `{}` for restore",
                            entry.relative_path,
                            inode_kind_label(kind.clone())
                        ),
                    });
                }
            }
        }

        hooks.checkpoint("conflict.restore_subtree.after_stage_ready");
        rename_path_durably_with_hooks(&stage_root, destination_root, hooks)?;
        hooks.checkpoint("conflict.restore_subtree.after_local_apply");
        Ok(())
    })();

    if restore_result.is_err() {
        let _ = cleanup_restore_stage_path(&stage_root);
    }
    restore_result?;

    Ok(RestoredSubtreeConflictArtifact {
        namespace_id: namespace_id.clone(),
        conflict_id: row.conflict_id.clone(),
        destination_root: destination_root.to_path_buf(),
        restored_entry_count: envelope.entries.len(),
    })
}

fn validate_restore_destination(destination_path: &Path) -> Result<(), ConflictArtifactError> {
    if destination_path.exists() {
        return Err(ConflictArtifactError::DestinationExists {
            path: destination_path.display().to_string(),
        });
    }

    let parent_dir = destination_parent_dir(destination_path)?;
    if !parent_dir.exists() {
        return Err(ConflictArtifactError::DestinationParentMissing {
            path: parent_dir.display().to_string(),
        });
    }
    if !parent_dir.is_dir() {
        return Err(ConflictArtifactError::DestinationParentNotDirectory {
            path: parent_dir.display().to_string(),
        });
    }
    Ok(())
}

fn restore_stage_path(
    destination_root: &Path,
    conflict_id: &str,
) -> Result<PathBuf, ConflictArtifactError> {
    let parent_dir = destination_parent_dir(destination_root)?;
    let file_name = destination_root.file_name().ok_or_else(|| {
        ConflictArtifactError::DestinationPathMissingLeaf {
            path: destination_root.display().to_string(),
        }
    })?;
    Ok(parent_dir.join(format!(
        ".{}.loon-restore-{}",
        file_name.to_string_lossy(),
        conflict_id
    )))
}

fn cleanup_restore_stage_path(stage_path: &Path) -> Result<(), ConflictArtifactError> {
    let metadata = match fs::metadata(stage_path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(ConflictArtifactError::LocalApply(LocalApplyError {
                operation: "stat_restore_stage",
                path: stage_path.display().to_string(),
                source,
            }));
        }
    };

    if metadata.is_dir() {
        remove_tree_durably(stage_path)?;
    } else {
        remove_path_durably(stage_path)?;
    }
    Ok(())
}

fn destination_parent_dir(destination_path: &Path) -> Result<&Path, ConflictArtifactError> {
    destination_path
        .parent()
        .ok_or_else(|| ConflictArtifactError::DestinationPathMissingLeaf {
            path: destination_path.display().to_string(),
        })
}

fn compare_subtree_entries(
    left: &SubtreeConflictArtifactEntry,
    right: &SubtreeConflictArtifactEntry,
) -> Ordering {
    left.relative_path
        .cmp(&right.relative_path)
        .then_with(|| left.client_file_id.cmp(&right.client_file_id))
        .then_with(|| left.inode_id.cmp(&right.inode_id))
}

fn inode_kind_label(inode_kind: InodeKind) -> &'static str {
    match inode_kind {
        InodeKind::File => "file",
        InodeKind::Dir => "dir",
        InodeKind::Symlink => "symlink",
        InodeKind::Mount => "mount",
    }
}
