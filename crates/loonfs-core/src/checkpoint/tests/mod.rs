//! Checkpoint lifecycle, manifest, cache, index, recovery, and retention tests.

#![allow(clippy::panic)]
// These tests use panic in impossible match arms to preserve precise failure messages.

mod active_deletions;
mod attributes;
mod cache;
mod cas_recovery;
mod index_parity;
pub(crate) mod inspection_materialization;
mod inventory;
mod manifest_round_trips;
mod retention;

use super::build::{
    build_manifest_tables, build_manifest_tables_from_rows, MetadataTableSegmentation,
};
use super::cache::{MetadataTableBlockKind, MetadataTableCache, MetadataTableCacheConfig};
use super::create::load_checkpoint_projection_metadata_state;
use super::error::ManifestLoadError;
use super::load::load_verified_manifest_tables_with_cache;
use super::load::{
    head_from_manifest, load_manifest_materialization_for_inspection,
    load_manifest_metadata_state_for_inspection_from_manifest, load_verified_manifest_tables,
};
use super::publish::{publish_metadata_root, write_namespace_manifest, ManifestPublicationOutcome};
use super::record::read_checkpoint_record;
use super::retention::advance_retention_floor;
use super::row::{manifest_rows_for_family, metadata_states_equivalent};
use super::runs::{
    flatten_manifest_tables, runs_from_metadata_files, runs_in_scan_order, MetadataLsmPolicy,
    MetadataRunManifest, CHECKPOINT_BASE_RUN_LEVEL, CHECKPOINT_L0_RUN_LEVEL,
    CHECKPOINT_TABLE_FAMILIES, DEFAULT_MAX_CHECKPOINT_L0_RUNS, REORGANIZE_FAMILY_GROUPS,
};
use super::stored_block_cache::{
    StoredMetadataBlockCache, StoredMetadataBlockKey, StoredMetadataBlockKind,
};
use super::{
    block_fetch, create, data_block_load, flush, load, record, reorganize,
    reorganize_metadata_step, row, scan, MetadataReorganizeOutcome,
};
use crate::error::{CoreError, ErrorCode, MetadataProjectionLoadError};
use crate::metadata::MetadataState;
use crate::namespace::catalog::load_namespace_catalog_entry;
use crate::namespace::control::{
    read_head_object, read_metadata_root_object, read_wal_floor_object,
};
use crate::namespace::status::load_namespace_head_summary;
use crate::namespace::writer_epoch::acquire_writer_epoch;
use crate::path::write::ops::{
    delete_path, move_path, put_file_bytes, restore_file_revision, write_file_bytes,
};
use crate::protocol::list_changes_after;
use crate::publish::{
    CommitCandidate, CommitRequest, FilesystemOperation, NamespaceCommitEngine, PublishTailOptions,
};
use crate::storage::content::{prepare_stored_content, store_bytes_as_content};
use crate::test_support::{RecordedStoredMetadataBlockCall, RecordingStoredMetadataBlockCache};
use crate::MutationContext;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_api::wire::control::{HeadState, MetadataRootState};
use loonfs_api::wire::manifest::{
    decode_namespace_manifest_json, encode_namespace_manifest_json, lookup_keys, MetadataFileRef,
    MetadataReorganizeProgress, MetadataRow, MetadataRunId,
    MetadataTableFamily as ApiMetadataTableFamily, NamespaceManifestEnvelope,
    NamespaceManifestPayload,
};
use loonfs_api::wire::sst_blocks::{
    decode_data_block, string_prefix_upper_bound, BlockHandle, DecodedDataBlock,
    SegmentBlocksBuilder, SegmentIndexEntry, DEFAULT_TARGET_BLOCK_BYTES,
};
use loonfs_api::{
    AbsolutePath, ChangeSeq, CheckpointId, CommitId, DestinationBehavior, EffectiveLimit, InodeId,
    ManifestId, ManifestObjectId, NameKey, NamespaceId, RevisionNo,
};
use loonfs_objectstore::keys::{
    metadata_manifest_object, metadata_manifest_prefix, metadata_table, wal_head, wal_segment,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use loonfs_test_support::stores::{
    CountingStore, FailStore, InjectedError, KeyPredicate, OperationClass,
};
use std::collections::BTreeSet;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

/// Every lifecycle test in this file pins as one user owner; owner-specific
/// behavior (fork owners, distinct-owner records) is exercised explicitly
/// where it matters.
pub(crate) async fn create_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> crate::error::Result<loonfs_api::CreateCheckpointResponse> {
    super::create::create_checkpoint(
        store,
        namespace_id,
        loonfs_api::wire::control::CheckpointOwner::User {
            name: "test-pin".to_owned(),
        },
        None,
        context,
    )
    .await
}

pub(crate) fn mutation_context(writer_id: &str, now_ms: u64) -> MutationContext {
    MutationContext {
        writer_id: writer_id.to_owned(),
        now_ms,
    }
}

pub(crate) async fn write_test_file<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    path: &str,
    commit_id: &str,
    context: &MutationContext,
) {
    let stored = store_bytes_as_content(store, namespace_id, b"body\n")
        .await
        .expect("store content");
    let content_ref = stored.content_ref.clone();
    let catalog = load_namespace_catalog_entry(store, namespace_id)
        .await
        .expect("load namespace catalog");
    let prepared = prepare_stored_content(&catalog, stored).expect("prepare stored content");
    NamespaceCommitEngine::new(namespace_id.clone())
        .publish_batch(
            store,
            vec![CommitCandidate::prepared(
                CommitRequest::single(
                    CommitId::parse(commit_id).expect("commit id"),
                    None,
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse(path).expect("path"),
                        content_ref,
                        behavior: DestinationBehavior::NoReplace,
                        expected_revision_no: None,
                    },
                ),
                vec![prepared],
            )],
            context,
            &PublishTailOptions::default(),
        )
        .await
        .results
        .pop()
        .expect("one result")
        .expect("write file");
}

#[derive(Debug)]
struct CurrentProjection {
    head: HeadState,
    root: MetadataRootState,
    metadata_state: MetadataState,
}

/// Creates a namespace and publishes its first manifest.
///
/// Creation itself writes only the head; these tests are about manifest,
/// root, and floor mechanics, so they start from a namespace that has
/// flushed once — the durable shape the tests were written against.
async fn bootstrap_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    allow_existing: bool,
) -> Result<loonfs_api::NamespaceSummary, crate::namespace::BootstrapNamespaceError> {
    let summary = crate::namespace::bootstrap::bootstrap_namespace(
        store,
        namespace_id,
        context,
        allow_existing,
    )
    .await?;
    flush::flush_wal(store, namespace_id, context)
        .await
        .expect("publish the first manifest");
    Ok(summary)
}

/// The namespace's effective retention floor: the floor object when it
/// exists, and the namespace's birth sequence until the first advance
/// publishes one.
async fn read_floor_seq<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> ChangeSeq {
    let head = read_head_object(store, namespace_id)
        .await
        .expect("read head")
        .envelope
        .state;
    crate::namespace::basis::resolve_retention_floor_seq(store, &head)
        .await
        .expect("resolve retention floor")
}

/// Checkpoints, then folds every L0 run into the base through
/// reorganization units, returning the resulting current manifest id. The
/// old synchronous rebuild produced this shape in one checkpoint call;
/// tests that need a compacted base with a specific segmentation policy use
/// this instead.
async fn checkpoint_then_reorganize<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
) -> ManifestId {
    create_checkpoint(store, namespace_id, context)
        .await
        .expect("create checkpoint");
    drain_reorganization(store, namespace_id, context, policy).await
}

/// Runs reorganization units until nothing is left to fold, with the
/// trigger forced so even one L0 run folds.
async fn drain_reorganization<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
) -> ManifestId {
    let fold_policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        ..policy
    };
    loop {
        let report = super::reorganize_metadata_step(store, namespace_id, context, fold_policy)
            .await
            .expect("reorganization step");
        match report.outcome {
            super::MetadataReorganizeOutcome::UnitPublished { .. }
            | super::MetadataReorganizeOutcome::Superseded => continue,
            super::MetadataReorganizeOutcome::NotNeeded { .. } => break,
            super::MetadataReorganizeOutcome::BudgetExhausted { .. } => {
                panic!("test reorganization budget should admit a progress-making subset")
            }
        }
    }
    read_metadata_root_object(store, namespace_id)
        .await
        .expect("read metadata root")
        .envelope
        .state
        .manifest_id
}

async fn current_manifest_object_id<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> ManifestObjectId {
    read_metadata_root_object(store, namespace_id)
        .await
        .expect("read metadata root")
        .envelope
        .state
        .manifest_object_id
}

async fn current_manifest_key<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> String {
    metadata_manifest_object(
        namespace_id.as_str(),
        &current_manifest_object_id(store, namespace_id).await,
    )
}

fn manifest_object_id(manifest_id: ManifestId) -> ManifestObjectId {
    ManifestObjectId::parse(format!("{:020}-0123456789abcdef", manifest_id.0))
        .expect("valid manifest object id")
}

async fn load_current_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<CurrentProjection, CoreError> {
    let (head, metadata_state) =
        load_checkpoint_projection_metadata_state(store, namespace_id).await?;
    let root = read_metadata_root_object(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?
        .envelope
        .state;
    Ok(CurrentProjection {
        head,
        root,
        metadata_state,
    })
}

fn base_run(manifest: &NamespaceManifestEnvelope) -> MetadataRunManifest {
    runs_from_metadata_files(&manifest.payload)
        .into_iter()
        .find(|run| run.level == CHECKPOINT_BASE_RUN_LEVEL)
        .expect("base run")
}

fn l0_runs(manifest: &NamespaceManifestEnvelope) -> Vec<MetadataRunManifest> {
    runs_from_metadata_files(&manifest.payload)
        .into_iter()
        .filter(|run| run.level == CHECKPOINT_L0_RUN_LEVEL)
        .collect()
}

fn base_segment_object_keys_for_family(
    manifest: &NamespaceManifestEnvelope,
    family: ApiMetadataTableFamily,
) -> Vec<String> {
    base_run(manifest)
        .tables
        .iter()
        .find(|table| table.family == family)
        .expect("table")
        .segments
        .iter()
        .map(|descriptor| descriptor.object_key.clone())
        .collect()
}

fn test_context() -> MutationContext {
    mutation_context("test-writer", 1_000)
}

fn manifest_id(seq: ChangeSeq) -> ManifestId {
    ManifestId(seq.0)
}

async fn write_file_and_checkpoint(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    index: u64,
) -> ChangeSeq {
    let path = format!("/docs/file-{index}.txt");
    let bytes = format!("file {index}\n");
    write_file_bytes(store, namespace_id, &path, bytes.as_bytes(), context, None)
        .await
        .expect("write file");
    create_checkpoint(store, namespace_id, context)
        .await
        .expect("create checkpoint")
        .checkpoint_seq
}

#[derive(Debug)]
enum ManifestConflictReplacement {
    Fixed(Vec<u8>),
    MutateCandidateNextInode,
}

#[derive(Debug)]
struct ConflictOnManifestCreateStore {
    inner: LocalFsStore,
    manifest_key: String,
    replacement: ManifestConflictReplacement,
    injected: Mutex<bool>,
}

impl ConflictOnManifestCreateStore {
    fn new(inner: LocalFsStore, manifest_key: String, replacement_bytes: Vec<u8>) -> Self {
        Self {
            inner,
            manifest_key,
            replacement: ManifestConflictReplacement::Fixed(replacement_bytes),
            injected: Mutex::new(false),
        }
    }

    fn mutate_next_inode(inner: LocalFsStore, manifest_key: String) -> Self {
        Self {
            inner,
            manifest_key,
            replacement: ManifestConflictReplacement::MutateCandidateNextInode,
            injected: Mutex::new(false),
        }
    }
}

#[async_trait]
impl ObjectStore for ConflictOnManifestCreateStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let manifest_matches = key == self.manifest_key
            || ((self.manifest_key.ends_with('/') || self.manifest_key.ends_with('-'))
                && key.starts_with(&self.manifest_key));
        if manifest_matches && matches!(&mode, PutMode::CreateIfAbsent) {
            let should_inject = {
                let mut injected = self
                    .injected
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let should_inject = !*injected;
                if should_inject {
                    *injected = true;
                }
                should_inject
            };
            if should_inject {
                let replacement_bytes = match &self.replacement {
                    ManifestConflictReplacement::Fixed(bytes) => Bytes::copy_from_slice(bytes),
                    ManifestConflictReplacement::MutateCandidateNextInode => {
                        let candidate = decode_namespace_manifest_json(&bytes)
                            .map_err(|error| ObjectStoreError::transport(key, error.to_string()))?;
                        let mut payload = candidate.payload;
                        payload.next_inode_id = InodeId(payload.next_inode_id.0 + 1);
                        let mutated = NamespaceManifestEnvelope::from_payload(payload)
                            .map_err(|error| ObjectStoreError::transport(key, error.to_string()))?;
                        Bytes::from(
                            encode_namespace_manifest_json(&mutated).map_err(|error| {
                                ObjectStoreError::transport(key, error.to_string())
                            })?,
                        )
                    }
                };
                self.inner.put_overwrite(key, replacement_bytes).await?;
                return Err(ObjectStoreError::PreconditionFailed {
                    object_key: key.to_owned(),
                });
            }
        }
        self.inner.put(key, bytes, mode).await
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

use super::build::{
    build_manifest_l0_run_tables, debug_assert_manifest_table_segments_do_not_overlap,
};
use super::runs::l0_run_count;

// Test support: a manifest built directly from a MetadataState, used to
// author arbitrary layouts without driving the full checkpoint pipeline.
#[cfg(test)]
pub(crate) struct ManifestMetadataSource<'a> {
    pub(super) head: &'a HeadState,
    pub(super) basis_manifest_id: Option<ManifestId>,
    pub(super) retention_floor_seq: ChangeSeq,
    pub(super) metadata_state: &'a MetadataState,
}

#[cfg(test)]
pub(crate) async fn build_namespace_manifest_from_metadata_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    source: ManifestMetadataSource<'_>,
    policy: MetadataLsmPolicy,
    manifest_id: ManifestId,
) -> crate::error::Result<NamespaceManifestEnvelope> {
    let manifest_object_id = ManifestObjectId::generate(manifest_id);
    let head = source.head;
    let metadata_state = source.metadata_state;
    let head_seq = head.seq;
    let previous_manifest = match source.basis_manifest_id {
        Some(previous_id) => Some(
            load_manifest_materialization_for_inspection(store, namespace_id, previous_id)
                .await
                .map_err(|error| {
                    CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
                })?,
        ),
        _ => None,
    };

    let (base_seq, metadata_files) = match previous_manifest {
        Some(previous) if is_bootstrap_seed_manifest(&previous.manifest.payload) => {
            let run_tables = build_manifest_tables(
                store,
                namespace_id,
                head_seq,
                CHECKPOINT_BASE_RUN_LEVEL,
                metadata_state,
                policy.max_rows_per_segment,
            )
            .await?;
            debug_assert_manifest_table_segments_do_not_overlap(&run_tables);
            (head_seq, flatten_manifest_tables(run_tables))
        }
        Some(previous) if l0_run_count(&previous.manifest.payload) < policy.max_l0_runs.get() => {
            let mut metadata_files = previous.manifest.payload.metadata_files.clone();
            if previous.manifest.payload.head_seq < head_seq {
                metadata_files.extend(flatten_manifest_tables(
                    build_manifest_l0_run_tables(
                        store,
                        namespace_id,
                        head_seq,
                        previous.manifest.payload.head_seq,
                        metadata_state,
                    )
                    .await?,
                ));
            }
            (previous.manifest.payload.base_seq, metadata_files)
        }
        Some(_) => {
            let run_tables = build_manifest_tables(
                store,
                namespace_id,
                head_seq,
                CHECKPOINT_BASE_RUN_LEVEL,
                metadata_state,
                policy.max_rows_per_segment,
            )
            .await?;
            debug_assert_manifest_table_segments_do_not_overlap(&run_tables);
            (head_seq, flatten_manifest_tables(run_tables))
        }
        _ => {
            let run_tables = build_manifest_tables(
                store,
                namespace_id,
                head_seq,
                CHECKPOINT_BASE_RUN_LEVEL,
                metadata_state,
                policy.max_rows_per_segment,
            )
            .await?;
            (head_seq, flatten_manifest_tables(run_tables))
        }
    };

    NamespaceManifestEnvelope::from_payload(NamespaceManifestPayload {
        namespace_id: namespace_id.clone(),
        manifest_id,
        manifest_object_id,
        head_seq,
        head_commit_id: head.head_commit_id.clone(),
        base_seq,
        writer_epoch: head.writer_epoch,
        next_inode_id: head.next_inode_id,
        retention_floor_seq: source.retention_floor_seq,
        metadata_files,
        reorganize: None,
    })
    .map_err(|err| {
        CoreError::Internal(format!(
            "failed to build namespace manifest envelope: {err}"
        ))
    })
}

#[cfg(test)]
fn is_bootstrap_seed_manifest(payload: &NamespaceManifestPayload) -> bool {
    payload.head_seq == ChangeSeq(0) && payload.base_seq == ChangeSeq(0)
}
