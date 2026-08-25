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
mod streaming_compaction;

use super::build::{
    build_manifest_segments, build_manifest_segments_from_rows, MetadataSegmentation,
};
use super::cache::{MetadataSegmentBlockKind, MetadataSegmentCache, MetadataSegmentCacheConfig};
use super::compaction_merge::locality_of;
use super::compaction_retention::RetentionRule;
use super::create::load_checkpoint_projection_metadata_state;
use super::error::ManifestLoadError;
use super::frozen_floor::{bind_survives_frozen_floor, unbindings_at_or_below_floor};
use super::load::load_verified_manifest_segments_with_cache;
use super::load::{
    head_from_manifest, load_manifest_materialization_for_inspection,
    load_manifest_metadata_state_for_inspection_from_manifest, load_verified_manifest_segments,
};
use super::publish::{publish_metadata_root, write_namespace_manifest, ManifestPublicationOutcome};
use super::record::load_checkpoint_record;
use super::retention::advance_retention_floor;
use super::row::{manifest_rows_for_family, metadata_states_equivalent};
use super::runs::{
    flatten_manifest_segments, runs_from_segments, runs_in_scan_order, MetadataFamilyGroup,
    MetadataFamilySegments, MetadataLsmPolicy, MetadataRunManifest, CHECKPOINT_BASE_RUN_LEVEL,
    CHECKPOINT_DELTA_RUN_LEVEL, CHECKPOINT_ROW_FAMILIES, DEFAULT_MAX_CHECKPOINT_DELTA_RUNS,
    REORGANIZE_FAMILY_GROUPS,
};
use super::stored_block_cache::{
    StoredMetadataBlockCache, StoredMetadataBlockKey, StoredMetadataBlockKind,
};
use super::streaming_compaction::{
    retention_clusters, run_metadata_compaction_job, MetadataCompactionCancellation,
    MetadataCompactionJobOutcome, MetadataCompactionSpec,
};
use super::{
    block_fetch, create, data_block_load, flush, load, record, reorganize,
    reorganize_metadata_step, row, scan, MetadataCompactionView, MetadataReorganizeOutcome,
};
use crate::error::{CoreError, ErrorCode, MetadataProjectionLoadError};
use crate::metadata::MetadataState;
use crate::namespace::catalog::load_namespace_catalog_entry;
use crate::namespace::control::{
    load_head_object, load_metadata_root_object, load_wal_floor_object,
};
use crate::namespace::status::{load_namespace, load_namespace_diagnostics};
use crate::namespace::writer_epoch::acquire_writer_epoch;
use crate::path::read::{load_current_metadata_view, resolve_current_files, CurrentFileState};
use crate::protocol::list_changes_after;
use crate::publish::{
    CommitCandidate, CommitRequest, FilesystemOperation, NamespaceCommitEngine, PublishTailOptions,
};
use crate::storage::content::{prepare_stored_content, store_bytes_as_content};
use crate::test_support::ops::{
    delete_path, move_path, put_file_bytes, restore_file_revision, write_file_bytes,
};
use crate::test_support::{RecordedStoredMetadataBlockCall, RecordingStoredMetadataBlockCache};
use crate::MutationContext;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_api::wire::control::{HeadState, ManifestRef, MetadataRootState};
use loonfs_api::wire::manifest::{
    decode_namespace_manifest_json, encode_namespace_manifest_json, lookup_keys, MetadataRow,
    MetadataRowFamily as ApiMetadataRowFamily, MetadataSegmentRef, NamespaceManifestEnvelope,
    NamespaceManifestPayload,
};
use loonfs_api::wire::sst_blocks::{
    decode_data_block, string_prefix_upper_bound, BlockHandle, DecodedDataBlock,
    SegmentBlocksBuilder, SegmentIndexEntry, DEFAULT_TARGET_BLOCK_BYTES,
};
use loonfs_api::{
    AbsolutePath, ChangeSeq, CheckpointId, CommitId, DestinationBehavior, EffectiveLimit, InodeId,
    ManifestNo, ManifestObjectId, NameKey, NamespaceId, RevisionNo, RunNo,
};
use loonfs_objectstore::keys::{
    metadata_manifest_object, metadata_manifest_prefix, metadata_root, metadata_segment_object_key,
    wal_floor, wal_head, wal_segment,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use loonfs_test_support::stores::{
    CountingStore, FailStore, InjectedError, KeyPredicate, OperationClass,
};
use std::collections::{BTreeMap, BTreeSet};
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
) -> crate::error::Result<loonfs_api::Checkpoint> {
    super::create::create_checkpoint(
        store,
        namespace_id,
        loonfs_api::wire::control::CheckpointOwner::User {
            name: "test-pin".to_owned(),
            expires_at_ms: None,
        },
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
    let content_ref = stored.content_ref().clone();
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
                    loonfs_test_support::test_actor(),
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
pub(crate) struct CurrentProjection {
    pub(crate) head: HeadState,
    pub(crate) root: MetadataRootState,
    pub(crate) metadata_state: MetadataState,
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
) -> Result<loonfs_api::Namespace, crate::namespace::BootstrapNamespaceError> {
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
    let head = load_head_object(store, namespace_id)
        .await
        .expect("read head")
        .state;
    crate::namespace::control_snapshot::resolve_retention_floor_seq(store, &head)
        .await
        .expect("resolve retention floor")
}

/// Checkpoints, then folds every delta run into the base through
/// reorganization units, returning the resulting current manifest number. The
/// old synchronous rebuild produced this shape in one checkpoint call;
/// tests that need a compacted base with a specific segmentation policy use
/// this instead.
async fn checkpoint_then_reorganize<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
) -> ManifestNo {
    create_checkpoint(store, namespace_id, context)
        .await
        .expect("create checkpoint");
    drain_reorganization(store, namespace_id, context, policy).await
}

/// Runs the merge engine's retention operators over rows a test holds in
/// memory, so a rule test can state a handful of rows and a floor and assert
/// what survives without a namespace, a manifest, or segments.
///
/// It is a harness, not a second implementation: the clusters, the locality
/// grouping, and the operators are the engine's own. The one thing it does
/// differently is the reverse bind index, which a merge decides by point-reading
/// the unbinds of one binding out of its snapshot. Here the whole unbind family
/// is in hand, so the shared rules are applied to it directly — which is what
/// the point read does with the rows it fetched.
fn fold_rows_with_retention(
    group: MetadataFamilyGroup,
    rows_by_family: &mut BTreeMap<ApiMetadataRowFamily, Vec<MetadataRow>>,
    floor_seq: ChangeSeq,
) -> crate::error::Result<()> {
    // Built from the input rows, before any cluster replaces them: a merge's
    // point reads go to its immutable snapshot, never to what it has written.
    let unbound_at_floor = unbindings_at_or_below_floor(
        rows_by_family
            .get(&ApiMetadataRowFamily::DirentryUnbinds)
            .map_or(&[][..], Vec::as_slice),
        floor_seq,
    );
    for cluster in retention_clusters(group) {
        // The stream a merge sees: the cluster's rows by locality, then family,
        // then row key.
        let mut merged: Vec<(ApiMetadataRowFamily, String, MetadataRow)> = Vec::new();
        for family in cluster.families {
            for row in rows_by_family.get(family).map_or(&[][..], Vec::as_slice) {
                merged.push((*family, row.row_key_for_family(*family), row.clone()));
            }
        }
        merged.sort_by(|left, right| {
            locality_of(left.0, &left.1, cluster.locality)
                .cmp(locality_of(right.0, &right.1, cluster.locality))
                .then(left.0.cmp(&right.0))
                .then(left.1.cmp(&right.1))
        });

        let mut kept: BTreeMap<ApiMetadataRowFamily, Vec<MetadataRow>> = cluster
            .families
            .iter()
            .map(|family| (*family, Vec::new()))
            .collect();
        let mut operator = cluster.rule.operator();
        let mut locality: Option<String> = None;
        for (family, row_key, row) in merged {
            let row_locality = locality_of(family, &row_key, cluster.locality);
            if locality.as_deref() != Some(row_locality) {
                if let Some((family, row)) = operator.close_group(floor_seq)? {
                    kept.entry(family).or_default().push(row);
                }
                locality = Some(row_locality.to_owned());
            }
            let survivor = match cluster.rule {
                RetentionRule::ReverseBindProbe => {
                    bind_survives_frozen_floor(&row, floor_seq, &unbound_at_floor)
                        .then_some((family, row))
                }
                _ => operator.push(family, row, floor_seq)?,
            };
            if let Some((family, row)) = survivor {
                kept.entry(family).or_default().push(row);
            }
        }
        if let Some((family, row)) = operator.close_group(floor_seq)? {
            kept.entry(family).or_default().push(row);
        }
        for (family, rows) in kept {
            rows_by_family.insert(family, rows);
        }
    }
    Ok(())
}

/// Runs reorganization units until nothing is left to fold, with the
/// trigger forced so even one delta run folds.
async fn drain_reorganization<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
) -> ManifestNo {
    let fold_policy = MetadataLsmPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        ..policy
    };
    loop {
        let report = super::reorganize_metadata_step(
            store,
            namespace_id,
            context,
            fold_policy,
            MetadataCompactionView::default(),
        )
        .await
        .expect("reorganization step");
        match report.outcome {
            super::MetadataReorganizeOutcome::UnitPublished { .. }
            | super::MetadataReorganizeOutcome::Superseded => continue,
            super::MetadataReorganizeOutcome::NotNeeded { .. } => break,
            super::MetadataReorganizeOutcome::CompactionPlanned { .. } => {
                panic!("test reorganization budget should admit a progress-making subset")
            }
        }
    }
    load_metadata_root_object(store, namespace_id)
        .await
        .expect("read metadata root")
        .state
        .manifest
        .manifest_no
}

/// Runs the job a step planned, the way the maintenance runner's background
/// task runs it — except that this waits for it, which is what makes a test
/// deterministic.
async fn run_planned_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
    spec: &MetadataCompactionSpec,
) -> MetadataCompactionJobOutcome {
    run_metadata_compaction_job(
        store,
        namespace_id,
        context,
        spec,
        policy,
        &MetadataCompactionCancellation::default(),
    )
    .await
    .expect("run the planned streaming compaction")
}

/// What a reader sees right now: every inode the namespace knows, resolved
/// the way a read resolves it — visible or not, at what path, at what
/// revision.
///
/// This is the answer that must not move while a fold or a rebuild runs.
/// Comparing rows would say the opposite of what is wanted: both drop rows
/// precisely because no read can observe them, so the row set is meant to
/// change and this is not.
async fn visible_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Vec<CurrentFileState> {
    let manifest_no = load_metadata_root_object(store, namespace_id)
        .await
        .expect("read metadata root")
        .state
        .manifest
        .manifest_no;
    let materialized =
        load_manifest_materialization_for_inspection(store, namespace_id, manifest_no)
            .await
            .expect("materialize the manifest");
    let inode_ids: Vec<InodeId> = materialized
        .metadata_state
        .inodes()
        .iter()
        .map(|inode| inode.inode_id)
        .collect();
    let view = load_current_metadata_view(store, namespace_id)
        .await
        .expect("load the read view");
    resolve_current_files(&view, &inode_ids)
        .await
        .expect("resolve every inode the namespace knows")
}

/// Rebuilds one family group through a streaming compaction and publishes the
/// swap, answering with the staged object keys the manifest now names.
///
/// For a caller that needs a namespace whose manifest references segments
/// under the compaction staging directory: the collector's tests, which have
/// to tell a published job's output from an orphan.
pub(crate) async fn compact_a_family_group_into_staging<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> BTreeSet<String> {
    let (policy, spec) = plan_a_family_group_compaction(store, namespace_id, context).await;
    publish_planned_compaction(store, namespace_id, context, policy, &spec).await;
    staged_keys_of_the_current_manifest(store, namespace_id).await
}

/// Plans one family group's streaming compaction without running it, with the
/// budgets that make a small namespace's group unfoldable.
///
/// For a caller that has to do something between the plan and the
/// publication — the collector's tests, which run a pass across that gap.
pub(crate) async fn plan_a_family_group_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> (MetadataLsmPolicy, MetadataCompactionSpec) {
    // One byte admits no run whole, so the group a step selects has no window
    // that makes progress and is handed to a job.
    let policy = MetadataLsmPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        max_decoded_input_bytes_per_step: NonZeroUsize::MIN,
        ..MetadataLsmPolicy::default()
    };
    let report = reorganize_metadata_step(
        store,
        namespace_id,
        context,
        policy,
        MetadataCompactionView::default(),
    )
    .await
    .expect("plan a streaming compaction");
    let MetadataReorganizeOutcome::CompactionPlanned { spec, .. } = report.outcome else {
        panic!("a budget that admits no run whole must plan a streaming compaction");
    };
    (policy, spec)
}

/// The staged object keys the namespace's current manifest names.
pub(crate) async fn staged_keys_of_the_current_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> BTreeSet<String> {
    let staging_prefix = loonfs_objectstore::keys::metadata_compaction_prefix(namespace_id);
    let manifest_object_id = current_manifest_object_id(store, namespace_id).await;
    let staged: BTreeSet<String> =
        load_verified_manifest_segments(store, namespace_id, &manifest_object_id)
            .await
            .expect("load the published manifest")
            .manifest()
            .payload
            .segments
            .iter()
            .map(metadata_segment_object_key)
            .filter(|key| key.starts_with(&staging_prefix))
            .collect();
    assert!(
        !staged.is_empty(),
        "a published job's output is referenced from the staging directory"
    );
    staged
}

/// Runs one planned job, where anything but a publication is a test failure.
pub(crate) async fn publish_planned_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
    spec: &MetadataCompactionSpec,
) {
    let outcome = run_planned_compaction(store, namespace_id, context, policy, spec).await;
    assert!(
        matches!(outcome, MetadataCompactionJobOutcome::Published { .. }),
        "the planned job must publish, got {outcome:?}"
    );
}

async fn current_manifest_object_id<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> ManifestObjectId {
    load_metadata_root_object(store, namespace_id)
        .await
        .expect("read metadata root")
        .state
        .manifest
        .manifest_object_id
}

async fn current_manifest_key<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> String {
    metadata_manifest_object(
        namespace_id,
        &current_manifest_object_id(store, namespace_id).await,
    )
}

fn manifest_object_id(manifest_no: ManifestNo) -> ManifestObjectId {
    ManifestObjectId::parse(format!("man_{:020}-0123456789abcdef", manifest_no.0))
        .expect("valid manifest object id")
}

pub(crate) async fn load_current_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<CurrentProjection, CoreError> {
    let (head, metadata_state) =
        load_checkpoint_projection_metadata_state(store, namespace_id).await?;
    let root = load_metadata_root_object(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?
        .state;
    Ok(CurrentProjection {
        head,
        root,
        metadata_state,
    })
}

/// Every base-tier segment the manifest holds, gathered per family.
///
/// One family group has at most one base run, but the groups rebuild
/// separately and each rebuild writes a run of its own, so a manifest holds
/// as many base runs as it has rebuilt groups. A caller asking about "the
/// base" means all of them.
fn base_tier(manifest: &NamespaceManifestEnvelope) -> Vec<MetadataFamilySegments> {
    let base_runs = runs_from_segments(&manifest.payload)
        .into_iter()
        .filter(|run| run.level == CHECKPOINT_BASE_RUN_LEVEL)
        .collect::<Vec<_>>();
    assert!(!base_runs.is_empty(), "base tier");
    CHECKPOINT_ROW_FAMILIES
        .into_iter()
        .map(|family| MetadataFamilySegments {
            family,
            segments: base_runs
                .iter()
                .flat_map(|run| &run.segments)
                .filter(|family_segments| family_segments.family == family)
                .flat_map(|family_segments| family_segments.segments.clone())
                .collect(),
        })
        .collect()
}

fn delta_runs(manifest: &NamespaceManifestEnvelope) -> Vec<MetadataRunManifest> {
    runs_from_segments(&manifest.payload)
        .into_iter()
        .filter(|run| run.level == CHECKPOINT_DELTA_RUN_LEVEL)
        .collect()
}

/// Every run that holds rows of one family group right now.
fn group_runs(
    manifest: &NamespaceManifestEnvelope,
    group: &[ApiMetadataRowFamily],
) -> Vec<MetadataRunManifest> {
    runs_from_segments(&manifest.payload)
        .into_iter()
        .filter(|run| {
            run.segments.iter().any(|family_segments| {
                group.contains(&family_segments.family) && !family_segments.segments.is_empty()
            })
        })
        .collect()
}

/// The base-tier runs one family group holds right now.
///
/// A group holds at most one: only a merge that starts at the group's oldest
/// run writes a base run, and such a merge always replaces the one that was
/// there. More than one is the fragmented base a merge above the base used to
/// create, which manifest load now refuses.
fn group_base_runs(
    manifest: &NamespaceManifestEnvelope,
    group: &[ApiMetadataRowFamily],
) -> Vec<MetadataRunManifest> {
    group_runs(manifest, group)
        .into_iter()
        .filter(|run| run.level != CHECKPOINT_DELTA_RUN_LEVEL)
        .collect()
}

/// The delta runs one family group holds right now.
fn group_delta_runs(
    manifest: &NamespaceManifestEnvelope,
    group: &[ApiMetadataRowFamily],
) -> Vec<MetadataRunManifest> {
    group_runs(manifest, group)
        .into_iter()
        .filter(|run| run.level == CHECKPOINT_DELTA_RUN_LEVEL)
        .collect()
}

/// Every family group's base-tier runs, for a caller asserting that none of
/// them has fragmented.
fn base_runs_per_family_group(
    manifest: &NamespaceManifestEnvelope,
) -> Vec<(&'static [ApiMetadataRowFamily], Vec<MetadataRunManifest>)> {
    REORGANIZE_FAMILY_GROUPS
        .into_iter()
        .map(|group| {
            (
                group.families(),
                group_base_runs(manifest, group.families()),
            )
        })
        .collect()
}

fn group_containing(family: ApiMetadataRowFamily) -> &'static [ApiMetadataRowFamily] {
    REORGANIZE_FAMILY_GROUPS
        .into_iter()
        .find(|group| group.contains(family))
        .expect("every family belongs to a reorganization group")
        .families()
}

fn base_segment_object_keys_for_family(
    manifest: &NamespaceManifestEnvelope,
    family: ApiMetadataRowFamily,
) -> Vec<String> {
    base_tier(manifest)
        .iter()
        .find(|family_segments| family_segments.family == family)
        .expect("the family's segments")
        .segments
        .iter()
        .map(metadata_segment_object_key)
        .collect()
}

fn test_context() -> MutationContext {
    mutation_context("test-writer", 1_000)
}

fn manifest_no(seq: ChangeSeq) -> ManifestNo {
    ManifestNo(seq.0)
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

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}

use super::build::{
    build_manifest_delta_run_segments, debug_assert_manifest_segments_do_not_overlap,
};
use super::flush::next_run_no_after;
use super::runs::delta_run_count;

// Test support: a manifest built directly from a MetadataState, used to
// author arbitrary layouts without driving the full checkpoint pipeline.
#[cfg(test)]
pub(crate) struct ManifestMetadataSource<'a> {
    pub(crate) head: &'a HeadState,
    pub(crate) basis_manifest_no: Option<ManifestNo>,
    pub(crate) retention_floor_seq: ChangeSeq,
    pub(crate) metadata_state: &'a MetadataState,
}

#[cfg(test)]
pub(crate) async fn build_namespace_manifest_from_metadata_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    source: ManifestMetadataSource<'_>,
    policy: MetadataLsmPolicy,
    manifest_no: ManifestNo,
) -> crate::error::Result<NamespaceManifestEnvelope> {
    let manifest_object_id = ManifestObjectId::generate(manifest_no);
    let head = source.head;
    let metadata_state = source.metadata_state;
    let head_seq = head.seq;
    let previous_manifest = match source.basis_manifest_no {
        Some(previous_id) => Some(
            load_manifest_materialization_for_inspection(store, namespace_id, previous_id)
                .await
                .map_err(|error| {
                    CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
                })?,
        ),
        _ => None,
    };

    // Same allocation rule as the production flush: the manifest that first
    // names a run takes the number, and a manifest that writes no run leaves
    // the counter alone.
    let run_no = previous_manifest
        .as_ref()
        .map_or(RunNo(0), |previous| previous.manifest.payload.next_run_no);
    let mut next_run_no = next_run_no_after(run_no)?;
    let (base_seq, segments) = match previous_manifest {
        Some(previous) if is_bootstrap_seed_manifest(&previous.manifest.payload) => {
            let run_segments = build_manifest_segments(
                store,
                namespace_id,
                run_no,
                head_seq,
                CHECKPOINT_BASE_RUN_LEVEL,
                metadata_state,
                policy.max_rows_per_segment,
            )
            .await?;
            debug_assert_manifest_segments_do_not_overlap(&run_segments);
            (head_seq, flatten_manifest_segments(run_segments))
        }
        Some(previous)
            if delta_run_count(&previous.manifest.payload) < policy.max_delta_runs.get() =>
        {
            let mut segments = previous.manifest.payload.segments.clone();
            if previous.manifest.payload.head_seq < head_seq {
                segments.extend(flatten_manifest_segments(
                    build_manifest_delta_run_segments(
                        store,
                        namespace_id,
                        run_no,
                        head_seq,
                        previous.manifest.payload.head_seq,
                        metadata_state,
                    )
                    .await?,
                ));
            } else {
                next_run_no = run_no;
            }
            (previous.manifest.payload.base_seq, segments)
        }
        Some(_) => {
            let run_segments = build_manifest_segments(
                store,
                namespace_id,
                run_no,
                head_seq,
                CHECKPOINT_BASE_RUN_LEVEL,
                metadata_state,
                policy.max_rows_per_segment,
            )
            .await?;
            debug_assert_manifest_segments_do_not_overlap(&run_segments);
            (head_seq, flatten_manifest_segments(run_segments))
        }
        _ => {
            let run_segments = build_manifest_segments(
                store,
                namespace_id,
                run_no,
                head_seq,
                CHECKPOINT_BASE_RUN_LEVEL,
                metadata_state,
                policy.max_rows_per_segment,
            )
            .await?;
            (head_seq, flatten_manifest_segments(run_segments))
        }
    };

    NamespaceManifestEnvelope::from_payload(NamespaceManifestPayload {
        namespace_id: namespace_id.clone(),
        manifest_no,
        manifest_object_id,
        head_seq,
        head_commit_id: head.head_commit_id.clone(),
        base_seq,
        writer_epoch: head.writer_epoch,
        next_inode_id: head.next_inode_id,
        next_run_no,
        retention_floor_seq: source.retention_floor_seq,
        segments,
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
