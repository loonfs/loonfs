//! Manifest, cache, index, recovery, and retention tests.

mod access;
mod active_deletions;
mod attributes;
mod basis_publications;
mod bounded_pages;
mod cache;
mod cas_recovery;
mod directory_bindings;
mod expired_deletion_rows;
mod expired_subtrees;
mod fold_races;
mod index_parity;
pub(crate) mod inspection_materialization;
mod manifest_gc_races;
mod manifest_round_trips;
mod newest_first_lookups;
mod read_ahead_windows;
mod retention;
mod streaming_compaction;

use super::error::ManifestLoadError;
use super::load::{
    append_rows_to_metadata, head_from_manifest, load_manifest_materialization_for_inspection,
    load_manifest_metadata_state_for_inspection_from_manifest,
    load_manifest_segments_for_inspection,
};
use super::publish::{encode_manifest, ManifestPublicationOutcome};
use super::retention::RetentionTarget;
use super::{fold, load};
use crate::block_cache::ObjectStoreScope;
use crate::changes::list_changes_after;
use crate::compaction::group_merge::run_metadata_compaction;
use crate::compaction::merge_iterator::locality_of;
use crate::compaction::plan;
use crate::compaction::plan::MetadataCompactionSpec;
use crate::compaction::publication::{input_segment_keys, CompactionPublication};
use crate::compaction::retention::retention_clusters;
use crate::compaction::step::MetadataCompactionCancellation;
use crate::compaction::{CompactionUnitOutcome, MetadataCompactionPolicy};
use crate::error::{CoreError, ErrorCode, MetadataProjectionLoadError};
use crate::metadata::{MetadataState, MetadataStateBuilder};
use crate::namespace::state::NamespaceReadState;
use crate::path::read::{stat_current_files_by_inode, CurrentFile, CurrentFileState};
use crate::publish::{CommitCandidate, CommitIntent, FilesystemOperation, NamespaceCommitEngine};
use crate::segment::cache::SegmentBlockKind;
use crate::segment::cache::{MetadataSegmentCache, SharedSegmentBlocks};
use crate::segment::row::{manifest_rows_for_family, metadata_states_equivalent};
use crate::segment::runs::{
    flatten_manifest_segments, runs_in_materialization_order, runs_newest_first,
    MetadataFamilyGroup, MetadataFamilySegments, MetadataRunManifest, MetadataRunPolicy,
    DEFAULT_MAX_DELTA_RUNS,
};
use crate::segment::{data_block_load, index_block_load, row, scan};
use crate::storage::content::prepare_stored_content;
use crate::test_support::WriterAt;
use crate::time::{OperationClock, StdMonotonicTimer};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_objectstore::keys::{
    hint, metadata_manifest_object, metadata_manifest_prefix, metadata_segment_object_key,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, LostCreateStore, OperationSelector,
    RecordingStore,
};
use loonfs_types::format::manifest::{
    encode_namespace_manifest_json, lookup_keys, MetadataRow,
    MetadataRowFamily as ApiMetadataRowFamily, MetadataRunRef, MetadataSegmentRef,
    NamespaceManifestEnvelope, NamespaceManifestPayload, RunTier,
};
use loonfs_types::format::segment_blocks::{
    decode_data_block, string_prefix_upper_bound, BlockHandle, DecodedDataBlock,
    SegmentBlocksBuilder, SegmentIndexEntry, DEFAULT_TARGET_BLOCK_BYTES,
};
use loonfs_types::{
    AbsolutePath, ChangeSeq, CommitId, DestinationBehavior, EffectiveLimit, InodeId, ManifestNo,
    NameKey, NamespaceId, PinId, RevisionNo, RunNo,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use tempfile::tempdir;

impl<S: ObjectStore> crate::namespace::store::NamespaceStore<S> {
    /// Folds the visible WAL tail into segments and publishes the next manifest.
    pub(crate) async fn fold_wal(&self) -> crate::error::Result<crate::manifest::FoldWalResponse> {
        let clock = self.start_clock();
        self.fold_wal_tail(None, &clock)
            .await
            .map(|folded| folded.response)
    }
}

async fn load_manifest_projection_metadata_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> crate::error::Result<(NamespaceReadState, MetadataState)> {
    let namespace = crate::test_support::namespace_store(store, namespace_id);
    let projection = namespace
        .load_manifest_projection(crate::test_support::observed_now())
        .await?;
    let tail_state = projection.tail_with_deletion_inodes().await?;
    let tail_rows = projection.tail_state.merged_rows();
    let mut metadata_state = MetadataStateBuilder::default();
    for family in ApiMetadataRowFamily::ALL {
        let mut rows = projection.manifest_segments.scan_prefix(family, "").await?;
        let state = if family == ApiMetadataRowFamily::ActiveDeletions {
            &tail_state
        } else {
            &tail_rows
        };
        rows.extend(manifest_rows_for_family(state, family));
        rows.sort_by_key(|row| row.row_key_for_family(family));
        append_rows_to_metadata(&mut metadata_state, family, "checkpoint projection", &rows)?;
    }
    drop(tail_state);
    Ok((projection.head, metadata_state.build()))
}

/// Every lifecycle test in this file pins as one user owner; owner-specific
/// behavior (fork owners, distinct-owner records) is exercised explicitly
/// where it matters.
pub(crate) async fn create_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &WriterAt,
) -> crate::error::Result<loonfs_types::Checkpoint> {
    crate::test_support::namespace_store(store, namespace_id)
        .create_pin(
            loonfs_types::format::control::PinOwner::User {
                name: "test-pin".to_owned(),
                expires_at_ms: None,
            },
            &context.clock(),
        )
        .await
        .map(|report| crate::pin::checkpoint_summary(report.response))
}

pub(crate) use crate::test_support::writer_at;

pub(crate) async fn write_test_file<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    path: &str,
    commit_id: &str,
    context: &WriterAt,
) {
    let stored = crate::test_support::namespace_store(store, namespace_id)
        .store_bytes_as_content(b"body\n")
        .await
        .expect("store content");
    let content_ref = stored.content_ref().clone();
    let catalog = crate::test_support::namespace_store(store, namespace_id)
        .load_catalog_entry()
        .await
        .expect("load namespace catalog");
    let prepared = prepare_stored_content(&catalog, stored);
    NamespaceCommitEngine::new(
        crate::test_support::namespace_store(store, namespace_id),
        context.writer_id.clone(),
    )
    .publish_batch(
        vec![CommitCandidate::prepared(
            CommitIntent::single(
                CommitId::parse(commit_id).expect("commit id"),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::PutFile {
                    path: AbsolutePath::parse(path).expect("path"),
                    content: loonfs_types::FileContent::Ref(content_ref),
                    behavior: DestinationBehavior::NoReplace,
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
            ),
            vec![prepared],
        )],
        &context.clock(),
    )
    .await
    .results
    .pop()
    .expect("one result")
    .expect("write file");
}

#[derive(Debug)]
pub(crate) struct CurrentProjection {
    pub(crate) head: NamespaceReadState,
    pub(crate) manifest: crate::namespace::control::LoadedManifest,
    pub(crate) metadata_state: MetadataState,
}

pub(crate) async fn bootstrap_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &WriterAt,
) -> crate::error::Result<loonfs_types::NamespaceMetadata> {
    let summary = crate::test_support::NamespaceFixture::new(store, namespace_id, context)
        .create()
        .await?;
    crate::test_support::namespace_store(store, namespace_id)
        .acquire_writer_epoch(&context.writer_id)
        .await
        .expect("acquire fixture writer");
    crate::test_support::namespace_store(store, namespace_id)
        .fold_wal()
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
    let head = crate::test_support::namespace_store(store, namespace_id)
        .load_read_state()
        .await
        .expect("read head");
    crate::test_support::namespace_store(store, &head.namespace_id)
        .load_current_manifest()
        .await
        .expect("resolve retention floor")
        .retention_floor_seq()
}

/// The current manifest's acknowledged floor.
async fn read_acknowledged_floor_seq<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> ChangeSeq {
    crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest")
        .payload()
        .acknowledged_floor_seq
}

/// Folds the WAL tail and advances the retention floor to `deletion_seq`,
/// which expires the deletion committed at that sequence and every older
/// one, at a time when every commit is a grace window old.
async fn advance_floor_through<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    deletion_seq: ChangeSeq,
    context: &WriterAt,
) {
    crate::test_support::namespace_store(store, namespace_id)
        .fold_wal()
        .await
        .expect("fold the deletion");
    let advanced = crate::test_support::NamespaceFixture::new(
        store,
        namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Seq(deletion_seq))
    .await
    .expect("advance the floor to the deletion");
    assert_eq!(advanced.response.retention_floor_seq, deletion_seq);
}

/// Acknowledges the current retention floor the way a maintainer does for a
/// namespace that nobody writes to: acquires the writer epoch, whose fence
/// records the floor, and folds the fence.
async fn acknowledge_floor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &WriterAt,
) {
    crate::test_support::namespace_store(store, namespace_id)
        .acquire_writer_epoch(&context.writer_id)
        .await
        .expect("write a fence");
    crate::test_support::namespace_store(store, namespace_id)
        .fold_wal()
        .await
        .expect("fold the fence");
    assert_eq!(
        read_acknowledged_floor_seq(store, namespace_id).await,
        read_floor_seq(store, namespace_id).await,
        "the fold of the fence acknowledges the floor"
    );
}

/// Checkpoints, then compacts every delta run into the base through
/// compaction units, returning the resulting current manifest number. The
/// old synchronous rebuild produced this shape in one checkpoint call;
/// tests that need a compacted base with a specific segmentation policy use
/// this instead.
async fn checkpoint_then_compact<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &WriterAt,
    policy: MetadataRunPolicy,
) -> ManifestNo {
    create_checkpoint(store, namespace_id, context)
        .await
        .expect("create checkpoint");
    drain_compaction(
        store,
        namespace_id,
        MetadataRunPolicy {
            max_delta_runs: NonZeroUsize::MIN,
            ..policy
        },
    )
    .await
    .0
}

fn merge_rows_with_retention(
    group: MetadataFamilyGroup,
    rows_by_family: &mut BTreeMap<ApiMetadataRowFamily, Vec<MetadataRow>>,
    floor_seq: ChangeSeq,
    acknowledged_floor_seq: ChangeSeq,
) -> crate::error::Result<()> {
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
        let mut operator = cluster.rule.operator(acknowledged_floor_seq);
        let mut locality: Option<String> = None;
        for (family, row_key, row) in merged {
            let row_locality = locality_of(family, &row_key, cluster.locality);
            if locality.as_deref() != Some(row_locality) {
                if let Some((family, row)) = operator.close_group(floor_seq)? {
                    kept.entry(family).or_default().push(row);
                }
                locality = Some(row_locality.to_owned());
            }
            if let Some((family, row)) = operator.take_floor_value_before(&row, floor_seq)? {
                kept.entry(family).or_default().push(row);
            }
            let survivor = operator.push(family, row, floor_seq)?;
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

async fn drain_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    policy: MetadataRunPolicy,
) -> (ManifestNo, usize) {
    let mut published = 0;
    loop {
        let outcome = {
            let mut fixture = crate::test_support::NamespaceFixture::new(
                store,
                namespace_id,
                &crate::test_support::writer_at("test-writer", 0),
            );
            fixture.namespace.run_policy = policy;
            fixture
        }
        .engine()
        .compact_metadata_unit(
            None,
            MetadataCompactionPolicy::default(),
            crate::compaction::CompactionAdmission::AnySize,
            loonfs_types::CompactorEpoch(0),
            &crate::compaction::MetadataCompactionCancellation::default(),
        )
        .await
        .expect("compaction step");
        match outcome {
            CompactionUnitOutcome::Published { .. } => published += 1,
            CompactionUnitOutcome::Abandoned | CompactionUnitOutcome::Fenced => continue,
            CompactionUnitOutcome::NotNeeded => {
                let manifest_no = crate::test_support::namespace_store(store, namespace_id)
                    .load_current_manifest()
                    .await
                    .expect("read metadata manifest")
                    .manifest()
                    .manifest_no;
                return (manifest_no, published);
            }
            other => panic!("an admitted unit should publish or find nothing, got {other:?}"),
        }
    }
}

/// Runs a unit over a plan made earlier, against the current manifest, the
/// way `NamespaceEngine::compact_metadata_unit` runs the plan it makes.
///
/// For a test that has to do something between the plan and the merge, which
/// stands for other publications during a long merge.
async fn run_planned_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    _context: &WriterAt,
    policy: MetadataRunPolicy,
    spec: &MetadataCompactionSpec,
) -> CompactionUnitOutcome {
    run_planned_compaction_under(
        store,
        namespace_id,
        policy,
        spec,
        &MetadataCompactionCancellation::default(),
    )
    .await
}

/// The same, with the token a test that cancels the unit sets.
async fn run_planned_compaction_under<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    policy: MetadataRunPolicy,
    spec: &MetadataCompactionSpec,
    cancellation: &MetadataCompactionCancellation,
) -> CompactionUnitOutcome {
    let current = crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest");
    let segments = super::metadata_basis_from_manifest(store, None, &current).segments;
    let Some(input_keys) = input_segment_keys(&segments.scan_runs, spec) else {
        return CompactionUnitOutcome::Abandoned;
    };
    let merged = match run_metadata_compaction(
        &segments,
        &crate::cache::MetadataSegmentCache::unshared(0),
        namespace_id,
        spec,
        policy,
        cancellation,
    )
    .await
    .expect("merge the planned window")
    {
        Ok(merged) => merged,
        Err(stopped) => return stopped,
    };
    drop(segments);
    let timer = crate::test_support::std_timer();
    crate::test_support::namespace_store(store, namespace_id)
        .finalize_metadata_compaction(
            None,
            spec,
            &input_keys,
            merged,
            cancellation,
            &CompactionPublication {
                compactor_epoch: loonfs_types::CompactorEpoch(0),
                compaction: OperationClock::start(0, Arc::clone(&timer)),
                publication: OperationClock::start(0, timer),
            },
        )
        .await
        .expect("publish the planned unit")
}

/// What a reader sees right now: every visible inode, resolved the way a
/// read resolves it, at what path and at what revision.
///
/// This is the answer that must not move while a fold or a compaction runs.
/// Comparing rows would say the opposite of what is wanted: both drop rows
/// precisely because no read can observe them, so the row set is meant to
/// change and this is not.
async fn visible_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Vec<CurrentFileState> {
    // Every inode with a stored row, and the root, which has no row until a
    // compaction stores one. A base compaction removes the rows of an inode
    // that an expired deletion hides, so the answer keeps only the visible
    // inodes: a hidden inode that a read wrongly reported as visible before
    // the removal would still change the answer.
    let state = load_current_projection(store, namespace_id)
        .await
        .expect("load the current projection")
        .metadata_state;
    let inode_ids: Vec<InodeId> = std::iter::once(loonfs_types::ROOT_INODE_ID)
        .chain(
            manifest_rows_for_family(&state, ApiMetadataRowFamily::Inodes)
                .into_iter()
                .map(|row| match row {
                    MetadataRow::Inode(record) => record.inode_id,
                    other => panic!("the inodes family holds a foreign row: {other:?}"),
                }),
        )
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let namespace = crate::test_support::namespace_store(store, namespace_id);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("load the read view");
    stat_current_files_by_inode(
        &view,
        &inode_ids,
        &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
    )
    .await
    .expect("resolve every inode the namespace knows")
    .into_iter()
    .filter(|state| matches!(state.file, CurrentFile::Visible { .. }))
    .collect()
}

/// Copies every file below `from` to the same relative path below `to`, so
/// a test can run two operations on identical durable state, including the
/// same generated inode and content IDs.
fn copy_directory(from: &std::path::Path, to: &std::path::Path) {
    for entry in std::fs::read_dir(from).expect("read directory") {
        let entry = entry.expect("directory entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            std::fs::create_dir_all(&target).expect("create directory");
            copy_directory(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy file");
        }
    }
}

/// Rebuilds one family group through a compaction unit and publishes the
/// swap, answering with the staged object keys the manifest now names.
///
/// For a caller that needs a namespace whose manifest references segments
/// under the compaction segment directory: the collector's tests, which have
/// to tell a published job's output from an orphan.
pub(crate) async fn compact_a_family_group<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &WriterAt,
) -> BTreeSet<String> {
    let (policy, spec) = plan_a_family_group_compaction(store, namespace_id, context).await;
    publish_planned_compaction(store, namespace_id, context, policy, &spec).await;
    segment_keys_of_the_current_manifest(store, namespace_id).await
}

/// Plans one family group's compaction unit without running it.
///
/// For a caller that has to do something between the plan and the
/// publication, such as the collector's tests, which run a pass across that
/// gap.
pub(crate) async fn plan_a_family_group_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    _context: &WriterAt,
) -> (MetadataRunPolicy, MetadataCompactionSpec) {
    let policy = MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        ..MetadataRunPolicy::default()
    };
    let current = crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest");
    let payload = current.payload();
    let spec = plan::plan_compaction(
        payload,
        &runs_newest_first(payload),
        policy,
        MetadataCompactionPolicy::default(),
    )
    .expect("a namespace with delta runs plans a unit");
    (policy, spec)
}

/// The staged object keys the namespace's current manifest names.
pub(crate) async fn segment_keys_of_the_current_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> BTreeSet<String> {
    let segment_prefix = loonfs_objectstore::keys::metadata_segment_prefix(namespace_id);
    let manifest_number = current_manifest_number(store, namespace_id).await;
    let staged: BTreeSet<String> =
        load_manifest_segments_for_inspection(store, None, namespace_id, &manifest_number)
            .await
            .expect("load the published manifest")
            .manifest()
            .payload()
            .runs
            .iter()
            .flat_map(|run| &run.segments)
            .map(metadata_segment_object_key)
            .filter(|key| key.starts_with(&segment_prefix))
            .collect();
    assert!(
        !staged.is_empty(),
        "a published job's output is referenced from the segment directory"
    );
    staged
}

/// Runs one planned job, where anything but a publication is a test failure.
pub(crate) async fn publish_planned_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &WriterAt,
    policy: MetadataRunPolicy,
    spec: &MetadataCompactionSpec,
) {
    let outcome = run_planned_compaction(store, namespace_id, context, policy, spec).await;
    assert_matches!(
        outcome,
        CompactionUnitOutcome::Published { .. },
        "the planned job must publish, got {outcome:?}"
    );
}

async fn current_manifest_number<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> ManifestNo {
    crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("read metadata manifest")
        .manifest()
        .manifest_no
}

async fn current_manifest_key<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> String {
    metadata_manifest_object(
        namespace_id,
        &current_manifest_number(store, namespace_id).await,
    )
}

pub(crate) async fn load_current_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> crate::error::Result<CurrentProjection> {
    let (head, metadata_state) =
        load_manifest_projection_metadata_state(store, namespace_id).await?;
    let manifest = crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await?;
    Ok(CurrentProjection {
        head,
        manifest,
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
    let base_runs = runs_newest_first(manifest.payload())
        .into_iter()
        .filter(|run| run.tier == RunTier::Base)
        .collect::<Vec<_>>();
    assert!(!base_runs.is_empty(), "base tier");
    ApiMetadataRowFamily::ALL
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
    runs_in_materialization_order(manifest.payload())
        .into_iter()
        .filter(|run| run.tier == RunTier::Delta)
        .collect()
}

/// Every run that holds rows of one family group right now.
fn group_runs(
    manifest: &NamespaceManifestEnvelope,
    group: &[ApiMetadataRowFamily],
) -> Vec<MetadataRunManifest> {
    runs_newest_first(manifest.payload())
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
/// there. Manifest loading rejects multiple base runs for a group.
fn group_base_runs(
    manifest: &NamespaceManifestEnvelope,
    group: &[ApiMetadataRowFamily],
) -> Vec<MetadataRunManifest> {
    group_runs(manifest, group)
        .into_iter()
        .filter(|run| run.tier == RunTier::Base)
        .collect()
}

/// The delta runs one family group holds right now.
fn group_delta_runs(
    manifest: &NamespaceManifestEnvelope,
    group: &[ApiMetadataRowFamily],
) -> Vec<MetadataRunManifest> {
    group_runs(manifest, group)
        .into_iter()
        .filter(|run| run.tier == RunTier::Delta)
        .collect()
}

/// Every family group's base-tier runs, for a caller asserting that none of
/// them has fragmented.
fn base_runs_per_family_group(
    manifest: &NamespaceManifestEnvelope,
) -> Vec<(&'static [ApiMetadataRowFamily], Vec<MetadataRunManifest>)> {
    MetadataFamilyGroup::ALL
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
    MetadataFamilyGroup::ALL
        .into_iter()
        .find(|group| group.families().contains(&family))
        .expect("every family belongs to a compaction group")
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

pub(crate) fn test_context() -> WriterAt {
    writer_at("test-writer", 1_000)
}

fn manifest_no(seq: ChangeSeq) -> ManifestNo {
    ManifestNo(seq.0)
}

async fn write_file_and_checkpoint(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    context: &WriterAt,
    index: u64,
) -> ChangeSeq {
    let path = format!("/docs/file-{index}.txt");
    let bytes = format!("file {index}\n");
    crate::test_support::NamespaceFixture::new(store, namespace_id, context)
        .write_file_bytes(&path, bytes.as_bytes(), None)
        .await
        .expect("write file");
    create_checkpoint(store, namespace_id, context)
        .await
        .expect("create checkpoint")
        .captured_seq
}

use super::fold::next_run_no_after;
use crate::segment::build::debug_assert_manifest_segments_do_not_overlap;
use std::assert_matches;

fn delta_run_count(payload: &NamespaceManifestPayload) -> usize {
    payload
        .runs
        .iter()
        .filter(|run| run.tier == RunTier::Delta)
        .count()
}

// Test support: a manifest built directly from a MetadataState, used to
// author arbitrary layouts without driving the full checkpoint pipeline.
#[cfg(test)]
pub(crate) struct ManifestMetadataSource<'a> {
    pub(crate) head: &'a NamespaceReadState,
    pub(crate) basis_manifest_no: Option<ManifestNo>,
    pub(crate) retention_floor_seq: ChangeSeq,
    pub(crate) metadata_state: &'a MetadataState,
}

#[cfg(test)]
pub(crate) async fn build_namespace_manifest_from_metadata_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    source: ManifestMetadataSource<'_>,
    policy: MetadataRunPolicy,
    manifest_no: ManifestNo,
) -> crate::error::Result<NamespaceManifestEnvelope> {
    let head = source.head;
    let metadata_state = source.metadata_state;
    let head_seq = head.seq;
    let previous_manifest = match source.basis_manifest_no {
        Some(previous_id) => Some(
            load_manifest_materialization_for_inspection(store, namespace_id, previous_id).await?,
        ),
        _ => None,
    };

    // Same allocation rule as the production fold: the manifest that first
    // names a run takes the number, and a manifest that writes no run leaves
    // the counter alone.
    let run_no = previous_manifest
        .as_ref()
        .map_or(RunNo(0), |previous| previous.manifest.payload().next_run_no);
    let mut next_run_no = next_run_no_after(run_no)?;
    let runs = match previous_manifest {
        Some(previous) if is_bootstrap_seed_manifest(previous.manifest.payload()) => {
            let run_segments = crate::NamespaceStore {
                run_policy: policy,
                ..crate::test_support::namespace_store(store, namespace_id)
            }
            .build_manifest_segments(metadata_state, &HashMap::new())
            .await?;
            debug_assert_manifest_segments_do_not_overlap(&run_segments);
            vec![MetadataRunRef {
                run_no,
                run_seq: head_seq,
                tier: RunTier::Base,
                segments: flatten_manifest_segments(run_segments),
            }]
        }
        Some(previous)
            if delta_run_count(previous.manifest.payload()) < policy.max_delta_runs.get() =>
        {
            let mut runs = previous.manifest.payload().runs.clone();
            if previous.manifest.payload().head_seq < head_seq {
                runs.push(MetadataRunRef {
                    run_no,
                    run_seq: head_seq,
                    tier: RunTier::Delta,
                    segments: flatten_manifest_segments(
                        crate::NamespaceStore {
                            run_policy: policy,
                            ..crate::test_support::namespace_store(store, namespace_id)
                        }
                        .build_manifest_delta_run_segments(
                            previous.manifest.payload().head_seq,
                            metadata_state,
                            &HashMap::new(),
                        )
                        .await?,
                    ),
                });
            } else {
                next_run_no = run_no;
            }
            runs
        }
        Some(_) => {
            let run_segments = crate::NamespaceStore {
                run_policy: policy,
                ..crate::test_support::namespace_store(store, namespace_id)
            }
            .build_manifest_segments(metadata_state, &HashMap::new())
            .await?;
            debug_assert_manifest_segments_do_not_overlap(&run_segments);
            vec![MetadataRunRef {
                run_no,
                run_seq: head_seq,
                tier: RunTier::Base,
                segments: flatten_manifest_segments(run_segments),
            }]
        }
        _ => {
            let run_segments = crate::NamespaceStore {
                run_policy: policy,
                ..crate::test_support::namespace_store(store, namespace_id)
            }
            .build_manifest_segments(metadata_state, &HashMap::new())
            .await?;
            vec![MetadataRunRef {
                run_no,
                run_seq: head_seq,
                tier: RunTier::Base,
                segments: flatten_manifest_segments(run_segments),
            }]
        }
    };

    Ok(encode_namespace_manifest_json(NamespaceManifestPayload {
        activity: Default::default(),
        created_at_ms: head.created_at_ms,
        created_by: head.created_by.clone(),
        access: loonfs_types::NamespaceAccess::Unrestricted {},
        retention_window_ms: None,
        fork_basis: head.fork_basis.clone(),
        status: head.status,
        writer: head.writer.clone(),
        folded_wal_no: head.wal_no,
        compactor_epoch: loonfs_types::CompactorEpoch(0),
        namespace_id: namespace_id.clone(),
        manifest_no,

        head_seq,
        writer_epoch: head.writer_epoch,
        next_run_no,
        retention_floor_seq: source.retention_floor_seq,
        acknowledged_floor_seq: head.basis_floor_seq,
        runs,
    })
    .map(|encoded| encoded.into_envelope())
    .expect("a test namespace manifest should encode"))
}

#[cfg(test)]
fn is_bootstrap_seed_manifest(payload: &NamespaceManifestPayload) -> bool {
    payload.head_seq == ChangeSeq(0) && payload.base_seq() == ChangeSeq(0)
}

pub(crate) async fn write_namespace_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    payload: NamespaceManifestPayload,
) -> crate::error::Result<NamespaceManifestEnvelope> {
    let key = metadata_manifest_object(&payload.namespace_id, &payload.manifest_no);
    let (envelope, bytes) = encode_namespace_manifest_json(payload)
        .expect("fixture envelope")
        .into_parts();
    store
        .put_if_absent(&key, Bytes::from(bytes))
        .await
        .map_err(|error| CoreError::store(&key, &error))?;
    Ok(envelope)
}

/// Returns the requests for the namespace's hint and manifests in
/// `operations`, as a request kind and a key, with each run of HEAD requests
/// sorted by key (see `sort_head_runs`).
pub(crate) fn control_requests(
    namespace_id: &NamespaceId,
    operations: &[loonfs_test_support::stores::RecordedOperation],
) -> Vec<(&'static str, String)> {
    use loonfs_test_support::stores::RecordedOperation;
    let manifests = metadata_manifest_prefix(namespace_id);
    let hint = hint(namespace_id);
    let mut operations = operations
        .iter()
        .filter(|operation| operation.key() == hint || operation.key().starts_with(&manifests))
        .cloned()
        .collect::<Vec<_>>();
    loonfs_test_support::stores::sort_head_runs(&mut operations);
    operations
        .iter()
        .map(|operation| {
            let kind = match operation {
                RecordedOperation::Head { .. } => "head",
                RecordedOperation::Get { .. } | RecordedOperation::GetWithMetadata { .. } => "get",
                RecordedOperation::Put {
                    mode: PutMode::CompareAndSwap { .. },
                    ..
                } => "cas",
                RecordedOperation::Put { .. } | RecordedOperation::PutStreamed { .. } => "put",
                RecordedOperation::Assemble { .. } => "assemble",
                RecordedOperation::Delete { .. } => "delete",
                RecordedOperation::List { .. } => "list",
            };
            (kind, operation.key().to_owned())
        })
        .collect()
}

/// Publishes `manifest` with one attempt of the publication loop, over the
/// current manifest that it loads first.
async fn publish_once<S: ObjectStore + ?Sized>(
    store: &S,
    manifest: loonfs_types::format::envelope::EncodedEnvelope<NamespaceManifestPayload>,
) -> crate::error::Result<ManifestPublicationOutcome> {
    let deadline = OperationClock::start(0, Arc::new(StdMonotonicTimer::default()));
    publish_once_within(store, manifest, &deadline).await
}

/// The same, under the caller's deadline.
async fn publish_once_within<S: ObjectStore + ?Sized>(
    store: &S,
    manifest: loonfs_types::format::envelope::EncodedEnvelope<NamespaceManifestPayload>,
    deadline: &OperationClock,
) -> crate::error::Result<ManifestPublicationOutcome> {
    let namespace_id = manifest.envelope().payload().namespace_id.clone();
    let checked = deadline.observe();
    let current = crate::test_support::namespace_store(store, &namespace_id)
        .load_current_manifest()
        .await?;
    let basis = crate::namespace::PublicationBasis {
        manifest: current,
        checked,
    };
    crate::test_support::namespace_store(store, &manifest.envelope().payload().namespace_id)
        .publish_manifest_from(manifest, deadline, Some(&basis))
        .await
}

pub(super) async fn publish_manifest_with_segments<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_no: ManifestNo,
    head_seq: ChangeSeq,
    segments: Vec<MetadataSegmentRef>,
) -> ManifestNo {
    let manifest_number = manifest_no;
    let manifest = encode_namespace_manifest_json(NamespaceManifestPayload {
        activity: Default::default(),
        created_at_ms: 1_000,
        created_by: loonfs_test_support::test_actor(),
        access: loonfs_types::NamespaceAccess::Unrestricted {},
        retention_window_ms: None,
        fork_basis: None,
        status: loonfs_types::format::control::NamespaceStatus::Active {},
        writer: None,
        folded_wal_no: loonfs_types::WalNo(0),
        compactor_epoch: loonfs_types::CompactorEpoch(0),
        namespace_id: namespace_id.clone(),
        manifest_no,

        head_seq,

        writer_epoch: loonfs_types::WriterEpoch(1),
        next_run_no: RunNo(1),
        retention_floor_seq: ChangeSeq(0),
        acknowledged_floor_seq: ChangeSeq(0),
        runs: vec![MetadataRunRef {
            run_no: RunNo(0),
            run_seq: head_seq,
            tier: RunTier::Base,
            segments,
        }],
    })
    .expect("manifest envelope")
    .into_envelope();
    write_namespace_manifest(store, manifest.payload().clone())
        .await
        .expect("write manifest");
    manifest_number
}
