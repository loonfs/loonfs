//! Request accounting for metadata and WAL prefetch behavior.
//!
//! Run with:
//!   cargo test -p loonfs --test it request_accounting -- --ignored --nocapture

use loonfs::{
    CreateNamespaceOptions, ErrorCode, FsAdmin, FsReader, FsWriter, MaintenancePlan,
    MetadataMaintenanceOptions, NamespaceId, PageRequest, PaginationPolicy, PutFileOptions,
    RuntimeCacheConfig, SharedObjectStore,
};
use loonfs_api::AbsolutePath;

use loonfs_api::wire::manifest::{
    decode_namespace_manifest_json, MetadataRowFamily, NamespaceManifestEnvelope, RunTier,
};
use loonfs_objectstore::keys::{
    metadata_manifest_object, metadata_segment_object_key, wal_segment_prefix,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{
    BlockingStore, ConcurrencyWatchStore, FailStore, InjectedError, KeyPredicate, OperationClass,
    RecordedGet, RecordingStore,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tempfile::tempdir;

const FILES: usize = 10_000;
const BATCH: usize = 100;
const STEP_EVERY_BATCHES: usize = 33;

/// (family, tier, index_offset, filter_offset) per segment object key.
type SegmentMap = BTreeMap<String, (String, RunTier, u64, u64)>;

async fn segment_map(store: &SharedObjectStore, namespace_id: &NamespaceId) -> SegmentMap {
    let root = loonfs_core::control::load_namespace_metadata_root_control(store, namespace_id)
        .await
        .expect("load metadata root");
    let manifest_key =
        metadata_manifest_object(namespace_id, &root.state.manifest.manifest_object_id);
    let bytes = store
        .get(&manifest_key, None)
        .await
        .expect("read manifest")
        .expect("manifest exists");
    let manifest = decode_namespace_manifest_json(&bytes).expect("decode manifest");
    manifest
        .payload
        .runs
        .iter()
        .flat_map(|run| {
            run.segments.iter().map(|descriptor| {
                (
                    metadata_segment_object_key(descriptor),
                    (
                        format!("{:?}", descriptor.family),
                        run.tier,
                        descriptor.index_block.offset,
                        descriptor.filter_block.offset,
                    ),
                )
            })
        })
        .collect()
}

#[allow(clippy::print_stdout)]
fn report(phase: &str, gets: &[RecordedGet], segments: &SegmentMap) {
    let mut by_class: BTreeMap<String, usize> = BTreeMap::new();
    let mut by_segment: BTreeMap<String, usize> = BTreeMap::new();
    for (key, range) in gets {
        let class = if let Some((family, tier, index_offset, filter_offset)) = segments.get(key) {
            let section = match range {
                Some((start, _)) if start == index_offset => "index",
                Some((start, _)) if start == filter_offset => "filter",
                Some(_) => "data",
                None => "whole",
            };
            by_segment
                .entry(format!("{tier:?} {family} {section}"))
                .and_modify(|count| *count += 1)
                .or_insert(1);
            format!("segment:{section}")
        } else if key.contains("/wal/segments/") {
            "wal".to_owned()
        } else if key.contains("/manifests/") {
            "manifest".to_owned()
        } else if key.contains("content-stores/") {
            "content".to_owned()
        } else {
            "control".to_owned()
        };
        by_class
            .entry(class)
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }
    println!("== {phase}: {} GETs", gets.len());
    for (class, count) in &by_class {
        println!("   {class:<16} {count}");
    }
    for (bucket, count) in &by_segment {
        println!("     {bucket:<44} {count}");
    }
}

/// Publishes already-classified candidates through the publication
/// service. Every candidate is admitted before the publisher's worker can
/// take any of them, so they coalesce into one publication.
async fn publish_candidates(
    writer: &FsWriter,
    namespace_id: &NamespaceId,
    candidates: Vec<loonfs::publish::CommitCandidate>,
) {
    let publisher = writer.publisher();
    let submissions = candidates
        .into_iter()
        .map(|candidate| publisher.submit_candidate(namespace_id.clone(), candidate));
    for outcome in futures::future::join_all(submissions).await {
        outcome.expect("publish batch member");
    }
}

async fn build_folded_namespace(
    root: &std::path::Path,
    namespace_id: &NamespaceId,
) -> NamespaceManifestEnvelope {
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(root).expect("create local-fs store"));
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("open-prefetch-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("open-prefetch-admin")
        .build()
        .await
        .expect("build admin");
    writer
        .create_namespace(namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    writer
        .put_file_bytes(
            namespace_id,
            "/docs/file.txt",
            b"file",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("put file");
    admin.flush_wal(namespace_id).await.expect("flush WAL");

    let root = loonfs_core::control::load_namespace_metadata_root_control(&store, namespace_id)
        .await
        .expect("load metadata root");
    let manifest_key =
        metadata_manifest_object(namespace_id, &root.state.manifest.manifest_object_id);
    let manifest_bytes = store
        .get(&manifest_key, None)
        .await
        .expect("read manifest")
        .expect("manifest exists");
    decode_namespace_manifest_json(&manifest_bytes).expect("decode manifest")
}

fn manifest_segment_keys(manifest: &NamespaceManifestEnvelope) -> BTreeSet<String> {
    manifest
        .payload
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(metadata_segment_object_key)
        .collect()
}

#[tokio::test]
async fn open_prefetches_segment_tails_in_one_wave_and_zero_disables_it() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("open-prefetch").expect("valid namespace id");
    let manifest = build_folded_namespace(temp_dir.path(), &namespace_id).await;
    let families = manifest
        .payload
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(|descriptor| descriptor.family)
        .collect::<BTreeSet<_>>();
    for family in [
        MetadataRowFamily::Inodes,
        MetadataRowFamily::DirentryBinds,
        MetadataRowFamily::Revisions,
    ] {
        assert!(families.contains(&family));
    }
    let segment_keys = manifest_segment_keys(&manifest);
    assert!(segment_keys.len() > 1);

    let recording = RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("open local-fs store"),
        KeyPredicate::any(),
    );
    let watched = Arc::new(ConcurrencyWatchStore::new(
        recording,
        KeyPredicate::metadata_segment(),
    ));
    let store: SharedObjectStore = watched.clone();
    let reader = FsReader::builder_with_store(store)
        .build()
        .await
        .expect("build reader");
    let error = reader
        .get_path_entry(&namespace_id, "relative", Default::default())
        .await
        .expect_err("relative path is invalid");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    let open_gets = watched.inner().take_gets();
    let open_segment_gets = open_gets
        .iter()
        .filter(|(key, _)| segment_keys.contains(key))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(open_segment_gets.len(), segment_keys.len());
    assert_eq!(
        open_segment_gets
            .iter()
            .map(|(key, _)| key)
            .collect::<BTreeSet<_>>(),
        segment_keys.iter().collect::<BTreeSet<_>>()
    );
    assert!(watched.reads().peak_in_flight > 1);

    reader
        .get_path_entry(&namespace_id, "/docs/file.txt", Default::default())
        .await
        .expect("stat file");
    let stat_segment_gets = watched
        .inner()
        .take_gets()
        .into_iter()
        .filter(|(key, _)| segment_keys.contains(key))
        .collect::<Vec<_>>();
    assert!(stat_segment_gets.len() <= 2);
    for tail_get in open_segment_gets {
        assert!(!stat_segment_gets.contains(&tail_get));
    }

    let recording = RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("open local-fs store"),
        KeyPredicate::any(),
    );
    let recording = Arc::new(recording);
    let store: SharedObjectStore = recording.clone();
    let mut runtime_cache = RuntimeCacheConfig::default();
    runtime_cache
        .metadata_segment_cache
        .open_prefetch_max_stored_bytes = 0;
    let reader = FsReader::builder_with_store(store)
        .runtime_cache(runtime_cache)
        .build()
        .await
        .expect("build reader");
    let error = reader
        .get_path_entry(&namespace_id, "relative", Default::default())
        .await
        .expect_err("relative path is invalid");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert!(recording
        .take_gets()
        .iter()
        .all(|(key, _)| !segment_keys.contains(key)));

    reader
        .get_path_entry(&namespace_id, "/docs/file.txt", Default::default())
        .await
        .expect("stat file");
    assert!(recording
        .take_gets()
        .iter()
        .any(|(key, _)| segment_keys.contains(key)));
}

#[tokio::test]
async fn segment_tail_prefetch_failures_do_not_fail_open() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("prefetch-failure").expect("valid namespace id");
    let manifest = build_folded_namespace(temp_dir.path(), &namespace_id).await;
    let segment_count = manifest_segment_keys(&manifest).len();
    let failing = Arc::new(FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("open local-fs store"),
        KeyPredicate::metadata_segment(),
        OperationClass::Get,
        InjectedError::Transport("segment read failed".to_owned()),
    ));
    failing.fail_all();
    let store: SharedObjectStore = failing.clone();
    let reader = FsReader::builder_with_store(store)
        .build()
        .await
        .expect("build reader");

    let error = reader
        .get_path_entry(&namespace_id, "relative", Default::default())
        .await
        .expect_err("relative path is invalid after open");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert_eq!(failing.attempts(), segment_count);

    let error = reader
        .get_path_entry(&namespace_id, "/docs/file.txt", Default::default())
        .await
        .expect_err("the lookup reports the segment failure");
    assert_eq!(error.code(), ErrorCode::ServerError);
    assert!(failing.attempts() > segment_count);
}

#[tokio::test]
async fn hinted_wal_prefetch_starts_while_the_manifest_is_blocked() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("wal-prefetch").expect("valid namespace id");
    build_folded_namespace(temp_dir.path(), &namespace_id).await;
    let setup_store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("open local-fs store"));
    let writer = FsWriter::builder_with_store(setup_store.clone())
        .writer_id("wal-prefetch-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    for index in 0..3 {
        writer
            .put_file_bytes(
                &namespace_id,
                &format!("/docs/tail-{index}.txt"),
                b"tail",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("put tail file");
    }
    let head = loonfs_core::control::load_namespace_head_control(&setup_store, &namespace_id)
        .await
        .expect("load namespace head");
    assert!(!head.state.recent_segments.is_empty());
    let root =
        loonfs_core::control::load_namespace_metadata_root_control(&setup_store, &namespace_id)
            .await
            .expect("load metadata root");
    let manifest_key =
        metadata_manifest_object(&namespace_id, &root.state.manifest.manifest_object_id);
    drop(writer);
    drop(setup_store);

    let blocking = BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("open local-fs store"),
        KeyPredicate::exact(manifest_key),
        OperationClass::Get,
    );
    let watched = Arc::new(ConcurrencyWatchStore::new(
        blocking,
        KeyPredicate::prefix(wal_segment_prefix(&namespace_id)),
    ));
    let store: SharedObjectStore = watched.clone();
    let reader = FsReader::builder_with_store(store)
        .build()
        .await
        .expect("build reader");
    watched.inner().arm();
    let open = tokio::spawn({
        let namespace_id = namespace_id.clone();
        async move {
            reader
                .get_path_entry(&namespace_id, "relative", Default::default())
                .await
        }
    });
    watched.inner().wait_until_blocked().await;
    assert!(watched.reads().total > 0);
    watched.inner().release();
    let error = open
        .await
        .expect("join open")
        .expect_err("relative path is invalid");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
}

#[allow(clippy::print_stdout)]
#[tokio::test]
#[ignore = "diagnostic: prints warm-phase request accounting"]
async fn warm_phase_request_accounting() {
    let temp_dir = tempdir().expect("tempdir");
    let log = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = log.clone();
    let namespace_id = NamespaceId::parse("acct").expect("valid namespace id");

    // Build phase: bench-like shape — one wide hot directory, maintenance
    // steps a few times so the manifest ends with a seed base plus a few delta
    // runs and a WAL tail, like the 10k benchmark build.
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("acct-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("acct-admin")
        .build()
        .await
        .expect("build admin");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let catalog = loonfs_core::control::load_namespace_catalog_entry(&store, &namespace_id)
        .await
        .expect("load namespace catalog");
    // The bench publishes 100-mutation batches (one WAL segment each) and
    // steps a few times across the build; mirror that shape.
    let mut index = 0usize;
    while index < FILES {
        let mut candidates = Vec::with_capacity(BATCH);
        for _ in 0..BATCH.min(FILES - index) {
            let bytes = format!("body {index}");
            let stored = loonfs_core::content::store_bytes_as_content(
                &store,
                &namespace_id,
                bytes.as_bytes(),
            )
            .await
            .expect("stage content");
            // External tests prepare through the validating producer: one
            // content HEAD and one full GET by design.
            let prepared = loonfs_core::content::prepare_existing_content_ref(
                &store,
                &catalog,
                stored.into_content_ref(),
            )
            .await
            .expect("prepare existing content");
            let content_ref = prepared.content_ref().clone();
            candidates.push(loonfs::publish::CommitCandidate::prepared(
                loonfs::publish::CommitRequest::single(
                    loonfs::CommitId::generate(),
                    loonfs_test_support::test_actor(),
                    None,
                    loonfs::publish::FilesystemOperation::PutFile {
                        path: AbsolutePath::parse(format!("/hot/file-{index:05}.txt"))
                            .expect("path"),
                        content_ref: content_ref.clone(),
                        behavior: loonfs::DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ),
                vec![prepared],
            ));
            index += 1;
        }
        publish_candidates(&writer, &namespace_id, candidates).await;
        if (index / BATCH) % STEP_EVERY_BATCHES == 0 {
            admin
                .run_maintenance(
                    &namespace_id,
                    MaintenancePlan {
                        metadata: Some(MetadataMaintenanceOptions {
                            max_wal_tail_segments: std::num::NonZeroU64::MIN,
                        }),
                        ..MaintenancePlan::default()
                    },
                )
                .await
                .expect("step");
        }
    }
    let segments = segment_map(&store, &namespace_id).await;
    println!(
        "layout: {} segment objects across tiers {:?}",
        segments.len(),
        segments
            .values()
            .map(|(_, tier, _, _)| *tier)
            .collect::<std::collections::BTreeSet<_>>()
    );
    let _ = log.take_gets();

    // Warm phases, each on the same fresh handle like the bench: a full
    // paged list, then stat, read, write.
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader");
    let limit = PaginationPolicy::default()
        .resolve_limit(Some(1_000))
        .expect("valid limit");
    let mut listed = 0usize;
    let mut cursor = None;
    loop {
        let page = reader
            .list_path_entries_page(
                &namespace_id,
                "/hot",
                PageRequest { limit, cursor },
                Default::default(),
            )
            .await
            .expect("list page");
        listed += page.entries.len();
        match page.next_cursor.as_deref() {
            Some(encoded) => {
                cursor = Some(loonfs_api::decode_cursor(encoded).expect("valid directory cursor"));
            }
            None => break,
        }
    }
    assert_eq!(listed, FILES);
    report("warm full list", &log.take_gets(), &segments);

    reader
        .get_path_entry(&namespace_id, "/hot/file-04999.txt", Default::default())
        .await
        .expect("stat");
    report("warm stat", &log.take_gets(), &segments);

    reader
        .get_file_bytes(&namespace_id, "/hot/file-05000.txt")
        .await
        .expect("read");
    report("warm read", &log.take_gets(), &segments);

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("acct-writer-2")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build second writer");
    writer
        .put_file_bytes(
            &namespace_id,
            "/hot/file-05001.txt",
            b"replaced",
            PutFileOptions {
                behavior: loonfs::DestinationBehavior::Replace,
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: None,
                    message: None,
                },
                expected_inode_id: None,
                expected_revision_no: None,
            },
        )
        .await
        .expect("warm write");
    report("warm write", &log.take_gets(), &segments);

    // A second write on the same handle separates per-handle warmup cost
    // from per-write cost.
    writer
        .put_file_bytes(
            &namespace_id,
            "/hot/file-05002.txt",
            b"replaced again",
            PutFileOptions {
                behavior: loonfs::DestinationBehavior::Replace,
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: None,
                    message: None,
                },
                expected_inode_id: None,
                expected_revision_no: None,
            },
        )
        .await
        .expect("second warm write");
    report(
        "warm write (same handle, second)",
        &log.take_gets(),
        &segments,
    );
}
