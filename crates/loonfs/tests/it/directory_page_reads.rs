//! Store reads and entry metadata for a directory page across folded batches.

use loonfs::{
    CreateDirectoryOptions, CreateNamespaceOptions, DestinationBehavior, FsMaintenance, FsReader,
    FsWriter, PageRequest, PutFileOptions, SharedObjectStore, StatPathOptions,
};
use loonfs_api::wire::manifest::MetadataRowFamily;
use loonfs_api::{AccessGrants, AccessRight, AccessRights, NamespaceAccess, Subject};
use loonfs_core::test_support::STORE_READ_WAVE;
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{namespace_id, page_limit};
use loonfs_test_support::stores::{ConcurrencyWatchStore, KeyPredicate, RecordingStore};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

async fn family_keys(
    store: &SharedObjectStore,
    namespace_id: &loonfs::NamespaceId,
    family: MetadataRowFamily,
) -> BTreeSet<String> {
    loonfs_core::control::load_namespace_current_manifest(store, namespace_id)
        .await
        .expect("load manifest")
        .state
        .envelope
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .filter(|segment| segment.family == family)
        .map(metadata_segment_object_key)
        .collect()
}

#[tokio::test]
async fn directory_pages_use_bindings_and_load_revision_heads_concurrently() {
    let temporary = tempdir().expect("temporary directory");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temporary.path()).expect("local store"));
    let namespace_id = namespace_id("directory-page-reads");
    let actor = loonfs_test_support::test_actor();
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("directory-page-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let maintenance = FsMaintenance::builder_with_store(store.clone())
        .actor_id("directory-page-maintenance")
        .build()
        .await
        .expect("maintenance");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::new(actor.clone()))
        .await
        .expect("namespace");
    writer
        .create_directory(
            &namespace_id,
            "/files",
            CreateDirectoryOptions::new(actor.clone()),
        )
        .await
        .expect("directory");
    maintenance
        .flush_wal(&namespace_id)
        .await
        .expect("fold parents");
    let parent_inode_keys = family_keys(&store, &namespace_id, MetadataRowFamily::Inodes).await;
    for batch in 0..6 {
        for index in batch * 50..(batch + 1) * 50 {
            writer
                .put_file_bytes(
                    &namespace_id,
                    &format!("/files/{index:03}.txt"),
                    &vec![b'a'; index + 1],
                    PutFileOptions::new(actor.clone()),
                )
                .await
                .expect("create file");
        }
        maintenance
            .flush_wal(&namespace_id)
            .await
            .expect("fold batch");
    }
    for index in (0..300).step_by(3) {
        writer
            .put_file_bytes(
                &namespace_id,
                &format!("/files/{index:03}.txt"),
                &vec![b'b'; index + 2],
                PutFileOptions {
                    behavior: DestinationBehavior::Replace,
                    ..PutFileOptions::new(loonfs_api::ActorId::parse("editor").expect("actor"))
                },
            )
            .await
            .expect("replace file");
    }
    maintenance
        .flush_wal(&namespace_id)
        .await
        .expect("fold replacements");
    let child_inode_keys: BTreeSet<_> =
        family_keys(&store, &namespace_id, MetadataRowFamily::Inodes)
            .await
            .difference(&parent_inode_keys)
            .cloned()
            .collect();
    assert!(child_inode_keys.len() >= 6);
    let revision_keys = family_keys(&store, &namespace_id, MetadataRowFamily::Revisions).await;
    assert!(revision_keys.len() >= 6);
    let revisions = Arc::new(ConcurrencyWatchStore::new(
        store,
        KeyPredicate::new(move |key| revision_keys.contains(key)),
    ));
    let recording = Arc::new(RecordingStore::metadata_segments(revisions.clone()));
    let reader = FsReader::builder_with_store(recording.clone())
        .build()
        .await
        .expect("fresh reader");
    let page = reader
        .list_path_entries_page(
            &namespace_id,
            "/files",
            PageRequest {
                limit: page_limit(300),
                cursor: None,
            },
            Default::default(),
        )
        .await
        .expect("directory page");
    assert_eq!(page.entries.len(), 300);
    assert!(page.next_cursor.is_none());
    for key in recording.take_get_keys() {
        assert!(
            !child_inode_keys.contains(&key),
            "read child inode object {key}"
        );
    }
    let concurrency = revisions.reads();
    assert!(concurrency.peak_in_flight > 1, "{concurrency:?}");
    assert!(
        concurrency.peak_in_flight <= STORE_READ_WAVE,
        "{concurrency:?}"
    );
    for entry in page.entries {
        let stat = reader
            .get_path_entry(
                &namespace_id,
                entry.path.as_str(),
                StatPathOptions::default(),
            )
            .await
            .expect("stat listed file");
        assert_eq!(entry.created_by, actor);
        assert_eq!(entry.created_by, stat.created_by);
        assert_eq!(entry.created_at_ms, stat.created_at_ms);
        assert_eq!(entry.kind, stat.kind);
    }
}

async fn create_compacted_directories(
    store: &SharedObjectStore,
    namespace_id: &loonfs::NamespaceId,
    subject: Option<&Subject>,
) {
    use loonfs::publish::{CommitCandidate, CommitRequest, FilesystemOperation, InlineContent};
    use loonfs_api::{AbsolutePath, ActorId, CommitId, MetadataCompactionRequest};

    // Long attribution produces enough base segments without a million files.
    let actor = ActorId::parse("a".repeat(256)).expect("actor");
    let grants = AccessGrants::new(
        subject
            .into_iter()
            .flat_map(|subject| subject.principals.iter())
            .map(|principal| {
                (
                    principal.clone(),
                    AccessRights::from_iter([
                        AccessRight::Read,
                        AccessRight::Create,
                        AccessRight::Manage,
                    ]),
                )
            })
            .collect(),
    )
    .expect("grants");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("compacted-page-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let maintenance = FsMaintenance::builder_with_store(store.clone())
        .actor_id("compacted-page-maintenance")
        .build()
        .await
        .expect("maintenance");
    writer
        .create_namespace(
            namespace_id,
            CreateNamespaceOptions {
                access: match subject {
                    Some(subject) => NamespaceAccess::Acl {
                        principal_scope: subject.principal_scope.clone(),
                        root_grants: grants.clone(),
                    },
                    None => NamespaceAccess::unrestricted(),
                },
                ..CreateNamespaceOptions::new(actor.clone())
            },
        )
        .await
        .expect("namespace");
    let writer = match subject {
        Some(subject) => writer.as_subject(subject.clone()),
        None => writer,
    };
    for directory in 0..64 {
        writer
            .create_directory(
                namespace_id,
                &format!("/directory-{directory:02}"),
                CreateDirectoryOptions::new(actor.clone()),
            )
            .await
            .expect("directory");
    }
    let rounds_per_fold = if subject.is_some() { 25 } else { 50 };
    for fold in 0..2000 / rounds_per_fold {
        let content = InlineContent::new(
            namespace_id.clone(),
            loonfs_api::ContentId::generate(),
            bytes::Bytes::new(),
        );
        let content_ref = content.content_ref();
        let grants = &grants;
        // Permuted names spread the first page across all folds.
        let operations = (fold * rounds_per_fold..(fold + 1) * rounds_per_fold)
            .flat_map(|round| {
                (0..64).flat_map(move |directory| {
                    let path = AbsolutePath::parse(format!(
                        "/directory-{directory:02}/file-{:04}",
                        (round * 37) % 2000
                    ))
                    .expect("file path");
                    let put = FilesystemOperation::PutFile {
                        path: path.clone(),
                        content_ref: Some(content_ref.clone()),
                        inline_content: None,
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    };
                    let access = subject.map(|_| FilesystemOperation::UpdateAccess {
                        path,
                        boundary: false,
                        grants: grants.clone(),
                        expected_inode_id: None,
                        expected_access_revision_no: None,
                    });
                    std::iter::once(put).chain(access)
                })
            })
            .collect();
        writer
            .commit_candidate(
                namespace_id,
                CommitCandidate::with_inline_content(
                    CommitRequest {
                        commit_id: CommitId::generate(),
                        actor_id: actor.clone(),
                        subject: None,
                        message: None,
                        operations,
                        preconditions: Vec::new(),
                    },
                    Vec::new(),
                    vec![content],
                ),
            )
            .await
            .expect("file batch");
        maintenance
            .flush_wal(namespace_id)
            .await
            .expect("fold file batch");
    }
    loop {
        let response = maintenance
            .run_maintenance(
                namespace_id,
                loonfs_api::RunMaintenanceRequest::MetadataCompaction(MetadataCompactionRequest {}),
            )
            .await
            .expect("compact metadata");
        if matches!(
            response,
            loonfs_api::RunMaintenanceResponse::MetadataCompaction(
                loonfs_api::MetadataCompactionResponse {
                    compaction: loonfs_api::MetadataCompactionOutcome::NotNeeded,
                    ..
                }
            )
        ) {
            break;
        }
    }
}

#[tokio::test]
async fn compacted_directory_page_overlaps_revision_segment_reads() {
    compacted_directory_page_overlaps_segment_reads(
        MetadataRowFamily::Revisions,
        None,
        Duration::from_millis(500),
    )
    .await;
}

#[tokio::test]
async fn compacted_directory_page_overlaps_access_segment_reads() {
    use loonfs_api::{PrincipalId, PrincipalScope, PrincipalSet, SubjectId};

    let subject = Subject {
        principal_scope: PrincipalScope::parse("directory-page").expect("scope"),
        subject_id: SubjectId::parse("viewer").expect("subject"),
        principals: PrincipalSet::new(BTreeSet::from([
            PrincipalId::parse("v".repeat(256)).expect("principal")
        ]))
        .expect("principals"),
    };
    // Authorization and revision reads must fit below the serialized access-read budget.
    compacted_directory_page_overlaps_segment_reads(
        MetadataRowFamily::Access,
        Some(subject),
        Duration::from_secs(2),
    )
    .await;
}

async fn compacted_directory_page_overlaps_segment_reads(
    family: MetadataRowFamily,
    subject: Option<Subject>,
    latency: Duration,
) {
    use loonfs_api::MonotonicTimer;
    use loonfs_objectstore::timing::StdMonotonicTimer;
    use loonfs_test_support::stores::LatencyStore;

    let temporary = tempdir().expect("temporary directory");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(temporary.path()).expect("store"));
    let namespace_id = namespace_id("compacted-directory-page");
    create_compacted_directories(&store, &namespace_id, subject.as_ref()).await;
    let manifest = loonfs_core::control::load_namespace_current_manifest(&store, &namespace_id)
        .await
        .expect("manifest");
    let runs: Vec<_> = manifest
        .state
        .envelope
        .payload()
        .runs
        .iter()
        .filter(|run| run.segments.iter().any(|segment| segment.family == family))
        .collect();
    assert_eq!(runs.len(), 1);
    let segment_keys = family_keys(&store, &namespace_id, family).await;
    assert!(segment_keys.len() >= 8, "{} segments", segment_keys.len());
    let keys = KeyPredicate::new(move |key| segment_keys.contains(key));
    let delayed = Arc::new(LatencyStore::new(store, keys.clone(), latency));
    let reads = Arc::new(ConcurrencyWatchStore::new(delayed.clone(), keys.clone()));
    let recording = Arc::new(RecordingStore::new(reads.clone(), keys));
    let reader = FsReader::builder_with_store(recording.clone())
        .build()
        .await
        .expect("reader");
    let reader = match subject {
        Some(subject) => reader.as_subject(subject),
        None => reader,
    };
    let timer = StdMonotonicTimer::default();
    let started_at_ms = timer.monotonic_now_ms();
    let page = reader
        .list_path_entries_page(
            &namespace_id,
            "/directory-00",
            PageRequest {
                limit: page_limit(1000),
                cursor: None,
            },
            Default::default(),
        )
        .await
        .expect("page");
    let elapsed_ms = timer.monotonic_now_ms() - started_at_ms;
    assert_eq!(page.entries.len(), 1000);
    assert!(page.next_cursor.is_some());
    let touched = recording
        .take_get_keys()
        .into_iter()
        .collect::<BTreeSet<_>>()
        .len();
    let concurrency = reads.reads();
    let starts_ms = delayed.read_starts_ms();
    assert!(
        concurrency.peak_in_flight >= 8,
        "{concurrency:?}; starts_ms={starts_ms:?}"
    );
    assert!(
        concurrency.peak_in_flight <= STORE_READ_WAVE,
        "{concurrency:?}"
    );
    let serial_ms = touched as u128 * latency.as_millis();
    assert!(
        u128::from(elapsed_ms) < serial_ms * 3 / 4,
        "{elapsed_ms} ms for {touched} segments; starts_ms={starts_ms:?}"
    );
}
