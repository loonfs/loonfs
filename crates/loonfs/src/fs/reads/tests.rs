//! A cold directory page over a compacted run overlaps its segment reads.
//!
//! The shipped segment target is 8 MiB of decoded rows, so a compacted run
//! only splits into many segments at a scale no test can write in seconds.
//! These tests narrow the rows per compacted segment through
//! [`Maintenance::narrow_segment_row_budget`]. Everything else — folds,
//! compaction planning, and the page read — is the shipped path.

use crate::publish::{CommitCandidate, CommitRequest, FilesystemOperation, InlineContent};
use crate::{
    CreateNamespaceOptions, DestinationBehavior, LoonFs, NamespaceId, PageRequest,
    SharedObjectStore,
};
use loonfs_api::wire::manifest::MetadataRowFamily;
use loonfs_api::{
    AbsolutePath, AccessGrants, AccessRight, AccessRights, CommitId, ContentId,
    MetadataCompactionOutcome, MonotonicTimer, NamespaceAccess, PrincipalId, PrincipalScope,
    PrincipalSet, Subject, SubjectId,
};
use loonfs_core::test_support::STORE_READ_WAVE;
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::timing::StdMonotonicTimer;
use loonfs_test_support::ids::{namespace_id, page_limit};
use loonfs_test_support::stores::{
    ConcurrencyWatchStore, KeyPredicate, LatencyStore, RecordingStore,
};
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

/// Files in the listed directory; the page reads the first half.
const FILES: usize = 2000;
const PAGE: usize = 1000;
/// Committed and folded batches the compaction merges into one run.
const BATCHES: usize = 8;
/// About eleven compacted segments per family for `FILES` rows.
const ROWS_PER_SEGMENT: usize = 200;

async fn family_keys(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    family: MetadataRowFamily,
) -> (usize, BTreeSet<String>) {
    let manifest = loonfs_core::control::load_namespace_current_manifest(store, namespace_id)
        .await
        .expect("load manifest");
    let runs = &manifest.state.envelope.payload().runs;
    let family_runs = runs
        .iter()
        .filter(|run| run.segments.iter().any(|segment| segment.family == family))
        .count();
    let keys = runs
        .iter()
        .flat_map(|run| &run.segments)
        .filter(|segment| segment.family == family)
        .map(metadata_segment_object_key)
        .collect();
    (family_runs, keys)
}

async fn create_compacted_directory(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    subject: Option<&Subject>,
) {
    let actor = loonfs_test_support::test_actor();
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
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id("compacted-page-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let maintenance = LoonFs::builder_with_store(store.clone())
        .writer_id("compacted-page-maintenance")
        .build()
        .await
        .expect("maintenance")
        .maintenance(loonfs_test_support::ids::writer_id(
            "compacted-page-maintenance",
        ))
        .narrow_segment_row_budget(NonZeroUsize::new(ROWS_PER_SEGMENT).expect("nonzero"));
    writer
        .create_namespace_with_options(
            namespace_id,
            &actor,
            &CreateNamespaceOptions {
                access: match subject {
                    Some(subject) => NamespaceAccess::Acl {
                        principal_scope: subject.principal_scope.clone(),
                        root_grants: grants.clone(),
                    },
                    None => NamespaceAccess::unrestricted(),
                },
                ..Default::default()
            },
        )
        .await
        .expect("namespace");
    let writer = match subject {
        Some(subject) => writer.with_subject(subject.clone()),
        None => writer,
    };
    let namespace = writer.open_namespace(namespace_id).expect("open namespace");
    namespace
        .create_directory("/directory", &actor)
        .await
        .expect("directory");
    let per_batch = FILES / BATCHES;
    for batch in 0..BATCHES {
        let content = InlineContent::new(
            namespace_id.clone(),
            ContentId::generate(),
            bytes::Bytes::new(),
        );
        let content_ref = content.content_ref();
        // Permuted names spread the first page across every batch, so the
        // page's inodes span the whole compacted run.
        let operations = (batch * per_batch..(batch + 1) * per_batch)
            .flat_map(|index| {
                let path =
                    AbsolutePath::parse(format!("/directory/file-{:04}", (index * 37) % FILES))
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
            .collect();
        namespace
            .commit_candidate(CommitCandidate::with_inline_content(
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
            ))
            .await
            .expect("file batch");
        maintenance
            .fold_wal(namespace_id)
            .await
            .expect("fold file batch");
    }
    loop {
        let response = maintenance
            .compact_metadata(namespace_id)
            .await
            .expect("compact metadata");
        if response.compaction == MetadataCompactionOutcome::NotNeeded {
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
    let subject = Subject {
        principal_scope: PrincipalScope::parse("directory-page").expect("scope"),
        subject_id: SubjectId::parse("viewer").expect("subject"),
        principals: PrincipalSet::new(BTreeSet::from([
            PrincipalId::parse("viewer").expect("principal")
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
    let temporary = tempdir().expect("temporary directory");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(temporary.path()).expect("store"));
    let namespace_id = namespace_id("compacted-directory-page");
    create_compacted_directory(&store, &namespace_id, subject.as_ref()).await;
    let (family_runs, segment_keys) = family_keys(&store, &namespace_id, family).await;
    assert_eq!(family_runs, 1);
    assert!(segment_keys.len() >= 8, "{} segments", segment_keys.len());
    let keys = KeyPredicate::new(move |key| segment_keys.contains(key));
    let delayed = Arc::new(LatencyStore::new(store, keys.clone(), latency));
    let reads = Arc::new(ConcurrencyWatchStore::new(delayed.clone(), keys.clone()));
    let recording = Arc::new(RecordingStore::new(reads.clone(), keys));
    let reader = LoonFs::builder_with_store(recording.clone())
        .read_only()
        .build()
        .await
        .expect("reader");
    let reader = match subject {
        Some(subject) => reader.with_subject(subject),
        None => reader,
    };
    let namespace = reader.namespace(&namespace_id);
    let timer = StdMonotonicTimer::default();
    let started_at_ms = timer.monotonic_now_ms();
    let page = namespace
        .list("/directory")
        .page(PageRequest {
            limit: page_limit(PAGE),
            cursor: None,
        })
        .await
        .expect("page");
    let elapsed_ms = timer.monotonic_now_ms() - started_at_ms;
    assert_eq!(page.entries.len(), PAGE);
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
