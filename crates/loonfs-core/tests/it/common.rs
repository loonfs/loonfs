//! Shared fixtures for the crate's integration tests.

use loonfs_core::cache::{HeadAndWalTailCache, MetadataSegmentCache, ObjectCache};
use loonfs_core::content::prepare_existing_content_ref;
use loonfs_core::publish::{CommitCandidate, CommitIntent, FilesystemOperation};
use loonfs_core::test_support::{namespace_store, NamespaceFixture};
use loonfs_core::{CoreError, MutationContext, NamespaceWriterEngine, RuntimeReadContext};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::ids::page_limit;
use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
use loonfs_types::{AbsolutePath, ChangeSeq, CommitId, ContentRef, NamespaceId};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

/// Returns a writable engine over `namespace_id` that commits as
/// `context.writer_id` and reads the system clock.
pub(crate) fn namespace_engine<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> NamespaceWriterEngine<&'a S> {
    NamespaceWriterEngine::writer(
        namespace_store(store, namespace_id),
        context.writer_id.clone(),
    )
}

pub(crate) async fn read_context<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> RuntimeReadContext {
    let anchor = namespace_store(store, namespace_id)
        .load_read_anchor(loonfs_core::test_support::observed_now())
        .await
        .expect("load read anchor");
    RuntimeReadContext {
        basis: anchor.basis(),
        head: anchor.read_state,
        segment_cache: Arc::new(MetadataSegmentCache::unshared(usize::MAX)),
        head_and_wal_tail_cache: Arc::new(HeadAndWalTailCache::unshared(usize::MAX)),
        object_cache: Arc::new(ObjectCache::unshared(0, 0)),
        tail: None,
    }
}

/// Publishes one mutation request through the commit engine — the single
/// publish pipeline — with its content already prepared.
pub(crate) async fn submit_commit<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request: CommitIntent,
    context: &MutationContext,
) -> Result<loonfs_types::api::v0::Commit, CoreError> {
    let candidate = prepared_candidate(store, namespace_id, request).await;
    NamespaceFixture::new(store, namespace_id, context)
        .publish_candidates(vec![candidate])
        .await
        .into_iter()
        .next()
        .expect("single commit result")
}

/// Publishes several mutation requests as one batch, so their outcomes
/// are decided against the same durable state.
pub(crate) async fn submit_commits_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    requests: Vec<CommitIntent>,
    context: &MutationContext,
) -> Vec<Result<loonfs_types::api::v0::Commit, CoreError>> {
    let mut candidates = Vec::with_capacity(requests.len());
    for request in requests {
        candidates.push(prepared_candidate(store, namespace_id, request).await);
    }
    NamespaceFixture::new(store, namespace_id, context)
        .publish_candidates(candidates)
        .await
}

/// Wraps a request with a preparation proof for each distinct content ref
/// it puts, which is what publication requires of new external content.
///
/// The namespace catalog is loaded only when there is content to prepare,
/// so a request that touches no content leaves the store untouched and
/// tests that count store operations still see what they expect.
pub(crate) async fn prepared_candidate<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request: CommitIntent,
) -> CommitCandidate {
    let mut seen = HashSet::new();
    let mut prepared = Vec::new();
    let mut catalog = None;
    for content_ref in request
        .operations
        .iter()
        .filter_map(FilesystemOperation::content_ref)
    {
        if seen.insert(content_ref.clone()) {
            if catalog.is_none() {
                catalog = Some(
                    namespace_store(store, namespace_id)
                        .load_catalog_entry()
                        .await
                        .expect("load namespace catalog"),
                );
            }
            prepared.push(
                prepare_existing_content_ref(
                    store,
                    catalog
                        .as_ref()
                        .expect("external content should load the namespace catalog"),
                    content_ref.clone(),
                )
                .await
                .expect("prepare existing content"),
            );
        }
    }
    CommitCandidate::prepared(request, prepared)
}

pub(crate) async fn list_changes_after<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    after_seq: ChangeSeq,
) -> Result<loonfs_types::api::v0::ListChangesResponse, CoreError> {
    let context = read_context(store, namespace_id).await;
    namespace_engine(store, namespace_id, &mutation_context())
        .list_changes_after(
            after_seq,
            page_limit(loonfs_types::DEFAULT_PAGE_LIMIT),
            &context,
        )
        .await
}

pub(crate) async fn create_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<loonfs_types::Snapshot, CoreError> {
    namespace_engine(store, namespace_id, context)
        .create_snapshot("test-pin".to_owned(), None)
        .await
        .map(|report| report.response)
}

/// Publishes one operation as a whole request, which is the shape almost
/// every fixture below wants: one operation, one commit id, no message.
pub(crate) async fn submit_operation<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    commit_id: CommitId,
    operation: FilesystemOperation,
    context: &MutationContext,
) -> Result<loonfs_types::Commit, CoreError> {
    submit_commit(
        store,
        namespace_id,
        CommitIntent::single(
            commit_id,
            loonfs_test_support::test_actor(),
            None,
            operation,
        ),
        context,
    )
    .await
}

pub(crate) async fn create_directory_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    commit_id: Option<&CommitId>,
) -> Result<loonfs_types::Commit, CoreError> {
    submit_operation(
        store,
        namespace_id,
        commit_id.cloned().unwrap_or_else(CommitId::generate),
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse(absolute_path).expect("path"),
            parents: false,
        },
        context,
    )
    .await
}

pub(crate) async fn resolve_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<loonfs_types::PathEntry, CoreError> {
    let context = read_context(store, namespace_id).await;
    namespace_engine(store, namespace_id, &mutation_context())
        .resolve_path(
            absolute_path,
            loonfs_types::options::StatOptions::default(),
            &context,
        )
        .await
}

pub(crate) async fn read_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<loonfs_core::FileBytes, CoreError> {
    let context = read_context(store, namespace_id).await;
    namespace_engine(store, namespace_id, &mutation_context())
        .read_file(absolute_path, &context, None)
        .await
}

/// The context of the suites' default writer, `writer-a` at 1,000 ms.
pub(crate) fn mutation_context() -> MutationContext {
    loonfs_core::test_support::mutation_context("writer-a", 1_000)
}

pub(crate) fn content_ref(seed: &str) -> ContentRef {
    loonfs_test_support::ids::content_ref(seed.as_bytes())
}

pub(crate) fn content_blob_counting_store(root: impl AsRef<Path>) -> RecordingStore<LocalFsStore> {
    RecordingStore::new(
        LocalFsStore::new(root.as_ref()).expect("store"),
        KeyPredicate::content_blob(),
    )
}
