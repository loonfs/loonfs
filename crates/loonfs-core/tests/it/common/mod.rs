//! Shared fixtures for the crate's integration tests.

#![allow(dead_code)]

use loonfs_core::cache::{HeadStateCache, MetadataSegmentCache};
use loonfs_core::control::load_read_anchor;
use loonfs_core::{MutationContext, NamespaceWriterEngine, RuntimeReadContext};
use loonfs_objectstore::ObjectStore;
use loonfs_types::NamespaceId;
use std::sync::Arc;

pub(crate) use loonfs_core::test_support::mutation_context;

pub(crate) fn namespace_engine<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> NamespaceWriterEngine<&'a S> {
    NamespaceWriterEngine::writer(
        store,
        namespace_id.clone(),
        context.writer_id.clone(),
        std::sync::Arc::new(tokio::sync::Semaphore::new(32)),
    )
}

pub(crate) async fn read_context<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> RuntimeReadContext {
    let anchor = load_read_anchor(
        store,
        namespace_id,
        loonfs_core::test_support::observed_now(),
    )
    .await
    .expect("load read anchor");
    RuntimeReadContext {
        basis: anchor.basis(),
        head: anchor.read_state,
        segment_cache: Arc::new(MetadataSegmentCache::unshared(usize::MAX)),
        head_state: Arc::new(HeadStateCache::unshared(usize::MAX)),
    }
}

pub(crate) mod commit_split_support {
    #![allow(dead_code)]

    use loonfs_core::time::Deadline;
    use loonfs_objectstore::timing::StdMonotonicTimer;

    use super::{namespace_engine, read_context};
    use loonfs_types::{
        AbsolutePath, ChangeSeq, CommitId, ContentRef, DestinationBehavior, NamespaceId,
    };

    use loonfs_core::content::{prepare_existing_content_ref, store_bytes_as_content};

    use loonfs_core::publish::{
        CommitCandidate, CommitIntent, FilesystemOperation, NamespaceCommitEngine,
    };
    use loonfs_core::{CoreError, CreateNamespaceOptions, MutationContext};

    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::ObjectStore;
    use loonfs_test_support::ids::page_limit;
    use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
    use std::collections::HashSet;
    use std::path::Path;

    use std::sync::Arc;

    pub(crate) async fn bootstrap_namespace<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        context: &MutationContext,
    ) -> Result<loonfs_types::NamespaceMetadata, CoreError> {
        bootstrap_namespace_with_options(
            store,
            namespace_id,
            context,
            &CreateNamespaceOptions::default(),
        )
        .await
    }

    pub(crate) async fn bootstrap_namespace_allowing_existing<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        context: &MutationContext,
    ) -> Result<loonfs_types::NamespaceMetadata, CoreError> {
        bootstrap_namespace_with_options(
            store,
            namespace_id,
            context,
            &CreateNamespaceOptions {
                allow_existing: true,
                ..Default::default()
            },
        )
        .await
    }

    pub(crate) async fn bootstrap_namespace_with_options<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        context: &MutationContext,
        options: &CreateNamespaceOptions,
    ) -> Result<loonfs_types::NamespaceMetadata, CoreError> {
        namespace_engine(store, namespace_id, context)
            .bootstrap_namespace(&loonfs_test_support::test_actor(), options)
            .await
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
        publish_namespace_commits_batch(store, namespace_id, vec![candidate], context)
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
        publish_namespace_commits_batch(store, namespace_id, candidates, context).await
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
                        loonfs_core::control::load_namespace_catalog_entry(store, namespace_id)
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

    pub(crate) async fn publish_namespace_commits_batch<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        candidates: Vec<CommitCandidate>,
        context: &MutationContext,
    ) -> Vec<Result<loonfs_types::api::v0::Commit, CoreError>> {
        let mut engine = NamespaceCommitEngine::new(
            namespace_id.clone(),
            std::sync::Arc::new(tokio::sync::Semaphore::new(32)),
        );
        engine
            .publish_batch(
                store,
                candidates,
                context,
                &Deadline::start(Arc::new(StdMonotonicTimer::default())),
            )
            .await
            .results
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
    ) -> Result<loonfs_types::Checkpoint, CoreError> {
        namespace_engine(store, namespace_id, context)
            .create_checkpoint("test-pin".to_owned(), None)
            .await
    }

    pub(crate) fn test_commit_id(commit_id: Option<&str>) -> CommitId {
        commit_id
            .map(|value| CommitId::parse(value).expect("valid test commit id"))
            .unwrap_or_else(CommitId::generate)
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

    pub(crate) async fn put_file<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        behavior: DestinationBehavior,
        context: &MutationContext,
        commit_id: Option<&str>,
    ) -> Result<loonfs_types::Commit, CoreError> {
        let content = store_bytes_as_content(store, namespace_id, bytes).await?;
        submit_operation(
            store,
            namespace_id,
            test_commit_id(commit_id),
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse(absolute_path).expect("path"),
                content_ref: Some(content.into_content_ref()),
                inline_content: None,
                behavior,
                expected_inode_id: None,
                expected_revision_no: None,
            },
            context,
        )
        .await
    }

    pub(crate) async fn write_file_bytes<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        context: &MutationContext,
        commit_id: Option<&str>,
    ) -> Result<loonfs_types::Commit, CoreError> {
        put_file(
            store,
            namespace_id,
            absolute_path,
            bytes,
            DestinationBehavior::Replace,
            context,
            commit_id,
        )
        .await
    }

    pub(crate) async fn create_directory_path<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        context: &MutationContext,
        commit_id: Option<&str>,
    ) -> Result<loonfs_types::Commit, CoreError> {
        submit_operation(
            store,
            namespace_id,
            test_commit_id(commit_id),
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
    ) -> Result<loonfs_types::FileBytes, CoreError> {
        let context = read_context(store, namespace_id).await;
        namespace_engine(store, namespace_id, &mutation_context())
            .read_file(absolute_path, &context, None)
            .await
    }

    pub(crate) fn mutation_context() -> MutationContext {
        super::mutation_context("writer-a", 1_000)
    }

    pub(crate) fn content_ref(seed: &str) -> ContentRef {
        loonfs_test_support::ids::content_ref(seed.as_bytes())
    }

    pub(crate) fn content_blob_counting_store(
        root: impl AsRef<Path>,
    ) -> RecordingStore<LocalFsStore> {
        RecordingStore::new(
            LocalFsStore::new(root.as_ref()).expect("store"),
            KeyPredicate::content_blob(),
        )
    }
}
