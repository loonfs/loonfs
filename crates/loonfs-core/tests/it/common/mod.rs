//! Shared fixtures for the crate's integration tests.

#![allow(dead_code)]

use loonfs_api::NamespaceId;
use loonfs_core::cache::{
    MetadataSegmentCache, MetadataSegmentCacheConfig, WalTailProjectionCache,
    WalTailProjectionCacheConfig, DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES,
    DEFAULT_WAL_TAIL_PROJECTION_ROWS,
};
use loonfs_core::control::load_namespace_read_anchor;
use loonfs_core::{MutationContext, NamespaceWriterEngine, RuntimeReadContext};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;

pub(crate) fn mutation_context(writer_id: &str, now_ms: u64) -> MutationContext {
    MutationContext {
        writer_id: writer_id.to_owned(),
        now_ms,
    }
}

pub(crate) fn namespace_engine<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> NamespaceWriterEngine<&'a S> {
    NamespaceWriterEngine::writer(store, namespace_id.clone(), context.writer_id.clone())
        .expect("build namespace engine")
}

pub(crate) async fn read_context<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> RuntimeReadContext {
    let (head, basis) = load_namespace_read_anchor(store, namespace_id)
        .await
        .expect("load read anchor");
    RuntimeReadContext {
        head: head.state,
        head_etag: head.identity.etag,
        basis,
        segment_cache: Arc::new(MetadataSegmentCache::new(
            MetadataSegmentCacheConfig::default(),
        )),
        tail_cache: Arc::new(WalTailProjectionCache::new(WalTailProjectionCacheConfig {
            max_entries: 4,
            max_rows: DEFAULT_WAL_TAIL_PROJECTION_ROWS,
            max_decoded_bytes: DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES,
        })),
    }
}

pub(crate) mod commit_split_support {
    #![allow(dead_code)]

    use super::{namespace_engine, read_context};
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use loonfs_api::{
        AbsolutePath, ChangeSeq, CommitId, ContentRef, DestinationBehavior, NamespaceId,
    };

    use loonfs_core::content::{prepare_existing_content_ref, store_bytes_as_content};

    use loonfs_core::publish::{
        CommitCandidate, CommitRequest, FilesystemOperation, NamespaceCommitEngine,
        PublishTailOptions,
    };
    use loonfs_core::{BootstrapOptions, Error as CoreError, MutationContext};

    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::{
        ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
    };
    use loonfs_test_support::ids::page_limit;
    use loonfs_test_support::stores::{CountingStore, KeyPredicate};
    use std::collections::HashSet;
    use std::path::Path;

    use std::sync::Mutex;

    pub(crate) async fn bootstrap_namespace<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        context: &MutationContext,
        allow_existing: bool,
    ) -> Result<loonfs_api::Namespace, loonfs_core::BootstrapNamespaceError> {
        namespace_engine(store, namespace_id, context)
            .bootstrap_namespace(BootstrapOptions { allow_existing })
            .await
    }

    /// Publishes one mutation request through the commit engine — the single
    /// publish pipeline — with its content already prepared.
    pub(crate) async fn submit_commit<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        request: CommitRequest,
        context: &MutationContext,
    ) -> Result<loonfs_api::v0::CommitResponse, CoreError> {
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
        requests: Vec<CommitRequest>,
        context: &MutationContext,
    ) -> Vec<Result<loonfs_api::v0::CommitResponse, CoreError>> {
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
        request: CommitRequest,
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
    ) -> Vec<Result<loonfs_api::v0::CommitResponse, CoreError>> {
        let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
        engine
            .publish_batch(store, candidates, context, &PublishTailOptions::default())
            .await
            .results
    }

    pub(crate) async fn list_changes_after<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
    ) -> Result<loonfs_api::v0::ListChangesResponse, CoreError> {
        namespace_engine(store, namespace_id, &mutation_context())
            .list_changes_after(after_seq, page_limit(loonfs_api::DEFAULT_PAGE_LIMIT))
            .await
    }

    pub(crate) async fn create_checkpoint<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        context: &MutationContext,
    ) -> Result<loonfs_api::Checkpoint, CoreError> {
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
    ) -> Result<loonfs_api::CommitResponse, CoreError> {
        submit_commit(
            store,
            namespace_id,
            CommitRequest::single(
                commit_id,
                loonfs_test_support::test_actor(),
                None,
                operation,
            ),
            context,
        )
        .await
    }

    pub(crate) async fn put_file_bytes<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        behavior: DestinationBehavior,
        context: &MutationContext,
        commit_id: Option<&str>,
    ) -> Result<loonfs_api::CommitResponse, CoreError> {
        let content = store_bytes_as_content(store, namespace_id, bytes).await?;
        submit_operation(
            store,
            namespace_id,
            test_commit_id(commit_id),
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse(absolute_path).expect("path"),
                content_ref: content.into_content_ref(),
                behavior,
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
    ) -> Result<loonfs_api::CommitResponse, CoreError> {
        put_file_bytes(
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
    ) -> Result<loonfs_api::CommitResponse, CoreError> {
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
    ) -> Result<loonfs_api::PathEntry, CoreError> {
        let context = read_context(store, namespace_id).await;
        namespace_engine(store, namespace_id, &mutation_context())
            .resolve_path(
                absolute_path,
                loonfs_api::options::StatPathOptions::default(),
                &context,
            )
            .await
    }

    pub(crate) async fn read_file_bytes<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<loonfs_api::FileBytes, CoreError> {
        let context = read_context(store, namespace_id).await;
        namespace_engine(store, namespace_id, &mutation_context())
            .get_file(absolute_path, &context, None)
            .await
    }

    #[derive(Debug)]
    pub(crate) struct InjectCreateFailureStore {
        pub(crate) inner: LocalFsStore,
        pub(crate) matcher: KeyMatcher,
        pub(crate) failure: InjectedCreateFailure,
        pub(crate) injected: Mutex<bool>,
    }

    impl InjectCreateFailureStore {
        pub(crate) fn new(
            inner: LocalFsStore,
            matcher: KeyMatcher,
            failure: InjectedCreateFailure,
        ) -> Self {
            Self {
                inner,
                matcher,
                failure,
                injected: Mutex::new(false),
            }
        }
    }

    #[derive(Debug)]
    pub(crate) enum KeyMatcher {
        Exact(String),
        Prefix(String),
    }

    impl KeyMatcher {
        pub(crate) fn matches(&self, key: &str) -> bool {
            match self {
                Self::Exact(expected) => key == expected,
                Self::Prefix(prefix) => key.starts_with(prefix),
            }
        }
    }

    #[derive(Debug)]
    pub(crate) enum InjectedCreateFailure {
        Transport {
            message: &'static str,
        },
        PreconditionFailed {
            write_attempted_object: bool,
            additional_writes: Vec<(String, Vec<u8>)>,
        },
    }

    impl InjectedCreateFailure {
        pub(crate) async fn apply_before_error(
            &self,
            inner: &LocalFsStore,
            attempted_key: &str,
            attempted_bytes: Bytes,
        ) -> Result<(), ObjectStoreError> {
            match self {
                Self::Transport { .. } => Ok(()),
                Self::PreconditionFailed {
                    write_attempted_object,
                    additional_writes,
                } => {
                    if *write_attempted_object {
                        inner
                            .put_overwrite(attempted_key, attempted_bytes.clone())
                            .await?;
                    }
                    for (key, bytes) in additional_writes {
                        inner
                            .put_overwrite(key, Bytes::copy_from_slice(bytes))
                            .await?;
                    }
                    Ok(())
                }
            }
        }

        pub(crate) fn error(&self, attempted_key: &str) -> ObjectStoreError {
            match self {
                Self::Transport { message } => {
                    ObjectStoreError::transport(attempted_key, (*message).to_owned())
                }
                Self::PreconditionFailed { .. } => ObjectStoreError::PreconditionFailed {
                    object_key: attempted_key.to_owned(),
                },
            }
        }
    }

    #[async_trait]
    impl ObjectStore for InjectCreateFailureStore {
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

        async fn get_with_metadata(
            &self,
            key: &str,
        ) -> Result<Option<ObjectBody>, ObjectStoreError> {
            self.inner.get_with_metadata(key).await
        }

        async fn put(
            &self,
            key: &str,
            bytes: Bytes,
            mode: PutMode,
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            if matches!(&mode, PutMode::CreateIfAbsent) && self.matcher.matches(key) {
                let should_inject = {
                    let mut injected = self
                        .injected
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if *injected {
                        false
                    } else {
                        *injected = true;
                        true
                    }
                };
                if should_inject {
                    self.failure
                        .apply_before_error(&self.inner, key, bytes.clone())
                        .await?;
                    return Err(self.failure.error(key));
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

    pub(crate) fn mutation_context() -> MutationContext {
        super::mutation_context("writer-a", 1_000)
    }

    pub(crate) fn content_ref(seed: &str) -> ContentRef {
        loonfs_test_support::ids::content_ref(seed.as_bytes())
    }

    pub(crate) fn content_blob_counting_store(
        root: impl AsRef<Path>,
    ) -> CountingStore<LocalFsStore> {
        CountingStore::new(
            LocalFsStore::new(root.as_ref()).expect("store"),
            KeyPredicate::content_blob(),
        )
    }
}
