//! Shared fixtures for the crate's integration tests.

#![allow(dead_code)]

use loonfs_api::NamespaceId;
use loonfs_core::cache::{
    MetadataTableCache, MetadataTableCacheConfig, WalTailProjectionCache,
    WalTailProjectionCacheConfig, DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES,
    DEFAULT_WAL_TAIL_PROJECTION_ROWS,
};
use loonfs_core::control::load_namespace_read_anchor;
use loonfs_core::{MutationContext, NamespaceEngine, RuntimeReadContext};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;

pub(crate) fn mutation_context(
    writer_id: &str,
    writer_session_id: &str,
    writer_version: &str,
    now_ms: u64,
) -> MutationContext {
    MutationContext {
        writer_id: writer_id.to_owned(),
        writer_session_id: writer_session_id.to_owned(),
        writer_version: writer_version.to_owned(),
        now_ms,
    }
}

pub(crate) fn namespace_engine<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> NamespaceEngine<&'a S> {
    NamespaceEngine::builder(store)
        .namespace_id(namespace_id.clone())
        .writer_id(context.writer_id.clone())
        .writer_session_id(context.writer_session_id.clone())
        .writer_version(context.writer_version.clone())
        .build()
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
        table_cache: Arc::new(MetadataTableCache::new(MetadataTableCacheConfig::default())),
        tail_cache: Arc::new(WalTailProjectionCache::new(WalTailProjectionCacheConfig {
            max_entries: 4,
            max_rows: DEFAULT_WAL_TAIL_PROJECTION_ROWS,
            max_decoded_bytes: DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES,
        })),
    }
}

pub(crate) mod mutation_split_support {
    #![allow(dead_code)]

    use super::{namespace_engine, read_context};
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use loonfs_api::{
        sha256_digest,
        v0::{CommitOp as ApiCommitOp, CommitRequest as ApiCommitRequest},
        AbsolutePath, ChangeSeq, CommitId, ContentRef, ContentRefKind, DestinationBehavior,
        DisplayName, InodeId, NameKey, NamespaceId,
    };

    use loonfs_core::content::{prepare_existing_content_ref, store_bytes_as_content};

    use loonfs_core::publish::{
        NamespaceCommitEngine, NamespaceMutationCandidate, PathMutationIntent, PublishTailOptions,
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

    #[derive(Debug, Clone)]
    pub(crate) struct BindingIdentity {
        pub(crate) parent_inode_id: InodeId,
        pub(crate) name_key: NameKey,
        pub(crate) child_inode_id: InodeId,
        pub(crate) bind_seq: ChangeSeq,
        pub(crate) bind_delta_index: u32,
    }

    pub(crate) async fn bootstrap_namespace<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        context: &MutationContext,
        allow_existing: bool,
    ) -> Result<loonfs_api::NamespaceSummary, loonfs_core::BootstrapNamespaceError> {
        namespace_engine(store, namespace_id, context)
            .bootstrap_namespace(BootstrapOptions { allow_existing })
            .await
    }

    pub(crate) async fn commit_operations<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        request: ApiCommitRequest,
        context: &MutationContext,
    ) -> Result<loonfs_api::v0::CommitResponse, CoreError> {
        let candidate = prepared_commit_candidate(store, namespace_id, request).await?;
        publish_namespace_mutations_batch(store, namespace_id, vec![candidate], context)
            .await
            .into_iter()
            .next()
            .expect("single commit result")
    }

    pub(crate) async fn commit_operations_batch<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        requests: Vec<ApiCommitRequest>,
        context: &MutationContext,
    ) -> Vec<Result<loonfs_api::v0::CommitResponse, CoreError>> {
        let request_count = requests.len();
        let mut candidates = Vec::with_capacity(request_count);
        for request in requests {
            match prepared_commit_candidate(store, namespace_id, request).await {
                Ok(candidate) => candidates.push(candidate),
                Err(error) => return (0..request_count).map(|_| Err(error.clone())).collect(),
            }
        }
        publish_namespace_mutations_batch(store, namespace_id, candidates, context).await
    }

    pub(crate) async fn prepared_commit_candidate<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        request: ApiCommitRequest,
    ) -> Result<NamespaceMutationCandidate, CoreError> {
        let mut seen = HashSet::new();
        let mut prepared = Vec::new();
        let mut catalog = None;
        for content_ref in request.ops.iter().filter_map(|op| match op {
            ApiCommitOp::CreateFile { content_ref, .. }
            | ApiCommitOp::ReplaceFile { content_ref, .. } => Some(content_ref),
            _ => None,
        }) {
            if seen.insert(content_ref.clone()) {
                if catalog.is_none() {
                    catalog = Some(
                        loonfs_core::control::load_namespace_catalog_entry(store, namespace_id)
                            .await?,
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
                    .await?,
                );
            }
        }
        Ok(NamespaceMutationCandidate::commit_prepared(
            request, prepared,
        ))
    }

    pub(crate) async fn publish_namespace_mutations_batch<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        candidates: Vec<NamespaceMutationCandidate>,
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
    ) -> Result<loonfs_api::v0::ChangesResponse, CoreError> {
        namespace_engine(store, namespace_id, &mutation_context())
            .list_changes_after(after_seq, page_limit(loonfs_api::DEFAULT_PAGE_LIMIT))
            .await
    }

    pub(crate) async fn create_checkpoint<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        context: &MutationContext,
    ) -> Result<loonfs_api::CreateCheckpointResponse, CoreError> {
        namespace_engine(store, namespace_id, context)
            .create_checkpoint("test-pin".to_owned(), None)
            .await
    }

    pub(crate) fn test_commit_id(commit_id: Option<&str>) -> CommitId {
        commit_id
            .map(|value| CommitId::parse(value).expect("valid test commit id"))
            .unwrap_or_else(CommitId::generate)
    }

    pub(crate) fn test_display_name(value: impl AsRef<str>) -> DisplayName {
        DisplayName::parse(value.as_ref()).expect("test display name should be valid")
    }

    /// Pins the current head and manifest the way the runtime does before a
    /// read.
    /// Publishes one path intent through the commit engine — the single publish
    /// pipeline.
    pub(crate) async fn admitted_candidate<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        intent: PathMutationIntent,
    ) -> NamespaceMutationCandidate {
        match &intent {
            PathMutationIntent::PutFile { content_ref, .. } => {
                let catalog =
                    loonfs_core::control::load_namespace_catalog_entry(store, namespace_id)
                        .await
                        .expect("load namespace catalog");
                let prepared = prepare_existing_content_ref(store, &catalog, content_ref.clone())
                    .await
                    .expect("prepare existing content");
                NamespaceMutationCandidate::path_prepared(intent, vec![prepared])
            }
            _ => NamespaceMutationCandidate::path(intent),
        }
    }

    pub(crate) async fn submit_intent_async<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        intent: PathMutationIntent,
        context: &MutationContext,
    ) -> Result<loonfs_api::CommitResponse, CoreError> {
        let candidate = admitted_candidate(store, namespace_id, intent).await;
        NamespaceCommitEngine::new(namespace_id.clone())
            .publish_batch(
                store,
                vec![candidate],
                context,
                &PublishTailOptions::default(),
            )
            .await
            .results
            .pop()
            .expect("one publish result")
    }

    pub(crate) async fn submit_intent<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        intent: PathMutationIntent,
        context: &MutationContext,
    ) -> Result<loonfs_api::CommitResponse, CoreError> {
        submit_intent_async(store, namespace_id, intent, context).await
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
        submit_intent(
            store,
            namespace_id,
            PathMutationIntent::PutFile {
                commit_id: test_commit_id(commit_id),
                message: None,
                absolute_path: AbsolutePath::parse(absolute_path).expect("path"),
                content_ref: content.content_ref,
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
        submit_intent(
            store,
            namespace_id,
            PathMutationIntent::CreateDir {
                commit_id: test_commit_id(commit_id),
                message: None,
                absolute_path: AbsolutePath::parse(absolute_path).expect("path"),
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
    ) -> Result<loonfs_api::AuthoritativePathEntry, CoreError> {
        let context = read_context(store, namespace_id).await;
        namespace_engine(store, namespace_id, &mutation_context())
            .resolve_path(absolute_path, &context)
            .await
    }

    pub(crate) async fn read_file_bytes<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<loonfs_api::AuthoritativeFileBytes, CoreError> {
        let context = read_context(store, namespace_id).await;
        namespace_engine(store, namespace_id, &mutation_context())
            .read_file(absolute_path, &context, None)
            .await
    }

    /// Reads the child's active parent binding, with the generation identity
    /// (`bind_seq`, `bind_delta_index`) a `BindingIs` precondition pins. The
    /// change feed reports semantic events without delta positions, so tests
    /// capture the observed binding from the metadata view like the embedded
    /// planner does.
    pub(crate) async fn current_binding_for_child<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        target_child_inode_id: InodeId,
    ) -> BindingIdentity {
        let context = read_context(store, namespace_id).await;
        let engine = namespace_engine(store, namespace_id, &mutation_context());
        let view = engine
            .load_grep_view(&context)
            .await
            .expect("load metadata view");
        let binding = view
            .grep_session()
            .current_parent_binding_for_child(target_child_inode_id)
            .await
            .expect("read current binding")
            .expect("binding exists");
        BindingIdentity {
            parent_inode_id: binding.parent_inode_id,
            name_key: binding.name_key,
            child_inode_id: binding.child_inode_id,
            bind_seq: binding.bind_seq,
            bind_delta_index: binding.bind_delta_index,
        }
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

        fn list_prefix_stream(
            &self,
            prefix: &str,
        ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
            self.inner.list_prefix_stream(prefix)
        }
    }

    pub(crate) fn mutation_context() -> MutationContext {
        super::mutation_context("writer-a", "wrs_test", "writer-a/0.1.0", 1_000)
    }

    pub(crate) fn content_ref(seed: &str) -> ContentRef {
        ContentRef {
            kind: ContentRefKind::WholeFileV0,
            digest: sha256_digest(seed.as_bytes()),
            size_bytes: seed.len() as u64,
        }
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
