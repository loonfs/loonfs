use crate::context::MutationContext;
use crate::error::{CoreError, CoreErrorKind};
use crate::services::PutFileBehavior;
use loon_api::v0::{CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse};
use loon_api::{CommitId, ContentRef, MutationResult, NamespaceId};
use loon_objectstore::ObjectStore;

const DEFAULT_STALE_HEAD_RETRY_LIMIT: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathMutationIntent {
    PutFile {
        commit_id: CommitId,
        absolute_path: String,
        content_ref: ContentRef,
        behavior: PutFileBehavior,
    },
    DeletePath {
        commit_id: CommitId,
        absolute_path: String,
        recursive: bool,
    },
    MovePath {
        commit_id: CommitId,
        from_path: String,
        to_path: String,
    },
    CopyFilePath {
        commit_id: CommitId,
        from_path: String,
        to_path: String,
    },
}

impl PathMutationIntent {
    pub fn commit_id(&self) -> &CommitId {
        match self {
            Self::PutFile { commit_id, .. }
            | Self::DeletePath { commit_id, .. }
            | Self::MovePath { commit_id, .. }
            | Self::CopyFilePath { commit_id, .. } => commit_id,
        }
    }

    pub fn semantic_commit_fingerprint_sha256(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<String, CoreError> {
        crate::services::semantic_commit_fingerprint_for_path_intent(namespace_id, self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedPathMutation {
    pub commit_id: CommitId,
    pub semantic_commit_fingerprint_sha256: String,
    pub commit_request: ApiCommitRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedNamespaceMutation {
    pub commit_request: ApiCommitRequest,
    pub semantic_commit_fingerprint_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceMutationCandidate {
    Commit(ApiCommitRequest),
    Planned(PlannedNamespaceMutation),
    Path(PathMutationIntent),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushPolicy {
    Immediate,
    Coalesce { max_delay_ms: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishOptions {
    pub flush: FlushPolicy,
    pub stale_head_retry_limit: usize,
}

impl Default for PublishOptions {
    fn default() -> Self {
        Self {
            flush: FlushPolicy::Immediate,
            stale_head_retry_limit: DEFAULT_STALE_HEAD_RETRY_LIMIT,
        }
    }
}

pub struct DirectObjectStorePublisher<'a, S: ObjectStore + ?Sized> {
    store: &'a S,
}

impl<'a, S: ObjectStore + ?Sized> DirectObjectStorePublisher<'a, S> {
    pub fn new(store: &'a S) -> Self {
        Self { store }
    }

    pub fn plan_path_intent(
        &self,
        namespace_id: &NamespaceId,
        intent: &PathMutationIntent,
    ) -> Result<PlannedPathMutation, CoreError> {
        crate::services::plan_path_mutation(self.store, namespace_id, intent)
    }

    pub fn submit_path_intent(
        &self,
        namespace_id: &NamespaceId,
        intent: PathMutationIntent,
        context: &MutationContext,
        options: PublishOptions,
    ) -> Result<MutationResult, CoreError> {
        let attempts = options.stale_head_retry_limit.max(1);
        let mut last_error = None;

        for attempt in 0..attempts {
            let mut results = publish_namespace_mutations_batch(
                self.store,
                namespace_id,
                vec![NamespaceMutationCandidate::Path(intent.clone())],
                context,
            );
            let result = results
                .pop()
                .unwrap_or_else(|| Err(CoreError::Store("empty path mutation batch".to_owned())));
            match result {
                Ok(response) => {
                    return Ok(MutationResult {
                        namespace_id: response.namespace_id,
                        committed_seq: response.committed_seq,
                    });
                }
                Err(error)
                    if error.kind() == CoreErrorKind::StaleHead && attempt + 1 < attempts =>
                {
                    last_error = Some(error);
                }
                Err(error) => return Err(error),
            }
        }

        Err(last_error.unwrap_or_else(|| {
            CoreError::Store("path mutation stale-head retry exhausted".to_owned())
        }))
    }

    pub fn submit_commit_request(
        &self,
        namespace_id: &NamespaceId,
        request: ApiCommitRequest,
        context: &MutationContext,
    ) -> Result<ApiCommitResponse, CoreError> {
        crate::protocol::commit_operations(self.store, namespace_id, request, context)
    }

    pub fn submit_commit_batch(
        &self,
        namespace_id: &NamespaceId,
        requests: Vec<ApiCommitRequest>,
        context: &MutationContext,
    ) -> Vec<Result<ApiCommitResponse, CoreError>> {
        crate::protocol::commit_operations_batch(self.store, namespace_id, requests, context)
    }
}

pub fn publish_namespace_mutations_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: Vec<NamespaceMutationCandidate>,
    context: &MutationContext,
) -> Vec<Result<ApiCommitResponse, CoreError>> {
    crate::protocol::publish_namespace_mutations_batch(store, namespace_id, candidates, context)
}
