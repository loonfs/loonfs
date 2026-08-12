//! [`PublishPlanningSession`]: plans a batch's candidates in admission
//! order, each seeing the rows earlier candidates would persist.

use super::intent::CommitRequest;
use super::planner::{plan_commit_against_publish_view, PlannedCommit};
use crate::commit::{
    validate_commit_for_publish, CandidateAllocation, CommitIr, CommitPlan, InodeAllocator,
    ValidatedCommitPlan,
};
use crate::error::Result;
use crate::metadata::{DurableVisibilityCache, MetadataState, MetadataView};
use loonfs_api::wire::control::HeadState;
use loonfs_api::wire::wal::WalCommitPayload;
#[cfg(test)]
use loonfs_api::AbsolutePath;
#[cfg(test)]
use loonfs_api::NamespaceId;
use loonfs_objectstore::ObjectStore;

/// Working view of one publish attempt.
///
/// The session owns the batch's evolving head and only the rows accepted
/// during this publish attempt. Durable base reads come from the loaded
/// manifest-plus-tail view; accepted rows are a small overlay so later
/// candidates observe earlier accepted candidates without cloning the whole
/// namespace.
pub(crate) struct PublishPlanningSession {
    head: HeadState,
    inode_allocator: InodeAllocator,
    accepted_rows: MetadataState,
    /// Durable-layer lookups memoized across the whole batch attempt; the
    /// accepted-rows overlay is the only layer that changes between
    /// candidates and is composed per lookup.
    durable_cache: DurableVisibilityCache,
}

impl PublishPlanningSession {
    pub(crate) fn new(head: &HeadState) -> Self {
        Self {
            head: head.clone(),
            inode_allocator: InodeAllocator::new(head.next_inode_id),
            accepted_rows: MetadataState::default(),
            durable_cache: DurableVisibilityCache::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn durable_cache(&self) -> &DurableVisibilityCache {
        &self.durable_cache
    }

    pub(crate) fn begin_candidate(&self) -> CandidateAllocation {
        self.inode_allocator.begin_candidate()
    }

    pub(crate) fn discard_candidate(&self, allocation: CandidateAllocation) {
        self.inode_allocator.discard_candidate(allocation);
    }

    pub(crate) async fn plan_commit<S: ObjectStore + ?Sized>(
        &self,
        request: &CommitRequest,
        base_view: MetadataView<'_, '_, S>,
        committed_at_ms: u64,
        allocation: &mut CandidateAllocation,
    ) -> Result<PlannedCommit> {
        let cached_view = base_view.with_durable_cache(&self.durable_cache);
        plan_commit_against_publish_view(
            request,
            &self.head,
            cached_view,
            &self.accepted_rows,
            committed_at_ms,
            allocation,
        )
        .await
    }

    pub(crate) async fn validate_commit<S: ObjectStore + ?Sized>(
        &self,
        request: &CommitIr,
        base_view: MetadataView<'_, '_, S>,
        committed_at_ms: u64,
    ) -> Result<ValidatedCommitPlan> {
        validate_commit_for_publish(
            request,
            committed_at_ms,
            &self.head,
            base_view.with_durable_cache(&self.durable_cache),
            &self.accepted_rows,
        )
        .await
    }

    /// Commits the candidate-local allocator fork and returns the new
    /// authoritative batch position.
    pub(crate) fn commit_candidate(
        &mut self,
        allocation: CandidateAllocation,
    ) -> Result<loonfs_api::InodeId> {
        self.inode_allocator.commit_candidate(allocation)
    }

    /// Folds an accepted commit into the session so later candidates in the
    /// same batch plan and validate against it.
    pub(crate) fn apply_accepted_commit(&mut self, preview: &WalCommitPayload, plan: &CommitPlan) {
        self.accepted_rows.apply_committed_wal_record_mut(preview);
        self.head.seq = plan.assigned_seq;
        self.head.next_inode_id = plan.resulting_next_inode_id;
    }
}

#[cfg(test)]
mod tests {
    use super::super::intent::FilesystemOperation;
    use super::*;
    use crate::commit_engine::{publish_namespace_commits_batch, CommitCandidate};
    use crate::context::MutationContext;
    use crate::error::{CoreError, ErrorCode};
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::namespace::control::load_namespace_head_control;
    use crate::path::read::{load_metadata_view, ReadLoadContext};
    use crate::storage::content::store_bytes_as_content;
    use crate::storage::content_admission::{ContentAdmission, PreparedContent};
    use loonfs_api::{CommitId, DeleteDirectoryBehavior, DestinationBehavior, InodeId};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use tempfile::tempdir;

    fn test_context() -> MutationContext {
        MutationContext {
            writer_id: "writer".to_owned(),
            now_ms: 1,
        }
    }

    async fn setup_namespace() -> (
        tempfile::TempDir,
        LocalFsStore,
        NamespaceId,
        MutationContext,
    ) {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false)
            .await
            .expect("bootstrap");
        (temp_dir, store, namespace_id, context)
    }

    fn put_file_candidate(
        commit_id: &str,
        absolute_path: &str,
        content_store_id: &loonfs_api::ContentStoreId,
        content_ref: loonfs_api::ContentRef,
    ) -> CommitCandidate {
        let admission = ContentAdmission::for_durable_content_write(
            content_store_id.clone(),
            content_ref.clone(),
        );
        CommitCandidate::prepared(
            CommitRequest::single(
                CommitId::parse(commit_id).expect("valid commit id"),
                None,
                FilesystemOperation::PutFile {
                    path: AbsolutePath::parse(absolute_path).expect("path"),
                    content_ref: content_ref.clone(),
                    behavior: DestinationBehavior::NoReplace,
                    expected_revision_no: None,
                },
            ),
            vec![PreparedContent::from_admission(admission)],
        )
    }

    fn create_directory_candidate(commit_id: &str, absolute_path: &str) -> CommitCandidate {
        CommitCandidate::new(CommitRequest::single(
            CommitId::parse(commit_id).expect("valid commit id"),
            None,
            FilesystemOperation::CreateDirectory {
                path: AbsolutePath::parse(absolute_path).expect("path"),
                parents: false,
            },
        ))
    }

    fn candidate_that_allocates_then_fails(commit_id: &str) -> CommitCandidate {
        CommitCandidate::new(CommitRequest {
            commit_id: CommitId::parse(commit_id).expect("valid commit id"),
            message: None,
            operations: vec![
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/discarded").expect("path"),
                    parents: false,
                },
                FilesystemOperation::DeletePath {
                    path: AbsolutePath::parse("/missing").expect("path"),
                    behavior: DeleteDirectoryBehavior::NonRecursive,
                    expected_inode_id: None,
                },
            ],
        })
    }

    async fn visible_inode_id(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> InodeId {
        crate::path::read::load_metadata_view(
            store,
            namespace_id,
            crate::path::read::ReadLoadContext::latest(),
        )
        .await
        .expect("load view")
        .resolve_path(absolute_path, crate::path::read::AttributeProjection::Omit)
        .await
        .expect("visible path")
        .inode_id
    }

    /// Two plans through one session share the durable-layer memo: the
    /// second plan's path walk answers from cache instead of re-scanning
    /// the manifest for the components both paths share.
    #[tokio::test]
    async fn second_plan_in_a_session_hits_the_durable_cache() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let staged = store_bytes_as_content(&store, &namespace_id, b"hello")
            .await
            .expect("stage");
        // Durable parent directory so the walks touch manifest-backed state.
        publish_namespace_commits_batch(
            &store,
            &namespace_id,
            vec![put_file_candidate(
                "seed-docs",
                "/docs/seed.txt",
                &staged.content_store_id,
                staged.content_ref.clone(),
            )],
            &context,
        )
        .await
        .remove(0)
        .expect("seed publish");

        let view = load_metadata_view(&store, &namespace_id, ReadLoadContext::latest())
            .await
            .expect("load metadata view");
        let session = PublishPlanningSession::new(view.head());

        let first_request = CommitRequest::single(
            CommitId::parse("plan-a").expect("valid commit id"),
            None,
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                content_ref: staged.content_ref.clone(),
                behavior: DestinationBehavior::NoReplace,
                expected_revision_no: None,
            },
        );
        let mut first_allocation = session.begin_candidate();
        session
            .plan_commit(
                &first_request,
                view.projected_metadata_view(),
                1,
                &mut first_allocation,
            )
            .await
            .expect("first plan");
        session.discard_candidate(first_allocation);
        let after_first = session.durable_cache().stats();

        let second_request = CommitRequest::single(
            CommitId::parse("plan-b").expect("valid commit id"),
            None,
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/docs/b.txt").expect("path"),
                content_ref: staged.content_ref.clone(),
                behavior: DestinationBehavior::NoReplace,
                expected_revision_no: None,
            },
        );
        let mut second_allocation = session.begin_candidate();
        session
            .plan_commit(
                &second_request,
                view.projected_metadata_view(),
                1,
                &mut second_allocation,
            )
            .await
            .expect("second plan");
        session.discard_candidate(second_allocation);
        let after_second = session.durable_cache().stats();

        assert!(
            after_second.hits > after_first.hits,
            "second plan should reuse durable lookups from the first: {after_first:?} -> {after_second:?}"
        );
        assert!(
            after_second.misses < after_first.misses * 2,
            "shared path components should not re-scan per plan: {after_first:?} -> {after_second:?}"
        );
    }

    /// The first candidate implicitly creates `/wide`; the second must plan
    /// against the session state that already contains it, or its own
    /// duplicate `CreateDirectory` would fail child-name-absent validation.
    #[tokio::test]
    async fn batch_creates_under_one_new_parent_share_session_state() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let staged = store_bytes_as_content(&store, &namespace_id, b"hello")
            .await
            .expect("stage");

        let results = publish_namespace_commits_batch(
            &store,
            &namespace_id,
            vec![
                put_file_candidate(
                    "create-wide-a",
                    "/wide/a.txt",
                    &staged.content_store_id,
                    staged.content_ref.clone(),
                ),
                put_file_candidate(
                    "create-wide-b",
                    "/wide/b.txt",
                    &staged.content_store_id,
                    staged.content_ref.clone(),
                ),
            ],
            &context,
        )
        .await;

        let first = results[0].as_ref().expect("first create succeeds");
        let second = results[1].as_ref().expect("second create succeeds");
        assert!(second.committed_seq > first.committed_seq);

        assert_eq!(
            visible_inode_id(&store, &namespace_id, "/wide").await,
            InodeId(2)
        );
        assert_eq!(
            visible_inode_id(&store, &namespace_id, "/wide/a.txt").await,
            InodeId(3)
        );
        assert_eq!(
            visible_inode_id(&store, &namespace_id, "/wide/b.txt").await,
            InodeId(4)
        );
        let head = load_namespace_head_control(&store, &namespace_id)
            .await
            .expect("load head");
        assert_eq!(head.state.next_inode_id, InodeId(5));
    }

    #[tokio::test]
    async fn accepted_candidate_followed_by_rejected_candidate_does_not_consume_rejected_ids() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;

        let results = publish_namespace_commits_batch(
            &store,
            &namespace_id,
            vec![
                create_directory_candidate("accept-first", "/kept"),
                candidate_that_allocates_then_fails("reject-second"),
            ],
            &context,
        )
        .await;

        results[0].as_ref().expect("first candidate accepted");
        results[1].as_ref().expect_err("second candidate rejected");
        assert_eq!(
            visible_inode_id(&store, &namespace_id, "/kept").await,
            InodeId(2)
        );
        let head = load_namespace_head_control(&store, &namespace_id)
            .await
            .expect("load head");
        assert_eq!(head.state.next_inode_id, InodeId(3));
    }

    #[tokio::test]
    async fn rejected_candidate_followed_by_accepted_candidate_reuses_the_rejected_id() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;

        let results = publish_namespace_commits_batch(
            &store,
            &namespace_id,
            vec![
                candidate_that_allocates_then_fails("reject-first"),
                create_directory_candidate("accept-second", "/kept"),
            ],
            &context,
        )
        .await;

        results[0].as_ref().expect_err("first candidate rejected");
        results[1].as_ref().expect("second candidate accepted");
        assert_eq!(
            visible_inode_id(&store, &namespace_id, "/kept").await,
            InodeId(2)
        );
        let head = load_namespace_head_control(&store, &namespace_id)
            .await
            .expect("load head");
        assert_eq!(head.state.next_inode_id, InodeId(3));
    }

    #[tokio::test]
    async fn duplicate_no_replace_put_in_one_batch_is_destination_exists() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let staged = store_bytes_as_content(&store, &namespace_id, b"hello")
            .await
            .expect("stage");

        let results = publish_namespace_commits_batch(
            &store,
            &namespace_id,
            vec![
                put_file_candidate(
                    "create-a-first",
                    "/docs/a.txt",
                    &staged.content_store_id,
                    staged.content_ref.clone(),
                ),
                put_file_candidate(
                    "create-a-second",
                    "/docs/a.txt",
                    &staged.content_store_id,
                    staged.content_ref.clone(),
                ),
            ],
            &context,
        )
        .await;

        results[0].as_ref().expect("first create succeeds");
        let error = results[1].as_ref().expect_err("duplicate create rejected");
        assert_eq!(error.code(), ErrorCode::PathConflict);
        assert!(matches!(error, CoreError::DestinationExists { .. }));
    }

    #[tokio::test]
    async fn create_then_delete_in_one_batch_respects_candidate_order() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let staged = store_bytes_as_content(&store, &namespace_id, b"hello")
            .await
            .expect("stage");

        let results = publish_namespace_commits_batch(
            &store,
            &namespace_id,
            vec![
                put_file_candidate(
                    "create-doomed",
                    "/docs/doomed.txt",
                    &staged.content_store_id,
                    staged.content_ref.clone(),
                ),
                CommitCandidate::new(CommitRequest::single(
                    CommitId::parse("delete-doomed").expect("valid commit id"),
                    None,
                    FilesystemOperation::DeletePath {
                        path: AbsolutePath::parse("/docs/doomed.txt").expect("path"),
                        behavior: DeleteDirectoryBehavior::NonRecursive,
                        expected_inode_id: None,
                    },
                )),
            ],
            &context,
        )
        .await;

        results[0].as_ref().expect("create succeeds");
        results[1]
            .as_ref()
            .expect("delete sees the create from the same batch");

        crate::path::read::load_metadata_view(
            &store,
            &namespace_id,
            crate::path::read::ReadLoadContext::latest(),
        )
        .await
        .expect("load view")
        .resolve_path(
            "/docs/doomed.txt",
            crate::path::read::AttributeProjection::Omit,
        )
        .await
        .expect_err("deleted file is no longer visible");
    }
}
