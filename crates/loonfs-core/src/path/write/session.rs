//! [`PublishPlanningSession`]: plans a batch's candidates in admission
//! order, each seeing the rows earlier candidates would persist.

use super::planned_pieces::PlannedPieces;
use super::planner::prepare_commit_against_publish_view;
use super::preconditions::evaluate_preconditions;
use crate::commit::{
    CandidateAllocation, CommitFingerprint, CommitPlan, InodeAllocator, ValidatedCommitPlan,
};
use crate::commit_engine::CommitCandidate;
use crate::error::Result;
use crate::metadata::{DurableVisibilityCache, MetadataState, MetadataView};
use crate::namespace::state::NamespaceReadState;
use crate::wal::{inline_content_refs, ProjectedContent, ProjectedWalTail};
use crate::PermitPool;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::WalCommitPayload;
#[cfg(test)]
use loonfs_types::AbsolutePath;
#[cfg(test)]
use loonfs_types::CommitPrecondition;
use loonfs_types::ContentRef;
#[cfg(test)]
use loonfs_types::NamespaceId;

/// Working view of one publish attempt.
///
/// The session keeps accepted rows and resident pieces so later candidates
/// can resolve earlier candidates and materialize references to them.
pub(crate) struct PublishPlanningSession<'a, S: ObjectStore + ?Sized> {
    head: NamespaceReadState,
    inode_allocator: InodeAllocator,
    tail: &'a ProjectedWalTail,
    accepted_rows: MetadataState,
    accepted_pieces: PlannedPieces,
    /// Durable-layer lookups memoized across the whole batch attempt; the
    /// accepted-rows overlay is the only layer that changes between
    /// candidates and is composed per lookup.
    durable_cache: DurableVisibilityCache,
    /// The store that planning writes content objects to.
    store: &'a S,
    /// Where each content write that planning sends takes its permit.
    content_writes: &'a PermitPool,
}

impl<'a, S: ObjectStore + ?Sized> PublishPlanningSession<'a, S> {
    pub(crate) fn new(
        head: &NamespaceReadState,
        tail: &'a ProjectedWalTail,
        store: &'a S,
        content_writes: &'a PermitPool,
    ) -> Self {
        Self {
            head: head.clone(),
            inode_allocator: InodeAllocator::new(head.next_inode_id),
            tail,
            accepted_rows: MetadataState::default(),
            accepted_pieces: PlannedPieces::default(),
            durable_cache: DurableVisibilityCache::default(),
            store,
            content_writes,
        }
    }

    pub(super) fn head(&self) -> &NamespaceReadState {
        &self.head
    }

    pub(super) fn store(&self) -> &'a S {
        self.store
    }

    pub(super) fn content_writes(&self) -> &'a PermitPool {
        self.content_writes
    }

    pub(super) fn accepted_rows(&self) -> &MetadataState {
        &self.accepted_rows
    }

    pub(super) fn content(&self, content_ref: &ContentRef) -> Option<ProjectedContent> {
        self.accepted_pieces
            .content(content_ref)
            .cloned()
            .or_else(|| self.tail.content(content_ref))
    }

    #[cfg(test)]
    pub(crate) fn durable_cache(&self) -> &DurableVisibilityCache {
        &self.durable_cache
    }

    pub(crate) fn begin_candidate(&self) -> CandidateAllocation {
        self.inode_allocator.begin_candidate()
    }

    /// Plans and validates a mutation request in one pass, producing the
    /// validated plan that only awaits the accepted allocation position.
    pub(crate) async fn prepare_commit(
        &self,
        candidate: &CommitCandidate,
        semantic_identity: CommitFingerprint,
        base_view: MetadataView<'_, '_, S>,
        committed_at_ms: u64,
        allocation: &mut CandidateAllocation,
    ) -> Result<ValidatedCommitPlan> {
        let base_view = base_view.with_durable_cache(&self.durable_cache);
        let pre_state = base_view.with_overlay(None, &self.accepted_rows, self.head.seq);
        evaluate_preconditions(
            &candidate.intent().preconditions,
            candidate.authority(),
            self,
            &pre_state,
        )
        .await?;
        prepare_commit_against_publish_view(
            candidate,
            semantic_identity,
            self,
            base_view,
            committed_at_ms,
            allocation,
        )
        .await
    }

    /// Commits the candidate-local allocator fork and returns the new
    /// authoritative batch position.
    pub(crate) fn commit_candidate(
        &mut self,
        allocation: CandidateAllocation,
    ) -> loonfs_types::InodeId {
        self.inode_allocator.commit_candidate(allocation)
    }

    /// Folds an accepted commit into the session so later candidates in the
    /// same batch plan and validate against it.
    pub(crate) fn apply_accepted_commit(&mut self, preview: &WalCommitPayload, plan: &CommitPlan) {
        for (content_ref, entry) in inline_content_refs(&self.head.namespace_id, preview) {
            self.accepted_pieces.insert_piece(
                self.tail.content(content_ref).as_ref(),
                content_ref,
                entry,
                preview.committed_seq,
            );
        }
        self.accepted_rows.apply_committed_wal_record_mut(preview);
        self.head.seq = plan.assigned_seq;
        self.head.next_inode_id = plan.resulting_next_inode_id;
    }
}

#[cfg(test)]
mod tests {
    use super::super::intent::CommitIntent;
    use super::super::intent::FilesystemOperation;
    use super::*;
    use crate::commit_engine::CommitCandidate;
    use crate::error::{CoreError, ErrorCode};
    use crate::storage::content_admission::PreparedContent;
    use crate::test_support::WriterAt;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_types::{CommitId, DeleteDirectoryBehavior, DestinationBehavior, InodeId};
    use std::assert_matches;
    use tempfile::tempdir;

    fn test_context() -> WriterAt {
        crate::test_support::writer_at("writer", 1)
    }

    async fn setup_namespace() -> (tempfile::TempDir, LocalFsStore, NamespaceId, WriterAt) {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .create()
            .await
            .expect("bootstrap");
        (temp_dir, store, namespace_id, context)
    }

    fn put_file_candidate(
        commit_id: &str,
        absolute_path: &str,
        content_ref: loonfs_types::ContentRef,
    ) -> CommitCandidate {
        let admission = PreparedContent::for_durable_content_write(content_ref.clone());
        CommitCandidate::prepared(
            CommitIntent::single(
                CommitId::parse(commit_id).expect("valid commit id"),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::PutFile {
                    path: AbsolutePath::parse(absolute_path).expect("path"),
                    content: loonfs_types::FileContent::Ref(content_ref.clone()),
                    behavior: DestinationBehavior::NoReplace,
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
            ),
            vec![admission],
        )
    }

    fn create_directory_candidate(commit_id: &str, absolute_path: &str) -> CommitCandidate {
        CommitCandidate::new(CommitIntent::single(
            CommitId::parse(commit_id).expect("valid commit id"),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::CreateDirectory {
                path: AbsolutePath::parse(absolute_path).expect("path"),
                parents: false,
            },
        ))
    }

    fn candidate_that_allocates_then_fails(commit_id: &str) -> CommitCandidate {
        CommitCandidate::new(CommitIntent {
            preconditions: Vec::new(),
            commit_id: CommitId::parse(commit_id).expect("valid commit id"),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
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
        crate::test_support::namespace_store(store, namespace_id)
            .load_current_metadata_view()
            .await
            .expect("load view")
            .resolve_path(
                absolute_path,
                loonfs_types::AttributeInclusion::Omit,
                &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
            )
            .await
            .expect("visible path")
            .inode_id
    }

    fn test_fingerprint() -> CommitFingerprint {
        loonfs_test_support::ids::commit_fingerprint("test")
    }

    #[tokio::test]
    async fn preconditions_change_only_the_fingerprint_in_the_wal_payload() {
        use super::super::planner::commit_fingerprint;
        use crate::commit::{wal_payload_from_prepared_commit, PreparedCommit};

        let (_temp_dir, store, namespace_id, _) = setup_namespace().await;
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        let view = namespace.load_current_metadata_view().await.expect("view");
        let request = create_directory_candidate("wal", "/docs").intent().clone();
        let mut payloads = Vec::new();
        for preconditions in [
            Vec::new(),
            vec![CommitPrecondition::NamespaceHead {
                expected_head_seq: view.head().seq,
            }],
        ] {
            let request = request.clone().preconditions(preconditions);
            let content_writes = crate::test_support::content_writes();
            let mut session = PublishPlanningSession::new(
                view.head(),
                view.wal_tail(),
                &namespace.store,
                &content_writes,
            );
            let mut allocation = session.begin_candidate();
            let plan = session
                .prepare_commit(
                    &CommitCandidate::new(request.clone()),
                    commit_fingerprint(&namespace_id, &request, &std::collections::BTreeMap::new()),
                    view.projected_metadata_view(),
                    1,
                    &mut allocation,
                )
                .await
                .expect("plan");
            let next_inode_id = session.commit_candidate(allocation);
            payloads.push(wal_payload_from_prepared_commit(&PreparedCommit {
                commit: plan.finish(next_inode_id),
                committed_at_ms: 1,
                inline_content: Vec::new(),
            }));
        }
        assert_ne!(
            payloads[0].semantic_commit_fingerprint,
            payloads[1].semantic_commit_fingerprint
        );
        payloads[1].semantic_commit_fingerprint = payloads[0].semantic_commit_fingerprint.clone();
        assert_eq!(payloads[0], payloads[1]);
    }

    #[tokio::test]
    async fn second_plan_in_a_session_hits_the_durable_cache() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let staged = crate::test_support::namespace_store(&store, &namespace_id)
            .store_bytes_as_content(b"hello")
            .await
            .expect("stage");
        // Durable parent directory so the walks touch manifest-backed state.
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .publish_candidates(vec![put_file_candidate(
                "seed-docs",
                "/docs/seed.txt",
                staged.content_ref().clone(),
            )])
            .await
            .remove(0)
            .expect("seed publish");

        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        let view = namespace
            .load_current_metadata_view()
            .await
            .expect("load metadata view");
        let content_writes = crate::test_support::content_writes();
        let session = PublishPlanningSession::new(
            view.head(),
            view.wal_tail(),
            &namespace.store,
            &content_writes,
        );

        let first_request = CommitIntent::single(
            CommitId::parse("plan-a").expect("valid commit id"),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                content: loonfs_types::FileContent::Ref(staged.content_ref().clone()),
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        );
        let mut first_allocation = session.begin_candidate();
        session
            .prepare_commit(
                &CommitCandidate::new(first_request.clone()),
                test_fingerprint(),
                view.projected_metadata_view(),
                1,
                &mut first_allocation,
            )
            .await
            .expect("first plan");
        let after_first = session.durable_cache().stats();

        let second_request = CommitIntent::single(
            CommitId::parse("plan-b").expect("valid commit id"),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/docs/b.txt").expect("path"),
                content: loonfs_types::FileContent::Ref(staged.content_ref().clone()),
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        );
        let mut second_allocation = session.begin_candidate();
        session
            .prepare_commit(
                &CommitCandidate::new(second_request.clone()),
                test_fingerprint(),
                view.projected_metadata_view(),
                1,
                &mut second_allocation,
            )
            .await
            .expect("second plan");
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

    #[tokio::test]
    async fn batch_creates_under_one_new_parent_share_session_state() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let staged = crate::test_support::namespace_store(&store, &namespace_id)
            .store_bytes_as_content(b"hello")
            .await
            .expect("stage");

        let results = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .publish_candidates(vec![
                put_file_candidate("create-wide-a", "/wide/a.txt", staged.content_ref().clone()),
                put_file_candidate("create-wide-b", "/wide/b.txt", staged.content_ref().clone()),
            ])
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
        let head = crate::test_support::namespace_store(&store, &namespace_id)
            .load_read_state()
            .await
            .expect("load head");
        assert_eq!(head.next_inode_id, InodeId(5));
    }

    #[tokio::test]
    async fn accepted_candidate_followed_by_rejected_candidate_does_not_consume_rejected_ids() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;

        let results = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .publish_candidates(vec![
                create_directory_candidate("accept-first", "/kept"),
                candidate_that_allocates_then_fails("reject-second"),
            ])
            .await;

        results[0].as_ref().expect("first candidate accepted");
        results[1].as_ref().expect_err("second candidate rejected");
        assert_eq!(
            visible_inode_id(&store, &namespace_id, "/kept").await,
            InodeId(2)
        );
        let head = crate::test_support::namespace_store(&store, &namespace_id)
            .load_read_state()
            .await
            .expect("load head");
        assert_eq!(head.next_inode_id, InodeId(3));
    }

    #[tokio::test]
    async fn rejected_candidate_followed_by_accepted_candidate_reuses_the_rejected_id() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;

        let results = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .publish_candidates(vec![
                candidate_that_allocates_then_fails("reject-first"),
                create_directory_candidate("accept-second", "/kept"),
            ])
            .await;

        results[0].as_ref().expect_err("first candidate rejected");
        results[1].as_ref().expect("second candidate accepted");
        assert_eq!(
            visible_inode_id(&store, &namespace_id, "/kept").await,
            InodeId(2)
        );
        let head = crate::test_support::namespace_store(&store, &namespace_id)
            .load_read_state()
            .await
            .expect("load head");
        assert_eq!(head.next_inode_id, InodeId(3));
    }

    #[tokio::test]
    async fn duplicate_no_replace_put_in_one_batch_is_destination_exists() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let staged = crate::test_support::namespace_store(&store, &namespace_id)
            .store_bytes_as_content(b"hello")
            .await
            .expect("stage");

        let results = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .publish_candidates(vec![
                put_file_candidate(
                    "create-a-first",
                    "/docs/a.txt",
                    staged.content_ref().clone(),
                ),
                put_file_candidate(
                    "create-a-second",
                    "/docs/a.txt",
                    staged.content_ref().clone(),
                ),
            ])
            .await;

        results[0].as_ref().expect("first create succeeds");
        let error = results[1].as_ref().expect_err("duplicate create rejected");
        assert_eq!(error.code(), ErrorCode::PathConflict);
        assert_matches!(
            error,
            CoreError::FailedOperation { operation_index: 0, error: source }
            if matches!(source.as_ref(), CoreError::DestinationExists { .. })
        );
    }

    #[tokio::test]
    async fn create_then_delete_in_one_batch_respects_candidate_order() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let staged = crate::test_support::namespace_store(&store, &namespace_id)
            .store_bytes_as_content(b"hello")
            .await
            .expect("stage");

        let results = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .publish_candidates(vec![
                put_file_candidate(
                    "create-doomed",
                    "/docs/doomed.txt",
                    staged.content_ref().clone(),
                ),
                CommitCandidate::new(CommitIntent::single(
                    CommitId::parse("delete-doomed").expect("valid commit id"),
                    loonfs_test_support::test_actor(),
                    None,
                    FilesystemOperation::DeletePath {
                        path: AbsolutePath::parse("/docs/doomed.txt").expect("path"),
                        behavior: DeleteDirectoryBehavior::NonRecursive,
                        expected_inode_id: None,
                    },
                )),
            ])
            .await;

        results[0].as_ref().expect("create succeeds");
        results[1]
            .as_ref()
            .expect("delete sees the create from the same batch");

        crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_metadata_view()
            .await
            .expect("load view")
            .resolve_path(
                "/docs/doomed.txt",
                loonfs_types::AttributeInclusion::Omit,
                &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
            )
            .await
            .expect_err("deleted file is no longer visible");
    }
}
