//! [`PublishPlanningSession`]: plans a batch's candidates in admission
//! order, each seeing the rows earlier candidates would persist.

use super::planned_pieces::{CommitPieces, PlannedPieces};
use super::planner::OperationPlanningSession;
use super::publish_path_planning::PlanningView;
use crate::authorize::Authorizer;
use crate::commit::engine::CommitCandidate;
use crate::commit::{
    AppendedContent, CandidateAllocation, CommitFingerprint, CommitPlan, CommitValidationSession,
};
use crate::error::{CoreError, Result};
use crate::metadata::{DurableVisibilityCache, MetadataState, MetadataView};
use crate::namespace::state::NamespaceReadState;
use crate::storage::tail_content::{cut_layout, materialize_content_layout, ContentWrites};
use crate::wal::{inline_content_refs, ProjectedContent, ProjectedWalTail};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::{WalCommitPayload, WalDelta, WalInlineContent};
use loonfs_types::{next_public_ordinal, ChangeSeq, ContentRef, InodeId};
use std::collections::BTreeSet;

/// Working view of one publish attempt.
///
/// The session keeps the batch's constants (the publish view, where content
/// writes go, and the commit time), and the accepted rows and resident
/// pieces so later candidates can resolve earlier candidates and
/// materialize references to them.
pub(crate) struct PublishPlanningSession<'a, S: ObjectStore + ?Sized> {
    /// The head after the batch's accepted candidates. Its next inode ID is
    /// where the next candidate's allocation starts.
    head: NamespaceReadState,
    tail: &'a ProjectedWalTail,
    base_view: MetadataView<'a, 'a, S>,
    content_writes: ContentWrites<'a, S>,
    committed_at_ms: u64,
    accepted_rows: MetadataState,
    accepted_pieces: PlannedPieces,
    /// Durable-layer lookups memoized across the whole batch attempt; the
    /// accepted-rows overlay is the only layer that changes between
    /// candidates and is composed per lookup.
    durable_cache: DurableVisibilityCache,
}

impl<'a, S: ObjectStore + ?Sized> PublishPlanningSession<'a, S> {
    pub(crate) fn new(
        head: &NamespaceReadState,
        tail: &'a ProjectedWalTail,
        base_view: MetadataView<'a, 'a, S>,
        content_writes: ContentWrites<'a, S>,
        committed_at_ms: u64,
    ) -> Self {
        Self {
            head: head.clone(),
            tail,
            base_view,
            content_writes,
            committed_at_ms,
            accepted_rows: MetadataState::default(),
            accepted_pieces: PlannedPieces::default(),
            durable_cache: DurableVisibilityCache::default(),
        }
    }

    /// Returns the namespace's next inode ID after the batch's accepted
    /// candidates.
    pub(crate) fn next_inode_id(&self) -> InodeId {
        self.head.next_inode_id
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

    /// Checks a candidate's preconditions and plans and validates its
    /// operations in one pass, against the batch's state before the
    /// candidate.
    pub(crate) async fn prepare_commit(
        &self,
        candidate: &CommitCandidate,
        authorizer: Authorizer<'_>,
        semantic_identity: CommitFingerprint,
    ) -> Result<CommitPlan> {
        let base_view = self.base_view.with_durable_cache(&self.durable_cache);
        let pre_state = base_view.with_overlay(None, &self.accepted_rows, self.head.seq);
        self.planning_view(authorizer, pre_state)
            .evaluate_preconditions(&candidate.intent().preconditions, self.head.seq)
            .await?;
        self.plan_commit(
            candidate,
            authorizer,
            semantic_identity,
            base_view,
            &pre_state,
        )
        .await
    }

    /// Folds an accepted commit into the session so later candidates in the
    /// same batch plan and validate against it.
    pub(crate) fn apply_accepted_commit(&mut self, preview: &WalCommitPayload, plan: &CommitPlan) {
        // A plan belongs to the head it was planned against; accepting it at
        // another head would rewind or skip inode IDs.
        assert_eq!(
            plan.apply_after_seq, self.head.seq,
            "a commit plan should be accepted at the head it was planned against"
        );
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

    fn planning_view<'v>(
        &'v self,
        authorizer: Authorizer<'v>,
        metadata: MetadataView<'v, 'v, S>,
    ) -> PlanningView<'v, S> {
        PlanningView {
            namespace_id: &self.head.namespace_id,
            access: &self.head.access,
            authorizer,
            metadata,
        }
    }

    /// Plans each operation against the view, validates its compiled
    /// operations at once, and applies the accepted effects, so later
    /// operations observe earlier ones. The first failure aborts the request
    /// and attaches the failing operation's index.
    ///
    /// The head is the validated source of the namespace and writer epoch:
    /// `load_publish_metadata_view` fences on epoch equality, and the batch
    /// rejects namespace mismatches before admission.
    async fn plan_commit(
        &self,
        candidate: &CommitCandidate,
        authorizer: Authorizer<'_>,
        semantic_identity: CommitFingerprint,
        base_view: MetadataView<'_, '_, S>,
        pre_state: &MetadataView<'_, '_, S>,
    ) -> Result<CommitPlan> {
        let head = &self.head;
        let request = candidate.intent();
        let committed_seq = next_public_ordinal(head.seq.0)
            .map(ChangeSeq)
            .ok_or_else(|| {
                CoreError::NamespaceCorrupt(format!(
                    "the namespace head is at sequence {}, the largest sequence number, so no \
                     commit can follow it",
                    head.seq
                ))
            })?;
        let mut validation = CommitValidationSession::new(
            base_view,
            &self.accepted_rows,
            committed_seq,
            &request.commit_id,
            &request.actor_id,
            self.committed_at_ms,
        );
        let mut allocation = CandidateAllocation::new(head.next_inode_id);
        let mut deltas = Vec::new();
        let mut appended: Vec<AppendedContent> = Vec::new();
        let mut pieces = CommitPieces::new(self);
        for value in candidate.inline_content() {
            pieces.insert_piece(
                value.content_ref(),
                &WalInlineContent {
                    content_id: value.content_ref().content_id.clone(),
                    offset: 0,
                    bytes: value.bytes().to_vec(),
                },
                committed_seq,
            );
        }
        for (index, operation) in request.operations.iter().enumerate() {
            let compiled = OperationPlanningSession {
                view: self.planning_view(authorizer, validation.view()),
                pieces: &pieces,
                content_writes: self.content_writes,
                allocation: &mut allocation,
            }
            .plan(operation)
            .await
            .map_err(|error| error.at_operation(index))?;
            if let Some(value) = &compiled.appended {
                for piece in &value.pieces {
                    pieces.insert_piece(&value.content_ref, piece, committed_seq);
                }
            }
            appended.extend(compiled.appended);
            let operation_deltas = validation
                .validate(&compiled.ops, |content_ref| {
                    if let Some(value) = appended
                        .iter()
                        .find(|value| value.content_ref == *content_ref)
                    {
                        return value.layout.clone();
                    }
                    candidate.revision_layout(content_ref)
                })
                .await
                .map_err(|error| error.at_operation(index))?;
            deltas.extend(operation_deltas);
        }

        let mut plan = CommitPlan {
            namespace_id: head.namespace_id.clone(),
            commit_id: request.commit_id.clone(),
            actor_id: request.actor_id.clone(),
            writer_epoch: head.writer_epoch,
            message: request.message.clone(),
            semantic_identity,
            apply_after_seq: head.seq,
            assigned_seq: committed_seq,
            committed_at_ms: self.committed_at_ms,
            deltas,
            appended: appended
                .into_iter()
                .flat_map(|value| value.pieces)
                .collect(),
            inline_content: candidate.inline_content().to_vec(),
            resulting_next_inode_id: allocation.resulting_next_inode_id(),
        };
        self.carry_revision_layouts(&validation.view(), pre_state, &pieces, candidate, &mut plan)
            .await?;
        Ok(plan)
    }

    async fn carry_revision_layouts(
        &self,
        view: &MetadataView<'_, '_, S>,
        before: &MetadataView<'_, '_, S>,
        pieces: &CommitPieces<'_, S>,
        candidate: &CommitCandidate,
        plan: &mut CommitPlan,
    ) -> Result<()> {
        let mut revision_content_ids = BTreeSet::new();
        for delta in &mut plan.deltas {
            let WalDelta::AppendFileRevision {
                content_ref,
                layout,
                ..
            } = &mut delta.delta
            else {
                continue;
            };
            let earlier_revision = !revision_content_ids.insert(content_ref.content_id.clone());
            let first_offset = candidate
                .inline_content()
                .iter()
                .filter(|value| value.content_ref().content_id == content_ref.content_id)
                .map(|_| 0)
                .chain(
                    plan.appended
                        .iter()
                        .filter(|piece| piece.content_id == content_ref.content_id)
                        .map(|piece| piece.offset),
                )
                .min();
            if first_offset == Some(0) {
                *layout = None;
                continue;
            }
            if first_offset.is_some()
                && (earlier_revision || before.content_published(&content_ref.content_id).await?)
            {
                *layout = None;
                continue;
            }
            let mut prefix = content_ref.clone();
            prefix.size_bytes = first_offset.unwrap_or(content_ref.size_bytes);
            if let Some(layout) = layout
                .as_mut()
                .filter(|layout| layout.size_bytes() >= prefix.size_bytes)
            {
                cut_layout(layout, prefix.size_bytes);
            } else {
                assert!(
                    first_offset.is_none(),
                    "append planning should give fresh content `{}` a layout covering {} bytes",
                    content_ref.content_id,
                    prefix.size_bytes
                );
                let content = pieces.content(&prefix);
                *layout = Some(
                    materialize_content_layout(
                        self.content_writes,
                        view,
                        content.as_ref(),
                        &prefix,
                    )
                    .await
                    .map_err(|error| error.at_operation(delta.semantic_operation_index as usize))?,
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::intent::CommitIntent;
    use super::super::intent::FilesystemOperation;
    use super::*;
    use crate::commit::engine::CommitCandidate;
    use crate::error::{CoreError, ErrorCode};
    use crate::storage::content_admission::PreparedContent;
    use crate::test_support::WriterAt;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_types::{
        AbsolutePath, CommitId, CommitPrecondition, DeleteDirectoryBehavior, DestinationBehavior,
        NamespaceId,
    };
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
                loonfs_types::options::AttributeInclusion::Omit,
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
        use crate::commit::wal_payload_from_commit_plan;

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
            let session = PublishPlanningSession::new(
                view.head(),
                view.wal_tail(),
                view.projected_metadata_view(),
                ContentWrites::new(&namespace.store, &content_writes),
                1,
            );
            let plan = session
                .prepare_commit(
                    &CommitCandidate::new(request.clone()),
                    Authorizer::Unrestricted,
                    commit_fingerprint(&namespace_id, &request, &std::collections::BTreeMap::new()),
                )
                .await
                .expect("plan");
            payloads.push(wal_payload_from_commit_plan(&plan));
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
            view.projected_metadata_view(),
            ContentWrites::new(&namespace.store, &content_writes),
            1,
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
        session
            .prepare_commit(
                &CommitCandidate::new(first_request.clone()),
                Authorizer::Unrestricted,
                test_fingerprint(),
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
        session
            .prepare_commit(
                &CommitCandidate::new(second_request.clone()),
                Authorizer::Unrestricted,
                test_fingerprint(),
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
                loonfs_types::options::AttributeInclusion::Omit,
                &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
            )
            .await
            .expect_err("deleted file is no longer visible");
    }
}
