//! [`PublishPlanningSession`]: plans a batch's candidates in admission
//! order, each seeing the rows earlier candidates would persist.

use super::intent::PathMutationIntent;
use super::planner::{plan_path_mutation_against_publish_view, PlannedPathMutation};
use crate::commit::CommitPlan;
use crate::error::CoreError;
use crate::metadata::{DurableVisibilityCache, MetadataState, MetadataView};
use loonfs_api::wire::control::HeadState;
use loonfs_api::wire::wal::WalCommitPayload;
#[cfg(test)]
use loonfs_api::AbsolutePath;
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
            accepted_rows: MetadataState::default(),
            durable_cache: DurableVisibilityCache::default(),
        }
    }

    pub(crate) fn head(&self) -> &HeadState {
        &self.head
    }

    pub(crate) fn accepted_rows(&self) -> &MetadataState {
        &self.accepted_rows
    }

    pub(crate) fn durable_cache(&self) -> &DurableVisibilityCache {
        &self.durable_cache
    }

    pub(crate) async fn plan_path_mutation<S: ObjectStore + ?Sized>(
        &self,
        namespace_id: &NamespaceId,
        intent: &PathMutationIntent,
        base_view: MetadataView<'_, '_, S>,
    ) -> Result<PlannedPathMutation, CoreError> {
        let cached_view = base_view.with_durable_cache(&self.durable_cache);
        let view = cached_view.with_overlay(&self.accepted_rows, self.head.seq);
        plan_path_mutation_against_publish_view(namespace_id, intent, &self.head, &view).await
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
    use super::*;
    use crate::commit_engine::{publish_namespace_mutations_batch, NamespaceMutationCandidate};
    use crate::context::MutationContext;
    use crate::error::ErrorCode;
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::protocol::{load_publish_metadata_view, PublishTailOptions};
    use crate::storage::content::store_bytes_as_content;
    use crate::storage::content_admission::{ContentAdmission, PreparedContent};
    use loonfs_api::{CommitId, DeleteDirectoryBehavior, DestinationBehavior};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use tempfile::tempdir;

    fn test_context() -> MutationContext {
        MutationContext {
            writer_id: "writer".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            writer_version: "test".to_owned(),
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
        content_ref: loonfs_api::ContentRef,
    ) -> NamespaceMutationCandidate {
        let admission = ContentAdmission::for_durable_content_write(
            NamespaceId::parse("demo").expect("valid namespace id"),
            content_ref.clone(),
        );
        NamespaceMutationCandidate::path_prepared(
            PathMutationIntent::PutFile {
                commit_id: CommitId::parse(commit_id).expect("valid commit id"),
                absolute_path: AbsolutePath::parse(absolute_path).expect("path"),
                content_ref: content_ref.clone(),
                behavior: DestinationBehavior::NoReplace,
            },
            vec![PreparedContent::from_admission(content_ref, admission)],
        )
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
        publish_namespace_mutations_batch(
            &store,
            &namespace_id,
            vec![put_file_candidate(
                "seed-docs",
                "/docs/seed.txt",
                staged.content_ref.clone(),
            )],
            &context,
        )
        .await
        .remove(0)
        .expect("seed publish");

        let (view, _projection) = load_publish_metadata_view(
            &store,
            None,
            None,
            &namespace_id,
            None,
            None,
            &PublishTailOptions::default(),
        )
        .await
        .expect("load publish view");
        let session = PublishPlanningSession::new(view.head());

        let first_intent = PathMutationIntent::PutFile {
            commit_id: CommitId::parse("plan-a").expect("valid commit id"),
            absolute_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            content_ref: staged.content_ref.clone(),
            behavior: DestinationBehavior::NoReplace,
        };
        session
            .plan_path_mutation(&namespace_id, &first_intent, view.metadata_view())
            .await
            .expect("first plan");
        let after_first = session.durable_cache().stats();

        let second_intent = PathMutationIntent::PutFile {
            commit_id: CommitId::parse("plan-b").expect("valid commit id"),
            absolute_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
            content_ref: staged.content_ref.clone(),
            behavior: DestinationBehavior::NoReplace,
        };
        session
            .plan_path_mutation(&namespace_id, &second_intent, view.metadata_view())
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

    /// The first candidate implicitly creates `/wide`; the second must plan
    /// against the session state that already contains it, or its own
    /// duplicate `CreateDir` would fail child-name-absent validation.
    #[tokio::test]
    async fn batch_creates_under_one_new_parent_share_session_state() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let staged = store_bytes_as_content(&store, &namespace_id, b"hello")
            .await
            .expect("stage");

        let results = publish_namespace_mutations_batch(
            &store,
            &namespace_id,
            vec![
                put_file_candidate("create-wide-a", "/wide/a.txt", staged.content_ref.clone()),
                put_file_candidate("create-wide-b", "/wide/b.txt", staged.content_ref.clone()),
            ],
            &context,
        )
        .await;

        let first = results[0].as_ref().expect("first create succeeds");
        let second = results[1].as_ref().expect("second create succeeds");
        assert!(second.committed_seq > first.committed_seq);

        for path in ["/wide/a.txt", "/wide/b.txt"] {
            crate::path::read::load_metadata_view(
                &store,
                &namespace_id,
                crate::path::read::ReadLoadContext::latest(),
            )
            .await
            .expect("load view")
            .resolve_path(path)
            .await
            .expect("published file is visible");
        }
    }

    #[tokio::test]
    async fn duplicate_no_replace_put_in_one_batch_is_destination_exists() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let staged = store_bytes_as_content(&store, &namespace_id, b"hello")
            .await
            .expect("stage");

        let results = publish_namespace_mutations_batch(
            &store,
            &namespace_id,
            vec![
                put_file_candidate("create-a-first", "/docs/a.txt", staged.content_ref.clone()),
                put_file_candidate("create-a-second", "/docs/a.txt", staged.content_ref.clone()),
            ],
            &context,
        )
        .await;

        results[0].as_ref().expect("first create succeeds");
        let error = results[1].as_ref().expect_err("duplicate create rejected");
        assert_eq!(error.code(), ErrorCode::PathConflict);
        assert!(matches!(error, CoreError::DestinationExists(_)));
    }

    #[tokio::test]
    async fn create_then_delete_in_one_batch_respects_candidate_order() {
        let (_temp_dir, store, namespace_id, context) = setup_namespace().await;
        let staged = store_bytes_as_content(&store, &namespace_id, b"hello")
            .await
            .expect("stage");

        let results = publish_namespace_mutations_batch(
            &store,
            &namespace_id,
            vec![
                put_file_candidate(
                    "create-doomed",
                    "/docs/doomed.txt",
                    staged.content_ref.clone(),
                ),
                NamespaceMutationCandidate::path(PathMutationIntent::DeletePath {
                    commit_id: CommitId::parse("delete-doomed").expect("valid commit id"),
                    absolute_path: AbsolutePath::parse("/docs/doomed.txt").expect("path"),
                    behavior: DeleteDirectoryBehavior::NonRecursive,
                    expected_inode_id: None,
                }),
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
        .resolve_path("/docs/doomed.txt")
        .await
        .expect_err("deleted file is no longer visible");
    }
}
