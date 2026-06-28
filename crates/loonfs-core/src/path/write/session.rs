use super::intent::PathMutationIntent;
use super::planner::{plan_path_mutation_against_publish_view, PlannedPathMutation};
use crate::commit::CommitPlan;
use crate::error::CoreError;
use crate::metadata::{MetadataApplyError, MetadataState, MetadataView};
use loonfs_api::wire::control::HeadState;
use loonfs_api::wire::wal::WalCommitPayload;
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
}

impl PublishPlanningSession {
    pub(crate) fn new(head: &HeadState) -> Self {
        Self {
            head: head.clone(),
            accepted_rows: MetadataState::default(),
        }
    }

    pub(crate) fn head(&self) -> &HeadState {
        &self.head
    }

    pub(crate) fn accepted_rows(&self) -> &MetadataState {
        &self.accepted_rows
    }

    pub(crate) async fn plan_path_mutation<S: ObjectStore + ?Sized>(
        &self,
        namespace_id: &NamespaceId,
        intent: &PathMutationIntent,
        base_view: MetadataView<'_, '_, S>,
    ) -> Result<PlannedPathMutation, CoreError> {
        let view =
            base_view.with_overlay(&self.accepted_rows, self.head.seq, self.head.name_policy);
        plan_path_mutation_against_publish_view(namespace_id, intent, &self.head, &view).await
    }

    /// Folds an accepted commit into the session so later candidates in the
    /// same batch plan and validate against it.
    pub(crate) fn apply_accepted_commit(
        &mut self,
        preview: &WalCommitPayload,
        plan: &CommitPlan,
    ) -> Result<(), MetadataApplyError> {
        self.accepted_rows.apply_committed_wal_record_mut(preview)?;
        self.head.seq = plan.assigned_seq;
        self.head.next_inode_id = plan.resulting_next_inode_id;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::MutationContext;
    use crate::engine::NamespaceEngine;
    use crate::error::ErrorCode;
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::publisher::{publish_namespace_mutations_batch, NamespaceMutationCandidate};
    use crate::storage::content::store_bytes_as_content;
    use loonfs_api::{CommitId, DeleteDirectoryBehavior, PutBehavior};
    use loonfs_objectstore::fs::LocalFsStore;
    use tempfile::tempdir;

    fn test_context() -> MutationContext {
        MutationContext {
            writer_id: "writer".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            writer_version: "test".to_owned(),
            now_ms: 1,
            lease_duration_ms: 60_000,
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
        NamespaceMutationCandidate::Path(PathMutationIntent::PutFile {
            commit_id: CommitId::parse(commit_id).expect("valid commit id"),
            absolute_path: absolute_path.to_owned(),
            content_ref,
            behavior: PutBehavior::NoReplace,
        })
    }

    fn test_engine<'a>(
        store: &'a LocalFsStore,
        namespace_id: &NamespaceId,
        context: &MutationContext,
    ) -> NamespaceEngine<&'a LocalFsStore> {
        NamespaceEngine::builder(store)
            .namespace(namespace_id.clone())
            .writer(context.writer_id.clone())
            .writer_version(context.writer_version.clone())
            .lease_duration_ms(context.lease_duration_ms)
            .build()
            .expect("test engine")
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

        let engine = test_engine(&store, &namespace_id, &context);
        for path in ["/wide/a.txt", "/wide/b.txt"] {
            engine
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
                NamespaceMutationCandidate::Path(PathMutationIntent::DeletePath {
                    commit_id: CommitId::parse("delete-doomed").expect("valid commit id"),
                    absolute_path: "/docs/doomed.txt".to_owned(),
                    behavior: DeleteDirectoryBehavior::NonRecursive,
                }),
            ],
            &context,
        )
        .await;

        results[0].as_ref().expect("create succeeds");
        results[1]
            .as_ref()
            .expect("delete sees the create from the same batch");

        test_engine(&store, &namespace_id, &context)
            .resolve_path("/docs/doomed.txt")
            .await
            .expect_err("deleted file is no longer visible");
    }
}
