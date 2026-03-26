use super::loads::{
    assess_hierarchy_parent_materialization_from_conn, assess_remote_subtree_delete_from_conn,
    assess_remote_subtree_rename_from_conn, load_bound_apply_remote_delete_views_from_conn,
    load_bound_apply_remote_rename_views_from_conn,
    load_bound_apply_remote_subtree_delete_views_from_conn,
    load_bound_apply_remote_subtree_rename_views_from_conn,
    load_bound_download_remote_edit_views_from_conn,
    load_bound_resolve_subtree_delete_conflict_views_from_conn,
    load_bound_resolve_subtree_rename_conflict_views_from_conn,
    load_bound_upload_local_edit_views_from_conn, load_conflict_artifact,
    load_conflict_artifact_archive, load_conflict_artifact_archives_for_namespace,
    load_conflict_artifacts_for_namespace, load_conflicts_and_errors,
    load_conflicts_and_errors_for_namespace, load_inode_upload, load_local_file,
    load_local_only_candidates_for_namespace, load_local_only_conflicts_and_errors,
    load_local_only_conflicts_and_errors_for_namespace, load_local_only_descendants_under_subtree,
    load_local_only_file, load_local_only_parent_link, load_local_only_parent_links_for_namespace,
    load_local_only_planned_actions_for_namespace, load_local_only_state_for_namespace,
    load_local_only_transfer_ledger, load_local_only_transfer_ledgers_for_namespace,
    load_local_only_upload, load_local_state_for_namespace, load_local_subtree_inode_ids,
    load_next_deferred_planned_action, load_next_executable_planned_action,
    load_next_planned_action, load_next_planned_local_only_action,
    load_next_runnable_planned_local_only_action, load_pending_client_mutation,
    load_pending_client_mutation_for_client_file, load_pending_client_mutations_for_namespace,
    load_pending_inode_mutation, load_pending_inode_mutation_for_inode,
    load_pending_inode_mutations_for_namespace, load_planned_action,
    load_planned_actions_for_namespace, load_planned_local_only_action, load_remote_file,
    load_remote_state_for_namespace, load_remote_subtree_descendant_inode_ids, load_sync_anchor,
    load_sync_anchors_for_namespace, load_transfer_ledger_for_inode,
    load_transfer_ledgers_for_namespace,
};
use super::schema::initialize_connection;
use super::*;
use crate::planner::{plan_file_in_tx, plan_local_only_inode_in_tx, PlannerDecision};
use crate::upload::UploadedContent;
use rusqlite::{params, Connection};
use serde_json::json;
use std::path::Path;

fn observed_remote_as_remote_file_state(observed: &ObservedRemoteInode) -> RemoteFileStateRow {
    RemoteFileStateRow {
        namespace_id: observed.namespace_id.clone(),
        inode_id: observed.inode_id,
        inode_kind: observed.inode_kind.clone(),
        observed_seq: observed.observed_seq,
        revision_no: observed.revision_no,
        content_digest: observed.content_digest.clone(),
        content_manifest_digest: observed.content_manifest_digest.clone(),
        parent_inode_id: observed.parent_inode_id,
        display_name: observed.display_name.clone(),
        is_deleted: observed.is_deleted,
    }
}

impl SqliteStateDb {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StateDbError> {
        let conn = Connection::open(path)?;
        initialize_connection(&conn)?;

        let mut db = Self { conn };
        db.apply_migrations()?;
        Ok(db)
    }

    pub fn open_in_memory() -> Result<Self, StateDbError> {
        let conn = Connection::open_in_memory()?;
        initialize_connection(&conn)?;

        let mut db = Self { conn };
        db.apply_migrations()?;
        Ok(db)
    }

    pub fn schema_version(&self) -> Result<i32, StateDbError> {
        Ok(self
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))?)
    }

    pub fn planner_transaction<T, F>(&mut self, label: &str, f: F) -> Result<T, StateDbError>
    where
        F: FnOnce(&mut PlannerTxn<'_>) -> Result<T, StateDbError>,
    {
        let _ = label;
        let tx = self.conn.transaction()?;
        let mut planner_tx = PlannerTxn { tx };
        let result = f(&mut planner_tx)?;
        planner_tx.tx.commit()?;
        Ok(result)
    }

    pub fn load_file_sync_views(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<FileSyncViews, StateDbError> {
        Ok(FileSyncViews {
            namespace_id: namespace_id.clone(),
            inode_id,
            remote: load_remote_file(&self.conn, namespace_id, inode_id)?,
            local: load_local_file(&self.conn, namespace_id, inode_id)?,
            sync_anchor: load_sync_anchor(&self.conn, namespace_id, inode_id)?,
        })
    }

    pub fn load_bound_upload_local_edit_views(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<(RemoteFileStateRow, LocalFileStateRow, SyncAnchorRow), StateDbError> {
        load_bound_upload_local_edit_views_from_conn(&self.conn, namespace_id, inode_id)
    }

    pub fn load_bound_download_remote_edit_views(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<(RemoteFileStateRow, LocalFileStateRow, SyncAnchorRow), StateDbError> {
        load_bound_download_remote_edit_views_from_conn(&self.conn, namespace_id, inode_id)
    }

    pub fn load_bound_apply_remote_rename_views(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<(RemoteFileStateRow, LocalFileStateRow, SyncAnchorRow), StateDbError> {
        load_bound_apply_remote_rename_views_from_conn(&self.conn, namespace_id, inode_id)
    }

    pub fn load_bound_apply_remote_delete_views(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<(RemoteFileStateRow, LocalFileStateRow, SyncAnchorRow), StateDbError> {
        load_bound_apply_remote_delete_views_from_conn(&self.conn, namespace_id, inode_id)
    }

    pub fn assess_remote_subtree_delete(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<RemoteSubtreeDeleteAssessment, StateDbError> {
        assess_remote_subtree_delete_from_conn(&self.conn, namespace_id, inode_id)
    }

    pub fn assess_remote_subtree_rename(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<RemoteSubtreeRenameAssessment, StateDbError> {
        assess_remote_subtree_rename_from_conn(&self.conn, namespace_id, inode_id)
    }

    pub fn load_bound_apply_remote_subtree_delete_views(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<BoundApplyRemoteSubtreeDeleteViews, StateDbError> {
        load_bound_apply_remote_subtree_delete_views_from_conn(&self.conn, namespace_id, inode_id)
    }

    pub fn load_bound_apply_remote_subtree_rename_views(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<BoundApplyRemoteSubtreeRenameViews, StateDbError> {
        load_bound_apply_remote_subtree_rename_views_from_conn(&self.conn, namespace_id, inode_id)
    }

    pub fn load_bound_resolve_subtree_delete_conflict_views(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<BoundResolveSubtreeDeleteConflictViews, StateDbError> {
        load_bound_resolve_subtree_delete_conflict_views_from_conn(
            &self.conn,
            namespace_id,
            inode_id,
        )
    }

    pub fn load_bound_resolve_subtree_rename_conflict_views(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<BoundResolveSubtreeRenameConflictViews, StateDbError> {
        load_bound_resolve_subtree_rename_conflict_views_from_conn(
            &self.conn,
            namespace_id,
            inode_id,
        )
    }

    pub fn load_planned_action(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<Option<PlannedActionRow>, StateDbError> {
        load_planned_action(&self.conn, namespace_id, inode_id)
    }

    pub fn load_next_planned_action(&self) -> Result<Option<PlannedActionRow>, StateDbError> {
        load_next_planned_action(&self.conn)
    }

    pub fn load_next_executable_planned_action(
        &self,
    ) -> Result<Option<PlannedActionRow>, StateDbError> {
        load_next_executable_planned_action(&self.conn)
    }

    pub fn load_next_deferred_planned_action(
        &self,
    ) -> Result<Option<PlannedActionRow>, StateDbError> {
        load_next_deferred_planned_action(&self.conn)
    }

    pub fn allocate_local_file_id(
        &mut self,
        namespace_id: &NamespaceId,
    ) -> Result<ClientFileId, StateDbError> {
        self.planner_transaction("allocate_local_file_id", |tx| {
            tx.allocate_local_file_id(namespace_id)
        })
    }

    pub fn allocate_client_request_id(&mut self) -> Result<String, StateDbError> {
        self.planner_transaction("allocate_client_request_id", |tx| {
            tx.allocate_client_request_id()
        })
    }

    pub fn load_local_only_file(
        &self,
        client_file_id: &ClientFileId,
    ) -> Result<Option<LocalOnlyFileStateRow>, StateDbError> {
        load_local_only_file(&self.conn, client_file_id)
    }

    pub fn load_local_only_candidates_for_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Vec<LocalOnlyFileStateRow>, StateDbError> {
        load_local_only_candidates_for_namespace(&self.conn, namespace_id)
    }

    pub fn load_local_only_parent_links_for_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Vec<LocalOnlyParentLinkRow>, StateDbError> {
        load_local_only_parent_links_for_namespace(&self.conn, namespace_id)
    }

    pub fn load_planned_local_only_action(
        &self,
        client_file_id: &ClientFileId,
    ) -> Result<Option<LocalOnlyPlannedActionRow>, StateDbError> {
        load_planned_local_only_action(&self.conn, client_file_id)
    }

    pub fn load_next_planned_local_only_action(
        &self,
    ) -> Result<Option<LocalOnlyPlannedActionRow>, StateDbError> {
        load_next_planned_local_only_action(&self.conn)
    }

    pub fn load_next_runnable_planned_local_only_action(
        &self,
    ) -> Result<Option<LocalOnlyPlannedActionRow>, StateDbError> {
        load_next_runnable_planned_local_only_action(&self.conn)
    }

    pub fn load_local_only_upload(
        &self,
        client_file_id: &ClientFileId,
    ) -> Result<Option<LocalOnlyUploadRow>, StateDbError> {
        load_local_only_upload(&self.conn, client_file_id)
    }

    pub fn load_local_only_transfer_ledger(
        &self,
        client_file_id: &ClientFileId,
        direction: TransferDirection,
    ) -> Result<Option<LocalOnlyTransferLedgerRow>, StateDbError> {
        load_local_only_transfer_ledger(&self.conn, client_file_id, direction)
    }

    pub fn load_inode_upload(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<Option<InodeUploadRow>, StateDbError> {
        load_inode_upload(&self.conn, namespace_id, inode_id)
    }

    pub fn load_pending_client_mutation(
        &self,
        client_request_id: &str,
    ) -> Result<Option<PendingClientMutationRow>, StateDbError> {
        load_pending_client_mutation(&self.conn, client_request_id)
    }

    pub fn load_pending_client_mutation_for_client_file(
        &self,
        client_file_id: &ClientFileId,
    ) -> Result<Option<PendingClientMutationRow>, StateDbError> {
        load_pending_client_mutation_for_client_file(&self.conn, client_file_id)
    }

    pub fn load_pending_inode_mutation(
        &self,
        client_request_id: &str,
    ) -> Result<Option<PendingInodeMutationRow>, StateDbError> {
        load_pending_inode_mutation(&self.conn, client_request_id)
    }

    pub fn load_pending_inode_mutation_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<Option<PendingInodeMutationRow>, StateDbError> {
        load_pending_inode_mutation_for_inode(&self.conn, namespace_id, inode_id)
    }

    pub fn load_conflicts_and_errors(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<Vec<ConflictOrErrorRow>, StateDbError> {
        load_conflicts_and_errors(&self.conn, namespace_id, inode_id)
    }

    pub fn load_conflict_artifact(
        &self,
        namespace_id: &NamespaceId,
        conflict_id: &str,
    ) -> Result<Option<ConflictArtifactRow>, StateDbError> {
        load_conflict_artifact(&self.conn, namespace_id, conflict_id)
    }

    pub fn load_conflict_artifacts_for_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Vec<ConflictArtifactRow>, StateDbError> {
        load_conflict_artifacts_for_namespace(&self.conn, namespace_id)
    }

    pub fn load_conflict_artifact_archive(
        &self,
        namespace_id: &NamespaceId,
        conflict_id: &str,
    ) -> Result<Option<ConflictArtifactArchiveRow>, StateDbError> {
        load_conflict_artifact_archive(&self.conn, namespace_id, conflict_id)
    }

    pub fn load_conflict_artifact_archives_for_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Vec<ConflictArtifactArchiveRow>, StateDbError> {
        load_conflict_artifact_archives_for_namespace(&self.conn, namespace_id)
    }

    pub fn load_local_only_conflicts_and_errors(
        &self,
        client_file_id: &ClientFileId,
    ) -> Result<Vec<LocalOnlyConflictOrErrorRow>, StateDbError> {
        load_local_only_conflicts_and_errors(&self.conn, client_file_id)
    }

    pub fn load_transfer_ledger_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        direction: TransferDirection,
    ) -> Result<Option<TransferLedgerRow>, StateDbError> {
        load_transfer_ledger_for_inode(&self.conn, namespace_id, inode_id, direction)
    }

    pub fn load_namespace_state_summary(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<ClientNamespaceStateSummary, StateDbError> {
        Ok(ClientNamespaceStateSummary {
            namespace_id: namespace_id.clone(),
            remote_state: load_remote_state_for_namespace(&self.conn, namespace_id)?,
            local_state: load_local_state_for_namespace(&self.conn, namespace_id)?,
            sync_anchors: load_sync_anchors_for_namespace(&self.conn, namespace_id)?,
            local_only_state: load_local_only_state_for_namespace(&self.conn, namespace_id)?,
            planned_actions: load_planned_actions_for_namespace(&self.conn, namespace_id)?,
            local_only_planned_actions: load_local_only_planned_actions_for_namespace(
                &self.conn,
                namespace_id,
            )?,
            pending_client_mutations: load_pending_client_mutations_for_namespace(
                &self.conn,
                namespace_id,
            )?,
            pending_inode_mutations: load_pending_inode_mutations_for_namespace(
                &self.conn,
                namespace_id,
            )?,
            transfer_ledgers: load_transfer_ledgers_for_namespace(&self.conn, namespace_id)?,
            local_only_transfer_ledgers: load_local_only_transfer_ledgers_for_namespace(
                &self.conn,
                namespace_id,
            )?,
            conflicts_and_errors: load_conflicts_and_errors_for_namespace(
                &self.conn,
                namespace_id,
            )?,
            local_only_conflicts_and_errors: load_local_only_conflicts_and_errors_for_namespace(
                &self.conn,
                namespace_id,
            )?,
        })
    }

    pub fn observe_local_only_inode_under_parent(
        &mut self,
        observed: &ObservedLocalOnlyInode,
    ) -> Result<LocalOnlyFileStateRow, StateDbError> {
        self.planner_transaction("observe_local_only_inode_under_parent", |tx| {
            tx.observe_local_only_inode_under_parent(observed)
        })
    }

    pub fn observe_bound_inode_and_plan(
        &mut self,
        observed: &ObservedBoundInode,
        planned_at_ms: u64,
    ) -> Result<crate::planner::PlannedActionRecord, StateDbError> {
        self.planner_transaction("observe_bound_inode_and_plan", |tx| {
            tx.observe_bound_inode_and_plan(observed, planned_at_ms)
        })
    }

    pub fn observe_local_only_inode_under_parent_and_plan(
        &mut self,
        observed: &ObservedLocalOnlyInode,
        planned_at_ms: u64,
    ) -> Result<ObservedLocalOnlyInodeResult, StateDbError> {
        self.planner_transaction("observe_local_only_inode_under_parent_and_plan", |tx| {
            tx.observe_local_only_inode_under_parent_and_plan(observed, planned_at_ms)
        })
    }

    pub fn observe_local_only_inode_under_parent_ref_and_plan(
        &mut self,
        observed: &ObservedLocalOnlySubtreeInode,
        planned_at_ms: u64,
    ) -> Result<ObservedLocalOnlyInodeResult, StateDbError> {
        self.planner_transaction("observe_local_only_inode_under_parent_ref_and_plan", |tx| {
            tx.observe_local_only_inode_under_parent_ref_and_plan(observed, planned_at_ms)
        })
    }

    pub fn observe_local_only_move_and_plan(
        &mut self,
        client_file_id: &ClientFileId,
        new_parent: &LocalOnlyParentRef,
        inode_kind: InodeKind,
        new_display_name: &str,
        content_digest: Option<String>,
        exists_on_disk: bool,
        dirty: bool,
        last_local_change_ms: u64,
        planned_at_ms: u64,
    ) -> Result<ObservedLocalOnlyInodeResult, StateDbError> {
        self.planner_transaction("observe_local_only_move_and_plan", |tx| {
            tx.observe_local_only_move_and_plan(
                client_file_id,
                new_parent,
                inode_kind,
                new_display_name,
                content_digest,
                exists_on_disk,
                dirty,
                last_local_change_ms,
                planned_at_ms,
            )
        })
    }

    pub fn observe_local_only_delete(
        &mut self,
        client_file_id: &ClientFileId,
    ) -> Result<ObservedLocalOnlyDeleteResult, StateDbError> {
        self.planner_transaction("observe_local_only_delete", |tx| {
            tx.observe_local_only_delete(client_file_id)
        })
    }

    pub fn observe_subtree_and_plan(
        &mut self,
        operations: &[SubtreeObservationOp],
        planned_at_ms: u64,
    ) -> Result<Vec<SubtreeObservationOutcome>, StateDbError> {
        self.planner_transaction("observe_subtree_and_plan", |tx| {
            tx.observe_subtree_and_plan(operations, planned_at_ms)
        })
    }

    pub fn bind_local_only_file_to_remote(
        &mut self,
        client_file_id: &ClientFileId,
        remote: &RemoteFileStateRow,
    ) -> Result<BoundLocalOnlyFile, StateDbError> {
        self.bind_local_only_inode_to_remote(client_file_id, remote)
    }

    pub fn bind_local_only_inode_to_remote(
        &mut self,
        client_file_id: &ClientFileId,
        remote: &RemoteFileStateRow,
    ) -> Result<BoundLocalOnlyFile, StateDbError> {
        self.planner_transaction("bind_local_only_inode_to_remote", |tx| {
            tx.bind_local_only_inode_to_remote(client_file_id, remote)
        })
    }

    pub fn record_local_only_upload(
        &mut self,
        client_file_id: &ClientFileId,
        uploaded: &UploadedContent,
        uploaded_at_ms: u64,
    ) -> Result<LocalOnlyUploadRow, StateDbError> {
        self.planner_transaction("record_local_only_upload", |tx| {
            tx.record_local_only_upload(client_file_id, uploaded, uploaded_at_ms)
        })
    }

    pub fn record_inode_upload(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        uploaded: &UploadedContent,
        uploaded_at_ms: u64,
    ) -> Result<InodeUploadRow, StateDbError> {
        self.planner_transaction("record_inode_upload", |tx| {
            tx.record_inode_upload(namespace_id, inode_id, uploaded, uploaded_at_ms)
        })
    }

    pub fn resolve_local_only_upload_content_manifest_digest(
        &self,
        local_only: &LocalOnlyFileStateRow,
    ) -> Result<String, StateDbError> {
        let upload = self
            .load_local_only_upload(&local_only.client_file_id)?
            .ok_or_else(|| StateDbError::UploadedContentMissing {
                client_file_id: local_only.client_file_id.as_str().to_owned(),
            })?;
        validate_local_only_upload(local_only, &upload.namespace_id, &upload.file_digest_sha256)?;
        Ok(upload.content_manifest_digest)
    }

    pub fn resolve_inode_upload_content_manifest_digest(
        &self,
        local: &LocalFileStateRow,
    ) -> Result<String, StateDbError> {
        let upload = self
            .load_inode_upload(&local.namespace_id, local.inode_id)?
            .ok_or_else(|| StateDbError::InodeUploadMissing {
                namespace_id: local.namespace_id.as_str().to_owned(),
                inode_id: local.inode_id.0,
            })?;
        validate_inode_upload(local, &upload.namespace_id, &upload.file_digest_sha256)?;
        Ok(upload.content_manifest_digest)
    }

    pub fn record_pending_client_mutation(
        &mut self,
        client_file_id: &ClientFileId,
        request: &ClientMutationRequest,
        created_at_ms: u64,
    ) -> Result<PendingClientMutationRow, StateDbError> {
        self.planner_transaction("record_pending_client_mutation", |tx| {
            tx.record_pending_client_mutation(client_file_id, request, created_at_ms)
        })
    }

    pub fn record_pending_inode_mutation(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        request: &ClientMutationRequest,
        created_at_ms: u64,
    ) -> Result<PendingInodeMutationRow, StateDbError> {
        self.planner_transaction("record_pending_inode_mutation", |tx| {
            tx.record_pending_inode_mutation(namespace_id, inode_id, request, created_at_ms)
        })
    }

    pub fn apply_client_mutation_response(
        &mut self,
        response: &ClientMutationResponse,
    ) -> Result<BoundLocalOnlyFile, StateDbError> {
        self.planner_transaction("apply_client_mutation_response", |tx| {
            tx.apply_client_mutation_response(response)
        })
    }

    pub fn apply_inode_mutation_response(
        &mut self,
        response: &ClientMutationResponse,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        self.planner_transaction("apply_inode_mutation_response", |tx| {
            tx.apply_inode_mutation_response(response)
        })
    }

    pub fn apply_download_remote_edit(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        self.planner_transaction("apply_download_remote_edit", |tx| {
            tx.apply_download_remote_edit(namespace_id, inode_id, applied_at_ms)
        })
    }

    pub fn apply_materialize_remote_dir(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        self.planner_transaction("apply_materialize_remote_dir", |tx| {
            tx.apply_materialize_remote_dir(namespace_id, inode_id, applied_at_ms)
        })
    }

    pub fn apply_remote_rename(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        self.planner_transaction("apply_remote_rename", |tx| {
            tx.apply_remote_rename(namespace_id, inode_id, applied_at_ms)
        })
    }

    pub fn apply_same_inode_conflict_resolution(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        self.planner_transaction("apply_same_inode_conflict_resolution", |tx| {
            tx.apply_same_inode_conflict_resolution(namespace_id, inode_id, applied_at_ms)
        })
    }

    pub fn apply_delete_vs_edit_conflict_resolution(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        self.planner_transaction("apply_delete_vs_edit_conflict_resolution", |tx| {
            tx.apply_delete_vs_edit_conflict_resolution(namespace_id, inode_id, applied_at_ms)
        })
    }

    pub fn apply_rename_vs_edit_conflict_resolution(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        self.planner_transaction("apply_rename_vs_edit_conflict_resolution", |tx| {
            tx.apply_rename_vs_edit_conflict_resolution(namespace_id, inode_id, applied_at_ms)
        })
    }

    pub fn apply_remote_rename_and_replace(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        self.planner_transaction("apply_remote_rename_and_replace", |tx| {
            tx.apply_remote_rename_and_replace(namespace_id, inode_id, applied_at_ms)
        })
    }

    pub fn apply_path_binding_collision_resolution(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        client_file_id: &ClientFileId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        self.planner_transaction("apply_path_binding_collision_resolution", |tx| {
            tx.apply_path_binding_collision_resolution(
                namespace_id,
                inode_id,
                client_file_id,
                applied_at_ms,
            )
        })
    }

    pub fn apply_remote_delete(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        self.planner_transaction("apply_remote_delete", |tx| {
            tx.apply_remote_delete(namespace_id, inode_id, applied_at_ms)
        })
    }

    pub fn apply_remote_subtree_delete(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        self.planner_transaction("apply_remote_subtree_delete", |tx| {
            tx.apply_remote_subtree_delete(namespace_id, inode_id, applied_at_ms)
        })
    }

    pub fn apply_remote_subtree_rename(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        self.planner_transaction("apply_remote_subtree_rename", |tx| {
            tx.apply_remote_subtree_rename(namespace_id, inode_id, applied_at_ms)
        })
    }

    pub fn apply_resolved_subtree_delete_conflict(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        self.planner_transaction("apply_resolved_subtree_delete_conflict", |tx| {
            tx.apply_resolved_subtree_delete_conflict(namespace_id, inode_id, applied_at_ms)
        })
    }

    pub fn apply_resolved_subtree_rename_conflict(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        self.planner_transaction("apply_resolved_subtree_rename_conflict", |tx| {
            tx.apply_resolved_subtree_rename_conflict(namespace_id, inode_id, applied_at_ms)
        })
    }

    pub fn apply_remote_observation(
        &mut self,
        observed: &ObservedRemoteInode,
        applied_at_ms: u64,
    ) -> Result<AppliedRemoteObservation, StateDbError> {
        self.planner_transaction("apply_remote_observation", |tx| {
            let outcome = tx.apply_remote_observation(observed, applied_at_ms)?;
            tx.replan_after_remote_observation(&outcome, applied_at_ms)?;
            Ok(outcome)
        })
    }

    pub fn apply_remote_observations_batch(
        &mut self,
        observations: &[ObservedRemoteInode],
        applied_at_ms: u64,
    ) -> Result<Vec<AppliedRemoteObservation>, StateDbError> {
        self.planner_transaction("apply_remote_observations_batch", |tx| {
            let mut outcomes = Vec::with_capacity(observations.len());
            let expected_namespace_id = observations.first().map(|observed| &observed.namespace_id);
            for (index, observed) in observations.iter().enumerate() {
                if let Some(expected_namespace_id) = expected_namespace_id {
                    if &observed.namespace_id != expected_namespace_id {
                        return Err(StateDbError::RemoteObservationBatchNamespaceMismatch {
                            expected_namespace_id: expected_namespace_id.as_str().to_owned(),
                            actual_namespace_id: observed.namespace_id.as_str().to_owned(),
                            index,
                        });
                    }
                }
                let outcome = tx.apply_remote_observation(observed, applied_at_ms)?;
                tx.replan_after_remote_observation(&outcome, applied_at_ms)?;
                outcomes.push(outcome);
            }
            Ok(outcomes)
        })
    }

    pub(crate) fn record_conflict_or_error(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        kind: &str,
        summary: &str,
        detail_json: &serde_json::Value,
        created_at_ms: u64,
    ) -> Result<ConflictOrErrorRow, StateDbError> {
        self.planner_transaction("record_conflict_or_error", |tx| {
            tx.record_conflict_or_error(
                namespace_id,
                inode_id,
                kind,
                summary,
                detail_json,
                created_at_ms,
            )
        })
    }

    pub(crate) fn record_local_only_conflict_or_error(
        &mut self,
        client_file_id: &ClientFileId,
        namespace_id: &NamespaceId,
        kind: &str,
        summary: &str,
        detail_json: &serde_json::Value,
        created_at_ms: u64,
    ) -> Result<LocalOnlyConflictOrErrorRow, StateDbError> {
        self.planner_transaction("record_local_only_conflict_or_error", |tx| {
            tx.record_local_only_conflict_or_error(
                client_file_id,
                namespace_id,
                kind,
                summary,
                detail_json,
                created_at_ms,
            )
        })
    }

    pub(crate) fn clear_conflict_or_error_kind(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        kind: &str,
    ) -> Result<(), StateDbError> {
        self.planner_transaction("clear_conflict_or_error_kind", |tx| {
            tx.delete_conflict_or_error_kind(namespace_id, inode_id, kind)
        })
    }

    pub(crate) fn clear_local_only_conflict_or_error_kind(
        &mut self,
        client_file_id: &ClientFileId,
        kind: &str,
    ) -> Result<(), StateDbError> {
        self.planner_transaction("clear_local_only_conflict_or_error_kind", |tx| {
            tx.delete_local_only_conflict_or_error_kind(client_file_id, kind)
        })
    }

    pub fn upsert_transfer_ledger(
        &mut self,
        row: &TransferLedgerRow,
    ) -> Result<TransferLedgerRow, StateDbError> {
        self.planner_transaction("upsert_transfer_ledger", |tx| {
            tx.upsert_transfer_ledger(row)
        })
    }

    pub fn upsert_local_only_transfer_ledger(
        &mut self,
        row: &LocalOnlyTransferLedgerRow,
    ) -> Result<LocalOnlyTransferLedgerRow, StateDbError> {
        self.planner_transaction("upsert_local_only_transfer_ledger", |tx| {
            tx.upsert_local_only_transfer_ledger(row)
        })
    }

    pub fn delete_transfer_ledger_for_inode(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        direction: TransferDirection,
    ) -> Result<(), StateDbError> {
        self.planner_transaction("delete_transfer_ledger_for_inode", |tx| {
            tx.delete_transfer_ledger_for_inode(namespace_id, inode_id, direction)
        })
    }

    pub fn delete_local_only_transfer_ledger(
        &mut self,
        client_file_id: &ClientFileId,
        direction: TransferDirection,
    ) -> Result<(), StateDbError> {
        self.planner_transaction("delete_local_only_transfer_ledger", |tx| {
            tx.delete_local_only_transfer_ledger(client_file_id, direction)
        })
    }
}

impl PlannerTxn<'_> {
    fn replan_after_remote_observation(
        &mut self,
        outcome: &AppliedRemoteObservation,
        now_ms: u64,
    ) -> Result<(), StateDbError> {
        let target = match outcome {
            AppliedRemoteObservation::BoundLocalOnly(bound) => {
                Some((&bound.namespace_id, bound.inode_id))
            }
            AppliedRemoteObservation::ConvergedBoundInode(applied) => {
                Some((&applied.namespace_id, applied.inode_id))
            }
            AppliedRemoteObservation::DiscoveredRemoteOnly {
                namespace_id,
                inode_id,
            }
            | AppliedRemoteObservation::UpdatedBoundRemoteState {
                namespace_id,
                inode_id,
            } => Some((namespace_id, *inode_id)),
            AppliedRemoteObservation::RecordedConflictOrError { .. }
            | AppliedRemoteObservation::IgnoredStale { .. }
            | AppliedRemoteObservation::IgnoredUnmatched { .. } => None,
        };

        if let Some((namespace_id, inode_id)) = target {
            let _ = plan_file_in_tx(self, namespace_id, inode_id, now_ms)?;
        }

        Ok(())
    }

    pub fn load_planned_action(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<Option<PlannedActionRow>, StateDbError> {
        load_planned_action(&self.tx, namespace_id, inode_id)
    }

    pub fn load_planned_local_only_action(
        &self,
        client_file_id: &ClientFileId,
    ) -> Result<Option<LocalOnlyPlannedActionRow>, StateDbError> {
        load_planned_local_only_action(&self.tx, client_file_id)
    }

    pub fn load_local_only_parent_links_for_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Vec<LocalOnlyParentLinkRow>, StateDbError> {
        load_local_only_parent_links_for_namespace(&self.tx, namespace_id)
    }

    pub fn load_namespace_state_summary(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<ClientNamespaceStateSummary, StateDbError> {
        Ok(ClientNamespaceStateSummary {
            namespace_id: namespace_id.clone(),
            remote_state: load_remote_state_for_namespace(&self.tx, namespace_id)?,
            local_state: load_local_state_for_namespace(&self.tx, namespace_id)?,
            sync_anchors: load_sync_anchors_for_namespace(&self.tx, namespace_id)?,
            local_only_state: load_local_only_state_for_namespace(&self.tx, namespace_id)?,
            planned_actions: load_planned_actions_for_namespace(&self.tx, namespace_id)?,
            local_only_planned_actions: load_local_only_planned_actions_for_namespace(
                &self.tx,
                namespace_id,
            )?,
            pending_client_mutations: load_pending_client_mutations_for_namespace(
                &self.tx,
                namespace_id,
            )?,
            pending_inode_mutations: load_pending_inode_mutations_for_namespace(
                &self.tx,
                namespace_id,
            )?,
            transfer_ledgers: load_transfer_ledgers_for_namespace(&self.tx, namespace_id)?,
            local_only_transfer_ledgers: load_local_only_transfer_ledgers_for_namespace(
                &self.tx,
                namespace_id,
            )?,
            conflicts_and_errors: load_conflicts_and_errors_for_namespace(&self.tx, namespace_id)?,
            local_only_conflicts_and_errors: load_local_only_conflicts_and_errors_for_namespace(
                &self.tx,
                namespace_id,
            )?,
        })
    }

    pub fn load_pending_inode_mutation_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<Option<PendingInodeMutationRow>, StateDbError> {
        load_pending_inode_mutation_for_inode(&self.tx, namespace_id, inode_id)
    }

    pub fn load_transfer_ledger_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        direction: TransferDirection,
    ) -> Result<Option<TransferLedgerRow>, StateDbError> {
        load_transfer_ledger_for_inode(&self.tx, namespace_id, inode_id, direction)
    }

    pub fn allocate_local_file_id(
        &mut self,
        namespace_id: &NamespaceId,
    ) -> Result<ClientFileId, StateDbError> {
        let next_counter = self.tx.query_row(
            "SELECT value_integer FROM client_metadata WHERE key = 'next_local_file_id'",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        let next_counter = from_sql_u64(next_counter, "next_local_file_id")?;

        self.tx.execute(
            "UPDATE client_metadata
            SET value_integer = ?1
            WHERE key = 'next_local_file_id'",
            params![to_sql_u64(
                next_counter.saturating_add(1),
                "next_local_file_id"
            )?],
        )?;

        Ok(ClientFileId::new(format!(
            "tmp:{}:{next_counter:020}",
            namespace_id.as_str()
        )))
    }

    pub fn allocate_client_request_id(&mut self) -> Result<String, StateDbError> {
        let next_counter = self.tx.query_row(
            "SELECT value_integer FROM client_metadata WHERE key = 'next_client_request_id'",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        let next_counter = from_sql_u64(next_counter, "next_client_request_id")?;

        self.tx.execute(
            "UPDATE client_metadata
            SET value_integer = ?1
            WHERE key = 'next_client_request_id'",
            params![to_sql_u64(
                next_counter.saturating_add(1),
                "next_client_request_id"
            )?],
        )?;

        Ok(format!("client-req-{next_counter:020}"))
    }

    pub fn record_conflict_or_error(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        kind: &str,
        summary: &str,
        detail_json: &serde_json::Value,
        created_at_ms: u64,
    ) -> Result<ConflictOrErrorRow, StateDbError> {
        self.delete_conflict_or_error_kind(namespace_id, inode_id, kind)?;
        let detail_json_text =
            serde_json::to_string(detail_json).map_err(StateDbError::ConflictOrErrorDetailCodec)?;
        self.tx.execute(
            "INSERT INTO conflicts_and_errors (
                namespace_id,
                inode_id,
                kind,
                summary,
                detail_json,
                created_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                namespace_id.as_str(),
                to_sql_u64(inode_id.0, "inode_id")?,
                kind,
                summary,
                detail_json_text,
                to_sql_u64(created_at_ms, "created_at_ms")?,
            ],
        )?;

        Ok(ConflictOrErrorRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            record_id: from_sql_u64(self.tx.last_insert_rowid(), "record_id")?,
            kind: kind.to_owned(),
            summary: summary.to_owned(),
            detail_json: detail_json.clone(),
            created_at_ms,
        })
    }

    pub fn record_local_only_conflict_or_error(
        &mut self,
        client_file_id: &ClientFileId,
        namespace_id: &NamespaceId,
        kind: &str,
        summary: &str,
        detail_json: &serde_json::Value,
        created_at_ms: u64,
    ) -> Result<LocalOnlyConflictOrErrorRow, StateDbError> {
        self.delete_local_only_conflict_or_error_kind(client_file_id, kind)?;
        let detail_json_text =
            serde_json::to_string(detail_json).map_err(StateDbError::ConflictOrErrorDetailCodec)?;
        self.tx.execute(
            "INSERT INTO local_only_conflicts_and_errors (
                client_file_id,
                namespace_id,
                kind,
                summary,
                detail_json,
                created_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                client_file_id.as_str(),
                namespace_id.as_str(),
                kind,
                summary,
                detail_json_text,
                to_sql_u64(created_at_ms, "created_at_ms")?,
            ],
        )?;

        Ok(LocalOnlyConflictOrErrorRow {
            client_file_id: client_file_id.clone(),
            namespace_id: namespace_id.clone(),
            record_id: from_sql_u64(self.tx.last_insert_rowid(), "record_id")?,
            kind: kind.to_owned(),
            summary: summary.to_owned(),
            detail_json: detail_json.clone(),
            created_at_ms,
        })
    }

    pub fn delete_conflict_or_error_kind(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        kind: &str,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "DELETE FROM conflicts_and_errors
            WHERE namespace_id = ?1 AND inode_id = ?2 AND kind = ?3",
            params![
                namespace_id.as_str(),
                to_sql_u64(inode_id.0, "inode_id")?,
                kind,
            ],
        )?;
        Ok(())
    }

    pub fn delete_local_only_conflict_or_error_kind(
        &mut self,
        client_file_id: &ClientFileId,
        kind: &str,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "DELETE FROM local_only_conflicts_and_errors
            WHERE client_file_id = ?1 AND kind = ?2",
            params![client_file_id.as_str(), kind],
        )?;
        Ok(())
    }

    pub fn delete_local_only_conflicts_and_errors(
        &mut self,
        client_file_id: &ClientFileId,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "DELETE FROM local_only_conflicts_and_errors
            WHERE client_file_id = ?1",
            params![client_file_id.as_str()],
        )?;
        Ok(())
    }

    pub fn upsert_remote_file(&mut self, row: &RemoteFileStateRow) -> Result<(), StateDbError> {
        self.tx.execute(
            "INSERT INTO remote_state (
                namespace_id,
                inode_id,
                inode_kind,
                observed_seq,
                revision_no,
                content_digest,
                content_manifest_digest,
                parent_inode_id,
                display_name,
                is_deleted
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
            ON CONFLICT(namespace_id, inode_id) DO UPDATE SET
                inode_kind = excluded.inode_kind,
                observed_seq = excluded.observed_seq,
                revision_no = excluded.revision_no,
                content_digest = excluded.content_digest,
                content_manifest_digest = excluded.content_manifest_digest,
                parent_inode_id = excluded.parent_inode_id,
                display_name = excluded.display_name,
                is_deleted = excluded.is_deleted",
            params![
                row.namespace_id.as_str(),
                to_sql_u64(row.inode_id.0, "inode_id")?,
                inode_kind_as_str(&row.inode_kind),
                to_sql_u64(row.observed_seq.0, "observed_seq")?,
                to_sql_u64(row.revision_no.0, "revision_no")?,
                row.content_digest.as_deref(),
                row.content_manifest_digest.as_deref(),
                row.parent_inode_id
                    .map(|inode_id| to_sql_u64(inode_id.0, "parent_inode_id"))
                    .transpose()?,
                &row.display_name,
                row.is_deleted,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_local_file(&mut self, row: &LocalFileStateRow) -> Result<(), StateDbError> {
        self.tx.execute(
            "INSERT INTO local_state (
                namespace_id,
                inode_id,
                inode_kind,
                content_digest,
                parent_inode_id,
                display_name,
                exists_on_disk,
                dirty,
                last_local_change_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(namespace_id, inode_id) DO UPDATE SET
                inode_kind = excluded.inode_kind,
                content_digest = excluded.content_digest,
                parent_inode_id = excluded.parent_inode_id,
                display_name = excluded.display_name,
                exists_on_disk = excluded.exists_on_disk,
                dirty = excluded.dirty,
                last_local_change_ms = excluded.last_local_change_ms",
            params![
                row.namespace_id.as_str(),
                to_sql_u64(row.inode_id.0, "inode_id")?,
                inode_kind_as_str(&row.inode_kind),
                row.content_digest.as_deref(),
                row.parent_inode_id
                    .map(|inode_id| to_sql_u64(inode_id.0, "parent_inode_id"))
                    .transpose()?,
                &row.display_name,
                row.exists_on_disk,
                row.dirty,
                to_sql_u64(row.last_local_change_ms, "last_local_change_ms")?,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_sync_anchor(&mut self, row: &SyncAnchorRow) -> Result<(), StateDbError> {
        self.tx.execute(
            "INSERT INTO sync_anchor (
                namespace_id,
                inode_id,
                inode_kind,
                synced_seq,
                revision_no,
                content_digest,
                content_manifest_digest,
                parent_inode_id,
                display_name
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(namespace_id, inode_id) DO UPDATE SET
                inode_kind = excluded.inode_kind,
                synced_seq = excluded.synced_seq,
                revision_no = excluded.revision_no,
                content_digest = excluded.content_digest,
                content_manifest_digest = excluded.content_manifest_digest,
                parent_inode_id = excluded.parent_inode_id,
                display_name = excluded.display_name",
            params![
                row.namespace_id.as_str(),
                to_sql_u64(row.inode_id.0, "inode_id")?,
                inode_kind_as_str(&row.inode_kind),
                to_sql_u64(row.synced_seq.0, "synced_seq")?,
                to_sql_u64(row.revision_no.0, "revision_no")?,
                row.content_digest.as_deref(),
                row.content_manifest_digest.as_deref(),
                row.parent_inode_id
                    .map(|inode_id| to_sql_u64(inode_id.0, "parent_inode_id"))
                    .transpose()?,
                &row.display_name,
            ],
        )?;
        Ok(())
    }

    pub fn delete_local_file(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "DELETE FROM local_state WHERE namespace_id = ?1 AND inode_id = ?2",
            params![namespace_id.as_str(), to_sql_u64(inode_id.0, "inode_id")?],
        )?;
        Ok(())
    }

    pub fn delete_sync_anchor(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "DELETE FROM sync_anchor WHERE namespace_id = ?1 AND inode_id = ?2",
            params![namespace_id.as_str(), to_sql_u64(inode_id.0, "inode_id")?],
        )?;
        Ok(())
    }

    pub fn delete_local_files_for_inodes(
        &mut self,
        namespace_id: &NamespaceId,
        inode_ids: &[InodeId],
    ) -> Result<(), StateDbError> {
        for inode_id in inode_ids {
            self.delete_local_file(namespace_id, *inode_id)?;
        }
        Ok(())
    }

    pub fn delete_sync_anchors_for_inodes(
        &mut self,
        namespace_id: &NamespaceId,
        inode_ids: &[InodeId],
    ) -> Result<(), StateDbError> {
        for inode_id in inode_ids {
            self.delete_sync_anchor(namespace_id, *inode_id)?;
        }
        Ok(())
    }

    pub fn delete_remote_files_for_inodes(
        &mut self,
        namespace_id: &NamespaceId,
        inode_ids: &[InodeId],
    ) -> Result<(), StateDbError> {
        for inode_id in inode_ids {
            self.tx.execute(
                "DELETE FROM remote_state WHERE namespace_id = ?1 AND inode_id = ?2",
                params![namespace_id.as_str(), to_sql_u64(inode_id.0, "inode_id")?],
            )?;
        }
        Ok(())
    }

    pub fn upsert_local_only_file(
        &mut self,
        row: &LocalOnlyFileStateRow,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "INSERT INTO local_only_state (
                client_file_id,
                namespace_id,
                inode_kind,
                parent_inode_id,
                display_name,
                content_digest,
                exists_on_disk,
                dirty,
                last_local_change_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(client_file_id) DO UPDATE SET
                namespace_id = excluded.namespace_id,
                inode_kind = excluded.inode_kind,
                parent_inode_id = excluded.parent_inode_id,
                display_name = excluded.display_name,
                content_digest = excluded.content_digest,
                exists_on_disk = excluded.exists_on_disk,
                dirty = excluded.dirty,
                last_local_change_ms = excluded.last_local_change_ms",
            params![
                row.client_file_id.as_str(),
                row.namespace_id.as_str(),
                inode_kind_as_str(&row.inode_kind),
                row.parent_inode_id
                    .map(|inode_id| to_sql_u64(inode_id.0, "parent_inode_id"))
                    .transpose()?,
                &row.display_name,
                row.content_digest.as_deref(),
                row.exists_on_disk,
                row.dirty,
                to_sql_u64(row.last_local_change_ms, "last_local_change_ms")?,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_local_only_parent_link(
        &mut self,
        client_file_id: &ClientFileId,
        parent_client_file_id: &ClientFileId,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "INSERT INTO local_only_parent_links (
                client_file_id,
                parent_client_file_id
            ) VALUES (?1, ?2)
            ON CONFLICT(client_file_id) DO UPDATE SET
                parent_client_file_id = excluded.parent_client_file_id",
            params![client_file_id.as_str(), parent_client_file_id.as_str()],
        )?;
        Ok(())
    }

    pub fn delete_local_only_parent_link(
        &mut self,
        client_file_id: &ClientFileId,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "DELETE FROM local_only_parent_links WHERE client_file_id = ?1",
            params![client_file_id.as_str()],
        )?;
        Ok(())
    }

    pub fn load_local_only_parent_link(
        &self,
        client_file_id: &ClientFileId,
    ) -> Result<Option<ClientFileId>, StateDbError> {
        load_local_only_parent_link(&self.tx, client_file_id)
    }

    pub fn observe_local_only_inode_under_parent(
        &mut self,
        observed: &ObservedLocalOnlyInode,
    ) -> Result<LocalOnlyFileStateRow, StateDbError> {
        self.ensure_bound_parent_directory(&observed.namespace_id, observed.parent_inode_id)?;

        let client_file_id = self.allocate_local_file_id(&observed.namespace_id)?;
        let row = LocalOnlyFileStateRow {
            client_file_id,
            namespace_id: observed.namespace_id.clone(),
            inode_kind: observed.inode_kind.clone(),
            parent_inode_id: Some(observed.parent_inode_id),
            display_name: observed.display_name.clone(),
            content_digest: observed.content_digest.clone(),
            exists_on_disk: observed.exists_on_disk,
            dirty: observed.dirty,
            last_local_change_ms: observed.last_local_change_ms,
        };
        self.upsert_local_only_file(&row)?;
        self.delete_local_only_parent_link(&row.client_file_id)?;
        Ok(row)
    }

    pub fn observe_bound_inode_and_plan(
        &mut self,
        observed: &ObservedBoundInode,
        planned_at_ms: u64,
    ) -> Result<crate::planner::PlannedActionRecord, StateDbError> {
        let views = self.load_file_sync_views(&observed.namespace_id, observed.inode_id)?;
        if views.remote.is_none() && views.sync_anchor.is_none() {
            return Err(StateDbError::BoundObservationMissing {
                namespace_id: observed.namespace_id.as_str().to_owned(),
                inode_id: observed.inode_id.0,
            });
        }

        self.upsert_local_file(&LocalFileStateRow {
            namespace_id: observed.namespace_id.clone(),
            inode_id: observed.inode_id,
            inode_kind: observed.inode_kind.clone(),
            content_digest: observed.content_digest.clone(),
            parent_inode_id: observed.parent_inode_id,
            display_name: observed.display_name.clone(),
            exists_on_disk: observed.exists_on_disk,
            dirty: observed.dirty,
            last_local_change_ms: observed.last_local_change_ms,
        })?;

        plan_file_in_tx(
            self,
            &observed.namespace_id,
            observed.inode_id,
            planned_at_ms,
        )
    }

    pub fn observe_local_only_inode_under_parent_and_plan(
        &mut self,
        observed: &ObservedLocalOnlyInode,
        planned_at_ms: u64,
    ) -> Result<ObservedLocalOnlyInodeResult, StateDbError> {
        self.observe_local_only_inode_under_parent_ref_and_plan(
            &ObservedLocalOnlySubtreeInode {
                relative_path: String::new(),
                namespace_id: observed.namespace_id.clone(),
                inode_kind: observed.inode_kind.clone(),
                parent: SubtreeLocalOnlyParentRef::Bound {
                    parent_inode_id: observed.parent_inode_id,
                },
                display_name: observed.display_name.clone(),
                content_digest: observed.content_digest.clone(),
                exists_on_disk: observed.exists_on_disk,
                dirty: observed.dirty,
                last_local_change_ms: observed.last_local_change_ms,
            },
            planned_at_ms,
        )
    }

    pub fn observe_local_only_inode_under_parent_ref_and_plan(
        &mut self,
        observed: &ObservedLocalOnlySubtreeInode,
        planned_at_ms: u64,
    ) -> Result<ObservedLocalOnlyInodeResult, StateDbError> {
        let parent = match &observed.parent {
            SubtreeLocalOnlyParentRef::Bound { parent_inode_id } => LocalOnlyParentRef::Bound {
                parent_inode_id: *parent_inode_id,
            },
            SubtreeLocalOnlyParentRef::ExistingLocalOnly {
                parent_client_file_id,
            } => LocalOnlyParentRef::LocalOnly {
                parent_client_file_id: parent_client_file_id.clone(),
            },
            SubtreeLocalOnlyParentRef::BatchLocalOnly {
                parent_relative_path,
            } => {
                return Err(StateDbError::SubtreeObservationBatchParentMissing {
                    parent_relative_path: parent_relative_path.clone(),
                });
            }
        };

        self.observe_local_only_inode_under_resolved_parent_and_plan(
            observed,
            &parent,
            planned_at_ms,
        )
    }

    fn observe_local_only_inode_under_resolved_parent_and_plan(
        &mut self,
        observed: &ObservedLocalOnlySubtreeInode,
        parent: &LocalOnlyParentRef,
        planned_at_ms: u64,
    ) -> Result<ObservedLocalOnlyInodeResult, StateDbError> {
        self.ensure_local_only_parent_directory(&observed.namespace_id, parent)?;

        let sibling_matches = self.load_local_only_rows_by_parent_ref_and_name(
            &observed.namespace_id,
            parent,
            &observed.display_name,
        )?;

        let (client_file_id, reused_existing_identity) = match sibling_matches.as_slice() {
            [] => (self.allocate_local_file_id(&observed.namespace_id)?, false),
            [existing] => (existing.client_file_id.clone(), true),
            _ => match parent {
                LocalOnlyParentRef::Bound { parent_inode_id } => {
                    return Err(StateDbError::LocalOnlyObservationAmbiguous {
                        namespace_id: observed.namespace_id.as_str().to_owned(),
                        parent_inode_id: parent_inode_id.0,
                        display_name: observed.display_name.clone(),
                    })
                }
                LocalOnlyParentRef::LocalOnly {
                    parent_client_file_id,
                } => {
                    return Err(StateDbError::LocalOnlyMoveTargetOccupied {
                        namespace_id: observed.namespace_id.as_str().to_owned(),
                        display_name: format!(
                            "{}@{}",
                            observed.display_name,
                            parent_client_file_id.as_str()
                        ),
                    })
                }
            },
        };

        let row = LocalOnlyFileStateRow {
            client_file_id: client_file_id.clone(),
            namespace_id: observed.namespace_id.clone(),
            inode_kind: observed.inode_kind.clone(),
            parent_inode_id: match parent {
                LocalOnlyParentRef::Bound { parent_inode_id } => Some(*parent_inode_id),
                LocalOnlyParentRef::LocalOnly { .. } => None,
            },
            display_name: observed.display_name.clone(),
            content_digest: observed.content_digest.clone(),
            exists_on_disk: observed.exists_on_disk,
            dirty: observed.dirty,
            last_local_change_ms: observed.last_local_change_ms,
        };
        self.upsert_local_only_file(&row)?;
        match parent {
            LocalOnlyParentRef::Bound { .. } => {
                self.delete_local_only_parent_link(&row.client_file_id)?;
            }
            LocalOnlyParentRef::LocalOnly {
                parent_client_file_id,
            } => {
                self.upsert_local_only_parent_link(&row.client_file_id, parent_client_file_id)?;
            }
        }

        let action = plan_local_only_inode_in_tx(self, &client_file_id, planned_at_ms)?;

        Ok(ObservedLocalOnlyInodeResult {
            local_only: row,
            planned_action: action,
            reused_existing_identity,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn observe_local_only_move_and_plan(
        &mut self,
        client_file_id: &ClientFileId,
        new_parent: &LocalOnlyParentRef,
        inode_kind: InodeKind,
        new_display_name: &str,
        content_digest: Option<String>,
        exists_on_disk: bool,
        dirty: bool,
        last_local_change_ms: u64,
        planned_at_ms: u64,
    ) -> Result<ObservedLocalOnlyInodeResult, StateDbError> {
        let existing = self.load_local_only_file(client_file_id)?.ok_or_else(|| {
            StateDbError::LocalOnlyFileMissing {
                client_file_id: client_file_id.as_str().to_owned(),
            }
        })?;
        if existing.inode_kind != inode_kind {
            return Err(StateDbError::UnsupportedLocalOnlyInodeKind(inode_kind));
        }

        self.ensure_local_only_parent_directory(&existing.namespace_id, new_parent)?;
        let subtree_client_file_ids = self
            .collect_local_only_subtree_client_file_ids(&existing.namespace_id, client_file_id)?;
        if let LocalOnlyParentRef::LocalOnly {
            parent_client_file_id,
        } = new_parent
        {
            if subtree_client_file_ids.contains(parent_client_file_id) {
                return Err(StateDbError::LocalOnlyMoveParentCycle {
                    client_file_id: client_file_id.as_str().to_owned(),
                    parent_client_file_id: parent_client_file_id.as_str().to_owned(),
                });
            }
        }

        let sibling_matches = self.load_local_only_rows_by_parent_ref_and_name(
            &existing.namespace_id,
            new_parent,
            new_display_name,
        )?;
        if sibling_matches
            .iter()
            .any(|row| row.client_file_id != *client_file_id)
        {
            return Err(StateDbError::LocalOnlyMoveTargetOccupied {
                namespace_id: existing.namespace_id.as_str().to_owned(),
                display_name: new_display_name.to_owned(),
            });
        }

        let row = LocalOnlyFileStateRow {
            client_file_id: client_file_id.clone(),
            namespace_id: existing.namespace_id.clone(),
            inode_kind,
            parent_inode_id: match new_parent {
                LocalOnlyParentRef::Bound { parent_inode_id } => Some(*parent_inode_id),
                LocalOnlyParentRef::LocalOnly { .. } => None,
            },
            display_name: new_display_name.to_owned(),
            content_digest,
            exists_on_disk,
            dirty,
            last_local_change_ms,
        };
        self.upsert_local_only_file(&row)?;
        match new_parent {
            LocalOnlyParentRef::Bound { .. } => {
                self.delete_local_only_parent_link(client_file_id)?;
            }
            LocalOnlyParentRef::LocalOnly {
                parent_client_file_id,
            } => {
                self.upsert_local_only_parent_link(client_file_id, parent_client_file_id)?;
            }
        }

        let action = plan_local_only_inode_in_tx(self, client_file_id, planned_at_ms)?;

        Ok(ObservedLocalOnlyInodeResult {
            local_only: row,
            planned_action: action,
            reused_existing_identity: true,
        })
    }

    pub fn observe_local_only_delete(
        &mut self,
        client_file_id: &ClientFileId,
    ) -> Result<ObservedLocalOnlyDeleteResult, StateDbError> {
        let existing = self.load_local_only_file(client_file_id)?.ok_or_else(|| {
            StateDbError::LocalOnlyFileMissing {
                client_file_id: client_file_id.as_str().to_owned(),
            }
        })?;
        let subtree_rows =
            self.collect_local_only_subtree_rows(&existing.namespace_id, client_file_id)?;
        let mut removed_client_file_ids = Vec::with_capacity(subtree_rows.len());
        for row in subtree_rows.iter().rev() {
            removed_client_file_ids.push(row.client_file_id.clone());
            self.delete_planned_local_only_action(&row.client_file_id)?;
            self.delete_local_only_transfer_ledger(&row.client_file_id, TransferDirection::Upload)?;
            self.delete_local_only_upload(&row.client_file_id)?;
            if let Some(pending) =
                load_pending_client_mutation_for_client_file(&self.tx, &row.client_file_id)?
            {
                self.delete_pending_client_mutation(&pending.client_request_id)?;
            }
            self.delete_local_only_conflicts_and_errors(&row.client_file_id)?;
            self.delete_local_only_file(&row.client_file_id)?;
        }

        Ok(ObservedLocalOnlyDeleteResult {
            root_client_file_id: client_file_id.clone(),
            removed_client_file_ids,
        })
    }

    pub fn observe_subtree_and_plan(
        &mut self,
        operations: &[SubtreeObservationOp],
        planned_at_ms: u64,
    ) -> Result<Vec<SubtreeObservationOutcome>, StateDbError> {
        let mut outcomes = Vec::with_capacity(operations.len());
        let mut batch_local_only_ids = std::collections::BTreeMap::new();

        for operation in operations {
            match operation {
                SubtreeObservationOp::ObserveBound { observed } => {
                    let planned = self.observe_bound_inode_and_plan(observed, planned_at_ms)?;
                    outcomes.push(SubtreeObservationOutcome::ObservedBound {
                        inode_id: observed.inode_id,
                        inode_kind: observed.inode_kind.clone(),
                        planned_action: planned,
                    });
                }
                SubtreeObservationOp::MoveBound {
                    from_relative_path,
                    observed,
                } => {
                    let planned = self.observe_bound_inode_and_plan(observed, planned_at_ms)?;
                    outcomes.push(SubtreeObservationOutcome::MovedBound {
                        from_relative_path: from_relative_path.clone(),
                        inode_id: observed.inode_id,
                        inode_kind: observed.inode_kind.clone(),
                        planned_action: planned,
                    });
                }
                SubtreeObservationOp::ObserveLocalOnly { observed } => {
                    let resolved_parent = match &observed.parent {
                        SubtreeLocalOnlyParentRef::Bound { parent_inode_id } => {
                            LocalOnlyParentRef::Bound {
                                parent_inode_id: *parent_inode_id,
                            }
                        }
                        SubtreeLocalOnlyParentRef::ExistingLocalOnly {
                            parent_client_file_id,
                        } => LocalOnlyParentRef::LocalOnly {
                            parent_client_file_id: parent_client_file_id.clone(),
                        },
                        SubtreeLocalOnlyParentRef::BatchLocalOnly {
                            parent_relative_path,
                        } => LocalOnlyParentRef::LocalOnly {
                            parent_client_file_id: batch_local_only_ids
                                .get(parent_relative_path)
                                .cloned()
                                .ok_or_else(|| {
                                    StateDbError::SubtreeObservationBatchParentMissing {
                                        parent_relative_path: parent_relative_path.clone(),
                                    }
                                })?,
                        },
                    };
                    let result = self.observe_local_only_inode_under_resolved_parent_and_plan(
                        observed,
                        &resolved_parent,
                        planned_at_ms,
                    )?;
                    batch_local_only_ids.insert(
                        observed.relative_path.clone(),
                        result.local_only.client_file_id.clone(),
                    );
                    outcomes.push(SubtreeObservationOutcome::ObservedLocalOnly {
                        relative_path: observed.relative_path.clone(),
                        result,
                    });
                }
                SubtreeObservationOp::MoveLocalOnly { observed } => {
                    let resolved_parent = match &observed.parent {
                        SubtreeLocalOnlyParentRef::Bound { parent_inode_id } => {
                            LocalOnlyParentRef::Bound {
                                parent_inode_id: *parent_inode_id,
                            }
                        }
                        SubtreeLocalOnlyParentRef::ExistingLocalOnly {
                            parent_client_file_id,
                        } => LocalOnlyParentRef::LocalOnly {
                            parent_client_file_id: parent_client_file_id.clone(),
                        },
                        SubtreeLocalOnlyParentRef::BatchLocalOnly {
                            parent_relative_path,
                        } => LocalOnlyParentRef::LocalOnly {
                            parent_client_file_id: batch_local_only_ids
                                .get(parent_relative_path)
                                .cloned()
                                .ok_or_else(|| {
                                    StateDbError::SubtreeObservationBatchParentMissing {
                                        parent_relative_path: parent_relative_path.clone(),
                                    }
                                })?,
                        },
                    };
                    let result = self.observe_local_only_move_and_plan(
                        &observed.client_file_id,
                        &resolved_parent,
                        observed.inode_kind.clone(),
                        &observed.display_name,
                        observed.content_digest.clone(),
                        observed.exists_on_disk,
                        observed.dirty,
                        observed.last_local_change_ms,
                        planned_at_ms,
                    )?;
                    batch_local_only_ids.insert(
                        observed.relative_path.clone(),
                        result.local_only.client_file_id.clone(),
                    );
                    outcomes.push(SubtreeObservationOutcome::MovedLocalOnly {
                        from_relative_path: observed.from_relative_path.clone(),
                        relative_path: observed.relative_path.clone(),
                        result,
                    });
                }
                SubtreeObservationOp::DeleteBound { observed } => {
                    let planned = self.observe_bound_inode_and_plan(
                        &ObservedBoundInode {
                            namespace_id: observed.namespace_id.clone(),
                            inode_id: observed.inode_id,
                            inode_kind: observed.inode_kind.clone(),
                            content_digest: observed.content_digest.clone(),
                            parent_inode_id: observed.parent_inode_id,
                            display_name: observed.display_name.clone(),
                            exists_on_disk: false,
                            dirty: true,
                            last_local_change_ms: observed.last_local_change_ms,
                        },
                        planned_at_ms,
                    )?;
                    outcomes.push(SubtreeObservationOutcome::DeletedBound {
                        inode_id: observed.inode_id,
                        inode_kind: observed.inode_kind.clone(),
                        planned_action: planned,
                    });
                }
                SubtreeObservationOp::DeleteLocalOnly { client_file_id } => {
                    let result = self.observe_local_only_delete(client_file_id)?;
                    outcomes.push(SubtreeObservationOutcome::DeletedLocalOnly { result });
                }
            }
        }

        Ok(outcomes)
    }

    pub fn record_local_only_upload(
        &mut self,
        client_file_id: &ClientFileId,
        uploaded: &UploadedContent,
        uploaded_at_ms: u64,
    ) -> Result<LocalOnlyUploadRow, StateDbError> {
        let local_only = self.load_local_only_file(client_file_id)?.ok_or_else(|| {
            StateDbError::LocalOnlyFileMissing {
                client_file_id: client_file_id.as_str().to_owned(),
            }
        })?;
        validate_local_only_upload(
            &local_only,
            &uploaded.namespace_id,
            &uploaded.file_digest_sha256,
        )?;

        let row = LocalOnlyUploadRow {
            client_file_id: client_file_id.clone(),
            namespace_id: uploaded.namespace_id.clone(),
            file_digest_sha256: uploaded.file_digest_sha256.clone(),
            content_manifest_digest: uploaded.content_manifest_digest.clone(),
            manifest_object_key: uploaded.manifest_object_key.clone(),
            file_size_bytes: uploaded.file_size_bytes,
            uploaded_at_ms,
        };

        self.tx.execute(
            "INSERT INTO local_only_uploads (
                client_file_id,
                namespace_id,
                file_digest_sha256,
                content_manifest_digest,
                manifest_object_key,
                file_size_bytes,
                uploaded_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(client_file_id) DO UPDATE SET
                namespace_id = excluded.namespace_id,
                file_digest_sha256 = excluded.file_digest_sha256,
                content_manifest_digest = excluded.content_manifest_digest,
                manifest_object_key = excluded.manifest_object_key,
                file_size_bytes = excluded.file_size_bytes,
                uploaded_at_ms = excluded.uploaded_at_ms",
            params![
                row.client_file_id.as_str(),
                row.namespace_id.as_str(),
                &row.file_digest_sha256,
                &row.content_manifest_digest,
                &row.manifest_object_key,
                to_sql_u64(row.file_size_bytes, "file_size_bytes")?,
                to_sql_u64(row.uploaded_at_ms, "uploaded_at_ms")?,
            ],
        )?;
        self.delete_local_only_transfer_ledger(client_file_id, TransferDirection::Upload)?;

        Ok(row)
    }

    pub fn record_inode_upload(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        uploaded: &UploadedContent,
        uploaded_at_ms: u64,
    ) -> Result<InodeUploadRow, StateDbError> {
        let local = self
            .load_file_sync_views(namespace_id, inode_id)?
            .local
            .ok_or_else(|| StateDbError::UploadLocalEditStateMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
        validate_inode_upload(&local, &uploaded.namespace_id, &uploaded.file_digest_sha256)?;

        let row = InodeUploadRow {
            namespace_id: uploaded.namespace_id.clone(),
            inode_id,
            file_digest_sha256: uploaded.file_digest_sha256.clone(),
            content_manifest_digest: uploaded.content_manifest_digest.clone(),
            manifest_object_key: uploaded.manifest_object_key.clone(),
            file_size_bytes: uploaded.file_size_bytes,
            uploaded_at_ms,
        };

        self.tx.execute(
            "INSERT INTO inode_uploads (
                namespace_id,
                inode_id,
                file_digest_sha256,
                content_manifest_digest,
                manifest_object_key,
                file_size_bytes,
                uploaded_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(namespace_id, inode_id) DO UPDATE SET
                file_digest_sha256 = excluded.file_digest_sha256,
                content_manifest_digest = excluded.content_manifest_digest,
                manifest_object_key = excluded.manifest_object_key,
                file_size_bytes = excluded.file_size_bytes,
                uploaded_at_ms = excluded.uploaded_at_ms",
            params![
                row.namespace_id.as_str(),
                to_sql_u64(row.inode_id.0, "inode_id")?,
                &row.file_digest_sha256,
                &row.content_manifest_digest,
                &row.manifest_object_key,
                to_sql_u64(row.file_size_bytes, "file_size_bytes")?,
                to_sql_u64(row.uploaded_at_ms, "uploaded_at_ms")?,
            ],
        )?;
        self.delete_transfer_ledger_for_inode(namespace_id, inode_id, TransferDirection::Upload)?;

        Ok(row)
    }

    pub fn upsert_transfer_ledger(
        &mut self,
        row: &TransferLedgerRow,
    ) -> Result<TransferLedgerRow, StateDbError> {
        self.tx.execute(
            "DELETE FROM transfer_ledger
            WHERE namespace_id = ?1 AND inode_id = ?2 AND direction = ?3 AND transfer_id != ?4",
            params![
                row.namespace_id.as_str(),
                to_sql_u64(row.inode_id.0, "inode_id")?,
                transfer_direction_as_str(row.direction),
                &row.transfer_id,
            ],
        )?;
        self.tx.execute(
            "INSERT INTO transfer_ledger (
                namespace_id,
                inode_id,
                transfer_id,
                direction,
                object_key,
                block_index,
                block_count,
                state,
                updated_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(transfer_id) DO UPDATE SET
                namespace_id = excluded.namespace_id,
                inode_id = excluded.inode_id,
                direction = excluded.direction,
                object_key = excluded.object_key,
                block_index = excluded.block_index,
                block_count = excluded.block_count,
                state = excluded.state,
                updated_at_ms = excluded.updated_at_ms",
            params![
                row.namespace_id.as_str(),
                to_sql_u64(row.inode_id.0, "inode_id")?,
                &row.transfer_id,
                transfer_direction_as_str(row.direction),
                &row.object_key,
                to_sql_u64(row.block_index, "block_index")?,
                to_sql_u64(row.block_count, "block_count")?,
                transfer_state_as_str(row.state),
                to_sql_u64(row.updated_at_ms, "updated_at_ms")?,
            ],
        )?;

        Ok(row.clone())
    }

    pub fn upsert_local_only_transfer_ledger(
        &mut self,
        row: &LocalOnlyTransferLedgerRow,
    ) -> Result<LocalOnlyTransferLedgerRow, StateDbError> {
        self.tx.execute(
            "DELETE FROM local_only_transfer_ledger
            WHERE client_file_id = ?1 AND direction = ?2 AND transfer_id != ?3",
            params![
                row.client_file_id.as_str(),
                transfer_direction_as_str(row.direction),
                &row.transfer_id,
            ],
        )?;
        self.tx.execute(
            "INSERT INTO local_only_transfer_ledger (
                client_file_id,
                namespace_id,
                transfer_id,
                direction,
                object_key,
                block_index,
                block_count,
                state,
                updated_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(transfer_id) DO UPDATE SET
                client_file_id = excluded.client_file_id,
                namespace_id = excluded.namespace_id,
                direction = excluded.direction,
                object_key = excluded.object_key,
                block_index = excluded.block_index,
                block_count = excluded.block_count,
                state = excluded.state,
                updated_at_ms = excluded.updated_at_ms",
            params![
                row.client_file_id.as_str(),
                row.namespace_id.as_str(),
                &row.transfer_id,
                transfer_direction_as_str(row.direction),
                &row.object_key,
                to_sql_u64(row.block_index, "block_index")?,
                to_sql_u64(row.block_count, "block_count")?,
                transfer_state_as_str(row.state),
                to_sql_u64(row.updated_at_ms, "updated_at_ms")?,
            ],
        )?;

        Ok(row.clone())
    }

    pub fn delete_transfer_ledger_for_inode(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        direction: TransferDirection,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "DELETE FROM transfer_ledger
            WHERE namespace_id = ?1 AND inode_id = ?2 AND direction = ?3",
            params![
                namespace_id.as_str(),
                to_sql_u64(inode_id.0, "inode_id")?,
                transfer_direction_as_str(direction),
            ],
        )?;
        Ok(())
    }

    pub fn delete_local_only_transfer_ledger(
        &mut self,
        client_file_id: &ClientFileId,
        direction: TransferDirection,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "DELETE FROM local_only_transfer_ledger
            WHERE client_file_id = ?1 AND direction = ?2",
            params![
                client_file_id.as_str(),
                transfer_direction_as_str(direction),
            ],
        )?;
        Ok(())
    }

    pub fn record_pending_client_mutation(
        &mut self,
        client_file_id: &ClientFileId,
        request: &ClientMutationRequest,
        created_at_ms: u64,
    ) -> Result<PendingClientMutationRow, StateDbError> {
        if let Some(existing) = load_pending_client_mutation(&self.tx, &request.client_request_id)?
        {
            if existing.namespace_id == request.namespace_id
                && existing.client_file_id == *client_file_id
                && existing.request == *request
            {
                return Ok(existing);
            }

            return Err(StateDbError::PendingClientMutationConflict {
                client_request_id: request.client_request_id.clone(),
                existing_client_file_id: existing.client_file_id.as_str().to_owned(),
                new_client_file_id: client_file_id.as_str().to_owned(),
            });
        }

        if let Some(existing) =
            load_pending_client_mutation_for_client_file(&self.tx, client_file_id)?
        {
            if existing.request == *request {
                return Ok(existing);
            }

            return Err(StateDbError::PendingClientMutationClientFileConflict {
                client_file_id: client_file_id.as_str().to_owned(),
                existing_client_request_id: existing.client_request_id,
                new_client_request_id: request.client_request_id.clone(),
            });
        }

        let row = PendingClientMutationRow {
            client_request_id: request.client_request_id.clone(),
            namespace_id: request.namespace_id.clone(),
            client_file_id: client_file_id.clone(),
            request: request.clone(),
            created_at_ms,
        };
        self.tx.execute(
            "INSERT INTO pending_client_mutations (
                client_request_id,
                namespace_id,
                client_file_id,
                request_json,
                created_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                &row.client_request_id,
                row.namespace_id.as_str(),
                row.client_file_id.as_str(),
                serde_json::to_string(&row.request)?,
                to_sql_u64(row.created_at_ms, "created_at_ms")?,
            ],
        )?;

        Ok(row)
    }

    pub fn record_pending_inode_mutation(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        request: &ClientMutationRequest,
        created_at_ms: u64,
    ) -> Result<PendingInodeMutationRow, StateDbError> {
        if let Some(existing) = load_pending_inode_mutation(&self.tx, &request.client_request_id)? {
            if existing.namespace_id == *namespace_id
                && existing.inode_id == inode_id
                && existing.request == *request
            {
                return Ok(existing);
            }

            return Err(StateDbError::PendingInodeMutationConflict {
                client_request_id: request.client_request_id.clone(),
                existing_inode_id: existing.inode_id.0,
                new_inode_id: inode_id.0,
            });
        }

        if let Some(existing) =
            load_pending_inode_mutation_for_inode(&self.tx, namespace_id, inode_id)?
        {
            if existing.request == *request {
                return Ok(existing);
            }

            return Err(StateDbError::PendingInodeMutationInodeConflict {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                existing_client_request_id: existing.client_request_id,
                new_client_request_id: request.client_request_id.clone(),
            });
        }

        let row = PendingInodeMutationRow {
            client_request_id: request.client_request_id.clone(),
            namespace_id: namespace_id.clone(),
            inode_id,
            request: request.clone(),
            created_at_ms,
        };
        self.tx.execute(
            "INSERT INTO pending_inode_mutations (
                client_request_id,
                namespace_id,
                inode_id,
                request_json,
                created_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                &row.client_request_id,
                row.namespace_id.as_str(),
                to_sql_u64(row.inode_id.0, "inode_id")?,
                serde_json::to_string(&row.request)?,
                to_sql_u64(row.created_at_ms, "created_at_ms")?,
            ],
        )?;

        Ok(row)
    }

    pub fn apply_client_mutation_response(
        &mut self,
        response: &ClientMutationResponse,
    ) -> Result<BoundLocalOnlyFile, StateDbError> {
        if client_mutation_response_result_count(response) == 0 {
            return Err(StateDbError::ClientMutationResponseMissingResult {
                client_request_id: response.client_request_id.clone(),
            });
        }
        if client_mutation_response_result_count(response) > 1 {
            return Err(StateDbError::ClientMutationResponseConflictingResults {
                client_request_id: response.client_request_id.clone(),
            });
        }
        let pending = load_pending_client_mutation(&self.tx, &response.client_request_id)?
            .ok_or_else(|| StateDbError::PendingClientMutationMissing {
                client_request_id: response.client_request_id.clone(),
            })?;

        if pending.namespace_id != response.namespace_id {
            return Err(StateDbError::PendingClientMutationNamespaceMismatch {
                client_request_id: response.client_request_id.clone(),
                pending_namespace_id: pending.namespace_id.as_str().to_owned(),
                response_namespace_id: response.namespace_id.as_str().to_owned(),
            });
        }

        let created_inode = response.created_inode.as_ref().ok_or_else(|| {
            StateDbError::ClientMutationResponseMissingResult {
                client_request_id: response.client_request_id.clone(),
            }
        })?;
        let content_manifest_digest = match &pending.request.op {
            ClientMutationOp::CreateFile {
                content_manifest_digest,
                ..
            } => Some(content_manifest_digest.clone()),
            ClientMutationOp::CreateDir { .. } => None,
            ClientMutationOp::ReplaceFile { .. }
            | ClientMutationOp::Rename { .. }
            | ClientMutationOp::DeleteFile { .. }
            | ClientMutationOp::DeleteSubtree { .. } => None,
        };
        let remote = RemoteFileStateRow {
            namespace_id: response.namespace_id.clone(),
            inode_id: created_inode.inode_id,
            inode_kind: created_inode.inode_kind.clone(),
            observed_seq: response.committed_seq,
            revision_no: created_inode.revision_no,
            content_digest: created_inode.content_digest.clone(),
            content_manifest_digest,
            parent_inode_id: Some(created_inode.parent_inode_id),
            display_name: created_inode.display_name.clone(),
            is_deleted: false,
        };
        let bound = match self.bind_local_only_inode_to_remote(&pending.client_file_id, &remote) {
            Ok(bound) => bound,
            Err(StateDbError::LocalOnlyFileMissing { .. }) => {
                let views =
                    self.load_file_sync_views(&response.namespace_id, created_inode.inode_id)?;
                match &pending.request.op {
                    ClientMutationOp::CreateFile { .. } => {
                        if create_file_response_matches_current_state(
                            response,
                            created_inode,
                            &views,
                        ) {
                            BoundLocalOnlyFile {
                                client_file_id: pending.client_file_id.clone(),
                                namespace_id: response.namespace_id.clone(),
                                inode_id: created_inode.inode_id,
                            }
                        } else {
                            return Err(StateDbError::LocalOnlyFileMissing {
                                client_file_id: pending.client_file_id.as_str().to_owned(),
                            });
                        }
                    }
                    ClientMutationOp::CreateDir { .. } => {
                        match (views.remote, views.local, views.sync_anchor) {
                            (Some(bound_remote), Some(_bound_local), Some(_bound_anchor))
                                if bound_remote.inode_kind == created_inode.inode_kind
                                    && bound_remote.revision_no == created_inode.revision_no
                                    && bound_remote.content_digest
                                        == created_inode.content_digest
                                    && bound_remote.parent_inode_id
                                        == Some(created_inode.parent_inode_id)
                                    && bound_remote.display_name == created_inode.display_name
                                    && !bound_remote.is_deleted =>
                            {
                                BoundLocalOnlyFile {
                                    client_file_id: pending.client_file_id.clone(),
                                    namespace_id: response.namespace_id.clone(),
                                    inode_id: created_inode.inode_id,
                                }
                            }
                            _ => {
                                return Err(StateDbError::LocalOnlyFileMissing {
                                    client_file_id: pending.client_file_id.as_str().to_owned(),
                                })
                            }
                        }
                    }
                    ClientMutationOp::ReplaceFile { .. } => {
                        return Err(StateDbError::LocalOnlyFileMissing {
                            client_file_id: pending.client_file_id.as_str().to_owned(),
                        })
                    }
                    ClientMutationOp::Rename { .. }
                    | ClientMutationOp::DeleteFile { .. }
                    | ClientMutationOp::DeleteSubtree { .. } => {
                        return Err(StateDbError::ClientMutationResponseUnexpectedResult {
                            client_request_id: response.client_request_id.clone(),
                            expected: "created_inode",
                        })
                    }
                }
            }
            Err(error) => return Err(error),
        };
        self.delete_pending_client_mutation(&response.client_request_id)?;

        Ok(bound)
    }

    pub fn apply_inode_mutation_response(
        &mut self,
        response: &ClientMutationResponse,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        if client_mutation_response_result_count(response) == 0 {
            return Err(StateDbError::ClientMutationResponseMissingResult {
                client_request_id: response.client_request_id.clone(),
            });
        }
        if client_mutation_response_result_count(response) > 1 {
            return Err(StateDbError::ClientMutationResponseConflictingResults {
                client_request_id: response.client_request_id.clone(),
            });
        }
        let pending = match load_pending_inode_mutation(&self.tx, &response.client_request_id)? {
            Some(pending) => pending,
            None => {
                if let Some(replaced) = response.replaced_file.as_ref() {
                    let views =
                        self.load_file_sync_views(&response.namespace_id, replaced.inode_id)?;
                    if replace_response_matches_current_state(response, replaced, &views) {
                        return Ok(AppliedInodeMutation {
                            namespace_id: response.namespace_id.clone(),
                            inode_id: replaced.inode_id,
                        });
                    }
                }
                if let Some(renamed) = response.renamed_inode.as_ref() {
                    let views =
                        self.load_file_sync_views(&response.namespace_id, renamed.inode_id)?;
                    if rename_response_matches_current_state(response, renamed, &views) {
                        return Ok(AppliedInodeMutation {
                            namespace_id: response.namespace_id.clone(),
                            inode_id: renamed.inode_id,
                        });
                    }
                }
                if let Some(deleted) = response.deleted_inode.as_ref() {
                    let views =
                        self.load_file_sync_views(&response.namespace_id, deleted.inode_id)?;
                    if delete_response_matches_current_state(response, deleted, &views) {
                        return Ok(AppliedInodeMutation {
                            namespace_id: response.namespace_id.clone(),
                            inode_id: deleted.inode_id,
                        });
                    }
                }
                return Err(StateDbError::PendingInodeMutationMissing {
                    client_request_id: response.client_request_id.clone(),
                });
            }
        };

        if pending.namespace_id != response.namespace_id {
            return Err(StateDbError::PendingInodeMutationNamespaceMismatch {
                client_request_id: response.client_request_id.clone(),
                pending_namespace_id: pending.namespace_id.as_str().to_owned(),
                response_namespace_id: response.namespace_id.as_str().to_owned(),
            });
        }

        match (
            &pending.request.op,
            &response.replaced_file,
            &response.renamed_inode,
            &response.deleted_inode,
        ) {
            (ClientMutationOp::ReplaceFile { .. }, Some(replaced), None, None) => {
                let (remote, local, anchor) = self
                    .load_bound_upload_local_edit_views(&pending.namespace_id, pending.inode_id)?;

                let next_remote = RemoteFileStateRow {
                    namespace_id: pending.namespace_id.clone(),
                    inode_id: pending.inode_id,
                    inode_kind: replaced.inode_kind.clone(),
                    observed_seq: response.committed_seq,
                    revision_no: replaced.revision_no,
                    content_digest: Some(replaced.content_digest.clone()),
                    content_manifest_digest: match &pending.request.op {
                        ClientMutationOp::ReplaceFile {
                            content_manifest_digest,
                            ..
                        } => Some(content_manifest_digest.clone()),
                        _ => None,
                    },
                    parent_inode_id: remote.parent_inode_id,
                    display_name: remote.display_name,
                    is_deleted: false,
                };
                let next_local = LocalFileStateRow {
                    namespace_id: pending.namespace_id.clone(),
                    inode_id: pending.inode_id,
                    inode_kind: local.inode_kind,
                    content_digest: Some(replaced.content_digest.clone()),
                    parent_inode_id: local.parent_inode_id,
                    display_name: local.display_name,
                    exists_on_disk: local.exists_on_disk,
                    dirty: false,
                    last_local_change_ms: local.last_local_change_ms,
                };
                let next_anchor = SyncAnchorRow {
                    namespace_id: pending.namespace_id.clone(),
                    inode_id: pending.inode_id,
                    inode_kind: anchor.inode_kind,
                    synced_seq: response.committed_seq,
                    revision_no: replaced.revision_no,
                    content_digest: Some(replaced.content_digest.clone()),
                    content_manifest_digest: match &pending.request.op {
                        ClientMutationOp::ReplaceFile {
                            content_manifest_digest,
                            ..
                        } => Some(content_manifest_digest.clone()),
                        _ => None,
                    },
                    parent_inode_id: anchor.parent_inode_id,
                    display_name: anchor.display_name,
                };

                self.upsert_remote_file(&next_remote)?;
                self.upsert_local_file(&next_local)?;
                self.upsert_sync_anchor(&next_anchor)?;
                self.delete_planned_action(&pending.namespace_id, pending.inode_id)?;
                self.delete_pending_inode_mutation(&response.client_request_id)?;

                Ok(AppliedInodeMutation {
                    namespace_id: pending.namespace_id,
                    inode_id: pending.inode_id,
                })
            }
            (ClientMutationOp::Rename { .. }, None, Some(renamed), None) => {
                self.apply_local_rename_mutation_response(&pending, response, renamed)
            }
            (ClientMutationOp::DeleteFile { .. }, None, None, Some(deleted)) => {
                self.apply_local_delete_file_mutation_response(&pending, response, deleted)
            }
            (ClientMutationOp::DeleteSubtree { .. }, None, None, Some(deleted)) => {
                self.apply_local_delete_subtree_mutation_response(&pending, response, deleted)
            }
            (ClientMutationOp::ReplaceFile { .. }, _, _, _)
            | (ClientMutationOp::Rename { .. }, _, _, _)
            | (ClientMutationOp::DeleteFile { .. }, _, _, _)
            | (ClientMutationOp::DeleteSubtree { .. }, _, _, _) => {
                Err(StateDbError::ClientMutationResponseUnexpectedResult {
                    client_request_id: response.client_request_id.clone(),
                    expected: match pending.request.op {
                        ClientMutationOp::ReplaceFile { .. } => "replaced_file",
                        ClientMutationOp::Rename { .. } => "renamed_inode",
                        ClientMutationOp::DeleteFile { .. }
                        | ClientMutationOp::DeleteSubtree { .. } => "deleted_inode",
                        _ => unreachable!(),
                    },
                })
            }
            _ => Err(StateDbError::ClientMutationResponseUnexpectedResult {
                client_request_id: response.client_request_id.clone(),
                expected: "inode mutation result",
            }),
        }
    }

    fn apply_local_rename_mutation_response(
        &mut self,
        pending: &PendingInodeMutationRow,
        response: &ClientMutationResponse,
        renamed: &loon_types::RenamedRemoteInode,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        let vacated_relative_path =
            self.resolve_current_relative_path_for_inode(&pending.namespace_id, pending.inode_id)?;
        let views = self.load_file_sync_views(&pending.namespace_id, pending.inode_id)?;
        let next_remote = rename_remote_state_from_response(pending, response, renamed, &views)?;
        let current_local =
            views
                .local
                .ok_or_else(|| StateDbError::ApplyRemoteRenameStateMissing {
                    namespace_id: pending.namespace_id.as_str().to_owned(),
                    inode_id: pending.inode_id.0,
                })?;

        let mut next_local = LocalFileStateRow {
            namespace_id: pending.namespace_id.clone(),
            inode_id: pending.inode_id,
            inode_kind: current_local.inode_kind,
            content_digest: current_local.content_digest.clone(),
            parent_inode_id: Some(renamed.parent_inode_id),
            display_name: renamed.display_name.clone(),
            exists_on_disk: current_local.exists_on_disk,
            dirty: current_local.dirty,
            last_local_change_ms: current_local.last_local_change_ms,
        };
        let next_anchor = SyncAnchorRow {
            namespace_id: pending.namespace_id.clone(),
            inode_id: pending.inode_id,
            inode_kind: next_remote.inode_kind.clone(),
            synced_seq: response.committed_seq,
            revision_no: next_remote.revision_no,
            content_digest: next_remote.content_digest.clone(),
            content_manifest_digest: next_remote.content_manifest_digest.clone(),
            parent_inode_id: next_remote.parent_inode_id,
            display_name: next_remote.display_name.clone(),
        };

        if next_local.exists_on_disk
            && next_local.inode_kind == next_anchor.inode_kind
            && next_local.content_digest == next_anchor.content_digest
            && next_local.parent_inode_id == next_anchor.parent_inode_id
            && next_local.display_name == next_anchor.display_name
        {
            next_local.dirty = false;
        }

        self.upsert_remote_file(&next_remote)?;
        self.upsert_local_file(&next_local)?;
        self.upsert_sync_anchor(&next_anchor)?;
        self.delete_pending_inode_mutation(&response.client_request_id)?;
        self.delete_conflict_or_error_kind(
            &pending.namespace_id,
            pending.inode_id,
            "apply_remote_rename_local_apply_failed",
        )?;
        let _ = plan_file_in_tx(
            self,
            &pending.namespace_id,
            pending.inode_id,
            next_local.last_local_change_ms,
        )?;
        if let Some(relative_path) = vacated_relative_path.as_deref() {
            self.replan_waiting_local_only_at_relative_path(&pending.namespace_id, relative_path)?;
        }

        Ok(AppliedInodeMutation {
            namespace_id: pending.namespace_id.clone(),
            inode_id: pending.inode_id,
        })
    }

    fn apply_local_delete_file_mutation_response(
        &mut self,
        pending: &PendingInodeMutationRow,
        response: &ClientMutationResponse,
        deleted: &loon_types::DeletedRemoteInode,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        let vacated_relative_path =
            self.resolve_current_relative_path_for_inode(&pending.namespace_id, pending.inode_id)?;
        let views = self.load_file_sync_views(&pending.namespace_id, pending.inode_id)?;
        let tombstone = delete_remote_state_from_response(pending, response, deleted, &views)?;
        self.upsert_remote_file(&tombstone)?;
        self.delete_pending_inode_mutation(&response.client_request_id)?;
        self.delete_planned_action(&pending.namespace_id, pending.inode_id)?;
        self.delete_inode_upload(&pending.namespace_id, pending.inode_id)?;
        self.delete_transfer_ledger_for_inode(
            &pending.namespace_id,
            pending.inode_id,
            TransferDirection::Upload,
        )?;
        self.delete_transfer_ledger_for_inode(
            &pending.namespace_id,
            pending.inode_id,
            TransferDirection::Download,
        )?;
        self.delete_local_file(&pending.namespace_id, pending.inode_id)?;
        self.delete_sync_anchor(&pending.namespace_id, pending.inode_id)?;
        self.delete_conflict_or_error_kind(
            &pending.namespace_id,
            pending.inode_id,
            "apply_remote_delete_local_apply_failed",
        )?;
        if let Some(relative_path) = vacated_relative_path.as_deref() {
            self.replan_waiting_local_only_at_relative_path(&pending.namespace_id, relative_path)?;
        }

        Ok(AppliedInodeMutation {
            namespace_id: pending.namespace_id.clone(),
            inode_id: pending.inode_id,
        })
    }

    fn apply_local_delete_subtree_mutation_response(
        &mut self,
        pending: &PendingInodeMutationRow,
        response: &ClientMutationResponse,
        deleted: &loon_types::DeletedRemoteInode,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        let vacated_relative_path =
            self.resolve_current_relative_path_for_inode(&pending.namespace_id, pending.inode_id)?;
        let views = self.load_file_sync_views(&pending.namespace_id, pending.inode_id)?;
        let tombstone = delete_remote_state_from_response(pending, response, deleted, &views)?;
        let subtree_inode_ids =
            load_local_subtree_inode_ids(&self.tx, &pending.namespace_id, pending.inode_id)?;
        let descendant_remote_inode_ids = load_remote_subtree_descendant_inode_ids(
            &self.tx,
            &pending.namespace_id,
            pending.inode_id,
        )?;
        let local_only_descendants = load_local_only_descendants_under_subtree(
            &self.tx,
            &pending.namespace_id,
            pending.inode_id,
        )?;

        self.upsert_remote_file(&tombstone)?;
        self.delete_pending_inode_mutation(&response.client_request_id)?;
        self.delete_planned_actions_for_inodes(&pending.namespace_id, &subtree_inode_ids)?;
        self.delete_conflicts_and_errors_for_inodes(&pending.namespace_id, &subtree_inode_ids)?;
        for inode_id in subtree_inode_ids.iter().copied() {
            self.delete_inode_upload(&pending.namespace_id, inode_id)?;
            self.delete_transfer_ledger_for_inode(
                &pending.namespace_id,
                inode_id,
                TransferDirection::Upload,
            )?;
            self.delete_transfer_ledger_for_inode(
                &pending.namespace_id,
                inode_id,
                TransferDirection::Download,
            )?;
            if let Some(descendant_pending) =
                load_pending_inode_mutation_for_inode(&self.tx, &pending.namespace_id, inode_id)?
            {
                self.delete_pending_inode_mutation(&descendant_pending.client_request_id)?;
            }
        }
        self.delete_remote_files_for_inodes(&pending.namespace_id, &descendant_remote_inode_ids)?;
        self.delete_local_files_for_inodes(&pending.namespace_id, &subtree_inode_ids)?;
        self.delete_sync_anchors_for_inodes(&pending.namespace_id, &subtree_inode_ids)?;
        self.cleanup_local_only_subtree_roots(&local_only_descendants)?;
        if let Some(relative_path) = vacated_relative_path.as_deref() {
            self.replan_waiting_local_only_at_relative_path(&pending.namespace_id, relative_path)?;
        }

        Ok(AppliedInodeMutation {
            namespace_id: pending.namespace_id.clone(),
            inode_id: pending.inode_id,
        })
    }

    pub fn apply_download_remote_edit(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        let views = self.load_file_sync_views(namespace_id, inode_id)?;
        let (remote, local) = match (views.remote, views.local, views.sync_anchor) {
            (Some(_), Some(_), Some(_)) => {
                let (remote, local, _anchor) = load_bound_download_remote_edit_views_from_conn(
                    &self.tx,
                    namespace_id,
                    inode_id,
                )?;
                (remote, local)
            }
            (Some(remote), Some(local), None)
                if remote_only_placeholder_matches_remote_state(&local, &remote)
                    && remote.inode_kind == InodeKind::File
                    && !remote.is_deleted =>
            {
                (remote, local)
            }
            _ => {
                return Err(StateDbError::DownloadRemoteEditStateMissing {
                    namespace_id: namespace_id.as_str().to_owned(),
                    inode_id: inode_id.0,
                })
            }
        };
        let next_local = LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: local.inode_kind,
            content_digest: remote.content_digest.clone(),
            parent_inode_id: remote.parent_inode_id,
            display_name: remote.display_name.clone(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: applied_at_ms,
        };
        let next_anchor = SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: remote.inode_kind.clone(),
            synced_seq: remote.observed_seq,
            revision_no: remote.revision_no,
            content_digest: remote.content_digest.clone(),
            content_manifest_digest: remote.content_manifest_digest.clone(),
            parent_inode_id: remote.parent_inode_id,
            display_name: remote.display_name,
        };

        self.upsert_local_file(&next_local)?;
        self.upsert_sync_anchor(&next_anchor)?;
        self.delete_planned_action(namespace_id, inode_id)?;
        self.delete_transfer_ledger_for_inode(namespace_id, inode_id, TransferDirection::Download)?;
        self.delete_conflict_or_error_kind(
            namespace_id,
            inode_id,
            "download_remote_edit_remote_digest_mismatch",
        )?;
        self.delete_conflict_or_error_kind(
            namespace_id,
            inode_id,
            "download_remote_edit_local_apply_failed",
        )?;
        self.delete_conflict_or_error_kind(
            namespace_id,
            inode_id,
            "download_remote_edit_transfer_reset",
        )?;

        Ok(AppliedInodeMutation {
            namespace_id: namespace_id.clone(),
            inode_id,
        })
    }

    pub fn apply_materialize_remote_dir(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        let views = self.load_file_sync_views(namespace_id, inode_id)?;
        let (remote, local) = match (views.remote, views.local, views.sync_anchor) {
            (Some(remote), Some(local), None) => (remote, local),
            _ => {
                return Err(StateDbError::MaterializeRemoteDirStateMissing {
                    namespace_id: namespace_id.as_str().to_owned(),
                    inode_id: inode_id.0,
                })
            }
        };

        if remote.inode_kind != InodeKind::Dir || local.inode_kind != InodeKind::Dir {
            return Err(StateDbError::MaterializeRemoteDirRequiresDirectory {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                inode_kind: inode_kind_as_str(&local.inode_kind).to_owned(),
            });
        }
        if remote.is_deleted {
            return Err(StateDbError::MaterializeRemoteDirPlaceholderMismatch {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                field: "is_deleted",
                local: "false".to_owned(),
                remote: "true".to_owned(),
            });
        }
        if local.exists_on_disk {
            return Err(StateDbError::MaterializeRemoteDirPlaceholderMismatch {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                field: "exists_on_disk",
                local: "true".to_owned(),
                remote: "false".to_owned(),
            });
        }
        if local.dirty {
            return Err(StateDbError::MaterializeRemoteDirPlaceholderMismatch {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                field: "dirty",
                local: "true".to_owned(),
                remote: "false".to_owned(),
            });
        }
        if local.parent_inode_id != remote.parent_inode_id {
            return Err(StateDbError::MaterializeRemoteDirPlaceholderMismatch {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                field: "parent_inode_id",
                local: format!("{:?}", local.parent_inode_id),
                remote: format!("{:?}", remote.parent_inode_id),
            });
        }
        if local.display_name != remote.display_name {
            return Err(StateDbError::MaterializeRemoteDirPlaceholderMismatch {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                field: "display_name",
                local: local.display_name.clone(),
                remote: remote.display_name.clone(),
            });
        }

        let next_local = LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: local.inode_kind,
            content_digest: None,
            parent_inode_id: remote.parent_inode_id,
            display_name: remote.display_name.clone(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: applied_at_ms,
        };
        let next_anchor = SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: remote.inode_kind.clone(),
            synced_seq: remote.observed_seq,
            revision_no: remote.revision_no,
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: remote.parent_inode_id,
            display_name: remote.display_name,
        };

        self.upsert_local_file(&next_local)?;
        self.upsert_sync_anchor(&next_anchor)?;
        self.delete_planned_action(namespace_id, inode_id)?;
        self.delete_conflict_or_error_kind(
            namespace_id,
            inode_id,
            "materialize_remote_dir_local_apply_failed",
        )?;
        self.replan_direct_authoritative_children(namespace_id, inode_id, applied_at_ms)?;

        Ok(AppliedInodeMutation {
            namespace_id: namespace_id.clone(),
            inode_id,
        })
    }

    pub fn apply_remote_rename(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        let vacated_relative_path =
            self.resolve_current_relative_path_for_inode(namespace_id, inode_id)?;
        let (remote, local, _anchor) =
            load_bound_apply_remote_rename_views_from_conn(&self.tx, namespace_id, inode_id)?;

        let next_local = LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: local.inode_kind,
            content_digest: local.content_digest,
            parent_inode_id: remote.parent_inode_id,
            display_name: remote.display_name.clone(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: applied_at_ms,
        };
        let next_anchor = SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: remote.inode_kind.clone(),
            synced_seq: remote.observed_seq,
            revision_no: remote.revision_no,
            content_digest: remote.content_digest.clone(),
            content_manifest_digest: remote.content_manifest_digest.clone(),
            parent_inode_id: remote.parent_inode_id,
            display_name: remote.display_name,
        };

        self.upsert_local_file(&next_local)?;
        self.upsert_sync_anchor(&next_anchor)?;
        self.delete_planned_action(namespace_id, inode_id)?;
        self.delete_conflict_or_error_kind(
            namespace_id,
            inode_id,
            "apply_remote_rename_local_apply_failed",
        )?;
        if let Some(relative_path) = vacated_relative_path.as_deref() {
            self.replan_waiting_local_only_at_relative_path(namespace_id, relative_path)?;
        }

        Ok(AppliedInodeMutation {
            namespace_id: namespace_id.clone(),
            inode_id,
        })
    }

    pub fn apply_same_inode_conflict_resolution(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        let views = self.load_file_sync_views(namespace_id, inode_id)?;
        let remote = views
            .remote
            .ok_or_else(|| StateDbError::UploadLocalEditStateMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;

        let next_local = LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: remote.inode_kind.clone(),
            content_digest: remote.content_digest.clone(),
            parent_inode_id: remote.parent_inode_id,
            display_name: remote.display_name.clone(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: applied_at_ms,
        };
        let next_anchor = SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: remote.inode_kind.clone(),
            synced_seq: remote.observed_seq,
            revision_no: remote.revision_no,
            content_digest: remote.content_digest.clone(),
            content_manifest_digest: remote.content_manifest_digest.clone(),
            parent_inode_id: remote.parent_inode_id,
            display_name: remote.display_name,
        };

        self.upsert_local_file(&next_local)?;
        self.upsert_sync_anchor(&next_anchor)?;
        self.delete_planned_action(namespace_id, inode_id)?;
        self.delete_inode_upload(namespace_id, inode_id)?;
        self.delete_transfer_ledger_for_inode(namespace_id, inode_id, TransferDirection::Upload)?;
        if let Some(pending) =
            load_pending_inode_mutation_for_inode(&self.tx, namespace_id, inode_id)?
        {
            self.delete_pending_inode_mutation(&pending.client_request_id)?;
        }
        self.delete_conflict_or_error_kind(
            namespace_id,
            inode_id,
            "resolve_same_inode_conflict_local_apply_failed",
        )?;

        Ok(AppliedInodeMutation {
            namespace_id: namespace_id.clone(),
            inode_id,
        })
    }

    pub fn apply_delete_vs_edit_conflict_resolution(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        _applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        let vacated_relative_path =
            self.resolve_current_relative_path_for_inode(namespace_id, inode_id)?;
        self.delete_planned_action(namespace_id, inode_id)?;
        self.delete_inode_upload(namespace_id, inode_id)?;
        self.delete_transfer_ledger_for_inode(namespace_id, inode_id, TransferDirection::Upload)?;
        if let Some(pending) =
            load_pending_inode_mutation_for_inode(&self.tx, namespace_id, inode_id)?
        {
            self.delete_pending_inode_mutation(&pending.client_request_id)?;
        }
        self.delete_local_file(namespace_id, inode_id)?;
        self.delete_sync_anchor(namespace_id, inode_id)?;
        self.delete_conflict_or_error_kind(
            namespace_id,
            inode_id,
            "resolve_delete_vs_edit_conflict_local_apply_failed",
        )?;
        if let Some(relative_path) = vacated_relative_path.as_deref() {
            self.replan_waiting_local_only_at_relative_path(namespace_id, relative_path)?;
        }

        Ok(AppliedInodeMutation {
            namespace_id: namespace_id.clone(),
            inode_id,
        })
    }

    pub fn apply_rename_vs_edit_conflict_resolution(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        let vacated_relative_path =
            self.resolve_current_relative_path_for_inode(namespace_id, inode_id)?;
        let views = self.load_file_sync_views(namespace_id, inode_id)?;
        let remote = views
            .remote
            .ok_or_else(|| StateDbError::ApplyRemoteRenameStateMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;

        let next_local = LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: remote.inode_kind.clone(),
            content_digest: remote.content_digest.clone(),
            parent_inode_id: remote.parent_inode_id,
            display_name: remote.display_name.clone(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: applied_at_ms,
        };
        let next_anchor = SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: remote.inode_kind.clone(),
            synced_seq: remote.observed_seq,
            revision_no: remote.revision_no,
            content_digest: remote.content_digest.clone(),
            content_manifest_digest: remote.content_manifest_digest.clone(),
            parent_inode_id: remote.parent_inode_id,
            display_name: remote.display_name,
        };

        self.upsert_local_file(&next_local)?;
        self.upsert_sync_anchor(&next_anchor)?;
        self.delete_planned_action(namespace_id, inode_id)?;
        self.delete_inode_upload(namespace_id, inode_id)?;
        self.delete_transfer_ledger_for_inode(namespace_id, inode_id, TransferDirection::Upload)?;
        if let Some(pending) =
            load_pending_inode_mutation_for_inode(&self.tx, namespace_id, inode_id)?
        {
            self.delete_pending_inode_mutation(&pending.client_request_id)?;
        }
        self.delete_conflict_or_error_kind(
            namespace_id,
            inode_id,
            "resolve_rename_vs_edit_conflict_local_apply_failed",
        )?;
        if let Some(relative_path) = vacated_relative_path.as_deref() {
            self.replan_waiting_local_only_at_relative_path(namespace_id, relative_path)?;
        }

        Ok(AppliedInodeMutation {
            namespace_id: namespace_id.clone(),
            inode_id,
        })
    }

    pub fn apply_remote_rename_and_replace(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        let vacated_relative_path =
            self.resolve_current_relative_path_for_inode(namespace_id, inode_id)?;
        let views = self.load_file_sync_views(namespace_id, inode_id)?;
        let remote = views
            .remote
            .ok_or_else(|| StateDbError::ApplyRemoteRenameStateMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;

        let next_local = LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: remote.inode_kind.clone(),
            content_digest: remote.content_digest.clone(),
            parent_inode_id: remote.parent_inode_id,
            display_name: remote.display_name.clone(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: applied_at_ms,
        };
        let next_anchor = SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: remote.inode_kind.clone(),
            synced_seq: remote.observed_seq,
            revision_no: remote.revision_no,
            content_digest: remote.content_digest.clone(),
            content_manifest_digest: remote.content_manifest_digest.clone(),
            parent_inode_id: remote.parent_inode_id,
            display_name: remote.display_name,
        };

        self.upsert_local_file(&next_local)?;
        self.upsert_sync_anchor(&next_anchor)?;
        self.delete_planned_action(namespace_id, inode_id)?;
        self.delete_conflict_or_error_kind(
            namespace_id,
            inode_id,
            "apply_remote_rename_and_replace_local_apply_failed",
        )?;
        if let Some(relative_path) = vacated_relative_path.as_deref() {
            self.replan_waiting_local_only_at_relative_path(namespace_id, relative_path)?;
        }

        Ok(AppliedInodeMutation {
            namespace_id: namespace_id.clone(),
            inode_id,
        })
    }

    pub fn apply_path_binding_collision_resolution(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        client_file_id: &ClientFileId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        let views = self.load_file_sync_views(namespace_id, inode_id)?;
        let remote = views
            .remote
            .ok_or_else(|| StateDbError::DownloadRemoteEditStateMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;

        let next_local = LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: remote.inode_kind.clone(),
            content_digest: remote.content_digest.clone(),
            parent_inode_id: remote.parent_inode_id,
            display_name: remote.display_name.clone(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: applied_at_ms,
        };
        let next_anchor = SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: remote.inode_kind.clone(),
            synced_seq: remote.observed_seq,
            revision_no: remote.revision_no,
            content_digest: remote.content_digest.clone(),
            content_manifest_digest: remote.content_manifest_digest.clone(),
            parent_inode_id: remote.parent_inode_id,
            display_name: remote.display_name,
        };

        self.upsert_local_file(&next_local)?;
        self.upsert_sync_anchor(&next_anchor)?;
        self.delete_planned_action(namespace_id, inode_id)?;
        self.delete_planned_local_only_action(client_file_id)?;
        self.delete_local_only_transfer_ledger(client_file_id, TransferDirection::Upload)?;
        self.delete_local_only_upload(client_file_id)?;
        if let Some(pending) =
            load_pending_client_mutation_for_client_file(&self.tx, client_file_id)?
        {
            self.delete_pending_client_mutation(&pending.client_request_id)?;
        }
        self.delete_local_only_conflicts_and_errors(client_file_id)?;
        self.delete_local_only_file(client_file_id)?;
        self.delete_conflict_or_error_kind(
            namespace_id,
            inode_id,
            "resolve_path_binding_collision_local_apply_failed",
        )?;

        Ok(AppliedInodeMutation {
            namespace_id: namespace_id.clone(),
            inode_id,
        })
    }

    pub fn apply_remote_delete(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        _applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        let vacated_relative_path =
            self.resolve_current_relative_path_for_inode(namespace_id, inode_id)?;
        let (_remote, _local, _anchor) =
            load_bound_apply_remote_delete_views_from_conn(&self.tx, namespace_id, inode_id)?;

        self.delete_planned_action(namespace_id, inode_id)?;
        self.delete_inode_upload(namespace_id, inode_id)?;
        self.delete_local_file(namespace_id, inode_id)?;
        self.delete_sync_anchor(namespace_id, inode_id)?;
        self.delete_conflict_or_error_kind(
            namespace_id,
            inode_id,
            "apply_remote_delete_local_apply_failed",
        )?;
        if let Some(relative_path) = vacated_relative_path.as_deref() {
            self.replan_waiting_local_only_at_relative_path(namespace_id, relative_path)?;
        }

        Ok(AppliedInodeMutation {
            namespace_id: namespace_id.clone(),
            inode_id,
        })
    }

    pub fn apply_remote_subtree_rename(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        let vacated_relative_path =
            self.resolve_current_relative_path_for_inode(namespace_id, inode_id)?;
        let views = load_bound_apply_remote_subtree_rename_views_from_conn(
            &self.tx,
            namespace_id,
            inode_id,
        )?;

        let next_local = LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: views.root_local.inode_kind,
            content_digest: views.root_local.content_digest,
            parent_inode_id: views.root_remote.parent_inode_id,
            display_name: views.root_remote.display_name.clone(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: applied_at_ms,
        };
        let next_anchor = SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: views.root_remote.inode_kind.clone(),
            synced_seq: views.root_remote.observed_seq,
            revision_no: views.root_remote.revision_no,
            content_digest: views.root_remote.content_digest.clone(),
            content_manifest_digest: views.root_remote.content_manifest_digest.clone(),
            parent_inode_id: views.root_remote.parent_inode_id,
            display_name: views.root_remote.display_name,
        };

        self.upsert_local_file(&next_local)?;
        self.upsert_sync_anchor(&next_anchor)?;
        self.delete_planned_action(namespace_id, inode_id)?;
        self.delete_conflict_or_error_kind(
            namespace_id,
            inode_id,
            "apply_remote_subtree_rename_local_apply_failed",
        )?;
        self.replan_direct_authoritative_children(namespace_id, inode_id, applied_at_ms)?;
        if let Some(relative_path) = vacated_relative_path.as_deref() {
            self.replan_waiting_local_only_at_relative_path(namespace_id, relative_path)?;
        }

        Ok(AppliedInodeMutation {
            namespace_id: namespace_id.clone(),
            inode_id,
        })
    }

    pub fn apply_remote_subtree_delete(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        _applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        let vacated_relative_path =
            self.resolve_current_relative_path_for_inode(namespace_id, inode_id)?;
        let views = load_bound_apply_remote_subtree_delete_views_from_conn(
            &self.tx,
            namespace_id,
            inode_id,
        )?;

        self.delete_planned_actions_for_inodes(namespace_id, &views.subtree_inode_ids)?;
        self.delete_conflicts_and_errors_for_inodes(namespace_id, &views.subtree_inode_ids)?;
        self.delete_remote_files_for_inodes(namespace_id, &views.descendant_remote_inode_ids)?;
        self.delete_local_files_for_inodes(namespace_id, &views.subtree_inode_ids)?;
        self.delete_sync_anchors_for_inodes(namespace_id, &views.subtree_inode_ids)?;
        if let Some(relative_path) = vacated_relative_path.as_deref() {
            self.replan_waiting_local_only_at_relative_path(namespace_id, relative_path)?;
        }

        Ok(AppliedInodeMutation {
            namespace_id: namespace_id.clone(),
            inode_id,
        })
    }

    pub fn apply_resolved_subtree_delete_conflict(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        _applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        let views = load_bound_resolve_subtree_delete_conflict_views_from_conn(
            &self.tx,
            namespace_id,
            inode_id,
        )?;

        self.delete_planned_actions_for_inodes(namespace_id, &views.subtree_inode_ids)?;
        self.delete_conflicts_and_errors_for_inodes(namespace_id, &views.subtree_inode_ids)?;
        self.delete_remote_files_for_inodes(namespace_id, &views.descendant_remote_inode_ids)?;
        self.delete_local_files_for_inodes(namespace_id, &views.subtree_inode_ids)?;
        self.delete_sync_anchors_for_inodes(namespace_id, &views.subtree_inode_ids)?;
        self.cleanup_local_only_conflict_subtree_entries(&views.local_only_descendants)?;

        Ok(AppliedInodeMutation {
            namespace_id: namespace_id.clone(),
            inode_id,
        })
    }

    pub fn apply_resolved_subtree_rename_conflict(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        applied_at_ms: u64,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        let views = load_bound_resolve_subtree_rename_conflict_views_from_conn(
            &self.tx,
            namespace_id,
            inode_id,
        )?;

        let next_root_local = LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: views.root_local.inode_kind,
            content_digest: views.root_local.content_digest.clone(),
            parent_inode_id: views.root_remote.parent_inode_id,
            display_name: views.root_remote.display_name.clone(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: applied_at_ms,
        };
        let next_root_anchor = SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: views.root_remote.inode_kind.clone(),
            synced_seq: views.root_remote.observed_seq,
            revision_no: views.root_remote.revision_no,
            content_digest: views.root_remote.content_digest.clone(),
            content_manifest_digest: views.root_remote.content_manifest_digest.clone(),
            parent_inode_id: views.root_remote.parent_inode_id,
            display_name: views.root_remote.display_name.clone(),
        };

        self.upsert_local_file(&next_root_local)?;
        self.upsert_sync_anchor(&next_root_anchor)?;

        for entry in &views.bound_descendants {
            self.delete_planned_action(namespace_id, entry.inode_id)?;
            self.delete_inode_upload(namespace_id, entry.inode_id)?;
            self.delete_transfer_ledger_for_inode(
                namespace_id,
                entry.inode_id,
                TransferDirection::Upload,
            )?;
            if let Some(pending) =
                load_pending_inode_mutation_for_inode(&self.tx, namespace_id, entry.inode_id)?
            {
                self.delete_pending_inode_mutation(&pending.client_request_id)?;
            }

            if entry.inode_id == inode_id {
                continue;
            }
            if let Some(anchor) = entry.sync_anchor.as_ref() {
                let next_local = LocalFileStateRow {
                    namespace_id: namespace_id.clone(),
                    inode_id: entry.inode_id,
                    inode_kind: anchor.inode_kind.clone(),
                    content_digest: anchor.content_digest.clone(),
                    parent_inode_id: anchor.parent_inode_id,
                    display_name: anchor.display_name.clone(),
                    exists_on_disk: true,
                    dirty: false,
                    last_local_change_ms: applied_at_ms,
                };
                self.upsert_local_file(&next_local)?;
            }
        }

        self.delete_conflicts_and_errors_for_inodes(namespace_id, &views.subtree_inode_ids)?;
        self.cleanup_local_only_conflict_subtree_entries(&views.local_only_descendants)?;

        Ok(AppliedInodeMutation {
            namespace_id: namespace_id.clone(),
            inode_id,
        })
    }

    pub fn apply_remote_observation(
        &mut self,
        observed: &ObservedRemoteInode,
        applied_at_ms: u64,
    ) -> Result<AppliedRemoteObservation, StateDbError> {
        let observed_remote = observed_remote_as_remote_file_state(observed);
        let current_remote = load_remote_file(&self.tx, &observed.namespace_id, observed.inode_id)?;
        if let Some(current_remote) = current_remote.as_ref() {
            if observed.observed_seq <= current_remote.observed_seq {
                return Ok(AppliedRemoteObservation::IgnoredStale {
                    namespace_id: observed.namespace_id.clone(),
                    inode_id: observed.inode_id,
                });
            }
        }

        let views = self.load_file_sync_views(&observed.namespace_id, observed.inode_id)?;
        if let (Some(_remote), Some(local), None) = (
            views.remote.as_ref(),
            views.local.as_ref(),
            views.sync_anchor.as_ref(),
        ) {
            if !local.exists_on_disk
                && !local.dirty
                && remote_only_discovery_supported(&observed_remote)
            {
                let next_local = LocalFileStateRow {
                    namespace_id: observed.namespace_id.clone(),
                    inode_id: observed.inode_id,
                    inode_kind: observed.inode_kind.clone(),
                    content_digest: None,
                    parent_inode_id: observed.parent_inode_id,
                    display_name: observed.display_name.clone(),
                    exists_on_disk: false,
                    dirty: false,
                    last_local_change_ms: applied_at_ms,
                };
                self.upsert_remote_file(&observed_remote)?;
                self.upsert_local_file(&next_local)?;
                self.delete_conflict_or_error_kind(
                    &observed.namespace_id,
                    observed.inode_id,
                    "remote_observation_bind_ambiguous",
                )?;
                return Ok(AppliedRemoteObservation::DiscoveredRemoteOnly {
                    namespace_id: observed.namespace_id.clone(),
                    inode_id: observed.inode_id,
                });
            }
        }
        if let (Some(_remote), Some(local), Some(_anchor)) = (
            views.remote.as_ref(),
            views.local.as_ref(),
            views.sync_anchor.as_ref(),
        ) {
            self.upsert_remote_file(&observed_remote)?;
            let has_active_transfer = load_transfer_ledger_for_inode(
                &self.tx,
                &observed.namespace_id,
                observed.inode_id,
                TransferDirection::Download,
            )?
            .is_some()
                || load_transfer_ledger_for_inode(
                    &self.tx,
                    &observed.namespace_id,
                    observed.inode_id,
                    TransferDirection::Upload,
                )?
                .is_some();
            if has_active_transfer {
                self.delete_conflict_or_error_kind(
                    &observed.namespace_id,
                    observed.inode_id,
                    "remote_observation_bind_ambiguous",
                )?;
                return Ok(AppliedRemoteObservation::UpdatedBoundRemoteState {
                    namespace_id: observed.namespace_id.clone(),
                    inode_id: observed.inode_id,
                });
            }
            if bound_local_matches_remote_observation(local, &observed_remote) {
                let next_local = LocalFileStateRow {
                    namespace_id: observed.namespace_id.clone(),
                    inode_id: observed.inode_id,
                    inode_kind: local.inode_kind.clone(),
                    content_digest: observed_remote.content_digest.clone(),
                    parent_inode_id: local.parent_inode_id,
                    display_name: local.display_name.clone(),
                    exists_on_disk: true,
                    dirty: false,
                    last_local_change_ms: applied_at_ms,
                };
                let next_anchor = SyncAnchorRow {
                    namespace_id: observed.namespace_id.clone(),
                    inode_id: observed.inode_id,
                    inode_kind: observed.inode_kind.clone(),
                    synced_seq: observed.observed_seq,
                    revision_no: observed.revision_no,
                    content_digest: observed.content_digest.clone(),
                    content_manifest_digest: observed.content_manifest_digest.clone(),
                    parent_inode_id: observed.parent_inode_id,
                    display_name: observed.display_name.clone(),
                };
                self.upsert_local_file(&next_local)?;
                self.upsert_sync_anchor(&next_anchor)?;
                self.delete_planned_action(&observed.namespace_id, observed.inode_id)?;
                if let Some(pending) = load_pending_inode_mutation_for_inode(
                    &self.tx,
                    &observed.namespace_id,
                    observed.inode_id,
                )? {
                    self.delete_pending_inode_mutation(&pending.client_request_id)?;
                }
                self.delete_conflict_or_error_kind(
                    &observed.namespace_id,
                    observed.inode_id,
                    "remote_observation_bind_ambiguous",
                )?;
                return Ok(AppliedRemoteObservation::ConvergedBoundInode(
                    AppliedInodeMutation {
                        namespace_id: observed.namespace_id.clone(),
                        inode_id: observed.inode_id,
                    },
                ));
            }

            self.delete_conflict_or_error_kind(
                &observed.namespace_id,
                observed.inode_id,
                "remote_observation_bind_ambiguous",
            )?;
            return Ok(AppliedRemoteObservation::UpdatedBoundRemoteState {
                namespace_id: observed.namespace_id.clone(),
                inode_id: observed.inode_id,
            });
        }

        let matching_local_only =
            load_local_only_candidates_for_namespace(&self.tx, &observed.namespace_id)?
                .into_iter()
                .filter(|candidate| {
                    local_only_matches_remote_observation(candidate, &observed_remote)
                })
                .collect::<Vec<_>>();
        match matching_local_only.as_slice() {
            [] => {
                if remote_only_discovery_supported(&observed_remote) {
                    let placeholder = LocalFileStateRow {
                        namespace_id: observed.namespace_id.clone(),
                        inode_id: observed.inode_id,
                        inode_kind: observed.inode_kind.clone(),
                        content_digest: None,
                        parent_inode_id: observed.parent_inode_id,
                        display_name: observed.display_name.clone(),
                        exists_on_disk: false,
                        dirty: false,
                        last_local_change_ms: applied_at_ms,
                    };
                    self.upsert_remote_file(&observed_remote)?;
                    self.upsert_local_file(&placeholder)?;
                    self.delete_conflict_or_error_kind(
                        &observed.namespace_id,
                        observed.inode_id,
                        "remote_observation_bind_ambiguous",
                    )?;
                    Ok(AppliedRemoteObservation::DiscoveredRemoteOnly {
                        namespace_id: observed.namespace_id.clone(),
                        inode_id: observed.inode_id,
                    })
                } else {
                    Ok(AppliedRemoteObservation::IgnoredUnmatched {
                        namespace_id: observed.namespace_id.clone(),
                        inode_id: observed.inode_id,
                    })
                }
            }
            [candidate] => {
                let bound = self
                    .bind_local_only_inode_to_remote(&candidate.client_file_id, &observed_remote)?;
                self.delete_conflict_or_error_kind(
                    &observed.namespace_id,
                    observed.inode_id,
                    "remote_observation_bind_ambiguous",
                )?;
                Ok(AppliedRemoteObservation::BoundLocalOnly(bound))
            }
            many => {
                self.record_conflict_or_error(
                    &observed.namespace_id,
                    observed.inode_id,
                    "remote_observation_bind_ambiguous",
                    &format!(
                        "ambiguous remote observation bind matched {} local-only candidates",
                        many.len()
                    ),
                    &json!({
                        "matches": many.len(),
                        "observed_seq": observed.observed_seq.0,
                        "revision_no": observed.revision_no.0,
                        "inode_kind": inode_kind_as_str(&observed.inode_kind),
                        "parent_inode_id": observed.parent_inode_id.map(|inode_id| inode_id.0),
                        "display_name": observed.display_name.clone(),
                    }),
                    applied_at_ms,
                )?;
                Ok(AppliedRemoteObservation::RecordedConflictOrError {
                    namespace_id: observed.namespace_id.clone(),
                    inode_id: observed.inode_id,
                    kind: "remote_observation_bind_ambiguous".to_owned(),
                })
            }
        }
    }

    pub fn bind_local_only_inode_to_remote(
        &mut self,
        client_file_id: &ClientFileId,
        remote: &RemoteFileStateRow,
    ) -> Result<BoundLocalOnlyFile, StateDbError> {
        let local_only = self.load_local_only_file(client_file_id)?.ok_or_else(|| {
            StateDbError::LocalOnlyFileMissing {
                client_file_id: client_file_id.as_str().to_owned(),
            }
        })?;

        if local_only.namespace_id != remote.namespace_id {
            return Err(StateDbError::BindNamespaceMismatch {
                client_file_id: client_file_id.as_str().to_owned(),
                local_namespace_id: local_only.namespace_id.as_str().to_owned(),
                remote_namespace_id: remote.namespace_id.as_str().to_owned(),
            });
        }

        if local_only.inode_kind != remote.inode_kind {
            return Err(StateDbError::BindKindMismatch {
                client_file_id: client_file_id.as_str().to_owned(),
                local_kind: inode_kind_as_str(&local_only.inode_kind).to_owned(),
                remote_kind: inode_kind_as_str(&remote.inode_kind).to_owned(),
            });
        }

        if remote.is_deleted {
            return Err(StateDbError::BindRemoteDeleted {
                client_file_id: client_file_id.as_str().to_owned(),
                inode_id: remote.inode_id.0,
            });
        }

        ensure_bind_match(
            client_file_id,
            "exists_on_disk",
            local_only.exists_on_disk.to_string(),
            "true".to_owned(),
        )?;
        ensure_bind_match(
            client_file_id,
            "content_digest",
            format!("{:?}", local_only.content_digest),
            format!("{:?}", remote.content_digest),
        )?;
        ensure_bind_match(
            client_file_id,
            "parent_inode_id",
            format!("{:?}", local_only.parent_inode_id),
            format!("{:?}", remote.parent_inode_id),
        )?;
        ensure_bind_match(
            client_file_id,
            "display_name",
            local_only.display_name.clone(),
            remote.display_name.clone(),
        )?;

        let local_row = LocalFileStateRow {
            namespace_id: remote.namespace_id.clone(),
            inode_id: remote.inode_id,
            inode_kind: local_only.inode_kind.clone(),
            content_digest: local_only.content_digest.clone(),
            parent_inode_id: local_only.parent_inode_id,
            display_name: local_only.display_name.clone(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: local_only.last_local_change_ms,
        };
        let anchor_row = SyncAnchorRow {
            namespace_id: remote.namespace_id.clone(),
            inode_id: remote.inode_id,
            inode_kind: remote.inode_kind.clone(),
            synced_seq: remote.observed_seq,
            revision_no: remote.revision_no,
            content_digest: remote.content_digest.clone(),
            content_manifest_digest: remote.content_manifest_digest.clone(),
            parent_inode_id: remote.parent_inode_id,
            display_name: remote.display_name.clone(),
        };
        let direct_local_only_children = if local_only.inode_kind == InodeKind::Dir {
            self.load_direct_local_only_child_rows(&local_only.namespace_id, client_file_id)?
        } else {
            Vec::new()
        };

        self.upsert_remote_file(remote)?;
        self.upsert_local_file(&local_row)?;
        self.upsert_sync_anchor(&anchor_row)?;
        self.delete_planned_action(&remote.namespace_id, remote.inode_id)?;
        self.delete_planned_local_only_action(client_file_id)?;
        self.delete_local_only_transfer_ledger(client_file_id, TransferDirection::Upload)?;
        self.delete_local_only_upload(client_file_id)?;
        self.delete_local_only_conflicts_and_errors(client_file_id)?;
        self.delete_local_only_file(client_file_id)?;

        for child in direct_local_only_children {
            let next_child = LocalOnlyFileStateRow {
                parent_inode_id: Some(remote.inode_id),
                ..child.clone()
            };
            self.upsert_local_only_file(&next_child)?;
            self.delete_local_only_parent_link(&next_child.client_file_id)?;
            let _ = plan_local_only_inode_in_tx(
                self,
                &next_child.client_file_id,
                next_child.last_local_change_ms,
            )?;
        }

        Ok(BoundLocalOnlyFile {
            client_file_id: client_file_id.clone(),
            namespace_id: remote.namespace_id.clone(),
            inode_id: remote.inode_id,
        })
    }

    pub fn load_file_sync_views(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<FileSyncViews, StateDbError> {
        Ok(FileSyncViews {
            namespace_id: namespace_id.clone(),
            inode_id,
            remote: load_remote_file(&self.tx, namespace_id, inode_id)?,
            local: load_local_file(&self.tx, namespace_id, inode_id)?,
            sync_anchor: load_sync_anchor(&self.tx, namespace_id, inode_id)?,
        })
    }

    pub fn assess_remote_subtree_delete(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<RemoteSubtreeDeleteAssessment, StateDbError> {
        assess_remote_subtree_delete_from_conn(&self.tx, namespace_id, inode_id)
    }

    pub fn assess_remote_subtree_rename(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<RemoteSubtreeRenameAssessment, StateDbError> {
        assess_remote_subtree_rename_from_conn(&self.tx, namespace_id, inode_id)
    }

    pub fn assess_hierarchy_parent_materialization(
        &self,
        namespace_id: &NamespaceId,
        target_parent_inode_id: InodeId,
    ) -> Result<HierarchyParentMaterializationAssessment, StateDbError> {
        assess_hierarchy_parent_materialization_from_conn(
            &self.tx,
            namespace_id,
            target_parent_inode_id,
        )
    }

    pub fn load_bound_apply_remote_subtree_delete_views(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<BoundApplyRemoteSubtreeDeleteViews, StateDbError> {
        load_bound_apply_remote_subtree_delete_views_from_conn(&self.tx, namespace_id, inode_id)
    }

    pub fn load_bound_apply_remote_subtree_rename_views(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<BoundApplyRemoteSubtreeRenameViews, StateDbError> {
        load_bound_apply_remote_subtree_rename_views_from_conn(&self.tx, namespace_id, inode_id)
    }

    pub fn load_local_only_file(
        &self,
        client_file_id: &ClientFileId,
    ) -> Result<Option<LocalOnlyFileStateRow>, StateDbError> {
        load_local_only_file(&self.tx, client_file_id)
    }

    pub fn load_local_only_candidates_for_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Vec<LocalOnlyFileStateRow>, StateDbError> {
        load_local_only_candidates_for_namespace(&self.tx, namespace_id)
    }

    pub fn upsert_planned_action(&mut self, row: &PlannedActionRow) -> Result<(), StateDbError> {
        self.tx.execute(
            "INSERT INTO planned_actions (
                namespace_id,
                inode_id,
                decision,
                reason,
                created_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(namespace_id, inode_id) DO UPDATE SET
                decision = excluded.decision,
                reason = excluded.reason,
                created_at_ms = excluded.created_at_ms",
            params![
                row.namespace_id.as_str(),
                to_sql_u64(row.inode_id.0, "inode_id")?,
                &row.decision,
                &row.reason,
                to_sql_u64(row.created_at_ms, "created_at_ms")?,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_planned_local_only_action(
        &mut self,
        row: &LocalOnlyPlannedActionRow,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "INSERT INTO planned_local_only_actions (
                client_file_id,
                namespace_id,
                decision,
                reason,
                created_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(client_file_id) DO UPDATE SET
                namespace_id = excluded.namespace_id,
                decision = excluded.decision,
                reason = excluded.reason,
                created_at_ms = excluded.created_at_ms",
            params![
                row.client_file_id.as_str(),
                row.namespace_id.as_str(),
                &row.decision,
                &row.reason,
                to_sql_u64(row.created_at_ms, "created_at_ms")?,
            ],
        )?;
        Ok(())
    }

    pub fn delete_planned_action(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "DELETE FROM planned_actions WHERE namespace_id = ?1 AND inode_id = ?2",
            params![namespace_id.as_str(), to_sql_u64(inode_id.0, "inode_id")?],
        )?;
        Ok(())
    }

    fn load_direct_authoritative_child_inode_ids(
        &self,
        namespace_id: &NamespaceId,
        parent_inode_id: InodeId,
    ) -> Result<Vec<InodeId>, StateDbError> {
        let mut stmt = self.tx.prepare(
            "SELECT inode_id
            FROM remote_state
            WHERE namespace_id = ?1 AND parent_inode_id = ?2
            ORDER BY inode_id ASC",
        )?;
        let mut rows = stmt.query(params![
            namespace_id.as_str(),
            to_sql_u64(parent_inode_id.0, "parent_inode_id")?,
        ])?;
        let mut child_inode_ids = Vec::new();
        while let Some(row) = rows.next()? {
            child_inode_ids.push(InodeId(from_sql_u64(row.get::<_, i64>(0)?, "inode_id")?));
        }
        Ok(child_inode_ids)
    }

    fn replan_direct_authoritative_children(
        &mut self,
        namespace_id: &NamespaceId,
        parent_inode_id: InodeId,
        now_ms: u64,
    ) -> Result<(), StateDbError> {
        for child_inode_id in
            self.load_direct_authoritative_child_inode_ids(namespace_id, parent_inode_id)?
        {
            plan_file_in_tx(self, namespace_id, child_inode_id, now_ms)?;
        }
        Ok(())
    }

    fn resolve_current_relative_path_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<Option<String>, StateDbError> {
        let summary = self.load_namespace_state_summary(namespace_id)?;
        let parent_links = self.load_local_only_parent_links_for_namespace(namespace_id)?;
        let path_index = crate::local_fs::NamespacePathIndex::build(&summary, &parent_links);
        Ok(path_index
            .resolve_current_inode_relative_path(inode_id)
            .map(str::to_owned))
    }

    fn replan_waiting_local_only_at_relative_path(
        &mut self,
        namespace_id: &NamespaceId,
        relative_path: &str,
    ) -> Result<(), StateDbError> {
        let summary = self.load_namespace_state_summary(namespace_id)?;
        let parent_links = self.load_local_only_parent_links_for_namespace(namespace_id)?;
        let path_index = crate::local_fs::NamespacePathIndex::build(&summary, &parent_links);
        let client_file_ids = path_index
            .local_only_file_matches(relative_path)
            .iter()
            .chain(path_index.local_only_dir_matches(relative_path).iter())
            .map(|row| row.client_file_id.clone())
            .collect::<Vec<_>>();

        for client_file_id in client_file_ids {
            let Some(planned) = self.load_planned_local_only_action(&client_file_id)? else {
                continue;
            };
            if planned.decision.as_str() != PlannerDecision::WaitForExactPathVacate.as_str() {
                continue;
            }
            let _ = plan_local_only_inode_in_tx(self, &client_file_id, planned.created_at_ms)?;
        }

        Ok(())
    }

    fn cleanup_local_only_conflict_subtree_entries(
        &mut self,
        entries: &[ConflictLocalOnlySubtreeEntry],
    ) -> Result<(), StateDbError> {
        let roots = entries
            .iter()
            .map(|entry| entry.local_only.clone())
            .collect::<Vec<_>>();
        self.cleanup_local_only_subtree_roots(&roots)
    }

    fn cleanup_local_only_subtree_roots(
        &mut self,
        entries: &[LocalOnlyFileStateRow],
    ) -> Result<(), StateDbError> {
        let mut removed = std::collections::BTreeSet::new();
        for entry in entries {
            let subtree_rows =
                self.collect_local_only_subtree_rows(&entry.namespace_id, &entry.client_file_id)?;
            for row in subtree_rows.iter().rev() {
                if !removed.insert(row.client_file_id.clone()) {
                    continue;
                }
                let client_file_id = &row.client_file_id;
                self.delete_planned_local_only_action(client_file_id)?;
                self.delete_local_only_transfer_ledger(client_file_id, TransferDirection::Upload)?;
                self.delete_local_only_upload(client_file_id)?;
                if let Some(pending) =
                    load_pending_client_mutation_for_client_file(&self.tx, client_file_id)?
                {
                    self.delete_pending_client_mutation(&pending.client_request_id)?;
                }
                self.delete_local_only_conflicts_and_errors(client_file_id)?;
                self.delete_local_only_file(client_file_id)?;
            }
        }
        Ok(())
    }

    pub fn delete_planned_actions_for_inodes(
        &mut self,
        namespace_id: &NamespaceId,
        inode_ids: &[InodeId],
    ) -> Result<(), StateDbError> {
        for inode_id in inode_ids {
            self.delete_planned_action(namespace_id, *inode_id)?;
        }
        Ok(())
    }

    pub fn delete_planned_local_only_action(
        &mut self,
        client_file_id: &ClientFileId,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "DELETE FROM planned_local_only_actions WHERE client_file_id = ?1",
            params![client_file_id.as_str()],
        )?;
        Ok(())
    }

    pub fn delete_local_only_file(
        &mut self,
        client_file_id: &ClientFileId,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "DELETE FROM local_only_state WHERE client_file_id = ?1",
            params![client_file_id.as_str()],
        )?;
        Ok(())
    }

    pub fn delete_local_only_upload(
        &mut self,
        client_file_id: &ClientFileId,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "DELETE FROM local_only_uploads WHERE client_file_id = ?1",
            params![client_file_id.as_str()],
        )?;
        Ok(())
    }

    pub fn delete_inode_upload(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "DELETE FROM inode_uploads WHERE namespace_id = ?1 AND inode_id = ?2",
            params![namespace_id.as_str(), to_sql_u64(inode_id.0, "inode_id")?],
        )?;
        Ok(())
    }

    pub fn delete_pending_client_mutation(
        &mut self,
        client_request_id: &str,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "DELETE FROM pending_client_mutations WHERE client_request_id = ?1",
            params![client_request_id],
        )?;
        Ok(())
    }

    pub fn delete_pending_inode_mutation(
        &mut self,
        client_request_id: &str,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "DELETE FROM pending_inode_mutations WHERE client_request_id = ?1",
            params![client_request_id],
        )?;
        Ok(())
    }

    pub fn delete_conflicts_and_errors_for_inodes(
        &mut self,
        namespace_id: &NamespaceId,
        inode_ids: &[InodeId],
    ) -> Result<(), StateDbError> {
        for inode_id in inode_ids {
            self.tx.execute(
                "DELETE FROM conflicts_and_errors
                WHERE namespace_id = ?1 AND inode_id = ?2",
                params![namespace_id.as_str(), to_sql_u64(inode_id.0, "inode_id")?],
            )?;
        }
        Ok(())
    }

    pub fn upsert_conflict_artifact(
        &mut self,
        row: &ConflictArtifactRow,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "INSERT INTO conflict_artifacts (
                namespace_id,
                conflict_id,
                object_key,
                artifact_kind,
                conflict_class,
                artifact_json,
                created_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(namespace_id, conflict_id) DO UPDATE SET
                object_key = excluded.object_key,
                artifact_kind = excluded.artifact_kind,
                conflict_class = excluded.conflict_class,
                artifact_json = excluded.artifact_json,
                created_at_ms = excluded.created_at_ms",
            params![
                row.namespace_id.as_str(),
                &row.conflict_id,
                &row.object_key,
                row.artifact_kind.as_str(),
                row.conflict_class.as_str(),
                match &row.envelope {
                    ConflictArtifactEnvelopeRecord::File(envelope) =>
                        serde_json::to_string(envelope),
                    ConflictArtifactEnvelopeRecord::Subtree(envelope) => {
                        serde_json::to_string(envelope)
                    }
                }
                .map_err(StateDbError::ConflictArtifactCodec)?,
                to_sql_u64(row.created_at_ms, "created_at_ms")?,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_conflict_artifact_archive(
        &mut self,
        row: &ConflictArtifactArchiveRow,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "INSERT INTO conflict_artifact_archives (
                namespace_id,
                conflict_id,
                object_key,
                archived_at_ms
            ) VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(namespace_id, conflict_id) DO UPDATE SET
                object_key = excluded.object_key,
                archived_at_ms = excluded.archived_at_ms",
            params![
                row.namespace_id.as_str(),
                &row.conflict_id,
                &row.object_key,
                to_sql_u64(row.archived_at_ms, "archived_at_ms")?,
            ],
        )?;
        Ok(())
    }

    pub fn delete_conflict_artifact_archive(
        &mut self,
        namespace_id: &NamespaceId,
        conflict_id: &str,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "DELETE FROM conflict_artifact_archives
            WHERE namespace_id = ?1 AND conflict_id = ?2",
            params![namespace_id.as_str(), conflict_id],
        )?;
        Ok(())
    }

    pub fn delete_conflict_artifact_archives_for_namespace(
        &mut self,
        namespace_id: &NamespaceId,
    ) -> Result<(), StateDbError> {
        self.tx.execute(
            "DELETE FROM conflict_artifact_archives
            WHERE namespace_id = ?1",
            params![namespace_id.as_str()],
        )?;
        Ok(())
    }

    pub fn ensure_bound_parent_directory(
        &self,
        namespace_id: &NamespaceId,
        parent_inode_id: InodeId,
    ) -> Result<(), StateDbError> {
        let views = self.load_file_sync_views(namespace_id, parent_inode_id)?;
        let (remote, local, anchor) = match (views.remote, views.local, views.sync_anchor) {
            (Some(remote), Some(local), Some(anchor)) => (remote, local, anchor),
            _ => {
                return Err(StateDbError::LocalOnlyParentMissing {
                    namespace_id: namespace_id.as_str().to_owned(),
                    parent_inode_id: parent_inode_id.0,
                })
            }
        };

        if remote.inode_kind != InodeKind::Dir
            || local.inode_kind != InodeKind::Dir
            || anchor.inode_kind != InodeKind::Dir
        {
            return Err(StateDbError::LocalOnlyParentNotDirectory {
                namespace_id: namespace_id.as_str().to_owned(),
                parent_inode_id: parent_inode_id.0,
            });
        }

        if remote.is_deleted
            || !local.exists_on_disk
            || local.dirty
            || remote.inode_kind != anchor.inode_kind
            || local.inode_kind != anchor.inode_kind
            || remote.content_digest != anchor.content_digest
            || local.content_digest != anchor.content_digest
            || remote.parent_inode_id != anchor.parent_inode_id
            || local.parent_inode_id != anchor.parent_inode_id
            || remote.display_name != anchor.display_name
            || local.display_name != anchor.display_name
        {
            return Err(StateDbError::LocalOnlyParentNotBound {
                namespace_id: namespace_id.as_str().to_owned(),
                parent_inode_id: parent_inode_id.0,
            });
        }

        Ok(())
    }

    fn ensure_local_only_parent_directory(
        &self,
        namespace_id: &NamespaceId,
        parent: &LocalOnlyParentRef,
    ) -> Result<(), StateDbError> {
        match parent {
            LocalOnlyParentRef::Bound { parent_inode_id } => {
                self.ensure_bound_parent_directory(namespace_id, *parent_inode_id)
            }
            LocalOnlyParentRef::LocalOnly {
                parent_client_file_id,
            } => {
                let parent_row = self
                    .load_local_only_file(parent_client_file_id)?
                    .ok_or_else(|| StateDbError::LocalOnlyParentClientFileMissing {
                        client_file_id: parent_client_file_id.as_str().to_owned(),
                    })?;
                if parent_row.namespace_id != *namespace_id
                    || parent_row.inode_kind != InodeKind::Dir
                {
                    return Err(StateDbError::LocalOnlyParentClientFileNotDirectory {
                        client_file_id: parent_client_file_id.as_str().to_owned(),
                    });
                }
                Ok(())
            }
        }
    }

    fn load_local_only_rows_by_parent_and_name(
        &self,
        namespace_id: &NamespaceId,
        parent_inode_id: InodeId,
        display_name: &str,
    ) -> Result<Vec<LocalOnlyFileStateRow>, StateDbError> {
        let mut stmt = self.tx.prepare(
            "SELECT
                client_file_id,
                namespace_id,
                inode_kind,
                parent_inode_id,
                display_name,
                content_digest,
                exists_on_disk,
                dirty,
                last_local_change_ms
            FROM local_only_state
            WHERE namespace_id = ?1
              AND parent_inode_id = ?2
              AND display_name = ?3
            ORDER BY client_file_id",
        )?;
        let rows = stmt.query_map(
            params![namespace_id.as_str(), parent_inode_id.0, display_name],
            |row| {
                let client_file_id = row.get::<_, String>(0)?;
                let namespace_id = row.get::<_, String>(1)?;
                let inode_kind = row.get::<_, String>(2)?;
                Ok(LocalOnlyFileStateRow {
                    client_file_id: ClientFileId::from(client_file_id.as_str()),
                    namespace_id: NamespaceId::from(namespace_id.as_str()),
                    inode_kind: inode_kind_from_str(&inode_kind).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })?,
                    parent_inode_id: row.get::<_, Option<u64>>(3)?.map(InodeId),
                    display_name: row.get(4)?,
                    content_digest: row.get(5)?,
                    exists_on_disk: row.get(6)?,
                    dirty: row.get(7)?,
                    last_local_change_ms: row.get(8)?,
                })
            },
        )?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StateDbError::from)
    }

    fn load_local_only_rows_by_local_only_parent_and_name(
        &self,
        namespace_id: &NamespaceId,
        parent_client_file_id: &ClientFileId,
        display_name: &str,
    ) -> Result<Vec<LocalOnlyFileStateRow>, StateDbError> {
        let mut stmt = self.tx.prepare(
            "SELECT
                s.client_file_id,
                s.namespace_id,
                s.inode_kind,
                s.parent_inode_id,
                s.display_name,
                s.content_digest,
                s.exists_on_disk,
                s.dirty,
                s.last_local_change_ms
            FROM local_only_state s
            JOIN local_only_parent_links l ON l.client_file_id = s.client_file_id
            WHERE s.namespace_id = ?1
              AND l.parent_client_file_id = ?2
              AND s.display_name = ?3
            ORDER BY s.client_file_id",
        )?;
        let rows = stmt.query_map(
            params![
                namespace_id.as_str(),
                parent_client_file_id.as_str(),
                display_name
            ],
            |row| {
                let client_file_id = row.get::<_, String>(0)?;
                let namespace_id = row.get::<_, String>(1)?;
                let inode_kind = row.get::<_, String>(2)?;
                Ok(LocalOnlyFileStateRow {
                    client_file_id: ClientFileId::from(client_file_id.as_str()),
                    namespace_id: NamespaceId::from(namespace_id.as_str()),
                    inode_kind: inode_kind_from_str(&inode_kind).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })?,
                    parent_inode_id: row.get::<_, Option<u64>>(3)?.map(InodeId),
                    display_name: row.get(4)?,
                    content_digest: row.get(5)?,
                    exists_on_disk: row.get(6)?,
                    dirty: row.get(7)?,
                    last_local_change_ms: row.get(8)?,
                })
            },
        )?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StateDbError::from)
    }

    fn load_local_only_rows_by_parent_ref_and_name(
        &self,
        namespace_id: &NamespaceId,
        parent: &LocalOnlyParentRef,
        display_name: &str,
    ) -> Result<Vec<LocalOnlyFileStateRow>, StateDbError> {
        match parent {
            LocalOnlyParentRef::Bound { parent_inode_id } => self
                .load_local_only_rows_by_parent_and_name(
                    namespace_id,
                    *parent_inode_id,
                    display_name,
                ),
            LocalOnlyParentRef::LocalOnly {
                parent_client_file_id,
            } => self.load_local_only_rows_by_local_only_parent_and_name(
                namespace_id,
                parent_client_file_id,
                display_name,
            ),
        }
    }

    fn load_direct_local_only_child_rows(
        &self,
        namespace_id: &NamespaceId,
        parent_client_file_id: &ClientFileId,
    ) -> Result<Vec<LocalOnlyFileStateRow>, StateDbError> {
        let mut stmt = self.tx.prepare(
            "SELECT
                s.client_file_id,
                s.namespace_id,
                s.inode_kind,
                s.parent_inode_id,
                s.display_name,
                s.content_digest,
                s.exists_on_disk,
                s.dirty,
                s.last_local_change_ms
            FROM local_only_state s
            JOIN local_only_parent_links l ON l.client_file_id = s.client_file_id
            WHERE s.namespace_id = ?1
              AND l.parent_client_file_id = ?2
            ORDER BY s.client_file_id",
        )?;
        let rows = stmt.query_map(
            params![namespace_id.as_str(), parent_client_file_id.as_str()],
            |row| {
                let client_file_id = row.get::<_, String>(0)?;
                let namespace_id = row.get::<_, String>(1)?;
                let inode_kind = row.get::<_, String>(2)?;
                Ok(LocalOnlyFileStateRow {
                    client_file_id: ClientFileId::from(client_file_id.as_str()),
                    namespace_id: NamespaceId::from(namespace_id.as_str()),
                    inode_kind: inode_kind_from_str(&inode_kind).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })?,
                    parent_inode_id: row.get::<_, Option<u64>>(3)?.map(InodeId),
                    display_name: row.get(4)?,
                    content_digest: row.get(5)?,
                    exists_on_disk: row.get(6)?,
                    dirty: row.get(7)?,
                    last_local_change_ms: row.get(8)?,
                })
            },
        )?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StateDbError::from)
    }

    fn collect_local_only_subtree_rows(
        &self,
        namespace_id: &NamespaceId,
        root_client_file_id: &ClientFileId,
    ) -> Result<Vec<LocalOnlyFileStateRow>, StateDbError> {
        let root = self
            .load_local_only_file(root_client_file_id)?
            .ok_or_else(|| StateDbError::LocalOnlyFileMissing {
                client_file_id: root_client_file_id.as_str().to_owned(),
            })?;
        let mut rows = vec![root];
        let mut cursor = 0usize;
        while cursor < rows.len() {
            let parent = rows[cursor].client_file_id.clone();
            rows.extend(self.load_direct_local_only_child_rows(namespace_id, &parent)?);
            cursor += 1;
        }
        Ok(rows)
    }

    fn collect_local_only_subtree_client_file_ids(
        &self,
        namespace_id: &NamespaceId,
        root_client_file_id: &ClientFileId,
    ) -> Result<std::collections::BTreeSet<ClientFileId>, StateDbError> {
        Ok(self
            .collect_local_only_subtree_rows(namespace_id, root_client_file_id)?
            .into_iter()
            .map(|row| row.client_file_id)
            .collect())
    }

    pub fn load_bound_upload_local_edit_views(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<(RemoteFileStateRow, LocalFileStateRow, SyncAnchorRow), StateDbError> {
        load_bound_upload_local_edit_views_from_conn(&self.tx, namespace_id, inode_id)
    }
}

fn replace_response_matches_current_state(
    response: &ClientMutationResponse,
    replaced: &loon_types::ReplacedRemoteFile,
    views: &FileSyncViews,
) -> bool {
    let (Some(remote), Some(local), Some(anchor)) = (
        views.remote.as_ref(),
        views.local.as_ref(),
        views.sync_anchor.as_ref(),
    ) else {
        return false;
    };

    remote.namespace_id == response.namespace_id
        && remote.inode_id == replaced.inode_id
        && remote.inode_kind == replaced.inode_kind
        && remote.observed_seq == response.committed_seq
        && remote.revision_no == replaced.revision_no
        && remote.content_digest.as_deref() == Some(replaced.content_digest.as_str())
        && !remote.is_deleted
        && local.namespace_id == response.namespace_id
        && local.inode_id == replaced.inode_id
        && local.inode_kind == replaced.inode_kind
        && local.content_digest.as_deref() == Some(replaced.content_digest.as_str())
        && local.exists_on_disk
        && !local.dirty
        && local.parent_inode_id == remote.parent_inode_id
        && local.display_name == remote.display_name
        && anchor.namespace_id == response.namespace_id
        && anchor.inode_id == replaced.inode_id
        && anchor.inode_kind == replaced.inode_kind
        && anchor.synced_seq == response.committed_seq
        && anchor.revision_no == replaced.revision_no
        && anchor.content_digest.as_deref() == Some(replaced.content_digest.as_str())
        && anchor.parent_inode_id == remote.parent_inode_id
        && anchor.display_name == remote.display_name
}

fn rename_response_matches_current_state(
    response: &ClientMutationResponse,
    renamed: &loon_types::RenamedRemoteInode,
    views: &FileSyncViews,
) -> bool {
    let (Some(remote), Some(local), Some(anchor)) = (
        views.remote.as_ref(),
        views.local.as_ref(),
        views.sync_anchor.as_ref(),
    ) else {
        return false;
    };

    remote.namespace_id == response.namespace_id
        && remote.inode_id == renamed.inode_id
        && remote.inode_kind == renamed.inode_kind
        && remote.observed_seq == response.committed_seq
        && remote.parent_inode_id == Some(renamed.parent_inode_id)
        && remote.display_name == renamed.display_name
        && !remote.is_deleted
        && local.namespace_id == response.namespace_id
        && local.inode_id == renamed.inode_id
        && local.inode_kind == renamed.inode_kind
        && local.parent_inode_id == Some(renamed.parent_inode_id)
        && local.display_name == renamed.display_name
        && local.exists_on_disk
        && !local.dirty
        && anchor.namespace_id == response.namespace_id
        && anchor.inode_id == renamed.inode_id
        && anchor.inode_kind == renamed.inode_kind
        && anchor.synced_seq == response.committed_seq
        && anchor.parent_inode_id == Some(renamed.parent_inode_id)
        && anchor.display_name == renamed.display_name
}

fn delete_response_matches_current_state(
    response: &ClientMutationResponse,
    deleted: &loon_types::DeletedRemoteInode,
    views: &FileSyncViews,
) -> bool {
    let Some(remote) = views.remote.as_ref() else {
        return false;
    };

    remote.namespace_id == response.namespace_id
        && remote.inode_id == deleted.inode_id
        && remote.inode_kind == deleted.inode_kind
        && remote.observed_seq == response.committed_seq
        && remote.is_deleted
        && views.local.is_none()
        && views.sync_anchor.is_none()
}

fn client_mutation_response_result_count(response: &ClientMutationResponse) -> usize {
    usize::from(response.created_inode.is_some())
        + usize::from(response.replaced_file.is_some())
        + usize::from(response.renamed_inode.is_some())
        + usize::from(response.deleted_inode.is_some())
}

fn rename_remote_state_from_response(
    pending: &PendingInodeMutationRow,
    response: &ClientMutationResponse,
    renamed: &loon_types::RenamedRemoteInode,
    views: &FileSyncViews,
) -> Result<RemoteFileStateRow, StateDbError> {
    let source = views
        .remote
        .as_ref()
        .map(RemoteSourceState::from_remote)
        .or_else(|| {
            views
                .sync_anchor
                .as_ref()
                .map(RemoteSourceState::from_anchor)
        })
        .or_else(|| views.local.as_ref().map(RemoteSourceState::from_local))
        .ok_or_else(|| StateDbError::PendingInodeMutationRequestMissing {
            client_request_id: pending.client_request_id.clone(),
        })?;

    Ok(RemoteFileStateRow {
        namespace_id: pending.namespace_id.clone(),
        inode_id: pending.inode_id,
        inode_kind: renamed.inode_kind.clone(),
        observed_seq: response.committed_seq,
        revision_no: source.revision_no,
        content_digest: source.content_digest,
        content_manifest_digest: source.content_manifest_digest,
        parent_inode_id: Some(renamed.parent_inode_id),
        display_name: renamed.display_name.clone(),
        is_deleted: false,
    })
}

fn delete_remote_state_from_response(
    pending: &PendingInodeMutationRow,
    response: &ClientMutationResponse,
    deleted: &loon_types::DeletedRemoteInode,
    views: &FileSyncViews,
) -> Result<RemoteFileStateRow, StateDbError> {
    let source = views
        .remote
        .as_ref()
        .map(RemoteSourceState::from_remote)
        .or_else(|| {
            views
                .sync_anchor
                .as_ref()
                .map(RemoteSourceState::from_anchor)
        })
        .or_else(|| views.local.as_ref().map(RemoteSourceState::from_local))
        .ok_or_else(|| StateDbError::PendingInodeMutationRequestMissing {
            client_request_id: pending.client_request_id.clone(),
        })?;

    Ok(RemoteFileStateRow {
        namespace_id: pending.namespace_id.clone(),
        inode_id: pending.inode_id,
        inode_kind: deleted.inode_kind.clone(),
        observed_seq: response.committed_seq,
        revision_no: source.revision_no,
        content_digest: source.content_digest,
        content_manifest_digest: source.content_manifest_digest,
        parent_inode_id: source.parent_inode_id,
        display_name: source.display_name,
        is_deleted: true,
    })
}

struct RemoteSourceState {
    revision_no: RevisionNo,
    content_digest: Option<String>,
    content_manifest_digest: Option<String>,
    parent_inode_id: Option<InodeId>,
    display_name: String,
}

impl RemoteSourceState {
    fn from_remote(remote: &RemoteFileStateRow) -> Self {
        Self {
            revision_no: remote.revision_no,
            content_digest: remote.content_digest.clone(),
            content_manifest_digest: remote.content_manifest_digest.clone(),
            parent_inode_id: remote.parent_inode_id,
            display_name: remote.display_name.clone(),
        }
    }

    fn from_anchor(anchor: &SyncAnchorRow) -> Self {
        Self {
            revision_no: anchor.revision_no,
            content_digest: anchor.content_digest.clone(),
            content_manifest_digest: anchor.content_manifest_digest.clone(),
            parent_inode_id: anchor.parent_inode_id,
            display_name: anchor.display_name.clone(),
        }
    }

    fn from_local(local: &LocalFileStateRow) -> Self {
        Self {
            revision_no: RevisionNo(1),
            content_digest: local.content_digest.clone(),
            content_manifest_digest: None,
            parent_inode_id: local.parent_inode_id,
            display_name: local.display_name.clone(),
        }
    }
}

fn create_file_response_matches_current_state(
    response: &ClientMutationResponse,
    created: &loon_types::CreatedRemoteInode,
    views: &FileSyncViews,
) -> bool {
    let (Some(remote), Some(local), Some(anchor)) = (
        views.remote.as_ref(),
        views.local.as_ref(),
        views.sync_anchor.as_ref(),
    ) else {
        return false;
    };

    remote.namespace_id == response.namespace_id
        && remote.inode_id == created.inode_id
        && remote.inode_kind == created.inode_kind
        && remote.observed_seq == response.committed_seq
        && remote.revision_no == created.revision_no
        && remote.content_digest == created.content_digest
        && remote.parent_inode_id == Some(created.parent_inode_id)
        && remote.display_name == created.display_name
        && !remote.is_deleted
        && local.namespace_id == response.namespace_id
        && local.inode_id == created.inode_id
        && local.inode_kind == created.inode_kind
        && local.content_digest == created.content_digest
        && local.parent_inode_id == Some(created.parent_inode_id)
        && local.display_name == created.display_name
        && local.exists_on_disk
        && !local.dirty
        && anchor.namespace_id == response.namespace_id
        && anchor.inode_id == created.inode_id
        && anchor.inode_kind == created.inode_kind
        && anchor.synced_seq == response.committed_seq
        && anchor.revision_no == created.revision_no
        && anchor.content_digest == created.content_digest
        && anchor.parent_inode_id == Some(created.parent_inode_id)
        && anchor.display_name == created.display_name
}
