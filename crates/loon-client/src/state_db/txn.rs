use super::loads::{
    load_bound_download_remote_edit_views_from_conn, load_bound_upload_local_edit_views_from_conn,
    load_conflicts_and_errors, load_inode_upload, load_local_file,
    load_local_only_candidates_for_namespace, load_local_only_file, load_local_only_upload,
    load_next_deferred_planned_action, load_next_executable_planned_action,
    load_next_planned_action, load_next_planned_local_only_action, load_pending_client_mutation,
    load_pending_client_mutation_for_client_file, load_pending_inode_mutation,
    load_pending_inode_mutation_for_inode, load_planned_action, load_planned_local_only_action,
    load_remote_file, load_sync_anchor, load_transfer_ledger_for_inode,
};
use super::schema::initialize_connection;
use super::*;
use crate::upload::UploadedContent;
use rusqlite::{params, Connection};
use serde_json::json;
use std::path::Path;

impl ObservedRemoteInode {
    fn as_remote_file_state(&self) -> RemoteFileStateRow {
        RemoteFileStateRow {
            namespace_id: self.namespace_id.clone(),
            inode_id: self.inode_id,
            inode_kind: self.inode_kind.clone(),
            observed_seq: self.observed_seq,
            revision_no: self.revision_no,
            content_digest: self.content_digest.clone(),
            content_manifest_digest: self.content_manifest_digest.clone(),
            parent_inode_id: self.parent_inode_id,
            display_name: self.display_name.clone(),
            is_deleted: self.is_deleted,
        }
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

    pub fn load_local_only_upload(
        &self,
        client_file_id: &ClientFileId,
    ) -> Result<Option<LocalOnlyUploadRow>, StateDbError> {
        load_local_only_upload(&self.conn, client_file_id)
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

    pub fn load_transfer_ledger_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        direction: TransferDirection,
    ) -> Result<Option<TransferLedgerRow>, StateDbError> {
        load_transfer_ledger_for_inode(&self.conn, namespace_id, inode_id, direction)
    }

    pub fn observe_local_only_inode_under_parent(
        &mut self,
        observed: &ObservedLocalOnlyInode,
    ) -> Result<LocalOnlyFileStateRow, StateDbError> {
        self.planner_transaction("observe_local_only_inode_under_parent", |tx| {
            tx.observe_local_only_inode_under_parent(observed)
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

    pub fn apply_remote_observation(
        &mut self,
        observed: &ObservedRemoteInode,
        applied_at_ms: u64,
    ) -> Result<AppliedRemoteObservation, StateDbError> {
        self.planner_transaction("apply_remote_observation", |tx| {
            tx.apply_remote_observation(observed, applied_at_ms)
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

    pub fn upsert_transfer_ledger(
        &mut self,
        row: &TransferLedgerRow,
    ) -> Result<TransferLedgerRow, StateDbError> {
        self.planner_transaction("upsert_transfer_ledger", |tx| {
            tx.upsert_transfer_ledger(row)
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
}

impl PlannerTxn<'_> {
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
        Ok(row)
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
        if response.created_inode.is_none() && response.replaced_file.is_none() {
            return Err(StateDbError::ClientMutationResponseMissingResult {
                client_request_id: response.client_request_id.clone(),
            });
        }
        if response.created_inode.is_some() && response.replaced_file.is_some() {
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
            ClientMutationOp::ReplaceFile { .. } => None,
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
        let bound = self.bind_local_only_inode_to_remote(&pending.client_file_id, &remote)?;
        self.delete_pending_client_mutation(&response.client_request_id)?;

        Ok(bound)
    }

    pub fn apply_inode_mutation_response(
        &mut self,
        response: &ClientMutationResponse,
    ) -> Result<AppliedInodeMutation, StateDbError> {
        if response.created_inode.is_none() && response.replaced_file.is_none() {
            return Err(StateDbError::ClientMutationResponseMissingResult {
                client_request_id: response.client_request_id.clone(),
            });
        }
        if response.created_inode.is_some() && response.replaced_file.is_some() {
            return Err(StateDbError::ClientMutationResponseConflictingResults {
                client_request_id: response.client_request_id.clone(),
            });
        }

        let replaced = response.replaced_file.as_ref().ok_or_else(|| {
            StateDbError::ClientMutationResponseMissingResult {
                client_request_id: response.client_request_id.clone(),
            }
        })?;
        let pending = load_pending_inode_mutation(&self.tx, &response.client_request_id)?
            .ok_or_else(|| StateDbError::PendingInodeMutationMissing {
                client_request_id: response.client_request_id.clone(),
            })?;

        if pending.namespace_id != response.namespace_id {
            return Err(StateDbError::PendingInodeMutationNamespaceMismatch {
                client_request_id: response.client_request_id.clone(),
                pending_namespace_id: pending.namespace_id.as_str().to_owned(),
                response_namespace_id: response.namespace_id.as_str().to_owned(),
            });
        }

        let (remote, local, anchor) =
            self.load_bound_upload_local_edit_views(&pending.namespace_id, pending.inode_id)?;

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
        let observed_remote = observed.as_remote_file_state();
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
                self.delete_conflict_or_error_kind(
                    &observed.namespace_id,
                    observed.inode_id,
                    "remote_observation_bind_ambiguous",
                )?;
                if let Some(pending) = load_pending_inode_mutation_for_inode(
                    &self.tx,
                    &observed.namespace_id,
                    observed.inode_id,
                )? {
                    self.delete_pending_inode_mutation(&pending.client_request_id)?;
                }
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
                if let Some(pending) = load_pending_client_mutation_for_client_file(
                    &self.tx,
                    &candidate.client_file_id,
                )? {
                    self.delete_pending_client_mutation(&pending.client_request_id)?;
                }
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

        self.upsert_remote_file(remote)?;
        self.upsert_local_file(&local_row)?;
        self.upsert_sync_anchor(&anchor_row)?;
        self.delete_planned_action(&remote.namespace_id, remote.inode_id)?;
        self.delete_planned_local_only_action(client_file_id)?;
        self.delete_local_only_upload(client_file_id)?;
        self.delete_local_only_file(client_file_id)?;

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

    pub fn load_local_only_file(
        &self,
        client_file_id: &ClientFileId,
    ) -> Result<Option<LocalOnlyFileStateRow>, StateDbError> {
        load_local_only_file(&self.tx, client_file_id)
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

    pub fn load_bound_upload_local_edit_views(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<(RemoteFileStateRow, LocalFileStateRow, SyncAnchorRow), StateDbError> {
        load_bound_upload_local_edit_views_from_conn(&self.tx, namespace_id, inode_id)
    }
}
