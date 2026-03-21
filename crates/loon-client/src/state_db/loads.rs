use super::*;
use rusqlite::{params, Connection, OptionalExtension};

pub(super) fn load_remote_file(
    conn: &Connection,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<Option<RemoteFileStateRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT
            inode_kind,
            observed_seq,
            revision_no,
            content_digest,
            content_manifest_digest,
            parent_inode_id,
            display_name,
            is_deleted
        FROM remote_state
        WHERE namespace_id = ?1 AND inode_id = ?2",
            params![namespace_id.as_str(), to_sql_u64(inode_id.0, "inode_id")?],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, bool>(7)?,
                ))
            },
        )
        .optional()?;

    raw.map(
        |(
            inode_kind,
            observed_seq,
            revision_no,
            content_digest,
            content_manifest_digest,
            parent_inode_id,
            display_name,
            is_deleted,
        )| {
            Ok(RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id,
                inode_kind: inode_kind_from_str(&inode_kind)?,
                observed_seq: ChangeSeq(from_sql_u64(observed_seq, "observed_seq")?),
                revision_no: RevisionNo(from_sql_u64(revision_no, "revision_no")?),
                content_digest,
                content_manifest_digest,
                parent_inode_id: parent_inode_id
                    .map(|value| from_sql_u64(value, "parent_inode_id").map(InodeId))
                    .transpose()?,
                display_name,
                is_deleted,
            })
        },
    )
    .transpose()
}

pub(super) fn load_local_file(
    conn: &Connection,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<Option<LocalFileStateRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT
            inode_kind,
            content_digest,
            parent_inode_id,
            display_name,
            exists_on_disk,
            dirty,
            last_local_change_ms
        FROM local_state
        WHERE namespace_id = ?1 AND inode_id = ?2",
            params![namespace_id.as_str(), to_sql_u64(inode_id.0, "inode_id")?],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, bool>(4)?,
                    row.get::<_, bool>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            },
        )
        .optional()?;

    raw.map(
        |(
            inode_kind,
            content_digest,
            parent_inode_id,
            display_name,
            exists_on_disk,
            dirty,
            last_local_change_ms,
        )| {
            Ok(LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id,
                inode_kind: inode_kind_from_str(&inode_kind)?,
                content_digest,
                parent_inode_id: parent_inode_id
                    .map(|value| from_sql_u64(value, "parent_inode_id").map(InodeId))
                    .transpose()?,
                display_name,
                exists_on_disk,
                dirty,
                last_local_change_ms: from_sql_u64(last_local_change_ms, "last_local_change_ms")?,
            })
        },
    )
    .transpose()
}

pub(super) fn load_sync_anchor(
    conn: &Connection,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<Option<SyncAnchorRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT
            inode_kind,
            synced_seq,
            revision_no,
            content_digest,
            content_manifest_digest,
            parent_inode_id,
            display_name
        FROM sync_anchor
        WHERE namespace_id = ?1 AND inode_id = ?2",
            params![namespace_id.as_str(), to_sql_u64(inode_id.0, "inode_id")?],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, String>(6)?,
                ))
            },
        )
        .optional()?;

    raw.map(
        |(
            inode_kind,
            synced_seq,
            revision_no,
            content_digest,
            content_manifest_digest,
            parent_inode_id,
            display_name,
        )| {
            Ok(SyncAnchorRow {
                namespace_id: namespace_id.clone(),
                inode_id,
                inode_kind: inode_kind_from_str(&inode_kind)?,
                synced_seq: ChangeSeq(from_sql_u64(synced_seq, "synced_seq")?),
                revision_no: RevisionNo(from_sql_u64(revision_no, "revision_no")?),
                content_digest,
                content_manifest_digest,
                parent_inode_id: parent_inode_id
                    .map(|value| from_sql_u64(value, "parent_inode_id").map(InodeId))
                    .transpose()?,
                display_name,
            })
        },
    )
    .transpose()
}

pub(super) fn load_planned_action(
    conn: &Connection,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<Option<PlannedActionRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT decision, reason, created_at_ms
        FROM planned_actions
        WHERE namespace_id = ?1 AND inode_id = ?2",
            params![namespace_id.as_str(), to_sql_u64(inode_id.0, "inode_id")?],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()?;

    raw.map(|(decision, reason, created_at_ms)| {
        Ok(PlannedActionRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            decision,
            reason,
            created_at_ms: from_sql_u64(created_at_ms, "created_at_ms")?,
        })
    })
    .transpose()
}

pub(super) fn load_conflicts_and_errors(
    conn: &Connection,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<Vec<ConflictOrErrorRow>, StateDbError> {
    let mut stmt = conn.prepare(
        "SELECT record_id, kind, summary, detail_json, created_at_ms
        FROM conflicts_and_errors
        WHERE namespace_id = ?1 AND inode_id = ?2
        ORDER BY created_at_ms ASC, record_id ASC",
    )?;
    let mut rows = stmt.query(params![
        namespace_id.as_str(),
        to_sql_u64(inode_id.0, "inode_id")?
    ])?;
    let mut issues = Vec::new();
    while let Some(row) = rows.next()? {
        let record_id = row.get::<_, i64>(0)?;
        let kind = row.get::<_, String>(1)?;
        let summary = row.get::<_, String>(2)?;
        let detail_json_text = row.get::<_, String>(3)?;
        let created_at_ms = row.get::<_, i64>(4)?;
        let detail_json = serde_json::from_str(&detail_json_text)
            .map_err(StateDbError::ConflictOrErrorDetailCodec)?;
        issues.push(ConflictOrErrorRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            record_id: from_sql_u64(record_id, "record_id")?,
            kind,
            summary,
            detail_json,
            created_at_ms: from_sql_u64(created_at_ms, "created_at_ms")?,
        });
    }
    Ok(issues)
}

pub(super) fn load_local_only_conflicts_and_errors(
    conn: &Connection,
    client_file_id: &ClientFileId,
) -> Result<Vec<LocalOnlyConflictOrErrorRow>, StateDbError> {
    let mut stmt = conn.prepare(
        "SELECT record_id, namespace_id, kind, summary, detail_json, created_at_ms
        FROM local_only_conflicts_and_errors
        WHERE client_file_id = ?1
        ORDER BY created_at_ms ASC, record_id ASC",
    )?;
    let mut rows = stmt.query(params![client_file_id.as_str()])?;
    let mut issues = Vec::new();
    while let Some(row) = rows.next()? {
        let record_id = row.get::<_, i64>(0)?;
        let namespace_id_text = row.get::<_, String>(1)?;
        let kind = row.get::<_, String>(2)?;
        let summary = row.get::<_, String>(3)?;
        let detail_json_text = row.get::<_, String>(4)?;
        let created_at_ms = row.get::<_, i64>(5)?;
        let detail_json = serde_json::from_str(&detail_json_text)
            .map_err(StateDbError::ConflictOrErrorDetailCodec)?;
        issues.push(LocalOnlyConflictOrErrorRow {
            client_file_id: client_file_id.clone(),
            namespace_id: NamespaceId::from(namespace_id_text),
            record_id: from_sql_u64(record_id, "record_id")?,
            kind,
            summary,
            detail_json,
            created_at_ms: from_sql_u64(created_at_ms, "created_at_ms")?,
        });
    }
    Ok(issues)
}

pub(super) fn load_transfer_ledger_for_inode(
    conn: &Connection,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    direction: TransferDirection,
) -> Result<Option<TransferLedgerRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT transfer_id, direction, object_key, block_index, block_count, state, updated_at_ms
            FROM transfer_ledger
            WHERE namespace_id = ?1 AND inode_id = ?2 AND direction = ?3",
            params![
                namespace_id.as_str(),
                to_sql_u64(inode_id.0, "inode_id")?,
                transfer_direction_as_str(direction),
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            },
        )
        .optional()?;

    raw.map(
        |(transfer_id, direction, object_key, block_index, block_count, state, updated_at_ms)| {
            Ok(TransferLedgerRow {
                namespace_id: namespace_id.clone(),
                inode_id,
                transfer_id,
                direction: transfer_direction_from_str(&direction)?,
                object_key,
                block_index: from_sql_u64(block_index, "block_index")?,
                block_count: from_sql_u64(block_count, "block_count")?,
                state: transfer_state_from_str(&state)?,
                updated_at_ms: from_sql_u64(updated_at_ms, "updated_at_ms")?,
            })
        },
    )
    .transpose()
}

pub(super) fn load_local_only_transfer_ledger(
    conn: &Connection,
    client_file_id: &ClientFileId,
    direction: TransferDirection,
) -> Result<Option<LocalOnlyTransferLedgerRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT
                namespace_id,
                transfer_id,
                direction,
                object_key,
                block_index,
                block_count,
                state,
                updated_at_ms
            FROM local_only_transfer_ledger
            WHERE client_file_id = ?1 AND direction = ?2",
            params![
                client_file_id.as_str(),
                transfer_direction_as_str(direction),
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, i64>(7)?,
                ))
            },
        )
        .optional()?;

    raw.map(
        |(
            namespace_id,
            transfer_id,
            direction,
            object_key,
            block_index,
            block_count,
            state,
            updated_at_ms,
        )| {
            Ok(LocalOnlyTransferLedgerRow {
                client_file_id: client_file_id.clone(),
                namespace_id: NamespaceId::from(namespace_id),
                transfer_id,
                direction: transfer_direction_from_str(&direction)?,
                object_key,
                block_index: from_sql_u64(block_index, "block_index")?,
                block_count: from_sql_u64(block_count, "block_count")?,
                state: transfer_state_from_str(&state)?,
                updated_at_ms: from_sql_u64(updated_at_ms, "updated_at_ms")?,
            })
        },
    )
    .transpose()
}

pub(super) fn load_next_planned_action(
    conn: &Connection,
) -> Result<Option<PlannedActionRow>, StateDbError> {
    load_next_planned_action_query(
        conn,
        "SELECT namespace_id, inode_id, decision, reason, created_at_ms
        FROM planned_actions
        ORDER BY created_at_ms ASC, namespace_id ASC, inode_id ASC
        LIMIT 1",
    )
}

pub(super) fn load_next_executable_planned_action(
    conn: &Connection,
) -> Result<Option<PlannedActionRow>, StateDbError> {
    load_next_planned_action_query(
        conn,
        "SELECT namespace_id, inode_id, decision, reason, created_at_ms
        FROM planned_actions
        WHERE decision IN ('upload_local_edit', 'download_remote_edit', 'apply_remote_delete', 'apply_remote_subtree_delete', 'apply_remote_rename', 'materialize_remote_dir')
        ORDER BY created_at_ms ASC, namespace_id ASC, inode_id ASC
        LIMIT 1",
    )
}

pub(super) fn load_next_deferred_planned_action(
    conn: &Connection,
) -> Result<Option<PlannedActionRow>, StateDbError> {
    load_next_planned_action_query(
        conn,
        "SELECT namespace_id, inode_id, decision, reason, created_at_ms
        FROM planned_actions
        WHERE decision NOT IN ('upload_local_edit', 'download_remote_edit', 'apply_remote_delete', 'apply_remote_subtree_delete', 'apply_remote_rename', 'materialize_remote_dir')
        ORDER BY created_at_ms ASC, namespace_id ASC, inode_id ASC
        LIMIT 1",
    )
}

fn load_next_planned_action_query(
    conn: &Connection,
    query: &str,
) -> Result<Option<PlannedActionRow>, StateDbError> {
    let raw = conn
        .query_row(query, [], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })
        .optional()?;

    raw.map(
        |(namespace_id, inode_id, decision, reason, created_at_ms)| {
            Ok(PlannedActionRow {
                namespace_id: NamespaceId::from(namespace_id),
                inode_id: InodeId(from_sql_u64(inode_id, "inode_id")?),
                decision,
                reason,
                created_at_ms: from_sql_u64(created_at_ms, "created_at_ms")?,
            })
        },
    )
    .transpose()
}

pub(super) fn load_bound_upload_local_edit_views_from_conn(
    conn: &Connection,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<(RemoteFileStateRow, LocalFileStateRow, SyncAnchorRow), StateDbError> {
    let remote = load_remote_file(conn, namespace_id, inode_id)?;
    let local = load_local_file(conn, namespace_id, inode_id)?;
    let anchor = load_sync_anchor(conn, namespace_id, inode_id)?;
    let (remote, local, anchor) = match (remote, local, anchor) {
        (Some(remote), Some(local), Some(anchor)) => (remote, local, anchor),
        _ => {
            return Err(StateDbError::UploadLocalEditStateMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })
        }
    };

    if remote.inode_kind != InodeKind::File
        || local.inode_kind != InodeKind::File
        || anchor.inode_kind != InodeKind::File
    {
        return Err(StateDbError::UploadLocalEditRequiresFile {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            inode_kind: inode_kind_as_str(&local.inode_kind).to_owned(),
        });
    }

    ensure_upload_local_edit_local_anchor_match(
        namespace_id,
        inode_id,
        "parent_inode_id",
        format!("{:?}", local.parent_inode_id),
        format!("{:?}", anchor.parent_inode_id),
    )?;
    ensure_upload_local_edit_local_anchor_match(
        namespace_id,
        inode_id,
        "display_name",
        local.display_name.clone(),
        anchor.display_name.clone(),
    )?;

    ensure_upload_local_edit_remote_anchor_match(
        namespace_id,
        inode_id,
        "revision_no",
        remote.revision_no.0.to_string(),
        anchor.revision_no.0.to_string(),
    )?;
    ensure_upload_local_edit_remote_anchor_match(
        namespace_id,
        inode_id,
        "content_digest",
        format!("{:?}", remote.content_digest),
        format!("{:?}", anchor.content_digest),
    )?;
    ensure_upload_local_edit_remote_anchor_match(
        namespace_id,
        inode_id,
        "content_manifest_digest",
        format!("{:?}", remote.content_manifest_digest),
        format!("{:?}", anchor.content_manifest_digest),
    )?;
    ensure_upload_local_edit_remote_anchor_match(
        namespace_id,
        inode_id,
        "parent_inode_id",
        format!("{:?}", remote.parent_inode_id),
        format!("{:?}", anchor.parent_inode_id),
    )?;
    ensure_upload_local_edit_remote_anchor_match(
        namespace_id,
        inode_id,
        "display_name",
        remote.display_name.clone(),
        anchor.display_name.clone(),
    )?;

    if remote.is_deleted {
        return Err(StateDbError::UploadLocalEditRemoteNotConverged {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            field: "is_deleted",
            remote: "true".to_owned(),
            anchor: "false".to_owned(),
        });
    }

    Ok((remote, local, anchor))
}

pub(super) fn load_bound_download_remote_edit_views_from_conn(
    conn: &Connection,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<(RemoteFileStateRow, LocalFileStateRow, SyncAnchorRow), StateDbError> {
    let remote = load_remote_file(conn, namespace_id, inode_id)?;
    let local = load_local_file(conn, namespace_id, inode_id)?;
    let anchor = load_sync_anchor(conn, namespace_id, inode_id)?;
    let (remote, local, anchor) = match (remote, local, anchor) {
        (Some(remote), Some(local), Some(anchor)) => (remote, local, anchor),
        _ => {
            return Err(StateDbError::DownloadRemoteEditStateMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })
        }
    };

    if remote.inode_kind != InodeKind::File
        || local.inode_kind != InodeKind::File
        || anchor.inode_kind != InodeKind::File
    {
        return Err(StateDbError::DownloadRemoteEditRequiresFile {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            inode_kind: inode_kind_as_str(&local.inode_kind).to_owned(),
        });
    }

    ensure_download_remote_edit_remote_anchor_match(
        namespace_id,
        inode_id,
        "parent_inode_id",
        format!("{:?}", remote.parent_inode_id),
        format!("{:?}", anchor.parent_inode_id),
    )?;
    ensure_download_remote_edit_remote_anchor_match(
        namespace_id,
        inode_id,
        "display_name",
        remote.display_name.clone(),
        anchor.display_name.clone(),
    )?;

    ensure_download_remote_edit_local_anchor_match(
        namespace_id,
        inode_id,
        "content_digest",
        format!("{:?}", local.content_digest),
        format!("{:?}", anchor.content_digest),
    )?;
    ensure_download_remote_edit_local_anchor_match(
        namespace_id,
        inode_id,
        "parent_inode_id",
        format!("{:?}", local.parent_inode_id),
        format!("{:?}", anchor.parent_inode_id),
    )?;
    ensure_download_remote_edit_local_anchor_match(
        namespace_id,
        inode_id,
        "display_name",
        local.display_name.clone(),
        anchor.display_name.clone(),
    )?;

    if !local.exists_on_disk {
        return Err(StateDbError::DownloadRemoteEditLocalNotConverged {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            field: "exists_on_disk",
            local: "false".to_owned(),
            anchor: "true".to_owned(),
        });
    }
    if local.dirty {
        return Err(StateDbError::DownloadRemoteEditLocalNotConverged {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            field: "dirty",
            local: "true".to_owned(),
            anchor: "false".to_owned(),
        });
    }
    if remote.is_deleted {
        return Err(StateDbError::DownloadRemoteEditPathChangeNotSupported {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            field: "is_deleted",
            remote: "true".to_owned(),
            anchor: "false".to_owned(),
        });
    }
    if remote.content_manifest_digest.is_none() {
        return Err(StateDbError::DownloadRemoteEditManifestMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        });
    }
    if remote.content_digest.is_none() {
        return Err(StateDbError::DownloadRemoteEditRemoteDigestMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        });
    }

    Ok((remote, local, anchor))
}

pub(super) fn load_bound_apply_remote_rename_views_from_conn(
    conn: &Connection,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<(RemoteFileStateRow, LocalFileStateRow, SyncAnchorRow), StateDbError> {
    let remote = load_remote_file(conn, namespace_id, inode_id)?;
    let local = load_local_file(conn, namespace_id, inode_id)?;
    let anchor = load_sync_anchor(conn, namespace_id, inode_id)?;
    let (remote, local, anchor) = match (remote, local, anchor) {
        (Some(remote), Some(local), Some(anchor)) => (remote, local, anchor),
        _ => {
            return Err(StateDbError::ApplyRemoteRenameStateMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })
        }
    };

    if remote.inode_kind != InodeKind::File
        || local.inode_kind != InodeKind::File
        || anchor.inode_kind != InodeKind::File
    {
        return Err(StateDbError::ApplyRemoteRenameRequiresFile {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            inode_kind: inode_kind_as_str(&local.inode_kind).to_owned(),
        });
    }

    ensure_apply_remote_rename_local_anchor_match(
        namespace_id,
        inode_id,
        "content_digest",
        format!("{:?}", local.content_digest),
        format!("{:?}", anchor.content_digest),
    )?;
    ensure_apply_remote_rename_local_anchor_match(
        namespace_id,
        inode_id,
        "parent_inode_id",
        format!("{:?}", local.parent_inode_id),
        format!("{:?}", anchor.parent_inode_id),
    )?;
    ensure_apply_remote_rename_local_anchor_match(
        namespace_id,
        inode_id,
        "display_name",
        local.display_name.clone(),
        anchor.display_name.clone(),
    )?;
    if !local.exists_on_disk {
        return Err(StateDbError::ApplyRemoteRenameLocalNotConverged {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            field: "exists_on_disk",
            local: "false".to_owned(),
            anchor: "true".to_owned(),
        });
    }
    if local.dirty {
        return Err(StateDbError::ApplyRemoteRenameLocalNotConverged {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            field: "dirty",
            local: "true".to_owned(),
            anchor: "false".to_owned(),
        });
    }

    ensure_apply_remote_rename_remote_anchor_match(
        namespace_id,
        inode_id,
        "revision_no",
        remote.revision_no.0.to_string(),
        anchor.revision_no.0.to_string(),
    )?;
    ensure_apply_remote_rename_remote_anchor_match(
        namespace_id,
        inode_id,
        "content_digest",
        format!("{:?}", remote.content_digest),
        format!("{:?}", anchor.content_digest),
    )?;
    ensure_apply_remote_rename_remote_anchor_match(
        namespace_id,
        inode_id,
        "content_manifest_digest",
        format!("{:?}", remote.content_manifest_digest),
        format!("{:?}", anchor.content_manifest_digest),
    )?;
    if remote.is_deleted {
        return Err(StateDbError::ApplyRemoteRenameRemoteNotPathOnly {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            field: "is_deleted",
            remote: "true".to_owned(),
            anchor: "false".to_owned(),
        });
    }
    if remote.parent_inode_id == anchor.parent_inode_id
        && remote.display_name == anchor.display_name
    {
        return Err(StateDbError::ApplyRemoteRenamePathChangeMissing {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        });
    }

    Ok((remote, local, anchor))
}

pub(super) fn load_bound_apply_remote_delete_views_from_conn(
    conn: &Connection,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<(RemoteFileStateRow, LocalFileStateRow, SyncAnchorRow), StateDbError> {
    let remote = load_remote_file(conn, namespace_id, inode_id)?;
    let local = load_local_file(conn, namespace_id, inode_id)?;
    let anchor = load_sync_anchor(conn, namespace_id, inode_id)?;
    let (remote, local, anchor) = match (remote, local, anchor) {
        (Some(remote), Some(local), Some(anchor)) => (remote, local, anchor),
        _ => {
            return Err(StateDbError::ApplyRemoteDeleteStateMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })
        }
    };

    if remote.inode_kind != InodeKind::File
        || local.inode_kind != InodeKind::File
        || anchor.inode_kind != InodeKind::File
    {
        return Err(StateDbError::ApplyRemoteDeleteRequiresFile {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            inode_kind: inode_kind_as_str(&local.inode_kind).to_owned(),
        });
    }

    ensure_apply_remote_delete_local_anchor_match(
        namespace_id,
        inode_id,
        "content_digest",
        format!("{:?}", local.content_digest),
        format!("{:?}", anchor.content_digest),
    )?;
    ensure_apply_remote_delete_local_anchor_match(
        namespace_id,
        inode_id,
        "parent_inode_id",
        format!("{:?}", local.parent_inode_id),
        format!("{:?}", anchor.parent_inode_id),
    )?;
    ensure_apply_remote_delete_local_anchor_match(
        namespace_id,
        inode_id,
        "display_name",
        local.display_name.clone(),
        anchor.display_name.clone(),
    )?;

    if !local.exists_on_disk {
        return Err(StateDbError::ApplyRemoteDeleteLocalNotConverged {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            field: "exists_on_disk",
            local: "false".to_owned(),
            anchor: "true".to_owned(),
        });
    }
    if local.dirty {
        return Err(StateDbError::ApplyRemoteDeleteLocalNotConverged {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            field: "dirty",
            local: "true".to_owned(),
            anchor: "false".to_owned(),
        });
    }
    if !remote.is_deleted {
        return Err(StateDbError::ApplyRemoteDeleteRemoteNotDeleted {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        });
    }

    Ok((remote, local, anchor))
}

struct RemoteSubtreeDeleteAnalysis {
    root_remote: Option<RemoteFileStateRow>,
    root_local: Option<LocalFileStateRow>,
    root_anchor: Option<SyncAnchorRow>,
    subtree_inode_ids: Vec<InodeId>,
    descendant_remote_inode_ids: Vec<InodeId>,
    first_busy_descendant_inode_id: Option<InodeId>,
    first_nonconverged_descendant_inode_id: Option<InodeId>,
    first_local_only_descendant_parent_inode_id: Option<InodeId>,
}

pub(super) fn assess_remote_subtree_delete_from_conn(
    conn: &Connection,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<RemoteSubtreeDeleteAssessment, StateDbError> {
    let analysis = analyze_remote_subtree_delete(conn, namespace_id, inode_id)?;
    let Some(root_remote) = analysis.root_remote else {
        return Ok(RemoteSubtreeDeleteAssessment::NotApplicable);
    };
    if root_remote.inode_kind != InodeKind::Dir || !root_remote.is_deleted {
        return Ok(RemoteSubtreeDeleteAssessment::NotApplicable);
    }

    match (analysis.root_local, analysis.root_anchor) {
        (None, None) => Ok(RemoteSubtreeDeleteAssessment::InertWithoutAnchor),
        (Some(root_local), Some(root_anchor)) => {
            if root_local.inode_kind != InodeKind::Dir || root_anchor.inode_kind != InodeKind::Dir {
                return Ok(RemoteSubtreeDeleteAssessment::DeferredRootLocalDiffers);
            }
            if !subtree_local_matches_anchor(&root_local, &root_anchor) {
                return Ok(RemoteSubtreeDeleteAssessment::DeferredRootLocalDiffers);
            }
            if analysis.first_busy_descendant_inode_id.is_some() {
                return Ok(RemoteSubtreeDeleteAssessment::DeferredDescendantsBusy);
            }
            if analysis.first_nonconverged_descendant_inode_id.is_some()
                || analysis
                    .first_local_only_descendant_parent_inode_id
                    .is_some()
            {
                return Ok(RemoteSubtreeDeleteAssessment::DeferredDescendantsDiffer);
            }

            Ok(RemoteSubtreeDeleteAssessment::Ready(
                BoundApplyRemoteSubtreeDeleteViews {
                    namespace_id: namespace_id.clone(),
                    root_inode_id: inode_id,
                    root_remote,
                    root_local,
                    root_anchor,
                    subtree_inode_ids: analysis.subtree_inode_ids,
                    descendant_remote_inode_ids: analysis.descendant_remote_inode_ids,
                },
            ))
        }
        _ => Ok(RemoteSubtreeDeleteAssessment::DeferredRootLocalDiffers),
    }
}

pub(super) fn load_bound_apply_remote_subtree_delete_views_from_conn(
    conn: &Connection,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<BoundApplyRemoteSubtreeDeleteViews, StateDbError> {
    let analysis = analyze_remote_subtree_delete(conn, namespace_id, inode_id)?;
    let root_remote =
        analysis
            .root_remote
            .ok_or_else(|| StateDbError::ApplyRemoteSubtreeDeleteStateMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
    let root_local =
        analysis
            .root_local
            .ok_or_else(|| StateDbError::ApplyRemoteSubtreeDeleteStateMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;
    let root_anchor =
        analysis
            .root_anchor
            .ok_or_else(|| StateDbError::ApplyRemoteSubtreeDeleteStateMissing {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
            })?;

    if root_remote.inode_kind != InodeKind::Dir
        || root_local.inode_kind != InodeKind::Dir
        || root_anchor.inode_kind != InodeKind::Dir
    {
        return Err(StateDbError::ApplyRemoteSubtreeDeleteRequiresDirectory {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            inode_kind: inode_kind_as_str(&root_local.inode_kind).to_owned(),
        });
    }

    ensure_apply_remote_subtree_delete_local_anchor_match(
        namespace_id,
        inode_id,
        "content_digest",
        format!("{:?}", root_local.content_digest),
        format!("{:?}", root_anchor.content_digest),
    )?;
    ensure_apply_remote_subtree_delete_local_anchor_match(
        namespace_id,
        inode_id,
        "parent_inode_id",
        format!("{:?}", root_local.parent_inode_id),
        format!("{:?}", root_anchor.parent_inode_id),
    )?;
    ensure_apply_remote_subtree_delete_local_anchor_match(
        namespace_id,
        inode_id,
        "display_name",
        root_local.display_name.clone(),
        root_anchor.display_name.clone(),
    )?;
    if !root_local.exists_on_disk {
        return Err(StateDbError::ApplyRemoteSubtreeDeleteLocalNotConverged {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            field: "exists_on_disk",
            local: "false".to_owned(),
            anchor: "true".to_owned(),
        });
    }
    if root_local.dirty {
        return Err(StateDbError::ApplyRemoteSubtreeDeleteLocalNotConverged {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            field: "dirty",
            local: "true".to_owned(),
            anchor: "false".to_owned(),
        });
    }
    if !root_remote.is_deleted {
        return Err(StateDbError::ApplyRemoteSubtreeDeleteRemoteNotDeleted {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
        });
    }
    if let Some(descendant_inode_id) = analysis.first_busy_descendant_inode_id {
        return Err(StateDbError::ApplyRemoteSubtreeDeleteDescendantBusy {
            namespace_id: namespace_id.as_str().to_owned(),
            inode_id: inode_id.0,
            descendant_inode_id: descendant_inode_id.0,
        });
    }
    if let Some(descendant_inode_id) = analysis.first_nonconverged_descendant_inode_id {
        return Err(
            StateDbError::ApplyRemoteSubtreeDeleteDescendantNotConverged {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                descendant_inode_id: descendant_inode_id.0,
                reason: "local_or_remote_mismatch",
            },
        );
    }
    if let Some(parent_inode_id) = analysis.first_local_only_descendant_parent_inode_id {
        return Err(
            StateDbError::ApplyRemoteSubtreeDeleteDescendantNotConverged {
                namespace_id: namespace_id.as_str().to_owned(),
                inode_id: inode_id.0,
                descendant_inode_id: parent_inode_id.0,
                reason: "local_only_descendant_present",
            },
        );
    }

    Ok(BoundApplyRemoteSubtreeDeleteViews {
        namespace_id: namespace_id.clone(),
        root_inode_id: inode_id,
        root_remote,
        root_local,
        root_anchor,
        subtree_inode_ids: analysis.subtree_inode_ids,
        descendant_remote_inode_ids: analysis.descendant_remote_inode_ids,
    })
}

fn analyze_remote_subtree_delete(
    conn: &Connection,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<RemoteSubtreeDeleteAnalysis, StateDbError> {
    let root_remote = load_remote_file(conn, namespace_id, inode_id)?;
    let root_local = load_local_file(conn, namespace_id, inode_id)?;
    let root_anchor = load_sync_anchor(conn, namespace_id, inode_id)?;
    let subtree_inode_ids = load_local_subtree_inode_ids(conn, namespace_id, inode_id)?;
    let descendant_remote_inode_ids =
        load_remote_subtree_descendant_inode_ids(conn, namespace_id, inode_id)?;

    let mut first_busy_descendant_inode_id = None;
    let mut first_nonconverged_descendant_inode_id = None;
    for descendant_inode_id in subtree_inode_ids
        .iter()
        .copied()
        .filter(|candidate| *candidate != inode_id)
    {
        let has_active_transfer = conn.query_row(
            "SELECT EXISTS(
                SELECT 1
                FROM transfer_ledger
                WHERE namespace_id = ?1 AND inode_id = ?2
            )",
            params![
                namespace_id.as_str(),
                to_sql_u64(descendant_inode_id.0, "inode_id")?,
            ],
            |row| row.get::<_, bool>(0),
        )?;
        let has_pending_inode_mutation =
            load_pending_inode_mutation_for_inode(conn, namespace_id, descendant_inode_id)?
                .is_some();
        if has_active_transfer || has_pending_inode_mutation {
            first_busy_descendant_inode_id = Some(descendant_inode_id);
            break;
        }

        let descendant_remote = load_remote_file(conn, namespace_id, descendant_inode_id)?;
        let descendant_local = load_local_file(conn, namespace_id, descendant_inode_id)?
            .expect("recursive local subtree should only contain local rows");
        let descendant_anchor = load_sync_anchor(conn, namespace_id, descendant_inode_id)?;
        let descendant_is_clean = match (descendant_remote.as_ref(), descendant_anchor.as_ref()) {
            (Some(remote), Some(anchor)) => {
                subtree_local_matches_anchor(&descendant_local, anchor)
                    && subtree_remote_matches_anchor(remote, anchor)
            }
            _ => false,
        };
        if !descendant_is_clean {
            first_nonconverged_descendant_inode_id = Some(descendant_inode_id);
            break;
        }
    }

    let first_local_only_descendant_parent_inode_id =
        load_local_only_descendants_under_subtree(conn, namespace_id, inode_id)?
            .into_iter()
            .find_map(|row| row.parent_inode_id);

    Ok(RemoteSubtreeDeleteAnalysis {
        root_remote,
        root_local,
        root_anchor,
        subtree_inode_ids,
        descendant_remote_inode_ids,
        first_busy_descendant_inode_id,
        first_nonconverged_descendant_inode_id,
        first_local_only_descendant_parent_inode_id,
    })
}

fn load_local_subtree_inode_ids(
    conn: &Connection,
    namespace_id: &NamespaceId,
    root_inode_id: InodeId,
) -> Result<Vec<InodeId>, StateDbError> {
    let mut stmt = conn.prepare(
        "WITH RECURSIVE rooted_local(inode_id) AS (
            SELECT inode_id
            FROM local_state
            WHERE namespace_id = ?1 AND inode_id = ?2
            UNION ALL
            SELECT child.inode_id
            FROM local_state child
            JOIN rooted_local parent ON child.parent_inode_id = parent.inode_id
            WHERE child.namespace_id = ?1
        )
        SELECT inode_id
        FROM rooted_local
        ORDER BY inode_id ASC",
    )?;
    let mut rows = stmt.query(params![
        namespace_id.as_str(),
        to_sql_u64(root_inode_id.0, "inode_id")?,
    ])?;
    let mut inode_ids = Vec::new();
    while let Some(row) = rows.next()? {
        inode_ids.push(InodeId(from_sql_u64(row.get::<_, i64>(0)?, "inode_id")?));
    }
    Ok(inode_ids)
}

fn load_remote_subtree_descendant_inode_ids(
    conn: &Connection,
    namespace_id: &NamespaceId,
    root_inode_id: InodeId,
) -> Result<Vec<InodeId>, StateDbError> {
    let mut stmt = conn.prepare(
        "WITH RECURSIVE rooted_remote(inode_id) AS (
            SELECT inode_id
            FROM remote_state
            WHERE namespace_id = ?1 AND inode_id = ?2
            UNION ALL
            SELECT child.inode_id
            FROM remote_state child
            JOIN rooted_remote parent ON child.parent_inode_id = parent.inode_id
            WHERE child.namespace_id = ?1
        )
        SELECT inode_id
        FROM rooted_remote
        WHERE inode_id != ?2
        ORDER BY inode_id ASC",
    )?;
    let mut rows = stmt.query(params![
        namespace_id.as_str(),
        to_sql_u64(root_inode_id.0, "inode_id")?,
    ])?;
    let mut inode_ids = Vec::new();
    while let Some(row) = rows.next()? {
        inode_ids.push(InodeId(from_sql_u64(row.get::<_, i64>(0)?, "inode_id")?));
    }
    Ok(inode_ids)
}

fn load_local_only_descendants_under_subtree(
    conn: &Connection,
    namespace_id: &NamespaceId,
    root_inode_id: InodeId,
) -> Result<Vec<LocalOnlyFileStateRow>, StateDbError> {
    let mut stmt = conn.prepare(
        "WITH RECURSIVE rooted_dirs(inode_id) AS (
            SELECT inode_id
            FROM local_state
            WHERE namespace_id = ?1 AND inode_id = ?2 AND inode_kind = 'dir'
            UNION ALL
            SELECT child.inode_id
            FROM local_state child
            JOIN rooted_dirs parent ON child.parent_inode_id = parent.inode_id
            WHERE child.namespace_id = ?1 AND child.inode_kind = 'dir'
        )
        SELECT
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
          AND parent_inode_id IN (SELECT inode_id FROM rooted_dirs)
        ORDER BY client_file_id ASC",
    )?;
    let mut rows = stmt.query(params![
        namespace_id.as_str(),
        to_sql_u64(root_inode_id.0, "inode_id")?,
    ])?;
    let mut descendants = Vec::new();
    while let Some(row) = rows.next()? {
        let parent_inode_id = row.get::<_, Option<i64>>(3)?;
        descendants.push(LocalOnlyFileStateRow {
            client_file_id: ClientFileId::new(row.get::<_, String>(0)?),
            namespace_id: NamespaceId::from(row.get::<_, String>(1)?),
            inode_kind: inode_kind_from_str(&row.get::<_, String>(2)?)?,
            parent_inode_id: parent_inode_id
                .map(|value| from_sql_u64(value, "parent_inode_id").map(InodeId))
                .transpose()?,
            display_name: row.get::<_, String>(4)?,
            content_digest: row.get::<_, Option<String>>(5)?,
            exists_on_disk: row.get::<_, bool>(6)?,
            dirty: row.get::<_, bool>(7)?,
            last_local_change_ms: from_sql_u64(row.get::<_, i64>(8)?, "last_local_change_ms")?,
        });
    }
    Ok(descendants)
}

fn subtree_local_matches_anchor(local: &LocalFileStateRow, anchor: &SyncAnchorRow) -> bool {
    local.exists_on_disk
        && !local.dirty
        && local.inode_kind == anchor.inode_kind
        && local.content_digest == anchor.content_digest
        && local.parent_inode_id == anchor.parent_inode_id
        && local.display_name == anchor.display_name
}

fn subtree_remote_matches_anchor(remote: &RemoteFileStateRow, anchor: &SyncAnchorRow) -> bool {
    !remote.is_deleted
        && remote.inode_kind == anchor.inode_kind
        && remote.revision_no == anchor.revision_no
        && remote.content_digest == anchor.content_digest
        && remote.content_manifest_digest == anchor.content_manifest_digest
        && remote.parent_inode_id == anchor.parent_inode_id
        && remote.display_name == anchor.display_name
}

pub(super) fn load_local_only_file(
    conn: &Connection,
    client_file_id: &ClientFileId,
) -> Result<Option<LocalOnlyFileStateRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT
                namespace_id,
                inode_kind,
                parent_inode_id,
                display_name,
                content_digest,
                exists_on_disk,
                dirty,
                last_local_change_ms
            FROM local_only_state
            WHERE client_file_id = ?1",
            params![client_file_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, bool>(5)?,
                    row.get::<_, bool>(6)?,
                    row.get::<_, i64>(7)?,
                ))
            },
        )
        .optional()?;

    raw.map(
        |(
            namespace_id,
            inode_kind,
            parent_inode_id,
            display_name,
            content_digest,
            exists_on_disk,
            dirty,
            last_local_change_ms,
        )| {
            Ok(LocalOnlyFileStateRow {
                client_file_id: client_file_id.clone(),
                namespace_id: NamespaceId::from(namespace_id),
                inode_kind: inode_kind_from_str(&inode_kind)?,
                parent_inode_id: parent_inode_id
                    .map(|value| from_sql_u64(value, "parent_inode_id").map(InodeId))
                    .transpose()?,
                display_name,
                content_digest,
                exists_on_disk,
                dirty,
                last_local_change_ms: from_sql_u64(last_local_change_ms, "last_local_change_ms")?,
            })
        },
    )
    .transpose()
}

pub(super) fn load_local_only_candidates_for_namespace(
    conn: &Connection,
    namespace_id: &NamespaceId,
) -> Result<Vec<LocalOnlyFileStateRow>, StateDbError> {
    let mut stmt = conn.prepare(
        "SELECT
            client_file_id,
            inode_kind,
            parent_inode_id,
            display_name,
            content_digest,
            exists_on_disk,
            dirty,
            last_local_change_ms
        FROM local_only_state
        WHERE namespace_id = ?1",
    )?;
    let rows = stmt.query_map(params![namespace_id.as_str()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<i64>>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, bool>(5)?,
            row.get::<_, bool>(6)?,
            row.get::<_, i64>(7)?,
        ))
    })?;

    rows.map(|row| {
        let (
            client_file_id,
            inode_kind,
            parent_inode_id,
            display_name,
            content_digest,
            exists_on_disk,
            dirty,
            last_local_change_ms,
        ) = row?;
        Ok(LocalOnlyFileStateRow {
            client_file_id: ClientFileId::new(client_file_id),
            namespace_id: namespace_id.clone(),
            inode_kind: inode_kind_from_str(&inode_kind)?,
            parent_inode_id: parent_inode_id
                .map(|value| from_sql_u64(value, "parent_inode_id").map(InodeId))
                .transpose()?,
            display_name,
            content_digest,
            exists_on_disk,
            dirty,
            last_local_change_ms: from_sql_u64(last_local_change_ms, "last_local_change_ms")?,
        })
    })
    .collect()
}

pub(super) fn load_planned_local_only_action(
    conn: &Connection,
    client_file_id: &ClientFileId,
) -> Result<Option<LocalOnlyPlannedActionRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT namespace_id, decision, reason, created_at_ms
            FROM planned_local_only_actions
            WHERE client_file_id = ?1",
            params![client_file_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()?;

    raw.map(|(namespace_id, decision, reason, created_at_ms)| {
        Ok(LocalOnlyPlannedActionRow {
            client_file_id: client_file_id.clone(),
            namespace_id: NamespaceId::from(namespace_id),
            decision,
            reason,
            created_at_ms: from_sql_u64(created_at_ms, "created_at_ms")?,
        })
    })
    .transpose()
}

pub(super) fn load_next_planned_local_only_action(
    conn: &Connection,
) -> Result<Option<LocalOnlyPlannedActionRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT client_file_id, namespace_id, decision, reason, created_at_ms
            FROM planned_local_only_actions
            ORDER BY created_at_ms ASC, client_file_id ASC
            LIMIT 1",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .optional()?;

    raw.map(
        |(client_file_id, namespace_id, decision, reason, created_at_ms)| {
            Ok(LocalOnlyPlannedActionRow {
                client_file_id: ClientFileId::from(client_file_id.as_str()),
                namespace_id: NamespaceId::from(namespace_id),
                decision,
                reason,
                created_at_ms: from_sql_u64(created_at_ms, "created_at_ms")?,
            })
        },
    )
    .transpose()
}

pub(super) fn load_local_only_upload(
    conn: &Connection,
    client_file_id: &ClientFileId,
) -> Result<Option<LocalOnlyUploadRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT
                namespace_id,
                file_digest_sha256,
                content_manifest_digest,
                manifest_object_key,
                file_size_bytes,
                uploaded_at_ms
            FROM local_only_uploads
            WHERE client_file_id = ?1",
            params![client_file_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .optional()?;

    raw.map(
        |(
            namespace_id,
            file_digest_sha256,
            content_manifest_digest,
            manifest_object_key,
            file_size_bytes,
            uploaded_at_ms,
        )| {
            Ok(LocalOnlyUploadRow {
                client_file_id: client_file_id.clone(),
                namespace_id: NamespaceId::from(namespace_id),
                file_digest_sha256,
                content_manifest_digest,
                manifest_object_key,
                file_size_bytes: from_sql_u64(file_size_bytes, "file_size_bytes")?,
                uploaded_at_ms: from_sql_u64(uploaded_at_ms, "uploaded_at_ms")?,
            })
        },
    )
    .transpose()
}

pub(super) fn load_inode_upload(
    conn: &Connection,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<Option<InodeUploadRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT
                file_digest_sha256,
                content_manifest_digest,
                manifest_object_key,
                file_size_bytes,
                uploaded_at_ms
            FROM inode_uploads
            WHERE namespace_id = ?1 AND inode_id = ?2",
            params![namespace_id.as_str(), to_sql_u64(inode_id.0, "inode_id")?],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .optional()?;

    raw.map(
        |(
            file_digest_sha256,
            content_manifest_digest,
            manifest_object_key,
            file_size_bytes,
            uploaded_at_ms,
        )| {
            Ok(InodeUploadRow {
                namespace_id: namespace_id.clone(),
                inode_id,
                file_digest_sha256,
                content_manifest_digest,
                manifest_object_key,
                file_size_bytes: from_sql_u64(file_size_bytes, "file_size_bytes")?,
                uploaded_at_ms: from_sql_u64(uploaded_at_ms, "uploaded_at_ms")?,
            })
        },
    )
    .transpose()
}

pub(super) fn load_pending_client_mutation(
    conn: &Connection,
    client_request_id: &str,
) -> Result<Option<PendingClientMutationRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT namespace_id, client_file_id, request_json, created_at_ms
            FROM pending_client_mutations
            WHERE client_request_id = ?1",
            params![client_request_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()?;

    raw.map(
        |(namespace_id, client_file_id, request_json, created_at_ms)| {
            let request_json =
                request_json.ok_or_else(|| StateDbError::PendingClientMutationRequestMissing {
                    client_request_id: client_request_id.to_owned(),
                })?;
            Ok(PendingClientMutationRow {
                client_request_id: client_request_id.to_owned(),
                namespace_id: NamespaceId::from(namespace_id),
                client_file_id: ClientFileId::new(client_file_id),
                request: serde_json::from_str(&request_json)?,
                created_at_ms: from_sql_u64(created_at_ms, "created_at_ms")?,
            })
        },
    )
    .transpose()
}

pub(super) fn load_pending_client_mutation_for_client_file(
    conn: &Connection,
    client_file_id: &ClientFileId,
) -> Result<Option<PendingClientMutationRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT client_request_id
            FROM pending_client_mutations
            WHERE client_file_id = ?1",
            params![client_file_id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()?;

    match raw {
        Some(client_request_id) => load_pending_client_mutation(conn, &client_request_id),
        None => Ok(None),
    }
}

pub(super) fn load_pending_inode_mutation(
    conn: &Connection,
    client_request_id: &str,
) -> Result<Option<PendingInodeMutationRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT namespace_id, inode_id, request_json, created_at_ms
            FROM pending_inode_mutations
            WHERE client_request_id = ?1",
            params![client_request_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()?;

    raw.map(|(namespace_id, inode_id, request_json, created_at_ms)| {
        let request_json =
            request_json.ok_or_else(|| StateDbError::PendingInodeMutationRequestMissing {
                client_request_id: client_request_id.to_owned(),
            })?;
        Ok(PendingInodeMutationRow {
            client_request_id: client_request_id.to_owned(),
            namespace_id: NamespaceId::from(namespace_id),
            inode_id: InodeId(from_sql_u64(inode_id, "inode_id")?),
            request: serde_json::from_str(&request_json)?,
            created_at_ms: from_sql_u64(created_at_ms, "created_at_ms")?,
        })
    })
    .transpose()
}

pub(super) fn load_pending_inode_mutation_for_inode(
    conn: &Connection,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<Option<PendingInodeMutationRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT client_request_id
            FROM pending_inode_mutations
            WHERE namespace_id = ?1 AND inode_id = ?2",
            params![namespace_id.as_str(), to_sql_u64(inode_id.0, "inode_id")?],
            |row| row.get::<_, String>(0),
        )
        .optional()?;

    match raw {
        Some(client_request_id) => load_pending_inode_mutation(conn, &client_request_id),
        None => Ok(None),
    }
}
