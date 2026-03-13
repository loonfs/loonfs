use loon_types::{ChangeSeq, InodeId, NamespaceId, RevisionNo};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;

const SCHEMA_VERSION: i32 = 2;
const SCHEMA_V1_SQL: &str = r#"
CREATE TABLE remote_state (
    namespace_id TEXT NOT NULL,
    inode_id INTEGER NOT NULL,
    observed_seq INTEGER NOT NULL,
    revision_no INTEGER NOT NULL,
    content_digest TEXT,
    parent_inode_id INTEGER,
    display_name TEXT NOT NULL,
    is_deleted INTEGER NOT NULL,
    PRIMARY KEY (namespace_id, inode_id)
);

CREATE TABLE local_state (
    namespace_id TEXT NOT NULL,
    inode_id INTEGER NOT NULL,
    content_digest TEXT,
    parent_inode_id INTEGER,
    display_name TEXT NOT NULL,
    exists_on_disk INTEGER NOT NULL,
    dirty INTEGER NOT NULL,
    last_local_change_ms INTEGER NOT NULL,
    PRIMARY KEY (namespace_id, inode_id)
);

CREATE TABLE sync_anchor (
    namespace_id TEXT NOT NULL,
    inode_id INTEGER NOT NULL,
    synced_seq INTEGER NOT NULL,
    revision_no INTEGER NOT NULL,
    content_digest TEXT,
    parent_inode_id INTEGER,
    display_name TEXT NOT NULL,
    PRIMARY KEY (namespace_id, inode_id)
);

CREATE TABLE planned_actions (
    namespace_id TEXT NOT NULL,
    inode_id INTEGER NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (namespace_id, inode_id)
);

CREATE TABLE transfer_ledger (
    namespace_id TEXT NOT NULL,
    inode_id INTEGER NOT NULL,
    transfer_id TEXT NOT NULL,
    direction TEXT NOT NULL,
    object_key TEXT NOT NULL,
    block_index INTEGER NOT NULL,
    block_count INTEGER NOT NULL,
    state TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (transfer_id)
);

CREATE TABLE conflicts_and_errors (
    namespace_id TEXT NOT NULL,
    inode_id INTEGER NOT NULL,
    record_id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    summary TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
"#;

const SCHEMA_V2_SQL: &str = r#"
CREATE TABLE client_metadata (
    key TEXT NOT NULL PRIMARY KEY,
    value_integer INTEGER NOT NULL
);

INSERT INTO client_metadata (key, value_integer)
VALUES ('next_local_file_id', 1);

CREATE TABLE local_only_state (
    client_file_id TEXT NOT NULL PRIMARY KEY,
    namespace_id TEXT NOT NULL,
    parent_inode_id INTEGER,
    display_name TEXT NOT NULL,
    content_digest TEXT,
    exists_on_disk INTEGER NOT NULL,
    dirty INTEGER NOT NULL,
    last_local_change_ms INTEGER NOT NULL
);

CREATE TABLE planned_local_only_actions (
    client_file_id TEXT NOT NULL PRIMARY KEY,
    namespace_id TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
"#;

#[derive(Debug, Error)]
pub enum StateDbError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("unsupported client state schema version {0}")]
    UnsupportedSchemaVersion(i32),
    #[error("SQLite integer out of range for {field}: {value}")]
    IntegerOutOfRange { field: &'static str, value: i64 },
    #[error("value out of range for SQLite {field}: {value}")]
    UnsignedOutOfRange { field: &'static str, value: u64 },
    #[error("local_only_file_missing: `{client_file_id}`")]
    LocalOnlyFileMissing { client_file_id: String },
    #[error(
        "bind_namespace_mismatch: `{client_file_id}` local namespace `{local_namespace_id}` != remote namespace `{remote_namespace_id}`"
    )]
    BindNamespaceMismatch {
        client_file_id: String,
        local_namespace_id: String,
        remote_namespace_id: String,
    },
    #[error(
        "bind_remote_deleted: `{client_file_id}` cannot bind to deleted remote inode `{inode_id}`"
    )]
    BindRemoteDeleted {
        client_file_id: String,
        inode_id: u64,
    },
    #[error(
        "bind_observation_mismatch: `{client_file_id}` field `{field}` local `{local}` != remote `{remote}`"
    )]
    BindObservationMismatch {
        client_file_id: String,
        field: &'static str,
        local: String,
        remote: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientFileId(pub String);

impl ClientFileId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ClientFileId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteFileStateRow {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub observed_seq: ChangeSeq,
    pub revision_no: RevisionNo,
    pub content_digest: Option<String>,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
    pub is_deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalFileStateRow {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub content_digest: Option<String>,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
    pub exists_on_disk: bool,
    pub dirty: bool,
    pub last_local_change_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncAnchorRow {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub synced_seq: ChangeSeq,
    pub revision_no: RevisionNo,
    pub content_digest: Option<String>,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSyncViews {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub remote: Option<RemoteFileStateRow>,
    pub local: Option<LocalFileStateRow>,
    pub sync_anchor: Option<SyncAnchorRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedActionRow {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub decision: String,
    pub reason: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalOnlyFileStateRow {
    pub client_file_id: ClientFileId,
    pub namespace_id: NamespaceId,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
    pub content_digest: Option<String>,
    pub exists_on_disk: bool,
    pub dirty: bool,
    pub last_local_change_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalOnlyPlannedActionRow {
    pub client_file_id: ClientFileId,
    pub namespace_id: NamespaceId,
    pub decision: String,
    pub reason: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundLocalOnlyFile {
    pub client_file_id: ClientFileId,
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
}

pub struct SqliteStateDb {
    conn: Connection,
}

pub struct PlannerTxn<'db> {
    tx: Transaction<'db>,
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

    pub fn load_planned_action(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<Option<PlannedActionRow>, StateDbError> {
        load_planned_action(&self.conn, namespace_id, inode_id)
    }

    pub fn allocate_local_file_id(
        &mut self,
        namespace_id: &NamespaceId,
    ) -> Result<ClientFileId, StateDbError> {
        self.planner_transaction("allocate_local_file_id", |tx| {
            tx.allocate_local_file_id(namespace_id)
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

    pub fn bind_local_only_file_to_remote(
        &mut self,
        client_file_id: &ClientFileId,
        remote: &RemoteFileStateRow,
    ) -> Result<BoundLocalOnlyFile, StateDbError> {
        self.planner_transaction("bind_local_only_file_to_remote", |tx| {
            tx.bind_local_only_file_to_remote(client_file_id, remote)
        })
    }

    fn apply_migrations(&mut self) -> Result<(), StateDbError> {
        let mut current_version = self.schema_version()?;
        if current_version > SCHEMA_VERSION {
            return Err(StateDbError::UnsupportedSchemaVersion(current_version));
        }

        if current_version == 0 {
            let tx = self.conn.transaction()?;
            tx.execute_batch(SCHEMA_V1_SQL)?;
            tx.pragma_update(None, "user_version", 1)?;
            tx.commit()?;
            current_version = 1;
        }

        if current_version == 1 {
            let tx = self.conn.transaction()?;
            tx.execute_batch(SCHEMA_V2_SQL)?;
            tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            tx.commit()?;
        }

        Ok(())
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

    pub fn upsert_remote_file(&mut self, row: &RemoteFileStateRow) -> Result<(), StateDbError> {
        self.tx.execute(
            "INSERT INTO remote_state (
                namespace_id,
                inode_id,
                observed_seq,
                revision_no,
                content_digest,
                parent_inode_id,
                display_name,
                is_deleted
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(namespace_id, inode_id) DO UPDATE SET
                observed_seq = excluded.observed_seq,
                revision_no = excluded.revision_no,
                content_digest = excluded.content_digest,
                parent_inode_id = excluded.parent_inode_id,
                display_name = excluded.display_name,
                is_deleted = excluded.is_deleted",
            params![
                row.namespace_id.as_str(),
                to_sql_u64(row.inode_id.0, "inode_id")?,
                to_sql_u64(row.observed_seq.0, "observed_seq")?,
                to_sql_u64(row.revision_no.0, "revision_no")?,
                row.content_digest.as_deref(),
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
                content_digest,
                parent_inode_id,
                display_name,
                exists_on_disk,
                dirty,
                last_local_change_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(namespace_id, inode_id) DO UPDATE SET
                content_digest = excluded.content_digest,
                parent_inode_id = excluded.parent_inode_id,
                display_name = excluded.display_name,
                exists_on_disk = excluded.exists_on_disk,
                dirty = excluded.dirty,
                last_local_change_ms = excluded.last_local_change_ms",
            params![
                row.namespace_id.as_str(),
                to_sql_u64(row.inode_id.0, "inode_id")?,
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
                synced_seq,
                revision_no,
                content_digest,
                parent_inode_id,
                display_name
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(namespace_id, inode_id) DO UPDATE SET
                synced_seq = excluded.synced_seq,
                revision_no = excluded.revision_no,
                content_digest = excluded.content_digest,
                parent_inode_id = excluded.parent_inode_id,
                display_name = excluded.display_name",
            params![
                row.namespace_id.as_str(),
                to_sql_u64(row.inode_id.0, "inode_id")?,
                to_sql_u64(row.synced_seq.0, "synced_seq")?,
                to_sql_u64(row.revision_no.0, "revision_no")?,
                row.content_digest.as_deref(),
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
                parent_inode_id,
                display_name,
                content_digest,
                exists_on_disk,
                dirty,
                last_local_change_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(client_file_id) DO UPDATE SET
                namespace_id = excluded.namespace_id,
                parent_inode_id = excluded.parent_inode_id,
                display_name = excluded.display_name,
                content_digest = excluded.content_digest,
                exists_on_disk = excluded.exists_on_disk,
                dirty = excluded.dirty,
                last_local_change_ms = excluded.last_local_change_ms",
            params![
                row.client_file_id.as_str(),
                row.namespace_id.as_str(),
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

    pub fn bind_local_only_file_to_remote(
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
            synced_seq: remote.observed_seq,
            revision_no: remote.revision_no,
            content_digest: remote.content_digest.clone(),
            parent_inode_id: remote.parent_inode_id,
            display_name: remote.display_name.clone(),
        };

        self.upsert_remote_file(remote)?;
        self.upsert_local_file(&local_row)?;
        self.upsert_sync_anchor(&anchor_row)?;
        self.delete_planned_action(&remote.namespace_id, remote.inode_id)?;
        self.delete_planned_local_only_action(client_file_id)?;
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
}

fn initialize_connection(conn: &Connection) -> Result<(), StateDbError> {
    conn.pragma_update(None, "foreign_keys", "ON")?;
    Ok(())
}

fn load_remote_file(
    conn: &Connection,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<Option<RemoteFileStateRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT
            observed_seq,
            revision_no,
            content_digest,
            parent_inode_id,
            display_name,
            is_deleted
        FROM remote_state
        WHERE namespace_id = ?1 AND inode_id = ?2",
            params![namespace_id.as_str(), to_sql_u64(inode_id.0, "inode_id")?],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, bool>(5)?,
                ))
            },
        )
        .optional()?;

    raw.map(
        |(observed_seq, revision_no, content_digest, parent_inode_id, display_name, is_deleted)| {
            Ok(RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id,
                observed_seq: ChangeSeq(from_sql_u64(observed_seq, "observed_seq")?),
                revision_no: RevisionNo(from_sql_u64(revision_no, "revision_no")?),
                content_digest,
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

fn load_local_file(
    conn: &Connection,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<Option<LocalFileStateRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT
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
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, bool>(3)?,
                    row.get::<_, bool>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .optional()?;

    raw.map(
        |(
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

fn load_sync_anchor(
    conn: &Connection,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<Option<SyncAnchorRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT
            synced_seq,
            revision_no,
            content_digest,
            parent_inode_id,
            display_name
        FROM sync_anchor
        WHERE namespace_id = ?1 AND inode_id = ?2",
            params![namespace_id.as_str(), to_sql_u64(inode_id.0, "inode_id")?],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .optional()?;

    raw.map(
        |(synced_seq, revision_no, content_digest, parent_inode_id, display_name)| {
            Ok(SyncAnchorRow {
                namespace_id: namespace_id.clone(),
                inode_id,
                synced_seq: ChangeSeq(from_sql_u64(synced_seq, "synced_seq")?),
                revision_no: RevisionNo(from_sql_u64(revision_no, "revision_no")?),
                content_digest,
                parent_inode_id: parent_inode_id
                    .map(|value| from_sql_u64(value, "parent_inode_id").map(InodeId))
                    .transpose()?,
                display_name,
            })
        },
    )
    .transpose()
}

fn load_planned_action(
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

fn load_local_only_file(
    conn: &Connection,
    client_file_id: &ClientFileId,
) -> Result<Option<LocalOnlyFileStateRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT
                namespace_id,
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
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, bool>(4)?,
                    row.get::<_, bool>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            },
        )
        .optional()?;

    raw.map(
        |(
            namespace_id,
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

fn load_planned_local_only_action(
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

fn ensure_bind_match(
    client_file_id: &ClientFileId,
    field: &'static str,
    local: String,
    remote: String,
) -> Result<(), StateDbError> {
    if local == remote {
        return Ok(());
    }

    Err(StateDbError::BindObservationMismatch {
        client_file_id: client_file_id.as_str().to_owned(),
        field,
        local,
        remote,
    })
}

fn to_sql_u64(value: u64, field: &'static str) -> Result<i64, StateDbError> {
    i64::try_from(value).map_err(|_| StateDbError::UnsignedOutOfRange { field, value })
}

fn from_sql_u64(value: i64, field: &'static str) -> Result<u64, StateDbError> {
    u64::try_from(value).map_err(|_| StateDbError::IntegerOutOfRange { field, value })
}

#[cfg(test)]
mod tests {
    use super::{
        BoundLocalOnlyFile, ClientFileId, FileSyncViews, LocalFileStateRow, LocalOnlyFileStateRow,
        LocalOnlyPlannedActionRow, RemoteFileStateRow, SqliteStateDb, StateDbError, SyncAnchorRow,
        SCHEMA_VERSION,
    };
    use loon_types::{ChangeSeq, InodeId, NamespaceId, RevisionNo};

    #[test]
    fn sqlite_state_db_applies_schema_v2() {
        let db = SqliteStateDb::open_in_memory().expect("open in-memory DB");

        assert_eq!(
            db.schema_version().expect("read schema version"),
            SCHEMA_VERSION
        );

        for table in [
            "remote_state",
            "local_state",
            "sync_anchor",
            "planned_actions",
            "client_metadata",
            "local_only_state",
            "planned_local_only_actions",
            "transfer_ledger",
            "conflicts_and_errors",
        ] {
            let exists: i64 = db
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                    [table],
                    |row| row.get(0),
                )
                .expect("query sqlite_master");
            assert_eq!(exists, 1, "expected table {table} to exist");
        }
    }

    #[test]
    fn planner_transaction_rolls_back_partial_write() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let namespace_id = NamespaceId::from("ns-1");
        let inode_id = InodeId(42);

        let error = db
            .planner_transaction("rollback-test", |tx| {
                tx.upsert_remote_file(&sample_remote())?;
                Err::<(), _>(StateDbError::UnsupportedSchemaVersion(99))
            })
            .expect_err("transaction should fail");

        assert!(matches!(error, StateDbError::UnsupportedSchemaVersion(99)));
        assert_eq!(
            db.load_file_sync_views(&namespace_id, inode_id)
                .expect("load views after rollback"),
            FileSyncViews {
                namespace_id,
                inode_id,
                remote: None,
                local: None,
                sync_anchor: None,
            }
        );
    }

    #[test]
    fn planner_transaction_persists_three_views() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let namespace_id = NamespaceId::from("ns-1");
        let inode_id = InodeId(42);

        db.planner_transaction("seed-views", |tx| {
            tx.upsert_remote_file(&sample_remote())?;
            tx.upsert_local_file(&sample_local())?;
            tx.upsert_sync_anchor(&sample_anchor())?;
            Ok(())
        })
        .expect("seed state");

        let views = db
            .load_file_sync_views(&namespace_id, inode_id)
            .expect("load seeded views");
        assert_eq!(views.remote, Some(sample_remote()));
        assert_eq!(views.local, Some(sample_local()));
        assert_eq!(views.sync_anchor, Some(sample_anchor()));
    }

    #[test]
    fn allocate_local_file_ids_monotonically() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let first = db
            .allocate_local_file_id(&NamespaceId::from("ns-1"))
            .expect("allocate first temp id");
        let second = db
            .allocate_local_file_id(&NamespaceId::from("ns-1"))
            .expect("allocate second temp id");

        assert_eq!(first, ClientFileId::from("tmp:ns-1:00000000000000000001"));
        assert_eq!(second, ClientFileId::from("tmp:ns-1:00000000000000000002"));
    }

    #[test]
    fn planner_transaction_persists_local_only_state() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let local_only = sample_local_only();

        db.planner_transaction("seed-local-only", |tx| {
            tx.upsert_local_only_file(&local_only)?;
            Ok(())
        })
        .expect("seed local-only state");

        assert_eq!(
            db.load_local_only_file(&local_only.client_file_id)
                .expect("load local-only state"),
            Some(local_only)
        );
    }

    #[test]
    fn bind_local_only_file_to_remote_migrates_into_inode_keyed_tables() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let client_file_id = ClientFileId::from("tmp:ns-1:00000000000000000001");
        let remote = sample_bound_remote();

        db.planner_transaction("seed-bindable-local-only", |tx| {
            tx.upsert_local_only_file(&sample_local_only())?;
            tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
                client_file_id: client_file_id.clone(),
                namespace_id: NamespaceId::from("ns-1"),
                decision: "upload_local_create".to_owned(),
                reason: "local_only_file_without_remote_identity".to_owned(),
                created_at_ms: 1_700_000_105_000,
            })?;
            Ok(())
        })
        .expect("seed bindable local-only state");

        let bound = db
            .bind_local_only_file_to_remote(&client_file_id, &remote)
            .expect("bind local-only file");

        assert_eq!(
            bound,
            BoundLocalOnlyFile {
                client_file_id: client_file_id.clone(),
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(901),
            }
        );
        assert_eq!(
            db.load_file_sync_views(&NamespaceId::from("ns-1"), InodeId(901))
                .expect("load bound views"),
            FileSyncViews {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(901),
                remote: Some(remote.clone()),
                local: Some(LocalFileStateRow {
                    namespace_id: NamespaceId::from("ns-1"),
                    inode_id: InodeId(901),
                    content_digest: Some("sha256:new-local-file".to_owned()),
                    parent_inode_id: Some(InodeId(2)),
                    display_name: "draft.txt".to_owned(),
                    exists_on_disk: true,
                    dirty: false,
                    last_local_change_ms: 1_700_000_100_000,
                }),
                sync_anchor: Some(SyncAnchorRow {
                    namespace_id: NamespaceId::from("ns-1"),
                    inode_id: InodeId(901),
                    synced_seq: ChangeSeq(500),
                    revision_no: RevisionNo(1),
                    content_digest: Some("sha256:new-local-file".to_owned()),
                    parent_inode_id: Some(InodeId(2)),
                    display_name: "draft.txt".to_owned(),
                }),
            }
        );
        assert_eq!(
            db.load_local_only_file(&client_file_id)
                .expect("load temp local-only state"),
            None
        );
        assert_eq!(
            db.load_planned_local_only_action(&client_file_id)
                .expect("load temp planned action"),
            None
        );
    }

    #[test]
    fn bind_local_only_file_to_remote_rejects_mismatched_observation_without_partial_write() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let client_file_id = ClientFileId::from("tmp:ns-1:00000000000000000001");
        let mut remote = sample_bound_remote();
        remote.display_name = "renamed.txt".to_owned();

        db.planner_transaction("seed-mismatched-local-only", |tx| {
            tx.upsert_local_only_file(&sample_local_only())?;
            Ok(())
        })
        .expect("seed local-only state");

        let error = db
            .bind_local_only_file_to_remote(&client_file_id, &remote)
            .expect_err("bind should reject mismatched observation");

        assert!(matches!(
            error,
            StateDbError::BindObservationMismatch {
                field: "display_name",
                ..
            }
        ));
        assert_eq!(
            db.load_file_sync_views(&NamespaceId::from("ns-1"), InodeId(901))
                .expect("load views after rejected bind"),
            FileSyncViews {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(901),
                remote: None,
                local: None,
                sync_anchor: None,
            }
        );
        assert_eq!(
            db.load_local_only_file(&client_file_id)
                .expect("load temp local-only state after rejection"),
            Some(sample_local_only())
        );
    }

    fn sample_remote() -> RemoteFileStateRow {
        RemoteFileStateRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            observed_seq: ChangeSeq(420),
            revision_no: RevisionNo(18),
            content_digest: Some("sha256:remote-18".to_owned()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            is_deleted: false,
        }
    }

    fn sample_local() -> LocalFileStateRow {
        LocalFileStateRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            content_digest: Some("sha256:local-edit".to_owned()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 1_700_000_001_000,
        }
    }

    fn sample_anchor() -> SyncAnchorRow {
        SyncAnchorRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            synced_seq: ChangeSeq(419),
            revision_no: RevisionNo(17),
            content_digest: Some("sha256:anchor-17".to_owned()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
        }
    }

    fn sample_local_only() -> LocalOnlyFileStateRow {
        LocalOnlyFileStateRow {
            client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000001"),
            namespace_id: NamespaceId::from("ns-1"),
            parent_inode_id: Some(InodeId(2)),
            display_name: "draft.txt".to_owned(),
            content_digest: Some("sha256:new-local-file".to_owned()),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 1_700_000_100_000,
        }
    }

    fn sample_bound_remote() -> RemoteFileStateRow {
        RemoteFileStateRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(901),
            observed_seq: ChangeSeq(500),
            revision_no: RevisionNo(1),
            content_digest: Some("sha256:new-local-file".to_owned()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "draft.txt".to_owned(),
            is_deleted: false,
        }
    }
}
