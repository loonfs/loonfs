use loon_types::{ChangeSeq, InodeId, NamespaceId, RevisionNo};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;

const SCHEMA_VERSION: i32 = 1;
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

    fn apply_migrations(&mut self) -> Result<(), StateDbError> {
        let current_version = self.schema_version()?;
        if current_version > SCHEMA_VERSION {
            return Err(StateDbError::UnsupportedSchemaVersion(current_version));
        }

        if current_version == 0 {
            let tx = self.conn.transaction()?;
            tx.execute_batch(SCHEMA_V1_SQL)?;
            tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            tx.commit()?;
        }

        Ok(())
    }
}

impl PlannerTxn<'_> {
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

fn to_sql_u64(value: u64, field: &'static str) -> Result<i64, StateDbError> {
    i64::try_from(value).map_err(|_| StateDbError::UnsignedOutOfRange { field, value })
}

fn from_sql_u64(value: i64, field: &'static str) -> Result<u64, StateDbError> {
    u64::try_from(value).map_err(|_| StateDbError::IntegerOutOfRange { field, value })
}

#[cfg(test)]
mod tests {
    use super::{
        FileSyncViews, LocalFileStateRow, RemoteFileStateRow, SqliteStateDb, StateDbError,
        SyncAnchorRow, SCHEMA_VERSION,
    };
    use loon_types::{ChangeSeq, InodeId, NamespaceId, RevisionNo};

    #[test]
    fn sqlite_state_db_applies_schema_v1() {
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
}
