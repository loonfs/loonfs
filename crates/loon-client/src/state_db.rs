use crate::upload::UploadedContent;
use loon_types::{
    ChangeSeq, ClientMutationOp, ClientMutationRequest, ClientMutationResponse, InodeId,
    InodeKind, NamespaceId, RevisionNo,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json;
use std::path::Path;
use thiserror::Error;

const SCHEMA_VERSION: i32 = 8;
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

const SCHEMA_V3_SQL: &str = r#"
ALTER TABLE remote_state
ADD COLUMN inode_kind TEXT NOT NULL DEFAULT 'file';

ALTER TABLE local_state
ADD COLUMN inode_kind TEXT NOT NULL DEFAULT 'file';

ALTER TABLE sync_anchor
ADD COLUMN inode_kind TEXT NOT NULL DEFAULT 'file';

ALTER TABLE local_only_state
ADD COLUMN inode_kind TEXT NOT NULL DEFAULT 'file';
"#;

const SCHEMA_V4_SQL: &str = r#"
CREATE TABLE pending_client_mutations (
    client_request_id TEXT NOT NULL PRIMARY KEY,
    namespace_id TEXT NOT NULL,
    client_file_id TEXT NOT NULL UNIQUE,
    created_at_ms INTEGER NOT NULL
);
"#;

const SCHEMA_V5_SQL: &str = r#"
CREATE TABLE local_only_uploads (
    client_file_id TEXT NOT NULL PRIMARY KEY,
    namespace_id TEXT NOT NULL,
    file_digest_sha256 TEXT NOT NULL,
    content_manifest_digest TEXT NOT NULL,
    manifest_object_key TEXT NOT NULL,
    file_size_bytes INTEGER NOT NULL,
    uploaded_at_ms INTEGER NOT NULL
);
"#;

const SCHEMA_V6_SQL: &str = r#"
INSERT INTO client_metadata (key, value_integer)
VALUES ('next_client_request_id', 1);

ALTER TABLE pending_client_mutations
ADD COLUMN request_json TEXT;
"#;

const SCHEMA_V7_SQL: &str = r#"
CREATE TABLE inode_uploads (
    namespace_id TEXT NOT NULL,
    inode_id INTEGER NOT NULL,
    file_digest_sha256 TEXT NOT NULL,
    content_manifest_digest TEXT NOT NULL,
    manifest_object_key TEXT NOT NULL,
    file_size_bytes INTEGER NOT NULL,
    uploaded_at_ms INTEGER NOT NULL,
    PRIMARY KEY (namespace_id, inode_id)
);

CREATE TABLE pending_inode_mutations (
    client_request_id TEXT NOT NULL PRIMARY KEY,
    namespace_id TEXT NOT NULL,
    inode_id INTEGER NOT NULL,
    request_json TEXT,
    created_at_ms INTEGER NOT NULL,
    UNIQUE (namespace_id, inode_id)
);
"#;

const SCHEMA_V8_SQL: &str = r#"
ALTER TABLE remote_state
ADD COLUMN content_manifest_digest TEXT;

ALTER TABLE sync_anchor
ADD COLUMN content_manifest_digest TEXT;
"#;

#[derive(Debug, Error)]
pub enum StateDbError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("unsupported client state schema version {0}")]
    UnsupportedSchemaVersion(i32),
    #[error("client mutation request JSON codec error: {0}")]
    ClientMutationRequestCodec(#[from] serde_json::Error),
    #[error("SQLite integer out of range for {field}: {value}")]
    IntegerOutOfRange { field: &'static str, value: i64 },
    #[error("value out of range for SQLite {field}: {value}")]
    UnsignedOutOfRange { field: &'static str, value: u64 },
    #[error("unknown inode kind `{0}` in SQLite row")]
    UnknownInodeKind(String),
    #[error("unsupported local-only inode kind `{0:?}`")]
    UnsupportedLocalOnlyInodeKind(InodeKind),
    #[error(
        "local_only_parent_missing: namespace `{namespace_id}` parent inode `{parent_inode_id}`"
    )]
    LocalOnlyParentMissing {
        namespace_id: String,
        parent_inode_id: u64,
    },
    #[error(
        "local_only_parent_not_directory: namespace `{namespace_id}` parent inode `{parent_inode_id}`"
    )]
    LocalOnlyParentNotDirectory {
        namespace_id: String,
        parent_inode_id: u64,
    },
    #[error(
        "local_only_parent_not_bound: namespace `{namespace_id}` parent inode `{parent_inode_id}`"
    )]
    LocalOnlyParentNotBound {
        namespace_id: String,
        parent_inode_id: u64,
    },
    #[error("local_only_file_missing: `{client_file_id}`")]
    LocalOnlyFileMissing { client_file_id: String },
    #[error("uploaded_content_missing: `{client_file_id}`")]
    UploadedContentMissing { client_file_id: String },
    #[error("uploaded_content_requires_file: `{client_file_id}` kind `{inode_kind}`")]
    UploadedContentRequiresFile {
        client_file_id: String,
        inode_kind: String,
    },
    #[error("uploaded_content_local_digest_missing: `{client_file_id}`")]
    UploadedContentLocalDigestMissing { client_file_id: String },
    #[error(
        "uploaded_content_namespace_mismatch: `{client_file_id}` local namespace `{local_namespace_id}` != uploaded namespace `{uploaded_namespace_id}`"
    )]
    UploadedContentNamespaceMismatch {
        client_file_id: String,
        local_namespace_id: String,
        uploaded_namespace_id: String,
    },
    #[error(
        "uploaded_content_digest_mismatch: `{client_file_id}` local digest `{local_content_digest}` != uploaded digest `{uploaded_file_digest}`"
    )]
    UploadedContentDigestMismatch {
        client_file_id: String,
        local_content_digest: String,
        uploaded_file_digest: String,
    },
    #[error("inode_upload_missing: namespace `{namespace_id}` inode `{inode_id}`")]
    InodeUploadMissing { namespace_id: String, inode_id: u64 },
    #[error("inode_upload_requires_file: namespace `{namespace_id}` inode `{inode_id}` kind `{inode_kind}`")]
    InodeUploadRequiresFile {
        namespace_id: String,
        inode_id: u64,
        inode_kind: String,
    },
    #[error("inode_upload_local_digest_missing: namespace `{namespace_id}` inode `{inode_id}`")]
    InodeUploadLocalDigestMissing { namespace_id: String, inode_id: u64 },
    #[error(
        "inode_upload_namespace_mismatch: namespace `{namespace_id}` inode `{inode_id}` local namespace `{local_namespace_id}` != uploaded namespace `{uploaded_namespace_id}`"
    )]
    InodeUploadNamespaceMismatch {
        namespace_id: String,
        inode_id: u64,
        local_namespace_id: String,
        uploaded_namespace_id: String,
    },
    #[error(
        "inode_upload_digest_mismatch: namespace `{namespace_id}` inode `{inode_id}` local digest `{local_content_digest}` != uploaded digest `{uploaded_file_digest}`"
    )]
    InodeUploadDigestMismatch {
        namespace_id: String,
        inode_id: u64,
        local_content_digest: String,
        uploaded_file_digest: String,
    },
    #[error("pending_client_mutation_missing: `{client_request_id}`")]
    PendingClientMutationMissing { client_request_id: String },
    #[error(
        "pending_client_mutation_conflict: `{client_request_id}` existing temp `{existing_client_file_id}` != new temp `{new_client_file_id}`"
    )]
    PendingClientMutationConflict {
        client_request_id: String,
        existing_client_file_id: String,
        new_client_file_id: String,
    },
    #[error(
        "pending_client_mutation_client_file_conflict: `{client_file_id}` existing request `{existing_client_request_id}` != new request `{new_client_request_id}`"
    )]
    PendingClientMutationClientFileConflict {
        client_file_id: String,
        existing_client_request_id: String,
        new_client_request_id: String,
    },
    #[error(
        "pending_client_mutation_namespace_mismatch: `{client_request_id}` pending namespace `{pending_namespace_id}` != response namespace `{response_namespace_id}`"
    )]
    PendingClientMutationNamespaceMismatch {
        client_request_id: String,
        pending_namespace_id: String,
        response_namespace_id: String,
    },
    #[error("pending_client_mutation_request_missing: `{client_request_id}`")]
    PendingClientMutationRequestMissing { client_request_id: String },
    #[error("pending_inode_mutation_missing: `{client_request_id}`")]
    PendingInodeMutationMissing { client_request_id: String },
    #[error(
        "pending_inode_mutation_conflict: `{client_request_id}` existing inode `{existing_inode_id}` != new inode `{new_inode_id}`"
    )]
    PendingInodeMutationConflict {
        client_request_id: String,
        existing_inode_id: u64,
        new_inode_id: u64,
    },
    #[error(
        "pending_inode_mutation_inode_conflict: namespace `{namespace_id}` inode `{inode_id}` existing request `{existing_client_request_id}` != new request `{new_client_request_id}`"
    )]
    PendingInodeMutationInodeConflict {
        namespace_id: String,
        inode_id: u64,
        existing_client_request_id: String,
        new_client_request_id: String,
    },
    #[error(
        "pending_inode_mutation_namespace_mismatch: `{client_request_id}` pending namespace `{pending_namespace_id}` != response namespace `{response_namespace_id}`"
    )]
    PendingInodeMutationNamespaceMismatch {
        client_request_id: String,
        pending_namespace_id: String,
        response_namespace_id: String,
    },
    #[error("pending_inode_mutation_request_missing: `{client_request_id}`")]
    PendingInodeMutationRequestMissing { client_request_id: String },
    #[error("client_mutation_response_missing_result: `{client_request_id}`")]
    ClientMutationResponseMissingResult { client_request_id: String },
    #[error("client_mutation_response_conflicting_results: `{client_request_id}`")]
    ClientMutationResponseConflictingResults { client_request_id: String },
    #[error("upload_local_edit_state_missing: namespace `{namespace_id}` inode `{inode_id}`")]
    UploadLocalEditStateMissing { namespace_id: String, inode_id: u64 },
    #[error("upload_local_edit_requires_file: namespace `{namespace_id}` inode `{inode_id}` kind `{inode_kind}`")]
    UploadLocalEditRequiresFile {
        namespace_id: String,
        inode_id: u64,
        inode_kind: String,
    },
    #[error("upload_local_edit_path_change_not_supported: namespace `{namespace_id}` inode `{inode_id}` field `{field}` local `{local}` != anchor `{anchor}`")]
    UploadLocalEditPathChangeNotSupported {
        namespace_id: String,
        inode_id: u64,
        field: &'static str,
        local: String,
        anchor: String,
    },
    #[error("upload_local_edit_remote_not_converged: namespace `{namespace_id}` inode `{inode_id}` field `{field}` remote `{remote}` != anchor `{anchor}`")]
    UploadLocalEditRemoteNotConverged {
        namespace_id: String,
        inode_id: u64,
        field: &'static str,
        remote: String,
        anchor: String,
    },
    #[error("download_remote_edit_manifest_missing: namespace `{namespace_id}` inode `{inode_id}`")]
    DownloadRemoteEditManifestMissing { namespace_id: String, inode_id: u64 },
    #[error("download_remote_edit_remote_digest_missing: namespace `{namespace_id}` inode `{inode_id}`")]
    DownloadRemoteEditRemoteDigestMissing { namespace_id: String, inode_id: u64 },
    #[error("download_remote_edit_state_missing: namespace `{namespace_id}` inode `{inode_id}`")]
    DownloadRemoteEditStateMissing { namespace_id: String, inode_id: u64 },
    #[error("download_remote_edit_requires_file: namespace `{namespace_id}` inode `{inode_id}` kind `{inode_kind}`")]
    DownloadRemoteEditRequiresFile {
        namespace_id: String,
        inode_id: u64,
        inode_kind: String,
    },
    #[error("download_remote_edit_path_change_not_supported: namespace `{namespace_id}` inode `{inode_id}` field `{field}` remote `{remote}` != anchor `{anchor}`")]
    DownloadRemoteEditPathChangeNotSupported {
        namespace_id: String,
        inode_id: u64,
        field: &'static str,
        remote: String,
        anchor: String,
    },
    #[error("download_remote_edit_local_not_converged: namespace `{namespace_id}` inode `{inode_id}` field `{field}` local `{local}` != anchor `{anchor}`")]
    DownloadRemoteEditLocalNotConverged {
        namespace_id: String,
        inode_id: u64,
        field: &'static str,
        local: String,
        anchor: String,
    },
    #[error(
        "bind_kind_mismatch: `{client_file_id}` local kind `{local_kind}` != remote kind `{remote_kind}`"
    )]
    BindKindMismatch {
        client_file_id: String,
        local_kind: String,
        remote_kind: String,
    },
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
    pub inode_kind: InodeKind,
    pub observed_seq: ChangeSeq,
    pub revision_no: RevisionNo,
    pub content_digest: Option<String>,
    pub content_manifest_digest: Option<String>,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
    pub is_deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalFileStateRow {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
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
    pub inode_kind: InodeKind,
    pub synced_seq: ChangeSeq,
    pub revision_no: RevisionNo,
    pub content_digest: Option<String>,
    pub content_manifest_digest: Option<String>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    pub inode_kind: InodeKind,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
    pub content_digest: Option<String>,
    pub exists_on_disk: bool,
    pub dirty: bool,
    pub last_local_change_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedLocalOnlyInode {
    pub namespace_id: NamespaceId,
    pub inode_kind: InodeKind,
    pub parent_inode_id: InodeId,
    pub display_name: String,
    pub content_digest: Option<String>,
    pub exists_on_disk: bool,
    pub dirty: bool,
    pub last_local_change_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalOnlyPlannedActionRow {
    pub client_file_id: ClientFileId,
    pub namespace_id: NamespaceId,
    pub decision: String,
    pub reason: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalOnlyUploadRow {
    pub client_file_id: ClientFileId,
    pub namespace_id: NamespaceId,
    pub file_digest_sha256: String,
    pub content_manifest_digest: String,
    pub manifest_object_key: String,
    pub file_size_bytes: u64,
    pub uploaded_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InodeUploadRow {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub file_digest_sha256: String,
    pub content_manifest_digest: String,
    pub manifest_object_key: String,
    pub file_size_bytes: u64,
    pub uploaded_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingClientMutationRow {
    pub client_request_id: String,
    pub namespace_id: NamespaceId,
    pub client_file_id: ClientFileId,
    pub request: ClientMutationRequest,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingInodeMutationRow {
    pub client_request_id: String,
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub request: ClientMutationRequest,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundLocalOnlyFile {
    pub client_file_id: ClientFileId,
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedInodeMutation {
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
            tx.pragma_update(None, "user_version", 2)?;
            tx.commit()?;
            current_version = 2;
        }

        if current_version == 2 {
            let tx = self.conn.transaction()?;
            tx.execute_batch(SCHEMA_V3_SQL)?;
            tx.pragma_update(None, "user_version", 3)?;
            tx.commit()?;
            current_version = 3;
        }

        if current_version == 3 {
            let tx = self.conn.transaction()?;
            tx.execute_batch(SCHEMA_V4_SQL)?;
            tx.pragma_update(None, "user_version", 4)?;
            tx.commit()?;
            current_version = 4;
        }

        if current_version == 4 {
            let tx = self.conn.transaction()?;
            tx.execute_batch(SCHEMA_V5_SQL)?;
            tx.pragma_update(None, "user_version", 5)?;
            tx.commit()?;
            current_version = 5;
        }

        if current_version == 5 {
            let tx = self.conn.transaction()?;
            tx.execute_batch(SCHEMA_V6_SQL)?;
            tx.pragma_update(None, "user_version", 6)?;
            tx.commit()?;
            current_version = 6;
        }

        if current_version == 6 {
            let tx = self.conn.transaction()?;
            tx.execute_batch(SCHEMA_V7_SQL)?;
            tx.pragma_update(None, "user_version", 7)?;
            tx.commit()?;
            current_version = 7;
        }

        if current_version == 7 {
            let tx = self.conn.transaction()?;
            tx.execute_batch(SCHEMA_V8_SQL)?;
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
        let (remote, local, _anchor) =
            load_bound_download_remote_edit_views_from_conn(&self.tx, namespace_id, inode_id)?;
        let next_local = LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: local.inode_kind,
            content_digest: remote.content_digest.clone(),
            parent_inode_id: local.parent_inode_id,
            display_name: local.display_name,
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

        Ok(AppliedInodeMutation {
            namespace_id: namespace_id.clone(),
            inode_id,
        })
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

fn load_local_file(
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

fn load_sync_anchor(
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

fn load_next_planned_action(conn: &Connection) -> Result<Option<PlannedActionRow>, StateDbError> {
    let raw = conn
        .query_row(
            "SELECT namespace_id, inode_id, decision, reason, created_at_ms
            FROM planned_actions
            ORDER BY created_at_ms ASC, namespace_id ASC, inode_id ASC
            LIMIT 1",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
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

fn load_bound_upload_local_edit_views_from_conn(
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

fn load_bound_download_remote_edit_views_from_conn(
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

fn load_local_only_file(
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

fn load_next_planned_local_only_action(
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

fn load_local_only_upload(
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

fn load_inode_upload(
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

fn load_pending_client_mutation(
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

fn load_pending_client_mutation_for_client_file(
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

fn load_pending_inode_mutation(
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

fn load_pending_inode_mutation_for_inode(
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

fn inode_kind_as_str(kind: &InodeKind) -> &'static str {
    match kind {
        InodeKind::File => "file",
        InodeKind::Dir => "dir",
        InodeKind::Symlink => "symlink",
        InodeKind::Mount => "mount",
    }
}

fn inode_kind_from_str(value: &str) -> Result<InodeKind, StateDbError> {
    match value {
        "file" => Ok(InodeKind::File),
        "dir" => Ok(InodeKind::Dir),
        "symlink" => Ok(InodeKind::Symlink),
        "mount" => Ok(InodeKind::Mount),
        other => Err(StateDbError::UnknownInodeKind(other.to_owned())),
    }
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

fn ensure_upload_local_edit_local_anchor_match(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    field: &'static str,
    local: String,
    anchor: String,
) -> Result<(), StateDbError> {
    if local == anchor {
        return Ok(());
    }

    Err(StateDbError::UploadLocalEditPathChangeNotSupported {
        namespace_id: namespace_id.as_str().to_owned(),
        inode_id: inode_id.0,
        field,
        local,
        anchor,
    })
}

fn ensure_upload_local_edit_remote_anchor_match(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    field: &'static str,
    remote: String,
    anchor: String,
) -> Result<(), StateDbError> {
    if remote == anchor {
        return Ok(());
    }

    Err(StateDbError::UploadLocalEditRemoteNotConverged {
        namespace_id: namespace_id.as_str().to_owned(),
        inode_id: inode_id.0,
        field,
        remote,
        anchor,
    })
}

fn ensure_download_remote_edit_remote_anchor_match(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    field: &'static str,
    remote: String,
    anchor: String,
) -> Result<(), StateDbError> {
    if remote == anchor {
        return Ok(());
    }

    Err(StateDbError::DownloadRemoteEditPathChangeNotSupported {
        namespace_id: namespace_id.as_str().to_owned(),
        inode_id: inode_id.0,
        field,
        remote,
        anchor,
    })
}

fn ensure_download_remote_edit_local_anchor_match(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    field: &'static str,
    local: String,
    anchor: String,
) -> Result<(), StateDbError> {
    if local == anchor {
        return Ok(());
    }

    Err(StateDbError::DownloadRemoteEditLocalNotConverged {
        namespace_id: namespace_id.as_str().to_owned(),
        inode_id: inode_id.0,
        field,
        local,
        anchor,
    })
}

fn validate_local_only_upload(
    local_only: &LocalOnlyFileStateRow,
    upload_namespace_id: &NamespaceId,
    uploaded_file_digest: &str,
) -> Result<(), StateDbError> {
    if local_only.inode_kind != InodeKind::File {
        return Err(StateDbError::UploadedContentRequiresFile {
            client_file_id: local_only.client_file_id.as_str().to_owned(),
            inode_kind: inode_kind_as_str(&local_only.inode_kind).to_owned(),
        });
    }

    if local_only.namespace_id != *upload_namespace_id {
        return Err(StateDbError::UploadedContentNamespaceMismatch {
            client_file_id: local_only.client_file_id.as_str().to_owned(),
            local_namespace_id: local_only.namespace_id.as_str().to_owned(),
            uploaded_namespace_id: upload_namespace_id.as_str().to_owned(),
        });
    }

    let local_content_digest = local_only.content_digest.as_deref().ok_or_else(|| {
        StateDbError::UploadedContentLocalDigestMissing {
            client_file_id: local_only.client_file_id.as_str().to_owned(),
        }
    })?;

    if local_content_digest != uploaded_file_digest {
        return Err(StateDbError::UploadedContentDigestMismatch {
            client_file_id: local_only.client_file_id.as_str().to_owned(),
            local_content_digest: local_content_digest.to_owned(),
            uploaded_file_digest: uploaded_file_digest.to_owned(),
        });
    }

    Ok(())
}

fn validate_inode_upload(
    local: &LocalFileStateRow,
    upload_namespace_id: &NamespaceId,
    uploaded_file_digest: &str,
) -> Result<(), StateDbError> {
    if local.inode_kind != InodeKind::File {
        return Err(StateDbError::InodeUploadRequiresFile {
            namespace_id: local.namespace_id.as_str().to_owned(),
            inode_id: local.inode_id.0,
            inode_kind: inode_kind_as_str(&local.inode_kind).to_owned(),
        });
    }

    if local.namespace_id != *upload_namespace_id {
        return Err(StateDbError::InodeUploadNamespaceMismatch {
            namespace_id: local.namespace_id.as_str().to_owned(),
            inode_id: local.inode_id.0,
            local_namespace_id: local.namespace_id.as_str().to_owned(),
            uploaded_namespace_id: upload_namespace_id.as_str().to_owned(),
        });
    }

    let local_content_digest = local.content_digest.as_deref().ok_or_else(|| {
        StateDbError::InodeUploadLocalDigestMissing {
            namespace_id: local.namespace_id.as_str().to_owned(),
            inode_id: local.inode_id.0,
        }
    })?;

    if local_content_digest != uploaded_file_digest {
        return Err(StateDbError::InodeUploadDigestMismatch {
            namespace_id: local.namespace_id.as_str().to_owned(),
            inode_id: local.inode_id.0,
            local_content_digest: local_content_digest.to_owned(),
            uploaded_file_digest: uploaded_file_digest.to_owned(),
        });
    }

    Ok(())
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
        LocalOnlyPlannedActionRow, LocalOnlyUploadRow, ObservedLocalOnlyInode,
        PendingClientMutationRow, PlannedActionRow, RemoteFileStateRow, SqliteStateDb,
        StateDbError, SyncAnchorRow, SCHEMA_VERSION,
    };
    use crate::upload::UploadedContent;
    use loon_types::{
        ChangeSeq, ClientMutationOp, ClientMutationRequest, ClientMutationResponse,
        ContentManifestEnvelope, ContentManifestPayload, CreatedRemoteInode, InodeId, InodeKind,
        NamespaceId, RevisionNo, CONTENT_BLOCK_SIZE_BYTES,
    };

    #[test]
    fn sqlite_state_db_applies_schema_v8() {
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
            "local_only_uploads",
            "inode_uploads",
            "pending_client_mutations",
            "pending_inode_mutations",
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
    fn allocate_client_request_ids_monotonically() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let first = db
            .allocate_client_request_id()
            .expect("allocate first request id");
        let second = db
            .allocate_client_request_id()
            .expect("allocate second request id");

        assert_eq!(first, "client-req-00000000000000000001");
        assert_eq!(second, "client-req-00000000000000000002");
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
    fn load_next_planned_local_only_action_orders_deterministically() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");

        db.planner_transaction("seed-planned-local-only-actions", |tx| {
            tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
                client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000003"),
                namespace_id: NamespaceId::from("ns-1"),
                decision: "upload_local_create".to_owned(),
                reason: "local_only_file_without_remote_identity".to_owned(),
                created_at_ms: 1_700_000_300_000,
            })?;
            tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
                client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000002"),
                namespace_id: NamespaceId::from("ns-1"),
                decision: "create_remote_dir".to_owned(),
                reason: "local_only_directory_without_remote_identity".to_owned(),
                created_at_ms: 1_700_000_200_000,
            })?;
            tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
                client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000001"),
                namespace_id: NamespaceId::from("ns-1"),
                decision: "upload_local_create".to_owned(),
                reason: "local_only_file_without_remote_identity".to_owned(),
                created_at_ms: 1_700_000_200_000,
            })?;
            Ok(())
        })
        .expect("seed planned local-only actions");

        assert_eq!(
            db.load_next_planned_local_only_action()
                .expect("load next planned local-only action"),
            Some(LocalOnlyPlannedActionRow {
                client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000001"),
                namespace_id: NamespaceId::from("ns-1"),
                decision: "upload_local_create".to_owned(),
                reason: "local_only_file_without_remote_identity".to_owned(),
                created_at_ms: 1_700_000_200_000,
            })
        );
    }

    #[test]
    fn load_next_planned_action_orders_deterministically() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");

        db.planner_transaction("seed-planned-actions", |tx| {
            tx.upsert_planned_action(&PlannedActionRow {
                namespace_id: NamespaceId::from("ns-2"),
                inode_id: InodeId(9),
                decision: "download_remote_edit".to_owned(),
                reason: "remote_differs_from_anchor".to_owned(),
                created_at_ms: 1_700_000_300_000,
            })?;
            tx.upsert_planned_action(&PlannedActionRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(8),
                decision: "upload_local_edit".to_owned(),
                reason: "local_differs_from_anchor".to_owned(),
                created_at_ms: 1_700_000_200_000,
            })?;
            tx.upsert_planned_action(&PlannedActionRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(7),
                decision: "download_remote_edit".to_owned(),
                reason: "remote_differs_from_anchor".to_owned(),
                created_at_ms: 1_700_000_200_000,
            })?;
            Ok(())
        })
        .expect("seed planned actions");

        assert_eq!(
            db.load_next_planned_action()
                .expect("load next planned action"),
            Some(PlannedActionRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(7),
                decision: "download_remote_edit".to_owned(),
                reason: "remote_differs_from_anchor".to_owned(),
                created_at_ms: 1_700_000_200_000,
            })
        );
    }

    #[test]
    fn record_local_only_upload_persists_and_resolves_manifest_digest() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let local_only = sample_local_only();
        let uploaded = sample_uploaded_content();

        db.planner_transaction("seed-local-only", |tx| {
            tx.upsert_local_only_file(&local_only)?;
            Ok(())
        })
        .expect("seed local-only state");

        let recorded = db
            .record_local_only_upload(&local_only.client_file_id, &uploaded, 1_700_000_104_000)
            .expect("record local-only upload");

        assert_eq!(
            recorded,
            LocalOnlyUploadRow {
                client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000001"),
                namespace_id: NamespaceId::from("ns-1"),
                file_digest_sha256: "sha256:new-local-file".to_owned(),
                content_manifest_digest: "sha256:manifest-new-local-file".to_owned(),
                manifest_object_key:
                    "namespaces/ns-1/manifests/sha256:manifest-new-local-file.json".to_owned(),
                file_size_bytes: 15,
                uploaded_at_ms: 1_700_000_104_000,
            }
        );
        assert_eq!(
            db.load_local_only_upload(&local_only.client_file_id)
                .expect("load persisted upload"),
            Some(recorded)
        );
        assert_eq!(
            db.resolve_local_only_upload_content_manifest_digest(&local_only)
                .expect("resolve content manifest digest"),
            "sha256:manifest-new-local-file"
        );
    }

    #[test]
    fn record_local_only_upload_rejects_mismatched_digest() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let mut local_only = sample_local_only();
        local_only.content_digest = Some("sha256:edited-after-upload".to_owned());

        db.planner_transaction("seed-local-only", |tx| {
            tx.upsert_local_only_file(&local_only)?;
            Ok(())
        })
        .expect("seed local-only state");

        let error = db
            .record_local_only_upload(
                &local_only.client_file_id,
                &sample_uploaded_content(),
                1_700_000_104_000,
            )
            .expect_err("stale upload should be rejected");

        assert!(matches!(
            error,
            StateDbError::UploadedContentDigestMismatch { .. }
        ));
    }

    #[test]
    fn record_pending_client_mutation_persists_and_reuses_same_mapping() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let request = sample_client_create_file_request();
        let client_file_id = ClientFileId::from("tmp:ns-1:00000000000000000001");

        let recorded = db
            .record_pending_client_mutation(&client_file_id, &request, 1_700_000_106_000)
            .expect("record pending mutation");
        let reused = db
            .record_pending_client_mutation(&client_file_id, &request, 1_700_000_106_999)
            .expect("reuse same pending mutation");

        assert_eq!(
            recorded,
            PendingClientMutationRow {
                client_request_id: "client-req-0001".to_owned(),
                namespace_id: NamespaceId::from("ns-1"),
                client_file_id: client_file_id.clone(),
                request: request.clone(),
                created_at_ms: 1_700_000_106_000,
            }
        );
        assert_eq!(reused, recorded);
        assert_eq!(
            db.load_pending_client_mutation("client-req-0001")
                .expect("load pending mutation"),
            Some(recorded.clone())
        );
        assert_eq!(
            db.load_pending_client_mutation_for_client_file(&client_file_id)
                .expect("load pending mutation by client file"),
            Some(recorded)
        );
    }

    #[test]
    fn record_pending_client_mutation_rejects_conflicting_temp_identity() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let request = sample_client_create_file_request();

        db.record_pending_client_mutation(
            &ClientFileId::from("tmp:ns-1:00000000000000000001"),
            &request,
            1_700_000_106_000,
        )
        .expect("record pending mutation");

        let error = db
            .record_pending_client_mutation(
                &ClientFileId::from("tmp:ns-1:00000000000000000002"),
                &request,
                1_700_000_106_001,
            )
            .expect_err("conflicting mapping should fail");

        assert!(matches!(
            error,
            StateDbError::PendingClientMutationConflict { .. }
        ));
    }

    #[test]
    fn record_pending_client_mutation_rejects_conflicting_request_for_same_client_file() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let client_file_id = ClientFileId::from("tmp:ns-1:00000000000000000001");

        db.record_pending_client_mutation(
            &client_file_id,
            &sample_client_create_file_request(),
            1_700_000_106_000,
        )
        .expect("record first pending mutation");

        let error = db
            .record_pending_client_mutation(
                &client_file_id,
                &ClientMutationRequest {
                    namespace_id: NamespaceId::from("ns-1"),
                    client_request_id: "client-req-0002".to_owned(),
                    op: ClientMutationOp::CreateFile {
                        parent_inode_id: InodeId(2),
                        display_name: "draft.txt".to_owned(),
                        content_manifest_digest: "sha256:different-manifest".to_owned(),
                    },
                },
                1_700_000_106_001,
            )
            .expect_err("conflicting request for same client file should fail");

        assert!(matches!(
            error,
            StateDbError::PendingClientMutationClientFileConflict { .. }
        ));
    }

    #[test]
    fn observe_local_only_inode_under_bound_parent_allocates_and_persists_child() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        seed_bound_directory_parent(&mut db, InodeId(902));

        let observed = ObservedLocalOnlyInode {
            namespace_id: NamespaceId::from("ns-1"),
            inode_kind: InodeKind::File,
            parent_inode_id: InodeId(902),
            display_name: "note.txt".to_owned(),
            content_digest: Some("sha256:child-note".to_owned()),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 1_700_000_300_000,
        };

        let persisted = db
            .observe_local_only_inode_under_parent(&observed)
            .expect("observe local-only child");

        assert_eq!(
            persisted,
            LocalOnlyFileStateRow {
                client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000001"),
                namespace_id: NamespaceId::from("ns-1"),
                inode_kind: InodeKind::File,
                parent_inode_id: Some(InodeId(902)),
                display_name: "note.txt".to_owned(),
                content_digest: Some("sha256:child-note".to_owned()),
                exists_on_disk: true,
                dirty: true,
                last_local_change_ms: 1_700_000_300_000,
            }
        );
        assert_eq!(
            db.load_local_only_file(&persisted.client_file_id)
                .expect("load persisted child"),
            Some(persisted)
        );
    }

    #[test]
    fn observe_local_only_inode_under_parent_rejects_unbound_parent() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        db.planner_transaction("seed-unbound-parent", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(902),
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(501),
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "drafts".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(902),
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "drafts-renamed".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_200_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(902),
                inode_kind: InodeKind::Dir,
                synced_seq: ChangeSeq(501),
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "drafts".to_owned(),
            })?;
            Ok(())
        })
        .expect("seed unbound parent");

        let error = db
            .observe_local_only_inode_under_parent(&ObservedLocalOnlyInode {
                namespace_id: NamespaceId::from("ns-1"),
                inode_kind: InodeKind::File,
                parent_inode_id: InodeId(902),
                display_name: "note.txt".to_owned(),
                content_digest: Some("sha256:child-note".to_owned()),
                exists_on_disk: true,
                dirty: true,
                last_local_change_ms: 1_700_000_300_000,
            })
            .expect_err("observe should reject unbound parent");

        assert!(matches!(
            error,
            StateDbError::LocalOnlyParentNotBound {
                parent_inode_id: 902,
                ..
            }
        ));
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
            tx.record_local_only_upload(
                &client_file_id,
                &sample_uploaded_content(),
                1_700_000_104_000,
            )?;
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
                    inode_kind: InodeKind::File,
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
                    inode_kind: InodeKind::File,
                    synced_seq: ChangeSeq(500),
                    revision_no: RevisionNo(1),
                    content_digest: Some("sha256:new-local-file".to_owned()),
                    content_manifest_digest: Some(
                        "sha256:manifest-new-local-file".to_owned(),
                    ),
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
        assert_eq!(
            db.load_local_only_upload(&client_file_id)
                .expect("load temp upload row"),
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

    #[test]
    fn apply_client_mutation_response_binds_and_clears_pending_row() {
        let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
        let request = sample_client_create_file_request();
        let response = sample_client_create_file_response();
        let client_file_id = ClientFileId::from("tmp:ns-1:00000000000000000001");

        db.planner_transaction("seed-local-only-and-plan", |tx| {
            tx.upsert_local_only_file(&sample_local_only())?;
            tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
                client_file_id: client_file_id.clone(),
                namespace_id: NamespaceId::from("ns-1"),
                decision: "upload_local_create".to_owned(),
                reason: "local_only_file_without_remote_identity".to_owned(),
                created_at_ms: 1_700_000_105_000,
            })?;
            tx.record_local_only_upload(
                &client_file_id,
                &sample_uploaded_content(),
                1_700_000_104_000,
            )?;
            tx.record_pending_client_mutation(&client_file_id, &request, 1_700_000_106_000)?;
            Ok(())
        })
        .expect("seed local-only state and pending mutation");

        let bound = db
            .apply_client_mutation_response(&response)
            .expect("apply client mutation response");

        assert_eq!(
            bound,
            BoundLocalOnlyFile {
                client_file_id: client_file_id.clone(),
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(501),
            }
        );
        assert_eq!(
            db.load_file_sync_views(&NamespaceId::from("ns-1"), InodeId(501))
                .expect("load bound views"),
            FileSyncViews {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(501),
                remote: Some(RemoteFileStateRow {
                    namespace_id: NamespaceId::from("ns-1"),
                    inode_id: InodeId(501),
                    inode_kind: InodeKind::File,
                    observed_seq: ChangeSeq(42),
                    revision_no: RevisionNo(1),
                    content_digest: Some("sha256:new-local-file".to_owned()),
                    content_manifest_digest: Some(
                        "sha256:new-local-file".to_owned(),
                    ),
                    parent_inode_id: Some(InodeId(2)),
                    display_name: "draft.txt".to_owned(),
                    is_deleted: false,
                }),
                local: Some(LocalFileStateRow {
                    namespace_id: NamespaceId::from("ns-1"),
                    inode_id: InodeId(501),
                    inode_kind: InodeKind::File,
                    content_digest: Some("sha256:new-local-file".to_owned()),
                    parent_inode_id: Some(InodeId(2)),
                    display_name: "draft.txt".to_owned(),
                    exists_on_disk: true,
                    dirty: false,
                    last_local_change_ms: 1_700_000_100_000,
                }),
                sync_anchor: Some(SyncAnchorRow {
                    namespace_id: NamespaceId::from("ns-1"),
                    inode_id: InodeId(501),
                    inode_kind: InodeKind::File,
                    synced_seq: ChangeSeq(42),
                    revision_no: RevisionNo(1),
                    content_digest: Some("sha256:new-local-file".to_owned()),
                    content_manifest_digest: Some(
                        "sha256:new-local-file".to_owned(),
                    ),
                    parent_inode_id: Some(InodeId(2)),
                    display_name: "draft.txt".to_owned(),
                }),
            }
        );
        assert_eq!(
            db.load_pending_client_mutation("client-req-0001")
                .expect("load pending mutation after bind"),
            None
        );
        assert_eq!(
            db.load_local_only_file(&client_file_id)
                .expect("load temp local-only state after bind"),
            None
        );
        assert_eq!(
            db.load_local_only_upload(&client_file_id)
                .expect("load temp upload row after bind"),
            None
        );
    }

    fn sample_remote() -> RemoteFileStateRow {
        RemoteFileStateRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(420),
            revision_no: RevisionNo(18),
            content_digest: Some("sha256:remote-18".to_owned()),
            content_manifest_digest: Some("sha256:manifest-remote-18".to_owned()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            is_deleted: false,
        }
    }

    fn sample_local() -> LocalFileStateRow {
        LocalFileStateRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
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
            inode_kind: InodeKind::File,
            synced_seq: ChangeSeq(419),
            revision_no: RevisionNo(17),
            content_digest: Some("sha256:anchor-17".to_owned()),
            content_manifest_digest: Some("sha256:manifest-anchor-17".to_owned()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
        }
    }

    fn sample_local_only() -> LocalOnlyFileStateRow {
        LocalOnlyFileStateRow {
            client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000001"),
            namespace_id: NamespaceId::from("ns-1"),
            inode_kind: InodeKind::File,
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
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(500),
            revision_no: RevisionNo(1),
            content_digest: Some("sha256:new-local-file".to_owned()),
            content_manifest_digest: Some("sha256:manifest-new-local-file".to_owned()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "draft.txt".to_owned(),
            is_deleted: false,
        }
    }

    fn sample_client_create_file_request() -> ClientMutationRequest {
        ClientMutationRequest {
            namespace_id: NamespaceId::from("ns-1"),
            client_request_id: "client-req-0001".to_owned(),
            op: ClientMutationOp::CreateFile {
                parent_inode_id: InodeId(2),
                display_name: "draft.txt".to_owned(),
                content_manifest_digest: "sha256:new-local-file".to_owned(),
            },
        }
    }

    fn sample_client_create_file_response() -> ClientMutationResponse {
        ClientMutationResponse {
            namespace_id: NamespaceId::from("ns-1"),
            client_request_id: "client-req-0001".to_owned(),
            committed_seq: ChangeSeq(42),
            created_inode: Some(CreatedRemoteInode {
                inode_id: InodeId(501),
                inode_kind: InodeKind::File,
                revision_no: RevisionNo(1),
                parent_inode_id: InodeId(2),
                display_name: "draft.txt".to_owned(),
                content_digest: Some("sha256:new-local-file".to_owned()),
            }),
            replaced_file: None,
        }
    }

    fn sample_uploaded_content() -> UploadedContent {
        UploadedContent {
            namespace_id: NamespaceId::from("ns-1"),
            file_size_bytes: 15,
            file_digest_sha256: "sha256:new-local-file".to_owned(),
            content_manifest_digest: "sha256:manifest-new-local-file".to_owned(),
            manifest_object_key: "namespaces/ns-1/manifests/sha256:manifest-new-local-file.json"
                .to_owned(),
            manifest_envelope: ContentManifestEnvelope::from_payload(ContentManifestPayload {
                namespace_id: NamespaceId::from("ns-1"),
                file_size_bytes: 15,
                file_digest_sha256: "sha256:new-local-file".to_owned(),
                block_size_bytes: CONTENT_BLOCK_SIZE_BYTES,
                blocks: Vec::new(),
            })
            .expect("build sample manifest envelope"),
            block_objects: Vec::new(),
        }
    }

    fn seed_bound_directory_parent(db: &mut SqliteStateDb, inode_id: InodeId) {
        db.planner_transaction("seed-bound-directory-parent", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id,
                inode_kind: InodeKind::Dir,
                observed_seq: ChangeSeq(501),
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "drafts".to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id,
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "drafts".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_200_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id,
                inode_kind: InodeKind::Dir,
                synced_seq: ChangeSeq(501),
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: Some(InodeId(2)),
                display_name: "drafts".to_owned(),
            })?;
            Ok(())
        })
        .expect("seed bound directory parent");
    }
}
