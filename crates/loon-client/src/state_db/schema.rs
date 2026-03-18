use super::{SqliteStateDb, StateDbError};
use rusqlite::Connection;

pub(crate) const SCHEMA_VERSION: i32 = 10;

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

const SCHEMA_V9_SQL: &str = r#"
CREATE TABLE local_only_transfer_ledger (
    client_file_id TEXT NOT NULL,
    namespace_id TEXT NOT NULL,
    transfer_id TEXT NOT NULL,
    direction TEXT NOT NULL,
    object_key TEXT NOT NULL,
    block_index INTEGER NOT NULL,
    block_count INTEGER NOT NULL,
    state TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (transfer_id)
);
"#;

const SCHEMA_V10_SQL: &str = r#"
CREATE TABLE local_only_conflicts_and_errors (
    client_file_id TEXT NOT NULL,
    namespace_id TEXT NOT NULL,
    record_id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    summary TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
"#;

pub(super) fn initialize_connection(conn: &Connection) -> Result<(), StateDbError> {
    conn.pragma_update(None, "foreign_keys", "ON")?;
    Ok(())
}

impl SqliteStateDb {
    pub(super) fn apply_migrations(&mut self) -> Result<(), StateDbError> {
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
            tx.pragma_update(None, "user_version", 8)?;
            tx.commit()?;
            current_version = 8;
        }

        if current_version == 8 {
            let tx = self.conn.transaction()?;
            tx.execute_batch(SCHEMA_V9_SQL)?;
            tx.pragma_update(None, "user_version", 9)?;
            tx.commit()?;
            current_version = 9;
        }

        if current_version == 9 {
            let tx = self.conn.transaction()?;
            tx.execute_batch(SCHEMA_V10_SQL)?;
            tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            tx.commit()?;
        }

        Ok(())
    }
}
