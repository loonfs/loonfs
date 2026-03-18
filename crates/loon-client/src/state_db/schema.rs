use super::{SqliteStateDb, StateDbError};
use rusqlite::{Connection, OptionalExtension};

pub(crate) const SCHEMA_VERSION: i32 = 11;

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

const SCHEMA_V11_RENAME_SQL: &str = r#"
ALTER TABLE remote_state RENAME TO remote_state_old;
ALTER TABLE local_state RENAME TO local_state_old;
ALTER TABLE sync_anchor RENAME TO sync_anchor_old;
ALTER TABLE planned_actions RENAME TO planned_actions_old;
ALTER TABLE transfer_ledger RENAME TO transfer_ledger_old;
ALTER TABLE conflicts_and_errors RENAME TO conflicts_and_errors_old;
ALTER TABLE local_only_state RENAME TO local_only_state_old;
ALTER TABLE planned_local_only_actions RENAME TO planned_local_only_actions_old;
ALTER TABLE pending_client_mutations RENAME TO pending_client_mutations_old;
ALTER TABLE local_only_uploads RENAME TO local_only_uploads_old;
ALTER TABLE inode_uploads RENAME TO inode_uploads_old;
ALTER TABLE pending_inode_mutations RENAME TO pending_inode_mutations_old;
ALTER TABLE local_only_transfer_ledger RENAME TO local_only_transfer_ledger_old;
ALTER TABLE local_only_conflicts_and_errors RENAME TO local_only_conflicts_and_errors_old;
"#;

const SCHEMA_V11_CREATE_SQL: &str = r#"
CREATE TABLE remote_state (
    namespace_id TEXT NOT NULL,
    inode_id INTEGER NOT NULL,
    observed_seq INTEGER NOT NULL,
    revision_no INTEGER NOT NULL,
    content_digest TEXT,
    content_manifest_digest TEXT,
    parent_inode_id INTEGER,
    display_name TEXT NOT NULL,
    is_deleted INTEGER NOT NULL,
    inode_kind TEXT NOT NULL,
    PRIMARY KEY (namespace_id, inode_id),
    CHECK (is_deleted IN (0, 1)),
    CHECK (inode_kind IN ('file', 'dir', 'symlink', 'mount'))
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
    inode_kind TEXT NOT NULL,
    PRIMARY KEY (namespace_id, inode_id),
    CHECK (exists_on_disk IN (0, 1)),
    CHECK (dirty IN (0, 1)),
    CHECK (inode_kind IN ('file', 'dir', 'symlink', 'mount'))
);

CREATE TABLE sync_anchor (
    namespace_id TEXT NOT NULL,
    inode_id INTEGER NOT NULL,
    synced_seq INTEGER NOT NULL,
    revision_no INTEGER NOT NULL,
    content_digest TEXT,
    parent_inode_id INTEGER,
    display_name TEXT NOT NULL,
    inode_kind TEXT NOT NULL,
    content_manifest_digest TEXT,
    PRIMARY KEY (namespace_id, inode_id),
    CHECK (inode_kind IN ('file', 'dir', 'symlink', 'mount'))
);

CREATE TABLE planned_actions (
    namespace_id TEXT NOT NULL,
    inode_id INTEGER NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (namespace_id, inode_id),
    FOREIGN KEY (namespace_id, inode_id)
        REFERENCES local_state(namespace_id, inode_id),
    CHECK (decision IN ('create_remote_dir', 'upload_local_create', 'upload_local_edit', 'download_remote_edit', 'materialize_remote_dir', 'create_conflict_copy', 'no_op')),
    CHECK (reason IN ('already_converged', 'no_observed_state', 'local_only_directory_without_remote_identity', 'local_only_file_without_remote_identity', 'local_differs_from_anchor', 'remote_differs_from_anchor', 'local_and_remote_differ_from_anchor', 'local_observed_without_anchor', 'remote_observed_without_anchor', 'local_and_remote_observed_without_anchor'))
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
    PRIMARY KEY (transfer_id),
    FOREIGN KEY (namespace_id, inode_id)
        REFERENCES local_state(namespace_id, inode_id),
    CHECK (direction IN ('download', 'upload')),
    CHECK (state IN ('staging', 'uploading'))
);

CREATE TABLE conflicts_and_errors (
    namespace_id TEXT NOT NULL,
    inode_id INTEGER NOT NULL,
    record_id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    summary TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    CHECK (kind IN ('remote_observation_bind_ambiguous', 'download_remote_edit_remote_digest_mismatch', 'download_remote_edit_local_apply_failed', 'download_remote_edit_transfer_reset', 'materialize_remote_dir_local_apply_failed', 'upload_local_edit_upload_failed', 'upload_local_edit_transfer_reset'))
);

CREATE TABLE local_only_state (
    client_file_id TEXT NOT NULL PRIMARY KEY,
    namespace_id TEXT NOT NULL,
    parent_inode_id INTEGER,
    display_name TEXT NOT NULL,
    content_digest TEXT,
    exists_on_disk INTEGER NOT NULL,
    dirty INTEGER NOT NULL,
    last_local_change_ms INTEGER NOT NULL,
    inode_kind TEXT NOT NULL,
    CHECK (exists_on_disk IN (0, 1)),
    CHECK (dirty IN (0, 1)),
    CHECK (inode_kind IN ('file', 'dir', 'symlink', 'mount'))
);

CREATE TABLE planned_local_only_actions (
    client_file_id TEXT NOT NULL PRIMARY KEY,
    namespace_id TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    FOREIGN KEY (client_file_id)
        REFERENCES local_only_state(client_file_id)
        ON DELETE CASCADE,
    CHECK (decision IN ('create_remote_dir', 'upload_local_create', 'upload_local_edit', 'download_remote_edit', 'materialize_remote_dir', 'create_conflict_copy', 'no_op')),
    CHECK (reason IN ('already_converged', 'no_observed_state', 'local_only_directory_without_remote_identity', 'local_only_file_without_remote_identity', 'local_differs_from_anchor', 'remote_differs_from_anchor', 'local_and_remote_differ_from_anchor', 'local_observed_without_anchor', 'remote_observed_without_anchor', 'local_and_remote_observed_without_anchor'))
);

CREATE TABLE pending_client_mutations (
    client_request_id TEXT NOT NULL PRIMARY KEY,
    namespace_id TEXT NOT NULL,
    client_file_id TEXT NOT NULL UNIQUE,
    created_at_ms INTEGER NOT NULL,
    request_json TEXT
);

CREATE TABLE local_only_uploads (
    client_file_id TEXT NOT NULL PRIMARY KEY,
    namespace_id TEXT NOT NULL,
    file_digest_sha256 TEXT NOT NULL,
    content_manifest_digest TEXT NOT NULL,
    manifest_object_key TEXT NOT NULL,
    file_size_bytes INTEGER NOT NULL,
    uploaded_at_ms INTEGER NOT NULL,
    FOREIGN KEY (client_file_id)
        REFERENCES local_only_state(client_file_id)
        ON DELETE CASCADE
);

CREATE TABLE inode_uploads (
    namespace_id TEXT NOT NULL,
    inode_id INTEGER NOT NULL,
    file_digest_sha256 TEXT NOT NULL,
    content_manifest_digest TEXT NOT NULL,
    manifest_object_key TEXT NOT NULL,
    file_size_bytes INTEGER NOT NULL,
    uploaded_at_ms INTEGER NOT NULL,
    PRIMARY KEY (namespace_id, inode_id),
    FOREIGN KEY (namespace_id, inode_id)
        REFERENCES local_state(namespace_id, inode_id)
);

CREATE TABLE pending_inode_mutations (
    client_request_id TEXT NOT NULL PRIMARY KEY,
    namespace_id TEXT NOT NULL,
    inode_id INTEGER NOT NULL,
    request_json TEXT,
    created_at_ms INTEGER NOT NULL,
    UNIQUE (namespace_id, inode_id),
    FOREIGN KEY (namespace_id, inode_id)
        REFERENCES local_state(namespace_id, inode_id)
);

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
    PRIMARY KEY (transfer_id),
    FOREIGN KEY (client_file_id)
        REFERENCES local_only_state(client_file_id)
        ON DELETE CASCADE,
    CHECK (direction IN ('download', 'upload')),
    CHECK (state IN ('staging', 'uploading'))
);

CREATE TABLE local_only_conflicts_and_errors (
    client_file_id TEXT NOT NULL,
    namespace_id TEXT NOT NULL,
    record_id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    summary TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    FOREIGN KEY (client_file_id)
        REFERENCES local_only_state(client_file_id)
        ON DELETE CASCADE,
    CHECK (kind IN ('upload_local_create_upload_failed', 'upload_local_create_transfer_reset'))
);

CREATE INDEX idx_planned_actions_created_at
    ON planned_actions(created_at_ms, namespace_id, inode_id);
CREATE INDEX idx_planned_local_only_actions_created_at
    ON planned_local_only_actions(created_at_ms, client_file_id);
CREATE INDEX idx_transfer_ledger_inode_direction
    ON transfer_ledger(namespace_id, inode_id, direction);
CREATE INDEX idx_local_only_transfer_ledger_client_direction
    ON local_only_transfer_ledger(client_file_id, direction);
CREATE INDEX idx_conflicts_and_errors_inode_created_at
    ON conflicts_and_errors(namespace_id, inode_id, created_at_ms, record_id);
CREATE INDEX idx_local_only_conflicts_and_errors_client_created_at
    ON local_only_conflicts_and_errors(client_file_id, created_at_ms, record_id);
"#;

const SCHEMA_V11_COPY_SQL: &str = r#"
INSERT INTO remote_state (
    namespace_id,
    inode_id,
    observed_seq,
    revision_no,
    content_digest,
    content_manifest_digest,
    parent_inode_id,
    display_name,
    is_deleted,
    inode_kind
)
SELECT
    namespace_id,
    inode_id,
    observed_seq,
    revision_no,
    content_digest,
    content_manifest_digest,
    parent_inode_id,
    display_name,
    is_deleted,
    inode_kind
FROM remote_state_old;

INSERT INTO local_state (
    namespace_id,
    inode_id,
    content_digest,
    parent_inode_id,
    display_name,
    exists_on_disk,
    dirty,
    last_local_change_ms,
    inode_kind
)
SELECT
    namespace_id,
    inode_id,
    content_digest,
    parent_inode_id,
    display_name,
    exists_on_disk,
    dirty,
    last_local_change_ms,
    inode_kind
FROM local_state_old;

INSERT INTO sync_anchor (
    namespace_id,
    inode_id,
    synced_seq,
    revision_no,
    content_digest,
    parent_inode_id,
    display_name,
    inode_kind,
    content_manifest_digest
)
SELECT
    namespace_id,
    inode_id,
    synced_seq,
    revision_no,
    content_digest,
    parent_inode_id,
    display_name,
    inode_kind,
    content_manifest_digest
FROM sync_anchor_old;

INSERT INTO planned_actions (
    namespace_id,
    inode_id,
    decision,
    reason,
    created_at_ms
)
SELECT
    namespace_id,
    inode_id,
    decision,
    reason,
    created_at_ms
FROM planned_actions_old;

INSERT INTO transfer_ledger (
    namespace_id,
    inode_id,
    transfer_id,
    direction,
    object_key,
    block_index,
    block_count,
    state,
    updated_at_ms
)
SELECT
    namespace_id,
    inode_id,
    transfer_id,
    direction,
    object_key,
    block_index,
    block_count,
    state,
    updated_at_ms
FROM transfer_ledger_old;

INSERT INTO conflicts_and_errors (
    namespace_id,
    inode_id,
    record_id,
    kind,
    summary,
    detail_json,
    created_at_ms
)
SELECT
    namespace_id,
    inode_id,
    record_id,
    kind,
    summary,
    detail_json,
    created_at_ms
FROM conflicts_and_errors_old;

INSERT INTO local_only_state (
    client_file_id,
    namespace_id,
    parent_inode_id,
    display_name,
    content_digest,
    exists_on_disk,
    dirty,
    last_local_change_ms,
    inode_kind
)
SELECT
    client_file_id,
    namespace_id,
    parent_inode_id,
    display_name,
    content_digest,
    exists_on_disk,
    dirty,
    last_local_change_ms,
    inode_kind
FROM local_only_state_old;

INSERT INTO planned_local_only_actions (
    client_file_id,
    namespace_id,
    decision,
    reason,
    created_at_ms
)
SELECT
    client_file_id,
    namespace_id,
    decision,
    reason,
    created_at_ms
FROM planned_local_only_actions_old;

INSERT INTO pending_client_mutations (
    client_request_id,
    namespace_id,
    client_file_id,
    created_at_ms,
    request_json
)
SELECT
    client_request_id,
    namespace_id,
    client_file_id,
    created_at_ms,
    request_json
FROM pending_client_mutations_old;

INSERT INTO local_only_uploads (
    client_file_id,
    namespace_id,
    file_digest_sha256,
    content_manifest_digest,
    manifest_object_key,
    file_size_bytes,
    uploaded_at_ms
)
SELECT
    client_file_id,
    namespace_id,
    file_digest_sha256,
    content_manifest_digest,
    manifest_object_key,
    file_size_bytes,
    uploaded_at_ms
FROM local_only_uploads_old;

INSERT INTO inode_uploads (
    namespace_id,
    inode_id,
    file_digest_sha256,
    content_manifest_digest,
    manifest_object_key,
    file_size_bytes,
    uploaded_at_ms
)
SELECT
    namespace_id,
    inode_id,
    file_digest_sha256,
    content_manifest_digest,
    manifest_object_key,
    file_size_bytes,
    uploaded_at_ms
FROM inode_uploads_old;

INSERT INTO pending_inode_mutations (
    client_request_id,
    namespace_id,
    inode_id,
    request_json,
    created_at_ms
)
SELECT
    client_request_id,
    namespace_id,
    inode_id,
    request_json,
    created_at_ms
FROM pending_inode_mutations_old;

INSERT INTO local_only_transfer_ledger (
    client_file_id,
    namespace_id,
    transfer_id,
    direction,
    object_key,
    block_index,
    block_count,
    state,
    updated_at_ms
)
SELECT
    client_file_id,
    namespace_id,
    transfer_id,
    direction,
    object_key,
    block_index,
    block_count,
    state,
    updated_at_ms
FROM local_only_transfer_ledger_old;

INSERT INTO local_only_conflicts_and_errors (
    client_file_id,
    namespace_id,
    record_id,
    kind,
    summary,
    detail_json,
    created_at_ms
)
SELECT
    client_file_id,
    namespace_id,
    record_id,
    kind,
    summary,
    detail_json,
    created_at_ms
FROM local_only_conflicts_and_errors_old;
"#;

const SCHEMA_V11_DROP_OLD_SQL: &str = r#"
DROP TABLE remote_state_old;
DROP TABLE local_state_old;
DROP TABLE sync_anchor_old;
DROP TABLE planned_actions_old;
DROP TABLE transfer_ledger_old;
DROP TABLE conflicts_and_errors_old;
DROP TABLE local_only_state_old;
DROP TABLE planned_local_only_actions_old;
DROP TABLE pending_client_mutations_old;
DROP TABLE local_only_uploads_old;
DROP TABLE inode_uploads_old;
DROP TABLE pending_inode_mutations_old;
DROP TABLE local_only_transfer_ledger_old;
DROP TABLE local_only_conflicts_and_errors_old;
"#;

pub(super) fn initialize_connection(conn: &Connection) -> Result<(), StateDbError> {
    conn.pragma_update(None, "foreign_keys", "ON")?;
    Ok(())
}

#[cfg(test)]
pub(super) fn install_schema_version_for_test(
    conn: &Connection,
    target_version: i32,
) -> Result<(), StateDbError> {
    if !(0..SCHEMA_VERSION).contains(&target_version) {
        return Err(StateDbError::UnsupportedSchemaVersion(target_version));
    }

    if target_version >= 1 {
        conn.execute_batch(SCHEMA_V1_SQL)?;
        conn.pragma_update(None, "user_version", 1)?;
    }
    if target_version >= 2 {
        conn.execute_batch(SCHEMA_V2_SQL)?;
        conn.pragma_update(None, "user_version", 2)?;
    }
    if target_version >= 3 {
        conn.execute_batch(SCHEMA_V3_SQL)?;
        conn.pragma_update(None, "user_version", 3)?;
    }
    if target_version >= 4 {
        conn.execute_batch(SCHEMA_V4_SQL)?;
        conn.pragma_update(None, "user_version", 4)?;
    }
    if target_version >= 5 {
        conn.execute_batch(SCHEMA_V5_SQL)?;
        conn.pragma_update(None, "user_version", 5)?;
    }
    if target_version >= 6 {
        conn.execute_batch(SCHEMA_V6_SQL)?;
        conn.pragma_update(None, "user_version", 6)?;
    }
    if target_version >= 7 {
        conn.execute_batch(SCHEMA_V7_SQL)?;
        conn.pragma_update(None, "user_version", 7)?;
    }
    if target_version >= 8 {
        conn.execute_batch(SCHEMA_V8_SQL)?;
        conn.pragma_update(None, "user_version", 8)?;
    }
    if target_version >= 9 {
        conn.execute_batch(SCHEMA_V9_SQL)?;
        conn.pragma_update(None, "user_version", 9)?;
    }
    if target_version >= 10 {
        conn.execute_batch(SCHEMA_V10_SQL)?;
        conn.pragma_update(None, "user_version", 10)?;
    }

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
            tx.pragma_update(None, "user_version", 10)?;
            tx.commit()?;
            current_version = 10;
        }

        if current_version == 10 {
            apply_v11_hardening(&mut self.conn)?;
        }

        Ok(())
    }
}

fn apply_v11_hardening(conn: &mut Connection) -> Result<(), StateDbError> {
    conn.pragma_update(None, "foreign_keys", "OFF")?;
    let result: Result<(), StateDbError> = (|| {
        let tx = conn.transaction()?;
        tx.execute_batch(SCHEMA_V11_RENAME_SQL)?;
        tx.execute_batch(SCHEMA_V11_CREATE_SQL)?;
        tx.execute_batch(SCHEMA_V11_COPY_SQL)?;
        tx.execute_batch(SCHEMA_V11_DROP_OLD_SQL)?;
        tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        tx.commit()?;
        Ok(())
    })();
    conn.pragma_update(None, "foreign_keys", "ON")?;
    result?;
    ensure_no_foreign_key_violations(conn)?;
    Ok(())
}

fn ensure_no_foreign_key_violations(conn: &Connection) -> Result<(), StateDbError> {
    let violation = conn
        .query_row("PRAGMA foreign_key_check", [], |row| {
            let table: String = row.get(0)?;
            let rowid: i64 = row.get(1)?;
            let parent: String = row.get(2)?;
            let fk_index: i64 = row.get(3)?;
            Ok(format!(
                "table={table} rowid={rowid} parent={parent} fk_index={fk_index}"
            ))
        })
        .optional()?;
    if let Some(violation) = violation {
        return Err(StateDbError::SchemaForeignKeyViolation(violation));
    }
    Ok(())
}
