//! `tree.db`, the transactional database that keeps one mount's state: the
//! mount's settings and position in the change feed, its root and the root's
//! ancestors, every item's record, the cache's complete files, and the
//! directories that entered the mount and aren't shown yet.

use crate::config::MountConfig;
use crate::error::{MountError, Result};
use crate::item::ItemId;
use crate::item_table::{from_sql_u64, sql_u64};
use crate::status::{PublicationStop, Transports};
use loonfs_types::{AbsolutePath, ChangeSeq, Checksum, ContentId, InodeId, PinId};
use rusqlite::{params, Connection, Transaction};
use std::path::{Path, PathBuf};

/// The version of the schema below, stored as the database's `user_version`.
const SCHEMA_VERSION: i64 = 1;

const SCHEMA: &str = "
CREATE TABLE mount (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    config TEXT NOT NULL,
    applied_seq INTEGER NOT NULL,
    root_inode_id INTEGER NOT NULL,
    root_path TEXT NOT NULL,
    next_item_id INTEGER NOT NULL,
    inline_limit_bytes INTEGER NOT NULL,
    transports TEXT NOT NULL,
    publication_stop TEXT,
    admitted INTEGER NOT NULL
);
CREATE TABLE ancestors (
    depth INTEGER PRIMARY KEY,
    inode_id INTEGER NOT NULL,
    path TEXT NOT NULL
);
CREATE TABLE items (
    item_id INTEGER PRIMARY KEY,
    remote_inode_id INTEGER UNIQUE,
    kind TEXT NOT NULL CHECK (kind IN ('file', 'directory', 'symlink')),
    c_parent INTEGER,
    c_name TEXT,
    c_binding_version TEXT,
    c_revision_no INTEGER,
    c_content_ref TEXT,
    c_size INTEGER,
    c_revision_committed_at_ms INTEGER,
    c_mode INTEGER NOT NULL,
    c_mtime_ms INTEGER NOT NULL,
    c_target TEXT,
    c_attributes_revision_no INTEGER NOT NULL,
    c_mtime_attribute_ms INTEGER,
    c_mtime_attribute_revision_no INTEGER,
    s_parent INTEGER,
    s_name TEXT,
    s_binding_version TEXT,
    s_revision_no INTEGER,
    s_content_ref TEXT,
    s_size INTEGER,
    s_revision_committed_at_ms INTEGER,
    s_mode INTEGER NOT NULL,
    s_mtime_ms INTEGER NOT NULL,
    s_target TEXT,
    s_attributes_revision_no INTEGER NOT NULL,
    s_mtime_attribute_ms INTEGER,
    s_mtime_attribute_revision_no INTEGER,
    s_hidden_by_inode_id INTEGER,
    s_hidden_by_seq INTEGER,
    s_append_only INTEGER NOT NULL,
    l_parent INTEGER,
    l_name TEXT,
    l_size INTEGER NOT NULL,
    l_mode INTEGER NOT NULL,
    l_mtime_ms INTEGER NOT NULL,
    l_target TEXT,
    l_mtime_explicit INTEGER NOT NULL,
    materialized INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    published_generation INTEGER NOT NULL,
    lowest_write_offset INTEGER,
    truncated_below_base INTEGER NOT NULL,
    fingerprint BLOB,
    dirty_groups INTEGER NOT NULL
);
CREATE INDEX items_server_parent ON items (s_parent);
CREATE INDEX items_dirty ON items (item_id) WHERE dirty_groups != 0;
CREATE INDEX items_server_content ON items (json_extract(s_content_ref, '$.content_id'));
CREATE INDEX items_committed_content ON items (json_extract(c_content_ref, '$.content_id'));
CREATE TABLE cache_files (
    content_id TEXT PRIMARY KEY,
    size_bytes INTEGER NOT NULL,
    checksum TEXT NOT NULL
);
CREATE TABLE entering_directories (
    root_item_id INTEGER PRIMARY KEY,
    snapshot_id TEXT NOT NULL,
    captured_seq INTEGER NOT NULL,
    listed INTEGER NOT NULL
);
";

/// The mount's row: its settings, its position in the feed, and its root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MountRow {
    pub(crate) config: MountConfig,
    pub(crate) applied_seq: ChangeSeq,
    pub(crate) root_inode_id: InodeId,
    pub(crate) root_path: AbsolutePath,
    pub(crate) inline_limit_bytes: u64,
    pub(crate) transports: Transports,
    pub(crate) publication_stop: Option<PublicationStop>,
}

/// One of the root's ancestors, from the root's parent up to the namespace's
/// root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Ancestor {
    pub(crate) inode_id: InodeId,
    pub(crate) path: AbsolutePath,
}

/// A complete cache file, which the cache keeps across restarts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CacheFileRow {
    pub(crate) content_id: ContentId,
    pub(crate) size_bytes: u64,
    pub(crate) checksum: Checksum,
}

/// A directory that a move or an undelete brought into the mount, listed at
/// a snapshot that the feed hasn't reached yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EnteringDirectory {
    pub(crate) root_item_id: ItemId,
    pub(crate) snapshot_id: PinId,
    pub(crate) captured_seq: ChangeSeq,
    /// Whether the listing wrote every row of the directory's subtree.
    pub(crate) listed: bool,
}

/// The open `tree.db` of one mount.
#[derive(Debug)]
pub(crate) struct Database {
    connection: Connection,
}

fn json<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("a mount row value should serialize")
}

fn decode<T: serde::de::DeserializeOwned>(value: &str) -> rusqlite::Result<T> {
    serde_json::from_str(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

fn decode_path(value: &str) -> rusqlite::Result<AbsolutePath> {
    AbsolutePath::parse(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

impl Database {
    /// Creates a new `tree.db` at `path` for a mount, replacing any file
    /// there.
    pub(crate) fn create(path: &Path, mount: &MountRow) -> Result<Self> {
        remove_files(path)?;
        let mut database = Self::connect(path)?;
        let transaction = database.connection.transaction()?;
        transaction.execute_batch(SCHEMA)?;
        transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        transaction.execute(
            "INSERT INTO mount (singleton, config, applied_seq, root_inode_id, root_path, \
             next_item_id, inline_limit_bytes, transports, publication_stop, admitted) \
             VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0)",
            params![
                json(&mount.config),
                sql_u64(mount.applied_seq.0),
                sql_u64(mount.root_inode_id.0),
                mount.root_path.as_str(),
                sql_u64(ItemId::ROOT.0 + 1),
                sql_u64(mount.inline_limit_bytes),
                json(&mount.transports),
                mount.publication_stop.as_ref().map(json),
            ],
        )?;
        transaction.commit()?;
        Ok(database)
    }

    /// Opens the `tree.db` at `path`, or returns `None` when there's none,
    /// or when admission didn't finish writing it, in which case the file is
    /// deleted. A file without a schema is one that admission created and
    /// stopped before it wrote the schema.
    pub(crate) fn open(path: &Path) -> Result<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }
        let database = Self::connect(path)?;
        let version: i64 = database
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        if version == 0 {
            drop(database);
            remove_files(path)?;
            return Ok(None);
        }
        if version != SCHEMA_VERSION {
            return Err(MountError::Database {
                message: format!(
                    "`{}` has schema version {version}, and this engine reads version \
                     {SCHEMA_VERSION}",
                    path.display()
                ),
            });
        }
        let admitted: i64 =
            database
                .connection
                .query_row("SELECT admitted FROM mount", [], |row| row.get(0))?;
        if admitted == 0 {
            drop(database);
            remove_files(path)?;
            return Ok(None);
        }
        Ok(Some(database))
    }

    /// Records that admission wrote the whole tree, so a later open resumes
    /// the state instead of admitting the mount again.
    pub(crate) fn mark_admitted(&self) -> Result<()> {
        self.connection
            .execute("UPDATE mount SET admitted = 1", [])?;
        Ok(())
    }

    fn connect(path: &Path) -> Result<Self> {
        let connection = Connection::open(path)?;
        // Write-ahead logging lets one transaction per commit stay cheap. With
        // `NORMAL`, a transaction isn't synced when it commits, only at a
        // checkpoint, so a machine crash can lose the last transactions but
        // never corrupts the file. The tree can be listed again from the
        // server, and the feed applies the lost commits again from the
        // applied sequence that the database kept.
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.pragma_update(None, "foreign_keys", true)?;
        Ok(Self { connection })
    }

    /// Returns the connection, for reads outside a transaction.
    pub(crate) fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Starts a transaction.
    pub(crate) fn transaction(&mut self) -> Result<Transaction<'_>> {
        Ok(self.connection.transaction()?)
    }

    /// Returns the mount's row.
    pub(crate) fn mount(&self) -> Result<MountRow> {
        Ok(self.connection.query_row(
            "SELECT config, applied_seq, root_inode_id, root_path, inline_limit_bytes, \
             transports, publication_stop FROM mount",
            [],
            |row| {
                let publication_stop: Option<String> = row.get(6)?;
                Ok(MountRow {
                    config: decode(&row.get::<_, String>(0)?)?,
                    applied_seq: ChangeSeq(from_sql_u64(row.get(1)?)),
                    root_inode_id: InodeId(from_sql_u64(row.get(2)?)),
                    root_path: decode_path(&row.get::<_, String>(3)?)?,
                    inline_limit_bytes: from_sql_u64(row.get(4)?),
                    transports: decode(&row.get::<_, String>(5)?)?,
                    publication_stop: publication_stop.as_deref().map(decode).transpose()?,
                })
            },
        )?)
    }
}

/// Deletes a database and its write-ahead log.
pub(crate) fn remove_files(path: &Path) -> Result<()> {
    for file in [path.to_path_buf(), wal_path(path), shm_path(path)] {
        match std::fs::remove_file(&file) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(MountError::io(&file, &error)),
        }
    }
    Ok(())
}

fn wal_path(path: &Path) -> PathBuf {
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    PathBuf::from(wal)
}

fn shm_path(path: &Path) -> PathBuf {
    let mut shm = path.as_os_str().to_owned();
    shm.push("-shm");
    PathBuf::from(shm)
}

/// Records the sequence of the last commit whose changes are in the trees.
pub(crate) fn write_applied_seq(connection: &Connection, seq: ChangeSeq) -> rusqlite::Result<()> {
    connection.execute("UPDATE mount SET applied_seq = ?1", params![sql_u64(seq.0)])?;
    Ok(())
}

/// Records the root's path, after a move of the root or an ancestor.
pub(crate) fn write_root_path(
    connection: &Connection,
    path: &AbsolutePath,
) -> rusqlite::Result<()> {
    connection.execute("UPDATE mount SET root_path = ?1", params![path.as_str()])?;
    Ok(())
}

/// Records why publication stopped.
pub(crate) fn write_publication_stop(
    connection: &Connection,
    stop: &PublicationStop,
) -> rusqlite::Result<()> {
    connection.execute(
        "UPDATE mount SET publication_stop = ?1",
        params![json(stop)],
    )?;
    Ok(())
}

/// Returns the root's ancestors, from the root's parent up.
pub(crate) fn read_ancestors(connection: &Connection) -> rusqlite::Result<Vec<Ancestor>> {
    let mut statement =
        connection.prepare("SELECT inode_id, path FROM ancestors ORDER BY depth")?;
    let ancestors = statement
        .query_map([], |row| {
            Ok(Ancestor {
                inode_id: InodeId(from_sql_u64(row.get(0)?)),
                path: decode_path(&row.get::<_, String>(1)?)?,
            })
        })?
        .collect();
    ancestors
}

/// Replaces the root's ancestors.
pub(crate) fn write_ancestors(
    connection: &Connection,
    ancestors: &[Ancestor],
) -> rusqlite::Result<()> {
    connection.execute("DELETE FROM ancestors", [])?;
    for (depth, ancestor) in ancestors.iter().enumerate() {
        connection.execute(
            "INSERT INTO ancestors (depth, inode_id, path) VALUES (?1, ?2, ?3)",
            params![
                i64::try_from(depth).expect("an ancestor depth should fit i64"),
                sql_u64(ancestor.inode_id.0),
                ancestor.path.as_str()
            ],
        )?;
    }
    Ok(())
}

/// Returns every complete cache file.
pub(crate) fn read_cache_files(connection: &Connection) -> rusqlite::Result<Vec<CacheFileRow>> {
    let mut statement =
        connection.prepare("SELECT content_id, size_bytes, checksum FROM cache_files")?;
    let rows = statement
        .query_map([], |row| {
            Ok(CacheFileRow {
                content_id: ContentId::parse(row.get::<_, String>(0)?).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?,
                size_bytes: from_sql_u64(row.get(1)?),
                checksum: decode(&row.get::<_, String>(2)?)?,
            })
        })?
        .collect();
    rows
}

/// Records a complete cache file.
pub(crate) fn write_cache_file(
    connection: &Connection,
    row: &CacheFileRow,
) -> rusqlite::Result<()> {
    connection.execute(
        "INSERT OR REPLACE INTO cache_files (content_id, size_bytes, checksum) \
         VALUES (?1, ?2, ?3)",
        params![
            row.content_id.as_str(),
            sql_u64(row.size_bytes),
            json(&row.checksum)
        ],
    )?;
    Ok(())
}

/// Forgets a cache file.
pub(crate) fn delete_cache_file(
    connection: &Connection,
    content_id: &ContentId,
) -> rusqlite::Result<()> {
    connection.execute(
        "DELETE FROM cache_files WHERE content_id = ?1",
        params![content_id.as_str()],
    )?;
    Ok(())
}

/// Returns the directories that entered the mount and aren't shown yet.
pub(crate) fn read_entering_directories(
    connection: &Connection,
) -> rusqlite::Result<Vec<EnteringDirectory>> {
    let mut statement = connection.prepare(
        "SELECT root_item_id, snapshot_id, captured_seq, listed FROM entering_directories \
         ORDER BY captured_seq",
    )?;
    let rows = statement
        .query_map([], |row| {
            Ok(EnteringDirectory {
                root_item_id: ItemId(from_sql_u64(row.get(0)?)),
                snapshot_id: PinId::parse(row.get::<_, String>(1)?).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        1,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?,
                captured_seq: ChangeSeq(from_sql_u64(row.get(2)?)),
                listed: row.get::<_, i64>(3)? != 0,
            })
        })?
        .collect();
    rows
}

/// Records a directory that entered the mount.
pub(crate) fn write_entering_directory(
    connection: &Connection,
    directory: &EnteringDirectory,
) -> rusqlite::Result<()> {
    connection.execute(
        "INSERT OR REPLACE INTO entering_directories \
         (root_item_id, snapshot_id, captured_seq, listed) VALUES (?1, ?2, ?3, ?4)",
        params![
            sql_u64(directory.root_item_id.0),
            directory.snapshot_id.as_str(),
            sql_u64(directory.captured_seq.0),
            i64::from(directory.listed)
        ],
    )?;
    Ok(())
}

/// Forgets a directory that entered the mount, once it's shown or gone.
pub(crate) fn delete_entering_directory(
    connection: &Connection,
    root_item_id: ItemId,
) -> rusqlite::Result<()> {
    connection.execute(
        "DELETE FROM entering_directories WHERE root_item_id = ?1",
        params![sql_u64(root_item_id.0)],
    )?;
    Ok(())
}

/// Returns the number of items, which status compares with the entry
/// budget.
pub(crate) fn count_items(connection: &Connection) -> rusqlite::Result<u64> {
    connection
        .query_row("SELECT COUNT(*) FROM items", [], |row| row.get::<_, i64>(0))
        .map(from_sql_u64)
}

#[cfg(test)]
mod tests;
