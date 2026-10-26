//! The `items` table of `tree.db`: how an item's record maps to its 46
//! columns, and the queries that read and write records.

use crate::attributes::MtimeAttribute;
use crate::config::CONTROL_DIRECTORY_NAME;
use crate::item::{ItemId, ItemKind};
use crate::records::{
    AttributeState, Binding, FileContent, HiddenBy, ItemRecord, LocalState, RemoteState, WriteState,
};
use loonfs_types::{
    AttributesRevisionNo, BindingVersion, ChangeSeq, ContentId, InodeId, RevisionNo,
};
use rusqlite::types::{Type, Value};
use rusqlite::{params, Connection, OptionalExtension as _, Row, Statement};
use std::collections::hash_map::Entry;
use std::collections::HashMap;

/// The `items` columns, in the order of [`record_values`], as a literal that
/// `concat!` can build statements from.
macro_rules! item_columns {
    () => {
        "item_id, remote_inode_id, kind, \
    c_parent, c_name, c_binding_version, c_revision_no, c_content_ref, c_size, \
    c_revision_committed_at_ms, c_mode, c_mtime_ms, c_target, c_attributes_revision_no, \
    c_mtime_attribute_ms, c_mtime_attribute_revision_no, \
    s_parent, s_name, s_binding_version, s_revision_no, s_content_ref, s_size, \
    s_revision_committed_at_ms, s_mode, s_mtime_ms, s_target, s_attributes_revision_no, \
    s_mtime_attribute_ms, s_mtime_attribute_revision_no, \
    s_hidden_by_inode_id, s_hidden_by_seq, s_append_only, \
    l_parent, l_name, l_size, l_mode, l_mtime_ms, l_target, l_mtime_explicit, \
    materialized, generation, published_generation, lowest_write_offset, \
    truncated_below_base, fingerprint, dirty_groups"
    };
}

/// One placeholder for each `items` column.
macro_rules! item_placeholders {
    () => {
        "?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, \
         ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34, ?35, ?36, \
         ?37, ?38, ?39, ?40, ?41, ?42, ?43, ?44, ?45, ?46"
    };
}

const SELECT_ITEMS: &str = concat!("SELECT ", item_columns!(), " FROM items");
const FIND_ITEM: &str = concat!("SELECT ", item_columns!(), " FROM items WHERE item_id = ?1");
const FIND_ITEM_BY_INODE: &str = concat!(
    "SELECT ",
    item_columns!(),
    " FROM items WHERE remote_inode_id = ?1"
);
const FIND_ITEM_ID_BY_INODE: &str = "SELECT item_id FROM items WHERE remote_inode_id = ?1";
const WRITE_ITEM: &str = concat!(
    "INSERT OR REPLACE INTO items (",
    item_columns!(),
    ") VALUES (",
    item_placeholders!(),
    ")"
);
const DELETE_ITEM: &str = "DELETE FROM items WHERE item_id = ?1";
const SERVER_CHILDREN: &str = "SELECT item_id FROM items WHERE s_parent = ?1";
const ALLOCATE_ITEM_ID: &str =
    "UPDATE mount SET next_item_id = next_item_id + 1 RETURNING next_item_id - 1";
const ITEM_COLUMN_COUNT: usize = 46;

/// Converts a `u64` to the `i64` that the database stores, keeping every bit.
///
/// Inode IDs use the whole `u64` range, so the conversion reinterprets the
/// bits instead of narrowing, and the engine compares these columns only for
/// equality.
pub(crate) fn sql_u64(value: u64) -> i64 {
    i64::from_ne_bytes(value.to_ne_bytes())
}

/// Reverses [`sql_u64`].
pub(crate) fn from_sql_u64(value: i64) -> u64 {
    u64::from_ne_bytes(value.to_ne_bytes())
}

fn integer(value: u64) -> Value {
    Value::Integer(sql_u64(value))
}

fn optional_integer(value: Option<u64>) -> Value {
    value.map_or(Value::Null, integer)
}

fn optional_text(value: Option<&str>) -> Value {
    value.map_or(Value::Null, |text| Value::Text(text.to_owned()))
}

fn flag(value: bool) -> Value {
    Value::Integer(i64::from(value))
}

fn push_remote(values: &mut Vec<Value>, state: &RemoteState) {
    let binding = state.binding.as_ref();
    let content = state.content.as_ref();
    let attributes = &state.attributes;
    values.extend([
        optional_integer(binding.map(|binding| binding.parent.0)),
        optional_text(binding.map(|binding| binding.name.as_str())),
        optional_text(binding.map(|binding| binding.binding_version.as_str())),
        optional_integer(content.map(|content| content.revision_no.0)),
        content.map_or(Value::Null, |content| {
            Value::Text(
                serde_json::to_string(&content.content_ref)
                    .expect("a content reference should serialize"),
            )
        }),
        optional_integer(content.map(|content| content.content_ref.size_bytes)),
        optional_integer(content.map(|content| content.committed_at_ms)),
        integer(u64::from(attributes.mode)),
        integer(attributes.mtime_ms),
        optional_text(attributes.target.as_deref()),
        integer(attributes.revision_no.0),
        optional_integer(attributes.mtime_attribute.map(|mtime| mtime.mtime_ms)),
        optional_integer(attributes.mtime_attribute.map(|mtime| mtime.revision_no.0)),
    ]);
}

fn record_values(record: &ItemRecord) -> Vec<Value> {
    let mut values = Vec::with_capacity(ITEM_COLUMN_COUNT);
    values.extend([
        integer(record.item_id.0),
        optional_integer(record.remote_inode_id.map(|inode_id| inode_id.0)),
        Value::Text(record.kind.as_str().to_owned()),
    ]);
    push_remote(&mut values, &record.committed);
    push_remote(&mut values, &record.server);
    let local = &record.local;
    let write = &record.write;
    values.extend([
        optional_integer(record.hidden_by.map(|hidden_by| hidden_by.inode_id.0)),
        optional_integer(record.hidden_by.map(|hidden_by| hidden_by.deleted_seq.0)),
        flag(record.append_only),
        optional_integer(local.binding.as_ref().map(|(parent, _)| parent.0)),
        optional_text(local.binding.as_ref().map(|(_, name)| name.as_str())),
        integer(local.size_bytes),
        integer(u64::from(local.mode)),
        integer(local.mtime_ms),
        optional_text(local.target.as_deref()),
        flag(write.mtime_explicit),
        flag(write.materialized),
        integer(write.generation),
        integer(write.published_generation),
        optional_integer(write.lowest_write_offset),
        flag(write.truncated_below_base),
        write.fingerprint.clone().map_or(Value::Null, Value::Blob),
        integer(u64::from(write.dirty_groups)),
    ]);
    assert_eq!(
        values.len(),
        ITEM_COLUMN_COUNT,
        "every item column should have one value"
    );
    values
}

/// Reads a row's columns in order.
struct Columns<'row> {
    row: &'row Row<'row>,
    index: usize,
}

fn conversion_error(
    index: usize,
    error: impl std::error::Error + Send + Sync + 'static,
) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(error))
}

impl Columns<'_> {
    fn next<T: rusqlite::types::FromSql>(&mut self) -> rusqlite::Result<T> {
        let value = self.row.get(self.index)?;
        self.index += 1;
        Ok(value)
    }

    fn optional_u64(&mut self) -> rusqlite::Result<Option<u64>> {
        Ok(self.next::<Option<i64>>()?.map(from_sql_u64))
    }

    fn u64(&mut self) -> rusqlite::Result<u64> {
        Ok(from_sql_u64(self.next::<i64>()?))
    }

    fn u32(&mut self) -> rusqlite::Result<u32> {
        let index = self.index;
        u32::try_from(self.u64()?).map_err(|error| conversion_error(index, error))
    }

    fn flag(&mut self) -> rusqlite::Result<bool> {
        Ok(self.next::<i64>()? != 0)
    }

    fn remote(&mut self) -> rusqlite::Result<RemoteState> {
        let parent = self.optional_u64()?;
        let name: Option<String> = self.next()?;
        let binding_version: Option<String> = self.next()?;
        let binding = match (parent, name, binding_version) {
            (Some(parent), Some(name), Some(binding_version)) => Some(Binding {
                parent: ItemId(parent),
                name,
                binding_version: BindingVersion::parse(&binding_version)
                    .map_err(|error| conversion_error(self.index, error))?,
            }),
            _ => None,
        };
        let revision_no = self.optional_u64()?;
        let content_ref: Option<String> = self.next()?;
        let _size_bytes = self.optional_u64()?;
        let committed_at_ms = self.optional_u64()?;
        let content = match (revision_no, content_ref, committed_at_ms) {
            (Some(revision_no), Some(content_ref), Some(committed_at_ms)) => Some(FileContent {
                revision_no: RevisionNo(revision_no),
                content_ref: serde_json::from_str(&content_ref)
                    .map_err(|error| conversion_error(self.index, error))?,
                committed_at_ms,
            }),
            _ => None,
        };
        let mode = self.u32()?;
        let mtime_ms = self.u64()?;
        let target = self.next()?;
        let attributes_revision_no = AttributesRevisionNo(self.u64()?);
        let mtime_attribute =
            self.optional_u64()?
                .zip(self.optional_u64()?)
                .map(|(mtime_ms, revision_no)| MtimeAttribute {
                    mtime_ms,
                    revision_no: RevisionNo(revision_no),
                });
        Ok(RemoteState {
            binding,
            content,
            attributes: AttributeState {
                revision_no: attributes_revision_no,
                mode,
                mtime_ms,
                target,
                mtime_attribute,
            },
        })
    }
}

fn record_from_row(row: &Row<'_>) -> rusqlite::Result<ItemRecord> {
    let mut columns = Columns { row, index: 0 };
    let item_id = ItemId(columns.u64()?);
    let remote_inode_id = columns.optional_u64()?.map(InodeId);
    let kind: String = columns.next()?;
    let kind = ItemKind::parse(&kind).ok_or_else(|| {
        conversion_error(
            2,
            std::io::Error::other(format!("unknown item kind {kind:?}")),
        )
    })?;
    let committed = columns.remote()?;
    let server = columns.remote()?;
    let hidden_by =
        columns
            .optional_u64()?
            .zip(columns.optional_u64()?)
            .map(|(inode_id, deleted_seq)| HiddenBy {
                inode_id: InodeId(inode_id),
                deleted_seq: ChangeSeq(deleted_seq),
            });
    let append_only = columns.flag()?;
    let local_parent = columns.optional_u64()?;
    let local_name: Option<String> = columns.next()?;
    let local = LocalState {
        binding: local_parent
            .zip(local_name)
            .map(|(parent, name)| (ItemId(parent), name)),
        size_bytes: columns.u64()?,
        mode: columns.u32()?,
        mtime_ms: columns.u64()?,
        target: columns.next()?,
    };
    let write = WriteState {
        mtime_explicit: columns.flag()?,
        materialized: columns.flag()?,
        generation: columns.u64()?,
        published_generation: columns.u64()?,
        lowest_write_offset: columns.optional_u64()?,
        truncated_below_base: columns.flag()?,
        fingerprint: columns.next()?,
        dirty_groups: u8::try_from(columns.u64()?)
            .map_err(|error| conversion_error(ITEM_COLUMN_COUNT - 1, error))?,
    };
    Ok(ItemRecord {
        item_id,
        remote_inode_id,
        kind,
        committed,
        server,
        hidden_by,
        append_only,
        local,
        write,
    })
}

/// The `items` table through one connection, which prepares each statement
/// once and keeps it for later calls. A transaction that changes many items,
/// such as a listing page, prepares each statement once instead of once for
/// each item.
pub(crate) struct ItemTable<'c> {
    connection: &'c Connection,
    prepared: HashMap<&'static str, Statement<'c>>,
}

impl<'c> ItemTable<'c> {
    pub(crate) fn new(connection: &'c Connection) -> Self {
        Self {
            connection,
            prepared: HashMap::new(),
        }
    }

    fn statement(&mut self, sql: &'static str) -> rusqlite::Result<&mut Statement<'c>> {
        match self.prepared.entry(sql) {
            Entry::Occupied(prepared) => Ok(prepared.into_mut()),
            Entry::Vacant(vacant) => Ok(vacant.insert(self.connection.prepare(sql)?)),
        }
    }

    /// Writes an item's row, replacing the row it had.
    pub(crate) fn write(&mut self, record: &ItemRecord) -> rusqlite::Result<()> {
        self.statement(WRITE_ITEM)?
            .execute(rusqlite::params_from_iter(record_values(record)))?;
        Ok(())
    }

    /// Returns the row of `item_id`.
    pub(crate) fn find(&mut self, item_id: ItemId) -> rusqlite::Result<Option<ItemRecord>> {
        self.statement(FIND_ITEM)?
            .query_row(params![sql_u64(item_id.0)], record_from_row)
            .optional()
    }

    /// Returns the row of the item that publishes to `inode_id`.
    pub(crate) fn find_by_inode(
        &mut self,
        inode_id: InodeId,
    ) -> rusqlite::Result<Option<ItemRecord>> {
        self.statement(FIND_ITEM_BY_INODE)?
            .query_row(params![sql_u64(inode_id.0)], record_from_row)
            .optional()
    }

    /// Returns the item that publishes to `inode_id`.
    pub(crate) fn find_id_by_inode(
        &mut self,
        inode_id: InodeId,
    ) -> rusqlite::Result<Option<ItemId>> {
        self.statement(FIND_ITEM_ID_BY_INODE)?
            .query_row(params![sql_u64(inode_id.0)], |row| row.get::<_, i64>(0))
            .optional()
            .map(|item_id| item_id.map(|item_id| ItemId(from_sql_u64(item_id))))
    }

    /// Deletes an item's row.
    pub(crate) fn delete(&mut self, item_id: ItemId) -> rusqlite::Result<()> {
        self.statement(DELETE_ITEM)?
            .execute(params![sql_u64(item_id.0)])?;
        Ok(())
    }

    /// Returns the items whose server-observed parent is `parent`.
    pub(crate) fn server_children(&mut self, parent: ItemId) -> rusqlite::Result<Vec<ItemId>> {
        let children = self
            .statement(SERVER_CHILDREN)?
            .query_map(params![sql_u64(parent.0)], |row| row.get::<_, i64>(0))?
            .map(|item_id| item_id.map(|item_id| ItemId(from_sql_u64(item_id))))
            .collect();
        children
    }

    /// Allocates a new item ID.
    pub(crate) fn allocate_id(&mut self) -> rusqlite::Result<ItemId> {
        self.statement(ALLOCATE_ITEM_ID)?
            .query_row([], |row| row.get::<_, i64>(0))
            .map(|item_id| ItemId(from_sql_u64(item_id)))
    }
}

/// Writes an item's row, replacing the row it had.
pub(crate) fn write_item(connection: &Connection, record: &ItemRecord) -> rusqlite::Result<()> {
    ItemTable::new(connection).write(record)
}

/// Returns the row of `item_id`.
pub(crate) fn find_item(
    connection: &Connection,
    item_id: ItemId,
) -> rusqlite::Result<Option<ItemRecord>> {
    ItemTable::new(connection).find(item_id)
}

/// Returns the item that publishes to `inode_id`.
pub(crate) fn find_item_id_by_inode(
    connection: &Connection,
    inode_id: InodeId,
) -> rusqlite::Result<Option<ItemId>> {
    ItemTable::new(connection).find_id_by_inode(inode_id)
}

/// Calls `visit` with every item's row.
pub(crate) fn for_each_item(
    connection: &Connection,
    mut visit: impl FnMut(ItemRecord),
) -> rusqlite::Result<()> {
    let mut statement = connection.prepare(SELECT_ITEMS)?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        visit(record_from_row(row)?);
    }
    Ok(())
}

/// Returns whether any item's current revision refers to `content_id`.
/// Returns the inode of the `.loonfs` entry at the root, which the mount
/// doesn't show, when the server has one.
pub(crate) fn reserved_inode(connection: &Connection) -> rusqlite::Result<Option<InodeId>> {
    connection
        .query_row(
            "SELECT remote_inode_id FROM items WHERE s_parent = ?1 AND s_name = ?2 LIMIT 1",
            params![sql_u64(ItemId::ROOT.0), CONTROL_DIRECTORY_NAME],
            |row| row.get::<_, Option<i64>>(0),
        )
        .optional()
        .map(|inode| inode.flatten().map(|inode| InodeId(from_sql_u64(inode))))
}

/// Returns the items whose server or committed state has `content_id`.
pub(crate) fn items_with_content(
    connection: &Connection,
    content_id: &ContentId,
) -> rusqlite::Result<Vec<ItemId>> {
    let mut statement = connection.prepare(
        "SELECT item_id FROM items \
         WHERE json_extract(s_content_ref, '$.content_id') = ?1 \
         OR json_extract(c_content_ref, '$.content_id') = ?1",
    )?;
    let items = statement
        .query_map(params![content_id.as_str()], |row| {
            Ok(ItemId(from_sql_u64(row.get(0)?)))
        })?
        .collect();
    items
}

pub(crate) fn content_in_use(
    connection: &Connection,
    content_id: &ContentId,
) -> rusqlite::Result<bool> {
    connection.query_row(
        "SELECT EXISTS (SELECT 1 FROM items \
         WHERE json_extract(s_content_ref, '$.content_id') = ?1 \
         OR json_extract(c_content_ref, '$.content_id') = ?1)",
        params![content_id.as_str()],
        |row| row.get(0),
    )
}
