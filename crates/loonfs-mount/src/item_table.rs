//! The `items` table of `tree.db`: how an item's record maps to its 46
//! columns, and the queries that read and write records.

use crate::attributes::MtimeAttribute;
use crate::item::{ItemId, ItemKind};
use crate::records::{
    AttributeState, Binding, FileContent, HiddenBy, ItemRecord, LocalState, RemoteState, WriteState,
};
use loonfs_types::{
    AttributesRevisionNo, BindingVersion, ChangeSeq, ContentId, InodeId, RevisionNo,
};
use rusqlite::types::{Type, Value};
use rusqlite::{params, Connection, OptionalExtension as _, Row};

const ITEM_COLUMNS: &str = "item_id, remote_inode_id, kind, \
    c_parent, c_name, c_binding_version, c_revision_no, c_content_ref, c_size, \
    c_revision_committed_at_ms, c_mode, c_mtime_ms, c_target, c_attributes_revision_no, \
    c_mtime_attribute_ms, c_mtime_attribute_revision_no, \
    s_parent, s_name, s_binding_version, s_revision_no, s_content_ref, s_size, \
    s_revision_committed_at_ms, s_mode, s_mtime_ms, s_target, s_attributes_revision_no, \
    s_mtime_attribute_ms, s_mtime_attribute_revision_no, \
    s_hidden_by_inode_id, s_hidden_by_seq, s_append_only, \
    l_parent, l_name, l_size, l_mode, l_mtime_ms, l_target, l_mtime_explicit, \
    materialized, generation, published_generation, lowest_write_offset, \
    truncated_below_base, fingerprint, dirty_groups";
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

/// Writes an item's row, replacing the row it had.
pub(crate) fn write_item(connection: &Connection, record: &ItemRecord) -> rusqlite::Result<()> {
    let placeholders = (1..=ITEM_COLUMN_COUNT)
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    connection.execute(
        &format!("INSERT OR REPLACE INTO items ({ITEM_COLUMNS}) VALUES ({placeholders})"),
        rusqlite::params_from_iter(record_values(record)),
    )?;
    Ok(())
}

/// Returns the row of `item_id`.
pub(crate) fn find_item(
    connection: &Connection,
    item_id: ItemId,
) -> rusqlite::Result<Option<ItemRecord>> {
    connection
        .query_row(
            &format!("SELECT {ITEM_COLUMNS} FROM items WHERE item_id = ?1"),
            params![sql_u64(item_id.0)],
            record_from_row,
        )
        .optional()
}

/// Returns the row of the item that publishes to `inode_id`.
pub(crate) fn find_item_by_inode(
    connection: &Connection,
    inode_id: InodeId,
) -> rusqlite::Result<Option<ItemRecord>> {
    connection
        .query_row(
            &format!("SELECT {ITEM_COLUMNS} FROM items WHERE remote_inode_id = ?1"),
            params![sql_u64(inode_id.0)],
            record_from_row,
        )
        .optional()
}

/// Returns the item that publishes to `inode_id`.
pub(crate) fn find_item_id_by_inode(
    connection: &Connection,
    inode_id: InodeId,
) -> rusqlite::Result<Option<ItemId>> {
    connection
        .query_row(
            "SELECT item_id FROM items WHERE remote_inode_id = ?1",
            params![sql_u64(inode_id.0)],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map(|item_id| item_id.map(|item_id| ItemId(from_sql_u64(item_id))))
}

/// Deletes an item's row.
pub(crate) fn delete_item(connection: &Connection, item_id: ItemId) -> rusqlite::Result<()> {
    connection.execute(
        "DELETE FROM items WHERE item_id = ?1",
        params![sql_u64(item_id.0)],
    )?;
    Ok(())
}

/// Returns the items whose server-observed parent is `parent`.
pub(crate) fn server_children(
    connection: &Connection,
    parent: ItemId,
) -> rusqlite::Result<Vec<ItemId>> {
    let mut statement = connection.prepare("SELECT item_id FROM items WHERE s_parent = ?1")?;
    let children = statement
        .query_map(params![sql_u64(parent.0)], |row| row.get::<_, i64>(0))?
        .map(|item_id| item_id.map(|item_id| ItemId(from_sql_u64(item_id))))
        .collect();
    children
}

/// Calls `visit` with every item's row.
pub(crate) fn for_each_item(
    connection: &Connection,
    mut visit: impl FnMut(ItemRecord),
) -> rusqlite::Result<()> {
    let mut statement = connection.prepare(&format!("SELECT {ITEM_COLUMNS} FROM items"))?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        visit(record_from_row(row)?);
    }
    Ok(())
}

/// Returns whether any item's current revision refers to `content_id`.
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
