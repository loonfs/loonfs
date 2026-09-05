//! Binary carry merges keep durable marking progress independent of root count.

use super::mark_table::{merge_equal, MarkTables};
use crate::error::{CoreError, Result};
use loonfs_api::wire::gc::{
    GcMarkEntry, GcMarkIndex, GcMarkMerge, GcMarkPosition, GcMarkTable, GC_MARK_PAGE_ENTRIES,
};
use loonfs_api::GcMarkTableId;
use loonfs_objectstore::ObjectStore;

const MAX_LEVELS: usize = 64;

pub(super) fn insert(index: &mut GcMarkIndex, table: GcMarkTable, level: usize) -> Result<()> {
    if index.merge.is_some() || level >= MAX_LEVELS {
        return Err(corrupt(
            "invalid GC mark insertion while merging or past level bound",
        ));
    }
    if table.entry_count == 0 {
        return Ok(());
    }
    index.levels.resize(index.levels.len().max(level + 1), None);
    match index.levels[level].take() {
        None => index.levels[level] = Some(table),
        Some(previous) => index.merge = Some(merge([previous, table], level + 1)?),
    }
    Ok(())
}

fn merge(inputs: [GcMarkTable; 2], output_level: usize) -> Result<GcMarkMerge> {
    if output_level >= MAX_LEVELS {
        return Err(corrupt("GC mark index exceeds its level bound"));
    }
    Ok(GcMarkMerge {
        inputs,
        positions: [GcMarkPosition::default(); 2],
        output: GcMarkTable {
            table_id: GcMarkTableId::generate(),
            page_count: 0,
            entry_count: 0,
        },
        output_level: output_level as u32,
    })
}

/// Starts a final merge if more than one level remains. Once this returns
/// false, the index consists of at most one complete sorted table.
pub(super) fn seal(index: &mut GcMarkIndex) -> Result<bool> {
    if index.merge.is_some() {
        return Ok(true);
    }
    let mut occupied = index
        .levels
        .iter()
        .enumerate()
        .filter_map(|(i, table)| table.as_ref().map(|_| i));
    let (Some(first), Some(second)) = (occupied.next(), occupied.next()) else {
        return Ok(false);
    };
    index.merge = Some(merge(
        [
            index.levels[first].take().expect("occupied level"),
            index.levels[second].take().expect("occupied level"),
        ],
        second + 1,
    )?);
    Ok(true)
}

pub(super) async fn step<S: ObjectStore + ?Sized>(
    tables: &mut MarkTables<'_, S>,
    index: &mut GcMarkIndex,
) -> Result<()> {
    let mut pending = index
        .merge
        .clone()
        .ok_or_else(|| corrupt("GC mark merge is absent"))?;
    let page = tables
        .merge_page(&pending.inputs, &mut pending.positions)
        .await?;
    let ended = page.len() < GC_MARK_PAGE_ENTRIES
        || pending
            .positions
            .iter()
            .zip(&pending.inputs)
            .all(|(position, table)| position.page_no == table.page_count);
    if !page.is_empty() {
        let count = page.len() as u64;
        tables
            .write_page(&pending.output.table_id, pending.output.page_count, page)
            .await?;
        pending.output.page_count = pending
            .output
            .page_count
            .checked_add(1)
            .ok_or_else(|| corrupt("GC mark page count overflow"))?;
        pending.output.entry_count = pending
            .output
            .entry_count
            .checked_add(count)
            .ok_or_else(|| corrupt("GC mark entry count overflow"))?;
    }
    if ended {
        index.merge = None;
        insert(index, pending.output, pending.output_level as usize)?;
    } else {
        index.merge = Some(pending);
    }
    Ok(())
}

pub(super) async fn write_sorted<S: ObjectStore + ?Sized>(
    tables: &MarkTables<'_, S>,
    mut entries: Vec<GcMarkEntry>,
) -> Result<GcMarkTable> {
    entries.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    let mut unique: Vec<GcMarkEntry> = Vec::with_capacity(entries.len());
    for entry in entries {
        match unique.last_mut() {
            Some(previous) if previous.key == entry.key => {
                *previous = merge_equal(previous.clone(), entry)?
            }
            _ => unique.push(entry),
        }
    }
    let entries = unique;
    let table = GcMarkTable {
        table_id: GcMarkTableId::generate(),
        page_count: entries.len().div_ceil(GC_MARK_PAGE_ENTRIES) as u64,
        entry_count: entries.len() as u64,
    };
    for (page_no, page) in entries.chunks(GC_MARK_PAGE_ENTRIES).enumerate() {
        tables
            .write_page(&table.table_id, page_no as u64, page.to_vec())
            .await?;
    }
    Ok(table)
}

pub(super) async fn lookup<S: ObjectStore + ?Sized>(
    tables: &mut MarkTables<'_, S>,
    index: &GcMarkIndex,
    key: &str,
) -> Result<Option<GcMarkEntry>> {
    for table in index
        .levels
        .iter()
        .flatten()
        .chain(index.merge.iter().flat_map(|merge| &merge.inputs))
    {
        if let Some(entry) = tables.lookup(table, key).await? {
            return Ok(Some(entry));
        }
    }
    Ok(None)
}

fn corrupt(message: &str) -> CoreError {
    CoreError::NamespaceCorrupt(message.to_owned())
}
