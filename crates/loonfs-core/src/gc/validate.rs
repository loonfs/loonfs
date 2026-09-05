//! Structural checks for server-owned progress before it can drive I/O.
use super::cursor::CandidateFamilyExt;
use crate::error::{CoreError, Result};
use loonfs_api::wire::gc::*;
use loonfs_objectstore::keys::{checkpoint_prefix, gc_runs_prefix, metadata_manifest_prefix};

pub(super) fn run(state: &GcRunState) -> Result<()> {
    if state.grace_window_ms < crate::limits::GC_MIN_GRACE_WINDOW_MS {
        return Err(invalid());
    }
    match &state.phase {
        GcPhase::Marking { work } => {
            index(&work.index)?;
            match &work.source {
                GcMarkSource::Root {
                    manifest: Some(manifest),
                } if manifest.owner_namespace_id != state.namespace_id => return Err(invalid()),
                GcMarkSource::Checkpoints { last_key } => {
                    position_key(last_key.as_deref(), &checkpoint_prefix(&state.namespace_id))?
                }
                GcMarkSource::AnchorDiscovery {
                    last_key,
                    current,
                    aged,
                    ..
                } => {
                    position_key(
                        last_key.as_deref(),
                        &metadata_manifest_prefix(&state.namespace_id),
                    )?;
                    for range in current.iter().chain(aged) {
                        manifest_range(state, range)?;
                    }
                }
                GcMarkSource::AnchorManifests { range, last_key } => {
                    manifest_range(state, range)?;
                    if last_key
                        .as_ref()
                        .is_some_and(|key| key < &range.first_key || key > &range.last_key)
                    {
                        return Err(invalid());
                    }
                }
                _ => {}
            }
        }
        GcPhase::Revisions {
            objects,
            position: next,
            content,
            ..
        } => {
            table(objects)?;
            position(objects, *next)?;
            index(content)?;
        }
        GcPhase::Sealing { index: pending, .. } => index(pending)?,
        GcPhase::Sweeping {
            table: marks,
            family,
            last_key,
            ..
        } => {
            table(marks)?;
            position_key(last_key.as_deref(), &family.prefix(&state.namespace_id))?;
        }
        GcPhase::Cleaning { last_key } => {
            position_key(last_key.as_deref(), &gc_runs_prefix(&state.namespace_id))?
        }
        GcPhase::Starting {} | GcPhase::Complete {} => {}
    }
    Ok(())
}

fn manifest_range(state: &GcRunState, range: &GcManifestRange) -> Result<()> {
    let prefix = metadata_manifest_prefix(&state.namespace_id);
    if range.first_key > range.last_key {
        return Err(invalid());
    }
    for key in [&range.first_key, &range.last_key] {
        position_key(Some(key), &prefix)?;
        let generation = loonfs_objectstore::layout::manifest_object_id_of(key)
            .and_then(|id| id.ok())
            .and_then(|id| loonfs_api::manifest_object_id_manifest_no(id.as_str()));
        if generation != Some(range.manifest_no) {
            return Err(invalid());
        }
    }
    Ok(())
}

fn position_key(key: Option<&str>, prefix: &str) -> Result<()> {
    if key.is_some_and(|key| !key.starts_with(prefix)) {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn table(table: &GcMarkTable) -> Result<()> {
    if table.page_count != table.entry_count.div_ceil(GC_MARK_PAGE_ENTRIES as u64) {
        return Err(invalid());
    }
    Ok(())
}

fn position(table: &GcMarkTable, position: GcMarkPosition) -> Result<()> {
    if position.page_no > table.page_count
        || position.entry_no as usize >= GC_MARK_PAGE_ENTRIES
        || (position.page_no == table.page_count && position.entry_no != 0)
    {
        return Err(invalid());
    }
    let consumed = position
        .page_no
        .checked_mul(GC_MARK_PAGE_ENTRIES as u64)
        .and_then(|count| count.checked_add(u64::from(position.entry_no)));
    if position.page_no < table.page_count
        && consumed.is_none_or(|count| count >= table.entry_count)
    {
        return Err(invalid());
    }
    Ok(())
}

fn index(index: &GcMarkIndex) -> Result<()> {
    if index.levels.len() > 64 {
        return Err(invalid());
    }
    for complete in index.levels.iter().flatten() {
        table(complete)?;
    }
    if let Some(merge) = &index.merge {
        if merge.output_level >= 64
            || merge.inputs[0].table_id == merge.inputs[1].table_id
            || merge
                .inputs
                .iter()
                .any(|input| input.table_id == merge.output.table_id)
        {
            return Err(invalid());
        }
        for (input, next) in merge.inputs.iter().zip(merge.positions) {
            table(input)?;
            position(input, next)?;
        }
        table(&merge.output)?;
        if merge.output.entry_count % GC_MARK_PAGE_ENTRIES as u64 != 0 {
            return Err(invalid());
        }
    }
    Ok(())
}

fn invalid() -> CoreError {
    CoreError::NamespaceCorrupt("invalid durable GC progress".to_owned())
}
