//! Writes WAL pieces as immutable extents and merges the chain's own extents.

use super::content::{
    content_object_key_for_ref, validate_loaded_content_bytes, CONTENT_READ_CHUNK_BYTES,
};
use super::content_location::{extent_object_key, ContentLocation, LayoutLookup, LocatedExtent};
use crate::error::{CoreError, Result};
use crate::limits::{MAX_LAYOUT_EXTENTS, MAX_MERGED_EXTENT_BYTES};
use crate::wal::{ProjectedContent, ProjectedWalTail};
use bytes::Bytes;
use loonfs_objectstore::{
    AssemblySource, ByteRange, ImmutableWriteError, ObjectStore, ObjectStoreError,
};
use loonfs_types::format::manifest::ContentLayoutRecord;
use loonfs_types::{
    Checksum, ContentExtent, ContentLayout, ContentRef, ExtentObject, StreamingChecksum,
};
use tokio::sync::Semaphore;

/// Joins the tail pieces and checks whole values before anything is written.
pub(crate) fn assemble_tail_content(content: &ProjectedContent) -> Result<Bytes> {
    let newest = &content.content_ref;
    let mut end = content.pieces[0].offset;
    for piece in &content.pieces {
        if piece.offset != end {
            break;
        }
        end = piece.end();
    }
    if end != newest.size_bytes {
        return Err(CoreError::NamespaceCorrupt(format!(
            "content `{}` has unfolded pieces that do not reach its newest reference",
            newest.content_id
        )));
    }
    let bytes = match content.pieces.as_slice() {
        [piece] => piece.bytes.clone(),
        pieces => pieces
            .iter()
            .map(|piece| piece.bytes.as_ref())
            .collect::<Vec<_>>()
            .concat()
            .into(),
    };
    if content.pieces[0].offset == 0 {
        validate_loaded_content_bytes(content_object_key_for_ref(newest)?, newest, &bytes)?;
    }
    Ok(bytes)
}

pub(crate) async fn materialize_content_layout<S: ObjectStore + ?Sized, L: LayoutLookup>(
    store: &S,
    lookup: &L,
    tail: &ProjectedWalTail,
    content_ref: &ContentRef,
    merge_memory: &Semaphore,
) -> Result<ContentLayout> {
    let location = ContentLocation::resolve(lookup, Some(tail), content_ref).await?;
    let mut layout =
        if location.has_pieces() || (content_ref.size_bytes == 0 && location.is_resident()) {
            let content = tail
                .content_by_id(&content_ref.owner_namespace_id, &content_ref.content_id)
                .expect("resident pieces should name a projected chain");
            write_tail_content(
                store,
                lookup,
                tail,
                content,
                assemble_tail_content(content)?,
                merge_memory,
            )
            .await?
            .layout
        } else {
            ContentLayout {
                extents: location
                    .extents
                    .into_iter()
                    .map(|located| located.extent)
                    .collect(),
            }
        };
    cut_layout(&mut layout, content_ref.size_bytes);
    Ok(layout)
}

pub(crate) fn cut_layout(layout: &mut ContentLayout, size_bytes: u64) {
    let mut remaining = size_bytes;
    layout.extents.retain_mut(|extent| {
        if remaining == 0 && size_bytes != 0 {
            return false;
        }
        extent.length = extent.length.min(remaining);
        remaining -= extent.length;
        true
    });
}

/// Writes the tail's bytes for a chain as one object, merging the chain's own small
/// last extents by the doubling rule, and returns the layout.
pub(crate) async fn write_tail_content<S: ObjectStore + ?Sized, L: LayoutLookup>(
    store: &S,
    lookup: &L,
    tail: &ProjectedWalTail,
    content: &ProjectedContent,
    pieces: Bytes,
    merge_memory: &Semaphore,
) -> Result<ContentLayoutRecord> {
    let newest = &content.content_ref;
    let mut location = ContentLocation::resolve(lookup, Some(tail), newest).await?;
    if newest.size_bytes == 0 {
        location.extents.clear();
    }
    let mut start = location.extents_length();
    let mut candidate = if content.pieces[0].offset == start {
        pieces
    } else {
        location.joined_pieces()
    };
    // The whole key can already hold the first reference after a failed fold.
    let whole = location.extents.is_empty()
        && tail.rows.revisions().iter().all(|revision| {
            revision.content_ref.content_id != newest.content_id
                || revision.content_ref.size_bytes == newest.size_bytes
        });
    let mut extents = location.extents;
    let MergePlan {
        retained,
        candidate_length,
        assembly,
    } = merge_plan(&extents, newest, candidate.len() as u64);
    let merged_length = candidate_length - candidate.len() as u64;
    let _merge_permits = if assembly {
        None
    } else {
        Some(
            merge_memory
                .acquire_many(merged_length as u32)
                .await
                .expect("merge semaphore should remain open"),
        )
    };
    if !assembly && retained != extents.len() {
        let mut bytes = Vec::with_capacity(candidate_length as usize);
        for extent in &extents[retained..] {
            read_extent(store, extent, &mut bytes).await?;
        }
        bytes.extend_from_slice(&candidate);
        candidate = bytes.into();
    }
    start -= merged_length;
    let object = if whole && start == 0 {
        ExtentObject::Whole
    } else {
        ExtentObject::Span {
            start,
            end: newest.size_bytes,
        }
    };
    let extent = ContentExtent {
        owner_namespace_id: newest.owner_namespace_id.clone(),
        content_id: newest.content_id.clone(),
        object,
        offset: 0,
        length: candidate_length,
    };
    let object_key = extent_object_key(&extent);
    let written = if assembly {
        let (sources, expected) =
            assembly_sources(store, &object_key, &extents[retained..], &candidate).await?;
        store
            .assemble(&object_key, &sources, candidate, &expected)
            .await
    } else {
        store.put_immutable_verified(&object_key, candidate).await
    };
    if let Err(error) = written {
        tracing::error!(object_key, %error, "content write failed");
        return Err(match error {
            ImmutableWriteError::DifferentObject { .. }
            | ImmutableWriteError::Unattested { .. } => CoreError::NamespaceCorrupt(format!(
                "content object `{object_key}` requires an equal attestation"
            )),
            ImmutableWriteError::Transport {
                object_key,
                source: ObjectStoreError::Transport { message, .. },
            } => CoreError::store(
                &object_key,
                &ObjectStoreError::retryable_transport(&object_key, message),
            ),
            error => CoreError::from(error),
        });
    }
    extents.truncate(retained);
    let mut extents: Vec<_> = extents.into_iter().map(|located| located.extent).collect();
    extents.push(extent);
    Ok(ContentLayoutRecord {
        owner_namespace_id: newest.owner_namespace_id.clone(),
        content_id: newest.content_id.clone(),
        committed_seq: content.committed_seq,
        size_bytes: newest.size_bytes,
        layout: ContentLayout { extents },
    })
}

struct MergePlan {
    retained: usize,
    candidate_length: u64,
    assembly: bool,
}

fn merge_plan(extents: &[LocatedExtent], newest: &ContentRef, tail_length: u64) -> MergePlan {
    let mut retained = extents.len();
    let mut candidate_length = tail_length;
    for located in extents.iter().rev() {
        let extent = &located.extent;
        // A fresh chain keeps its base's extents shared.
        if extent.content_id != newest.content_id
            || extent.owner_namespace_id != newest.owner_namespace_id
            || extent.length > 2 * candidate_length
        {
            break;
        }
        candidate_length += extent.length;
        retained -= 1;
    }
    let own = |located: &&LocatedExtent| {
        located.extent.content_id == newest.content_id
            && located.extent.owner_namespace_id == newest.owner_namespace_id
    };
    let mut assembly = candidate_length > MAX_MERGED_EXTENT_BYTES && retained < extents.len();
    if extents[..retained].iter().filter(own).count() + 1 > MAX_LAYOUT_EXTENTS {
        let largest = extents
            .iter()
            .enumerate()
            .filter(|(_, extent)| own(extent))
            .max_by_key(|(index, extent)| (extent.extent.length, std::cmp::Reverse(*index)))
            .map(|(index, _)| index)
            .expect("a chain over its extent bound should have own extents");
        retained = retained.min(largest + 1);
        candidate_length = tail_length
            + extents[retained..]
                .iter()
                .map(|located| located.extent.length)
                .sum::<u64>();
        assembly = true;
    }
    MergePlan {
        retained,
        candidate_length,
        assembly,
    }
}

async fn assembly_sources<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    extents: &[LocatedExtent],
    tail: &[u8],
) -> Result<(Vec<AssemblySource>, Checksum)> {
    let mut sources = Vec::with_capacity(extents.len());
    let mut expected: Option<Checksum> = None;
    for located in extents {
        let source_key = &located.object_key;
        let corrupt = || {
            CoreError::NamespaceCorrupt(format!(
                "content object `{source_key}` does not hold its complete extent"
            ))
        };
        let stored = store
            .head_stored_checksum(source_key)
            .await
            .map_err(|error| CoreError::store(source_key, &error))?
            .ok_or_else(corrupt)?;
        if located.extent.offset != 0 || stored.size_bytes != located.extent.length {
            return Err(corrupt());
        }
        expected = Some(match expected {
            None => stored.checksum.clone(),
            Some(checksum) => checksum
                .crc_combine(&stored.checksum, stored.size_bytes)
                .ok_or_else(corrupt)?,
        });
        sources.push(AssemblySource {
            key: source_key.clone(),
            range: None,
            checksum: stored.checksum,
        });
    }
    let expected = expected.expect("an assembly merge should have source extents");
    let expected = expected
        .crc_combine(
            &Checksum::compute(expected.algorithm, tail),
            tail.len() as u64,
        )
        .ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!("content object `{key}` has no combinable CRC"))
        })?;
    Ok((sources, expected))
}

// Attestations cover the whole object, including bytes outside a shared prefix.
async fn read_extent<S: ObjectStore + ?Sized>(
    store: &S,
    located: &LocatedExtent,
    bytes: &mut Vec<u8>,
) -> Result<()> {
    let key = &located.object_key;
    let corrupt = || {
        CoreError::NamespaceCorrupt(format!(
            "content object `{key}` does not hold its attested extent bytes"
        ))
    };
    let metadata = store
        .head(key)
        .await
        .map_err(|error| CoreError::store(key, &error))?
        .ok_or_else(corrupt)?;
    let extent = &located.extent;
    if metadata.size_bytes < extent.offset + extent.length {
        return Err(corrupt());
    }
    let (mut offset, end) = if metadata.attestation.is_some() {
        (0, metadata.size_bytes)
    } else {
        (extent.offset, extent.offset + extent.length)
    };
    let mut state = metadata
        .attestation
        .as_ref()
        .map(|checksum| StreamingChecksum::for_algorithm(checksum.algorithm));
    while offset < end {
        let chunk_end = end.min(offset + CONTENT_READ_CHUNK_BYTES);
        let chunk = store
            .get(
                key,
                Some(ByteRange {
                    start_inclusive: offset,
                    end_exclusive: chunk_end,
                }),
            )
            .await
            .map_err(|error| CoreError::store(key, &error))?
            .ok_or_else(corrupt)?;
        if chunk.len() as u64 != chunk_end - offset {
            return Err(corrupt());
        }
        if let Some(state) = &mut state {
            state.update(&chunk);
        }
        let first = offset.max(extent.offset);
        let last = chunk_end.min(extent.offset + extent.length);
        if first < last {
            bytes.extend_from_slice(&chunk[(first - offset) as usize..(last - offset) as usize]);
        }
        offset = chunk_end;
    }
    if let (Some(expected), Some(state)) = (metadata.attestation, state) {
        if state.finish() != expected {
            return Err(corrupt());
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "tail_content_tests.rs"]
mod tests;
