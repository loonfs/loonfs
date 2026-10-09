//! Writes WAL pieces as immutable extents and merges small tail extents.

use super::content::{
    content_object_key_for_ref, validate_loaded_content_bytes, CONTENT_READ_CHUNK_BYTES,
};
use super::content_location::{extent_object_key, ContentLocation, LayoutLookup, LocatedExtent};
use crate::error::{CoreError, Result};
use crate::limits::MAX_MERGED_EXTENT_BYTES;
use crate::wal::{ProjectedContent, ProjectedWalTail};
use bytes::Bytes;
use loonfs_objectstore::{ByteRange, ImmutableWriteError, ObjectStore, ObjectStoreError};
use loonfs_types::format::manifest::ContentLayoutRecord;
use loonfs_types::{ContentExtent, ContentLayout, ExtentObject, Sha256State};

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

/// Writes the tail's bytes for a chain as one object, merging the chain's own small
/// last extents by the doubling rule, and returns the layout.
pub(crate) async fn write_tail_content<S: ObjectStore + ?Sized, L: LayoutLookup>(
    store: &S,
    lookup: &L,
    tail: &ProjectedWalTail,
    content: &ProjectedContent,
    pieces: Bytes,
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
    while let Some(last) = extents.last().filter(|last| {
        // A fresh chain keeps its base's extents shared.
        last.extent.content_id == newest.content_id
            && last.extent.owner_namespace_id == newest.owner_namespace_id
            && last.extent.length <= 2 * candidate.len() as u64
            && last.extent.length + candidate.len() as u64 <= MAX_MERGED_EXTENT_BYTES
    }) {
        let mut bytes = read_extent(store, last).await?;
        start -= last.extent.length;
        bytes.extend_from_slice(&candidate);
        candidate = bytes.into();
        extents.pop();
    }
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
        length: candidate.len() as u64,
    };
    let object_key = extent_object_key(&extent);
    if let Err(error) = store.put_immutable_verified(&object_key, candidate).await {
        tracing::error!(object_key, %error, "content write failed");
        return Err(match error {
            ImmutableWriteError::DifferentObject { .. }
            | ImmutableWriteError::Unattested { .. } => CoreError::NamespaceCorrupt(format!(
                "content object `{object_key}` requires an equal SHA-256 attestation"
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

/// Reads the whole object when attested because its SHA-256 covers every byte,
/// including bytes outside a shared prefix.
async fn read_extent<S: ObjectStore + ?Sized>(
    store: &S,
    located: &LocatedExtent,
) -> Result<Vec<u8>> {
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
    let (mut offset, end) = if metadata.sha256.is_some() {
        (0, metadata.size_bytes)
    } else {
        (extent.offset, extent.offset + extent.length)
    };
    let mut state = Sha256State::new();
    let mut bytes = Vec::with_capacity(extent.length as usize);
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
        state.update(&chunk);
        let first = offset.max(extent.offset);
        let last = chunk_end.min(extent.offset + extent.length);
        if first < last {
            bytes.extend_from_slice(&chunk[(first - offset) as usize..(last - offset) as usize]);
        }
        offset = chunk_end;
    }
    if metadata
        .sha256
        .is_some_and(|expected| state.finish() != expected)
    {
        return Err(corrupt());
    }
    Ok(bytes)
}
