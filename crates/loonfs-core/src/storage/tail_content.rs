//! Writes WAL pieces as immutable extents and merges the chain's own extents.

use super::content::{content_object_key_for_ref, DurableContentValidationError};
use super::content_location::{extent_object_key, ContentLocation, LayoutLookup, LocatedExtent};
use crate::error::{CoreError, Result};
use crate::limits::MAX_LAYOUT_EXTENTS;
use crate::wal::ProjectedContent;
use crate::PermitPool;
use bytes::Bytes;
use loonfs_objectstore::{AssemblySource, ObjectStore, ObjectStoreError};
use loonfs_types::format::manifest::ContentLayoutRecord;
use loonfs_types::{
    Checksum, ChecksumAlgorithm, ChecksumDigest, ContentExtent, ContentLayout, ContentRef,
    ExtentObject,
};

/// Checks whole values before any chain writes its content.
pub(crate) fn assemble_tail_content(content: &ProjectedContent) -> Result<Vec<Bytes>> {
    let newest = &content.content_ref;
    let corrupt = || {
        CoreError::NamespaceCorrupt(format!(
            "content `{}` has unfolded pieces that do not reach its newest reference",
            newest.content_id
        ))
    };
    let mut end = content.pieces[0].offset;
    for piece in &content.pieces {
        if piece.offset != end {
            return Err(corrupt());
        }
        end = piece.end();
    }
    if end != newest.size_bytes {
        return Err(corrupt());
    }
    let pieces: Vec<_> = content
        .pieces
        .iter()
        .map(|piece| piece.bytes.clone())
        .collect();
    if content.pieces[0].offset == 0 {
        let object_key = content_object_key_for_ref(newest)?;
        let actual = checksum_pieces(&pieces, newest.checksum.algorithm);
        if actual != newest.checksum {
            return Err(DurableContentValidationError::ContentChecksumMismatch {
                object_key,
                expected: format!("{}:{}", newest.checksum.algorithm, newest.checksum.value),
                actual: format!("{}:{}", actual.algorithm, actual.value),
            }
            .into());
        }
    }
    Ok(pieces)
}

pub(crate) async fn materialize_content_layout<S: ObjectStore + ?Sized, L: LayoutLookup>(
    store: &S,
    lookup: &L,
    content: Option<&ProjectedContent>,
    content_ref: &ContentRef,
    content_writes: &PermitPool,
) -> Result<ContentLayout> {
    let location = ContentLocation::resolve(lookup, content, content_ref).await?;
    let mut layout =
        if location.has_pieces() || (content_ref.size_bytes == 0 && location.is_resident()) {
            let content = content.expect("resident pieces should name a projected chain");
            write_tail_content(
                store,
                lookup,
                content,
                assemble_tail_content(content)?,
                content_writes,
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

pub(crate) async fn write_tail_content<S: ObjectStore + ?Sized, L: LayoutLookup>(
    store: &S,
    lookup: &L,
    content: &ProjectedContent,
    pieces: Vec<Bytes>,
    content_writes: &PermitPool,
) -> Result<ContentLayoutRecord> {
    let newest = &content.content_ref;
    let mut location = ContentLocation::resolve(lookup, Some(content), newest).await?;
    if newest.size_bytes == 0 {
        location.extents.clear();
    }
    let mut start = location.extents_length();
    let mut candidate = if content.pieces[0].offset == start {
        pieces
    } else {
        location.pieces().to_vec()
    };
    if let Some(last) = location.extents.last_mut().filter(|last| {
        last.extent.content_id == newest.content_id
            && last.extent.owner_namespace_id == newest.owner_namespace_id
    }) {
        let full_length = match last.extent.object {
            ExtentObject::Span { start, end } => end - start,
            ExtentObject::Whole => {
                let row = lookup
                    .content_layout(&newest.content_id)
                    .await?
                    .expect("resolved extents should have a layout");
                row.layout
                    .extents
                    .iter()
                    .find(|extent| extent_object_key(extent) == last.object_key)
                    .expect("resolved extent should occur in its layout")
                    .length
            }
        };
        let mut skip = full_length - last.extent.length;
        if skip > newest.size_bytes - start {
            return Err(CoreError::NamespaceCorrupt(format!(
                "content object `{}` extends past its newest reference",
                last.object_key
            )));
        }
        start += skip;
        last.extent.length = full_length;
        // Validation can materialize bytes that remain in the WAL tail.
        candidate = candidate
            .into_iter()
            .filter_map(|piece| {
                let skipped = loonfs_types::usize_from_u64(skip.min(piece.len() as u64));
                skip -= skipped as u64;
                (skipped < piece.len()).then(|| piece.slice(skipped..))
            })
            .collect();
    }
    // The whole key can already hold the first reference after a failed fold.
    let whole = location.extents.is_empty() && !lookup.content_has_other_size(newest);
    let mut extents = location.extents;
    let tail_length = candidate.iter().map(|piece| piece.len() as u64).sum();
    let MergePlan {
        retained,
        candidate_length,
    } = merge_plan(&extents, newest, tail_length);
    let merged_length = candidate_length - tail_length;
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
    let _permit = content_writes.acquire().await;
    let (sources, expected) =
        assembly_sources(store, &object_key, &extents[retained..], &candidate).await?;
    let written = store
        .assemble(&object_key, &sources, candidate, &expected)
        .await;
    if let Err(error) = written {
        tracing::error!(
            namespace_id = %newest.owner_namespace_id,
            object_key = %object_key,
            error = %error,
            "content write failed"
        );
        return Err(match error {
            ObjectStoreError::Transport { message, .. } => CoreError::store(
                &object_key,
                &ObjectStoreError::retryable_transport(&object_key, message),
            ),
            ObjectStoreError::ChecksumMismatch {
                object_key: mismatched,
            } => CoreError::NamespaceCorrupt(format!(
                "content object `{mismatched}` does not match its stored checksum"
            )),
            error => CoreError::store(&object_key, &error),
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

fn checksum_pieces(pieces: &[Bytes], algorithm: ChecksumAlgorithm) -> Checksum {
    let mut checksum = ChecksumDigest::new(algorithm);
    for piece in pieces {
        checksum.update(piece);
    }
    checksum.finish()
}

struct MergePlan {
    retained: usize,
    candidate_length: u64,
}

fn merge_plan(extents: &[LocatedExtent], newest: &ContentRef, tail_length: u64) -> MergePlan {
    let mut retained = extents.len();
    let mut candidate_length = tail_length;
    if tail_length == 0 {
        if let Some(last) = extents.last().filter(|last| {
            last.extent.content_id == newest.content_id
                && last.extent.owner_namespace_id == newest.owner_namespace_id
        }) {
            retained -= 1;
            candidate_length = last.extent.length;
        }
    }
    for located in extents[..retained].iter().rev() {
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
    }
    MergePlan {
        retained,
        candidate_length,
    }
}

async fn assembly_sources<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    extents: &[LocatedExtent],
    tail: &[Bytes],
) -> Result<(Vec<AssemblySource>, Checksum)> {
    let mut sources = Vec::with_capacity(extents.len());
    let mut expected = Checksum::compute(store.checksum_algorithm(), &[]);
    for located in extents {
        let source_key = &located.object_key;
        let corrupt = || {
            CoreError::NamespaceCorrupt(format!(
                "content object `{source_key}` does not hold its complete extent"
            ))
        };
        let stored = store
            .head(source_key)
            .await
            .map_err(|error| CoreError::store(source_key, &error))?
            .ok_or_else(corrupt)?;
        if located.extent.offset != 0 || stored.size_bytes != located.extent.length {
            return Err(corrupt());
        }
        let checksum = stored
            .checksum
            .clone()
            .filter(|checksum| checksum.algorithm == store.checksum_algorithm())
            .ok_or_else(corrupt)?;
        expected = expected
            .crc_combine(&checksum, stored.size_bytes)
            .ok_or_else(corrupt)?;
        sources.push(AssemblySource {
            key: source_key.clone(),
            range: None,
            checksum,
            metadata: stored,
        });
    }
    let expected = expected
        .crc_combine(
            &checksum_pieces(tail, expected.algorithm),
            tail.iter().map(|piece| piece.len() as u64).sum(),
        )
        .ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!("content object `{key}` has no combinable CRC"))
        })?;
    Ok((sources, expected))
}

#[cfg(test)]
mod tests;
