//! Writes WAL pieces as immutable extents and combines the chain's own extents.

use super::content::{
    content_object_key_for_ref, describe_checksum, DurableContentValidationError,
};
use super::content_location::{extent_object_key, ContentLocation, LayoutLookup, LocatedExtent};
use crate::error::{CoreError, Result};
use crate::limits::MAX_LAYOUT_EXTENTS;
use crate::namespace::state::NamespaceReadState;
use crate::wal::ProjectedContent;
use crate::PermitPool;
use bytes::Bytes;
use loonfs_objectstore::{AssemblySource, ObjectStore, ObjectStoreError};
use loonfs_types::format::manifest::ContentLayoutRecord;
use loonfs_types::{
    Checksum, ChecksumAlgorithm, ChecksumDigest, ContentExtent, ContentId, ContentLayout,
    ContentRef, ExtentObject, NamespaceId, PoolId,
};

/// Checks whole values before any chain writes its content. `pool_id`
/// identifies the prefix that an error's object key is under.
pub(crate) fn assemble_tail_content(
    pool_id: &PoolId,
    content: &ProjectedContent,
) -> Result<Vec<Bytes>> {
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
        let object_key = content_object_key_for_ref(pool_id, newest);
        let actual = checksum_pieces(&pieces, newest.checksum.algorithm());
        if actual != newest.checksum {
            return Err(DurableContentValidationError::ContentChecksumMismatch {
                object_key,
                expected: describe_checksum(&newest.checksum),
                actual: describe_checksum(&actual),
            }
            .into());
        }
    }
    Ok(pieces)
}

/// Where content writes go: the store that contains the content objects, the
/// permits that bound how many writes run at once, the pool whose prefix
/// stores the objects, and the namespace whose WAL pieces the writes
/// store.
pub(crate) struct ContentWrites<'a, S: ?Sized> {
    pub(crate) store: &'a S,
    pub(crate) permits: &'a PermitPool,
    pub(crate) pool_id: &'a PoolId,
    /// The namespace whose WAL includes the pieces that the writes store. A
    /// namespace's WAL includes pieces only for chains that the namespace
    /// created, so every layout that the writes produce records it as the
    /// creator.
    pub(crate) namespace_id: &'a NamespaceId,
}

impl<'a, S: ?Sized> ContentWrites<'a, S> {
    /// Returns where the content writes of the namespace whose head is
    /// `head` go: its pool's prefix in `store`.
    pub(crate) fn new(store: &'a S, permits: &'a PermitPool, head: &'a NamespaceReadState) -> Self {
        Self {
            store,
            permits,
            pool_id: &head.pool_id,
            namespace_id: &head.namespace_id,
        }
    }
}

impl<S: ?Sized> Clone for ContentWrites<'_, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S: ?Sized> Copy for ContentWrites<'_, S> {}

/// Returns the layout of the first `content_ref.size_bytes` bytes of a chain
/// in the view that `lookup` and `content` describe, after writing the
/// chain's pending pieces to content objects.
///
/// The layout records the chain's creator: the creator that the chain's
/// layout row records, or, for a chain without a layout row, the namespace
/// whose own unfolded tail started it, which is `content_writes.namespace_id`.
pub(crate) async fn materialize_content_layout<S: ObjectStore + ?Sized, L: LayoutLookup>(
    content_writes: ContentWrites<'_, S>,
    lookup: &L,
    content: Option<&ProjectedContent>,
    content_ref: &ContentRef,
) -> Result<ContentLayout> {
    let location =
        ContentLocation::resolve(lookup, content_writes.pool_id, content, content_ref).await?;
    let mut layout =
        if location.has_pieces() || (content_ref.size_bytes == 0 && location.is_resident()) {
            let content = content.expect("resident pieces should name a projected chain");
            write_tail_content(
                content_writes,
                lookup,
                content,
                assemble_tail_content(content_writes.pool_id, content)?,
            )
            .await?
            .layout
        } else {
            let creator_namespace_id = match location.creator_namespace_id {
                Some(creator_namespace_id) => creator_namespace_id,
                // A location of an empty chain reads no layout row.
                None => lookup
                    .content_layout(&content_ref.content_id)
                    .await?
                    .map_or_else(
                        || content_writes.namespace_id.clone(),
                        |row| row.layout.creator_namespace_id,
                    ),
            };
            ContentLayout {
                creator_namespace_id,
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

/// Shortens `layout` to its first `size_bytes` bytes, keeping its creator.
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

/// Writes a chain's tail pieces as its next extent under the pool's prefix,
/// and returns the chain's new layout row, which records
/// `content_writes.namespace_id` as the chain's creator.
pub(crate) async fn write_tail_content<S: ObjectStore + ?Sized, L: LayoutLookup>(
    content_writes: ContentWrites<'_, S>,
    lookup: &L,
    content: &ProjectedContent,
    pieces: Vec<Bytes>,
) -> Result<ContentLayoutRecord> {
    let newest = &content.content_ref;
    let pool_id = content_writes.pool_id;
    let TailExtentPlan {
        mut extents,
        retained,
        extent,
        candidate,
    } = tail_extent_plan(lookup, pool_id, content, pieces).await?;
    let object_key = extent_object_key(pool_id, &extent);
    let _permit = content_writes.permits.acquire().await;
    let (sources, expected) = assembly_sources(
        content_writes.store,
        &object_key,
        &extents[retained..],
        &candidate,
    )
    .await?;
    let written = content_writes
        .store
        .assemble(&object_key, &sources, candidate, &expected)
        .await;
    if let Err(error) = written {
        tracing::error!(
            namespace_id = %content_writes.namespace_id,
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
        content_id: newest.content_id.clone(),
        committed_seq: content.committed_seq,
        size_bytes: newest.size_bytes,
        layout: ContentLayout {
            creator_namespace_id: content_writes.namespace_id.clone(),
            extents,
        },
    })
}

/// Returns whether `extent` stores bytes of the chain `chain_content_id`
/// itself, rather than bytes of a shared base chain whose extents a new chain
/// lists first.
pub(crate) fn is_own_extent(extent: &ContentExtent, chain_content_id: &ContentId) -> bool {
    extent.content_id == *chain_content_id
}

/// What one write of a chain's tail assembles: the chain's extents, how many
/// of them the new layout keeps, the extent that the write puts after them,
/// and the bytes that the new extent ends with.
struct TailExtentPlan {
    extents: Vec<LocatedExtent>,
    retained: usize,
    extent: ContentExtent,
    candidate: Vec<Bytes>,
}

async fn tail_extent_plan<L: LayoutLookup>(
    lookup: &L,
    pool_id: &PoolId,
    content: &ProjectedContent,
    pieces: Vec<Bytes>,
) -> Result<TailExtentPlan> {
    let newest = &content.content_ref;
    let mut location = ContentLocation::resolve(lookup, pool_id, Some(content), newest).await?;
    if newest.size_bytes == 0 {
        location.extents.clear();
    }
    let mut start = location.extents_length();
    let mut candidate = if content.pieces[0].offset == start {
        pieces
    } else {
        location.pieces().to_vec()
    };
    if let Some(last) = location
        .extents
        .last_mut()
        .filter(|last| is_own_extent(&last.extent, &newest.content_id))
    {
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
                    .find(|extent| extent_object_key(pool_id, extent) == last.object_key)
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
    // The whole key can already contain the first reference after a failed fold.
    let whole = location.extents.is_empty() && !lookup.content_has_other_size(newest);
    let extents = location.extents;
    let tail_length = candidate.iter().map(|piece| piece.len() as u64).sum();
    let CombinePlan {
        retained,
        candidate_length,
    } = combine_plan(&extents, newest, tail_length);
    let combined_length = candidate_length - tail_length;
    start -= combined_length;
    let object = if whole && start == 0 {
        ExtentObject::Whole
    } else {
        ExtentObject::Span {
            start,
            end: newest.size_bytes,
        }
    };
    let extent = ContentExtent {
        content_id: newest.content_id.clone(),
        object,
        offset: 0,
        length: candidate_length,
    };
    Ok(TailExtentPlan {
        extents,
        retained,
        extent,
        candidate,
    })
}

fn checksum_pieces(pieces: &[Bytes], algorithm: ChecksumAlgorithm) -> Checksum {
    let mut checksum = ChecksumDigest::new(algorithm);
    for piece in pieces {
        checksum.update(piece);
    }
    checksum.finish()
}

/// How a fold combines the chain's newest own extents with the tail's pieces.
///
/// `retained` counts the leading extents that the fold keeps, and
/// `candidate_length` is the length of the extent that combines the rest.
struct CombinePlan {
    retained: usize,
    candidate_length: u64,
}

/// Returns which of `extents` a fold keeps, and how long the extent is that
/// combines the newest own extents with `tail_length` bytes of tail pieces.
fn combine_plan(extents: &[LocatedExtent], newest: &ContentRef, tail_length: u64) -> CombinePlan {
    let mut retained = extents.len();
    let mut candidate_length = tail_length;
    if tail_length == 0 {
        if let Some(last) = extents
            .last()
            .filter(|last| is_own_extent(&last.extent, &newest.content_id))
        {
            retained -= 1;
            candidate_length = last.extent.length;
        }
    }
    for located in extents[..retained].iter().rev() {
        let extent = &located.extent;
        // A fresh chain keeps its base's extents shared.
        if !is_own_extent(extent, &newest.content_id) || extent.length > 2 * candidate_length {
            break;
        }
        candidate_length += extent.length;
        retained -= 1;
    }
    let own = |located: &&LocatedExtent| is_own_extent(&located.extent, &newest.content_id);
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
    CombinePlan {
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
            .filter(|checksum| checksum.algorithm() == store.checksum_algorithm())
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
            &checksum_pieces(tail, expected.algorithm()),
            tail.iter().map(|piece| piece.len() as u64).sum(),
        )
        .ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!("content object `{key}` has no combinable CRC"))
        })?;
    Ok((sources, expected))
}

#[cfg(test)]
mod tests;
