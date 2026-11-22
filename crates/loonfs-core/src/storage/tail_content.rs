//! Writes WAL pieces as immutable extents and combines a chain's extents.

use super::content::{
    content_object_key_for_ref, describe_checksum, DurableContentValidationError,
};
use super::content_location::{
    chain_object_key, extent_object_key, ContentLocation, LayoutLookup, LocatedExtent,
};
use crate::error::{CoreError, Result};
use crate::limits::MAX_LAYOUT_EXTENTS;
use crate::namespace::state::NamespaceReadState;
use crate::wal::ProjectedContent;
use crate::Permits;
use bytes::Bytes;
use loonfs_objectstore::{required_checksum, AssemblySource, ObjectStore, ObjectStoreError};
use loonfs_types::format::manifest::ContentLayoutRecord;
use loonfs_types::{
    Checksum, ChecksumAlgorithm, ChecksumDigest, ContentExtent, ContentId, ContentLayout,
    ContentRef, ExtentObject, NamespaceId, PoolId, SpanObjectId,
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
/// stores the objects, and the namespace whose operation writes them.
pub(crate) struct ContentWrites<'a, S: ?Sized> {
    pub(crate) store: &'a S,
    pub(crate) permits: &'a Permits,
    pub(crate) pool_id: &'a PoolId,
    /// The namespace whose fold or validation writes the extents. A layout
    /// that the writes produce records the chain's creator, which is this
    /// namespace only for a chain that it created (see [`write_tail_content`]).
    pub(crate) namespace_id: &'a NamespaceId,
}

impl<'a, S: ?Sized> ContentWrites<'a, S> {
    /// Returns where the content writes of the namespace whose head is
    /// `head` go: its pool's prefix in `store`.
    pub(crate) fn new(store: &'a S, permits: &'a Permits, head: &'a NamespaceReadState) -> Self {
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

/// Which of a chain's extents a write of the chain's tail may merge with the
/// tail's pieces.
///
/// `OwnExtents` lets the write merge the extents that store the chain's own
/// bytes, and `OwnAndSharedBaseExtents` also lets it merge a shared base
/// extent that takes the whole of its object. Only the fold combines shared
/// base extents, because validation, merges, append planning, and direct
/// downloads must not copy a chain's inherited bytes on a request path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CombinePolicy {
    OwnExtents,
    OwnAndSharedBaseExtents,
}

/// Returns the layout of the first `content_ref.size_bytes` bytes of a chain
/// in the view that `lookup` and `content` describe, after writing the
/// chain's pending pieces to content objects.
///
/// The layout records the chain's creator: the creator that the chain's
/// layout row records, or, for a chain without a layout row, the namespace
/// whose WAL object started it (see [`write_tail_content`]).
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
                CombinePolicy::OwnExtents,
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
/// and returns the chain's new layout row.
///
/// The extent is a new span object under a fresh ID (format spec, "File
/// contents and ownership"), so two writes of the same pieces, such as a
/// validation's and a later fold's, store two objects, and each layout lists
/// its own. An object at the new key is corruption, because no other write
/// generates the same ID. (The store accepts an object with the same stored
/// checksum, which only this write's own retry of the put can have written.)
///
/// The row records the chain's creator: the creator that the chain's layout
/// row records, or, for a chain that started in the tail, the namespace whose
/// WAL object started it. When `content_writes.namespace_id` didn't create the
/// chain, the pieces are written as one extent that is never combined with
/// the chain's existing extents (format spec, "Publishing a manifest").
/// Otherwise `policy` says which of the chain's extents the write may merge.
pub(crate) async fn write_tail_content<S: ObjectStore + ?Sized, L: LayoutLookup>(
    content_writes: ContentWrites<'_, S>,
    lookup: &L,
    content: &ProjectedContent,
    pieces: Vec<Bytes>,
    policy: CombinePolicy,
) -> Result<ContentLayoutRecord> {
    let newest = &content.content_ref;
    let pool_id = content_writes.pool_id;
    let TailExtentPlan {
        creator_namespace_id,
        mut extents,
        retained,
        object,
        length,
        candidate,
    } = tail_extent_plan(lookup, content_writes, content, pieces, policy).await?;
    let object_key = chain_object_key(pool_id, &newest.content_id, &object);
    let _permit = content_writes.permits.acquire().await;
    let (sources, expected) = assembly_sources(
        content_writes.store,
        &object_key,
        &extents[retained..],
        &candidate,
    )
    .await?;
    let written = match content_writes
        .store
        .assemble(&object_key, &sources, candidate, &expected)
        .await
    {
        Ok(written) => written,
        Err(error) => {
            tracing::error!(
                namespace_id = %content_writes.namespace_id,
                object_key = %object_key,
                error = %error,
                "content write failed"
            );
            return Err(content_write_error(&object_key, error));
        }
    };
    // The extent records the checksum that the store keeps for the new
    // object, which the assembly checked against the combined checksum of its
    // sources and tail, so no byte is hashed again.
    let object_checksum = required_checksum(&object_key, written.checksum)
        .map_err(|error| CoreError::store(&object_key, &error))?;
    extents.truncate(retained);
    let mut extents: Vec<_> = extents.into_iter().map(|located| located.extent).collect();
    extents.push(ContentExtent {
        content_id: newest.content_id.clone(),
        object,
        object_checksum,
        offset: 0,
        length,
    });
    Ok(ContentLayoutRecord {
        content_id: newest.content_id.clone(),
        committed_seq: content.committed_seq,
        size_bytes: newest.size_bytes,
        layout: ContentLayout {
            creator_namespace_id,
            extents,
        },
    })
}

/// Returns the error for a failed write of the content object `object_key`.
fn content_write_error(object_key: &str, error: ObjectStoreError) -> CoreError {
    match error {
        ObjectStoreError::Transport { message, .. } => CoreError::store(
            object_key,
            &ObjectStoreError::retryable_transport(object_key, message),
        ),
        ObjectStoreError::ChecksumMismatch {
            object_key: mismatched,
        } => CoreError::NamespaceCorrupt(format!(
            "content object `{mismatched}` does not match its stored checksum"
        )),
        error => CoreError::store(object_key, &error),
    }
}

/// Returns whether `extent` stores bytes of the chain `chain_content_id`
/// itself, rather than bytes of a shared base chain whose extents a new chain
/// lists first.
pub(crate) fn is_own_extent(extent: &ContentExtent, chain_content_id: &ContentId) -> bool {
    extent.content_id == *chain_content_id
}

/// What one write of a chain's tail assembles: the chain's creator, the
/// chain's extents, how many of them the new layout keeps, the object that the
/// write creates and the length of the extent that lists it after them, and the
/// bytes that the new extent ends with.
struct TailExtentPlan {
    creator_namespace_id: NamespaceId,
    extents: Vec<LocatedExtent>,
    retained: usize,
    object: ExtentObject,
    length: u64,
    candidate: Vec<Bytes>,
}

async fn tail_extent_plan<S: ?Sized, L: LayoutLookup>(
    lookup: &L,
    content_writes: ContentWrites<'_, S>,
    content: &ProjectedContent,
    pieces: Vec<Bytes>,
    policy: CombinePolicy,
) -> Result<TailExtentPlan> {
    let newest = &content.content_ref;
    let pool_id = content_writes.pool_id;
    let mut location = ContentLocation::resolve(lookup, pool_id, Some(content), newest).await?;
    if newest.size_bytes == 0 {
        location.extents.clear();
    }
    let creator_namespace_id = location
        .creator_namespace_id
        .take()
        .unwrap_or_else(|| content.creator_namespace_id.clone());
    // A namespace that didn't create the chain never combines the chain's
    // extents, so it never copies the creator's bytes into an object that
    // only its own layout lists.
    let created_here = creator_namespace_id == *content_writes.namespace_id;
    let own = |extent: &ContentExtent| created_here && is_own_extent(extent, &newest.content_id);
    let mut start = location.extents_length();
    let mut candidate = if content.pieces[0].offset == start {
        pieces
    } else {
        location.pieces().to_vec()
    };
    if let Some(last) = location.extents.last_mut().filter(|last| own(&last.extent)) {
        let full_length = match last.extent.object {
            ExtentObject::Span { start, end, .. } => end - start,
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
    let extents = location.extents;
    // An assembly copies whole objects, so a shared base extent that takes
    // part of its object can't be merged, and the plan can't tell how much
    // of a whole-object extent's object it takes without a request. The
    // restoration above makes an own extent whole.
    let combinable = |extent: &ContentExtent| {
        own(extent)
            || (created_here
                && policy == CombinePolicy::OwnAndSharedBaseExtents
                && takes_whole_span(extent))
    };
    let tail_length = candidate.iter().map(|piece| piece.len() as u64).sum();
    let CombinePlan {
        retained,
        candidate_length,
    } = combine_plan(&extents, tail_length, own, combinable);
    let combined_length = candidate_length - tail_length;
    start -= combined_length;
    // A layout of zero bytes is one whole extent of length zero. Every other
    // write is a new span object with an ID of its own, so no other write,
    // in this namespace or another member of the pool, writes its key.
    let object = if newest.size_bytes == 0 {
        ExtentObject::Whole
    } else {
        ExtentObject::Span {
            object_id: SpanObjectId::generate(),
            start,
            end: newest.size_bytes,
        }
    };
    Ok(TailExtentPlan {
        creator_namespace_id,
        extents,
        retained,
        object,
        length: candidate_length,
        candidate,
    })
}

/// Returns whether `extent` is a span that takes every byte of its object.
/// A layout records a span object's bounds but not a whole object's length,
/// so a whole-object extent never qualifies.
fn takes_whole_span(extent: &ContentExtent) -> bool {
    match extent.object {
        ExtentObject::Whole => false,
        ExtentObject::Span { start, end, .. } => extent.offset == 0 && extent.length == end - start,
    }
}

fn checksum_pieces(pieces: &[Bytes], algorithm: ChecksumAlgorithm) -> Checksum {
    let mut checksum = ChecksumDigest::new(algorithm);
    for piece in pieces {
        checksum.update(piece);
    }
    checksum.finish()
}

/// How a write combines the chain's newest extents with the tail's pieces.
///
/// `retained` counts the leading extents that the write keeps, and
/// `candidate_length` is the length of the extent that combines the rest.
struct CombinePlan {
    retained: usize,
    candidate_length: u64,
}

/// Returns which of `extents` a write keeps, and how long the extent is that
/// combines the newest combinable extents with `tail_length` bytes of tail
/// pieces.
///
/// `combinable` returns whether the write may merge an extent, and `own`
/// returns whether an extent stores the chain's own bytes. Two rules choose
/// the extents to merge (format spec, "Publishing a manifest"):
///
/// - The doubling rule merges the newest extent into the candidate while that
///   extent is combinable and at most twice the candidate's length.
/// - When more than [`MAX_LAYOUT_EXTENTS`] combinable extents after the last
///   extent that the write can't combine would remain, the cap keeps the
///   longest prefix of those extents, of fewer than [`MAX_LAYOUT_EXTENTS`],
///   in which each extent is at least as long as every extent after it and
///   the candidate together, and merges the rest.
///   Each merged span is then at most as long as the extent before it. While
///   fewer than 31 extents form the prefix, a later cap copies the span again
///   only after the extents behind it have grown to its length. When 31 do,
///   the span after them absorbs each write's pieces and is copied again on
///   every write until it outgrows the last prefix extent, which is at most
///   the chain's length divided by 2^29.
fn combine_plan(
    extents: &[LocatedExtent],
    tail_length: u64,
    own: impl Fn(&ContentExtent) -> bool,
    combinable: impl Fn(&ContentExtent) -> bool,
) -> CombinePlan {
    let length = |located: &LocatedExtent| located.extent.length;
    let mut retained = extents.len();
    let mut candidate_length = tail_length;
    if tail_length == 0 {
        if let Some(last) = extents.last().filter(|last| own(&last.extent)) {
            retained -= 1;
            candidate_length = last.extent.length;
        }
    }
    for located in extents[..retained].iter().rev() {
        let extent = &located.extent;
        if !combinable(extent) || extent.length > 2 * candidate_length {
            break;
        }
        candidate_length += extent.length;
        retained -= 1;
    }
    // No write can merge an extent before the last one that it can't
    // combine, so the cap counts only the extents after that one.
    let run_start = extents[..retained]
        .iter()
        .rposition(|located| !combinable(&located.extent))
        .map_or(0, |index| index + 1);
    if retained - run_start + 1 > MAX_LAYOUT_EXTENTS {
        let mut after =
            extents[run_start..retained].iter().map(length).sum::<u64>() + candidate_length;
        let mut kept = run_start;
        // The prefix leaves room for the candidate under the cap.
        for located in extents[run_start..retained]
            .iter()
            .take(MAX_LAYOUT_EXTENTS - 1)
        {
            after -= located.extent.length;
            if located.extent.length < after {
                break;
            }
            kept += 1;
        }
        retained = kept;
        candidate_length = tail_length + extents[retained..].iter().map(length).sum::<u64>();
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
        if checksum != located.extent.object_checksum {
            return Err(CoreError::NamespaceCorrupt(format!(
                "content object `{source_key}` is stored with checksum `{}:{}`, not the `{}:{}` that its extent records",
                checksum.algorithm(),
                checksum.value(),
                located.extent.object_checksum.algorithm(),
                located.extent.object_checksum.value()
            )));
        }
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
