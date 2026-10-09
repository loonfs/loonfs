//! Resolves a reference through its layout and visible WAL pieces.

use super::content::{content_object_key_for_ref, DurableContentValidationError};
use crate::error::{CoreError, Result};
use crate::wal::ProjectedWalTail;
use bytes::Bytes;
use loonfs_objectstore::keys::{content_blob, content_span};
use loonfs_objectstore::{ByteRange, ObjectStore};
use loonfs_types::format::manifest::ContentLayoutRecord;
use loonfs_types::{ContentExtent, ContentId, ContentRef, ExtentObject};

#[async_trait::async_trait]
pub(crate) trait LayoutLookup: Sync {
    async fn content_layout(&self, content_id: &ContentId) -> Result<Option<ContentLayoutRecord>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocatedExtent {
    pub(crate) object_key: String,
    pub(crate) extent: ContentExtent,
}

pub(crate) fn extent_object_key(extent: &ContentExtent) -> String {
    match extent.object {
        ExtentObject::Whole => content_blob(&extent.owner_namespace_id, &extent.content_id),
        ExtentObject::Span { start, end } => {
            content_span(&extent.owner_namespace_id, &extent.content_id, start, end)
        }
    }
}

/// Objects and resident pieces needed to read a reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentLocation {
    object_key: String,
    pub(crate) extents: Vec<LocatedExtent>,
    pieces: Vec<Bytes>,
}

impl ContentLocation {
    /// Truncates the layout at the lowest tail offset, then takes the pieces.
    /// A nonzero folded prefix without a layout is corruption.
    pub(crate) async fn resolve<L: LayoutLookup>(
        lookup: &L,
        tail: Option<&ProjectedWalTail>,
        content_ref: &ContentRef,
    ) -> Result<Self> {
        let object_key = content_object_key_for_ref(content_ref)?;
        let mut chain = (
            content_ref.owner_namespace_id.clone(),
            content_ref.content_id.clone(),
        );
        let mut end = content_ref.size_bytes;
        let mut runs = Vec::new();
        for _ in 0..=tail.map_or(0, |tail| tail.contents().len()) {
            let Some(content) = tail
                .and_then(|tail| tail.content_by_id(&chain.0, &chain.1))
                .filter(|content| content.pieces[0].offset <= end)
            else {
                break;
            };
            runs.push(
                content
                    .pieces
                    .iter()
                    .take_while(|piece| piece.offset < end)
                    .map(|piece| {
                        piece
                            .bytes
                            .slice(..(end - piece.offset).min(piece.bytes.len() as u64) as usize)
                    })
                    .collect::<Vec<_>>(),
            );
            let first = &content.pieces[0];
            end = first.offset;
            match &first.base {
                Some(base) if end > 0 => {
                    chain = (base.owner_namespace_id.clone(), base.content_id.clone())
                }
                _ => break,
            }
        }
        let mut extents = Vec::new();
        if end > 0 {
            let row = lookup.content_layout(&chain.1).await?.ok_or_else(|| {
                CoreError::NamespaceCorrupt(format!(
                    "content `{}` has no layout for its first {end} bytes",
                    chain.1
                ))
            })?;
            if row.owner_namespace_id != chain.0
                || row.size_bytes < end
                || row.layout.validate(row.size_bytes).is_err()
            {
                return Err(CoreError::NamespaceCorrupt(format!(
                    "content `{}` requires a layout owned by `{}` covering {end} bytes",
                    chain.1, chain.0
                )));
            }
            let mut remaining = end;
            for mut extent in row.layout.extents {
                if remaining == 0 {
                    break;
                }
                let object_key = extent_object_key(&extent);
                extent.length = extent.length.min(remaining);
                remaining -= extent.length;
                extents.push(LocatedExtent { object_key, extent });
            }
        } else if content_ref.size_bytes == 0 && runs.is_empty() {
            return Self::whole(content_ref).map_err(Into::into);
        }
        Ok(Self {
            object_key,
            extents,
            pieces: runs.into_iter().rev().flatten().collect(),
        })
    }

    pub(crate) fn whole(
        content_ref: &ContentRef,
    ) -> std::result::Result<Self, DurableContentValidationError> {
        let object_key = content_object_key_for_ref(content_ref)?;
        Ok(Self {
            extents: vec![LocatedExtent {
                object_key: object_key.clone(),
                extent: ContentExtent {
                    owner_namespace_id: content_ref.owner_namespace_id.clone(),
                    content_id: content_ref.content_id.clone(),
                    object: ExtentObject::Whole,
                    offset: 0,
                    length: content_ref.size_bytes,
                },
            }],
            object_key,
            pieces: Vec::new(),
        })
    }

    pub(crate) fn object_key(&self) -> &str {
        &self.object_key
    }
    pub(crate) fn has_pieces(&self) -> bool {
        !self.pieces.is_empty()
    }
    pub(crate) fn is_resident(&self) -> bool {
        self.extents.is_empty()
    }
    pub(crate) fn extents_length(&self) -> u64 {
        self.extents.iter().map(|extent| extent.extent.length).sum()
    }
    pub(crate) fn joined_pieces(&self) -> Bytes {
        match self.pieces.as_slice() {
            [piece] => piece.clone(),
            pieces => pieces.concat().into(),
        }
    }

    pub(crate) async fn read_range<S: ObjectStore + ?Sized>(
        &self,
        store: &S,
        start: u64,
        end: u64,
    ) -> std::result::Result<Bytes, DurableContentValidationError> {
        let mut parts = Vec::new();
        let mut offset = 0;
        for located in &self.extents {
            let extent_end = offset + located.extent.length;
            if offset < end && extent_end > start {
                let range = ByteRange {
                    start_inclusive: located.extent.offset + start.max(offset) - offset,
                    end_exclusive: located.extent.offset + end.min(extent_end) - offset,
                };
                let expected = range.end_exclusive - range.start_inclusive;
                let bytes = store
                    .get(&located.object_key, Some(range))
                    .await
                    .map_err(|error| DurableContentValidationError::Store {
                        object_key: located.object_key.clone(),
                        message: error.public_message().into_owned(),
                    })?
                    .ok_or_else(|| DurableContentValidationError::MissingContentObject {
                        object_key: located.object_key.clone(),
                    })?;
                if bytes.len() as u64 != expected {
                    return Err(DurableContentValidationError::ContentLengthMismatch {
                        object_key: located.object_key.clone(),
                        expected,
                        actual: bytes.len() as u64,
                    });
                }
                parts.push(bytes);
            }
            offset = extent_end;
        }
        for piece in &self.pieces {
            let piece_end = offset + piece.len() as u64;
            if offset < end && piece_end > start {
                parts.push(piece.slice(
                    (start.max(offset) - offset) as usize..(end.min(piece_end) - offset) as usize,
                ));
            }
            offset = piece_end;
        }
        Ok(match parts.len() {
            1 => parts.remove(0),
            _ => parts.concat().into(),
        })
    }

    pub(crate) async fn get_bytes<S: ObjectStore + ?Sized>(
        &self,
        store: &S,
        content_ref: &ContentRef,
    ) -> std::result::Result<Vec<u8>, DurableContentValidationError> {
        let bytes = self.read_range(store, 0, content_ref.size_bytes).await?;
        super::content::validate_loaded_content_bytes(
            self.object_key.clone(),
            content_ref,
            &bytes,
        )?;
        Ok(bytes.to_vec())
    }
}
