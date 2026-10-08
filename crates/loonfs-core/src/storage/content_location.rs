//! Resolves published content to an object prefix and the unfolded pieces
//! that follow it.

use super::content::{content_object_key_for_ref, DurableContentValidationError};
use crate::wal::ProjectedWalTail;
use bytes::Bytes;
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::{ByteRange, ObjectStore};
use loonfs_types::{ContentId, ContentRef, NamespaceId};

/// Where a published reference's bytes are read from: the first bytes of an
/// object, then pieces the WAL tail holds. Content errors are reported under
/// the reference's own object key, which holds the content after folding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentLocation {
    object_key: String,
    prefix: Option<ObjectPrefix>,
    pieces: Vec<Bytes>,
}

/// The first `length` bytes of the object at `object_key`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ObjectPrefix {
    object_key: String,
    length: u64,
}

impl ContentLocation {
    /// Locates the bytes a reference names.
    pub(crate) fn resolve(
        tail: Option<&ProjectedWalTail>,
        content_ref: &ContentRef,
    ) -> Result<Self, DurableContentValidationError> {
        let object_key = content_object_key_for_ref(content_ref)?;
        Ok(Self {
            object_key,
            ..Self::prefix_of(
                tail,
                &content_ref.owner_namespace_id,
                &content_ref.content_id,
                content_ref.size_bytes,
            )
        })
    }

    /// Locates the first `length` bytes of a content id. The object prefix
    /// ends at the lowest unfolded offset of the id at or below `length`, or
    /// at `length` when the tail holds nothing there. A first piece that
    /// names a base takes its prefix from the base, located the same way.
    pub(crate) fn prefix_of(
        tail: Option<&ProjectedWalTail>,
        owner_namespace_id: &NamespaceId,
        content_id: &ContentId,
        length: u64,
    ) -> Self {
        let object_key = content_blob(owner_namespace_id, content_id);
        let mut chain = (owner_namespace_id.clone(), content_id.clone());
        let mut end = length;
        let mut runs = Vec::new();
        // Each step moves to an older chain, so a tail of n chains takes at
        // most n steps; a longer walk is a corrupt tail and reads the object.
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
                    chain = (base.owner_namespace_id.clone(), base.content_id.clone());
                }
                _ => break,
            }
        }
        Self {
            object_key,
            prefix: (end > 0 || runs.is_empty()).then(|| ObjectPrefix {
                object_key: content_blob(&chain.0, &chain.1),
                length: end,
            }),
            pieces: runs.into_iter().rev().flatten().collect(),
        }
    }

    /// The object that holds the referenced content once it is folded.
    pub(crate) fn object_key(&self) -> &str {
        &self.object_key
    }

    /// Whether the WAL tail supplies any of the bytes.
    pub(crate) fn has_pieces(&self) -> bool {
        !self.pieces.is_empty()
    }

    /// Whether every byte is in memory, so reading needs no store request.
    pub(crate) fn is_resident(&self) -> bool {
        self.prefix.is_none()
    }

    fn prefix_length(&self) -> u64 {
        self.prefix.as_ref().map_or(0, |prefix| prefix.length)
    }

    /// Checks that the object prefix exists and is at least as long as the
    /// bytes read from it.
    pub(crate) async fn check_prefix<S: ObjectStore + ?Sized>(
        &self,
        store: &S,
    ) -> Result<(), DurableContentValidationError> {
        let Some(prefix) = &self.prefix else {
            return Ok(());
        };
        let metadata = match store.head(&prefix.object_key).await {
            Ok(Some(metadata)) => metadata,
            Ok(None) => {
                return Err(DurableContentValidationError::MissingContentObject {
                    object_key: prefix.object_key.clone(),
                })
            }
            Err(error) => {
                return Err(DurableContentValidationError::Store {
                    object_key: prefix.object_key.clone(),
                    message: error.public_message().into_owned(),
                })
            }
        };
        if metadata.size_bytes < prefix.length {
            return Err(DurableContentValidationError::ContentLengthMismatch {
                object_key: prefix.object_key.clone(),
                expected: prefix.length,
                actual: metadata.size_bytes,
            });
        }
        Ok(())
    }

    /// Reads bytes `[start, end)` of the assembled content: the part below
    /// the prefix length from the object, the rest from the pieces. A short
    /// object yields a short answer, which the caller reports.
    pub(crate) async fn read_range<S: ObjectStore + ?Sized>(
        &self,
        store: &S,
        start: u64,
        end: u64,
    ) -> Result<Bytes, DurableContentValidationError> {
        let prefix_length = self.prefix_length();
        let mut parts = Vec::new();
        if let Some(prefix) = self.prefix.as_ref().filter(|_| start < prefix_length) {
            let range = ByteRange {
                start_inclusive: start,
                end_exclusive: end.min(prefix_length),
            };
            match store.get(&prefix.object_key, Some(range)).await {
                Ok(Some(bytes)) => parts.push(bytes),
                Ok(None) => {
                    return Err(DurableContentValidationError::MissingContentObject {
                        object_key: prefix.object_key.clone(),
                    })
                }
                Err(error) => {
                    return Err(DurableContentValidationError::Store {
                        object_key: prefix.object_key.clone(),
                        message: error.public_message().into_owned(),
                    })
                }
            }
        }
        let mut offset = prefix_length;
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

    /// Reads and verifies all of the referenced content.
    pub(crate) async fn get_bytes<S: ObjectStore + ?Sized>(
        &self,
        store: &S,
        content_ref: &ContentRef,
    ) -> Result<Vec<u8>, DurableContentValidationError> {
        if self.prefix_length() == 0 {
            self.check_prefix(store).await?;
        }
        let bytes = self.read_range(store, 0, content_ref.size_bytes).await?;
        super::content::validate_loaded_content_bytes(
            self.object_key.clone(),
            content_ref,
            &bytes,
        )?;
        Ok(bytes.to_vec())
    }
}
