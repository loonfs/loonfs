//! Ordered immutable objects that store a content chain.

use crate::{ContentId, NamespaceId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Selects an object under a chain's own key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExtentObject {
    /// The object at the chain's own key.
    Whole,
    /// An object named by the chain offsets it was written for.
    Span {
        /// Inclusive chain offset.
        start: u64,
        /// Exclusive chain offset.
        end: u64,
    },
}

/// One run of chain bytes stored in an immutable object under the pool's
/// prefix.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentExtent {
    /// Chain whose key contains the object.
    pub content_id: ContentId,
    /// Object under the chain's key.
    pub object: ExtentObject,
    /// Offset of the first byte in the object.
    pub offset: u64,
    /// Number of bytes taken from the object.
    pub length: u64,
}

/// Objects in order from the first byte of a chain, and the namespace that
/// created the chain.
///
/// See [metadata rows](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a6-metadata-rows-and-row-keys).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentLayout {
    /// The namespace whose commit started the chain, which is the only
    /// namespace that extends the chain in place. Every layout of one chain
    /// records the same creator.
    pub creator_namespace_id: NamespaceId,
    /// Consecutive runs of chain bytes.
    pub extents: Vec<ContentExtent>,
}

/// A layout that cannot describe the expected chain size.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid content layout: {reason}")]
#[non_exhaustive]
pub struct ContentLayoutError {
    /// Format rule violated by the layout.
    pub reason: &'static str,
}

impl ContentLayout {
    /// Returns the total length covered by the extents.
    pub fn size_bytes(&self) -> u64 {
        self.extents.iter().map(|extent| extent.length).sum()
    }

    /// Checks coverage and object bounds. The creator and each content ID
    /// are valid by construction, because their types parse every value.
    pub fn validate(&self, size_bytes: u64) -> Result<(), ContentLayoutError> {
        let invalid = |reason| ContentLayoutError { reason };
        if size_bytes == 0
            && !matches!(
                self.extents.as_slice(),
                [ContentExtent {
                    object: ExtentObject::Whole,
                    length: 0,
                    ..
                }]
            )
        {
            return Err(invalid(
                "zero bytes require one whole extent of length zero",
            ));
        }
        let mut length = 0_u64;
        for extent in &self.extents {
            if size_bytes > 0 && extent.length == 0 {
                return Err(invalid("extent lengths must be positive"));
            }
            let object_end = extent
                .offset
                .checked_add(extent.length)
                .ok_or_else(|| invalid("extent object range overflows"))?;
            if let ExtentObject::Span { start, end } = extent.object {
                if start >= end || object_end > end - start {
                    return Err(invalid("extent range must fit within the span"));
                }
            }
            length = length
                .checked_add(extent.length)
                .ok_or_else(|| invalid("layout length overflows"))?;
        }
        if length != size_bytes {
            return Err(invalid("extent lengths must equal the chain size"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layouts_cover_the_chain_and_validate_empty_and_span_extents() {
        let whole = ContentExtent {
            content_id: ContentId::parse("con_014d2pf2dbsqqg28t5cy4tqkff").expect("content"),
            object: ExtentObject::Whole,
            offset: 0,
            length: 10,
        };
        let span = ContentExtent {
            object: ExtentObject::Span { start: 10, end: 15 },
            length: 5,
            ..whole.clone()
        };
        let creator = NamespaceId::parse("creator").expect("namespace");
        let layout = ContentLayout {
            creator_namespace_id: creator.clone(),
            extents: vec![whole.clone(), span.clone()],
        };
        assert_eq!(layout.size_bytes(), 15);
        assert!(layout.validate(15).is_ok());
        layout
            .validate(14)
            .expect_err("a size below the layout length should be rejected");
        layout
            .validate(16)
            .expect_err("a size above the layout length should be rejected");
        ContentLayout {
            creator_namespace_id: creator.clone(),
            extents: vec![],
        }
        .validate(0)
        .expect_err("an empty layout should be rejected");
        assert!(ContentLayout {
            creator_namespace_id: creator.clone(),
            extents: vec![ContentExtent {
                length: 0,
                ..whole.clone()
            }]
        }
        .validate(0)
        .is_ok());
        for (object, length, size_bytes) in [
            (ExtentObject::Span { start: 2, end: 2 }, 0, 0),
            (ExtentObject::Span { start: 3, end: 2 }, 1, 1),
            (ExtentObject::Span { start: 10, end: 15 }, 6, 6),
            (ExtentObject::Whole, 0, 1),
        ] {
            ContentLayout {
                creator_namespace_id: creator.clone(),
                extents: vec![ContentExtent {
                    object,
                    length,
                    ..whole.clone()
                }],
            }
            .validate(size_bytes)
            .expect_err("an extent whose length does not fit its object should be rejected");
        }
        assert!(ContentLayout {
            creator_namespace_id: creator.clone(),
            extents: vec![ContentExtent {
                length: 3,
                ..span.clone()
            }]
        }
        .validate(3)
        .is_ok());
        ContentLayout {
            creator_namespace_id: creator.clone(),
            extents: vec![ContentExtent {
                offset: 4,
                length: 2,
                ..span.clone()
            }],
        }
        .validate(2)
        .expect_err("a first extent that does not start at offset 0 should be rejected");
        assert!(ContentLayout {
            creator_namespace_id: creator.clone(),
            extents: vec![ContentExtent {
                offset: 1,
                length: 2,
                ..span
            }]
        }
        .validate(2)
        .is_ok());
        ContentLayout {
            creator_namespace_id: creator.clone(),
            extents: vec![
                ContentExtent {
                    length: u64::MAX,
                    ..whole.clone()
                },
                whole,
            ],
        }
        .validate(9)
        .expect_err("an overflowing layout should be rejected");
    }
}
