//! Ordered immutable objects that hold a content chain.

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

/// One run of chain bytes held in an immutable object.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentExtent {
    /// Namespace that owns the object.
    pub owner_namespace_id: NamespaceId,
    /// Chain whose key holds the object.
    pub content_id: ContentId,
    /// Object under the chain's key.
    pub object: ExtentObject,
    /// Offset of the first byte in the object.
    pub offset: u64,
    /// Number of bytes taken from the object.
    pub length: u64,
}

/// Objects in order from the first byte of a chain.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentLayout {
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

    /// Checks coverage, object bounds, and identifiers.
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
            NamespaceId::parse(extent.owner_namespace_id.as_str())
                .map_err(|_| invalid("invalid extent owner namespace id"))?;
            ContentId::parse(extent.content_id.as_str())
                .map_err(|_| invalid("invalid extent content id"))?;
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
            owner_namespace_id: NamespaceId::parse("owner").expect("namespace"),
            content_id: ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content"),
            object: ExtentObject::Whole,
            offset: 0,
            length: 10,
        };
        let span = ContentExtent {
            object: ExtentObject::Span { start: 10, end: 15 },
            length: 5,
            ..whole.clone()
        };
        let layout = ContentLayout {
            extents: vec![whole.clone(), span.clone()],
        };
        assert_eq!(layout.size_bytes(), 15);
        assert!(layout.validate(15).is_ok());
        assert!(layout.validate(14).is_err());
        assert!(layout.validate(16).is_err());
        assert!(ContentLayout { extents: vec![] }.validate(0).is_err());
        assert!(ContentLayout {
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
            assert!(ContentLayout {
                extents: vec![ContentExtent {
                    object,
                    length,
                    ..whole.clone()
                }]
            }
            .validate(size_bytes)
            .is_err());
        }
        assert!(ContentLayout {
            extents: vec![ContentExtent {
                length: 3,
                ..span.clone()
            }]
        }
        .validate(3)
        .is_ok());
        assert!(ContentLayout {
            extents: vec![ContentExtent {
                offset: 4,
                length: 2,
                ..span.clone()
            }]
        }
        .validate(2)
        .is_err());
        assert!(ContentLayout {
            extents: vec![ContentExtent {
                offset: 1,
                length: 2,
                ..span
            }]
        }
        .validate(2)
        .is_ok());
        assert!(ContentLayout {
            extents: vec![
                ContentExtent {
                    length: u64::MAX,
                    ..whole.clone()
                },
                whole
            ]
        }
        .validate(9)
        .is_err());
    }
}
