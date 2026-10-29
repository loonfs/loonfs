//! Key validation and the key prefix that scopes a store's keys.

use crate::object_store::Result;
use crate::ObjectStoreError;

/// Whether a key may end in one `/`, as a listing prefix may.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrailingSeparator {
    /// One trailing `/` is accepted and dropped.
    Allowed,
    /// A trailing `/` is an empty segment and is refused.
    Refused,
}

pub(crate) fn validate_segments(
    key: &str,
    trailing_separator: TrailingSeparator,
) -> Result<Vec<&str>> {
    if key.is_empty() {
        return Ok(Vec::new());
    }

    let raw_segments: Vec<_> = key.split('/').collect();
    let mut segments = Vec::with_capacity(raw_segments.len());

    for (index, segment) in raw_segments.iter().enumerate() {
        if segment.is_empty() {
            let is_trailing =
                trailing_separator == TrailingSeparator::Allowed && index + 1 == raw_segments.len();
            if is_trailing {
                continue;
            }

            return Err(ObjectStoreError::InvalidKey {
                object_key: key.to_owned(),
                message: "key must not contain empty segments".to_owned(),
            });
        }

        if *segment == "." || *segment == ".." {
            return Err(ObjectStoreError::InvalidKey {
                object_key: key.to_owned(),
                message: "key must not contain `.` or `..` segments".to_owned(),
            });
        }

        segments.push(*segment);
    }

    Ok(segments)
}

/// The key prefix that one store scopes every key beneath.
///
/// A store, its request signer, and its presigner share one value, so all
/// three resolve a key to the same provider key. A signed write under another
/// spelling of the prefix would be stored where the store's reads, listings,
/// and garbage collection never look.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct KeyScope {
    /// The normalized prefix, which is never empty.
    prefix: Option<String>,
}

/// The provider prefix of one listing, and the provider key it starts after.
pub(crate) struct ScopedListing {
    pub(crate) prefix: String,
    pub(crate) start_after: Option<String>,
}

impl KeyScope {
    /// Normalizes a configured key prefix. A blank prefix scopes nothing.
    pub(crate) fn new(raw: Option<&str>) -> Result<Self> {
        let prefix = match raw {
            Some(raw) if !raw.trim().is_empty() => {
                Some(validate_segments(raw, TrailingSeparator::Refused)?.join("/"))
            }
            _ => None,
        };
        Ok(Self { prefix })
    }

    /// Returns the normalized prefix, or `None` when keys are not scoped.
    pub(crate) fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// Returns the provider key of the object at `key`.
    pub(crate) fn object_key(&self, key: &str) -> Result<String> {
        validate_segments(key, TrailingSeparator::Refused)?;
        Ok(match &self.prefix {
            Some(prefix) if key.is_empty() => prefix.clone(),
            Some(prefix) => format!("{prefix}/{key}"),
            None => key.to_owned(),
        })
    }

    /// Returns the provider prefix of a listing of `prefix`.
    pub(crate) fn listing_prefix(&self, prefix: &str) -> Result<String> {
        validate_segments(prefix, TrailingSeparator::Allowed)?;
        Ok(match &self.prefix {
            Some(scope) if prefix.is_empty() => format!("{scope}/"),
            Some(scope) => format!("{scope}/{prefix}"),
            None => prefix.to_owned(),
        })
    }

    /// Returns the provider prefix of a child-prefix listing, which names a
    /// level of the key tree and so must be empty or end with `/`.
    pub(crate) fn child_listing_prefix(&self, prefix: &str) -> Result<String> {
        if !prefix.is_empty() && !prefix.ends_with('/') {
            return Err(ObjectStoreError::InvalidKey {
                object_key: prefix.to_owned(),
                message: "a child-prefix listing needs an empty prefix or one ending with `/`"
                    .to_owned(),
            });
        }
        self.listing_prefix(prefix)
    }

    /// Returns the provider prefix of a listing of `prefix` and the provider
    /// key of `start_after`.
    pub(crate) fn listing(&self, prefix: &str, start_after: Option<&str>) -> Result<ScopedListing> {
        Ok(ScopedListing {
            prefix: self.listing_prefix(prefix)?,
            start_after: start_after.map(|key| self.object_key(key)).transpose()?,
        })
    }

    /// Returns the caller's key for a listed provider key, or `None` when the
    /// provider key is outside this scope.
    pub(crate) fn unscope(&self, scoped_key: &str) -> Option<String> {
        match &self.prefix {
            Some(prefix) => scoped_key
                .strip_prefix(prefix.as_str())?
                .strip_prefix('/')
                .map(str::to_owned),
            None => Some(scoped_key.to_owned()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{validate_segments, KeyScope, TrailingSeparator};
    use crate::keys::hint;
    use crate::ObjectStoreError;
    use std::assert_matches;

    #[test]
    fn a_key_prefix_rejects_traversal_and_empty_segments() {
        assert_matches!(
            KeyScope::new(Some("tenant-a//bad")),
            Err(ObjectStoreError::InvalidKey { object_key, .. }) if object_key == "tenant-a//bad"
        );
        assert_matches!(
            KeyScope::new(Some("../escape")),
            Err(ObjectStoreError::InvalidKey { object_key, .. }) if object_key == "../escape"
        );
    }

    #[test]
    fn a_plain_key_prefix_survives_normalization_and_a_blank_one_scopes_nothing() {
        let scope = KeyScope::new(Some("tenant-a/reports")).expect("plain prefix");
        assert_eq!(scope.prefix(), Some("tenant-a/reports"));
        assert_eq!(
            KeyScope::new(Some("   ")).expect("blank prefix").prefix(),
            None
        );
    }

    #[test]
    fn scoped_keys_keep_prefix_isolation() {
        let hint_key = hint(&loonfs_types::NamespaceId::parse("ns-1").expect("valid namespace id"));
        let scope = KeyScope::new(Some("tenant-a")).expect("prefix");
        assert_matches!(
            scope.object_key(&hint_key),
            Ok(scoped) if scoped == format!("tenant-a/{hint_key}")
        );
        assert_matches!(
            scope.listing_prefix("namespaces/ns-1/"),
            Ok(scoped) if scoped == "tenant-a/namespaces/ns-1/"
        );
        assert_eq!(
            scope.unscope(&format!("tenant-a/{hint_key}")),
            Some(hint_key.clone())
        );
        assert_eq!(scope.unscope("tenant-b/namespaces/ns-1/hint.json"), None);
        assert_eq!(scope.unscope("tenant-ab/namespaces/ns-1/hint.json"), None);
        assert_eq!(
            KeyScope::default().unscope(&hint_key),
            Some(hint_key.clone()),
            "an unscoped store lists every key as the caller wrote it"
        );
    }

    #[test]
    fn validate_segments_allows_trailing_separator_only_for_prefixes() {
        assert!(validate_segments("namespaces/ns-1/", TrailingSeparator::Allowed).is_ok());
        assert_matches!(
            validate_segments("namespaces/ns-1/", TrailingSeparator::Refused),
            Err(ObjectStoreError::InvalidKey { .. })
        );
    }
}
