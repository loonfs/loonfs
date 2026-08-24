//! Key validation and prefix scoping shared by the provider stores.

use crate::object_store::Result;
use crate::ObjectStoreError;

pub(crate) fn validate_segments(key: &str, allow_trailing_separator: bool) -> Result<Vec<&str>> {
    if key.is_empty() {
        return Ok(Vec::new());
    }

    let raw_segments: Vec<_> = key.split('/').collect();
    let mut segments = Vec::with_capacity(raw_segments.len());

    for (index, segment) in raw_segments.iter().enumerate() {
        if segment.is_empty() {
            let is_trailing = allow_trailing_separator && index + 1 == raw_segments.len();
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

pub(crate) fn normalize_key_prefix(key_prefix: Option<&str>) -> Result<Option<String>> {
    let Some(key_prefix) = key_prefix else {
        return Ok(None);
    };
    if key_prefix.trim().is_empty() {
        return Ok(None);
    }

    let segments = validate_segments(key_prefix, false)?;
    Ok(Some(segments.join("/")))
}

pub(crate) fn scope_object_key(key_prefix: Option<&str>, key: &str) -> Result<String> {
    validate_segments(key, false)?;

    match key_prefix {
        Some(key_prefix) if !key_prefix.is_empty() && !key.is_empty() => {
            Ok(format!("{key_prefix}/{key}"))
        }
        Some(key_prefix) if !key_prefix.is_empty() => Ok(key_prefix.to_owned()),
        _ => Ok(key.to_owned()),
    }
}

pub(crate) fn scope_list_prefix(key_prefix: Option<&str>, prefix: &str) -> Result<String> {
    validate_segments(prefix, true)?;

    match key_prefix {
        Some(key_prefix) if !key_prefix.is_empty() && !prefix.is_empty() => {
            Ok(format!("{key_prefix}/{prefix}"))
        }
        Some(key_prefix) if !key_prefix.is_empty() => Ok(format!("{key_prefix}/")),
        _ => Ok(prefix.to_owned()),
    }
}

pub(crate) fn unscope_listed_key(key_prefix: Option<&str>, scoped_key: &str) -> Option<String> {
    let key_prefix = key_prefix.filter(|value| !value.is_empty())?;
    let prefix = format!("{key_prefix}/");
    scoped_key.strip_prefix(&prefix).map(str::to_owned)
}

/// A parsed `http(s)://authority[/base-path]` endpoint, shared by the
/// provider client and the presigner so the two cannot drift.
pub(crate) struct ParsedEndpoint<'a> {
    pub(crate) scheme: &'a str,
    pub(crate) authority: &'a str,
    pub(crate) path: &'a str,
}

pub(crate) fn parse_endpoint_url(value: &str) -> Result<ParsedEndpoint<'_>> {
    let (scheme, rest) = value
        .strip_prefix("https://")
        .map(|rest| ("https", rest))
        .or_else(|| value.strip_prefix("http://").map(|rest| ("http", rest)))
        .ok_or_else(|| {
            ObjectStoreError::Configuration(
                "endpoint url must start with http:// or https://".to_owned(),
            )
        })?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    if authority.is_empty() {
        return Err(ObjectStoreError::Configuration(
            "endpoint url must include authority".to_owned(),
        ));
    }
    Ok(ParsedEndpoint {
        scheme,
        authority,
        path: path.trim_end_matches('/'),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_key_prefix, scope_list_prefix, scope_object_key, unscope_listed_key,
        validate_segments,
    };
    use crate::keys::wal_head;
    use crate::ObjectStoreError;

    #[test]
    fn normalize_key_prefix_rejects_traversal_and_empty_segments() {
        assert!(matches!(
            normalize_key_prefix(Some("tenant-a//bad")),
            Err(ObjectStoreError::InvalidKey { object_key, .. }) if object_key == "tenant-a//bad"
        ));
        assert!(matches!(
            normalize_key_prefix(Some("../escape")),
            Err(ObjectStoreError::InvalidKey { object_key, .. }) if object_key == "../escape"
        ));
    }

    #[test]
    fn a_plain_key_prefix_survives_normalization_and_a_blank_one_becomes_none() {
        assert!(matches!(
            normalize_key_prefix(Some("tenant-a/reports")),
            Ok(Some(prefix)) if prefix == "tenant-a/reports"
        ));
        assert!(matches!(normalize_key_prefix(Some("   ")), Ok(None)));
    }

    #[test]
    fn scoped_key_helpers_keep_prefix_isolation() {
        let head_key =
            wal_head(&loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id"));
        assert!(matches!(
            scope_object_key(Some("tenant-a"), &head_key),
            Ok(scoped) if scoped == format!("tenant-a/{head_key}")
        ));
        assert!(matches!(
            scope_list_prefix(Some("tenant-a"), "namespaces/ns-1/"),
            Ok(scoped) if scoped == "tenant-a/namespaces/ns-1/"
        ));
        assert_eq!(
            unscope_listed_key(Some("tenant-a"), &format!("tenant-a/{head_key}")),
            Some(head_key)
        );
        assert_eq!(
            unscope_listed_key(
                Some("tenant-a"),
                "tenant-b/namespaces/ns-1/control/head.json"
            ),
            None
        );
    }

    #[test]
    fn validate_segments_allows_trailing_separator_only_for_prefixes() {
        assert!(validate_segments("namespaces/ns-1/", true).is_ok());
        assert!(matches!(
            validate_segments("namespaces/ns-1/", false),
            Err(ObjectStoreError::InvalidKey { .. })
        ));
    }
}
