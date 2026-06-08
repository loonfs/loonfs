use crate::ObjectStoreError;

pub(crate) fn validate_segments(
    key: &str,
    allow_trailing_separator: bool,
) -> Result<Vec<&str>, ObjectStoreError> {
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

            return Err(ObjectStoreError::InvalidKey(key.to_owned()));
        }

        if *segment == "." || *segment == ".." {
            return Err(ObjectStoreError::InvalidKey(key.to_owned()));
        }

        segments.push(*segment);
    }

    Ok(segments)
}

pub(crate) fn normalize_key_prefix(
    key_prefix: Option<&str>,
) -> Result<Option<String>, ObjectStoreError> {
    let Some(key_prefix) = key_prefix else {
        return Ok(None);
    };
    if key_prefix.trim().is_empty() {
        return Ok(None);
    }

    let segments = validate_segments(key_prefix, false)?;
    Ok(Some(segments.join("/")))
}

pub(crate) fn scope_object_key(
    key_prefix: Option<&str>,
    key: &str,
) -> Result<String, ObjectStoreError> {
    validate_segments(key, false)?;

    match key_prefix {
        Some(key_prefix) if !key_prefix.is_empty() && !key.is_empty() => {
            Ok(format!("{key_prefix}/{key}"))
        }
        Some(key_prefix) if !key_prefix.is_empty() => Ok(key_prefix.to_owned()),
        _ => Ok(key.to_owned()),
    }
}

pub(crate) fn scope_list_prefix(
    key_prefix: Option<&str>,
    prefix: &str,
) -> Result<String, ObjectStoreError> {
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
    scoped_key
        .strip_prefix(&prefix)
        .map(|unscoped| unscoped.to_owned())
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_key_prefix, scope_list_prefix, scope_object_key, unscope_listed_key,
        validate_segments,
    };
    use crate::keys::namespace_head;
    use crate::ObjectStoreError;

    #[test]
    fn normalize_key_prefix_rejects_traversal_and_empty_segments() {
        assert!(matches!(
            normalize_key_prefix(Some("tenant-a//bad")),
            Err(ObjectStoreError::InvalidKey(key)) if key == "tenant-a//bad"
        ));
        assert!(matches!(
            normalize_key_prefix(Some("../escape")),
            Err(ObjectStoreError::InvalidKey(key)) if key == "../escape"
        ));
    }

    #[test]
    fn normalize_key_prefix_trims_redundant_separators_by_segment_join() {
        assert!(matches!(
            normalize_key_prefix(Some("tenant-a/reports")),
            Ok(Some(prefix)) if prefix == "tenant-a/reports"
        ));
        assert!(matches!(normalize_key_prefix(Some("   ")), Ok(None)));
    }

    #[test]
    fn scoped_key_helpers_keep_prefix_isolation() {
        let head_key = namespace_head("ns-1");
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
            Err(ObjectStoreError::InvalidKey(_))
        ));
    }
}
