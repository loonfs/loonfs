use crate::error::ObjectStoreError;

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
