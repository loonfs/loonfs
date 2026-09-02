//! Shared endpoint parsing and addressing helpers.

use crate::object_store::Result;
use crate::ObjectStoreError;

pub(crate) struct ParsedEndpoint<'a> {
    pub(crate) scheme: &'static str,
    pub(crate) authority: &'a str,
    pub(crate) path: &'a str,
}

pub(crate) fn parse_endpoint_url(value: &str) -> Result<ParsedEndpoint<'_>> {
    let (scheme, rest) = value.split_once("://").ok_or_else(|| {
        ObjectStoreError::Configuration(
            "endpoint url must start with http:// or https://".to_owned(),
        )
    })?;
    let scheme = if scheme.eq_ignore_ascii_case("https") {
        "https"
    } else if scheme.eq_ignore_ascii_case("http") {
        "http"
    } else {
        return Err(ObjectStoreError::Configuration(
            "endpoint url must start with http:// or https://".to_owned(),
        ));
    };
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

pub(crate) fn virtual_hosted_authority(bucket: &str, authority: &str) -> String {
    let bucket = bucket.trim();
    if authority.starts_with(&format!("{bucket}.")) {
        return authority.to_owned();
    }
    format!("{bucket}.{authority}")
}
