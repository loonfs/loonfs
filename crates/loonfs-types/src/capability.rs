//! API groups, features, and limits advertised by a deployment.

use crate::{DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

/// The protocol version this build speaks.
pub const PROTOCOL_VERSION: &str = "v0";

/// The mandatory filesystem API group.
pub const API_GROUP_FILESYSTEM_V0: &str = "filesystem/v0";
/// The optional maintenance API group.
pub const API_GROUP_MAINTENANCE_V0: &str = "maintenance/v0";
/// The optional derived-index query API group.
pub const API_GROUP_QUERY_V0: &str = "query/v0";

/// Gates inline bytes on commit operations.
pub const FEATURE_COMMIT_INLINE_CONTENT: &str = "filesystem.commits.inline_content";
/// Gates direct upload sessions that are authorized with short-lived presigned URLs.
pub const FEATURE_UPLOADS_DIRECT_PUT: &str = "filesystem.uploads.direct_put";
/// Starting presigned `direct_multipart` upload sessions.
///
/// Independent of [`FEATURE_UPLOADS_DIRECT_PUT`]: a provider may sign
/// whole-object writes without having an S3-style multipart API at all.
pub const FEATURE_UPLOADS_DIRECT_MULTIPART: &str = "filesystem.uploads.direct_multipart";
/// Gates download grants that are authorized with short-lived presigned URLs.
///
/// A deployment that offers any direct transfer advertises this one, because
/// letting a client write an object too large to proxy back means being able to
/// hand it back.
pub const FEATURE_DOWNLOADS_DIRECT_GET: &str = "filesystem.downloads.direct_get";

/// Gates grep index content search: the serving half of the capability;
/// the namespace's verified active grep manifest is the data half.
pub const FEATURE_QUERY_GREP: &str = "query.grep";

/// Gates grep index maintenance: enabling a namespace's grep manifest,
/// disabling it, collecting its garbage, and reading its lifecycle.
///
/// The maintenance half of the same capability, and independent of
/// [`FEATURE_QUERY_GREP`]: searching an index and keeping one built are
/// separately deployable, so a deployment may advertise either alone. It is
/// a `maintenance.` key because its routes are maintenance routes, and because a
/// deployment that maintains an index it does not serve advertises no
/// `query/v0` API group for a `query.` key to be parented by.
pub const FEATURE_MAINTENANCE_GREP_INDEX: &str = "maintenance.grep.index";

macro_rules! limit_keys {
    ($( $(#[$meta:meta])* $name:ident = $value:literal; )+) => {
        $(
            $(#[$meta])*
            pub const $name: &str = $value;
        )+

        /// Every registered advisory limit key.
        pub const ALL_LIMIT_KEYS: &[&str] = &[$($name),+];
    };
}

// The [capability document](https://github.com/loonfs/loonfs/blob/main/docs/specs/api.md#21-the-capability-document)
// defines each limit key, and the docs below summarize it.
limit_keys! {
    /// Advisory limit: the largest request body accepted by one service-proxied
    /// upload content request. This is the proxy's cap, not the provider's, and not a
    /// maximum file size.
    LIMIT_UPLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES = "upload.service_proxied.max_content_bytes";
    /// Advisory limit: the largest object this deployment's provider accepts in
    /// one presigned `direct_put` request.
    ///
    /// Unrelated to [`LIMIT_UPLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES`],
    /// which bounds what the service will proxy on a client's behalf. This one is
    /// the provider's own
    /// single-request ceiling, and it is typically far larger; a claim above it
    /// returns `content_too_large` at begin rather than being signed into a
    /// write the provider would reject.
    LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES = "upload.direct_put.max_content_bytes";
    /// Advisory limit: the largest JSON body accepted when completing an upload.
    /// It is large enough for the maximum number of multipart entries.
    LIMIT_UPLOAD_COMPLETE_MAX_REQUEST_BODY_BYTES = "upload.complete.max_request_body_bytes";
    /// Advisory limit: the largest file content one service-proxied read will stream.
    /// The check is against the whole file, so a larger file needs a download grant.
    LIMIT_DOWNLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES = "download.service_proxied.max_content_bytes";
    /// Advisory limit: how many service-proxied upload requests a serving process
    /// streams at once. The cap is shared by all callers and is not a per-caller
    /// allowance; requests past it return `server_busy`.
    LIMIT_UPLOAD_SERVICE_PROXIED_MAX_CONCURRENT_REQUESTS = "upload.service_proxied.max_concurrent_requests";
    /// Advisory limit: how many service-proxied content reads a serving process
    /// streams at once. The cap is shared by all callers and is not a per-caller
    /// allowance; requests past it return `server_busy`.
    LIMIT_DOWNLOAD_SERVICE_PROXIED_MAX_CONCURRENT_REQUESTS = "download.service_proxied.max_concurrent_requests";
    /// Advisory limit: the most path operations one commit may include; a longer
    /// list returns `invalid_request` before planning.
    LIMIT_COMMIT_MAX_OPERATIONS = "commit.max_operations";
    /// Advisory limit: the largest `inline_content` value on one commit operation,
    /// measured before base64 encoding.
    LIMIT_COMMIT_MAX_INLINE_CONTENT_BYTES_PER_OPERATION = "commit.max_inline_content_bytes_per_operation";
    /// Advisory limit: the largest JSON body accepted on a commit request,
    /// base64 inline content included.
    LIMIT_COMMIT_MAX_REQUEST_BODY_BYTES = "commit.max_request_body_bytes";
    /// Advisory limit: the most principal ids one request may act as.
    LIMIT_ACCESS_MAX_PRINCIPALS_PER_REQUEST = "access.max_principals_per_request";
    /// Counts precondition entries, not resources; a longer list returns
    /// `invalid_request` before planning.
    LIMIT_COMMIT_MAX_PRECONDITIONS = "commit.max_preconditions";
    /// Advisory limit: the most content tokens one commit may include.
    LIMIT_COMMIT_MAX_CONTENT_TOKENS = "commit.max_content_tokens";
    /// Advisory limit: the most distinct external content refs one commit's
    /// operations may name.
    LIMIT_COMMIT_MAX_EXTERNAL_CONTENT_REFS = "commit.max_external_content_refs";
    /// Advisory limit: the largest accepted commit `message`, in bytes.
    LIMIT_COMMIT_MAX_MESSAGE_BYTES = "commit.max_message_bytes";
    /// Advisory capability key for the default page size applied when callers omit `limit`.
    LIMIT_PAGINATION_DEFAULT_LIMIT = "pagination.default_limit";
    /// Advisory capability key for the largest page size accepted by a deployment.
    LIMIT_PAGINATION_MAX_LIMIT = "pagination.max_limit";
    /// Advisory limit: the smallest accepted `grace_window_ms` on a `gc`
    /// request; smaller values return `invalid_request`. Derived from the
    /// publication budgets, not tuned.
    LIMIT_GC_MIN_GRACE_WINDOW_MS = "maintenance.gc.min_grace_window_ms";
    /// Advisory limit: files a plan-less `allow_scan` grep will scan before
    /// refusing with `query_unindexable`.
    LIMIT_QUERY_GREP_SCAN_BUDGET_FILES = "query.grep.scan_budget_files";
    /// Advisory limit: unindexed-tail revisions one grep scans exhaustively
    /// before failing with `index_lagging`.
    LIMIT_QUERY_GREP_TAIL_BUDGET_FILES = "query.grep.tail_budget_files";
}

/// The API groups, features, and limits that a deployment advertises.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CapabilityDocument {
    /// The protocol version, `v0`.
    pub protocol_version: String,
    /// The advertised API groups, as `group/version`. The deployment implements every required
    /// operation in each group.
    pub api_groups: Vec<String>,
    /// Feature keys, and whether this deployment supports each one. An absent key means the feature
    /// isn't supported.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub features: BTreeMap<String, bool>,
    /// Advisory numeric limits that clients can use to check requests before sending them.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub limits: BTreeMap<String, u64>,
}

/// Violation of the capability document rules.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum CapabilityDocumentError {
    /// Reports a malformed feature key.
    #[error(
        "feature `{feature}` must have at least two non-empty dotted components \
         and start with an advertised API group name"
    )]
    MalformedFeature {
        /// Feature key rejected while validating the deployment document.
        feature: String,
    },
}

impl CapabilityDocument {
    /// Reports whether a feature is advertised as supported.
    ///
    /// Absent keys are unsupported.
    pub fn supports(&self, feature: &str) -> bool {
        self.features.get(feature).copied().unwrap_or(false)
    }

    /// Returns the largest object this deployment's provider accepts in one
    /// `direct_put` request, when it advertises the limit.
    pub fn direct_put_max_content_bytes(&self) -> Option<u64> {
        self.limits
            .get(LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES)
            .copied()
    }

    /// Requires at least two non-empty dotted components, starting with an advertised group name.
    pub fn validate(&self) -> Result<(), CapabilityDocumentError> {
        let advertised_api_groups = api_group_names(&self.api_groups);
        for feature in self.features.keys() {
            if !feature_is_well_formed(&advertised_api_groups, feature) {
                return Err(CapabilityDocumentError::MalformedFeature {
                    feature: feature.clone(),
                });
            }
        }
        Ok(())
    }

    /// Drops feature keys that violate the feature-key rule, the
    /// client-side "ignore" handling for malformed documents.
    pub fn retain_well_formed(&mut self) {
        let advertised_api_groups = api_group_names(&self.api_groups);
        self.features
            .retain(|feature, _| feature_is_well_formed(&advertised_api_groups, feature));
    }
}

fn feature_is_well_formed(api_group_names: &[&str], feature: &str) -> bool {
    let Some((api_group, components)) = feature.split_once('.') else {
        return false;
    };
    !api_group.is_empty()
        && api_group_names.contains(&api_group)
        && components.split('.').all(|component| !component.is_empty())
}

fn api_group_names(api_groups: &[String]) -> Vec<&str> {
    api_groups
        .iter()
        .map(|api_group| api_group_name(api_group))
        .collect()
}

/// Returns the advisory capability limits of the pagination contract.
pub fn pagination_capability_limits() -> BTreeMap<String, u64> {
    BTreeMap::from([
        (
            LIMIT_PAGINATION_DEFAULT_LIMIT.to_owned(),
            u64::from(DEFAULT_PAGE_LIMIT),
        ),
        (
            LIMIT_PAGINATION_MAX_LIMIT.to_owned(),
            u64::from(MAX_PAGE_LIMIT),
        ),
    ])
}

/// The group name of a versioned API group: `filesystem/v0` has group `filesystem`.
fn api_group_name(api_group: &str) -> &str {
    api_group.split('/').next().unwrap_or(api_group)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn document() -> CapabilityDocument {
        CapabilityDocument {
            protocol_version: PROTOCOL_VERSION.to_owned(),
            api_groups: vec![
                API_GROUP_FILESYSTEM_V0.to_owned(),
                API_GROUP_MAINTENANCE_V0.to_owned(),
            ],
            features: BTreeMap::from([
                (FEATURE_COMMIT_INLINE_CONTENT.to_owned(), true),
                (FEATURE_UPLOADS_DIRECT_PUT.to_owned(), false),
            ]),
            limits: BTreeMap::new(),
        }
    }

    #[test]
    fn pagination_limits_are_advertised() {
        let limits = pagination_capability_limits();

        assert_eq!(
            limits.get("pagination.default_limit"),
            Some(&u64::from(DEFAULT_PAGE_LIMIT))
        );
        assert_eq!(
            limits.get("pagination.max_limit"),
            Some(&u64::from(MAX_PAGE_LIMIT))
        );
    }

    #[test]
    fn supports_answers_gating_questions() {
        let document = document();
        assert!(document.supports(FEATURE_COMMIT_INLINE_CONTENT));
        // Advertised-false and absent keys are both unsupported.
        assert!(!document.supports(FEATURE_UPLOADS_DIRECT_PUT));
        assert!(!document.supports(FEATURE_UPLOADS_DIRECT_MULTIPART));
    }

    #[test]
    fn feature_keys_require_non_empty_components_and_an_advertised_group() {
        for (feature, valid) in [
            (FEATURE_COMMIT_INLINE_CONTENT, true),
            ("filesystem.custom_feature", true),
            ("filesystem", false),
            ("query.index.fulltext", false),
            ("filesystem.uploads.", false),
            ("filesystem..upload", false),
            (".upload", false),
        ] {
            let mut document = document();
            document.features = BTreeMap::from([(feature.to_owned(), true)]);
            let expected = if valid {
                Ok(())
            } else {
                Err(CapabilityDocumentError::MalformedFeature {
                    feature: feature.to_owned(),
                })
            };
            assert_eq!(document.validate(), expected, "{feature}");
            document.retain_well_formed();
            assert_eq!(document.features.contains_key(feature), valid, "{feature}");
            assert!(document.validate().is_ok());
        }
    }

    #[test]
    fn capability_document_round_trips_and_tolerates_unknown_fields() {
        let document = document();
        let encoded = serde_json::to_string(&document).expect("encode");
        assert!(encoded.contains("\"api_groups\""));
        let decoded: CapabilityDocument = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded, document);

        let mut missing_groups = serde_json::to_value(&document).expect("encode");
        missing_groups
            .as_object_mut()
            .expect("document")
            .remove("api_groups");
        serde_json::from_value::<CapabilityDocument>(missing_groups)
            .expect_err("a document without groups should be rejected");

        let future = encoded.replacen('{', "{\"field_from_the_future\":true,", 1);
        let decoded: CapabilityDocument =
            serde_json::from_str(&future).expect("unknown fields are ignored");
        assert_eq!(decoded, document);
    }
}
