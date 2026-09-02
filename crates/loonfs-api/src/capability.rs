//! Profiles, features, and limits advertised by a deployment.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

/// The protocol generation this build speaks.
pub const PROTOCOL_VERSION: &str = "v0";

/// The mandatory data plane.
pub const PROFILE_CORE_V0: &str = "core/v0";
/// The optional maintenance plane.
pub const PROFILE_ADMIN_V0: &str = "admin/v0";
/// The optional derived-index query plane.
pub const PROFILE_QUERY_V0: &str = "query/v0";

/// Gates namespace creation.
pub const FEATURE_NAMESPACES_CREATE: &str = "core.namespaces.create";
/// Gates namespace forking.
pub const FEATURE_NAMESPACES_FORK: &str = "core.namespaces.fork";
/// Gates namespace deletion.
pub const FEATURE_NAMESPACES_DELETE: &str = "core.namespaces.delete";
/// Gates read-snapshot lifecycle operations.
pub const FEATURE_SNAPSHOTS: &str = "core.snapshots";
/// Gates inode attributes: writing them, and projecting them onto reads.
/// Attributes are part of the core plane, not a composed extension, so a
/// deployment that serves the core profile serves them.
pub const FEATURE_ATTRIBUTES: &str = "core.attributes";
/// Gates listing a directory's children by parent inode ID. Part of the core
/// plane and implemented by the runtime, so current deployments advertise it;
/// the key exists so inode-driven sync clients can gate on deployments built
/// before the route.
pub const FEATURE_INODES_LIST_CHILDREN: &str = "core.inodes.list_children";
/// Gates direct upload sessions that are authorized with short-lived presigned URLs.
pub const FEATURE_UPLOADS_DIRECT_PUT: &str = "core.uploads.direct_put";
/// Starting presigned `direct_multipart` upload sessions. Independent of
/// [`FEATURE_UPLOADS_DIRECT_PUT`]: a provider may sign whole-object writes
/// without having an S3-style multipart API at all.
pub const FEATURE_UPLOADS_DIRECT_MULTIPART: &str = "core.uploads.direct_multipart";
/// Gates download grants that are authorized with short-lived presigned
/// URLs. A deployment that offers any direct transfer advertises this one,
/// because letting a client write an object too large to proxy back means
/// being able to hand it back.
pub const FEATURE_DOWNLOADS_DIRECT_GET: &str = "core.downloads.direct_get";

/// Gates grep-index content search: the serving half of the capability;
/// the namespace's verified active grep root is the data half.
pub const FEATURE_QUERY_GREP: &str = "query.grep";

/// Gates grep-index administration: enabling a namespace's grep root,
/// disabling it, collecting its garbage, and reading its lifecycle.
///
/// The maintenance half of the same capability, and independent of
/// [`FEATURE_QUERY_GREP`]: searching an index and keeping one built are
/// separately deployable, so a deployment may advertise either alone. It is
/// an `admin.` key because its routes are admin routes, and because a
/// deployment that maintains an index it does not serve advertises no
/// `query/v0` profile for a `query.` key to be parented by.
pub const FEATURE_ADMIN_GREP_INDEX: &str = "admin.grep.index";

/// Advisory limit: the largest request body accepted for service-proxied
/// upload content requests. This is the proxy's cap, not the provider's.
pub const LIMIT_UPLOAD_MAX_CONTENT_BYTES: &str = "upload.max_content_bytes";
/// Advisory limit: the largest object this deployment's provider accepts in
/// one presigned `direct_put` request.
///
/// Unrelated to [`LIMIT_UPLOAD_MAX_CONTENT_BYTES`], which bounds what the
/// service will buffer on a client's behalf. This one is the provider's own
/// single-request ceiling, and it is typically far larger; a claim above it
/// answers `content_too_large` at begin rather than being signed into a
/// write the provider would reject.
pub const LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES: &str = "upload.direct_put_max_content_bytes";
/// Advisory limit: the largest JSON body accepted when completing an upload.
/// It is large enough for the maximum number of multipart entries.
pub const LIMIT_UPLOAD_COMPLETION_MAX_BODY_BYTES: &str = "upload.completion_max_body_bytes";
/// Advisory limit: the largest file content a service-proxied read will
/// buffer and return in one response.
pub const LIMIT_DOWNLOAD_MAX_CONTENT_BYTES: &str = "download.max_content_bytes";
/// Advisory limit: how many service-proxied upload bodies the deployment
/// buffers at once; requests past the cap answer `server_busy`.
pub const LIMIT_UPLOAD_MAX_CONCURRENT: &str = "upload.max_concurrent";
/// Advisory limit: how many service-proxied content reads the deployment
/// materializes at once; requests past the cap answer `server_busy`.
pub const LIMIT_DOWNLOAD_MAX_CONCURRENT: &str = "download.max_concurrent";
/// Advisory limit: the largest snapshot TTL one request may ask for.
pub const LIMIT_SNAPSHOT_MAX_TTL_MS: &str = "snapshot.max_ttl_ms";
/// Advisory limit: the largest snapshot expiry measured from record creation.
pub const LIMIT_SNAPSHOT_MAX_LIFETIME_MS: &str = "snapshot.max_lifetime_ms";
/// Advisory limit: the most live snapshots one namespace may hold.
pub const LIMIT_SNAPSHOT_MAX_LIVE_PER_NAMESPACE: &str = "snapshot.max_live_per_namespace";
/// Advisory limit: the most path operations one commit may carry; a longer
/// list answers `invalid_request` before planning.
pub const LIMIT_COMMIT_MAX_OPERATIONS: &str = "commit.max_operations";
/// Advisory limit: the most content tokens one commit may carry.
pub const LIMIT_COMMIT_MAX_CONTENT_TOKENS: &str = "commit.max_content_tokens";
/// Advisory limit: the most distinct external content refs one commit's
/// operations may name.
pub const LIMIT_COMMIT_MAX_EXTERNAL_CONTENT_REFS: &str = "commit.max_external_content_refs";
/// Advisory limit: the largest accepted commit `message`, in bytes.
pub const LIMIT_COMMIT_MAX_MESSAGE_BYTES: &str = "commit.max_message_bytes";
/// Advisory capability key for the default page size applied when callers omit `limit`.
pub const LIMIT_PAGINATION_DEFAULT: &str = "pagination.default_limit";
/// Advisory capability key for the largest page size accepted by a deployment.
pub const LIMIT_PAGINATION_MAX: &str = "pagination.max_limit";
/// Advisory limit: the smallest accepted `grace_window_ms` on a `gc`
/// request; smaller values answer `invalid_request`. Derived from the
/// publication budgets, not tuned.
pub const LIMIT_GC_MIN_GRACE_WINDOW_MS: &str = "maintenance.gc.min_grace_window_ms";
/// Advisory limit: matches per grep page when the request omits `limit`.
pub const LIMIT_QUERY_GREP_DEFAULT: &str = "query.grep.default_limit";
/// Advisory limit: the largest accepted grep page limit. Distinct from the
/// pagination keys — a grep item costs a verified file read, not a row.
pub const LIMIT_QUERY_GREP_MAX: &str = "query.grep.max_limit";
/// Advisory limit: files a plan-less `allow_scan` grep will scan before
/// refusing with `query_unindexable`.
pub const LIMIT_QUERY_GREP_SCAN_BUDGET_FILES: &str = "query.grep.scan_budget_files";
/// Advisory limit: unindexed-tail revisions one grep scans exhaustively
/// before failing with `index_lagging`.
pub const LIMIT_QUERY_GREP_TAIL_BUDGET_FILES: &str = "query.grep.tail_budget_files";

/// The profiles, features, and limits advertised by a deployment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CapabilityDocument {
    /// The protocol generation, currently `v0`.
    pub protocol_version: String,
    /// The advertised `plane/version` profiles, each with every required operation implemented.
    pub profiles: Vec<String>,
    /// The named features supported by this deployment, with absent keys treated as unsupported.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub features: BTreeMap<String, bool>,
    /// Advisory numeric limits clients may use to pre-validate requests.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub limits: BTreeMap<String, u64>,
}

/// Violation of the capability document rules.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CapabilityDocumentError {
    /// Reports a feature whose dotted plane prefix has no advertised profile.
    #[error(
        "feature `{feature}` is not parented by an advertised profile \
         (its first dotted segment must be one of the advertised plane names)"
    )]
    UnparentedFeature {
        /// Feature key rejected while validating the deployment document.
        feature: String,
    },
}

impl CapabilityDocument {
    /// Whether a profile (for example `core/v0`) is advertised.
    pub fn has_profile(&self, profile: &str) -> bool {
        self.profiles.iter().any(|advertised| advertised == profile)
    }

    /// Whether a feature is advertised as supported. Absent keys are
    /// unsupported.
    pub fn supports(&self, feature: &str) -> bool {
        self.features.get(feature).copied().unwrap_or(false)
    }

    /// The largest object this deployment's provider accepts in one
    /// `direct_put` request, when it advertises the limit.
    pub fn direct_put_max_content_bytes(&self) -> Option<u64> {
        self.limits
            .get(LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES)
            .copied()
    }

    /// Checks the feature-key rule (API spec, "Capability discovery"): every
    /// feature key's first dotted segment must be the plane name of an
    /// advertised profile.
    pub fn validate(&self) -> Result<(), CapabilityDocumentError> {
        for feature in self.features.keys() {
            if !self.feature_is_parented(feature) {
                return Err(CapabilityDocumentError::UnparentedFeature {
                    feature: feature.clone(),
                });
            }
        }
        Ok(())
    }

    /// Drops feature keys that violate the feature-key rule, the
    /// client-side "ignore" handling for malformed documents.
    pub fn retain_well_formed(&mut self) {
        let advertised_planes: Vec<&str> = self.profiles.iter().map(|p| plane_name(p)).collect();
        self.features
            .retain(|feature, _| feature_is_parented(&advertised_planes, feature));
    }

    fn feature_is_parented(&self, feature: &str) -> bool {
        let advertised_planes: Vec<&str> = self.profiles.iter().map(|p| plane_name(p)).collect();
        feature_is_parented(&advertised_planes, feature)
    }
}

fn feature_is_parented(planes: &[&str], feature: &str) -> bool {
    match feature.split('.').next() {
        Some(plane) if !plane.is_empty() => planes.contains(&plane),
        _ => false,
    }
}

/// The plane name of a profile: `core/v0` has plane `core`.
fn plane_name(profile: &str) -> &str {
    profile.split('/').next().unwrap_or(profile)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn document() -> CapabilityDocument {
        CapabilityDocument {
            protocol_version: PROTOCOL_VERSION.to_owned(),
            profiles: vec![PROFILE_CORE_V0.to_owned(), PROFILE_ADMIN_V0.to_owned()],
            features: BTreeMap::from([
                (FEATURE_NAMESPACES_CREATE.to_owned(), true),
                (FEATURE_NAMESPACES_DELETE.to_owned(), false),
            ]),
            limits: BTreeMap::new(),
        }
    }

    #[test]
    fn supports_and_has_profile_answer_gating_questions() {
        let document = document();
        assert!(document.has_profile(PROFILE_CORE_V0));
        assert!(!document.has_profile("query/v0"));
        assert!(document.supports(FEATURE_NAMESPACES_CREATE));
        // Advertised-false and absent keys are both unsupported.
        assert!(!document.supports(FEATURE_NAMESPACES_DELETE));
        assert!(!document.supports(FEATURE_NAMESPACES_FORK));
    }

    #[test]
    fn feature_keys_must_be_parented_by_an_advertised_profile() {
        let mut document = document();
        document
            .features
            .insert("query.index.fulltext".to_owned(), true);

        assert_eq!(
            document.validate(),
            Err(CapabilityDocumentError::UnparentedFeature {
                feature: "query.index.fulltext".to_owned(),
            })
        );

        document.retain_well_formed();
        assert!(document.validate().is_ok());
        assert!(!document.features.contains_key("query.index.fulltext"));
        assert!(document.features.contains_key(FEATURE_NAMESPACES_CREATE));
    }

    #[test]
    fn capability_document_round_trips_and_tolerates_unknown_fields() {
        let document = document();
        let encoded = serde_json::to_string(&document).expect("encode");
        let decoded: CapabilityDocument = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded, document);

        let future = encoded.replacen('{', "{\"field_from_the_future\":true,", 1);
        let decoded: CapabilityDocument =
            serde_json::from_str(&future).expect("unknown fields are ignored");
        assert_eq!(decoded, document);
    }
}
