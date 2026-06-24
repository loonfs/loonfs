use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

/// The protocol generation this build speaks.
pub const PROTOCOL_VERSION: &str = "v0";

/// The mandatory data plane.
pub const PROFILE_CORE_V0: &str = "core/v0";
/// The optional maintenance plane.
pub const PROFILE_ADMIN_V0: &str = "admin/v0";

/// Gates namespace enumeration.
pub const FEATURE_NAMESPACES_LIST: &str = "core.namespaces.list";
/// Gates namespace creation.
pub const FEATURE_NAMESPACES_CREATE: &str = "core.namespaces.create";
/// Gates namespace forking.
pub const FEATURE_NAMESPACES_FORK: &str = "core.namespaces.fork";
/// Gates namespace deletion.
pub const FEATURE_NAMESPACES_DELETE: &str = "core.namespaces.delete";
/// Gates direct upload sessions that are authorized with short-lived presigned URLs.
pub const FEATURE_UPLOADS_DIRECT_PUT: &str = "core.uploads.direct_put";

/// A deployment's self-description (API spec, "Capability discovery").
///
/// A remote client fetches this from `GET /v0/config` and caches it; an
/// embedded engine exposes the same document as a constant. SDK gating logic
/// is therefore identical for both backends: check [`supports`] or
/// [`has_profile`], and treat a `not_supported` error as authoritative when
/// the two disagree.
///
/// [`supports`]: CapabilityDocument::supports
/// [`has_profile`]: CapabilityDocument::has_profile
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CapabilityDocument {
    /// The protocol generation, currently `v0`.
    pub protocol_version: String,
    /// Advertised profiles, each `plane/version`. All-or-nothing: every
    /// required op of an advertised profile is implemented.
    pub profiles: Vec<String>,
    /// Named features and whether this deployment supports them. An absent
    /// key means unsupported.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub features: BTreeMap<String, bool>,
    /// Advisory numeric limits clients may use to pre-validate requests.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub limits: BTreeMap<String, u64>,
}

/// Violation of the capability document rules.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CapabilityDocumentError {
    #[error(
        "feature `{feature}` is not parented by an advertised profile \
         (its first dotted segment must be one of the advertised plane names)"
    )]
    UnparentedFeature { feature: String },
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
            .retain(|feature, _| match feature.split('.').next() {
                Some(plane) if !plane.is_empty() => advertised_planes.contains(&plane),
                _ => false,
            });
    }

    fn feature_is_parented(&self, feature: &str) -> bool {
        match feature.split('.').next() {
            Some(plane) if !plane.is_empty() => self
                .profiles
                .iter()
                .any(|profile| plane_name(profile) == plane),
            _ => false,
        }
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
                (FEATURE_NAMESPACES_LIST.to_owned(), true),
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
        assert!(document.supports(FEATURE_NAMESPACES_LIST));
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
        assert!(document.features.contains_key(FEATURE_NAMESPACES_LIST));
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
