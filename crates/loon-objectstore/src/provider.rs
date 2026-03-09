use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Expectation {
    ExpectedYes,
    ExpectedNo,
    VerifyByConformance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderProfile {
    pub name: &'static str,
    pub create_if_absent: Expectation,
    pub compare_and_swap_small_object: Expectation,
    pub strong_list_after_write: Expectation,
    pub strong_list_after_delete: Expectation,
    pub range_read: Expectation,
    pub multipart_upload: Expectation,
    pub provider_etag_is_canonical_identity: Expectation,
}

pub const LOCAL_FS: ProviderProfile = ProviderProfile {
    name: "local-fs",
    create_if_absent: Expectation::VerifyByConformance,
    compare_and_swap_small_object: Expectation::VerifyByConformance,
    strong_list_after_write: Expectation::VerifyByConformance,
    strong_list_after_delete: Expectation::VerifyByConformance,
    range_read: Expectation::VerifyByConformance,
    multipart_upload: Expectation::ExpectedNo,
    provider_etag_is_canonical_identity: Expectation::ExpectedNo,
};

pub const AWS_S3: ProviderProfile = ProviderProfile {
    name: "aws-s3",
    create_if_absent: Expectation::ExpectedYes,
    compare_and_swap_small_object: Expectation::ExpectedYes,
    strong_list_after_write: Expectation::ExpectedYes,
    strong_list_after_delete: Expectation::ExpectedYes,
    range_read: Expectation::ExpectedYes,
    multipart_upload: Expectation::ExpectedYes,
    provider_etag_is_canonical_identity: Expectation::ExpectedNo,
};

pub const CLOUDFLARE_R2: ProviderProfile = ProviderProfile {
    name: "cloudflare-r2",
    create_if_absent: Expectation::VerifyByConformance,
    compare_and_swap_small_object: Expectation::VerifyByConformance,
    strong_list_after_write: Expectation::ExpectedYes,
    strong_list_after_delete: Expectation::ExpectedYes,
    range_read: Expectation::ExpectedYes,
    multipart_upload: Expectation::ExpectedYes,
    provider_etag_is_canonical_identity: Expectation::ExpectedNo,
};
