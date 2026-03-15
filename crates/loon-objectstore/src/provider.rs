use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Expectation {
    ExpectedYes,
    ExpectedNo,
    VerifyByConformance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveContractProfile {
    pub create_if_absent: Expectation,
    pub compare_and_swap_small_object: Expectation,
    pub opaque_compare_token_for_cas: Expectation,
    pub strong_list_after_write: Expectation,
    pub strong_list_after_delete: Expectation,
    pub range_read: Expectation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FutureCapabilityProfile {
    pub multipart_upload: Expectation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderProfile {
    pub name: &'static str,
    pub active_contract: ActiveContractProfile,
    pub future_capabilities: FutureCapabilityProfile,
}

pub const LOCAL_FS: ProviderProfile = ProviderProfile {
    name: "local-fs",
    active_contract: ActiveContractProfile {
        create_if_absent: Expectation::VerifyByConformance,
        compare_and_swap_small_object: Expectation::VerifyByConformance,
        opaque_compare_token_for_cas: Expectation::VerifyByConformance,
        strong_list_after_write: Expectation::VerifyByConformance,
        strong_list_after_delete: Expectation::VerifyByConformance,
        range_read: Expectation::VerifyByConformance,
    },
    future_capabilities: FutureCapabilityProfile {
        multipart_upload: Expectation::ExpectedNo,
    },
};

pub const AWS_S3: ProviderProfile = ProviderProfile {
    name: "aws-s3",
    active_contract: ActiveContractProfile {
        create_if_absent: Expectation::ExpectedYes,
        compare_and_swap_small_object: Expectation::ExpectedYes,
        opaque_compare_token_for_cas: Expectation::ExpectedYes,
        strong_list_after_write: Expectation::ExpectedYes,
        strong_list_after_delete: Expectation::ExpectedYes,
        range_read: Expectation::ExpectedYes,
    },
    future_capabilities: FutureCapabilityProfile {
        multipart_upload: Expectation::ExpectedYes,
    },
};

pub const CLOUDFLARE_R2: ProviderProfile = ProviderProfile {
    name: "cloudflare-r2",
    active_contract: ActiveContractProfile {
        create_if_absent: Expectation::VerifyByConformance,
        compare_and_swap_small_object: Expectation::VerifyByConformance,
        opaque_compare_token_for_cas: Expectation::VerifyByConformance,
        strong_list_after_write: Expectation::ExpectedYes,
        strong_list_after_delete: Expectation::ExpectedYes,
        range_read: Expectation::ExpectedYes,
    },
    future_capabilities: FutureCapabilityProfile {
        multipart_upload: Expectation::ExpectedYes,
    },
};
