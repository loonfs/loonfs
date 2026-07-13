//! Provider contract profiles: what each provider is expected to support
//! and what stays aspirational.

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
    pub full_object_read_identity: Expectation,
    pub overwrite: Expectation,
    pub delete_idempotent: Expectation,
    pub head_reflects_latest_write_and_delete: Expectation,
    pub strong_list_after_write: Expectation,
    pub strong_list_after_delete: Expectation,
    pub range_read: Expectation,
    pub scoped_key_prefixing: Expectation,
    pub traversal_rejection: Expectation,
    pub sorted_list_prefix: Expectation,
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
        full_object_read_identity: Expectation::VerifyByConformance,
        overwrite: Expectation::VerifyByConformance,
        delete_idempotent: Expectation::VerifyByConformance,
        head_reflects_latest_write_and_delete: Expectation::VerifyByConformance,
        strong_list_after_write: Expectation::VerifyByConformance,
        strong_list_after_delete: Expectation::VerifyByConformance,
        range_read: Expectation::VerifyByConformance,
        scoped_key_prefixing: Expectation::ExpectedNo,
        traversal_rejection: Expectation::VerifyByConformance,
        sorted_list_prefix: Expectation::VerifyByConformance,
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
        full_object_read_identity: Expectation::ExpectedYes,
        overwrite: Expectation::ExpectedYes,
        delete_idempotent: Expectation::ExpectedYes,
        head_reflects_latest_write_and_delete: Expectation::ExpectedYes,
        strong_list_after_write: Expectation::ExpectedYes,
        strong_list_after_delete: Expectation::ExpectedYes,
        range_read: Expectation::ExpectedYes,
        scoped_key_prefixing: Expectation::ExpectedYes,
        traversal_rejection: Expectation::ExpectedYes,
        sorted_list_prefix: Expectation::ExpectedYes,
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
        full_object_read_identity: Expectation::VerifyByConformance,
        overwrite: Expectation::VerifyByConformance,
        delete_idempotent: Expectation::VerifyByConformance,
        head_reflects_latest_write_and_delete: Expectation::VerifyByConformance,
        strong_list_after_write: Expectation::ExpectedYes,
        strong_list_after_delete: Expectation::ExpectedYes,
        range_read: Expectation::ExpectedYes,
        scoped_key_prefixing: Expectation::ExpectedYes,
        traversal_rejection: Expectation::ExpectedYes,
        sorted_list_prefix: Expectation::ExpectedYes,
    },
    future_capabilities: FutureCapabilityProfile {
        multipart_upload: Expectation::ExpectedYes,
    },
};

pub const GCP_GCS: ProviderProfile = ProviderProfile {
    name: "gcp-gcs",
    active_contract: ActiveContractProfile {
        create_if_absent: Expectation::VerifyByConformance,
        compare_and_swap_small_object: Expectation::VerifyByConformance,
        opaque_compare_token_for_cas: Expectation::VerifyByConformance,
        full_object_read_identity: Expectation::VerifyByConformance,
        overwrite: Expectation::VerifyByConformance,
        delete_idempotent: Expectation::VerifyByConformance,
        head_reflects_latest_write_and_delete: Expectation::VerifyByConformance,
        strong_list_after_write: Expectation::ExpectedYes,
        strong_list_after_delete: Expectation::ExpectedYes,
        range_read: Expectation::ExpectedYes,
        scoped_key_prefixing: Expectation::ExpectedYes,
        traversal_rejection: Expectation::ExpectedYes,
        sorted_list_prefix: Expectation::ExpectedYes,
    },
    future_capabilities: FutureCapabilityProfile {
        multipart_upload: Expectation::ExpectedYes,
    },
};

pub const AZURE_ABS: ProviderProfile = ProviderProfile {
    name: "azure-abs",
    active_contract: ActiveContractProfile {
        create_if_absent: Expectation::VerifyByConformance,
        compare_and_swap_small_object: Expectation::VerifyByConformance,
        opaque_compare_token_for_cas: Expectation::VerifyByConformance,
        full_object_read_identity: Expectation::VerifyByConformance,
        overwrite: Expectation::VerifyByConformance,
        delete_idempotent: Expectation::VerifyByConformance,
        head_reflects_latest_write_and_delete: Expectation::VerifyByConformance,
        strong_list_after_write: Expectation::ExpectedYes,
        strong_list_after_delete: Expectation::ExpectedYes,
        range_read: Expectation::ExpectedYes,
        scoped_key_prefixing: Expectation::ExpectedYes,
        traversal_rejection: Expectation::ExpectedYes,
        sorted_list_prefix: Expectation::ExpectedYes,
    },
    future_capabilities: FutureCapabilityProfile {
        multipart_upload: Expectation::ExpectedYes,
    },
};
