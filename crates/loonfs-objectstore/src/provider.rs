//! Provider contract profiles: what each provider is expected to support.

/// Classifies whether a provider claim is assumed or must be established by live probes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expectation {
    /// Declares behavior the provider is expected to support without environment-specific doubt.
    ExpectedYes,
    /// Declares behavior intentionally absent from this adapter.
    ExpectedNo,
    /// Requires the live conformance suite to establish behavior for the configured account.
    VerifyByConformance,
}

/// Enumerates the storage behaviors exercised by the provider conformance suite.
///
/// See [provider conformance](../../../docs/specs/format.md#18-provider-conformance).
#[derive(Debug, Clone)]
pub struct ActiveContractProfile {
    /// Expectation for atomically creating an object only while its key is absent.
    pub create_if_absent: Expectation,
    /// Expectation for compare-and-swap on mutable control-sized objects.
    pub compare_and_swap_small_object: Expectation,
    /// Expectation that read metadata supplies an opaque token usable for compare-and-swap.
    pub opaque_compare_token_for_cas: Expectation,
    /// Expectation that full bytes and their compare token come from one observation.
    pub full_object_read_identity: Expectation,
    /// Expectation for unconditional replacement of existing bytes.
    pub overwrite: Expectation,
    /// Expectation that deleting an already-absent key succeeds.
    pub delete_idempotent: Expectation,
    /// Expectation that point metadata immediately reflects successful writes and deletes.
    pub head_reflects_latest_write_and_delete: Expectation,
    /// Expectation that listings immediately include successful writes.
    pub strong_list_after_write: Expectation,
    /// Expectation that listings immediately exclude successful deletes.
    pub strong_list_after_delete: Expectation,
    /// Expectation for normalized half-open byte-range reads.
    pub range_read: Expectation,
    /// Expectation that the adapter confines keys beneath a configured logical prefix.
    pub scoped_key_prefixing: Expectation,
    /// Expectation that traversal-shaped keys are rejected before provider IO.
    pub traversal_rejection: Expectation,
    /// Expectation that streamed prefix listings use ascending lexicographic key order.
    pub sorted_list_prefix: Expectation,
    /// Expectation that large overwrites use a provider-native multipart surface.
    pub multipart_upload: Expectation,
}

/// Names one provider and the contract expectations its conformance run applies.
#[derive(Debug, Clone)]
pub struct ProviderProfile {
    /// Stable provider label used by configuration and test diagnostics.
    pub name: &'static str,
    /// Required behaviors and provider-specific verification status.
    pub active_contract: ActiveContractProfile,
}

/// Describes the local-filesystem adapter's conformance expectations.
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
        multipart_upload: Expectation::ExpectedNo,
    },
};

/// Describes the AWS S3 adapter's conformance expectations.
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
        multipart_upload: Expectation::ExpectedYes,
    },
};

/// Describes the Cloudflare R2 adapter's conformance expectations.
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
        multipart_upload: Expectation::ExpectedYes,
    },
};

/// Describes the native Google Cloud Storage adapter's conformance expectations.
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
        multipart_upload: Expectation::ExpectedYes,
    },
};

/// Describes the native Azure Blob Storage adapter's conformance expectations.
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
        multipart_upload: Expectation::ExpectedYes,
    },
};
