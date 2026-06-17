//! Low-cardinality labels attached to runtime trace spans and metrics.

/// Runtime deployment mode label used in trace spans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceMode {
    /// The runtime runs in-process with its caller.
    Embedded,
    /// The runtime is reached through a remote server.
    Remote,
}

impl TraceMode {
    /// Returns the stable low-cardinality span label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Embedded => "embedded",
            Self::Remote => "remote",
        }
    }
}

/// Object-store backend label used in trace spans and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceStoreKind {
    /// Local-filesystem object store.
    LocalFs,
    /// Amazon S3.
    S3,
    /// Cloudflare R2.
    R2,
    /// Google Cloud Storage.
    Gcs,
    /// Azure Blob Storage.
    AzureAbs,
    /// Backend not identified by the caller.
    Unknown,
}

impl TraceStoreKind {
    /// Returns the stable low-cardinality span label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocalFs => "local_fs",
            Self::S3 => "s3",
            Self::R2 => "r2",
            Self::Gcs => "gcs",
            Self::AzureAbs => "azure_abs",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CachePath {
    WarmReuse,
    EtagProbe,
    ColdReconstruct,
    MaterializedTables,
}

impl CachePath {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::WarmReuse => "warm_reuse",
            Self::EtagProbe => "etag_probe",
            Self::ColdReconstruct => "cold_reconstruct",
            Self::MaterializedTables => "materialized_tables",
        }
    }
}

/// Classifies a payload size into the low-cardinality `small`, `medium`, or
/// `large` trace label.
pub fn payload_class(size_bytes: usize) -> &'static str {
    match size_bytes {
        0..=16_383 => "small",
        16_384..=1_048_575 => "medium",
        _ => "large",
    }
}

#[cfg(test)]
mod tests {
    use super::{payload_class, CachePath, TraceMode, TraceStoreKind};

    #[test]
    fn trace_labels_are_low_cardinality() {
        assert_eq!(TraceMode::Embedded.as_str(), "embedded");
        assert_eq!(TraceMode::Remote.as_str(), "remote");
        assert_eq!(TraceStoreKind::LocalFs.as_str(), "local_fs");
        assert_eq!(TraceStoreKind::S3.as_str(), "s3");
        assert_eq!(TraceStoreKind::R2.as_str(), "r2");
        assert_eq!(TraceStoreKind::Gcs.as_str(), "gcs");
        assert_eq!(TraceStoreKind::AzureAbs.as_str(), "azure_abs");
        assert_eq!(TraceStoreKind::Unknown.as_str(), "unknown");
        assert_eq!(CachePath::WarmReuse.as_str(), "warm_reuse");
        assert_eq!(CachePath::EtagProbe.as_str(), "etag_probe");
        assert_eq!(CachePath::ColdReconstruct.as_str(), "cold_reconstruct");
        assert_eq!(
            CachePath::MaterializedTables.as_str(),
            "materialized_tables"
        );
    }

    #[test]
    fn trace_helpers_classify_payloads_and_results() {
        assert_eq!(payload_class(0), "small");
        assert_eq!(payload_class(16_383), "small");
        assert_eq!(payload_class(16_384), "medium");
        assert_eq!(payload_class(1_048_575), "medium");
        assert_eq!(payload_class(1_048_576), "large");
    }
}
