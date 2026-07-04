//! Low-cardinality labels attached to runtime trace spans and metrics.

use loonfs_objectstore::ConfiguredObjectStoreKind;

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
    Abs,
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
            Self::Abs => "abs",
            Self::Unknown => "unknown",
        }
    }
}

impl From<ConfiguredObjectStoreKind> for TraceStoreKind {
    /// Derives the trace label from a configured object-store kind, so
    /// callers wiring a [`ConfiguredObjectStoreKind`]-reporting store into the
    /// runtime never hand-maintain the mapping.
    fn from(kind: ConfiguredObjectStoreKind) -> Self {
        match kind {
            ConfiguredObjectStoreKind::LocalFs => Self::LocalFs,
            ConfiguredObjectStoreKind::AwsS3 => Self::S3,
            ConfiguredObjectStoreKind::CloudflareR2 => Self::R2,
            ConfiguredObjectStoreKind::GcpGcs => Self::Gcs,
            ConfiguredObjectStoreKind::AzureAbs => Self::Abs,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CachePath {
    MaterializedTables,
}

impl CachePath {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
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
    use loonfs_objectstore::ConfiguredObjectStoreKind;

    #[test]
    fn trace_store_kind_derives_from_configured_kind() {
        let cases = [
            (ConfiguredObjectStoreKind::LocalFs, TraceStoreKind::LocalFs),
            (ConfiguredObjectStoreKind::AwsS3, TraceStoreKind::S3),
            (ConfiguredObjectStoreKind::CloudflareR2, TraceStoreKind::R2),
            (ConfiguredObjectStoreKind::GcpGcs, TraceStoreKind::Gcs),
            (ConfiguredObjectStoreKind::AzureAbs, TraceStoreKind::Abs),
        ];
        for (kind, expected) in cases {
            assert_eq!(TraceStoreKind::from(kind), expected);
        }
    }

    #[test]
    fn trace_labels_are_low_cardinality() {
        assert_eq!(TraceMode::Embedded.as_str(), "embedded");
        assert_eq!(TraceMode::Remote.as_str(), "remote");
        assert_eq!(TraceStoreKind::LocalFs.as_str(), "local_fs");
        assert_eq!(TraceStoreKind::S3.as_str(), "s3");
        assert_eq!(TraceStoreKind::R2.as_str(), "r2");
        assert_eq!(TraceStoreKind::Gcs.as_str(), "gcs");
        assert_eq!(TraceStoreKind::Abs.as_str(), "abs");
        assert_eq!(TraceStoreKind::Unknown.as_str(), "unknown");
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
