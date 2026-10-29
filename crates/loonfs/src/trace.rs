//! The spans of public handle operations, and the low-cardinality labels
//! attached to runtime trace spans and metrics.

use futures::TryFutureExt;
use loonfs_objectstore::ConfiguredObjectStoreKind;
use tracing::Instrument;

/// Runtime deployment mode label used in trace spans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
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

/// Returns the object-store backend label used in trace spans and metrics,
/// or `unknown` when the caller didn't identify the backend.
pub(crate) fn store_kind_label(kind: Option<ConfiguredObjectStoreKind>) -> &'static str {
    kind.map_or("unknown", ConfiguredObjectStoreKind::as_str)
}

/// Opens the `loonfs.<operation>` debug span of a public handle operation,
/// with the runtime's mode and store kind, the namespace when one is given,
/// and the fields that follow.
macro_rules! operation_span {
    ($core:expr, $operation:literal) => {
        tracing::debug_span!(
            concat!("loonfs.", $operation),
            operation = $operation,
            mode = $core.trace_mode(),
            store_kind = $core.trace_store_kind(),
        )
    };
    ($core:expr, $operation:literal, $namespace_id:expr $(, $($field:tt)+)?) => {
        tracing::debug_span!(
            concat!("loonfs.", $operation),
            operation = $operation,
            namespace_id = %$namespace_id,
            mode = $core.trace_mode(),
            store_kind = $core.trace_store_kind(),
            $($($field)+)?
        )
    };
}

pub(crate) use operation_span;

/// Runs `operation` in `span`, the span of a public handle operation, and
/// records the error of a failed operation as a debug event in the span.
pub(crate) fn in_operation<T>(
    span: tracing::Span,
    operation: impl std::future::Future<Output = crate::Result<T>>,
) -> impl std::future::Future<Output = crate::Result<T>> {
    operation
        .inspect_err(|error| tracing::debug!(error = %error))
        .instrument(span)
}

/// Runs `operation` in `span`, as [`in_operation`] does, for the one
/// synchronous handle operation.
pub(crate) fn in_operation_sync<T>(
    span: tracing::Span,
    operation: impl FnOnce() -> crate::Result<T>,
) -> crate::Result<T> {
    let _entered = span.enter();
    let result = operation();
    if let Err(error) = &result {
        tracing::debug!(error = %error);
    }
    result
}

/// Opens a `loonfs.phase` debug span for `$phase`, a
/// [`Phase`](loonfs_core::Phase), with the namespace, the runtime's mode and
/// store kind, and the fields that follow.
macro_rules! phase_span {
    ($core:expr, $phase:expr, $namespace_id:expr $(, $($field:tt)+)?) => {
        loonfs_core::phase_span!(
            $phase,
            namespace_id = %$namespace_id,
            mode = $core.trace_mode(),
            store_kind = $core.trace_store_kind(),
            $($($field)+)?
        )
    };
}

pub(crate) use phase_span;

/// Records an event of `$phase`, a [`Phase`](loonfs_core::Phase), at
/// `$level`, with the fields of [`phase_span!`].
macro_rules! phase_event {
    ($core:expr, $phase:expr, $namespace_id:expr, $level:expr $(, $($field:tt)+)?) => {
        tracing::event!(
            $level,
            phase = loonfs_core::Phase::as_str($phase),
            namespace_id = %$namespace_id,
            mode = $core.trace_mode(),
            store_kind = $core.trace_store_kind(),
            $($($field)+)?
        )
    };
}

pub(crate) use phase_event;

/// Classifies a payload size into the low-cardinality `small`, `medium`, or
/// `large` trace label.
pub(crate) fn payload_class(size_bytes: u64) -> &'static str {
    match size_bytes {
        0..=16_383 => "small",
        16_384..=1_048_575 => "medium",
        _ => "large",
    }
}

#[cfg(test)]
mod tests {
    use super::{payload_class, store_kind_label, TraceMode};
    use loonfs_objectstore::ConfiguredObjectStoreKind;

    #[test]
    fn trace_labels_are_low_cardinality() {
        assert_eq!(TraceMode::Embedded.as_str(), "embedded");
        assert_eq!(TraceMode::Remote.as_str(), "remote");
        assert_eq!(
            store_kind_label(Some(ConfiguredObjectStoreKind::LocalFs)),
            "local_fs"
        );
        assert_eq!(
            store_kind_label(Some(ConfiguredObjectStoreKind::AwsS3)),
            "aws_s3"
        );
        assert_eq!(
            store_kind_label(Some(ConfiguredObjectStoreKind::CloudflareR2)),
            "cloudflare_r2"
        );
        assert_eq!(
            store_kind_label(Some(ConfiguredObjectStoreKind::GcpGcs)),
            "gcp_gcs"
        );
        assert_eq!(store_kind_label(None), "unknown");
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
