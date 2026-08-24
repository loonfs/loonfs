//! Runtime options and request-to-options conversions.
//!
//! Options shared with the HTTP client are re-exported from
//! [`loonfs_api::options`]. Runtime-only options remain in this module.
//!
//! Results are the `loonfs-api` wire shapes themselves: the handles return
//! `MaintenanceStepResponse` and `GcResponse` directly, the same way they
//! already return `CommitResponse` and `FlushWalResponse`.

use crate::{EffectiveLimit, GcConfig, Result, RuntimeError};
use loonfs_api::{
    CreateCheckpointRequest, GcRequest, MaintenanceStepRequest, MetadataMaintenanceRequest,
};
use loonfs_core::publish::WalTailPolicy;
use std::num::NonZeroU64;

pub use loonfs_api::options::{
    CommitOptions, CopyOptions, CreateDirectoryOptions, DeleteOptions,
    DirectMultipartUploadOptions, ListPathEntriesOptions, MoveOptions, PutFileOptions,
    RestoreRevisionOptions, StatPathOptions, UndeleteOptions, UpdateAttributesOptions,
};

/// The actions one maintenance step performs.
///
/// Selection is presence: a step runs exactly the actions this plan
/// carries, in the fixed order metadata, retention, collection. Nothing
/// surrenders replay history or sweeps objects without being named here,
/// and a plan that names nothing is not a step.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[must_use]
pub struct MaintenancePlan {
    /// Fold the visible WAL tail and merge one bounded reorganization unit.
    pub metadata: Option<MetadataMaintenanceOptions>,
    /// Advance the retention floor to the flushed manifest head.
    pub advance_retention: bool,
    /// Run one bounded mark-and-sweep pass. An absent `max_objects` inside
    /// the config resolves to the per-step default.
    pub gc: Option<GcConfig>,
}

/// Overrides for the metadata-upkeep action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataMaintenanceOptions {
    /// Flush the visible WAL tail once it reaches this many segments.
    pub max_wal_tail_segments: NonZeroU64,
}

impl Default for MetadataMaintenanceOptions {
    fn default() -> Self {
        Self {
            max_wal_tail_segments: const {
                NonZeroU64::new(WalTailPolicy::DEFAULT.checkpoint_at_segments).unwrap()
            },
        }
    }
}

impl MaintenancePlan {
    /// The plan for one metadata-upkeep pass at the default threshold.
    pub fn metadata() -> Self {
        Self {
            metadata: Some(MetadataMaintenanceOptions::default()),
            ..Self::default()
        }
    }

    /// True when the plan selects no action at all.
    pub fn is_empty(&self) -> bool {
        self.metadata.is_none() && !self.advance_retention && self.gc.is_none()
    }

    /// Resolves a wire-level step request into the plan it selects.
    ///
    /// A request that selects nothing resolves to an empty plan, which
    /// [`FsAdmin::run_maintenance`](crate::FsAdmin::run_maintenance)
    /// then refuses — one gate, wherever the plan came from.
    pub fn from_request(request: MaintenanceStepRequest) -> Result<Self> {
        Ok(Self {
            metadata: request
                .metadata_maintenance
                .map(metadata_options_from_request)
                .transpose()?,
            advance_retention: request.retention.is_some(),
            gc: request.gc.map(|request| {
                let mut config = gc_config_from_request(request);
                if config.max_objects.is_none() {
                    config.max_objects = Some(loonfs_core::limits::DEFAULT_GC_MAX_OBJECTS);
                }
                config
            }),
        })
    }
}

/// Resolves the wire-level flush threshold, rejecting the two values that
/// would make the action useless.
fn metadata_options_from_request(
    request: MetadataMaintenanceRequest,
) -> Result<MetadataMaintenanceOptions> {
    let Some(threshold) = request.max_wal_tail_segments else {
        return Ok(MetadataMaintenanceOptions::default());
    };
    let Some(max_wal_tail_segments) = NonZeroU64::new(threshold) else {
        return Err(RuntimeError::Config(
            "max_wal_tail_segments must be greater than zero".to_owned(),
        ));
    };
    // A threshold above the write-rejection cap would make the step answer
    // `not needed` while every publish is being rejected — the one tool that
    // relieves backpressure refusing to act.
    let reject_writes_at_segments = WalTailPolicy::DEFAULT.reject_writes_at_segments;
    if max_wal_tail_segments.get() > reject_writes_at_segments {
        return Err(RuntimeError::Config(format!(
            "max_wal_tail_segments may not exceed the write-rejection threshold \
             ({reject_writes_at_segments})"
        )));
    }
    Ok(MetadataMaintenanceOptions {
        max_wal_tail_segments,
    })
}

/// Resolves wire-level GC window overrides onto the conservative defaults.
///
/// [`GcRequest`] carries optional overrides; [`GcConfig`] carries the values
/// the pass actually runs with, so the two are deliberately distinct shapes.
pub(crate) fn gc_config_from_request(request: GcRequest) -> GcConfig {
    let defaults = GcConfig::default();
    GcConfig {
        grace_window_ms: request.grace_window_ms.unwrap_or(defaults.grace_window_ms),
        max_objects: request.max_objects,
        cursor: request.cursor,
    }
}

/// Options for creating a durable checkpoint pin.
///
/// The name is a label recorded on the record, not a key. No `Default`:
/// a checkpoint always names its owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateCheckpointOptions {
    /// Label recorded on the checkpoint record.
    pub name: String,
    /// Optional lifetime; the record's expiry is computed from the runtime's
    /// clock. Absent means the pin holds until explicitly released.
    pub ttl_ms: Option<u64>,
}

impl CreateCheckpointOptions {
    /// Resolves the wire-level create request onto runtime options.
    pub fn from_request(request: CreateCheckpointRequest) -> Self {
        Self {
            name: request.name,
            ttl_ms: request.ttl_ms,
        }
    }
}

/// Options for creating a namespace; feeds core's
/// [`loonfs_core::BootstrapOptions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CreateNamespaceOptions {
    /// If true, creating an already-existing namespace is treated as success.
    pub allow_existing: bool,
}

/// Options for reading the change feed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListChangesOptions {
    /// Page limit; `None` resolves the default pagination policy.
    pub limit: Option<EffectiveLimit>,
}

/// Options for a streaming file read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadFileStreamOptions {
    /// Bytes one ranged read fetches, which is the most of the file the read
    /// holds at once. Defaults to
    /// [`CONTENT_READ_CHUNK_BYTES`](loonfs_core::CONTENT_READ_CHUNK_BYTES);
    /// a caller with a tighter memory budget than that says so here, the way
    /// a caller of [`FsReader::read_content_ref`](crate::FsReader::read_content_ref)
    /// declares its own. Non-zero by type, so there is no chunk size that
    /// makes no progress.
    pub chunk_bytes: NonZeroU64,
    /// Where the read starts, for a caller that already holds the bytes
    /// below it — an interrupted download picking up where it stopped.
    ///
    /// The read still reports on the whole object, so a nonzero offset
    /// obliges the caller to hand over what it holds through
    /// [`FileContentStream::fold_resumed_prefix`](loonfs_core::FileContentStream::fold_resumed_prefix)
    /// before driving the stream. Zero reads from the first byte and asks
    /// nothing of the caller.
    pub start_offset: u64,
}

impl Default for ReadFileStreamOptions {
    fn default() -> Self {
        Self {
            chunk_bytes: const { NonZeroU64::new(loonfs_core::CONTENT_READ_CHUNK_BYTES).unwrap() },
            start_offset: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::AdvanceRetentionRequest;

    fn plan(request: MaintenanceStepRequest) -> MaintenancePlan {
        MaintenancePlan::from_request(request).expect("the request selects an action")
    }

    #[test]
    fn explicit_gc_stays_unbounded_while_step_gc_gets_the_default_budget() {
        let explicit = gc_config_from_request(GcRequest::default());
        assert_eq!(explicit.max_objects, None);
        assert_eq!(explicit.cursor, None);

        let step = plan(MaintenanceStepRequest {
            gc: Some(GcRequest::default()),
            ..MaintenanceStepRequest::default()
        });
        assert_eq!(
            step.gc.expect("GC opted in").max_objects,
            Some(loonfs_core::limits::DEFAULT_GC_MAX_OBJECTS)
        );
    }

    /// Selection is presence, so a body that names nothing resolves to a
    /// plan the step refuses rather than one that quietly does nothing.
    #[test]
    fn a_request_that_selects_nothing_is_an_empty_plan() {
        assert!(plan(MaintenanceStepRequest::default()).is_empty());
        assert!(!MaintenancePlan::metadata().is_empty());
    }

    #[test]
    fn each_action_is_selected_on_its_own() {
        assert!(plan(MaintenanceStepRequest {
            metadata_maintenance: Some(MetadataMaintenanceRequest::default()),
            ..MaintenanceStepRequest::default()
        })
        .metadata
        .is_some());

        let retention = plan(MaintenanceStepRequest {
            retention: Some(AdvanceRetentionRequest::default()),
            ..MaintenanceStepRequest::default()
        });
        assert!(retention.advance_retention);
        assert!(retention.metadata.is_none() && retention.gc.is_none());
    }

    #[test]
    fn an_absent_threshold_resolves_to_the_default() {
        let step = plan(MaintenanceStepRequest {
            metadata_maintenance: Some(MetadataMaintenanceRequest::default()),
            ..MaintenanceStepRequest::default()
        });
        assert_eq!(
            step.metadata.expect("metadata selected"),
            MetadataMaintenanceOptions::default()
        );
    }

    #[test]
    fn a_useless_flush_threshold_is_rejected() {
        for threshold in [0, WalTailPolicy::DEFAULT.reject_writes_at_segments + 1] {
            let error = MaintenancePlan::from_request(MaintenanceStepRequest {
                metadata_maintenance: Some(MetadataMaintenanceRequest {
                    max_wal_tail_segments: Some(threshold),
                }),
                ..MaintenanceStepRequest::default()
            })
            .expect_err("the threshold is out of range");
            assert_eq!(error.code(), crate::ErrorCode::InvalidRequest);
        }
    }

    #[test]
    fn step_gc_preserves_explicit_budget_and_cursor() {
        let step = plan(MaintenanceStepRequest {
            gc: Some(GcRequest {
                max_objects: Some(7),
                cursor: Some("opaque".to_owned()),
                ..GcRequest::default()
            }),
            ..MaintenanceStepRequest::default()
        });
        let gc = step.gc.expect("GC opted in");
        assert_eq!(gc.max_objects, Some(7));
        assert_eq!(gc.cursor.as_deref(), Some("opaque"));
    }
}
