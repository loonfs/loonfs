//! Per-operation option types for the runtime handle surface, plus the
//! request-to-options resolutions the server and embedded hosts share.
//!
//! The options that also parameterize the HTTP client's identical operations
//! are defined once in [`loonfs_api::options`] and re-exported here, so the
//! two surfaces cannot drift a field apart. That is every path operation. What
//! stays defined below is runtime-only: maintenance, checkpoints, namespace
//! creation, and change-feed paging.
//!
//! Results are the `loonfs-api` wire shapes themselves: the handles return
//! `MaintenanceStepResponse` and `GcResponse` directly, the same way they
//! already return `CommitResponse` and `FlushWalResponse`.

use crate::{EffectiveLimit, GcConfig};
use loonfs_api::v0::{
    CreateCheckpointRequest, GcRequest, MaintenanceStepKind, MaintenanceStepRequest,
};
use loonfs_core::publish::WalTailPolicy;
use std::num::NonZeroU64;

pub use loonfs_api::options::{
    CopyOptions, CreateDirectoryOptions, DeleteOptions, MoveOptions, PutFileOptions,
    RestoreRevisionOptions, UndeleteOptions,
};

/// Options for one maintenance step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceStepOptions {
    /// Flush the visible WAL tail into metadata tables when it reaches this many
    /// segments.
    pub max_wal_tail_segments: u64,
    /// Advance the retention floor to the flushed manifest head as part of
    /// the step. Nothing surrenders replay history unless this is set or
    /// the step is restricted to `only: Retention`.
    pub retention: bool,
    /// Run the mark-and-sweep garbage collector after the step's flush work.
    /// Nothing sweeps unless this is set; an absent `max_objects` inside the
    /// config resolves to the per-step default.
    pub gc: Option<GcConfig>,
    /// Restrict the step to one sub-step. Absent runs all of them.
    pub only: Option<MaintenanceStepKind>,
}

impl Default for MaintenanceStepOptions {
    fn default() -> Self {
        Self {
            max_wal_tail_segments: WalTailPolicy::DEFAULT.checkpoint_at_segments,
            retention: false,
            gc: None,
            only: None,
        }
    }
}

impl MaintenanceStepOptions {
    /// Resolves wire-level step overrides onto the runtime defaults.
    pub fn from_request(request: MaintenanceStepRequest) -> Self {
        let defaults = Self::default();
        Self {
            max_wal_tail_segments: request
                .max_wal_tail_segments
                .unwrap_or(defaults.max_wal_tail_segments),
            retention: request.retention.unwrap_or(defaults.retention),
            gc: request.gc.map(|request| {
                let mut config = gc_config_from_request(request);
                if config.max_objects.is_none() {
                    config.max_objects = Some(loonfs_core::limits::DEFAULT_GC_MAX_OBJECTS);
                }
                config
            }),
            only: request.only,
        }
    }
}

/// Resolves wire-level GC window overrides onto the conservative defaults.
///
/// [`GcRequest`] carries optional overrides; [`GcConfig`] carries the values
/// the pass actually runs with, so the two are deliberately distinct shapes.
pub fn gc_config_from_request(request: GcRequest) -> GcConfig {
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
}

impl Default for ReadFileStreamOptions {
    fn default() -> Self {
        Self {
            chunk_bytes: NonZeroU64::new(loonfs_core::CONTENT_READ_CHUNK_BYTES)
                .expect("the default read chunk size is non-zero"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_gc_stays_unbounded_while_step_gc_gets_the_default_budget() {
        let explicit = gc_config_from_request(GcRequest::default());
        assert_eq!(explicit.max_objects, None);
        assert_eq!(explicit.cursor, None);

        let step = MaintenanceStepOptions::from_request(MaintenanceStepRequest {
            max_wal_tail_segments: None,
            retention: None,
            gc: Some(GcRequest::default()),
            only: None,
        });
        assert_eq!(
            step.gc.expect("GC opted in").max_objects,
            Some(loonfs_core::limits::DEFAULT_GC_MAX_OBJECTS)
        );
    }

    #[test]
    fn retention_stays_off_unless_the_request_opts_in() {
        let defaults = MaintenanceStepOptions::default();
        assert!(!defaults.retention);

        let absent = MaintenanceStepOptions::from_request(MaintenanceStepRequest::default());
        assert!(!absent.retention);

        let opted_in = MaintenanceStepOptions::from_request(MaintenanceStepRequest {
            retention: Some(true),
            ..MaintenanceStepRequest::default()
        });
        assert!(opted_in.retention);
    }

    #[test]
    fn step_gc_preserves_explicit_budget_and_cursor() {
        let step = MaintenanceStepOptions::from_request(MaintenanceStepRequest {
            max_wal_tail_segments: None,
            retention: None,
            gc: Some(GcRequest {
                max_objects: Some(7),
                cursor: Some("opaque".to_owned()),
                ..GcRequest::default()
            }),
            only: None,
        });
        let gc = step.gc.expect("GC opted in");
        assert_eq!(gc.max_objects, Some(7));
        assert_eq!(gc.cursor.as_deref(), Some("opaque"));
    }
}
