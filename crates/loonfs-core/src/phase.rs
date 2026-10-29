//! [`Phase`]: the internal stages that `loonfs.phase` spans record, and
//! [`phase_span!`](crate::phase_span), which opens one.

/// An internal stage of an operation, which a `loonfs.phase` span records in
/// its `phase` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    ApplyCommittedWalRecord,
    BatchCollect,
    BatchPublish,
    BuildWalPayload,
    ChildAccess,
    Enqueue,
    ListPageBuildEntries,
    ListPageSelectChildren,
    LoadManifestSegments,
    LoadNamespaceManifest,
    LoadView,
    MetadataCompaction,
    MetadataCompactionStep,
    PinRead,
    PositionManifestProbe,
    PrepareBatch,
    PrepareCommit,
    ProjectMetadataState,
    PublishManifest,
    ScanNamespaceState,
    Serialize,
    UpdateCache,
    ValidationAnchorLoad,
    ValidationManifestProbe,
    ValidationRefresh,
    ValidationWalProbe,
    WaitForBatch,
    WaitForResult,
    WalFold,
    WalkPath,
    WriteContentBlob,
    WriteManifestSegments,
}

impl Phase {
    /// Returns the value of the span's `phase` field.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ApplyCommittedWalRecord => "apply_committed_wal_record",
            Self::BatchCollect => "batch_collect",
            Self::BatchPublish => "batch_publish",
            Self::BuildWalPayload => "build_wal_payload",
            Self::ChildAccess => "child_access",
            Self::Enqueue => "enqueue",
            Self::ListPageBuildEntries => "list_page_build_entries",
            Self::ListPageSelectChildren => "list_page_select_children",
            Self::LoadManifestSegments => "load_manifest_segments",
            Self::LoadNamespaceManifest => "load_namespace_manifest",
            Self::LoadView => "load_view",
            Self::MetadataCompaction => "metadata_compaction",
            Self::MetadataCompactionStep => "metadata_compaction_step",
            Self::PinRead => "pin_read",
            Self::PositionManifestProbe => "position_manifest_probe",
            Self::PrepareBatch => "prepare_batch",
            Self::PrepareCommit => "prepare_commit",
            Self::ProjectMetadataState => "project_metadata_state",
            Self::PublishManifest => "publish_manifest",
            Self::ScanNamespaceState => "scan_namespace_state",
            Self::Serialize => "serialize",
            Self::UpdateCache => "update_cache",
            Self::ValidationAnchorLoad => "validation_anchor_load",
            Self::ValidationManifestProbe => "validation_manifest_probe",
            Self::ValidationRefresh => "validation_refresh",
            Self::ValidationWalProbe => "validation_wal_probe",
            Self::WaitForBatch => "wait_for_batch",
            Self::WaitForResult => "wait_for_result",
            Self::WalFold => "wal_fold",
            Self::WalkPath => "walk_path",
            Self::WriteContentBlob => "write_content_blob",
            Self::WriteManifestSegments => "write_manifest_segments",
        }
    }
}

/// Opens a `loonfs.phase` debug span for `$phase`, a [`Phase`], with the
/// fields that follow it.
#[macro_export]
macro_rules! phase_span {
    ($phase:expr $(, $($field:tt)+)?) => {
        tracing::debug_span!(
            "loonfs.phase",
            phase = $crate::Phase::as_str($phase),
            $($($field)+)?
        )
    };
}
