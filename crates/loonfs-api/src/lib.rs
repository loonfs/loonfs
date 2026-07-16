//! Wire types and durable-format codecs for LoonFS.
//!
//! Everything that crosses a process or storage boundary is defined here:
//! validated identifier and path types at the crate root, the versioned HTTP
//! protocol shapes in [`v0`], and the durable storage formats in [`wire`]
//! (WAL segments, metadata SSTs, namespace manifests, and control objects).
//! Other LoonFS crates depend on this one for vocabulary; it depends on none
//! of them.
//!
//! Module rule: v0 HTTP shapes live in [`v0`]; the crate root keeps the
//! ids/paths/errors/wire-format modules and re-exports the common v0
//! surface as a curated explicit list below.

mod capability;
mod content;
mod control;
mod digest;
mod envelope;
mod error;
mod ids;
mod index_grams;
mod manifest;
mod name_policy;
mod pagination;
mod path;
mod sst_blocks;
pub mod v0;
mod wal;

pub mod wire {
    pub mod index_grams {
        pub use crate::index_grams::*;
    }

    pub mod manifest {
        pub use crate::manifest::*;
    }

    pub mod control {
        pub use crate::control::*;
    }

    pub mod sst_blocks {
        pub use crate::sst_blocks::*;
    }

    pub mod wal {
        pub use crate::wal::*;
    }
}

pub use capability::{
    CapabilityDocument, CapabilityDocumentError, FEATURE_NAMESPACES_CREATE,
    FEATURE_NAMESPACES_DELETE, FEATURE_NAMESPACES_FORK, FEATURE_QUERY_GREP,
    FEATURE_UPLOADS_DIRECT_PUT, LIMIT_DOWNLOAD_MAX_CONTENT_BYTES, LIMIT_QUERY_GREP_DEFAULT,
    LIMIT_QUERY_GREP_MAX, LIMIT_UPLOAD_MAX_CONTENT_BYTES, PROFILE_ADMIN_V0, PROFILE_CORE_V0,
    PROFILE_QUERY_V0, PROTOCOL_VERSION,
};
pub use content::{ContentRef, ContentRefKind};
pub use digest::sha256_digest;
pub use error::{ErrorCode, ErrorKind};
pub use ids::{
    generated_id, manifest_object_id_manifest_id, wal_segment_id_start_seq, ChangeSeq,
    CheckpointId, CommitId, CommitIdValidationError, ContentStoreId, GeneratedIdValidationError,
    IndexSegmentId, InodeId, InodeKind, ManifestId, ManifestObjectId, MetadataTableId, NameKey,
    NameKeyValidationError, NamespaceId, NamespaceIdValidationError, RevisionNo, UploadId,
    WalSegmentId, WriterEpoch,
};
pub use name_policy::{name_key_for_display_name, NamePolicy};
pub use pagination::{
    decode_directory_cursor, decode_file_revisions_cursor, decode_grep_cursor,
    encode_directory_cursor, encode_file_revisions_cursor, encode_grep_cursor, DirectoryPageCursor,
    EffectiveLimit, FileRevisionsPageCursor, GrepPageCursor, LimitError, Page, PageCursorError,
    PageRequest, PaginationPolicy, PaginationPolicyError, DEFAULT_MAX_PAGE_LIMIT,
    DEFAULT_PAGE_LIMIT, LIMIT_PAGINATION_DEFAULT, LIMIT_PAGINATION_MAX, PAGE_CURSOR_VERSION,
};
pub use path::{AbsolutePath, DisplayName, PathComponent, PathError};

// Curated root re-exports of the common v0 HTTP surface. v0 HTTP shapes live
// in `v0`; add here only what most consumers touch.
pub use v0::{
    AdvanceRetentionResponse, ApiError, AuthoritativeFileBytes, AuthoritativePathEntry,
    CommitResponse, CopyBehavior, CreateCheckpointRequest, CreateCheckpointResponse,
    CreateNamespaceRequest, DeleteDirectoryBehavior, DeleteNamespaceResponse,
    DisableGramsIndexResponse, EnableGramsIndexResponse, ErrorDetails, FileRevision,
    FilesystemOperation, FilesystemOperationRequest, FlushWalOutcome, FlushWalResponse,
    ForkNamespaceRequest, GcRequest, GcResponse, GrepMatch, GrepRequest, GrepResponse,
    ListFileRevisionsResponse, ListPathEntriesResponse, MaintenanceTickOutcome,
    MaintenanceTickRequest, MaintenanceTickResponse, MoveBehavior, NamespaceStatusResponse,
    NamespaceSummary, PutBehavior, ReleaseCheckpointResponse, RestoreFileRevisionRequest,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn curated_root_exports_cover_common_public_types() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let commit_id = CommitId::generate();
        let path = AbsolutePath::parse("/docs/report.txt").expect("valid path");
        let display_name = DisplayName::parse("Report.txt").expect("valid display name");
        let name_key = NameKey::for_display_name(NamePolicy::default(), &display_name);
        let content_ref = ContentRef::whole_file_v0(b"hello");

        assert_eq!(namespace_id.as_str(), "demo");
        assert!(commit_id.as_str().starts_with("c_"));
        assert_eq!(path.as_str(), "/docs/report.txt");
        assert_eq!(name_key.as_str(), "report.txt");
        assert_eq!(content_ref.size_bytes, 5);
    }

    #[test]
    fn durable_protocol_types_are_available_under_wire() {
        let _head = wire::control::HeadState::initial(
            NamespaceId::parse("demo").expect("valid namespace id"),
        );
        let _wal_delta = wire::wal::WalDelta::TombstoneSubtree {
            delta_index: 0,
            root_inode_id: InodeId(1),
        };
        let _manifest_row = wire::manifest::MetadataRow::Tombstone {
            root_inode_id: InodeId(1),
            tombstone_seq: ChangeSeq(1),
            tombstone_delta_index: 0,
            cleared: false,
        };
    }
}
