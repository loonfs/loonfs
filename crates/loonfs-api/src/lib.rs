//! Wire types and durable-format codecs for LoonFS.
//!
//! Everything that crosses a process or storage boundary is defined here:
//! validated identifier and path types at the crate root, the versioned HTTP
//! protocol shapes in [`v0`], and the durable storage formats in [`wire`]
//! (WAL segments, metadata SSTs, namespace manifests, and control objects).
//! Other LoonFS crates depend on this one for vocabulary; it depends on none
//! of them.
//!
//! One module here is deliberately not a boundary format: [`options`] holds
//! the per-operation argument structs that the embedded runtime and the HTTP
//! client both expose. They parameterize the same semantic operations on both
//! surfaces, so this crate — the shared vocabulary — owns the single
//! definition rather than each surface keeping its own copy to drift.
//!
//! One module is a function rather than a shape: `commit_identity` computes
//! the durable fingerprint of a mutation. It lives here for the same reason
//! the operation language does — the engine that stamps a fingerprint on a
//! commit receipt and the client that recomputes one to prove a retry is the
//! same request must produce identical values, so there is one implementation
//! and no second reading of the rules.
//!
//! Module rule: v0 HTTP shapes live in [`v0`]; the crate root keeps the
//! ids/paths/errors/wire-format modules and re-exports the common v0
//! surface as a curated explicit list below.

#![warn(missing_docs)]

mod capability;
mod commit_identity;
mod content;
mod control;
mod digest;
mod envelope;
mod error;
mod hex;
mod ids;
mod manifest;
mod name_policy;
pub mod options;
mod pagination;
mod path;
mod sst_blocks;
pub mod v0;
mod wal;

pub mod wire {
    //! Durable wire formats grouped by their owning format family.

    pub mod hex {
        //! Lowercase hexadecimal primitives shared by durable codecs.

        pub use crate::hex::*;
    }

    pub mod manifest {
        //! Namespace-manifest envelopes, rows, and key constructors.

        pub use crate::manifest::*;
    }

    pub mod control {
        //! Mutable namespace control-object envelopes and payloads.

        pub use crate::control::*;
    }

    pub mod envelope {
        //! The shared durable envelope codec: probe, validation rules, JSON
        //! codec, and the one error vocabulary every family reports through.
        //!
        //! Published so a durable format outside this crate — a first-party
        //! extension's own objects — parameterizes the same codec instead of
        //! copying it and drifting from the rules in section 4 of the format
        //! spec.

        pub use crate::envelope::*;
    }

    pub mod sst_blocks {
        //! Metadata SST block handles, builders, and codecs.

        pub use crate::sst_blocks::*;
    }

    pub mod wal {
        //! WAL segment envelopes, records, and codecs.

        pub use crate::wal::*;
    }
}

pub use capability::{
    CapabilityDocument, CapabilityDocumentError, FEATURE_DOWNLOADS_DIRECT_GET,
    FEATURE_NAMESPACES_CREATE, FEATURE_NAMESPACES_DELETE, FEATURE_NAMESPACES_FORK,
    FEATURE_QUERY_GREP, FEATURE_UPLOADS_DIRECT_MULTIPART, FEATURE_UPLOADS_DIRECT_PUT,
    LIMIT_COMMIT_MAX_CONTENT_TOKENS, LIMIT_COMMIT_MAX_EXTERNAL_CONTENT_REFS,
    LIMIT_COMMIT_MAX_MESSAGE_BYTES, LIMIT_COMMIT_MAX_OPERATIONS, LIMIT_DOWNLOAD_MAX_CONCURRENT,
    LIMIT_DOWNLOAD_MAX_CONTENT_BYTES, LIMIT_GC_MIN_GRACE_WINDOW_MS, LIMIT_PAGINATION_DEFAULT,
    LIMIT_PAGINATION_MAX, LIMIT_QUERY_GREP_DEFAULT, LIMIT_QUERY_GREP_MAX,
    LIMIT_QUERY_GREP_SCAN_BUDGET_FILES, LIMIT_QUERY_GREP_TAIL_BUDGET_FILES,
    LIMIT_UPLOAD_MAX_CONCURRENT, LIMIT_UPLOAD_MAX_CONTENT_BYTES, PROFILE_ADMIN_V0, PROFILE_CORE_V0,
    PROFILE_QUERY_V0, PROTOCOL_VERSION,
};
pub use commit_identity::{
    put_retry_fingerprint, semantic_commit_fingerprint, SemanticFingerprintError,
};
pub use content::{
    ChecksumAlgorithm, ContentRef, ContentRefKind, ContentRefValidationError, Crc32c, Crc64Nvme,
    Sha256, StorageChecksum, StreamingChecksum,
};
pub use digest::sha256_digest;
pub use error::{ErrorCode, ErrorKind};
pub use ids::{
    generated_id, manifest_object_id_manifest_id, wal_segment_id_start_seq, ChangeSeq,
    CheckpointId, CommitId, CommitIdValidationError, ContentId, ContentStoreId,
    GeneratedIdValidationError, IndexSegmentId, InodeId, InodeKind, ManifestId, ManifestObjectId,
    MetadataTableId, NameKey, NameKeyValidationError, NamespaceId, NamespaceIdValidationError,
    RevisionNo, UploadId, WalSegmentId, WriterEpoch, MAX_ID_BYTES, MAX_NAME_KEY_BYTES,
    ROOT_INODE_ID,
};
pub use name_policy::name_key_for_display_name;
pub use pagination::{
    decode_cursor, decode_namespace_cursor, encode_cursor, DirectoryPageCursor, EffectiveLimit,
    FileRevisionsPageCursor, GrepPageCursor, LimitError, NamespaceCursor, NamespaceCursorError,
    Page, PageCursor, PageCursorError, PageRequest, PaginationPolicy, PaginationPolicyError,
    TrashPageCursor, DEFAULT_MAX_PAGE_LIMIT, DEFAULT_PAGE_LIMIT, PAGE_CURSOR_VERSION,
};
pub use path::{
    AbsolutePath, DisplayName, PathComponent, PathError, MAX_DISPLAY_NAME_BYTES, MAX_PATH_BYTES,
    MAX_PATH_DEPTH,
};

// Curated root re-exports of the common v0 HTTP surface. v0 HTTP shapes live
// in `v0`; add here only what most consumers touch.
pub use v0::{
    AdvanceRetentionResponse, ApiError, AuthoritativeFileBytes, AuthoritativePathEntry,
    CheckpointOwnerSummary, CheckpointSummary, CommitRequest, CommitResponse,
    CreateCheckpointRequest, CreateCheckpointResponse, CreateNamespaceRequest,
    DeleteDirectoryBehavior, DeleteNamespaceResponse, DestinationBehavior, ErrorDetails,
    FileRevision, FilesystemOperation, FlushWalOutcome, FlushWalResponse, ForkNamespaceRequest,
    GcRequest, GcResponse, GrepMatch, GrepRequest, GrepResponse, ListCheckpointsResponse,
    ListFileRevisionsResponse, ListPathEntriesResponse, ListTrashResponse, MaintenanceStepKind,
    MaintenanceStepRequest, MaintenanceStepResponse, NamespaceStatusResponse, NamespaceSummary,
    ReleaseCheckpointResponse, ReorganizeStepOutcome, RetainedCandidates, RetainedReason,
    TrashEntry, WalFlushStepOutcome,
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
        let name_key = NameKey::for_display_name(&display_name);
        let content_ref = ContentRef::blob_v1(ContentId::generate(), b"hello");

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
            ContentStoreId::parse("cs_0123456789abcdef0123456789abcdef")
                .expect("valid content store id"),
        );
        let _wal_delta = wire::wal::WalDelta::TombstoneSubtree {
            delta_index: 0,
            root_inode_id: InodeId(1),
            parent_inode_id: None,
            name_key: None,
            display_name: None,
        };
        let _manifest_row = wire::manifest::MetadataRow::Tombstone {
            root_inode_id: InodeId(1),
            tombstone_seq: ChangeSeq(1),
            tombstone_delta_index: 0,
            action: wire::manifest::TombstoneRowAction::Set,
            deleted_at_ms: 4_000,
            parent_inode_id: None,
            name_key: None,
            display_name: None,
        };
    }
}
