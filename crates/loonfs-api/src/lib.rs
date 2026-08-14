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
//! The `commit_identity` module contains shared logic rather than wire types.
//! It computes durable mutation fingerprints and verifies retried PUT requests
//! against existing commit receipts. Keeping this logic here ensures that the
//! embedded runtime and HTTP client apply the same identity and content checks.
//!
//! Module rule: v0 HTTP shapes live in [`v0`]; the crate root keeps the
//! ids/paths/errors/wire-format modules and re-exports the common v0
//! surface as a curated explicit list below.

#![warn(missing_docs)]

mod actor;
mod attributes;
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
mod secret;
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

pub use actor::{ActorId, ActorIdValidationError, ActorKind, ActorRef};
pub use attributes::{
    AttributeKey, AttributeKeyValidationError, AttributeRevisionNo, AttributeValue,
    AttributeValueValidationError, Attributes, AttributesError, MAX_ATTRIBUTES_TOTAL_BYTES,
    MAX_ATTRIBUTE_ENTRIES, MAX_ATTRIBUTE_KEY_BYTES, MAX_ATTRIBUTE_VALUE_BYTES,
    RESERVED_ATTRIBUTE_KEY_PREFIX,
};
pub use capability::{
    direct_put_checksum_feature, CapabilityDocument, CapabilityDocumentError,
    FEATURE_ADMIN_GREP_INDEX, FEATURE_ATTRIBUTES, FEATURE_DOWNLOADS_DIRECT_GET,
    FEATURE_NAMESPACES_CREATE, FEATURE_NAMESPACES_DELETE, FEATURE_NAMESPACES_FORK,
    FEATURE_QUERY_GREP, FEATURE_UPLOADS_DIRECT_MULTIPART, FEATURE_UPLOADS_DIRECT_PUT,
    FEATURE_UPLOADS_DIRECT_PUT_CHECKSUM_CRC32C, FEATURE_UPLOADS_DIRECT_PUT_CHECKSUM_CRC64NVME,
    FEATURE_UPLOADS_DIRECT_PUT_CHECKSUM_SHA256, LIMIT_COMMIT_MAX_CONTENT_TOKENS,
    LIMIT_COMMIT_MAX_EXTERNAL_CONTENT_REFS, LIMIT_COMMIT_MAX_MESSAGE_BYTES,
    LIMIT_COMMIT_MAX_OPERATIONS, LIMIT_DOWNLOAD_MAX_CONCURRENT, LIMIT_DOWNLOAD_MAX_CONTENT_BYTES,
    LIMIT_GC_MIN_GRACE_WINDOW_MS, LIMIT_PAGINATION_DEFAULT, LIMIT_PAGINATION_MAX,
    LIMIT_QUERY_GREP_DEFAULT, LIMIT_QUERY_GREP_MAX, LIMIT_QUERY_GREP_SCAN_BUDGET_FILES,
    LIMIT_QUERY_GREP_TAIL_BUDGET_FILES, LIMIT_UPLOAD_COMPLETION_MAX_BODY_BYTES,
    LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES, LIMIT_UPLOAD_MAX_CONCURRENT,
    LIMIT_UPLOAD_MAX_CONTENT_BYTES, PROFILE_ADMIN_V0, PROFILE_CORE_V0, PROFILE_QUERY_V0,
    PROTOCOL_VERSION, UPLOADS_DIRECT_PUT_CHECKSUM_FEATURES,
};
pub use commit_identity::{
    put_retry_fingerprint, reconcile_put_commit_id_reuse, semantic_commit_fingerprint,
    PutRetryAttempt, PutRetryErrorClassification, PutRetryReceipt, SemanticFingerprintError,
};
pub use content::{
    Checksum, ChecksumAlgorithm, ChecksumValidationError, ContentEvidence, ContentRef,
    ContentRefKind, ContentRefValidationError, Crc32c, Crc64Nvme, Sha256, StreamingChecksum,
};
pub use digest::sha256_digest;
pub use error::{ErrorCode, ErrorKind};
pub use ids::{
    generated_id, manifest_object_id_manifest_id, wal_segment_id_start_seq, ChangeSeq,
    CheckpointId, CommitId, CommitIdValidationError, ContentId, ContentStoreId,
    GeneratedIdValidationError, IndexSegmentId, InodeId, InodeKind, ManifestId, ManifestObjectId,
    MetadataCompactionId, MetadataTableId, NameKey, NameKeyValidationError, NamespaceId,
    NamespaceIdValidationError, RevisionNo, UploadId, WalSegmentId, WriterEpoch, MAX_ID_BYTES,
    MAX_NAME_KEY_BYTES, ROOT_INODE_ID,
};
pub use name_policy::name_key_for_display_name;
pub use pagination::{
    decode_cursor, decode_namespace_cursor, encode_cursor, DirectoryPageCursor, EffectiveLimit,
    FileRevisionsPageCursor, GrepPageCursor, LimitError, NamespaceCursor, NamespaceCursorError,
    Page, PageCursor, PageCursorError, PageRequest, PaginationPolicy, TrashPageCursor,
    DEFAULT_MAX_PAGE_LIMIT, DEFAULT_PAGE_LIMIT, PAGE_CURSOR_VERSION,
};
pub use path::{
    AbsolutePath, DisplayName, PathComponent, PathError, MAX_DISPLAY_NAME_BYTES, MAX_PATH_BYTES,
    MAX_PATH_DEPTH,
};
pub use secret::SecretString;

// Curated root re-exports of the common v0 HTTP surface. v0 HTTP shapes live
// in `v0`; add here only what most consumers touch.
pub use v0::{
    AdvanceRetentionResponse, ApiError, AttributesProjection, AuthoritativeFileBytes,
    AuthoritativePathEntry, AuthoritativePathEntryKind, CheckpointOwnerSummary, CheckpointSummary,
    CommitRequest, CommitResponse, CreateCheckpointRequest, CreateCheckpointResponse,
    CreateNamespaceRequest, DeleteDirectoryBehavior, DeleteNamespaceResponse, DestinationBehavior,
    ErrorDetails, FileRevision, FilesystemOperation, FlushWalOutcome, FlushWalResponse,
    ForkNamespaceRequest, GcRequest, GcResponse, GrepMatch, GrepRequest, GrepResponse,
    ListCheckpointsResponse, ListFileRevisionsResponse, ListPathEntriesResponse, ListTrashResponse,
    MaintenanceStepRequest, MaintenanceStepResponse, MetadataMaintenanceRequest,
    MetadataMaintenanceResponse, NamespaceStatusResponse, NamespaceSummary,
    ReleaseCheckpointResponse, ReorganizeStepOutcome, RetainedCandidates, RetainedReason,
    TrashEntry, WalFlushStepOutcome,
};
