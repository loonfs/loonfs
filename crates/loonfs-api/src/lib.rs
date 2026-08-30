//! Wire types and durable-format codecs for LoonFS.
//!
//! The crate defines validated identifiers and paths, HTTP protocol shapes in
//! [`v0`], durable storage formats in [`wire`], and shared operation options in
//! [`options`].

#![warn(missing_docs)]

mod actor;
mod attributes;
mod capability;
mod commit_identity;
mod content;
mod control;
mod digest;
pub mod env;
mod envelope;
mod error;
mod hex;
mod ids;
mod manifest;
mod name_policy;
pub mod options;
mod pagination;
mod path;
pub mod public_inode_id;
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
        //! Shared durable envelope codec and validation errors.

        pub use crate::envelope::*;
    }

    pub mod sst_blocks {
        //! Block handles, builders, and codecs for metadata and index segments.

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
    CapabilityDocument, CapabilityDocumentError, FEATURE_ADMIN_GREP_INDEX, FEATURE_ATTRIBUTES,
    FEATURE_DOWNLOADS_DIRECT_GET, FEATURE_INODES_LIST_CHILDREN, FEATURE_NAMESPACES_CREATE,
    FEATURE_NAMESPACES_DELETE, FEATURE_NAMESPACES_FORK, FEATURE_QUERY_GREP, FEATURE_SNAPSHOTS,
    FEATURE_UPLOADS_DIRECT_MULTIPART, FEATURE_UPLOADS_DIRECT_PUT, FEATURE_WRITE_GUARDS,
    LIMIT_COMMIT_MAX_CONTENT_TOKENS, LIMIT_COMMIT_MAX_EXTERNAL_CONTENT_REFS,
    LIMIT_COMMIT_MAX_MESSAGE_BYTES, LIMIT_COMMIT_MAX_OPERATIONS, LIMIT_DOWNLOAD_MAX_CONCURRENT,
    LIMIT_DOWNLOAD_MAX_CONTENT_BYTES, LIMIT_GC_MIN_GRACE_WINDOW_MS, LIMIT_PAGINATION_DEFAULT,
    LIMIT_PAGINATION_MAX, LIMIT_QUERY_GREP_DEFAULT, LIMIT_QUERY_GREP_MAX,
    LIMIT_QUERY_GREP_SCAN_BUDGET_FILES, LIMIT_QUERY_GREP_TAIL_BUDGET_FILES,
    LIMIT_SNAPSHOT_MAX_LIFETIME_MS, LIMIT_SNAPSHOT_MAX_LIVE_PER_NAMESPACE,
    LIMIT_SNAPSHOT_MAX_TTL_MS, LIMIT_UPLOAD_COMPLETION_MAX_BODY_BYTES,
    LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES, LIMIT_UPLOAD_MAX_CONCURRENT,
    LIMIT_UPLOAD_MAX_CONTENT_BYTES, PROFILE_ADMIN_V0, PROFILE_CORE_V0, PROFILE_QUERY_V0,
    PROTOCOL_VERSION,
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
    generated_id, manifest_object_id_manifest_no, next_public_ordinal, wal_segment_id_start_seq,
    ChangeSeq, CheckpointId, CommitId, CommitIdValidationError, ContentId, ContentStoreId,
    GeneratedIdValidationError, GrepManifestObjectId, IndexSegmentId, InodeId, InodeKind,
    ManifestNo, ManifestObjectId, MetadataCompactionId, MetadataSegmentId, NameKey,
    NameKeyValidationError, NamespaceId, NamespaceIdValidationError, PublicOrdinalRangeError,
    RevisionNo, RunNo, UploadId, WalSegmentId, WriterEpoch, FIRST_ALLOCATABLE_INODE_ID,
    MAX_ID_BYTES, MAX_NAME_KEY_BYTES, MAX_PUBLIC_INTEGER, ROOT_INODE_ID,
};
pub use name_policy::name_key_for_display_name;
pub use pagination::{
    decode_cursor, decode_namespace_cursor, encode_cursor, DirectoryPageCursor, EffectiveLimit,
    FileRevisionsPageCursor, GrepPageCursor, LimitError, NamespaceCursor, NamespaceCursorError,
    Page, PageCursor, PageCursorError, PageRequest, PaginationPolicy, TrashPageCursor,
    DEFAULT_MAX_PAGE_LIMIT, DEFAULT_PAGE_LIMIT, PAGE_CURSOR_FORMAT_VERSION,
};
pub use path::{
    AbsolutePath, DisplayName, PathComponent, PathError, MAX_DISPLAY_NAME_BYTES, MAX_PATH_BYTES,
    MAX_PATH_DEPTH,
};
pub use secret::SecretString;

// Curated root re-exports of the common v0 HTTP surface. v0 HTTP shapes live
// in `v0`; add here only what most consumers touch.
pub use v0::{
    AdvanceRetentionRequest, AdvanceRetentionResponse, ApiError, AttributesProjection, Checkpoint,
    CheckpointOwnerSummary, CommitRequest, CommitResponse, CreateCheckpointRequest,
    CreateNamespaceRequest, CreateSnapshotRequest, DeleteDirectoryBehavior,
    DeleteNamespaceResponse, DeletedObjectCounts, DestinationBehavior, ErrorDetails,
    ExtendSnapshotRequest, FileBytes, FileRevision, FilesystemOperation, FlushWalOutcome,
    FlushWalResponse, ForkNamespaceRequest, GcRequest, GcResponse, GrepMatch, GrepRequest,
    GrepResponse, ListCheckpointsResponse, ListFileRevisionsResponse, ListInodeChildrenResponse,
    ListPathEntriesResponse, ListSnapshotsResponse, ListTrashResponse, MaintenanceStepRequest,
    MaintenanceStepResponse, MetadataMaintenanceRequest, MetadataMaintenanceResponse, Namespace,
    NamespaceDiagnostics, PathEntry, PathEntryKind, ReleaseCheckpointResponse,
    ReleaseSnapshotResponse, ReleasedCheckpointCounts, ReorganizeStepOutcome, RetainedCandidates,
    RetainedReason, SnapshotSummary, TrashEntry, WalFlushStepOutcome,
};
