//! Shared LoonFS types: validated identifiers and paths, HTTP API shapes, and
//! durable-format codecs.
//!
//! The crate defines validated identifiers and paths at its root, HTTP API
//! shapes in [`api`], durable storage formats in [`format`](mod@format), and
//! shared operation options in [`options`].
//!
//! I/O doesn't belong here: the crate defines shapes, validation, and codecs,
//! and reads or writes no object.

#![warn(missing_docs)]

mod access;
mod actor;
mod attributes;
mod base64_bytes;
mod capability;
mod civil_date;
mod clock;
mod commit_identity;
pub mod config;
mod content;
mod control;
pub mod crypto;
mod digest;
pub mod env;
mod envelope;
mod error;
mod hex;
mod id_macros;
mod ids;
mod layout;
pub mod limits;
mod manifest;
mod metadata_rows;
mod name_policy;
pub mod options;
mod pagination;
mod path;
pub mod public_inode_id;
mod retry;
mod row_keys;
mod secret;
mod segment_blocks;
mod wal;
mod widths;

pub mod api {
    //! HTTP API request and response shapes, one module per protocol version.

    pub mod v0;
}

pub mod format {
    //! Durable formats grouped by their owning format family.

    pub mod hex {
        //! Lowercase hexadecimal primitives shared by durable codecs.

        pub use crate::hex::*;
    }

    pub mod manifest {
        //! Namespace-manifest envelopes, rows, and key constructors.

        pub use crate::manifest::*;
        pub use crate::metadata_rows::*;
        pub use crate::row_keys::*;
    }

    pub mod control {
        //! Mutable namespace control-object envelopes and payloads.

        pub use crate::control::*;
    }

    pub mod envelope {
        //! Shared durable envelope codec and validation errors.

        pub use crate::envelope::*;
    }

    pub mod segment_blocks {
        //! Block handles, builders, and codecs for metadata and index segments.

        pub use crate::segment_blocks::*;
    }

    pub mod wal {
        //! WAL object envelopes, records, and codecs.

        pub use crate::wal::*;
    }
}

pub use access::{
    AccessGrants, AccessGrantsError, AccessRevisionNo, AccessRight, AccessRights,
    AccessRightsError, PrincipalId, PrincipalIdValidationError, PrincipalScope,
    PrincipalScopeValidationError, PrincipalSet, PrincipalSetError, Subject, SubjectId,
    SubjectIdValidationError, SubjectPart, SubjectPartsError, MAX_ACCESS_GRANTS_PRINCIPAL_BYTES,
    MAX_ACCESS_GRANT_ENTRIES, MAX_SUBJECT_PRINCIPALS,
};
pub use actor::{ActorId, ActorIdValidationError, MAX_ACTOR_ID_BYTES};
pub use attributes::{
    AttributeKey, AttributeKeyValidationError, AttributeValue, AttributeValueValidationError,
    Attributes, AttributesError, AttributesRevisionNo, MAX_ATTRIBUTES_TOTAL_BYTES,
    MAX_ATTRIBUTE_ENTRIES, MAX_ATTRIBUTE_KEY_BYTES, MAX_ATTRIBUTE_VALUE_BYTES,
    RESERVED_ATTRIBUTE_KEY_PREFIX,
};
pub use capability::pagination_capability_limits;
pub use capability::{
    CapabilityDocument, CapabilityDocumentError, ALL_LIMIT_KEYS, API_GROUP_FILESYSTEM_V0,
    API_GROUP_MAINTENANCE_V0, API_GROUP_QUERY_V0, FEATURE_COMMIT_INLINE_CONTENT,
    FEATURE_DOWNLOADS_DIRECT_GET, FEATURE_MAINTENANCE_GREP_INDEX, FEATURE_QUERY_GREP,
    FEATURE_UPLOADS_DIRECT_MULTIPART, FEATURE_UPLOADS_DIRECT_PUT,
    LIMIT_ACCESS_MAX_PRINCIPALS_PER_REQUEST, LIMIT_COMMIT_MAX_CONTENT_TOKENS,
    LIMIT_COMMIT_MAX_EXTERNAL_CONTENT_REFS, LIMIT_COMMIT_MAX_INLINE_CONTENT_BYTES_PER_OPERATION,
    LIMIT_COMMIT_MAX_MESSAGE_BYTES, LIMIT_COMMIT_MAX_OPERATIONS, LIMIT_COMMIT_MAX_PRECONDITIONS,
    LIMIT_COMMIT_MAX_REQUEST_BODY_BYTES, LIMIT_DOWNLOAD_SERVICE_PROXIED_MAX_CONCURRENT_REQUESTS,
    LIMIT_DOWNLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES, LIMIT_GC_MIN_GRACE_WINDOW_MS,
    LIMIT_PAGINATION_DEFAULT_LIMIT, LIMIT_PAGINATION_MAX_LIMIT, LIMIT_QUERY_GREP_SCAN_BUDGET_FILES,
    LIMIT_QUERY_GREP_TAIL_BUDGET_FILES, LIMIT_UPLOAD_COMPLETE_MAX_REQUEST_BODY_BYTES,
    LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES,
    LIMIT_UPLOAD_SERVICE_PROXIED_MAX_CONCURRENT_REQUESTS,
    LIMIT_UPLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES, PROTOCOL_VERSION,
};
pub use civil_date::{civil_from_days, CivilDate};
pub use clock::{
    duration_ms, saturating_duration_ms, MonotonicTimer, StdMonotonicTimer, SystemWallClock,
    WallClock,
};
pub use commit_identity::{
    CommitFingerprint, CommitFingerprintInput, CommitFingerprintValidationError,
    InlineContentIdentity,
};
pub use content::{
    Checksum, ChecksumAlgorithm, ChecksumDigest, ChecksumHex, ChecksumHexValidationError,
    ChecksumValidationError, ChunkAfterEndError, ContentMismatchError, ContentRef, ContentRefKind,
    ContentVerifier, ResumedPrefixError, ResumedPrefixIncompleteError,
};
pub use digest::{
    sha256_digest, sha256_hex, PayloadChecksum, PayloadChecksumValidationError, SHA256_DIGEST_BYTES,
};
pub use error::{ErrorCode, ErrorKind};
pub use ids::{
    generated_id, generated_id_body_prefixes, next_public_ordinal, BindingVersion,
    BindingVersionValidationError, ChangeSeq, CommitId, CommitIdValidationError, CompactorEpoch,
    ContentId, GeneratedIdValidationError, IndexSegmentId, InodeId, ManifestNo,
    MetadataCompactionId, MetadataSegmentId, NameKey, NameKeyValidationError, NamespaceId,
    NamespaceIdValidationError, PinId, ProbeRunId, PublicOrdinalRangeError, RevisionNo, RunNo,
    UploadId, WalNo, WriterEpoch, WriterId, WriterIdValidationError, FIRST_ALLOCATABLE_INODE_ID,
    GENERATED_ID_BODY_BYTES, MAX_ID_BYTES, MAX_NAME_KEY_BYTES, MAX_PUBLIC_INTEGER,
    MAX_WRITER_ID_BYTES, ROOT_INODE_ID,
};
pub use layout::{ContentExtent, ContentLayout, ContentLayoutError, ExtentObject};
pub use manifest::NamespaceAccess;
pub use metadata_rows::MetadataFamilyGroup;
pub use name_policy::name_key_for_display_name;
pub use options::AttributeInclusion;
pub use pagination::{
    decode_cursor, decode_token, decode_token_bytes, encode_cursor, encode_token,
    encode_token_bytes, DirectoryPageCursor, DirectoryTreeFrame, DirectoryTreePageCursor,
    EffectiveLimit, FileRevisionsPageCursor, GrepCursorPosition, GrepPageCursor, LimitError,
    ListPageCursor, OpaqueToken, OpaqueTokenError, Page, PageCursor, PageRequest, PagedResponse,
    Pager, TrashPageCursor, DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT, PAGE_CURSOR_FORMAT_VERSION,
};
pub use path::{
    AbsolutePath, DisplayName, InodeKind, PathComponent, PathError, MAX_DISPLAY_NAME_BYTES,
    MAX_PATH_BYTES, MAX_PATH_DEPTH,
};
pub use retry::{OperationDeadline, TransportRetryOutcome, TransportRetryPolicy};
pub use secret::{SecretString, REDACTED};
pub use widths::{u64_from_usize, usize_from_u64};

// Curated root re-exports of the common v0 HTTP surface. v0 HTTP shapes live
// in `api::v0`; add here only what most consumers touch.
pub use api::v0::{
    headers, AdvanceRetentionRequest, AdvanceRetentionResponse, AttributesProjection, Checkpoint,
    CheckpointOwner, Commit, CommitPrecondition, CommitRequest, CreateNamespaceRequest,
    CreateSnapshotRequest, DeleteDirectoryBehavior, DeleteNamespaceResponse,
    DeleteSnapshotResponse, DeletedCheckpointsByOwner, DeletedObjectCounts, DestinationBehavior,
    DestinationPrecondition, EntryInodeKind, ErrorDetails, ErrorResponse, ExtendSnapshotRequest,
    ExtensionGcReports, FileContent, FileRevision, FilesystemOperation, ForkNamespaceRequest,
    GcRequest, GcResponse, GrepGcReport, GrepMatch, GrepRequest, GrepResponse, GrepScope,
    ListCheckpointsResponse, ListFileRevisionsResponse, ListInodeChildrenResponse,
    ListPathEntriesResponse, ListSnapshotsResponse, ListTrashResponse, MetadataMaintenanceOutcome,
    MetadataMaintenanceRequest, MetadataMaintenanceResponse, NamespaceAccessMode,
    NamespaceDiagnostics, NamespaceForkBasis, NamespaceMetadata, PathEntry, PathEntryKind,
    RecoverAdministratorRequest, RecoverAdministratorResponse, RetainedCandidates, RetainedReason,
    RunMaintenanceRequest, RunMaintenanceResponse, SetRetentionWindowRequest,
    SetRetentionWindowResponse, Snapshot, TrashEntry,
};
