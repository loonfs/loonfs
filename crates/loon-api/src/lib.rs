#![forbid(unsafe_code)]

mod content;
mod control;
mod digest;
mod http;
mod ids;
mod manifest;
mod name_policy;
mod path;
mod server;
pub mod v0;
mod wal;

pub mod wire {
    pub mod manifest {
        pub use crate::manifest::*;
    }

    pub mod control {
        pub use crate::control::*;
    }

    pub mod wal {
        pub use crate::wal::*;
    }
}

pub use content::{ContentRef, ContentRefKind};
pub use digest::sha256_digest;
pub use http::{
    AdvanceRetentionResponse, ApiError, CreateCheckpointResponse, CreateNamespaceRequest,
    FileRevision, FilesystemOperation, FilesystemOperationRequest, FilesystemOperationResponse,
    FilesystemPutBehavior, ForkNamespaceRequest, ListFileRevisionsResponse, ListNamespacesResponse,
    MutationResult, NamespaceSummary, RestoreFileRevisionRequest,
};
pub use ids::{
    generate_checkpoint_id, generate_gc_pin_id, generate_metadata_table_id, generate_upload_id,
    generate_wal_segment_id, generated_id, validate_checkpoint_id, validate_gc_pin_id,
    validate_generated_id, validate_metadata_table_id, validate_upload_id, validate_wal_segment_id,
    ChangeSeq, CommitId, CommitIdValidationError, ConflictDisposition, ContentStoreId, FenceToken,
    GeneratedIdValidationError, Identity, InodeId, InodeKind, ManifestId, NameKey,
    NameKeyValidationError, NamespaceId, NamespaceIdValidationError, RevisionNo,
};
pub use name_policy::{name_key_for_display_name, NamePolicy};
pub use path::{AbsolutePath, DisplayName, PathComponent, PathError};
pub use server::{AuthoritativeFileBytes, AuthoritativePathEntry};

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
            root_inode: InodeId(1),
        };
        let _manifest_row = wire::manifest::MetadataRow::Tombstone {
            root_inode_id: InodeId(1),
            tombstone_seq: ChangeSeq(1),
            tombstone_delta_index: 0,
        };
    }
}
