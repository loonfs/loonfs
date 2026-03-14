use loon_core::metadata::{InodeRecord, MetadataState, RevisionRecord};
use loon_types::{ChangeSeq, ClientMutationOp, ClientMutationRequest, InodeKind};

pub fn server_metadata_for_request(request: &ClientMutationRequest) -> MetadataState {
    match &request.op {
        ClientMutationOp::CreateDir {
            parent_inode_id, ..
        }
        | ClientMutationOp::CreateFile {
            parent_inode_id, ..
        } => MetadataState {
            inodes: vec![InodeRecord {
                inode_id: *parent_inode_id,
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            }],
            ..MetadataState::default()
        },
        ClientMutationOp::ReplaceFile {
            inode_id,
            base_revision_no,
            ..
        } => MetadataState {
            inodes: vec![InodeRecord {
                inode_id: *inode_id,
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(1),
            }],
            revisions: vec![RevisionRecord {
                inode_id: *inode_id,
                revision_no: *base_revision_no,
                committed_seq: ChangeSeq(base_revision_no.0),
                content_manifest_digest: "sha256:previous-manifest".to_owned(),
            }],
            ..MetadataState::default()
        },
    }
}
