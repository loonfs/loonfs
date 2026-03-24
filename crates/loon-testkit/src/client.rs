use loon_core::checkpoint::prepare_checkpoint;
use loon_core::metadata::{DirentryRecord, InodeRecord, MetadataState, RevisionRecord};
use loon_objectstore::keys::namespace_head;
use loon_objectstore::ObjectStore;
use loon_types::{
    ChangeSeq, ClientMutationOp, ClientMutationRequest, ControlObjectKind, HeadStateEnvelope,
    InodeId, InodeKind,
};

pub fn seed_server_basis_for_request<S: ObjectStore>(
    store: &S,
    request: &ClientMutationRequest,
    writer_version: &str,
) {
    let head_key = namespace_head(request.namespace_id.as_str());
    let head_bytes = store
        .get(&head_key, None)
        .expect("read seeded head object")
        .expect("seeded head should exist");
    let mut head: HeadStateEnvelope =
        serde_json::from_slice(&head_bytes).expect("decode seeded head object");
    head.state.snapshot_hint_seq = Some(head.state.seq);

    let metadata_state = minimal_server_basis_metadata_for_request(request);
    let checkpoint = prepare_checkpoint(&head.state, &metadata_state, writer_version)
        .expect("prepare checkpoint");
    store
        .put_overwrite(
            &checkpoint.manifest.object_key,
            &checkpoint.manifest.encoded_bytes,
        )
        .expect("seed checkpoint manifest");
    for segment in &checkpoint.segments {
        store
            .put_overwrite(&segment.object_key, &segment.encoded_bytes)
            .expect("seed checkpoint segment");
    }

    let head_envelope =
        HeadStateEnvelope::from_state(ControlObjectKind::NamespaceHead, writer_version, head.state)
            .expect("encode basis-backed head");
    let head_bytes = serde_json::to_vec(&head_envelope).expect("serialize basis-backed head");
    store
        .put_overwrite(&head_key, &head_bytes)
        .expect("overwrite head with checkpoint-backed snapshot hint");
}

fn minimal_server_basis_metadata_for_request(request: &ClientMutationRequest) -> MetadataState {
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
                revision_op_index: 0,
                content_manifest_digest: "sha256:previous-manifest".to_owned(),
            }],
            ..MetadataState::default()
        },
        ClientMutationOp::Rename {
            inode_id,
            new_parent_inode_id,
            ..
        } => MetadataState {
            inodes: vec![
                InodeRecord {
                    inode_id: InodeId(1),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: *new_parent_inode_id,
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: *inode_id,
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(1),
                },
            ],
            direntries: vec![DirentryRecord {
                parent_inode_id: InodeId(1),
                name_key: "before".to_owned(),
                display_name: "before".to_owned(),
                child_inode_id: *inode_id,
                bind_seq: ChangeSeq(1),
                bind_op_index: 0,
            }],
            revisions: vec![RevisionRecord {
                inode_id: *inode_id,
                revision_no: loon_types::RevisionNo(1),
                committed_seq: ChangeSeq(1),
                revision_op_index: 0,
                content_manifest_digest: "sha256:previous-manifest".to_owned(),
            }],
            ..MetadataState::default()
        },
        ClientMutationOp::DeleteFile { inode_id } => MetadataState {
            inodes: vec![InodeRecord {
                inode_id: *inode_id,
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(1),
            }],
            revisions: vec![RevisionRecord {
                inode_id: *inode_id,
                revision_no: loon_types::RevisionNo(1),
                committed_seq: ChangeSeq(1),
                revision_op_index: 0,
                content_manifest_digest: "sha256:previous-manifest".to_owned(),
            }],
            ..MetadataState::default()
        },
        ClientMutationOp::DeleteSubtree { root_inode_id } => MetadataState {
            inodes: vec![InodeRecord {
                inode_id: *root_inode_id,
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            }],
            ..MetadataState::default()
        },
    }
}
