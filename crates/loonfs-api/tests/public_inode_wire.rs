use loonfs_api::v0::{
    BeginDownloadByInodeResponse, DeletedDirentry, FilesystemChange, GrepIndexLifecycle,
    ObjectTransferAccess,
};
use loonfs_api::{
    AbsolutePath, ActorRef, AttributeRevisionNo, Attributes, AuthoritativePathEntry,
    AuthoritativePathEntryKind, ChangeSeq, CheckpointId, ContentId, ContentRef,
    DeleteDirectoryBehavior, DisplayName, ErrorDetails, FileRevision, FilesystemOperation,
    GrepMatch, InodeId, ListFileRevisionsResponse, NameKey, NamespaceId, RevisionNo, TrashEntry,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

fn json(value: impl Serialize) -> Value {
    serde_json::to_value(value).expect("serialize public inode-bearing shape")
}

fn assert_public_inode_values(value: &Value, path: &str, count: &mut usize) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                let child_path = format!("{path}.{key}");
                if key.ends_with("inode_id") {
                    *count += 1;
                    assert!(
                        value.is_string(),
                        "{child_path} is not a JSON string: {value}"
                    );
                    let encoded = value.as_str().expect("checked JSON string");
                    let decoded = loonfs_api::public_inode_id::decode(encoded);
                    assert!(decoded.is_ok(), "{child_path} is invalid: {decoded:?}");
                    let decoded = decoded.expect("checked public inode id");
                    assert_eq!(
                        loonfs_api::public_inode_id::encode(decoded),
                        encoded,
                        "{child_path} is not canonical"
                    );
                }
                assert_public_inode_values(value, &child_path, count);
            }
        }
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                assert_public_inode_values(value, &format!("{path}[{index}]"), count);
            }
        }
        _ => {}
    }
}

#[test]
fn every_public_inode_field_serializes_with_the_public_codec() {
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let actor = ActorRef::loonfs_system();
    let display_name = DisplayName::parse("report.txt").expect("display name");
    let name_key = NameKey::parse("report.txt").expect("name key");
    let content_ref = ContentRef::blob_v1(ContentId::generate(), b"hello");

    let values = vec![
        json(AuthoritativePathEntry {
            namespace_id: namespace_id.clone(),
            path: AbsolutePath::parse("/report.txt").expect("path"),
            inode_id: InodeId(2),
            created_by: actor.clone(),
            created_at_ms: 1,
            kind: AuthoritativePathEntryKind::Directory {},
            head_seq: ChangeSeq(1),
            parent_inode_id: Some(InodeId(1)),
            display_name: Some(display_name.clone()),
            attributes: None,
        }),
        json(TrashEntry {
            inode_id: InodeId(2),
            deletion_seq: ChangeSeq(3),
            deleted_at_ms: 4,
            deleted_by: actor.clone(),
            parent_inode_id: Some(InodeId(1)),
            name_key: Some(name_key.clone()),
            display_name: Some(display_name.clone()),
        }),
        json(FilesystemChange::DirectoryCreated {
            inode_id: InodeId(2),
            parent_inode_id: InodeId(1),
            display_name: display_name.clone(),
        }),
        json(FilesystemChange::FileCreated {
            inode_id: InodeId(2),
            parent_inode_id: InodeId(1),
            display_name: display_name.clone(),
            revision_no: RevisionNo(1),
            content_ref: content_ref.clone(),
        }),
        json(FilesystemChange::ContentChanged {
            inode_id: InodeId(2),
            revision_no: RevisionNo(2),
            content_ref: content_ref.clone(),
        }),
        json(FilesystemChange::Moved {
            inode_id: InodeId(2),
            from_parent_inode_id: InodeId(1),
            from_display_name: display_name.clone(),
            to_parent_inode_id: InodeId(3),
            to_display_name: display_name.clone(),
        }),
        json(FilesystemChange::Deleted {
            inode_id: InodeId(2),
            deleted_direntry: Some(DeletedDirentry {
                parent_inode_id: InodeId(1),
                name_key: name_key.clone(),
                display_name: display_name.clone(),
            }),
        }),
        json(FilesystemChange::Undeleted {
            inode_id: InodeId(2),
            parent_inode_id: InodeId(1),
            display_name: display_name.clone(),
        }),
        json(FilesystemChange::AttributesChanged {
            inode_id: InodeId(2),
            attributes_revision_no: AttributeRevisionNo(1),
            attributes: Attributes::default(),
        }),
        json(ErrorDetails {
            inode_id: Some(InodeId(2)),
            ..ErrorDetails::default()
        }),
        json(FilesystemOperation::DeletePath {
            path: AbsolutePath::parse("/report.txt").expect("path"),
            behavior: DeleteDirectoryBehavior::NonRecursive,
            expected_inode_id: Some(InodeId(2)),
        }),
        json(FilesystemOperation::Undelete {
            inode_id: InodeId(2),
            deletion_seq: ChangeSeq(3),
            path: None,
        }),
        json(FilesystemOperation::UpdateAttributes {
            path: AbsolutePath::parse("/report.txt").expect("path"),
            set: BTreeMap::new(),
            remove: Vec::new(),
            expected_inode_id: Some(InodeId(2)),
            expected_attributes_revision_no: None,
        }),
        json(FileRevision {
            inode_id: InodeId(2),
            revision_no: RevisionNo(1),
            committed_seq: ChangeSeq(1),
            committed_at_ms: 1,
            actor: actor.clone(),
            content_ref: content_ref.clone(),
        }),
        json(ListFileRevisionsResponse {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(2),
            head_seq: ChangeSeq(1),
            revisions: Vec::new(),
            next_cursor: None,
        }),
        json(GrepMatch {
            path: AbsolutePath::parse("/report.txt").expect("path"),
            inode_id: InodeId(2),
            revision_no: RevisionNo(1),
            line_number: 1,
            byte_offset: 0,
            line: "hello".to_owned(),
            line_truncated: false,
        }),
        json(GrepIndexLifecycle::Backfilling {
            target_seq: ChangeSeq(1),
            cursor_inode_id: Some(InodeId(2)),
            checkpoint_id: CheckpointId::parse("chk_00000000000000000000000000000001")
                .expect("checkpoint id"),
        }),
        json(BeginDownloadByInodeResponse {
            namespace_id,
            inode_id: InodeId(2),
            revision_no: RevisionNo(1),
            content_ref,
            access: ObjectTransferAccess::PresignedUrl {
                method: "GET".to_owned(),
                url: "https://bucket.example/object".to_owned(),
                headers: BTreeMap::new(),
                expires_at_ms: 1,
            },
        }),
    ];

    let mut count = 0;
    for (index, value) in values.iter().enumerate() {
        assert_public_inode_values(value, &format!("shape[{index}]"), &mut count);
    }
    assert_eq!(count, 26, "update the exhaustive public inode inventory");
}
