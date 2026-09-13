//! Bounds the WAL records produced by a request before metadata planning.

use crate::path::write::CommitRequest;
use loonfs_api::{
    AbsolutePath, FilesystemOperation, MAX_ATTRIBUTES_TOTAL_BYTES, MAX_ATTRIBUTE_ENTRIES,
    MAX_DISPLAY_NAME_BYTES, MAX_ID_BYTES, MAX_NAME_KEY_BYTES,
};

const INTEGER_BYTES: usize = 9;
const INDEX_BYTES: usize = 5;
const NAMES_BYTES: usize = string_bytes(MAX_NAME_KEY_BYTES) + string_bytes(MAX_DISPLAY_NAME_BYTES);
const CREATE_INODE_BYTES: usize = delta_bytes(
    "create_inode",
    &[
        ("inode_id", INTEGER_BYTES),
        ("inode_kind", string_bytes("file".len())),
    ],
);
const BIND_BYTES: usize = delta_bytes(
    "bind_direntry",
    &[
        ("parent_inode_id", INTEGER_BYTES),
        ("name_key", 0),
        ("display_name", 0),
        ("child_inode_id", INTEGER_BYTES),
    ],
);
const UNBIND_BYTES: usize = delta_bytes(
    "unbind_direntry",
    &[
        ("parent_inode_id", INTEGER_BYTES),
        ("name_key", 0),
        ("display_name", 0),
        ("child_inode_id", INTEGER_BYTES),
        ("bind_seq", INTEGER_BYTES),
        ("bind_delta_index", INDEX_BYTES),
    ],
) + NAMES_BYTES;
const TOMBSTONE_BYTES: usize = delta_bytes(
    "tombstone_subtree",
    &[
        ("root_inode_id", INTEGER_BYTES),
        (
            "deleted_direntry",
            map_bytes(&[
                ("parent_inode_id", INTEGER_BYTES),
                ("name_key", 0),
                ("display_name", 0),
            ]) + NAMES_BYTES,
        ),
    ],
);
const DELETE_BYTES: usize = UNBIND_BYTES + TOMBSTONE_BYTES;
const CONTENT_REF_BYTES: usize = map_bytes(&[
    ("kind", string_bytes("blob_v1".len())),
    ("owner_namespace_id", string_bytes(MAX_ID_BYTES)),
    ("content_id", string_bytes(MAX_ID_BYTES)),
    ("size_bytes", INTEGER_BYTES),
    (
        "checksum",
        map_bytes(&[
            ("algorithm", string_bytes("crc64nvme".len())),
            ("value", string_bytes(64)),
        ]),
    ),
]);
const REVISION_BYTES: usize = delta_bytes(
    "append_file_revision",
    &[
        ("inode_id", INTEGER_BYTES),
        ("revision_no", INTEGER_BYTES),
        ("content_ref", CONTENT_REF_BYTES),
    ],
);
const ATTRIBUTES_BYTES: usize = delta_bytes(
    "append_attributes_revision",
    &[
        ("inode_id", INTEGER_BYTES),
        ("attributes_revision_no", INTEGER_BYTES),
        (
            "attributes",
            9 + MAX_ATTRIBUTES_TOTAL_BYTES + MAX_ATTRIBUTE_ENTRIES * (2 + 3),
        ),
    ],
);
const REVOKE_BYTES: usize = delta_bytes(
    "revoke_subtree_tombstone",
    &[
        ("root_inode_id", INTEGER_BYTES),
        (
            "target",
            map_bytes(&[("seq", INTEGER_BYTES), ("delta_index", INDEX_BYTES)]),
        ),
    ],
);

// CBOR lengths and u64 values use at most nine bytes; u32 values use five.
const fn string_bytes(length: usize) -> usize {
    9 + length
}

const fn map_bytes(fields: &[(&str, usize)]) -> usize {
    let mut bytes = 9;
    let mut index = 0;
    while index < fields.len() {
        bytes += string_bytes(fields[index].0.len()) + fields[index].1;
        index += 1;
    }
    bytes
}

const fn delta_bytes(kind: &str, fields: &[(&str, usize)]) -> usize {
    map_bytes(&[
        ("semantic_op_index", INDEX_BYTES),
        (
            "delta",
            map_bytes(&[
                ("kind", string_bytes(kind.len())),
                ("delta_index", INDEX_BYTES),
            ]),
        ),
    ]) + map_bytes(fields)
        - 9
}

pub(crate) fn estimated_wal_record_bytes(request: &CommitRequest) -> usize {
    let fixed_bytes = map_bytes(&[
        ("seq", INTEGER_BYTES),
        ("commit_id", string_bytes(request.commit_id.as_str().len())),
        (
            "committed_by",
            string_bytes(request.actor_id.as_str().len()),
        ),
        (
            "semantic_commit_fingerprint",
            string_bytes("v4:sha256:".len() + 64),
        ),
        ("committed_at_ms", INTEGER_BYTES),
        (
            "message",
            string_bytes(request.message.as_ref().map_or(0, String::len)),
        ),
        ("deltas", 9),
    ]);
    request
        .operations
        .iter()
        .fold(fixed_bytes, |bytes, operation| {
            bytes.saturating_add(operation_bytes(operation))
        })
}

fn operation_bytes(operation: &FilesystemOperation) -> usize {
    match operation {
        FilesystemOperation::CreateDirectory { path, parents } => {
            if *parents {
                create_path_bytes(path)
            } else {
                CREATE_INODE_BYTES + BIND_BYTES + NAMES_BYTES
            }
        }
        FilesystemOperation::CreateDirectoryByInode { .. } => {
            CREATE_INODE_BYTES + BIND_BYTES + NAMES_BYTES
        }
        FilesystemOperation::PutFile { path, .. } => create_path_bytes(path) + REVISION_BYTES,
        FilesystemOperation::CreateFileByInode { .. } => {
            CREATE_INODE_BYTES + BIND_BYTES + NAMES_BYTES + REVISION_BYTES
        }
        FilesystemOperation::PutFileRevisionByInode { .. }
        | FilesystemOperation::RestoreRevision { .. } => REVISION_BYTES,
        FilesystemOperation::DeletePath { .. } | FilesystemOperation::DeleteByInode { .. } => {
            DELETE_BYTES
        }
        FilesystemOperation::MovePath { .. } | FilesystemOperation::MoveByInode { .. } => {
            DELETE_BYTES + UNBIND_BYTES + BIND_BYTES + NAMES_BYTES
        }
        FilesystemOperation::CopyPath { .. } => {
            CREATE_INODE_BYTES + BIND_BYTES + NAMES_BYTES + REVISION_BYTES + ATTRIBUTES_BYTES
        }
        FilesystemOperation::Undelete { .. } => REVOKE_BYTES + BIND_BYTES + NAMES_BYTES,
        FilesystemOperation::UpdateAttributes { .. } => ATTRIBUTES_BYTES,
    }
}

fn create_path_bytes(path: &AbsolutePath) -> usize {
    path.components().iter().fold(0_usize, |bytes, component| {
        let display_name = component.as_str();
        let name_key = loonfs_api::name_key_for_display_name(display_name);
        bytes.saturating_add(
            CREATE_INODE_BYTES
                + BIND_BYTES
                + string_bytes(display_name.len())
                + string_bytes(name_key.len()),
        )
    })
}

#[cfg(test)]
#[path = "commit_wal_size_tests.rs"]
mod tests;
