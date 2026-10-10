//! Bounds the WAL records produced by a request before metadata planning.

use crate::path::write::CommitRequest;
use crate::storage::inline_content::InlineContent;
use loonfs_types::{
    AbsolutePath, AccessRight, FilesystemOperation, NamespaceNaming,
    MAX_ACCESS_GRANTS_PRINCIPAL_BYTES, MAX_ACCESS_GRANT_ENTRIES, MAX_ATTRIBUTES_TOTAL_BYTES,
    MAX_ATTRIBUTE_ENTRIES, MAX_DISPLAY_NAME_BYTES, MAX_ID_BYTES, MAX_NAME_KEY_BYTES,
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
        ("child_kind", string_bytes("file".len())),
        ("child_created_by", string_bytes(256)),
        ("child_created_at_ms", INTEGER_BYTES),
    ],
);
const UNBIND_BYTES: usize = delta_bytes(
    "unbind_direntry",
    &[
        ("parent_inode_id", INTEGER_BYTES),
        ("name_key", 0),
        ("display_name", 0),
        ("child_inode_id", INTEGER_BYTES),
        ("child_kind", string_bytes("file".len())),
        ("child_created_by", string_bytes(256)),
        ("child_created_at_ms", INTEGER_BYTES),
        (
            "target",
            map_bytes(&[("seq", INTEGER_BYTES), ("delta_index", INDEX_BYTES)]),
        ),
    ],
) + NAMES_BYTES;
const TOMBSTONE_BYTES: usize = delta_bytes(
    "tombstone_subtree",
    &[
        ("root_inode_id", INTEGER_BYTES),
        (
            "deleted_binding",
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
        (
            "layout",
            map_bytes(&[(
                "extents",
                9 + map_bytes(&[
                    ("owner_namespace_id", string_bytes(MAX_ID_BYTES)),
                    ("content_id", string_bytes(MAX_ID_BYTES)),
                    (
                        "object",
                        map_bytes(&[("kind", string_bytes("whole".len()))]),
                    ),
                    ("offset", INTEGER_BYTES),
                    ("length", INTEGER_BYTES),
                ]),
            )]),
        ),
        (
            "hash_state",
            map_bytes(&[
                ("words", 9 + 8 * INDEX_BYTES),
                ("tail", string_bytes(63)),
                ("length", INTEGER_BYTES),
            ]),
        ),
        (
            "crc64nvme",
            map_bytes(&[
                ("algorithm", string_bytes("crc64nvme".len())),
                ("value", string_bytes(16)),
            ]),
        ),
    ],
);
/// One append's piece, apart from its bytes: the entry's fields and a base
/// in another content object.
const APPEND_PIECE_BYTES: usize = map_bytes(&[
    ("content_id", string_bytes(MAX_ID_BYTES)),
    ("offset", INTEGER_BYTES),
    ("bytes", string_bytes(0)),
    (
        "base",
        map_bytes(&[
            ("owner_namespace_id", string_bytes(MAX_ID_BYTES)),
            ("content_id", string_bytes(MAX_ID_BYTES)),
        ]),
    ),
]);
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
/// The longest encoded rights list: every right name with its framing,
/// plus the array header.
const fn rights_list_bytes() -> usize {
    let mut bytes = 1;
    let mut index = 0;
    while index < AccessRight::ALL.len() {
        bytes += AccessRight::ALL[index].as_str().len() + 2;
        index += 1;
    }
    bytes
}
const ACCESS_BYTES: usize = delta_bytes(
    "append_access_revision",
    &[
        ("inode_id", INTEGER_BYTES),
        ("access_revision_no", INTEGER_BYTES),
        ("boundary", 1),
        (
            "grants",
            9 + MAX_ACCESS_GRANTS_PRINCIPAL_BYTES
                + MAX_ACCESS_GRANT_ENTRIES * (2 + 3 + rights_list_bytes()),
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
        ("semantic_operation_index", INDEX_BYTES),
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

pub(crate) fn estimated_wal_record_bytes(
    request: &CommitRequest,
    inline_content: &[InlineContent],
) -> usize {
    let fixed_bytes = map_bytes(&[
        ("committed_seq", INTEGER_BYTES),
        ("commit_id", string_bytes(request.commit_id.as_str().len())),
        (
            "committed_by",
            string_bytes(request.actor_id.as_str().len()),
        ),
        (
            "semantic_commit_fingerprint",
            string_bytes("v1:sha256:".len() + 64),
        ),
        ("committed_at_ms", INTEGER_BYTES),
        (
            "message",
            string_bytes(request.message.as_ref().map_or(0, String::len)),
        ),
        ("deltas", 9),
    ]);
    let carries_pieces = !inline_content.is_empty()
        || request
            .operations
            .iter()
            .any(|operation| operation.appended_content().is_some());
    let inline_bytes = if !carries_pieces {
        0
    } else {
        inline_content
            .iter()
            .fold(string_bytes("inline_content".len()) + 9, |bytes, value| {
                bytes.saturating_add(map_bytes(&[
                    (
                        "content_id",
                        string_bytes(value.content_ref().content_id.as_str().len()),
                    ),
                    ("offset", INTEGER_BYTES),
                    ("bytes", string_bytes(value.bytes().len())),
                ]))
            })
    };
    request.operations.iter().fold(
        fixed_bytes.saturating_add(inline_bytes),
        |bytes, operation| bytes.saturating_add(operation_bytes(operation)),
    )
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
        | FilesystemOperation::RestoreRevision { .. }
        | FilesystemOperation::RestoreRevisionByInode { .. } => REVISION_BYTES,
        FilesystemOperation::AppendFile { inline_content, .. }
        | FilesystemOperation::AppendFileByInode { inline_content, .. } => {
            REVISION_BYTES + APPEND_PIECE_BYTES + inline_content.len()
        }
        FilesystemOperation::DeletePath { .. } | FilesystemOperation::DeleteByInode { .. } => {
            DELETE_BYTES
        }
        FilesystemOperation::MovePath { .. } | FilesystemOperation::MoveByInode { .. } => {
            DELETE_BYTES + UNBIND_BYTES + BIND_BYTES + NAMES_BYTES
        }
        FilesystemOperation::CopyPath { .. } | FilesystemOperation::CopyByInode { .. } => {
            CREATE_INODE_BYTES + BIND_BYTES + NAMES_BYTES + REVISION_BYTES + ATTRIBUTES_BYTES
        }
        FilesystemOperation::Undelete { .. } => REVOKE_BYTES + BIND_BYTES + NAMES_BYTES,
        FilesystemOperation::UpdateAttributes { .. }
        | FilesystemOperation::UpdateAttributesByInode { .. } => ATTRIBUTES_BYTES,
        FilesystemOperation::UpdateAccess { .. }
        | FilesystemOperation::UpdateAccessByInode { .. } => ACCESS_BYTES,
    }
}

/// The estimate runs before the namespace's naming mode is loaded, so it
/// counts the longer of the two keys a component can have.
fn create_path_bytes(path: &AbsolutePath) -> usize {
    path.components().iter().fold(0_usize, |bytes, component| {
        let display_name = component.as_str();
        let key_bytes =
            |naming| loonfs_types::name_key_for_display_name(naming, display_name).len();
        let name_key_bytes = key_bytes(NamespaceNaming::CaseInsensitive)
            .max(key_bytes(NamespaceNaming::CaseSensitive));
        bytes.saturating_add(
            CREATE_INODE_BYTES
                + BIND_BYTES
                + string_bytes(display_name.len())
                + string_bytes(name_key_bytes),
        )
    })
}

#[cfg(test)]
#[path = "commit_wal_size_tests.rs"]
mod tests;
