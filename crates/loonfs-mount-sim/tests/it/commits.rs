//! Commits: chained appends, the per-operation and request-level precondition errors, and commit
//! receipts.

use crate::support::{append, fixture, mkdir, name, path, put, request, Fixture};
use loonfs_client::{NamespacePath, UndeleteDestination};
use loonfs_types::api::v0::{CommitPrecondition, FilesystemChange, FilesystemOperation};
use loonfs_types::{
    AttributesRevisionNo, BindingVersion, ChangeSeq, DeleteDirectoryBehavior,
    DestinationPrecondition, ErrorCode, InodeId, RevisionNo, ROOT_INODE_ID,
};

const KIB: usize = 1024;

fn content_changes(events: &[FilesystemChange]) -> Vec<(RevisionNo, u64, Option<u64>, String)> {
    events
        .iter()
        .map(|event| match event {
            FilesystemChange::ContentChanged {
                revision_no,
                content_ref,
                append_offset,
                ..
            } => (
                *revision_no,
                content_ref.size_bytes,
                *append_offset,
                content_ref.content_id.to_string(),
            ),
            other => panic!("expected a content change, got {other:?}"),
        })
        .collect()
}

#[tokio::test]
async fn appends_chained_in_one_request_extend_one_chain_in_order() {
    let fixture = fixture();
    let created = fixture.commit(vec![put("/log", &[b'a'; 100])]);
    let FilesystemChange::FileCreated {
        inode_id,
        content_ref,
        ..
    } = &created.events[0]
    else {
        panic!("expected a file creation, got {:?}", created.events);
    };
    let tails = [
        vec![b'b'; 256 * KIB],
        vec![b'c'; 256 * KIB],
        vec![b'd'; 88 * KIB],
    ];
    let operations = (1..)
        .zip(&tails)
        .map(|(base, tail)| FilesystemOperation::AppendFileByInode {
            inode_id: *inode_id,
            inline_content: tail.clone(),
            expected_revision_no: Some(RevisionNo(base)),
        })
        .collect();
    let appended = fixture.commit(operations);

    let chain = content_ref.content_id.to_string();
    let (first, second, third) = (100, 100 + 256 * 1024, 100 + 512 * 1024);
    assert_eq!(
        content_changes(&appended.events),
        vec![
            (RevisionNo(2), second, Some(first), chain.clone()),
            (RevisionNo(3), third, Some(second), chain.clone()),
            (RevisionNo(4), third + 88 * 1024, Some(third), chain),
        ]
    );
    let bytes = fixture
        .client
        .read_file(&NamespacePath::parse("demo", "/log").expect("namespace path"))
        .await
        .expect("read the appended file");
    let expected: Vec<u8> = [vec![b'a'; 100]]
        .iter()
        .chain(&tails)
        .flatten()
        .copied()
        .collect();
    assert_eq!(bytes, expected);
}

#[tokio::test]
async fn an_append_to_a_shorter_reference_of_a_chain_starts_a_new_chain() {
    let fixture = fixture();
    fixture.commit(vec![put("/a", b"abc")]);
    let a = fixture.stat("/a").await.inode_id;
    fixture.commit(vec![FilesystemOperation::CopyByInode {
        inode_id: a,
        destination_parent_inode_id: ROOT_INODE_ID,
        destination_display_name: name("b"),
        precondition: DestinationPrecondition::default(),
    }]);
    let b = fixture.stat("/b").await.inode_id;
    let extended = fixture.commit(vec![append(a, b"de")]);
    let forked = fixture.commit(vec![append(b, b"XY")]);

    let a_changes = content_changes(&extended.events);
    let b_changes = content_changes(&forked.events);
    let [(_, _, _, a_chain)] = a_changes.as_slice() else {
        panic!("one append");
    };
    let [(_, size, offset, b_chain)] = b_changes.as_slice() else {
        panic!("one append");
    };
    assert_eq!((*size, *offset), (5, Some(3)));
    assert_ne!(b_chain, a_chain, "`/b` no longer ends where the chain does");
    let read = |file: &'static str| {
        let client = fixture.client.clone();
        async move {
            client
                .read_file(&NamespacePath::parse("demo", file).expect("namespace path"))
                .await
                .expect("read")
        }
    };
    assert_eq!(read("/a").await, b"abcde");
    assert_eq!(read("/b").await, b"abcXY");
}

/// Sets up `/a` (a file at revision 1), `/dir` (a directory), and `/dir/b`, and returns the
/// inode IDs of `/a` and `/dir`.
async fn tree(fixture: &Fixture) -> (InodeId, InodeId) {
    fixture.commit(vec![put("/a", b"a"), mkdir("/dir"), put("/dir/b", b"b")]);
    (
        fixture.stat("/a").await.inode_id,
        fixture.stat("/dir").await.inode_id,
    )
}

#[tokio::test]
async fn an_operation_error_reports_its_code_and_the_operation_that_stopped_the_request() {
    let fixture = fixture();
    let (a, dir) = tree(&fixture).await;
    let a_version = fixture
        .stat("/a")
        .await
        .binding_version
        .expect("a binding version");
    let stale = |inode_id| FilesystemOperation::PutFileRevisionByInode {
        inode_id,
        content_ref: None,
        inline_content: Some(b"x".to_vec()),
        expected_revision_no: RevisionNo(9),
    };
    let cases: Vec<(Vec<FilesystemOperation>, ErrorCode, u32)> = vec![
        (vec![mkdir("/x"), mkdir("/x")], ErrorCode::PathConflict, 1),
        (
            vec![put("/new", b"n"), stale(a)],
            ErrorCode::StaleRevision,
            1,
        ),
        (
            vec![
                append(a, b"1"),
                FilesystemOperation::AppendFileByInode {
                    inode_id: a,
                    inline_content: b"2".to_vec(),
                    expected_revision_no: Some(RevisionNo(1)),
                },
            ],
            ErrorCode::StaleRevision,
            1,
        ),
        (
            vec![FilesystemOperation::DeletePath {
                path: path("/dir"),
                behavior: DeleteDirectoryBehavior::NonRecursive,
                expected_inode_id: None,
            }],
            ErrorCode::DirectoryNotEmpty,
            0,
        ),
        (
            vec![
                mkdir("/y"),
                FilesystemOperation::MoveByInode {
                    inode_id: a,
                    expected_binding_version: BindingVersion::parse("bm90LWEtdG9rZW4")
                        .expect("token"),
                    destination_parent_inode_id: dir,
                    destination_display_name: name("a"),
                    precondition: DestinationPrecondition::default(),
                },
            ],
            ErrorCode::InvalidRequest,
            1,
        ),
        (
            vec![FilesystemOperation::MoveByInode {
                inode_id: dir,
                expected_binding_version: fixture
                    .stat("/dir")
                    .await
                    .binding_version
                    .expect("version"),
                destination_parent_inode_id: dir,
                destination_display_name: name("inside"),
                precondition: DestinationPrecondition::default(),
            }],
            ErrorCode::WouldCycle,
            0,
        ),
        (
            vec![
                FilesystemOperation::MoveByInode {
                    inode_id: a,
                    expected_binding_version: a_version.clone(),
                    destination_parent_inode_id: dir,
                    destination_display_name: name("moved"),
                    precondition: DestinationPrecondition::default(),
                },
                FilesystemOperation::DeleteByInode {
                    inode_id: a,
                    expected_binding_version: a_version,
                    behavior: DeleteDirectoryBehavior::NonRecursive,
                },
            ],
            ErrorCode::BindingVersionMismatch,
            1,
        ),
        (
            vec![FilesystemOperation::UpdateAttributesByInode {
                inode_id: a,
                set: [(
                    loonfs_types::AttributeKey::parse("k").expect("key"),
                    loonfs_types::AttributeValue::parse("v").expect("value"),
                )]
                .into(),
                remove: Vec::new(),
                expected_attributes_revision_no: Some(AttributesRevisionNo(5)),
            }],
            ErrorCode::StaleAttributes,
            0,
        ),
        (
            vec![FilesystemOperation::undelete(
                a,
                ChangeSeq(1),
                &UndeleteDestination::Recorded,
            )],
            ErrorCode::NotDeleted,
            0,
        ),
        (
            vec![mkdir("/z"), append(dir, b"x")],
            ErrorCode::PathConflict,
            1,
        ),
        (vec![append(InodeId(99), b"x")], ErrorCode::InodeNotFound, 0),
        (vec![mkdir("/")], ErrorCode::InvalidRequest, 0),
    ];
    for (operations, code, operation_index) in cases {
        let request = request(operations);
        let error = fixture
            .try_commit(&request)
            .expect_err("the request should fail");
        assert_eq!(
            (error.code(), error.details().operation_index),
            (code, Some(operation_index)),
            "{error}"
        );
        assert_eq!(error.details().commit_id.as_ref(), Some(&request.commit_id));
    }
    assert_eq!(
        fixture.server.head_seq(&fixture.namespace_id),
        ChangeSeq(1),
        "nothing committed"
    );
}

#[tokio::test]
async fn a_request_precondition_is_checked_against_the_state_before_the_commit() {
    let fixture = fixture();
    let (a, _) = tree(&fixture).await;
    let a_version = fixture
        .stat("/a")
        .await
        .binding_version
        .expect("a binding version");
    fixture.commit(vec![FilesystemOperation::MoveByInode {
        inode_id: a,
        expected_binding_version: a_version.clone(),
        destination_parent_inode_id: ROOT_INODE_ID,
        destination_display_name: name("a2"),
        precondition: DestinationPrecondition::default(),
    }]);
    let head = fixture.server.head_seq(&fixture.namespace_id);
    let cases = [
        (
            vec![
                CommitPrecondition::NamespaceHead {
                    expected_head_seq: head,
                },
                CommitPrecondition::FileRevision {
                    inode_id: a,
                    expected_revision_no: RevisionNo(9),
                },
            ],
            ErrorCode::StaleRevision,
            1,
        ),
        (
            vec![CommitPrecondition::NamespaceHead {
                expected_head_seq: ChangeSeq(1),
            }],
            ErrorCode::StaleHead,
            0,
        ),
        (
            vec![CommitPrecondition::PathBinding {
                path: path("/a2"),
                expected_inode_id: InodeId(99),
                expected_binding_version: None,
            }],
            ErrorCode::PathConflict,
            0,
        ),
        (
            vec![CommitPrecondition::PathAbsence { path: path("/a2") }],
            ErrorCode::PathConflict,
            0,
        ),
        (
            vec![CommitPrecondition::NameAbsence {
                parent_inode_id: ROOT_INODE_ID,
                display_name: name("dir"),
            }],
            ErrorCode::PathConflict,
            0,
        ),
        (
            vec![CommitPrecondition::InodeBinding {
                inode_id: a,
                expected_binding_version: a_version,
            }],
            ErrorCode::BindingVersionMismatch,
            0,
        ),
        (
            vec![CommitPrecondition::AttributesRevision {
                inode_id: a,
                expected_attributes_revision_no: AttributesRevisionNo(3),
            }],
            ErrorCode::StaleAttributes,
            0,
        ),
    ];
    for (preconditions, code, precondition_index) in cases {
        let request = request(vec![put("/new", b"n")]).preconditions(preconditions);
        let error = fixture
            .try_commit(&request)
            .expect_err("a precondition should fail");
        assert_eq!(
            (error.code(), error.details().precondition_index),
            (code, Some(precondition_index)),
            "{error}"
        );
    }
    // Preconditions read the state before the operations, which then see each other.
    let request = request(vec![put("/new", b"n"), append(a, b"!")]).preconditions(vec![
        CommitPrecondition::PathAbsence { path: path("/new") },
        CommitPrecondition::FileRevision {
            inode_id: a,
            expected_revision_no: RevisionNo(1),
        },
    ]);
    fixture
        .try_commit(&request)
        .expect("the preconditions hold before the commit");
}

#[tokio::test]
async fn a_retried_commit_replays_and_a_changed_one_conflicts() {
    let fixture = fixture();
    let original = request(vec![put("/f", b"one")]);
    let committed = fixture.try_commit(&original).expect("commit");
    fixture.commit(vec![put("/g", b"later")]);

    let replayed = fixture.try_commit(&original).expect("a retry replays");
    assert_eq!(replayed, committed);
    assert_eq!(fixture.server.head_seq(&fixture.namespace_id), ChangeSeq(2));

    let mut changed = original.clone();
    changed.operations = vec![put("/f", b"two")];
    let error = fixture
        .try_commit(&changed)
        .expect_err("changed bytes conflict");
    assert_eq!(error.code(), ErrorCode::CommitIdReuseConflict);
    assert_eq!(error.details().committed_seq, Some(committed.committed_seq));
    assert!(error.details().committed_fingerprint.is_some());
}

#[tokio::test]
async fn a_replacing_copy_appends_a_revision_and_a_vacant_copy_takes_the_attributes() {
    let fixture = fixture();
    fixture.commit(vec![put("/source", b"new bytes"), put("/target", b"old")]);
    let (source, target) = (fixture.stat("/source").await, fixture.stat("/target").await);
    let mode = [(
        loonfs_types::AttributeKey::parse("fs.mode").expect("key"),
        loonfs_types::AttributeValue::parse("755").expect("value"),
    )];
    fixture.commit(vec![FilesystemOperation::UpdateAttributes {
        path: path("/source"),
        set: mode.clone().into(),
        remove: Vec::new(),
        expected_inode_id: Some(source.inode_id),
        expected_attributes_revision_no: Some(AttributesRevisionNo(0)),
    }]);
    let copy =
        |name_: &str,
         behavior: loonfs_types::DestinationBehavior,
         expected_revision_no: Option<RevisionNo>| FilesystemOperation::CopyByInode {
            inode_id: source.inode_id,
            destination_parent_inode_id: ROOT_INODE_ID,
            destination_display_name: name(name_),
            precondition: DestinationPrecondition {
                behavior,
                expected_inode_id: expected_revision_no.map(|_| target.inode_id),
                expected_revision_no,
            },
        };
    use loonfs_types::DestinationBehavior::{NoReplace, Replace};
    let replaced = fixture.commit(vec![copy("target", Replace, Some(RevisionNo(1)))]);
    let [FilesystemChange::ContentChanged {
        inode_id,
        revision_no,
        content_ref,
        append_offset: None,
    }] = replaced.events.as_slice()
    else {
        panic!(
            "a replacing copy publishes one revision, got {:?}",
            replaced.events
        );
    };
    assert_eq!((*inode_id, *revision_no), (target.inode_id, RevisionNo(2)));
    assert_eq!(
        Some(content_ref),
        source.content_ref(),
        "the copy shares the source's content"
    );
    let after = fixture.stat("/target").await;
    assert_eq!(
        after.attributes.expect("attributes").attributes_revision_no,
        AttributesRevisionNo(0)
    );

    let vacant = fixture.commit(vec![copy("fresh", NoReplace, None)]);
    let kinds: Vec<_> = vacant
        .events
        .iter()
        .map(|event| match event {
            FilesystemChange::FileCreated { .. } => "file_created",
            FilesystemChange::AttributesChanged { .. } => "attributes_changed",
            other => panic!("unexpected event {other:?}"),
        })
        .collect();
    assert_eq!(kinds, ["file_created", "attributes_changed"]);
    let fresh = fixture.stat("/fresh").await.attributes.expect("attributes");
    assert_eq!(
        (fresh.attributes_revision_no, fresh.attributes.len()),
        (AttributesRevisionNo(1), 1)
    );
}
