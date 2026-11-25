//! Unit tests for WAL record validation and CBOR encoding.

use super::*;
use std::assert_matches;

fn activity(
    content_bytes: u64,
    file_revisions: u64,
    mutations: u64,
) -> Option<crate::manifest::ManifestActivity> {
    let counter = |value| crate::manifest::ActivityCounter::parse(value).expect("activity counter");
    Some(crate::manifest::ManifestActivity {
        content_bytes: counter(content_bytes),
        file_revisions: counter(file_revisions),
        mutations: counter(mutations),
    })
}

#[test]
fn committed_activity_counts_full_revisions_and_semantic_groups() {
    let payload = inline_wal_object(&[5, 5, 0]);
    let mut record = payload.records()[0].clone();
    record.inline_content.clear();
    record.deltas = payload
        .records()
        .iter()
        .flat_map(|record| record.deltas.iter().cloned())
        .collect();
    // Several deltas in one group count once, and indices can have gaps.
    record.deltas[0].semantic_operation_index = 2;
    record.deltas[1].semantic_operation_index = 2;
    record.deltas[2].semantic_operation_index = 9;
    assert_eq!(committed_activity(&record), activity(10, 3, 2));
    let mut empty = record.clone();
    empty.deltas.clear();
    assert_eq!(committed_activity(&empty), Some(Default::default()));
    if let WalDelta::AppendFileRevision { content_ref, .. } = &mut record.deltas[0].delta {
        content_ref.size_bytes = crate::MAX_PUBLIC_INTEGER;
    }
    assert_eq!(committed_activity(&record), None);
}

#[test]
fn committed_activity_counts_appended_bytes_once() {
    let mut record = appended_wal_object().records()[0].clone();
    assert_eq!(committed_activity(&record), activity(5, 1, 1));
    let repeated = record.deltas[0].clone();
    record.deltas.push(WalCommitDelta {
        semantic_operation_index: 1,
        ..repeated
    });
    assert_eq!(committed_activity(&record), activity(13, 2, 2));
}

fn inline_wal_object(lengths: &[usize]) -> WalObjectPayload {
    let namespace_id = NamespaceId::parse("bounded").expect("namespace");
    let records: Vec<WalCommitPayload> = lengths
        .iter()
        .enumerate()
        .map(|(index, &length)| {
            let bytes = vec![42; length];
            let content_id =
                ContentId::parse("con_014d2pf2dbsqqg28t5cy4tqkff").expect("content id");
            WalCommitPayload {
                committed_seq: CommitSeq(index as u64 + 1),
                commit_id: CommitId::parse(format!("c_{index:032x}")).expect("commit id"),
                committed_by: crate::ActorId::parse("test").expect("actor"),
                semantic_commit_fingerprint: CommitFingerprint::parse(format!(
                    "v1:sha256:{}",
                    crate::sha256_hex(b"test")
                ))
                .expect("fingerprint"),
                committed_at_ms: 0,
                message: None,
                merge: None,
                deltas: vec![WalCommitDelta {
                    semantic_operation_index: 0,
                    delta: WalDelta::AppendFileRevision {
                        delta_index: 0,
                        inode_id: InodeId(2),
                        parent_inode_id: crate::ROOT_INODE_ID,
                        name_key: NameKey::parse("file.txt").expect("name key"),
                        display_name: DisplayName::parse("file.txt").expect("display name"),
                        bind_position: DeltaPosition {
                            seq: CommitSeq(1),
                            delta_index: 0,
                        },
                        child_created_by: crate::ActorId::parse("test").expect("actor"),
                        child_created_at_ms: 0,
                        revision_no: RevisionNo(index as u64 + 1),
                        content_ref: ContentRef::blob_v1(
                            content_id.clone(),
                            &bytes,
                            crate::ChecksumAlgorithm::Crc64nvme,
                        ),
                        layout: None,
                        append_offset: None,
                    },
                }],
                inline_content: vec![WalInlineContent {
                    content_id,
                    offset: 0,
                    bytes,
                }],
            }
        })
        .collect();

    WalObjectPayload {
        namespace_id,
        wal_no: WalNo(1),
        writer_epoch: WriterEpoch(1),
        head_seq: CommitSeq(lengths.len() as u64),
        body: WalObjectBody::Commits(WalCommits::new(records).expect("commits")),
    }
}

/// Returns the commits of a data WAL object for a test to edit.
fn commits_mut(payload: &mut WalObjectPayload) -> &mut WalCommits {
    match &mut payload.body {
        WalObjectBody::Commits(commits) => commits,
        other => panic!("expected a data WAL object, got {other:?}"),
    }
}

fn unchecked_wal_object_bytes(payload: &WalObjectPayload) -> Vec<u8> {
    let mut payload_bytes = Vec::new();
    into_writer(&WalPayloadDocument::from(payload), &mut payload_bytes)
        .expect("encode payload directly");
    let document = WalObjectDocument {
        kind: WalEnvelopeKind::WalObject.as_str().to_owned(),
        format_version: WAL_FORMAT_VERSION,
        payload_checksum: sha256_digest(&payload_bytes),
        payload: payload_bytes,
        padding: None,
    };
    let mut document_bytes = Vec::new();
    into_writer(&document, &mut document_bytes).expect("encode document directly");
    zstd::stream::encode_all(document_bytes.as_slice(), 0).expect("compress document")
}

fn assert_inline_content_rejected(
    payload: WalObjectPayload,
    record_index: usize,
    expected_reason: &str,
) {
    let expected_seq = payload.records()[record_index].committed_seq;
    let expected_content_id = payload.records()[record_index].inline_content[0]
        .content_id
        .clone();
    let decoded_error = decode_wal_object_envelope_zstd(&unchecked_wal_object_bytes(&payload))
        .expect_err("invalid inline content should not decode");
    let encoded_error = encode_wal_object_envelope_zstd(payload)
        .expect_err("invalid inline content should not encode");
    for error in [decoded_error, encoded_error] {
        assert_eq!(
            error.to_string(),
            format!(
                "invalid WAL inline content in commit `{expected_seq}` for `content_id` `{expected_content_id}`: {expected_reason}"
            ),
        );
        match error {
            WalCodecError::InvalidWalInlineContent {
                seq,
                content_id,
                reason,
            } => {
                assert_eq!(seq, expected_seq);
                assert_eq!(content_id, expected_content_id);
                assert_eq!(reason, expected_reason);
            }
            other => panic!("expected invalid inline content, got {other:?}"),
        }
    }
}

fn assert_inline_content_accepted(payload: WalObjectPayload) {
    let encoded = encode_wal_object_envelope_zstd(payload.clone()).expect("encode WAL object");
    let decoded = decode_wal_object_envelope_zstd(encoded.as_bytes()).expect("decode WAL object");
    assert_eq!(decoded.into_payload(), payload);
}

#[test]
fn inline_content_requires_a_revision_reference_in_the_same_commit() {
    let expected_reason = "no `append_file_revision` reference in the same commit";
    let mut missing = inline_wal_object(&[3, 3]);
    commits_mut(&mut missing)[0].deltas.clear();
    assert_inline_content_rejected(missing, 0, expected_reason);

    let mut wrong_id = inline_wal_object(&[3]);
    commits_mut(&mut wrong_id)[0].inline_content[0].content_id =
        ContentId::parse("con_7yvjx9gxjm688fxq5tk1v58cgg").expect("content id");
    assert_inline_content_rejected(wrong_id, 0, expected_reason);
}

/// Checks rule 1 of the WAL records: a layout of a chain with pieces in
/// the commit records the WAL object's namespace as its creator, and a
/// layout of a chain without pieces, such as a copy's, can record any
/// member of the pool.
#[test]
fn a_layout_of_a_chain_with_pieces_records_the_wal_namespace_as_its_creator() {
    let with_creator = |payload: &mut WalObjectPayload, creator: &str| {
        if let WalDelta::AppendFileRevision {
            layout: Some(layout),
            ..
        } = &mut commits_mut(payload)[0].deltas[0].delta
        {
            layout.creator_namespace_id = NamespaceId::parse(creator).expect("namespace");
        }
    };
    assert_inline_content_accepted(appended_wal_object());
    let mut payload = appended_wal_object();
    with_creator(&mut payload, "other");
    assert_inline_content_rejected(
        payload,
        0,
        "a layout of a chain with pieces records another namespace as its creator",
    );

    let mut copy = appended_wal_object();
    commits_mut(&mut copy)[0].inline_content.clear();
    if let WalDelta::AppendFileRevision {
        content_ref,
        layout: Some(layout),
        ..
    } = &mut commits_mut(&mut copy)[0].deltas[0].delta
    {
        content_ref.size_bytes = layout.size_bytes();
    }
    with_creator(&mut copy, "other");
    assert_inline_content_accepted(copy);
}

#[test]
fn inline_content_must_end_at_the_longest_reference() {
    let expected_reason = "entries do not end at the reference `size_bytes`";
    let mut payload = inline_wal_object(&[3]);
    commits_mut(&mut payload)[0].inline_content[0]
        .bytes
        .push(42);
    assert_inline_content_rejected(payload, 0, expected_reason);

    let mut payload = inline_wal_object(&[3]);
    let mut other_reference = commits_mut(&mut payload)[0].deltas[0].clone();
    other_reference.semantic_operation_index = 1;
    match &mut other_reference.delta {
        WalDelta::AppendFileRevision {
            delta_index,
            revision_no,
            content_ref,
            ..
        } => {
            *delta_index = 1;
            *revision_no = RevisionNo(2);
            content_ref.size_bytes += 1;
        }
        other => panic!("expected file revision, got {other:?}"),
    }
    commits_mut(&mut payload)[0].deltas.push(other_reference);
    assert_inline_content_rejected(payload, 0, expected_reason);
}

#[test]
fn inline_content_offsets_must_be_unique_within_each_commit() {
    let mut payload = inline_wal_object(&[0]);
    let entry = commits_mut(&mut payload)[0].inline_content[0].clone();
    commits_mut(&mut payload)[0].inline_content.push(entry);
    assert_inline_content_rejected(payload, 0, "duplicate `content_id` and `offset` in commit");
}

/// One commit appending `[3, 8)` to a content id, as two entries.
fn appended_wal_object() -> WalObjectPayload {
    let mut payload = inline_wal_object(&[8]);
    let namespace_id = payload.namespace_id.clone();
    let record = &mut commits_mut(&mut payload)[0];
    let bytes = std::mem::take(&mut record.inline_content[0].bytes);
    let entry = record.inline_content[0].clone();
    if let WalDelta::AppendFileRevision {
        content_ref,
        layout,
        ..
    } = &mut record.deltas[0].delta
    {
        *layout = Some(crate::ContentLayout {
            creator_namespace_id: namespace_id,
            extents: vec![crate::ContentExtent {
                content_id: content_ref.content_id.clone(),
                object: crate::ExtentObject::Whole,
                object_bytes: 3,
                object_checksum: crate::Checksum::crc64nvme(b""),
                offset: 0,
                length: 3,
            }],
        });
    }
    record.inline_content = vec![
        WalInlineContent {
            offset: 3,
            bytes: bytes[3..5].to_vec(),
            ..entry.clone()
        },
        WalInlineContent {
            offset: 5,
            bytes: bytes[5..].to_vec(),
            ..entry
        },
    ];
    payload
}

#[test]
fn inline_entries_extend_contiguously() {
    assert_inline_content_accepted(appended_wal_object());
    let mut gap = appended_wal_object();
    commits_mut(&mut gap)[0].inline_content[1].offset = 6;
    commits_mut(&mut gap)[0].inline_content[1].bytes.pop();
    assert_inline_content_rejected(gap, 0, "entries are not contiguous");
}

#[test]
fn inline_content_accepts_the_value_limit_and_rejects_one_byte_more() {
    assert_eq!(MAX_WAL_INLINE_CONTENT_BYTES, 262_144);
    assert_inline_content_accepted(inline_wal_object(&[MAX_WAL_INLINE_CONTENT_BYTES]));
    assert_inline_content_rejected(
        inline_wal_object(&[MAX_WAL_INLINE_CONTENT_BYTES + 1]),
        0,
        "value exceeds `MAX_WAL_INLINE_CONTENT_BYTES`",
    );
}

#[test]
fn inline_content_accepts_the_wal_object_limit_and_rejects_one_byte_more() {
    assert_eq!(MAX_WAL_OBJECT_INLINE_CONTENT_BYTES, 4_194_304);
    let mut lengths = vec![
        MAX_WAL_INLINE_CONTENT_BYTES;
        MAX_WAL_OBJECT_INLINE_CONTENT_BYTES / MAX_WAL_INLINE_CONTENT_BYTES
    ];
    assert_inline_content_accepted(inline_wal_object(&lengths));
    lengths.push(1);
    assert_inline_content_rejected(
        inline_wal_object(&lengths),
        lengths.len() - 1,
        "WAL object inline total exceeds `MAX_WAL_OBJECT_INLINE_CONTENT_BYTES`",
    );
}

#[test]
fn inline_content_is_not_hashed_against_the_reference_checksum() {
    let mut payload = inline_wal_object(&[3]);
    commits_mut(&mut payload)[0].inline_content[0].bytes[0] = 43;
    assert_inline_content_accepted(payload);
}

#[test]
fn decoder_accepts_the_limit_and_rejects_the_next_byte_before_decoding() {
    let encoded = encode_wal_object_envelope_zstd(WalObjectPayload {
        namespace_id: NamespaceId::parse("n".repeat(crate::MAX_ID_BYTES)).expect("namespace"),
        wal_no: WalNo(crate::MAX_PUBLIC_INTEGER),
        writer_epoch: WriterEpoch(crate::MAX_PUBLIC_INTEGER),
        head_seq: CommitSeq(crate::MAX_PUBLIC_INTEGER),
        body: WalObjectBody::Floor {
            retention_floor_seq: CommitSeq(crate::MAX_PUBLIC_INTEGER),
        },
    })
    .expect("encode");
    let document = zstd::stream::decode_all(encoded.as_bytes()).expect("decompress");
    assert_eq!(document.len(), encoded.document_bytes());
    assert!(document.len() <= RECORDLESS_WAL_DOCUMENT_BYTES);
    assert_eq!(
        &decode_wal_object_envelope_zstd_with_limit(encoded.as_bytes(), document.len())
            .expect("at limit"),
        encoded.envelope(),
    );
    assert_matches!(
        decode_wal_object_envelope_zstd_with_limit(encoded.as_bytes(), document.len() - 1),
        Err(WalCodecError::Envelope(EnvelopeCodecError::DocumentTooLarge { max_bytes, .. }))
            if max_bytes == document.len() - 1
    );
    let invalid = zstd::stream::encode_all(&[0xff; 64][..], 0).expect("compress");
    assert_matches!(
        decode_wal_object_envelope_zstd_with_limit(&invalid, 8),
        Err(WalCodecError::Envelope(
            EnvelopeCodecError::DocumentTooLarge { max_bytes: 8, .. }
        ))
    );
    assert_matches!(
        decode_wal_object_envelope_zstd_with_limit(&invalid, 64),
        Err(WalCodecError::Envelope(EnvelopeCodecError::EnvelopeDecode(
            _
        )))
    );
}

#[test]
fn revision_layouts_equal_the_reference_size_or_first_piece_offset() {
    for (piece_offset, layout_size, valid) in [
        (None, Some(3), true),
        (None, Some(5), false),
        (None, Some(2), false),
        (None, None, false),
        (Some(2), Some(2), true),
        (Some(2), Some(1), false),
        (Some(2), Some(3), false),
        (Some(2), None, true),
        (Some(0), Some(1), false),
        (Some(0), None, true),
    ] {
        let mut payload = inline_wal_object(&[3]);
        if let Some(offset) = piece_offset {
            let piece = &mut commits_mut(&mut payload)[0].inline_content[0];
            piece.offset = offset;
            piece.bytes.drain(..crate::usize_from_u64(offset));
        } else {
            commits_mut(&mut payload)[0].inline_content.clear();
        }
        let namespace_id = payload.namespace_id.clone();
        let WalDelta::AppendFileRevision { layout, .. } =
            &mut commits_mut(&mut payload)[0].deltas[0].delta
        else {
            panic!("revision");
        };
        *layout = layout_size.map(|length| crate::ContentLayout {
            creator_namespace_id: namespace_id,
            extents: vec![crate::ContentExtent {
                content_id: ContentId::parse("con_7yvjx9gxjm688fxq5tk1v58cgg").expect("content"),
                object: crate::ExtentObject::Whole,
                object_bytes: length,
                object_checksum: crate::Checksum::crc64nvme(b""),
                offset: 0,
                length,
            }],
        });
        let decoded = decode_wal_object_envelope_zstd(&unchecked_wal_object_bytes(&payload));
        let encoded = encode_wal_object_envelope_zstd(payload);
        if valid {
            assert!(
                decoded.is_ok(),
                "{piece_offset:?}, {layout_size:?}: {decoded:?}"
            );
            assert!(encoded.is_ok());
        } else {
            if piece_offset == Some(0) {
                for error in [decoded.as_ref().err(), encoded.as_ref().err()] {
                    assert_matches!(
                        error,
                        Some(WalCodecError::InvalidWalRevisionLayout { reason, .. })
                        if *reason == "a whole value carries no layout"
                    );
                }
            }
            assert_matches!(decoded, Err(WalCodecError::InvalidWalRevisionLayout { .. }));
            assert_matches!(encoded, Err(WalCodecError::InvalidWalRevisionLayout { .. }));
        }
    }
}

/// Checks that encoding and decoding refuse a merge record when one of the
/// ranges that it covers contains no sequence, is of the committing
/// namespace, or is of a fork that another of its ranges covers, and that
/// the error names that range's fork.
#[test]
fn a_merge_record_covers_at_least_one_commit_of_another_namespace() {
    let range = |source: &str, after: u64, through: u64| ForkRange {
        source_namespace_id: NamespaceId::parse(source).expect("namespace"),
        source_after_seq: CommitSeq(after),
        source_through_seq: CommitSeq(through),
    };
    let merge = |own: ForkRange, covered: &[ForkRange]| MergeRecord {
        source_namespace_id: own.source_namespace_id,
        source_after_seq: own.source_after_seq,
        source_through_seq: own.source_through_seq,
        covered_ranges: covered.to_vec(),
        conflicts: Vec::new(),
    };
    let inverted = "`source_after_seq` must be below `source_through_seq`";
    let committing = "the source is the committing namespace";
    let twice = "the record covers the source twice";
    for (record, rejected) in [
        (merge(range("fork", 4, 9), &[]), None),
        (merge(range("fork", 4, 5), &[]), None),
        (merge(range("fork", 4, 9), &[range("original", 2, 6)]), None),
        (merge(range("fork", 9, 4), &[]), Some(("fork", inverted))),
        (merge(range("fork", 4, 4), &[]), Some(("fork", inverted))),
        (
            merge(range("bounded", 4, 9), &[]),
            Some(("bounded", committing)),
        ),
        (
            merge(range("fork", 4, 9), &[range("original", 6, 6)]),
            Some(("original", inverted)),
        ),
        (
            merge(range("fork", 4, 9), &[range("bounded", 2, 6)]),
            Some(("bounded", committing)),
        ),
        (
            merge(range("fork", 4, 9), &[range("fork", 2, 4)]),
            Some(("fork", twice)),
        ),
        (
            merge(
                range("fork", 4, 9),
                &[range("original", 2, 6), range("original", 6, 8)],
            ),
            Some(("original", twice)),
        ),
    ] {
        let mut payload = inline_wal_object(&[0]);
        commits_mut(&mut payload)[0].merge = Some(record.clone());
        let decoded = decode_wal_object_envelope_zstd(&unchecked_wal_object_bytes(&payload));
        let encoded = encode_wal_object_envelope_zstd(payload);
        match rejected {
            None => {
                assert!(decoded.is_ok(), "{record:?}: {decoded:?}");
                assert!(encoded.is_ok(), "{record:?}");
            }
            Some((source, expected)) => {
                for error in [decoded.err(), encoded.err()] {
                    assert_matches!(
                        error,
                        Some(WalCodecError::InvalidWalMerge {
                            seq: CommitSeq(1),
                            source_namespace_id,
                            reason,
                        }) if reason == expected && source_namespace_id.as_str() == source,
                        "{record:?}"
                    );
                }
            }
        }
    }
}

/// Checks that a replay commit's merge record round-trips its conflict
/// report, with a revision flattened into one version, and that the
/// report adds its field name and [`conflict_report_cbor_bytes`] to the
/// encoded payload. A conflict with an unknown field is rejected.
#[test]
fn a_merge_record_round_trips_its_conflict_report() {
    let without = MergeRecord {
        source_namespace_id: NamespaceId::parse("fork").expect("namespace"),
        source_after_seq: CommitSeq(4),
        source_through_seq: CommitSeq(9),
        covered_ranges: Vec::new(),
        conflicts: Vec::new(),
    };
    let version = |path: &str| crate::MergeConflictVersion {
        path: crate::AbsolutePath::parse(path).expect("path"),
        revision: Some(crate::MergeConflictRevision {
            revision_no: RevisionNo(3),
            content_ref: ContentRef::blob_v1(
                ContentId::parse("con_014d2pf2dbsqqg28t5cy4tqkff").expect("content id"),
                b"ours",
                crate::ChecksumAlgorithm::Crc64nvme,
            ),
        }),
    };
    let conflicts = vec![crate::MergeConflict {
        inode_id: InodeId(2),
        kind: crate::MergeConflictKind::Content,
        base: Some(version("/a.txt")),
        ours: Some(version("/a.txt")),
        theirs: Some(crate::MergeConflictVersion {
            path: crate::AbsolutePath::parse("/b").expect("path"),
            revision: None,
        }),
    }];
    let with = MergeRecord {
        conflicts: conflicts.clone(),
        ..without.clone()
    };
    let payload = |merge: &MergeRecord| {
        let mut payload = inline_wal_object(&[0]);
        commits_mut(&mut payload)[0].merge = Some(merge.clone());
        payload
    };
    let encoded =
        encode_wal_object_envelope_zstd(payload(&with)).expect("encode the replay commit");
    let decoded = decode_wal_object_envelope_zstd(&encoded.into_bytes()).expect("decode");
    assert_eq!(decoded.payload().records()[0].merge.as_ref(), Some(&with));
    let payload_bytes = |merge| {
        encode_wal_payload_cbor(&payload(merge))
            .expect("encode the payload")
            .len()
    };
    assert_eq!(
        payload_bytes(&with) - payload_bytes(&without),
        1 + "conflicts".len() + conflict_report_cbor_bytes(&conflicts)
    );

    let mut value = serde_json::to_value(&conflicts[0]).expect("conflict as JSON");
    value["unknown"] = serde_json::json!(1);
    let error = serde_json::from_value::<crate::MergeConflict>(value)
        .expect_err("a conflict with an unknown field");
    assert!(error.to_string().contains("unknown field"), "{error}");
}

/// Checks that a bind includes a revision summary exactly when its child
/// is a file, on both the write and the read side.
#[test]
fn a_bind_includes_a_revision_summary_exactly_when_its_child_is_a_file() {
    let summary = RevisionSummary {
        revision_no: RevisionNo(1),
        content_ref: ContentRef::blob_v1(
            ContentId::parse("con_014d2pf2dbsqqg28t5cy4tqkff").expect("content id"),
            b"hello",
            crate::ChecksumAlgorithm::Crc64nvme,
        ),
        revision_committed_by: crate::ActorId::parse("test").expect("actor"),
        revision_committed_at_ms: 0,
    };
    for (child_kind, revision, rejected) in [
        (InodeKind::File, Some(summary.clone()), None),
        (InodeKind::Directory, None, None),
        (
            InodeKind::File,
            None,
            Some("a file bind requires the file's revision summary"),
        ),
        (
            InodeKind::Directory,
            Some(summary),
            Some("a directory bind can't include a revision summary"),
        ),
    ] {
        let mut payload = inline_wal_object(&[0]);
        let record = &mut commits_mut(&mut payload)[0];
        record.inline_content.clear();
        record.deltas = vec![WalCommitDelta {
            semantic_operation_index: 0,
            delta: WalDelta::BindDirentry {
                delta_index: 0,
                parent_inode_id: crate::ROOT_INODE_ID,
                name_key: NameKey::parse("child").expect("name key"),
                display_name: DisplayName::parse("child").expect("display name"),
                child_inode_id: InodeId(2),
                child_kind,
                child_created_by: crate::ActorId::parse("test").expect("actor"),
                child_created_at_ms: 0,
                revision,
            },
        }];
        let decoded = decode_wal_object_envelope_zstd(&unchecked_wal_object_bytes(&payload));
        let encoded = encode_wal_object_envelope_zstd(payload);
        match rejected {
            None => {
                assert!(decoded.is_ok(), "{child_kind:?}: {decoded:?}");
                assert!(encoded.is_ok(), "{child_kind:?}");
            }
            Some(expected) => {
                for error in [decoded.err(), encoded.err()] {
                    assert_matches!(
                        error,
                        Some(WalCodecError::InvalidWalBind { reason, child_inode_id, .. })
                        if reason == expected && child_inode_id == InodeId(2)
                    );
                }
            }
        }
    }
}

fn tiny_commit_object() -> WalObjectPayload {
    WalObjectPayload {
        namespace_id: NamespaceId::parse("n").expect("namespace"),
        wal_no: WalNo(1),
        writer_epoch: WriterEpoch(1),
        head_seq: CommitSeq(1),
        body: WalObjectBody::Commits(
            WalCommits::new(vec![WalCommitPayload {
                committed_seq: CommitSeq(1),
                commit_id: CommitId::parse("c").expect("commit id"),
                committed_by: crate::ActorId::parse("a").expect("actor"),
                semantic_commit_fingerprint: CommitFingerprint::parse(format!(
                    "v1:sha256:{}",
                    crate::sha256_hex(b"test")
                ))
                .expect("fingerprint"),
                committed_at_ms: 0,
                message: None,
                merge: None,
                deltas: vec![WalCommitDelta {
                    semantic_operation_index: 0,
                    delta: WalDelta::CreateInode {
                        delta_index: 0,
                        inode_id: InodeId(2),
                        inode_kind: InodeKind::File,
                    },
                }],
                inline_content: Vec::new(),
            }])
            .expect("commits"),
        ),
    }
}

fn stored_document(bytes: &[u8]) -> WalObjectDocument {
    let document = zstd::stream::decode_all(bytes).expect("decompress");
    from_reader(document.as_slice()).expect("decode document")
}

#[test]
fn the_fence_and_floor_object_bound_is_zstds_bound_for_the_longest_recordless_document() {
    assert_eq!(RECORDLESS_WAL_DOCUMENT_BYTES, 508);
    assert_eq!(MAX_FENCE_OR_FLOOR_OBJECT_BYTES, 572);
    assert_eq!(
        MAX_FENCE_OR_FLOOR_OBJECT_BYTES,
        zstd::zstd_safe::compress_bound(RECORDLESS_WAL_DOCUMENT_BYTES),
    );
}

#[test]
fn every_fence_and_floor_object_is_stored_at_or_below_the_bound() {
    let hex_id = format!(
        "{}{}",
        crate::sha256_hex(b"first"),
        crate::sha256_hex(b"second")
    );
    let max = crate::MAX_PUBLIC_INTEGER;
    for namespace_id in ["n".repeat(crate::MAX_ID_BYTES), hex_id, "n".to_owned()] {
        for (integer, floor) in [(max, None), (max, Some(max)), (0, None), (0, Some(0))] {
            let encoded = encode_wal_object_envelope_zstd(WalObjectPayload {
                namespace_id: NamespaceId::parse(namespace_id.as_str()).expect("namespace"),
                wal_no: WalNo(integer),
                writer_epoch: WriterEpoch(integer),
                head_seq: CommitSeq(integer),
                body: floor.map_or(WalObjectBody::Fence, |floor| WalObjectBody::Floor {
                    retention_floor_seq: CommitSeq(floor),
                }),
            })
            .expect("encode");
            assert!(encoded.document_bytes() <= RECORDLESS_WAL_DOCUMENT_BYTES);
            assert!(
                encoded.as_bytes().len() <= MAX_FENCE_OR_FLOOR_OBJECT_BYTES,
                "{namespace_id} {integer} {floor:?}: {} bytes",
                encoded.as_bytes().len()
            );
            assert!(stored_document(encoded.as_bytes()).padding.is_none());
        }
    }
}

#[test]
fn a_small_commit_object_is_padded_above_the_bound() {
    let payload = tiny_commit_object();
    let unpadded = unchecked_wal_object_bytes(&payload);
    assert!(unpadded.len() <= MAX_FENCE_OR_FLOOR_OBJECT_BYTES);

    let encoded = encode_wal_object_envelope_zstd(payload.clone()).expect("encode");
    assert!(encoded.as_bytes().len() > MAX_FENCE_OR_FLOOR_OBJECT_BYTES);
    assert!(encoded.document_bytes() <= WAL_OBJECT_OVERHEAD_BYTES);
    let document = stored_document(encoded.as_bytes());
    assert!(document.padding.is_some());
    assert_eq!(document.payload_checksum, sha256_digest(&document.payload));

    let again = encode_wal_object_envelope_zstd(payload.clone()).expect("encode again");
    assert_eq!(again.as_bytes(), encoded.as_bytes());
    let decoded = decode_wal_object_envelope_zstd(encoded.as_bytes()).expect("decode");
    assert_eq!(decoded.into_payload(), payload);
}

#[test]
fn a_large_commit_object_has_no_padding() {
    let mut payload = inline_wal_object(&[2048]);
    commits_mut(&mut payload)[0].inline_content[0].bytes =
        wal_object_padding(&sha256_digest(b"incompressible"), 2048);
    let encoded = encode_wal_object_envelope_zstd(payload).expect("encode");
    assert!(encoded.as_bytes().len() > MAX_FENCE_OR_FLOOR_OBJECT_BYTES);
    assert!(stored_document(encoded.as_bytes()).padding.is_none());
}

#[test]
fn the_decoder_ignores_the_content_of_padding() {
    let payload = tiny_commit_object();
    let mut payload_bytes = Vec::new();
    into_writer(&WalPayloadDocument::from(&payload), &mut payload_bytes).expect("encode payload");
    let document = WalObjectDocument {
        kind: WalEnvelopeKind::WalObject.as_str().to_owned(),
        format_version: WAL_FORMAT_VERSION,
        payload_checksum: sha256_digest(&payload_bytes),
        payload: payload_bytes,
        padding: Some(vec![7; 3]),
    };
    let mut document_bytes = Vec::new();
    into_writer(&document, &mut document_bytes).expect("encode document");
    let stored = zstd::stream::encode_all(document_bytes.as_slice(), 0).expect("compress");
    let decoded = decode_wal_object_envelope_zstd(&stored).expect("decode");
    assert_eq!(decoded.into_payload(), payload);
}
