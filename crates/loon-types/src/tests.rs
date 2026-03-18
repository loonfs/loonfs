use super::*;

#[test]
fn namespace_id_serializes_as_string() {
    let namespace_id = NamespaceId::from("ns-home");
    let json = serde_json::to_string(&namespace_id).expect("serialize namespace id");
    let round_trip: NamespaceId = serde_json::from_str(&json).expect("deserialize namespace id");

    assert_eq!(json, "\"ns-home\"");
    assert_eq!(round_trip, namespace_id);
}

#[test]
fn control_object_envelope_computes_payload_checksum() {
    let state = HeadState {
        namespace_id: NamespaceId::from("ns-1"),
        seq: ChangeSeq(41),
        active_fence_token: FenceToken(8),
        next_inode_id: InodeId(501),
        snapshot_hint_seq: Some(ChangeSeq(40)),
        retention_floor_seq: ChangeSeq(40),
    };

    let envelope =
        ControlObjectEnvelope::from_state(ControlObjectKind::NamespaceHead, "test-writer", state)
            .expect("build envelope");

    assert_eq!(envelope.kind, ControlObjectKind::NamespaceHead);
    assert_eq!(envelope.format_version, CONTROL_OBJECT_FORMAT_VERSION);
    assert!(envelope
        .has_valid_payload_checksum()
        .expect("recompute payload checksum"));
}

#[test]
fn lease_state_expiration_is_explicit() {
    let lease = LeaseState {
        namespace_id: NamespaceId::from("ns-1"),
        holder_id: "writer-a".to_owned(),
        fence_token: FenceToken(8),
        lease_expires_at_ms: 1_000,
    };

    assert!(lease.is_valid_at(999));
    assert!(!lease.is_valid_at(1_000));
}

#[test]
fn checksum_helper_matches_envelope_value() {
    let state = LeaseState {
        namespace_id: NamespaceId::from("ns-1"),
        holder_id: "writer-a".to_owned(),
        fence_token: FenceToken(8),
        lease_expires_at_ms: 1_000,
    };

    let envelope =
        ControlObjectEnvelope::from_state(ControlObjectKind::NamespaceLease, "test-writer", state)
            .expect("build envelope");

    assert_eq!(
        envelope.payload_checksum_sha256,
        payload_checksum_sha256(&envelope.state).expect("recompute checksum")
    );
}

#[test]
fn progress_state_envelope_round_trips_through_json() {
    let state = ProgressState {
        namespace_id: NamespaceId::from("ns-1"),
        work_class: "BuildListingIndex".to_owned(),
        through_seq: ChangeSeq(42),
    };
    let envelope = ControlObjectEnvelope::from_state(
        ControlObjectKind::NamespaceProgress,
        "test-writer",
        state.clone(),
    )
    .expect("build progress envelope");
    let encoded = serde_json::to_vec(&envelope).expect("encode progress envelope");
    let decoded: ControlObjectEnvelope<ProgressState> =
        serde_json::from_slice(&encoded).expect("decode progress envelope");

    assert_eq!(decoded.kind, ControlObjectKind::NamespaceProgress);
    assert_eq!(decoded.format_version, CONTROL_OBJECT_FORMAT_VERSION);
    assert_eq!(decoded.state, state);
    assert!(decoded
        .has_valid_payload_checksum()
        .expect("recompute progress checksum"));
}

#[test]
fn content_manifest_round_trips_through_json() {
    let payload = sample_content_manifest_payload();
    let envelope =
        ContentManifestEnvelope::from_payload(payload.clone()).expect("build content manifest");

    let encoded = encode_content_manifest_json(&envelope).expect("encode content manifest");
    let decoded = decode_content_manifest_json(&encoded).expect("decode content manifest");

    assert_eq!(decoded.kind, ContentManifestKind::NamespaceContentManifest);
    assert_eq!(decoded.format_version, CONTENT_MANIFEST_FORMAT_VERSION);
    assert_eq!(decoded.payload, payload);
    assert!(decoded
        .has_valid_payload_checksum()
        .expect("recompute content manifest checksum"));
}

#[test]
fn content_manifest_checksum_detects_tampering() {
    let payload = sample_content_manifest_payload();
    let mut envelope =
        ContentManifestEnvelope::from_payload(payload).expect("build content manifest");
    envelope.payload.file_size_bytes = 17;

    let encoded = encode_content_manifest_json(&envelope).expect("encode content manifest");
    let error = decode_content_manifest_json(&encoded).expect_err("tampered manifest should fail");

    assert!(matches!(
        error,
        ContentManifestCodecError::ChecksumMismatch { .. }
    ));
}

#[test]
fn content_manifest_digest_is_content_addressed() {
    let payload = sample_content_manifest_payload();
    let envelope =
        ContentManifestEnvelope::from_payload(payload.clone()).expect("build content manifest");
    let encoded = encode_content_manifest_json(&envelope).expect("encode content manifest");

    assert_eq!(
        content_manifest_digest_sha256(&envelope).expect("compute content manifest digest"),
        sha256_digest(&encoded)
    );
    assert_eq!(
        envelope.payload_checksum_sha256,
        content_manifest_payload_checksum_sha256(&payload)
            .expect("recompute content manifest payload checksum")
    );
}

#[test]
fn wal_commit_envelope_round_trips_through_cbor_zstd() {
    let payload = sample_wal_payload();
    let envelope = WalCommitEnvelope::from_payload("test-writer", payload.clone())
        .expect("build WAL envelope");

    let encoded = encode_wal_commit_envelope_zstd(&envelope).expect("encode WAL envelope");
    let decoded = decode_wal_commit_envelope_zstd(&encoded).expect("decode WAL envelope");

    assert_eq!(decoded.kind, envelope.kind);
    assert_eq!(decoded.format_version, WAL_FORMAT_VERSION);
    assert_eq!(decoded.payload, payload);
    assert!(decoded
        .has_valid_payload_checksum()
        .expect("recompute payload checksum"));
}

#[test]
fn wal_payload_checksum_detects_tampering() {
    let payload = sample_wal_payload();
    let mut envelope =
        WalCommitEnvelope::from_payload("test-writer", payload).expect("build WAL envelope");

    envelope.payload.seq = ChangeSeq(43);
    let encoded = encode_wal_commit_envelope_zstd(&envelope).expect("encode WAL envelope");
    let error = decode_wal_commit_envelope_zstd(&encoded).expect_err("tampering should fail");

    assert!(matches!(error, WalCodecError::ChecksumMismatch { .. }));
}

#[test]
fn wal_checksum_helper_matches_envelope_value() {
    let payload = sample_wal_payload();
    let envelope =
        WalCommitEnvelope::from_payload("test-writer", payload).expect("build WAL envelope");

    assert_eq!(
        envelope.payload_checksum_sha256,
        wal_payload_checksum_sha256(&envelope.payload).expect("recompute checksum")
    );
}

#[test]
fn checkpoint_manifest_round_trips_through_json() {
    let payload = sample_checkpoint_manifest_payload();
    let envelope = CheckpointManifestEnvelope::from_payload("test-writer", payload.clone())
        .expect("build manifest envelope");

    let encoded = encode_checkpoint_manifest_json(&envelope).expect("encode manifest");
    let decoded = decode_checkpoint_manifest_json(&encoded).expect("decode manifest");

    assert_eq!(
        decoded.kind,
        CheckpointManifestKind::NamespaceCheckpointManifest
    );
    assert_eq!(decoded.format_version, CHECKPOINT_MANIFEST_FORMAT_VERSION);
    assert_eq!(decoded.payload, payload);
    assert!(decoded
        .has_valid_payload_checksum()
        .expect("recompute manifest checksum"));
}

#[test]
fn checkpoint_manifest_checksum_detects_tampering() {
    let payload = sample_checkpoint_manifest_payload();
    let mut envelope = CheckpointManifestEnvelope::from_payload("test-writer", payload)
        .expect("build manifest envelope");
    envelope.payload.checkpoint_seq = ChangeSeq(41);

    let encoded = encode_checkpoint_manifest_json(&envelope).expect("encode manifest");
    let error = decode_checkpoint_manifest_json(&encoded).expect_err("tampering should fail");

    assert!(matches!(
        error,
        CheckpointManifestCodecError::ChecksumMismatch { .. }
    ));
}

#[test]
fn checkpoint_manifest_checksum_helper_matches_envelope_value() {
    let payload = sample_checkpoint_manifest_payload();
    let envelope = CheckpointManifestEnvelope::from_payload("test-writer", payload)
        .expect("build manifest envelope");

    assert_eq!(
        envelope.payload_checksum_sha256,
        checkpoint_manifest_payload_checksum_sha256(&envelope.payload)
            .expect("recompute manifest checksum")
    );
}

#[test]
fn checkpoint_segment_round_trips_through_cbor_zstd() {
    let payload = sample_checkpoint_segment_payload();
    let envelope = CheckpointSegmentEnvelope::from_payload("test-writer", payload.clone())
        .expect("build checkpoint segment envelope");

    let encoded =
        encode_checkpoint_segment_envelope_zstd(&envelope).expect("encode checkpoint segment");
    let decoded =
        decode_checkpoint_segment_envelope_zstd(&encoded).expect("decode checkpoint segment");

    assert_eq!(
        decoded.kind,
        CheckpointSegmentKind::NamespaceCheckpointSegment
    );
    assert_eq!(decoded.format_version, CHECKPOINT_SEGMENT_FORMAT_VERSION);
    assert_eq!(decoded.payload, payload);
    assert!(decoded
        .has_valid_payload_checksum()
        .expect("recompute checkpoint segment checksum"));
    assert_eq!(
        decoded
            .page_checksums_sha256()
            .expect("compute page checksums from payload"),
        vec![checkpoint_page_checksum_sha256(&decoded.payload.pages[0])
            .expect("compute page checksum")]
    );
}

#[test]
fn checkpoint_segment_checksum_detects_tampering() {
    let payload = sample_checkpoint_segment_payload();
    let mut envelope = CheckpointSegmentEnvelope::from_payload("test-writer", payload)
        .expect("build checkpoint segment envelope");
    envelope.payload.row_count = 3;

    let encoded =
        encode_checkpoint_segment_envelope_zstd(&envelope).expect("encode checkpoint segment");
    let error = decode_checkpoint_segment_envelope_zstd(&encoded)
        .expect_err("tampered checkpoint segment should fail");

    assert!(matches!(
        error,
        CheckpointSegmentCodecError::ChecksumMismatch { .. }
    ));
}

#[test]
fn checkpoint_segment_checksum_helper_matches_envelope_value() {
    let payload = sample_checkpoint_segment_payload();
    let envelope = CheckpointSegmentEnvelope::from_payload("test-writer", payload)
        .expect("build checkpoint segment envelope");

    assert_eq!(
        envelope.payload_checksum_sha256,
        checkpoint_segment_payload_checksum_sha256(&envelope.payload)
            .expect("recompute checkpoint segment checksum")
    );
}

fn sample_wal_payload() -> WalCommitPayload {
    WalCommitPayload {
        namespace_id: NamespaceId::from("ns-1"),
        seq: ChangeSeq(42),
        base_head_seq: ChangeSeq(41),
        commit_id: "req-20260311-0001".to_owned(),
        request_id: "req-20260311-0001".to_owned(),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(8),
        ops: vec![WalOp::ReplaceFile {
            op_index: 0,
            inode_id: InodeId(42),
            base_revision: RevisionNo(7),
            content_manifest_digest: "sha256:manifest".to_owned(),
        }],
        preconditions: vec![
            WalPrecondition::HeadSeqIs(ChangeSeq(41)),
            WalPrecondition::InodeRevisionIs {
                inode_id: InodeId(42),
                revision: RevisionNo(7),
            },
        ],
    }
}

fn sample_content_manifest_payload() -> ContentManifestPayload {
    ContentManifestPayload {
        namespace_id: NamespaceId::from("ns-1"),
        file_size_bytes: 16,
        file_digest_sha256: sha256_digest(b"hello from loon\n"),
        block_size_bytes: CONTENT_BLOCK_SIZE_BYTES,
        blocks: vec![ContentBlockDescriptor {
            content_digest_sha256: sha256_digest(b"hello from loon\n"),
            plaintext_size_bytes: 16,
        }],
    }
}

fn sample_checkpoint_manifest_payload() -> CheckpointManifestPayload {
    CheckpointManifestPayload {
        namespace_id: NamespaceId::from("ns-1"),
        checkpoint_seq: ChangeSeq(40),
        active_fence_token: FenceToken(8),
        next_inode_id: InodeId(501),
        retention_floor_seq: ChangeSeq(40),
        verified: true,
        tables: vec![CheckpointTableManifest {
            family: CheckpointTableFamily::Inodes,
            segments: vec![CheckpointSegmentDescriptor {
                object_key:
                    "namespaces/ns-1/snapshots/00000000000000000040/tables/inodes-00000.sst.zst"
                        .to_owned(),
                segment_index: 0,
                row_count: 500,
                min_key: "inode-1".to_owned(),
                max_key: "inode-500".to_owned(),
                payload_checksum_sha256: "seg-checksum-1".to_owned(),
                page_checksums_sha256: vec!["page-checksum-1".to_owned()],
            }],
        }],
    }
}

fn sample_checkpoint_segment_payload() -> CheckpointSegmentPayload {
    CheckpointSegmentPayload {
        namespace_id: NamespaceId::from("ns-1"),
        checkpoint_seq: ChangeSeq(40),
        family: CheckpointTableFamily::Inodes,
        segment_index: 0,
        row_count: 2,
        min_key: "inode-1".to_owned(),
        max_key: "inode-2".to_owned(),
        pages: vec![CheckpointPage {
            page_index: 0,
            min_key: "inode-1".to_owned(),
            max_key: "inode-2".to_owned(),
            row_keys: vec!["inode-1".to_owned(), "inode-2".to_owned()],
            rows: vec![
                CheckpointRow::Inode {
                    inode_id: InodeId(1),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                CheckpointRow::Inode {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(2),
                },
            ],
        }],
    }
}
