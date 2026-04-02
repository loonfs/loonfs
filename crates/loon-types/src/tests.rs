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
