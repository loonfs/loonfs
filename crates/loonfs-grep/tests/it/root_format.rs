//! What the grep root and manifest decoders accept and refuse.
//!
//! The bytes these tests start from are the golden fixtures the
//! `golden_formats` module pins, so no rejection probe carries its own copy
//! of a durable encoding. A probe that needs a document no encoder writes
//! edits a fixture and restamps its checksum.

#![allow(clippy::panic)]

use crate::golden_formats::{
    read_golden, sample_active_manifest, sample_backfilling_manifest, sample_disabled_manifest,
    segment_id, segment_ref, ACTIVE_MANIFEST_FIXTURE, BACKFILLING_MANIFEST_FIXTURE,
    DISABLED_MANIFEST_FIXTURE, ROOT_POINTER_FIXTURE,
};
use loonfs_api::wire::envelope::EnvelopeCodecError;
use loonfs_api::{ChangeSeq, RunNo};
use loonfs_grep::root::{
    decode_grep_manifest, decode_grep_root, encode_grep_manifest, GrepEnvelopeCodecError,
    GrepIndexState, GrepIndexStatus, GrepManifestEnvelope, GrepManifestState,
    GrepManifestStateError, GrepReorganizeState,
};
use loonfs_test_support::ids::namespace_id;

/// One envelope field no grep encoder writes, spelled as the fragment a
/// probe appends after the payload.
const UNKNOWN_ENVELOPE_FIELD: &str = ",\"future_envelope\":{\"retained\":true}";

/// Rebuilds one envelope around a payload `edit` changed, restamping the
/// checksum so a probe tests the rule it names rather than the corruption
/// check that would otherwise fire first. `extra_envelope` is written after
/// the payload, for probes that need an envelope field no encoder writes.
/// The core control families are probed the same way in `loonfs-api`.
fn edited_document(
    fixture: &str,
    extra_envelope: &str,
    edit: impl FnOnce(&mut serde_json::Value),
) -> Vec<u8> {
    let mut document: serde_json::Value =
        serde_json::from_slice(&read_golden(fixture)).expect("decode a grep fixture");
    edit(&mut document["payload"]);
    let payload = serde_json::to_string(&document["payload"]).expect("encode the edited payload");
    let payload_checksum = serde_json::Value::from(loonfs_api::sha256_digest(payload.as_bytes()));
    format!(
        "{{\"kind\":{},\"format_version\":{},\"payload_checksum\":{payload_checksum},\
         \"payload\":{payload}{extra_envelope}}}",
        document["kind"], document["format_version"],
    )
    .into_bytes()
}

#[test]
fn every_status_round_trips_carrying_only_its_own_position() {
    for (state, absent_field) in [
        (sample_backfilling_manifest(), "built_through_seq"),
        (sample_active_manifest(ChangeSeq(11), 0), "target_seq"),
        (sample_disabled_manifest(), "built_through_seq"),
    ] {
        let encoded = encode_grep_manifest(
            &GrepManifestEnvelope::from_state(state.clone()).expect("build manifest envelope"),
        )
        .expect("encode grep manifest");
        assert!(
            !String::from_utf8(encoded.clone())
                .expect("manifest JSON is UTF-8")
                .contains(absent_field),
            "{:?} must not spell `{absent_field}`",
            state.status()
        );
        assert_eq!(
            decode_grep_manifest(&encoded)
                .expect("decode grep manifest")
                .manifest_state(),
            &state
        );
    }
}

#[test]
fn the_manifest_status_is_a_kind_tagged_object() {
    for fixture in [
        ACTIVE_MANIFEST_FIXTURE,
        BACKFILLING_MANIFEST_FIXTURE,
        DISABLED_MANIFEST_FIXTURE,
    ] {
        let document: serde_json::Value =
            serde_json::from_slice(&read_golden(fixture)).expect("decode manifest fixture");
        let payload = document["payload"].as_object().expect("object payload");
        assert!(
            !payload.contains_key("lifecycle") && !payload.contains_key("state"),
            "the manifest spells its lifecycle field `status`: {fixture}"
        );
        let tag = payload
            .get("status")
            .expect("the manifest writes a `status`")
            .as_object()
            .expect("the manifest writes `status` as an object")
            .get("kind")
            .expect("the manifest tags its `status` with `kind`");
        assert!(tag.is_string(), "the tag is a string, got {tag}");
    }
}

#[test]
fn manifest_decoder_rejects_unknown_fields_at_every_level() {
    let envelope = edited_document(ACTIVE_MANIFEST_FIXTURE, UNKNOWN_ENVELOPE_FIELD, |_| {});
    assert!(matches!(
        decode_grep_manifest(&envelope),
        Err(GrepEnvelopeCodecError::Envelope(
            EnvelopeCodecError::EnvelopeDecode(_)
        ))
    ));
    for path in [
        "",
        "/status",
        "/index",
        "/segments/0",
        "/segments/0/index_block",
    ] {
        let edited = edited_document(ACTIVE_MANIFEST_FIXTURE, "", |payload| {
            payload.pointer_mut(path).expect("fixture object")["field_from_the_future"] =
                serde_json::Value::from(true);
        });
        assert!(
            matches!(decode_grep_manifest(&edited),
            Err(GrepEnvelopeCodecError::Envelope(EnvelopeCodecError::PayloadDecode(message)))
                if message.contains("field_from_the_future")),
            "path {path}"
        );
    }
}

#[test]
fn mutable_pointer_payload_rejects_unknown_fields_as_corruption() {
    let edited = edited_document(ROOT_POINTER_FIXTURE, "", |payload| {
        payload["field_from_the_future"] = serde_json::Value::from(true);
    });

    assert!(matches!(
        decode_grep_root(&edited),
        Err(GrepEnvelopeCodecError::Envelope(
            EnvelopeCodecError::PayloadDecode(_)
        ))
    ));
}

#[test]
fn mutable_pointer_envelope_rejects_unknown_fields_as_corruption() {
    let edited = edited_document(ROOT_POINTER_FIXTURE, UNKNOWN_ENVELOPE_FIELD, |_| {});

    assert!(matches!(
        decode_grep_root(&edited),
        Err(GrepEnvelopeCodecError::Envelope(
            EnvelopeCodecError::EnvelopeDecode(_)
        ))
    ));
}

#[test]
fn decoder_rejects_the_string_format_version_without_a_shim() {
    let manifest = String::from_utf8(read_golden(DISABLED_MANIFEST_FIXTURE))
        .expect("manifest JSON is UTF-8")
        .replace("\"format_version\":1", "\"format_version\":\"v1\"");

    assert!(matches!(
        decode_grep_manifest(manifest.as_bytes()),
        Err(GrepEnvelopeCodecError::Envelope(
            EnvelopeCodecError::EnvelopeDecode(_)
        ))
    ));
    assert!(matches!(
        decode_grep_root(
            manifest
                .replacen("grep_manifest", "grep_root", 1)
                .as_bytes()
        ),
        Err(GrepEnvelopeCodecError::Envelope(
            EnvelopeCodecError::EnvelopeDecode(_)
        ))
    ));
}

#[test]
fn decoder_rejects_an_active_status_that_omits_its_event_index() {
    let edited = edited_document(ACTIVE_MANIFEST_FIXTURE, "", |payload| {
        payload["status"]
            .as_object_mut()
            .expect("the status is an object")
            .remove("next_event_index");
    });

    assert!(matches!(
        decode_grep_manifest(&edited),
        Err(GrepEnvelopeCodecError::Envelope(
            EnvelopeCodecError::PayloadDecode(_)
        ))
    ));
}

#[test]
fn decoder_rejects_unknown_version_without_fallback() {
    let wrong_version = String::from_utf8(read_golden(ACTIVE_MANIFEST_FIXTURE))
        .expect("manifest JSON is UTF-8")
        .replace("\"format_version\":1", "\"format_version\":7");

    assert!(matches!(
        decode_grep_manifest(wrong_version.as_bytes()),
        Err(GrepEnvelopeCodecError::Envelope(
            EnvelopeCodecError::UnsupportedFormatVersion { kind, found, supported }
        )) if kind == "grep_manifest" && found == 7 && supported == 1
    ));
}

#[test]
fn decoder_rejects_corrupted_checksum() {
    let corrupted = String::from_utf8(read_golden(ACTIVE_MANIFEST_FIXTURE))
        .expect("manifest JSON is UTF-8")
        .replacen("\"active\"", "\"disabled\"", 1);

    assert!(matches!(
        decode_grep_manifest(corrupted.as_bytes()),
        Err(GrepEnvelopeCodecError::Envelope(
            EnvelopeCodecError::ChecksumMismatch { .. }
        ))
    ));
}

#[test]
fn decoder_rejects_truncated_payload() {
    let manifest = read_golden(ACTIVE_MANIFEST_FIXTURE);
    let truncated = &manifest[..manifest.len() - 8];

    assert!(matches!(
        decode_grep_manifest(truncated),
        Err(GrepEnvelopeCodecError::Envelope(
            EnvelopeCodecError::EnvelopeDecode(_)
        ))
    ));
}

#[test]
fn constructor_rejects_reorganization_segment_mismatch() {
    let index = GrepIndexState {
        reorganize: Some(GrepReorganizeState {
            snapshot_segment_ids: vec![segment_id(9)],
            output_segment_ids: Vec::new(),
            row_key_cursor: String::new(),
            output_level: 1,
            run_no: RunNo(1),
        }),
        next_run_no: RunNo(2),
    };

    assert!(matches!(
        GrepManifestState::new(
            namespace_id("docs"),
            GrepIndexStatus::Active {
                built_through_seq: ChangeSeq(7),
                next_event_index: 0,
            },
            index,
            vec![segment_ref(1, 1, 0, 0)]
        ),
        Err(GrepManifestStateError::MissingReorganizeSnapshotSegment { .. })
    ));
}

#[test]
fn constructor_rejects_a_segment_with_no_rows() {
    let mut segment = segment_ref(1, 1, 0, 0);
    segment.row_count = 0;

    assert!(matches!(
        GrepManifestState::new(
            namespace_id("docs"),
            GrepIndexStatus::Active {
                built_through_seq: ChangeSeq(7),
                next_event_index: 0,
            },
            GrepIndexState {
                reorganize: None,
                next_run_no: RunNo(2),
            },
            vec![segment]
        ),
        Err(GrepManifestStateError::EmptySegment { .. })
    ));
}
