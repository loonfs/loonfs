//! What grep's entry decoder accepts and refuses.
//!
//! The entries these tests start from are the golden fixtures that the
//! `golden_formats` module pins, so no rejection probe has its own copy of a
//! durable encoding. A probe that needs an entry no encoder writes edits a
//! fixture. Core decodes the entry's `version` and `runs`, and grep decodes
//! the rest.

use crate::golden_formats::{
    read_golden, run_ref, segment_id, ACTIVE_ENTRY_FIXTURE, BACKFILLING_ENTRY_FIXTURE,
    DISABLED_ENTRY_FIXTURE,
};
use loonfs_grep::entry::{GrepEntry, GrepEntryError, GrepIndexStatus, GrepReorganizeState};
use loonfs_types::format::manifest::ExtensionEntry;
use loonfs_types::{ChangeSeq, RunNo};
use std::assert_matches;

/// Decodes a fixture after `edit` changed its JSON.
fn decode_edited(
    fixture: &str,
    edit: impl FnOnce(&mut serde_json::Value),
) -> Result<GrepEntry, GrepEntryError> {
    let mut document: serde_json::Value =
        serde_json::from_slice(&read_golden(fixture)).expect("decode a grep fixture");
    edit(&mut document);
    let entry: ExtensionEntry =
        serde_json::from_value(document).expect("core decodes the edited entry");
    GrepEntry::decode(&entry)
}

#[test]
fn the_entry_status_is_a_kind_tagged_object() {
    for fixture in [
        ACTIVE_ENTRY_FIXTURE,
        BACKFILLING_ENTRY_FIXTURE,
        DISABLED_ENTRY_FIXTURE,
    ] {
        let document: serde_json::Value =
            serde_json::from_slice(&read_golden(fixture)).expect("decode entry fixture");
        let tag = document
            .get("status")
            .expect("the entry writes a `status`")
            .as_object()
            .expect("the entry writes `status` as an object")
            .get("kind")
            .expect("the entry tags its `status` with `kind`");
        assert!(tag.is_string(), "the tag is a string, got {tag}");
    }
}

#[test]
fn the_decoder_rejects_unknown_members_at_every_level_grep_defines() {
    for path in ["", "/status", "/reorganize", "/reorganize/pruning"] {
        let decoded = decode_edited(ACTIVE_ENTRY_FIXTURE, |entry| {
            entry.pointer_mut(path).expect("fixture object")["field_from_the_future"] =
                serde_json::Value::from(true);
        });
        assert_matches!(
            decoded,
            Err(GrepEntryError::UndecodableState { message })
                if message.contains("field_from_the_future"),
            "path {path}"
        );
    }
}

#[test]
fn the_decoder_rejects_an_entry_that_omits_its_event_index() {
    let decoded = decode_edited(ACTIVE_ENTRY_FIXTURE, |entry| {
        entry
            .pointer_mut("/status")
            .expect("fixture object")
            .as_object_mut()
            .expect("object")
            .remove("next_event_index");
    });
    assert_matches!(decoded, Err(GrepEntryError::UndecodableState { .. }));
}

#[test]
fn the_decoder_rejects_other_versions_without_fallback() {
    for version in [0, 2] {
        let decoded = decode_edited(ACTIVE_ENTRY_FIXTURE, |entry| {
            entry["version"] = serde_json::Value::from(version);
        });
        assert_matches!(
            decoded,
            Err(GrepEntryError::UnsupportedVersion { version: found }) if found == version
        );
    }
}

#[test]
fn the_constructor_rejects_a_reorganization_whose_segments_the_entry_does_not_list() {
    let status = GrepIndexStatus::Active {
        captured_seq: ChangeSeq(0),
        built_through_seq: ChangeSeq(7),
        next_event_index: 0,
    };
    let reorganize = GrepReorganizeState {
        input_segment_ids: vec![segment_id(9)],
        output_segment_ids: Vec::new(),
        row_key_cursor: String::new(),
        output_level: 1,
        run_no: RunNo(1),
        pruning: None,
    };
    assert_matches!(
        GrepEntry::new(
            status.clone(),
            Some(reorganize.clone()),
            vec![run_ref(1, 0, &[1])]
        ),
        Err(GrepEntryError::MissingReorganizeInputSegment { .. })
    );
    let wrong_level = GrepReorganizeState {
        input_segment_ids: vec![segment_id(1)],
        output_segment_ids: vec![segment_id(2)],
        ..reorganize
    };
    assert_matches!(
        GrepEntry::new(status, Some(wrong_level), vec![run_ref(1, 0, &[1, 2])]),
        Err(GrepEntryError::ReorganizeOutputDescriptorMismatch { .. })
    );
}

#[test]
fn a_disabled_entry_lists_no_segments() {
    assert_matches!(
        GrepEntry::new(
            GrepIndexStatus::Disabled {},
            None,
            vec![run_ref(1, 0, &[1])]
        ),
        Err(GrepEntryError::DisabledHasRuns)
    );
    let decoded = decode_edited(DISABLED_ENTRY_FIXTURE, |entry| {
        entry["runs"] = serde_json::to_value(vec![run_ref(1, 0, &[1])]).expect("runs");
    });
    assert_matches!(decoded, Err(GrepEntryError::DisabledHasRuns));
}
