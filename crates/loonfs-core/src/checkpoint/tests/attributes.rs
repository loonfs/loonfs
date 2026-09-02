//! The attribute family: its row keys, the fold that drops superseded
//! revisions, and the sequence-correct lookup over published segments.

use super::*;
use crate::metadata::{AttributesRevisionRecord, MetadataStateBuilder, MetadataView};
use loonfs_api::{AttributeKey, AttributeRevisionNo, AttributeValue, Attributes, RunNo};
use std::collections::BTreeMap;

fn attributes(entries: &[(&str, &str)]) -> Attributes {
    Attributes::new(
        entries
            .iter()
            .map(|(key, value)| {
                (
                    AttributeKey::parse(key).expect("attribute key"),
                    AttributeValue::parse(value).expect("attribute value"),
                )
            })
            .collect(),
    )
    .expect("attribute map")
}

fn attributes_record(
    inode_id: InodeId,
    revision: u64,
    seq: u64,
    entries: &[(&str, &str)],
) -> AttributesRevisionRecord {
    AttributesRevisionRecord {
        inode_id,
        attributes_revision_no: AttributeRevisionNo(revision),
        committed_seq: ChangeSeq(seq),
        commit_id: CommitId::parse(format!("c_attributes_{seq}")).expect("commit id"),
        delta_index: 0,
        updated_by: loonfs_api::ActorRef::loonfs_system(),
        updated_at_ms: 1_000 + seq,
        attributes: attributes(entries),
    }
}

fn state_from_attributes(records: Vec<AttributesRevisionRecord>) -> MetadataState {
    let mut builder = MetadataStateBuilder::default();
    for record in records {
        builder.push_attributes_revision(record);
    }
    builder.finish()
}

fn attribute_rows(state: &MetadataState) -> BTreeMap<ApiMetadataRowFamily, Vec<MetadataRow>> {
    BTreeMap::from([(
        ApiMetadataRowFamily::Attributes,
        manifest_rows_for_family(state, ApiMetadataRowFamily::Attributes),
    )])
}

fn kept_revisions(
    rows_by_family: &BTreeMap<ApiMetadataRowFamily, Vec<MetadataRow>>,
) -> Vec<(u64, u64)> {
    rows_by_family
        .get(&ApiMetadataRowFamily::Attributes)
        .expect("family rows")
        .iter()
        .map(|row| match row {
            MetadataRow::AttributesRevision {
                inode_id,
                attributes_revision_no,
                ..
            } => (inode_id.0, attributes_revision_no.0),
            other => panic!("unexpected row: {other:?}"),
        })
        .collect()
}

#[test]
fn attribute_rows_sort_newest_first_per_inode() {
    let state = state_from_attributes(vec![
        attributes_record(InodeId(7), 1, 4, &[("owner", "ada")]),
        attributes_record(InodeId(7), 2, 6, &[("owner", "grace")]),
        attributes_record(InodeId(8), 1, 5, &[("owner", "ada")]),
    ]);

    assert_eq!(
        kept_revisions(&attribute_rows(&state)),
        vec![(7, 2), (7, 1), (8, 1)]
    );
}

#[test]
fn the_fold_keeps_every_revision_above_the_floor_and_the_newest_at_it() {
    let state = state_from_attributes(vec![
        attributes_record(InodeId(7), 1, 3, &[("owner", "ada")]),
        attributes_record(InodeId(7), 2, 5, &[("owner", "grace")]),
        attributes_record(InodeId(7), 3, 9, &[("owner", "hopper")]),
        attributes_record(InodeId(8), 1, 4, &[("owner", "ada")]),
    ]);
    let mut rows_by_family = attribute_rows(&state);

    fold_rows_with_retention(
        MetadataFamilyGroup::Attributes,
        &mut rows_by_family,
        ChangeSeq(6),
    )
    .expect("fold attributes");

    assert_eq!(
        kept_revisions(&rows_by_family),
        vec![(7, 3), (7, 2), (8, 1)],
        "revision 1 of inode 7 is superseded at the floor and goes; \
         the newest at the floor and everything above it stay"
    );
}

#[test]
fn the_fold_keeps_a_latest_empty_revision() {
    let state = state_from_attributes(vec![
        attributes_record(InodeId(7), 1, 3, &[("owner", "ada")]),
        AttributesRevisionRecord {
            inode_id: InodeId(7),
            attributes_revision_no: AttributeRevisionNo(2),
            committed_seq: ChangeSeq(4),
            commit_id: CommitId::parse("c_attributes_4").expect("commit id"),
            delta_index: 0,
            updated_by: loonfs_api::ActorRef::loonfs_system(),
            updated_at_ms: 1_004,
            attributes: Attributes::default(),
        },
    ]);
    let mut rows_by_family = attribute_rows(&state);

    fold_rows_with_retention(
        MetadataFamilyGroup::Attributes,
        &mut rows_by_family,
        ChangeSeq(9),
    )
    .expect("fold attributes");

    let kept = rows_by_family
        .remove(&ApiMetadataRowFamily::Attributes)
        .expect("family rows");
    assert!(
        matches!(
            kept.as_slice(),
            [MetadataRow::AttributesRevision {
                attributes_revision_no: AttributeRevisionNo(2),
                attributes,
                ..
            }] if attributes.is_empty()
        ),
        "only the cleared revision survives: {kept:?}"
    );
}

#[test]
fn the_fold_never_drops_attributes_for_being_unreachable() {
    let state = state_from_attributes(vec![attributes_record(
        InodeId(7),
        1,
        3,
        &[("owner", "ada")],
    )]);
    let mut rows_by_family = attribute_rows(&state);
    // No inode or bind rows travel with the attribute group, so nothing here
    // could establish reachability even if the rule wanted to.
    fold_rows_with_retention(
        MetadataFamilyGroup::Attributes,
        &mut rows_by_family,
        ChangeSeq(10_000),
    )
    .expect("fold attributes");

    assert_eq!(kept_revisions(&rows_by_family), vec![(7, 1)]);
}

#[test]
fn the_fold_refuses_to_compact_repeated_revision_numbers() {
    let state = state_from_attributes(vec![
        attributes_record(InodeId(7), 2, 3, &[("owner", "ada")]),
        attributes_record(InodeId(7), 3, 4, &[("owner", "grace")]),
        attributes_record(InodeId(7), 2, 5, &[("owner", "hopper")]),
    ]);
    let mut rows_by_family = attribute_rows(&state);

    let error = fold_rows_with_retention(
        MetadataFamilyGroup::Attributes,
        &mut rows_by_family,
        ChangeSeq(9),
    )
    .expect_err("repeated revision numbers are corruption");

    assert!(
        matches!(&error, CoreError::NamespaceCorrupt(_)),
        "{error:?}"
    );
    assert!(error.to_string().contains("two attribute rows"), "{error}");
}

#[tokio::test]
async fn a_published_segment_answers_at_the_sequence_the_read_asks_for() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let state = state_from_attributes(vec![
        attributes_record(InodeId(7), 1, 3, &[("owner", "ada")]),
        attributes_record(InodeId(7), 2, 6, &[("owner", "grace")]),
        attributes_record(InodeId(7), 3, 9, &[("owner", "hopper")]),
    ]);

    let segments = build_manifest_segments(
        &store,
        &namespace_id,
        &state,
        NonZeroUsize::new(64).expect("segment row budget"),
    )
    .await
    .expect("build segments");
    let attribute_segments = segments
        .iter()
        .find(|family_segments| family_segments.family == ApiMetadataRowFamily::Attributes)
        .expect("the flush writes the attribute family")
        .segments
        .clone();
    assert_eq!(
        attribute_segments
            .iter()
            .map(|descriptor| descriptor.row_count)
            .sum::<u64>(),
        3,
        "every attribute revision is published"
    );

    let manifest = publish_manifest_with_segments(
        &store,
        &namespace_id,
        ManifestNo(1),
        ChangeSeq(9),
        flatten_manifest_segments(segments),
    )
    .await;
    let verified = load_verified_manifest_segments(&store, None, &namespace_id, &manifest)
        .await
        .expect("load manifest segments");

    for (visible_seq, expected_revision, expected_owner) in [
        (9_u64, 3_u64, "hopper"),
        (8, 2, "grace"),
        (6, 2, "grace"),
        (5, 1, "ada"),
    ] {
        let view = MetadataView::over_manifest_segments(&verified, ChangeSeq(visible_seq));
        let (revision, map) = view
            .attributes_at_visible_seq(InodeId(7))
            .await
            .expect("read attributes");
        assert_eq!(revision, AttributeRevisionNo(expected_revision));
        assert_eq!(
            map,
            attributes(&[("owner", expected_owner)]),
            "at seq {visible_seq}"
        );
    }

    // Below every row, and for an inode that has none: the concrete empty
    // state, not a missing answer.
    let view = MetadataView::over_manifest_segments(&verified, ChangeSeq(2));
    assert_eq!(
        view.attributes_at_visible_seq(InodeId(7))
            .await
            .expect("read attributes"),
        (AttributeRevisionNo(0), Attributes::default())
    );
    let view = MetadataView::over_manifest_segments(&verified, ChangeSeq(9));
    assert_eq!(
        view.attributes_at_visible_seq(InodeId(8))
            .await
            .expect("read attributes"),
        (AttributeRevisionNo(0), Attributes::default())
    );
}

async fn publish_manifest_with_segments<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_no: ManifestNo,
    head_seq: ChangeSeq,
    segments: Vec<MetadataSegmentRef>,
) -> ManifestObjectId {
    let manifest_object_id = ManifestObjectId::generate(manifest_no);
    let manifest = NamespaceManifestEnvelope::from_payload(NamespaceManifestPayload {
        namespace_id: namespace_id.clone(),
        manifest_no,
        manifest_object_id: manifest_object_id.clone(),
        head_seq,
        head_commit_id: CommitId::parse("c_00000000000000000000000000000001").expect("commit id"),
        base_seq: head_seq,
        writer_epoch: loonfs_api::WriterEpoch(1),
        next_inode_id: InodeId(64),
        next_run_no: RunNo(1),
        retention_floor_seq: ChangeSeq(0),
        runs: vec![MetadataRunRef {
            run_no: RunNo(0),
            run_seq: head_seq,
            tier: RunTier::Base,
            segments,
        }],
    })
    .expect("manifest envelope");
    write_namespace_manifest(store, &manifest)
        .await
        .expect("write manifest");
    manifest_object_id
}
