//! The attribute family: its row keys, the fold that drops superseded
//! revisions, and the sequence-correct lookup over published tables.

use super::*;
use crate::metadata::{AttributesRevisionRecord, MetadataStateBuilder, MetadataView};
use loonfs_api::{AttributeKey, AttributeRevisionNo, AttributeValue, Attributes};
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
        delta_index: 0,
        actor: loonfs_api::ActorRef::loonfs_system(),
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

fn attribute_rows(state: &MetadataState) -> BTreeMap<ApiMetadataTableFamily, Vec<MetadataRow>> {
    BTreeMap::from([(
        ApiMetadataTableFamily::Attributes,
        manifest_rows_for_family(state, ApiMetadataTableFamily::Attributes),
    )])
}

fn kept_revisions(
    rows_by_family: &BTreeMap<ApiMetadataTableFamily, Vec<MetadataRow>>,
) -> Vec<(u64, u64)> {
    rows_by_family
        .get(&ApiMetadataTableFamily::Attributes)
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

/// Rows for one inode sort newest revision first, which is what makes the
/// first row of a prefix scan the answer.
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

/// A cleared map is the state a caller asked for. Dropping its row would let
/// the older non-empty map become the newest one and resurrect attributes the
/// clear removed.
#[test]
fn the_fold_keeps_a_latest_empty_revision() {
    let state = state_from_attributes(vec![
        attributes_record(InodeId(7), 1, 3, &[("owner", "ada")]),
        AttributesRevisionRecord {
            inode_id: InodeId(7),
            attributes_revision_no: AttributeRevisionNo(2),
            committed_seq: ChangeSeq(4),
            delta_index: 0,
            actor: loonfs_api::ActorRef::loonfs_system(),
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
        .remove(&ApiMetadataTableFamily::Attributes)
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

/// A deleted inode keeps its attributes, which is what makes an undelete give
/// back the map the inode had.
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

/// One inode's revisions are numbered without repeats, so two rows sharing a
/// number make "the newest at the floor" arbitrary. Refuse to compact rather
/// than pick one.
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

/// The published family answers a read at the sequence it asks for: a row
/// committed after that sequence is skipped, and the newest one at or below
/// it is the answer. This is what a checkpoint-basis or fork-basis read
/// depends on.
#[tokio::test]
async fn a_published_table_answers_at_the_sequence_the_read_asks_for() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let state = state_from_attributes(vec![
        attributes_record(InodeId(7), 1, 3, &[("owner", "ada")]),
        attributes_record(InodeId(7), 2, 6, &[("owner", "grace")]),
        attributes_record(InodeId(7), 3, 9, &[("owner", "hopper")]),
    ]);

    let tables = build_manifest_tables(
        &store,
        &namespace_id,
        ChangeSeq(9),
        CHECKPOINT_BASE_RUN_LEVEL,
        &state,
        NonZeroUsize::new(64).expect("segment row budget"),
    )
    .await
    .expect("build tables");
    let attribute_segments = tables
        .iter()
        .find(|table| table.family == ApiMetadataTableFamily::Attributes)
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

    let manifest = publish_manifest_with_tables(
        &store,
        &namespace_id,
        ManifestId(1),
        ChangeSeq(9),
        flatten_manifest_tables(tables),
    )
    .await;
    let verified = load_verified_manifest_tables(&store, &namespace_id, &manifest)
        .await
        .expect("load manifest tables");

    for (visible_seq, expected_revision, expected_owner) in [
        (9_u64, 3_u64, "hopper"),
        (8, 2, "grace"),
        (6, 2, "grace"),
        (5, 1, "ada"),
    ] {
        let view = MetadataView::over_manifest_tables(&verified, ChangeSeq(visible_seq));
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
    let view = MetadataView::over_manifest_tables(&verified, ChangeSeq(2));
    assert_eq!(
        view.attributes_at_visible_seq(InodeId(7))
            .await
            .expect("read attributes"),
        (AttributeRevisionNo(0), Attributes::default())
    );
    let view = MetadataView::over_manifest_tables(&verified, ChangeSeq(9));
    assert_eq!(
        view.attributes_at_visible_seq(InodeId(8))
            .await
            .expect("read attributes"),
        (AttributeRevisionNo(0), Attributes::default())
    );
}

async fn publish_manifest_with_tables<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_id: ManifestId,
    head_seq: ChangeSeq,
    metadata_files: Vec<MetadataFileRef>,
) -> ManifestObjectId {
    let manifest_object_id = ManifestObjectId::generate(manifest_id);
    let manifest = NamespaceManifestEnvelope::from_payload(NamespaceManifestPayload {
        namespace_id: namespace_id.clone(),
        manifest_id,
        manifest_object_id: manifest_object_id.clone(),
        head_seq,
        head_commit_id: CommitId::parse("c_00000000000000000000000000000001").expect("commit id"),
        base_seq: head_seq,
        writer_epoch: loonfs_api::WriterEpoch(1),
        next_inode_id: InodeId(64),
        retention_floor_seq: ChangeSeq(0),
        metadata_files,
    })
    .expect("manifest envelope");
    write_namespace_manifest(store, &manifest)
        .await
        .expect("write manifest");
    manifest_object_id
}
