//! Access rows at historical sequences and effective rights across metadata sources.

use super::*;
use crate::metadata::access::{access_fixture_cases, access_fixture_state, effective_rights};
use crate::metadata::{AccessRevisionRecord, MetadataView};
use loonfs_api::{AccessGrants, AccessRevisionNo, AccessRight, PrincipalId};

fn access_record(
    revision: u64,
    seq: u64,
    rights: &[AccessRight],
    boundary: bool,
) -> AccessRevisionRecord {
    AccessRevisionRecord {
        inode_id: InodeId(7),
        access_revision_no: AccessRevisionNo(revision),
        committed_seq: ChangeSeq(seq),
        commit_id: CommitId::parse(format!("c_access_{seq}")).expect("commit id"),
        delta_index: 0,
        committed_by: loonfs_api::ActorId::loonfs(),
        committed_at_ms: 1_000 + seq,
        boundary,
        grants: AccessGrants::new(BTreeMap::from([(
            PrincipalId::parse("prn_ada").expect("principal id"),
            rights.iter().copied().collect(),
        )]))
        .expect("access grants"),
    }
}

#[tokio::test]
async fn a_published_segment_answers_at_the_sequence_the_read_asks_for() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let records = [
        access_record(1, 3, &[AccessRight::Read], false),
        access_record(2, 6, &[AccessRight::Read, AccessRight::Write], false),
        access_record(3, 9, &[AccessRight::Manage], true),
    ];
    let mut builder = MetadataStateBuilder::default();
    for record in &records {
        builder.push_access_revision(record.clone());
    }
    let state = builder.finish();

    let segments = build_manifest_segments(
        &store,
        &namespace_id,
        &state,
        MetadataLsmPolicy {
            max_rows_per_segment: NonZeroUsize::new(64).expect("segment row budget"),
            ..MetadataLsmPolicy::default()
        },
    )
    .await
    .expect("build segments");
    let access_segments = segments
        .iter()
        .find(|family_segments| family_segments.family == ApiMetadataRowFamily::Access)
        .expect("the flush writes the access family")
        .segments
        .clone();
    assert_eq!(
        access_segments
            .iter()
            .map(|descriptor| descriptor.row_count)
            .sum::<u64>(),
        3,
        "every access revision is published"
    );

    let manifest = publish_manifest_with_segments(
        &store,
        &namespace_id,
        ManifestNo(1),
        ChangeSeq(9),
        flatten_manifest_segments(segments),
    )
    .await;
    let verified = load_manifest_segments_for_inspection(&store, None, &namespace_id, &manifest)
        .await
        .expect("load manifest segments");

    for (visible_seq, expected) in [
        (9, &records[2]),
        (8, &records[1]),
        (6, &records[1]),
        (5, &records[0]),
    ] {
        let view = MetadataView::over_manifest_segments(&verified, ChangeSeq(visible_seq));
        assert_eq!(
            view.latest_access_revision(InodeId(7))
                .await
                .expect("read access"),
            Some(expected.clone()),
            "at seq {visible_seq}"
        );
    }
    let view = MetadataView::over_manifest_segments(&verified, ChangeSeq(2));
    assert_eq!(
        view.latest_access_revision(InodeId(7))
            .await
            .expect("read access"),
        None
    );
    let view = MetadataView::over_manifest_segments(&verified, ChangeSeq(9));
    assert_eq!(
        view.latest_access_revision(InodeId(8))
            .await
            .expect("read access"),
        None
    );
}

#[tokio::test]
async fn every_metadata_source_resolves_the_same_effective_rights() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let state = access_fixture_state();
    let visible_seq = ChangeSeq(10);
    let segments = build_manifest_segments(
        &store,
        &namespace_id,
        &state,
        MetadataLsmPolicy {
            max_rows_per_segment: NonZeroUsize::new(64).expect("segment row budget"),
            ..MetadataLsmPolicy::default()
        },
    )
    .await
    .expect("build segments");
    let manifest = publish_manifest_with_segments(
        &store,
        &namespace_id,
        ManifestNo(1),
        visible_seq,
        flatten_manifest_segments(segments),
    )
    .await;
    let verified = load_manifest_segments_for_inspection(&store, None, &namespace_id, &manifest)
        .await
        .expect("load manifest segments");
    let view = MetadataView::over_manifest_segments(&verified, visible_seq);
    let mut state_reads = state.reads_at_seq(visible_seq);
    let mut view_reads = view.reads();
    let mut session = view.session();

    for (principals, inode_id, expected) in access_fixture_cases() {
        assert_eq!(
            effective_rights(&mut state_reads, &principals, inode_id)
                .await
                .expect("state rights"),
            expected,
            "state: principals {principals:?}, inode {inode_id}"
        );
        assert_eq!(
            effective_rights(&mut view_reads, &principals, inode_id)
                .await
                .expect("view rights"),
            expected,
            "view: principals {principals:?}, inode {inode_id}"
        );
        assert_eq!(
            effective_rights(&mut session, &principals, inode_id)
                .await
                .expect("session rights"),
            expected,
            "session: principals {principals:?}, inode {inode_id}"
        );
    }
}
