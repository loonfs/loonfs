//! Reads by inode ID of an expired subtree whose bindings a base compaction
//! has already removed, while the revisions group still stores its rows.

use super::active_deletions::{tombstone_expire, tombstone_set};
use super::*;
use crate::authorize::{Absence, Authorizer, ReadAccess};
use crate::metadata::{DirentryBindingRecord, RevisionRecord};
use crate::path::read::load_current_metadata_view;
use loonfs_types::format::manifest::{AccessRevisionRecord, DirentryBindingState, InodeRecord};
use loonfs_types::{
    AccessGrants, AccessRevisionNo, AccessRight, AccessRights, ActorId, ContentId, ContentRef,
    DisplayName, EffectiveLimit, InodeKind, NameKey, PageRequest, PrincipalId, PrincipalSet,
    RevisionNo,
};

const ROOT: InodeId = InodeId(1);
/// `/old`, whose deletion expired.
const OLD: InodeId = InodeId(2);
/// `/old/notes.txt`.
const NOTES: InodeId = InodeId(3);
/// `/keep`, whose deletion is recoverable.
const KEEP: InodeId = InodeId(4);
/// `/keep/a.txt`.
const KEPT_FILE: InodeId = InodeId(5);

fn commit_id() -> CommitId {
    CommitId::parse("c_expired_subtrees").expect("commit id")
}

fn binding(parent: InodeId, child: InodeId, name: &str, seq: u64) -> DirentryBindingRecord {
    let display_name = DisplayName::parse(name).expect("display name");
    DirentryBindingRecord {
        parent_inode_id: parent,
        name_key: NameKey::for_display_name(&display_name),
        state: DirentryBindingState::Bound { display_name },
        child_inode_id: child,
        child_kind: if matches!(child, NOTES | KEPT_FILE) {
            InodeKind::File
        } else {
            InodeKind::Directory
        },
        child_created_by: ActorId::loonfs(),
        child_created_at_ms: 1_000,
        committed_seq: ChangeSeq(seq),
        delta_index: 0,
    }
}

fn grants(principal: &str, rights: &[AccessRight]) -> AccessRevisionRecord {
    AccessRevisionRecord {
        inode_id: ROOT,
        access_revision_no: AccessRevisionNo(1),
        committed_seq: ChangeSeq(1),
        commit_id: commit_id(),
        delta_index: 0,
        committed_by: ActorId::loonfs(),
        committed_at_ms: 1_000,
        boundary: false,
        grants: AccessGrants::new(BTreeMap::from([(
            PrincipalId::parse(principal).expect("principal"),
            rights.iter().copied().collect(),
        )]))
        .expect("grants"),
    }
}

/// The state after `/old` was deleted at sequence 3 and expired at sequence
/// 4, `/keep` was deleted at sequence 5, and a base compaction of the
/// bindings group removed `/old`'s edge and every edge under it. Ada has a
/// direct grant on `/old/notes.txt`, which the access group still stores,
/// and one on `/keep`.
fn compacted_bindings_state(namespace_id: &NamespaceId) -> MetadataState {
    let mut builder = MetadataStateBuilder::default();
    for inode_id in [ROOT, OLD, NOTES, KEEP, KEPT_FILE] {
        builder.push_inode(InodeRecord {
            inode_id,
            inode_kind: if matches!(inode_id, NOTES | KEPT_FILE) {
                InodeKind::File
            } else {
                InodeKind::Directory
            },
            committed_seq: ChangeSeq(u64::from(inode_id != ROOT)),
            commit_id: commit_id(),
            committed_by: ActorId::loonfs(),
            committed_at_ms: 1_000,
        });
    }
    builder.push_direntry_binding(binding(ROOT, KEEP, "keep", 1));
    builder.push_direntry_binding(DirentryBindingRecord {
        state: DirentryBindingState::Unbound,
        ..binding(ROOT, KEEP, "keep", 5)
    });
    builder.push_direntry_binding(binding(KEEP, KEPT_FILE, "a.txt", 1));
    for tombstone in [
        tombstone_set(OLD, 3, "old"),
        tombstone_expire(OLD, 4, 3),
        tombstone_set(KEEP, 5, "keep"),
    ] {
        builder.push_subtree_tombstone(tombstone);
    }
    for inode_id in [NOTES, KEPT_FILE] {
        builder.push_revision(RevisionRecord {
            inode_id,
            revision_no: RevisionNo(1),
            committed_seq: ChangeSeq(1),
            commit_id: commit_id(),
            committed_at_ms: 1_000,
            committed_by: ActorId::loonfs(),
            delta_index: 0,
            content_ref: ContentRef::blob_v1(
                namespace_id.clone(),
                ContentId::parse("con_00000000000000000000000000000000").expect("content"),
                b"text",
                loonfs_types::ChecksumAlgorithm::Crc64nvme,
            ),
        });
    }
    let reader = [AccessRight::Read, AccessRight::History];
    for (inode_id, row) in [
        (ROOT, grants("prn_root", &[AccessRight::Admin])),
        (NOTES, grants("ada", &reader)),
        (KEEP, grants("ada", &reader)),
    ] {
        builder.push_access_revision(AccessRevisionRecord { inode_id, ..row });
    }
    builder.build()
}

#[tokio::test]
async fn a_read_by_inode_whose_ancestry_was_compacted_away_is_refused_for_every_principal() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("compacted-bindings").expect("namespace id");
    bootstrap_namespace(&store, &namespace_id, &test_context())
        .await
        .expect("bootstrap");
    let current = load_current_projection(&store, &namespace_id)
        .await
        .expect("current projection");
    let mut head = current.head.clone();
    head.seq = ChangeSeq(5);
    head.next_inode_id = InodeId(6);
    let manifest = build_namespace_manifest_from_metadata_state(
        &store,
        &namespace_id,
        ManifestMetadataSource {
            head: &head,
            basis_manifest_no: None,
            retention_floor_seq: head.seq,
            metadata_state: &compacted_bindings_state(&namespace_id),
        },
        MetadataRunPolicy::default(),
        ManifestNo(current.manifest.manifest().manifest_no.0 + 1),
    )
    .await
    .expect("build manifest");
    write_namespace_manifest(&store, manifest.into_payload())
        .await
        .expect("publish manifest");
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("load read view");
    let page = || PageRequest {
        limit: EffectiveLimit::new(NonZeroU32::new(10).expect("nonzero")),
        cursor: None,
    };

    for principal in ["ada", "prn_root"] {
        let principals = PrincipalSet::new(BTreeSet::from([
            PrincipalId::parse(principal).expect("principal")
        ]))
        .expect("principals");
        let live = ReadAccess::live(Authorizer::Subject {
            principals: &principals,
        });
        let refused = [
            view.authorized_revision_by_inode(NOTES, RevisionNo(1), &live)
                .await
                .map(|_| ()),
            view.list_file_revisions_by_inode_page(NOTES, page(), &live)
                .await
                .map(|_| ()),
        ];
        for outcome in refused {
            assert_eq!(
                outcome.expect_err("a broken chain").code(),
                ErrorCode::InodeNotFound,
                "{principal}"
            );
        }
        // The walk from `/keep/a.txt` stops at `/keep`'s recoverable tombstone.
        let kept = view
            .authorized_revision_by_inode(KEPT_FILE, RevisionNo(1), &live)
            .await
            .expect("a recoverable deletion's descendant");
        assert_eq!(kept.inode_id, KEPT_FILE, "{principal}");

        // A snapshot read evaluates the same chain at the head.
        let at_head = ReadAccess::at_head(
            Authorizer::Subject {
                principals: &principals,
            },
            &view,
        );
        let snapshot = at_head
            .require(
                &mut view.metadata_view().session(),
                NOTES,
                AccessRights::from_iter([AccessRight::Read]),
                Absence::Inode,
            )
            .await;
        if principal == "prn_root" {
            snapshot.expect("an administrator reads the snapshot");
        } else {
            assert_eq!(
                snapshot.expect_err("a broken chain at the head").code(),
                ErrorCode::InodeNotFound
            );
        }
    }
}
