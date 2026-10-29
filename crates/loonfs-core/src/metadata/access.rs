//! Effective access: the rights a set of principals has on an inode,
//! resolved from the inode's own access row and its ancestors' rows up to
//! the nearest boundary.

use super::visibility::{AncestorWalk, MetadataVisibilityReads};
use crate::error::{CoreError, Result};
use loonfs_types::format::manifest::{AccessRevisionRecord, TombstoneRowAction};
use loonfs_types::{AccessRight, AccessRights, InodeId, PrincipalSet, ROOT_INODE_ID};

/// The access rows the inheritance walk visits, nearest first.
///
/// The walk starts at the inode, follows current parent bindings, and
/// stops after a row whose boundary is set or at the root. A deletion root
/// has no current binding; while the deletion is recoverable (above the
/// view's retention floor), its `set` event's saved parent repairs that one
/// edge, so a deleted item's own row and boundary are evaluated before the
/// walk continues. The walk stops at the root of an expired deletion, so an
/// expired subtree inherits no grant from above it. Inodes without a row
/// contribute nothing and are not recorded.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct AccessChain {
    rows: Vec<AccessRevisionRecord>,
}

impl AccessChain {
    pub(crate) fn rows(&self) -> &[AccessRevisionRecord] {
        &self.rows
    }

    /// Rights the principals have through this chain. `admin` never
    /// inherits, so it is left out; see [`is_administrator`].
    pub(crate) fn rights_for(&self, principals: &PrincipalSet) -> AccessRights {
        self.rows
            .iter()
            .flat_map(|row| {
                principals
                    .iter()
                    .map(move |principal| row.grants.get(principal))
            })
            .fold(AccessRights::EMPTY, AccessRights::union)
            .difference(AccessRights::ADMIN)
    }
}

pub(crate) async fn access_chain<R: MetadataVisibilityReads>(
    reads: &mut R,
    inode_id: InodeId,
) -> Result<AccessChain>
where
    CoreError: From<R::Error>,
{
    let mut rows = Vec::new();
    let mut walk = AncestorWalk::new(inode_id);
    let mut last = inode_id;
    loop {
        let Some(current) = walk.next(reads).await? else {
            // A recoverable deletion's root has no current binding, and its
            // `set` event's saved parent repairs that one edge.
            match reads.find_hiding_subtree_tombstone(last).await? {
                Some(tombstone) if tombstone.committed_seq > reads.retention_floor_seq() => {
                    match tombstone.action {
                        TombstoneRowAction::Set { deleted_binding } => {
                            walk.continue_at(deleted_binding.parent_inode_id);
                            continue;
                        }
                        TombstoneRowAction::Revoke { .. } => break,
                    }
                }
                Some(_) | None => break,
            }
        };
        last = current;
        let mut boundary = false;
        if let Some(row) = reads.find_access_row(current).await? {
            boundary = row.boundary;
            rows.push(row);
        }
        if boundary || current == ROOT_INODE_ID {
            break;
        }
    }
    Ok(AccessChain { rows })
}

/// Whether any of the principals has `admin` on the root row.
pub(crate) async fn is_administrator<R: MetadataVisibilityReads>(
    reads: &mut R,
    principals: &PrincipalSet,
) -> std::result::Result<bool, R::Error> {
    Ok(reads
        .find_access_row(ROOT_INODE_ID)
        .await?
        .is_some_and(|row| {
            principals
                .iter()
                .any(|principal| row.grants.get(principal).contains(AccessRight::Admin))
        }))
}

/// The rights `principals` have on `inode_id`: every right for an
/// administrator, otherwise the union over the inheritance chain.
pub(crate) async fn effective_rights<R: MetadataVisibilityReads>(
    reads: &mut R,
    principals: &PrincipalSet,
    inode_id: InodeId,
) -> Result<AccessRights>
where
    CoreError: From<R::Error>,
{
    if is_administrator(reads, principals).await? {
        return Ok(AccessRights::ALL);
    }
    Ok(access_chain(reads, inode_id).await?.rights_for(principals))
}

#[cfg(test)]
pub(crate) use tests::{access_fixture_cases, access_fixture_state};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{
        DirentryBindingRecord, InodeRecord, MetadataState, MetadataStateBuilder,
        SubtreeTombstoneRecord,
    };
    use loonfs_types::format::manifest::DeletedBinding;
    use loonfs_types::{
        AccessGrants, AccessRevisionNo, ActorId, ChangeSeq, CommitId, DisplayName, InodeKind,
        NameKey, PrincipalId,
    };
    use std::assert_matches;

    fn principal(id: &str) -> PrincipalId {
        PrincipalId::parse(id).expect("principal id")
    }

    fn principals(ids: &[&str]) -> PrincipalSet {
        PrincipalSet::new(ids.iter().map(|id| principal(id)).collect()).expect("principal set")
    }

    fn grants(entries: &[(&str, &[AccessRight])]) -> AccessGrants {
        AccessGrants::new(
            entries
                .iter()
                .map(|(id, rights)| (principal(id), rights.iter().copied().collect()))
                .collect(),
        )
        .expect("access grants")
    }

    pub(crate) fn access_fixture_state() -> MetadataState {
        use AccessRight::{Admin, Create, Read, Remove, Write};

        let mut builder = MetadataStateBuilder::default();
        let commit_id = CommitId::parse("c_access_fixture").expect("commit id");
        for inode in 1..=8 {
            builder.push_inode(InodeRecord {
                inode_id: InodeId(inode),
                inode_kind: if matches!(inode, 4 | 7 | 8) {
                    InodeKind::File
                } else {
                    InodeKind::Directory
                },
                committed_seq: ChangeSeq(1),
                commit_id: commit_id.clone(),
                committed_by: ActorId::loonfs(),
                committed_at_ms: 1_000,
            });
        }
        for (parent, child, name) in [
            (1, 2, "docs"),
            (2, 3, "secret"),
            (3, 4, "file"),
            (1, 5, "pub"),
            (6, 7, "x"),
            (2, 8, "tool"),
        ] {
            let display_name = DisplayName::parse(name).expect("display name");
            builder.push_direntry_binding(DirentryBindingRecord {
                parent_inode_id: InodeId(parent),
                name_key: NameKey::for_display_name(&display_name),
                state: loonfs_types::format::manifest::DirentryBindingState::Bound { display_name },
                child_inode_id: InodeId(child),
                child_kind: if matches!(child, 4 | 7 | 8) {
                    InodeKind::File
                } else {
                    InodeKind::Directory
                },
                child_created_by: loonfs_types::ActorId::loonfs(),
                child_created_at_ms: 1_000,
                committed_seq: ChangeSeq(1),
                delta_index: 0,
            });
        }
        let display_name = DisplayName::parse("old").expect("display name");
        builder.push_subtree_tombstone(SubtreeTombstoneRecord {
            root_inode_id: InodeId(6),
            committed_seq: ChangeSeq(3),
            delta_index: 0,
            commit_id: commit_id.clone(),
            action: TombstoneRowAction::Set {
                deleted_binding: DeletedBinding {
                    parent_inode_id: InodeId(5),
                    name_key: NameKey::for_display_name(&display_name),
                    display_name,
                },
            },
            committed_at_ms: 1_003,
            committed_by: ActorId::loonfs(),
        });
        for (inode, boundary, grants) in [
            (
                1,
                false,
                grants(&[("team", &[Read, Write, Create, Remove]), ("ops", &[Admin])]),
            ),
            (3, true, grants(&[("finance", &[Read])])),
            (4, false, grants(&[("ada", &[Write])])),
            (6, false, grants(&[("bob", &[Read])])),
            (8, false, grants(&[("eve", &[Admin])])),
        ] {
            builder.push_access_revision(AccessRevisionRecord {
                inode_id: InodeId(inode),
                access_revision_no: AccessRevisionNo(1),
                committed_seq: ChangeSeq(2),
                commit_id: commit_id.clone(),
                delta_index: 0,
                committed_by: ActorId::loonfs(),
                committed_at_ms: 1_002,
                boundary,
                grants,
            });
        }
        builder.build()
    }

    pub(crate) fn access_fixture_cases() -> Vec<(PrincipalSet, InodeId, AccessRights)> {
        use AccessRight::{Create, Read, Remove, Write};

        let team_rights = [Read, Write, Create, Remove].into_iter().collect();
        let read = [Read].into_iter().collect();
        let write = [Write].into_iter().collect();
        [
            ("team", 2, team_rights),
            ("team", 4, AccessRights::EMPTY),
            ("team", 7, team_rights),
            ("finance", 4, read),
            ("finance", 2, AccessRights::EMPTY),
            ("ada", 4, write),
            ("bob", 6, read),
            ("bob", 7, read),
            ("bob", 5, AccessRights::EMPTY),
            ("ops", 4, AccessRights::ALL),
            ("eve", 8, AccessRights::EMPTY),
        ]
        .into_iter()
        .map(|(id, inode, rights)| (principals(&[id]), InodeId(inode), rights))
        .collect()
    }

    #[tokio::test]
    async fn effective_rights_follow_the_chain_to_the_nearest_boundary() {
        let state = access_fixture_state();
        let mut reads = state.reads_at_seq(ChangeSeq(10));
        for (principals, inode_id, expected) in access_fixture_cases() {
            assert_eq!(
                effective_rights(&mut reads, &principals, inode_id)
                    .await
                    .expect("effective rights"),
                expected,
                "principals {principals:?}, inode {inode_id}"
            );
        }
        let ops = principals(&["ops"]);
        let eve = principals(&["eve"]);
        assert!(is_administrator(&mut reads, &ops)
            .await
            .expect("administrator"));
        assert!(!is_administrator(&mut reads, &eve)
            .await
            .expect("administrator"));
        assert!(access_chain(&mut reads, InodeId(4))
            .await
            .expect("access chain")
            .rights_for(&ops)
            .is_empty());
    }

    #[tokio::test]
    async fn an_access_inheritance_cycle_is_namespace_corruption() {
        let mut builder = MetadataStateBuilder::default();
        let commit_id = CommitId::parse("c_access_cycle").expect("commit id");
        for inode_id in [InodeId(2), InodeId(3)] {
            builder.push_inode(InodeRecord {
                inode_id,
                inode_kind: InodeKind::Directory,
                committed_seq: ChangeSeq(1),
                commit_id: commit_id.clone(),
                committed_by: ActorId::loonfs(),
                committed_at_ms: 1_000,
            });
        }
        for (parent_inode_id, child_inode_id, name) in [
            (InodeId(2), InodeId(3), "three"),
            (InodeId(3), InodeId(2), "two"),
        ] {
            let display_name = DisplayName::parse(name).expect("display name");
            builder.push_direntry_binding(DirentryBindingRecord {
                parent_inode_id,
                name_key: NameKey::for_display_name(&display_name),
                state: loonfs_types::format::manifest::DirentryBindingState::Bound { display_name },
                child_inode_id,
                child_kind: InodeKind::Directory,
                child_created_by: ActorId::loonfs(),
                child_created_at_ms: 1_000,
                committed_seq: ChangeSeq(1),
                delta_index: 0,
            });
        }
        let state = builder.build();
        let error = effective_rights(
            &mut state.reads_at_seq(ChangeSeq(1)),
            &principals(&["viewer"]),
            InodeId(2),
        )
        .await
        .expect_err("inheritance cycle");
        assert_eq!(error.code(), loonfs_types::ErrorCode::NamespaceCorrupt);
        assert_matches!(
            error,
            CoreError::VisiblePath(crate::metadata::VisiblePathError::ParentBindingCycle { .. })
        );
    }
}
