//! Effective access: the rights a set of principals has on an inode,
//! resolved from the inode's own access row and its ancestors' rows up to
//! the nearest boundary.

use super::visibility::{AncestorWalk, MetadataVisibilityReads};
use crate::error::{CoreError, Result};
use loonfs_types::format::manifest::{AccessRevisionRecord, TombstoneRowAction};
use loonfs_types::{AccessRight, AccessRights, InodeId, PrincipalSet, ROOT_INODE_ID};
use std::collections::HashMap;

/// The inodes an inheritance walk reached, nearest first, each with its own
/// access row, and the known ancestor the walk stopped before, if any.
struct AccessWalk {
    walked: Vec<(InodeId, Option<AccessRevisionRecord>)>,
    known_ancestor: Option<InodeId>,
}

/// Walks the inheritance chain up from `inode_id`.
///
/// The walk starts at the inode, follows current parent bindings, and
/// stops after a row whose boundary is set or at the root. A deletion root
/// has no current binding; while the deletion is in the trash (above the
/// view's retention floor), its `set` event's saved parent repairs that one
/// edge, so a deleted item's own row and boundary are evaluated before the
/// walk continues. The walk stops at the root of an expired deletion, so an
/// expired subtree inherits no grant from above it. The walk also stops
/// before the first inode for which `known` returns true.
async fn walk_access_rows<R: MetadataVisibilityReads>(
    reads: &mut R,
    inode_id: InodeId,
    known: impl Fn(InodeId) -> bool,
) -> Result<AccessWalk>
where
    CoreError: From<R::Error>,
{
    let mut walked = Vec::new();
    let mut walk = AncestorWalk::new(inode_id);
    let mut last = inode_id;
    loop {
        let Some(current) = walk.next(reads).await? else {
            // The root of a deletion in the trash has no current binding, and its
            // `set` event's saved parent repairs that one edge.
            let floor = reads.retention_floor_seq();
            let Some(TombstoneRowAction::Set {
                deleted_binding, ..
            }) = reads
                .find_hiding_subtree_tombstone(last)
                .await?
                .filter(|tombstone| tombstone.committed_seq > floor)
                .map(|tombstone| tombstone.action)
            else {
                break;
            };
            walk.continue_at(deleted_binding.parent_inode_id);
            continue;
        };
        if known(current) {
            return Ok(AccessWalk {
                walked,
                known_ancestor: Some(current),
            });
        }
        last = current;
        let row = reads.find_access_row(current).await?;
        let boundary = row.as_ref().is_some_and(|row| row.boundary);
        walked.push((current, row));
        if boundary || current == ROOT_INODE_ID {
            break;
        }
    }
    Ok(AccessWalk {
        walked,
        known_ancestor: None,
    })
}

/// Returns the access rows on the inheritance chain of `inode_id`, nearest
/// first. Inodes without a row contribute nothing and are left out.
pub(crate) async fn access_chain<R: MetadataVisibilityReads>(
    reads: &mut R,
    inode_id: InodeId,
) -> Result<Vec<AccessRevisionRecord>>
where
    CoreError: From<R::Error>,
{
    let walk = walk_access_rows(reads, inode_id, |_| false).await?;
    Ok(walk.walked.into_iter().filter_map(|(_, row)| row).collect())
}

/// Effective rights that one subject's checks have found on one view,
/// memoized per inode.
///
/// The rights on an inode are the rights its own row grants, united with
/// the rights on its parent unless that row is a boundary, so a walk from an
/// inode stops at the first ancestor whose rights are memoized, and it
/// memoizes every inode it reached. The children of one directory then share
/// one walk above the directory.
#[derive(Debug, Default)]
pub(crate) struct EffectiveRightsMemo {
    administrator: Option<bool>,
    inherited: HashMap<InodeId, AccessRights>,
}

impl EffectiveRightsMemo {
    /// Returns the rights `principals` have on `inode_id`: every right for
    /// an administrator, otherwise the union over the inheritance chain.
    pub(crate) async fn effective_rights<R: MetadataVisibilityReads>(
        &mut self,
        reads: &mut R,
        principals: &PrincipalSet,
        inode_id: InodeId,
    ) -> Result<AccessRights>
    where
        CoreError: From<R::Error>,
    {
        let administrator = match self.administrator {
            Some(administrator) => administrator,
            None => *self
                .administrator
                .insert(is_administrator(reads, principals).await?),
        };
        if administrator {
            return Ok(AccessRights::ALL);
        }
        self.inherited_rights(reads, principals, inode_id).await
    }

    /// Returns the union of the grants to `principals` over the inheritance
    /// chain of `inode_id`. `admin` never inherits, so it is left out; see
    /// [`is_administrator`].
    async fn inherited_rights<R: MetadataVisibilityReads>(
        &mut self,
        reads: &mut R,
        principals: &PrincipalSet,
        inode_id: InodeId,
    ) -> Result<AccessRights>
    where
        CoreError: From<R::Error>,
    {
        let walk = walk_access_rows(reads, inode_id, |inode_id| {
            self.inherited.contains_key(&inode_id)
        })
        .await?;
        let mut rights = walk.known_ancestor.map_or(AccessRights::EMPTY, |inode_id| {
            self.inherited
                .get(&inode_id)
                .copied()
                .expect("the walk stops only at a memoized ancestor")
        });
        // The walk stops after a boundary row, so a boundary is the last
        // inode reached, and nothing above the boundary reaches its rights.
        for (walked, row) in walk.walked.into_iter().rev() {
            if let Some(row) = row {
                rights = principals
                    .iter()
                    .map(|principal| row.grants.get(principal))
                    .fold(rights, AccessRights::union)
                    .difference(AccessRights::ADMIN);
            }
            self.inherited.insert(walked, rights);
        }
        Ok(rights)
    }
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

/// The rights `principals` have on `inode_id`, through a memo of its own;
/// see [`EffectiveRightsMemo::effective_rights`].
pub(crate) async fn effective_rights<R: MetadataVisibilityReads>(
    reads: &mut R,
    principals: &PrincipalSet,
    inode_id: InodeId,
) -> Result<AccessRights>
where
    CoreError: From<R::Error>,
{
    EffectiveRightsMemo::default()
        .effective_rights(reads, principals, inode_id)
        .await
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
        AccessGrants, AccessRevisionNo, ActorId, CommitId, CommitSeq, DisplayName, InodeKind,
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
        // Inode 0 is the root directory.
        for inode in [0, 2, 3, 4, 5, 6, 7, 8] {
            builder.push_inode(InodeRecord {
                inode_id: InodeId(inode),
                inode_kind: if matches!(inode, 4 | 7 | 8) {
                    InodeKind::File
                } else {
                    InodeKind::Directory
                },
                committed_seq: CommitSeq(1),
                commit_id: commit_id.clone(),
                committed_by: ActorId::loonfs(),
                committed_at_ms: 1_000,
            });
        }
        for (parent, child, name) in [
            (0, 2, "docs"),
            (2, 3, "secret"),
            (3, 4, "file"),
            (0, 5, "pub"),
            (6, 7, "x"),
            (2, 8, "tool"),
        ] {
            let display_name = DisplayName::parse(name).expect("display name");
            builder.push_direntry_binding(DirentryBindingRecord {
                parent_inode_id: InodeId(parent),
                name_key: NameKey::for_display_name(&display_name),
                state: loonfs_types::format::manifest::DirentryBindingState::Bound {
                    display_name,
                    revision: matches!(child, 4 | 7 | 8)
                        .then(crate::test_support::revision_summary),
                },
                child_inode_id: InodeId(child),
                child_kind: if matches!(child, 4 | 7 | 8) {
                    InodeKind::File
                } else {
                    InodeKind::Directory
                },
                child_created_by: loonfs_types::ActorId::loonfs(),
                child_created_at_ms: 1_000,
                committed_seq: CommitSeq(1),
                delta_index: 0,
                commit_id: crate::test_support::binding_commit_id(),
                committed_by: loonfs_types::ActorId::loonfs(),
                committed_at_ms: 0,
                bind_position: loonfs_types::format::manifest::DeltaPosition {
                    seq: CommitSeq(1),
                    delta_index: 0,
                },
            });
        }
        let display_name = DisplayName::parse("old").expect("display name");
        builder.push_subtree_tombstone(SubtreeTombstoneRecord {
            root_inode_id: InodeId(6),
            committed_seq: CommitSeq(3),
            delta_index: 0,
            commit_id: commit_id.clone(),
            action: TombstoneRowAction::Set {
                root_kind: InodeKind::Directory,
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
                0,
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
                committed_seq: CommitSeq(2),
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
        let mut reads = state.reads_at_seq(CommitSeq(10));
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
        assert!(EffectiveRightsMemo::default()
            .inherited_rights(&mut reads, &ops, InodeId(4))
            .await
            .expect("inherited rights")
            .is_empty());
    }

    #[tokio::test]
    async fn a_memo_resolves_the_same_rights_in_any_order() {
        let state = access_fixture_state();
        let mut reads = state.reads_at_seq(CommitSeq(10));
        let inode_ids: Vec<_> = [0, 2, 3, 4, 5, 6, 7, 8].into_iter().map(InodeId).collect();
        for (principals, _, _) in access_fixture_cases() {
            let mut expected = Vec::new();
            for &inode_id in &inode_ids {
                expected.push(
                    effective_rights(&mut reads, &principals, inode_id)
                        .await
                        .expect("effective rights"),
                );
            }
            for reversed in [false, true] {
                let mut memo = EffectiveRightsMemo::default();
                let mut order: Vec<_> = inode_ids.iter().copied().zip(&expected).collect();
                if reversed {
                    order.reverse();
                }
                for (inode_id, expected) in order {
                    assert_eq!(
                        memo.effective_rights(&mut reads, &principals, inode_id)
                            .await
                            .expect("memoized rights"),
                        *expected,
                        "principals {principals:?}, inode {inode_id}, reversed {reversed}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn an_access_inheritance_cycle_is_namespace_corruption() {
        let mut builder = MetadataStateBuilder::default();
        let commit_id = CommitId::parse("c_access_cycle").expect("commit id");
        for inode_id in [InodeId(2), InodeId(3)] {
            builder.push_inode(InodeRecord {
                inode_id,
                inode_kind: InodeKind::Directory,
                committed_seq: CommitSeq(1),
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
                state: loonfs_types::format::manifest::DirentryBindingState::Bound {
                    display_name,
                    revision: None,
                },
                child_inode_id,
                child_kind: InodeKind::Directory,
                child_created_by: ActorId::loonfs(),
                child_created_at_ms: 1_000,
                committed_seq: CommitSeq(1),
                delta_index: 0,
                commit_id: crate::test_support::binding_commit_id(),
                committed_by: loonfs_types::ActorId::loonfs(),
                committed_at_ms: 0,
                bind_position: loonfs_types::format::manifest::DeltaPosition {
                    seq: CommitSeq(1),
                    delta_index: 0,
                },
            });
        }
        let state = builder.build();
        let error = effective_rights(
            &mut state.reads_at_seq(CommitSeq(1)),
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
