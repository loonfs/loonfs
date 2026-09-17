//! Authorizes a commit's operations against the subject's effective
//! rights, inside the planner, before each operation's own state checks.

use super::publish_path_planning::PublishPathPlanningView;
use crate::error::{CoreError, Result};
use crate::metadata::access::{access_chain, effective_rights, is_administrator};
use crate::metadata::MetadataVisibilityReads;
use crate::metadata::ResolvedVisiblePath;
use loonfs_api::{
    AccessGrants, AccessRight, AccessRights, DestinationBehavior, InodeId, NamespaceAccess,
    NamespaceId, PrincipalId, PrincipalSet, Subject, ROOT_INODE_ID,
};
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeSet;

pub(super) enum Authorizer<'a> {
    /// An unrestricted namespace: every check passes without a lookup.
    Unrestricted,
    Subject {
        principals: &'a PrincipalSet,
    },
}

impl<'a> Authorizer<'a> {
    pub(super) fn for_request(
        namespace_id: &NamespaceId,
        access: &NamespaceAccess,
        subject: Option<&'a Subject>,
    ) -> Result<Self> {
        match (access, subject) {
            (NamespaceAccess::Unrestricted {}, _) => Ok(Self::Unrestricted),
            (NamespaceAccess::Acl { .. }, Some(subject)) => Ok(Self::Subject {
                principals: &subject.principals,
            }),
            (NamespaceAccess::Acl { .. }, None) => Err(CoreError::SubjectRequired {
                namespace_id: namespace_id.clone(),
            }),
        }
    }
}

/// How an inode the subject cannot see is reported: as the path the
/// request named, or as the inode id it named.
#[derive(Clone, Copy)]
pub(super) enum Absence<'p> {
    Path(&'p str),
    Inode,
}

impl Absence<'_> {
    fn not_found(self, inode_id: InodeId) -> CoreError {
        match self {
            Self::Path(path) => CoreError::PathNotFound(path.to_owned()),
            Self::Inode => CoreError::InodeNotFound(inode_id),
        }
    }
}

/// Which right a replacing move or copy needs at an occupied destination.
#[derive(Clone, Copy)]
pub(super) enum Replacement {
    /// A move deletes the occupant: `remove` on the destination parent.
    RemovesEntry,
    /// A copy appends a revision to the occupant: `write` on the file.
    WritesFile,
}

impl<S: ObjectStore + ?Sized> PublishPathPlanningView<'_, '_, '_, S> {
    /// Requires `rights` on `inode_id`.
    pub(super) async fn authorize(
        &self,
        inode_id: InodeId,
        rights: AccessRights,
        absence: Absence<'_>,
    ) -> Result<()> {
        let Authorizer::Subject { principals } = self.authorizer else {
            return Ok(());
        };
        let mut reads = self.view.reads();
        let effective = effective_rights(&mut reads, principals, inode_id).await?;
        if rights.is_subset_of(effective) {
            Ok(())
        } else if effective.is_empty() {
            Err(absence.not_found(inode_id))
        } else {
            Err(CoreError::Forbidden { inode_id })
        }
    }

    /// Authorizes the entry right an occupied or vacant destination needs,
    /// before the conflict rule can reveal whether the name exists.
    pub(super) async fn authorize_destination(
        &self,
        occupant: Option<&ResolvedVisiblePath>,
        behavior: DestinationBehavior,
        source_inode_id: Option<InodeId>,
        destination_parent: InodeId,
        replacement: Replacement,
        absence: Absence<'_>,
    ) -> Result<()> {
        if matches!(self.authorizer, Authorizer::Unrestricted) {
            return Ok(());
        }
        let (inode_id, rights) = match occupant {
            None => (
                destination_parent,
                AccessRights::from_iter([AccessRight::Create]),
            ),
            Some(existing) if Some(existing.inode_id) == source_inode_id => (
                destination_parent,
                AccessRights::from_iter([AccessRight::Create]),
            ),
            Some(existing) if behavior == DestinationBehavior::Replace => match replacement {
                Replacement::RemovesEntry => (
                    destination_parent,
                    AccessRights::from_iter([AccessRight::Create, AccessRight::Remove]),
                ),
                Replacement::WritesFile => (
                    existing.inode_id,
                    AccessRights::from_iter([AccessRight::Write]),
                ),
            },
            Some(_) => (
                destination_parent,
                AccessRights::from_iter([AccessRight::Create]),
            ),
        };
        self.authorize(inode_id, rights, absence).await
    }

    /// Authorizes moving `moved` from `source_parent` to `destination_parent`
    /// as if the mover had granted every right the move confers.
    pub(super) async fn authorize_relocation(
        &self,
        moved: InodeId,
        source_parent: InodeId,
        destination_parent: InodeId,
    ) -> Result<()> {
        let Authorizer::Subject { principals } = self.authorizer else {
            return Ok(());
        };
        if source_parent == destination_parent {
            return Ok(());
        }
        let mut reads = self.view.reads();
        let own = reads.find_access_row(moved).await?;
        if own.as_ref().is_some_and(|row| row.boundary) {
            return Ok(());
        }
        let before_chain = access_chain(&mut reads, source_parent).await?;
        let after_chain = access_chain(&mut reads, destination_parent).await?;
        let before_rows: Vec<_> = own.iter().chain(before_chain.rows()).collect();
        let after_rows: Vec<_> = own.iter().chain(after_chain.rows()).collect();
        let root = reads.find_access_row(ROOT_INODE_ID).await?;
        let named: BTreeSet<&PrincipalId> = before_rows
            .iter()
            .chain(&after_rows)
            .flat_map(|row| row.grants.iter().map(|(principal, _)| principal))
            .collect();
        let mut gain = AccessRights::EMPTY;
        for principal in named {
            if root
                .as_ref()
                .is_some_and(|row| row.grants.get(principal).contains(AccessRight::Admin))
            {
                continue;
            }
            let before = before_rows
                .iter()
                .map(|row| row.grants.get(principal))
                .fold(AccessRights::EMPTY, AccessRights::union)
                .difference(AccessRights::ADMIN);
            let after = after_rows
                .iter()
                .map(|row| row.grants.get(principal))
                .fold(AccessRights::EMPTY, AccessRights::union)
                .difference(AccessRights::ADMIN);
            gain = gain.union(after.difference(before));
        }
        if gain.is_empty() || is_administrator(&mut reads, principals).await? {
            return Ok(());
        }
        let mover = effective_rights(&mut reads, principals, moved).await?;
        if mover.contains(AccessRight::Manage)
            || (mover.contains(AccessRight::Share) && gain.is_subset_of(mover))
        {
            return Ok(());
        }
        Err(CoreError::Forbidden { inode_id: moved })
    }

    /// Authorizes replacing `target`'s access row with `next`.
    pub(super) async fn authorize_access_update(
        &self,
        target: InodeId,
        current: (bool, &AccessGrants),
        next: (bool, &AccessGrants),
        absence: Absence<'_>,
    ) -> Result<()> {
        let Authorizer::Subject { principals } = self.authorizer else {
            return Ok(());
        };
        let mut reads = self.view.reads();
        let effective = effective_rights(&mut reads, principals, target).await?;
        if effective.is_empty() {
            return Err(absence.not_found(target));
        }
        if target == ROOT_INODE_ID && administrators(current.1) != administrators(next.1) {
            return if is_administrator(&mut reads, principals).await? {
                Ok(())
            } else {
                Err(CoreError::Forbidden { inode_id: target })
            };
        }
        if effective.contains(AccessRight::Manage) {
            return Ok(());
        }
        if current.0 != next.0 {
            return Err(CoreError::Forbidden { inode_id: target });
        }
        let named: BTreeSet<&PrincipalId> = current
            .1
            .iter()
            .chain(next.1.iter())
            .map(|(principal, _)| principal)
            .collect();
        for principal in named {
            let added = next.1.get(principal).difference(current.1.get(principal));
            let removed = current.1.get(principal).difference(next.1.get(principal));
            if !added.union(removed).is_subset_of(effective) {
                return Err(CoreError::Forbidden { inode_id: target });
            }
        }
        if effective.contains(AccessRight::Share) {
            Ok(())
        } else {
            Err(CoreError::Forbidden { inode_id: target })
        }
    }
}

fn administrators(grants: &AccessGrants) -> BTreeSet<&PrincipalId> {
    grants
        .iter()
        .filter_map(|(principal, rights)| rights.contains(AccessRight::Admin).then_some(principal))
        .collect()
}
