//! The authorization checks that the planners share: the rights on a target,
//! on a destination, on both ends of a relocation, and on an access update.

use crate::authorize::{Absence, Authorizer, Replacement};

use crate::error::{CoreError, Result};
use crate::metadata::access::{access_chain, effective_rights, is_administrator};
use crate::metadata::AccessRevisionRecord;
use crate::metadata::MetadataVisibilityReads;

use loonfs_objectstore::ObjectStore;

use loonfs_types::{AccessGrants, AccessRight, AccessRights, PrincipalId};
use loonfs_types::{DestinationBehavior, InodeId, ROOT_INODE_ID};
use std::collections::BTreeSet;

use crate::path::write::publish_path_planning::PlanningView;
use crate::path::write::publish_path_planning::ResolvedDestination;
use std::sync::LazyLock;

impl<S: ObjectStore + ?Sized> PlanningView<'_, S> {
    /// Requires `rights` on `inode_id`.
    pub(super) async fn authorize(
        &self,
        inode_id: InodeId,
        rights: AccessRights,
        absence: Absence<'_>,
    ) -> Result<()> {
        crate::authorize::require(
            &self.authorizer,
            &mut self.metadata.reads(),
            inode_id,
            rights,
            absence,
        )
        .await
    }

    /// Authorizes the entry right an occupied or vacant destination of a move
    /// or copy needs, before the conflict rule can reveal whether the name
    /// exists.
    pub(super) async fn authorize_destination(
        &self,
        destination: &ResolvedDestination<'_>,
        behavior: DestinationBehavior,
        source_inode_id: InodeId,
        replacement: Replacement,
    ) -> Result<()> {
        if matches!(self.authorizer, Authorizer::Unrestricted) {
            return Ok(());
        }
        let parent = destination.parent_inode_id;
        let (inode_id, rights) = match &destination.occupant {
            Some(existing)
                if existing.inode_id != source_inode_id
                    && behavior == DestinationBehavior::Replace =>
            {
                match replacement {
                    Replacement::RemovesEntry => (
                        parent,
                        AccessRights::from_iter([AccessRight::Create, AccessRight::Remove]),
                    ),
                    Replacement::WritesFile => (existing.inode_id, AccessRight::Write.into()),
                }
            }
            _ => (parent, AccessRight::Create.into()),
        };
        self.authorize(inode_id, rights, destination.absence).await
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
        let mut reads = self.metadata.reads();
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
        current: AccessStateRef<'_>,
        next: AccessStateRef<'_>,
        absence: Absence<'_>,
    ) -> Result<()> {
        let Authorizer::Subject { principals } = self.authorizer else {
            return Ok(());
        };
        let mut reads = self.metadata.reads();
        let effective = effective_rights(&mut reads, principals, target).await?;
        if effective.is_empty() {
            return Err(absence.not_found(target));
        }
        if target == ROOT_INODE_ID && administrators(current.grants) != administrators(next.grants)
        {
            return if is_administrator(&mut reads, principals).await? {
                Ok(())
            } else {
                Err(CoreError::Forbidden { inode_id: target })
            };
        }
        if effective.contains(AccessRight::Manage) {
            return Ok(());
        }
        if current.boundary != next.boundary {
            return Err(CoreError::Forbidden { inode_id: target });
        }
        let named: BTreeSet<&PrincipalId> = current
            .grants
            .iter()
            .chain(next.grants.iter())
            .map(|(principal, _)| principal)
            .collect();
        for principal in named {
            let added = next
                .grants
                .get(principal)
                .difference(current.grants.get(principal));
            let removed = current
                .grants
                .get(principal)
                .difference(next.grants.get(principal));
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

/// An inode's boundary flag and direct grants, borrowed for one access check.
#[derive(Clone, Copy)]
pub(super) struct AccessStateRef<'a> {
    pub(super) boundary: bool,
    pub(super) grants: &'a AccessGrants,
}

impl<'a> AccessStateRef<'a> {
    /// Returns the access that `row` records, which is no boundary and no
    /// grants when the inode has no access row.
    pub(super) fn from_row(row: Option<&'a AccessRevisionRecord>) -> Self {
        static NO_GRANTS: LazyLock<AccessGrants> = LazyLock::new(AccessGrants::default);
        match row {
            Some(row) => Self {
                boundary: row.boundary,
                grants: &row.grants,
            },
            None => Self {
                boundary: false,
                grants: &NO_GRANTS,
            },
        }
    }
}

fn administrators(grants: &AccessGrants) -> BTreeSet<&PrincipalId> {
    grants
        .iter()
        .filter_map(|(principal, rights)| rights.contains(AccessRight::Admin).then_some(principal))
        .collect()
}
