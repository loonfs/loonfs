//! Authorizes reads and commits against the subject's effective rights.

use crate::error::{CoreError, Result};
use crate::metadata::access::{effective_rights, is_administrator, EffectiveRightsMemo};
use crate::metadata::MetadataViewSession;
use crate::metadata::{ancestry_allows_read, MetadataVisibilityReads};
use crate::path::read::LoadedMetadataView;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{AccessRight, AccessRights, InodeId, PrincipalSet, Subject};

#[derive(Clone, Copy)]
pub(crate) enum Authorizer<'a> {
    /// The service context, an internal read, or maintenance: every check
    /// passes without a lookup.
    Service,
    Subject {
        principals: &'a PrincipalSet,
    },
}

impl<'a> Authorizer<'a> {
    /// Returns the authorizer for a request that acts as `subject`, or in
    /// the service context when `subject` is `None`.
    pub(crate) fn for_request(subject: Option<&'a Subject>) -> Self {
        match subject {
            Some(subject) => Self::Subject {
                principals: &subject.principals,
            },
            None => Self::Service,
        }
    }
}

/// How an inode the subject cannot see is reported: as the path the
/// request named, or as the inode id it named.
#[derive(Clone, Copy)]
pub(crate) enum Absence<'p> {
    Path(&'p str),
    Inode,
}

impl Absence<'_> {
    pub(crate) fn not_found(self, inode_id: InodeId) -> CoreError {
        match self {
            Self::Path(path) => CoreError::PathNotFound(path.to_owned()),
            Self::Inode => CoreError::InodeNotFound(inode_id),
        }
    }
}

/// Which right a replacing move or copy needs at an occupied destination.
#[derive(Clone, Copy)]
pub(crate) enum Replacement {
    /// A move deletes the occupant: `remove` on the destination parent.
    RemovesEntry,
    /// A copy appends a revision to the occupant: `write` on the file.
    WritesFile,
}

/// Requires `rights` on `inode_id` for the authorizer's subject.
pub(crate) async fn require<R>(
    authorizer: &Authorizer<'_>,
    reads: &mut R,
    inode_id: InodeId,
    rights: AccessRights,
    absence: Absence<'_>,
) -> Result<()>
where
    R: MetadataVisibilityReads<Error = CoreError>,
{
    let Authorizer::Subject { principals } = authorizer else {
        return Ok(());
    };
    let effective = effective_rights(reads, principals, inode_id).await?;
    check_rights(effective, rights, inode_id, absence)
}

/// Requires `rights` within `effective`, the subject's rights on
/// `inode_id`. A subject with no right on the inode gets the not-found error,
/// and a subject with some right gets `forbidden`.
pub(crate) fn check_rights(
    effective: AccessRights,
    rights: AccessRights,
    inode_id: InodeId,
    absence: Absence<'_>,
) -> Result<()> {
    if rights.is_subset_of(effective) {
        Ok(())
    } else if effective.is_empty() {
        Err(absence.not_found(inode_id))
    } else {
        Err(CoreError::Forbidden { inode_id })
    }
}

/// Authorization for one read: the subject's rights, and where they are
/// evaluated. A live read evaluates on the session it reads through. A
/// snapshot read evaluates at the current head, on a separate view, and
/// every snapshot read is a history read.
///
/// The rights that [`Self::can_read`] finds are memoized for the rest of the
/// read, so a listing walks the ancestors that its children share once.
pub(crate) struct ReadAccess<'a, S: ObjectStore + ?Sized> {
    authorizer: Authorizer<'a>,
    at_head: Option<HeadAccess<'a, S>>,
    rights: EffectiveRightsMemo,
}

/// The current head that a snapshot read's checks evaluate at.
struct HeadAccess<'a, S: ObjectStore + ?Sized> {
    view: &'a LoadedMetadataView<'a, S>,
    /// The session that every [`ReadAccess::can_read`] of the read evaluates
    /// on, so its caches serve every check.
    session: MetadataViewSession<'a, 'a, S>,
}

impl<'a, S: ObjectStore + ?Sized> HeadAccess<'a, S> {
    fn new(view: &'a LoadedMetadataView<'a, S>) -> Self {
        Self {
            view,
            session: view.metadata_view().session(),
        }
    }
}

impl<'a, S: ObjectStore + ?Sized> ReadAccess<'a, S> {
    pub(crate) fn live(authorizer: Authorizer<'a>) -> Self {
        Self {
            authorizer,
            at_head: None,
            rights: EffectiveRightsMemo::default(),
        }
    }

    pub(crate) fn at_head(authorizer: Authorizer<'a>, head: &'a LoadedMetadataView<'a, S>) -> Self {
        Self {
            authorizer,
            at_head: Some(HeadAccess::new(head)),
            rights: EffectiveRightsMemo::default(),
        }
    }

    /// Returns the same read's authorization with nothing memoized, for a
    /// lookup that runs concurrently with the read's other lookups.
    pub(crate) fn with_empty_memo(&self) -> Self {
        Self {
            authorizer: self.authorizer,
            at_head: self.at_head.as_ref().map(|head| HeadAccess::new(head.view)),
            rights: EffectiveRightsMemo::default(),
        }
    }

    pub(crate) fn is_service(&self) -> bool {
        matches!(self.authorizer, Authorizer::Service)
    }

    /// Requires `rights` on `inode_id`, plus `history` for a snapshot read.
    ///
    /// For a request with principals, a snapshot read of an inode that an expired
    /// deletion hides at the head (a deletion at or below the head
    /// manifest's retention floor), or whose ancestry is disconnected there,
    /// also requires an administrator, so any other subject gets the
    /// not-found error that a live read gives
    /// ([API section 6.8](https://github.com/loonfs/loonfs/blob/main/docs/specs/api.md#authorization)).
    /// A base compaction can remove a hidden inode's boundary row while a grant
    /// above it is still stored (for example, when a false positive of the kept
    /// inode filter keeps the grant), so judging a non-administrator's rights on
    /// the head's rows under an expired subtree could widen them. The walk is
    /// [`ancestry_allows_read`].
    pub(crate) async fn require(
        &self,
        session: &mut MetadataViewSession<'_, '_, S>,
        inode_id: InodeId,
        rights: AccessRights,
        absence: Absence<'_>,
    ) -> Result<()> {
        if let Some(head) = &self.at_head {
            let rights = rights.union(AccessRight::History.into());
            let mut head = head.view.metadata_view().session();
            if refuses_at_head(&self.authorizer, &mut head, inode_id).await? {
                return Err(absence.not_found(inode_id));
            }
            require(&self.authorizer, &mut head, inode_id, rights, absence).await
        } else {
            require(&self.authorizer, session, inode_id, rights, absence).await
        }
    }

    /// Whether the subject has `read` on `inode_id` (always in the service
    /// context). For a snapshot read, the subject also needs
    /// administrator rights when the inode's ancestry at the head refuses the
    /// read, as [`Self::require`] describes.
    pub(crate) async fn can_read(
        &mut self,
        session: &mut MetadataViewSession<'_, '_, S>,
        inode_id: InodeId,
    ) -> Result<bool> {
        let Authorizer::Subject { principals } = self.authorizer else {
            return Ok(true);
        };
        let mut rights = AccessRights::from(AccessRight::Read);
        let effective = if let Some(head) = &mut self.at_head {
            rights = rights.union(AccessRight::History.into());
            if refuses_at_head(&self.authorizer, &mut head.session, inode_id).await? {
                return Ok(false);
            }
            self.rights
                .effective_rights(&mut head.session, principals, inode_id)
                .await?
        } else {
            self.rights
                .effective_rights(session, principals, inode_id)
                .await?
        };
        Ok(rights.is_subset_of(effective))
    }
}

/// Whether a snapshot read of `inode_id` is refused at the head, as
/// [`ReadAccess::require`] describes: the request has principals, the
/// subject isn't an administrator, and the inode's ancestry at the head
/// refuses the read.
async fn refuses_at_head<S: ObjectStore + ?Sized>(
    authorizer: &Authorizer<'_>,
    head: &mut MetadataViewSession<'_, '_, S>,
    inode_id: InodeId,
) -> Result<bool> {
    let Authorizer::Subject { principals } = authorizer else {
        return Ok(false);
    };
    Ok(!is_administrator(head, principals).await? && !ancestry_allows_read(head, inode_id).await?)
}
