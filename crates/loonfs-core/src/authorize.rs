//! Authorizes reads and commits against the subject's effective rights.

use crate::error::{CoreError, Result};
use crate::metadata::access::{effective_rights, is_administrator};
use crate::metadata::MetadataViewSession;
use crate::metadata::{ancestry_allows_read, MetadataVisibilityReads};
use crate::path::read::LoadedMetadataView;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{
    AccessRight, AccessRights, InodeId, NamespaceAccess, NamespaceId, PrincipalSet, Subject,
};

pub(crate) enum Authorizer<'a> {
    /// An unrestricted namespace: every check passes without a lookup.
    Unrestricted,
    Subject {
        principals: &'a PrincipalSet,
    },
}

/// Who a commit is authorized as.
#[derive(Clone, Copy)]
pub(crate) enum CommitAuthority<'a> {
    /// The request's subject, or the token holder when absent.
    Subject(Option<&'a Subject>),
    /// A maintenance commit: no subject check on any namespace.
    Maintenance,
}

impl<'a> Authorizer<'a> {
    pub(crate) fn for_request(
        namespace_id: &NamespaceId,
        access: &NamespaceAccess,
        authority: CommitAuthority<'a>,
    ) -> Result<Self> {
        match (access, authority) {
            (NamespaceAccess::Unrestricted {}, _) | (_, CommitAuthority::Maintenance) => {
                Ok(Self::Unrestricted)
            }
            (
                NamespaceAccess::Acl {
                    principal_scope, ..
                },
                CommitAuthority::Subject(Some(subject)),
            ) if subject.principal_scope == *principal_scope => Ok(Self::Subject {
                principals: &subject.principals,
            }),
            (
                NamespaceAccess::Acl {
                    principal_scope, ..
                },
                CommitAuthority::Subject(Some(subject)),
            ) => Err(CoreError::PrincipalScopeMismatch {
                expected_principal_scope: principal_scope.clone(),
                actual_principal_scope: subject.principal_scope.clone(),
            }),
            (NamespaceAccess::Acl { .. }, CommitAuthority::Subject(None)) => {
                Err(CoreError::SubjectRequired {
                    namespace_id: namespace_id.clone(),
                })
            }
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
pub(crate) struct ReadAccess<'a, S: ObjectStore + ?Sized> {
    authorizer: Authorizer<'a>,
    at_head: Option<&'a LoadedMetadataView<'a, S>>,
}

impl<'a, S: ObjectStore + ?Sized> ReadAccess<'a, S> {
    pub(crate) fn live(authorizer: Authorizer<'a>) -> Self {
        Self {
            authorizer,
            at_head: None,
        }
    }

    pub(crate) fn at_head(authorizer: Authorizer<'a>, head: &'a LoadedMetadataView<'a, S>) -> Self {
        Self {
            authorizer,
            at_head: Some(head),
        }
    }

    pub(crate) fn is_unrestricted(&self) -> bool {
        matches!(self.authorizer, Authorizer::Unrestricted)
    }

    /// Requires `rights` on `inode_id`, plus `history` for a snapshot read.
    ///
    /// In an ACL namespace, a snapshot read of an inode that an expired
    /// deletion hides at the head (a deletion at or below the head
    /// manifest's retention floor), or whose ancestry is disconnected there,
    /// also requires an administrator, so any other subject gets the
    /// not-found error that a live read gives
    /// ([API section 6.8](https://github.com/loonfs/loonfs/blob/main/docs/specs/api.md#authorization-in-acl-namespaces)).
    /// Base compactions remove the head's rows under an expired subtree a
    /// family group at a time, so a boundary row can be gone while a grant
    /// above it is still stored, and judging a non-administrator's rights on
    /// those rows could widen them. The walk is [`ancestry_allows_read`].
    pub(crate) async fn require(
        &self,
        session: &mut MetadataViewSession<'_, '_, S>,
        inode_id: InodeId,
        rights: AccessRights,
        absence: Absence<'_>,
    ) -> Result<()> {
        if let Some(head) = self.at_head {
            let rights = rights.union(AccessRights::from_iter([AccessRight::History]));
            let mut head = head.metadata_view().session();
            if self.refuses_at_head(&mut head, inode_id).await? {
                return Err(absence.not_found(inode_id));
            }
            require(&self.authorizer, &mut head, inode_id, rights, absence).await
        } else {
            require(&self.authorizer, session, inode_id, rights, absence).await
        }
    }

    /// Whether the subject holds `read` on `inode_id` (always for an
    /// unrestricted namespace). For a snapshot read, the subject also needs
    /// administrator rights when the inode's ancestry at the head refuses the
    /// read, as [`Self::require`] describes.
    pub(crate) async fn can_read(
        &self,
        session: &mut MetadataViewSession<'_, '_, S>,
        inode_id: InodeId,
    ) -> Result<bool> {
        let Authorizer::Subject { principals } = &self.authorizer else {
            return Ok(true);
        };
        let mut rights = AccessRights::from_iter([AccessRight::Read]);
        let effective = if let Some(head) = self.at_head {
            rights = rights.union(AccessRights::from_iter([AccessRight::History]));
            let mut head = head.metadata_view().session();
            if self.refuses_at_head(&mut head, inode_id).await? {
                return Ok(false);
            }
            effective_rights(&mut head, principals, inode_id).await?
        } else {
            effective_rights(session, principals, inode_id).await?
        };
        Ok(rights.is_subset_of(effective))
    }

    /// Whether a snapshot read of `inode_id` is refused at the head, as
    /// [`Self::require`] describes: the namespace is an ACL namespace, the
    /// subject isn't an administrator, and the inode's ancestry at the head
    /// refuses the read.
    async fn refuses_at_head(
        &self,
        head: &mut MetadataViewSession<'_, '_, S>,
        inode_id: InodeId,
    ) -> Result<bool> {
        let Authorizer::Subject { principals } = &self.authorizer else {
            return Ok(false);
        };
        Ok(!is_administrator(head, principals).await?
            && !ancestry_allows_read(head, inode_id).await?)
    }
}
