//! Routing: the URL paths and methods that the simulated server serves, and their parameters.

use crate::error::{MountSimError, Result};
use crate::trace::Operation;
use http::Method;
use loonfs_types::{ErrorCode, InodeId, NamespaceId, PinId, RevisionNo, UploadId};

/// The path prefix of the signed URLs in a download grant.
pub(crate) const GRANT_PATH_PREFIX: &str = "/sim/grants/";

/// One routed request, with its path parameters parsed.
#[derive(Debug)]
pub(crate) enum Route {
    GetCapabilities,
    GetNamespace(NamespaceId),
    GetPathEntry(NamespaceId),
    ListPathEntries(NamespaceId),
    GetFileContent(NamespaceId),
    CreateDownload(NamespaceId),
    GetInode(NamespaceId, InodeId),
    ListInodeChildren(NamespaceId, InodeId),
    GetFileContentByInode(NamespaceId, InodeId),
    CreateDownloadByInode(NamespaceId, InodeId),
    GetFileRevisionContentByInode(NamespaceId, InodeId, RevisionNo),
    CreateRevisionDownloadByInode(NamespaceId, InodeId, RevisionNo),
    ListChanges(NamespaceId),
    CreateCommit(NamespaceId),
    CreateSnapshot(NamespaceId),
    GetSnapshot(NamespaceId, PinId),
    ExtendSnapshot(NamespaceId, PinId),
    DeleteSnapshot(NamespaceId, PinId),
    CreateUpload(NamespaceId),
    GetUpload(NamespaceId, UploadId),
    PutUploadContent(NamespaceId, UploadId),
    CompleteUpload(NamespaceId, UploadId),
    AbortUpload(NamespaceId, UploadId),
    ReadDownloadRange(u64),
}

/// A request that couldn't be routed, with the operation its path matched, if any.
#[derive(Debug)]
pub(crate) struct Unrouted {
    pub(crate) operation: Operation,
    pub(crate) error: MountSimError,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Verb {
    Get,
    Post,
    Put,
    Delete,
    Other,
}

impl From<&Method> for Verb {
    fn from(method: &Method) -> Self {
        match *method {
            Method::GET => Self::Get,
            Method::POST => Self::Post,
            Method::PUT => Self::Put,
            Method::DELETE => Self::Delete,
            _ => Self::Other,
        }
    }
}

/// Routes a request by its method and URL path, and returns the operation it asks for.
pub(crate) fn route(
    method: &Method,
    path: &str,
) -> std::result::Result<(Operation, Route), Unrouted> {
    let segments: Vec<&str> = path.split('/').skip(1).collect();
    let operation = operation(Verb::from(method), &segments)?;
    parse_route(operation, &segments)
        .map(|route| (operation, route))
        .map_err(|error| Unrouted { operation, error })
}

fn operation(verb: Verb, segments: &[&str]) -> std::result::Result<Operation, Unrouted> {
    use Operation::{
        AbortUpload, CompleteUpload, CreateCommit, CreateDownload, CreateDownloadByInode,
        CreateRevisionDownloadByInode, CreateSnapshot, CreateUpload, DeleteSnapshot,
        ExtendSnapshot, GetCapabilities, GetFileContent, GetFileContentByInode,
        GetFileRevisionContentByInode, GetInode, GetNamespace, GetPathEntry, GetSnapshot,
        GetUpload, ListChanges, ListInodeChildren, ListPathEntries, PutUploadContent,
        ReadDownloadRange,
    };
    use Verb::{Delete, Get, Post, Put};
    let allowed: &[(Verb, Operation)] = match segments {
        ["v0", "capabilities"] => &[(Get, GetCapabilities)],
        ["v0", "namespaces", _] => &[(Get, GetNamespace)],
        ["v0", "namespaces", _, "filesystem", "entry"] => &[(Get, GetPathEntry)],
        ["v0", "namespaces", _, "filesystem", "entries"] => &[(Get, ListPathEntries)],
        ["v0", "namespaces", _, "filesystem", "content"] => &[(Get, GetFileContent)],
        ["v0", "namespaces", _, "filesystem", "downloads"] => &[(Post, CreateDownload)],
        ["v0", "namespaces", _, "inodes", _] => &[(Get, GetInode)],
        ["v0", "namespaces", _, "inodes", _, "children"] => &[(Get, ListInodeChildren)],
        ["v0", "namespaces", _, "inodes", _, "content"] => &[(Get, GetFileContentByInode)],
        ["v0", "namespaces", _, "inodes", _, "downloads"] => &[(Post, CreateDownloadByInode)],
        ["v0", "namespaces", _, "inodes", _, "revisions", _, "content"] => {
            &[(Get, GetFileRevisionContentByInode)]
        }
        ["v0", "namespaces", _, "inodes", _, "revisions", _, "downloads"] => {
            &[(Post, CreateRevisionDownloadByInode)]
        }
        ["v0", "namespaces", _, "changes"] => &[(Get, ListChanges)],
        ["v0", "namespaces", _, "commits"] => &[(Post, CreateCommit)],
        ["v0", "namespaces", _, "snapshots"] => &[(Post, CreateSnapshot)],
        ["v0", "namespaces", _, "snapshots", _] => &[(Get, GetSnapshot), (Delete, DeleteSnapshot)],
        ["v0", "namespaces", _, "snapshots", _, "extend"] => &[(Post, ExtendSnapshot)],
        ["v0", "namespaces", _, "uploads"] => &[(Post, CreateUpload)],
        ["v0", "namespaces", _, "uploads", _] => &[(Get, GetUpload)],
        ["v0", "namespaces", _, "uploads", _, "content"] => &[(Put, PutUploadContent)],
        ["v0", "namespaces", _, "uploads", _, "complete"] => &[(Post, CompleteUpload)],
        ["v0", "namespaces", _, "uploads", _, "abort"] => &[(Post, AbortUpload)],
        ["sim", "grants", _] => &[(Get, ReadDownloadRange)],
        _ => {
            return Err(Unrouted {
                operation: Operation::Unrouted,
                error: route_not_found(),
            })
        }
    };
    allowed
        .iter()
        .find(|(allowed_verb, _)| *allowed_verb == verb)
        .map(|(_, operation)| *operation)
        .ok_or_else(|| Unrouted {
            operation: Operation::Unrouted,
            error: MountSimError::new(
                ErrorCode::MethodNotAllowed,
                "the route doesn't serve this method",
            ),
        })
}

fn parse_route(operation: Operation, segments: &[&str]) -> Result<Route> {
    use Operation as Op;
    let ns = || {
        NamespaceId::parse(segments[2])
            .map_err(|error| MountSimError::invalid_request(error.to_string()).with_param("ns"))
    };
    let inode = || {
        loonfs_types::public_inode_id::decode(segments[4]).map_err(|error| {
            MountSimError::invalid_request(error.to_string()).with_param("inode_id")
        })
    };
    let revision = || {
        segments[6]
            .parse::<u64>()
            .ok()
            .and_then(|value| RevisionNo::parse(value).ok())
            .ok_or_else(|| {
                MountSimError::invalid_request(format!("invalid revision_no {:?}", segments[6]))
                    .with_param("revision_no")
            })
    };
    let snapshot = || {
        PinId::parse(segments[4]).map_err(|error| {
            MountSimError::invalid_request(error.to_string()).with_param("snapshot_id")
        })
    };
    let upload = || {
        UploadId::parse(segments[4]).map_err(|error| {
            MountSimError::invalid_request(error.to_string()).with_param("upload_id")
        })
    };
    Ok(match operation {
        Op::GetCapabilities => Route::GetCapabilities,
        Op::ReadDownloadRange => Route::ReadDownloadRange(segments[2].parse().map_err(|_| {
            MountSimError::new(ErrorCode::RouteNotFound, "no download grant has this ID")
        })?),
        Op::GetNamespace => Route::GetNamespace(ns()?),
        Op::GetPathEntry => Route::GetPathEntry(ns()?),
        Op::ListPathEntries => Route::ListPathEntries(ns()?),
        Op::GetFileContent => Route::GetFileContent(ns()?),
        Op::CreateDownload => Route::CreateDownload(ns()?),
        Op::GetInode => Route::GetInode(ns()?, inode()?),
        Op::ListInodeChildren => Route::ListInodeChildren(ns()?, inode()?),
        Op::GetFileContentByInode => Route::GetFileContentByInode(ns()?, inode()?),
        Op::CreateDownloadByInode => Route::CreateDownloadByInode(ns()?, inode()?),
        Op::GetFileRevisionContentByInode => {
            Route::GetFileRevisionContentByInode(ns()?, inode()?, revision()?)
        }
        Op::CreateRevisionDownloadByInode => {
            Route::CreateRevisionDownloadByInode(ns()?, inode()?, revision()?)
        }
        Op::ListChanges => Route::ListChanges(ns()?),
        Op::CreateCommit => Route::CreateCommit(ns()?),
        Op::CreateSnapshot => Route::CreateSnapshot(ns()?),
        Op::GetSnapshot => Route::GetSnapshot(ns()?, snapshot()?),
        Op::ExtendSnapshot => Route::ExtendSnapshot(ns()?, snapshot()?),
        Op::DeleteSnapshot => Route::DeleteSnapshot(ns()?, snapshot()?),
        Op::CreateUpload => Route::CreateUpload(ns()?),
        Op::GetUpload => Route::GetUpload(ns()?, upload()?),
        Op::PutUploadContent => Route::PutUploadContent(ns()?, upload()?),
        Op::CompleteUpload => Route::CompleteUpload(ns()?, upload()?),
        Op::AbortUpload => Route::AbortUpload(ns()?, upload()?),
        Op::Unrouted => return Err(route_not_found()),
    })
}

fn route_not_found() -> MountSimError {
    MountSimError::new(
        ErrorCode::RouteNotFound,
        "no route of the simulated server matches the request path",
    )
}
