//! Download grants, and the signed-range reads that their URLs make.
//!
//! A grant's URLs point back at the simulated server, so the client's download stream reads them
//! through the same service. In a real deployment, those reads go to the object store.

use crate::content_reads::{current_revision, file, find_revision, usize_offset};
use crate::entries::{inode_not_found, path_lookup_error};
use crate::error::{MountSimError, Result};
use crate::query::{ReadPoint, READ_REVALIDATION_BOUND_MS};
use crate::reply::Reply;
use crate::routes::GRANT_PATH_PREFIX;
use crate::state::ServerState;
use crate::trace::ByteRange;
use bytes::Bytes;
use http::header::{HeaderMap, CONTENT_LENGTH, CONTENT_RANGE, RANGE};
use http::StatusCode;
use loonfs_model::visibility::PathLookup;
use loonfs_types::api::v0::{
    CreateDownloadByInodeRequest, CreateDownloadByInodeResponse, CreateDownloadRequest,
    CreateDownloadResponse, CreateRevisionDownloadByInodeRequest, DownloadRange,
    ObjectTransferAccess,
};
use loonfs_types::options::ReadConsistency;
use loonfs_types::{
    ContentRef, ErrorCode, InodeId, NamespaceId, PinId, RevisionNo, FEATURE_DOWNLOADS_DIRECT_GET,
};
use std::collections::BTreeMap;

/// The signed ranges that download grants have issued, by the ID in their URL.
#[derive(Debug, Default)]
pub(crate) struct Grants {
    next_id: u64,
    issued: BTreeMap<u64, Grant>,
}

/// One signed range: the chain bytes that its URL reads, and when the URL expires.
#[derive(Debug)]
struct Grant {
    content_ref: ContentRef,
    range: ByteRange,
    expires_at_ms: u64,
}

/// The fields that select what a download reads, from any of the three request shapes.
struct DownloadTarget<'a> {
    snapshot_id: Option<&'a PinId>,
    consistency: Option<ReadConsistency>,
    max_staleness_ms: Option<u64>,
    start_offset: u64,
}

pub(crate) fn create_download(
    state: &mut ServerState,
    namespace_id: &NamespaceId,
    request: &CreateDownloadRequest,
    origin: &str,
) -> Result<CreateDownloadResponse> {
    let target = DownloadTarget {
        snapshot_id: request.snapshot_id.as_ref(),
        consistency: request.consistency,
        max_staleness_ms: request.max_staleness_ms,
        start_offset: request.start_offset,
    };
    let at = target.read_point(state)?;
    if request.revision_no.is_some() && at != ReadPoint::Live {
        return Err(MountSimError::invalid_request(
            "`revision_no` can't be combined with `snapshot_id`",
        )
        .with_param("/snapshot_id"));
    }
    let namespace = state.namespace(namespace_id)?;
    let view = namespace.view(namespace.read_seq(&at, state.now_ms())?);
    let inode = match view.resolve_path(&request.path) {
        PathLookup::Found { inode, .. } => file(inode)?,
        lookup => return Err(path_lookup_error(&lookup)),
    };
    let revision = match request.revision_no {
        Some(revision_no) => find_revision(&view, inode.inode_id, revision_no)?,
        None => current_revision(&view, inode),
    };
    let (revision_no, content_ref) = (revision.revision_no, revision.content_ref.clone());
    let path = view.inode_path(inode.inode_id);
    let ranges = grant_ranges(state, &content_ref, target.start_offset, origin)?;
    Ok(CreateDownloadResponse {
        namespace_id: namespace_id.clone(),
        path,
        revision_no,
        content_ref,
        ranges,
    })
}

pub(crate) fn create_download_by_inode(
    state: &mut ServerState,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    request: &CreateDownloadByInodeRequest,
    origin: &str,
) -> Result<CreateDownloadByInodeResponse> {
    let target = DownloadTarget {
        snapshot_id: request.snapshot_id.as_ref(),
        consistency: request.consistency,
        max_staleness_ms: request.max_staleness_ms,
        start_offset: request.start_offset,
    };
    let at = target.read_point(state)?;
    let namespace = state.namespace(namespace_id)?;
    let view = namespace.view(namespace.read_seq(&at, state.now_ms())?);
    let inode = file(
        view.visible_inode(inode_id)
            .ok_or_else(|| inode_not_found(inode_id))?,
    )?;
    let revision = current_revision(&view, inode);
    let (revision_no, content_ref) = (revision.revision_no, revision.content_ref.clone());
    grant_by_inode(
        state,
        namespace_id,
        inode_id,
        revision_no,
        content_ref,
        &target,
        origin,
    )
}

pub(crate) fn create_revision_download_by_inode(
    state: &mut ServerState,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    revision_no: RevisionNo,
    request: &CreateRevisionDownloadByInodeRequest,
    origin: &str,
) -> Result<CreateDownloadByInodeResponse> {
    let target = DownloadTarget {
        snapshot_id: None,
        consistency: request.consistency,
        max_staleness_ms: request.max_staleness_ms,
        start_offset: request.start_offset,
    };
    target.read_point(state)?;
    let namespace = state.namespace(namespace_id)?;
    let view = namespace.view(namespace.head_seq());
    let inode = file(
        view.retained_inode(inode_id)
            .ok_or_else(|| inode_not_found(inode_id))?,
    )?;
    let content_ref = find_revision(&view, inode.inode_id, revision_no)?
        .content_ref
        .clone();
    grant_by_inode(
        state,
        namespace_id,
        inode_id,
        revision_no,
        content_ref,
        &target,
        origin,
    )
}

fn grant_by_inode(
    state: &mut ServerState,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    revision_no: RevisionNo,
    content_ref: ContentRef,
    target: &DownloadTarget<'_>,
    origin: &str,
) -> Result<CreateDownloadByInodeResponse> {
    let ranges = grant_ranges(state, &content_ref, target.start_offset, origin)?;
    Ok(CreateDownloadByInodeResponse {
        namespace_id: namespace_id.clone(),
        inode_id,
        revision_no,
        content_ref,
        ranges,
    })
}

impl DownloadTarget<'_> {
    /// Checks that the deployment grants downloads and that the read fields are consistent, and
    /// returns what the download reads.
    fn read_point(&self, state: &ServerState) -> Result<ReadPoint> {
        if !state.config.capabilities.advertise_direct_get {
            return Err(MountSimError::new(
                ErrorCode::NotSupported,
                "the deployment doesn't grant direct downloads",
            )
            .with_feature(FEATURE_DOWNLOADS_DIRECT_GET));
        }
        if let Some(staleness) = self.max_staleness_ms {
            if self.consistency != Some(ReadConsistency::Eventual)
                || staleness > READ_REVALIDATION_BOUND_MS
            {
                return Err(MountSimError::invalid_request(format!(
                    "`max_staleness_ms` requires `consistency` `eventual` and can't exceed \
                     {READ_REVALIDATION_BOUND_MS}"
                ))
                .with_param("/max_staleness_ms"));
            }
        }
        match self.snapshot_id {
            None => Ok(ReadPoint::Live),
            Some(_) if self.consistency.is_some() => Err(MountSimError::invalid_request(
                "`consistency` can't be combined with `snapshot_id`",
            )
            .with_param("/consistency")),
            Some(snapshot_id) => Ok(ReadPoint::Snapshot(snapshot_id.clone())),
        }
    }
}

/// Signs the ranges that cover `[start_offset, size_bytes)` of `content_ref`.
fn grant_ranges(
    state: &mut ServerState,
    content_ref: &ContentRef,
    start_offset: u64,
    origin: &str,
) -> Result<Vec<DownloadRange>> {
    let size_bytes = content_ref.size_bytes;
    if start_offset >= size_bytes && !(size_bytes == 0 && start_offset == 0) {
        return Err(MountSimError::invalid_request(format!(
            "`start_offset` {start_offset} is at or past the end of the {size_bytes}-byte revision"
        ))
        .with_param("/start_offset"));
    }
    let expires_at_ms = state
        .now_ms()
        .saturating_add(state.config.download_url_ttl_ms);
    let max_range_bytes = state.config.max_download_range_bytes.max(1);
    let mut ranges = Vec::new();
    let mut offset = start_offset;
    loop {
        let end_offset = offset.saturating_add(max_range_bytes).min(size_bytes);
        let range = ByteRange {
            start_offset: offset,
            end_offset,
        };
        let id = state.grants.issue(Grant {
            content_ref: content_ref.clone(),
            range,
            expires_at_ms,
        });
        // A zero-byte revision's one range signs no `Range` header.
        let headers = match range.length() {
            0 => BTreeMap::new(),
            _ => BTreeMap::from([("range".to_owned(), signed_range(range))]),
        };
        ranges.push(DownloadRange {
            start_offset: offset,
            length: range.length(),
            access: ObjectTransferAccess::PresignedUrl {
                method: "GET".to_owned(),
                url: format!("{origin}{GRANT_PATH_PREFIX}{id}"),
                headers,
                expires_at_ms,
            },
        });
        offset = end_offset;
        if offset >= size_bytes {
            return Ok(ranges);
        }
    }
}

fn signed_range(range: ByteRange) -> String {
    format!("bytes={}-{}", range.start_offset, range.end_offset - 1)
}

impl Grants {
    fn issue(&mut self, grant: Grant) -> u64 {
        self.next_id += 1;
        self.issued.insert(self.next_id, grant);
        self.next_id
    }
}

/// Serves a `GET` of one signed range, refusing it as an object store would when the URL has
/// expired or the request's headers don't match the signature.
pub(crate) fn read_download_range(
    state: &ServerState,
    grant_id: u64,
    headers: &HeaderMap,
) -> Reply {
    let Some(grant) = state.grants.issued.get(&grant_id) else {
        return Reply::plain(StatusCode::NOT_FOUND, "no object matches the signed URL");
    };
    if state.now_ms() >= grant.expires_at_ms {
        return Reply::plain(StatusCode::FORBIDDEN, "the signed URL has expired");
    }
    let sent_range = headers.get(RANGE).and_then(|value| value.to_str().ok());
    let signed = (grant.range.length() > 0).then(|| signed_range(grant.range));
    if sent_range != signed.as_deref() {
        return Reply::plain(
            StatusCode::FORBIDDEN,
            "the request's headers don't match the signed headers",
        );
    }
    let bytes = state.content.read(&grant.content_ref);
    let slice =
        &bytes[usize_offset(grant.range.start_offset)..usize_offset(grant.range.end_offset)];
    Reply::content(
        StatusCode::PARTIAL_CONTENT,
        Bytes::copy_from_slice(slice),
        &grant.content_ref,
        grant.range,
    )
    .with_header(
        CONTENT_RANGE,
        format!(
            "bytes {}-{}/{}",
            grant.range.start_offset,
            grant.range.end_offset.saturating_sub(1),
            grant.content_ref.size_bytes
        ),
    )
    .with_header(CONTENT_LENGTH, grant.range.length().to_string())
}
