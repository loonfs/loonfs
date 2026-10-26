//! The tower service that the client sends its requests to, and the dispatch of each request to
//! its handler.

use crate::commits::{self, CommitIdentity, CommitOutcome, MAX_COMMIT_REQUEST_BODY_BYTES};
use crate::content_reads::{self, requested_range};
use crate::error::{MountSimError, Result};
use crate::faults::Delivery;
use crate::query::Query;
use crate::reply::{FreshCommit, Reply};
use crate::routes::{route, Route};
use crate::server::SIMULATED_SERVER_URL;
use crate::state::ServerState;
use crate::trace::RequestRecord;
use crate::{downloads, entries, feed, listing, snapshots, uploads};
use bytes::Bytes;
use futures::future::BoxFuture;
use http::request::Parts;
use http::{HeaderMap, Request, Response};
use http_body_util::BodyExt as _;
use loonfs_client::{Body, TransportError};
use loonfs_types::{ActorId, ErrorCode, NamespaceId, PrincipalScope, PrincipalSet, SubjectId};
use serde::de::DeserializeOwned;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tower::Service;

/// The simulated server as a tower service, which `loonfs_client::Client::with_transport`
/// accepts.
///
/// Clones share one server.
#[derive(Debug, Clone)]
pub struct SimulatedService {
    shared: Arc<Mutex<ServerState>>,
}

impl SimulatedService {
    pub(crate) fn new(shared: Arc<Mutex<ServerState>>) -> Self {
        Self { shared }
    }
}

impl Service<Request<Body>> for SimulatedService {
    type Response = Response<Body>;
    type Error = TransportError;
    type Future = BoxFuture<'static, std::result::Result<Response<Body>, TransportError>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), TransportError>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        Box::pin(serve(self.shared.clone(), request))
    }
}

async fn serve(
    shared: Arc<Mutex<ServerState>>,
    request: Request<Body>,
) -> std::result::Result<Response<Body>, TransportError> {
    let (parts, body) = request.into_parts();
    let body = body.collect().await?.to_bytes();
    let exchange = shared
        .lock()
        .expect("the simulated server's lock should not be poisoned")
        .respond(&parts, &body);
    match exchange.delivery {
        Delivery::Now => {}
        Delivery::Dropped => {
            return Err(TransportError::new(std::io::Error::other(
                "the simulated server dropped the response",
            )))
        }
        Delivery::Held(hold) => hold.hold_response().await,
    }
    Ok(exchange.reply.into_response(&exchange.request_id))
}

/// A response, and how the service delivers it.
struct Exchange {
    reply: Reply,
    delivery: Delivery,
    request_id: String,
}

impl ServerState {
    /// Handles one request, records it in the trace, and decides how its response is delivered.
    fn respond(&mut self, parts: &Parts, body: &Bytes) -> Exchange {
        let request_id = self.ids.request_id();
        let step = self.trace.next_step();
        let at_ms = self.now_ms();
        let query = Query::parse(parts.uri.query());
        let (operation, result) = match route(&parts.method, parts.uri.path()) {
            Ok((operation, route)) => (operation, self.dispatch(route, parts, &query, body)),
            Err(unrouted) => (unrouted.operation, Err(unrouted.error)),
        };
        let mut reply = result.unwrap_or_else(|error| Reply::error(&error, &request_id));
        let (delivery, fault) = self.faults.deliver(operation, &mut reply);
        self.trace.record(RequestRecord {
            step,
            at_ms,
            method: parts.method.clone(),
            operation,
            path: parts.uri.path().to_owned(),
            query: query.pairs().to_vec(),
            byte_range: reply.byte_range,
            status: reply.status.as_u16(),
            error_code: reply.error_code,
            fault,
        });
        Exchange {
            reply,
            delivery,
            request_id,
        }
    }

    fn dispatch(
        &mut self,
        route: Route,
        parts: &Parts,
        query: &Query,
        body: &Bytes,
    ) -> Result<Reply> {
        let origin = origin(parts);
        match route {
            Route::GetCapabilities => {
                query.ensure_only(&[])?;
                Ok(Reply::json(&self.config.capability_document()))
            }
            Route::GetNamespace(ns) => {
                query.ensure_only(&[])?;
                Ok(Reply::json(&self.namespace(&ns)?.metadata_response()))
            }
            Route::GetPathEntry(ns) => entries::get_path_entry(self, &ns, query),
            Route::GetInode(ns, inode_id) => entries::get_inode(self, &ns, inode_id, query),
            Route::ListPathEntries(ns) => listing::list_path_entries(self, &ns, query),
            Route::ListInodeChildren(ns, inode_id) => {
                listing::list_inode_children(self, &ns, inode_id, query)
            }
            Route::GetFileContent(ns) => {
                let range = requested_range(&parts.headers)?;
                content_reads::get_file_content(self, &ns, query, range)
            }
            Route::GetFileContentByInode(ns, inode_id) => {
                let range = requested_range(&parts.headers)?;
                content_reads::get_file_content_by_inode(self, &ns, inode_id, query, range)
            }
            Route::GetFileRevisionContentByInode(ns, inode_id, revision_no) => {
                let range = requested_range(&parts.headers)?;
                content_reads::get_file_revision_content_by_inode(
                    self,
                    &ns,
                    inode_id,
                    revision_no,
                    query,
                    range,
                )
            }
            Route::CreateDownload(ns) => {
                query.ensure_only(&[])?;
                let request = decode_json(body)?;
                downloads::create_download(self, &ns, &request, &origin).map(|r| Reply::json(&r))
            }
            Route::CreateDownloadByInode(ns, inode_id) => {
                query.ensure_only(&[])?;
                let request = decode_optional_json(body)?;
                downloads::create_download_by_inode(self, &ns, inode_id, &request, &origin)
                    .map(|r| Reply::json(&r))
            }
            Route::CreateRevisionDownloadByInode(ns, inode_id, revision_no) => {
                query.ensure_only(&[])?;
                let request = decode_optional_json(body)?;
                downloads::create_revision_download_by_inode(
                    self,
                    &ns,
                    inode_id,
                    revision_no,
                    &request,
                    &origin,
                )
                .map(|r| Reply::json(&r))
            }
            Route::ReadDownloadRange(grant_id) => Ok(downloads::read_download_range(
                self,
                grant_id,
                &parts.headers,
            )),
            Route::ListChanges(ns) => {
                let horizon = self.faults.feed_horizon(&ns);
                feed::list_changes(self, &ns, query, horizon)
            }
            Route::CreateCommit(ns) => self.create_commit(&ns, &parts.headers, query, body),
            Route::CreateSnapshot(ns) => {
                query.ensure_only(&[])?;
                snapshots::create_snapshot(self, &ns, &decode_json(body)?).map(|r| Reply::json(&r))
            }
            Route::GetSnapshot(ns, snapshot_id) => {
                query.ensure_only(&[])?;
                snapshots::get_snapshot(self, &ns, &snapshot_id).map(|r| Reply::json(&r))
            }
            Route::ExtendSnapshot(ns, snapshot_id) => {
                query.ensure_only(&[])?;
                snapshots::extend_snapshot(self, &ns, &snapshot_id, &decode_json(body)?)
                    .map(|r| Reply::json(&r))
            }
            Route::DeleteSnapshot(ns, snapshot_id) => {
                query.ensure_only(&[])?;
                snapshots::delete_snapshot(self, &ns, &snapshot_id).map(|r| Reply::json(&r))
            }
            Route::CreateUpload(ns) => {
                query.ensure_only(&[])?;
                uploads::create_upload(self, &ns, &decode_json(body)?).map(|r| Reply::json(&r))
            }
            Route::GetUpload(ns, upload_id) => {
                query.ensure_only(&[])?;
                uploads::get_upload(self, &ns, &upload_id).map(|r| Reply::json(&r))
            }
            Route::PutUploadContent(ns, upload_id) => {
                query.ensure_only(&[])?;
                uploads::put_upload_content(self, &ns, &upload_id, body).map(|r| Reply::json(&r))
            }
            Route::CompleteUpload(ns, upload_id) => {
                query.ensure_only(&[])?;
                uploads::complete_upload(self, &ns, &upload_id, &decode_json(body)?)
                    .map(|r| Reply::json(&r))
            }
            Route::AbortUpload(ns, upload_id) => {
                query.ensure_only(&[])?;
                uploads::abort_upload(self, &ns, &upload_id).map(|r| Reply::json(&r))
            }
        }
    }

    fn create_commit(
        &mut self,
        namespace_id: &NamespaceId,
        headers: &HeaderMap,
        query: &Query,
        body: &Bytes,
    ) -> Result<Reply> {
        query.ensure_only(&[])?;
        if body.len() > MAX_COMMIT_REQUEST_BODY_BYTES {
            return Err(MountSimError::new(
                ErrorCode::ContentTooLarge,
                format!("a commit body is at most {MAX_COMMIT_REQUEST_BODY_BYTES} bytes"),
            ));
        }
        let identity = commit_identity(headers)?;
        let request = decode_json(body)?;
        let outcome = commits::create_commit(self, namespace_id, &identity, &request)?;
        let fresh_commit = match &outcome {
            CommitOutcome::Committed(commit) => Some(FreshCommit {
                namespace_id: namespace_id.clone(),
                committed_seq: commit.committed_seq,
            }),
            CommitOutcome::Replayed(_) => None,
        };
        let mut reply = Reply::json(&outcome.into_commit());
        reply.fresh_commit = fresh_commit;
        Ok(reply)
    }
}

/// Reads a commit's actor, and its subject when the request includes a subject context (api.md,
/// "Identity headers").
fn commit_identity(headers: &HeaderMap) -> Result<CommitIdentity> {
    let header = |name: &str| -> Result<Option<&str>> {
        headers
            .get(name)
            .map(|value| {
                value.to_str().map_err(|_| {
                    MountSimError::invalid_request(format!("header {name} must be visible ASCII"))
                        .with_param(name)
                })
            })
            .transpose()
    };
    let actor = header("Loonfs-Actor")?.ok_or_else(|| {
        MountSimError::invalid_request("missing required header Loonfs-Actor")
            .with_param("Loonfs-Actor")
    })?;
    let actor = ActorId::parse(actor).map_err(|error| {
        MountSimError::invalid_request(error.to_string()).with_param("Loonfs-Actor")
    })?;
    let subject = header("Loonfs-Subject")?;
    let scope = header("Loonfs-Principal-Scope")?;
    let principals = header("Loonfs-Principals")?;
    let subject = match (subject, scope, principals) {
        (None, None, None) => None,
        (subject, Some(scope), Some(principals)) => {
            PrincipalScope::parse(scope).map_err(|error| {
                MountSimError::invalid_request(error.to_string())
                    .with_param("Loonfs-Principal-Scope")
            })?;
            principals.parse::<PrincipalSet>().map_err(|error| {
                MountSimError::invalid_request(error.to_string()).with_param("Loonfs-Principals")
            })?;
            let (value, param) = subject.map_or((actor.as_str(), "Loonfs-Actor"), |subject| {
                (subject, "Loonfs-Subject")
            });
            Some(SubjectId::parse(value).map_err(|error| {
                MountSimError::invalid_request(error.to_string()).with_param(param)
            })?)
        }
        (_, None, Some(_)) => {
            return Err(MountSimError::invalid_request(
                "`Loonfs-Principals` requires `Loonfs-Principal-Scope`",
            )
            .with_param("Loonfs-Principal-Scope"))
        }
        (_, _, None) => {
            return Err(MountSimError::invalid_request(
                "a subject context requires `Loonfs-Principals`",
            )
            .with_param("Loonfs-Principals"))
        }
    };
    Ok(CommitIdentity { actor, subject })
}

fn decode_json<T: DeserializeOwned>(body: &[u8]) -> Result<T> {
    serde_json::from_slice(body)
        .map_err(|error| MountSimError::invalid_request(format!("invalid request body: {error}")))
}

/// Decodes a body that the route accepts as optional, where an empty body means the defaults.
fn decode_optional_json<T: DeserializeOwned + Default>(body: &[u8]) -> Result<T> {
    if body.is_empty() {
        return Ok(T::default());
    }
    decode_json(body)
}

/// Returns the scheme and authority that the request was sent to, which a download grant's URLs
/// point back at.
fn origin(parts: &Parts) -> String {
    match (parts.uri.scheme_str(), parts.uri.authority()) {
        (Some(scheme), Some(authority)) => format!("{scheme}://{authority}"),
        _ => SIMULATED_SERVER_URL.to_owned(),
    }
}
