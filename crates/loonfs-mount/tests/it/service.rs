//! A scripted HTTP service for the client: each route responds with what the
//! test's responder returns, and the service records every request.

use bytes::Bytes;
use futures::future::BoxFuture;
use http::{Method, Request, Response, StatusCode};
use http_body_util::BodyExt as _;
use loonfs_client::{Body, Client, ClientConfig, TransportError};
use loonfs_types::{ErrorCode, ErrorResponse};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tower::Service;

/// One request that the service received.
#[derive(Debug, Clone)]
pub(crate) struct RecordedRequest {
    pub(crate) method: Method,
    pub(crate) path: String,
    pub(crate) query: Vec<(String, String)>,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

impl RecordedRequest {
    pub(crate) fn query(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// What a route responds with.
pub(crate) enum Reply {
    Json(serde_json::Value),
    Error(StatusCode, ErrorCode),
    /// A file's bytes, of which a request with a `Range` header gets one
    /// range.
    Content(Bytes),
}

type Responder = Box<dyn FnMut(&RecordedRequest) -> Reply + Send>;

struct Route {
    method: Method,
    path: String,
    responder: Responder,
}

#[derive(Default)]
struct State {
    routes: Vec<Route>,
    requests: Vec<RecordedRequest>,
}

/// The scripted service. Clones share routes and requests.
#[derive(Clone, Default)]
pub(crate) struct ScriptedService(Arc<Mutex<State>>);

impl ScriptedService {
    /// Answers requests for `method` and `path` (without the query) with
    /// `responder`, replacing an earlier route for them. A path that ends in
    /// `*` matches every path that starts with what's before it.
    pub(crate) fn route(
        &self,
        method: Method,
        path: &str,
        responder: impl FnMut(&RecordedRequest) -> Reply + Send + 'static,
    ) {
        let mut state = self
            .0
            .lock()
            .expect("the script lock should not be poisoned");
        state
            .routes
            .retain(|route| !(route.method == method && route.path == path));
        state.routes.push(Route {
            method,
            path: path.to_owned(),
            responder: Box::new(responder),
        });
    }

    /// Returns every request so far.
    pub(crate) fn requests(&self) -> Vec<RecordedRequest> {
        self.0
            .lock()
            .expect("the script lock should not be poisoned")
            .requests
            .clone()
    }

    /// Returns a client whose requests this service responds to.
    pub(crate) fn client(&self) -> Client {
        Client::with_transport(
            ClientConfig {
                server_url: "http://loonfs.invalid".to_owned(),
                auth_token: None,
                request_timeout_ms: None,
                disable_transient_retry: true,
                ca_cert_path: None,
            },
            self.clone(),
        )
        .expect("a valid client config")
    }

    fn answer(&self, request: RecordedRequest) -> Response<Body> {
        let mut state = self
            .0
            .lock()
            .expect("the script lock should not be poisoned");
        state.requests.push(request.clone());
        // An exact route comes first, then the route with the longest
        // wildcard prefix (a path that ends in `*`).
        let route = state
            .routes
            .iter_mut()
            .filter(|route| route.method == request.method)
            .filter_map(|route| {
                let rank = match route.path.strip_suffix('*') {
                    None if route.path == request.path => usize::MAX,
                    Some(prefix) if request.path.starts_with(prefix) => prefix.len(),
                    _ => return None,
                };
                Some((rank, route))
            })
            .max_by_key(|(rank, _)| *rank)
            .map(|(_, route)| route);
        let Some(route) = route else {
            return error_response(StatusCode::NOT_FOUND, ErrorCode::RouteNotFound);
        };
        match (route.responder)(&request) {
            Reply::Json(value) => Response::builder()
                .status(StatusCode::OK)
                .body(Body::from(Bytes::from(
                    serde_json::to_vec(&value).expect("a JSON reply should serialize"),
                )))
                .expect("a JSON response should build"),
            Reply::Error(status, code) => error_response(status, code),
            Reply::Content(bytes) => content_response(&bytes, request.header("range")),
        }
    }
}

fn error_response(status: StatusCode, code: ErrorCode) -> Response<Body> {
    let body = serde_json::to_vec(&ErrorResponse {
        code: code.as_str().to_owned(),
        feature: None,
        message: format!("scripted {}", code.as_str()),
        param: None,
        request_id: None,
        details: None,
    })
    .expect("an error envelope should serialize");
    Response::builder()
        .status(status)
        .body(Body::from(Bytes::from(body)))
        .expect("an error response should build")
}

fn content_response(bytes: &Bytes, range: Option<&str>) -> Response<Body> {
    let Some(range) = range else {
        return Response::new(Body::from(bytes.clone()));
    };
    let size = bytes.len() as u64;
    let (start, end) = range
        .strip_prefix("bytes=")
        .and_then(|range| range.split_once('-'))
        .map(|(start, end)| {
            let start: u64 = start.parse().expect("a range start");
            let end = if end.is_empty() {
                size
            } else {
                end.parse::<u64>().expect("a range end") + 1
            };
            (start, end.min(size))
        })
        .expect("a `bytes=start-end` range");
    if start >= size {
        return error_response(
            StatusCode::RANGE_NOT_SATISFIABLE,
            ErrorCode::RangeNotSatisfiable,
        );
    }
    let start_index = usize::try_from(start).expect("a test offset fits usize");
    let end_index = usize::try_from(end).expect("a test offset fits usize");
    Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(
            http::header::CONTENT_RANGE,
            format!("bytes {start}-{}/{size}", end - 1),
        )
        .body(Body::from(bytes.slice(start_index..end_index)))
        .expect("a ranged response should build")
}

impl Service<Request<Body>> for ScriptedService {
    type Response = Response<Body>;
    type Error = TransportError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move {
            let (parts, body) = request.into_parts();
            let body = body.collect().await?.to_bytes().to_vec();
            let query = parts
                .uri
                .query()
                .map(|query| {
                    form_urlencoded::parse(query.as_bytes())
                        .map(|(key, value)| (key.into_owned(), value.into_owned()))
                        .collect()
                })
                .unwrap_or_default();
            let recorded = RecordedRequest {
                method: parts.method.clone(),
                path: parts.uri.path().to_owned(),
                query,
                headers: parts
                    .headers
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.to_string(),
                            value.to_str().expect("a test header is text").to_owned(),
                        )
                    })
                    .collect(),
                body,
            };
            Ok(service.answer(recorded))
        })
    }
}
