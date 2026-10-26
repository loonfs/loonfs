//! A response before the service delivers it, and the conversion to an HTTP response.

use crate::error::MountSimError;
use crate::trace::ByteRange;
use bytes::Bytes;
use http::header::{HeaderName, HeaderValue, CONTENT_RANGE, CONTENT_TYPE};
use http::{Response, StatusCode};
use loonfs_client::Body;
use loonfs_types::{ChangeSeq, ContentId, ContentRef, ErrorCode, NamespaceId};
use serde::Serialize;

/// A response that the server built for one request.
#[derive(Debug)]
pub(crate) struct Reply {
    pub(crate) status: StatusCode,
    pub(crate) headers: Vec<(HeaderName, HeaderValue)>,
    pub(crate) body: ReplyBody,
    /// The file bytes that a content read returns, for the trace and the content read faults.
    pub(crate) byte_range: Option<ByteRange>,
    /// The content whose bytes a content read returns, for the trace.
    pub(crate) content_id: Option<ContentId>,
    pub(crate) error_code: Option<ErrorCode>,
    /// The commit that the request made, when it made one, for the commit faults.
    pub(crate) fresh_commit: Option<FreshCommit>,
}

/// A response body: complete, or ending with an error after the bytes it has.
#[derive(Debug)]
pub(crate) enum ReplyBody {
    Full(Bytes),
    Interrupted(Bytes),
}

/// A commit that a request made, as opposed to one that a retry replayed.
#[derive(Debug)]
pub(crate) struct FreshCommit {
    pub(crate) namespace_id: NamespaceId,
    pub(crate) committed_seq: ChangeSeq,
}

impl Reply {
    fn new(status: StatusCode, body: Bytes) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: ReplyBody::Full(body),
            byte_range: None,
            content_id: None,
            error_code: None,
            fresh_commit: None,
        }
    }

    pub(crate) fn json<T: Serialize>(value: &T) -> Self {
        Self::json_with_status(StatusCode::OK, value)
    }

    fn json_with_status<T: Serialize>(status: StatusCode, value: &T) -> Self {
        let body = serde_json::to_vec(value).expect("a response shape should encode as JSON");
        Self::new(status, body.into()).with_header(CONTENT_TYPE, "application/json")
    }

    pub(crate) fn error(error: &MountSimError, request_id: &str) -> Self {
        let mut reply = Self::json_with_status(error.status(), &error.to_response(request_id));
        reply.error_code = Some(error.code());
        match (error.code(), error.details().size_bytes) {
            // A 416 response states the file's size in `Content-Range` too (api.md, "Ranged
            // reads").
            (ErrorCode::RangeNotSatisfiable, Some(size_bytes)) => {
                reply.with_header(CONTENT_RANGE, format!("bytes */{size_bytes}"))
            }
            _ => reply,
        }
    }

    /// Builds a response that returns `bytes`, the bytes in `range` of the content that
    /// `content_ref` refers to.
    pub(crate) fn content(
        status: StatusCode,
        bytes: Bytes,
        content_ref: &ContentRef,
        range: ByteRange,
    ) -> Self {
        let mut reply =
            Self::new(status, bytes).with_header(CONTENT_TYPE, "application/octet-stream");
        reply.byte_range = Some(range);
        reply.content_id = Some(content_ref.content_id.clone());
        reply
    }

    /// Builds a plain-text response, as an object store returns for a request it refuses.
    pub(crate) fn plain(status: StatusCode, message: &str) -> Self {
        Self::new(status, Bytes::copy_from_slice(message.as_bytes()))
            .with_header(CONTENT_TYPE, "text/plain")
    }

    pub(crate) fn with_header(mut self, name: HeaderName, value: impl AsRef<str>) -> Self {
        let value = HeaderValue::from_str(value.as_ref())
            .expect("the simulated server's header values should be visible ASCII");
        self.headers.push((name, value));
        self
    }

    pub(crate) fn into_response(self, request_id: &str) -> Response<Body> {
        let body = match self.body {
            ReplyBody::Full(bytes) => Body::from(bytes),
            ReplyBody::Interrupted(prefix) => Body::from_stream(futures::stream::iter([
                Ok(prefix),
                Err(std::io::Error::other(
                    "the simulated server interrupted the response body",
                )),
            ])),
        };
        let mut response = Response::builder().status(self.status);
        for (name, value) in self.headers {
            response = response.header(name, value);
        }
        response
            .header("x-request-id", request_id)
            .body(body)
            .expect("the simulated server's response parts should be valid")
    }
}
