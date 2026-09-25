//! Reqwest conversion and TLS configuration for the default transport.

use crate::transport::IO_INACTIVITY_TIMEOUT;
use crate::{Body, ClientConfig, ClientError, Result, TransportError};
use http::{Request, Response};
use http_body_util::BodyExt as _;
use std::fs;
use tower::{Service, ServiceExt as _};

pub(crate) fn service(
    config: &ClientConfig,
) -> Result<
    impl Service<Request<Body>, Response = Response<Body>, Error = TransportError, Future: Send> + Clone,
> {
    let mut builder = reqwest::Client::builder().connect_timeout(IO_INACTIVITY_TIMEOUT);
    for certificate in extra_root_certificates(config)? {
        builder = builder.add_root_certificate(certificate);
    }
    let client = builder
        .build()
        .map_err(|err| ClientError::ConfigValidation {
            field: "http_client",
            reason: format!("failed to build: {err}"),
        })?;
    Ok(tower::service_fn(move |request: Request<Body>| {
        let client = client.clone();
        async move {
            let request = convert_request(request)?;
            let response = client.oneshot(request).await.map_err(transport_error)?;
            let response: Response<reqwest::Body> = response.into();
            Ok(response.map(|body| Body::new(body.map_err(transport_error))))
        }
    }))
}

fn convert_request(
    request: Request<Body>,
) -> std::result::Result<reqwest::Request, TransportError> {
    reqwest::Request::try_from(request.map(|body| match body.into_buffered() {
        Ok(bytes) => reqwest::Body::from(bytes),
        Err(body) => reqwest::Body::wrap_stream(body.into_data_stream()),
    }))
    .map_err(transport_error)
}

fn transport_error(error: reqwest::Error) -> TransportError {
    if error.is_connect() {
        TransportError::connect(error)
    } else if error.is_timeout() {
        TransportError::timeout(error)
    } else if error.is_body() {
        TransportError::body(error)
    } else {
        TransportError::new(error)
    }
}

fn extra_root_certificates(config: &ClientConfig) -> Result<Vec<reqwest::Certificate>> {
    let Some(path) = &config.ca_cert_path else {
        return Ok(Vec::new());
    };
    let path = path.trim();
    let pem = fs::read(path).map_err(|err| ClientError::ConfigValidation {
        field: "ca_cert_path",
        reason: format!("failed to read `{path}`: {err}"),
    })?;
    let certificates = reqwest::Certificate::from_pem_bundle(&pem).map_err(|err| {
        ClientError::ConfigValidation {
            field: "ca_cert_path",
            reason: format!("`{path}` is not a PEM certificate bundle: {err}"),
        }
    })?;
    if certificates.is_empty() {
        return Err(ClientError::ConfigValidation {
            field: "ca_cert_path",
            reason: format!("`{path}` holds no CERTIFICATE section"),
        });
    }
    Ok(certificates)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[test]
    fn conversion_preserves_buffered_replay_and_leaves_streams_unread() {
        let request = |body| {
            Request::builder()
                .method("PUT")
                .uri("https://example.invalid/object")
                .body(body)
                .expect("request")
        };
        let buffered = convert_request(request(Body::from(Bytes::from_static(b"content"))))
            .expect("conversion");
        assert_eq!(
            buffered.body().and_then(reqwest::Body::as_bytes),
            Some(b"content".as_slice())
        );
        assert!(buffered.try_clone().is_some());

        let polls = Arc::new(AtomicUsize::new(0));
        let observed = polls.clone();
        let body = Body::from_stream(futures::stream::poll_fn(
            move |_| -> std::task::Poll<Option<std::io::Result<Bytes>>> {
                observed.fetch_add(1, Ordering::SeqCst);
                std::task::Poll::Pending
            },
        ));
        let streamed = convert_request(request(body)).expect("conversion");
        assert!(streamed.body().and_then(reqwest::Body::as_bytes).is_none());
        assert!(streamed.try_clone().is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }
}
