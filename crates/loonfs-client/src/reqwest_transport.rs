//! Reqwest conversion and TLS configuration for the default transport.

use crate::transport::IO_INACTIVITY_TIMEOUT;
use crate::{Body, ClientConfig, ClientError, Result, TransportError};
use http::{Request, Response};
use http_body_util::BodyExt as _;
use loonfs_types::config::ConfigFieldError;
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::CertificateDer;
use std::fs;
use std::sync::Arc;
use tower::{Service, ServiceExt as _};

pub(crate) fn service(
    config: &ClientConfig,
) -> Result<
    impl Service<Request<Body>, Response = Response<Body>, Error = TransportError, Future: Send> + Clone,
> {
    let client = reqwest::Client::builder()
        .connect_timeout(IO_INACTIVITY_TIMEOUT)
        .tls_backend_preconfigured(tls_config(config)?)
        .http1_only()
        .build()
        .map_err(|err| invalid("http_client", format!("failed to build: {err}")))?;
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

/// Builds the client's TLS configuration: the ring provider, the protocol
/// versions rustls defaults to, the operating system's root certificates
/// plus the bundle at `ca_cert_path`, HTTP/1.1 in ALPN, and no client
/// certificate.
///
/// The client names each of these itself, so its TLS stays the same whichever
/// reqwest features the rest of a build turns on.
///
/// An operating system store often contains old certificates that rustls refuses
/// as trust anchors, so those are skipped and counted, and construction fails
/// only when the store yields certificates and none of them is usable. Every
/// certificate in the `ca_cert_path` bundle must load.
fn tls_config(config: &ClientConfig) -> Result<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    let (added, skipped) = roots.add_parsable_certificates(native.certs);
    if skipped > 0 {
        if added == 0 {
            let reason = match native.errors.first() {
                Some(err) => format!(
                    "none of the {skipped} platform root certificates is a usable trust anchor, \
                     and loading the store also reported: {err}"
                ),
                None => format!(
                    "none of the {skipped} platform root certificates is a usable trust anchor"
                ),
            };
            return Err(invalid("http_client", reason));
        }
        tracing::warn!(
            skipped,
            "skipped platform root certificates that are not usable trust anchors"
        );
    }
    if let Some(path) = &config.ca_cert_path {
        let path = path.trim();
        for certificate in ca_bundle(path)? {
            roots.add(certificate).map_err(|err| {
                invalid(
                    "ca_cert_path",
                    format!(
                        "`{path}` holds a certificate that is not a usable trust anchor: {err}"
                    ),
                )
            })?;
        }
    }
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("the ring provider should support the default protocol versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(tls)
}

/// Reads the PEM certificates at `path`, and refuses a file that contains none.
fn ca_bundle(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let pem = fs::read(path)
        .map_err(|err| invalid("ca_cert_path", format!("failed to read `{path}`: {err}")))?;
    let certificates = CertificateDer::pem_slice_iter(&pem)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|err| {
            invalid(
                "ca_cert_path",
                format!("`{path}` is not a PEM certificate bundle: {err}"),
            )
        })?;
    if certificates.is_empty() {
        return Err(invalid(
            "ca_cert_path",
            format!("`{path}` holds no CERTIFICATE section"),
        ));
    }
    Ok(certificates)
}

fn invalid(field: &'static str, reason: String) -> ClientError {
    ClientError::ConfigField(ConfigFieldError::Invalid { field, reason })
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
        let activity = Arc::new(crate::transport_body::RequestActivity::new(Arc::new(
            crate::transport::StdMonotonicTimer::default(),
        )));
        let request = |body| {
            Request::builder()
                .method("PUT")
                .uri("https://example.invalid/object")
                .body(body)
                .expect("request")
        };
        let buffered = convert_request(request(
            Body::from(Bytes::from_static(b"content")).track_activity(activity.clone()),
        ))
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
        let streamed = convert_request(request(body.track_activity(activity))).expect("conversion");
        assert!(streamed.body().and_then(reqwest::Body::as_bytes).is_none());
        assert!(streamed.try_clone().is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }
}
