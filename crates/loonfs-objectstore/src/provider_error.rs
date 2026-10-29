//! The classification of provider client errors, and the redaction of
//! credentials from the provider messages that those errors include.

use crate::ObjectStoreError;
use loonfs_types::REDACTED;
use object_store as provider_store;

/// The provider client that returned an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProviderClient {
    /// The client that retries each request it sends, under
    /// [`crate::retry::provider_retry_config`].
    Retrying,
    /// The client that sends each request once, for conditional puts.
    OneAttempt,
}

/// Classifies a provider client error, using `client` to determine whether a
/// `Generic` error is retryable.
///
/// The provider client reports a failed request as `Error::Generic`, whether
/// the provider responded with a status or no response arrived, and its retry
/// error type is private, so LoonFS can't read the status. LoonFS classifies
/// a `Generic` error by the client that returned it instead:
///
/// - From [`ProviderClient::Retrying`], it's a non-retryable transport
///   failure, because that client already spent its retry budget on the
///   request (or refused to spend it, for a status such as 400).
/// - From [`ProviderClient::OneAttempt`], it's a retryable transport failure,
///   because the outcome is unknown. The caller's receipt check determines
///   whether an earlier attempt succeeded.
pub(crate) fn map_provider_error(
    object_key: &str,
    error: provider_store::Error,
    client: ProviderClient,
) -> ObjectStoreError {
    match error {
        provider_store::Error::NotFound { .. } => ObjectStoreError::NotFound {
            object_key: object_key.to_owned(),
        },
        provider_store::Error::AlreadyExists { .. }
        | provider_store::Error::Precondition { .. }
        | provider_store::Error::NotModified { .. } => ObjectStoreError::PreconditionFailed {
            object_key: object_key.to_owned(),
        },
        provider_store::Error::InvalidPath { source } => ObjectStoreError::InvalidKey {
            object_key: object_key.to_owned(),
            message: source.to_string(),
        },
        provider_store::Error::NotSupported { .. }
        | provider_store::Error::NotImplemented { .. } => {
            ObjectStoreError::Unsupported("provider object store operation")
        }
        provider_store::Error::UnknownConfigurationKey { key, store } => {
            ObjectStoreError::transport(
                object_key,
                format!("unknown {store} configuration key `{key}`"),
            )
        }
        provider_store::Error::Generic { source, .. } => {
            match source.downcast::<ObjectStoreError>() {
                Ok(error) => *error,
                Err(source) => {
                    let message = sanitize_provider_message(&source.to_string());
                    match client {
                        ProviderClient::Retrying => {
                            ObjectStoreError::transport(object_key, message)
                        }
                        ProviderClient::OneAttempt => {
                            ObjectStoreError::retryable_transport(object_key, message)
                        }
                    }
                }
            }
        }
        provider_store::Error::JoinError { source } => {
            ObjectStoreError::transport(object_key, sanitize_provider_message(&source.to_string()))
        }
        provider_store::Error::PermissionDenied { source, .. }
        | provider_store::Error::Unauthenticated { source, .. } => {
            ObjectStoreError::PermissionDenied {
                object_key: object_key.to_owned(),
                message: sanitize_provider_message(&source.to_string()),
            }
        }
        other => {
            ObjectStoreError::transport(object_key, sanitize_provider_message(&other.to_string()))
        }
    }
}

/// Credential query parameters that providers may include in error messages.
const CREDENTIAL_QUERY_PARAMS: &[&str] = &[
    "X-Amz-Signature",
    "X-Amz-Credential",
    "X-Amz-Security-Token",
    "AWSAccessKeyId",
    "Signature",
    "sig",
];

/// XML elements that may contain signing data in authentication errors.
const CREDENTIAL_XML_ELEMENTS: &[&str] = &[
    "StringToSign",
    "StringToSignBytes",
    "CanonicalRequest",
    "SignatureProvided",
    "AWSAccessKeyId",
];

/// Redacts credential and signing data from provider error messages.
pub(crate) fn sanitize_provider_message(message: &str) -> String {
    let mut sanitized = message.to_owned();
    for param in CREDENTIAL_QUERY_PARAMS {
        sanitized = mask_query_param_values(&sanitized, param);
    }
    for element in CREDENTIAL_XML_ELEMENTS {
        sanitized = mask_xml_element_text(&sanitized, element);
    }
    sanitized
}

/// Redacts a query parameter without matching it inside a longer name.
fn mask_query_param_values(message: &str, param: &str) -> String {
    let needle = format!("{param}=");
    let mut out = String::with_capacity(message.len());
    let mut cursor = 0;
    while let Some(found) = message[cursor..].find(&needle) {
        let start = cursor + found;
        let value_start = start + needle.len();
        out.push_str(&message[cursor..value_start]);
        cursor = value_start;
        let at_boundary = start > 0 && matches!(message.as_bytes()[start - 1], b'?' | b'&');
        if at_boundary {
            let value_len = message[cursor..]
                .find(|c: char| {
                    matches!(c, '&' | '"' | '\'' | ')' | '<' | '>' | ':' | ',') || c.is_whitespace()
                })
                .unwrap_or(message.len() - cursor);
            out.push_str(REDACTED);
            cursor += value_len;
        }
    }
    out.push_str(&message[cursor..]);
    out
}

/// Redacts an XML element, including a truncated element without a closing tag.
fn mask_xml_element_text(message: &str, element: &str) -> String {
    let open = format!("<{element}>");
    let close = format!("</{element}>");
    let mut out = String::with_capacity(message.len());
    let mut rest = message;
    while let Some(position) = rest.find(&open) {
        let text_start = position + open.len();
        out.push_str(&rest[..text_start]);
        out.push_str(REDACTED);
        rest = &rest[text_start..];
        match rest.find(&close) {
            Some(text_end) => rest = &rest[text_end..],
            None => rest = "",
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aws_credentials::{aws_credentials_source, ObjectStoreAwsCredentialProvider};
    use crate::test_support::{
        auth_rejection, aws_environment_lock, isolated_aws_environment, transport_glitch,
    };
    use object_store::client::CredentialProvider;
    use provider_store::path::Path;

    /// Checks that an opaque provider client failure is retryable only from
    /// the client that sends each request once, and that a permission
    /// failure is classified the same way from either client.
    #[test]
    fn provider_client_failures_are_classified_at_the_adapter_boundary() {
        let path = Path::from("private-key");
        for (client, opaque) in [
            (ProviderClient::Retrying, crate::ObjectStoreErrorKind::Other),
            (
                ProviderClient::OneAttempt,
                crate::ObjectStoreErrorKind::RetryableTransport,
            ),
        ] {
            assert_eq!(
                map_provider_error("private-key", transport_glitch(), client).kind(),
                opaque
            );
            assert_eq!(
                map_provider_error("private-key", auth_rejection(&path), client).kind(),
                crate::ObjectStoreErrorKind::PermissionDenied
            );
        }
    }

    #[tokio::test]
    async fn credential_provider_failures_preserve_configuration_class_and_public_message() {
        let _lock = aws_environment_lock().await;
        let tempdir =
            tempfile::tempdir().expect("temporary AWS config directory should be created");
        let _environment = isolated_aws_environment(&tempdir, None);
        let source = aws_credentials_source(&crate::AwsS3Credentials::Ambient {}, "us-east-1")
            .expect("ambient credential source should be constructed");
        let provider = ObjectStoreAwsCredentialProvider::new(source);
        let error = provider
            .get_credential()
            .await
            .expect_err("isolated credential chain should be empty");

        let error = map_provider_error("private-key", error, ProviderClient::Retrying);

        assert_eq!(error.kind(), crate::ObjectStoreErrorKind::Configuration);
        assert_eq!(
            error.to_string(),
            "invalid object store configuration: could not resolve `store.credentials` through the standard AWS credential chain"
        );
        assert_eq!(
            error.public_message(),
            "object-store configuration is invalid; verify the provider, bucket, credentials, endpoint, and key prefix fields"
        );
    }

    #[test]
    fn provider_messages_drop_credential_material_and_keep_the_diagnosis() {
        let presigned = sanitize_provider_message(
            "Generic S3 error: error sending request for url \
             (https://bucket.s3.amazonaws.com/k?X-Amz-Algorithm=AWS4-HMAC-SHA256\
             &X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20260726%2Fus-east-1%2Fs3%2Faws4_request\
             &X-Amz-Signature=deadbeefcafe): operation timed out",
        );
        assert!(!presigned.contains("AKIAIOSFODNN7EXAMPLE"), "{presigned}");
        assert!(!presigned.contains("deadbeefcafe"), "{presigned}");
        assert!(
            presigned.contains("X-Amz-Signature=<redacted>"),
            "{presigned}"
        );
        assert!(presigned.contains("operation timed out"), "{presigned}");
        assert!(
            presigned.contains("bucket.s3.amazonaws.com/k"),
            "{presigned}"
        );

        let signature_mismatch = sanitize_provider_message(
            "Client error with status 403 Forbidden: <Error>\
             <Code>SignatureDoesNotMatch</Code>\
             <StringToSign>AWS4-HMAC-SHA256 20260726T000000Z scope digest</StringToSign>\
             <SignatureProvided>cafe0123</SignatureProvided>\
             <AWSAccessKeyId>AKIAIOSFODNN7EXAMPLE</AWSAccessKeyId></Error>",
        );
        assert!(
            !signature_mismatch.contains("AKIAIOSFODNN7EXAMPLE"),
            "{signature_mismatch}"
        );
        assert!(
            !signature_mismatch.contains("cafe0123"),
            "{signature_mismatch}"
        );
        assert!(
            !signature_mismatch.contains("20260726T000000Z"),
            "{signature_mismatch}"
        );
        assert!(
            signature_mismatch.contains("SignatureDoesNotMatch"),
            "{signature_mismatch}"
        );
        assert!(
            signature_mismatch.contains("403 Forbidden"),
            "{signature_mismatch}"
        );

        let azure_sas = sanitize_provider_message(
            "error for url https://account.blob.core.windows.net/c/k?sv=2021-08-06\
             &se=2026-07-26&sig=aGVsbG8: 403",
        );
        assert!(!azure_sas.contains("aGVsbG8"), "{azure_sas}");
        assert!(azure_sas.contains("sig=<redacted>"), "{azure_sas}");

        // A bare name inside a longer parameter is not a boundary match.
        let unrelated = sanitize_provider_message("policy?ResponseSignature=keep&sigil=keep2");
        assert!(unrelated.contains("keep2"), "{unrelated}");
        assert!(!unrelated.contains("Signature=<redacted>"), "{unrelated}");
    }
}
