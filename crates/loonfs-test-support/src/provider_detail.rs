//! Provider error detail that LoonFS must never repeat to a caller.

/// A provider error body that contains an account ID, a role ARN, a bucket,
/// an object key, request IDs, and an access key ID.
pub const POISON_PROVIDER_DETAIL: &str = "<Error>AccessDenied</Error> \
    arn:aws:iam::123456789012:role/private-role private-bucket \
    namespaces/customer-a/hint.json x-amz-request-id=provider-request \
    x-amz-id-2=provider-host-id AKIAEXAMPLE";

/// Fails when `output` contains any marker of [`POISON_PROVIDER_DETAIL`].
/// `context` describes the output in the failure message, such as
/// "API body".
pub fn assert_provider_markers_absent(context: &str, output: &str) {
    for marker in [
        "<Error>AccessDenied</Error>",
        "arn:aws:iam::123456789012:role/private-role",
        "private-bucket",
        "namespaces/customer-a/hint.json",
        "x-amz-request-id=provider-request",
        "x-amz-id-2=provider-host-id",
        "AKIAEXAMPLE",
    ] {
        assert!(
            !output.contains(marker),
            "{context} leaked {marker}: {output}"
        );
    }
}
