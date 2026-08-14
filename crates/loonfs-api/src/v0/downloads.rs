//! Download-grant shapes for the v0 HTTP API: what a client asks for when
//! it wants a file's bytes straight from object storage, and the short-lived
//! read capability it gets back.
//!
//! This is the read half of the direct transfer plane. The write half lives
//! in [`super::uploads`], and the two share one access envelope
//! ([`ObjectTransferAccess`]) because a client handles both the same way:
//! send this method to this URL with these headers, before this instant.

use super::ObjectTransferAccess;
use crate::{AbsolutePath, ContentRef, NamespaceId, RevisionNo};
use serde::{Deserialize, Serialize};

/// What a client names when it asks to read a file directly.
///
/// A path and, optionally, the revision it wants — the same two things the
/// proxied content read takes, so a caller switching transports changes
/// nothing about what it is asking for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct BeginDownloadRequest {
    /// Absolute path of the file to read.
    pub path: AbsolutePath,
    /// Revision to read, or `None` for the path's current revision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_no: Option<RevisionNo>,
}

impl BeginDownloadRequest {
    /// Asks for the path's current revision.
    pub fn for_path(path: AbsolutePath) -> Self {
        Self {
            path,
            revision_no: None,
        }
    }

    /// Asks for one prior revision of the path.
    pub fn for_revision(path: AbsolutePath, revision_no: RevisionNo) -> Self {
        Self {
            path,
            revision_no: Some(revision_no),
        }
    }
}

/// A short-lived capability to read one file's content object, plus
/// everything the reader needs to check what arrives.
///
/// The raw object key is deliberately not here, exactly as it is not in a
/// `direct_put` grant: a client learns a URL that expires, not an address it
/// can revisit.
///
/// The grant names one immutable content object, so it does not go stale
/// when the path moves on. A commit that replaces the file writes a new
/// object and leaves this one alone; what the capability reads is what the
/// requested revision held when the grant was issued, and the reference
/// says which bytes those are.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BeginDownloadResponse {
    /// Namespace that was read.
    pub namespace_id: NamespaceId,
    /// Absolute path as rendered from stored display names.
    pub absolute_path: AbsolutePath,
    /// Revision the capability reads, resolved from the request.
    pub revision_no: RevisionNo,
    /// Identity, byte length, and checksum evidence for the object the
    /// capability reads. A reader checks the bytes it receives against
    /// `size_bytes` and recomputes `checksum.algorithm` over the complete
    /// payload.
    pub content_ref: ContentRef,
    /// Short-lived read capability the client uses without learning the raw object key.
    pub access: ObjectTransferAccess,
}

#[cfg(test)]
mod tests {
    use super::{BeginDownloadRequest, BeginDownloadResponse};
    use crate::v0::ObjectTransferAccess;
    use crate::{AbsolutePath, ContentId, ContentRef, NamespaceId, RevisionNo};
    use std::collections::BTreeMap;

    fn absolute_path() -> AbsolutePath {
        AbsolutePath::parse("/docs/report.txt").expect("absolute path")
    }

    /// A download request names a path, never an object. Identity belongs
    /// to the server on the way out exactly as it does on the way in.
    #[test]
    fn a_download_request_names_only_a_path_and_a_revision() {
        let request: BeginDownloadRequest =
            serde_json::from_str(r#"{"path":"/docs/report.txt"}"#).expect("decode request");
        assert_eq!(request.path, absolute_path());
        assert_eq!(request.revision_no, None);

        let pinned: BeginDownloadRequest =
            serde_json::from_str(r#"{"path":"/docs/report.txt","revision_no":3}"#)
                .expect("decode pinned request");
        assert_eq!(pinned.revision_no, Some(RevisionNo(3)));

        assert!(
            serde_json::from_str::<BeginDownloadRequest>(
                r#"{"path":"/docs/report.txt","content_id":"con_0123456789abcdef0123456789abcdef"}"#
            )
            .is_err(),
            "a client must not be able to name the content object"
        );
    }

    #[test]
    fn a_download_grant_exposes_only_presigned_access() {
        let response = BeginDownloadResponse {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            absolute_path: absolute_path(),
            revision_no: RevisionNo(7),
            content_ref: ContentRef::blob_v1(ContentId::generate(), b"hello"),
            access: ObjectTransferAccess::PresignedUrl {
                method: "GET".to_owned(),
                url: "https://bucket.example/object?X-Amz-Signature=abc".to_owned(),
                headers: BTreeMap::new(),
                expires_at_ms: 1,
            },
        };

        let json = serde_json::to_string(&response).expect("serialize response");
        assert!(json.contains(r#""kind":"presigned_url""#));
        assert!(!json.contains("object_key"));
    }
}
