//! The simulated server's settings: the capability switches and the limits a test can change.

use crate::commits::{
    MAX_COMMIT_CONTENT_TOKENS, MAX_COMMIT_MESSAGE_BYTES, MAX_COMMIT_OPERATIONS,
    MAX_COMMIT_PRECONDITIONS, MAX_COMMIT_REQUEST_BODY_BYTES,
};
use loonfs_types::format::wal::MAX_WAL_INLINE_CONTENT_BYTES;
use loonfs_types::{
    CapabilityDocument, API_GROUP_FILESYSTEM_V0, DEFAULT_MAX_PAGE_LIMIT, DEFAULT_PAGE_LIMIT,
    FEATURE_COMMIT_INLINE_CONTENT, FEATURE_DOWNLOADS_DIRECT_GET, LIMIT_COMMIT_MAX_CONTENT_TOKENS,
    LIMIT_COMMIT_MAX_INLINE_CONTENT_BYTES_PER_OPERATION, LIMIT_COMMIT_MAX_MESSAGE_BYTES,
    LIMIT_COMMIT_MAX_OPERATIONS, LIMIT_COMMIT_MAX_PRECONDITIONS,
    LIMIT_COMMIT_MAX_REQUEST_BODY_BYTES, LIMIT_DOWNLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES,
    LIMIT_PAGINATION_DEFAULT, LIMIT_PAGINATION_MAX, LIMIT_UPLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES,
    PROTOCOL_VERSION,
};
use std::collections::BTreeMap;

/// Whether the server advertises `filesystem.commits.inline_content`, and with what limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InlineContent {
    /// The feature isn't advertised, and a put with inline content returns `not_supported`.
    NotAdvertised,
    /// The feature is advertised, with this `commit.max_inline_content_bytes_per_operation`.
    Advertised {
        /// The largest inline value of one put, before base64 encoding.
        max_bytes_per_operation: u64,
    },
}

/// The capability-document features that a test can switch on and off.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapabilitySwitches {
    /// Whether puts can include inline content, and how much.
    pub inline_content: InlineContent,
    /// Whether the server advertises `filesystem.downloads.direct_get` and grants downloads.
    pub advertise_direct_get: bool,
}

impl Default for CapabilitySwitches {
    /// Returns the reference server's defaults: inline content up to 64 KiB, and download grants.
    fn default() -> Self {
        Self {
            inline_content: InlineContent::Advertised {
                max_bytes_per_operation: 64 * 1024,
            },
            advertise_direct_get: true,
        }
    }
}

/// Construction-time settings of a [`SimulatedServer`](crate::SimulatedServer).
#[derive(Debug, Clone)]
pub struct SimulatedServerConfig {
    /// The virtual clock's starting time, in Unix milliseconds.
    pub start_at_ms: u64,
    /// The features the capability document advertises at the start.
    pub capabilities: CapabilitySwitches,
    /// The most encoded commit bytes that one change-feed page returns, unless its first commit is
    /// larger on its own. The reference server's budget is 4 MiB of event memory, and a test sets
    /// a smaller budget to see pages end before the head.
    pub feed_page_budget_bytes: usize,
    /// How long a download grant's signed URLs work on the virtual clock.
    pub download_url_ttl_ms: u64,
    /// The most bytes that one signed range of a download grant covers. The simulation splits a
    /// revision at these boundaries where the real server splits it at its extents.
    pub max_download_range_bytes: u64,
    /// How long a content token stays valid on the virtual clock.
    pub content_token_ttl_ms: u64,
    /// How long an open upload session accepts content on the virtual clock.
    pub upload_session_lease_ms: u64,
    /// The advertised `download.service_proxied.max_content_bytes`.
    pub max_proxied_read_bytes: u64,
    /// The advertised `upload.service_proxied.max_content_bytes`.
    pub max_proxied_upload_bytes: u64,
    /// The most requests that the trace keeps before the server panics. A long test drains the
    /// trace with [`SimulatedServer::take_trace`](crate::SimulatedServer::take_trace).
    pub trace_capacity: usize,
}

impl Default for SimulatedServerConfig {
    fn default() -> Self {
        Self {
            // 2025-07-16, so timestamps look like the ones in the specs' examples.
            start_at_ms: 1_752_624_000_000,
            capabilities: CapabilitySwitches::default(),
            feed_page_budget_bytes: 4 * 1024 * 1024,
            download_url_ttl_ms: 15 * 60 * 1000,
            max_download_range_bytes: 8 * 1024 * 1024,
            content_token_ttl_ms: 60 * 60 * 1000,
            upload_session_lease_ms: 60 * 60 * 1000,
            max_proxied_read_bytes: 256 * 1024 * 1024,
            max_proxied_upload_bytes: 256 * 1024 * 1024,
            trace_capacity: 100_000,
        }
    }
}

/// The most bytes that one append operation adds (api.md, "`POST /commits`").
pub(crate) const MAX_APPEND_BYTES: u64 = MAX_WAL_INLINE_CONTENT_BYTES as u64;

impl SimulatedServerConfig {
    /// Builds the capability document that `GET /v0/capabilities` returns.
    pub(crate) fn capability_document(&self) -> CapabilityDocument {
        let mut features = BTreeMap::new();
        let mut limits = BTreeMap::from([
            (LIMIT_COMMIT_MAX_OPERATIONS, as_u64(MAX_COMMIT_OPERATIONS)),
            (
                LIMIT_COMMIT_MAX_PRECONDITIONS,
                as_u64(MAX_COMMIT_PRECONDITIONS),
            ),
            (
                LIMIT_COMMIT_MAX_CONTENT_TOKENS,
                as_u64(MAX_COMMIT_CONTENT_TOKENS),
            ),
            (
                LIMIT_COMMIT_MAX_MESSAGE_BYTES,
                as_u64(MAX_COMMIT_MESSAGE_BYTES),
            ),
            (
                LIMIT_COMMIT_MAX_REQUEST_BODY_BYTES,
                as_u64(MAX_COMMIT_REQUEST_BODY_BYTES),
            ),
            (LIMIT_PAGINATION_DEFAULT, u64::from(DEFAULT_PAGE_LIMIT)),
            (LIMIT_PAGINATION_MAX, u64::from(DEFAULT_MAX_PAGE_LIMIT)),
            (
                LIMIT_DOWNLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES,
                self.max_proxied_read_bytes,
            ),
            (
                LIMIT_UPLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES,
                self.max_proxied_upload_bytes,
            ),
        ]);
        if let InlineContent::Advertised {
            max_bytes_per_operation,
        } = self.capabilities.inline_content
        {
            features.insert(FEATURE_COMMIT_INLINE_CONTENT.to_owned(), true);
            limits.insert(
                LIMIT_COMMIT_MAX_INLINE_CONTENT_BYTES_PER_OPERATION,
                max_bytes_per_operation,
            );
        }
        if self.capabilities.advertise_direct_get {
            features.insert(FEATURE_DOWNLOADS_DIRECT_GET.to_owned(), true);
        }
        CapabilityDocument {
            protocol_version: PROTOCOL_VERSION.to_owned(),
            api_groups: vec![API_GROUP_FILESYSTEM_V0.to_owned()],
            features,
            limits: limits
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect(),
        }
    }
}

fn as_u64(value: usize) -> u64 {
    u64::try_from(value).expect("a limit fits in u64")
}
