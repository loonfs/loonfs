//! Query API operations.

use super::*;
use crate::transport::{QueryBuilder, SendPolicy};

impl Client {
    /// Content search over the namespace's grep index (query API group).
    /// Gate on the `query.grep` capability before calling against unknown
    /// deployments; the namespace must also have a materialized active
    /// grep root or the server answers `not_supported`.
    pub async fn grep(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
        limit: Option<u32>,
    ) -> Result<GrepResponse> {
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{namespace_id}/grep",
            self.base_url
        ));
        query.push("pattern", &request.pattern);
        query.push("case_insensitive", request.case_insensitive);
        if let Some(path_prefix) = &request.path_prefix {
            query.push("path_prefix", path_prefix.as_str());
        }
        query.push("allow_scan", request.allow_scan);
        query.push("allow_stale", request.allow_stale);
        query.pagination(limit, request.cursor.as_deref());
        let url = query.finish();
        self.request_json::<(), _>(self.get(&url), None, SendPolicy::Retry)
            .await
    }
}
