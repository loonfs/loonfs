//! Query API operations.

use super::*;
use crate::transport::{QueryBuilder, SendPolicy};

/// A pager over grep matches.
pub type GrepPager = loonfs_types::Pager<GrepResponse, ClientError>;

impl Client {
    /// Content search over the namespace's grep index (query API group).
    /// Gate on the `query.grep` capability before calling against unknown
    /// deployments; the namespace must also have a materialized active
    /// grep manifest or the server answers `not_supported`.
    ///
    /// The pager supplies each page's cursor, so `request.cursor` is not read.
    pub fn grep(&self, namespace_id: &NamespaceId, request: &GrepRequest) -> GrepPager {
        let client = self.clone();
        let namespace_id = namespace_id.clone();
        let request = request.clone();
        loonfs_types::Pager::new(move |page| {
            let client = client.clone();
            let namespace_id = namespace_id.clone();
            let request = request.clone();
            async move { client.grep_page(&namespace_id, &request, page).await }
        })
    }

    async fn grep_page(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
        page: loonfs_types::PageRequest<String>,
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
        if let Some(inode_id) = request.inode_id {
            query.push("inode_id", loonfs_types::public_inode_id::encode(inode_id));
        }
        query.push("allow_scan", request.allow_scan);
        query.push("allow_stale", request.allow_stale);
        query.pagination(Some(page.limit.get()), page.cursor.as_deref());
        let url = query.build();
        self.request_json::<(), _>(self.get(&url), None, SendPolicy::Retry)
            .await
    }
}
