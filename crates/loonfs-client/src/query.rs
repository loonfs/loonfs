//! Query API operations.

use super::*;
use crate::operations;
use crate::transport::QueryBuilder;

/// A pager over grep matches.
pub type GrepPager = loonfs_types::Pager<GrepResponse, ClientError>;

impl Client {
    /// Searches content through the namespace's grep index (query API group).
    ///
    /// Gate on the `query.grep` capability before calling against unknown
    /// deployments; the namespace must also have a materialized active grep
    /// manifest or the server answers `not_supported`.
    ///
    /// The search starts at `request.cursor` when it is set: a page request
    /// without a cursor reads from there. From then on the pager owns the
    /// cursor and passes each page's `next_cursor` to the next request.
    pub fn grep(&self, namespace_id: &NamespaceId, request: &GrepRequest) -> GrepPager {
        let client = self.clone();
        let namespace_id = namespace_id.clone();
        let request = request.clone();
        loonfs_types::Pager::new(move |mut page: loonfs_types::PageRequest<String>| {
            let client = client.clone();
            let namespace_id = namespace_id.clone();
            let request = request.clone();
            if page.cursor.is_none() {
                page.cursor.clone_from(&request.cursor);
            }
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
        self.request_json::<(), _>(self.get(&url), None, operations::GREP)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scripted_transport::{self, Outcome};

    fn grep_page(next_cursor: Option<&str>) -> Outcome {
        Outcome::Success(
            serde_json::json!({
                "namespace_id": "demo",
                "head_seq": 7,
                "built_through_seq": 7,
                "tail_scanned": true,
                "matches": [],
                "next_cursor": next_cursor,
            })
            .to_string()
            .into_bytes(),
        )
    }

    #[tokio::test]
    async fn a_grep_starts_at_the_request_cursor_and_then_follows_each_page() {
        let transport = scripted_transport::script([grep_page(Some("second")), grep_page(None)]);
        let client = Client::with_transport(
            ClientConfig {
                server_url: "http://example.invalid".to_owned(),
                auth_token: None,
                request_timeout_ms: None,
                disable_transient_retry: false,
                ca_cert_path: None,
            },
            transport.clone(),
        )
        .expect("valid client config");
        let request = GrepRequest {
            pattern: "needle".to_owned(),
            case_insensitive: false,
            path_prefix: None,
            inode_id: None,
            cursor: Some("first".to_owned()),
            allow_stale: false,
            allow_scan: false,
        };
        let mut pager = client.grep(&NamespaceId::parse("demo").expect("namespace id"), &request);

        for _ in 0..2 {
            pager
                .next()
                .await
                .expect("another page")
                .expect("a decoded page");
        }
        assert!(pager.next().await.is_none());

        let urls = transport
            .sent()
            .into_iter()
            .map(|sent| sent.url)
            .collect::<Vec<_>>();
        assert_eq!(urls.len(), 2);
        assert!(urls[0].contains("cursor=first"), "{urls:?}");
        assert!(urls[1].contains("cursor=second"), "{urls:?}");
    }
}
