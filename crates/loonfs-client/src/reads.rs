//! Namespace lifecycle, path reads, revision history, trash, and change feeds.

use super::*;
use crate::transport::{append_optional_pagination_query, append_query_param};

impl Client {
    /// Creates an empty namespace with the given ID and returns its genesis state.
    pub async fn create_namespace(&self, namespace_id: &NamespaceId) -> Result<Namespace> {
        let url = format!("{}/v0/namespaces", self.base_url);
        // Namespace creation has no durable request identity to reconcile an ambiguous success.
        self.request_json_once::<_, Namespace>(
            self.post(&url),
            Some(&CreateNamespaceRequest {
                namespace_id: namespace_id.clone(),
            }),
        )
        .await
    }

    /// Returns the namespace's current state.
    pub async fn namespace_status(&self, namespace_id: &NamespaceId) -> Result<Namespace> {
        // Validated namespace ids are URL-safe by construction, like the
        // other parsed id segments interpolated into paths here and below.
        let url = format!("{}/v0/namespaces/{namespace_id}", self.base_url);
        self.request_json::<(), Namespace>(self.get(&url), None)
            .await
    }

    /// Deletes a namespace (feature `core.namespaces.delete`): terminal,
    /// and the id is permanently retired. Pass `expected_head_seq` to delete
    /// only if the namespace is still where you last observed it
    /// (`stale_head` on mismatch). Deleting an already-deleted namespace
    /// fails with `namespace_deleted`.
    pub async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
        expected_head_seq: Option<ChangeSeq>,
    ) -> Result<DeleteNamespaceResponse> {
        let mut url = format!("{}/v0/namespaces/{namespace_id}", self.base_url);
        if let Some(expected) = expected_head_seq {
            url.push_str(&format!("?expected_head_seq={}", expected.0));
        }
        // The expected head is a precondition, not an idempotency key for an ambiguous delete.
        self.request_json_once::<(), DeleteNamespaceResponse>(self.delete(&url), None)
            .await
    }

    /// Creates a new namespace from the source namespace's current state and
    /// returns the target's state at the fork point.
    pub async fn fork_namespace(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
    ) -> Result<Namespace> {
        let url = format!(
            "{}/v0/namespaces/{source_namespace_id}/forks",
            self.base_url
        );
        // Namespace forks have no durable request identity to replay after an ambiguous success.
        self.request_json_once::<_, Namespace>(
            self.post(&url),
            Some(&ForkNamespaceRequest {
                new_namespace_id: new_namespace_id.clone(),
            }),
        )
        .await
    }

    /// Lists a directory by aggregating every page into one response.
    ///
    /// Listing cursors tolerate commits landing mid-listing — each page
    /// resumes in name-key order against the head the server has loaded —
    /// so aggregation never restarts. The envelope's `head_seq` reports the
    /// newest head that served a page. Use
    /// [`Self::list_path_entries_page`] for page-level control. `options`
    /// selects the entry projection.
    pub async fn list_path_entries_all(
        &self,
        spec: &NamespacePath,
        options: &ListPathEntriesOptions,
    ) -> Result<ListPathEntriesResponse> {
        let first_page = self
            .list_path_entries_page(spec, None, None, options)
            .await?;
        let mut envelope = ListPathEntriesResponse {
            namespace_id: first_page.namespace_id,
            path: first_page.path,
            head_seq: first_page.head_seq,
            entries: first_page.entries,
            next_cursor: None,
        };
        let mut next_cursor = first_page.next_cursor;
        while let Some(cursor) = next_cursor {
            let page = self
                .list_path_entries_page(spec, None, Some(&cursor), options)
                .await?;
            envelope.head_seq = envelope.head_seq.max(page.head_seq);
            envelope.entries.extend(page.entries);
            next_cursor = page.next_cursor;
        }
        // Pages arrive in canonical name-key order; concatenation preserves
        // it, so aggregation must not re-sort.
        Ok(envelope)
    }

    /// Lists one directory page using the requested projection.
    pub async fn list_path_entries_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
        options: &ListPathEntriesOptions,
    ) -> Result<ListPathEntriesResponse> {
        let mut url = format!(
            "{}/v0/namespaces/{}/filesystem/list?path={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str())
        );
        let mut has_query = true;
        append_optional_pagination_query(&mut url, &mut has_query, limit, cursor);
        append_query_param(
            &mut url,
            &mut has_query,
            "include_attributes",
            &options.include_attributes.to_string(),
        );
        self.request_json::<(), _>(self.get(&url), None).await
    }

    /// Returns path metadata using the requested projection.
    pub async fn stat_path(
        &self,
        spec: &NamespacePath,
        options: &StatPathOptions,
    ) -> Result<AuthoritativePathEntry> {
        let mut url = format!(
            "{}/v0/namespaces/{}/filesystem/stat?path={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str())
        );
        let mut has_query = true;
        append_query_param(
            &mut url,
            &mut has_query,
            "include_attributes",
            &options.include_attributes.to_string(),
        );
        self.request_json::<(), _>(self.get(&url), None).await
    }

    /// Returns the current entry for a visible inode.
    pub async fn stat_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: &StatPathOptions,
    ) -> Result<AuthoritativePathEntry> {
        let inode_id = loonfs_api::public_inode_id::encode(inode_id);
        let mut url = format!(
            "{}/v0/namespaces/{namespace_id}/inodes/{inode_id}",
            self.base_url
        );
        let mut has_query = false;
        append_query_param(
            &mut url,
            &mut has_query,
            "include_attributes",
            &options.include_attributes.to_string(),
        );
        self.request_json::<(), _>(self.get(&url), None).await
    }

    /// Reads the file's current contents into memory.
    pub async fn get_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>> {
        let url = format!(
            "{}/v0/namespaces/{}/filesystem/content?path={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str())
        );
        self.request_bytes(&url).await
    }

    /// Reads the requested file revision into memory.
    pub async fn get_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        let url = format!(
            "{}/v0/namespaces/{}/filesystem/content?path={}&revision_no={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str()),
            revision_no.0
        );
        self.request_bytes(&url).await
    }

    /// Reads and verifies one retained file revision by inode identity.
    pub async fn get_file_revision_bytes_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        let inode_id = loonfs_api::public_inode_id::encode(inode_id);
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
            self.base_url
        );
        self.request_bytes(&url).await
    }

    /// Returns one page of revisions for a file.
    pub async fn list_file_revisions_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse> {
        let mut url = format!(
            "{}/v0/namespaces/{}/filesystem/revisions?path={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str())
        );
        let mut has_query = true;
        append_optional_pagination_query(&mut url, &mut has_query, limit, cursor);
        self.request_json::<(), ListFileRevisionsResponse>(self.get(&url), None)
            .await
    }

    /// Returns one page of retained revisions for a file inode.
    pub async fn list_file_revisions_by_inode_page(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse> {
        let inode_id = loonfs_api::public_inode_id::encode(inode_id);
        let mut url = format!(
            "{}/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions",
            self.base_url
        );
        let mut has_query = false;
        append_optional_pagination_query(&mut url, &mut has_query, limit, cursor);
        self.request_json::<(), ListFileRevisionsResponse>(self.get(&url), None)
            .await
    }

    /// Returns one page of recoverable deletions in a namespace.
    pub async fn list_trash_page(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListTrashResponse> {
        let mut url = format!(
            "{}/v0/namespaces/{}/filesystem/trash",
            self.base_url,
            namespace_id.as_str()
        );
        let mut has_query = false;
        append_optional_pagination_query(&mut url, &mut has_query, limit, cursor);
        self.request_json::<(), ListTrashResponse>(self.get(&url), None)
            .await
    }

    /// Returns committed changes after the given sequence number.
    pub async fn list_changes(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        limit: Option<u32>,
    ) -> Result<ChangesResponse> {
        let mut url = format!(
            "{}/v0/namespaces/{namespace_id}/changes?after_seq={}",
            self.base_url, after_seq.0
        );
        if let Some(limit) = limit {
            url.push_str(&format!("&limit={limit}"));
        }
        self.request_json::<(), ChangesResponse>(self.get(&url), None)
            .await
    }
}
