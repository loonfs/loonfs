//! Path reads, revision history, trash, and change feeds.

use crate::operations::{self, Operation};
use crate::read_at_fields::ReadAtFields;
use crate::transport::QueryBuilder;
use crate::{Client, ClientError, NamespacePath, PayloadStream, Result};
use loonfs_types::options::{
    ListChangesOptions, ListFileRevisionsOptions, ListOptions, ListTrashOptions, ReadAt,
    ReadConsistency, ReadFileOptions, StatOptions,
};
use loonfs_types::ListChangesResponse;
use loonfs_types::{
    ChangeSeq, EffectiveLimit, InodeId, ListFileRevisionsResponse, ListInodeChildrenResponse,
    ListPathEntriesResponse, ListTrashResponse, NamespaceId, PageRequest, Pager, PathEntry,
    RevisionNo,
};

/// Adds the query parameters that say what a read reads at.
fn push_read_at(query: &mut QueryBuilder, at: &ReadAt) {
    let fields = ReadAtFields::from(at);
    if let Some(snapshot_id) = &fields.snapshot_id {
        query.push("snapshot_id", snapshot_id.as_str());
    }
    if let Some(consistency) = fields.consistency {
        query.push(
            "consistency",
            match consistency {
                ReadConsistency::Eventual => "eventual",
                ReadConsistency::Strong => "strong",
            },
        );
    }
    if let Some(max_staleness_ms) = fields.max_staleness_ms {
        query.push("max_staleness_ms", max_staleness_ms);
    }
}

/// A pager over directory entries.
pub type PathEntriesPager = Pager<ListPathEntriesResponse, ClientError>;
/// A pager over directory children addressed by inode.
pub type InodeChildrenPager = Pager<ListInodeChildrenResponse, ClientError>;
/// A pager over retained file revisions.
pub type FileRevisionsPager = Pager<ListFileRevisionsResponse, ClientError>;
/// A pager over recoverable deletions.
pub type TrashPager = Pager<ListTrashResponse, ClientError>;
/// A pager over committed changes.
pub type ChangesPager = Pager<ListChangesResponse, ClientError>;

impl Client {
    /// Lists a directory.
    pub fn list(&self, spec: &NamespacePath) -> PathEntriesPager {
        self.list_with_options(spec, &ListOptions::default())
    }

    /// Lists a directory using the requested projection.
    pub fn list_with_options(
        &self,
        spec: &NamespacePath,
        options: &ListOptions,
    ) -> PathEntriesPager {
        Pager::with_state(
            (self.clone(), spec.clone(), options.clone()),
            |(client, spec, options), request| async move {
                client.path_entries_page(&spec, request, &options).await
            },
        )
    }

    async fn path_entries_page(
        &self,
        spec: &NamespacePath,
        request: PageRequest<String>,
        options: &ListOptions,
    ) -> Result<ListPathEntriesResponse> {
        let mut query =
            QueryBuilder::new(self.namespace_url(spec.namespace(), "/filesystem/entries"));
        query.push("path", spec.absolute_path().as_str());
        query.pagination(Some(request.limit.get()), request.cursor.as_deref());
        query.push("include_attributes", options.include_attributes);
        query.push("recursive", options.recursive);
        push_read_at(&mut query, &options.at);
        self.send_json(self.request(operations::LIST_PATH_ENTRIES, &query.build()))
            .await
    }

    /// Returns path metadata.
    pub async fn stat(&self, spec: &NamespacePath) -> Result<PathEntry> {
        self.stat_with_options(spec, &StatOptions::default()).await
    }

    /// Returns path metadata using the requested projection.
    pub async fn stat_with_options(
        &self,
        spec: &NamespacePath,
        options: &StatOptions,
    ) -> Result<PathEntry> {
        let mut query =
            QueryBuilder::new(self.namespace_url(spec.namespace(), "/filesystem/entry"));
        query.push("path", spec.absolute_path().as_str());
        query.push("include_attributes", options.include_attributes);
        push_read_at(&mut query, &options.at);
        self.send_json(self.request(operations::GET_PATH_ENTRY, &query.build()))
            .await
    }

    /// Returns the current entry for a visible inode.
    pub async fn stat_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<PathEntry> {
        self.stat_by_inode_with_options(namespace_id, inode_id, &StatOptions::default())
            .await
    }

    /// Returns the current entry for a visible inode using the requested
    /// projection.
    pub async fn stat_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: &StatOptions,
    ) -> Result<PathEntry> {
        let inode_id = inode_id.to_string();
        let mut query =
            QueryBuilder::new(self.namespace_url(namespace_id, &format!("/inodes/{inode_id}")));
        query.push("include_attributes", options.include_attributes);
        push_read_at(&mut query, &options.at);
        self.send_json(self.request(operations::GET_INODE, &query.build()))
            .await
    }

    /// Lists a directory's children by inode.
    pub fn list_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> InodeChildrenPager {
        self.list_by_inode_with_options(namespace_id, inode_id, &ListOptions::default())
    }

    /// Lists a directory's children by inode, using the requested projection.
    pub fn list_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: &ListOptions,
    ) -> InodeChildrenPager {
        Pager::with_state(
            (
                self.clone(),
                namespace_id.clone(),
                inode_id,
                options.clone(),
            ),
            |(client, namespace_id, inode_id, options), request| async move {
                client
                    .inode_children_page(&namespace_id, inode_id, request, &options)
                    .await
            },
        )
    }

    async fn inode_children_page(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        request: PageRequest<String>,
        options: &ListOptions,
    ) -> Result<ListInodeChildrenResponse> {
        let inode_id = inode_id.to_string();
        let mut query = QueryBuilder::new(
            self.namespace_url(namespace_id, &format!("/inodes/{inode_id}/children")),
        );
        query.pagination(Some(request.limit.get()), request.cursor.as_deref());
        query.push("include_attributes", options.include_attributes);
        query.push("recursive", options.recursive);
        push_read_at(&mut query, &options.at);
        self.send_json(self.request(operations::LIST_INODE_CHILDREN, &query.build()))
            .await
    }

    /// Lists a file's revisions by path.
    pub fn list_file_revisions(&self, spec: &NamespacePath) -> FileRevisionsPager {
        self.list_file_revisions_with_options(spec, &ListFileRevisionsOptions::default())
    }

    /// Lists a file's revisions by path, as `options` asks.
    pub fn list_file_revisions_with_options(
        &self,
        spec: &NamespacePath,
        options: &ListFileRevisionsOptions,
    ) -> FileRevisionsPager {
        Pager::with_state(
            (self.clone(), spec.clone(), *options),
            |(client, spec, options), request| async move {
                client.file_revisions_page(&spec, request, &options).await
            },
        )
    }

    async fn file_revisions_page(
        &self,
        spec: &NamespacePath,
        request: PageRequest<String>,
        options: &ListFileRevisionsOptions,
    ) -> Result<ListFileRevisionsResponse> {
        let mut query =
            QueryBuilder::new(self.namespace_url(spec.namespace(), "/filesystem/revisions"));
        query.push("path", spec.absolute_path().as_str());
        query.pagination(Some(request.limit.get()), request.cursor.as_deref());
        push_read_at(&mut query, &ReadAt::Live(options.consistency));
        self.send_json(self.request(operations::LIST_FILE_REVISIONS, &query.build()))
            .await
    }

    /// Lists the retained revisions of a file inode.
    pub fn list_file_revisions_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> FileRevisionsPager {
        self.list_file_revisions_by_inode_with_options(
            namespace_id,
            inode_id,
            &ListFileRevisionsOptions::default(),
        )
    }

    /// Lists the retained revisions of a file inode, as `options` asks.
    pub fn list_file_revisions_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: &ListFileRevisionsOptions,
    ) -> FileRevisionsPager {
        Pager::with_state(
            (self.clone(), namespace_id.clone(), inode_id, *options),
            |(client, namespace_id, inode_id, options), request| async move {
                client
                    .file_revisions_by_inode_page(&namespace_id, inode_id, request, &options)
                    .await
            },
        )
    }

    async fn file_revisions_by_inode_page(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        request: PageRequest<String>,
        options: &ListFileRevisionsOptions,
    ) -> Result<ListFileRevisionsResponse> {
        let inode_id = inode_id.to_string();
        let mut query = QueryBuilder::new(
            self.namespace_url(namespace_id, &format!("/inodes/{inode_id}/revisions")),
        );
        query.pagination(Some(request.limit.get()), request.cursor.as_deref());
        push_read_at(&mut query, &ReadAt::Live(options.consistency));
        self.send_json(self.request(operations::LIST_FILE_REVISIONS_BY_INODE, &query.build()))
            .await
    }

    /// Lists the namespace's recoverable deletions.
    pub fn list_trash(&self, namespace_id: &NamespaceId) -> TrashPager {
        self.list_trash_with_options(namespace_id, &ListTrashOptions::default())
    }

    /// Lists the namespace's recoverable deletions, as `options` asks.
    pub fn list_trash_with_options(
        &self,
        namespace_id: &NamespaceId,
        options: &ListTrashOptions,
    ) -> TrashPager {
        Pager::with_state(
            (self.clone(), namespace_id.clone(), *options),
            |(client, namespace_id, options), request| async move {
                client.trash_page(&namespace_id, request, &options).await
            },
        )
    }

    async fn trash_page(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<String>,
        options: &ListTrashOptions,
    ) -> Result<ListTrashResponse> {
        let mut query = QueryBuilder::new(self.namespace_url(namespace_id, "/filesystem/trash"));
        query.pagination(Some(request.limit.get()), request.cursor.as_deref());
        push_read_at(&mut query, &ReadAt::Live(options.consistency));
        self.send_json(self.request(operations::LIST_TRASH, &query.build()))
            .await
    }

    /// Lists committed changes after `after_seq`.
    pub fn list_changes(&self, namespace_id: &NamespaceId, after_seq: ChangeSeq) -> ChangesPager {
        self.list_changes_with_options(namespace_id, after_seq, &ListChangesOptions::default())
    }

    /// Lists committed changes after `after_seq`, bounded by the requested
    /// snapshot.
    pub fn list_changes_with_options(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        options: &ListChangesOptions,
    ) -> ChangesPager {
        Pager::with_state(
            (
                self.clone(),
                namespace_id.clone(),
                after_seq,
                options.clone(),
            ),
            |(client, namespace_id, after_seq, options), request: PageRequest<ChangeSeq>| async move {
                client
                    .changes_page(
                        &namespace_id,
                        request.cursor.unwrap_or(after_seq),
                        request.limit,
                        &options,
                    )
                    .await
            },
        )
    }

    async fn changes_page(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        limit: EffectiveLimit,
        options: &ListChangesOptions,
    ) -> Result<ListChangesResponse> {
        let mut query = QueryBuilder::new(self.namespace_url(namespace_id, "/changes"));
        query.push("after_seq", after_seq.0);
        query.push("limit", limit.get());
        push_read_at(&mut query, &options.at);
        self.send_json(self.request(operations::LIST_CHANGES, &query.build()))
            .await
    }

    /// Returns a file's current bytes.
    pub async fn read_file(&self, spec: &NamespacePath) -> Result<Vec<u8>> {
        self.read_file_with_options(spec, &ReadFileOptions::default())
            .await
    }

    /// Returns a file's bytes: the current revision, or the revision a
    /// snapshot captured when the options name one.
    pub async fn read_file_with_options(
        &self,
        spec: &NamespacePath,
        options: &ReadFileOptions,
    ) -> Result<Vec<u8>> {
        let url = self.file_content_url(spec, None, options);
        Ok(self
            .call(&self.request(operations::GET_FILE_CONTENT, &url))
            .await?
            .bytes)
    }

    /// Returns the bytes of one retained revision of a file.
    pub async fn read_file_revision(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        let url = self.file_content_url(spec, Some(revision_no), &ReadFileOptions::default());
        Ok(self
            .call(&self.request(operations::GET_FILE_CONTENT, &url))
            .await?
            .bytes)
    }

    /// Streams a file's current content through the server.
    ///
    /// See [`Self::read_file_stream_with_options`].
    pub async fn read_file_stream(&self, spec: &NamespacePath) -> Result<PayloadStream> {
        self.read_file_stream_with_options(spec, &ReadFileOptions::default())
            .await
    }

    /// Streams content through the server.
    ///
    /// Successful completion means the server verified the whole object; a late
    /// verification failure aborts the body. Bytes already consumed remain
    /// provisional until the stream ends cleanly.
    pub async fn read_file_stream_with_options(
        &self,
        spec: &NamespacePath,
        options: &ReadFileOptions,
    ) -> Result<PayloadStream> {
        self.read_content_stream(
            operations::GET_FILE_CONTENT,
            &self.file_content_url(spec, None, options),
        )
        .await
    }

    /// Streams one retained revision of a file through the server.
    ///
    /// See [`Self::read_file_revision_stream_with_options`].
    pub async fn read_file_revision_stream(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<PayloadStream> {
        self.read_file_revision_stream_with_options(spec, revision_no, &ReadFileOptions::default())
            .await
    }

    /// Streams one retained revision of a file through the server, at the
    /// consistency that `options` sets.
    ///
    /// See [`Self::read_file_stream_with_options`] for when the bytes are
    /// verified. A snapshot captures one revision of each file, so options
    /// that read a snapshot return `invalid_request`.
    pub async fn read_file_revision_stream_with_options(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
        options: &ReadFileOptions,
    ) -> Result<PayloadStream> {
        self.read_content_stream(
            operations::GET_FILE_CONTENT,
            &self.file_content_url(spec, Some(revision_no), options),
        )
        .await
    }

    pub(crate) fn file_content_url(
        &self,
        spec: &NamespacePath,
        revision_no: Option<RevisionNo>,
        options: &ReadFileOptions,
    ) -> String {
        let mut query =
            QueryBuilder::new(self.namespace_url(spec.namespace(), "/filesystem/content"));
        query.push("path", spec.absolute_path().as_str());
        if let Some(revision_no) = revision_no {
            query.push("revision_no", revision_no.0);
        }
        push_read_at(&mut query, &options.at);
        query.build()
    }

    /// Returns the current bytes of a visible file inode, wherever it is bound.
    pub async fn read_file_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<Vec<u8>> {
        self.read_file_by_inode_with_options(namespace_id, inode_id, &ReadFileOptions::default())
            .await
    }

    /// Returns the bytes of a visible file inode, wherever it is bound: the
    /// current revision, or the revision a snapshot captured when the options
    /// name one.
    pub async fn read_file_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: &ReadFileOptions,
    ) -> Result<Vec<u8>> {
        let url = self.inode_content_url(namespace_id, inode_id, options);
        Ok(self
            .call(&self.request(operations::GET_FILE_CONTENT_BY_INODE, &url))
            .await?
            .bytes)
    }

    /// Streams the current content of a visible file inode through the server,
    /// wherever it is bound.
    ///
    /// See [`Self::read_file_stream_with_options`] for when the bytes are
    /// verified.
    pub async fn read_file_stream_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<PayloadStream> {
        self.read_file_stream_by_inode_with_options(
            namespace_id,
            inode_id,
            &ReadFileOptions::default(),
        )
        .await
    }

    /// Streams the content of a visible file inode through the server, wherever
    /// it is bound: the current revision, or the revision a snapshot captured
    /// when the options name one.
    ///
    /// See [`Self::read_file_stream_with_options`] for when the bytes are
    /// verified.
    pub async fn read_file_stream_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: &ReadFileOptions,
    ) -> Result<PayloadStream> {
        self.read_content_stream(
            operations::GET_FILE_CONTENT_BY_INODE,
            &self.inode_content_url(namespace_id, inode_id, options),
        )
        .await
    }

    /// Reads and verifies one retained file revision by inode identity.
    pub async fn read_file_revision_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        let url = self.inode_revision_content_url(
            namespace_id,
            inode_id,
            revision_no,
            &ReadFileOptions::default(),
        );
        Ok(self
            .call(&self.request(operations::GET_FILE_REVISION_CONTENT_BY_INODE, &url))
            .await?
            .bytes)
    }

    /// Streams one retained file revision by inode identity through the server.
    ///
    /// See [`Self::read_file_revision_stream_by_inode_with_options`].
    pub async fn read_file_revision_stream_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<PayloadStream> {
        self.read_file_revision_stream_by_inode_with_options(
            namespace_id,
            inode_id,
            revision_no,
            &ReadFileOptions::default(),
        )
        .await
    }

    /// Streams one retained file revision by inode identity through the
    /// server, at the consistency that `options` sets.
    ///
    /// See [`Self::read_file_stream_with_options`] for when the bytes are
    /// verified. A snapshot captures one revision of each file, so options
    /// that read a snapshot return `invalid_request`.
    pub async fn read_file_revision_stream_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
        options: &ReadFileOptions,
    ) -> Result<PayloadStream> {
        self.read_content_stream(
            operations::GET_FILE_REVISION_CONTENT_BY_INODE,
            &self.inode_revision_content_url(namespace_id, inode_id, revision_no, options),
        )
        .await
    }

    async fn read_content_stream(&self, operation: Operation, url: &str) -> Result<PayloadStream> {
        self.call_for_response_stream(&self.request(operation, url))
            .await
    }

    pub(crate) fn inode_content_url(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: &ReadFileOptions,
    ) -> String {
        let inode_id = inode_id.to_string();
        let mut query = QueryBuilder::new(
            self.namespace_url(namespace_id, &format!("/inodes/{inode_id}/content")),
        );
        push_read_at(&mut query, &options.at);
        query.build()
    }

    fn inode_revision_content_url(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
        options: &ReadFileOptions,
    ) -> String {
        let inode_id = inode_id.to_string();
        let mut query = QueryBuilder::new(self.namespace_url(
            namespace_id,
            &format!("/inodes/{inode_id}/revisions/{revision_no}/content"),
        ));
        push_read_at(&mut query, &options.at);
        query.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scripted_transport::{self, Outcome};
    use crate::ClientConfig;
    use loonfs_types::options::Consistency;
    use loonfs_types::FilesystemChange;
    use loonfs_types::PinId;
    use loonfs_types::{ContentId, ContentRef, EntryInodeKind, PathEntryKind};

    fn client_for(transport: &scripted_transport::ScriptedTransport) -> Client {
        Client::with_transport(
            ClientConfig {
                server_url: "http://example.invalid".to_owned(),
                auth_token: None,
                request_timeout_ms: None,
                disable_transient_retry: false,
                ca_cert_path: None,
            },
            transport.clone(),
        )
        .expect("valid client config")
    }

    #[tokio::test]
    async fn an_unknown_inode_kind_decodes_as_unknown_and_a_known_kind_is_unchanged() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let listing = serde_json::json!({
            "namespace_id": "demo",
            "path": "/docs",
            "head_seq": 418,
            "entries": [
                {
                    "namespace_id": "demo",
                    "path": "/docs/report.txt",
                    "inode_id": "ino_0000000000000000000000001a",
                    "created_by": "usr_8f3c",
                    "created_at_ms": 1_752_623_000_000_u64,
                    "inode_kind": "file",
                    "revision_no": 7,
                    "size_bytes": 5,
                    "content_ref": ContentRef::blob_v1(
                        namespace_id.clone(),
                        ContentId::generate(),
                        b"hello", loonfs_types::ChecksumAlgorithm::Crc64nvme),
                    "revision_committed_by": "render-worker",
                    "revision_committed_at_ms": 1_752_624_000_000_u64,
                    "head_seq": 418,
                    "parent_inode_id": "ino_00000000000000000000000007",
                    "display_name": "report.txt",
                    "binding_version": "abc"
                },
                {
                    "namespace_id": "demo",
                    "path": "/docs/latest",
                    "inode_id": "ino_0000000000000000000000001b",
                    "created_by": "usr_8f3c",
                    "created_at_ms": 1_752_623_000_000_u64,
                    "inode_kind": "link",
                    "target_inode_id": "ino_0000000000000000000000001a",
                    "head_seq": 418,
                    "parent_inode_id": "ino_00000000000000000000000007",
                    "display_name": "latest",
                    "binding_version": "def"
                }
            ]
        });
        let trash = serde_json::json!({
            "namespace_id": "demo",
            "head_seq": 418,
            "entries": [{
                "inode_id": "ino_0000000000000000000000001c",
                "inode_kind": "link",
                "deletion_seq": 417,
                "deleted_at_ms": 1_752_625_000_000_u64,
                "deleted_by": "usr_8f3c",
                "deleted_binding": {
                    "parent_inode_id": "ino_00000000000000000000000007",
                    "name_key": "old",
                    "display_name": "old"
                }
            }]
        });
        let transport = scripted_transport::script([
            Outcome::Success(listing.to_string().into_bytes()),
            Outcome::Success(trash.to_string().into_bytes()),
        ]);
        let client = client_for(&transport);

        let page = client
            .list(&NamespacePath::parse("demo", "/docs").expect("namespace path"))
            .next()
            .await
            .expect("one page")
            .expect("decoded listing");
        assert_eq!(
            serde_json::to_value(&page.entries[0]).expect("serialize file entry"),
            listing["entries"][0]
        );
        let link = &page.entries[1];
        assert_eq!(link.kind, PathEntryKind::Unknown);
        assert_eq!(link.inode_kind(), EntryInodeKind::Unknown);
        assert_eq!(link.inode_id, InodeId(43));
        assert_eq!(link.path.as_str(), "/docs/latest");
        assert_eq!(
            link.display_name.as_ref().map(|name| name.as_str()),
            Some("latest")
        );
        assert_eq!(
            link.binding_version
                .as_ref()
                .map(|version| version.as_str()),
            Some("def")
        );

        let trash = client
            .list_trash(&namespace_id)
            .next()
            .await
            .expect("one page")
            .expect("decoded trash");
        assert_eq!(trash.entries[0].inode_kind, EntryInodeKind::Unknown);
    }

    /// Checks that a live read that sets no consistency sends neither
    /// consistency parameter, so the server's default applies, that a live
    /// read that sets one sends `consistency`, plus `max_staleness_ms` when it
    /// sets a maximum staleness, and that a snapshot read sends only
    /// `snapshot_id`.
    #[tokio::test]
    async fn reads_send_only_the_parameters_of_what_they_read_at() {
        let spec = NamespacePath::parse("demo", "/file.txt").expect("namespace path");
        let snapshot_id =
            PinId::parse("pin_00000000000000000001-0000000000000002").expect("snapshot ID");
        let transport = scripted_transport::script([
            Outcome::Success(b"file".to_vec()),
            Outcome::Success(b"file".to_vec()),
            Outcome::Success(b"file".to_vec()),
            Outcome::Success(b"file".to_vec()),
            Outcome::Success(b"file".to_vec()),
        ]);
        let client = client_for(&transport);
        for at in [
            ReadAt::Live(None),
            ReadAt::Live(Some(Consistency::Strong)),
            ReadAt::Live(Some(Consistency::Eventual {
                max_staleness_ms: None,
            })),
            ReadAt::Live(Some(Consistency::Eventual {
                max_staleness_ms: Some(2_500),
            })),
            ReadAt::Snapshot(snapshot_id.clone()),
        ] {
            client
                .read_file_with_options(&spec, &ReadFileOptions { at })
                .await
                .expect("read file");
        }
        let urls: Vec<_> = transport
            .sent()
            .into_iter()
            .map(|request| request.url)
            .collect();
        let content =
            "http://example.invalid/v0/namespaces/demo/filesystem/content?path=%2Ffile.txt";
        assert_eq!(
            urls,
            [
                content.to_owned(),
                format!("{content}&consistency=strong"),
                format!("{content}&consistency=eventual"),
                format!("{content}&consistency=eventual&max_staleness_ms=2500"),
                format!("{content}&snapshot_id={snapshot_id}"),
            ]
        );
    }

    /// Checks that the trash and revision listings send the consistency
    /// parameters only when they set a consistency, so the server's default
    /// applies to a listing that sets none.
    #[tokio::test]
    async fn listings_send_the_consistency_parameters_only_when_they_set_them() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let spec = NamespacePath::parse("demo", "/file.txt").expect("namespace path");
        let trash = serde_json::json!({"namespace_id": "demo", "head_seq": 1, "entries": []});
        let revisions = serde_json::json!({
            "namespace_id": "demo",
            "inode_id": "ino_00000000000000000000000002",
            "head_seq": 1,
            "revisions": []
        });
        let consistencies = [
            None,
            Some(Consistency::Strong),
            Some(Consistency::Eventual {
                max_staleness_ms: Some(2_500),
            }),
        ];
        let transport = scripted_transport::script(consistencies.iter().flat_map(|_| {
            [
                Outcome::Success(trash.to_string().into_bytes()),
                Outcome::Success(revisions.to_string().into_bytes()),
                Outcome::Success(revisions.to_string().into_bytes()),
            ]
        }));
        let client = client_for(&transport);
        for consistency in consistencies {
            client
                .list_trash_with_options(&namespace_id, &ListTrashOptions { consistency })
                .next()
                .await
                .expect("a page")
                .expect("trash");
            let options = ListFileRevisionsOptions { consistency };
            client
                .list_file_revisions_with_options(&spec, &options)
                .next()
                .await
                .expect("a page")
                .expect("revisions");
            client
                .list_file_revisions_by_inode_with_options(&namespace_id, InodeId(2), &options)
                .next()
                .await
                .expect("a page")
                .expect("revisions by inode");
        }
        let queries: Vec<_> = transport
            .sent()
            .into_iter()
            .map(|request| {
                let (_, query) = request.url.split_once('?').expect("a query string");
                query
                    .split('&')
                    .filter(|parameter| {
                        !parameter.starts_with("path=") && !parameter.starts_with("limit=")
                    })
                    .collect::<Vec<_>>()
                    .join("&")
            })
            .collect();
        let expected = [
            "",
            "consistency=strong",
            "consistency=eventual&max_staleness_ms=2500",
        ]
        .into_iter()
        .flat_map(|query| [query; 3])
        .collect::<Vec<_>>();
        assert_eq!(queries, expected);
    }

    #[tokio::test]
    async fn an_unknown_event_kind_decodes_as_unknown_and_a_known_event_is_unchanged() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let content_id = ContentId::parse("con_4z59p0wjvx9a8b2frdhhf6mat1").expect("content id");
        let content_ref = ContentRef::blob_v1(
            namespace_id.clone(),
            content_id,
            b"hello",
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
        );
        let feed = serde_json::json!({
            "namespace_id": "demo",
            "after_seq": 418,
            "head_seq": 419,
            "changes": [{
                "namespace_id": "demo",
                "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
                "committed_seq": 419,
                "committed_by": "usr_8f3c",
                "committed_at_ms": 1_752_624_000_000_u64,
                "events": [
                    {
                        "kind": "link_created",
                        "inode_id": "ino_0000000000000000000000001b",
                        "parent_inode_id": "ino_00000000000000000000000007",
                        "display_name": "latest",
                        "binding_version": "def",
                        "target_inode_id": "ino_0000000000000000000000001a"
                    },
                    {
                        "kind": "content_changed",
                        "inode_id": "ino_0000000000000000000000001a",
                        "revision_no": 8,
                        "content_ref": content_ref
                    }
                ]
            }]
        });
        let transport =
            scripted_transport::script([Outcome::Success(feed.to_string().into_bytes())]);

        let page = client_for(&transport)
            .list_changes(&namespace_id, ChangeSeq(418))
            .next()
            .await
            .expect("one page")
            .expect("decoded feed");
        let events = &page.changes[0].events;
        assert_eq!(events[0], FilesystemChange::Unknown);
        assert_eq!(
            serde_json::to_value(&events[1]).expect("serialize known event"),
            feed["changes"][0]["events"][1]
        );
    }
}
