//! Stages file bytes as durable content before a metadata publish.

use super::NamespaceFixture;
use crate::error::Result;
use crate::path::mutation_path::parse_mutation_path;
use crate::storage::content::{prepare_stored_content, stage_bytes_under_content_id};
use crate::storage::content_admission::PreparedContent;
use loonfs_objectstore::ObjectStore;

impl<S: ObjectStore + Clone> NamespaceFixture<S> {
    pub(super) async fn store_file_bytes_before_metadata_publish(
        &self,
        absolute_path: &str,
        bytes: &[u8],
    ) -> Result<PreparedContent> {
        parse_mutation_path(absolute_path)?;
        let catalog = self.namespace.load_catalog_entry().await?;
        let stored = stage_bytes_under_content_id(
            &self.namespace.store,
            catalog.namespace_id().clone(),
            loonfs_types::ContentId::generate(),
            bytes,
        )
        .await?;
        Ok(prepare_stored_content(&catalog, stored))
    }
}
