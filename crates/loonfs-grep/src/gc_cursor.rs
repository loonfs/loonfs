//! Continuation tokens for grep object collection.

use loonfs::{CoreError, NamespaceId};
use loonfs_api::{
    decode_namespace_cursor, encode_cursor, NamespaceCursor, NamespaceCursorError, PageCursor,
};
use serde::{Deserialize, Serialize};
type Result<T> = std::result::Result<T, CoreError>;

/// Defines the namespace key prefix and token kind for a GC cursor.
pub(crate) trait GcCursorKeyspace {
    const CURSOR_KIND: &'static str;

    fn prefix(&self, namespace_id: &NamespaceId) -> String;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "Keyspace: Serialize",
    deserialize = "Keyspace: Deserialize<'de>"
))]
pub(crate) struct NamespaceGcCursor<Keyspace> {
    namespace_id: NamespaceId,
    #[serde(flatten)]
    keyspace: Keyspace,
    #[serde(default)]
    last_key: Option<String>,
}

impl<Keyspace> PageCursor for NamespaceGcCursor<Keyspace>
where
    Keyspace: GcCursorKeyspace + Serialize + for<'de> Deserialize<'de>,
{
    const KIND: &'static str = Keyspace::CURSOR_KIND;
}

impl<Keyspace> NamespaceCursor for NamespaceGcCursor<Keyspace>
where
    Keyspace: GcCursorKeyspace + Serialize + for<'de> Deserialize<'de>,
{
    fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    fn last_key(&self) -> Option<&str> {
        self.last_key.as_deref()
    }

    fn key_prefix(&self) -> String {
        self.keyspace.prefix(&self.namespace_id)
    }
}

impl<Keyspace> NamespaceGcCursor<Keyspace>
where
    Keyspace: GcCursorKeyspace + Serialize + for<'de> Deserialize<'de>,
{
    pub(crate) fn initial(namespace_id: &NamespaceId, keyspace: Keyspace) -> Self {
        Self {
            namespace_id: namespace_id.clone(),
            keyspace,
            last_key: None,
        }
    }

    pub(crate) fn after(namespace_id: &NamespaceId, keyspace: Keyspace, key: String) -> Self {
        Self {
            namespace_id: namespace_id.clone(),
            keyspace,
            last_key: Some(key),
        }
    }

    pub(crate) fn decode(token: &str, namespace_id: &NamespaceId) -> Result<Self> {
        decode_namespace_cursor(token, namespace_id).map_err(|error| {
            if matches!(error, NamespaceCursorError::ForeignNamespace) {
                CoreError::InvalidGcConfig(error.to_string())
            } else {
                invalid_cursor()
            }
        })
    }

    pub(crate) fn encode(&self) -> Result<String> {
        encode_cursor(self)
            .map_err(|error| CoreError::Internal(format!("failed to encode gc cursor: {error}")))
    }

    pub(crate) fn last_key(&self) -> Option<&str> {
        self.last_key.as_deref()
    }
}

fn invalid_cursor() -> CoreError {
    CoreError::InvalidGcConfig("cursor is malformed".to_owned())
}
