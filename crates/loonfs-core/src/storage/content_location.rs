//! Resolves published content to resident WAL bytes or an object key.

use super::content::{
    content_object_key_for_ref, load_required_object, materialize_content,
    validate_loaded_content_bytes, DurableContentValidationError,
};
use crate::error::CoreError;
use bytes::Bytes;
use loonfs_api::{ContentRef, ContentStoreId, NamespaceGeneration, NamespaceId};
use loonfs_objectstore::ObjectStore;

/// Identifies where a published reference's bytes are read from.
/// `Tail` carries the object key that will hold the content after folding.
/// Content errors are reported under that key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentLocation {
    Tail { bytes: Bytes, object_key: String },
    Object { object_key: String },
}

impl ContentLocation {
    pub(crate) fn resolve(
        namespace_id: &NamespaceId,
        generation: NamespaceGeneration,
        content_store_id: &ContentStoreId,
        tail: Option<&crate::wal::ProjectedWalTail>,
        content_ref: &ContentRef,
    ) -> Result<Self, DurableContentValidationError> {
        let object_key = content_object_key_for_ref(content_store_id, content_ref)?;
        if content_ref.owner_namespace_id == *namespace_id
            && content_ref.owner_generation == generation
        {
            if let Some(bytes) = tail.and_then(|tail| tail.inline_content(&content_ref.content_id))
            {
                return Ok(Self::Tail {
                    bytes: bytes.clone(),
                    object_key,
                });
            }
        }
        Ok(Self::Object { object_key })
    }

    pub(crate) fn object_key(&self) -> &str {
        match self {
            Self::Tail { object_key, .. } | Self::Object { object_key } => object_key,
        }
    }

    pub(crate) async fn materialize_download_key<S: ObjectStore + ?Sized>(
        self,
        store: &S,
        content_ref: &ContentRef,
    ) -> crate::error::Result<String> {
        match self {
            Self::Object { object_key } => Ok(object_key),
            Self::Tail { bytes, object_key } => {
                let current = crate::namespace::control::load_current_manifest(
                    store,
                    &content_ref.owner_namespace_id,
                )
                .await?;
                if current.state.generation != content_ref.owner_generation {
                    return Err(DurableContentValidationError::MissingContentGeneration {
                        owner_namespace_id: content_ref.owner_namespace_id.clone(),
                        owner_generation: content_ref.owner_generation,
                    }
                    .into());
                }
                validate_loaded_content_bytes(object_key.clone(), content_ref, &bytes)?;
                if let Some(stored) = store
                    .get(&object_key, None)
                    .await
                    .map_err(|error| CoreError::store(&object_key, &error))?
                {
                    if stored != bytes {
                        return Err(loonfs_objectstore::ImmutableWriteError::DifferentObject {
                            object_key,
                        }
                        .into());
                    }
                    return Ok(object_key);
                }
                materialize_content(store, &object_key, content_ref, bytes)
                    .await
                    .map_err(|error| match error {
                        CoreError::Store {
                            class: crate::error::StoreFailureClass::PermissionDenied,
                            ..
                        } => CoreError::ContentNotMaterialized {
                            content_id: content_ref.content_id.clone(),
                        },
                        error => error,
                    })?;
                Ok(object_key)
            }
        }
    }

    pub(crate) async fn get_bytes<S: ObjectStore + ?Sized>(
        &self,
        store: &S,
        content_ref: &ContentRef,
    ) -> Result<Vec<u8>, DurableContentValidationError> {
        match self {
            Self::Tail { bytes, object_key } => {
                validate_loaded_content_bytes(object_key.clone(), content_ref, bytes)?;
                Ok(bytes.to_vec())
            }
            Self::Object { object_key } => {
                let bytes = load_required_object(store, object_key, None).await?;
                validate_loaded_content_bytes(object_key.clone(), content_ref, &bytes)?;
                Ok(bytes)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::ProjectedWalTail;
    use loonfs_test_support::ids::content_ref;

    #[test]
    fn resident_content_requires_the_reading_namespace_and_generation() {
        let bytes = Bytes::from_static(b"resident bytes");
        let reference = content_ref(&bytes);
        let content_store_id = ContentStoreId::generate();
        let mut tail = ProjectedWalTail::default();
        tail.insert_inline_content(reference.clone(), bytes.clone());
        let resolve = |namespace_id: &NamespaceId, generation| {
            ContentLocation::resolve(
                namespace_id,
                generation,
                &content_store_id,
                Some(&tail),
                &reference,
            )
            .expect("content location")
        };
        let object_key =
            content_object_key_for_ref(&content_store_id, &reference).expect("content object key");
        assert_eq!(
            resolve(&reference.owner_namespace_id, reference.owner_generation),
            ContentLocation::Tail {
                bytes,
                object_key: object_key.clone(),
            }
        );
        assert_eq!(
            resolve(&reference.owner_namespace_id, NamespaceGeneration(2)),
            ContentLocation::Object {
                object_key: object_key.clone(),
            }
        );
        assert_eq!(
            resolve(
                &NamespaceId::parse("another-reader").expect("namespace"),
                reference.owner_generation,
            ),
            ContentLocation::Object { object_key }
        );
    }
}
