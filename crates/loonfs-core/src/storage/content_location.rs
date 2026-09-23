//! Resolves published content to resident WAL bytes or an object key.

use super::content::{
    content_object_key_for_ref, load_required_object, materialize_content,
    validate_loaded_content_bytes, DurableContentValidationError,
};
use crate::error::CoreError;
use bytes::Bytes;
use loonfs_api::ContentRef;
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
        tail: &crate::wal::ProjectedWalTail,
        content_ref: &ContentRef,
    ) -> Result<Self, DurableContentValidationError> {
        let object_key = content_object_key_for_ref(content_ref)?;
        if let Some(value) = tail.inline_content(&content_ref.content_id) {
            if value.content_ref == *content_ref {
                return Ok(Self::Tail {
                    bytes: value.bytes.clone(),
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
                let current = crate::namespace::control::load_current_manifest(
                    store,
                    &content_ref.owner_namespace_id,
                )
                .await?;
                if current.state.generation != content_ref.owner_generation {
                    return Err(
                        DurableContentValidationError::MissingContentObject { object_key }.into(),
                    );
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
    fn resident_content_requires_the_exact_reference() {
        let bytes = Bytes::from_static(b"resident bytes");
        let reference = content_ref(&bytes);
        let mut tail = ProjectedWalTail::default();
        tail.insert_inline_content(reference.clone(), bytes.clone());
        let resolve = |reference: &ContentRef| {
            ContentLocation::resolve(&tail, reference).expect("content location")
        };
        let object_key = content_object_key_for_ref(&reference).expect("content object key");
        assert_eq!(
            resolve(&reference),
            ContentLocation::Tail { bytes, object_key }
        );
        let mut other_owner = reference.clone();
        other_owner.owner_namespace_id =
            loonfs_api::NamespaceId::parse("other").expect("namespace");
        let mut other_generation = reference.clone();
        other_generation.owner_generation = loonfs_api::NamespaceGeneration(2);
        let mut other_size = reference.clone();
        other_size.size_bytes += 1;
        let mut other_checksum = reference;
        other_checksum.checksum = loonfs_api::Checksum::sha256(b"other bytes");
        for reference in [other_owner, other_generation, other_size, other_checksum] {
            assert_eq!(
                resolve(&reference),
                ContentLocation::Object {
                    object_key: content_object_key_for_ref(&reference).expect("content object key"),
                }
            );
        }
    }
}
