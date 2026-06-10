use crate::keys::{derived_progress, namespace_head, namespace_lease, DerivedWorkClass};
use crate::{ObjectStore, ObjectStoreError};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractProbeReport {
    pub run_id: String,
    pub checks: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ContractProbeError {
    #[error("probe `{probe}` failed: {message}")]
    Probe {
        probe: &'static str,
        message: String,
    },
}

pub async fn run_contract_probes<S: ObjectStore + ?Sized>(
    store: &S,
    run_id: &str,
) -> Result<ContractProbeReport, ContractProbeError> {
    let mut checks = Vec::new();

    probe_create_if_absent(store, run_id).await?;
    checks.push("create_if_absent".to_owned());

    probe_compare_and_swap(store, run_id).await?;
    checks.push("compare_and_swap".to_owned());

    probe_get_with_metadata(store, run_id).await?;
    checks.push("get_with_metadata".to_owned());

    probe_visibility_after_write(store, run_id).await?;
    checks.push("visibility_after_write".to_owned());

    probe_visibility_after_delete(store, run_id).await?;
    checks.push("visibility_after_delete".to_owned());

    probe_sorted_listing(store, run_id).await?;
    checks.push("sorted_listing".to_owned());

    probe_scoped_prefix_behavior(store, run_id).await?;
    checks.push("scoped_prefix_behavior".to_owned());

    Ok(ContractProbeReport {
        run_id: run_id.to_owned(),
        checks,
    })
}

async fn probe_create_if_absent<S: ObjectStore + ?Sized>(
    store: &S,
    run_id: &str,
) -> Result<(), ContractProbeError> {
    let key = namespace_head(&probe_namespace(run_id, "create-if-absent"));
    let _ = store.delete(&key).await;

    store
        .put_if_absent(&key, Bytes::from_static(br#"{"seq":41}"#))
        .await
        .map_err(|err| probe_error("create_if_absent", err))?;

    let second = store
        .put_if_absent(&key, Bytes::from_static(br#"{"seq":42}"#))
        .await;
    assert_precondition_failed("create_if_absent", second)?;

    let body = store
        .get(&key, None)
        .await
        .map_err(|err| probe_error("create_if_absent", err))?
        .ok_or_else(|| ContractProbeError::Probe {
            probe: "create_if_absent",
            message: "expected created object to exist".to_owned(),
        })?;
    if body.as_ref() != br#"{"seq":41}"# {
        return Err(ContractProbeError::Probe {
            probe: "create_if_absent",
            message: "create-if-absent allowed bytes to change".to_owned(),
        });
    }

    cleanup_key(store, "create_if_absent", &key).await;
    Ok(())
}

async fn probe_get_with_metadata<S: ObjectStore + ?Sized>(
    store: &S,
    run_id: &str,
) -> Result<(), ContractProbeError> {
    let key = namespace_head(&probe_namespace(run_id, "get-with-metadata"));
    let _ = store.delete(&key).await;
    let bytes = br#"{"seq":41,"read":"full"}"#;

    let written = store
        .put_if_absent(&key, Bytes::copy_from_slice(bytes))
        .await
        .map_err(|err| probe_error("get_with_metadata", err))?;
    let loaded = store
        .get_with_metadata(&key)
        .await
        .map_err(|err| probe_error("get_with_metadata", err))?
        .ok_or_else(|| ContractProbeError::Probe {
            probe: "get_with_metadata",
            message: "expected full-object read to find written object".to_owned(),
        })?;

    if loaded.bytes != bytes {
        return Err(ContractProbeError::Probe {
            probe: "get_with_metadata",
            message: "full-object read returned unexpected bytes".to_owned(),
        });
    }
    if loaded.metadata.size_bytes != bytes.len() as u64 {
        return Err(ContractProbeError::Probe {
            probe: "get_with_metadata",
            message: "full-object read returned unexpected size metadata".to_owned(),
        });
    }
    if loaded.metadata.etag != written.etag {
        return Err(ContractProbeError::Probe {
            probe: "get_with_metadata",
            message: "full-object read returned unexpected object identity".to_owned(),
        });
    }

    cleanup_key(store, "get_with_metadata", &key).await;
    Ok(())
}

async fn probe_compare_and_swap<S: ObjectStore + ?Sized>(
    store: &S,
    run_id: &str,
) -> Result<(), ContractProbeError> {
    let key = namespace_head(&probe_namespace(run_id, "cas"));
    let _ = store.delete(&key).await;

    store
        .put_if_absent(&key, Bytes::from_static(br#"{"seq":41,"fence_token":8}"#))
        .await
        .map_err(|err| probe_error("compare_and_swap", err))?;
    let first_read = store
        .head(&key)
        .await
        .map_err(|err| probe_error("compare_and_swap", err))?
        .ok_or_else(|| ContractProbeError::Probe {
            probe: "compare_and_swap",
            message: "expected compare-and-swap object metadata".to_owned(),
        })?
        .etag
        .ok_or_else(|| ContractProbeError::Probe {
            probe: "compare_and_swap",
            message: "expected compare-and-swap object etag".to_owned(),
        })?;

    store
        .compare_and_swap(
            &key,
            &first_read,
            Bytes::from_static(br#"{"seq":42,"fence_token":8}"#),
        )
        .await
        .map_err(|err| probe_error("compare_and_swap", err))?;

    let stale = store
        .compare_and_swap(
            &key,
            &first_read,
            Bytes::from_static(br#"{"seq":43,"fence_token":9}"#),
        )
        .await;
    assert_precondition_failed("compare_and_swap", stale)?;

    let body = store
        .get(&key, None)
        .await
        .map_err(|err| probe_error("compare_and_swap", err))?
        .ok_or_else(|| ContractProbeError::Probe {
            probe: "compare_and_swap",
            message: "expected compare-and-swap body to exist".to_owned(),
        })?;
    if body.as_ref() != br#"{"seq":42,"fence_token":8}"# {
        return Err(ContractProbeError::Probe {
            probe: "compare_and_swap",
            message: "compare-and-swap stale write changed stored bytes".to_owned(),
        });
    }

    cleanup_key(store, "compare_and_swap", &key).await;
    Ok(())
}

async fn probe_visibility_after_write<S: ObjectStore + ?Sized>(
    store: &S,
    run_id: &str,
) -> Result<(), ContractProbeError> {
    let key = derived_progress(
        &probe_namespace(run_id, "visibility"),
        DerivedWorkClass::ManifestBuilder,
    );
    let _ = store.delete(&key).await;

    store
        .put_if_absent(&key, Bytes::from_static(br#"{"built_through_seq":420}"#))
        .await
        .map_err(|err| probe_error("visibility_after_write", err))?;
    let listed = store
        .list_prefix(&format!(
            "namespaces/{}/derived/",
            probe_namespace(run_id, "visibility")
        ))
        .await
        .map_err(|err| probe_error("visibility_after_write", err))?;
    if listed != vec![key.clone()] {
        return Err(ContractProbeError::Probe {
            probe: "visibility_after_write",
            message: format!("unexpected keys after write: {listed:?}"),
        });
    }

    cleanup_key(store, "visibility_after_write", &key).await;
    Ok(())
}

async fn probe_visibility_after_delete<S: ObjectStore + ?Sized>(
    store: &S,
    run_id: &str,
) -> Result<(), ContractProbeError> {
    let namespace = probe_namespace(run_id, "delete");
    let key = derived_progress(&namespace, DerivedWorkClass::ManifestBuilder);
    let _ = store.delete(&key).await;

    store
        .put_if_absent(&key, Bytes::from_static(br#"{"built_through_seq":420}"#))
        .await
        .map_err(|err| probe_error("visibility_after_delete", err))?;
    store
        .delete(&key)
        .await
        .map_err(|err| probe_error("visibility_after_delete", err))?;
    let listed = store
        .list_prefix(&format!("namespaces/{namespace}/derived/"))
        .await
        .map_err(|err| probe_error("visibility_after_delete", err))?;
    if !listed.is_empty() {
        return Err(ContractProbeError::Probe {
            probe: "visibility_after_delete",
            message: format!("unexpected keys after delete: {listed:?}"),
        });
    }

    Ok(())
}

async fn probe_sorted_listing<S: ObjectStore + ?Sized>(
    store: &S,
    run_id: &str,
) -> Result<(), ContractProbeError> {
    let namespace = probe_namespace(run_id, "sorted");
    let keys = vec![
        derived_progress(&namespace, DerivedWorkClass::ManifestBuilder),
        namespace_head(&namespace),
        namespace_lease(&namespace),
    ];
    for key in &keys {
        let _ = store.delete(key).await;
    }

    store
        .put_if_absent(&keys[1], Bytes::from_static(br#"{"seq":1}"#))
        .await
        .map_err(|err| probe_error("sorted_listing", err))?;
    store
        .put_if_absent(&keys[2], Bytes::from_static(br#"{"lease":1}"#))
        .await
        .map_err(|err| probe_error("sorted_listing", err))?;
    store
        .put_if_absent(&keys[0], Bytes::from_static(br#"{"through_seq":1}"#))
        .await
        .map_err(|err| probe_error("sorted_listing", err))?;

    let listed = store
        .list_prefix(&format!("namespaces/{namespace}/"))
        .await
        .map_err(|err| probe_error("sorted_listing", err))?;
    let mut expected = keys.clone();
    expected.sort();
    if listed != expected {
        return Err(ContractProbeError::Probe {
            probe: "sorted_listing",
            message: format!("expected sorted keys {expected:?}, got {listed:?}"),
        });
    }

    for key in &keys {
        cleanup_key(store, "sorted_listing", key).await;
    }
    Ok(())
}

async fn probe_scoped_prefix_behavior<S: ObjectStore + ?Sized>(
    store: &S,
    run_id: &str,
) -> Result<(), ContractProbeError> {
    let left_namespace = probe_namespace(run_id, "scope-a");
    let right_namespace = probe_namespace(run_id, "scope-b");
    let left_key = namespace_head(&left_namespace);
    let right_key = namespace_head(&right_namespace);
    let _ = store.delete(&left_key).await;
    let _ = store.delete(&right_key).await;

    store
        .put_if_absent(&left_key, Bytes::from_static(br#"{"seq":1}"#))
        .await
        .map_err(|err| probe_error("scoped_prefix_behavior", err))?;
    store
        .put_if_absent(&right_key, Bytes::from_static(br#"{"seq":2}"#))
        .await
        .map_err(|err| probe_error("scoped_prefix_behavior", err))?;

    let left_list = store
        .list_prefix(&format!("namespaces/{left_namespace}/"))
        .await
        .map_err(|err| probe_error("scoped_prefix_behavior", err))?;
    if left_list != vec![left_key.clone()] {
        return Err(ContractProbeError::Probe {
            probe: "scoped_prefix_behavior",
            message: format!("unexpected scoped listing for left namespace: {left_list:?}"),
        });
    }
    let right_list = store
        .list_prefix(&format!("namespaces/{right_namespace}/"))
        .await
        .map_err(|err| probe_error("scoped_prefix_behavior", err))?;
    if right_list != vec![right_key.clone()] {
        return Err(ContractProbeError::Probe {
            probe: "scoped_prefix_behavior",
            message: format!("unexpected scoped listing for right namespace: {right_list:?}"),
        });
    }

    cleanup_key(store, "scoped_prefix_behavior", &left_key).await;
    cleanup_key(store, "scoped_prefix_behavior", &right_key).await;
    Ok(())
}

fn probe_namespace(run_id: &str, suffix: &str) -> String {
    format!("loonfs-doctor-{run_id}-{suffix}")
}

fn assert_precondition_failed<T>(
    probe: &'static str,
    result: Result<T, ObjectStoreError>,
) -> Result<(), ContractProbeError> {
    match result {
        Err(ObjectStoreError::PreconditionFailed) => Ok(()),
        Err(err) => Err(probe_error(probe, err)),
        Ok(_) => Err(ContractProbeError::Probe {
            probe,
            message: "expected precondition failure".to_owned(),
        }),
    }
}

async fn cleanup_key<S: ObjectStore + ?Sized>(store: &S, probe: &'static str, key: &str) {
    let _ = store
        .delete(key)
        .await
        .map_err(|err| ContractProbeError::Probe {
            probe,
            message: format!("cleanup failed for `{key}`: {err}"),
        });
}

fn probe_error(probe: &'static str, error: ObjectStoreError) -> ContractProbeError {
    ContractProbeError::Probe {
        probe,
        message: error.to_string(),
    }
}
