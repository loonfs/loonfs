use crate::keys::{derived_progress, namespace_head, namespace_lease, DerivedWorkClass};
use crate::{ObjectStore, ObjectStoreError};
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

pub fn run_contract_probes<S: ObjectStore + ?Sized>(
    store: &S,
    run_id: &str,
) -> Result<ContractProbeReport, ContractProbeError> {
    let mut checks = Vec::new();

    probe_create_if_absent(store, run_id)?;
    checks.push("create_if_absent".to_owned());

    probe_compare_and_swap(store, run_id)?;
    checks.push("compare_and_swap".to_owned());

    probe_visibility_after_write(store, run_id)?;
    checks.push("visibility_after_write".to_owned());

    probe_visibility_after_delete(store, run_id)?;
    checks.push("visibility_after_delete".to_owned());

    probe_sorted_listing(store, run_id)?;
    checks.push("sorted_listing".to_owned());

    probe_scoped_prefix_behavior(store, run_id)?;
    checks.push("scoped_prefix_behavior".to_owned());

    Ok(ContractProbeReport {
        run_id: run_id.to_owned(),
        checks,
    })
}

fn probe_create_if_absent<S: ObjectStore + ?Sized>(
    store: &S,
    run_id: &str,
) -> Result<(), ContractProbeError> {
    let key = namespace_head(&probe_namespace(run_id, "create-if-absent"));
    let _ = store.delete(&key);

    store
        .put_if_absent(&key, br#"{"seq":41}"#)
        .map_err(|err| probe_error("create_if_absent", err))?;

    let second = store.put_if_absent(&key, br#"{"seq":42}"#);
    assert_precondition_failed("create_if_absent", second)?;

    let body = store
        .get(&key, None)
        .map_err(|err| probe_error("create_if_absent", err))?
        .ok_or_else(|| ContractProbeError::Probe {
            probe: "create_if_absent",
            message: "expected created object to exist".to_owned(),
        })?;
    if body != br#"{"seq":41}"# {
        return Err(ContractProbeError::Probe {
            probe: "create_if_absent",
            message: "create-if-absent allowed bytes to change".to_owned(),
        });
    }

    cleanup_key(store, "create_if_absent", &key);
    Ok(())
}

fn probe_compare_and_swap<S: ObjectStore + ?Sized>(
    store: &S,
    run_id: &str,
) -> Result<(), ContractProbeError> {
    let key = namespace_head(&probe_namespace(run_id, "cas"));
    let _ = store.delete(&key);

    store
        .put_if_absent(&key, br#"{"seq":41,"fence_token":8}"#)
        .map_err(|err| probe_error("compare_and_swap", err))?;
    let first_read = store
        .head(&key)
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
        .compare_and_swap(&key, &first_read, br#"{"seq":42,"fence_token":8}"#)
        .map_err(|err| probe_error("compare_and_swap", err))?;

    let stale = store.compare_and_swap(&key, &first_read, br#"{"seq":43,"fence_token":9}"#);
    assert_precondition_failed("compare_and_swap", stale)?;

    let body = store
        .get(&key, None)
        .map_err(|err| probe_error("compare_and_swap", err))?
        .ok_or_else(|| ContractProbeError::Probe {
            probe: "compare_and_swap",
            message: "expected compare-and-swap body to exist".to_owned(),
        })?;
    if body != br#"{"seq":42,"fence_token":8}"# {
        return Err(ContractProbeError::Probe {
            probe: "compare_and_swap",
            message: "compare-and-swap stale write changed stored bytes".to_owned(),
        });
    }

    cleanup_key(store, "compare_and_swap", &key);
    Ok(())
}

fn probe_visibility_after_write<S: ObjectStore + ?Sized>(
    store: &S,
    run_id: &str,
) -> Result<(), ContractProbeError> {
    let key = derived_progress(
        &probe_namespace(run_id, "visibility"),
        DerivedWorkClass::CheckpointBuilder,
    );
    let _ = store.delete(&key);

    store
        .put_if_absent(&key, br#"{"built_through_seq":420}"#)
        .map_err(|err| probe_error("visibility_after_write", err))?;
    let listed = store
        .list_prefix(&format!(
            "namespaces/{}/derived/",
            probe_namespace(run_id, "visibility")
        ))
        .map_err(|err| probe_error("visibility_after_write", err))?;
    if listed != vec![key.clone()] {
        return Err(ContractProbeError::Probe {
            probe: "visibility_after_write",
            message: format!("unexpected keys after write: {listed:?}"),
        });
    }

    cleanup_key(store, "visibility_after_write", &key);
    Ok(())
}

fn probe_visibility_after_delete<S: ObjectStore + ?Sized>(
    store: &S,
    run_id: &str,
) -> Result<(), ContractProbeError> {
    let namespace = probe_namespace(run_id, "delete");
    let key = derived_progress(&namespace, DerivedWorkClass::CheckpointBuilder);
    let _ = store.delete(&key);

    store
        .put_if_absent(&key, br#"{"built_through_seq":420}"#)
        .map_err(|err| probe_error("visibility_after_delete", err))?;
    store
        .delete(&key)
        .map_err(|err| probe_error("visibility_after_delete", err))?;
    let listed = store
        .list_prefix(&format!("namespaces/{namespace}/derived/"))
        .map_err(|err| probe_error("visibility_after_delete", err))?;
    if !listed.is_empty() {
        return Err(ContractProbeError::Probe {
            probe: "visibility_after_delete",
            message: format!("unexpected keys after delete: {listed:?}"),
        });
    }

    Ok(())
}

fn probe_sorted_listing<S: ObjectStore + ?Sized>(
    store: &S,
    run_id: &str,
) -> Result<(), ContractProbeError> {
    let namespace = probe_namespace(run_id, "sorted");
    let keys = vec![
        derived_progress(&namespace, DerivedWorkClass::CheckpointBuilder),
        namespace_head(&namespace),
        namespace_lease(&namespace),
    ];
    for key in &keys {
        let _ = store.delete(key);
    }

    store
        .put_if_absent(&keys[1], br#"{"seq":1}"#)
        .map_err(|err| probe_error("sorted_listing", err))?;
    store
        .put_if_absent(&keys[2], br#"{"lease":1}"#)
        .map_err(|err| probe_error("sorted_listing", err))?;
    store
        .put_if_absent(&keys[0], br#"{"through_seq":1}"#)
        .map_err(|err| probe_error("sorted_listing", err))?;

    let listed = store
        .list_prefix(&format!("namespaces/{namespace}/"))
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
        cleanup_key(store, "sorted_listing", key);
    }
    Ok(())
}

fn probe_scoped_prefix_behavior<S: ObjectStore + ?Sized>(
    store: &S,
    run_id: &str,
) -> Result<(), ContractProbeError> {
    let left_namespace = probe_namespace(run_id, "scope-a");
    let right_namespace = probe_namespace(run_id, "scope-b");
    let left_key = namespace_head(&left_namespace);
    let right_key = namespace_head(&right_namespace);
    let _ = store.delete(&left_key);
    let _ = store.delete(&right_key);

    store
        .put_if_absent(&left_key, br#"{"seq":1}"#)
        .map_err(|err| probe_error("scoped_prefix_behavior", err))?;
    store
        .put_if_absent(&right_key, br#"{"seq":2}"#)
        .map_err(|err| probe_error("scoped_prefix_behavior", err))?;

    let left_list = store
        .list_prefix(&format!("namespaces/{left_namespace}/"))
        .map_err(|err| probe_error("scoped_prefix_behavior", err))?;
    if left_list != vec![left_key.clone()] {
        return Err(ContractProbeError::Probe {
            probe: "scoped_prefix_behavior",
            message: format!("unexpected scoped listing for left namespace: {left_list:?}"),
        });
    }
    let right_list = store
        .list_prefix(&format!("namespaces/{right_namespace}/"))
        .map_err(|err| probe_error("scoped_prefix_behavior", err))?;
    if right_list != vec![right_key.clone()] {
        return Err(ContractProbeError::Probe {
            probe: "scoped_prefix_behavior",
            message: format!("unexpected scoped listing for right namespace: {right_list:?}"),
        });
    }

    cleanup_key(store, "scoped_prefix_behavior", &left_key);
    cleanup_key(store, "scoped_prefix_behavior", &right_key);
    Ok(())
}

fn probe_namespace(run_id: &str, suffix: &str) -> String {
    format!("doctor-{run_id}-{suffix}")
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

fn cleanup_key<S: ObjectStore + ?Sized>(store: &S, probe: &'static str, key: &str) {
    let _ = store.delete(key).map_err(|err| ContractProbeError::Probe {
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
