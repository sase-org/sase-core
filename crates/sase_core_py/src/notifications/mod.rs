//! Notification store and pending-action transport bindings.

use crate::prelude::*;

use crate::json_bridge::{
    json_record_from_pydict, json_value_to_py, py_to_json_value,
};

use pyo3::wrap_pyfunction;

// --- Notification store bindings -----------------------------------------
/// Read the notification JSONL store and return a snapshot dict.
///
/// The GIL is released while Rust performs filesystem work. When
/// ``expire_due_snoozes`` is true, due snoozes are expired under the same
/// store lock before the returned snapshot is built.
#[pyfunction]
#[pyo3(name = "read_notifications_snapshot", signature = (path, include_dismissed, expire_due_snoozes = false))]
fn py_read_notifications_snapshot<'py>(
    py: Python<'py>,
    path: &str,
    include_dismissed: bool,
    expire_due_snoozes: bool,
) -> PyResult<PyObject> {
    let path = PathBuf::from(path);
    let snapshot = py.allow_threads(|| {
        core_read_notifications_snapshot_with_options(
            &path,
            include_dismissed,
            expire_due_snoozes,
        )
    });
    let value = serde_json::to_value(snapshot.map_err(PyValueError::new_err)?)
        .map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?;
    json_value_to_py(py, &value)
}

/// Read and reconcile the user-facing current notification state.
#[pyfunction]
#[pyo3(name = "read_current_notifications_snapshot")]
fn py_read_current_notifications_snapshot<'py>(
    py: Python<'py>,
    path: &str,
    include_dismissed: bool,
) -> PyResult<PyObject> {
    let path = PathBuf::from(path);
    let snapshot = py.allow_threads(|| {
        core_read_current_notifications_snapshot(&path, include_dismissed)
    });
    let value = serde_json::to_value(snapshot.map_err(PyValueError::new_err)?)
        .map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?;
    json_value_to_py(py, &value)
}

/// Apply one notification state update and return the outcome dict.
#[pyfunction]
#[pyo3(name = "apply_notification_state_update")]
fn py_apply_notification_state_update<'py>(
    py: Python<'py>,
    path: &str,
    update: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let update = notification_update_from_pydict(update)?;
    let path = PathBuf::from(path);
    let outcome = py
        .allow_threads(|| core_apply_notification_state_update(&path, &update));
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Apply one notification state update and return only mutation metadata.
#[pyfunction]
#[pyo3(name = "apply_notification_state_update_counts")]
fn py_apply_notification_state_update_counts<'py>(
    py: Python<'py>,
    path: &str,
    update: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let update = notification_update_from_pydict(update)?;
    let path = PathBuf::from(path);
    let outcome = py.allow_threads(|| {
        core_apply_notification_state_update_counts(&path, &update)
    });
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Append one notification dict and return the outcome dict.
#[pyfunction]
#[pyo3(name = "append_notification")]
fn py_append_notification<'py>(
    py: Python<'py>,
    path: &str,
    notification: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let notification = notification_from_pydict(notification)?;
    let path = PathBuf::from(path);
    let outcome =
        py.allow_threads(|| core_append_notification(&path, &notification));
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Append one notification dict and return only mutation metadata.
#[pyfunction]
#[pyo3(name = "append_notification_counts")]
fn py_append_notification_counts<'py>(
    py: Python<'py>,
    path: &str,
    notification: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let notification = notification_from_pydict(notification)?;
    let path = PathBuf::from(path);
    let outcome = py.allow_threads(|| {
        core_append_notification_counts(&path, &notification)
    });
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Append one plus-one entry by id or `(sender, dedup_key)`.
#[pyfunction]
#[pyo3(name = "append_notification_plus_one")]
fn py_append_notification_plus_one<'py>(
    py: Python<'py>,
    path: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request = plus_one_request_from_pydict(request)?;
    let path = PathBuf::from(path);
    let outcome =
        py.allow_threads(|| core_append_notification_plus_one(&path, &request));
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Create a minted notification or +1 the matching `(sender, dedup_key)` row.
#[pyfunction]
#[pyo3(name = "upsert_notification")]
fn py_upsert_notification<'py>(
    py: Python<'py>,
    path: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request = upsert_request_from_pydict(request)?;
    let path = PathBuf::from(path);
    let outcome =
        py.allow_threads(|| core_upsert_notification(&path, &request));
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Rewrite the notification JSONL store from notification dicts.
#[pyfunction]
#[pyo3(name = "rewrite_notifications")]
fn py_rewrite_notifications<'py>(
    py: Python<'py>,
    path: &str,
    notifications: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let notifications = notifications_from_py_list(notifications)?;
    let path = PathBuf::from(path);
    let outcome =
        py.allow_threads(|| core_rewrite_notifications(&path, &notifications));
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Rewrite the notification JSONL store and return only mutation metadata.
#[pyfunction]
#[pyo3(name = "rewrite_notifications_counts")]
fn py_rewrite_notifications_counts<'py>(
    py: Python<'py>,
    path: &str,
    notifications: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let notifications = notifications_from_py_list(notifications)?;
    let path = PathBuf::from(path);
    let outcome = py.allow_threads(|| {
        core_rewrite_notifications_counts(&path, &notifications)
    });
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Classify notification dicts into ordered tabs and per-row tab keys.
///
/// One call classifies a whole page, so callers never pay one FFI hop per row.
#[pyfunction]
#[pyo3(name = "classify_notification_tabs")]
fn py_classify_notification_tabs<'py>(
    py: Python<'py>,
    notifications: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let notifications = notifications_from_py_list(notifications)?;
    let classification =
        py.allow_threads(|| core_classify_notification_tabs(&notifications));
    let value = serde_json::to_value(classification).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Resolve the delivery of each notification dict against delivery rule dicts.
///
/// One call resolves a whole poll batch, so callers never pay one FFI hop per
/// row. The result lists one delivery dict per notification, in input order.
/// A malformed rule dict (including an unknown key) raises `ValueError`.
#[pyfunction]
#[pyo3(name = "resolve_notification_deliveries")]
fn py_resolve_notification_deliveries<'py>(
    py: Python<'py>,
    rules: &Bound<'py, PyList>,
    notifications: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let rules = notification_rules_from_py_list(rules)?;
    let notifications = notifications_from_py_list(notifications)?;
    let deliveries = py.allow_threads(|| {
        core_resolve_notification_deliveries(&rules, &notifications)
    });
    let value = serde_json::to_value(deliveries).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Build one pending-action entry from a notification dict.
#[pyfunction]
#[pyo3(name = "pending_action_from_notification")]
fn py_pending_action_from_notification<'py>(
    py: Python<'py>,
    notification: &Bound<'py, PyDict>,
    now_unix: f64,
) -> PyResult<Option<PyObject>> {
    let notification = notification_from_pydict(notification)?;
    let Some(action) =
        core_pending_action_from_notification(&notification, now_unix)
    else {
        return Ok(None);
    };
    let value = serde_json::to_value(action).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    Ok(Some(json_value_to_py(py, &value)?))
}

/// Register one pending action and return the updated store.
#[pyfunction]
#[pyo3(name = "register_pending_action")]
fn py_register_pending_action<'py>(
    py: Python<'py>,
    path: &str,
    action: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let action = pending_action_from_pydict(action)?;
    let path = PathBuf::from(path);
    pending_action_result_to_py(
        py,
        py.allow_threads(|| core_register_pending_action(&path, &action)),
    )
}

/// Read the pending-action store, optionally merged with legacy Telegram rows.
#[pyfunction]
#[pyo3(name = "read_pending_action_store")]
#[pyo3(signature = (path, legacy_path = None))]
fn py_read_pending_action_store<'py>(
    py: Python<'py>,
    path: &str,
    legacy_path: Option<&str>,
) -> PyResult<PyObject> {
    let path = PathBuf::from(path);
    let legacy_path = legacy_path.map(PathBuf::from);
    pending_action_result_to_py(
        py,
        py.allow_threads(|| {
            core_read_pending_action_store(&path, legacy_path.as_deref())
        }),
    )
}

/// Merge a transport-owned record into one existing pending action.
#[pyfunction]
#[pyo3(name = "merge_pending_action_transport")]
#[pyo3(signature = (path, identifier, transport, record, now_unix = None))]
fn py_merge_pending_action_transport(
    py: Python<'_>,
    path: &str,
    identifier: &str,
    transport: &str,
    record: &Bound<'_, PyDict>,
    now_unix: Option<f64>,
) -> PyResult<bool> {
    let record = json_record_from_pydict(record)?;
    let path = PathBuf::from(path);
    let now_unix = now_unix.unwrap_or_else(core_current_unix_time);
    py.allow_threads(|| {
        core_merge_pending_action_transport(
            &path, identifier, transport, &record, now_unix,
        )
    })
    .map_err(PyValueError::new_err)
}

/// Mark one pending action as already handled.
#[pyfunction]
#[pyo3(name = "mark_pending_action_handled")]
#[pyo3(signature = (path, identifier, source, action = None, now_unix = None))]
fn py_mark_pending_action_handled(
    py: Python<'_>,
    path: &str,
    identifier: &str,
    source: &str,
    action: Option<&str>,
    now_unix: Option<f64>,
) -> PyResult<bool> {
    let path = PathBuf::from(path);
    let now_unix = now_unix.unwrap_or_else(core_current_unix_time);
    py.allow_threads(|| {
        core_mark_pending_action_handled(
            &path, identifier, source, action, now_unix,
        )
    })
    .map_err(PyValueError::new_err)
}

/// Remove one pending action by full id or unique prefix.
#[pyfunction]
#[pyo3(name = "remove_pending_action")]
fn py_remove_pending_action(
    py: Python<'_>,
    path: &str,
    identifier: &str,
) -> PyResult<bool> {
    let path = PathBuf::from(path);
    py.allow_threads(|| core_remove_pending_action(&path, identifier))
        .map_err(PyValueError::new_err)
}

/// Execute a transport-scoped pending-action operation.
#[pyfunction]
#[pyo3(name = "pending_action_transport")]
fn py_pending_action_transport<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request = pending_action_transport_request_from_pydict(request)?;
    let value = py
        .allow_threads(|| core_pending_action_transport(&request))
        .map_err(PyValueError::new_err)?;
    json_value_to_py(py, &value)
}

fn notification_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<NotificationWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "notification is not a valid NotificationWire dict: {e}"
        ))
    })
}

fn notifications_from_py_list(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<NotificationWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        let notification: NotificationWire =
            serde_json::from_value(value).map_err(|e| {
                PyValueError::new_err(format!(
                    "notifications[{idx}] is not a valid NotificationWire dict: {e}"
                ))
            })?;
        values.push(notification);
    }
    Ok(values)
}

fn notification_rules_from_py_list(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<NotificationRuleWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        let rule: NotificationRuleWire = serde_json::from_value(value)
            .map_err(|e| {
                PyValueError::new_err(format!(
                    "rules[{idx}] is not a valid NotificationRuleWire dict: {e}"
                ))
            })?;
        values.push(rule);
    }
    Ok(values)
}

fn plus_one_request_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<NotificationPlusOneRequestWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "request is not a valid NotificationPlusOneRequestWire dict: {e}"
        ))
    })
}

fn upsert_request_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<NotificationUpsertRequestWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "request is not a valid NotificationUpsertRequestWire dict: {e}"
        ))
    })
}

fn notification_update_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<NotificationStateUpdateWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "update is not a valid NotificationStateUpdateWire dict: {e}"
        ))
    })
}

fn pending_action_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<PendingActionWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "action is not a valid PendingActionWire dict: {e}"
        ))
    })
}

fn pending_action_transport_request_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<PendingActionTransportRequestWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "request is not a valid PendingActionTransportRequestWire dict: {e}"
        ))
    })
}

fn pending_action_result_to_py<T>(
    py: Python<'_>,
    result: Result<T, String>,
) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value = serde_json::to_value(result.map_err(PyValueError::new_err)?)
        .map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?;
    json_value_to_py(py, &value)
}

pub(crate) fn register_notifications(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_read_notifications_snapshot, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_read_current_notifications_snapshot,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_apply_notification_state_update, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_apply_notification_state_update_counts,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_append_notification, m)?)?;
    m.add_function(wrap_pyfunction!(py_append_notification_counts, m)?)?;
    m.add_function(wrap_pyfunction!(py_append_notification_plus_one, m)?)?;
    m.add_function(wrap_pyfunction!(py_upsert_notification, m)?)?;
    m.add_function(wrap_pyfunction!(py_rewrite_notifications, m)?)?;
    m.add_function(wrap_pyfunction!(py_rewrite_notifications_counts, m)?)?;
    m.add_function(wrap_pyfunction!(py_classify_notification_tabs, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_notification_deliveries, m)?)?;
    m.add_function(wrap_pyfunction!(py_pending_action_from_notification, m)?)?;
    m.add_function(wrap_pyfunction!(py_register_pending_action, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_pending_action_store, m)?)?;
    m.add_function(wrap_pyfunction!(py_merge_pending_action_transport, m)?)?;
    m.add_function(wrap_pyfunction!(py_mark_pending_action_handled, m)?)?;
    m.add_function(wrap_pyfunction!(py_remove_pending_action, m)?)?;
    m.add_function(wrap_pyfunction!(py_pending_action_transport, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
