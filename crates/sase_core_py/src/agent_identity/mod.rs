//! Agent naming, identity, ownership roots, and tribe relationships.

use crate::prelude::*;

use crate::json_bridge::{json_value_to_py, py_to_json_value, serialize_to_py};

use crate::provider_policy::provider_priority_dict_from_py;

use pyo3::wrap_pyfunction;

#[pyfunction]
#[pyo3(name = "is_agent_name_template")]
fn py_is_agent_name_template(value: &str) -> bool {
    core_is_agent_name_template(value)
}

#[pyfunction]
#[pyo3(name = "parse_agent_name_template")]
fn py_parse_agent_name_template<'py>(
    py: Python<'py>,
    template: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let parsed = core_parse_agent_name_template(template)
        .map_err(|err| PyValueError::new_err(format!("{err}")))?;
    let dict = PyDict::new_bound(py);
    dict.set_item("template", parsed.template)?;
    dict.set_item("prefix", parsed.prefix)?;
    dict.set_item("suffix", parsed.suffix)?;
    dict.set_item("marker", parsed.marker)?;
    match parsed.key {
        Some(key) => {
            dict.set_item("key", agent_name_template_key_to_py(py, &key)?)?
        }
        None => dict.set_item("key", py.None())?,
    }
    Ok(dict)
}

fn agent_name_template_key_to_py<'py>(
    py: Python<'py>,
    key: &AgentNameTemplateKey,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new_bound(py);
    dict.set_item("id", &key.id)?;
    dict.set_item("qualified", key.qualified)?;
    Ok(dict)
}

#[pyfunction]
#[pyo3(name = "agent_name_template_key")]
fn py_agent_name_template_key<'py>(
    py: Python<'py>,
    template: &str,
) -> PyResult<Option<Bound<'py, PyDict>>> {
    core_agent_name_template_key(template)
        .map_err(|err| PyValueError::new_err(format!("{err}")))?
        .as_ref()
        .map(|key| agent_name_template_key_to_py(py, key))
        .transpose()
}

#[pyfunction]
#[pyo3(name = "iter_agent_name_key_markers")]
fn py_iter_agent_name_key_markers<'py>(
    py: Python<'py>,
    text: &str,
) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty_bound(py);
    for marker in core_iter_agent_name_key_markers(text) {
        let dict = PyDict::new_bound(py);
        dict.set_item("start", marker.start)?;
        dict.set_item("end", marker.end)?;
        dict.set_item("id", marker.id.as_deref())?;
        dict.set_item("qualified", marker.qualified)?;
        dict.set_item("braced", marker.braced)?;
        list.append(dict)?;
    }
    Ok(list)
}

#[pyfunction]
#[pyo3(name = "render_agent_name_template")]
fn py_render_agent_name_template(
    template: &str,
    token: &str,
) -> PyResult<String> {
    core_render_agent_name_template(template, token)
        .map_err(|err| PyValueError::new_err(format!("{err}")))
}

#[pyfunction]
#[pyo3(name = "agent_name_template_namespace_template")]
fn py_agent_name_template_namespace_template(
    template: &str,
) -> PyResult<String> {
    core_agent_name_template_namespace_template(template)
        .map_err(|err| PyValueError::new_err(format!("{err}")))
}

#[pyfunction]
#[pyo3(name = "match_agent_name_template")]
fn py_match_agent_name_template(
    template: &str,
    concrete: &str,
) -> PyResult<Option<String>> {
    core_match_agent_name_template(template, concrete)
        .map_err(|err| PyValueError::new_err(format!("{err}")))
}

#[pyfunction]
#[pyo3(name = "compare_agent_name_template_tokens")]
fn py_compare_agent_name_template_tokens(
    left: &str,
    right: &str,
) -> PyResult<i8> {
    let ordering = core_compare_agent_name_template_tokens(left, right)
        .map_err(|err| PyValueError::new_err(format!("{err}")))?;
    Ok(match ordering {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    })
}

#[pyfunction]
#[pyo3(name = "agent_name_template_tokens_after", signature = (after=None, count=1))]
fn py_agent_name_template_tokens_after(
    after: Option<&str>,
    count: usize,
) -> PyResult<Vec<String>> {
    core_agent_name_template_tokens_after(after, count)
        .map_err(|err| PyValueError::new_err(format!("{err}")))
}

/// Validate a machine name (non-empty, `^[a-z_]+$`); raise `ValueError`
/// otherwise.
#[pyfunction]
#[pyo3(name = "validate_machine_name")]
fn py_validate_machine_name(name: &str) -> PyResult<()> {
    core_validate_machine_name(name)
        .map_err(|err| PyValueError::new_err(format!("{err}")))
}

/// Prepend `<machine_name>.` to an agent name unless already qualified.
#[pyfunction]
#[pyo3(name = "qualify_machine_agent_name")]
fn py_qualify_machine_agent_name(name: &str, machine_name: &str) -> String {
    core_qualify_machine_agent_name(name, machine_name)
}

/// Strip a leading `<machine_name>.` from an agent name when present.
#[pyfunction]
#[pyo3(name = "strip_machine_agent_name")]
fn py_strip_machine_agent_name(name: &str, machine_name: &str) -> String {
    core_strip_machine_agent_name(name, machine_name)
}

/// Return the leading hood segment of `name` when it names a known machine.
#[pyfunction]
#[pyo3(name = "machine_hood_of")]
fn py_machine_hood_of(
    name: &str,
    known_machines: Vec<String>,
) -> Option<String> {
    core_machine_hood_of(name, &known_machines)
}

#[pyfunction]
#[pyo3(name = "managed_origin_reconciliation_wire_schema_version")]
fn py_managed_origin_reconciliation_wire_schema_version() -> u32 {
    MANAGED_ORIGIN_RECONCILIATION_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "decide_managed_origin_reconciliation")]
fn py_decide_managed_origin_reconciliation<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request: ManagedOriginReconciliationRequestWire =
        provider_priority_dict_from_py(
            request.as_any(),
            "managed origin request",
        )?;
    let decision = core_decide_managed_origin_reconciliation(&request);
    serialize_to_py(py, &decision)
}

// The machine-hood bindings above are migration shims. New code should use
// the explicit owner-aware domain below.
fn explicit_owner(
    username: &str,
    machine_name: &str,
) -> PyResult<AgentOwnerIdentity> {
    AgentOwnerIdentity::new(username, machine_name)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

fn identity_wire_to_py<'py, T: serde::Serialize>(
    py: Python<'py>,
    value: &T,
) -> PyResult<PyObject> {
    let value = serde_json::to_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "internal agent identity serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

fn optional_owner_roots(known_owner_roots: Option<Vec<String>>) -> Vec<String> {
    known_owner_roots.unwrap_or_default()
}

#[pyfunction]
#[pyo3(name = "validate_agent_username")]
fn py_validate_agent_username(username: &str) -> PyResult<()> {
    core_validate_agent_username(username)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(name = "validate_owner_root")]
fn py_validate_owner_root(root: &str) -> PyResult<()> {
    core_validate_owner_root(root)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(name = "validate_agent_name")]
fn py_validate_agent_name(name: &str) -> PyResult<()> {
    core_validate_agent_name(name)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(
    name = "validate_owned_agent_name",
    signature = (name, username, machine_name, known_owner_roots = None)
)]
fn py_validate_owned_agent_name(
    name: &str,
    username: &str,
    machine_name: &str,
    known_owner_roots: Option<Vec<String>>,
) -> PyResult<()> {
    let roots = optional_owner_roots(known_owner_roots);
    core_validate_owned_agent_name(
        name,
        &explicit_owner(username, machine_name)?,
        &roots,
    )
    .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(name = "validate_agent_owner")]
fn py_validate_agent_owner(username: &str, machine_name: &str) -> PyResult<()> {
    explicit_owner(username, machine_name).map(|_| ())
}

fn agent_tribe_error_to_pyerr(error: AgentTribeDomainError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

#[pyfunction]
#[pyo3(name = "validate_tribe_name")]
fn py_validate_tribe_name(tribe: &str) -> PyResult<String> {
    core_validate_tribe_name(tribe)
        .map(str::to_string)
        .map_err(agent_tribe_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "canonicalize_public_tribe_name")]
fn py_canonicalize_public_tribe_name(tribe: &str) -> PyResult<String> {
    core_canonicalize_public_tribe_name(tribe)
        .map_err(agent_tribe_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "public_tribe_name")]
fn py_public_tribe_name(tribe: &str) -> String {
    core_public_tribe_name(tribe)
}

#[pyfunction]
#[pyo3(name = "parse_tribe_reference")]
fn py_parse_tribe_reference(value: &str) -> PyResult<Option<String>> {
    core_parse_tribe_reference(value).map_err(agent_tribe_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "is_reserved_tribe_name")]
fn py_is_reserved_tribe_name(tribe: &str) -> bool {
    core_is_reserved_tribe_name(tribe)
}

#[pyfunction]
#[pyo3(name = "reserved_tribe_target_reason")]
fn py_reserved_tribe_target_reason(tribe: &str) -> String {
    core_reserved_tribe_target_reason(tribe)
}

#[pyfunction]
#[pyo3(name = "canonicalize_agent_tribe_metadata")]
fn py_canonicalize_agent_tribe_metadata<'py>(
    py: Python<'py>,
    data: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(data.as_any())?;
    let serde_json::Value::Object(map) = value else {
        return Err(PyValueError::new_err(
            "agent tribe metadata must be a JSON object",
        ));
    };
    let result = core_canonicalize_agent_tribe_metadata(map);
    json_value_to_py(py, &serde_json::Value::Object(result))
}

#[pyfunction]
#[pyo3(name = "agent_tribe_display_key")]
fn py_agent_tribe_display_key(
    stored_tribe: &str,
    configured_keys: Vec<String>,
) -> PyResult<String> {
    core_agent_tribe_display_key(stored_tribe, &configured_keys)
        .map_err(agent_tribe_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "resolve_agent_tribe_display_config")]
fn py_resolve_agent_tribe_display_config<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: AgentTribeDisplayResolutionRequestWire =
        serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid agent tribe display resolution request: {error}"
            ))
        })?;
    let result = core_resolve_agent_tribe_display_config(&request);
    serialize_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "resolve_agent_tribe_identity")]
fn py_resolve_agent_tribe_identity<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: AgentTribeIdentityResolutionRequestWire =
        serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid agent tribe identity resolution request: {error}"
            ))
        })?;
    let result = core_resolve_agent_tribe_identity(&request)
        .map_err(agent_tribe_error_to_pyerr)?;
    serialize_to_py(py, &result)
}

#[pyfunction]
#[pyo3(name = "normalize_agent_archive_name")]
fn py_normalize_agent_archive_name(name: &str) -> PyResult<String> {
    core_normalize_agent_archive_name(name)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(
    name = "normalize_owned_agent_name",
    signature = (name, username, machine_name, known_owner_roots = None)
)]
fn py_normalize_owned_agent_name(
    name: &str,
    username: &str,
    machine_name: &str,
    known_owner_roots: Option<Vec<String>>,
) -> PyResult<String> {
    let roots = optional_owner_roots(known_owner_roots);
    core_normalize_owned_agent_name(
        name,
        &explicit_owner(username, machine_name)?,
        &roots,
    )
    .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(name = "globalize_agent_name")]
fn py_globalize_agent_name(
    local_name: &str,
    username: &str,
    machine_name: &str,
) -> PyResult<String> {
    core_globalize_agent_name(
        local_name,
        &explicit_owner(username, machine_name)?,
    )
    .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(
    name = "globalize_owned_agent_name",
    signature = (name, username, machine_name, known_owner_roots = None)
)]
fn py_globalize_owned_agent_name(
    name: &str,
    username: &str,
    machine_name: &str,
    known_owner_roots: Option<Vec<String>>,
) -> PyResult<String> {
    let roots = optional_owner_roots(known_owner_roots);
    core_globalize_owned_agent_name(
        name,
        &explicit_owner(username, machine_name)?,
        &roots,
    )
    .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(
    name = "foreign_agent_owner_root",
    signature = (name, username, machine_name, known_owner_roots = None)
)]
fn py_foreign_agent_owner_root(
    name: &str,
    username: &str,
    machine_name: &str,
    known_owner_roots: Option<Vec<String>>,
) -> PyResult<Option<String>> {
    let roots = optional_owner_roots(known_owner_roots);
    core_foreign_agent_owner_root(
        name,
        &explicit_owner(username, machine_name)?,
        &roots,
    )
    .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(name = "strip_global_agent_name")]
fn py_strip_global_agent_name(
    global_name: &str,
    username: &str,
    machine_name: &str,
) -> PyResult<String> {
    core_strip_global_agent_name(
        global_name,
        &explicit_owner(username, machine_name)?,
    )
    .map_err(|error| PyValueError::new_err(error.to_string()))
}

fn parse_agent_session_name_impl(
    py: Python<'_>,
    name: &str,
) -> PyResult<PyObject> {
    let parsed = core_parse_agent_session_name(name)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    identity_wire_to_py(py, &parsed)
}

#[pyfunction]
#[pyo3(name = "parse_agent_session_name")]
fn py_parse_agent_session_name(
    py: Python<'_>,
    name: &str,
) -> PyResult<PyObject> {
    parse_agent_session_name_impl(py, name)
}

#[pyfunction]
#[pyo3(name = "parse_owned_agent_name", signature = (name, known_owner_roots = None))]
fn py_parse_owned_agent_name(
    py: Python<'_>,
    name: &str,
    known_owner_roots: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let roots = optional_owner_roots(known_owner_roots);
    let parsed = core_parse_owned_agent_name(name, &roots)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    identity_wire_to_py(py, &parsed)
}

#[pyfunction]
#[pyo3(name = "agent_local_hood", signature = (name, known_owner_roots = None))]
fn py_agent_local_hood(
    name: &str,
    known_owner_roots: Option<Vec<String>>,
) -> PyResult<String> {
    match known_owner_roots {
        Some(roots) => core_agent_local_hood_with_owner_roots(name, &roots),
        None => core_agent_local_hood(name),
    }
    .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(
    name = "agent_name_in_hood",
    signature = (name, hood, known_owner_roots = None)
)]
fn py_agent_name_in_hood(
    name: &str,
    hood: &str,
    known_owner_roots: Option<Vec<String>>,
) -> PyResult<bool> {
    match known_owner_roots {
        Some(roots) => {
            core_agent_name_in_hood_with_owner_roots(name, hood, &roots)
        }
        None => core_agent_name_in_hood(name, hood),
    }
    .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(name = "agent_name_ancestors", signature = (name, known_owner_roots = None))]
fn py_agent_name_ancestors(
    name: &str,
    known_owner_roots: Option<Vec<String>>,
) -> PyResult<Vec<String>> {
    match known_owner_roots {
        Some(roots) => core_agent_name_ancestors_with_owner_roots(name, &roots),
        None => core_agent_name_ancestors(name),
    }
    .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(
    name = "agent_link_target",
    signature = (name, username, machine_name, known_owner_roots = None)
)]
fn py_agent_link_target(
    py: Python<'_>,
    name: &str,
    username: &str,
    machine_name: &str,
    known_owner_roots: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let owner = explicit_owner(username, machine_name)?;
    let target = match known_owner_roots {
        Some(roots) => {
            core_agent_link_target_with_owner_roots(name, &owner, &roots)
        }
        None => core_agent_link_target(name, &owner),
    }
    .map_err(|error| PyValueError::new_err(error.to_string()))?;
    identity_wire_to_py(py, &target)
}

#[pyfunction]
#[pyo3(name = "agent_relationship_schema_version")]
fn py_agent_relationship_schema_version() -> u32 {
    AGENT_RELATIONSHIP_SCHEMA_VERSION
}

fn relationship_batch_from_pydict(
    batch: &Bound<'_, PyDict>,
) -> PyResult<AgentRelationshipBatchWire> {
    let value = py_to_json_value(batch.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "invalid agent relationship batch: {error}"
        ))
    })
}

#[pyfunction]
#[pyo3(name = "validate_agent_relationship_batch")]
fn py_validate_agent_relationship_batch(
    py: Python<'_>,
    batch: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let batch = relationship_batch_from_pydict(batch)?;
    let summary = core_validate_agent_relationship_batch(&batch)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    identity_wire_to_py(py, &summary)
}

#[pyfunction]
#[pyo3(name = "rewrite_agent_relationship_batch")]
fn py_rewrite_agent_relationship_batch(
    py: Python<'_>,
    batch: &Bound<'_, PyDict>,
    destination_ids: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let batch = relationship_batch_from_pydict(batch)?;
    let destination_ids = serde_json::from_value::<BTreeMap<String, String>>(
        py_to_json_value(destination_ids.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!("invalid destination ID map: {error}"))
    })?;
    let rewritten =
        core_rewrite_agent_relationship_batch(&batch, &destination_ids)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
    identity_wire_to_py(py, &rewritten)
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(
    name = "project_agent_relationship_graph",
    signature = (
        batch,
        destination_ids,
        source_username,
        source_machine_name,
        destination_username,
        destination_machine_name,
        known_owner_roots = None
    )
)]
fn py_project_agent_relationship_graph(
    py: Python<'_>,
    batch: &Bound<'_, PyDict>,
    destination_ids: &Bound<'_, PyDict>,
    source_username: &str,
    source_machine_name: &str,
    destination_username: &str,
    destination_machine_name: &str,
    known_owner_roots: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let batch = relationship_batch_from_pydict(batch)?;
    let destination_ids = serde_json::from_value::<BTreeMap<String, String>>(
        py_to_json_value(destination_ids.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!("invalid destination ID map: {error}"))
    })?;
    let roots = optional_owner_roots(known_owner_roots);
    let projection = core_project_agent_relationship_graph(
        &batch,
        &destination_ids,
        &explicit_owner(source_username, source_machine_name)?,
        &explicit_owner(destination_username, destination_machine_name)?,
        &roots,
    )
    .map_err(|error| PyValueError::new_err(error.to_string()))?;
    identity_wire_to_py(py, &projection)
}

fn resolve_agent_session_parent_impl<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: AgentSessionParentResolutionRequestWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid AgentSessionParentResolutionRequestWire dict: {e}"
            ))
        })?;
    let result = py
        .allow_threads(|| core_resolve_agent_session_parent(request))
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "resolve_agent_session_parent")]
fn py_resolve_agent_session_parent<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    resolve_agent_session_parent_impl(py, request)
}

pub(crate) fn register_agent_identity(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_is_agent_name_template, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_agent_name_template, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_name_template_key, m)?)?;
    m.add_function(wrap_pyfunction!(py_iter_agent_name_key_markers, m)?)?;
    m.add_function(wrap_pyfunction!(py_render_agent_name_template, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_agent_name_template_namespace_template,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_match_agent_name_template, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_compare_agent_name_template_tokens,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_agent_name_template_tokens_after, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_machine_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_qualify_machine_agent_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_strip_machine_agent_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_machine_hood_of, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_agent_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_agent_username, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_owner_root, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_owned_agent_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_agent_owner, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_tribe_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_canonicalize_public_tribe_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_public_tribe_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_tribe_reference, m)?)?;
    m.add_function(wrap_pyfunction!(py_is_reserved_tribe_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_reserved_tribe_target_reason, m)?)?;
    m.add_function(wrap_pyfunction!(py_canonicalize_agent_tribe_metadata, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_tribe_display_key, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_resolve_agent_tribe_display_config,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_resolve_agent_tribe_identity, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_managed_origin_reconciliation_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_decide_managed_origin_reconciliation,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_normalize_agent_archive_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_normalize_owned_agent_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_globalize_agent_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_globalize_owned_agent_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_foreign_agent_owner_root, m)?)?;
    m.add_function(wrap_pyfunction!(py_strip_global_agent_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_agent_session_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_owned_agent_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_local_hood, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_name_in_hood, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_name_ancestors, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_link_target, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_relationship_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_agent_relationship_batch, m)?)?;
    m.add_function(wrap_pyfunction!(py_rewrite_agent_relationship_batch, m)?)?;
    m.add_function(wrap_pyfunction!(py_project_agent_relationship_graph, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_agent_session_parent, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
