//! Provider and model policy bindings: disables, priorities, usage, and overrides.

use crate::prelude::*;

use crate::json_bridge::{json_value_to_py, py_to_json_value, serialize_to_py};

use pyo3::wrap_pyfunction;

// --- Temporary default reasoning-effort override -------------------------
fn effort_override_error_to_pyerr(err: EffortOverrideDomainError) -> PyErr {
    match err {
        EffortOverrideDomainError::Validation(message) => {
            PyValueError::new_err(message)
        }
        EffortOverrideDomainError::LockTimeout => {
            PyTimeoutError::new_err(err.to_string())
        }
        EffortOverrideDomainError::Io(_)
        | EffortOverrideDomainError::Json(_) => {
            PyRuntimeError::new_err(err.to_string())
        }
    }
}

pub(crate) fn effort_override_now(now: Option<f64>) -> PyResult<f64> {
    if let Some(value) = now {
        return Ok(value);
    }
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .map_err(|error| {
            PyRuntimeError::new_err(format!(
                "could not read the current Unix timestamp: {error}"
            ))
        })
}

fn effort_wire_to_py<'py, T: serde::Serialize>(
    py: Python<'py>,
    value: &T,
) -> PyResult<PyObject> {
    let json = serde_json::to_value(value).map_err(|error| {
        PyRuntimeError::new_err(format!(
            "internal effort serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &json)
}

#[pyfunction]
#[pyo3(name = "effort_override_wire_schema_version")]
fn py_effort_override_wire_schema_version() -> u32 {
    sase_core::EFFORT_OVERRIDE_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "effort_override_get", signature = (sase_home, now = None))]
fn py_effort_override_get<'py>(
    py: Python<'py>,
    sase_home: &str,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let record = core_get_effort_override(
        &PathBuf::from(sase_home),
        effort_override_now(now)?,
    )
    .map_err(effort_override_error_to_pyerr)?;
    effort_wire_to_py(py, &record)
}

#[pyfunction]
#[pyo3(
    name = "effort_override_set_relative",
    signature = (
        sase_home,
        effort,
        source,
        duration_seconds = None,
        now = None
    )
)]
fn py_effort_override_set_relative<'py>(
    py: Python<'py>,
    sase_home: &str,
    effort: &str,
    source: &str,
    duration_seconds: Option<f64>,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let record = core_set_effort_override_relative(
        &PathBuf::from(sase_home),
        effort,
        duration_seconds,
        source,
        effort_override_now(now)?,
    )
    .map_err(effort_override_error_to_pyerr)?;
    effort_wire_to_py(py, &record)
}

#[pyfunction]
#[pyo3(
    name = "effort_override_set_until",
    signature = (sase_home, effort, expires_at, source, now = None)
)]
fn py_effort_override_set_until<'py>(
    py: Python<'py>,
    sase_home: &str,
    effort: &str,
    expires_at: f64,
    source: &str,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let record = core_set_effort_override_until(
        &PathBuf::from(sase_home),
        effort,
        expires_at,
        source,
        effort_override_now(now)?,
    )
    .map_err(effort_override_error_to_pyerr)?;
    effort_wire_to_py(py, &record)
}

#[pyfunction]
#[pyo3(name = "effort_override_clear")]
fn py_effort_override_clear(sase_home: &str) -> PyResult<bool> {
    core_clear_effort_override(&PathBuf::from(sase_home))
        .map_err(effort_override_error_to_pyerr)
}

// --- Temporary maximum-running-agents override -----------------------
fn runner_limit_override_error_to_pyerr(
    err: RunnerLimitOverrideDomainError,
) -> PyErr {
    match err {
        RunnerLimitOverrideDomainError::Validation(message) => {
            PyValueError::new_err(message)
        }
        RunnerLimitOverrideDomainError::LockTimeout => {
            PyTimeoutError::new_err(err.to_string())
        }
        RunnerLimitOverrideDomainError::Io(_)
        | RunnerLimitOverrideDomainError::Json(_) => {
            PyRuntimeError::new_err(err.to_string())
        }
    }
}

#[pyfunction]
#[pyo3(name = "runner_limit_override_wire_schema_version")]
fn py_runner_limit_override_wire_schema_version() -> u32 {
    sase_core::RUNNER_LIMIT_OVERRIDE_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(
    name = "runner_limit_override_get",
    signature = (sase_home, now = None)
)]
fn py_runner_limit_override_get<'py>(
    py: Python<'py>,
    sase_home: &str,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let record = core_get_runner_limit_override(
        &PathBuf::from(sase_home),
        effort_override_now(now)?,
    )
    .map_err(runner_limit_override_error_to_pyerr)?;
    effort_wire_to_py(py, &record)
}

#[pyfunction]
#[pyo3(
    name = "runner_limit_override_set_relative",
    signature = (
        sase_home,
        limit,
        source,
        duration_seconds = None,
        now = None
    )
)]
fn py_runner_limit_override_set_relative<'py>(
    py: Python<'py>,
    sase_home: &str,
    limit: u64,
    source: &str,
    duration_seconds: Option<f64>,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let record = core_set_runner_limit_override_relative(
        &PathBuf::from(sase_home),
        limit,
        duration_seconds,
        source,
        effort_override_now(now)?,
    )
    .map_err(runner_limit_override_error_to_pyerr)?;
    effort_wire_to_py(py, &record)
}

#[pyfunction]
#[pyo3(
    name = "runner_limit_override_set_until",
    signature = (sase_home, limit, expires_at, source, now = None)
)]
fn py_runner_limit_override_set_until<'py>(
    py: Python<'py>,
    sase_home: &str,
    limit: u64,
    expires_at: f64,
    source: &str,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let record = core_set_runner_limit_override_until(
        &PathBuf::from(sase_home),
        limit,
        expires_at,
        source,
        effort_override_now(now)?,
    )
    .map_err(runner_limit_override_error_to_pyerr)?;
    effort_wire_to_py(py, &record)
}

#[pyfunction]
#[pyo3(name = "runner_limit_override_clear")]
fn py_runner_limit_override_clear(sase_home: &str) -> PyResult<bool> {
    core_clear_runner_limit_override(&PathBuf::from(sase_home))
        .map_err(runner_limit_override_error_to_pyerr)
}

// --- Temporary LLM provider disables ----------------------------------
fn provider_disable_error_to_pyerr(err: ProviderDisableDomainError) -> PyErr {
    match err {
        ProviderDisableDomainError::Validation(message) => {
            PyValueError::new_err(message)
        }
        ProviderDisableDomainError::LockTimeout
        | ProviderDisableDomainError::Io(_)
        | ProviderDisableDomainError::Json(_) => {
            PyRuntimeError::new_err(err.to_string())
        }
    }
}

fn provider_disable_wire_to_py<'py, T: serde::Serialize>(
    py: Python<'py>,
    value: &T,
) -> PyResult<PyObject> {
    let json = serde_json::to_value(value).map_err(|error| {
        PyRuntimeError::new_err(format!(
            "internal provider-disable serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &json)
}

fn parse_provider_disable_mode(mode: &str) -> PyResult<ProviderDisableMode> {
    ProviderDisableMode::parse(mode).map_err(provider_disable_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "provider_disable_wire_schema_version")]
fn py_provider_disable_wire_schema_version() -> u32 {
    sase_core::PROVIDER_DISABLE_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "provider_disable_get", signature = (sase_home, now = None))]
fn py_provider_disable_get<'py>(
    py: Python<'py>,
    sase_home: &str,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let snapshot = core_get_provider_disables(
        &PathBuf::from(sase_home),
        effort_override_now(now)?,
    )
    .map_err(provider_disable_error_to_pyerr)?;
    provider_disable_wire_to_py(py, &snapshot)
}

#[pyfunction]
#[pyo3(
    name = "provider_disable_set_relative",
    signature = (
        sase_home,
        provider,
        source,
        mode = "hard",
        duration_seconds = None,
        now = None
    )
)]
fn py_provider_disable_set_relative<'py>(
    py: Python<'py>,
    sase_home: &str,
    provider: &str,
    source: &str,
    mode: &str,
    duration_seconds: Option<f64>,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let record = core_set_provider_disable_relative(
        &PathBuf::from(sase_home),
        provider,
        duration_seconds,
        source,
        effort_override_now(now)?,
        parse_provider_disable_mode(mode)?,
    )
    .map_err(provider_disable_error_to_pyerr)?;
    provider_disable_wire_to_py(py, &record)
}

#[pyfunction]
#[pyo3(
    name = "provider_disable_set_until",
    signature = (sase_home, provider, expires_at, source, mode = "hard", now = None)
)]
fn py_provider_disable_set_until<'py>(
    py: Python<'py>,
    sase_home: &str,
    provider: &str,
    expires_at: f64,
    source: &str,
    mode: &str,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let record = core_set_provider_disable_until(
        &PathBuf::from(sase_home),
        provider,
        expires_at,
        source,
        effort_override_now(now)?,
        parse_provider_disable_mode(mode)?,
    )
    .map_err(provider_disable_error_to_pyerr)?;
    provider_disable_wire_to_py(py, &record)
}

#[pyfunction]
#[pyo3(
    name = "provider_disable_try_set_relative",
    signature = (
        sase_home,
        provider,
        source,
        mode = "hard",
        duration_seconds = None,
        now = None
    )
)]
fn py_provider_disable_try_set_relative<'py>(
    py: Python<'py>,
    sase_home: &str,
    provider: &str,
    source: &str,
    mode: &str,
    duration_seconds: Option<f64>,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let outcome = core_try_set_provider_disable_relative(
        &PathBuf::from(sase_home),
        provider,
        duration_seconds,
        source,
        effort_override_now(now)?,
        parse_provider_disable_mode(mode)?,
    )
    .map_err(provider_disable_error_to_pyerr)?;
    provider_disable_wire_to_py(py, &outcome)
}

#[pyfunction]
#[pyo3(
    name = "provider_disable_try_set_until",
    signature = (sase_home, provider, expires_at, source, mode = "hard", now = None)
)]
fn py_provider_disable_try_set_until<'py>(
    py: Python<'py>,
    sase_home: &str,
    provider: &str,
    expires_at: f64,
    source: &str,
    mode: &str,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let outcome = core_try_set_provider_disable_until(
        &PathBuf::from(sase_home),
        provider,
        expires_at,
        source,
        effort_override_now(now)?,
        parse_provider_disable_mode(mode)?,
    )
    .map_err(provider_disable_error_to_pyerr)?;
    provider_disable_wire_to_py(py, &outcome)
}

#[pyfunction]
#[pyo3(name = "provider_disable_clear")]
fn py_provider_disable_clear(
    sase_home: &str,
    provider: &str,
) -> PyResult<bool> {
    core_clear_provider_disable(&PathBuf::from(sase_home), provider)
        .map_err(provider_disable_error_to_pyerr)
}

// --- Temporary LLM provider priority ----------------------------------
fn provider_priority_error_to_pyerr(err: ProviderPriorityDomainError) -> PyErr {
    match err {
        ProviderPriorityDomainError::Validation(message) => {
            PyValueError::new_err(message)
        }
        ProviderPriorityDomainError::LockTimeout
        | ProviderPriorityDomainError::Io(_)
        | ProviderPriorityDomainError::Json(_) => {
            PyRuntimeError::new_err(err.to_string())
        }
    }
}

fn provider_priority_wire_to_py<'py, T: serde::Serialize>(
    py: Python<'py>,
    value: &T,
) -> PyResult<PyObject> {
    let json = serde_json::to_value(value).map_err(|error| {
        PyRuntimeError::new_err(format!(
            "internal provider-priority serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &json)
}

pub(crate) fn provider_priority_dict_from_py<T>(
    value: &Bound<'_, PyAny>,
    label: &str,
) -> PyResult<T>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(py_to_json_value(value)?).map_err(|error| {
        PyValueError::new_err(format!("{label} is not a valid dict: {error}"))
    })
}

fn provider_priority_optional_record_from_py(
    expected: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<ProviderPriorityWire>> {
    let Some(value) = expected else {
        return Ok(None);
    };
    if value.is_none() {
        return Ok(None);
    }
    provider_priority_dict_from_py(value, "expected priority").map(Some)
}

#[pyfunction]
#[pyo3(name = "provider_priority_wire_schema_version")]
fn py_provider_priority_wire_schema_version() -> u32 {
    sase_core::PROVIDER_PRIORITY_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "provider_routing_context_wire_schema_version")]
fn py_provider_routing_context_wire_schema_version() -> u32 {
    sase_core::PROVIDER_ROUTING_CONTEXT_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "provider_availability_wire_schema_version")]
fn py_provider_availability_wire_schema_version() -> u32 {
    sase_core::PROVIDER_AVAILABILITY_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "provider_priority_get", signature = (sase_home, now = None))]
fn py_provider_priority_get<'py>(
    py: Python<'py>,
    sase_home: &str,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let priority = core_get_provider_priority(
        &PathBuf::from(sase_home),
        effort_override_now(now)?,
    )
    .map_err(provider_priority_error_to_pyerr)?;
    provider_priority_wire_to_py(py, &priority)
}

#[pyfunction]
#[pyo3(name = "provider_priority_peek", signature = (sase_home, now = None))]
fn py_provider_priority_peek<'py>(
    py: Python<'py>,
    sase_home: &str,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let decoded = core_peek_provider_priority(
        &PathBuf::from(sase_home),
        effort_override_now(now)?,
    )
    .map_err(provider_priority_error_to_pyerr)?;
    provider_priority_wire_to_py(py, &decoded)
}

#[pyfunction]
#[pyo3(name = "provider_priority_decode", signature = (data, now = None))]
fn py_provider_priority_decode<'py>(
    py: Python<'py>,
    data: Option<&Bound<'py, PyBytes>>,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let decoded = core_decode_provider_priority_bytes(
        data.map(|value| value.as_bytes()),
        effort_override_now(now)?,
    )
    .map_err(provider_priority_error_to_pyerr)?;
    provider_priority_wire_to_py(py, &decoded)
}

#[pyfunction]
#[pyo3(
    name = "provider_priority_set_relative",
    signature = (
        sase_home,
        provider,
        source,
        facts,
        expected = None,
        duration_seconds = None,
        now = None
    )
)]
#[allow(clippy::too_many_arguments)]
fn py_provider_priority_set_relative<'py>(
    py: Python<'py>,
    sase_home: &str,
    provider: &str,
    source: &str,
    facts: &Bound<'_, PyDict>,
    expected: Option<&Bound<'_, PyAny>>,
    duration_seconds: Option<f64>,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let facts: ProviderPriorityTargetFactsWire =
        provider_priority_dict_from_py(facts.as_any(), "facts")?;
    let expected = provider_priority_optional_record_from_py(expected)?;
    let outcome = core_set_provider_priority_relative(
        &PathBuf::from(sase_home),
        provider,
        duration_seconds,
        source,
        &facts,
        expected.as_ref(),
        effort_override_now(now)?,
    )
    .map_err(provider_priority_error_to_pyerr)?;
    provider_priority_wire_to_py(py, &outcome)
}

#[pyfunction]
#[pyo3(
    name = "provider_priority_set_until",
    signature = (sase_home, provider, expires_at, source, facts, expected = None, now = None)
)]
#[allow(clippy::too_many_arguments)]
fn py_provider_priority_set_until<'py>(
    py: Python<'py>,
    sase_home: &str,
    provider: &str,
    expires_at: f64,
    source: &str,
    facts: &Bound<'_, PyDict>,
    expected: Option<&Bound<'_, PyAny>>,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let facts: ProviderPriorityTargetFactsWire =
        provider_priority_dict_from_py(facts.as_any(), "facts")?;
    let expected = provider_priority_optional_record_from_py(expected)?;
    let outcome = core_set_provider_priority_until(
        &PathBuf::from(sase_home),
        provider,
        expires_at,
        source,
        &facts,
        expected.as_ref(),
        effort_override_now(now)?,
    )
    .map_err(provider_priority_error_to_pyerr)?;
    provider_priority_wire_to_py(py, &outcome)
}

#[pyfunction]
#[pyo3(name = "provider_priority_clear", signature = (sase_home, expected = None, now = None))]
fn py_provider_priority_clear<'py>(
    py: Python<'py>,
    sase_home: &str,
    expected: Option<&Bound<'_, PyAny>>,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let expected = provider_priority_optional_record_from_py(expected)?;
    let outcome = core_clear_provider_priority(
        &PathBuf::from(sase_home),
        expected.as_ref(),
        effort_override_now(now)?,
    )
    .map_err(provider_priority_error_to_pyerr)?;
    provider_priority_wire_to_py(py, &outcome)
}

#[pyfunction]
#[pyo3(name = "provider_routing_context_get", signature = (sase_home, now = None))]
fn py_provider_routing_context_get<'py>(
    py: Python<'py>,
    sase_home: &str,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let context = core_get_provider_routing_context(
        &PathBuf::from(sase_home),
        effort_override_now(now)?,
    )
    .map_err(provider_priority_error_to_pyerr)?;
    provider_priority_wire_to_py(py, &context)
}

#[pyfunction]
fn provider_routing_context_from_parts<'py>(
    py: Python<'py>,
    disables: &Bound<'_, PyList>,
    priority: &Bound<'_, PyAny>,
    captured_at: f64,
) -> PyResult<PyObject> {
    let disables =
        provider_priority_dict_from_py(disables.as_any(), "disables")?;
    let priority = provider_priority_optional_record_from_py(Some(priority))?;
    let context = core_provider_routing_context_from_parts(
        disables,
        priority,
        captured_at,
    )
    .map_err(provider_priority_error_to_pyerr)?;
    provider_priority_wire_to_py(py, &context)
}

#[pyfunction]
fn provider_availability_classify<'py>(
    py: Python<'py>,
    context: &Bound<'_, PyDict>,
    facts: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let context: ProviderRoutingContextWire =
        provider_priority_dict_from_py(context.as_any(), "context")?;
    let facts: ProviderAvailabilityFactsWire =
        provider_priority_dict_from_py(facts.as_any(), "facts")?;
    let availability = core_classify_provider_availability(&context, &facts)
        .map_err(provider_priority_error_to_pyerr)?;
    provider_priority_wire_to_py(py, &availability)
}

#[pyfunction]
fn provider_availability_classify_many<'py>(
    py: Python<'py>,
    context: &Bound<'_, PyDict>,
    facts: &Bound<'_, PyList>,
) -> PyResult<PyObject> {
    let context: ProviderRoutingContextWire =
        provider_priority_dict_from_py(context.as_any(), "context")?;
    let facts: Vec<ProviderAvailabilityFactsWire> =
        provider_priority_dict_from_py(facts.as_any(), "facts")?;
    let availability =
        core_classify_provider_availability_many(&context, &facts)
            .map_err(provider_priority_error_to_pyerr)?;
    provider_priority_wire_to_py(py, &availability)
}

#[pyfunction]
fn provider_pool_eligibility_mask<'py>(
    py: Python<'py>,
    records: &Bound<'_, PyList>,
) -> PyResult<PyObject> {
    let records: Vec<ProviderAvailabilityWire> =
        provider_priority_dict_from_py(records.as_any(), "records")?;
    let mask = core_pool_eligibility_mask(&records)
        .map_err(provider_priority_error_to_pyerr)?;
    provider_priority_wire_to_py(py, &mask)
}

#[pyfunction]
fn provider_pool_reservation_eligible(
    records: &Bound<'_, PyList>,
    reserved_index: i64,
) -> PyResult<bool> {
    if reserved_index < 0 {
        return Err(PyValueError::new_err(
            "reserved member index must be non-negative",
        ));
    }
    let records: Vec<ProviderAvailabilityWire> =
        provider_priority_dict_from_py(records.as_any(), "records")?;
    core_pool_reservation_eligible(&records, reserved_index as usize)
        .map_err(provider_priority_error_to_pyerr)
}

fn provider_usage_error_to_pyerr(err: ProviderUsageDomainError) -> PyErr {
    PyValueError::new_err(err.to_string())
}

fn provider_usage_store_error_to_pyerr(
    err: ProviderUsageStoreDomainError,
) -> PyErr {
    let message = err.to_string();
    match err {
        ProviderUsageStoreDomainError::Validation(_) => {
            PyValueError::new_err(message)
        }
        ProviderUsageStoreDomainError::LockTimeout(_) => {
            PyTimeoutError::new_err(message)
        }
        ProviderUsageStoreDomainError::Io(_)
        | ProviderUsageStoreDomainError::Json(_) => {
            PyRuntimeError::new_err(message)
        }
    }
}

#[pyfunction]
#[pyo3(name = "provider_usage_observation_schema_version")]
fn py_provider_usage_observation_schema_version() -> u32 {
    PROVIDER_USAGE_OBSERVATION_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "provider_usage_public_schema_version")]
fn py_provider_usage_public_schema_version() -> u32 {
    PROVIDER_USAGE_PUBLIC_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "provider_usage_indicator_schema_version")]
fn py_provider_usage_indicator_schema_version() -> u32 {
    PROVIDER_USAGE_INDICATOR_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "provider_usage_store_schema_version")]
fn py_provider_usage_store_schema_version() -> u32 {
    PROVIDER_USAGE_STORE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "provider_usage_collector_failing_threshold")]
fn py_provider_usage_collector_failing_threshold() -> u32 {
    USAGE_COLLECTOR_FAILING_THRESHOLD
}

#[pyfunction]
#[pyo3(name = "provider_usage_state_path")]
fn py_provider_usage_state_path(sase_home: &str) -> String {
    core_provider_usage_state_path(&PathBuf::from(sase_home))
        .to_string_lossy()
        .into_owned()
}

#[pyfunction]
#[pyo3(
    name = "provider_usage_load",
    signature = (
        sase_home,
        now,
        cadence_seconds = DEFAULT_USAGE_CADENCE_SECONDS,
        warn_percent = DEFAULT_USAGE_WARN_PERCENT,
        critical_percent = DEFAULT_USAGE_CRITICAL_PERCENT,
    )
)]
fn py_provider_usage_load<'py>(
    py: Python<'py>,
    sase_home: &str,
    now: f64,
    cadence_seconds: f64,
    warn_percent: f64,
    critical_percent: f64,
) -> PyResult<PyObject> {
    let home = PathBuf::from(sase_home);
    let read = py
        .allow_threads(|| {
            core_load_provider_usage_store(
                &home,
                now,
                cadence_seconds,
                warn_percent,
                critical_percent,
            )
        })
        .map_err(provider_usage_store_error_to_pyerr)?;
    serialize_to_py(py, &read)
}

#[pyfunction]
#[pyo3(name = "provider_usage_record_observation")]
fn py_provider_usage_record_observation<'py>(
    py: Python<'py>,
    sase_home: &str,
    observation: &Bound<'_, PyDict>,
    now: f64,
) -> PyResult<PyObject> {
    let observation: ProviderUsageObservationWire =
        provider_priority_dict_from_py(observation.as_any(), "observation")?;
    let home = PathBuf::from(sase_home);
    let outcome = py
        .allow_threads(|| {
            core_record_provider_usage_observation(&home, observation, now)
        })
        .map_err(provider_usage_store_error_to_pyerr)?;
    serialize_to_py(py, &outcome)
}

#[pyfunction]
#[pyo3(name = "provider_usage_prepare_account_context")]
fn py_provider_usage_prepare_account_context<'py>(
    py: Python<'py>,
    sase_home: &str,
    provider: &str,
    context_id: &str,
    now: f64,
) -> PyResult<PyObject> {
    let home = PathBuf::from(sase_home);
    let context = py
        .allow_threads(|| {
            core_prepare_provider_usage_account_context(
                &home, provider, context_id, now,
            )
        })
        .map_err(provider_usage_store_error_to_pyerr)?;
    serialize_to_py(py, &context)
}

#[pyfunction]
#[pyo3(name = "provider_usage_reserve_refresh")]
fn py_provider_usage_reserve_refresh<'py>(
    py: Python<'py>,
    sase_home: &str,
    request: &Bound<'_, PyDict>,
    now: f64,
) -> PyResult<PyObject> {
    let request: ProviderUsageRefreshReservationRequestWire =
        provider_priority_dict_from_py(request.as_any(), "request")?;
    let home = PathBuf::from(sase_home);
    let outcome = py
        .allow_threads(|| {
            core_reserve_provider_usage_refresh(&home, request, now)
        })
        .map_err(provider_usage_store_error_to_pyerr)?;
    serialize_to_py(py, &outcome)
}

#[pyfunction]
#[pyo3(name = "provider_usage_release_refresh")]
fn py_provider_usage_release_refresh(
    sase_home: &str,
    provider: &str,
    context_id: &str,
    account_generation: u64,
    lease_id: &str,
    now: f64,
) -> PyResult<bool> {
    core_release_provider_usage_refresh(
        &PathBuf::from(sase_home),
        provider,
        context_id,
        account_generation,
        lease_id,
        now,
    )
    .map_err(provider_usage_store_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "provider_usage_refresh_due")]
fn py_provider_usage_refresh_due<'py>(
    py: Python<'py>,
    sase_home: &str,
    request: &Bound<'_, PyDict>,
    now: f64,
) -> PyResult<PyObject> {
    let request: ProviderUsageRefreshDueRequestWire =
        provider_priority_dict_from_py(request.as_any(), "request")?;
    let home = PathBuf::from(sase_home);
    let outcome = py
        .allow_threads(|| {
            core_evaluate_provider_usage_refresh_due(&home, request, now)
        })
        .map_err(provider_usage_store_error_to_pyerr)?;
    serialize_to_py(py, &outcome)
}

#[pyfunction]
#[pyo3(name = "provider_usage_admit_refresh")]
fn py_provider_usage_admit_refresh<'py>(
    py: Python<'py>,
    sase_home: &str,
    request: &Bound<'_, PyDict>,
    now: f64,
) -> PyResult<PyObject> {
    let request: ProviderUsageRefreshAdmitRequestWire =
        provider_priority_dict_from_py(request.as_any(), "request")?;
    let home = PathBuf::from(sase_home);
    let outcome = py
        .allow_threads(|| {
            core_admit_provider_usage_refresh(&home, request, now)
        })
        .map_err(provider_usage_store_error_to_pyerr)?;
    serialize_to_py(py, &outcome)
}

#[pyfunction]
#[pyo3(name = "provider_usage_mark_refresh_due")]
fn py_provider_usage_mark_refresh_due<'py>(
    py: Python<'py>,
    sase_home: &str,
    request: &Bound<'_, PyDict>,
    now: f64,
) -> PyResult<PyObject> {
    let request: ProviderUsageRefreshMarkDueRequestWire =
        provider_priority_dict_from_py(request.as_any(), "request")?;
    let home = PathBuf::from(sase_home);
    let outcome = py
        .allow_threads(|| {
            core_mark_provider_usage_refresh_due(&home, request, now)
        })
        .map_err(provider_usage_store_error_to_pyerr)?;
    serialize_to_py(py, &outcome)
}

#[pyfunction]
#[pyo3(name = "provider_usage_record_refresh_attempt")]
fn py_provider_usage_record_refresh_attempt<'py>(
    py: Python<'py>,
    sase_home: &str,
    request: &Bound<'_, PyDict>,
    now: f64,
) -> PyResult<PyObject> {
    let request: ProviderUsageRefreshAttemptWire =
        provider_priority_dict_from_py(request.as_any(), "request")?;
    let home = PathBuf::from(sase_home);
    let outcome = py
        .allow_threads(|| {
            core_record_provider_usage_refresh_attempt(&home, request, now)
        })
        .map_err(provider_usage_store_error_to_pyerr)?;
    serialize_to_py(py, &outcome)
}

#[pyfunction]
#[pyo3(name = "provider_usage_normalize_grok_billing")]
fn py_provider_usage_normalize_grok_billing<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request: ProviderUsageNormalizeGrokBillingRequestWire =
        provider_priority_dict_from_py(request.as_any(), "request")?;
    let observation = core_normalize_grok_billing(request)
        .map_err(provider_usage_error_to_pyerr)?;
    serialize_to_py(py, &observation)
}

#[pyfunction]
#[pyo3(name = "provider_usage_normalize_muse_usage")]
fn py_provider_usage_normalize_muse_usage<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request: ProviderUsageNormalizeMuseUsageRequestWire =
        provider_priority_dict_from_py(request.as_any(), "request")?;
    let observation = core_normalize_muse_usage(request)
        .map_err(provider_usage_error_to_pyerr)?;
    serialize_to_py(py, &observation)
}

#[pyfunction]
#[pyo3(name = "provider_usage_validate_observation")]
fn py_provider_usage_validate_observation<'py>(
    py: Python<'py>,
    observation: &Bound<'_, PyDict>,
    now: f64,
) -> PyResult<PyObject> {
    let observation: ProviderUsageObservationWire =
        provider_priority_dict_from_py(observation.as_any(), "observation")?;
    let validated = core_validate_usage_observation(observation, now)
        .map_err(provider_usage_error_to_pyerr)?;
    serialize_to_py(py, &validated)
}

#[pyfunction]
#[pyo3(
    name = "provider_usage_project_snapshot",
    signature = (
        observations,
        now,
        cadence_seconds = DEFAULT_USAGE_CADENCE_SECONDS,
        warn_percent = DEFAULT_USAGE_WARN_PERCENT,
        critical_percent = DEFAULT_USAGE_CRITICAL_PERCENT,
    )
)]
fn py_provider_usage_project_snapshot<'py>(
    py: Python<'py>,
    observations: &Bound<'_, PyList>,
    now: f64,
    cadence_seconds: f64,
    warn_percent: f64,
    critical_percent: f64,
) -> PyResult<PyObject> {
    let observations: Vec<ProviderUsageObservationWire> =
        provider_priority_dict_from_py(observations.as_any(), "observations")?;
    let snapshot = core_project_usage_snapshot(
        &observations,
        now,
        cadence_seconds,
        warn_percent,
        critical_percent,
    )
    .map_err(provider_usage_error_to_pyerr)?;
    serialize_to_py(py, &snapshot)
}

#[pyfunction]
#[pyo3(name = "provider_usage_validate_indicator_config")]
#[pyo3(signature = (indicator = None))]
fn py_provider_usage_validate_indicator_config<'py>(
    py: Python<'py>,
    indicator: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyObject> {
    let raw = match indicator {
        Some(value) if !value.is_none() => Some(py_to_json_value(value)?),
        _ => None,
    };
    let validation = core_validate_usage_indicator_config(raw);
    serialize_to_py(py, &validation)
}

#[pyfunction]
#[pyo3(name = "provider_usage_project_indicator")]
fn py_provider_usage_project_indicator<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let request: UsageIndicatorProjectionRequestWire =
        provider_priority_dict_from_py(request.as_any(), "request")?;
    let projection = core_project_usage_indicator(request)
        .map_err(provider_usage_error_to_pyerr)?;
    serialize_to_py(py, &projection)
}

#[pyfunction]
#[pyo3(name = "provider_usage_remaining_percent")]
fn py_provider_usage_remaining_percent(used_percent: f64) -> PyResult<f64> {
    core_remaining_percent(used_percent).map_err(provider_usage_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "provider_usage_format_remaining_text")]
fn py_provider_usage_format_remaining_text(
    used_percent: f64,
) -> PyResult<String> {
    core_format_remaining_text(used_percent)
        .map_err(provider_usage_error_to_pyerr)
}

#[pyfunction]
#[pyo3(
    name = "provider_usage_classify_freshness",
    signature = (observed_at, now, cadence_seconds = DEFAULT_USAGE_CADENCE_SECONDS)
)]
fn py_provider_usage_classify_freshness(
    observed_at: f64,
    now: f64,
    cadence_seconds: f64,
) -> PyResult<&'static str> {
    Ok(core_classify_freshness(observed_at, now, cadence_seconds)
        .map_err(provider_usage_error_to_pyerr)?
        .as_str())
}

#[pyfunction]
#[pyo3(
    name = "provider_usage_window_applies",
    signature = (applicability, model_id = None)
)]
fn py_provider_usage_window_applies(
    applicability: &Bound<'_, PyDict>,
    model_id: Option<&str>,
) -> PyResult<&'static str> {
    let applicability: UsageApplicabilityWire = provider_priority_dict_from_py(
        applicability.as_any(),
        "applicability",
    )?;
    Ok(core_usage_window_applies(&applicability, model_id).as_str())
}

#[pyfunction]
#[pyo3(name = "provider_usage_summarize_for_model")]
fn py_provider_usage_summarize_for_model<'py>(
    py: Python<'py>,
    windows: &Bound<'_, PyList>,
    model_id: &str,
) -> PyResult<PyObject> {
    let windows: Vec<UsagePublicWindowWire> =
        provider_priority_dict_from_py(windows.as_any(), "windows")?;
    let summary = core_summarize_usage_windows(&windows, Some(model_id));
    serialize_to_py(py, &summary)
}

#[pyfunction]
#[pyo3(
    name = "resolve_effective_effort",
    signature = (
        explicit_effort = None,
        alias_effort = None,
        temporary_effort = None,
        configured_effort = None
    )
)]
fn py_resolve_effective_effort<'py>(
    py: Python<'py>,
    explicit_effort: Option<&str>,
    alias_effort: Option<&str>,
    temporary_effort: Option<&str>,
    configured_effort: Option<&str>,
) -> PyResult<PyObject> {
    effort_wire_to_py(
        py,
        &core_resolve_effective_effort(
            explicit_effort,
            alias_effort,
            temporary_effort,
            configured_effort,
        ),
    )
}

fn model_route_error_to_pyerr(err: ModelRouteDomainError) -> PyErr {
    PyValueError::new_err(err.to_string())
}

fn py_model_route_int(value: &Bound<'_, PyAny>, name: &str) -> PyResult<i64> {
    if value.is_instance_of::<pyo3::types::PyBool>() {
        return Err(PyValueError::new_err(format!(
            "{name} must be an integer, not a boolean"
        )));
    }
    value.extract::<i64>().map_err(|_| {
        PyValueError::new_err(format!("{name} must be an integer"))
    })
}

#[pyfunction]
#[pyo3(name = "size_model_route")]
fn py_size_model_route<'py>(py: Python<'py>, size: &str) -> PyResult<PyObject> {
    effort_wire_to_py(
        py,
        &core_size_model_route_from_name(size)
            .map_err(model_route_error_to_pyerr)?,
    )
}

#[pyfunction]
#[pyo3(
    name = "select_epic_land_model",
    signature = (
        explicit_model,
        phase_count,
        threshold,
        epic_lander_model,
        big_epic_lander_model
    )
)]
fn py_select_epic_land_model<'py>(
    py: Python<'py>,
    explicit_model: Option<&str>,
    phase_count: Bound<'py, PyAny>,
    threshold: Bound<'py, PyAny>,
    epic_lander_model: &str,
    big_epic_lander_model: &str,
) -> PyResult<PyObject> {
    let phase_count = py_model_route_int(&phase_count, "phase_count")?;
    let threshold = py_model_route_int(&threshold, "threshold")?;
    effort_wire_to_py(
        py,
        &core_select_epic_land_model(
            explicit_model,
            phase_count,
            threshold,
            epic_lander_model,
            big_epic_lander_model,
        )
        .map_err(model_route_error_to_pyerr)?,
    )
}

#[pyfunction]
#[pyo3(name = "runner_capacity_policy_schema_version")]
pub(crate) fn py_runner_capacity_policy_schema_version() -> u32 {
    core_runner_capacity_policy_schema_version()
}

#[pyfunction]
#[pyo3(name = "runner_capacity_snapshot")]
pub(crate) fn py_runner_capacity_snapshot<'py>(
    py: Python<'py>,
    request: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let mut value = py_to_json_value(request)?;
    sase_core::merge_queue_capacity_aliases_in_capacity_request(&mut value);
    let request: RunnerCapacityRequestWire = serde_json::from_value(value)
        .map_err(|err| {
            PyValueError::new_err(format!(
                "invalid runner capacity request: {err}"
            ))
        })?;
    let snapshot = core_runner_capacity_snapshot(&request);
    let value = serde_json::to_value(&snapshot).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

pub(crate) fn register_provider_policy(
    m: &Bound<'_, PyModule>,
) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        py_runner_capacity_policy_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_runner_capacity_snapshot, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_effort_override_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_effort_override_get, m)?)?;
    m.add_function(wrap_pyfunction!(py_effort_override_set_relative, m)?)?;
    m.add_function(wrap_pyfunction!(py_effort_override_set_until, m)?)?;
    m.add_function(wrap_pyfunction!(py_effort_override_clear, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_runner_limit_override_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_runner_limit_override_get, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_runner_limit_override_set_relative,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_runner_limit_override_set_until, m)?)?;
    m.add_function(wrap_pyfunction!(py_runner_limit_override_clear, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_provider_disable_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_provider_disable_get, m)?)?;
    m.add_function(wrap_pyfunction!(py_provider_disable_set_relative, m)?)?;
    m.add_function(wrap_pyfunction!(py_provider_disable_set_until, m)?)?;
    m.add_function(wrap_pyfunction!(py_provider_disable_try_set_relative, m)?)?;
    m.add_function(wrap_pyfunction!(py_provider_disable_try_set_until, m)?)?;
    m.add_function(wrap_pyfunction!(py_provider_disable_clear, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_provider_priority_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_provider_routing_context_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_provider_availability_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_provider_priority_get, m)?)?;
    m.add_function(wrap_pyfunction!(py_provider_priority_peek, m)?)?;
    m.add_function(wrap_pyfunction!(py_provider_priority_decode, m)?)?;
    m.add_function(wrap_pyfunction!(py_provider_priority_set_relative, m)?)?;
    m.add_function(wrap_pyfunction!(py_provider_priority_set_until, m)?)?;
    m.add_function(wrap_pyfunction!(py_provider_priority_clear, m)?)?;
    m.add_function(wrap_pyfunction!(py_provider_routing_context_get, m)?)?;
    m.add_function(wrap_pyfunction!(provider_routing_context_from_parts, m)?)?;
    m.add_function(wrap_pyfunction!(provider_availability_classify, m)?)?;
    m.add_function(wrap_pyfunction!(provider_availability_classify_many, m)?)?;
    m.add_function(wrap_pyfunction!(provider_pool_eligibility_mask, m)?)?;
    m.add_function(wrap_pyfunction!(provider_pool_reservation_eligible, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_provider_usage_observation_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_provider_usage_public_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_provider_usage_indicator_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_provider_usage_store_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_provider_usage_collector_failing_threshold,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_provider_usage_state_path, m)?)?;
    m.add_function(wrap_pyfunction!(py_provider_usage_load, m)?)?;
    m.add_function(wrap_pyfunction!(py_provider_usage_record_observation, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_provider_usage_prepare_account_context,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_provider_usage_reserve_refresh, m)?)?;
    m.add_function(wrap_pyfunction!(py_provider_usage_release_refresh, m)?)?;
    m.add_function(wrap_pyfunction!(py_provider_usage_refresh_due, m)?)?;
    m.add_function(wrap_pyfunction!(py_provider_usage_admit_refresh, m)?)?;
    m.add_function(wrap_pyfunction!(py_provider_usage_mark_refresh_due, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_provider_usage_record_refresh_attempt,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_provider_usage_normalize_grok_billing,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_provider_usage_normalize_muse_usage,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_provider_usage_validate_observation,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_provider_usage_project_snapshot, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_provider_usage_validate_indicator_config,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_provider_usage_project_indicator, m)?)?;
    m.add_function(wrap_pyfunction!(py_provider_usage_remaining_percent, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_provider_usage_format_remaining_text,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_provider_usage_classify_freshness, m)?)?;
    m.add_function(wrap_pyfunction!(py_provider_usage_window_applies, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_provider_usage_summarize_for_model,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_resolve_effective_effort, m)?)?;
    m.add_function(wrap_pyfunction!(py_size_model_route, m)?)?;
    m.add_function(wrap_pyfunction!(py_select_epic_land_model, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
