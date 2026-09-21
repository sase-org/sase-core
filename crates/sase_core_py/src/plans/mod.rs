//! Plan search, validation, reference, and SDD surface bindings.

use crate::prelude::*;

use crate::json_bridge::{json_value_to_py, py_to_json_value};

use pyo3::wrap_pyfunction;

/// Search and rank markdown plan artifacts under a repo `sdd/` tree and/or the
/// machine-local archive.
///
/// `repo_sdd_root`/`local_plans_dir` are passed through as `Option`s so callers
/// scope by `--source` (pass `None` to skip a corpus). The remaining arguments
/// mirror [`core_plan_search`]: optional `query` (browse when omitted), repo
/// `kinds`, frontmatter `statuses`, a `sources` filter, a `[since, until]` date
/// range, `sort` mode, and `limit` (`0`/`None` = unlimited).
/// `document_corpora`, when supplied, replaces the legacy repo-root scan with
/// explicit `(root, kind)` pairs. Returns a list of
/// `{plan, matched_fields, score}` dicts, following `bead_search`'s JSON shape.
#[pyfunction]
#[pyo3(name = "plan_search")]
#[pyo3(signature = (repo_sdd_root=None, local_plans_dir=None, query=None, kinds=None, statuses=None, sources=None, since=None, until=None, sort=None, limit=None, document_corpora=None))]
#[allow(clippy::too_many_arguments)]
fn py_plan_search<'py>(
    py: Python<'py>,
    repo_sdd_root: Option<String>,
    local_plans_dir: Option<String>,
    query: Option<String>,
    kinds: Option<Vec<String>>,
    statuses: Option<Vec<String>>,
    sources: Option<Vec<String>>,
    since: Option<String>,
    until: Option<String>,
    sort: Option<String>,
    limit: Option<usize>,
    document_corpora: Option<Vec<(String, String)>>,
) -> PyResult<PyObject> {
    let repo_sdd_root = repo_sdd_root.map(PathBuf::from);
    let local_plans_dir = local_plans_dir.map(PathBuf::from);
    let document_corpora = document_corpora.map(|corpora| {
        corpora
            .into_iter()
            .map(|(root, kind)| (PathBuf::from(root), kind))
            .collect::<Vec<_>>()
    });
    plan_result_to_py(
        py,
        py.allow_threads(|| {
            core_plan_search(
                repo_sdd_root.as_deref(),
                local_plans_dir.as_deref(),
                query.as_deref(),
                kinds.as_deref(),
                statuses.as_deref(),
                sources.as_deref(),
                since.as_deref(),
                until.as_deref(),
                sort.as_deref(),
                limit,
                document_corpora.as_deref(),
            )
        }),
    )
}

/// Strictly validate one complete markdown plan against an explicit tier.
#[pyfunction]
#[pyo3(name = "plan_validate")]
#[pyo3(signature = (content, tier, mode = "authoring"))]
fn py_plan_validate<'py>(
    py: Python<'py>,
    content: &str,
    tier: &str,
    mode: &str,
) -> PyResult<PyObject> {
    plan_result_to_py(
        py,
        py.allow_threads(|| core_plan_validate_with_mode(content, tier, mode)),
    )
}

/// Return ordered authoritative frontmatter field metadata for a plan tier.
#[pyfunction]
#[pyo3(name = "plan_frontmatter_schema")]
fn py_plan_frontmatter_schema<'py>(
    py: Python<'py>,
    tier: &str,
) -> PyResult<PyObject> {
    plan_result_to_py(
        py,
        py.allow_threads(|| core_plan_frontmatter_schema(tier)),
    )
}

/// Parse a canonical plan reference or preserve a legacy path.
#[pyfunction]
#[pyo3(name = "plan_reference_parse")]
fn py_plan_reference_parse<'py>(
    py: Python<'py>,
    value: &str,
) -> PyResult<PyObject> {
    plan_result_to_py(py, core_parse_plan_reference(value))
}

/// Render one validated canonical plan reference.
#[pyfunction]
#[pyo3(name = "plan_reference_render")]
fn py_plan_reference_render(kind: &str, path: &str) -> PyResult<String> {
    core_render_plan_reference(kind, path).map_err(plan_error_to_pyerr)
}

/// Canonicalize a plan path against ordered plan roots.
#[pyfunction]
#[pyo3(name = "plan_reference_canonicalize")]
fn py_plan_reference_canonicalize(
    path: &str,
    roots: Vec<String>,
) -> PyResult<Option<String>> {
    let path = PathBuf::from(path);
    let roots = roots.into_iter().map(PathBuf::from).collect::<Vec<_>>();
    core_canonicalize_plan_reference(&path, &roots).map_err(plan_error_to_pyerr)
}

/// Resolve a canonical or legacy plan reference against ordered roots.
#[pyfunction]
#[pyo3(name = "plan_reference_resolve")]
fn py_plan_reference_resolve<'py>(
    py: Python<'py>,
    value: &str,
    roots: Vec<String>,
) -> PyResult<PyObject> {
    let roots = roots.into_iter().map(PathBuf::from).collect::<Vec<_>>();
    plan_result_to_py(
        py,
        py.allow_threads(|| core_resolve_plan_reference(value, &roots)),
    )
}

/// Return the plan-reference resolution wire schema version.
#[pyfunction]
#[pyo3(name = "plan_reference_resolution_wire_schema_version")]
fn py_plan_reference_resolution_wire_schema_version() -> u64 {
    PLAN_REFERENCE_RESOLUTION_WIRE_SCHEMA_VERSION
}

/// Parse canonical and historical artifact links from one SDD document.
#[pyfunction]
#[pyo3(name = "sdd_artifact_link_parse")]
fn py_sdd_artifact_link_parse<'py>(
    py: Python<'py>,
    document: &str,
) -> PyResult<PyObject> {
    plan_result_to_py(py, Ok(core_parse_sdd_artifact_link(document)))
}

/// Render one canonical typed SDD artifact-link bullet.
#[pyfunction]
#[pyo3(name = "sdd_artifact_link_render")]
fn py_sdd_artifact_link_render(
    link_type: &str,
    label: &str,
    target: &str,
) -> PyResult<String> {
    core_render_sdd_artifact_link(link_type, label, target)
        .map_err(plan_error_to_pyerr)
}

/// Install a canonical artifact link and optionally remove its legacy field.
#[pyfunction]
#[pyo3(name = "sdd_artifact_link_upsert")]
fn py_sdd_artifact_link_upsert(
    document: &str,
    link_type: &str,
    label: &str,
    target: &str,
    remove_legacy: bool,
    allow_resolved_mixed: bool,
) -> PyResult<String> {
    core_upsert_sdd_artifact_link(
        document,
        link_type,
        label,
        target,
        remove_legacy,
        allow_resolved_mixed,
    )
    .map_err(plan_error_to_pyerr)
}

/// Return the plan-header block wire schema version.
#[pyfunction]
#[pyo3(name = "sdd_plan_header_block_wire_schema_version")]
fn py_sdd_plan_header_block_wire_schema_version() -> u64 {
    PLAN_HEADER_BLOCK_WIRE_SCHEMA_VERSION
}

/// Parse a complete SDD document's provenance header block.
#[pyfunction]
#[pyo3(name = "sdd_plan_header_block_parse")]
fn py_sdd_plan_header_block_parse<'py>(
    py: Python<'py>,
    document: &str,
) -> PyResult<PyObject> {
    plan_result_to_py(py, Ok(core_parse_sdd_plan_header_block(document)))
}

/// Render a complete canonical provenance header block.
#[pyfunction]
#[pyo3(name = "sdd_plan_header_block_render")]
fn py_sdd_plan_header_block_render(
    sections: &Bound<'_, PyList>,
) -> PyResult<String> {
    let sections = sdd_plan_header_sections_from_py_list(sections)?;
    core_render_sdd_plan_header_block(&sections).map_err(plan_error_to_pyerr)
}

/// Install or replace one provenance header section.
#[pyfunction]
#[pyo3(name = "sdd_plan_header_block_upsert_section")]
fn py_sdd_plan_header_block_upsert_section(
    document: &str,
    section: &Bound<'_, PyDict>,
    remove_legacy: bool,
    allow_resolved_mixed: bool,
) -> PyResult<String> {
    let section = sdd_plan_header_section_from_pydict(section)?;
    core_upsert_sdd_plan_header_section(
        document,
        section,
        remove_legacy,
        allow_resolved_mixed,
    )
    .map_err(plan_error_to_pyerr)
}

/// Replace the complete provenance header block.
#[pyfunction]
#[pyo3(name = "sdd_plan_header_block_replace")]
fn py_sdd_plan_header_block_replace(
    document: &str,
    sections: &Bound<'_, PyList>,
    remove_legacy: bool,
    allow_resolved_mixed: bool,
) -> PyResult<String> {
    let sections = sdd_plan_header_sections_from_py_list(sections)?;
    core_replace_sdd_plan_header_block(
        document,
        &sections,
        remove_legacy,
        allow_resolved_mixed,
    )
    .map_err(plan_error_to_pyerr)
}

/// Remove one provenance header section.
#[pyfunction]
#[pyo3(name = "sdd_plan_header_block_remove_section")]
fn py_sdd_plan_header_block_remove_section(
    document: &str,
    kind: &str,
    remove_legacy: bool,
    allow_resolved_mixed: bool,
) -> PyResult<String> {
    core_remove_sdd_plan_header_section(
        document,
        kind,
        remove_legacy,
        allow_resolved_mixed,
    )
    .map_err(plan_error_to_pyerr)
}

fn sdd_plan_header_section_from_pydict(
    section: &Bound<'_, PyDict>,
) -> PyResult<SddPlanHeaderSectionWire> {
    let value = py_to_json_value(section.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "invalid plan header section payload: {error}"
        ))
    })
}

fn sdd_plan_header_sections_from_py_list(
    sections: &Bound<'_, PyList>,
) -> PyResult<Vec<SddPlanHeaderSectionWire>> {
    sections
        .iter()
        .map(|section| {
            let value = py_to_json_value(&section)?;
            serde_json::from_value(value).map_err(|error| {
                PyValueError::new_err(format!(
                    "invalid plan header section payload: {error}"
                ))
            })
        })
        .collect()
}

fn plan_result_to_py<'py, T>(
    py: Python<'py>,
    result: Result<T, PlanError>,
) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value = serde_json::to_value(result.map_err(plan_error_to_pyerr)?)
        .map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?;
    json_value_to_py(py, &value)
}

fn plan_error_to_pyerr(err: PlanError) -> PyErr {
    PyValueError::new_err(format!("{err}"))
}

pub(crate) fn register_plans(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_plan_search, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_validate, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_frontmatter_schema, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_reference_parse, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_reference_render, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_reference_canonicalize, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_reference_resolve, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_plan_reference_resolution_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_sdd_artifact_link_parse, m)?)?;
    m.add_function(wrap_pyfunction!(py_sdd_artifact_link_render, m)?)?;
    m.add_function(wrap_pyfunction!(py_sdd_artifact_link_upsert, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_sdd_plan_header_block_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_sdd_plan_header_block_parse, m)?)?;
    m.add_function(wrap_pyfunction!(py_sdd_plan_header_block_render, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_sdd_plan_header_block_upsert_section,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_sdd_plan_header_block_replace, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_sdd_plan_header_block_remove_section,
        m
    )?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
