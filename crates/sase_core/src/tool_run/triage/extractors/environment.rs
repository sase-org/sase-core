//! `_setup` environment markers.

use super::raw::RawItem;

const MARKERS: &[(&str, &[&str], &str)] = &[
    (
        "missing_binding",
        &["missing required binding(s)", "does not expose binding"],
        "just install",
    ),
    (
        "core_import",
        &[
            "cannot import sase_core_rs",
            "No module named 'sase_core_rs'",
        ],
        "just install",
    ),
    (
        "stale_core",
        &["[setup] ERROR: the sase-core checkout is behind"],
        "sase update",
    ),
    (
        "core_wheel",
        &["SASE_CORE_WHEEL does not name a wheel file"],
        "just install",
    ),
    (
        "required_plugins",
        &["[setup] error: could not read plugins.required"],
        "sase update",
    ),
    (
        "keep_sorted_missing",
        &["error: keep-sorted is required"],
        "just install",
    ),
];

pub fn extract_environment(lines: &[String]) -> Vec<RawItem> {
    let mut items = Vec::new();
    for line in lines {
        for (kind, needles, remedy) in MARKERS {
            if needles.iter().any(|needle| line.contains(*needle)) {
                items.push(RawItem::new(
                    (*kind).to_string(),
                    &format!(
                        "environment: {kind} \u{2014} run {remedy}\n{line}"
                    ),
                    Vec::new(),
                ));
                break;
            }
        }
    }
    // One item per marker kind; collapse duplicates preserving first display.
    let mut seen = std::collections::HashSet::new();
    let mut deduped = Vec::new();
    for item in items {
        if seen.insert(item.key.clone()) {
            deduped.push(item);
        }
    }
    deduped
}

/// Remedy hint for a marker kind, exported for tests.
#[allow(dead_code)]
pub fn remedy_for(kind: &str) -> &'static str {
    match kind {
        "missing_binding"
        | "core_import"
        | "core_wheel"
        | "keep_sorted_missing" => "just install",
        _ => "sase update",
    }
}
