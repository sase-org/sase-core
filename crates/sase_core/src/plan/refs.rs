//! Stable logical references to plans stored below ordered plan roots.

use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::wire::{
    PlanError, PlanReferenceResolutionWire,
    PLAN_REFERENCE_RESOLUTION_WIRE_SCHEMA_VERSION,
};

const PLAN_REFERENCE_KIND: &str = "plans";
const PLAN_REFERENCE_PREFIX: &str = "plans:";
const LEGACY_PLAN_MARKERS: &[&[&str]] = &[
    &["sase", "repos", "plans"],
    &[".sase", "sdd", "plans"],
    &["sdd", "plans"],
    &["plans"],
];

/// A parsed typed plan reference or an unmodified legacy path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedPlanReference {
    pub kind: String,
    pub path: String,
    pub legacy: bool,
    pub rendered: String,
}

/// Parse a canonical `plans:` reference or preserve an accepted legacy path.
pub fn parse_plan_reference(
    value: &str,
) -> Result<ParsedPlanReference, PlanError> {
    let Some(path) = value.strip_prefix(PLAN_REFERENCE_PREFIX) else {
        return Ok(ParsedPlanReference {
            kind: PLAN_REFERENCE_KIND.to_string(),
            path: value.to_string(),
            legacy: true,
            rendered: value.to_string(),
        });
    };
    validate_reference_path(path)?;
    Ok(ParsedPlanReference {
        kind: PLAN_REFERENCE_KIND.to_string(),
        path: path.to_string(),
        legacy: false,
        rendered: format!("{PLAN_REFERENCE_PREFIX}{path}"),
    })
}

/// Render one validated logical reference.
pub fn render_plan_reference(
    kind: &str,
    path: &str,
) -> Result<String, PlanError> {
    if kind != PLAN_REFERENCE_KIND {
        return Err(PlanError::validation(format!(
            "unsupported plan reference kind: {kind}"
        )));
    }
    validate_reference_path(path)?;
    Ok(format!("{PLAN_REFERENCE_PREFIX}{path}"))
}

/// Convert a path below the first matching plan root into a logical reference.
pub fn canonicalize_plan_reference(
    path: &Path,
    roots: &[PathBuf],
) -> Result<Option<String>, PlanError> {
    for root in roots {
        if let Ok(relative) = path.strip_prefix(root) {
            let payload = path_to_reference_payload(relative)?;
            return render_plan_reference(PLAN_REFERENCE_KIND, &payload)
                .map(Some);
        }
    }
    Ok(None)
}

/// Resolve a logical reference or legacy plan path against ordered roots.
pub fn resolve_plan_reference(
    value: &str,
    roots: &[PathBuf],
) -> Result<PlanReferenceResolutionWire, PlanError> {
    let parsed = parse_plan_reference(value)?;
    let (payload, mut candidates) = if parsed.legacy {
        legacy_payload_and_candidates(&parsed.path)
    } else {
        (PathBuf::from(&parsed.path), Vec::new())
    };

    if parsed.legacy {
        for candidate in &candidates {
            if candidate.is_file() {
                return Ok(resolution(
                    "exact",
                    Some(candidate.clone()),
                    candidates,
                ));
            }
        }
    }

    for root in roots {
        let candidate = root.join(&payload);
        candidates.push(candidate.clone());
        if candidate.is_file() {
            return Ok(resolution("exact", Some(candidate), candidates));
        }
    }

    if let Some(filename) = drift_filename(&payload) {
        let mut drift_matches = Vec::new();
        for root in roots {
            let Ok(month_dirs) = std::fs::read_dir(root) else {
                continue;
            };
            let mut month_dirs = month_dirs
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .filter(|path| path.is_dir())
                .collect::<Vec<_>>();
            month_dirs.sort();
            for month_dir in month_dirs {
                let candidate = month_dir.join(filename);
                if candidate.is_file() {
                    drift_matches.push(candidate);
                }
            }
        }
        if drift_matches.len() == 1 {
            let resolved_path = drift_matches[0].clone();
            return Ok(resolution(
                "drifted",
                Some(resolved_path),
                drift_matches,
            ));
        }
        if drift_matches.len() > 1 {
            return Ok(resolution("ambiguous", None, drift_matches));
        }
    }

    Ok(resolution("missing", None, candidates))
}

fn validate_reference_path(path: &str) -> Result<(), PlanError> {
    if path.is_empty() {
        return Err(PlanError::validation(
            "plan reference path must not be empty",
        ));
    }
    if path.starts_with('/') {
        return Err(PlanError::validation(
            "plan reference path must be relative",
        ));
    }
    if path.contains('\\') {
        return Err(PlanError::validation(
            "plan reference path must use forward slashes",
        ));
    }
    if path.split('/').any(|segment| segment.is_empty()) {
        return Err(PlanError::validation(
            "plan reference path contains an empty segment",
        ));
    }
    if path.split('/').any(|segment| segment == "..") {
        return Err(PlanError::validation(
            "plan reference path must not contain '..'",
        ));
    }
    Ok(())
}

fn path_to_reference_payload(path: &Path) -> Result<String, PlanError> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                let part = part.to_str().ok_or_else(|| {
                    PlanError::validation(
                        "plan reference path must contain valid UTF-8",
                    )
                })?;
                parts.push(part);
            }
            Component::CurDir => {}
            Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(PlanError::validation(
                    "plan reference path escapes its plans root",
                ));
            }
        }
    }
    Ok(parts.join("/"))
}

fn legacy_payload_and_candidates(value: &str) -> (PathBuf, Vec<PathBuf>) {
    let path = PathBuf::from(value);
    let candidates = if path.is_absolute() {
        vec![path.clone()]
    } else {
        Vec::new()
    };
    let parts = path
        .components()
        .filter_map(|component| match component {
            Component::Normal(part) => part.to_str(),
            _ => None,
        })
        .collect::<Vec<_>>();
    for marker in LEGACY_PLAN_MARKERS {
        if let Some(start) = find_subsequence(&parts, marker) {
            let trailing = &parts[start + marker.len()..];
            if !trailing.is_empty() {
                return (trailing.iter().collect(), candidates);
            }
        }
    }
    (path, candidates)
}

fn find_subsequence(haystack: &[&str], needle: &[&str]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn drift_filename(path: &Path) -> Option<&Path> {
    let components = path.components().collect::<Vec<_>>();
    if components.len() != 2 {
        return None;
    }
    let month = match components[0] {
        Component::Normal(month) => month.to_str()?,
        _ => return None,
    };
    if month.len() != 6 || !month.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    match components[1] {
        Component::Normal(filename) => Some(Path::new(filename)),
        _ => None,
    }
}

fn resolution(
    status: &str,
    resolved_path: Option<PathBuf>,
    candidates: Vec<PathBuf>,
) -> PlanReferenceResolutionWire {
    PlanReferenceResolutionWire {
        schema_version: PLAN_REFERENCE_RESOLUTION_WIRE_SCHEMA_VERSION,
        status: status.to_string(),
        resolved_path: resolved_path.map(|path| path.to_string_lossy().into()),
        candidates: candidates
            .into_iter()
            .map(|path| path.to_string_lossy().into())
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn typed_reference_round_trips() {
        let rendered =
            render_plan_reference("plans", "202607/durable.md").unwrap();
        assert_eq!(rendered, "plans:202607/durable.md");
        assert_eq!(
            parse_plan_reference(&rendered).unwrap(),
            ParsedPlanReference {
                kind: "plans".to_string(),
                path: "202607/durable.md".to_string(),
                legacy: false,
                rendered,
            }
        );
    }

    #[test]
    fn typed_reference_rejects_unsafe_payloads_and_unknown_kinds() {
        for value in [
            "plans:",
            "plans:/tmp/plan.md",
            "plans:202607//plan.md",
            "plans:202607/../plan.md",
            r"plans:202607\plan.md",
        ] {
            assert!(parse_plan_reference(value).is_err(), "{value}");
        }
        assert!(render_plan_reference("research", "202607/plan.md").is_err());
    }

    #[test]
    fn unregistered_schemes_and_paths_remain_legacy() {
        for value in [
            "research:202607/plan.md",
            "sdd/plans/202607/plan.md",
            r"old\plan.md",
        ] {
            let parsed = parse_plan_reference(value).unwrap();
            assert!(parsed.legacy);
            assert_eq!(parsed.path, value);
            assert_eq!(parsed.rendered, value);
        }
    }

    #[test]
    fn canonicalize_uses_first_matching_root() {
        let temp = tempdir().unwrap();
        let outer = temp.path().join("plans");
        let inner = outer.join("202607");
        let path = inner.join("plan.md");
        assert_eq!(
            canonicalize_plan_reference(&path, &[outer.clone(), inner.clone()])
                .unwrap()
                .as_deref(),
            Some("plans:202607/plan.md")
        );
        assert_eq!(
            canonicalize_plan_reference(
                &temp.path().join("elsewhere/plan.md"),
                &[outer]
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn exact_resolution_uses_first_root() {
        let temp = tempdir().unwrap();
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        for root in [&first, &second] {
            fs::create_dir_all(root.join("202607")).unwrap();
            fs::write(root.join("202607/plan.md"), "# Plan\n").unwrap();
        }
        let result = resolve_plan_reference(
            "plans:202607/plan.md",
            &[first.clone(), second],
        )
        .unwrap();
        assert_eq!(result.status, "exact");
        assert_eq!(
            result.resolved_path.as_deref(),
            Some(first.join("202607/plan.md").to_str().unwrap())
        );
        assert_eq!(result.candidates.len(), 1);
    }

    #[test]
    fn unique_month_drift_resolves() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("plans");
        fs::create_dir_all(root.join("202608")).unwrap();
        fs::write(root.join("202608/plan.md"), "# Plan\n").unwrap();
        let result = resolve_plan_reference(
            "plans:202607/plan.md",
            std::slice::from_ref(&root),
        )
        .unwrap();
        assert_eq!(result.status, "drifted");
        assert_eq!(
            result.resolved_path.as_deref(),
            Some(root.join("202608/plan.md").to_str().unwrap())
        );
    }

    #[test]
    fn multiple_month_drift_matches_are_ambiguous() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("plans");
        for month in ["202608", "202609"] {
            fs::create_dir_all(root.join(month)).unwrap();
            fs::write(root.join(month).join("plan.md"), "# Plan\n").unwrap();
        }
        let result =
            resolve_plan_reference("plans:202607/plan.md", &[root]).unwrap();
        assert_eq!(result.status, "ambiguous");
        assert_eq!(result.resolved_path, None);
        assert_eq!(result.candidates.len(), 2);
    }

    #[test]
    fn missing_keeps_ordered_best_candidates() {
        let temp = tempdir().unwrap();
        let roots = [temp.path().join("store"), temp.path().join("local")];
        let result =
            resolve_plan_reference("plans:202607/plan.md", &roots).unwrap();
        assert_eq!(result.status, "missing");
        assert_eq!(result.resolved_path, None);
        assert_eq!(
            result.candidates,
            roots
                .iter()
                .map(|root| root
                    .join("202607/plan.md")
                    .to_string_lossy()
                    .into())
                .collect::<Vec<String>>()
        );
    }

    #[test]
    fn every_legacy_form_resolves_to_the_same_file() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("plans");
        let target = root.join("202607/plan.md");
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(&target, "# Plan\n").unwrap();
        let absolute_old =
            temp.path().join("retired/sase/repos/plans/202607/plan.md");
        let values = [
            target.to_string_lossy().into_owned(),
            absolute_old.to_string_lossy().into_owned(),
            "sase/repos/plans/202607/plan.md".to_string(),
            ".sase/sdd/plans/202607/plan.md".to_string(),
            "sdd/plans/202607/plan.md".to_string(),
            "plans/202607/plan.md".to_string(),
            "202607/plan.md".to_string(),
        ];
        for value in values {
            let result =
                resolve_plan_reference(&value, std::slice::from_ref(&root))
                    .unwrap();
            assert_eq!(result.status, "exact", "{value}");
            assert_eq!(
                result.resolved_path.as_deref(),
                Some(target.to_str().unwrap()),
                "{value}"
            );
        }
    }
}
