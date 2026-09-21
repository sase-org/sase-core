//! Stored plan/design reference display: labels, resolution, and
//! working-directory-relative rendering.

use std::path::{Path, PathBuf};

use crate::plan::refs::{parse_plan_reference, resolve_plan_reference};

/// Rendered when a stored plan reference matches no plan file.
const PLAN_REFERENCE_MISSING_LABEL: &str = "(unresolved: no plan file found)";
/// Rendered when a stored plan reference matches more than one plan file.
const PLAN_REFERENCE_AMBIGUOUS_LABEL: &str =
    "(ambiguous: multiple plans match this reference)";
/// Rendered when a stored plan reference violates the reference grammar.
const PLAN_REFERENCE_INVALID_LABEL: &str =
    "(unresolved: malformed plan reference)";
/// Marks a reference that only resolved after ignoring its month directory.
const PLAN_REFERENCE_DRIFT_SUFFIX: &str = " (month drift)";

/// Render a stored plan reference and where it currently resolves.
///
/// The first line is always the stable reference as stored. A second line
/// reports the resolved path, or says plainly that the reference resolves
/// nowhere; it is omitted when the resolved path is the reference itself.
pub(super) fn display_design_path(
    design: &str,
    cwd: &Path,
    relativize_design_paths: bool,
    plan_roots: &[PathBuf],
) -> Vec<String> {
    let reference = design.to_string();
    let resolved = resolve_design_reference(design, cwd, plan_roots);
    let detail = match resolved {
        DesignResolution::Resolved { path, drifted } => {
            let display =
                display_plan_path(&path, cwd, relativize_design_paths);
            if display == reference {
                return vec![reference];
            }
            format!(
                "{display}{}",
                if drifted {
                    PLAN_REFERENCE_DRIFT_SUFFIX
                } else {
                    ""
                }
            )
        }
        DesignResolution::Ambiguous => {
            PLAN_REFERENCE_AMBIGUOUS_LABEL.to_string()
        }
        DesignResolution::Invalid => PLAN_REFERENCE_INVALID_LABEL.to_string(),
        DesignResolution::Missing => PLAN_REFERENCE_MISSING_LABEL.to_string(),
    };
    vec![reference, format!("→ {detail}")]
}

/// Where one stored `design` value points once the shared resolver has run.
enum DesignResolution {
    Resolved { path: PathBuf, drifted: bool },
    Ambiguous,
    Invalid,
    Missing,
}

fn resolve_design_reference(
    design: &str,
    cwd: &Path,
    plan_roots: &[PathBuf],
) -> DesignResolution {
    let Ok(resolution) = resolve_plan_reference(design, plan_roots) else {
        return DesignResolution::Invalid;
    };
    if let Some(path) = resolution.resolved_path.as_ref() {
        return DesignResolution::Resolved {
            path: PathBuf::from(path),
            drifted: resolution.status == "drifted",
        };
    }
    // A legacy relative path still names a file below the working directory,
    // which is how in-tree stores linked plans before typed plan references.
    if let Some(path) = legacy_path_below_cwd(design, cwd) {
        return DesignResolution::Resolved {
            path,
            drifted: false,
        };
    }
    if resolution.status == "ambiguous" {
        return DesignResolution::Ambiguous;
    }
    DesignResolution::Missing
}

fn legacy_path_below_cwd(design: &str, cwd: &Path) -> Option<PathBuf> {
    let parsed = parse_plan_reference(design).ok()?;
    if !parsed.legacy {
        return None;
    }
    let path = Path::new(design);
    if path.is_absolute() {
        return None;
    }
    let candidate = cwd.join(path);
    candidate.is_file().then_some(candidate)
}

fn display_plan_path(
    path: &Path,
    cwd: &Path,
    relativize_design_paths: bool,
) -> String {
    if !relativize_design_paths {
        return path.display().to_string();
    }
    path.strip_prefix(cwd)
        .map(|relative| relative.display().to_string())
        .unwrap_or_else(|_| path.display().to_string())
}
