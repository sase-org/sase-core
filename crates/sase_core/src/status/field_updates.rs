//! Pure line helpers for the STATUS field.
//!
//! Mirrors `read_status_from_lines_python` and `apply_status_update_python`
//! in `sase_100/src/sase/status_state_machine/field_updates.py`. Both
//! helpers are line-oriented and side-effect free; the host owns the file
//! lock and atomic write.

/// Read the STATUS for a named ChangeSpec from a sequence of file lines.
///
/// Iterates lines in order, tracking the current ChangeSpec via `NAME:`
/// headers. Returns the value following `STATUS:` for the matching record,
/// preserving any workspace suffix as written. Returns `None` when the
/// ChangeSpec is not present in *lines*.
pub fn read_status_from_lines(
    lines: &[String],
    changespec_name: &str,
) -> Option<String> {
    let mut in_target = false;
    for line in lines {
        if let Some(rest) = line.strip_prefix("NAME:") {
            let current = rest.split_once('\n').map(|(s, _)| s).unwrap_or(rest);
            in_target = current.trim() == changespec_name;
        }
        if in_target {
            if let Some(rest) = line.strip_prefix("STATUS:") {
                let value =
                    rest.split_once('\n').map(|(s, _)| s).unwrap_or(rest);
                return Some(value.trim().to_string());
            }
        }
    }
    None
}

/// Replace the STATUS line of *changespec_name* in *lines* with
/// `STATUS: <new_status>\n`.
///
/// Returns the updated file content as a single string. Lines that do not
/// belong to the target ChangeSpec — including `STATUS:` lines for other
/// ChangeSpecs — pass through verbatim. When *changespec_name* is not
/// present, the input is returned unchanged.
///
/// Mirrors `apply_status_update_python` exactly: the target flag is
/// cleared after the first `STATUS:` rewrite so a second `STATUS:` line in
/// the same record (which the `.sase` format does not produce) would not be
/// touched.
pub fn apply_status_update(
    lines: &[String],
    changespec_name: &str,
    new_status: &str,
) -> String {
    let total_capacity: usize = lines.iter().map(|l| l.len()).sum();
    let mut updated = String::with_capacity(total_capacity);
    let mut in_target = false;
    for line in lines {
        if let Some(rest) = line.strip_prefix("NAME:") {
            let current = rest.split_once('\n').map(|(s, _)| s).unwrap_or(rest);
            in_target = current.trim() == changespec_name;
        }
        if in_target && line.starts_with("STATUS:") {
            updated.push_str("STATUS: ");
            updated.push_str(new_status);
            updated.push('\n');
            in_target = false;
        } else {
            updated.push_str(line);
        }
    }
    updated
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    fn project_lines() -> Vec<String> {
        lines(&[
            "## ChangeSpec\n",
            "\n",
            "NAME: alpha\n",
            "DESCRIPTION:\n",
            "  alpha desc\n",
            "STATUS: Ready\n",
            "\n",
            "---\n",
            "\n",
            "## ChangeSpec\n",
            "\n",
            "NAME: beta\n",
            "DESCRIPTION:\n",
            "  beta desc\n",
            "STATUS: Draft\n",
            "\n",
            "---\n",
        ])
    }

    #[test]
    fn read_status_matches_first_changespec() {
        assert_eq!(
            read_status_from_lines(&project_lines(), "alpha").as_deref(),
            Some("Ready")
        );
    }

    #[test]
    fn read_status_matches_second_changespec() {
        assert_eq!(
            read_status_from_lines(&project_lines(), "beta").as_deref(),
            Some("Draft")
        );
    }

    #[test]
    fn read_status_returns_none_when_missing() {
        assert!(read_status_from_lines(&project_lines(), "gamma").is_none());
    }

    #[test]
    fn read_status_preserves_workspace_suffix() {
        let lines = lines(&["NAME: thing\n", "STATUS: Ready (proj_2)\n"]);
        assert_eq!(
            read_status_from_lines(&lines, "thing").as_deref(),
            Some("Ready (proj_2)")
        );
    }

    #[test]
    fn apply_status_update_replaces_only_target_status() {
        let updated = apply_status_update(&project_lines(), "alpha", "Mailed");
        assert!(updated.contains("STATUS: Mailed\n"));
        // Other ChangeSpec untouched.
        assert!(updated.contains("STATUS: Draft\n"));
        // Replaced original gone.
        assert!(!updated.contains("STATUS: Ready\n"));
    }

    #[test]
    fn apply_status_update_preserves_unrelated_lines() {
        let updated = apply_status_update(&project_lines(), "alpha", "Mailed");
        assert!(updated.contains("NAME: alpha\n"));
        assert!(updated.contains("  alpha desc\n"));
        assert!(updated.contains("---\n"));
    }

    #[test]
    fn apply_status_update_no_matching_changespec_is_noop() {
        let pls = project_lines();
        let updated = apply_status_update(&pls, "gamma", "Mailed");
        assert_eq!(updated, pls.concat());
    }

    #[test]
    fn apply_status_update_idempotent_when_status_already_matches() {
        let pls = project_lines();
        let updated = apply_status_update(&pls, "beta", "Draft");
        assert_eq!(updated, pls.concat());
    }
}
