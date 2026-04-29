//! Predicates and helpers used during query evaluation.
//!
//! Mirrors `src/sase/ace/query/matchers.py` and the suffix-related helpers in
//! `src/sase/ace/changespec/validation.py`. Substring matching only — no
//! regex is exposed to query authors. The two regex helpers below are
//! private status strippers (Python: `_WORKSPACE_SUFFIX_RE` /
//! `_LEGACY_READY_TO_MAIL_RE`).

use std::sync::OnceLock;

use regex::Regex;

use crate::query::searchable::project_dir_name;
use crate::wire::ChangeSpecWire;

/// `r" \([a-zA-Z0-9_-]+_\d+\)$"` — matches workspace suffixes like
/// ` (foo_3)` at the end of a STATUS string.
fn workspace_suffix_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r" \([a-zA-Z0-9_-]+_\d+\)$").unwrap())
}

/// `r" - \(!\: READY TO MAIL\)$"` — legacy STATUS suffix.
fn legacy_ready_to_mail_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r" - \(!: READY TO MAIL\)$").unwrap())
}

/// Strip workspace and legacy READY-TO-MAIL suffixes from a STATUS string.
/// Mirrors `get_base_status` in Python.
pub fn get_base_status(status: &str) -> String {
    let s = workspace_suffix_re().replace(status, "");
    let s = legacy_ready_to_mail_re().replace(&s, "");
    s.trim().to_string()
}

/// Strip the trailing `_<N>` (or legacy `__<N>`) reverted suffix from a
/// ChangeSpec name. Mirrors `sase.core.changespec.strip_reverted_suffix` —
/// the double-underscore form is checked first so `name__123` returns
/// `name` rather than `name_`.
pub fn strip_reverted_suffix(name: &str) -> String {
    static DOUBLE: OnceLock<Regex> = OnceLock::new();
    static SINGLE: OnceLock<Regex> = OnceLock::new();
    let dbl = DOUBLE.get_or_init(|| Regex::new(r"^(.+)__\d+$").unwrap());
    let sgl = SINGLE.get_or_init(|| Regex::new(r"^(.+)_\d+$").unwrap());
    if let Some(c) = dbl.captures(name) {
        return c.get(1).unwrap().as_str().to_string();
    }
    if let Some(c) = sgl.captures(name) {
        return c.get(1).unwrap().as_str().to_string();
    }
    name.to_string()
}

/// Detect the `(!: ...)` error-suffix marker anywhere on a ChangeSpec
/// (STATUS, COMMITS, HOOKS, COMMENTS). Mirrors
/// `has_any_status_suffix` in `validation.py` — used to evaluate the `!!!`
/// shorthand without scanning searchable text.
pub fn has_any_status_suffix(cs: &ChangeSpecWire) -> bool {
    if cs.status.contains(" - (!: ") {
        return true;
    }
    for entry in &cs.commits {
        if entry.suffix_type.as_deref() == Some("error") {
            return true;
        }
    }
    for hook in &cs.hooks {
        for sl in &hook.status_lines {
            if sl.suffix_type.as_deref() == Some("error") {
                return true;
            }
        }
    }
    for comment in &cs.comments {
        if comment.suffix_type.as_deref() == Some("error") {
            return true;
        }
    }
    false
}

/// `status:<value>` — case-insensitive equality on the *base* status.
pub fn match_status(value: &str, cs: &ChangeSpecWire) -> bool {
    get_base_status(&cs.status).eq_ignore_ascii_case(value)
}

/// `project:<value>` — case-insensitive equality on the parent-directory
/// component of `file_path`. See `project_dir_name` for why this isn't
/// `ChangeSpecWire.project_basename`.
pub fn match_project(value: &str, cs: &ChangeSpecWire) -> bool {
    project_dir_name(&cs.file_path).eq_ignore_ascii_case(value)
}

/// `name:<value>` — case-insensitive exact match on the ChangeSpec NAME.
pub fn match_name(value: &str, cs: &ChangeSpecWire) -> bool {
    cs.name.eq_ignore_ascii_case(value)
}

/// `sibling:<value>` — case-insensitive equality on the reverted-suffix-
/// stripped names. `name__260102_010101` and `name` share a sibling family
/// only when stripping yields the same base.
pub fn match_sibling(value: &str, cs: &ChangeSpecWire) -> bool {
    let search_base = strip_reverted_suffix(value);
    let cs_base = strip_reverted_suffix(&cs.name);
    search_base.eq_ignore_ascii_case(&cs_base)
}
