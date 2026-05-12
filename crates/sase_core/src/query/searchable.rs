//! Searchable text extraction for `ChangeSpecWire`.
//!
//! Mirrors `src/sase/ace/query/searchable.py`. The output of
//! `get_searchable_text` is the concatenated text the substring matchers
//! search against, joined by `\n`. The exact join order and per-suffix
//! formatting matter — golden parity tests pin the result against the
//! Python evaluator.

use crate::wire::ChangeSpecWire;

/// Marker indicating a running agent inside searchable text. Mirrors the
/// Python `RUNNING_AGENT_MARKER`. The substring `"- (@"` is enough to match
/// both the bare `- (@)` form and the `- (@: msg)` form.
pub const RUNNING_AGENT_MARKER: &str = "- (@";

/// Marker indicating a running hook subprocess. Mirrors
/// `RUNNING_PROCESS_MARKER` in Python. The trailing space distinguishes it
/// from the running-agent marker.
pub const RUNNING_PROCESS_MARKER: &str = "- ($: ";

/// `basename(dirname(file_path))` — matches `ChangeSpec.project_name` in
/// Python, which is the *parent directory* of the project file (e.g.
/// `core_golden` for `tests/core_golden/myproj.sase`).
///
/// Note this is intentionally *not* `ChangeSpecWire.project_basename`
/// (the file basename minus extension); the Python query evaluator uses the
/// parent directory name and the golden corpus depends on that distinction.
pub fn project_dir_name(file_path: &str) -> &str {
    let bytes = file_path.as_bytes();
    let mut end = bytes.len();
    while end > 0 && bytes[end - 1] == b'/' {
        end -= 1;
    }
    let trimmed = &file_path[..end];
    let parent = match trimmed.rfind('/') {
        Some(i) => &trimmed[..i],
        None => return "",
    };
    let parent = parent.trim_end_matches('/');
    match parent.rfind('/') {
        Some(i) => &parent[i + 1..],
        None => parent,
    }
}

fn display_command(command: &str) -> &str {
    command.trim_start_matches(['!', '$'])
}

fn push_suffix_part(
    parts: &mut Vec<String>,
    suffix_type: Option<&str>,
    suffix: Option<&str>,
) {
    let suf = suffix.unwrap_or("");
    match suffix_type {
        Some("running_agent") => {
            if suf.is_empty() {
                parts.push("- (@)".to_string());
            } else {
                parts.push(format!("- (@: {suf})"));
            }
        }
        Some("running_process") => {
            parts.push(format!("- ($: {suf})"));
        }
        Some("killed_process") => {
            parts.push(format!("- (~$: {suf})"));
        }
        Some("error") if !suf.is_empty() => {
            parts.push(format!("(!: {suf})"));
        }
        Some(_) if !suf.is_empty() => {
            parts.push(format!("({suf})"));
        }
        _ => {}
    }
}

/// Build the concatenated searchable text for one ChangeSpec.
///
/// Mirrors `sase.ace.query.searchable.get_searchable_text` exactly, including
/// the `\n` join, the order of fields, and the suffix formatting per type.
pub fn get_searchable_text(cs: &ChangeSpecWire) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(16);
    parts.push(cs.name.clone());
    parts.push(cs.description.clone());
    parts.push(cs.status.clone());
    parts.push(project_dir_name(&cs.file_path).to_string());

    if let Some(parent) = &cs.parent {
        parts.push(parent.clone());
    }
    if let Some(cl) = &cs.cl_or_pr {
        parts.push(cl.clone());
    }
    for entry in &cs.commits {
        parts.push(entry.note.clone());
        if let Some(suf) = &entry.suffix {
            if entry.suffix_type.as_deref() == Some("error") {
                parts.push(format!("(!: {suf})"));
            } else {
                parts.push(format!("({suf})"));
            }
        }
    }

    for hook in &cs.hooks {
        parts.push(display_command(&hook.command).to_string());
        for sl in &hook.status_lines {
            push_suffix_part(
                &mut parts,
                sl.suffix_type.as_deref(),
                sl.suffix.as_deref(),
            );
        }
    }

    for comment in &cs.comments {
        parts.push(comment.reviewer.clone());
        parts.push(comment.file_path.clone());
        push_suffix_part(
            &mut parts,
            comment.suffix_type.as_deref(),
            comment.suffix.as_deref(),
        );
    }

    for mentor in &cs.mentors {
        for msl in &mentor.status_lines {
            // Python only emits running_agent and error/other-non-empty for
            // mentors (no running_process/killed_process branches).
            match msl.suffix_type.as_deref() {
                Some("running_agent") => {
                    let suf = msl.suffix.as_deref().unwrap_or("");
                    if suf.is_empty() {
                        parts.push("- (@)".to_string());
                    } else {
                        parts.push(format!("- (@: {suf})"));
                    }
                }
                Some("error") => {
                    if let Some(suf) = msl.suffix.as_deref() {
                        if !suf.is_empty() {
                            parts.push(format!("(!: {suf})"));
                        }
                    }
                }
                Some(_) => {
                    if let Some(suf) = msl.suffix.as_deref() {
                        if !suf.is_empty() {
                            parts.push(format!("({suf})"));
                        }
                    }
                }
                None => {}
            }
        }
    }

    parts.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_dir_name_basic() {
        assert_eq!(
            project_dir_name("/home/x/projects/myproj/myproj.sase"),
            "myproj"
        );
        assert_eq!(
            project_dir_name("tests/core_golden/myproj.sase"),
            "core_golden"
        );
        assert_eq!(project_dir_name("only.sase"), "");
        assert_eq!(project_dir_name("/just/file"), "just");
    }
}
