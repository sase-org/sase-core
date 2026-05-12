//! Phase 1C full-file parser with section parity.
//!
//! Mirrors `sase_100/src/sase/ace/changespec/parser.py` and
//! `section_parsers.py`:
//!
//! - ChangeSpec boundaries: `## ChangeSpec` headers, direct `NAME:` starts,
//!   end-on-next-header, end-on-two-blank-lines, end-on-new-NAME.
//! - Drop incomplete records that lack either `NAME` or `STATUS`.
//! - Scalar fields: `NAME`, `DESCRIPTION`, `PARENT`, `CL`/`PR`, `BUG`,
//!   `STATUS`, `TEST TARGETS`.
//! - Section bodies: `COMMITS`, `HOOKS`, `COMMENTS`, `MENTORS`,
//!   `TIMESTAMPS`, `DELTAS`. The wire records produced here match Python
//!   parser output for the golden corpus.
//! - Whitespace: inline header content stripped, two-space continuation
//!   strips the first two spaces, blank lines preserved inside
//!   description before the final trim.
//! - `project_basename` matches Python's `ChangeSpec.project_basename`
//!   (basename minus extension, with a trailing `-archive` suffix removed).
//! - `source_span.start_line` and `end_line` are inclusive 1-based. Unlike
//!   the Python facade's placeholder, `end_line` here is the real last
//!   non-blank line of the spec.

use crate::project_spec::project_spec_basename;
use crate::sections::{
    parse_comments_line, parse_commits_line, parse_deltas_line,
    parse_hooks_line, parse_mentors_line, parse_timestamps_line,
};
use crate::wire::{
    ChangeSpecWire, CommentWire, CommitWire, DeltaWire, HookWire, MentorWire,
    ParseErrorWire, SourceSpanWire, TimestampWire,
    CHANGESPEC_WIRE_SCHEMA_VERSION,
};

/// Parse all ChangeSpecs from a project file's raw bytes.
///
/// The Python equivalent is `parse_project_file_python`, which reads the
/// file from disk via `f.readlines()`. This Rust entry point takes bytes
/// directly so callers can avoid a temp-file round-trip.
pub fn parse_project_bytes(
    path: &str,
    data: &[u8],
) -> Result<Vec<ChangeSpecWire>, ParseErrorWire> {
    let text = std::str::from_utf8(data).map_err(|e| ParseErrorWire {
        kind: "encoding".to_string(),
        message: format!("invalid UTF-8: {e}"),
        file_path: path.to_string(),
        line: None,
        column: None,
    })?;

    let lines: Vec<&str> = text.lines().collect();
    let mut specs: Vec<ChangeSpecWire> = Vec::new();
    let mut idx = 0usize;

    while idx < lines.len() {
        let line = lines[idx];
        if is_changespec_header(line) {
            let (spec, next_idx) = parse_one_changespec(&lines, idx + 1, path);
            if let Some(s) = spec {
                specs.push(s);
            }
            idx = next_idx;
        } else if line.starts_with("NAME: ") {
            let (spec, next_idx) = parse_one_changespec(&lines, idx, path);
            if let Some(s) = spec {
                specs.push(s);
            }
            idx = next_idx;
        } else {
            idx += 1;
        }
    }

    Ok(specs)
}

/// Match Python's `re.match(r"^##\s+ChangeSpec", line.strip())`.
fn is_changespec_header(line: &str) -> bool {
    let trimmed = line.trim();
    if !trimmed.starts_with("##") {
        return false;
    }
    let after = &trimmed[2..];
    let mut chars = after.chars();
    match chars.next() {
        Some(c) if c.is_whitespace() => {}
        _ => return false,
    }
    after.trim_start().starts_with("ChangeSpec")
}

#[derive(Default)]
struct ParserState {
    name: Option<String>,
    description_lines: Vec<String>,
    parent: Option<String>,
    cl: Option<String>,
    bug: Option<String>,
    status: Option<String>,
    test_targets: Vec<String>,

    commits: Vec<CommitWire>,
    current_commit: Option<CommitWire>,
    hooks: Vec<HookWire>,
    current_hook: Option<HookWire>,
    comments: Vec<CommentWire>,
    mentors: Vec<MentorWire>,
    current_mentor: Option<MentorWire>,
    timestamps: Vec<TimestampWire>,
    deltas: Vec<DeltaWire>,

    in_description: bool,
    in_test_targets: bool,
    in_commits: bool,
    in_hooks: bool,
    in_comments: bool,
    in_mentors: bool,
    in_timestamps: bool,
    in_deltas: bool,
}

impl ParserState {
    fn reset_section_flags(&mut self) {
        self.in_description = false;
        self.in_test_targets = false;
        self.in_commits = false;
        self.in_hooks = false;
        self.in_comments = false;
        self.in_mentors = false;
        self.in_timestamps = false;
        self.in_deltas = false;
    }

    /// Mirrors Python's `_ParserState.save_pending_entries`.
    fn save_pending_entries(&mut self) {
        if let Some(c) = self.current_commit.take() {
            self.commits.push(c);
        }
        if let Some(h) = self.current_hook.take() {
            self.hooks.push(h);
        }
        if let Some(m) = self.current_mentor.take() {
            self.mentors.push(m);
        }
    }

    fn build(
        mut self,
        file_path: &str,
        start_line: u32,
        end_line: u32,
    ) -> Option<ChangeSpecWire> {
        self.save_pending_entries();
        let name = self.name?;
        let status = self.status?;
        let description = trim_block(&self.description_lines);
        Some(ChangeSpecWire {
            schema_version: CHANGESPEC_WIRE_SCHEMA_VERSION,
            name,
            project_basename: project_spec_basename(file_path),
            file_path: file_path.to_string(),
            source_span: SourceSpanWire {
                file_path: file_path.to_string(),
                start_line,
                end_line,
            },
            status,
            parent: self.parent,
            cl_or_pr: self.cl,
            bug: self.bug,
            description,
            test_targets: self.test_targets,
            commits: self.commits,
            hooks: self.hooks,
            comments: self.comments,
            mentors: self.mentors,
            timestamps: self.timestamps,
            deltas: self.deltas,
        })
    }
}

/// Python: `"\n".join(lines).strip()`.
fn trim_block(lines: &[String]) -> String {
    let joined = lines.join("\n");
    joined.trim().to_string()
}

enum FieldHeaderOutcome {
    Parsed,
    NewName,
    None,
}

fn try_field_header(state: &mut ParserState, line: &str) -> FieldHeaderOutcome {
    if let Some(rest) = line.strip_prefix("NAME: ") {
        if state.name.is_some() {
            return FieldHeaderOutcome::NewName;
        }
        state.name = Some(rest.trim().to_string());
        state.reset_section_flags();
        return FieldHeaderOutcome::Parsed;
    }
    if let Some(rest) = line.strip_prefix("DESCRIPTION:") {
        state.save_pending_entries();
        state.reset_section_flags();
        state.in_description = true;
        let inline = rest.trim();
        if !inline.is_empty() {
            state.description_lines.push(inline.to_string());
        }
        return FieldHeaderOutcome::Parsed;
    }
    if let Some(rest) = line.strip_prefix("PARENT: ") {
        state.save_pending_entries();
        state.parent = Some(rest.trim().to_string());
        state.reset_section_flags();
        return FieldHeaderOutcome::Parsed;
    }
    if let Some(rest) = line.strip_prefix("CL: ") {
        state.save_pending_entries();
        state.cl = Some(rest.trim().to_string());
        state.reset_section_flags();
        return FieldHeaderOutcome::Parsed;
    }
    if let Some(rest) = line.strip_prefix("PR: ") {
        state.save_pending_entries();
        state.cl = Some(rest.trim().to_string());
        state.reset_section_flags();
        return FieldHeaderOutcome::Parsed;
    }
    if let Some(rest) = line.strip_prefix("BUG: ") {
        state.save_pending_entries();
        state.bug = Some(rest.trim().to_string());
        state.reset_section_flags();
        return FieldHeaderOutcome::Parsed;
    }
    if let Some(rest) = line.strip_prefix("STATUS: ") {
        state.save_pending_entries();
        state.status = Some(rest.trim().to_string());
        state.reset_section_flags();
        return FieldHeaderOutcome::Parsed;
    }
    FieldHeaderOutcome::None
}

fn try_section_header(state: &mut ParserState, line: &str) -> bool {
    if line.starts_with("COMMITS:") {
        state.save_pending_entries();
        state.reset_section_flags();
        state.in_commits = true;
        return true;
    }
    if line.starts_with("HOOKS:") {
        state.save_pending_entries();
        state.reset_section_flags();
        state.in_hooks = true;
        return true;
    }
    if line.starts_with("COMMENTS:") {
        state.save_pending_entries();
        state.reset_section_flags();
        state.in_comments = true;
        return true;
    }
    if line.starts_with("MENTORS:") {
        state.save_pending_entries();
        state.reset_section_flags();
        state.in_mentors = true;
        return true;
    }
    if line.starts_with("TIMESTAMPS:") {
        state.save_pending_entries();
        state.reset_section_flags();
        state.in_timestamps = true;
        return true;
    }
    if line.starts_with("DELTAS:") {
        state.save_pending_entries();
        state.reset_section_flags();
        state.in_deltas = true;
        return true;
    }
    if let Some(rest) = line.strip_prefix("TEST TARGETS:") {
        state.save_pending_entries();
        state.reset_section_flags();
        state.in_test_targets = true;
        let inline = rest.trim();
        if !inline.is_empty() {
            state.test_targets.push(inline.to_string());
        }
        return true;
    }
    false
}

fn parse_section_content(state: &mut ParserState, line: &str) {
    let stripped = line.trim();

    // Section-specific parsing takes priority. Inside a section we never
    // fall through to description/test_targets handling, even if
    // the line looks like a continuation.
    if state.in_timestamps {
        parse_timestamps_line(line, stripped, &mut state.timestamps);
        return;
    }
    if state.in_deltas {
        parse_deltas_line(line, &mut state.deltas);
        return;
    }
    if state.in_hooks {
        parse_hooks_line(
            line,
            stripped,
            &mut state.current_hook,
            &mut state.hooks,
        );
        return;
    }
    if state.in_comments {
        parse_comments_line(line, stripped, &mut state.comments);
        return;
    }
    if state.in_mentors {
        parse_mentors_line(
            line,
            stripped,
            &mut state.current_mentor,
            &mut state.mentors,
        );
        return;
    }
    if state.in_commits {
        parse_commits_line(
            line,
            stripped,
            &mut state.current_commit,
            &mut state.commits,
        );
        return;
    }

    if state.in_description && line.starts_with("  ") {
        state.description_lines.push(line[2..].to_string());
        return;
    }
    if state.in_test_targets && line.starts_with("  ") {
        if !stripped.is_empty() {
            state.test_targets.push(stripped.to_string());
        }
        return;
    }
    if stripped.is_empty() {
        if state.in_description {
            state.description_lines.push(String::new());
        }
        return;
    }
    if !line.starts_with('#') {
        state.reset_section_flags();
    }
}

fn parse_one_changespec(
    lines: &[&str],
    start_idx: usize,
    file_path: &str,
) -> (Option<ChangeSpecWire>, usize) {
    let mut state = ParserState::default();
    let mut idx = start_idx;
    let mut consecutive_blank = 0usize;
    let mut last_content_idx = start_idx;

    while idx < lines.len() {
        let line = lines[idx];
        let stripped = line.trim();

        if is_changespec_header(line) && idx > start_idx {
            break;
        }

        if stripped.is_empty() {
            consecutive_blank += 1;
            if consecutive_blank >= 2 {
                break;
            }
        } else {
            consecutive_blank = 0;
        }

        match try_field_header(&mut state, line) {
            FieldHeaderOutcome::Parsed => {
                if !stripped.is_empty() {
                    last_content_idx = idx;
                }
                idx += 1;
                continue;
            }
            FieldHeaderOutcome::NewName => {
                state.save_pending_entries();
                break;
            }
            FieldHeaderOutcome::None => {}
        }

        if try_section_header(&mut state, line) {
            if !stripped.is_empty() {
                last_content_idx = idx;
            }
            idx += 1;
            continue;
        }

        parse_section_content(&mut state, line);
        if !stripped.is_empty() {
            last_content_idx = idx;
        }
        idx += 1;
    }

    let start_line = (start_idx as u32) + 1;
    let end_line = (last_content_idx as u32) + 1;
    let spec = state.build(file_path, start_line, end_line);
    (spec, idx)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(data: &str) -> Vec<ChangeSpecWire> {
        parse_project_bytes("myproj.gp", data.as_bytes()).unwrap()
    }

    #[test]
    fn project_basename_strips_extension_and_archive_suffix() {
        assert_eq!(project_spec_basename("/tmp/myproj.sase"), "myproj");
        assert_eq!(project_spec_basename("/tmp/myproj.gp"), "myproj");
        assert_eq!(project_spec_basename("myproj.sase"), "myproj");
        assert_eq!(project_spec_basename("myproj.gp"), "myproj");
        assert_eq!(project_spec_basename("/tmp/myproj-archive.sase"), "myproj");
        assert_eq!(project_spec_basename("/tmp/myproj-archive.gp"), "myproj");
        assert_eq!(project_spec_basename("/tmp/no_ext"), "no_ext");
        assert_eq!(project_spec_basename("foo.bar.sase"), "foo.bar");
    }

    #[test]
    fn changespec_header_detection_requires_whitespace_then_word() {
        assert!(is_changespec_header("## ChangeSpec"));
        assert!(is_changespec_header("##  ChangeSpec"));
        assert!(is_changespec_header("##\tChangeSpec"));
        assert!(is_changespec_header("   ## ChangeSpec   "));
        assert!(!is_changespec_header("##ChangeSpec"));
        assert!(!is_changespec_header("# ChangeSpec"));
        assert!(!is_changespec_header("NAME: foo"));
    }

    #[test]
    fn parses_headered_spec_with_inline_description() {
        let src = "## ChangeSpec\nNAME: foo\nDESCRIPTION: A short feature.\nSTATUS: WIP\n";
        let specs = parse(src);
        assert_eq!(specs.len(), 1);
        let s = &specs[0];
        assert_eq!(s.name, "foo");
        assert_eq!(s.status, "WIP");
        assert_eq!(s.description, "A short feature.");
        assert_eq!(s.source_span.start_line, 2);
        assert_eq!(s.source_span.end_line, 4);
        assert_eq!(s.project_basename, "myproj");
        assert_eq!(s.file_path, "myproj.gp");
    }

    #[test]
    fn parses_direct_name_spec_without_header() {
        let src = "NAME: foo\nSTATUS: WIP\n";
        let specs = parse(src);
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].name, "foo");
        assert_eq!(specs[0].source_span.start_line, 1);
        assert_eq!(specs[0].source_span.end_line, 2);
    }

    #[test]
    fn drops_incomplete_specs_missing_name_or_status() {
        let only_status = "STATUS: WIP\n";
        assert!(parse(only_status).is_empty());

        let no_status = "NAME: foo\nDESCRIPTION: x\n";
        assert!(parse(no_status).is_empty());

        let header_then_name_no_status =
            "## ChangeSpec\nNAME: foo\nDESCRIPTION: x\n";
        assert!(parse(header_then_name_no_status).is_empty());
    }

    #[test]
    fn multi_line_description_strips_two_space_continuation() {
        let src = "\
NAME: alpha
DESCRIPTION:
  Initial feature work.
  Spans multiple lines.
STATUS: Submitted
";
        let specs = parse(src);
        assert_eq!(specs.len(), 1);
        assert_eq!(
            specs[0].description,
            "Initial feature work.\nSpans multiple lines."
        );
    }

    #[test]
    fn description_preserves_internal_blank_lines_then_trims() {
        let src = "\
NAME: alpha
DESCRIPTION:
  First paragraph.

  Second paragraph.

STATUS: Submitted
";
        let specs = parse(src);
        assert_eq!(
            specs[0].description,
            "First paragraph.\n\nSecond paragraph."
        );
    }

    #[test]
    fn test_targets_inline_and_block_treats_indented_lines_as_targets() {
        let inline = "NAME: a\nTEST TARGETS: //foo:bar\nSTATUS: WIP\n";
        assert_eq!(parse(inline)[0].test_targets, vec!["//foo:bar"]);

        let block = "\
NAME: a
TEST TARGETS:
  //foo:bar
  //foo:baz (FAILED)
STATUS: WIP
";
        assert_eq!(
            parse(block)[0].test_targets,
            vec!["//foo:bar", "//foo:baz (FAILED)"]
        );
    }

    #[test]
    fn parent_cl_pr_bug_scalars_round_trip() {
        let src = "\
NAME: a
DESCRIPTION: x
PARENT: some_parent
PR: https://example/pr/1
BUG: BUG-100
STATUS: WIP
";
        let s = &parse(src)[0];
        assert_eq!(s.parent.as_deref(), Some("some_parent"));
        assert_eq!(s.cl_or_pr.as_deref(), Some("https://example/pr/1"));
        assert_eq!(s.bug.as_deref(), Some("BUG-100"));

        let cl_src = "NAME: a\nCL: 12345\nSTATUS: WIP\n";
        assert_eq!(parse(cl_src)[0].cl_or_pr.as_deref(), Some("12345"));
    }

    #[test]
    fn empty_value_lines_become_none_or_empty_per_python_rules() {
        let src = "NAME: a\nPARENT:\nPR:\nSTATUS: WIP\n";
        let s = &parse(src)[0];
        assert_eq!(s.parent, None);
        assert_eq!(s.cl_or_pr, None);

        let src2 = "NAME: a\nPARENT: \nSTATUS: WIP\n";
        let s2 = &parse(src2)[0];
        assert_eq!(s2.parent.as_deref(), Some(""));
    }

    #[test]
    fn two_blank_lines_separate_specs() {
        let src = "\
NAME: alpha
STATUS: WIP


NAME: beta
STATUS: Ready
";
        let specs = parse(src);
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].name, "alpha");
        assert_eq!(specs[1].name, "beta");
        assert_eq!(specs[1].source_span.start_line, 5);
        assert_eq!(specs[1].source_span.end_line, 6);
    }

    #[test]
    fn changespec_header_separates_specs_without_blank_lines() {
        let src = "\
## ChangeSpec
NAME: alpha
STATUS: WIP
## ChangeSpec
NAME: beta
STATUS: Ready
";
        let specs = parse(src);
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].name, "alpha");
        assert_eq!(specs[1].name, "beta");
        assert_eq!(specs[0].source_span.start_line, 2);
        assert_eq!(specs[0].source_span.end_line, 3);
        assert_eq!(specs[1].source_span.start_line, 5);
        assert_eq!(specs[1].source_span.end_line, 6);
    }

    #[test]
    fn new_name_inside_a_spec_starts_a_new_spec() {
        let src = "NAME: alpha\nSTATUS: WIP\nNAME: beta\nSTATUS: Ready\n";
        let specs = parse(src);
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].name, "alpha");
        assert_eq!(specs[1].name, "beta");
        assert_eq!(specs[0].source_span.start_line, 1);
        assert_eq!(specs[0].source_span.end_line, 2);
        assert_eq!(specs[1].source_span.start_line, 3);
        assert_eq!(specs[1].source_span.end_line, 4);
    }

    #[test]
    fn span_excludes_trailing_blank_separator() {
        let src = "NAME: a\nSTATUS: WIP\n\n\nNAME: b\nSTATUS: WIP\n";
        let specs = parse(src);
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].source_span.end_line, 2);
    }

    #[test]
    fn rust_end_line_is_real_not_placeholder() {
        let src = "## ChangeSpec\nNAME: a\nDESCRIPTION:\n  multi\n  line\nSTATUS: WIP\n";
        let s = &parse(src)[0];
        assert!(
            s.source_span.end_line > s.source_span.start_line,
            "Rust must report a real end_line, got start={} end={}",
            s.source_span.start_line,
            s.source_span.end_line
        );
        assert_eq!(s.source_span.start_line, 2);
        assert_eq!(s.source_span.end_line, 6);
    }

    // -- Phase 1C section parity ------------------------------------------

    #[test]
    fn full_section_parity_emits_structured_entries() {
        // The Phase 1B fixture, but now with non-empty section assertions.
        let src = "\
## ChangeSpec
NAME: alpha
DESCRIPTION:
  Initial feature work.
PARENT:
PR: https://example.test/repo/pull/1
BUG: BUG-100
STATUS: Submitted
TEST TARGETS: tests/test_alpha.py
COMMITS:
  (1) [run] Initial Commit
      | CHAT: ~/.sase/chats/alpha.md (0s)
HOOKS:
  just lint
      | (1) [260101_120000] PASSED (3s)
COMMENTS:
  [critique] ~/.sase/comments/alpha.json
MENTORS:
  (1) profileA[1/1]
      | [260101_130000] profileA:mentor1 - PASSED - (1m0s)
TIMESTAMPS:
  260101_120000 STATUS WIP -> Submitted
DELTAS:
  + src/alpha.py
  ~ src/util.py
";
        let s = &parse(src)[0];
        assert_eq!(s.name, "alpha");
        assert_eq!(s.status, "Submitted");

        assert_eq!(s.commits.len(), 1);
        let c = &s.commits[0];
        assert_eq!(c.number, 1);
        assert_eq!(c.note, "[run] Initial Commit");
        assert_eq!(c.chat.as_deref(), Some("~/.sase/chats/alpha.md (0s)"));

        assert_eq!(s.hooks.len(), 1);
        let h = &s.hooks[0];
        assert_eq!(h.command, "just lint");
        assert_eq!(h.status_lines.len(), 1);

        assert_eq!(s.comments.len(), 1);
        assert_eq!(s.comments[0].reviewer, "critique");

        assert_eq!(s.mentors.len(), 1);
        assert_eq!(s.mentors[0].profiles, vec!["profileA".to_string()]);
        assert_eq!(s.mentors[0].status_lines.len(), 1);

        assert_eq!(s.timestamps.len(), 1);
        assert_eq!(s.timestamps[0].event_type, "STATUS");

        assert_eq!(s.deltas.len(), 2);
        assert_eq!(s.deltas[0].change_type, "A");
        assert_eq!(s.deltas[1].change_type, "M");
    }

    #[test]
    fn save_pending_entries_flushes_at_field_boundary() {
        // STATUS comes after a section with an in-progress commit/hook/mentor;
        // the save must persist them rather than dropping them on the floor.
        let src = "\
NAME: a
COMMITS:
  (1) one
HOOKS:
  just lint
MENTORS:
  (1) p[1/1]
STATUS: WIP
";
        let s = &parse(src)[0];
        assert_eq!(s.commits.len(), 1);
        assert_eq!(s.hooks.len(), 1);
        assert_eq!(s.mentors.len(), 1);
    }

    #[test]
    fn invalid_utf8_returns_parse_error_wire() {
        let bad = b"\xff\xfe\x00";
        let err = parse_project_bytes("p.gp", bad).unwrap_err();
        assert_eq!(err.kind, "encoding");
        assert_eq!(err.file_path, "p.gp");
        assert!(err.message.contains("UTF-8"));
    }

    #[test]
    fn empty_input_yields_no_specs() {
        assert!(parse("").is_empty());
        assert!(parse("\n\n\n").is_empty());
        assert!(parse("not a changespec\n").is_empty());
    }
}
