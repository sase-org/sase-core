//! Phase 1B full-file parser skeleton.
//!
//! Mirrors the scalar-field portion of
//! `sase_100/src/sase/ace/changespec/parser.py`:
//!
//! - ChangeSpec boundaries: `## ChangeSpec` headers, direct `NAME:` starts,
//!   end-on-next-header, end-on-two-blank-lines, end-on-new-NAME.
//! - Drop incomplete records that lack either `NAME` or `STATUS`.
//! - Scalar fields: `NAME`, `DESCRIPTION`, `PARENT`, `CL`/`PR`, `BUG`,
//!   `STATUS`, `TEST TARGETS`, `KICKSTART`.
//! - Whitespace: inline header content stripped, two-space continuation
//!   strips the first two spaces, blank lines preserved inside
//!   description/kickstart before the final trim.
//! - `project_basename` matches Python's `ChangeSpec.project_basename`
//!   (basename minus extension, with a trailing `-archive` suffix removed).
//! - `source_span.start_line` and `end_line` are inclusive 1-based. Unlike
//!   the Python facade's placeholder, `end_line` here is the real last
//!   non-blank line of the spec.
//!
//! Section bodies (`COMMITS`, `HOOKS`, `COMMENTS`, `MENTORS`, `TIMESTAMPS`,
//! `DELTAS`) are recognized so the parser does not mistake their contents
//! for fields, but their entries are not yet emitted — that arrives in
//! Phase 1C.

use crate::wire::{
    ChangeSpecWire, ParseErrorWire, SourceSpanWire,
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

/// Match Python's `ChangeSpec.project_basename`:
/// strip directory, strip the extension, then strip a trailing `-archive`.
fn project_basename(file_path: &str) -> String {
    let base = file_path.rsplit('/').next().unwrap_or(file_path);
    let stem = match base.rfind('.') {
        Some(i) => &base[..i],
        None => base,
    };
    if let Some(prefix) = stem.strip_suffix("-archive") {
        prefix.to_string()
    } else {
        stem.to_string()
    }
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
    kickstart_lines: Vec<String>,

    in_description: bool,
    in_kickstart: bool,
    in_test_targets: bool,
    in_other_section: bool,
}

impl ParserState {
    fn reset_section_flags(&mut self) {
        self.in_description = false;
        self.in_kickstart = false;
        self.in_test_targets = false;
        self.in_other_section = false;
    }

    fn build(
        self,
        file_path: &str,
        start_line: u32,
        end_line: u32,
    ) -> Option<ChangeSpecWire> {
        let name = self.name?;
        let status = self.status?;
        let description = trim_block(&self.description_lines);
        let kickstart = if self.kickstart_lines.is_empty() {
            None
        } else {
            Some(trim_block(&self.kickstart_lines))
        };
        Some(ChangeSpecWire {
            schema_version: CHANGESPEC_WIRE_SCHEMA_VERSION,
            name,
            project_basename: project_basename(file_path),
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
            kickstart,
            commits: vec![],
            hooks: vec![],
            comments: vec![],
            mentors: vec![],
            timestamps: vec![],
            deltas: vec![],
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
        state.reset_section_flags();
        state.in_description = true;
        let inline = rest.trim();
        if !inline.is_empty() {
            state.description_lines.push(inline.to_string());
        }
        return FieldHeaderOutcome::Parsed;
    }
    if let Some(rest) = line.strip_prefix("KICKSTART:") {
        state.reset_section_flags();
        state.in_kickstart = true;
        let inline = rest.trim();
        if !inline.is_empty() {
            state.kickstart_lines.push(inline.to_string());
        }
        return FieldHeaderOutcome::Parsed;
    }
    if let Some(rest) = line.strip_prefix("PARENT: ") {
        state.parent = Some(rest.trim().to_string());
        state.reset_section_flags();
        return FieldHeaderOutcome::Parsed;
    }
    if let Some(rest) = line.strip_prefix("CL: ") {
        state.cl = Some(rest.trim().to_string());
        state.reset_section_flags();
        return FieldHeaderOutcome::Parsed;
    }
    if let Some(rest) = line.strip_prefix("PR: ") {
        state.cl = Some(rest.trim().to_string());
        state.reset_section_flags();
        return FieldHeaderOutcome::Parsed;
    }
    if let Some(rest) = line.strip_prefix("BUG: ") {
        state.bug = Some(rest.trim().to_string());
        state.reset_section_flags();
        return FieldHeaderOutcome::Parsed;
    }
    if let Some(rest) = line.strip_prefix("STATUS: ") {
        state.status = Some(rest.trim().to_string());
        state.reset_section_flags();
        return FieldHeaderOutcome::Parsed;
    }
    FieldHeaderOutcome::None
}

fn try_section_header(state: &mut ParserState, line: &str) -> bool {
    if line.starts_with("COMMITS:")
        || line.starts_with("HOOKS:")
        || line.starts_with("COMMENTS:")
        || line.starts_with("MENTORS:")
        || line.starts_with("TIMESTAMPS:")
        || line.starts_with("DELTAS:")
    {
        state.reset_section_flags();
        state.in_other_section = true;
        return true;
    }
    if let Some(rest) = line.strip_prefix("TEST TARGETS:") {
        state.reset_section_flags();
        state.in_test_targets = true;
        let inline = rest.trim();
        if !inline.is_empty() {
            // Python treats inline content as a single target even if it
            // contains spaces (e.g. `target (FAILED)`).
            state.test_targets.push(inline.to_string());
        }
        return true;
    }
    false
}

fn parse_section_content(state: &mut ParserState, line: &str) {
    if state.in_other_section {
        // Phase 1C will parse these into structured entries; for now, the
        // line is consumed without being interpreted, which is enough to
        // avoid spurious resets and to keep section bodies from being
        // mistaken for fields.
        return;
    }

    let stripped = line.trim();

    if state.in_description && line.starts_with("  ") {
        state.description_lines.push(line[2..].to_string());
        return;
    }
    if state.in_kickstart && line.starts_with("  ") {
        state.kickstart_lines.push(line[2..].to_string());
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
        } else if state.in_kickstart {
            state.kickstart_lines.push(String::new());
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
    // Tracks the highest 0-based index of a non-blank line consumed by this
    // spec. Initialized to start_idx so a one-line spec still has a valid
    // (start_line == end_line) span.
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
                // Don't consume this line — caller re-processes it as a
                // fresh ChangeSpec start.
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
        assert_eq!(project_basename("/tmp/myproj.gp"), "myproj");
        assert_eq!(project_basename("myproj.gp"), "myproj");
        assert_eq!(project_basename("/tmp/myproj-archive.gp"), "myproj");
        assert_eq!(project_basename("/tmp/no_ext"), "no_ext");
        // Python's `os.path.splitext` keeps the inner dots — only the last
        // one becomes the extension.
        assert_eq!(project_basename("foo.bar.gp"), "foo.bar");
    }

    #[test]
    fn changespec_header_detection_requires_whitespace_then_word() {
        assert!(is_changespec_header("## ChangeSpec"));
        assert!(is_changespec_header("##  ChangeSpec"));
        assert!(is_changespec_header("##\tChangeSpec"));
        assert!(is_changespec_header("   ## ChangeSpec   "));
        // No whitespace between `##` and the word — Python's `\s+` rejects.
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
        assert_eq!(s.source_span.start_line, 2); // line after `## ChangeSpec`
        assert_eq!(s.source_span.end_line, 4); // STATUS line
        assert_eq!(s.project_basename, "myproj");
        assert_eq!(s.file_path, "myproj.gp");
        assert_eq!(s.kickstart, None);
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
        // Has STATUS but no NAME (nothing kicks off the parse loop, so
        // this is really a "no spec at all" case).
        let only_status = "STATUS: WIP\n";
        assert!(parse(only_status).is_empty());

        // Has NAME but no STATUS — must be dropped.
        let no_status = "NAME: foo\nDESCRIPTION: x\n";
        assert!(parse(no_status).is_empty());

        // Has STATUS first, then NAME: without STATUS — both dropped.
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
        // Trailing blank before STATUS gets trimmed by the final strip().
        assert_eq!(
            specs[0].description,
            "First paragraph.\n\nSecond paragraph."
        );
    }

    #[test]
    fn kickstart_inline_and_continuation() {
        let src_inline = "NAME: a\nKICKSTART: do it\nSTATUS: WIP\n";
        let s = &parse(src_inline)[0];
        assert_eq!(s.kickstart.as_deref(), Some("do it"));

        let src_block = "\
NAME: a
KICKSTART:
  step one
  step two
STATUS: WIP
";
        let s = &parse(src_block)[0];
        assert_eq!(s.kickstart.as_deref(), Some("step one\nstep two"));
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

        // CL: alias of PR:.
        let cl_src = "NAME: a\nCL: 12345\nSTATUS: WIP\n";
        assert_eq!(parse(cl_src)[0].cl_or_pr.as_deref(), Some("12345"));
    }

    #[test]
    fn empty_value_lines_become_none_or_empty_per_python_rules() {
        // Python writes `PARENT: ` as a header without trailing content;
        // here we have just `PARENT:` without a space, which Python's
        // `line.startswith("PARENT: ")` does NOT match. So those records
        // leave parent unset.
        let src = "NAME: a\nPARENT:\nPR:\nSTATUS: WIP\n";
        let s = &parse(src)[0];
        assert_eq!(s.parent, None);
        assert_eq!(s.cl_or_pr, None);

        // With the trailing space, a blank value sets parent to "".
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
        // beta starts on the line after the two blank separators.
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
        // alpha spec content is lines 2..3 (NAME, STATUS).
        assert_eq!(specs[0].source_span.start_line, 2);
        assert_eq!(specs[0].source_span.end_line, 3);
        // beta starts on line 5 (after `## ChangeSpec` on line 4).
        assert_eq!(specs[1].source_span.start_line, 5);
        assert_eq!(specs[1].source_span.end_line, 6);
    }

    #[test]
    fn new_name_inside_a_spec_starts_a_new_spec() {
        // Mirrors Python's "new NAME with one already" branch — no blank
        // lines, no `## ChangeSpec`, just two NAME records back-to-back.
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
        // The two-blank separator is *not* part of the spec's span.
        let src = "NAME: a\nSTATUS: WIP\n\n\nNAME: b\nSTATUS: WIP\n";
        let specs = parse(src);
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].source_span.end_line, 2); // STATUS, not blank
    }

    #[test]
    fn rust_end_line_is_real_not_placeholder() {
        // Python `changespec_to_wire(end_line=None)` falls back to
        // `start_line == end_line`. Rust must do better whenever the spec
        // has more than one line of content.
        let src = "## ChangeSpec\nNAME: a\nDESCRIPTION:\n  multi\n  line\nSTATUS: WIP\n";
        let s = &parse(src)[0];
        assert!(
            s.source_span.end_line > s.source_span.start_line,
            "Rust must report a real end_line, got start={} end={}",
            s.source_span.start_line,
            s.source_span.end_line
        );
        assert_eq!(s.source_span.start_line, 2); // NAME line
        assert_eq!(s.source_span.end_line, 6); // STATUS line
    }

    #[test]
    fn section_bodies_are_consumed_without_being_interpreted() {
        // Phase 1B leaves COMMITS/HOOKS/etc. unparsed (entry lists empty)
        // but they must not break scalar-field parsing of later fields.
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
KICKSTART:
  Kick this off.
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
TIMESTAMPS:
  260101_120000 STATUS WIP -> Submitted
DELTAS:
  + src/alpha.py
  ~ src/util.py
";
        let s = &parse(src)[0];
        assert_eq!(s.name, "alpha");
        assert_eq!(s.status, "Submitted");
        assert_eq!(s.description, "Initial feature work.");
        // `PARENT:` without a trailing space does NOT match Python's
        // `startswith("PARENT: ")`, so the field stays unset.
        assert_eq!(s.parent, None);
        assert_eq!(
            s.cl_or_pr.as_deref(),
            Some("https://example.test/repo/pull/1")
        );
        assert_eq!(s.bug.as_deref(), Some("BUG-100"));
        assert_eq!(s.test_targets, vec!["tests/test_alpha.py"]);
        assert_eq!(s.kickstart.as_deref(), Some("Kick this off."));
        // Phase 1B: section entries are not yet emitted.
        assert!(s.commits.is_empty());
        assert!(s.hooks.is_empty());
        assert!(s.comments.is_empty());
        assert!(s.mentors.is_empty());
        assert!(s.timestamps.is_empty());
        assert!(s.deltas.is_empty());
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

    #[test]
    fn full_golden_corpus_smoke() {
        // Reproduces `sase_100/tests/core_golden/myproj.gp` inline so this
        // test can run without filesystem access. Phase 1C will swap this
        // for an actual fixture file.
        let myproj = "\
## ChangeSpec
NAME: alpha
DESCRIPTION:
  Initial feature work.
  Spans multiple lines.
PARENT:
PR: https://example.test/repo/pull/1
BUG: BUG-100
STATUS: Submitted
TEST TARGETS: tests/test_alpha.py
KICKSTART:
  Kick this off carefully.
COMMITS:
  (1) [run] Initial Commit
      | CHAT: ~/.sase/chats/alpha.md (0s)
      | DIFF: ~/.sase/diffs/alpha.diff
HOOKS:
  just lint
      | (1) [260101_120000] PASSED (3s)
COMMENTS:
  [critique] ~/.sase/comments/alpha.json - (!: Unresolved Critique Comments)
MENTORS:
  (1) profileA[1/1]
      | [260101_130000] profileA:mentor1 - PASSED - (1m0s)
TIMESTAMPS:
  260101_120000 STATUS WIP -> Submitted
DELTAS:
  + src/alpha.py
  ~ src/util.py


NAME: beta
DESCRIPTION: Sibling feature.
PARENT: alpha
PR:
STATUS: WIP


NAME: beta__260102_010101
DESCRIPTION: Reverted retry of beta.
PARENT: alpha
PR:
STATUS: Reverted


NAME: gamma
DESCRIPTION: Ready feature with running agent.
PARENT:
PR:
STATUS: Ready
HOOKS:
  just test
      | (1) [260103_140000] RUNNING - (@: ace-260103_140000)
";
        let specs = parse(myproj);
        let names: Vec<&str> = specs.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["alpha", "beta", "beta__260102_010101", "gamma"]
        );
        assert_eq!(specs[0].status, "Submitted");
        assert_eq!(specs[1].status, "WIP");
        assert_eq!(specs[2].status, "Reverted");
        assert_eq!(specs[3].status, "Ready");
        assert_eq!(specs[1].parent.as_deref(), Some("alpha"));
        assert_eq!(specs[1].description, "Sibling feature.");

        let archive = "\
NAME: archived_one
DESCRIPTION: An archived spec.
PARENT:
PR: https://example.test/repo/pull/99
STATUS: Archived
COMMITS:
  (1) [run] Initial Commit
      | CHAT: ~/.sase/chats/archived_one.md (0s)


NAME: reverted_two
DESCRIPTION: A reverted spec.
PARENT:
PR:
STATUS: Reverted
";
        let archive_specs =
            parse_project_bytes("myproj-archive.gp", archive.as_bytes())
                .unwrap();
        assert_eq!(archive_specs.len(), 2);
        assert_eq!(archive_specs[0].project_basename, "myproj");
        assert_eq!(archive_specs[0].name, "archived_one");
        assert_eq!(archive_specs[1].name, "reverted_two");
    }
}
