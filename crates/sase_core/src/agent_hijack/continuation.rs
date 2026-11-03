//! Continuation-prompt builder for hijacked agents.
//!
//! Pure text assembly: a `%model:` line, an optional `#fork:` line, an
//! optional guidance slot, the hijack frame, then the original prompt with
//! launch-only directives stripped (outside fenced code blocks).

use serde::{Deserialize, Serialize};

/// Wire schema version for the continuation request type.
pub const AGENT_HIJACK_CONTINUATION_WIRE_SCHEMA_VERSION: u32 = 1;

/// Launch-only directive names stripped from the original prompt.
///
/// Short aliases (`i`, `c`, `m`, `e`, `w`) are stripped alongside their
/// canonical names. Workspace references (`+proj`, `#gh:`, `#git:`) and
/// fan-out syntax (`%alt`, `%(`, `%{`) are handled by the same pass and are
/// covered by dedicated tests.
pub const STRIPPED_LAUNCH_DIRECTIVES: &[&str] =
    &["id", "clan", "tab", "wait", "model", "effort"];

/// Short directive aliases stripped alongside the canonical names.
pub const STRIPPED_LAUNCH_DIRECTIVE_ALIASES: &[&str] =
    &["i", "c", "m", "e", "w"];

/// Evidence facts rendered into the hijack frame.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackEvidenceWire {
    #[serde(default)]
    pub run_duration_display: String,
    #[serde(default)]
    pub last_tool: String,
    #[serde(default)]
    pub last_tool_age: String,
    #[serde(default)]
    pub files_changed: u64,
    #[serde(default)]
    pub insertions: u64,
    #[serde(default)]
    pub deletions: u64,
    #[serde(default)]
    pub snapshot_ref: String,
    #[serde(default)]
    pub model_display: String,
}

/// Continuation prompt request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackContinuationRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub original_prompt: String,
    #[serde(default)]
    pub hijacked_name: String,
    #[serde(default)]
    pub model_target: String,
    #[serde(default)]
    pub evidence: AgentHijackEvidenceWire,
    #[serde(default)]
    pub context_mode: String,
    #[serde(default)]
    pub transcript_path: Option<String>,
    #[serde(default)]
    pub guidance_requested: bool,
    #[serde(default)]
    pub stale_lock_files: Vec<String>,
}

fn is_fence_line(line: &str) -> bool {
    line.trim_start().starts_with("```")
}

fn directive_names() -> Vec<String> {
    let mut names: Vec<String> = STRIPPED_LAUNCH_DIRECTIVES
        .iter()
        .map(|s| s.to_string())
        .collect();
    names.extend(
        STRIPPED_LAUNCH_DIRECTIVE_ALIASES
            .iter()
            .map(|s| s.to_string()),
    );
    names
}

fn strip_directive_spans(segment: &str) -> String {
    let names = directive_names();
    let mut out = segment.to_owned();
    // Parenthesized forms: %name(...), balanced one level (enough for the
    // launch grammar; nested parens are rare and the remainder is still
    // stripped by the colon/bare passes below).
    for name in &names {
        loop {
            let needle = format!("%{name}(");
            let Some(start) = out.find(&needle) else {
                break;
            };
            // Do not strip when preceded by a word character (e.g. inside a
            // token); directives start at a boundary.
            if start > 0 {
                let prev = out.as_bytes()[start - 1] as char;
                if prev.is_alphanumeric() || prev == '_' {
                    break;
                }
            }
            let mut depth = 0usize;
            let mut end: Option<usize> = None;
            for (i, ch) in out[start..].char_indices() {
                if ch == '(' {
                    depth += 1;
                } else if ch == ')' {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(start + i + 1);
                        break;
                    }
                }
            }
            match end {
                Some(e) => {
                    out.replace_range(start..e, "");
                }
                None => break,
            }
        }
    }
    // Colon/backtick/plus/bare forms. The `regex` crate has no
    // look-around, so the boundary prefix is captured and re-emitted.
    let pattern = format!(
        r#"(?m)(^|[\s\(\["'])%(?:{})(?::`[^`]*`|:[^\s`\)\]]*|\+)?"#,
        names.join("|")
    );
    let re = regex::Regex::new(&pattern).expect("directive strip regex");
    let mut result = re.replace_all(&out, "$1").into_owned();
    // Workspace references: +proj-style tokens and #gh: / #git: macros.
    let ws_re = regex::Regex::new(
        r#"(?m)(^|[\s\(\["'])\+[A-Za-z0-9_][A-Za-z0-9_.:/-]*"#,
    )
    .expect("workspace strip regex");
    result = ws_re.replace_all(&result, "$1").into_owned();
    let gh_re = regex::Regex::new(r#"(?m)(^|[\s\(\["'])#[Gg][Hh]:[^\s]*"#)
        .expect("gh strip regex");
    result = gh_re.replace_all(&result, "$1").into_owned();
    let git_re = regex::Regex::new(r#"(?m)(^|[\s\(\["'])#[Gg][Ii][Tt]:[^\s]*"#)
        .expect("git strip regex");
    result = git_re.replace_all(&result, "$1").into_owned();
    // Parenthesized %alt(...) forms (not a launch directive, but fan-out).
    loop {
        let needle = "%alt(";
        let Some(start) = result.find(needle) else {
            break;
        };
        if start > 0 {
            let prev = result.as_bytes()[start - 1] as char;
            if prev.is_alphanumeric() || prev == '_' {
                break;
            }
        }
        let mut depth = 0usize;
        let mut end: Option<usize> = None;
        for (i, ch) in result[start..].char_indices() {
            if ch == '(' {
                depth += 1;
            } else if ch == ')' {
                depth -= 1;
                if depth == 0 {
                    end = Some(start + i + 1);
                    break;
                }
            }
        }
        match end {
            Some(e) => {
                result.replace_range(start..e, "");
            }
            None => break,
        }
    }
    // Fan-out syntax: %alt(...), %alt:..., %(...), %{...}.
    // Parenthesized %alt already handled above; strip the rest.
    let fanout_re =
        regex::Regex::new(r#"(?m)(^|[\s\(\["'])%(?:alt)(?::[^\s]*)?"#)
            .expect("fanout strip regex");
    result = fanout_re.replace_all(&result, "$1").into_owned();
    let paren_re = regex::Regex::new(r#"(?m)(^|[\s\(\["'])%\([^)]*\)"#)
        .expect("paren regex");
    result = paren_re.replace_all(&result, "$1").into_owned();
    let brace_re = regex::Regex::new(r#"(?m)(^|[\s\(\["'])%\{[^}]*\}"#)
        .expect("brace regex");
    result = brace_re.replace_all(&result, "$1").into_owned();
    // Collapse 3+ blank lines to two, trim trailing whitespace per line.
    let lines: Vec<String> =
        result.lines().map(|l| l.trim_end().to_owned()).collect();
    let mut collapsed: Vec<String> = Vec::with_capacity(lines.len());
    let mut blanks = 0usize;
    for line in lines {
        if line.trim().is_empty() {
            blanks += 1;
            if blanks <= 2 {
                collapsed.push(String::new());
            }
        } else {
            blanks = 0;
            collapsed.push(line);
        }
    }
    collapsed.join("\n").trim().to_owned()
}

/// Strip launch-only directives from *prompt*, preserving fenced code blocks.
///
/// A quoted `%model:` inside a fenced block is not a directive and is kept.
pub fn strip_launch_directives(prompt: &str) -> String {
    if !prompt.contains('%') && !prompt.contains('+') && !prompt.contains('#') {
        return prompt.trim().to_owned();
    }
    let mut segments: Vec<String> = Vec::new();
    let mut current: Vec<&str> = Vec::new();
    let mut in_fence = false;
    for line in prompt.lines() {
        if is_fence_line(line) {
            if !in_fence && !current.is_empty() {
                segments.push(strip_directive_spans(&current.join("\n")));
                current.clear();
            } else if in_fence {
                segments.push(current.join("\n"));
                current.clear();
            } else {
                // Fence opener with no pending prose.
            }
            in_fence = !in_fence;
            segments.push(line.to_owned());
        } else {
            current.push(line);
        }
    }
    if !current.is_empty() {
        let tail = current.join("\n");
        if in_fence {
            segments.push(tail);
        } else {
            segments.push(strip_directive_spans(&tail));
        }
    }
    segments.join("\n").trim().to_owned()
}

fn diff_summary(evidence: &AgentHijackEvidenceWire) -> String {
    format!(
        "{} files, +{} −{}",
        evidence.files_changed, evidence.insertions, evidence.deletions
    )
}

/// Build the continuation prompt for a hijack successor.
pub fn agent_hijack_continuation_prompt(
    request: &AgentHijackContinuationRequestWire,
) -> String {
    let mut out = String::new();
    let target = request.model_target.trim();
    if !target.is_empty() {
        out.push_str(&format!("%model:{target}\n"));
    }
    if request.context_mode.trim() == "fork" {
        let name = request.hijacked_name.trim();
        if !name.is_empty() {
            out.push_str(&format!("#fork:{name}\n"));
        }
    }
    if request.guidance_requested {
        out.push('\n');
    }
    let evidence = &request.evidence;
    let model = if evidence.model_display.trim().is_empty() {
        target.to_owned()
    } else {
        evidence.model_display.trim().to_owned()
    };
    let name = request.hijacked_name.trim();
    let duration = evidence.run_duration_display.trim();
    let last_tool = evidence.last_tool.trim();
    let last_age = evidence.last_tool_age.trim();
    let activity = if last_tool.is_empty() {
        "no recorded tool activity".to_owned()
    } else if last_age.is_empty() {
        format!("last activity: {last_tool}")
    } else {
        format!("last activity: {last_tool}, {last_age} before the hijack")
    };
    let snapshot = evidence.snapshot_ref.trim();
    let snapshot_text = if snapshot.is_empty() {
        "none yet".to_owned()
    } else {
        format!("`{snapshot}`")
    };
    out.push_str("**Hijack continuation.** ");
    out.push_str(&format!(
        "The user stopped `{name}` (`{model}`) because it stopped making progress \
(ran {duration}; {activity}). You are its successor in the same workspace. \
Its uncommitted changes are still here ({}; recovery snapshot: {snapshot_text}). \
Inspect `git status` and `git diff` before acting. Keep correct work, repair or \
revert wrong work, and do not repeat whatever stalled the previous run. \
The original task follows, unchanged.",
        diff_summary(evidence),
    ));
    if request.context_mode.trim() == "brief" {
        if let Some(path) = request.transcript_path.as_deref() {
            let trimmed = path.trim();
            if !trimmed.is_empty() {
                out.push_str(&format!(" Transcript: `{trimmed}`."));
            }
        }
    }
    if !request.stale_lock_files.is_empty() {
        let files: Vec<String> = request
            .stale_lock_files
            .iter()
            .map(|f| format!("`{f}`"))
            .collect();
        out.push_str(&format!(
            " Stale lock files remain at {} — do not delete them; work around them.",
            files.join(", ")
        ));
    }
    out.push('\n');
    let cleaned = strip_launch_directives(&request.original_prompt);
    if !cleaned.is_empty() {
        out.push('\n');
        out.push_str(&cleaned);
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_request() -> AgentHijackContinuationRequestWire {
        AgentHijackContinuationRequestWire {
            schema_version: AGENT_HIJACK_CONTINUATION_WIRE_SCHEMA_VERSION,
            original_prompt: "Do the thing.\n".to_owned(),
            hijacked_name: "sase-7f.2".to_owned(),
            model_target: "claude/opus@xhigh".to_owned(),
            evidence: AgentHijackEvidenceWire {
                run_duration_display: "47m".to_owned(),
                last_tool: "Bash `just check`".to_owned(),
                last_tool_age: "22m".to_owned(),
                files_changed: 5,
                insertions: 212,
                deletions: 31,
                snapshot_ref: "snap-abc123".to_owned(),
                model_display: "claude/opus@high".to_owned(),
            },
            context_mode: "fork".to_owned(),
            transcript_path: None,
            guidance_requested: false,
            stale_lock_files: vec![],
        }
    }

    #[test]
    fn fork_mode_emits_model_and_fork_lines() {
        let out = agent_hijack_continuation_prompt(&base_request());
        let mut lines = out.lines();
        assert_eq!(lines.next(), Some("%model:claude/opus@xhigh"));
        assert_eq!(lines.next(), Some("#fork:sase-7f.2"));
        assert!(out.contains("**Hijack continuation.**"));
        assert!(out.contains("sase-7f.2"));
        assert!(out.contains("Do the thing."));
    }

    #[test]
    fn brief_mode_omits_fork_and_adds_transcript() {
        let mut req = base_request();
        req.context_mode = "brief".to_owned();
        req.transcript_path = Some("/tmp/transcript.md".to_owned());
        let out = agent_hijack_continuation_prompt(&req);
        assert!(out.starts_with("%model:claude/opus@xhigh\n"));
        assert!(!out.contains("#fork:"));
        assert!(out.contains("Transcript: `/tmp/transcript.md`"));
    }

    #[test]
    fn guidance_slot_adds_blank_line() {
        let mut req = base_request();
        req.guidance_requested = true;
        let out = agent_hijack_continuation_prompt(&req);
        assert!(out.contains("#fork:sase-7f.2\n\n**Hijack continuation.**"));
    }

    #[test]
    fn stale_locks_are_mentioned_not_deleted() {
        let mut req = base_request();
        req.stale_lock_files = vec!["a/index.lock".to_owned()];
        let out = agent_hijack_continuation_prompt(&req);
        assert!(out.contains("Stale lock files remain"));
        assert!(out.contains("do not delete them"));
        assert!(out.contains("a/index.lock"));
    }

    #[test]
    fn strips_each_launch_directive() {
        for directive in STRIPPED_LAUNCH_DIRECTIVES {
            for spelling in [
                format!("%{directive}:value"),
                format!("%{directive}(value)"),
                format!("%{directive}+"),
            ] {
                let prompt = format!("Do work.\n{spelling}\nMore work.\n");
                let cleaned = strip_launch_directives(&prompt);
                assert!(
                    !cleaned.contains(&spelling),
                    "directive {spelling} survived"
                );
                assert!(cleaned.contains("Do work."));
                assert!(cleaned.contains("More work."));
            }
        }
    }

    #[test]
    fn strips_short_aliases() {
        let prompt = "Work.\n%w:other\n%m:@large\n%e:high\n%i:name\n%c:clan\n%tab:main\nDone.\n";
        let cleaned = strip_launch_directives(prompt);
        assert!(!cleaned.contains("%w:"));
        assert!(!cleaned.contains("%m:"));
        assert!(!cleaned.contains("%e:"));
        assert!(cleaned.contains("Work."));
        assert!(cleaned.contains("Done."));
    }

    #[test]
    fn strips_workspace_refs() {
        let prompt = "Work here +proj/foo and #gh:123 and #git:abc.\nDone.\n";
        let cleaned = strip_launch_directives(prompt);
        assert!(!cleaned.contains("+proj/foo"));
        assert!(!cleaned.contains("#gh:123"));
        assert!(!cleaned.contains("#git:abc"));
    }

    #[test]
    fn strips_fanout_syntax() {
        let prompt = "Work.\n%alt(a, b)\n%(x, y)\n%{a | b}\nDone.\n";
        let cleaned = strip_launch_directives(prompt);
        assert!(!cleaned.contains("%alt"));
        assert!(!cleaned.contains("%{"));
        assert!(cleaned.contains("Done."));
    }

    #[test]
    fn quoted_model_in_code_block_is_kept() {
        let prompt =
            "Intro.\n```\n%model:keep-me\n```\n%model:strip-me\nOutro.\n";
        let cleaned = strip_launch_directives(prompt);
        assert!(cleaned.contains("%model:keep-me"));
        assert!(!cleaned.contains("%model:strip-me"));
    }
}
