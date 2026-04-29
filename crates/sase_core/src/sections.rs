//! Per-section line parsers — direct ports of
//! `sase_100/src/sase/ace/changespec/section_parsers.py`.
//!
//! Each function reads one line at a time and folds it into the parser
//! state's running entry collections. Unknown shapes are quietly ignored
//! (matching Python), which keeps state in the same section across noise
//! such as blank lines or structural padding.

use regex::Regex;
use std::sync::OnceLock;

use crate::suffix::{is_entry_ref_suffix, parse_suffix_prefix};
use crate::wire::{
    CommentWire, CommitWire, DeltaWire, HookStatusLineWire, HookWire,
    MentorStatusLineWire, MentorWire, TimestampWire,
};

// -- COMMITS ----------------------------------------------------------------

/// Mirrors `parse_commits_line` in `section_parsers.py`.
///
/// Recognized line shapes (in priority order):
///
/// - `(N)` / `(Na) <note>` — a new entry header. Saves any in-progress
///   entry into `commits` and starts a new `current` with the note (and
///   parsed inline suffix).
/// - `| CHAT:` / `| DIFF:` / `| PLAN:` — drawer lines that attach to the
///   current entry.
/// - 6-space-indented body lines (not drawers) — appended to the
///   current entry's `body`. A body line whose stripped form is `.`
///   becomes an empty string.
pub fn parse_commits_line(
    line: &str,
    stripped: &str,
    current: &mut Option<CommitWire>,
    commits: &mut Vec<CommitWire>,
) {
    static ENTRY_RE: OnceLock<Regex> = OnceLock::new();
    let entry_re = ENTRY_RE
        .get_or_init(|| Regex::new(r"^\((\d+)([a-z])?\)\s+(.+)$").unwrap());
    static SUFFIX_RE: OnceLock<Regex> = OnceLock::new();
    let suffix_re = SUFFIX_RE.get_or_init(|| {
        // `\s+-\s+\((~!:|!:|~:|@:)?\s*([^)]+)\)$`
        Regex::new(r"\s+-\s+\((~!:|!:|~:|@:)?\s*([^)]+)\)$").unwrap()
    });

    if let Some(caps) = entry_re.captures(stripped) {
        // Save previous entry if any.
        if let Some(prev) = current.take() {
            commits.push(prev);
        }
        let number: u32 = caps.get(1).unwrap().as_str().parse().unwrap_or(0);
        let proposal_letter = caps.get(2).map(|m| m.as_str().to_string());
        let raw_note = caps.get(3).unwrap().as_str();

        let (note, suffix, suffix_type) = match suffix_re.captures(raw_note) {
            Some(scaps) => {
                let prefix_match = scaps.get(1).map(|m| m.as_str());
                let mut suffix_msg =
                    scaps.get(2).unwrap().as_str().trim().to_string();
                let mut suffix_type_val: Option<String> = match prefix_match {
                    Some("~!:") => Some("rejected_proposal".to_string()),
                    Some("!:") => Some("error".to_string()),
                    Some("~:") => None,
                    Some("@:") => Some("running_agent".to_string()),
                    _ => None,
                };
                // Standalone "@" marker promotes to running_agent with empty msg.
                if suffix_msg == "@" {
                    suffix_msg = String::new();
                    suffix_type_val = Some("running_agent".to_string());
                }
                let note_end = scaps.get(0).unwrap().start();
                (
                    raw_note[..note_end].to_string(),
                    Some(suffix_msg),
                    suffix_type_val,
                )
            }
            None => (raw_note.to_string(), None, None),
        };

        *current = Some(CommitWire {
            number,
            note,
            chat: None,
            diff: None,
            plan: None,
            proposal_letter,
            suffix,
            suffix_type,
            body: vec![],
        });
        return;
    }

    if let Some(rest) = stripped.strip_prefix("| CHAT:") {
        if let Some(c) = current.as_mut() {
            c.chat = Some(rest.trim().to_string());
        }
        return;
    }
    if let Some(rest) = stripped.strip_prefix("| DIFF:") {
        if let Some(c) = current.as_mut() {
            c.diff = Some(rest.trim().to_string());
        }
        return;
    }
    if let Some(rest) = stripped.strip_prefix("| PLAN:") {
        if let Some(c) = current.as_mut() {
            c.plan = Some(rest.trim().to_string());
        }
        return;
    }

    // 6-space body continuation, not a drawer.
    if line.starts_with("      ") && !stripped.starts_with("| ") {
        if let Some(c) = current.as_mut() {
            if stripped == "." {
                c.body.push(String::new());
            } else {
                c.body.push(stripped.to_string());
            }
        }
    }
}

// -- HOOKS ------------------------------------------------------------------

/// Mirrors `parse_hooks_line` in `section_parsers.py`.
pub fn parse_hooks_line(
    line: &str,
    stripped: &str,
    current: &mut Option<HookWire>,
    hooks: &mut Vec<HookWire>,
) {
    static STATUS_RE: OnceLock<Regex> = OnceLock::new();
    let status_re = STATUS_RE.get_or_init(|| {
        Regex::new(
            r"^\((\d+[a-z]?)\)\s+\[(\d{6})_(\d{6})\]\s*(RUNNING|PASSED|FAILED|DEAD)(?:\s+\(([^)]+)\))?(?:\s+-\s+\((.+)\))?$",
        )
        .unwrap()
    });

    // Command line: 2-space indented, NOT 4-space indented, and not starting
    // with `[` or `(` (which would be status-line markers).
    if line.starts_with("  ")
        && !line.starts_with("    ")
        && !stripped.starts_with('[')
        && !stripped.starts_with('(')
    {
        if let Some(prev) = current.take() {
            hooks.push(prev);
        }
        // Test-target shorthand expansion is sase-side concern (it lives in
        // `sase.ace.hooks.test_targets`). For the wire contract we keep the
        // raw command verbatim — this matches what the Python parser does
        // before shorthand expansion is applied during wire conversion.
        *current = Some(HookWire {
            command: stripped.to_string(),
            status_lines: vec![],
        });
        return;
    }

    // Status line: 6-space + "| " prefix.
    if line.starts_with("      | ") {
        // Python: `status_content = stripped[2:]` — strip the leading "| ".
        let status_content = &stripped[2..];
        let Some(caps) = status_re.captures(status_content) else {
            return;
        };
        let Some(hook) = current.as_mut() else {
            return;
        };
        let commit_num = caps.get(1).unwrap().as_str().to_string();
        let timestamp = format!(
            "{}_{}",
            caps.get(2).unwrap().as_str(),
            caps.get(3).unwrap().as_str()
        );
        let status = caps.get(4).unwrap().as_str().to_string();
        let duration = caps.get(5).map(|m| m.as_str().to_string());
        let raw_suffix = caps.get(6).map(|m| m.as_str().to_string());

        let mut suffix_str = raw_suffix;
        let mut summary: Option<String> = None;
        if let Some(s) = suffix_str.as_ref() {
            if let Some(idx) = s.find(" | ") {
                let before = s[..idx].to_string();
                let after = s[idx + 3..].to_string();
                suffix_str = Some(before);
                summary = Some(after);
            }
        }
        let parsed = parse_suffix_prefix(suffix_str.as_deref());

        hook.status_lines.push(HookStatusLineWire {
            commit_entry_num: commit_num,
            timestamp,
            status,
            duration,
            suffix: parsed.value,
            suffix_type: parsed.suffix_type,
            summary,
        });
    }
}

// -- COMMENTS ---------------------------------------------------------------

/// Mirrors `parse_comments_line` in `section_parsers.py`.
pub fn parse_comments_line(
    line: &str,
    stripped: &str,
    comments: &mut Vec<CommentWire>,
) {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(r"^\[([^\]]+)\]\s+(\S+)(?:\s+-\s+\(([^)]+)\))?$").unwrap()
    });

    if !line.starts_with("  ") || line.starts_with("    ") {
        return;
    }
    let Some(caps) = re.captures(stripped) else {
        return;
    };
    let reviewer = caps.get(1).unwrap().as_str().to_string();
    let file_path = caps.get(2).unwrap().as_str().to_string();
    let raw_suffix = caps.get(3).map(|m| m.as_str());
    let parsed = parse_suffix_prefix(raw_suffix);
    comments.push(CommentWire {
        reviewer,
        file_path,
        suffix: parsed.value,
        suffix_type: parsed.suffix_type,
    });
}

// -- MENTORS ----------------------------------------------------------------

/// Mirrors `parse_mentors_line` in `section_parsers.py`.
pub fn parse_mentors_line(
    line: &str,
    stripped: &str,
    current: &mut Option<MentorWire>,
    mentors: &mut Vec<MentorWire>,
) {
    static ENTRY_RE: OnceLock<Regex> = OnceLock::new();
    let entry_re = ENTRY_RE
        .get_or_init(|| Regex::new(r"^\((\d+[a-z]?)\)\s+(.+)$").unwrap());
    static PROFILE_RE: OnceLock<Regex> = OnceLock::new();
    let profile_re =
        PROFILE_RE.get_or_init(|| Regex::new(r"(\w+)\[\d+/\d+\]").unwrap());
    static STATUS_RE: OnceLock<Regex> = OnceLock::new();
    let status_re = STATUS_RE.get_or_init(|| {
        Regex::new(
            r"^(?:\[(\d{6}_\d{6})\]\s+)?([^:]+):(\S+)\s+-\s+(STARTING|RUNNING|PASSED|COMMENTED|FAILED|DEAD|KILLED)(?:\s+-\s+\(([^)]+)\))?$",
        )
        .unwrap()
    });

    // Entry line: 2-space indented, NOT 6-space indented.
    if line.starts_with("  ") && !line.starts_with("      ") {
        let Some(caps) = entry_re.captures(stripped) else {
            return;
        };
        if let Some(prev) = current.take() {
            mentors.push(prev);
        }
        let entry_id = caps.get(1).unwrap().as_str().to_string();
        let mut profiles_raw = caps.get(2).unwrap().as_str().to_string();

        // Detect and strip a trailing #Draft marker. Python uses
        // `profiles_raw.rstrip().endswith("#Draft")` then
        // `profiles_raw.replace(" #Draft", "").rstrip()`.
        let trimmed_right = profiles_raw.trim_end();
        let is_draft = trimmed_right.ends_with("#Draft");
        if is_draft {
            profiles_raw =
                profiles_raw.replace(" #Draft", "").trim_end().to_string();
        }

        let mut profiles: Vec<String> = profile_re
            .captures_iter(&profiles_raw)
            .map(|c| c.get(1).unwrap().as_str().to_string())
            .collect();
        if profiles.is_empty() {
            // Legacy format: whitespace-separated profile names.
            profiles = profiles_raw
                .split_whitespace()
                .map(|s| s.to_string())
                .collect();
        }
        *current = Some(MentorWire {
            entry_id,
            profiles,
            status_lines: vec![],
            is_draft,
        });
        return;
    }

    // Status line: 6-space + "| " prefix.
    if line.starts_with("      | ") {
        let status_content = &stripped[2..];
        let Some(caps) = status_re.captures(status_content) else {
            return;
        };
        let Some(mentor) = current.as_mut() else {
            return;
        };
        let timestamp = caps
            .get(1)
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();
        let profile_name = caps.get(2).unwrap().as_str().to_string();
        let mentor_name = caps.get(3).unwrap().as_str().to_string();
        let status = caps.get(4).unwrap().as_str().to_string();
        let raw_suffix = caps.get(5).map(|m| m.as_str().to_string());

        let mut suffix_val: Option<String> = None;
        let mut suffix_type_val: Option<String> = None;
        let mut duration_val: Option<String> = None;

        if let Some(raw) = raw_suffix {
            let parsed = parse_suffix_prefix(Some(&raw));
            if parsed.suffix_type.is_some() {
                suffix_val = parsed.value;
                suffix_type_val = parsed.suffix_type;
            } else if is_entry_ref_suffix(Some(&raw)) {
                // Entry reference (e.g. "2a") — keep as suffix, mark type.
                suffix_val = Some(raw);
                suffix_type_val = Some("entry_ref".to_string());
            } else {
                // Plain suffix — almost always a duration.
                duration_val = Some(raw);
                suffix_val = None;
                suffix_type_val = Some("plain".to_string());
            }
        }

        mentor.status_lines.push(MentorStatusLineWire {
            profile_name,
            mentor_name,
            status,
            timestamp,
            duration: duration_val,
            suffix: suffix_val,
            suffix_type: suffix_type_val,
        });
    }
}

// -- TIMESTAMPS -------------------------------------------------------------

/// Mirrors `parse_timestamps_line` in `section_parsers.py`. Accepts the
/// new (`YYMMDD_HHMMSS EVENT detail`) format plus the legacy bracketed
/// formats so the parser stays drop-in compatible with on-disk archives.
pub fn parse_timestamps_line(
    line: &str,
    stripped: &str,
    timestamps: &mut Vec<TimestampWire>,
) {
    static NEW_RE: OnceLock<Regex> = OnceLock::new();
    let new_re = NEW_RE.get_or_init(|| {
        Regex::new(
            r"^(\d{6}_\d{6})\s+(COMMIT|STATUS|SYNC|REWORD|REWIND|RENAME)\s+(.+)$",
        )
        .unwrap()
    });
    static OLD_RE: OnceLock<Regex> = OnceLock::new();
    let old_re = OLD_RE.get_or_init(|| {
        Regex::new(
            r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s+(COMMIT|STATUS|SYNC|REWORD|REWIND|RENAME)\s+(.+)$",
        )
        .unwrap()
    });
    static HYBRID_RE: OnceLock<Regex> = OnceLock::new();
    let hybrid_re = HYBRID_RE.get_or_init(|| {
        Regex::new(
            r"^\[(\d{6}_\d{6})\]\s+(COMMIT|STATUS|SYNC|REWORD|REWIND|RENAME)\s+(.+)$",
        )
        .unwrap()
    });

    if !line.starts_with("  ") {
        return;
    }
    let caps = new_re
        .captures(stripped)
        .or_else(|| old_re.captures(stripped))
        .or_else(|| hybrid_re.captures(stripped));
    let Some(caps) = caps else {
        return;
    };
    timestamps.push(TimestampWire {
        timestamp: caps.get(1).unwrap().as_str().to_string(),
        event_type: caps.get(2).unwrap().as_str().to_string(),
        // Python's `.rstrip()` removes any trailing whitespace.
        detail: caps.get(3).unwrap().as_str().trim_end().to_string(),
    });
}

// -- DELTAS -----------------------------------------------------------------

/// Mirrors `parse_deltas_line` in `section_parsers.py`. Format is
/// `  <glyph> <path>` where glyph is one of `+`/`~`/`-`. The on-disk
/// glyph maps to the long-form change_type stored on the wire.
pub fn parse_deltas_line(line: &str, deltas: &mut Vec<DeltaWire>) {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"^  ([+~\-]) (.+)$").unwrap());

    // Exactly two leading spaces — matches Python's
    // `if not (line.startswith("  ") and not line.startswith("   "))`.
    if !line.starts_with("  ") || line.starts_with("   ") {
        return;
    }
    let Some(caps) = re.captures(line) else {
        return;
    };
    let glyph = caps.get(1).unwrap().as_str();
    let path = caps.get(2).unwrap().as_str().to_string();
    let change_type = match glyph {
        "+" => "A",
        "~" => "M",
        "-" => "D",
        _ => return, // unreachable due to regex
    };
    deltas.push(DeltaWire {
        path,
        change_type: change_type.to_string(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> String {
        v.to_string()
    }

    // -- COMMITS ----------------------------------------------------------

    #[test]
    fn commits_line_starts_new_entry_and_strips_trailing_suffix() {
        let mut current: Option<CommitWire> = None;
        let mut commits: Vec<CommitWire> = vec![];
        let line = "  (1) initial commit - (!: bad)";
        parse_commits_line(line, line.trim(), &mut current, &mut commits);
        let c = current.as_ref().unwrap();
        assert_eq!(c.number, 1);
        assert_eq!(c.note, "initial commit");
        assert_eq!(c.suffix.as_deref(), Some("bad"));
        assert_eq!(c.suffix_type.as_deref(), Some("error"));
        assert!(commits.is_empty());
    }

    #[test]
    fn commits_proposal_letter_carries_through() {
        let mut current = None;
        let mut commits = vec![];
        let line = "  (2a) proposed work";
        parse_commits_line(line, line.trim(), &mut current, &mut commits);
        let c = current.as_ref().unwrap();
        assert_eq!(c.number, 2);
        assert_eq!(c.proposal_letter.as_deref(), Some("a"));
        assert_eq!(c.note, "proposed work");
    }

    #[test]
    fn commits_drawers_attach_to_current_entry() {
        let mut current = None;
        let mut commits = vec![];
        let entry = "  (1) work";
        parse_commits_line(entry, entry.trim(), &mut current, &mut commits);
        let chat_line = "      | CHAT: ~/.sase/chats/foo.md";
        parse_commits_line(
            chat_line,
            chat_line.trim(),
            &mut current,
            &mut commits,
        );
        let diff_line = "      | DIFF: ~/.sase/diffs/foo.diff";
        parse_commits_line(
            diff_line,
            diff_line.trim(),
            &mut current,
            &mut commits,
        );
        let plan_line = "      | PLAN: ~/.sase/plans/foo.md";
        parse_commits_line(
            plan_line,
            plan_line.trim(),
            &mut current,
            &mut commits,
        );
        let c = current.as_ref().unwrap();
        assert_eq!(c.chat.as_deref(), Some("~/.sase/chats/foo.md"));
        assert_eq!(c.diff.as_deref(), Some("~/.sase/diffs/foo.diff"));
        assert_eq!(c.plan.as_deref(), Some("~/.sase/plans/foo.md"));
    }

    #[test]
    fn commits_body_lines_use_six_space_indent_and_dot_for_blank() {
        let mut current = None;
        let mut commits = vec![];
        let entry = "  (1) work";
        parse_commits_line(entry, entry.trim(), &mut current, &mut commits);
        let body1 = "      first line";
        parse_commits_line(body1, body1.trim(), &mut current, &mut commits);
        let dot = "      .";
        parse_commits_line(dot, dot.trim(), &mut current, &mut commits);
        let body2 = "      after blank";
        parse_commits_line(body2, body2.trim(), &mut current, &mut commits);
        let c = current.as_ref().unwrap();
        assert_eq!(c.body, vec![s("first line"), s(""), s("after blank")]);
    }

    #[test]
    fn commits_running_agent_and_rejected_proposal_suffixes() {
        let mut current = None;
        let mut commits = vec![];
        let l1 = "  (1) work - (@: ace-12345-260101_010101)";
        parse_commits_line(l1, l1.trim(), &mut current, &mut commits);
        assert_eq!(
            current.as_ref().unwrap().suffix_type.as_deref(),
            Some("running_agent")
        );
        let l2 = "  (2) work - (~!: rejected)";
        parse_commits_line(l2, l2.trim(), &mut current, &mut commits);
        // Starting a new entry pushes the previous one.
        assert_eq!(commits.len(), 1);
        assert_eq!(
            current.as_ref().unwrap().suffix_type.as_deref(),
            Some("rejected_proposal")
        );
    }

    // -- HOOKS ------------------------------------------------------------

    #[test]
    fn hooks_command_then_status_line() {
        let mut current = None;
        let mut hooks = vec![];
        let cmd = "  just lint";
        parse_hooks_line(cmd, cmd.trim(), &mut current, &mut hooks);
        let status = "      | (1) [260101_120000] PASSED (3s)";
        parse_hooks_line(status, status.trim(), &mut current, &mut hooks);
        let h = current.as_ref().unwrap();
        assert_eq!(h.command, "just lint");
        assert_eq!(h.status_lines.len(), 1);
        let sl = &h.status_lines[0];
        assert_eq!(sl.commit_entry_num, "1");
        assert_eq!(sl.timestamp, "260101_120000");
        assert_eq!(sl.status, "PASSED");
        assert_eq!(sl.duration.as_deref(), Some("3s"));
    }

    #[test]
    fn hooks_running_agent_suffix_split_from_summary() {
        let mut current = None;
        let mut hooks = vec![];
        let cmd = "  just test";
        parse_hooks_line(cmd, cmd.trim(), &mut current, &mut hooks);
        let status =
            "      | (1) [260103_140000] RUNNING - (@: ace-260103_140000 | progress note)";
        parse_hooks_line(status, status.trim(), &mut current, &mut hooks);
        let sl = &current.as_ref().unwrap().status_lines[0];
        assert_eq!(sl.suffix.as_deref(), Some("ace-260103_140000"));
        assert_eq!(sl.suffix_type.as_deref(), Some("running_agent"));
        assert_eq!(sl.summary.as_deref(), Some("progress note"));
    }

    #[test]
    fn hooks_new_command_flushes_previous_hook() {
        let mut current = None;
        let mut hooks = vec![];
        let c1 = "  just lint";
        parse_hooks_line(c1, c1.trim(), &mut current, &mut hooks);
        let c2 = "  just test";
        parse_hooks_line(c2, c2.trim(), &mut current, &mut hooks);
        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks[0].command, "just lint");
        assert_eq!(current.as_ref().unwrap().command, "just test");
    }

    // -- COMMENTS ---------------------------------------------------------

    #[test]
    fn comments_basic_and_with_error_suffix() {
        let mut comments = vec![];
        let l1 = "  [critique] ~/.sase/comments/x.json";
        parse_comments_line(l1, l1.trim(), &mut comments);
        assert_eq!(comments.len(), 1);
        assert_eq!(comments[0].reviewer, "critique");
        assert_eq!(comments[0].file_path, "~/.sase/comments/x.json");
        assert!(comments[0].suffix.is_none());

        let l2 = "  [critique] ~/.sase/comments/y.json - (!: Unresolved Critique Comments)";
        parse_comments_line(l2, l2.trim(), &mut comments);
        assert_eq!(
            comments[1].suffix.as_deref(),
            Some("Unresolved Critique Comments")
        );
        assert_eq!(comments[1].suffix_type.as_deref(), Some("error"));
    }

    // -- MENTORS ----------------------------------------------------------

    #[test]
    fn mentors_entry_with_count_then_status() {
        let mut current = None;
        let mut mentors = vec![];
        let entry = "  (1) profileA[1/1]";
        parse_mentors_line(entry, entry.trim(), &mut current, &mut mentors);
        let status =
            "      | [260101_130000] profileA:mentor1 - PASSED - (1m0s)";
        parse_mentors_line(status, status.trim(), &mut current, &mut mentors);
        let m = current.as_ref().unwrap();
        assert_eq!(m.entry_id, "1");
        assert_eq!(m.profiles, vec![s("profileA")]);
        assert!(!m.is_draft);
        let sl = &m.status_lines[0];
        assert_eq!(sl.profile_name, "profileA");
        assert_eq!(sl.mentor_name, "mentor1");
        assert_eq!(sl.status, "PASSED");
        assert_eq!(sl.timestamp, "260101_130000");
        // Plain duration → suffix_type "plain", duration set, suffix unset.
        assert_eq!(sl.duration.as_deref(), Some("1m0s"));
        assert_eq!(sl.suffix, None);
        assert_eq!(sl.suffix_type.as_deref(), Some("plain"));
    }

    #[test]
    fn mentors_entry_ref_suffix_marks_type_entry_ref() {
        let mut current = None;
        let mut mentors = vec![];
        let entry = "  (1) profileA[1/1]";
        parse_mentors_line(entry, entry.trim(), &mut current, &mut mentors);
        let status =
            "      | [260101_130000] profileA:mentor1 - COMMENTED - (2a)";
        parse_mentors_line(status, status.trim(), &mut current, &mut mentors);
        let sl = &current.as_ref().unwrap().status_lines[0];
        assert_eq!(sl.suffix.as_deref(), Some("2a"));
        assert_eq!(sl.suffix_type.as_deref(), Some("entry_ref"));
        assert!(sl.duration.is_none());
    }

    #[test]
    fn mentors_legacy_profiles_without_counts_split_on_whitespace() {
        let mut current = None;
        let mut mentors = vec![];
        let entry = "  (1) profileA profileB";
        parse_mentors_line(entry, entry.trim(), &mut current, &mut mentors);
        let m = current.as_ref().unwrap();
        assert_eq!(m.profiles, vec![s("profileA"), s("profileB")]);
    }

    #[test]
    fn mentors_draft_marker_is_stripped() {
        let mut current = None;
        let mut mentors = vec![];
        let entry = "  (1) profileA[1/1] #Draft";
        parse_mentors_line(entry, entry.trim(), &mut current, &mut mentors);
        let m = current.as_ref().unwrap();
        assert!(m.is_draft);
        assert_eq!(m.profiles, vec![s("profileA")]);
    }

    // -- TIMESTAMPS -------------------------------------------------------

    #[test]
    fn timestamps_new_format_parses() {
        let mut ts = vec![];
        let l = "  260101_120000 STATUS WIP -> Submitted";
        parse_timestamps_line(l, l.trim(), &mut ts);
        assert_eq!(ts.len(), 1);
        assert_eq!(ts[0].timestamp, "260101_120000");
        assert_eq!(ts[0].event_type, "STATUS");
        assert_eq!(ts[0].detail, "WIP -> Submitted");
    }

    #[test]
    fn timestamps_legacy_bracketed_format_still_parses() {
        let mut ts = vec![];
        let l = "  [2026-01-01 12:00:00] COMMIT (1)";
        parse_timestamps_line(l, l.trim(), &mut ts);
        assert_eq!(ts.len(), 1);
        assert_eq!(ts[0].timestamp, "2026-01-01 12:00:00");
        assert_eq!(ts[0].event_type, "COMMIT");
        assert_eq!(ts[0].detail, "(1)");
    }

    #[test]
    fn timestamps_hybrid_bracketed_yymmdd_parses() {
        let mut ts = vec![];
        let l = "  [260101_120000] SYNC (2)";
        parse_timestamps_line(l, l.trim(), &mut ts);
        assert_eq!(ts.len(), 1);
        assert_eq!(ts[0].timestamp, "260101_120000");
        assert_eq!(ts[0].event_type, "SYNC");
        assert_eq!(ts[0].detail, "(2)");
    }

    // -- DELTAS -----------------------------------------------------------

    #[test]
    fn deltas_glyph_to_change_type_mapping() {
        let mut d = vec![];
        parse_deltas_line("  + src/a.py", &mut d);
        parse_deltas_line("  ~ src/b.py", &mut d);
        parse_deltas_line("  - src/c.py", &mut d);
        assert_eq!(d.len(), 3);
        assert_eq!(d[0].path, "src/a.py");
        assert_eq!(d[0].change_type, "A");
        assert_eq!(d[1].change_type, "M");
        assert_eq!(d[2].change_type, "D");
    }

    #[test]
    fn deltas_requires_exactly_two_leading_spaces() {
        let mut d = vec![];
        parse_deltas_line("   + src/three_spaces.py", &mut d);
        parse_deltas_line("+ src/no_indent.py", &mut d);
        assert!(d.is_empty());
    }
}
