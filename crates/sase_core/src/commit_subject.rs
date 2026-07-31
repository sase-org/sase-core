//! Structural parser for Conventional Commit subject lines.
//!
//! The domain model is deliberately pure: it validates only the first subject
//! line of a commit message and has no knowledge of Git, repositories, or
//! project configuration.

use serde::{Deserialize, Serialize};

pub const COMMIT_SUBJECT_WIRE_SCHEMA_VERSION: u32 = 1;

const DEFAULT_COMMIT_SUBJECT_TYPES: &[&str] = &[
    "build", "chore", "ci", "deps", "docs", "feat", "fix", "perf", "refactor",
    "revert", "style", "test",
];

const EXEMPT_SUBJECT_PREFIXES: &[&str] =
    &["Merge ", "Revert \"", "fixup!", "squash!", "amend!"];

/// The parsed result of validating a Conventional Commit subject line.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitSubjectWire {
    pub schema_version: u32,
    /// The first trimmed line of the message, as validated.
    pub subject: String,
    pub valid: bool,
    /// True when an exempt prefix skipped validation (`valid` is also true).
    pub exempt: bool,
    pub commit_type: Option<String>,
    pub scope: Option<String>,
    pub breaking: bool,
    pub description: Option<String>,
    /// Stable machine code, `None` when `valid`.
    pub violation: Option<String>,
    /// The offending type as written, when `violation` concerns the type.
    pub found_type: Option<String>,
}

/// Default Conventional Commit types accepted when a project configures none.
pub fn default_commit_subject_types() -> Vec<String> {
    DEFAULT_COMMIT_SUBJECT_TYPES
        .iter()
        .map(|value| (*value).to_string())
        .collect()
}

/// Parse and validate the first subject line of `message`.
pub fn parse_commit_subject(
    message: &str,
    allowed_types: &[String],
) -> CommitSubjectWire {
    let trimmed = message.trim();
    let subject = trimmed
        .split_once('\n')
        .map_or(trimmed, |(first, _)| first)
        .trim()
        .to_string();

    if subject.is_empty() {
        return invalid_subject(&subject, "empty_subject", None);
    }

    if EXEMPT_SUBJECT_PREFIXES
        .iter()
        .any(|prefix| subject.starts_with(prefix))
    {
        return CommitSubjectWire {
            schema_version: COMMIT_SUBJECT_WIRE_SCHEMA_VERSION,
            subject,
            valid: true,
            exempt: true,
            commit_type: None,
            scope: None,
            breaking: false,
            description: None,
            violation: None,
            found_type: None,
        };
    }

    let Some((header, raw_description)) = subject.split_once(':') else {
        return invalid_subject(&subject, "missing_type_separator", None);
    };
    let Some((commit_type, scope, breaking)) = parse_header(header) else {
        return invalid_subject(&subject, "missing_type_separator", None);
    };

    if commit_type.chars().any(char::is_uppercase) {
        return invalid_subject(
            &subject,
            "uppercase_type",
            Some(commit_type.to_string()),
        );
    }

    if !allowed_types.iter().any(|value| value == commit_type) {
        return invalid_subject(
            &subject,
            "unknown_type",
            Some(commit_type.to_string()),
        );
    }

    let description = raw_description.trim();
    if description.is_empty() {
        return invalid_subject(&subject, "empty_description", None);
    }
    if !raw_description.starts_with(' ') {
        return invalid_subject(&subject, "missing_type_separator", None);
    }

    let commit_type = commit_type.to_string();
    let scope = scope.map(str::to_string);
    let description = description.to_string();

    CommitSubjectWire {
        schema_version: COMMIT_SUBJECT_WIRE_SCHEMA_VERSION,
        subject,
        valid: true,
        exempt: false,
        commit_type: Some(commit_type),
        scope,
        breaking,
        description: Some(description),
        violation: None,
        found_type: None,
    }
}

fn parse_header(header: &str) -> Option<(&str, Option<&str>, bool)> {
    let type_end = header
        .char_indices()
        .find_map(|(index, ch)| (!ch.is_ascii_alphabetic()).then_some(index))
        .unwrap_or(header.len());
    if type_end == 0 {
        return None;
    }

    let commit_type = &header[..type_end];
    let mut remainder = &header[type_end..];
    let scope = if let Some(scoped) = remainder.strip_prefix('(') {
        let close = scoped.find(')')?;
        let raw_scope = &scoped[..close];
        if raw_scope.trim().is_empty()
            || raw_scope.contains('(')
            || raw_scope.contains('\n')
        {
            return None;
        }
        remainder = &scoped[close + 1..];
        Some(raw_scope.trim())
    } else {
        None
    };

    let breaking = if let Some(after_bang) = remainder.strip_prefix('!') {
        remainder = after_bang;
        true
    } else {
        false
    };
    if !remainder.is_empty() {
        return None;
    }

    Some((commit_type, scope, breaking))
}

fn invalid_subject(
    subject: &str,
    violation: &str,
    found_type: Option<String>,
) -> CommitSubjectWire {
    CommitSubjectWire {
        schema_version: COMMIT_SUBJECT_WIRE_SCHEMA_VERSION,
        subject: subject.to_string(),
        valid: false,
        exempt: false,
        commit_type: None,
        scope: None,
        breaking: false,
        description: None,
        violation: Some(violation.to_string()),
        found_type,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(message: &str) -> CommitSubjectWire {
        parse_commit_subject(message, &default_commit_subject_types())
    }

    #[test]
    fn parses_plain_scoped_breaking_and_spaced_subjects() {
        let plain = parse("fix: x");
        assert!(plain.valid);
        assert_eq!(plain.commit_type.as_deref(), Some("fix"));
        assert_eq!(plain.scope, None);
        assert!(!plain.breaking);
        assert_eq!(plain.description.as_deref(), Some("x"));

        let scoped = parse("feat(bead): x");
        assert_eq!(scoped.commit_type.as_deref(), Some("feat"));
        assert_eq!(scoped.scope.as_deref(), Some("bead"));

        let breaking = parse("feat!: x");
        assert!(breaking.breaking);
        assert_eq!(breaking.scope, None);

        let scoped_breaking = parse("feat(cli)!: x");
        assert!(scoped_breaking.breaking);
        assert_eq!(scoped_breaking.scope.as_deref(), Some("cli"));

        assert_eq!(parse("fix:   x").description.as_deref(), Some("x"));
    }

    #[test]
    fn reads_only_the_trimmed_first_line() {
        let parsed = parse("  fix(cli): keep subject  \n\nignored: body  ");
        assert!(parsed.valid);
        assert_eq!(parsed.subject, "fix(cli): keep subject");
        assert_eq!(parsed.description.as_deref(), Some("keep subject"));
    }

    #[test]
    fn exempts_git_generated_and_rebase_subjects() {
        for subject in [
            "Merge branch 'main'",
            "Revert \"fix: regression\"",
            "fixup! fix: regression",
            "squash! feat: change",
            "amend! docs: clarify",
        ] {
            let parsed = parse(subject);
            assert!(parsed.valid, "{subject}");
            assert!(parsed.exempt, "{subject}");
            assert_eq!(parsed.violation, None, "{subject}");
        }
    }

    #[test]
    fn rejects_missing_or_malformed_type_separator() {
        for subject in [
            "Update built-in model aliases for Claude and Codex catalog",
            ": x",
            "fix(): x",
            "fix(nested(scope)): x",
            "fix(scope)extra: x",
            "fix!!: x",
            "fix:x",
            "fix:\tx",
        ] {
            let parsed = parse(subject);
            assert!(!parsed.valid, "{subject}");
            assert_eq!(
                parsed.violation.as_deref(),
                Some("missing_type_separator"),
                "{subject}"
            );
        }
    }

    #[test]
    fn reports_uppercase_before_unknown_type() {
        let known = parse("Fix: x");
        assert_eq!(known.violation.as_deref(), Some("uppercase_type"));
        assert_eq!(known.found_type.as_deref(), Some("Fix"));

        let unknown = parse("feet: x");
        assert_eq!(unknown.violation.as_deref(), Some("unknown_type"));
        assert_eq!(unknown.found_type.as_deref(), Some("feet"));

        let uppercase_unknown = parse("Feet: x");
        assert_eq!(
            uppercase_unknown.violation.as_deref(),
            Some("uppercase_type")
        );
        assert_eq!(uppercase_unknown.found_type.as_deref(), Some("Feet"));
    }

    #[test]
    fn rejects_empty_descriptions_and_subjects() {
        for subject in ["fix:", "fix:   "] {
            assert_eq!(
                parse(subject).violation.as_deref(),
                Some("empty_description")
            );
        }
        for subject in ["", "   \n  "] {
            assert_eq!(
                parse(subject).violation.as_deref(),
                Some("empty_subject")
            );
        }
    }

    #[test]
    fn honors_custom_allowed_types() {
        let parsed = parse_commit_subject("fix: x", &["feat".to_string()]);
        assert_eq!(parsed.violation.as_deref(), Some("unknown_type"));
        assert_eq!(parsed.found_type.as_deref(), Some("fix"));
    }

    #[test]
    fn exposes_stable_ordered_defaults() {
        assert_eq!(
            default_commit_subject_types(),
            [
                "build", "chore", "ci", "deps", "docs", "feat", "fix", "perf",
                "refactor", "revert", "style", "test"
            ]
        );
    }
}
