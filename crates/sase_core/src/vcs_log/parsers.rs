//! Pure-Rust parser for a pinned `git log --format=...` stream.
//!
//! Mirrors `sase/src/sase/core/vcs_log_facade.py::_parse_git_log_python`
//! byte-for-byte. The function is pure over a string the host already
//! collected by running `git log`; no subprocess execution, no filesystem
//! access — the host (Python) keeps every side effect.
//!
//! The host pins the record shape with the same field/record separators
//! used here (the robust technique already proven in
//! `sase/src/sase/ace/revert_agent_discovery.py`): a unit separator
//! (`\x1f`) between fields and a record separator (`\x1e`) between
//! commits, so multi-line commit bodies never corrupt parsing.

use super::origin::classify_commit_origin;
use super::wire::{CommitPresenceWire, VcsCommitWire};

/// Field separator emitted by `%x1f` in the pinned `git log` format.
pub const UNIT_SEP: char = '\u{1f}';
/// Record separator emitted by `%x1e` in the pinned `git log` format.
pub const RECORD_SEP: char = '\u{1e}';

/// Number of fields per record in the legacy pinned format:
/// `%H %h %an %ae %at %s %b`.
const LEGACY_FIELD_COUNT: usize = 7;
/// Number of fields per record in the current pinned format:
/// `%H %h %an %ae %at %P %s %b`.
const CURRENT_FIELD_COUNT: usize = 8;

/// Parse the separator-delimited output of the pinned `git log` format
/// into a list of [`VcsCommitWire`], newest-first (git's natural order).
///
/// The expected per-commit format string is
/// `%H<US>%h<US>%an<US>%ae<US>%at<US>%P<US>%s<US>%b<RS>` where `<US>` is
/// [`UNIT_SEP`] and `<RS>` is [`RECORD_SEP`]. git appends a newline after
/// each record, so every chunk after the first record separator starts
/// with a leading `\n` that is stripped here.
///
/// Robustness contract (matches the forgiving Python golden):
///
/// - An empty stream returns an empty list.
/// - A trailing record separator (and the newline git appends after it)
///   yields a blank final chunk that is skipped.
/// - Current 8-field records carry `%P` parent ids before the subject.
/// - Legacy 7-field records are still accepted with no parent ids.
/// - A record that does not split into one of those field counts is
///   dropped so one malformed record cannot poison the list.
/// - A record whose `%at` timestamp field does not parse as an integer is
///   dropped for the same reason.
/// - `subject` and `body` are preserved verbatim (bodies may be empty or
///   span multiple lines); only the id and timestamp fields are trimmed.
pub fn parse_git_log(stdout: &str) -> Vec<VcsCommitWire> {
    if stdout.is_empty() {
        return Vec::new();
    }
    let mut commits: Vec<VcsCommitWire> = Vec::new();
    for raw in stdout.split(RECORD_SEP) {
        let chunk = raw.trim_start_matches('\n');
        if chunk.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> =
            chunk.splitn(CURRENT_FIELD_COUNT, UNIT_SEP).collect();
        if fields.len() != CURRENT_FIELD_COUNT
            && fields.len() != LEGACY_FIELD_COUNT
        {
            continue;
        }
        let timestamp: i64 = match fields[4].trim().parse() {
            Ok(value) => value,
            Err(_) => continue,
        };
        let (parent_ids, subject, body) = match fields.len() {
            CURRENT_FIELD_COUNT => (
                parse_parent_ids(fields[5]),
                fields[6].to_string(),
                fields[7].to_string(),
            ),
            LEGACY_FIELD_COUNT => {
                (Vec::new(), fields[5].to_string(), fields[6].to_string())
            }
            _ => {
                unreachable!("field count was validated before timestamp parse")
            }
        };
        commits.push(VcsCommitWire {
            full_id: fields[0].trim().to_string(),
            short_id: fields[1].trim().to_string(),
            author_name: fields[2].to_string(),
            author_email: fields[3].to_string(),
            timestamp,
            parent_ids,
            origin: classify_commit_origin(&commit_message(&subject, &body)),
            subject,
            body,
            presence: CommitPresenceWire::Unknown,
        });
    }
    commits
}

fn commit_message(subject: &str, body: &str) -> String {
    if body.is_empty() {
        subject.to_string()
    } else {
        format!("{subject}\n\n{body}")
    }
}

fn parse_parent_ids(value: &str) -> Vec<String> {
    if value.is_empty() {
        return Vec::new();
    }
    value
        .split(' ')
        .filter(|part| !part.is_empty())
        .map(ToString::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(
        full: &str,
        short: &str,
        ts: &str,
        parents: &str,
        subject: &str,
        body: &str,
    ) -> String {
        let name = "Bryan";
        let email = "bryan@example.com";
        format!(
            "{full}{US}{short}{US}{name}{US}{email}{US}{ts}{US}{parents}{US}{subject}{US}{body}{RS}",
            US = UNIT_SEP,
            RS = RECORD_SEP,
        )
    }

    fn legacy_record(
        full: &str,
        short: &str,
        ts: &str,
        subject: &str,
        body: &str,
    ) -> String {
        let name = "Bryan";
        let email = "bryan@example.com";
        format!(
            "{full}{US}{short}{US}{name}{US}{email}{US}{ts}{US}{subject}{US}{body}{RS}",
            US = UNIT_SEP,
            RS = RECORD_SEP,
        )
    }

    fn commit(
        full: &str,
        short: &str,
        ts: i64,
        parents: &[&str],
        subject: &str,
        body: &str,
    ) -> VcsCommitWire {
        VcsCommitWire {
            full_id: full.to_string(),
            short_id: short.to_string(),
            author_name: "Bryan".to_string(),
            author_email: "bryan@example.com".to_string(),
            timestamp: ts,
            parent_ids: parents.iter().map(|value| value.to_string()).collect(),
            subject: subject.to_string(),
            body: body.to_string(),
            presence: CommitPresenceWire::Unknown,
            origin: classify_commit_origin(&commit_message(subject, body)),
        }
    }

    #[test]
    fn empty_stream_returns_empty() {
        assert!(parse_git_log("").is_empty());
    }

    #[test]
    fn single_commit_parses_all_fields() {
        let stream = record(
            "a1b2c3d4e5",
            "a1b2c3d",
            "1700000000",
            "p1",
            "fix: thing",
            "",
        );
        assert_eq!(
            parse_git_log(&stream),
            vec![commit(
                "a1b2c3d4e5",
                "a1b2c3d",
                1700000000,
                &["p1"],
                "fix: thing",
                "",
            )]
        );
    }

    #[test]
    fn legacy_seven_field_commit_has_no_parents() {
        let stream = legacy_record(
            "a1b2c3d4e5",
            "a1b2c3d",
            "1700000000",
            "fix: legacy",
            "",
        );
        assert_eq!(
            parse_git_log(&stream),
            vec![commit(
                "a1b2c3d4e5",
                "a1b2c3d",
                1700000000,
                &[],
                "fix: legacy",
                "",
            )]
        );
    }

    #[test]
    fn root_commit_empty_parent_field_has_no_parents() {
        let stream =
            record("a1b2c3d4e5", "a1b2c3d", "1700000000", "", "initial", "");
        let parsed = parse_git_log(&stream);
        assert_eq!(parsed.len(), 1);
        assert!(parsed[0].parent_ids.is_empty());
    }

    #[test]
    fn octopus_merge_parses_all_parent_ids() {
        let stream = record(
            "merge",
            "merge",
            "1700000000",
            "p1 p2 p3",
            "Merge branches",
            "",
        );
        let parsed = parse_git_log(&stream);
        assert_eq!(
            parsed[0].parent_ids,
            vec!["p1".to_string(), "p2".to_string(), "p3".to_string()]
        );
        assert!(parsed[0].is_merge());
    }

    #[test]
    fn multiple_commits_preserve_order() {
        let stream = format!(
            "{}{}",
            record("h1", "s1", "300", "p0", "first", ""),
            record("h2", "s2", "200", "p1", "second", ""),
        );
        let parsed = parse_git_log(&stream);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].full_id, "h1");
        assert_eq!(parsed[1].full_id, "h2");
    }

    #[test]
    fn git_appends_newline_between_records_is_stripped() {
        // git emits "<record><RS>\n<record><RS>\n"; the leading newline on
        // each subsequent chunk must not leak into the full_id.
        let stream = format!(
            "{}\n{}\n",
            record("h1", "s1", "300", "p0", "first", ""),
            record("h2", "s2", "200", "p1", "second", ""),
        );
        let parsed = parse_git_log(&stream);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].full_id, "h1");
        assert_eq!(parsed[1].full_id, "h2");
    }

    #[test]
    fn trailing_record_separator_yields_no_blank_commit() {
        let stream = record("h1", "s1", "300", "p0", "only", "");
        assert_eq!(parse_git_log(&stream).len(), 1);
    }

    #[test]
    fn multiline_body_is_preserved_verbatim() {
        let body = "line one\nline two\n\nline four";
        let stream = record("h1", "s1", "300", "p0", "subject here", body);
        let parsed = parse_git_log(&stream);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].body, body);
        assert_eq!(parsed[0].subject, "subject here");
    }

    #[test]
    fn record_with_too_few_fields_is_dropped() {
        // Missing the subject and body separators -> only 6 fields.
        let malformed = format!(
            "h1{US}s1{US}A{US}a@x{US}300{US}p0{RS}",
            US = UNIT_SEP,
            RS = RECORD_SEP,
        );
        let good = record("h2", "s2", "200", "p1", "ok", "");
        let parsed = parse_git_log(&format!("{malformed}{good}"));
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].full_id, "h2");
    }

    #[test]
    fn record_with_unparseable_timestamp_is_dropped() {
        let bad = record("h1", "s1", "not-a-number", "p0", "x", "");
        let good = record("h2", "s2", "200", "p1", "ok", "");
        let parsed = parse_git_log(&format!("{bad}{good}"));
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].full_id, "h2");
    }

    #[test]
    fn body_containing_unit_separator_stays_in_body() {
        // splitn(8) means any extra unit separators fold into the body,
        // which is the last field. A pathological body cannot corrupt
        // the record count.
        let stream = format!(
            "h1{US}s1{US}A{US}a@x{US}300{US}p0{US}subj{US}bo{US}dy{RS}",
            US = UNIT_SEP,
            RS = RECORD_SEP,
        );
        let parsed = parse_git_log(&stream);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].subject, "subj");
        assert_eq!(parsed[0].body, format!("bo{}dy", UNIT_SEP));
    }

    #[test]
    fn commit_with_stitch_type_gets_stitch_origin() {
        let stream = record(
            "h1",
            "s1",
            "300",
            "p0",
            "fix: tracked",
            "Details\n\nSASE_TYPE=stitch",
        );
        let parsed = parse_git_log(&stream);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].origin, crate::vcs_log::CommitOriginWire::Stitch,);
    }
}
