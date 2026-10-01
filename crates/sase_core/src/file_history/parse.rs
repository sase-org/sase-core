//! Pure parser for the pinned `git log --raw -z -M` stream.
//!
//! The host ([`crate::file_history::index`]) pins the record shape with
//! NUL separators (`%x00`) so multi-line commit bodies never corrupt
//! parsing. This function is pure over bytes the host already
//! collected: no subprocess execution, no filesystem access.
//!
//! Per-commit byte layout (all header fields NUL-separated):
//!
//! ```text
//! COMMIT\0<sha>\0<parents>\0<ct>\0<at>\0<an>\0<ae>\0<subject>\0<body>\0END\0\n
//! [:<mode0> <mode1> <old> <new> <STATUS>\0<path>\0[<newpath>\0]]
//! ```
//!
//! Raw entries carry one path, or old then new paths for renames and
//! copies. The stream is newest-first (git's first-parent order) and
//! ends after the last entry's NUL with no trailing newline.
//!
//! Contract: commit messages are assumed to contain no NUL bytes (NUL
//! cannot appear in a Unix path, and SASE commit text never carries
//! one). A record that does not split into exactly eight header fields
//! or whose timestamps do not parse is dropped, and a trailing partial
//! record from a truncated read is dropped, so one malformed record
//! cannot poison the list. A malformed raw entry stops entry parsing
//! for its commit only, because entry boundaries are unrecoverable
//! after one.

use super::runner::nonzero_oid;

/// Raw status of one changed path, with rename/copy similarity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawChangeKind {
    /// Path appeared.
    Added,
    /// Content modified in place.
    Modified,
    /// File type or mode changed.
    Typechange,
    /// Path removed.
    Deleted,
    /// Path moved, with similarity score when git reported one.
    Renamed { score: Option<u8> },
    /// Path copied, with similarity score when git reported one.
    Copied { score: Option<u8> },
}

/// One raw path entry from `--raw -z`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawChangeEntry {
    /// Parsed status of this entry.
    pub kind: RawChangeKind,
    /// Pre-image file mode (`000000` for additions).
    pub mode_old: String,
    /// Post-image file mode (`000000` for deletions).
    pub mode_new: String,
    /// Pre-image blob OID (`None` for the all-zero placeholder).
    pub old_oid: Option<String>,
    /// Post-image blob OID (`None` for the all-zero placeholder).
    pub new_oid: Option<String>,
    /// Source path for renames and copies.
    pub old_path: Option<String>,
    /// Path at this commit (target for renames and copies).
    pub path: String,
}

/// One commit from the pinned raw log, newest-first in the stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawCommit {
    /// Full commit SHA.
    pub commit: String,
    /// Full parent SHAs in git order.
    pub parents: Vec<String>,
    /// Committer time, epoch seconds.
    pub committer_time: i64,
    /// Author time, epoch seconds.
    pub author_time: i64,
    /// Commit author display name.
    pub author_name: String,
    /// Commit author email address.
    pub author_email: String,
    /// First line of the commit message.
    pub subject: String,
    /// Remaining commit-message body.
    pub body: String,
    /// Raw path entries for this commit (possibly empty).
    pub entries: Vec<RawChangeEntry>,
}

const RECORD_MARKER: &[u8] = b"COMMIT\x00";
const HEADER_END: &[u8] = b"\x00END\x00";

/// Parse the pinned `--raw -z -M` byte stream into commits,
/// newest-first. Never fails: malformed records are dropped.
pub fn parse_raw_log(output: &[u8]) -> Vec<RawCommit> {
    if output.is_empty() {
        return Vec::new();
    }
    if !output.starts_with(RECORD_MARKER) {
        return Vec::new();
    }
    let mut commits = Vec::new();
    for chunk in split_bytes(&output[RECORD_MARKER.len()..], RECORD_MARKER) {
        let Some(end) = find_bytes(chunk, HEADER_END) else {
            continue;
        };
        let rest = chunk[end + HEADER_END.len()..]
            .strip_prefix(b"\n")
            .unwrap_or(&chunk[end + HEADER_END.len()..]);
        if let Some(commit) = parse_header(&chunk[..end], rest) {
            commits.push(commit);
        }
    }
    commits
}

fn parse_header(header: &[u8], rest: &[u8]) -> Option<RawCommit> {
    let fields = split_n_bytes(header, 0, 8);
    if fields.len() != 8 {
        return None;
    }
    let text: Vec<String> = fields
        .iter()
        .map(|field| String::from_utf8_lossy(field).into_owned())
        .collect();
    if text[0].trim().is_empty() {
        return None;
    }
    let committer_time: i64 = text[2].trim().parse().ok()?;
    let author_time: i64 = text[3].trim().parse().ok()?;
    Some(RawCommit {
        commit: text[0].trim().to_string(),
        parents: text[1]
            .split(' ')
            .filter(|part| !part.is_empty())
            .map(ToString::to_string)
            .collect(),
        committer_time,
        author_time,
        author_name: text[4].clone(),
        author_email: text[5].clone(),
        subject: text[6].clone(),
        body: text[7].clone(),
        entries: parse_entries(rest),
    })
}

fn parse_entries(mut rest: &[u8]) -> Vec<RawChangeEntry> {
    let mut entries = Vec::new();
    while rest.starts_with(b":") {
        let Some(consumed) = parse_entry(rest, &mut entries) else {
            break;
        };
        rest = &rest[consumed..];
    }
    entries
}

/// Parse one raw entry at the head of *rest*, pushing it onto
/// *entries*. Returns the bytes consumed, or `None` when the entry is
/// malformed (entry parsing for the commit stops there).
fn parse_entry(
    rest: &[u8],
    entries: &mut Vec<RawChangeEntry>,
) -> Option<usize> {
    let nul = find_bytes(rest, b"\x00")?;
    let info = String::from_utf8_lossy(&rest[..nul]).into_owned();
    let parts: Vec<&str> = info.split(' ').collect();
    if parts.len() != 5 {
        return None;
    }
    let mode_old = parse_mode(parts[0].strip_prefix(':')?)?;
    let mode_new = parse_mode(parts[1])?;
    if !parts[2].bytes().all(|byte| byte.is_ascii_hexdigit())
        || !parts[3].bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    let (kind, two_paths) = parse_status(parts[4])?;
    let mut cursor = nul + 1;
    let first = read_c_string(&rest[cursor..])?;
    cursor += first.len() + 1;
    let (old_path, path) = if two_paths {
        let second = read_c_string(&rest[cursor..])?;
        cursor += second.len() + 1;
        (
            Some(String::from_utf8_lossy(first).into_owned()),
            String::from_utf8_lossy(second).into_owned(),
        )
    } else {
        (None, String::from_utf8_lossy(first).into_owned())
    };
    if path.is_empty() {
        return None;
    }
    entries.push(RawChangeEntry {
        kind,
        mode_old,
        mode_new,
        old_oid: nonzero_oid(parts[2]),
        new_oid: nonzero_oid(parts[3]),
        old_path,
        path,
    });
    Some(cursor)
}

fn parse_mode(value: &str) -> Option<String> {
    if value.len() == 6 && value.bytes().all(|byte| matches!(byte, b'0'..=b'7'))
    {
        Some(value.to_string())
    } else {
        None
    }
}

fn parse_status(value: &str) -> Option<(RawChangeKind, bool)> {
    match value.as_bytes().first()? {
        b'A' if value.len() == 1 => Some((RawChangeKind::Added, false)),
        b'M' if value.len() == 1 => Some((RawChangeKind::Modified, false)),
        b'T' if value.len() == 1 => Some((RawChangeKind::Typechange, false)),
        b'D' if value.len() == 1 => Some((RawChangeKind::Deleted, false)),
        b'R' | b'C' => {
            let score = if value.len() > 1 {
                Some(value[1..].parse::<u8>().ok()?)
            } else {
                None
            };
            let kind = if value.starts_with('R') {
                RawChangeKind::Renamed { score }
            } else {
                RawChangeKind::Copied { score }
            };
            Some((kind, true))
        }
        _ => None,
    }
}

/// Read one NUL-terminated byte string; `None` when unterminated.
fn read_c_string(rest: &[u8]) -> Option<&[u8]> {
    let nul = find_bytes(rest, b"\x00")?;
    Some(&rest[..nul])
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn split_bytes<'a>(haystack: &'a [u8], needle: &[u8]) -> Vec<&'a [u8]> {
    let mut parts = Vec::new();
    let mut start = 0;
    while let Some(offset) = find_bytes(&haystack[start..], needle) {
        parts.push(&haystack[start..start + offset]);
        start += offset + needle.len();
    }
    parts.push(&haystack[start..]);
    parts
}

fn split_n_bytes(haystack: &[u8], sep: u8, n: usize) -> Vec<&[u8]> {
    let mut parts = Vec::with_capacity(n);
    let mut start = 0;
    for _ in 0..n - 1 {
        match haystack[start..].iter().position(|byte| *byte == sep) {
            Some(offset) => {
                parts.push(&haystack[start..start + offset]);
                start += offset + 1;
            }
            None => break,
        }
    }
    parts.push(&haystack[start..]);
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    const ZERO: &str = "0000000000000000000000000000000000000000";
    const OID_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const OID_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn header(sha: &str, parents: &str, subject: &str, body: &str) -> Vec<u8> {
        format!(
            "COMMIT\x00{sha}\x00{parents}\x001700000000\x001700000001\x00T\x00t@e.com\x00{subject}\x00{body}\x00END\x00\n"
        )
        .into_bytes()
    }

    fn raw_entry(info: &str, paths: &[&str]) -> Vec<u8> {
        let mut out = format!("{info}\x00").into_bytes();
        for path in paths {
            out.extend_from_slice(path.as_bytes());
            out.push(0);
        }
        out
    }

    #[test]
    fn empty_stream_returns_empty() {
        assert!(parse_raw_log(b"").is_empty());
        assert!(parse_raw_log(b"garbage").is_empty());
    }

    #[test]
    fn added_commit_parses_modes_and_zero_pre_image() {
        let mut stream = header("c1", "", "create", "");
        stream.extend(raw_entry(
            &format!(":000000 100644 {ZERO} {OID_A} A"),
            &["notes/a.md"],
        ));
        let commits = parse_raw_log(&stream);
        assert_eq!(commits.len(), 1);
        let commit = &commits[0];
        assert_eq!(commit.commit, "c1");
        assert!(commit.parents.is_empty());
        assert_eq!(commit.committer_time, 1_700_000_000);
        assert_eq!(commit.author_time, 1_700_000_001);
        assert_eq!(commit.entries.len(), 1);
        let entry = &commit.entries[0];
        assert_eq!(entry.kind, RawChangeKind::Added);
        assert_eq!(entry.mode_old, "000000");
        assert_eq!(entry.mode_new, "100644");
        assert_eq!(entry.old_oid, None);
        assert_eq!(entry.new_oid.as_deref(), Some(OID_A));
        assert_eq!(entry.path, "notes/a.md");
    }

    #[test]
    fn rename_reports_both_paths_and_score() {
        let mut stream = header("c2", "c1", "move", "a body\ntwo lines");
        stream.extend(raw_entry(
            &format!(":100644 100644 {OID_A} {OID_B} R063"),
            &["notes/a.md", "archive/b.md"],
        ));
        let commits = parse_raw_log(&stream);
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].body, "a body\ntwo lines");
        let entry = &commits[0].entries[0];
        assert_eq!(entry.kind, RawChangeKind::Renamed { score: Some(63) });
        assert_eq!(entry.old_path.as_deref(), Some("notes/a.md"));
        assert_eq!(entry.path, "archive/b.md");
        assert_eq!(entry.old_oid.as_deref(), Some(OID_A));
        assert_eq!(entry.new_oid.as_deref(), Some(OID_B));
    }

    #[test]
    fn multiple_commits_stay_newest_first() {
        let mut stream = header("new", "old", "edit", "");
        stream.extend(raw_entry(
            &format!(":100644 100644 {OID_A} {OID_B} M"),
            &["f.md"],
        ));
        stream.extend(header("old", "", "create", ""));
        stream.extend(raw_entry(
            &format!(":000000 100644 {ZERO} {OID_A} A"),
            &["f.md"],
        ));
        let commits = parse_raw_log(&stream);
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].commit, "new");
        assert_eq!(commits[1].commit, "old");
    }

    #[test]
    fn record_with_bad_timestamp_is_dropped() {
        let mut bad = b"COMMIT\x00c1\x00\x00not-a-number\x001\x00T\x00t@e.com\x00s\x00\x00END\x00\n".to_vec();
        bad.extend(header("c2", "", "ok", ""));
        let commits = parse_raw_log(&bad);
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].commit, "c2");
    }

    #[test]
    fn trailing_partial_record_is_dropped() {
        let mut stream = header("c1", "", "ok", "");
        stream.extend_from_slice(b"COMMIT\x00c2\x00\x00");
        let commits = parse_raw_log(&stream);
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].commit, "c1");
    }

    #[test]
    fn malformed_entry_stops_entries_not_commits() {
        let mut stream = header("c1", "", "ok", "");
        stream.extend_from_slice(b":bogus entry without nul");
        stream.extend(header("c2", "", "fine", ""));
        let commits = parse_raw_log(&stream);
        assert_eq!(commits.len(), 2);
        assert!(commits[0].entries.is_empty());
    }

    #[test]
    fn unicode_and_spaced_paths_survive() {
        let mut stream = header("c1", "", "ok", "");
        stream.extend(raw_entry(
            &format!(":100644 100644 {OID_A} {OID_B} M"),
            &["notes/игнор quux.md"],
        ));
        let commits = parse_raw_log(&stream);
        assert_eq!(commits[0].entries[0].path, "notes/игнор quux.md");
    }
}
