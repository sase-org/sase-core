//! Bounded, lock-free git runner for file history.
//!
//! Every invocation pins `-c core.quotepath=off -c diff.renames=true`
//! so user config cannot change the output, passes
//! `--no-optional-locks` with `GIT_OPTIONAL_LOCKS=0` so background
//! reads never fight writers, sets `GIT_TERMINAL_PROMPT=0` so git can
//! never block on credentials, and takes pathspecs only after `--`.
//! Timeouts and output caps yield `truncated = true`, never an error.

use std::io::Read;
use std::path::Path;
use std::process::{ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

use super::wire::FileHistoryError;

const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Output of one git invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitResult {
    /// Process exit code (`None` when killed after a timeout).
    pub code: Option<i32>,
    /// Captured stdout, cut at the caller's byte budget.
    pub stdout: Vec<u8>,
    /// True when the timeout fired or the byte budget was exceeded.
    pub truncated: bool,
}

/// Run git and require exit code zero.
pub fn run_git_checked(
    repo: &Path,
    args: &[&str],
    timeout: Duration,
    max_bytes: u64,
) -> Result<GitResult, FileHistoryError> {
    let result = run_git_unchecked(repo, args, timeout, max_bytes)?;
    if result.code == Some(0) {
        Ok(result)
    } else {
        Err(FileHistoryError::GitFailed(format!(
            "git {} exited with {}",
            describe_args(args),
            result
                .code
                .map_or("signal".to_string(), |code| code.to_string()),
        )))
    }
}

/// Run git and report the exit code without failing on it. Used for
/// probes (`merge-base --is-ancestor`, `check-ignore -q`) whose
/// non-zero exits carry meaning.
pub fn run_git_unchecked(
    repo: &Path,
    args: &[&str],
    timeout: Duration,
    max_bytes: u64,
) -> Result<GitResult, FileHistoryError> {
    let mut child = Command::new("git")
        .arg("-c")
        .arg("core.quotepath=off")
        .arg("-c")
        .arg("diff.renames=true")
        .arg("--no-optional-locks")
        .arg("-C")
        .arg(repo)
        .args(args)
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| {
            FileHistoryError::GitFailed(format!("spawn git: {error}"))
        })?;
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait().map_err(|error| {
            FileHistoryError::GitFailed(format!("wait git: {error}"))
        })? {
            Some(status) => {
                let (stdout, size_truncated) =
                    read_capped(child.stdout.take(), max_bytes);
                return Ok(GitResult {
                    code: status.code(),
                    stdout,
                    truncated: size_truncated,
                });
            }
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                let (stdout, _) = read_capped(child.stdout.take(), max_bytes);
                return Ok(GitResult {
                    code: None,
                    stdout,
                    truncated: true,
                });
            }
            None => std::thread::sleep(WAIT_POLL_INTERVAL),
        }
    }
}

/// Whether *value* is safe to pass as a git revision token: no
/// option-looking or range syntax, and a conservative charset.
pub fn safe_revision_token(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('-')
        && !value.contains("..")
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'.' | b'_' | b'/' | b'-')
        })
}

/// Whether *value* is a full 40-character lowercase-or-not hex SHA.
pub fn looks_like_full_sha(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Parse a blob OID from git output: `Some` for a real full SHA,
/// `None` for the all-zero placeholder or anything malformed.
pub fn nonzero_oid(value: &str) -> Option<String> {
    if looks_like_full_sha(value) && value.bytes().any(|byte| byte != b'0') {
        Some(value.to_ascii_lowercase())
    } else {
        None
    }
}

/// Whether *value* is safe to pass as an explicit git pathspec:
/// repo-relative, no escapes, no pathspec magic, no control bytes.
/// Colons are rejected outright (conservative: SASE memory paths never
/// contain them, and a colon risks `rev:path` reinterpretation).
pub fn safe_pathspec(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('/')
        && !value.starts_with('-')
        && !value.contains(':')
        && !value.contains('\0')
        && !value.contains("..")
        && value.bytes().all(|byte| !byte.is_ascii_control())
}

fn describe_args(args: &[&str]) -> String {
    args.iter()
        .take(4)
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ")
}

fn read_capped(pipe: Option<ChildStdout>, max_bytes: u64) -> (Vec<u8>, bool) {
    let Some(pipe) = pipe else {
        return (Vec::new(), false);
    };
    let limit =
        usize::try_from(max_bytes.saturating_add(1)).unwrap_or(usize::MAX);
    let mut buf = Vec::new();
    let truncated = pipe.take(limit as u64).read_to_end(&mut buf).is_ok()
        && buf.len() as u64 > max_bytes;
    buf.truncate(
        usize::try_from(max_bytes)
            .unwrap_or(usize::MAX)
            .min(buf.len()),
    );
    (buf, truncated)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revision_tokens_reject_options_and_ranges() {
        assert!(safe_revision_token("HEAD"));
        assert!(safe_revision_token("abc1234"));
        assert!(safe_revision_token("origin/master"));
        assert!(safe_revision_token(
            "0123456789abcdef0123456789abcdef01234567"
        ));
        assert!(!safe_revision_token(""));
        assert!(!safe_revision_token("--output=evil"));
        assert!(!safe_revision_token("-HEAD"));
        assert!(!safe_revision_token("a..b"));
        assert!(!safe_revision_token("a b"));
        assert!(!safe_revision_token("a:b"));
        assert!(!safe_revision_token("a;rm"));
    }

    #[test]
    fn full_sha_shape_is_exact() {
        assert!(looks_like_full_sha(
            "0123456789abcdef0123456789abcdef01234567"
        ));
        assert!(!looks_like_full_sha("abc1234"));
        assert!(!looks_like_full_sha(""));
        assert!(!looks_like_full_sha(
            "0123456789abcdeg0123456789abcdef01234567"
        ));
    }

    #[test]
    fn zero_oid_is_absent() {
        assert_eq!(nonzero_oid(&"0".repeat(40)), None);
        assert_eq!(
            nonzero_oid("0123456789abcdef0123456789abcdef01234567"),
            Some("0123456789abcdef0123456789abcdef01234567".to_string()),
        );
        assert_eq!(nonzero_oid("abc"), None);
    }

    #[test]
    fn pathspecs_reject_escapes_and_magic() {
        assert!(safe_pathspec("sase/memory"));
        assert!(safe_pathspec("AGENTS.md"));
        assert!(safe_pathspec("notes/my note.md"));
        assert!(!safe_pathspec(""));
        assert!(!safe_pathspec("/abs/path"));
        assert!(!safe_pathspec("../evil"));
        assert!(!safe_pathspec("a/../b"));
        assert!(!safe_pathspec("-weird"));
        assert!(!safe_pathspec(":(glob)**"));
        assert!(!safe_pathspec("a:b"));
        assert!(!safe_pathspec("a\nb"));
    }
}
