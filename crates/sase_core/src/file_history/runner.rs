//! Bounded, lock-free git runner for file history.
//!
//! Every invocation pins `-c core.quotepath=off -c diff.renames=true`
//! and `-c log.showSignature=false` so user config cannot change the
//! output, passes
//! `--no-optional-locks` with `GIT_OPTIONAL_LOCKS=0` so background
//! reads never fight writers, sets `GIT_TERMINAL_PROMPT=0` so git can
//! never block on credentials, sets `GIT_NO_LAZY_FETCH=1` so reads
//! cannot trigger on-demand fetches, and takes pathspecs only after
//! `--`. Repository-routing variables inherited from the parent
//! (`GIT_DIR`, `GIT_WORK_TREE`, `GIT_INDEX_FILE`,
//! `GIT_OBJECT_DIRECTORY`, `GIT_ALTERNATE_OBJECT_DIRECTORIES`,
//! `GIT_COMMON_DIR`, and `GIT_NAMESPACE`) are removed so `-C`
//! selects the repo. Timeouts and output caps yield
//! `truncated = true`, never an error.

use std::io::{self, Read, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::thread::ScopedJoinHandle;
use std::time::{Duration, Instant};

use super::wire::FileHistoryError;

const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(5);

type StdinWorker<'scope> = ScopedJoinHandle<'scope, io::Result<()>>;
type StdoutWorker<'scope> =
    ScopedJoinHandle<'scope, io::Result<CapturedStdout>>;

/// Parent-environment keys that would reroute git away from `-C`.
const GIT_ROUTING_VARS: &[&str] = &[
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_COMMON_DIR",
    "GIT_NAMESPACE",
];

#[derive(Debug, Default)]
struct CapturedStdout {
    bytes: Vec<u8>,
    truncated: bool,
}

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
    require_success(run_git_unchecked(repo, args, timeout, max_bytes)?, args)
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
    run_process(git_command(repo, args), None, timeout, max_bytes)
}

/// Run git with a bounded stdin payload and require exit code zero.
///
/// Callers must still inspect [`GitResult::truncated`] when this
/// returns `Ok`: git can exit zero after the byte cap or a timeout
/// kill reports through the unchecked helper first.
pub fn run_git_checked_with_stdin(
    repo: &Path,
    args: &[&str],
    stdin: &[u8],
    timeout: Duration,
    max_bytes: u64,
) -> Result<GitResult, FileHistoryError> {
    require_success(
        run_git_unchecked_with_stdin(repo, args, stdin, timeout, max_bytes)?,
        args,
    )
}

/// Run git with a bounded stdin payload, writing it on a dedicated
/// thread while stdout drains.
///
/// Stdin is closed after the write completes so batch commands such as
/// `cat-file --batch` see EOF. The same whole-command deadline and
/// output cap as [`run_git_unchecked`] apply. A stdin write or stdout
/// read failure is an error rather than a successful-looking partial
/// result; `BrokenPipe` on stdin is the expected early-exit path and
/// is not a failure. Callers must still inspect
/// [`GitResult::truncated`] when git exits zero.
pub fn run_git_unchecked_with_stdin(
    repo: &Path,
    args: &[&str],
    stdin: &[u8],
    timeout: Duration,
    max_bytes: u64,
) -> Result<GitResult, FileHistoryError> {
    run_git_unchecked_with_stdin_env(repo, args, stdin, &[], timeout, max_bytes)
}

/// Like [`run_git_unchecked_with_stdin`], with extra environment entries
/// applied before routing variables are scrubbed.
pub fn run_git_unchecked_with_stdin_env(
    repo: &Path,
    args: &[&str],
    stdin: &[u8],
    extra_env: &[(&str, &str)],
    timeout: Duration,
    max_bytes: u64,
) -> Result<GitResult, FileHistoryError> {
    run_process(
        git_command_with_injected_env(repo, args, extra_env),
        Some(stdin),
        timeout,
        max_bytes,
    )
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

fn require_success(
    result: GitResult,
    args: &[&str],
) -> Result<GitResult, FileHistoryError> {
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

fn git_command(repo: &Path, args: &[&str]) -> Command {
    git_command_with_injected_env(repo, args, &[])
}

fn git_command_with_injected_env(
    repo: &Path,
    args: &[&str],
    injected: &[(&str, &str)],
) -> Command {
    let mut command = Command::new("git");
    command
        .arg("-c")
        .arg("core.quotepath=off")
        .arg("-c")
        .arg("diff.renames=true")
        .arg("-c")
        .arg("log.showSignature=false")
        .arg("--no-optional-locks")
        .arg("-C")
        .arg(repo)
        .args(args);
    for &(key, value) in injected {
        command.env(key, value);
    }
    apply_git_env(&mut command);
    command
}

fn apply_git_env(command: &mut Command) {
    for key in GIT_ROUTING_VARS {
        command.env_remove(key);
    }
    command
        .env("GIT_NO_LAZY_FETCH", "1")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_TERMINAL_PROMPT", "0");
}

fn run_process(
    mut command: Command,
    stdin: Option<&[u8]>,
    timeout: Duration,
    max_bytes: u64,
) -> Result<GitResult, FileHistoryError> {
    if stdin.is_some() {
        command.stdin(Stdio::piped());
    } else {
        command.stdin(Stdio::null());
    }
    command.stdout(Stdio::piped()).stderr(Stdio::null());
    let mut child = command.spawn().map_err(|error| {
        FileHistoryError::GitFailed(format!("spawn git: {error}"))
    })?;
    match std::thread::scope(|scope| {
        run_spawned(scope, &mut child, stdin, timeout, max_bytes)
    }) {
        Ok(result) => Ok(result),
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            Err(error)
        }
    }
}

fn run_spawned<'scope>(
    scope: &'scope std::thread::Scope<'scope, '_>,
    child: &mut Child,
    stdin: Option<&'scope [u8]>,
    timeout: Duration,
    max_bytes: u64,
) -> Result<GitResult, FileHistoryError> {
    let stdin_pipe = child.stdin.take();
    let stdout_pipe = child.stdout.take();
    if stdin.is_some() && stdin_pipe.is_none() {
        let _ = child.kill();
        let _ = child.wait();
        return Err(FileHistoryError::GitFailed(
            "git stdin missing".to_string(),
        ));
    }
    if stdout_pipe.is_none() {
        let _ = child.kill();
        let _ = child.wait();
        return Err(FileHistoryError::GitFailed(
            "git stdout missing".to_string(),
        ));
    }
    let writer = match (stdin, stdin_pipe) {
        (Some(bytes), Some(pipe)) => {
            Some(scope.spawn(move || write_stdin(pipe, bytes)))
        }
        _ => None,
    };
    let reader = stdout_pipe
        .map(|pipe| scope.spawn(move || drain_stdout(pipe, max_bytes)));
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                return finish_exited(status.code(), writer, reader);
            }
            Ok(None) if Instant::now() >= deadline => {
                return finish_timeout(child, writer, reader);
            }
            Ok(None) => std::thread::sleep(WAIT_POLL_INTERVAL),
            Err(error) => {
                reap_and_join(child, true, writer, reader);
                return Err(FileHistoryError::GitFailed(format!(
                    "wait git: {error}"
                )));
            }
        }
    }
}

fn finish_exited(
    code: Option<i32>,
    writer: Option<StdinWorker<'_>>,
    reader: Option<StdoutWorker<'_>>,
) -> Result<GitResult, FileHistoryError> {
    let (write, read) = join_workers(writer, reader);
    write.map_err(|error| {
        FileHistoryError::GitFailed(format!("git stdin: {error}"))
    })?;
    let captured = read.map_err(|error| {
        FileHistoryError::GitFailed(format!("git stdout: {error}"))
    })?;
    Ok(GitResult {
        code,
        stdout: captured.bytes,
        truncated: captured.truncated,
    })
}

fn finish_timeout(
    child: &mut Child,
    writer: Option<StdinWorker<'_>>,
    reader: Option<StdoutWorker<'_>>,
) -> Result<GitResult, FileHistoryError> {
    let captured = reap_and_join(child, true, writer, reader);
    Ok(GitResult {
        code: None,
        stdout: captured.bytes,
        truncated: true,
    })
}

fn reap_and_join(
    child: &mut Child,
    kill: bool,
    writer: Option<StdinWorker<'_>>,
    reader: Option<StdoutWorker<'_>>,
) -> CapturedStdout {
    if kill {
        let _ = child.kill();
    }
    let _ = child.wait();
    let (_, read) = join_workers(writer, reader);
    read.unwrap_or_default()
}

fn join_workers(
    writer: Option<StdinWorker<'_>>,
    reader: Option<StdoutWorker<'_>>,
) -> (io::Result<()>, io::Result<CapturedStdout>) {
    let write = match writer {
        Some(handle) => match handle.join() {
            Ok(result) => result,
            Err(_) => Err(io::Error::other("git stdin thread panicked")),
        },
        None => Ok(()),
    };
    let read = match reader {
        Some(handle) => match handle.join() {
            Ok(result) => result,
            Err(_) => Err(io::Error::other("git stdout thread panicked")),
        },
        None => Ok(CapturedStdout::default()),
    };
    (write, read)
}

fn describe_args(args: &[&str]) -> String {
    args.iter()
        .take(4)
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ")
}

/// Write *bytes* then close stdin so batch git commands see EOF.
///
/// `BrokenPipe` means the child closed early (exit or kill) and is
/// not treated as a write failure.
fn write_stdin(mut pipe: ChildStdin, bytes: &[u8]) -> io::Result<()> {
    let result = match pipe.write_all(bytes) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => Ok(()),
        Err(error) => Err(error),
    };
    drop(pipe);
    result
}

/// Drain a child's stdout to EOF, keeping at most `max_bytes`.
///
/// The drain runs until the pipe closes even when the cap is exceeded:
/// stopping early would leave git blocked on write and deadlock the
/// wait loop for outputs larger than the pipe buffer. Excess bytes are
/// discarded; the flag reports whether the cap was exceeded.
fn drain_stdout(
    mut pipe: ChildStdout,
    max_bytes: u64,
) -> io::Result<CapturedStdout> {
    let limit = usize::try_from(max_bytes).unwrap_or(usize::MAX);
    let mut buf = Vec::new();
    let mut total: u64 = 0;
    let mut chunk = [0u8; 8192];
    loop {
        match pipe.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => {
                total += read as u64;
                if buf.len() < limit {
                    let room = limit - buf.len();
                    buf.extend_from_slice(&chunk[..read.min(room)]);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                continue;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(CapturedStdout {
        bytes: buf,
        truncated: total > max_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    const PIPE_BUFFER_BYTES: usize = 64 * 1024;
    const OVER_PIPE: usize = 200_000;

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
    fn output_larger_than_the_pipe_buffer_does_not_deadlock() {
        // One commit holding a 200 KiB file: `git show HEAD` writes far
        // more than the 64 KiB pipe buffer. Reading stdout only after
        // exit deadlocked here: git blocked on write while the wait
        // loop polled, so the deadline always fired first.
        let (_tmp, repo) = init_test_repo();
        std::fs::write(repo.join("big.txt"), "x".repeat(OVER_PIPE)).unwrap();
        git(&repo, &["add", "-A"]);
        git(&repo, &["commit", "-m", "big"]);
        let timeout = Duration::from_secs(30);
        let result = run_git_unchecked(
            &repo,
            &["show", "HEAD"],
            timeout,
            32 * 1024 * 1024,
        )
        .unwrap();
        assert_eq!(result.code, Some(0));
        assert!(
            result.stdout.len() > PIPE_BUFFER_BYTES,
            "output is {} bytes",
            result.stdout.len()
        );
        assert!(!result.truncated);
        // The byte cap still reports truncation without deadlocking:
        // the drain runs to EOF and discards the excess.
        let capped =
            run_git_unchecked(&repo, &["show", "HEAD"], timeout, 1024).unwrap();
        assert_eq!(capped.code, Some(0));
        assert_eq!(capped.stdout.len(), 1024);
        assert!(capped.truncated);
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

    #[test]
    fn routing_env_vars_are_removed_and_lock_vars_are_set() {
        let injected: Vec<(&str, &str)> = GIT_ROUTING_VARS
            .iter()
            .map(|key| (*key, "/sase-git-runner-bogus"))
            .chain([
                ("GIT_NO_LAZY_FETCH", "0"),
                ("GIT_OPTIONAL_LOCKS", "1"),
                ("GIT_TERMINAL_PROMPT", "1"),
            ])
            .collect();
        let dumped = child_env_with_injected(&injected);
        for key in GIT_ROUTING_VARS {
            assert!(
                !dumped
                    .lines()
                    .any(|line| line.starts_with(&format!("{key}="))),
                "{key} leaked into the child:\n{dumped}"
            );
        }
        assert!(
            dumped.lines().any(|line| line == "GIT_NO_LAZY_FETCH=1"),
            "GIT_NO_LAZY_FETCH missing:\n{dumped}"
        );
        assert!(
            dumped.lines().any(|line| line == "GIT_OPTIONAL_LOCKS=0"),
            "GIT_OPTIONAL_LOCKS missing:\n{dumped}"
        );
        assert!(
            dumped.lines().any(|line| line == "GIT_TERMINAL_PROMPT=0"),
            "GIT_TERMINAL_PROMPT missing:\n{dumped}"
        );
    }

    #[test]
    fn git_commands_pin_signature_output_off() {
        let args: Vec<String> = git_command(
            Path::new("/tmp/sase-runner-test"),
            &["log", "--format=%at", "HEAD"],
        )
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
        assert_eq!(
            &args[..9],
            [
                "-c",
                "core.quotepath=off",
                "-c",
                "diff.renames=true",
                "-c",
                "log.showSignature=false",
                "--no-optional-locks",
                "-C",
                "/tmp/sase-runner-test",
            ]
        );
        assert_eq!(&args[9..], ["log", "--format=%at", "HEAD"]);
    }

    #[test]
    fn injected_routing_env_does_not_reroute_git() {
        let (_tmp, repo) = init_test_repo();
        let injected: Vec<(&str, &str)> = GIT_ROUTING_VARS
            .iter()
            .map(|key| (*key, "/sase-git-runner-bogus"))
            .collect();
        let result = run_process(
            git_command_with_injected_env(
                &repo,
                &["rev-parse", "--is-inside-work-tree"],
                &injected,
            ),
            None,
            Duration::from_secs(10),
            4096,
        )
        .unwrap();
        assert_eq!(result.code, Some(0));
        assert_eq!(String::from_utf8_lossy(&result.stdout).trim(), "true");
        assert!(!result.truncated);
    }

    #[test]
    fn batch_stdin_success_hashes_payload() {
        let (_tmp, repo) = init_test_repo();
        let payload = b"hello\n";
        let result = run_git_unchecked_with_stdin(
            &repo,
            &["hash-object", "--stdin"],
            payload,
            Duration::from_secs(10),
            4096,
        )
        .unwrap();
        assert_eq!(result.code, Some(0));
        assert!(!result.truncated);
        let expected = git_hash_object(&repo, payload);
        assert_eq!(
            String::from_utf8_lossy(&result.stdout).trim(),
            expected.trim()
        );
    }

    #[test]
    fn batch_stdin_larger_than_the_pipe_buffer_does_not_deadlock() {
        let (_tmp, repo) = init_test_repo();
        let payload = vec![b'x'; OVER_PIPE];
        let result = run_git_unchecked_with_stdin(
            &repo,
            &["hash-object", "--stdin"],
            &payload,
            Duration::from_secs(30),
            4096,
        )
        .unwrap();
        assert_eq!(result.code, Some(0));
        assert!(!result.truncated);
        let expected = git_hash_object(&repo, &payload);
        assert_eq!(
            String::from_utf8_lossy(&result.stdout).trim(),
            expected.trim()
        );
    }

    #[test]
    fn batch_stdin_and_stdout_both_exceed_pipe_capacity() {
        let (_tmp, repo) = init_test_repo();
        let oid = "0".repeat(40);
        let mut input = Vec::new();
        for _ in 0..3000 {
            input.extend_from_slice(oid.as_bytes());
            input.push(b'\n');
        }
        assert!(input.len() > PIPE_BUFFER_BYTES);
        let result = run_git_unchecked_with_stdin(
            &repo,
            &["cat-file", "--batch-check"],
            &input,
            Duration::from_secs(30),
            32 * 1024 * 1024,
        )
        .unwrap();
        assert_eq!(result.code, Some(0));
        assert!(
            result.stdout.len() > PIPE_BUFFER_BYTES,
            "output is {} bytes",
            result.stdout.len()
        );
        assert!(!result.truncated);
        let text = String::from_utf8_lossy(&result.stdout);
        assert!(text.contains("missing"));
    }

    #[test]
    fn batch_stdin_reports_nonzero_git_exit() {
        let (_tmp, repo) = init_test_repo();
        let result = run_git_unchecked_with_stdin(
            &repo,
            &[
                "rev-parse",
                "--verify",
                "--end-of-options",
                "definitely-missing-ref",
            ],
            b"",
            Duration::from_secs(10),
            4096,
        )
        .unwrap();
        assert_ne!(result.code, Some(0));
        assert!(run_git_checked_with_stdin(
            &repo,
            &[
                "rev-parse",
                "--verify",
                "--end-of-options",
                "definitely-missing-ref",
            ],
            b"",
            Duration::from_secs(10),
            4096,
        )
        .is_err());
    }

    #[test]
    fn batch_stdin_truncation_is_visible_on_zero_exit() {
        let (_tmp, repo) = init_test_repo();
        let result = run_git_unchecked_with_stdin(
            &repo,
            &["hash-object", "--stdin"],
            b"hello\n",
            Duration::from_secs(10),
            4,
        )
        .unwrap();
        assert_eq!(result.code, Some(0));
        assert_eq!(result.stdout.len(), 4);
        assert!(result.truncated);
        let checked = run_git_checked_with_stdin(
            &repo,
            &["hash-object", "--stdin"],
            b"hello\n",
            Duration::from_secs(10),
            4,
        )
        .unwrap();
        assert_eq!(checked.code, Some(0));
        assert!(checked.truncated);
    }

    #[test]
    fn timeout_kills_child_and_joins_a_blocked_writer() {
        let payload = vec![b'x'; OVER_PIPE];
        let mut command = Command::new("sleep");
        command.arg("30");
        let result = run_process(
            command,
            Some(payload.as_slice()),
            Duration::from_millis(400),
            1024,
        )
        .unwrap();
        assert_eq!(result.code, None);
        assert!(result.truncated);
    }

    #[test]
    fn early_exit_broken_pipe_is_not_a_write_failure() {
        let (_tmp, repo) = init_test_repo();
        let payload = vec![b'x'; OVER_PIPE];
        let result = run_git_unchecked_with_stdin(
            &repo,
            &["rev-parse", "--is-inside-work-tree"],
            &payload,
            Duration::from_secs(10),
            4096,
        )
        .unwrap();
        assert_eq!(result.code, Some(0));
        assert_eq!(String::from_utf8_lossy(&result.stdout).trim(), "true");
        assert!(!result.truncated);
    }

    fn init_test_repo() -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        git(&repo, &["init", "--initial-branch=master"]);
        git(&repo, &["config", "user.name", "SASE Test"]);
        git(&repo, &["config", "user.email", "sase@example.com"]);
        git(&repo, &["config", "commit.gpgsign", "false"]);
        (tmp, repo)
    }

    fn git(repo: &Path, args: &[&str]) {
        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?} failed");
    }

    fn git_hash_object(repo: &Path, payload: &[u8]) -> String {
        let mut child = std::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(["hash-object", "--stdin"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(payload).unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success(), "git hash-object failed");
        String::from_utf8(output.stdout).unwrap()
    }

    fn child_env_with_injected(injected: &[(&str, &str)]) -> String {
        let mut command = Command::new("env");
        for &(key, value) in injected {
            command.env(key, value);
        }
        apply_git_env(&mut command);
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "env failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    }
}
