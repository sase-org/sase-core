use std::fs::{self};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use sase_core::{
    sudo_manifest_sha256, validate_sudo_exec_started, SudoCommandWire,
    SudoExecStartedWire, SudoLedgerWire, SudoManifestWire,
    SudoOutputPolicyWire, SUDO_EXEC_STARTED_KIND,
    SUDO_EXEC_STARTED_WIRE_SCHEMA_VERSION,
};

use super::super::*;
use std::os::unix::fs::PermissionsExt;
use tempfile::TempDir;

pub(crate) struct Fixture {
    pub(crate) _tmp: TempDir,
    pub(crate) sudo_path: PathBuf,
    pub(crate) handoff_dir: PathBuf,
    pub(crate) manifest_path: PathBuf,
    pub(crate) config: SudoRunnerConfig,
    pub(crate) digest: String,
}

impl Fixture {
    pub(crate) fn new(manifest: SudoManifestWire) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let sudo_path = tmp.path().join("fake-sudo");
        fs::write(&sudo_path, fake_sudo_script()).unwrap();
        fs::set_permissions(&sudo_path, fs::Permissions::from_mode(0o755))
            .unwrap();
        let executor_path = tmp.path().join("fake-executor");
        fs::write(&executor_path, fake_executor_script()).unwrap();
        fs::set_permissions(&executor_path, fs::Permissions::from_mode(0o755))
            .unwrap();
        fs::write(
            sudo_path.with_extension("executor"),
            executor_path.display().to_string(),
        )
        .unwrap();
        let manifest_path = tmp.path().join("manifest.json");
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap())
            .unwrap();
        let digest = sudo_manifest_sha256(&manifest).unwrap();
        let mut config = SudoRunnerConfig::test(sudo_path.clone());
        config.launcher = Some(SudoRunnerLauncher {
            program: executor_path.clone(),
            prefix: Vec::new(),
        });
        // Force the detached path so handshake tests exercise it on
        // every platform. On Linux the platform default already
        // resolves to true where procfs is available; elsewhere the
        // default would report "unsupported" and skip the handshake
        // these tests assert on.
        config.detached_execution = Some(true);
        Self {
            handoff_dir: tmp.path().to_path_buf(),
            _tmp: tmp,
            sudo_path,
            manifest_path,
            config,
            digest,
        }
    }

    pub(crate) fn run(
        &self,
    ) -> (Result<(), SudoRunnerCliError>, String, String) {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let result = run_sudo_runner_cli_with_io(
            [
                "-m".to_string(),
                self.manifest_path.display().to_string(),
                "-e".to_string(),
                self.digest.clone(),
            ],
            &self.config,
            &mut stdout,
            &mut stderr,
        );
        (
            result,
            String::from_utf8(stdout).unwrap(),
            String::from_utf8(stderr).unwrap(),
        )
    }

    pub(crate) fn run_detach(
        &self,
    ) -> (Result<(), SudoRunnerCliError>, String, String) {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let result = run_sudo_runner_cli_with_io(
            [
                "-m".to_string(),
                self.manifest_path.display().to_string(),
                "-e".to_string(),
                self.digest.clone(),
                "-d".to_string(),
                self.handoff_dir.display().to_string(),
            ],
            &self.config,
            &mut stdout,
            &mut stderr,
        );
        (
            result,
            String::from_utf8(stdout).unwrap(),
            String::from_utf8(stderr).unwrap(),
        )
    }

    pub(crate) fn calls(&self) -> Vec<String> {
        fs::read_to_string(self.sudo_path.with_extension("calls"))
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    pub(crate) fn argv_calls(&self) -> Vec<Vec<String>> {
        let content = fs::read_to_string(self.sudo_path.with_extension("argv"))
            .unwrap_or_default();
        let mut calls = Vec::new();
        let mut current = Vec::new();
        let mut in_call = false;
        for line in content.lines() {
            match line {
                "BEGIN" => {
                    in_call = true;
                    current.clear();
                }
                "END" if in_call => {
                    calls.push(std::mem::take(&mut current));
                    in_call = false;
                }
                _ if in_call => current.push(line.to_string()),
                _ => {}
            }
        }
        calls
    }

    pub(crate) fn cwd_log(&self) -> Vec<String> {
        fs::read_to_string(self.sudo_path.with_extension("cwd"))
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    pub(crate) fn env_log(&self) -> String {
        fs::read_to_string(self.sudo_path.with_extension("env"))
            .unwrap_or_default()
    }

    pub(crate) fn touch(&self, suffix: &str) {
        fs::write(self.sudo_path.with_extension(suffix), "").unwrap();
    }

    pub(crate) fn write_marker(&self, suffix: &str, value: &str) {
        fs::write(self.sudo_path.with_extension(suffix), value).unwrap();
    }

    pub(crate) fn runner_program(&self) -> &Path {
        &self
            .config
            .launcher
            .as_ref()
            .expect("fixture injects a launcher")
            .program
    }

    pub(crate) fn with_python_hosted_launcher(&mut self) {
        let launcher = self
            .config
            .launcher
            .as_mut()
            .expect("fixture injects a launcher");
        launcher.prefix = PYTHON_HOSTED_SUDO_RUNNER_PREFIX
            .iter()
            .map(|value| (*value).to_string())
            .collect();
    }

    pub(crate) fn exec_relaunch_argv(&self) -> Vec<String> {
        let call = self
            .argv_calls()
            .into_iter()
            .find(|argv| argv.iter().any(|arg| arg == INTERNAL_ROOT_EXEC_FLAG))
            .expect("missing --internal-root-exec sudo invocation");
        argv_after_separator(&call).to_vec()
    }
}

pub(crate) fn manifest() -> SudoManifestWire {
    SudoManifestWire {
        schema_version: 1,
        request_id: "req-1".to_string(),
        host: "athena".to_string(),
        host_is_remote: false,
        run_as: "root".to_string(),
        cwd: "/tmp".to_string(),
        env: [("APP_MODE".to_string(), "reviewed".to_string())]
            .into_iter()
            .collect(),
        stop_on_failure: true,
        output_to_agent: SudoOutputPolicyWire::Tail,
        commands: vec![
            SudoCommandWire {
                id: "one".to_string(),
                argv: vec!["ok".to_string()],
                why: "Run ok".to_string(),
                timeout_seconds: Some(1.0),
                shell: false,
            },
            SudoCommandWire {
                id: "two".to_string(),
                argv: vec!["ok".to_string()],
                why: "Run second".to_string(),
                timeout_seconds: Some(1.0),
                shell: false,
            },
        ],
        resume_from: None,
    }
}

pub(crate) fn fake_sudo_script() -> &'static str {
    r#"#!/bin/sh
set -eu
# Portable tool paths: this stub runs with a cleared environment (no PATH),
# and macOS keeps the BSD userland (cat, rm, rmdir) in /bin while Linux
# usrmerge provides them in /usr/bin as well. Resolve each tool once into
# an unexported variable so nothing leaks into the env capture below.
if [ -x /usr/bin/cat ]; then _CAT=/usr/bin/cat; else _CAT=/bin/cat; fi
if [ -x /usr/bin/rm ]; then _RM=/usr/bin/rm; else _RM=/bin/rm; fi
if [ -x /usr/bin/rmdir ]; then _RMDIR=/usr/bin/rmdir; else _RMDIR=/bin/rmdir; fi
base="$0"
{
  printf 'BEGIN\n'
  for arg do
    printf '%s\n' "$arg"
  done
  printf 'END\n'
} >> "$base.argv"
pwd >> "$base.cwd"
printf '%s\n' "$*" >> "$base.calls"
if [ "$#" -eq 1 ] && [ "$1" = "-k" ]; then
  printf 'cleanup\n' >> "$base.cleanups"
  if [ -f "$base.final_k_fail" ]; then
    count=$(/usr/bin/wc -l < "$base.cleanups")
    if [ "$count" -ge 2 ]; then
      exit 1
    fi
  fi
  exit 0
fi
if [ "$#" -eq 1 ] && [ "$1" = "-v" ]; then
  if [ -f "$base.auth_fail" ]; then exit 1; fi
  exit 0
fi
if [ "$#" -eq 2 ] && [ "$1" = "-n" ] && [ "$2" = "-v" ]; then
  if [ -f "$base.probe_fail" ]; then exit 1; fi
  if [ -f "$base.remove_cwd_on_probe" ]; then
    cwd_to_remove="$("$_CAT" "$base.remove_cwd_on_probe")"
    "$_RMDIR" "$cwd_to_remove" 2>/dev/null || "$_RM" -rf "$cwd_to_remove"
  fi
  exit 0
fi
for arg do
  if [ "$arg" = "--" ]; then
    break
  fi
  if [ "$arg" = "-D" ] || [ "$arg" = "--chdir" ]; then
    printf 'sudo: you are not permitted to use the -D option with simulated-command\n' >&2
    exit 1
  fi
done
/usr/bin/env | /usr/bin/sort > "$base.env"
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--" ]; then
    shift
    break
  fi
  shift
done
cmd="${1:-}"
executor_path="$("$_CAT" "$base.executor" 2>/dev/null || true)"
if [ -n "$executor_path" ] && [ "$cmd" = "$executor_path" ]; then
  if [ -f "$base.root_spawn_fail" ]; then exit 42; fi
  "$@"
  exit $?
fi
case "$cmd" in
  ok)
    printf 'stdout-ok\n'
    printf 'stderr-ok\n' >&2
    exit 0
    ;;
  fail)
    printf 'failed-command\n'
    exit 7
    ;;
  slow)
    /bin/sleep 5
    exit 0
    ;;
  read-file)
    "$_CAT" "$2"
    exit 0
    ;;
  /usr/bin/apt-get)
    printf 'apt-get-simulated\n'
    exit 0
    ;;
  *)
    exit 0
    ;;
esac
"#
}

pub(crate) fn fake_executor_script() -> &'static str {
    r#"#!/bin/sh
set -eu
detach_dir=""
expected=""
started_path=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --detach-dir)
      detach_dir="$2"
      shift 2
      ;;
    --expected-sha256)
      expected="$2"
      shift 2
      ;;
    --started-path)
      started_path="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
if [ -z "$started_path" ]; then
  exit 64
fi
tmp="$started_path.tmp"
cat > "$tmp" <<EOF
{"schema_version":1,"kind":"sudo_exec_started","manifest_sha256":"$expected","executor_pid":$$,"executor_identity":"boot-a:12345","ledger_path":"$detach_dir/ledger.json","log_path":"$detach_dir/output.log","started_at":1800000000.0}
EOF
mv "$tmp" "$started_path"
"#
}

pub(crate) fn ledger(stdout: &str) -> SudoLedgerWire {
    serde_json::from_str(stdout.trim()).unwrap()
}

pub(crate) fn current_username() -> String {
    let uid = unsafe { libc::geteuid() };
    let mut pwd = std::mem::MaybeUninit::<libc::passwd>::uninit();
    let mut result = std::ptr::null_mut();
    let mut buffer = vec![0u8; 16 * 1024];
    let code = unsafe {
        libc::getpwuid_r(
            uid,
            pwd.as_mut_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        )
    };
    assert_eq!(code, 0);
    assert!(!result.is_null());
    let pwd = unsafe { pwd.assume_init() };
    unsafe { std::ffi::CStr::from_ptr(pwd.pw_name) }
        .to_string_lossy()
        .into_owned()
}

pub(crate) fn write_manifest(
    path: &Path,
    manifest: &SudoManifestWire,
) -> String {
    fs::write(path, serde_json::to_vec(manifest).unwrap()).unwrap();
    sudo_manifest_sha256(manifest).unwrap()
}

pub(crate) fn worker_cli_args(
    manifest_path: &Path,
    digest: String,
    detach_dir: &Path,
) -> [String; 13] {
    [
        "--internal-root-worker".to_string(),
        "-m".to_string(),
        manifest_path.display().to_string(),
        "-e".to_string(),
        digest,
        "-d".to_string(),
        detach_dir.display().to_string(),
        "--started-path".to_string(),
        detach_dir.join(STARTED_FILENAME).display().to_string(),
        "--parent-uid".to_string(),
        unsafe { libc::geteuid() }.to_string(),
        "--parent-gid".to_string(),
        unsafe { libc::getegid() }.to_string(),
    ]
}

pub(crate) fn exec_cli_args(
    manifest_path: &Path,
    digest: String,
    detach_dir: &Path,
) -> [String; 13] {
    [
        "--internal-root-exec".to_string(),
        "-m".to_string(),
        manifest_path.display().to_string(),
        "-e".to_string(),
        digest,
        "-d".to_string(),
        detach_dir.display().to_string(),
        "--started-path".to_string(),
        detach_dir.join(STARTED_FILENAME).display().to_string(),
        "--parent-uid".to_string(),
        unsafe { libc::geteuid() }.to_string(),
        "--parent-gid".to_string(),
        unsafe { libc::getegid() }.to_string(),
    ]
}

/// Synthetic identity both sides of a worker handshake agree on in
/// tests. Production derives this from Linux procfs; tests inject the
/// same value through `SudoRunnerConfig::process_identity_override` so
/// the handshake round-trips on platforms without `/proc`.
pub(crate) const TEST_PROCESS_IDENTITY_TOKEN: &str = "test-boot-id:12345";

pub(crate) fn write_self_started(
    dir: &Path,
    digest: &str,
) -> SudoExecStartedWire {
    let pid = std::process::id();
    // The handshake binds ledger/log paths to the canonical handoff
    // directory, exactly as production's publisher writes them, so
    // canonicalize here. Otherwise the simulated handshake disagrees
    // with `validate_handshake_paths` under any temp root with a
    // symlinked ancestor.
    let dir = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
    let handshake = SudoExecStartedWire {
        schema_version: SUDO_EXEC_STARTED_WIRE_SCHEMA_VERSION,
        kind: SUDO_EXEC_STARTED_KIND.to_string(),
        manifest_sha256: digest.to_string(),
        executor_pid: pid,
        executor_identity: TEST_PROCESS_IDENTITY_TOKEN.to_string(),
        ledger_path: dir.join(LEDGER_FILENAME).display().to_string(),
        log_path: dir.join(LOG_FILENAME).display().to_string(),
        started_at: current_unix_time().unwrap(),
    };
    validate_sudo_exec_started(&handshake, None).unwrap();
    fs::write(
        dir.join(STARTED_FILENAME),
        format!("{}\n", serde_json::to_string(&handshake).unwrap()),
    )
    .unwrap();
    handshake
}

pub(crate) fn write_executable(path: &Path, contents: &str) {
    fs::write(path, contents).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

pub(crate) fn waiting_worker_script() -> &'static str {
    r#"#!/bin/sh
set -eu
{
  printf 'BEGIN\n'
  for arg do
    printf '%s\n' "$arg"
  done
  printf 'END\n'
} >> "$0.argv"
detach_dir=""
started_path=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --detach-dir)
      detach_dir="$2"
      shift 2
      ;;
    --started-path)
      started_path="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
printf '%s\n' "$$" > "$detach_dir/worker.pid"
printf 'waiting\n' > "$detach_dir/worker.state"
while [ ! -f "$started_path" ]; do
  sleep 0.05
done
printf 'started\n' > "$detach_dir/worker.state"
printf 'ran\n' > "$detach_dir/worker.ran"
"#
}

pub(crate) fn process_alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

pub(crate) fn argv_after_separator(call: &[String]) -> &[String] {
    let index = call
        .iter()
        .position(|arg| arg == "--")
        .expect("sudo invocation missing --");
    &call[index + 1..]
}

pub(crate) fn recorded_argv(path: &Path) -> Vec<String> {
    let content =
        fs::read_to_string(path.with_extension("argv")).unwrap_or_default();
    let mut current = Vec::new();
    let mut in_call = false;
    for line in content.lines() {
        match line {
            "BEGIN" => {
                in_call = true;
                current.clear();
            }
            "END" if in_call => return current,
            _ if in_call => current.push(line.to_string()),
            _ => {}
        }
    }
    current
}

pub(crate) fn python_hosted_prefix() -> Vec<String> {
    PYTHON_HOSTED_SUDO_RUNNER_PREFIX
        .iter()
        .map(|value| (*value).to_string())
        .collect()
}

pub(crate) fn run_waiting_worker_exec(
    prefix: Vec<String>,
) -> (Vec<String>, Result<(), SudoRunnerCliError>) {
    let tmp = tempfile::tempdir().unwrap();
    let mut manifest = manifest();
    manifest.run_as = current_username();
    manifest.cwd = tmp.path().display().to_string();
    manifest.commands.truncate(1);
    manifest.commands[0].argv = vec!["/bin/true".to_string()];
    let manifest_path = tmp.path().join("manifest.json");
    let digest = write_manifest(&manifest_path, &manifest);
    let worker_path = tmp.path().join("waiting-worker");
    write_executable(&worker_path, waiting_worker_script());
    let mut config = SudoRunnerConfig::test(tmp.path().join("unused-sudo"));
    config.launcher = Some(SudoRunnerLauncher {
        program: worker_path.clone(),
        prefix,
    });
    config.detached_execution = Some(true);
    config.process_identity_override =
        Some(TEST_PROCESS_IDENTITY_TOKEN.to_string());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let result = run_sudo_runner_cli_with_io(
        exec_cli_args(&manifest_path, digest, tmp.path()),
        &config,
        &mut stdout,
        &mut stderr,
    );
    let argv_path = worker_path.with_extension("argv");
    // The launcher stub records argv from a freshly spawned child; under
    // a parallel-test fork storm that child can wait seconds before its
    // first line runs (observed >1s on macOS), so the deadline keeps
    // headroom far beyond the steady-state milliseconds. The loop still
    // returns as soon as the file exists.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !argv_path.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    let argv = recorded_argv(&worker_path);
    if let Ok(pid) = fs::read_to_string(tmp.path().join("worker.pid")) {
        if let Ok(pid) = pid.trim().parse::<u32>() {
            let _ = unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
        }
    }
    (argv, result)
}
