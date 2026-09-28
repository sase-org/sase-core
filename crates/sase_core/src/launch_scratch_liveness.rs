//! Batch launch-scratch liveness observation over a procfs root.
//!
//! Runner-exit cleanup and the dead-launch backstop share this
//! observer: it scans `/proc` once and answers, for many
//! launch-keyed scratch candidates, whether a live same-uid process
//! still holds each one. A process holds a candidate when its
//! inherited environment names the candidate (a `TMPDIR`-shaped
//! variable at or under it, or `SASE_LAUNCH_SCRATCH_KEY=<key>`) or
//! its cwd resolves at or under it.
//!
//! Unreadable process detail is fail-closed, with one exception: a
//! process that started strictly before the scratch directory
//! existed (systemd --user, sd-pam, ssh-agent pre-date every
//! launch) cannot have inherited the launch environment, so it is
//! neither a holder nor an incomplete observation. Hosts without a
//! usable procfs report an explicit `unobservable` observer instead
//! of `complete=false`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

pub const LAUNCH_SCRATCH_LIVENESS_WIRE_SCHEMA_VERSION: u32 = 1;

pub const LAUNCH_SCRATCH_OBSERVER_PROCFS: &str = "procfs";
pub const LAUNCH_SCRATCH_OBSERVER_UNOBSERVABLE: &str = "unobservable";

const LIVE_PATH_ENV_VARS: [&str; 5] = [
    "TMPDIR",
    "TMP",
    "TEMP",
    "CARGO_TARGET_DIR",
    "CARGO_BUILD_BUILD_DIR",
];

const LAUNCH_SCRATCH_KEY_ENV: &str = "SASE_LAUNCH_SCRATCH_KEY";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchScratchLivenessCandidateWire {
    pub scratch_key: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchScratchLivenessRequestWire {
    pub schema_version: u32,
    pub candidates: Vec<LaunchScratchLivenessCandidateWire>,
    pub proc_root: String,
    pub current_pid: u32,
    #[serde(default)]
    pub exempt_pids: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LaunchScratchCandidateLivenessWire {
    pub scratch_key: String,
    pub path: String,
    pub live: bool,
    pub complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LaunchScratchLivenessResultWire {
    pub schema_version: u32,
    pub observer: String,
    pub candidates: Vec<LaunchScratchCandidateLivenessWire>,
    pub exempted_pre_launch: u64,
    pub unreadable: u64,
    pub diagnostics: Vec<String>,
}

struct Candidate {
    scratch_key: String,
    path: String,
    normalized: PathBuf,
    birth_epoch: Option<f64>,
    live: bool,
    complete: bool,
}

struct BootClock {
    btime_epoch: Option<f64>,
    ticks_per_second: Option<f64>,
}

pub fn observe_launch_scratch_liveness(
    request: &LaunchScratchLivenessRequestWire,
) -> LaunchScratchLivenessResultWire {
    let mut candidates: Vec<Candidate> = request
        .candidates
        .iter()
        .map(|candidate| {
            let raw = PathBuf::from(&candidate.path);
            Candidate {
                scratch_key: candidate.scratch_key.clone(),
                path: candidate.path.clone(),
                birth_epoch: candidate_birth_epoch(&raw),
                normalized: normalize_path(&raw),
                live: false,
                complete: true,
            }
        })
        .collect();

    let proc_root = Path::new(&request.proc_root);
    if fs::read_dir(proc_root).is_err() {
        for candidate in &mut candidates {
            candidate.complete = false;
        }
        return unobservable_result(
            candidates,
            format!("procfs at {} is not observable", proc_root.display()),
        );
    }

    let clock = read_boot_clock(proc_root);
    let exempt: BTreeSet<u32> = request.exempt_pids.iter().copied().collect();
    #[cfg(unix)]
    let own_uid = unsafe { libc::geteuid() };

    let mut diagnostics: Vec<String> = Vec::new();
    let mut exempted_pre_launch = 0_u64;
    let mut unreadable = 0_u64;

    let entries = match fs::read_dir(proc_root) {
        Ok(entries) => entries,
        Err(error) => {
            for candidate in &mut candidates {
                candidate.complete = false;
            }
            return unobservable_result(
                candidates,
                format!(
                    "could not list procfs at {}: {error}",
                    proc_root.display()
                ),
            );
        }
    };
    for entry in entries {
        let pid_dir = match entry {
            Ok(entry) => entry.path(),
            Err(error) => {
                diagnostics.push(format!(
                    "procfs entry unreadable under {}: {error}",
                    proc_root.display()
                ));
                for candidate in candidates.iter_mut() {
                    candidate.complete = false;
                }
                continue;
            }
        };
        let pid: u32 = match pid_dir
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.parse().ok())
        {
            Some(pid) => pid,
            None => continue,
        };
        if pid == request.current_pid || exempt.contains(&pid) {
            continue;
        }
        match process_uid(&pid_dir) {
            Some(uid) => {
                #[cfg(unix)]
                if uid != own_uid {
                    continue;
                }
                #[cfg(not(unix))]
                let _ = uid;
            }
            // A vanished pid dir means the process already exited.
            None => continue,
        }

        let mut tally = PidTally::default();
        match fs::read(pid_dir.join("environ")) {
            Ok(environ) => {
                observe_environ(&environ, &mut candidates);
            }
            Err(error) if !is_missing(&error) => {
                observe_unreadable(
                    pid,
                    "environ",
                    &error.to_string(),
                    &pid_dir,
                    &clock,
                    &mut candidates,
                    &mut diagnostics,
                    &mut tally,
                );
            }
            // A missing environ reads as an exited process; its cwd
            // check below still applies, matching the Python probe.
            Err(_) => {}
        }

        match fs::read_link(pid_dir.join("cwd")) {
            Ok(cwd) => {
                let cwd = normalize_path(&cwd);
                for candidate in candidates.iter_mut() {
                    if is_at_or_under(&cwd, &candidate.normalized) {
                        candidate.live = true;
                    }
                }
            }
            Err(error) if is_missing(&error) => {}
            Err(error) => {
                observe_unreadable(
                    pid,
                    "cwd",
                    &error.to_string(),
                    &pid_dir,
                    &clock,
                    &mut candidates,
                    &mut diagnostics,
                    &mut tally,
                );
            }
        }
        if tally.decisions > 0 {
            if tally.exempted == tally.decisions {
                exempted_pre_launch += 1;
            } else {
                unreadable += 1;
            }
        }
    }

    if exempted_pre_launch > 0 {
        diagnostics.push(format!(
            "pre-launch exemption: {exempted_pre_launch} processes started \
             before the observed scratch"
        ));
    }

    LaunchScratchLivenessResultWire {
        schema_version: LAUNCH_SCRATCH_LIVENESS_WIRE_SCHEMA_VERSION,
        observer: LAUNCH_SCRATCH_OBSERVER_PROCFS.to_string(),
        candidates: candidates
            .into_iter()
            .map(|candidate| LaunchScratchCandidateLivenessWire {
                scratch_key: candidate.scratch_key,
                path: candidate.path,
                live: candidate.live,
                complete: candidate.complete,
            })
            .collect(),
        exempted_pre_launch,
        unreadable,
        diagnostics,
    }
}

/// Per-pid exemption bookkeeping for one unreadable source at a time.
///
/// `decisions` counts candidates this pid needed an exemption
/// decision for; `exempted` counts the ones it passed. A pid whose
/// every decision exempts is pre-launch noise, otherwise it stays
/// an incomplete observation.
#[derive(Debug, Default)]
struct PidTally {
    decisions: u64,
    exempted: u64,
}

fn observe_environ(environ: &[u8], candidates: &mut [Candidate]) {
    for item in environ.split(|byte| *byte == 0) {
        let Some(position) = item.iter().position(|byte| *byte == b'=') else {
            continue;
        };
        let (key, value) = item.split_at(position);
        let value = &value[1..];
        let (Ok(key), Ok(value)) =
            (std::str::from_utf8(key), std::str::from_utf8(value))
        else {
            continue;
        };
        if key == LAUNCH_SCRATCH_KEY_ENV {
            for candidate in candidates.iter_mut() {
                if value == candidate.scratch_key {
                    candidate.live = true;
                }
            }
        } else if LIVE_PATH_ENV_VARS.contains(&key) {
            let value_path = normalize_path(&PathBuf::from(value));
            for candidate in candidates.iter_mut() {
                if is_at_or_under(&value_path, &candidate.normalized) {
                    candidate.live = true;
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn observe_unreadable(
    pid: u32,
    source: &str,
    detail: &str,
    pid_dir: &Path,
    clock: &BootClock,
    candidates: &mut [Candidate],
    diagnostics: &mut Vec<String>,
    tally: &mut PidTally,
) {
    let started = process_start_epoch(pid_dir, clock);
    let mut still_unreadable = false;
    for candidate in candidates.iter_mut() {
        tally.decisions += 1;
        if is_pre_launch(started, candidate.birth_epoch) {
            tally.exempted += 1;
        } else {
            candidate.complete = false;
            still_unreadable = true;
        }
    }
    if still_unreadable {
        diagnostics.push(format!("{pid}: {source} unreadable: {detail}"));
    }
}

fn unobservable_result(
    candidates: Vec<Candidate>,
    reason: String,
) -> LaunchScratchLivenessResultWire {
    LaunchScratchLivenessResultWire {
        schema_version: LAUNCH_SCRATCH_LIVENESS_WIRE_SCHEMA_VERSION,
        observer: LAUNCH_SCRATCH_OBSERVER_UNOBSERVABLE.to_string(),
        candidates: candidates
            .into_iter()
            .map(|candidate| LaunchScratchCandidateLivenessWire {
                scratch_key: candidate.scratch_key,
                path: candidate.path,
                live: false,
                complete: false,
            })
            .collect(),
        exempted_pre_launch: 0,
        unreadable: 0,
        diagnostics: vec![reason],
    }
}

fn process_uid(pid_dir: &Path) -> Option<u32> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        fs::symlink_metadata(pid_dir)
            .ok()
            .map(|metadata| metadata.uid())
    }
    #[cfg(not(unix))]
    {
        let _ = pid_dir;
        None
    }
}

fn is_missing(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
    )
}

fn normalize_path(path: &Path) -> PathBuf {
    if let Ok(canonical) = path.canonicalize() {
        return canonical;
    }
    if path.is_absolute() {
        return path.to_path_buf();
    }
    match std::env::current_dir() {
        Ok(cwd) => cwd.join(path),
        Err(_) => path.to_path_buf(),
    }
}

fn is_at_or_under(path: &Path, parent: &Path) -> bool {
    path == parent || path.starts_with(parent)
}

fn candidate_birth_epoch(path: &Path) -> Option<f64> {
    let metadata = fs::metadata(path).ok()?;
    let created = metadata.created().ok()?;
    created
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs_f64())
}

fn read_boot_clock(proc_root: &Path) -> BootClock {
    let content =
        fs::read_to_string(proc_root.join("stat")).unwrap_or_default();
    let btime_epoch = content
        .lines()
        .find_map(|line| line.strip_prefix("btime "))
        .and_then(|value| value.trim().parse::<f64>().ok());
    #[cfg(unix)]
    let ticks_per_second = {
        let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        if ticks > 0 {
            Some(ticks as f64)
        } else {
            None
        }
    };
    #[cfg(not(unix))]
    let ticks_per_second: Option<f64> = None;
    BootClock {
        btime_epoch,
        ticks_per_second,
    }
}

/// Start time of one pid as epoch seconds, from `stat` field 22
/// (starttime ticks since boot) plus `btime` in `stat`.
fn process_start_epoch(pid_dir: &Path, clock: &BootClock) -> Option<f64> {
    let btime = clock.btime_epoch?;
    let ticks_per_second = clock.ticks_per_second?;
    let content = fs::read_to_string(pid_dir.join("stat")).ok()?;
    let after_comm = content.rsplit_once(')')?.1;
    let starttime =
        after_comm.split_whitespace().nth(19)?.parse::<f64>().ok()?;
    Some(btime + starttime / ticks_per_second)
}

fn is_pre_launch(started: Option<f64>, birth: Option<f64>) -> bool {
    match (started, birth) {
        (Some(started), Some(birth)) => started < birth,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    const SCRATCH_KEY: &str = "proj-ws7-260914_120000";

    fn current_epoch() -> f64 {
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
    }

    fn request(
        candidates: &[(&str, &Path)],
        proc_root: &Path,
    ) -> LaunchScratchLivenessRequestWire {
        LaunchScratchLivenessRequestWire {
            schema_version: LAUNCH_SCRATCH_LIVENESS_WIRE_SCHEMA_VERSION,
            candidates: candidates
                .iter()
                .map(|(key, path)| LaunchScratchLivenessCandidateWire {
                    scratch_key: (*key).to_string(),
                    path: path.to_string_lossy().into_owned(),
                })
                .collect(),
            proc_root: proc_root.to_string_lossy().into_owned(),
            current_pid: u32::MAX,
            exempt_pids: Vec::new(),
        }
    }

    fn write_process_stat(pid_dir: &Path, starttime_ticks: u64) {
        // `stat` layout: pid (comm) state ... with starttime as field
        // 22 overall, the 20th whitespace token after the comm close.
        let mut fields = vec!["R".to_string(), "1".to_string()];
        fields.extend(std::iter::repeat_n("0".to_string(), 17));
        fields.push(starttime_ticks.to_string());
        let content = format!("1 (fake-proc) {}\n", fields.join(" "));
        let mut file = fs::File::create(pid_dir.join("stat")).unwrap();
        file.write_all(content.as_bytes()).unwrap();
    }

    fn write_proc_stat(proc_root: &Path, btime: f64) {
        let mut file = fs::File::create(proc_root.join("stat")).unwrap();
        writeln!(file, "cpu  0 0 0 0 0 0 0 0 0 0").unwrap();
        writeln!(file, "btime {}", btime.trunc() as u64).unwrap();
    }

    fn candidate_dir(root: &Path, bucket: &str) -> PathBuf {
        let path = root.join(bucket).join(SCRATCH_KEY);
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn live_holder_matches_environ_path() {
        let temp = tempdir().unwrap();
        let candidate = candidate_dir(temp.path(), "cargo-targets");
        let proc_root = temp.path().join("proc");
        fs::create_dir_all(&proc_root).unwrap();
        let pid_dir = proc_root.join("4242");
        fs::create_dir_all(&pid_dir).unwrap();
        fs::write(
            pid_dir.join("environ"),
            format!("TMPDIR={}\0", candidate.join("nested").display()),
        )
        .unwrap();

        let result = observe_launch_scratch_liveness(&request(
            &[(SCRATCH_KEY, &candidate)],
            &proc_root,
        ));

        assert_eq!(result.observer, LAUNCH_SCRATCH_OBSERVER_PROCFS);
        assert_eq!(result.candidates.len(), 1);
        assert!(result.candidates[0].live);
        assert!(result.candidates[0].complete);
    }

    #[test]
    fn live_holder_matches_launch_key_and_cwd() {
        let temp = tempdir().unwrap();
        let candidate = candidate_dir(temp.path(), "agent-tmp");
        let proc_root = temp.path().join("proc");
        fs::create_dir_all(&proc_root).unwrap();
        let key_dir = proc_root.join("1111");
        fs::create_dir_all(&key_dir).unwrap();
        fs::write(
            key_dir.join("environ"),
            format!("{LAUNCH_SCRATCH_KEY_ENV}={SCRATCH_KEY}\0"),
        )
        .unwrap();
        // The cwd holder needs a real symlink, so it only runs
        // on unix; the launch-key holder above runs everywhere.
        #[cfg(unix)]
        {
            let cwd_dir = proc_root.join("2222");
            fs::create_dir_all(&cwd_dir).unwrap();
            fs::write(cwd_dir.join("environ"), "PATH=/usr/bin\0").unwrap();
            std::os::unix::fs::symlink(&candidate, cwd_dir.join("cwd"))
                .unwrap();
        }

        let result = observe_launch_scratch_liveness(&request(
            &[(SCRATCH_KEY, &candidate)],
            &proc_root,
        ));

        assert!(result.candidates[0].live);
        assert!(result.candidates[0].complete);
    }

    #[test]
    fn pre_launch_unreadable_process_is_exempt() {
        let temp = tempdir().unwrap();
        let now = current_epoch();
        // A pre-launch daemon started ~1000s ago; the scratch dir is
        // brand new, so the daemon predates it and is exempt.
        let proc_root = temp.path().join("proc");
        fs::create_dir_all(&proc_root).unwrap();
        write_proc_stat(&proc_root, now - 1000.0);
        let pid_dir = proc_root.join("1");
        fs::create_dir_all(&pid_dir).unwrap();
        fs::create_dir(pid_dir.join("environ")).unwrap();
        write_process_stat(&pid_dir, 5);
        let candidate = candidate_dir(temp.path(), "cargo-targets");

        let result = observe_launch_scratch_liveness(&request(
            &[(SCRATCH_KEY, &candidate)],
            &proc_root,
        ));

        assert_eq!(result.observer, LAUNCH_SCRATCH_OBSERVER_PROCFS);
        assert!(!result.candidates[0].live);
        assert!(
            result.candidates[0].complete,
            "pre-launch process must not mark the observation incomplete: {}",
            result.diagnostics.join("; ")
        );
        assert_eq!(result.exempted_pre_launch, 1);
        assert_eq!(result.unreadable, 0);
    }

    #[test]
    fn later_unreadable_process_stays_incomplete() {
        let temp = tempdir().unwrap();
        let now = current_epoch();
        let proc_root = temp.path().join("proc");
        fs::create_dir_all(&proc_root).unwrap();
        write_proc_stat(&proc_root, now - 1000.0);
        let pid_dir = proc_root.join("9999");
        fs::create_dir_all(&pid_dir).unwrap();
        // environ as a directory reads as an error, like EACCES on a
        // non-dumpable process; starttime far in the future keeps the
        // pre-launch exemption from applying.
        fs::create_dir(pid_dir.join("environ")).unwrap();
        write_process_stat(&pid_dir, 200_000);
        let candidate = candidate_dir(temp.path(), "cargo-targets");

        let result = observe_launch_scratch_liveness(&request(
            &[(SCRATCH_KEY, &candidate)],
            &proc_root,
        ));

        assert!(!result.candidates[0].live);
        assert!(!result.candidates[0].complete);
        assert_eq!(result.exempted_pre_launch, 0);
        assert_eq!(result.unreadable, 1);
    }

    #[test]
    fn missing_proc_root_is_unobservable() {
        let temp = tempdir().unwrap();
        let candidate = candidate_dir(temp.path(), "cargo-targets");

        let result = observe_launch_scratch_liveness(&request(
            &[(SCRATCH_KEY, &candidate)],
            &temp.path().join("no-such-proc"),
        ));

        assert_eq!(result.observer, LAUNCH_SCRATCH_OBSERVER_UNOBSERVABLE);
        assert!(!result.candidates[0].live);
        assert!(!result.candidates[0].complete);
    }

    #[test]
    fn own_and_exempt_pids_are_skipped() {
        let temp = tempdir().unwrap();
        let candidate = candidate_dir(temp.path(), "cargo-targets");
        let proc_root = temp.path().join("proc");
        fs::create_dir_all(&proc_root).unwrap();
        for pid in [7_u32, 8_u32] {
            let pid_dir = proc_root.join(pid.to_string());
            fs::create_dir_all(&pid_dir).unwrap();
            fs::write(
                pid_dir.join("environ"),
                format!("TMPDIR={}\0", candidate.display()),
            )
            .unwrap();
        }

        let mut req = request(&[(SCRATCH_KEY, &candidate)], &proc_root);
        req.current_pid = 7;
        req.exempt_pids = vec![8];
        let result = observe_launch_scratch_liveness(&req);

        assert!(!result.candidates[0].live);
        assert!(result.candidates[0].complete);
    }
}
