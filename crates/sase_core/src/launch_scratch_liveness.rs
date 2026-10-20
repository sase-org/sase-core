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
//! Unreadable process detail is fail-closed, with three exceptions. A
//! process that started strictly before the scratch directory
//! existed (systemd --user, sd-pam, ssh-agent pre-date every
//! launch) cannot have inherited the launch environment, so it is
//! neither a holder nor an incomplete observation. Nor can a zombie
//! or dead process, which has already released its address space,
//! fs struct and cwd, so it is skipped outright rather than probed.
//! A same-uid process whose ancestry is an init-started system
//! service (`sshd-session` under `ssh.service`, Tailscale SSH
//! children) is also neither a holder nor an incomplete observation:
//! its environment came from the service, not from a launch. That
//! exemption fails closed on orphans, same-uid or sudo ancestors,
//! pid reuse, cgroup v1, missing proc files, and walk cycles.
//! Hosts without a usable procfs report an explicit `unobservable`
//! observer instead of `complete=false`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::UNIX_EPOCH;

const SYSTEM_SERVICE_WALK_LIMIT: usize = 32;
const SYSTEM_SLICE_PREFIX: &str = "/system.slice/";

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
    #[serde(default)]
    pub exempted_system_service: u64,
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
    let own_uid = unsafe { libc::geteuid() as u32 };
    #[cfg(not(unix))]
    let own_uid = 0_u32;

    let mut diagnostics: Vec<String> = Vec::new();
    let mut exempted_pre_launch = 0_u64;
    let mut exempted_system_service = 0_u64;
    let mut unreadable = 0_u64;
    let mut exited_processes = 0_u64;

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

        let proc_stat = read_proc_stat(&pid_dir);
        if matches!(
            proc_stat.as_ref().map(|stat| stat.state),
            Some('Z') | Some('X')
        ) {
            // A zombie or dead process has already released its
            // address space, fs struct and cwd, so it cannot hold a
            // launch's environment or working directory.
            exited_processes += 1;
            continue;
        }
        let started = process_start_epoch(proc_stat.as_ref(), &clock);

        let mut tally = PidTally::default();
        let mut system_service: Option<bool> = None;
        let resolve_system_service = |slot: &mut Option<bool>| -> bool {
            *slot.get_or_insert_with(|| {
                is_system_service_session(
                    pid,
                    proc_stat.as_ref(),
                    proc_root,
                    own_uid,
                )
            })
        };
        match fs::read(pid_dir.join("environ")) {
            Ok(environ) => {
                observe_environ(&environ, &mut candidates);
            }
            Err(error) if !is_missing(&error) => {
                let system_service =
                    resolve_system_service(&mut system_service);
                observe_unreadable(
                    UnreadableProbe {
                        pid,
                        source: "environ",
                        detail: error.to_string(),
                        started,
                        proc_stat: proc_stat.as_ref(),
                        system_service,
                    },
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
                let system_service =
                    resolve_system_service(&mut system_service);
                observe_unreadable(
                    UnreadableProbe {
                        pid,
                        source: "cwd",
                        detail: error.to_string(),
                        started,
                        proc_stat: proc_stat.as_ref(),
                        system_service,
                    },
                    &mut candidates,
                    &mut diagnostics,
                    &mut tally,
                );
            }
        }
        if tally.decisions > 0 {
            if tally.exempted == tally.decisions {
                if system_service == Some(true) {
                    exempted_system_service += 1;
                } else {
                    exempted_pre_launch += 1;
                }
            } else {
                unreadable += 1;
            }
        }
    }

    if exited_processes > 0 {
        diagnostics.push(format!(
            "zombie exemption: {exited_processes} exited processes skipped"
        ));
    }
    if exempted_pre_launch > 0 {
        diagnostics.push(format!(
            "pre-launch exemption: {exempted_pre_launch} processes started \
             before the observed scratch"
        ));
    }
    if exempted_system_service > 0 {
        diagnostics.push(format!(
            "system-service exemption: {exempted_system_service} processes \
             belong to a system-service session"
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
        exempted_system_service,
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

struct UnreadableProbe<'a> {
    pid: u32,
    source: &'a str,
    detail: String,
    started: Option<f64>,
    proc_stat: Option<&'a ProcStat>,
    system_service: bool,
}

fn observe_unreadable(
    probe: UnreadableProbe<'_>,
    candidates: &mut [Candidate],
    diagnostics: &mut Vec<String>,
    tally: &mut PidTally,
) {
    let mut still_unreadable = false;
    for candidate in candidates.iter_mut() {
        tally.decisions += 1;
        if is_pre_launch(probe.started, candidate.birth_epoch)
            || probe.system_service
        {
            tally.exempted += 1;
        } else {
            candidate.complete = false;
            still_unreadable = true;
        }
    }
    if still_unreadable {
        diagnostics.push(format_pid_unreadable(
            probe.pid,
            probe.proc_stat,
            probe.source,
            &probe.detail,
        ));
    }
}

fn format_pid_unreadable(
    pid: u32,
    proc_stat: Option<&ProcStat>,
    source: &str,
    detail: &str,
) -> String {
    match proc_stat {
        Some(stat) => format!(
            "{pid} ({comm}, ppid {ppid}): {source} unreadable: {detail}",
            comm = stat.comm,
            ppid = stat.ppid,
        ),
        None => format!("{pid}: {source} unreadable: {detail}"),
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
        exempted_system_service: 0,
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
    if matches!(
        error.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
    ) {
        return true;
    }
    #[cfg(unix)]
    {
        error.raw_os_error() == Some(libc::ESRCH)
    }
    #[cfg(not(unix))]
    {
        false
    }
}

fn normalize_path(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        match std::env::current_dir() {
            Ok(cwd) => cwd.join(path),
            Err(_) => path.to_path_buf(),
        }
    };

    let mut existing = absolute.clone();
    let mut missing_suffix = Vec::new();
    loop {
        if let Ok(mut canonical) = existing.canonicalize() {
            for component in missing_suffix.iter().rev() {
                canonical.push(component);
            }
            return canonical;
        }

        let Some(component) = existing.components().next_back() else {
            break;
        };
        let suffix_component = match component {
            Component::Normal(value) => value.to_os_string(),
            Component::ParentDir => "..".into(),
            Component::CurDir => {
                existing.pop();
                continue;
            }
            Component::RootDir | Component::Prefix(_) => break,
        };
        missing_suffix.push(suffix_component);
        if !existing.pop() {
            break;
        }
    }

    absolute
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

/// Identity, state, parent, and start time of one pid, parsed from
/// `/proc/<pid>/stat`. `comm` can contain spaces and parentheses, so
/// state (field 3), ppid (field 4), and starttime (field 22) are read
/// from the tokens after the last `)`.
struct ProcStat {
    comm: String,
    state: char,
    ppid: u32,
    starttime_ticks: u64,
}

fn read_proc_stat(pid_dir: &Path) -> Option<ProcStat> {
    let content = fs::read_to_string(pid_dir.join("stat")).ok()?;
    let (before_comm, after_comm) = content.rsplit_once(')')?;
    let comm = before_comm.rsplit_once('(')?.1.to_string();
    let mut tokens = after_comm.split_whitespace();
    let state = tokens.next()?.chars().next()?;
    let ppid = tokens.next()?.parse().ok()?;
    let starttime_ticks = tokens.nth(17)?.parse().ok()?;
    Some(ProcStat {
        comm,
        state,
        ppid,
        starttime_ticks,
    })
}

struct ProcStatus {
    ppid: u32,
    uids: [u32; 4],
}

fn read_proc_status(pid_dir: &Path) -> Option<ProcStatus> {
    let content = fs::read_to_string(pid_dir.join("status")).ok()?;
    let mut ppid = None;
    let mut uids = None;
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("PPid:") {
            ppid = Some(rest.split_whitespace().next()?.parse().ok()?);
        } else if let Some(rest) = line.strip_prefix("Uid:") {
            let mut parts = rest.split_whitespace();
            uids = Some([
                parts.next()?.parse().ok()?,
                parts.next()?.parse().ok()?,
                parts.next()?.parse().ok()?,
                parts.next()?.parse().ok()?,
            ]);
        }
    }
    Some(ProcStatus {
        ppid: ppid?,
        uids: uids?,
    })
}

fn cgroup_is_system_slice(pid_dir: &Path) -> Option<bool> {
    let content = fs::read_to_string(pid_dir.join("cgroup")).ok()?;
    let path = content.lines().find_map(|line| line.strip_prefix("0::"))?;
    Some(path.starts_with(SYSTEM_SLICE_PREFIX))
}

/// True when an unreadable same-uid process P is an OpenSSH/Tailscale
/// session whose environment came from an init-started system service.
///
/// Fails closed on orphans, same-uid or sudo ancestors, pid reuse, a
/// missing/unreadable/unparsable `stat`/`status`/`cgroup`, cgroup v1,
/// walk cycles, and a depth above [`SYSTEM_SERVICE_WALK_LIMIT`].
fn is_system_service_session(
    pid: u32,
    stat: Option<&ProcStat>,
    proc_root: &Path,
    own_uid: u32,
) -> bool {
    let Some(stat) = stat else {
        return false;
    };
    if stat.ppid <= 1 {
        return false;
    }
    let mut seen = BTreeSet::from([pid]);
    let mut child_start = stat.starttime_ticks;
    let mut ancestor_pid = stat.ppid;
    for _ in 0..SYSTEM_SERVICE_WALK_LIMIT {
        if !seen.insert(ancestor_pid) {
            return false;
        }
        let ancestor_dir = proc_root.join(ancestor_pid.to_string());
        let Some(ancestor_stat) = read_proc_stat(&ancestor_dir) else {
            return false;
        };
        let Some(ancestor_status) = read_proc_status(&ancestor_dir) else {
            return false;
        };
        if ancestor_status.ppid != ancestor_stat.ppid {
            return false;
        }
        if ancestor_stat.starttime_ticks > child_start {
            return false;
        }
        if ancestor_status.uids.contains(&own_uid) {
            return false;
        }
        if ancestor_stat.ppid == 1 {
            return cgroup_is_system_slice(&ancestor_dir) == Some(true);
        }
        if ancestor_stat.ppid == 0 {
            return false;
        }
        child_start = ancestor_stat.starttime_ticks;
        ancestor_pid = ancestor_stat.ppid;
    }
    false
}

/// Start time of one pid as epoch seconds, from its parsed `stat`
/// starttime ticks plus `btime`. An unreadable or malformed `stat`
/// (including one for which `read_proc_stat` returned `None`) yields
/// `None`, falling through to today's behavior.
fn process_start_epoch(
    stat: Option<&ProcStat>,
    clock: &BootClock,
) -> Option<f64> {
    let stat = stat?;
    let btime = clock.btime_epoch?;
    let ticks_per_second = clock.ticks_per_second?;
    Some(btime + stat.starttime_ticks as f64 / ticks_per_second)
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

    fn write_process_stat(pid_dir: &Path, state: &str, starttime_ticks: u64) {
        write_process_stat_full(
            pid_dir,
            "fake-proc",
            state,
            1,
            starttime_ticks,
        );
    }

    fn write_process_stat_full(
        pid_dir: &Path,
        comm: &str,
        state: &str,
        ppid: u32,
        starttime_ticks: u64,
    ) {
        // `stat` layout: pid (comm) state ppid ... with starttime as
        // field 22 overall, the 20th whitespace token after the comm
        // close.
        let mut fields = vec![state.to_string(), ppid.to_string()];
        fields.extend(std::iter::repeat_n("0".to_string(), 17));
        fields.push(starttime_ticks.to_string());
        let content = format!("1 ({comm}) {}\n", fields.join(" "));
        let mut file = fs::File::create(pid_dir.join("stat")).unwrap();
        file.write_all(content.as_bytes()).unwrap();
    }

    fn write_process_status(pid_dir: &Path, ppid: u32, uids: [u32; 4]) {
        fs::write(
            pid_dir.join("status"),
            format!(
                "Name:\tproc\nPPid:\t{ppid}\nUid:\t{}\t{}\t{}\t{}\n",
                uids[0], uids[1], uids[2], uids[3]
            ),
        )
        .unwrap();
    }

    fn write_process_cgroup(pid_dir: &Path, path: &str) {
        fs::write(pid_dir.join("cgroup"), format!("0::{path}\n")).unwrap();
    }

    fn process_dir(proc_root: &Path, pid: u32) -> PathBuf {
        let pid_dir = proc_root.join(pid.to_string());
        fs::create_dir_all(&pid_dir).unwrap();
        pid_dir
    }

    fn unreadable_environ(pid_dir: &Path) {
        fs::create_dir(pid_dir.join("environ")).unwrap();
    }

    #[cfg(unix)]
    fn own_euid() -> u32 {
        unsafe { libc::geteuid() as u32 }
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
    fn normalize_path_canonicalizes_existing_prefix_before_missing_suffix() {
        let temp = tempdir().unwrap();
        let prefix = temp.path().join("existing");
        fs::create_dir(&prefix).unwrap();

        let normalized = normalize_path(&prefix.join("missing/nested"));

        assert_eq!(
            normalized,
            prefix.canonicalize().unwrap().join("missing/nested")
        );
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

    #[cfg(unix)]
    #[test]
    fn live_holder_matches_environ_path_through_symlink_with_missing_suffix() {
        let temp = tempdir().unwrap();
        let real_candidate = candidate_dir(temp.path(), "real");
        let symlinked_bucket = temp.path().join("linked");
        std::os::unix::fs::symlink(temp.path().join("real"), &symlinked_bucket)
            .unwrap();
        let candidate = symlinked_bucket.join(SCRATCH_KEY);
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

        // temp.path() is itself under /var on macOS, which
        // canonicalizes to /private/var. Compare canonical
        // identities so the inner symlink is the only variable.
        assert_eq!(
            real_candidate.canonicalize().unwrap(),
            candidate.canonicalize().unwrap()
        );
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
        write_process_stat(&pid_dir, "R", 5);
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
        write_process_stat(&pid_dir, "R", 200_000);
        let candidate = candidate_dir(temp.path(), "cargo-targets");

        let result = observe_launch_scratch_liveness(&request(
            &[(SCRATCH_KEY, &candidate)],
            &proc_root,
        ));

        assert!(!result.candidates[0].live);
        assert!(!result.candidates[0].complete);
        assert_eq!(result.exempted_pre_launch, 0);
        assert_eq!(result.exempted_system_service, 0);
        assert_eq!(result.unreadable, 1);
        assert!(
            result.diagnostics.iter().any(|line| {
                line.contains("9999 (fake-proc, ppid 1): environ unreadable:")
            }),
            "per-pid diagnostic must name comm and ppid: {}",
            result.diagnostics.join("; ")
        );
    }

    #[test]
    fn zombie_process_is_not_counted_as_unreadable() {
        let temp = tempdir().unwrap();
        let now = current_epoch();
        let proc_root = temp.path().join("proc");
        fs::create_dir_all(&proc_root).unwrap();
        write_proc_stat(&proc_root, now - 1000.0);
        let pid_dir = proc_root.join("9999");
        fs::create_dir_all(&pid_dir).unwrap();
        // environ as a directory reads as an error, same shape as
        // `later_unreadable_process_stays_incomplete`; the difference
        // is the zombie state, which must exempt the pid outright
        // even though its starttime is far in the future.
        fs::create_dir(pid_dir.join("environ")).unwrap();
        write_process_stat(&pid_dir, "Z", 200_000);
        let candidate = candidate_dir(temp.path(), "cargo-targets");

        let result = observe_launch_scratch_liveness(&request(
            &[(SCRATCH_KEY, &candidate)],
            &proc_root,
        ));

        assert!(!result.candidates[0].live);
        assert!(
            result.candidates[0].complete,
            "a zombie process must not mark the observation incomplete: {}",
            result.diagnostics.join("; ")
        );
        assert_eq!(result.unreadable, 0);
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

    #[test]
    fn exempted_system_service_defaults_when_absent() {
        let value = serde_json::json!({
            "schema_version": 1,
            "observer": "procfs",
            "candidates": [],
            "exempted_pre_launch": 0,
            "unreadable": 0,
            "diagnostics": [],
        });
        let parsed: LaunchScratchLivenessResultWire =
            serde_json::from_value(value).unwrap();
        assert_eq!(parsed.exempted_system_service, 0);
    }

    #[cfg(unix)]
    #[test]
    fn esrch_is_treated_as_missing() {
        let error = std::io::Error::from_raw_os_error(libc::ESRCH);
        assert!(is_missing(&error));
    }

    const LISTENER_PID: u32 = 80;
    const PRIV_PID: u32 = 81;
    const SESSION_PID: u32 = 4242;
    const POST_BIRTH_TICKS: u64 = 200_000;
    const PRIV_TICKS: u64 = 50;
    const LISTENER_TICKS: u64 = 10;

    #[cfg(unix)]
    fn root_uids() -> [u32; 4] {
        [0, 0, 0, 0]
    }

    #[cfg(unix)]
    fn own_uids() -> [u32; 4] {
        let uid = own_euid();
        [uid, uid, uid, uid]
    }

    /// P (same-uid, unreadable, post-birth) → root `[priv]` → root
    /// listener with ppid 1.
    #[cfg(unix)]
    fn install_sshd_like_chain(proc_root: &Path, listener_cgroup: &str) {
        let listener = process_dir(proc_root, LISTENER_PID);
        write_process_stat_full(&listener, "sshd", "S", 1, LISTENER_TICKS);
        write_process_status(&listener, 1, root_uids());
        write_process_cgroup(&listener, listener_cgroup);

        let priv_dir = process_dir(proc_root, PRIV_PID);
        write_process_stat_full(
            &priv_dir,
            "sshd",
            "S",
            LISTENER_PID,
            PRIV_TICKS,
        );
        write_process_status(&priv_dir, LISTENER_PID, root_uids());
        fs::write(priv_dir.join("environ"), "PATH=/usr/bin\0").unwrap();

        let session = process_dir(proc_root, SESSION_PID);
        write_process_stat_full(
            &session,
            "sshd-session",
            "S",
            PRIV_PID,
            POST_BIRTH_TICKS,
        );
        unreadable_environ(&session);
    }

    #[cfg(unix)]
    fn observe_post_birth_scratch(
        proc_root: &Path,
        candidate: &Path,
    ) -> LaunchScratchLivenessResultWire {
        observe_launch_scratch_liveness(&request(
            &[(SCRATCH_KEY, candidate)],
            proc_root,
        ))
    }

    #[cfg(unix)]
    fn setup_post_birth_proc() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let temp = tempdir().unwrap();
        let now = current_epoch();
        let proc_root = temp.path().join("proc");
        fs::create_dir_all(&proc_root).unwrap();
        write_proc_stat(&proc_root, now - 1000.0);
        let candidate = candidate_dir(temp.path(), "cargo-targets");
        (temp, proc_root, candidate)
    }

    #[cfg(unix)]
    #[test]
    fn sshd_like_chain_is_system_service_exempt() {
        if own_euid() == 0 {
            return;
        }
        let (_temp, proc_root, candidate) = setup_post_birth_proc();
        install_sshd_like_chain(&proc_root, "/system.slice/ssh.service");

        let result = observe_post_birth_scratch(&proc_root, &candidate);

        assert!(!result.candidates[0].live);
        assert!(
            result.candidates[0].complete,
            "sshd-like chain must be complete: {}",
            result.diagnostics.join("; ")
        );
        assert_eq!(result.exempted_system_service, 1);
        assert_eq!(result.exempted_pre_launch, 0);
        assert_eq!(result.unreadable, 0);
        assert!(result
            .diagnostics
            .iter()
            .any(|line| { line.contains("system-service exemption:") }));
    }

    #[cfg(unix)]
    #[test]
    fn sudo_parent_is_not_system_service_exempt() {
        if own_euid() == 0 {
            return;
        }
        let (_temp, proc_root, candidate) = setup_post_birth_proc();
        let parent = process_dir(&proc_root, PRIV_PID);
        write_process_stat_full(&parent, "sudo", "S", 1, PRIV_TICKS);
        write_process_status(&parent, 1, [own_euid(), 0, 0, 0]);
        write_process_cgroup(&parent, "/system.slice/ssh.service");
        let session = process_dir(&proc_root, SESSION_PID);
        write_process_stat_full(
            &session,
            "sshd-session",
            "S",
            PRIV_PID,
            POST_BIRTH_TICKS,
        );
        unreadable_environ(&session);

        let result = observe_post_birth_scratch(&proc_root, &candidate);

        assert!(!result.candidates[0].complete);
        assert_eq!(result.exempted_system_service, 0);
        assert_eq!(result.unreadable, 1);
    }

    #[cfg(unix)]
    #[test]
    fn same_uid_parent_is_not_system_service_exempt() {
        if own_euid() == 0 {
            return;
        }
        let (_temp, proc_root, candidate) = setup_post_birth_proc();
        let parent = process_dir(&proc_root, PRIV_PID);
        write_process_stat_full(&parent, "systemd", "S", 1, PRIV_TICKS);
        write_process_status(&parent, 1, own_uids());
        write_process_cgroup(&parent, "/system.slice/ssh.service");
        let session = process_dir(&proc_root, SESSION_PID);
        write_process_stat_full(
            &session,
            "agent",
            "S",
            PRIV_PID,
            POST_BIRTH_TICKS,
        );
        unreadable_environ(&session);

        let result = observe_post_birth_scratch(&proc_root, &candidate);

        assert!(!result.candidates[0].complete);
        assert_eq!(result.exempted_system_service, 0);
        assert_eq!(result.unreadable, 1);
    }

    #[cfg(unix)]
    #[test]
    fn orphan_ppid_one_is_not_system_service_exempt() {
        let (_temp, proc_root, candidate) = setup_post_birth_proc();
        let session = process_dir(&proc_root, SESSION_PID);
        write_process_stat_full(
            &session,
            "sshd-session",
            "S",
            1,
            POST_BIRTH_TICKS,
        );
        write_process_status(&session, 1, own_uids());
        write_process_cgroup(&session, "/system.slice/ssh.service");
        unreadable_environ(&session);

        let result = observe_post_birth_scratch(&proc_root, &candidate);

        assert!(!result.candidates[0].complete);
        assert_eq!(result.exempted_system_service, 0);
        assert_eq!(result.unreadable, 1);
    }

    #[cfg(unix)]
    #[test]
    fn user_slice_top_is_not_system_service_exempt() {
        if own_euid() == 0 {
            return;
        }
        let (_temp, proc_root, candidate) = setup_post_birth_proc();
        install_sshd_like_chain(
            &proc_root,
            "/user.slice/user-1000.slice/session-1.scope",
        );

        let result = observe_post_birth_scratch(&proc_root, &candidate);

        assert!(!result.candidates[0].complete);
        assert_eq!(result.exempted_system_service, 0);
        assert_eq!(result.unreadable, 1);
    }

    #[cfg(unix)]
    #[test]
    fn missing_ancestor_status_is_not_system_service_exempt() {
        if own_euid() == 0 {
            return;
        }
        let (_temp, proc_root, candidate) = setup_post_birth_proc();
        let listener = process_dir(&proc_root, LISTENER_PID);
        write_process_stat_full(&listener, "sshd", "S", 1, LISTENER_TICKS);
        write_process_cgroup(&listener, "/system.slice/ssh.service");
        let priv_dir = process_dir(&proc_root, PRIV_PID);
        write_process_stat_full(
            &priv_dir,
            "sshd",
            "S",
            LISTENER_PID,
            PRIV_TICKS,
        );
        write_process_status(&priv_dir, LISTENER_PID, root_uids());
        let session = process_dir(&proc_root, SESSION_PID);
        write_process_stat_full(
            &session,
            "sshd-session",
            "S",
            PRIV_PID,
            POST_BIRTH_TICKS,
        );
        unreadable_environ(&session);

        let result = observe_post_birth_scratch(&proc_root, &candidate);

        assert!(!result.candidates[0].complete);
        assert_eq!(result.exempted_system_service, 0);
        assert_eq!(result.unreadable, 1);
    }

    #[cfg(unix)]
    #[test]
    fn ancestor_started_after_child_is_not_system_service_exempt() {
        if own_euid() == 0 {
            return;
        }
        let (_temp, proc_root, candidate) = setup_post_birth_proc();
        let listener = process_dir(&proc_root, LISTENER_PID);
        write_process_stat_full(
            &listener,
            "sshd",
            "S",
            1,
            POST_BIRTH_TICKS + 1,
        );
        write_process_status(&listener, 1, root_uids());
        write_process_cgroup(&listener, "/system.slice/ssh.service");
        let priv_dir = process_dir(&proc_root, PRIV_PID);
        write_process_stat_full(
            &priv_dir,
            "sshd",
            "S",
            LISTENER_PID,
            POST_BIRTH_TICKS + 1,
        );
        write_process_status(&priv_dir, LISTENER_PID, root_uids());
        let session = process_dir(&proc_root, SESSION_PID);
        write_process_stat_full(
            &session,
            "sshd-session",
            "S",
            PRIV_PID,
            POST_BIRTH_TICKS,
        );
        unreadable_environ(&session);

        let result = observe_post_birth_scratch(&proc_root, &candidate);

        assert!(!result.candidates[0].complete);
        assert_eq!(result.exempted_system_service, 0);
        assert_eq!(result.unreadable, 1);
    }

    #[cfg(unix)]
    #[test]
    fn ancestry_cycle_is_not_system_service_exempt() {
        if own_euid() == 0 {
            return;
        }
        let (_temp, proc_root, candidate) = setup_post_birth_proc();
        let one = process_dir(&proc_root, PRIV_PID);
        write_process_stat_full(&one, "sshd", "S", LISTENER_PID, PRIV_TICKS);
        write_process_status(&one, LISTENER_PID, root_uids());
        let two = process_dir(&proc_root, LISTENER_PID);
        write_process_stat_full(&two, "sshd", "S", PRIV_PID, LISTENER_TICKS);
        write_process_status(&two, PRIV_PID, root_uids());
        write_process_cgroup(&two, "/system.slice/ssh.service");
        let session = process_dir(&proc_root, SESSION_PID);
        write_process_stat_full(
            &session,
            "sshd-session",
            "S",
            PRIV_PID,
            POST_BIRTH_TICKS,
        );
        unreadable_environ(&session);

        let result = observe_post_birth_scratch(&proc_root, &candidate);

        assert!(!result.candidates[0].complete);
        assert_eq!(result.exempted_system_service, 0);
        assert_eq!(result.unreadable, 1);
    }
}
