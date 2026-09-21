use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;
use std::process::{Child, Command};
use std::sync::atomic::Ordering;
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use sase_core::{
    truncate_sudo_output_tail, validate_sudo_ledger, SudoLedgerWire,
    SudoManifestWire, SudoOutputPolicyWire,
};

use super::*;

pub(crate) fn runner_launcher(
    config: &SudoRunnerConfig,
) -> Result<SudoRunnerLauncher, SudoRunnerCliError> {
    match &config.launcher {
        Some(launcher) => Ok(launcher.clone()),
        None => SudoRunnerLauncher::native(),
    }
}

pub(crate) fn path_string(path: &Path) -> Result<String, SudoRunnerCliError> {
    path.to_str().map(str::to_string).ok_or_else(|| {
        cli_error(
            SudoRunnerExitStatus::InvalidInput,
            format!("path is not valid UTF-8: {}", path.display()),
        )
    })
}

pub(crate) fn current_unix_time() -> Result<f64, SudoRunnerCliError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!("system clock is before Unix epoch: {error}"),
            )
        })
}

#[cfg(target_os = "linux")]
pub(crate) fn process_identity_token(
    pid: u32,
) -> Result<String, SudoRunnerCliError> {
    let boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!("failed to read Linux boot id: {error}"),
            )
        })?;
    let boot_id = boot_id.trim();
    if boot_id.is_empty() {
        return Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            "Linux boot id is empty".to_string(),
        ));
    }
    let stat =
        fs::read_to_string(format!("/proc/{pid}/stat")).map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!("failed to read executor process stat: {error}"),
            )
        })?;
    let close = stat.rfind(')').ok_or_else(|| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            "executor process stat is malformed".to_string(),
        )
    })?;
    let fields = stat[close + 1..].split_whitespace().collect::<Vec<_>>();
    let start_ticks = fields.get(19).ok_or_else(|| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            "executor process stat is missing start ticks".to_string(),
        )
    })?;
    start_ticks.parse::<u64>().map_err(|_| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            "executor process start ticks are malformed".to_string(),
        )
    })?;
    Ok(format!("{boot_id}:{start_ticks}"))
}

#[cfg(all(unix, not(target_os = "linux")))]
pub(crate) fn process_identity_token(
    _pid: u32,
) -> Result<String, SudoRunnerCliError> {
    Err(cli_error(
        SudoRunnerExitStatus::RunnerError,
        "process identity tokens require Linux procfs; detached sudo execution is unavailable on this platform".to_string(),
    ))
}

#[cfg(unix)]
pub(crate) fn read_small_file_nofollow(
    path: &Path,
) -> Result<Vec<u8>, SudoRunnerCliError> {
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!("failed to open {}: {error}", path.display()),
            )
        })?;
    let metadata = file.metadata().map_err(|error| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("failed to stat {}: {error}", path.display()),
        )
    })?;
    if metadata.len() > 16 * 1024 {
        return Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("{} exceeds 16384 bytes", path.display()),
        ));
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(|error| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("failed to read {}: {error}", path.display()),
        )
    })?;
    Ok(bytes)
}

#[cfg(unix)]
pub(crate) struct ResolvedAccount {
    pub(crate) uid: libc::uid_t,
    pub(crate) gid: libc::gid_t,
    pub(crate) name: std::ffi::CString,
}

#[cfg(unix)]
#[cfg(any(test, target_vendor = "apple"))]
pub(crate) fn gid_to_c_int_for_initgroups(
    gid: libc::gid_t,
) -> io::Result<libc::c_int> {
    libc::c_int::try_from(gid).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "sudo run_as primary group id {gid} does not fit initgroups base group type"
            ),
        )
    })
}

#[cfg(all(unix, target_vendor = "apple"))]
pub(crate) fn initgroups_base_group(
    gid: libc::gid_t,
) -> io::Result<libc::c_int> {
    gid_to_c_int_for_initgroups(gid)
}

#[cfg(all(unix, not(target_vendor = "apple")))]
pub(crate) fn initgroups_base_group(
    gid: libc::gid_t,
) -> io::Result<libc::gid_t> {
    Ok(gid)
}

#[cfg(unix)]
pub(crate) fn configure_process_group_and_account(
    command: &mut Command,
    account: &ResolvedAccount,
) {
    use std::os::unix::process::CommandExt;

    let uid = account.uid;
    let gid = account.gid;
    let name = account.name.clone();
    unsafe {
        command.pre_exec(move || {
            if libc::setpgid(0, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::geteuid() == 0 {
                let initgroups_gid = initgroups_base_group(gid)?;
                if libc::initgroups(name.as_ptr(), initgroups_gid) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::setgid(gid) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::setuid(uid) != 0 {
                    return Err(io::Error::last_os_error());
                }
            } else if libc::geteuid() != uid {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "sudo root worker is not privileged enough to switch user",
                ));
            }
            Ok(())
        });
    }
}

#[cfg(unix)]
pub(crate) fn resolve_account(run_as: &str) -> Result<ResolvedAccount, String> {
    let name = std::ffi::CString::new(run_as)
        .map_err(|_| "sudo run_as contains an interior NUL".to_string())?;
    lookup_account_by_name(&name)
        .or_else(|| {
            run_as
                .parse::<libc::uid_t>()
                .ok()
                .and_then(lookup_account_by_uid)
        })
        .ok_or_else(|| format!("sudo run_as account {run_as:?} does not exist"))
}

#[cfg(unix)]
pub(crate) fn lookup_account_by_name(
    name: &std::ffi::CString,
) -> Option<ResolvedAccount> {
    let mut pwd = std::mem::MaybeUninit::<libc::passwd>::uninit();
    let mut result = std::ptr::null_mut();
    let mut buffer = vec![0u8; passwd_buffer_size()];
    let code = unsafe {
        libc::getpwnam_r(
            name.as_ptr(),
            pwd.as_mut_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        )
    };
    if code != 0 || result.is_null() {
        return None;
    }
    let pwd = unsafe { pwd.assume_init() };
    account_from_passwd(&pwd)
}

#[cfg(unix)]
pub(crate) fn lookup_account_by_uid(
    uid: libc::uid_t,
) -> Option<ResolvedAccount> {
    let mut pwd = std::mem::MaybeUninit::<libc::passwd>::uninit();
    let mut result = std::ptr::null_mut();
    let mut buffer = vec![0u8; passwd_buffer_size()];
    let code = unsafe {
        libc::getpwuid_r(
            uid,
            pwd.as_mut_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        )
    };
    if code != 0 || result.is_null() {
        return None;
    }
    let pwd = unsafe { pwd.assume_init() };
    account_from_passwd(&pwd)
}

#[cfg(unix)]
pub(crate) fn account_from_passwd(
    pwd: &libc::passwd,
) -> Option<ResolvedAccount> {
    let name = unsafe { std::ffi::CStr::from_ptr(pwd.pw_name) }.to_owned();
    Some(ResolvedAccount {
        uid: pwd.pw_uid,
        gid: pwd.pw_gid,
        name,
    })
}

#[cfg(unix)]
pub(crate) fn passwd_buffer_size() -> usize {
    let value = unsafe { libc::sysconf(libc::_SC_GETPW_R_SIZE_MAX) };
    if value > 0 {
        value as usize
    } else {
        16 * 1024
    }
}

#[cfg(unix)]
pub(crate) fn reject_existing_path(
    path: &Path,
    label: &str,
) -> Result<(), SudoRunnerCliError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("refusing symlink sudo {label} path {}", path.display()),
        )),
        Ok(_) => Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!(
                "refusing to replace existing sudo {label} path {}",
                path.display()
            ),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!(
                "failed to inspect sudo {label} path {}: {error}",
                path.display()
            ),
        )),
    }
}

#[cfg(unix)]
pub(crate) fn reject_symlink_path(
    path: &Path,
    label: &str,
) -> Result<(), SudoRunnerCliError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("refusing symlink sudo {label} path {}", path.display()),
        )),
        Ok(_) | Err(_) => Ok(()),
    }
}

#[cfg(unix)]
pub(crate) fn open_log_file(
    path: &Path,
    uid: u32,
    gid: u32,
) -> Result<File, SudoRunnerCliError> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    reject_existing_path(path, "log")?;
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW)
        .mode(0o600)
        .open(path)
        .map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!(
                    "failed to create sudo log {}: {error}",
                    path.display()
                ),
            )
        })?;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!("failed to chmod sudo log {}: {error}", path.display()),
            )
        })?;
    chown_file_for_user(&file, uid, gid, path)?;
    Ok(file)
}

#[cfg(unix)]
pub(crate) fn write_ledger_file(
    path: &Path,
    ledger: &SudoLedgerWire,
    manifest: &SudoManifestWire,
    uid: u32,
    gid: u32,
) -> Result<(), SudoRunnerCliError> {
    validate_sudo_ledger(ledger, Some(manifest)).map_err(|error| {
        cli_error(SudoRunnerExitStatus::RunnerError, error.message)
    })?;
    write_atomic_json_for_user(path, ledger, uid, gid)
}

#[cfg(unix)]
pub(crate) fn write_atomic_json_for_user<T: serde::Serialize>(
    path: &Path,
    value: &T,
    uid: u32,
    gid: u32,
) -> Result<(), SudoRunnerCliError> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    reject_existing_path(path, "output")?;
    let parent = path.parent().ok_or_else(|| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("sudo output path has no parent: {}", path.display()),
        )
    })?;
    let filename =
        path.file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                cli_error(
                    SudoRunnerExitStatus::RunnerError,
                    format!(
                        "sudo output path is not UTF-8: {}",
                        path.display()
                    ),
                )
            })?;
    let tmp_path =
        parent.join(format!(".{filename}.tmp.{}", std::process::id()));
    let _ = fs::remove_file(&tmp_path);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW)
        .mode(0o600)
        .open(&tmp_path)
        .map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!(
                    "failed to create sudo output {}: {error}",
                    tmp_path.display()
                ),
            )
        })?;
    serde_json::to_writer(&mut file, value).map_err(|error| {
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!("failed to serialize sudo output JSON: {error}"),
        )
    })?;
    writeln!(file).map_err(internal_io)?;
    file.flush().map_err(internal_io)?;
    file.sync_all().map_err(internal_io)?;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|error| {
            cli_error(
                SudoRunnerExitStatus::RunnerError,
                format!(
                    "failed to chmod sudo output {}: {error}",
                    tmp_path.display()
                ),
            )
        })?;
    chown_file_for_user(&file, uid, gid, &tmp_path)?;
    reject_existing_path(path, "output")?;
    fs::rename(&tmp_path, path).map_err(|error| {
        let _ = fs::remove_file(&tmp_path);
        cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!(
                "failed to publish sudo output {}: {error}",
                path.display()
            ),
        )
    })
}

#[cfg(unix)]
pub(crate) fn chown_file_for_user(
    file: &File,
    uid: u32,
    gid: u32,
    path: &Path,
) -> Result<(), SudoRunnerCliError> {
    use std::os::unix::io::AsRawFd;

    if unsafe { libc::geteuid() } != 0 {
        return Ok(());
    }
    if unsafe { libc::fchown(file.as_raw_fd(), uid, gid) } == 0 {
        Ok(())
    } else {
        Err(cli_error(
            SudoRunnerExitStatus::RunnerError,
            format!(
                "failed to chown sudo output {}: {}",
                path.display(),
                io::Error::last_os_error()
            ),
        ))
    }
}

#[cfg(unix)]
pub(crate) fn terminate_child_group(child: &mut Child) {
    let pgid = child.id() as libc::pid_t;
    let _ = unsafe { libc::kill(-pgid, libc::SIGTERM) };
    let grace = Instant::now() + KILL_GRACE;
    while Instant::now() < grace {
        if child.try_wait().ok().flatten().is_some() {
            return;
        }
        thread::sleep(POLL_INTERVAL);
    }
    let _ = unsafe { libc::kill(-pgid, libc::SIGKILL) };
    let _ = child.wait();
}

pub(crate) fn ledger_output(
    combined: &[u8],
    policy: SudoOutputPolicyWire,
) -> String {
    let bound = ledger_tail_bound(policy);
    if bound == 0 {
        return String::new();
    }
    let lossy = String::from_utf8_lossy(combined);
    truncate_sudo_output_tail(&lossy, bound)
}

pub(crate) fn tty_available(config: &SudoRunnerConfig) -> bool {
    if let Some(value) = config.tty_available {
        return value;
    }
    platform_tty_available()
}

#[cfg(unix)]
pub(crate) fn platform_tty_available() -> bool {
    (unsafe { libc::isatty(libc::STDIN_FILENO) == 1 })
        || fs::File::open("/dev/tty").is_ok()
}

#[cfg(not(unix))]
pub(crate) fn platform_tty_available() -> bool {
    false
}

#[cfg(unix)]
pub(crate) fn install_cancellation_handlers() {
    SIGNALS_INSTALLED.call_once(|| unsafe {
        libc::signal(
            libc::SIGINT,
            handle_signal as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGTERM,
            handle_signal as *const () as libc::sighandler_t,
        );
    });
}

#[cfg(unix)]
extern "C" fn handle_signal(_signal: libc::c_int) {
    CANCELLED.store(true, Ordering::SeqCst);
}

#[cfg(unix)]
pub(crate) fn harden_process() -> Result<(), String> {
    let rlimit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::setrlimit(libc::RLIMIT_CORE, &rlimit) } != 0 {
        return Err(format!(
            "failed to disable core dumps: {}",
            io::Error::last_os_error()
        ));
    }
    #[cfg(target_os = "linux")]
    {
        if unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) } != 0 {
            return Err(format!(
                "failed to disable process dumpability: {}",
                io::Error::last_os_error()
            ));
        }
    }
    Ok(())
}

pub(crate) fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();
    for index in 0..max_len {
        let a = left.get(index).copied().unwrap_or(0);
        let b = right.get(index).copied().unwrap_or(0);
        diff |= (a ^ b) as usize;
    }
    diff == 0
}

pub(crate) fn internal_io(error: io::Error) -> SudoRunnerCliError {
    cli_error(
        SudoRunnerExitStatus::RunnerError,
        format!("sudo runner I/O failed: {error}"),
    )
}

pub(crate) fn cli_error(
    status: SudoRunnerExitStatus,
    message: String,
) -> SudoRunnerCliError {
    SudoRunnerCliError { status, message }
}
