//! Deterministic regression tests for the fork-inherited
//! write-descriptor ETXTBSY race behind sase-15h.
//!
//! A test binary's threads share their descriptors across forks: when one
//! thread's Command::spawn forks while another thread still holds a
//! freshly written fixture script open for writing, the forked child
//! inherits the write descriptor until its own exec, and on Linux the
//! kernel refuses to exec the script with ETXTBSY ("Text file busy", os
//! error 26) until that child execs or exits. macOS/XNU has no such rule:
//! exec-ing a file that is open for writing simply succeeds there. The
//! real-kernel replay tests below are therefore Linux-only; the portable
//! tests further below cover the retry policy on every unix platform by
//! injecting a synthetic ETXTBSY from the spawn closure instead of relying
//! on kernel behaviour.

use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

use super::super::*;
use super::support::*;

#[cfg(target_os = "linux")]
#[test]
fn exec_while_fork_child_holds_write_descriptor_fails_with_etxtbsy() {
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;

    let tmp = tempfile::tempdir().unwrap();
    let script = tmp.path().join("held-open.sh");
    publish_executable(&script, "#!/bin/sh\nprintf 'replayed\\n'\n");

    let ready = tmp.path().join("inheritor-ready");
    let release = tmp.path().join("inheritor-release");
    let held = fs::File::options().write(true).open(&script).unwrap();
    let fd = held.as_raw_fd();

    let mut inheritor_command = Command::new("/bin/sh");
    inheritor_command
        .arg("-c")
        .arg(
            "test -e \"/dev/fd/$1\" || exit 88; : > \"$2\"; \
             i=0; while [ ! -e \"$3\" ] && [ \"$i\" -lt 50000000 ]; \
             do i=$((i + 1)); done",
        )
        .arg("sase-etxtbsy-inheritor")
        .arg(fd.to_string())
        .arg(&ready)
        .arg(&release);
    // Make only this child inherit the writer. The parent's descriptor stays
    // close-on-exec, so unrelated tests cannot prolong the held-open window.
    unsafe {
        inheritor_command.pre_exec(move || {
            let flags = libc::fcntl(fd, libc::F_GETFD);
            if flags < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut inheritor = retry_on_etxtbsy(|| inheritor_command.spawn()).unwrap();
    let ready_deadline =
        std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !ready.exists() && std::time::Instant::now() < ready_deadline {
        std::thread::yield_now();
    }
    assert!(ready.exists(), "child did not inherit the writer fd");

    drop(held);
    let error = Command::new(&script).status().unwrap_err();
    assert!(
        is_etxtbsy(&error),
        "expected ETXTBSY while a fork child holds the script open for \
         writing, got: {error}"
    );
    fs::write(&release, "release").unwrap();
    assert!(inheritor.wait().unwrap().success());

    let output = output_with_etxtbsy_retry(Command::new(&script)).unwrap();
    assert!(output.status.success());
    assert_eq!(output.stdout, b"replayed\n");
}

#[cfg(target_os = "linux")]
#[test]
fn etxtbsy_retry_recovers_once_writer_closes() {
    let tmp = tempfile::tempdir().unwrap();
    let script = tmp.path().join("closing.sh");
    publish_executable(&script, "#!/bin/sh\nprintf 'recovered\\n'\n");
    let held = fs::File::options().write(true).open(&script).unwrap();
    let mut attempts = 0;
    let mut held = Some(held);
    let mut command = Command::new(&script);
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let child = retry_on_etxtbsy(|| {
        attempts += 1;
        if attempts > 1 {
            drop(held.take());
        }
        command.spawn()
    })
    .unwrap();
    assert!(
        attempts >= 2,
        "expected at least one ETXTBSY retry, got {attempts} attempt(s)"
    );
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    assert_eq!(output.stdout, b"recovered\n");
}

#[test]
fn retry_on_etxtbsy_retries_synthetic_etxtbsy_then_returns_the_spawn() {
    assert!(is_etxtbsy(&io::Error::from_raw_os_error(libc::ETXTBSY)));
    let mut attempts = 0;
    let mut child = retry_on_etxtbsy(|| {
        attempts += 1;
        if attempts <= 2 {
            return Err(io::Error::from_raw_os_error(libc::ETXTBSY));
        }
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("exit 0")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        command.spawn()
    })
    .unwrap();
    assert_eq!(attempts, 3, "expected exactly 3 spawn attempts");
    assert!(child.wait().unwrap().success());
}

#[test]
fn retry_on_etxtbsy_returns_non_etxtbsy_errors_immediately() {
    let mut attempts = 0;
    let error = retry_on_etxtbsy(|| {
        attempts += 1;
        Err::<std::process::Child, _>(io::Error::from_raw_os_error(
            libc::ENOENT,
        ))
    })
    .unwrap_err();
    assert_eq!(attempts, 1, "expected exactly 1 spawn attempt");
    assert_eq!(error.raw_os_error(), Some(libc::ENOENT));
}

#[test]
fn publish_executable_lands_atomically_with_exec_permissions() {
    let tmp = tempfile::tempdir().unwrap();
    let script = tmp.path().join("published.sh");
    publish_executable(&script, "#!/bin/sh\nprintf published\n");
    assert_eq!(
        fs::read_to_string(&script).unwrap(),
        "#!/bin/sh\nprintf published\n"
    );
    assert_eq!(
        script.metadata().unwrap().permissions().mode() & 0o777,
        0o755
    );
    let leftover: Vec<_> = fs::read_dir(tmp.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(leftover, vec!["published.sh".to_string()]);
    let output = output_with_etxtbsy_retry(Command::new(&script)).unwrap();
    assert!(output.status.success());
}
