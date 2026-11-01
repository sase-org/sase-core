//! Deterministic regression tests for the fork-inherited
//! write-descriptor `ETXTBSY` race behind sase-15h.
//!
//! A test binary's threads share their descriptors across forks: when one
//! thread's `Command::spawn` forks while another thread still holds a
//! freshly written fixture script open for writing, the forked child
//! inherits the write descriptor until its own exec, and the kernel
//! refuses to exec the script with `ETXTBSY` ("Text file busy", os error
//! 26) until that child execs or exits. These tests replay that shape
//! deterministically: a `sleep` child plays the forked inheritor while
//! the test execs the script it holds open for writing.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use std::thread;
use std::time::Duration;

use super::super::*;
use super::support::*;

#[test]
fn exec_while_fork_child_holds_write_descriptor_fails_with_etxtbsy() {
    let tmp = tempfile::tempdir().unwrap();
    let script = tmp.path().join("held-open.sh");
    publish_executable(&script, "#!/bin/sh\nprintf 'replayed\\n'\n");
    // Hold the script open for writing, exactly as a forked child holds
    // the descriptor it inherited from a write still in flight.
    let held = fs::File::options().write(true).open(&script).unwrap();
    // A forked child that has not exec'd yet: it carries the inherited
    // write descriptor for its whole pre-exec life.
    let mut inheritor = Command::new("/bin/sleep").arg("30").spawn().unwrap();
    let error = Command::new(&script).status().unwrap_err();
    assert!(
        is_etxtbsy(&error),
        "expected ETXTBSY while a fork child holds the script open for \
         writing, got: {error}"
    );
    let _ = inheritor.kill();
    let _ = inheritor.wait();
    drop(held);
    // A foreign test thread may have forked while `held` was open and
    // its child may still be pre-exec; the backstop absorbs that tail.
    let output = retry_on_etxtbsy(|| Command::new(&script).output()).unwrap();
    assert!(output.status.success());
    assert_eq!(output.stdout, b"replayed\n");
}

#[test]
fn etxtbsy_retry_recovers_once_writer_closes() {
    let tmp = tempfile::tempdir().unwrap();
    let script = tmp.path().join("closing.sh");
    publish_executable(&script, "#!/bin/sh\nprintf 'recovered\\n'\n");
    let held = fs::File::options().write(true).open(&script).unwrap();
    // Drop the write descriptor after 100ms from a helper thread. Every
    // retry attempt before that must fail; the helper budget below is an
    // order of magnitude larger, so recovery is deterministic.
    let closer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        drop(held);
    });
    thread::sleep(Duration::from_millis(10));
    let mut attempts = 0;
    let output = retry_on_etxtbsy(|| {
        attempts += 1;
        Command::new(&script).output()
    })
    .unwrap();
    closer.join().unwrap();
    assert!(
        attempts >= 2,
        "expected at least one ETXTBSY retry, got {attempts} attempt(s)"
    );
    assert!(output.status.success());
    assert_eq!(output.stdout, b"recovered\n");
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
    let output = retry_on_etxtbsy(|| Command::new(&script).output()).unwrap();
    assert!(output.status.success());
}
