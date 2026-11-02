//! A waited sudo child must not retain the fixture publication/spawn lock.

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use super::super::*;
use super::support::publish_executable;

struct ReleaseChildOnDrop(PathBuf);

impl Drop for ReleaseChildOnDrop {
    fn drop(&mut self) {
        let _ = fs::write(&self.0, "release");
    }
}

#[test]
fn status_wait_releases_fixture_spawn_lock_for_publication_and_spawn() {
    let tmp = tempfile::tempdir().unwrap();
    let sudo_fixture = tmp.path().join("fake-sudo");
    let ready = tmp.path().join("sudo-ready");
    let release = tmp.path().join("sudo-release");
    publish_executable(
        &sudo_fixture,
        "#!/bin/sh\n: > \"$1\"\ni=0\nwhile [ ! -e \"$2\" ] && [ \"$i\" -lt 50000000 ]; do i=$((i + 1)); done\nexit 0\n",
    );

    let mut status_command = Command::new(&sudo_fixture);
    status_command.arg(&ready).arg(&release);
    let status_thread = thread::spawn(move || {
        status_with_etxtbsy_retry(status_command).unwrap()
    });

    let ready_deadline = Instant::now() + Duration::from_secs(2);
    while !ready.exists() && Instant::now() < ready_deadline {
        thread::yield_now();
    }
    let child_reached_handshake = ready.exists();

    let next_fixture = tmp.path().join("published-while-sudo-waits");
    let (complete_tx, complete_rx) = mpsc::channel();
    let publication_thread = thread::spawn(move || {
        publish_executable(&next_fixture, "#!/bin/sh\nexit 0\n");
        let mut command = Command::new(&next_fixture);
        match retry_on_etxtbsy(|| command.spawn()) {
            Ok(mut child) => {
                complete_tx.send(Ok(())).unwrap();
                child
                    .wait()
                    .map(|status| status.success())
                    .map_err(|error| error.to_string())
            }
            Err(error) => {
                let message = error.to_string();
                let _ = complete_tx.send(Err(message.clone()));
                Err(message)
            }
        }
    });

    let publication_and_spawn = {
        let _release = ReleaseChildOnDrop(release);
        complete_rx.recv_timeout(Duration::from_secs(2))
    };
    let sudo_status = status_thread.join().unwrap();
    let published_status = publication_thread.join().unwrap();

    assert!(
        child_reached_handshake,
        "sudo fixture did not reach its handshake"
    );
    assert!(sudo_status.success());
    assert!(
        matches!(publication_and_spawn, Ok(Ok(()))),
        "fixture publication and spawn did not finish while sudo waited: {publication_and_spawn:?}"
    );
    assert_eq!(published_status, Ok(true));
}
