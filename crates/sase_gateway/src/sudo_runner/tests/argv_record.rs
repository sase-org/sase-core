use std::fs;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use super::support::*;

fn worker_argv_path(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("worker")
}

fn read_record(
    worker: &std::path::Path,
) -> std::io::Result<Option<Vec<String>>> {
    recorded_argv(worker)
}

#[test]
fn absent_record_is_pending() {
    let tmp = tempfile::tempdir().unwrap();
    let worker = worker_argv_path(tmp.path());
    assert!(!worker.with_extension("argv").exists());
    assert_eq!(read_record(&worker).unwrap(), None);
}

#[test]
fn empty_record_is_pending() {
    let tmp = tempfile::tempdir().unwrap();
    let worker = worker_argv_path(tmp.path());
    fs::write(worker.with_extension("argv"), "").unwrap();
    assert_eq!(read_record(&worker).unwrap(), None);
}

#[test]
fn begin_without_end_is_pending() {
    let tmp = tempfile::tempdir().unwrap();
    let worker = worker_argv_path(tmp.path());
    fs::write(
        worker.with_extension("argv"),
        "BEGIN\n--internal-root-worker\n",
    )
    .unwrap();
    assert_eq!(read_record(&worker).unwrap(), None);
}

#[test]
fn partial_argument_list_is_pending() {
    let tmp = tempfile::tempdir().unwrap();
    let worker = worker_argv_path(tmp.path());
    fs::write(
        worker.with_extension("argv"),
        "BEGIN\n--internal-root-worker\n-I\n-m\n",
    )
    .unwrap();
    assert_eq!(read_record(&worker).unwrap(), None);
}

#[test]
fn complete_record_returns_arguments_in_order() {
    let tmp = tempfile::tempdir().unwrap();
    let worker = worker_argv_path(tmp.path());
    fs::write(
        worker.with_extension("argv"),
        "BEGIN\n--internal-root-worker\n\n-I\n-m\nsase_core_rs.sudo_runner\nEND\n",
    )
    .unwrap();
    assert_eq!(
        read_record(&worker).unwrap(),
        Some(vec![
            "--internal-root-worker".to_string(),
            String::new(),
            "-I".to_string(),
            "-m".to_string(),
            "sase_core_rs.sudo_runner".to_string(),
        ])
    );
}

#[test]
fn staged_producer_becomes_complete_through_readiness_helper() {
    let tmp = tempfile::tempdir().unwrap();
    let worker = worker_argv_path(tmp.path());
    let argv_path = worker.with_extension("argv");
    let (staged_tx, staged_rx) = mpsc::channel::<()>();
    let (finish_tx, finish_rx) = mpsc::channel::<()>();
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let producer = thread::spawn({
        let argv_path = argv_path.clone();
        move || {
            // Hold the record incomplete the way the old shell stub did:
            // the path exists with a framed prefix but no END yet.
            fs::write(&argv_path, "BEGIN\n--internal-root-worker\n").unwrap();
            staged_tx.send(()).unwrap();
            finish_rx.recv().unwrap();
            // Publish the finished record atomically, mirroring the
            // fixed worker stub's write-then-rename.
            let staging = argv_path.with_extension("argv.tmp");
            fs::write(&staging, "BEGIN\n--internal-root-worker\n-I\nEND\n")
                .unwrap();
            fs::rename(&staging, &argv_path).unwrap();
            done_tx.send(()).unwrap();
        }
    });
    staged_rx.recv().unwrap();
    assert_eq!(
        recorded_argv(&worker).unwrap(),
        None,
        "reader observed a partial record as complete"
    );
    finish_tx.send(()).unwrap();
    let argv = wait_for_complete_argv(&worker, Duration::from_secs(10))
        .unwrap()
        .expect("staged producer never published a complete record");
    assert_eq!(
        argv,
        vec!["--internal-root-worker".to_string(), "-I".to_string(),]
    );
    done_rx.recv().unwrap();
    producer.join().unwrap();
}

#[test]
fn incomplete_record_never_resolves_before_deadline() {
    let tmp = tempfile::tempdir().unwrap();
    let worker = worker_argv_path(tmp.path());
    fs::write(
        worker.with_extension("argv"),
        "BEGIN\n--internal-root-worker\n-I\n",
    )
    .unwrap();
    assert_eq!(
        wait_for_complete_argv(&worker, Duration::from_millis(150)).unwrap(),
        None,
        "an incomplete record must not resolve as success"
    );
}
