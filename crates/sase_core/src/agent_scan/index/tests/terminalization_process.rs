use super::super::terminalization::{
    HostProcessIdentityProbe, ProcessIdentityObservation, ProcessIdentityProbe,
};
use std::fs;
use tempfile::tempdir;

fn write_boot_id(proc_root: &std::path::Path, boot_id: &str) {
    let path = proc_root.join("sys/kernel/random/boot_id");
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, boot_id).unwrap();
}

fn write_proc(
    proc_root: &std::path::Path,
    pid: i64,
    state: char,
    start_ticks: u64,
) {
    let pid_dir = proc_root.join(pid.to_string());
    fs::create_dir_all(&pid_dir).unwrap();
    fs::write(
        pid_dir.join("status"),
        format!("Name:\tfoo\nState:\t{state} (running)\n"),
    )
    .unwrap();
    let mut fields = vec![state.to_string()];
    fields.extend(std::iter::repeat_n("0".to_string(), 18));
    fields.push(start_ticks.to_string());
    fs::write(
        pid_dir.join("stat"),
        format!("{pid} (foo) {}", fields.join(" ")),
    )
    .unwrap();
}

#[test]
fn host_probe_never_signals_host_for_custom_proc_root() {
    let tmp = tempdir().unwrap();
    let probe = HostProcessIdentityProbe::at(tmp.path().to_path_buf());
    assert_eq!(
        probe.observe(999_999, None),
        ProcessIdentityObservation::Unknown
    );
}

#[test]
fn host_probe_classifies_missing_zombie_live_reused_and_previous_boot() {
    let tmp = tempdir().unwrap();
    let proc_root = tmp.path();
    write_boot_id(proc_root, "boot-a");
    write_proc(proc_root, 10, 'R', 100);
    write_proc(proc_root, 11, 'Z', 200);
    write_proc(proc_root, 12, 'R', 300);

    let probe = HostProcessIdentityProbe::at(proc_root.to_path_buf());
    assert_eq!(
        probe.observe(9, Some("boot-a:1")),
        ProcessIdentityObservation::Missing
    );
    assert_eq!(
        probe.observe(11, Some("boot-a:200")),
        ProcessIdentityObservation::Zombie
    );
    assert_eq!(
        probe.observe(10, Some("boot-a:100")),
        ProcessIdentityObservation::MatchingLive
    );
    assert_eq!(
        probe.observe(10, Some("boot-a:1")),
        ProcessIdentityObservation::ReusedPid
    );
    assert_eq!(
        probe.observe(12, Some("boot-old:300")),
        ProcessIdentityObservation::PreviousBoot
    );
    assert_eq!(probe.observe(10, None), ProcessIdentityObservation::Unknown);
}
