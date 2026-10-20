#![cfg(unix)]

use super::*;
use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

const NOW: f64 = 1_800_000_000.0;
const HOUR: f64 = 3600.0;
const DAY: f64 = 24.0 * HOUR;

fn request(root: &Path) -> ManagedTmpReapRequestWire {
    ManagedTmpReapRequestWire {
        schema_version: MANAGED_TMP_REAP_WIRE_SCHEMA_VERSION,
        root: root.to_string_lossy().into_owned(),
        apply: true,
        now_epoch_seconds: NOW,
        age_reap: true,
        horizons: BTreeMap::from([
            ("agent-tmp".to_string(), 12.0 * HOUR),
            ("cargo-targets".to_string(), 3.0 * DAY),
        ]),
        default_horizon_seconds: 3.0 * DAY,
        max_removals: 2000,
        pressure_reap: false,
        pressure_max_bytes: None,
        pressure_target_bytes: 8 * 1024,
        pressure_min_available_bytes: None,
        pressure_recovery_available_bytes: 48 * 1024,
        pressure_min_age_seconds: 12.0 * HOUR,
        pressure_low_free_space_min_age_seconds: None,
        pressure_min_entry_bytes: 1024,
        pressure_reap_buckets: vec!["cargo-targets".to_string()],
        filesystem_available_bytes: None,
        launch_scratch: None,
        dead_launch: None,
    }
}

fn set_mtime(path: &Path, timestamp: f64) {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let seconds = timestamp.trunc() as libc::time_t;
    let nanos = ((timestamp.fract()) * 1_000_000_000.0) as libc::c_long;
    let times = [
        libc::timespec {
            tv_sec: seconds,
            tv_nsec: nanos,
        },
        libc::timespec {
            tv_sec: seconds,
            tv_nsec: nanos,
        },
    ];
    let c_path = CString::new(path.as_os_str().as_bytes()).unwrap();
    let rc = unsafe {
        libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0)
    };
    assert_eq!(rc, 0, "utimensat({}) failed", path.display());
}

fn restore_owner_write(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let mut stack = vec![path.to_path_buf()];
    while let Some(entry) = stack.pop() {
        let Ok(metadata) = fs::symlink_metadata(&entry) else {
            continue;
        };
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            let _ =
                fs::set_permissions(&entry, fs::Permissions::from_mode(0o755));
            if let Ok(children) = fs::read_dir(&entry) {
                for child in children.flatten() {
                    stack.push(child.path());
                }
            }
        } else {
            let _ =
                fs::set_permissions(&entry, fs::Permissions::from_mode(0o644));
        }
    }
}

fn write_aged_file(path: &Path, bytes: usize) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    let mut file = File::create(path).unwrap();
    file.write_all(&vec![b'x'; bytes]).unwrap();
    set_mtime(path, NOW - 4.0 * DAY);
}

fn lock_dir_tree(dir: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let mut files = Vec::new();
    let mut dirs = vec![dir.to_path_buf()];
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(entries) = fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let child = entry.path();
            let Ok(metadata) = fs::symlink_metadata(&child) else {
                continue;
            };
            if metadata.file_type().is_symlink() {
                continue;
            }
            if metadata.is_dir() {
                dirs.push(child.clone());
                stack.push(child);
            } else {
                files.push(child);
            }
        }
    }
    for file in files {
        fs::set_permissions(&file, fs::Permissions::from_mode(0o444)).unwrap();
    }
    for path in dirs.into_iter().rev() {
        fs::set_permissions(&path, fs::Permissions::from_mode(0o555)).unwrap();
    }
}

#[test]
fn aged_agent_tmp_with_0555_vault_is_removed() {
    let temp = tempdir().unwrap();
    let key = temp.path().join("agent-tmp/stale-key");
    let vault = key.join("x/vault");
    let secret = vault.join("secret.txt");
    write_aged_file(&secret, 64);
    set_mtime(&vault, NOW - 4.0 * DAY);
    set_mtime(&key.join("x"), NOW - 4.0 * DAY);
    set_mtime(&key, NOW - 4.0 * DAY);
    lock_dir_tree(&vault);

    let result = reap_managed_tmpdir(&request(temp.path())).unwrap();

    if key.exists() {
        restore_owner_write(&key);
    }
    assert!(
        !key.exists(),
        "0555 vault candidate should have been removed; errors={:?}",
        result.removal_errors
    );
    assert_eq!(result.removed, 1);
    assert_eq!(result.removed_bytes, 64);
    assert_eq!(result.failed, 0);
}

#[test]
fn symlink_to_outside_0555_dir_is_not_followed() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempdir().unwrap();
    let root = temp.path().join("root");
    fs::create_dir(&root).unwrap();
    // Sibling of the reaper root so age-reap cannot select it.
    let outside = temp.path().join("outside/vault");
    let outside_file = outside.join("keep.txt");
    write_aged_file(&outside_file, 32);
    lock_dir_tree(&outside);

    let key = root.join("agent-tmp/stale-key");
    fs::create_dir_all(&key).unwrap();
    let payload = key.join("payload.bin");
    write_aged_file(&payload, 16);
    std::os::unix::fs::symlink(&outside, key.join("link")).unwrap();
    set_mtime(&key, NOW - 4.0 * DAY);

    let result = reap_managed_tmpdir(&request(&root)).unwrap();

    if key.exists() {
        restore_owner_write(&key);
    }
    assert!(
        !key.exists(),
        "candidate with symlink should still be removed; errors={:?}",
        result.removal_errors
    );
    let mode =
        fs::symlink_metadata(&outside).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o555, "outside directory mode must stay 0555");
    assert!(outside_file.exists());
    restore_owner_write(&outside);
}

#[test]
fn dead_launch_candidate_with_nested_0555_is_removed() {
    let temp = tempdir().unwrap();
    let proc_root = temp.path().join("proc");
    fs::create_dir_all(&proc_root).unwrap();
    let key = temp.path().join("agent-tmp/dead-key");
    let vault = key.join("x/vault");
    let secret = vault.join("secret.txt");
    write_aged_file(&secret, 48);
    set_mtime(&vault, NOW - 3.0 * HOUR);
    set_mtime(&key.join("x"), NOW - 3.0 * HOUR);
    set_mtime(&key, NOW - 3.0 * HOUR);
    lock_dir_tree(&vault);

    let mut req = request(temp.path());
    req.age_reap = false;
    req.dead_launch = Some(ManagedTmpDeadLaunchRequestWire {
        enabled: true,
        grace_seconds: 2.0 * HOUR,
        proc_root: proc_root.to_string_lossy().into_owned(),
        current_pid: None,
        exempt_pids: Vec::new(),
    });

    let result = reap_managed_tmpdir(&req).unwrap();

    if key.exists() {
        restore_owner_write(&key);
    }
    assert!(
        !key.exists(),
        "dead-launch 0555 candidate should have been removed; errors={:?}",
        result.removal_errors
    );
    assert_eq!(result.dead_launch_removed, 1);
    assert_eq!(result.dead_launch_reclaimed_bytes, 48);
    assert_eq!(result.failed, 0);
}
