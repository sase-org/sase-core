use super::super::*;
use super::support::*;
use serde_json::json;
use std::fs;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Barrier,
};
use std::thread;
use tempfile::tempdir;

#[test]
fn installation_identity_creation_read_rotation_and_migration_are_fenced() {
    let temp = tempdir().unwrap();
    let first =
        ensure_installation_identity_with_generator(temp.path(), 100.0, || {
            id('a')
        })
        .unwrap();
    assert!(first.created);
    assert_eq!(first.record.installation_id, id('a'));
    let second =
        ensure_installation_identity_with_generator(temp.path(), 101.0, || {
            id('b')
        })
        .unwrap();
    assert!(!second.created);
    assert_eq!(second.record, first.record);
    let load = load_installation_identity(temp.path()).unwrap();
    assert_eq!(load.record, Some(first.record.clone()));

    let bad_rotate = rotate_installation_identity_with_generator(
        temp.path(),
        &InstallationIdentityRotateRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            expected_installation_id: id('b'),
            reason: "test".to_string(),
            rotated_at_unix: Some(200.0),
        },
        || id('c'),
    );
    assert!(bad_rotate.is_err());
    let rotated = rotate_installation_identity_with_generator(
        temp.path(),
        &InstallationIdentityRotateRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            expected_installation_id: id('a'),
            reason: "operator requested".to_string(),
            rotated_at_unix: Some(200.0),
        },
        || id('b'),
    )
    .unwrap();
    assert_eq!(rotated.old_record.installation_id, id('a'));
    assert_eq!(rotated.new_record.installation_id, id('b'));
    assert_eq!(rotated.new_record.prior_installation_id, Some(id('a')));

    let migrated = migrate_installation_identity(
        temp.path(),
        &InstallationIdentityMigrateRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            expected_current_installation_id: Some(id('b')),
            adopted_installation_id: id('c'),
            reason: "clone recovery".to_string(),
            adopted_at_unix: Some(300.0),
        },
    )
    .unwrap();
    assert_eq!(migrated.prior_record.unwrap().installation_id, id('b'));
    assert_eq!(migrated.new_record.installation_id, id('c'));
}
#[test]
fn concurrent_identity_creators_converge_on_one_record() {
    let temp = tempdir().unwrap();
    let home = Arc::new(temp.path().to_path_buf());
    let barrier = Arc::new(Barrier::new(2));
    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..2 {
        let home = Arc::clone(&home);
        let barrier = Arc::clone(&barrier);
        let counter = Arc::clone(&counter);
        handles.push(thread::spawn(move || {
            barrier.wait();
            ensure_installation_identity_with_generator(&home, 100.0, || {
                let next = counter.fetch_add(1, Ordering::SeqCst);
                if next == 0 {
                    id('a')
                } else {
                    id('b')
                }
            })
            .unwrap()
        }));
    }
    let outcomes: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    assert_eq!(outcomes.iter().filter(|outcome| outcome.created).count(), 1);
    assert_eq!(
        outcomes[0].record.installation_id,
        outcomes[1].record.installation_id
    );
}
#[test]
fn malformed_oversized_and_future_identity_files_are_left_unchanged() {
    let temp = tempdir().unwrap();
    let path = installation_identity_path(temp.path());
    fs::write(&path, b"{not-json").unwrap();
    let before = fs::read(&path).unwrap();
    assert!(ensure_installation_identity_with_generator(
        temp.path(),
        100.0,
        || id('a')
    )
    .is_err());
    assert_eq!(fs::read(&path).unwrap(), before);

    fs::write(
        &path,
        serde_json::to_vec(&json!({
            "schema_version": FLEET_INSTALLATION_IDENTITY_SCHEMA_VERSION + 1,
            "installation_id": id('a'),
            "created_at_unix": 1.0,
            "generation": 1,
            "prior_installation_id": null,
            "rotated_at_unix": null,
            "adopted_at_unix": null,
            "reason": null
        }))
        .unwrap(),
    )
    .unwrap();
    let before = fs::read(&path).unwrap();
    assert!(load_installation_identity(temp.path()).is_err());
    assert_eq!(fs::read(&path).unwrap(), before);

    fs::write(&path, vec![b'x'; FLEET_INSTALLATION_IDENTITY_MAX_BYTES + 1])
        .unwrap();
    let before = fs::read(&path).unwrap();
    assert!(ensure_installation_identity_with_generator(
        temp.path(),
        100.0,
        || id('b')
    )
    .is_err());
    assert_eq!(fs::read(&path).unwrap(), before);
}

#[cfg(unix)]
#[test]
fn identity_store_uses_private_modes() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempdir().unwrap();
    ensure_installation_identity_with_generator(temp.path(), 100.0, || id('a'))
        .unwrap();
    let home_mode =
        fs::metadata(temp.path()).unwrap().permissions().mode() & 0o777;
    let file_mode = fs::metadata(installation_identity_path(temp.path()))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(home_mode, 0o700);
    assert_eq!(file_mode, 0o600);
}
