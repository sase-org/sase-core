use std::{
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind, Write},
    path::{Path, PathBuf},
};

use chrono::{SecondsFormat, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::wire::GatewayBuildWire;

pub const DAEMON_LOCK_SCHEMA_VERSION: u32 = 1;
const DAEMON_LOCK_FILE_NAME: &str = "daemon.lock";
const DAEMON_LOCK_METADATA_FILE_NAME: &str = "daemon.lock.json";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonOwnershipMetadata {
    pub schema_version: u32,
    pub pid: u32,
    pub hostname: String,
    pub boot_session_hint: Option<String>,
    pub executable_path: Option<PathBuf>,
    pub socket_path: PathBuf,
    pub started_at: String,
    pub sase_home: PathBuf,
    pub build_version: String,
}

impl DaemonOwnershipMetadata {
    pub fn current(
        hostname: impl Into<String>,
        socket_path: PathBuf,
        sase_home: PathBuf,
        build: &GatewayBuildWire,
    ) -> Self {
        Self {
            schema_version: DAEMON_LOCK_SCHEMA_VERSION,
            pid: std::process::id(),
            hostname: hostname.into(),
            boot_session_hint: boot_session_hint(),
            executable_path: std::env::current_exe().ok(),
            socket_path,
            started_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            sase_home,
            build_version: build.package_version.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DaemonOwnershipPaths {
    pub run_root: PathBuf,
    pub lock_path: PathBuf,
    pub metadata_path: PathBuf,
    pub socket_path: PathBuf,
}

impl DaemonOwnershipPaths {
    pub fn new(run_root: PathBuf, socket_path: PathBuf) -> Self {
        let lock_path = run_root.join(DAEMON_LOCK_FILE_NAME);
        let metadata_path = run_root.join(DAEMON_LOCK_METADATA_FILE_NAME);
        Self {
            run_root,
            lock_path,
            metadata_path,
            socket_path,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnershipRecovery {
    Clean,
    RecoveredStale { stale: StaleOwnership },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StaleOwnership {
    DeadPid {
        metadata: DaemonOwnershipMetadata,
    },
    MalformedMetadata {
        metadata_path: PathBuf,
        message: String,
    },
    MissingMetadata,
}

#[derive(Debug)]
pub struct DaemonOwnershipGuard {
    lock_file: File,
    pub paths: DaemonOwnershipPaths,
    pub metadata: DaemonOwnershipMetadata,
    pub recovery: OwnershipRecovery,
}

impl DaemonOwnershipGuard {
    pub fn acquire(
        run_root: impl Into<PathBuf>,
        socket_path: impl Into<PathBuf>,
        hostname: impl Into<String>,
        sase_home: impl Into<PathBuf>,
        build: &GatewayBuildWire,
    ) -> Result<Self, OwnershipError> {
        let paths =
            DaemonOwnershipPaths::new(run_root.into(), socket_path.into());
        fs::create_dir_all(&paths.run_root).map_err(|source| {
            OwnershipError::CreateRunRoot {
                path: paths.run_root.clone(),
                source,
            }
        })?;

        let lock_file_existed = paths.lock_path.exists();
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&paths.lock_path)
            .map_err(|source| OwnershipError::OpenLock {
                path: paths.lock_path.clone(),
                source,
            })?;

        if let Err(source) = lock_file.try_lock_exclusive() {
            if source.kind() == ErrorKind::WouldBlock {
                return Err(OwnershipError::AlreadyLocked {
                    lock_path: paths.lock_path.clone(),
                    metadata: read_metadata_lossy(&paths.metadata_path)
                        .map(Box::new),
                });
            }
            return Err(OwnershipError::Lock {
                path: paths.lock_path.clone(),
                source,
            });
        }

        let hostname = hostname.into();
        let sase_home = sase_home.into();
        let recovery = diagnose_existing_metadata(
            &paths.metadata_path,
            lock_file_existed,
            &hostname,
        )?;
        let metadata = DaemonOwnershipMetadata::current(
            hostname,
            paths.socket_path.clone(),
            sase_home,
            build,
        );
        write_metadata(&paths.metadata_path, &metadata)?;

        Ok(Self {
            lock_file,
            paths,
            metadata,
            recovery,
        })
    }
}

impl Drop for DaemonOwnershipGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.paths.socket_path);
        let _ = fs::remove_file(&self.paths.metadata_path);
        let _ = FileExt::unlock(&self.lock_file);
        let _ = fs::remove_file(&self.paths.lock_path);
    }
}

#[derive(Debug, Error)]
pub enum OwnershipError {
    #[error("failed to create daemon run directory {path}: {source}")]
    CreateRunRoot { path: PathBuf, source: io::Error },
    #[error("failed to open daemon ownership lock {path}: {source}")]
    OpenLock { path: PathBuf, source: io::Error },
    #[error("failed to acquire daemon ownership lock {path}: {source}")]
    Lock { path: PathBuf, source: io::Error },
    #[error(
        "another local SASE daemon owns {lock_path}; stop that daemon first or inspect daemon.lock.json for recovery details"
    )]
    AlreadyLocked {
        lock_path: PathBuf,
        metadata: Option<Box<DaemonOwnershipMetadata>>,
    },
    #[error(
        "daemon ownership metadata at {metadata_path} belongs to host {metadata_host:?}, not this host {current_host:?}; refusing to overwrite synced or conflicting state"
    )]
    HostnameConflict {
        metadata_path: PathBuf,
        metadata_host: String,
        current_host: String,
    },
    #[error(
        "daemon ownership metadata at {metadata_path} has unsupported schema {schema_version}; refusing to overwrite it"
    )]
    UnsupportedSchema {
        metadata_path: PathBuf,
        schema_version: u32,
    },
    #[error(
        "daemon ownership metadata at {metadata_path} points at live pid {pid}; refusing to overwrite it"
    )]
    LiveMetadataPid { metadata_path: PathBuf, pid: u32 },
    #[error("failed to read daemon ownership metadata {path}: {source}")]
    ReadMetadata { path: PathBuf, source: io::Error },
    #[error("failed to parse daemon ownership metadata {path}: {source}")]
    ParseMetadata {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("failed to write daemon ownership metadata {path}: {source}")]
    WriteMetadata { path: PathBuf, source: io::Error },
    #[error("failed to serialize daemon ownership metadata: {0}")]
    SerializeMetadata(#[from] serde_json::Error),
}

fn diagnose_existing_metadata(
    metadata_path: &Path,
    lock_file_existed: bool,
    current_host: &str,
) -> Result<OwnershipRecovery, OwnershipError> {
    if !metadata_path.exists() {
        if lock_file_existed {
            return Ok(OwnershipRecovery::RecoveredStale {
                stale: StaleOwnership::MissingMetadata,
            });
        }
        return Ok(OwnershipRecovery::Clean);
    }

    let raw = fs::read(metadata_path).map_err(|source| {
        OwnershipError::ReadMetadata {
            path: metadata_path.to_path_buf(),
            source,
        }
    })?;
    let metadata: DaemonOwnershipMetadata = match serde_json::from_slice(&raw) {
        Ok(metadata) => metadata,
        Err(error) => {
            return Ok(OwnershipRecovery::RecoveredStale {
                stale: StaleOwnership::MalformedMetadata {
                    metadata_path: metadata_path.to_path_buf(),
                    message: error.to_string(),
                },
            });
        }
    };

    if metadata.hostname != current_host {
        return Err(OwnershipError::HostnameConflict {
            metadata_path: metadata_path.to_path_buf(),
            metadata_host: metadata.hostname,
            current_host: current_host.to_string(),
        });
    }
    if metadata.schema_version != DAEMON_LOCK_SCHEMA_VERSION {
        return Err(OwnershipError::UnsupportedSchema {
            metadata_path: metadata_path.to_path_buf(),
            schema_version: metadata.schema_version,
        });
    }
    if process_is_live(metadata.pid) {
        return Err(OwnershipError::LiveMetadataPid {
            metadata_path: metadata_path.to_path_buf(),
            pid: metadata.pid,
        });
    }

    Ok(OwnershipRecovery::RecoveredStale {
        stale: StaleOwnership::DeadPid { metadata },
    })
}

fn read_metadata_lossy(path: &Path) -> Option<DaemonOwnershipMetadata> {
    let bytes = fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn write_metadata(
    path: &Path,
    metadata: &DaemonOwnershipMetadata,
) -> Result<(), OwnershipError> {
    let mut payload = serde_json::to_vec_pretty(metadata)?;
    payload.push(b'\n');
    let tmp_path = path.with_extension("json.tmp");
    {
        let mut file = File::create(&tmp_path).map_err(|source| {
            OwnershipError::WriteMetadata {
                path: tmp_path.clone(),
                source,
            }
        })?;
        file.write_all(&payload).map_err(|source| {
            OwnershipError::WriteMetadata {
                path: tmp_path.clone(),
                source,
            }
        })?;
        file.sync_all()
            .map_err(|source| OwnershipError::WriteMetadata {
                path: tmp_path.clone(),
                source,
            })?;
    }
    fs::rename(&tmp_path, path).map_err(|source| {
        OwnershipError::WriteMetadata {
            path: path.to_path_buf(),
            source,
        }
    })?;
    Ok(())
}

fn boot_session_hint() -> Option<String> {
    fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn process_is_live(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    if pid == std::process::id() {
        return true;
    }
    PathBuf::from("/proc").join(pid.to_string()).exists()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn build() -> GatewayBuildWire {
        GatewayBuildWire {
            package_version: "test-version".to_string(),
            git_sha: None,
        }
    }

    fn stale_metadata(
        hostname: &str,
        pid: u32,
        socket_path: PathBuf,
        sase_home: PathBuf,
    ) -> DaemonOwnershipMetadata {
        DaemonOwnershipMetadata {
            schema_version: DAEMON_LOCK_SCHEMA_VERSION,
            pid,
            hostname: hostname.to_string(),
            boot_session_hint: Some("boot".to_string()),
            executable_path: Some(PathBuf::from("/tmp/sase_gateway")),
            socket_path,
            started_at: "2026-05-13T00:00:00Z".to_string(),
            sase_home,
            build_version: "old".to_string(),
        }
    }

    fn impossible_pid() -> u32 {
        4_294_967_000
    }

    #[test]
    fn two_guards_cannot_own_same_run_root() {
        let tmp = tempdir().unwrap();
        let run_root = tmp.path().join("run");
        let socket_path = run_root.join("sase-daemon.sock");
        let first = DaemonOwnershipGuard::acquire(
            run_root.clone(),
            socket_path.clone(),
            "host",
            tmp.path().join("home"),
            &build(),
        )
        .unwrap();

        let second = DaemonOwnershipGuard::acquire(
            run_root.clone(),
            socket_path,
            "host",
            tmp.path().join("home"),
            &build(),
        );

        assert!(matches!(second, Err(OwnershipError::AlreadyLocked { .. })));
        drop(first);
    }

    #[test]
    fn stale_pid_metadata_is_recovered_through_typed_path() {
        let tmp = tempdir().unwrap();
        let run_root = tmp.path().join("run");
        let socket_path = run_root.join("sase-daemon.sock");
        fs::create_dir_all(&run_root).unwrap();
        let metadata_path = run_root.join(DAEMON_LOCK_METADATA_FILE_NAME);
        let stale = stale_metadata(
            "host",
            impossible_pid(),
            socket_path.clone(),
            tmp.path().join("home"),
        );
        fs::write(&metadata_path, serde_json::to_vec_pretty(&stale).unwrap())
            .unwrap();

        let guard = DaemonOwnershipGuard::acquire(
            run_root,
            socket_path,
            "host",
            tmp.path().join("home"),
            &build(),
        )
        .unwrap();

        assert_eq!(
            guard.recovery,
            OwnershipRecovery::RecoveredStale {
                stale: StaleOwnership::DeadPid { metadata: stale }
            }
        );
        assert_eq!(guard.metadata.pid, std::process::id());
    }

    #[test]
    fn malformed_metadata_is_recovered_through_typed_path() {
        let tmp = tempdir().unwrap();
        let run_root = tmp.path().join("run");
        let socket_path = run_root.join("sase-daemon.sock");
        fs::create_dir_all(&run_root).unwrap();
        fs::write(run_root.join(DAEMON_LOCK_METADATA_FILE_NAME), b"{not-json")
            .unwrap();

        let guard = DaemonOwnershipGuard::acquire(
            run_root,
            socket_path,
            "host",
            tmp.path().join("home"),
            &build(),
        )
        .unwrap();

        assert!(matches!(
            guard.recovery,
            OwnershipRecovery::RecoveredStale {
                stale: StaleOwnership::MalformedMetadata { .. }
            }
        ));
    }

    #[test]
    fn missing_metadata_with_existing_lock_is_recovered_through_typed_path() {
        let tmp = tempdir().unwrap();
        let run_root = tmp.path().join("run");
        let socket_path = run_root.join("sase-daemon.sock");
        fs::create_dir_all(&run_root).unwrap();
        fs::write(run_root.join(DAEMON_LOCK_FILE_NAME), b"").unwrap();

        let guard = DaemonOwnershipGuard::acquire(
            run_root,
            socket_path,
            "host",
            tmp.path().join("home"),
            &build(),
        )
        .unwrap();

        assert_eq!(
            guard.recovery,
            OwnershipRecovery::RecoveredStale {
                stale: StaleOwnership::MissingMetadata
            }
        );
    }

    #[test]
    fn hostname_mismatch_is_conflict_not_overwritten() {
        let tmp = tempdir().unwrap();
        let run_root = tmp.path().join("run");
        let socket_path = run_root.join("sase-daemon.sock");
        fs::create_dir_all(&run_root).unwrap();
        let metadata_path = run_root.join(DAEMON_LOCK_METADATA_FILE_NAME);
        let stale = stale_metadata(
            "other-host",
            impossible_pid(),
            socket_path.clone(),
            tmp.path().join("home"),
        );
        let original = serde_json::to_vec_pretty(&stale).unwrap();
        fs::write(&metadata_path, &original).unwrap();

        let err = DaemonOwnershipGuard::acquire(
            run_root,
            socket_path,
            "host",
            tmp.path().join("home"),
            &build(),
        )
        .unwrap_err();

        assert!(matches!(err, OwnershipError::HostnameConflict { .. }));
        assert_eq!(fs::read(metadata_path).unwrap(), original);
    }

    #[test]
    fn live_metadata_pid_is_conflict_not_overwritten() {
        let tmp = tempdir().unwrap();
        let run_root = tmp.path().join("run");
        let socket_path = run_root.join("sase-daemon.sock");
        fs::create_dir_all(&run_root).unwrap();
        let metadata_path = run_root.join(DAEMON_LOCK_METADATA_FILE_NAME);
        let metadata = stale_metadata(
            "host",
            std::process::id(),
            socket_path.clone(),
            tmp.path().join("home"),
        );
        fs::write(
            &metadata_path,
            serde_json::to_vec_pretty(&metadata).unwrap(),
        )
        .unwrap();

        let err = DaemonOwnershipGuard::acquire(
            run_root,
            socket_path,
            "host",
            tmp.path().join("home"),
            &build(),
        )
        .unwrap_err();

        assert!(matches!(err, OwnershipError::LiveMetadataPid { .. }));
    }

    #[test]
    fn clean_shutdown_removes_lock_metadata_and_socket_path() {
        let tmp = tempdir().unwrap();
        let run_root = tmp.path().join("run");
        let socket_path = run_root.join("sase-daemon.sock");
        let guard = DaemonOwnershipGuard::acquire(
            run_root.clone(),
            socket_path.clone(),
            "host",
            tmp.path().join("home"),
            &build(),
        )
        .unwrap();
        fs::write(&socket_path, b"stale socket").unwrap();
        assert!(guard.paths.lock_path.exists());
        assert!(guard.paths.metadata_path.exists());

        let lock_path = guard.paths.lock_path.clone();
        let metadata_path = guard.paths.metadata_path.clone();
        drop(guard);

        assert!(!socket_path.exists());
        assert!(!metadata_path.exists());
        assert!(!lock_path.exists());
    }
}
