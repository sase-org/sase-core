use super::*;
use std::os::unix::{
    fs::{FileTypeExt, PermissionsExt},
    io::AsRawFd,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::net::{UnixListener, UnixStream};
use tokio::time;

pub async fn run(
    config: FederationWorkerConfig,
) -> Result<(), FederationWorkerError> {
    let prepared = prepare_listener(&config).await?;
    let state = Arc::new(FederationWorkerState::new(config.clone()));
    let active_connections = Arc::new(AtomicUsize::new(0));
    loop {
        let accept =
            time::timeout(config.idle_timeout, prepared.listener.accept());
        tokio::select! {
            _ = state.shutdown.notified() => break,
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(FederationWorkerError::Signal)?;
                break;
            }
            accepted = accept => {
                match accepted {
                    Ok(Ok((mut stream, _addr))) => {
                        if !peer_is_same_user(&stream)
                            .map_err(FederationWorkerError::PeerCredentials)?
                        {
                            let response = error_response(
                                "<peer>",
                                federation_error(
                                    "permission_denied",
                                    "IPC peer is not owned by the current user",
                                    Some("peer"),
                                ),
                            );
                            let _ = write_response(
                                &mut stream,
                                &response,
                                config.max_frame_bytes,
                            )
                            .await;
                            continue;
                        }
                        let Ok(permit) = state.connection_permits.clone().try_acquire_owned() else {
                            let response = error_response(
                                "<connection>",
                                federation_error(
                                    "rate_limited",
                                    "too many active IPC connections",
                                    Some("connection"),
                                ),
                            );
                            let _ = write_response(
                                &mut stream,
                                &response,
                                config.max_frame_bytes,
                            )
                            .await;
                            continue;
                        };
                        active_connections.fetch_add(1, Ordering::SeqCst);
                        tokio::spawn(handle_connection(
                            stream,
                            state.clone(),
                            config.max_frame_bytes,
                            active_connections.clone(),
                            permit,
                        ));
                    }
                    Ok(Err(error)) => return Err(FederationWorkerError::Accept(error)),
                    Err(_) => {
                        if active_connections.load(Ordering::SeqCst) == 0 {
                            break;
                        }
                    }
                }
            }
        }
    }
    drop(prepared);
    Ok(())
}

struct FederationLock {
    file: File,
}

impl Drop for FederationLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

pub(super) struct PreparedListener {
    listener: UnixListener,
    socket_path: PathBuf,
    _lock: FederationLock,
}

impl Drop for PreparedListener {
    fn drop(&mut self) {
        if let Ok(metadata) = fs::symlink_metadata(&self.socket_path) {
            if metadata.file_type().is_socket() {
                let _ = fs::remove_file(&self.socket_path);
            }
        }
    }
}

pub(super) async fn prepare_listener(
    config: &FederationWorkerConfig,
) -> Result<PreparedListener, FederationWorkerError> {
    secure_dir(&config.run_root)?;
    let lock_path = config.run_root.join(FEDERATION_LOCK_NAME);
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|source| FederationWorkerError::RuntimeDir {
            path: lock_path.clone(),
            source,
        })?;
    lock_file.try_lock_exclusive().map_err(|source| {
        if source.kind() == io::ErrorKind::WouldBlock {
            FederationWorkerError::AlreadyRunning {
                socket: config.socket_path.clone(),
            }
        } else {
            FederationWorkerError::RuntimeDir {
                path: lock_path.clone(),
                source,
            }
        }
    })?;
    let lock = FederationLock { file: lock_file };

    recover_socket_target(&config.socket_path).await?;
    let listener =
        UnixListener::bind(&config.socket_path).map_err(|source| {
            FederationWorkerError::Bind {
                path: config.socket_path.clone(),
                source,
            }
        })?;
    fs::set_permissions(&config.socket_path, fs::Permissions::from_mode(0o600))
        .map_err(|source| FederationWorkerError::SocketPermissions {
            path: config.socket_path.clone(),
            source,
        })?;
    Ok(PreparedListener {
        listener,
        socket_path: config.socket_path.clone(),
        _lock: lock,
    })
}

pub(super) fn secure_dir(path: &Path) -> Result<(), FederationWorkerError> {
    fs::create_dir_all(path).map_err(|source| {
        FederationWorkerError::RuntimeDir {
            path: path.to_path_buf(),
            source,
        }
    })?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(
        |source| FederationWorkerError::RuntimeDir {
            path: path.to_path_buf(),
            source,
        },
    )
}

pub(super) async fn recover_socket_target(
    path: &Path,
) -> Result<(), FederationWorkerError> {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return Ok(());
    };
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        return Err(FederationWorkerError::UnsafeSocket {
            path: path.to_path_buf(),
            reason: "target is a symlink".to_string(),
        });
    }
    if !file_type.is_socket() {
        return Err(FederationWorkerError::UnsafeSocket {
            path: path.to_path_buf(),
            reason: "target is not a socket".to_string(),
        });
    }
    match UnixStream::connect(path).await {
        Ok(_) => Err(FederationWorkerError::AlreadyRunning {
            socket: path.to_path_buf(),
        }),
        Err(_) => fs::remove_file(path).map_err(|source| {
            FederationWorkerError::RuntimeDir {
                path: path.to_path_buf(),
                source,
            }
        }),
    }
}

pub(super) async fn handle_connection(
    mut stream: UnixStream,
    state: Arc<FederationWorkerState>,
    max_frame_bytes: usize,
    active_connections: Arc<AtomicUsize>,
    _permit: tokio::sync::OwnedSemaphorePermit,
) {
    loop {
        let request = match read_request(&mut stream, max_frame_bytes).await {
            Ok(Some(request)) => request,
            Ok(None) => break,
            Err(error) => {
                let response = error_response("<frame>", error);
                let _ = write_response(&mut stream, &response, max_frame_bytes)
                    .await;
                break;
            }
        };
        let response = handle_request(state.clone(), request).await;
        let shutdown = matches!(
            response
                .result
                .as_ref()
                .and_then(|value| value.get("shutdown")),
            Some(JsonValue::Bool(true))
        );
        if write_response(&mut stream, &response, max_frame_bytes)
            .await
            .is_err()
        {
            break;
        }
        if shutdown {
            state.shutdown.notify_waiters();
            break;
        }
    }
    active_connections.fetch_sub(1, Ordering::SeqCst);
}

#[cfg(target_os = "linux")]
pub(super) fn peer_is_same_user(stream: &UnixStream) -> io::Result<bool> {
    let fd = stream.as_raw_fd();
    let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            &mut cred as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(cred.uid == unsafe { libc::geteuid() })
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd"
))]
pub(super) fn peer_is_same_user(stream: &UnixStream) -> io::Result<bool> {
    let fd = stream.as_raw_fd();
    let mut uid: libc::uid_t = 0;
    let mut gid: libc::gid_t = 0;
    let rc = unsafe { libc::getpeereid(fd, &mut uid, &mut gid) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(uid == unsafe { libc::geteuid() })
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd"
)))]
pub(super) fn peer_is_same_user(_stream: &UnixStream) -> io::Result<bool> {
    Ok(true)
}

#[cfg(test)]
mod listener_lock_tests {
    use super::*;

    fn test_config(tmp: &tempfile::TempDir) -> FederationWorkerConfig {
        let mut config = FederationWorkerConfig::new(tmp.path());
        config.run_root = tmp.path().join("run");
        config.socket_path = config.run_root.join("worker.sock");
        config
    }

    #[tokio::test]
    async fn duplicate_descriptor_does_not_block_symlink_rejection() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(&tmp);
        let prepared = prepare_listener(&config).await.unwrap();
        let _duplicate = prepared._lock.file.try_clone().unwrap();
        drop(prepared);
        std::os::unix::fs::symlink("/tmp/target", &config.socket_path).unwrap();
        match prepare_listener(&config).await {
            Err(FederationWorkerError::UnsafeSocket { .. }) => {}
            Err(error) => {
                panic!("expected UnsafeSocket, got: {error}")
            }
            Ok(_) => {
                panic!("symlink socket path should be rejected")
            }
        }
    }

    #[tokio::test]
    async fn restart_succeeds_while_duplicate_descriptor_survives() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(&tmp);
        let prepared = prepare_listener(&config).await.unwrap();
        let _duplicate = prepared._lock.file.try_clone().unwrap();
        drop(prepared);
        let restarted = prepare_listener(&config).await.unwrap();
        let dir_mode =
            fs::metadata(&config.run_root).unwrap().permissions().mode()
                & 0o777;
        assert_eq!(dir_mode, 0o700);
        drop(restarted);
    }

    #[tokio::test]
    async fn concurrent_prepare_is_rejected_while_listener_alive() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(&tmp);
        let _owner = prepare_listener(&config).await.unwrap();
        match prepare_listener(&config).await {
            Err(FederationWorkerError::AlreadyRunning { .. }) => {}
            Err(error) => {
                panic!("expected AlreadyRunning, got: {error}")
            }
            Ok(_) => panic!("second listener should be rejected"),
        }
    }

    #[tokio::test]
    async fn failed_contender_does_not_release_owner_lock() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(&tmp);
        let owner = prepare_listener(&config).await.unwrap();
        match prepare_listener(&config).await {
            Err(FederationWorkerError::AlreadyRunning { .. }) => {}
            Err(error) => {
                panic!("expected AlreadyRunning, got: {error}")
            }
            Ok(_) => panic!("contender should be rejected"),
        }
        // Owner must still be protected after the failed contender drops.
        match prepare_listener(&config).await {
            Err(FederationWorkerError::AlreadyRunning { .. }) => {}
            Err(error) => {
                panic!("owner lock released by contender: {error}")
            }
            Ok(_) => panic!("owner lock released by contender"),
        }
        drop(owner);
        // After the owner disposes, a fresh listener starts immediately.
        let _restarted = prepare_listener(&config).await.unwrap();
    }

    #[tokio::test]
    async fn failed_prepare_releases_lock_for_next_attempt() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(&tmp);
        let prepared = prepare_listener(&config).await.unwrap();
        let _duplicate = prepared._lock.file.try_clone().unwrap();
        drop(prepared);
        std::os::unix::fs::symlink("/tmp/target", &config.socket_path).unwrap();
        match prepare_listener(&config).await {
            Err(FederationWorkerError::UnsafeSocket { .. }) => {}
            Err(error) => {
                panic!("expected UnsafeSocket, got: {error}")
            }
            Ok(_) => panic!("symlink should be rejected"),
        }
        fs::remove_file(&config.socket_path).unwrap();
        // The failed symlink attempt must not leave the lock held, even
        // with the first generation's duplicate descriptor still open.
        let _restarted = prepare_listener(&config).await.unwrap();
    }

    #[test]
    fn lock_guard_releases_on_drop_while_duplicate_survives() {
        use fs2::FileExt;
        let tmp = tempfile::tempdir().unwrap();
        let lock_path = tmp.path().join("test.lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        file.try_lock_exclusive().unwrap();
        let guard = FederationLock { file };
        let _duplicate = guard.file.try_clone().unwrap();
        drop(guard);
        let fresh = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
            .unwrap();
        fresh.try_lock_exclusive().unwrap();
        let _ = fs2::FileExt::unlock(&fresh);
    }
}
