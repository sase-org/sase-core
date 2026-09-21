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

pub(super) struct PreparedListener {
    listener: UnixListener,
    socket_path: PathBuf,
    _lock_file: File,
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
        _lock_file: lock_file,
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
