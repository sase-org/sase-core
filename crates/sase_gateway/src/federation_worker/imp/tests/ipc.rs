use super::super::listener::prepare_listener;
use super::super::*;
use super::support::*;
use serde_json::json;
use std::os::unix::fs::PermissionsExt;
use tokio::{io::AsyncWriteExt, net::UnixStream};

#[tokio::test]
async fn framed_ipc_rejects_oversized_request() {
    let (mut client, mut server) = UnixStream::pair().unwrap();
    let server = tokio::spawn(async move {
        let err = read_request(&mut server, 8).await.unwrap_err();
        assert_eq!(err.code, "payload_too_large");
    });
    client.write_all(&16_u32.to_be_bytes()).await.unwrap();
    client.write_all(b"0123456789abcdef").await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn listener_creates_private_socket_and_rejects_symlink() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = FederationWorkerConfig::new(tmp.path());
    config.run_root = tmp.path().join("run");
    config.socket_path = config.run_root.join("worker.sock");
    let prepared = prepare_listener(&config).await.unwrap();
    let dir_mode =
        fs::metadata(&config.run_root).unwrap().permissions().mode() & 0o777;
    let socket_mode = fs::metadata(&config.socket_path)
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(dir_mode, 0o700);
    assert_eq!(socket_mode, 0o600);
    drop(prepared);

    #[cfg(target_family = "unix")]
    std::os::unix::fs::symlink("/tmp/target", &config.socket_path).unwrap();
    match prepare_listener(&config).await {
        Err(FederationWorkerError::UnsafeSocket { .. }) => {}
        Err(error) => panic!("unexpected error: {error}"),
        Ok(_) => panic!("symlink socket path should be rejected"),
    }
}

#[tokio::test]
async fn worker_answers_health_over_local_ipc() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = FederationWorkerConfig::new(tmp.path());
    config.run_root = tmp.path().join("run");
    config.socket_path = config.run_root.join("worker.sock");
    config.idle_timeout = Duration::from_secs(30);
    let socket_path = config.socket_path.clone();

    let worker = tokio::spawn(run(config));
    let health =
        request_worker(&socket_path, "health-1", json!({"op": "health"})).await;
    assert_eq!(health["service"], json!(FEDERATION_WORKER_SERVICE));
    assert_eq!(health["status"], json!("ok"));
    assert_eq!(health["configured_hosts"], json!(0));

    let shutdown =
        request_worker(&socket_path, "shutdown-1", json!({"op": "shutdown"}))
            .await;
    assert_eq!(shutdown["shutdown"], json!(true));
    worker.await.unwrap().unwrap();
}
