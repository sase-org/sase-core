use super::super::*;
use sase_core::fleet_contract::{
    fleet_catalog_snapshot_id, FleetCatalogScopeWire,
};
use serde_json::json;
use std::{path::Path, process::Command, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    time,
};

pub(super) async fn request_worker(
    socket_path: &Path,
    request_id: &str,
    operation: serde_json::Value,
) -> serde_json::Value {
    let mut last_error = None;
    let mut connected = None;
    for _ in 0..100 {
        match UnixStream::connect(socket_path).await {
            Ok(stream) => {
                connected = Some(stream);
                break;
            }
            Err(error) => {
                last_error = Some(error);
                time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    let mut stream = connected.unwrap_or_else(|| {
        panic!("worker socket did not become ready: {last_error:?}")
    });
    let payload = json!({
        "schema_version": FEDERATION_IPC_SCHEMA_VERSION,
        "request_id": request_id,
        "deadline_unix_ms": unix_now_ms() + 5000,
        "operation": operation,
    });
    let bytes = serde_json::to_vec(&payload).unwrap();
    stream
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&bytes).await.unwrap();

    let mut len_bytes = [0_u8; 4];
    stream.read_exact(&mut len_bytes).await.unwrap();
    let len = u32::from_be_bytes(len_bytes) as usize;
    let mut response_bytes = vec![0_u8; len];
    stream.read_exact(&mut response_bytes).await.unwrap();
    let response: serde_json::Value =
        serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(response["request_id"], json!(request_id));
    assert_eq!(response["ok"], json!(true));
    response["result"].clone()
}

pub(super) async fn request_worker_with_deadline(
    socket_path: &Path,
    request_id: &str,
    operation: serde_json::Value,
    deadline_unix_ms: u64,
) -> serde_json::Value {
    let mut stream = UnixStream::connect(socket_path).await.unwrap();
    let payload = json!({
        "schema_version": FEDERATION_IPC_SCHEMA_VERSION,
        "request_id": request_id,
        "deadline_unix_ms": deadline_unix_ms,
        "operation": operation,
    });
    let bytes = serde_json::to_vec(&payload).unwrap();
    stream
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&bytes).await.unwrap();

    let mut len_bytes = [0_u8; 4];
    stream.read_exact(&mut len_bytes).await.unwrap();
    let len = u32::from_be_bytes(len_bytes) as usize;
    let mut response_bytes = vec![0_u8; len];
    stream.read_exact(&mut response_bytes).await.unwrap();
    let response: serde_json::Value =
        serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(response["request_id"], json!(request_id));
    response
}

pub(super) fn host_result<'a>(
    response: &'a serde_json::Value,
    alias: &str,
) -> &'a serde_json::Value {
    response["result"]["hosts"]
        .as_array()
        .unwrap_or_else(|| panic!("response had no hosts array: {response}"))
        .iter()
        .find(|host| host["alias"] == json!(alias))
        .unwrap_or_else(|| {
            panic!("no host result for alias {alias:?} in {response}")
        })
}

pub(super) struct HttpsFixture {
    pub(super) port: u16,
    pub(super) requests: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
}

pub(super) async fn start_https_fixture(
    root: &Path,
    ca_ref: &str,
    installation_id: &str,
) -> HttpsFixture {
    let cert_dir = root.join("certs").join(ca_ref);
    fs::create_dir_all(&cert_dir).unwrap();
    let ca_path = cert_dir.join("ca.pem");
    let cert_path = cert_dir.join("cert.pem");
    let key_path = cert_dir.join("key.pem");
    generate_loopback_certificate(&ca_path, &cert_path, &key_path);
    let trust_dir = root.join("fleet").join("trust").join("ca");
    fs::create_dir_all(&trust_dir).unwrap();
    fs::copy(&ca_path, trust_dir.join(format!("{ca_ref}.pem"))).unwrap();

    let config = rustls_server_config(&cert_path, &key_path);
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = requests.clone();
    let installation = installation_id.to_string();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _addr)) = listener.accept().await else {
                return;
            };
            let acceptor = acceptor.clone();
            let recorded = recorded.clone();
            let installation = installation.clone();
            tokio::spawn(async move {
                let Ok(mut stream) = acceptor.accept(stream).await else {
                    return;
                };
                let Some((path, body)) = read_http_request(&mut stream).await
                else {
                    return;
                };
                recorded
                    .lock()
                    .unwrap()
                    .push(json!({"path": path, "body": body}));
                let payload =
                    https_fixture_payload(&installation, &path, body.as_ref());
                write_http_json(&mut stream, payload).await;
            });
        }
    });
    HttpsFixture { port, requests }
}

pub(super) fn generate_loopback_certificate(
    ca_path: &Path,
    cert_path: &Path,
    key_path: &Path,
) {
    let cert_dir = cert_path.parent().expect("cert parent");
    let ca_key_path = cert_dir.join("ca-key.pem");
    let csr_path = cert_dir.join("server.csr");
    let ext_path = cert_dir.join("server-ext.cnf");
    fs::write(
        &ext_path,
        "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=IP:127.0.0.1,DNS:localhost\n",
    )
    .unwrap();
    run_openssl(
        Command::new("openssl")
            .args(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout"])
            .arg(&ca_key_path)
            .args(["-out"])
            .arg(ca_path)
            .args([
                "-subj",
                "/CN=SASE Test CA",
                "-addext",
                "basicConstraints=critical,CA:TRUE",
                "-days",
                "1",
            ]),
    );
    run_openssl(
        Command::new("openssl")
            .args(["req", "-newkey", "rsa:2048", "-nodes", "-keyout"])
            .arg(key_path)
            .args(["-out"])
            .arg(&csr_path)
            .args(["-subj", "/CN=localhost"]),
    );
    let _ = fs::remove_file(cert_dir.join("ca.srl"));
    run_openssl(
        Command::new("openssl")
            .args(["x509", "-req", "-in"])
            .arg(&csr_path)
            .args(["-CA"])
            .arg(ca_path)
            .args(["-CAkey"])
            .arg(&ca_key_path)
            .args(["-CAcreateserial", "-out"])
            .arg(cert_path)
            .args(["-days", "1", "-extfile"])
            .arg(&ext_path),
    );
}

pub(super) fn run_openssl(command: &mut Command) {
    let output = command
        .output()
        .unwrap_or_else(|error| panic!("failed to execute openssl: {error}"));
    assert!(
        output.status.success(),
        "openssl failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

pub(super) fn rustls_server_config(
    cert_path: &Path,
    key_path: &Path,
) -> tokio_rustls::rustls::ServerConfig {
    let cert_bytes = fs::read(cert_path).unwrap();
    let mut cert_reader = std::io::BufReader::new(&cert_bytes[..]);
    let certs = rustls_pemfile::certs(&mut cert_reader)
        .unwrap()
        .into_iter()
        .map(tokio_rustls::rustls::Certificate)
        .collect::<Vec<_>>();
    let key_bytes = fs::read(key_path).unwrap();
    let mut key_reader = std::io::BufReader::new(&key_bytes[..]);
    let mut keys = rustls_pemfile::pkcs8_private_keys(&mut key_reader).unwrap();
    if keys.is_empty() {
        let mut key_reader = std::io::BufReader::new(&key_bytes[..]);
        keys = rustls_pemfile::rsa_private_keys(&mut key_reader).unwrap();
    }
    tokio_rustls::rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(
            certs,
            tokio_rustls::rustls::PrivateKey(
                keys.into_iter().next().expect("private key"),
            ),
        )
        .unwrap()
}

pub(super) async fn read_http_request<T>(
    stream: &mut T,
) -> Option<(String, Option<serde_json::Value>)>
where
    T: tokio::io::AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 1024];
    loop {
        let read = stream.read(&mut buffer).await.ok()?;
        if read == 0 {
            return None;
        }
        bytes.extend_from_slice(&buffer[..read]);
        let Some(header_end) = find_header_end(&bytes) else {
            continue;
        };
        let headers = String::from_utf8_lossy(&bytes[..header_end]).to_string();
        let content_length = http_content_length(&headers);
        let body_start = header_end + 4;
        if bytes.len() < body_start + content_length {
            continue;
        }
        let path = headers
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or("/")
            .to_string();
        let body = if content_length == 0 {
            None
        } else {
            serde_json::from_slice(
                &bytes[body_start..body_start + content_length],
            )
            .ok()
        };
        return Some((path, body));
    }
}

pub(super) fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

pub(super) fn http_content_length(headers: &str) -> usize {
    headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if !name.eq_ignore_ascii_case("content-length") {
                return None;
            }
            value.trim().parse::<usize>().ok()
        })
        .unwrap_or(0)
}

pub(super) async fn write_http_json<T>(
    stream: &mut T,
    payload: serde_json::Value,
) where
    T: tokio::io::AsyncWrite + Unpin,
{
    let status = if payload.get("error").is_some() {
        "404 Not Found"
    } else {
        "200 OK"
    };
    let body = serde_json::to_vec(&payload).unwrap();
    let header = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
}

pub(super) fn https_fixture_payload(
    installation_id: &str,
    path: &str,
    body: Option<&serde_json::Value>,
) -> serde_json::Value {
    match path {
        "/api/fleet/v1/hello" => hello_payload(installation_id),
        "/api/fleet/v1/summary" => json!({
            "schema_version": 1,
            "cursor": cursor_payload(),
            "catalog_scope": "presentation",
            "catalog_snapshot_id": empty_presentation_snapshot_id(),
            "counts": counts_payload(1, 1),
            "count_revision": 1,
            "freshness": freshness_payload(),
        }),
        "/api/fleet/v1/catalog" => {
            let cursor = body
                .and_then(|value| value.get("cursor"))
                .and_then(serde_json::Value::as_str);
            let snapshot_id = empty_presentation_snapshot_id();
            let next_cursor = format!("catcur_v1:p:{snapshot_id}:100");
            json!({
                "schema_version": 1,
                "cursor": cursor_payload(),
                "counts": counts_payload(1, 1),
                "count_revision": 1,
                "freshness": freshness_payload(),
                "page": {
                    "schema_version": 1,
                    "scope": "presentation",
                    "snapshot_id": snapshot_id,
                    "rows": [],
                    "limit": 100,
                    "total_matching_rows": 250,
                    "next_cursor": if cursor.is_none() {
                        json!(next_cursor)
                    } else {
                        serde_json::Value::Null
                    },
                    "has_more": cursor.is_none(),
                    "state": if cursor.is_none() {
                        json!("ready")
                    } else {
                        json!("finished")
                    },
                },
            })
        }
        _ => json!({"error": "not found"}),
    }
}

pub(super) fn empty_presentation_snapshot_id() -> String {
    fleet_catalog_snapshot_id(FleetCatalogScopeWire::Presentation, &[]).unwrap()
}

pub(super) fn hello_payload(installation_id: &str) -> serde_json::Value {
    json!({
        "schema_version": 1,
        "protocol_version": FLEET_PROTOCOL_VERSION,
        "installation": {
            "schema_version": 1,
            "installation_id": installation_id,
            "created_at_unix": 1_800_000_000.0,
            "generation": 1,
            "prior_installation_id": null,
            "rotated_at_unix": null,
            "adopted_at_unix": null,
            "reason": null,
        },
        "machine_selector": "loopback",
        "capabilities": {
            "schema_version": 1,
            "resource": ["catalog.read", "summary.read"],
            "host": [],
            "protocol": ["fleet.v1"],
        },
        "credential": {
            "schema_version": 1,
            "credential_id": "cred-1",
            "controller_id": Some("controller-1"),
            "controller": {
                "schema_version": 1,
                "controller_id": Some("controller-1"),
                "display_name": "test-controller",
                "platform": null,
                "app_version": null,
            },
            "scopes": ["fleet.read"],
            "issued_at_unix": 1_800_000_000.0,
            "expires_at_unix": null,
            "rotated_at_unix": null,
            "revoked_at_unix": null,
            "revoked_reason": null,
        },
        "cursor": cursor_payload(),
        "counts": counts_payload(1, 1),
        "count_revision": 1,
        "freshness": freshness_payload(),
    })
}

pub(super) fn cursor_payload() -> serde_json::Value {
    json!({
        "schema_version": 1,
        "store_generation": "test-generation",
        "sequence": 1,
    })
}

pub(super) fn counts_payload(total: u64, running: u64) -> serde_json::Value {
    json!({
        "schema_version": 1,
        "basis": {
            "schema_version": 1,
            "input_rows": total,
            "selected_rows": total,
            "max_revision": 1,
            "observed_at_unix_max": 1_800_000_000.0,
        },
        "logical_agent_total": total,
        "running": running,
        "waiting": 0,
        "attention": 0,
        "occupied_runner_slots": 0,
    })
}

pub(super) fn freshness_payload() -> serde_json::Value {
    json!({
        "schema_version": 1,
        "freshness": "fresh",
        "partial": false,
        "refreshed_at_unix": 1_800_000_000.0,
        "error": null,
    })
}

pub(super) fn tls_host(
    alias: &str,
    pin: &str,
    port: u16,
    mode: &str,
    ca_ref: Option<&str>,
) -> serde_json::Value {
    json!({
        "schema_version": FEDERATION_IPC_SCHEMA_VERSION,
        "alias": alias,
        "plan": {
            "schema_version": 1,
            "provider_ref": "builtin:https",
            "endpoint": format!("https://127.0.0.1:{port}"),
            "credential_ref": format!("cred-{alias}"),
            "pinned_installation_id": pin,
            "connection_kind": "gateway",
            "tls": {
                "schema_version": 1,
                "mode": mode,
                "ca_ref": ca_ref,
                "server_name_ref": null,
            },
        },
        "bearer_token": format!("token-{alias}"),
    })
}

pub(super) fn request_paths(
    requests: &Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
) -> Vec<String> {
    requests
        .lock()
        .unwrap()
        .iter()
        .filter_map(|request| {
            request
                .get("path")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .collect()
}
