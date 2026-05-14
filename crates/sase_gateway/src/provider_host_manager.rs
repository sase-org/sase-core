use std::{
    io::{BufRead, BufReader, Read, Write},
    path::Path,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use serde_json::{json, Value as JsonValue};

use crate::{
    host_bridge::split_command_words,
    wire::{
        HostErrorCodeWire, HostErrorWire, HostFallbackWire, HostLogLevelWire,
        HostLogRecordWire, HostRequestEnvelopeWire, HostResourceUsageWire,
        HostResponseEnvelopeWire, HostResponseStatusWire,
        PROVIDER_HOST_IPC_WIRE_SCHEMA_VERSION,
    },
};

const DEFAULT_PROVIDER_HOST_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_MAX_CONCURRENT_CALLS: usize = 4;
const MAX_CAPTURED_STDERR_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug)]
pub struct ProviderHostManager {
    inner: Arc<ProviderHostManagerInner>,
}

#[derive(Debug)]
struct ProviderHostManagerInner {
    command: Vec<String>,
    max_concurrent_calls: usize,
    active_calls: AtomicUsize,
    total_started: AtomicU64,
    total_completed: AtomicU64,
    total_failed: AtomicU64,
    total_timeouts: AtomicU64,
    total_cancellations: AtomicU64,
    total_backpressure: AtomicU64,
    total_manifest_denials: AtomicU64,
    cancellations: Mutex<Vec<(String, Arc<AtomicBool>)>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderHostStatus {
    pub command: Vec<String>,
    pub active_calls: usize,
    pub max_concurrent_calls: usize,
    pub total_started: u64,
    pub total_completed: u64,
    pub total_failed: u64,
    pub total_timeouts: u64,
    pub total_cancellations: u64,
    pub total_backpressure: u64,
    pub total_manifest_denials: u64,
}

#[derive(Debug)]
struct ActiveCallGuard {
    inner: Arc<ProviderHostManagerInner>,
}

impl ProviderHostManager {
    pub fn new(command: Vec<String>) -> Self {
        Self::new_with_limit(command, DEFAULT_MAX_CONCURRENT_CALLS)
    }

    pub fn new_with_limit(
        command: Vec<String>,
        max_concurrent_calls: usize,
    ) -> Self {
        Self {
            inner: Arc::new(ProviderHostManagerInner {
                command,
                max_concurrent_calls: max_concurrent_calls.max(1),
                active_calls: AtomicUsize::new(0),
                total_started: AtomicU64::new(0),
                total_completed: AtomicU64::new(0),
                total_failed: AtomicU64::new(0),
                total_timeouts: AtomicU64::new(0),
                total_cancellations: AtomicU64::new(0),
                total_backpressure: AtomicU64::new(0),
                total_manifest_denials: AtomicU64::new(0),
                cancellations: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn from_env_or_default() -> Self {
        let command = std::env::var("SASE_PROVIDER_HOST_COMMAND")
            .ok()
            .and_then(|raw| split_command_words(&raw).ok())
            .filter(|parts| !parts.is_empty())
            .unwrap_or_else(|| {
                vec![
                    "sase".to_string(),
                    "daemon".to_string(),
                    "provider-host".to_string(),
                ]
            });
        Self::new(command)
    }

    pub fn call_blocking(
        &self,
        request: HostRequestEnvelopeWire,
    ) -> HostResponseEnvelopeWire {
        if let Err(error) = sase_core::provider_host::validate_host_request(
            &request,
            &sase_core::provider_host::HostValidationPolicy::default(),
        ) {
            return response_from_validation_error(&request.request_id, error);
        }
        let Some(_guard) = self.try_acquire() else {
            self.inner.total_failed.fetch_add(1, Ordering::Relaxed);
            self.inner
                .total_backpressure
                .fetch_add(1, Ordering::Relaxed);
            return error_response(
                &request.request_id,
                HostErrorCodeWire::ResourceLimitExceeded,
                "provider host manager is at capacity",
                true,
                Some("host.backpressure".to_string()),
                Some(json!({
                    "active_calls": self.inner.active_calls.load(Ordering::Relaxed),
                    "max_concurrent_calls": self.inner.max_concurrent_calls,
                })),
                0,
            );
        };

        self.inner.total_started.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let timeout = Duration::from_millis(
            request
                .deadline
                .timeout_ms
                .unwrap_or(DEFAULT_PROVIDER_HOST_TIMEOUT_MS),
        );
        let cancel_flag = Arc::new(AtomicBool::new(false));
        self.register_cancellation(
            request.request_id.clone(),
            cancel_flag.clone(),
        );
        let result = self.call_child_process(&request, timeout, cancel_flag);
        self.unregister_cancellation(&request.request_id);
        match result {
            Ok(mut response) => {
                if let Err(error) =
                    sase_core::provider_host::validate_host_response(
                        &request, &response,
                    )
                {
                    self.inner.total_failed.fetch_add(1, Ordering::Relaxed);
                    self.record_validation_failure(&error);
                    return response_from_validation_error(
                        &request.request_id,
                        error,
                    );
                }
                self.record_response_diagnostics(&response);
                self.inner.total_completed.fetch_add(1, Ordering::Relaxed);
                if response.duration_ms == 0 {
                    response.duration_ms = started.elapsed().as_millis() as u64;
                }
                response
            }
            Err(error) => {
                self.inner.total_failed.fetch_add(1, Ordering::Relaxed);
                self.record_call_error(&error);
                error.into_response(&request.request_id, started.elapsed())
            }
        }
    }

    pub fn cancel_request(&self, request_id: &str) -> bool {
        let Ok(guard) = self.inner.cancellations.lock() else {
            return false;
        };
        let mut changed = false;
        for (candidate, flag) in guard.iter() {
            if candidate == request_id {
                flag.store(true, Ordering::SeqCst);
                changed = true;
            }
        }
        changed
    }

    pub fn status(&self) -> ProviderHostStatus {
        ProviderHostStatus {
            command: self.inner.command.clone(),
            active_calls: self.inner.active_calls.load(Ordering::Relaxed),
            max_concurrent_calls: self.inner.max_concurrent_calls,
            total_started: self.inner.total_started.load(Ordering::Relaxed),
            total_completed: self.inner.total_completed.load(Ordering::Relaxed),
            total_failed: self.inner.total_failed.load(Ordering::Relaxed),
            total_timeouts: self.inner.total_timeouts.load(Ordering::Relaxed),
            total_cancellations: self
                .inner
                .total_cancellations
                .load(Ordering::Relaxed),
            total_backpressure: self
                .inner
                .total_backpressure
                .load(Ordering::Relaxed),
            total_manifest_denials: self
                .inner
                .total_manifest_denials
                .load(Ordering::Relaxed),
        }
    }

    pub fn resource_policy_diagnostics(&self) -> JsonValue {
        json!({
            "timeout": {
                "state": "active",
                "default_ms": DEFAULT_PROVIDER_HOST_TIMEOUT_MS,
                "source": "request deadlines are bounded by daemon host validation policy"
            },
            "rss_soft_cap": {
                "state": "unavailable",
                "reason": "portable subprocess RSS cap enforcement is not enabled in v1"
            },
            "cgroup_v2_cpu_quota": cgroup_v2_diagnostics(),
            "seccomp": seccomp_diagnostics(),
            "compatibility_mode": {
                "state": "active",
                "reason": "known built-ins use compatibility manifests until real provider routing lands"
            }
        })
    }

    fn try_acquire(&self) -> Option<ActiveCallGuard> {
        let mut current = self.inner.active_calls.load(Ordering::Acquire);
        loop {
            if current >= self.inner.max_concurrent_calls {
                return None;
            }
            match self.inner.active_calls.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(ActiveCallGuard {
                        inner: Arc::clone(&self.inner),
                    });
                }
                Err(next) => current = next,
            }
        }
    }

    fn record_call_error(&self, error: &ProviderHostCallError) {
        match error {
            ProviderHostCallError::Timeout(_) => {
                self.inner.total_timeouts.fetch_add(1, Ordering::Relaxed);
            }
            ProviderHostCallError::Cancelled => {
                self.inner
                    .total_cancellations
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn record_response_diagnostics(&self, response: &HostResponseEnvelopeWire) {
        let Some(error) = response.error.as_ref() else {
            return;
        };
        match error.code {
            HostErrorCodeWire::HostTimeout => {
                self.inner.total_timeouts.fetch_add(1, Ordering::Relaxed);
            }
            HostErrorCodeWire::HostCancelled => {
                self.inner
                    .total_cancellations
                    .fetch_add(1, Ordering::Relaxed);
            }
            HostErrorCodeWire::OperationUnsupported
            | HostErrorCodeWire::NetworkDenied
            | HostErrorCodeWire::ManifestInvalid => {
                if error
                    .target
                    .as_deref()
                    .is_some_and(|target| target.starts_with("manifest."))
                {
                    self.inner
                        .total_manifest_denials
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            _ => {}
        }
    }

    fn record_validation_failure(
        &self,
        error: &sase_core::provider_host::HostValidationError,
    ) {
        let wire = error.to_error_wire();
        if matches!(
            wire.code,
            HostErrorCodeWire::OperationUnsupported
                | HostErrorCodeWire::NetworkDenied
                | HostErrorCodeWire::ManifestInvalid
        ) && wire
            .target
            .as_deref()
            .is_some_and(|target| target.starts_with("manifest."))
        {
            self.inner
                .total_manifest_denials
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn register_cancellation(&self, request_id: String, flag: Arc<AtomicBool>) {
        if let Ok(mut guard) = self.inner.cancellations.lock() {
            guard.push((request_id, flag));
        }
    }

    fn unregister_cancellation(&self, request_id: &str) {
        if let Ok(mut guard) = self.inner.cancellations.lock() {
            guard.retain(|(candidate, _)| candidate != request_id);
        }
    }

    fn call_child_process(
        &self,
        request: &HostRequestEnvelopeWire,
        timeout: Duration,
        cancel_flag: Arc<AtomicBool>,
    ) -> Result<HostResponseEnvelopeWire, ProviderHostCallError> {
        let (program, fixed_args) =
            self.inner.command.split_first().ok_or_else(|| {
                ProviderHostCallError::HostUnavailable(
                    "provider host command is empty".to_string(),
                )
            })?;
        let mut child = Command::new(program)
            .args(fixed_args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| {
                ProviderHostCallError::HostUnavailable(format!(
                    "failed to spawn provider host: {error}"
                ))
            })?;

        write_request(&mut child, request)?;
        let stdout = child.stdout.take().ok_or_else(|| {
            ProviderHostCallError::Protocol(
                "provider host stdout unavailable".to_string(),
            )
        })?;
        let stderr = child.stderr.take();
        let (line_tx, line_rx) = mpsc::channel();
        thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            let result = reader.read_line(&mut line).map(|_| line);
            let _ = line_tx.send(result);
        });
        let stderr_handle = stderr.map(|stream| {
            thread::spawn(move || {
                read_limited_stderr(stream, MAX_CAPTURED_STDERR_BYTES)
            })
        });

        let started = Instant::now();
        let line = loop {
            if cancel_flag.load(Ordering::SeqCst) {
                kill_child(&mut child);
                return Err(ProviderHostCallError::Cancelled);
            }
            if started.elapsed() >= timeout {
                kill_child(&mut child);
                return Err(ProviderHostCallError::Timeout(timeout));
            }
            match line_rx.recv_timeout(Duration::from_millis(10)) {
                Ok(Ok(line)) => break line,
                Ok(Err(error)) => {
                    kill_child(&mut child);
                    return Err(ProviderHostCallError::Protocol(format!(
                        "failed reading provider host stdout: {error}"
                    )));
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    kill_child(&mut child);
                    return Err(ProviderHostCallError::Protocol(
                        "provider host closed stdout without a response"
                            .to_string(),
                    ));
                }
            }
        };
        let status = child.wait().map_err(|error| {
            ProviderHostCallError::HostUnavailable(format!(
                "failed waiting for provider host: {error}"
            ))
        })?;
        let stderr = stderr_handle
            .and_then(|handle| handle.join().ok())
            .unwrap_or_default();
        let mut response: HostResponseEnvelopeWire =
            serde_json::from_str(&line).map_err(|error| {
                ProviderHostCallError::Protocol(format!(
                    "provider host returned invalid JSON: {error}"
                ))
            })?;
        response.logs.extend(stderr_logs(&stderr));
        if !status.success() && response.status == HostResponseStatusWire::Ok {
            return Err(ProviderHostCallError::Execution(format!(
                "provider host exited with status {status}"
            )));
        }
        Ok(response)
    }
}

impl ActiveCallGuard {
    fn release(&self) {
        self.inner.active_calls.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Drop for ActiveCallGuard {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Debug)]
enum ProviderHostCallError {
    HostUnavailable(String),
    Timeout(Duration),
    Cancelled,
    Protocol(String),
    Execution(String),
}

impl ProviderHostCallError {
    fn into_response(
        self,
        request_id: &str,
        duration: Duration,
    ) -> HostResponseEnvelopeWire {
        match self {
            Self::HostUnavailable(message) => error_response(
                request_id,
                HostErrorCodeWire::HostUnavailable,
                &message,
                true,
                Some("host.process".to_string()),
                None,
                duration.as_millis() as u64,
            ),
            Self::Timeout(timeout) => error_response(
                request_id,
                HostErrorCodeWire::HostTimeout,
                &format!(
                    "provider host call exceeded {}ms",
                    timeout.as_millis()
                ),
                true,
                Some("deadline.timeout_ms".to_string()),
                None,
                duration.as_millis() as u64,
            ),
            Self::Cancelled => error_response(
                request_id,
                HostErrorCodeWire::HostCancelled,
                "provider host call was cancelled",
                true,
                Some("request_id".to_string()),
                None,
                duration.as_millis() as u64,
            ),
            Self::Protocol(message) => error_response(
                request_id,
                HostErrorCodeWire::HostProtocolError,
                &message,
                false,
                Some("host.protocol".to_string()),
                None,
                duration.as_millis() as u64,
            ),
            Self::Execution(message) => error_response(
                request_id,
                HostErrorCodeWire::ProviderExecutionFailed,
                &message,
                false,
                Some("host.process".to_string()),
                None,
                duration.as_millis() as u64,
            ),
        }
    }
}

fn write_request(
    child: &mut Child,
    request: &HostRequestEnvelopeWire,
) -> Result<(), ProviderHostCallError> {
    let mut stdin = child.stdin.take().ok_or_else(|| {
        ProviderHostCallError::HostUnavailable(
            "provider host stdin unavailable".to_string(),
        )
    })?;
    serde_json::to_writer(&mut stdin, request).map_err(|error| {
        ProviderHostCallError::Protocol(format!(
            "failed encoding provider host request: {error}"
        ))
    })?;
    stdin.write_all(b"\n").map_err(|error| {
        ProviderHostCallError::HostUnavailable(format!(
            "failed writing provider host request: {error}"
        ))
    })?;
    Ok(())
}

fn kill_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn read_limited_stderr(mut stream: impl Read, max_bytes: usize) -> String {
    let mut buffer = Vec::new();
    let mut limited = stream.by_ref().take(max_bytes as u64);
    let _ = limited.read_to_end(&mut buffer);
    String::from_utf8_lossy(&buffer).to_string()
}

fn stderr_logs(stderr: &str) -> Vec<HostLogRecordWire> {
    stderr
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| HostLogRecordWire {
            level: HostLogLevelWire::Info,
            message: redact_host_log(line),
            target: Some("provider_host.stderr".to_string()),
            stream: Some("stderr".to_string()),
            timestamp: None,
        })
        .collect()
}

fn response_from_validation_error(
    request_id: &str,
    error: sase_core::provider_host::HostValidationError,
) -> HostResponseEnvelopeWire {
    let mut error_wire = error.to_error_wire();
    error_wire.details =
        Some(json!({"phase": "daemon_host_manager_validation"}));
    HostResponseEnvelopeWire {
        schema_version: PROVIDER_HOST_IPC_WIRE_SCHEMA_VERSION,
        request_id: request_id.to_string(),
        status: HostResponseStatusWire::Error,
        result: JsonValue::Object(Default::default()),
        error: Some(error_wire),
        logs: Vec::new(),
        duration_ms: 0,
        resource_usage: Some(HostResourceUsageWire {
            wall_ms: 0,
            cpu_ms: None,
            peak_rss_bytes: None,
            spawned_processes: 0,
            network_requests: 0,
        }),
        side_effects: Vec::new(),
    }
}

fn error_response(
    request_id: &str,
    code: HostErrorCodeWire,
    message: &str,
    retryable: bool,
    target: Option<String>,
    details: Option<JsonValue>,
    duration_ms: u64,
) -> HostResponseEnvelopeWire {
    HostResponseEnvelopeWire {
        schema_version: PROVIDER_HOST_IPC_WIRE_SCHEMA_VERSION,
        request_id: request_id.to_string(),
        status: if code == HostErrorCodeWire::HostCancelled {
            HostResponseStatusWire::Cancelled
        } else {
            HostResponseStatusWire::Error
        },
        result: JsonValue::Object(Default::default()),
        error: Some(HostErrorWire {
            schema_version: PROVIDER_HOST_IPC_WIRE_SCHEMA_VERSION,
            code,
            message: redact_host_log(message),
            retryable,
            target,
            details,
            fallback: HostFallbackWire {
                available: true,
                reason: Some("direct_python_provider_path".to_string()),
                message: Some(
                    "real provider routing is disabled for Phase 8C"
                        .to_string(),
                ),
            },
        }),
        logs: Vec::new(),
        duration_ms,
        resource_usage: Some(HostResourceUsageWire {
            wall_ms: duration_ms,
            cpu_ms: None,
            peak_rss_bytes: None,
            spawned_processes: 1,
            network_requests: 0,
        }),
        side_effects: Vec::new(),
    }
}

fn redact_host_log(message: &str) -> String {
    message
        .split_whitespace()
        .map(|part| {
            let lower = part.to_ascii_lowercase();
            if lower.starts_with("token=")
                || lower.starts_with("password=")
                || lower.starts_with("secret=")
                || lower.starts_with("api_key=")
            {
                "[REDACTED]"
            } else {
                part
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn cgroup_v2_diagnostics() -> JsonValue {
    let controllers = Path::new("/sys/fs/cgroup/cgroup.controllers");
    if controllers.exists() {
        json!({
            "state": "available",
            "enforced": false,
            "reason": "cgroup v2 detected; quota enforcement is opt-in for provider host v1"
        })
    } else {
        json!({
            "state": "unavailable",
            "enforced": false,
            "reason": "cgroup v2 controllers file is not present"
        })
    }
}

fn seccomp_diagnostics() -> JsonValue {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok();
        let mode = status
            .as_deref()
            .and_then(|content| {
                content.lines().find_map(|line| {
                    line.strip_prefix("Seccomp:")
                        .map(str::trim)
                        .map(str::to_string)
                })
            })
            .unwrap_or_else(|| "unknown".to_string());
        json!({
            "state": if mode == "unknown" { "unavailable" } else { "available" },
            "enforced": mode != "0" && mode != "unknown",
            "mode": mode,
            "reason": "seccomp profile detection only; provider host v1 does not install profiles"
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        json!({
            "state": "unavailable",
            "enforced": false,
            "reason": "seccomp detection is Linux-only"
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{
        HostActorWire, HostDeadlineWire, HostEnvironmentPolicyWire,
        HostOperationSelectorWire, HostWorkspaceIdentityWire, HOST_CAP_IPC_V1,
    };

    fn request(operation: &str, payload: JsonValue) -> HostRequestEnvelopeWire {
        HostRequestEnvelopeWire {
            schema_version: PROVIDER_HOST_IPC_WIRE_SCHEMA_VERSION,
            request_id: "host_req_rust".to_string(),
            deadline: HostDeadlineWire {
                timeout_ms: Some(1_000),
                deadline_unix_ms: None,
                cancellation_token: None,
            },
            actor: HostActorWire {
                schema_version: PROVIDER_HOST_IPC_WIRE_SCHEMA_VERSION,
                actor_type: "test".to_string(),
                name: "cargo".to_string(),
                version: None,
                runtime: Some("rust".to_string()),
            },
            operation: HostOperationSelectorWire {
                family: "config".to_string(),
                operation: operation.to_string(),
            },
            declared_capabilities: vec![HOST_CAP_IPC_V1.to_string()],
            workspace: HostWorkspaceIdentityWire {
                project_id: "project".to_string(),
                project_dir: None,
                workspace_dir: None,
                changespec: None,
            },
            environment: HostEnvironmentPolicyWire {
                inherit: false,
                allow: Vec::new(),
                deny: Vec::new(),
                required: Vec::new(),
            },
            manifest: None,
            payload,
        }
    }

    #[test]
    fn manager_returns_backpressure_error_at_capacity() {
        let manager = ProviderHostManager::new_with_limit(
            vec!["missing-provider-host".to_string()],
            1,
        );
        let _guard = manager.try_acquire().unwrap();

        let response = manager.call_blocking(request("fake.echo", json!({})));

        assert_eq!(response.status, HostResponseStatusWire::Error);
        assert_eq!(
            response.error.unwrap().code,
            HostErrorCodeWire::ResourceLimitExceeded
        );
        assert_eq!(manager.status().total_backpressure, 1);
    }

    #[test]
    fn manager_cancel_request_marks_running_call() {
        let manager = ProviderHostManager::new(vec!["missing".to_string()]);
        let flag = Arc::new(AtomicBool::new(false));
        manager.register_cancellation("req".to_string(), flag.clone());

        assert!(manager.cancel_request("req"));
        assert!(flag.load(Ordering::SeqCst));
        manager.unregister_cancellation("req");
    }

    #[test]
    fn manager_status_counts_timeout_failures() {
        let manager = ProviderHostManager::new(vec![
            "python3".to_string(),
            "-c".to_string(),
            "import time; time.sleep(2)".to_string(),
        ]);

        let response = manager.call_blocking(request("fake.echo", json!({})));

        assert_eq!(response.status, HostResponseStatusWire::Error);
        assert_eq!(
            response.error.unwrap().code,
            HostErrorCodeWire::HostTimeout
        );
        assert_eq!(manager.status().total_timeouts, 1);
    }

    #[test]
    fn manager_round_trips_fake_operation_through_python_host() {
        let Some(src_dir) = python_src_dir() else {
            return;
        };
        let python = std::env::var("PYO3_PYTHON")
            .or_else(|_| std::env::var("PYTHON"))
            .unwrap_or_else(|_| "python3".to_string());
        let manager = ProviderHostManager::new(vec![
            "env".to_string(),
            format!("PYTHONPATH={}", src_dir.display()),
            python,
            "-m".to_string(),
            "sase".to_string(),
            "daemon".to_string(),
            "provider-host".to_string(),
        ]);

        let response = manager.call_blocking(request(
            "fake.log",
            json!({"message": "hello token=secret"}),
        ));

        assert_eq!(response.status, HostResponseStatusWire::Ok);
        assert_eq!(response.result, json!({"logged": true}));
        assert!(response
            .logs
            .iter()
            .any(|log| log.message == "hello token=[REDACTED]"));
        assert!(response.logs.iter().any(|log| {
            log.stream.as_deref() == Some("stderr")
                && log.message.contains("provider host handled request_id=")
        }));
    }

    fn python_src_dir() -> Option<std::path::PathBuf> {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let org_dir = manifest_dir.parent()?.parent()?.parent()?.parent()?;
        for candidate in ["sase_100", "sase"] {
            let src_dir = org_dir.join(candidate).join("src");
            if src_dir
                .join("sase")
                .join("host")
                .join("runtime.py")
                .exists()
            {
                return Some(src_dir);
            }
        }
        None
    }
}
