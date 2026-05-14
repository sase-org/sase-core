//! Versioned provider/plugin host IPC wire contract.
//!
//! Phase 8B defines the shared request/response vocabulary and validation
//! helpers only. Production provider calls are intentionally still routed
//! through the existing Python paths until later phases install a real host.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

pub const PROVIDER_HOST_IPC_WIRE_SCHEMA_VERSION: u32 = 1;
pub const HOST_CAP_IPC_V1: &str = "host.ipc.v1";
pub const HOST_CAP_MANIFEST_V1: &str = "host.manifest.v1";
pub const HOST_CAP_LLM_METADATA: &str = "host.llm.metadata";
pub const HOST_CAP_XPROMPT_CATALOG: &str = "host.xprompt.catalog";

pub const HOST_OPERATION_FAMILIES: [&str; 6] = [
    "llm",
    "vcs",
    "workspace",
    "xprompt",
    "config",
    "workflow.step",
];

pub const HOST_ERROR_CODES: [&str; 12] = [
    "host_unavailable",
    "host_timeout",
    "host_cancelled",
    "plugin_not_found",
    "operation_unsupported",
    "capability_denied",
    "network_denied",
    "resource_limit_exceeded",
    "invalid_side_effect_intent",
    "host_protocol_error",
    "provider_execution_failed",
    "manifest_invalid",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostActorWire {
    pub schema_version: u32,
    pub actor_type: String,
    pub name: String,
    pub version: Option<String>,
    pub runtime: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostWorkspaceIdentityWire {
    pub project_id: String,
    pub project_dir: Option<String>,
    pub workspace_dir: Option<String>,
    pub changespec: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostOperationSelectorWire {
    pub family: String,
    pub operation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostDeadlineWire {
    pub timeout_ms: Option<u64>,
    pub deadline_unix_ms: Option<u64>,
    pub cancellation_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostEnvironmentPolicyWire {
    #[serde(default)]
    pub inherit: bool,
    #[serde(default)]
    pub allow: Vec<String>,
    #[serde(default)]
    pub deny: Vec<String>,
    #[serde(default)]
    pub required: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostNetworkPolicyWire {
    pub mode: String,
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostProcessPolicyWire {
    #[serde(default)]
    pub spawn_allowed: bool,
    #[serde(default)]
    pub allowed_commands: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostEnvironmentRequirementWire {
    #[serde(default)]
    pub required_vars: Vec<String>,
    #[serde(default)]
    pub optional_vars: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostManifestWire {
    pub schema_version: u32,
    pub plugin_id: String,
    pub version: String,
    #[serde(default)]
    pub operation_families: Vec<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    pub network: HostNetworkPolicyWire,
    #[serde(default)]
    pub filesystem_roots: Vec<String>,
    pub process: HostProcessPolicyWire,
    pub environment: HostEnvironmentRequirementWire,
    #[serde(default)]
    pub timeout_hints_ms: BTreeMap<String, u64>,
    #[serde(default)]
    pub warm_host_eligible: bool,
    #[serde(default)]
    pub wasm_compatible: bool,
    pub wasm_notes: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HostRequestEnvelopeWire {
    pub schema_version: u32,
    pub request_id: String,
    pub deadline: HostDeadlineWire,
    pub actor: HostActorWire,
    pub operation: HostOperationSelectorWire,
    #[serde(default)]
    pub declared_capabilities: Vec<String>,
    pub workspace: HostWorkspaceIdentityWire,
    pub environment: HostEnvironmentPolicyWire,
    pub manifest: Option<HostManifestWire>,
    #[serde(default)]
    pub payload: JsonValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostResponseStatusWire {
    Ok,
    Error,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostLogLevelWire {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostLogRecordWire {
    pub level: HostLogLevelWire,
    pub message: String,
    pub target: Option<String>,
    pub stream: Option<String>,
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostResourceUsageWire {
    pub wall_ms: u64,
    pub cpu_ms: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
    pub spawned_processes: u32,
    pub network_requests: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HostResponseEnvelopeWire {
    pub schema_version: u32,
    pub request_id: String,
    pub status: HostResponseStatusWire,
    #[serde(default)]
    pub result: JsonValue,
    pub error: Option<HostErrorWire>,
    #[serde(default)]
    pub logs: Vec<HostLogRecordWire>,
    pub duration_ms: u64,
    pub resource_usage: Option<HostResourceUsageWire>,
    #[serde(default)]
    pub side_effects: Vec<HostSideEffectIntentWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostErrorCodeWire {
    HostUnavailable,
    HostTimeout,
    HostCancelled,
    PluginNotFound,
    OperationUnsupported,
    CapabilityDenied,
    NetworkDenied,
    ResourceLimitExceeded,
    InvalidSideEffectIntent,
    HostProtocolError,
    ProviderExecutionFailed,
    ManifestInvalid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HostErrorWire {
    pub schema_version: u32,
    pub code: HostErrorCodeWire,
    pub message: String,
    pub retryable: bool,
    pub target: Option<String>,
    pub details: Option<JsonValue>,
    pub fallback: HostFallbackWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostFallbackWire {
    pub available: bool,
    pub reason: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "data")]
pub enum HostSideEffectIntentWire {
    FileWrite {
        root: String,
        path: String,
        content_sha256: String,
    },
    FileDelete {
        root: String,
        path: String,
    },
    FileMove {
        root: String,
        from_path: String,
        to_path: String,
    },
    ProcessSpawn {
        command: Vec<String>,
        cwd: Option<String>,
    },
    ProcessKill {
        pid: u32,
    },
    VcsMutation {
        provider: String,
        operation: String,
        workspace_dir: String,
    },
    NetworkRequest {
        method: String,
        url: String,
    },
    NotificationMutation {
        operation: String,
        payload: JsonValue,
    },
    WorkflowStateTransition {
        workflow_id: String,
        from: Option<String>,
        to: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostValidationPolicy {
    pub max_request_bytes: usize,
    pub min_timeout_ms: u64,
    pub max_timeout_ms: u64,
    pub allowed_capabilities: BTreeSet<String>,
    pub allowed_operation_families: BTreeSet<String>,
}

impl Default for HostValidationPolicy {
    fn default() -> Self {
        Self {
            max_request_bytes: 1_048_576,
            min_timeout_ms: 1,
            max_timeout_ms: 300_000,
            allowed_capabilities: [HOST_CAP_IPC_V1, HOST_CAP_MANIFEST_V1]
                .into_iter()
                .map(str::to_string)
                .collect(),
            allowed_operation_families: HOST_OPERATION_FAMILIES
                .into_iter()
                .map(str::to_string)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct HostValidationError {
    pub code: HostErrorCodeWire,
    pub message: String,
    pub target: Option<String>,
}

impl HostValidationError {
    pub fn to_error_wire(&self) -> HostErrorWire {
        HostErrorWire {
            schema_version: PROVIDER_HOST_IPC_WIRE_SCHEMA_VERSION,
            code: self.code.clone(),
            message: self.message.clone(),
            retryable: false,
            target: self.target.clone(),
            details: None,
            fallback: HostFallbackWire {
                available: true,
                reason: Some("direct_python_provider_path".to_string()),
                message: Some(
                    "host IPC validation failed before provider routing"
                        .to_string(),
                ),
            },
        }
    }
}

pub fn validate_host_request(
    request: &HostRequestEnvelopeWire,
    policy: &HostValidationPolicy,
) -> Result<(), HostValidationError> {
    if request.schema_version != PROVIDER_HOST_IPC_WIRE_SCHEMA_VERSION {
        return Err(validation_error(
            HostErrorCodeWire::HostProtocolError,
            format!(
                "host request schema mismatch: got {}, expected {}",
                request.schema_version, PROVIDER_HOST_IPC_WIRE_SCHEMA_VERSION
            ),
            Some("schema_version"),
        ));
    }
    if request.request_id.trim().is_empty() {
        return Err(validation_error(
            HostErrorCodeWire::HostProtocolError,
            "host request_id must not be empty",
            Some("request_id"),
        ));
    }
    validate_operation_family(&request.operation.family, policy)?;
    if request.operation.operation.trim().is_empty() {
        return Err(validation_error(
            HostErrorCodeWire::OperationUnsupported,
            "host operation name must not be empty",
            Some("operation.operation"),
        ));
    }
    validate_request_size(request, policy)?;
    validate_timeout_bounds(&request.deadline, policy)?;
    validate_declared_capabilities(&request.declared_capabilities, policy)?;
    if let Some(manifest) = &request.manifest {
        validate_manifest_for_request(manifest, request)?;
    }
    Ok(())
}

pub fn validate_operation_family(
    family: &str,
    policy: &HostValidationPolicy,
) -> Result<(), HostValidationError> {
    if !policy.allowed_operation_families.contains(family) {
        return Err(validation_error(
            HostErrorCodeWire::OperationUnsupported,
            format!("unsupported host operation family: {family}"),
            Some("operation.family"),
        ));
    }
    Ok(())
}

pub fn validate_request_size(
    request: &HostRequestEnvelopeWire,
    policy: &HostValidationPolicy,
) -> Result<(), HostValidationError> {
    let size = serde_json::to_vec(request)
        .map_err(|_| {
            validation_error(
                HostErrorCodeWire::HostProtocolError,
                "host request could not be encoded for size validation",
                Some("request"),
            )
        })?
        .len();
    if size > policy.max_request_bytes {
        return Err(validation_error(
            HostErrorCodeWire::ResourceLimitExceeded,
            format!(
                "host request payload is {size} bytes, max is {}",
                policy.max_request_bytes
            ),
            Some("payload"),
        ));
    }
    Ok(())
}

pub fn validate_timeout_bounds(
    deadline: &HostDeadlineWire,
    policy: &HostValidationPolicy,
) -> Result<(), HostValidationError> {
    let Some(timeout_ms) = deadline.timeout_ms else {
        return Ok(());
    };
    if timeout_ms < policy.min_timeout_ms || timeout_ms > policy.max_timeout_ms
    {
        return Err(validation_error(
            HostErrorCodeWire::ResourceLimitExceeded,
            format!(
                "host timeout {timeout_ms}ms is outside {}..={}ms",
                policy.min_timeout_ms, policy.max_timeout_ms
            ),
            Some("deadline.timeout_ms"),
        ));
    }
    Ok(())
}

pub fn validate_declared_capabilities(
    declared: &[String],
    policy: &HostValidationPolicy,
) -> Result<(), HostValidationError> {
    for capability in declared {
        if !policy.allowed_capabilities.contains(capability) {
            return Err(validation_error(
                HostErrorCodeWire::CapabilityDenied,
                format!(
                    "host capability is not currently advertised: {capability}"
                ),
                Some("declared_capabilities"),
            ));
        }
    }
    Ok(())
}

pub fn validate_side_effect_intent(
    intent: &HostSideEffectIntentWire,
) -> Result<(), HostValidationError> {
    match intent {
        HostSideEffectIntentWire::FileWrite {
            root,
            path,
            content_sha256,
        } => {
            require_nonempty(root, "side_effect.root")?;
            require_relative_path(path, "side_effect.path")?;
            if content_sha256.len() != 64
                || !content_sha256.chars().all(|c| c.is_ascii_hexdigit())
            {
                return Err(invalid_intent(
                    "file write content_sha256 must be a 64-character hex digest",
                    "side_effect.content_sha256",
                ));
            }
        }
        HostSideEffectIntentWire::FileDelete { root, path } => {
            require_nonempty(root, "side_effect.root")?;
            require_relative_path(path, "side_effect.path")?;
        }
        HostSideEffectIntentWire::FileMove {
            root,
            from_path,
            to_path,
        } => {
            require_nonempty(root, "side_effect.root")?;
            require_relative_path(from_path, "side_effect.from_path")?;
            require_relative_path(to_path, "side_effect.to_path")?;
        }
        HostSideEffectIntentWire::ProcessSpawn { command, .. } => {
            if command.is_empty() || command[0].trim().is_empty() {
                return Err(invalid_intent(
                    "process spawn command must not be empty",
                    "side_effect.command",
                ));
            }
        }
        HostSideEffectIntentWire::ProcessKill { pid } => {
            if *pid == 0 {
                return Err(invalid_intent(
                    "process kill pid must not be zero",
                    "side_effect.pid",
                ));
            }
        }
        HostSideEffectIntentWire::VcsMutation {
            provider,
            operation,
            workspace_dir,
        } => {
            require_nonempty(provider, "side_effect.provider")?;
            require_nonempty(operation, "side_effect.operation")?;
            require_nonempty(workspace_dir, "side_effect.workspace_dir")?;
        }
        HostSideEffectIntentWire::NetworkRequest { method, url } => {
            require_nonempty(method, "side_effect.method")?;
            if !(url.starts_with("https://") || url.starts_with("http://")) {
                return Err(invalid_intent(
                    "network request url must be http(s)",
                    "side_effect.url",
                ));
            }
        }
        HostSideEffectIntentWire::NotificationMutation {
            operation, ..
        } => {
            require_nonempty(operation, "side_effect.operation")?;
        }
        HostSideEffectIntentWire::WorkflowStateTransition {
            workflow_id,
            to,
            ..
        } => {
            require_nonempty(workflow_id, "side_effect.workflow_id")?;
            require_nonempty(to, "side_effect.to")?;
        }
    }
    Ok(())
}

pub trait ProviderHostTransport {
    fn send(
        &self,
        request: HostRequestEnvelopeWire,
    ) -> Result<HostResponseEnvelopeWire, HostValidationError>;
}

#[derive(Debug, Clone)]
pub struct FakeProviderHostTransport {
    policy: HostValidationPolicy,
    result: JsonValue,
}

impl FakeProviderHostTransport {
    pub fn new(result: JsonValue) -> Self {
        Self {
            policy: HostValidationPolicy::default(),
            result,
        }
    }

    pub fn with_policy(
        policy: HostValidationPolicy,
        result: JsonValue,
    ) -> Self {
        Self { policy, result }
    }
}

impl ProviderHostTransport for FakeProviderHostTransport {
    fn send(
        &self,
        request: HostRequestEnvelopeWire,
    ) -> Result<HostResponseEnvelopeWire, HostValidationError> {
        validate_host_request(&request, &self.policy)?;
        Ok(HostResponseEnvelopeWire {
            schema_version: PROVIDER_HOST_IPC_WIRE_SCHEMA_VERSION,
            request_id: request.request_id,
            status: HostResponseStatusWire::Ok,
            result: self.result.clone(),
            error: None,
            logs: Vec::new(),
            duration_ms: 0,
            resource_usage: None,
            side_effects: Vec::new(),
        })
    }
}

fn validate_manifest_for_request(
    manifest: &HostManifestWire,
    request: &HostRequestEnvelopeWire,
) -> Result<(), HostValidationError> {
    if manifest.schema_version != PROVIDER_HOST_IPC_WIRE_SCHEMA_VERSION {
        return Err(validation_error(
            HostErrorCodeWire::ManifestInvalid,
            format!(
                "host manifest schema mismatch: got {}, expected {}",
                manifest.schema_version, PROVIDER_HOST_IPC_WIRE_SCHEMA_VERSION
            ),
            Some("manifest.schema_version"),
        ));
    }
    if manifest.plugin_id.trim().is_empty() {
        return Err(validation_error(
            HostErrorCodeWire::ManifestInvalid,
            "host manifest plugin_id must not be empty",
            Some("manifest.plugin_id"),
        ));
    }
    if !manifest.operation_families.is_empty()
        && !manifest
            .operation_families
            .iter()
            .any(|family| family == &request.operation.family)
    {
        return Err(validation_error(
            HostErrorCodeWire::OperationUnsupported,
            format!(
                "manifest does not declare operation family {}",
                request.operation.family
            ),
            Some("manifest.operation_families"),
        ));
    }
    Ok(())
}

fn require_nonempty(
    value: &str,
    target: &'static str,
) -> Result<(), HostValidationError> {
    if value.trim().is_empty() {
        return Err(invalid_intent(
            format!("{target} must not be empty"),
            target,
        ));
    }
    Ok(())
}

fn require_relative_path(
    value: &str,
    target: &'static str,
) -> Result<(), HostValidationError> {
    if value.trim().is_empty() || value.starts_with('/') || value.contains("..")
    {
        return Err(invalid_intent(
            format!("{target} must be a non-empty relative path"),
            target,
        ));
    }
    Ok(())
}

fn invalid_intent(
    message: impl Into<String>,
    target: &'static str,
) -> HostValidationError {
    validation_error(
        HostErrorCodeWire::InvalidSideEffectIntent,
        message,
        Some(target),
    )
}

fn validation_error(
    code: HostErrorCodeWire,
    message: impl Into<String>,
    target: Option<&str>,
) -> HostValidationError {
    HostValidationError {
        code,
        message: message.into(),
        target: target.map(str::to_string),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn request() -> HostRequestEnvelopeWire {
        HostRequestEnvelopeWire {
            schema_version: PROVIDER_HOST_IPC_WIRE_SCHEMA_VERSION,
            request_id: "host_req_1".to_string(),
            deadline: HostDeadlineWire {
                timeout_ms: Some(30_000),
                deadline_unix_ms: None,
                cancellation_token: Some("cancel_1".to_string()),
            },
            actor: HostActorWire {
                schema_version: PROVIDER_HOST_IPC_WIRE_SCHEMA_VERSION,
                actor_type: "test".to_string(),
                name: "provider-host-test".to_string(),
                version: Some("0.1.0".to_string()),
                runtime: Some("rust".to_string()),
            },
            operation: HostOperationSelectorWire {
                family: "xprompt".to_string(),
                operation: "xprompt.catalog".to_string(),
            },
            declared_capabilities: vec![
                HOST_CAP_IPC_V1.to_string(),
                HOST_CAP_MANIFEST_V1.to_string(),
            ],
            workspace: HostWorkspaceIdentityWire {
                project_id: "project-a".to_string(),
                project_dir: Some("/tmp/project-a".to_string()),
                workspace_dir: Some("/tmp/project-a".to_string()),
                changespec: None,
            },
            environment: HostEnvironmentPolicyWire {
                inherit: false,
                allow: vec!["PATH".to_string()],
                deny: vec!["OPENAI_API_KEY".to_string()],
                required: Vec::new(),
            },
            manifest: None,
            payload: json!({"limit": 10}),
        }
    }

    #[test]
    fn host_request_round_trips_json_shape() {
        let value = serde_json::to_value(request()).unwrap();
        assert_eq!(value["schema_version"], json!(1));
        assert_eq!(value["operation"]["family"], json!("xprompt"));
        let decoded: HostRequestEnvelopeWire =
            serde_json::from_value(value).unwrap();
        assert_eq!(decoded.request_id, "host_req_1");
    }

    #[test]
    fn validation_rejects_unadvertised_capability_with_fallback_error() {
        let mut request = request();
        request
            .declared_capabilities
            .push(HOST_CAP_LLM_METADATA.to_string());
        let err =
            validate_host_request(&request, &HostValidationPolicy::default())
                .unwrap_err();
        assert_eq!(err.code, HostErrorCodeWire::CapabilityDenied);
        let wire = err.to_error_wire();
        assert!(wire.fallback.available);
    }

    #[test]
    fn validation_rejects_oversized_request() {
        let mut policy = HostValidationPolicy::default();
        policy.max_request_bytes = 64;
        let err = validate_host_request(&request(), &policy).unwrap_err();
        assert_eq!(err.code, HostErrorCodeWire::ResourceLimitExceeded);
    }

    #[test]
    fn validation_rejects_invalid_side_effect_shape() {
        let intent = HostSideEffectIntentWire::FileDelete {
            root: "workspace".to_string(),
            path: "../secret".to_string(),
        };
        let err = validate_side_effect_intent(&intent).unwrap_err();
        assert_eq!(err.code, HostErrorCodeWire::InvalidSideEffectIntent);
    }

    #[test]
    fn fake_transport_validates_and_echoes_request_id() {
        let response = FakeProviderHostTransport::new(json!({"ok": true}))
            .send(request())
            .unwrap();
        assert_eq!(response.request_id, "host_req_1");
        assert_eq!(response.status, HostResponseStatusWire::Ok);
        assert_eq!(response.result, json!({"ok": true}));
    }
}
