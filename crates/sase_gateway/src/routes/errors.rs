use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

use crate::fleet_attention::FleetAttentionStoreError;

use crate::fleet_auth::FleetStoreError;

use crate::fleet_launch::FleetLaunchStoreError;

use crate::fleet_mutations::FleetMutationStoreError;

use crate::fleet_reads::FleetReadError;

use crate::host_bridge::HostBridgeError;

use crate::storage::StoreError;

use crate::wire::{
    ApiErrorCodeWire, ApiErrorWire, GATEWAY_WIRE_SCHEMA_VERSION,
};

#[derive(Debug)]
pub(crate) struct ApiError {
    pub(crate) status: StatusCode,
    pub(crate) wire: Box<ApiErrorWire>,
}

impl ApiError {
    pub(crate) fn unauthorized(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::Unauthorized,
                message: "authentication is required for this endpoint"
                    .to_string(),
                target: Some(target.into()),
                details: None,
            }),
        }
    }

    pub(crate) fn not_found(path: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::NotFound,
                message: "route not found".to_string(),
                target: Some(path.into()),
                details: None,
            }),
        }
    }

    pub(crate) fn notification_not_found(id: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::NotFound,
                message: "notification not found".to_string(),
                target: Some(id.into()),
                details: None,
            }),
        }
    }

    pub(crate) fn push_subscription_not_found(id: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::NotFound,
                message: "push subscription not found".to_string(),
                target: Some(id.into()),
                details: None,
            }),
        }
    }

    pub(crate) fn invalid_request(
        target: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::InvalidRequest,
                message: message.into(),
                target: Some(target.into()),
                details: None,
            }),
        }
    }

    pub(crate) fn pairing_expired(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::PairingExpired,
                message: "pairing code expired".to_string(),
                target: Some(target.into()),
                details: None,
            }),
        }
    }

    pub(crate) fn pairing_rejected(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::PairingRejected,
                message: "pairing code rejected".to_string(),
                target: Some(target.into()),
                details: None,
            }),
        }
    }

    pub(crate) fn attachment_expired(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::GONE,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::AttachmentExpired,
                message: "attachment token is expired".to_string(),
                target: Some(target.into()),
                details: None,
            }),
        }
    }

    pub(crate) fn internal(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::Internal,
                message: "gateway internal error".to_string(),
                target: Some(target.into()),
                details: None,
            }),
        }
    }

    pub(crate) fn from_store(error: StoreError) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::Internal,
                message: error.to_string(),
                target: Some("device_store".to_string()),
                details: None,
            }),
        }
    }

    pub(crate) fn bootstrap_consumed(target: impl Into<String>) -> Self {
        Self::fleet_error(
            StatusCode::CONFLICT,
            ApiErrorCodeWire::BootstrapConsumed,
            "bootstrap secret was already used",
            target,
        )
    }

    pub(crate) fn bootstrap_expired(target: impl Into<String>) -> Self {
        Self::fleet_error(
            StatusCode::BAD_REQUEST,
            ApiErrorCodeWire::BootstrapExpired,
            "bootstrap secret is expired",
            target,
        )
    }

    pub(crate) fn bootstrap_rejected(target: impl Into<String>) -> Self {
        Self::fleet_error(
            StatusCode::UNAUTHORIZED,
            ApiErrorCodeWire::BootstrapRejected,
            "bootstrap secret was rejected",
            target,
        )
    }

    pub(crate) fn credential_expired(target: impl Into<String>) -> Self {
        Self::fleet_error(
            StatusCode::UNAUTHORIZED,
            ApiErrorCodeWire::CredentialExpired,
            "fleet credential is expired",
            target,
        )
    }

    pub(crate) fn credential_revoked(target: impl Into<String>) -> Self {
        Self::fleet_error(
            StatusCode::UNAUTHORIZED,
            ApiErrorCodeWire::CredentialRevoked,
            "fleet credential is revoked",
            target,
        )
    }

    pub(crate) fn incompatible_protocol(target: impl Into<String>) -> Self {
        Self::fleet_error(
            StatusCode::UPGRADE_REQUIRED,
            ApiErrorCodeWire::IncompatibleProtocol,
            "no mutually supported fleet protocol version",
            target,
        )
    }

    pub(crate) fn payload_too_large(target: impl Into<String>) -> Self {
        Self::fleet_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            ApiErrorCodeWire::PayloadTooLarge,
            "fleet request body exceeds the configured limit",
            target,
        )
    }

    pub(crate) fn rate_limited(target: impl Into<String>) -> Self {
        Self::fleet_error(
            StatusCode::TOO_MANY_REQUESTS,
            ApiErrorCodeWire::RateLimited,
            "too many fleet enrollment attempts",
            target,
        )
    }

    pub(crate) fn scope_denied(scope: impl Into<String>) -> Self {
        Self::fleet_error(
            StatusCode::FORBIDDEN,
            ApiErrorCodeWire::ScopeDenied,
            "fleet credential does not include the required scope",
            scope,
        )
    }

    pub(crate) fn fleet_error(
        status: StatusCode,
        code: ApiErrorCodeWire,
        message: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        Self {
            status,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code,
                message: message.into(),
                target: Some(target.into()),
                details: None,
            }),
        }
    }

    pub(crate) fn from_fleet_store(error: FleetStoreError) -> Self {
        match error {
            FleetStoreError::BootstrapConsumed => {
                Self::bootstrap_consumed("bootstrap_secret")
            }
            FleetStoreError::BootstrapExpired => {
                Self::bootstrap_expired("bootstrap_secret")
            }
            FleetStoreError::BootstrapRejected => {
                Self::bootstrap_rejected("bootstrap_secret")
            }
            FleetStoreError::CredentialExpired => {
                Self::credential_expired("authorization")
            }
            FleetStoreError::CredentialMissing => {
                Self::unauthorized("authorization")
            }
            FleetStoreError::CredentialRevoked => {
                Self::credential_revoked("authorization")
            }
            FleetStoreError::IncompatibleProtocol => {
                Self::incompatible_protocol("supported_protocol_versions")
            }
            FleetStoreError::ScopeDenied(scope) => Self::scope_denied(scope),
            FleetStoreError::Validation(message) => {
                Self::invalid_request("fleet_request", message)
            }
            FleetStoreError::LockPoisoned
            | FleetStoreError::Io { .. }
            | FleetStoreError::Json { .. }
            | FleetStoreError::FleetContract(_) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                wire: Box::new(ApiErrorWire {
                    schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                    code: ApiErrorCodeWire::Internal,
                    message: error.to_string(),
                    target: Some("fleet_store".to_string()),
                    details: None,
                }),
            },
        }
    }

    pub(crate) fn from_fleet_mutation_store(
        error: FleetMutationStoreError,
    ) -> Self {
        match error {
            FleetMutationStoreError::Validation(message) => {
                Self::invalid_request("fleet_mutate", message)
            }
            FleetMutationStoreError::Conflict(message) => Self::fleet_error(
                StatusCode::CONFLICT,
                ApiErrorCodeWire::InvalidRequest,
                message,
                "fleet_mutate",
            ),
            FleetMutationStoreError::Expired(message) => Self::fleet_error(
                StatusCode::GONE,
                ApiErrorCodeWire::GoneStale,
                message,
                "fleet_mutate",
            ),
            FleetMutationStoreError::Io { .. }
            | FleetMutationStoreError::Json { .. } => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                wire: Box::new(ApiErrorWire {
                    schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                    code: ApiErrorCodeWire::Internal,
                    message: error.to_string(),
                    target: Some("fleet_mutation_store".to_string()),
                    details: None,
                }),
            },
        }
    }

    pub(crate) fn from_mutation_precondition(
        decision: &sase_core::FleetMutationPreconditionDecisionWire,
    ) -> Self {
        use sase_core::FleetMutationPreconditionReasonWire;
        match decision.reason {
            FleetMutationPreconditionReasonWire::Ok => {
                Self::invalid_request("fleet_mutate", "precondition unexpectedly allowed")
            }
            FleetMutationPreconditionReasonWire::UnknownRow => Self::fleet_error(
                StatusCode::NOT_FOUND,
                ApiErrorCodeWire::AgentNotFound,
                "fleet mutation target row was not found",
                "intent.target",
            ),
            FleetMutationPreconditionReasonWire::InstanceMismatch => {
                Self::fleet_error(
                    StatusCode::CONFLICT,
                    ApiErrorCodeWire::ConflictAlreadyHandled,
                    "fleet mutation target instance does not match the current run",
                    "intent.target",
                )
            }
            FleetMutationPreconditionReasonWire::StaleRevision => Self::fleet_stale(
                "intent.row_revision",
            ),
            FleetMutationPreconditionReasonWire::CapabilityMissing => {
                Self::fleet_error(
                    StatusCode::FORBIDDEN,
                    ApiErrorCodeWire::UnsupportedAction,
                    format!(
                        "missing lifecycle capability {}",
                        decision.required_capability
                    ),
                    "capabilities",
                )
            }
            FleetMutationPreconditionReasonWire::AlreadyTerminal => {
                Self::fleet_error(
                    StatusCode::CONFLICT,
                    ApiErrorCodeWire::AgentNotRunning,
                    "fleet mutation target is already terminal",
                    "intent.target",
                )
            }
        }
    }

    pub(crate) fn from_fleet_attention_store(
        error: FleetAttentionStoreError,
    ) -> Self {
        match error {
            FleetAttentionStoreError::Validation(message) => {
                Self::invalid_request("fleet_attention", message)
            }
            FleetAttentionStoreError::Conflict(message) => Self::fleet_error(
                StatusCode::CONFLICT,
                ApiErrorCodeWire::InvalidRequest,
                message,
                "fleet_attention",
            ),
            FleetAttentionStoreError::Expired(message) => Self::fleet_error(
                StatusCode::GONE,
                ApiErrorCodeWire::GoneStale,
                message,
                "fleet_attention",
            ),
            FleetAttentionStoreError::Io { .. }
            | FleetAttentionStoreError::Json { .. } => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                wire: Box::new(ApiErrorWire {
                    schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                    code: ApiErrorCodeWire::Internal,
                    message: error.to_string(),
                    target: Some("fleet_attention_store".to_string()),
                    details: None,
                }),
            },
        }
    }

    pub(crate) fn from_fleet_launch_store(
        error: FleetLaunchStoreError,
    ) -> Self {
        match error {
            FleetLaunchStoreError::Validation(message) => {
                Self::invalid_request("fleet_launch", message)
            }
            FleetLaunchStoreError::Conflict(message) => Self::fleet_error(
                StatusCode::CONFLICT,
                ApiErrorCodeWire::InvalidRequest,
                message,
                "fleet_launch",
            ),
            FleetLaunchStoreError::Expired(message) => Self::fleet_error(
                StatusCode::GONE,
                ApiErrorCodeWire::GoneStale,
                message,
                "fleet_launch",
            ),
            FleetLaunchStoreError::Io { .. }
            | FleetLaunchStoreError::Json { .. } => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                wire: Box::new(ApiErrorWire {
                    schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                    code: ApiErrorCodeWire::Internal,
                    message: error.to_string(),
                    target: Some("fleet_launch_store".to_string()),
                    details: None,
                }),
            },
        }
    }

    pub(crate) fn fleet_timeout(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::GATEWAY_TIMEOUT,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::Timeout,
                message: "fleet read timed out".to_string(),
                target: Some(target.into()),
                details: None,
            }),
        }
    }

    pub(crate) fn fleet_stale(target: impl Into<String>) -> Self {
        Self {
            status: StatusCode::GONE,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::GoneStale,
                message: "fleet resource is stale".to_string(),
                target: Some(target.into()),
                details: None,
            }),
        }
    }

    pub(crate) fn from_fleet_read(error: FleetReadError) -> Self {
        match error {
            FleetReadError::Validation(message) => {
                Self::invalid_request("fleet_request", message)
            }
            FleetReadError::NotFound(target) => Self {
                status: StatusCode::NOT_FOUND,
                wire: Box::new(ApiErrorWire {
                    schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                    code: ApiErrorCodeWire::NotFound,
                    message: "fleet resource not found".to_string(),
                    target: Some(target),
                    details: None,
                }),
            },
            FleetReadError::Stale(target) => Self::fleet_stale(target),
            FleetReadError::Timeout(target) => Self::fleet_timeout(target),
            FleetReadError::Backend(target) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                wire: Box::new(ApiErrorWire {
                    schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                    code: ApiErrorCodeWire::Internal,
                    message: "fleet backend failed".to_string(),
                    target: Some(target),
                    details: None,
                }),
            },
        }
    }

    pub(crate) fn from_host_bridge(error: HostBridgeError) -> Self {
        let (status, code, target) = match &error {
            HostBridgeError::BridgeUnavailable(target) => (
                StatusCode::SERVICE_UNAVAILABLE,
                ApiErrorCodeWire::BridgeUnavailable,
                target.clone(),
            ),
            HostBridgeError::AgentNotFound(target) => (
                StatusCode::NOT_FOUND,
                ApiErrorCodeWire::AgentNotFound,
                target.clone(),
            ),
            HostBridgeError::AgentNotRunning(target) => (
                StatusCode::CONFLICT,
                ApiErrorCodeWire::AgentNotRunning,
                target.clone(),
            ),
            HostBridgeError::LaunchFailed(target) => (
                StatusCode::BAD_GATEWAY,
                ApiErrorCodeWire::LaunchFailed,
                target.clone(),
            ),
            HostBridgeError::InvalidUpload(target) => (
                StatusCode::BAD_REQUEST,
                ApiErrorCodeWire::InvalidUpload,
                target.clone(),
            ),
            HostBridgeError::PermissionDenied(target) => (
                StatusCode::FORBIDDEN,
                ApiErrorCodeWire::PermissionDenied,
                target.clone(),
            ),
            HostBridgeError::HelperNotFound(target) => (
                StatusCode::NOT_FOUND,
                ApiErrorCodeWire::HelperNotFound,
                target.clone(),
            ),
            HostBridgeError::UpdateAlreadyRunning(target) => (
                StatusCode::CONFLICT,
                ApiErrorCodeWire::UpdateAlreadyRunning,
                target.clone(),
            ),
            HostBridgeError::UpdateJobNotFound(target) => (
                StatusCode::NOT_FOUND,
                ApiErrorCodeWire::UpdateJobNotFound,
                target.clone(),
            ),
            HostBridgeError::NotificationMissing(target) => (
                StatusCode::NOT_FOUND,
                ApiErrorCodeWire::NotFound,
                target.clone(),
            ),
            HostBridgeError::ActionMissing(target) => (
                StatusCode::NOT_FOUND,
                ApiErrorCodeWire::NotFound,
                target.clone(),
            ),
            HostBridgeError::AmbiguousPrefix(target) => (
                StatusCode::CONFLICT,
                ApiErrorCodeWire::AmbiguousPrefix,
                target.clone(),
            ),
            HostBridgeError::UnsupportedAction(target) => (
                StatusCode::BAD_REQUEST,
                ApiErrorCodeWire::UnsupportedAction,
                target.clone(),
            ),
            HostBridgeError::ActionAlreadyHandled(target) => (
                StatusCode::CONFLICT,
                ApiErrorCodeWire::ConflictAlreadyHandled,
                target.clone(),
            ),
            HostBridgeError::ActionStale(target) => (
                StatusCode::GONE,
                ApiErrorCodeWire::GoneStale,
                target.clone(),
            ),
            HostBridgeError::MissingTarget(target)
            | HostBridgeError::InvalidActionRequest(target) => (
                StatusCode::BAD_REQUEST,
                ApiErrorCodeWire::InvalidRequest,
                target.clone(),
            ),
            HostBridgeError::ReadNotifications(_)
            | HostBridgeError::ReadPendingActions(_)
            | HostBridgeError::WriteResponse(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiErrorCodeWire::Internal,
                "notification_bridge".to_string(),
            ),
        };
        Self {
            status,
            wire: Box::new(ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code,
                message: error.to_string(),
                target: Some(target),
                details: None,
            }),
        }
    }

    pub(crate) fn from_json_rejection(rejection: JsonRejection) -> Self {
        Self::invalid_request("body", rejection.body_text())
    }

    pub(crate) fn from_fleet_json_rejection(rejection: JsonRejection) -> Self {
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            Self::payload_too_large("body")
        } else {
            Self::invalid_request("body", rejection.body_text())
        }
    }
}

impl ApiErrorCodeWire {
    pub(crate) fn outcome_label(&self) -> &'static str {
        match self {
            ApiErrorCodeWire::Unauthorized => "unauthorized",
            ApiErrorCodeWire::NotFound => "not_found",
            ApiErrorCodeWire::InvalidRequest => "invalid_request",
            ApiErrorCodeWire::PairingExpired => "pairing_expired",
            ApiErrorCodeWire::PairingRejected => "pairing_rejected",
            ApiErrorCodeWire::ConflictAlreadyHandled => "already_handled",
            ApiErrorCodeWire::GoneStale => "stale",
            ApiErrorCodeWire::AmbiguousPrefix => "ambiguous_prefix",
            ApiErrorCodeWire::UnsupportedAction => "unsupported_action",
            ApiErrorCodeWire::AttachmentExpired => "attachment_expired",
            ApiErrorCodeWire::AgentNotFound => "agent_not_found",
            ApiErrorCodeWire::AgentNotRunning => "agent_not_running",
            ApiErrorCodeWire::LaunchFailed => "launch_failed",
            ApiErrorCodeWire::InvalidUpload => "invalid_upload",
            ApiErrorCodeWire::BridgeUnavailable => "bridge_unavailable",
            ApiErrorCodeWire::HelperNotFound => "helper_not_found",
            ApiErrorCodeWire::UpdateAlreadyRunning => "update_already_running",
            ApiErrorCodeWire::UpdateJobNotFound => "update_job_not_found",
            ApiErrorCodeWire::PermissionDenied => "permission_denied",
            ApiErrorCodeWire::BootstrapConsumed => "bootstrap_consumed",
            ApiErrorCodeWire::BootstrapExpired => "bootstrap_expired",
            ApiErrorCodeWire::BootstrapRejected => "bootstrap_rejected",
            ApiErrorCodeWire::CredentialExpired => "credential_expired",
            ApiErrorCodeWire::CredentialRevoked => "credential_revoked",
            ApiErrorCodeWire::IncompatibleProtocol => "incompatible_protocol",
            ApiErrorCodeWire::InstallationPinMismatch => {
                "installation_pin_mismatch"
            }
            ApiErrorCodeWire::PayloadTooLarge => "payload_too_large",
            ApiErrorCodeWire::RateLimited => "rate_limited",
            ApiErrorCodeWire::ScopeDenied => "scope_denied",
            ApiErrorCodeWire::Timeout => "timeout",
            ApiErrorCodeWire::ResyncRequired => "resync_required",
            ApiErrorCodeWire::Internal => "internal",
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(*self.wire)).into_response()
    }
}
