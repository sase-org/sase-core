use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::Utc;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use thiserror::Error;

use crate::{
    storage::{format_time, AuditLogEntryWire, DeviceTokenStore},
    wire::{
        push_hint_from_event, EventRecordWire, PushGatewayStatusWire,
        PushHintCategoryWire, PushHintWire, PushProviderWire,
        PushSubscriptionRecordWire, GATEWAY_WIRE_SCHEMA_VERSION,
    },
};

const FCM_SCOPE: &str = "https://www.googleapis.com/auth/firebase.messaging";
const GOOGLE_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PushProviderMode {
    Disabled,
    Test,
    Fcm,
}

impl PushProviderMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Test => "test",
            Self::Fcm => "fcm",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushConfig {
    pub provider: PushProviderMode,
    pub fcm_project_id: Option<String>,
    pub fcm_service_account_json_path: Option<PathBuf>,
    pub fcm_credential_env: Option<String>,
    pub fcm_endpoint: Option<String>,
    pub fcm_dry_run: bool,
    pub timeout: Duration,
    pub retry_limit: u32,
}

impl Default for PushConfig {
    fn default() -> Self {
        Self {
            provider: PushProviderMode::Disabled,
            fcm_project_id: None,
            fcm_service_account_json_path: None,
            fcm_credential_env: None,
            fcm_endpoint: None,
            fcm_dry_run: false,
            timeout: Duration::from_secs(5),
            retry_limit: 1,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PushDispatcher {
    config: Arc<PushConfig>,
    client: reqwest::Client,
    diagnostics: Arc<Mutex<PushDiagnostics>>,
    test_attempts: Arc<Mutex<Vec<PushDeliveryAttempt>>>,
}

impl PushDispatcher {
    pub fn new(config: PushConfig) -> Self {
        let timeout = config.timeout;
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            config: Arc::new(config),
            client,
            diagnostics: Arc::new(Mutex::new(PushDiagnostics::default())),
            test_attempts: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn disabled() -> Self {
        Self::new(PushConfig::default())
    }

    pub fn status(&self) -> PushGatewayStatusWire {
        let diagnostics = self
            .diagnostics
            .lock()
            .ok()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        PushGatewayStatusWire {
            provider: self.config.provider.as_str().to_string(),
            enabled: self.config.provider != PushProviderMode::Disabled,
            attempted: diagnostics.attempted,
            succeeded: diagnostics.succeeded,
            failed: diagnostics.failed,
            last_attempt_at: diagnostics.last_attempt_at,
            last_success_at: diagnostics.last_success_at,
            last_failure_at: diagnostics.last_failure_at,
            last_failure: diagnostics.last_failure,
        }
    }

    pub fn dispatch_event(
        &self,
        store: DeviceTokenStore,
        record: &EventRecordWire,
    ) {
        if self.config.provider == PushProviderMode::Disabled {
            return;
        }
        let Some(hint) = push_hint_from_event(record) else {
            return;
        };
        let subscriptions = match store.list_all_push_subscriptions() {
            Ok(subscriptions) => subscriptions,
            Err(error) => {
                self.record_failure(format!("subscription_list:{error}"));
                store
                    .append_audit(AuditLogEntryWire {
                        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                        timestamp: format_time(Utc::now()),
                        device_id: None,
                        endpoint: "push.delivery".to_string(),
                        target_id: Some(hint.id),
                        outcome: "subscription_list_failed".to_string(),
                    })
                    .ok();
                return;
            }
        };
        for subscription in subscriptions
            .into_iter()
            .filter(|subscription| {
                subscription_matches(&self.config.provider, subscription)
            })
            .filter(|subscription| {
                subscription.hint_categories.contains(&hint.category)
            })
        {
            self.record_attempt();
            match self.config.provider {
                PushProviderMode::Disabled => {}
                PushProviderMode::Test => {
                    self.record_test_attempt(subscription, hint.clone());
                    self.record_success();
                }
                PushProviderMode::Fcm => {
                    let dispatcher = self.clone();
                    let store = store.clone();
                    let hint = hint.clone();
                    tokio::spawn(async move {
                        let outcome = dispatcher
                            .send_fcm_with_retries(&subscription, &hint)
                            .await;
                        match outcome {
                            Ok(()) => {
                                dispatcher.record_success();
                                audit_push_delivery(
                                    &store,
                                    &subscription,
                                    &hint,
                                    "success",
                                );
                            }
                            Err(error) => {
                                dispatcher.record_failure(error.to_string());
                                audit_push_delivery(
                                    &store,
                                    &subscription,
                                    &hint,
                                    "failed",
                                );
                            }
                        }
                    });
                }
            }
        }
    }

    pub fn test_attempts(&self) -> Vec<PushDeliveryAttempt> {
        self.test_attempts
            .lock()
            .ok()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    fn record_attempt(&self) {
        if let Ok(mut diagnostics) = self.diagnostics.lock() {
            diagnostics.attempted += 1;
            diagnostics.last_attempt_at = Some(format_time(Utc::now()));
        }
    }

    fn record_success(&self) {
        if let Ok(mut diagnostics) = self.diagnostics.lock() {
            diagnostics.succeeded += 1;
            diagnostics.last_success_at = Some(format_time(Utc::now()));
        }
    }

    fn record_failure(&self, error: String) {
        if let Ok(mut diagnostics) = self.diagnostics.lock() {
            diagnostics.failed += 1;
            diagnostics.last_failure_at = Some(format_time(Utc::now()));
            diagnostics.last_failure = Some(redact_push_error(&error));
        }
    }

    fn record_test_attempt(
        &self,
        subscription: PushSubscriptionRecordWire,
        hint: PushHintWire,
    ) {
        if let Ok(mut attempts) = self.test_attempts.lock() {
            attempts.push(PushDeliveryAttempt {
                subscription_id: subscription.id,
                device_id: subscription.device_id,
                provider: subscription.provider,
                hint,
            });
        }
    }

    async fn send_fcm_with_retries(
        &self,
        subscription: &PushSubscriptionRecordWire,
        hint: &PushHintWire,
    ) -> Result<(), PushDeliveryError> {
        let mut last_error = None;
        for _attempt in 0..=self.config.retry_limit {
            match self.send_fcm(subscription, hint).await {
                Ok(()) => return Ok(()),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or(PushDeliveryError::Config(
            "fcm retry loop did not run".to_string(),
        )))
    }

    async fn send_fcm(
        &self,
        subscription: &PushSubscriptionRecordWire,
        hint: &PushHintWire,
    ) -> Result<(), PushDeliveryError> {
        let project_id = self
            .config
            .fcm_project_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                PushDeliveryError::Config("missing fcm project id".to_string())
            })?;
        let access_token = self.resolve_fcm_access_token().await?;
        let url = fcm_send_url(self.config.fcm_endpoint.as_deref(), project_id);
        let response = self
            .client
            .post(url)
            .bearer_auth(access_token)
            .json(&fcm_message_body(
                subscription,
                hint,
                self.config.fcm_dry_run,
            ))
            .send()
            .await?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(PushDeliveryError::HttpStatus(response.status().as_u16()))
        }
    }

    async fn resolve_fcm_access_token(
        &self,
    ) -> Result<String, PushDeliveryError> {
        if let Some(env_name) = self.config.fcm_credential_env.as_deref() {
            let value = std::env::var(env_name).map_err(|_| {
                PushDeliveryError::Config(format!(
                    "missing credential env {env_name}"
                ))
            })?;
            if value.trim_start().starts_with('{') {
                let account: ServiceAccountKey = serde_json::from_str(&value)?;
                return self.service_account_access_token(account).await;
            }
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Err(PushDeliveryError::Config(format!(
                    "empty credential env {env_name}"
                )));
            }
            return Ok(trimmed.to_string());
        }
        if let Some(path) = self.config.fcm_service_account_json_path.as_deref()
        {
            let value = std::fs::read_to_string(path)?;
            let account: ServiceAccountKey = serde_json::from_str(&value)?;
            return self.service_account_access_token(account).await;
        }
        Err(PushDeliveryError::Config(
            "missing fcm credentials".to_string(),
        ))
    }

    async fn service_account_access_token(
        &self,
        account: ServiceAccountKey,
    ) -> Result<String, PushDeliveryError> {
        let token_uri = account
            .token_uri
            .unwrap_or_else(|| GOOGLE_TOKEN_URI.to_string());
        let now = Utc::now().timestamp();
        let claims = ServiceAccountClaims {
            iss: account.client_email,
            scope: FCM_SCOPE.to_string(),
            aud: token_uri.clone(),
            iat: now,
            exp: now + 3600,
        };
        let assertion = encode(
            &Header::new(Algorithm::RS256),
            &claims,
            &EncodingKey::from_rsa_pem(account.private_key.as_bytes())?,
        )?;
        let body = serde_urlencoded::to_string([
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", assertion.as_str()),
        ])
        .map_err(|error| PushDeliveryError::Config(error.to_string()))?;
        let response = self
            .client
            .post(token_uri)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(PushDeliveryError::HttpStatus(
                response.status().as_u16(),
            ));
        }
        let token: AccessTokenResponse = response.json().await?;
        Ok(token.access_token)
    }
}

#[derive(Clone, Debug, Default)]
struct PushDiagnostics {
    attempted: u64,
    succeeded: u64,
    failed: u64,
    last_attempt_at: Option<String>,
    last_success_at: Option<String>,
    last_failure_at: Option<String>,
    last_failure: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushDeliveryAttempt {
    pub subscription_id: String,
    pub device_id: String,
    pub provider: PushProviderWire,
    pub hint: PushHintWire,
}

#[derive(Debug, Deserialize)]
struct ServiceAccountKey {
    client_email: String,
    private_key: String,
    #[serde(default)]
    token_uri: Option<String>,
}

#[derive(Debug, Serialize)]
struct ServiceAccountClaims {
    iss: String,
    scope: String,
    aud: String,
    iat: i64,
    exp: i64,
}

#[derive(Debug, Deserialize)]
struct AccessTokenResponse {
    access_token: String,
}

#[derive(Debug, Error)]
enum PushDeliveryError {
    #[error("push config error: {0}")]
    Config(String),
    #[error("push http status: {0}")]
    HttpStatus(u16),
    #[error("push http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("push credential file error: {0}")]
    Io(#[from] std::io::Error),
    #[error("push credential json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("push credential signing error: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
}

fn subscription_matches(
    mode: &PushProviderMode,
    subscription: &PushSubscriptionRecordWire,
) -> bool {
    match mode {
        PushProviderMode::Disabled => false,
        PushProviderMode::Test => {
            subscription.provider == PushProviderWire::Test
        }
        PushProviderMode::Fcm => subscription.provider == PushProviderWire::Fcm,
    }
}

fn audit_push_delivery(
    store: &DeviceTokenStore,
    subscription: &PushSubscriptionRecordWire,
    hint: &PushHintWire,
    outcome: &str,
) {
    let _ = store.append_audit(AuditLogEntryWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        timestamp: format_time(Utc::now()),
        device_id: Some(subscription.device_id.clone()),
        endpoint: "push.delivery".to_string(),
        target_id: Some(format!("{}:{}", subscription.id, hint.id)),
        outcome: outcome.to_string(),
    });
}

fn fcm_send_url(endpoint: Option<&str>, project_id: &str) -> String {
    let base = endpoint
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("https://fcm.googleapis.com");
    format!(
        "{}/v1/projects/{}/messages:send",
        base.trim_end_matches('/'),
        project_id
    )
}

fn fcm_message_body(
    subscription: &PushSubscriptionRecordWire,
    hint: &PushHintWire,
    dry_run: bool,
) -> JsonValue {
    json!({
        "validate_only": dry_run,
        "message": {
            "token": subscription.provider_token,
            "data": fcm_data_payload(hint),
            "android": {
                "priority": "high"
            }
        }
    })
}

fn fcm_data_payload(hint: &PushHintWire) -> JsonValue {
    let mut data = serde_json::Map::new();
    data.insert(
        "schema_version".to_string(),
        json!(hint.schema_version.to_string()),
    );
    data.insert("id".to_string(), json!(hint.id));
    data.insert("created_at".to_string(), json!(hint.created_at));
    data.insert("category".to_string(), json!(category_name(&hint.category)));
    data.insert("reason".to_string(), json!(hint.reason));
    data.insert("title".to_string(), json!(hint.title));
    data.insert("body".to_string(), json!(hint.body));
    if let Some(value) = hint.notification_id.as_deref() {
        data.insert("notification_id".to_string(), json!(value));
    }
    if let Some(value) = hint.agent_name.as_deref() {
        data.insert("agent_name".to_string(), json!(value));
    }
    if let Some(value) = hint.helper.as_deref() {
        data.insert("helper".to_string(), json!(value));
    }
    if let Some(value) = hint.job_id.as_deref() {
        data.insert("job_id".to_string(), json!(value));
    }
    JsonValue::Object(data)
}

fn category_name(category: &PushHintCategoryWire) -> &'static str {
    match category {
        PushHintCategoryWire::Notifications => "notifications",
        PushHintCategoryWire::Agents => "agents",
        PushHintCategoryWire::Helpers => "helpers",
        PushHintCategoryWire::Update => "update",
        PushHintCategoryWire::Session => "session",
    }
}

fn redact_push_error(error: &str) -> String {
    let mut out = error.to_string();
    if out.len() > 160 {
        out.truncate(160);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[test]
    fn fcm_payload_contains_hint_only_data() {
        let subscription = PushSubscriptionRecordWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            id: "push_123".to_string(),
            device_id: "dev_123".to_string(),
            provider: PushProviderWire::Fcm,
            provider_token: "opaque-token".to_string(),
            app_instance_id: None,
            device_display_name: None,
            platform: Some("android".to_string()),
            app_version: None,
            hint_categories: vec![PushHintCategoryWire::Agents],
            enabled_at: "2026-05-06T14:00:00Z".to_string(),
            disabled_at: None,
            last_seen_at: None,
        };
        let hint = PushHintWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            id: "0000000000000007".to_string(),
            created_at: "2026-05-06T14:30:00Z".to_string(),
            category: PushHintCategoryWire::Agents,
            reason: "launch".to_string(),
            notification_id: None,
            agent_name: Some("safe-agent".to_string()),
            helper: None,
            job_id: None,
            title: "SASE agent update".to_string(),
            body: "Agent safe-agent changed.".to_string(),
        };

        let body = fcm_message_body(&subscription, &hint, true);

        assert_eq!(body["validate_only"], true);
        assert_eq!(body["message"]["token"], "opaque-token");
        assert_eq!(body["message"]["data"]["id"], "0000000000000007");
        assert!(body["message"]["data"]["hint_id"].is_null());
        assert_eq!(body["message"]["data"]["category"], "agents");
        assert_eq!(body["message"]["data"]["agent_name"], "safe-agent");
        let serialized = serde_json::to_string(&body).unwrap();
        assert!(!serialized.contains("prompt"));
        assert!(!serialized.contains("attachment"));
        assert!(!serialized.contains("bearer"));
    }

    #[tokio::test]
    async fn fcm_provider_posts_expected_http_v1_shape() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = vec![0_u8; 8192];
            let mut used = 0;
            loop {
                let n = stream.read(&mut buffer[used..]).await.unwrap();
                used += n;
                if used >= 4
                    && buffer[..used]
                        .windows(4)
                        .any(|chunk| chunk == b"\r\n\r\n")
                {
                    let request = String::from_utf8_lossy(&buffer[..used]);
                    let content_length = request
                        .lines()
                        .find_map(|line| {
                            line.strip_prefix("Content-Length: ")
                                .and_then(|value| value.parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    let header_end = request.find("\r\n\r\n").unwrap() + 4;
                    while used < header_end + content_length {
                        let n = stream.read(&mut buffer[used..]).await.unwrap();
                        used += n;
                    }
                    break;
                }
            }
            let request = String::from_utf8_lossy(&buffer[..used]).to_string();
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 16\r\n\r\n{\"name\":\"ok\"}\n",
                )
                .await
                .unwrap();
            request
        });
        let env_name = "SASE_GATEWAY_TEST_FCM_TOKEN";
        std::env::set_var(env_name, "test-access-token");
        let dispatcher = PushDispatcher::new(PushConfig {
            provider: PushProviderMode::Fcm,
            fcm_project_id: Some("demo-project".to_string()),
            fcm_service_account_json_path: None,
            fcm_credential_env: Some(env_name.to_string()),
            fcm_endpoint: Some(format!("http://{addr}")),
            fcm_dry_run: true,
            timeout: Duration::from_secs(2),
            retry_limit: 0,
        });
        let subscription = PushSubscriptionRecordWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            id: "push_123".to_string(),
            device_id: "dev_123".to_string(),
            provider: PushProviderWire::Fcm,
            provider_token: "device-token".to_string(),
            app_instance_id: None,
            device_display_name: None,
            platform: Some("android".to_string()),
            app_version: None,
            hint_categories: vec![PushHintCategoryWire::Notifications],
            enabled_at: "2026-05-06T14:00:00Z".to_string(),
            disabled_at: None,
            last_seen_at: None,
        };
        let hint = PushHintWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            id: "0000000000000008".to_string(),
            created_at: "2026-05-06T14:31:00Z".to_string(),
            category: PushHintCategoryWire::Notifications,
            reason: "mark_read".to_string(),
            notification_id: Some("notif_123".to_string()),
            agent_name: None,
            helper: None,
            job_id: None,
            title: "SASE notification update".to_string(),
            body: "Open SASE to view current notifications.".to_string(),
        };

        dispatcher.send_fcm(&subscription, &hint).await.unwrap();
        let request = server.await.unwrap();
        std::env::remove_var(env_name);

        assert!(request.starts_with(
            "POST /v1/projects/demo-project/messages:send HTTP/1.1"
        ));
        assert!(request.contains("authorization: Bearer test-access-token"));
        assert!(request.contains(r#""validate_only":true"#));
        assert!(request.contains(r#""token":"device-token"#));
        assert!(request.contains(r#""id":"0000000000000008"#));
        assert!(!request.contains("hint_id"));
        assert!(request.contains(r#""notification_id":"notif_123"#));
        assert!(!request.contains("pairing"));
        assert!(!request.contains("attachment_token"));
    }
}
