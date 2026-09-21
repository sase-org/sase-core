use super::*;
use reqwest::{Method, StatusCode};

#[derive(Clone)]
pub(super) struct ValidatedHostConfig {
    pub(super) alias: Option<String>,
    pub(super) plan: ConnectionPlanWire,
    bearer_token: String,
}

pub(super) fn validate_host_config(
    config: FederationHostConfigWire,
) -> Result<ValidatedHostConfig, FederationErrorWire> {
    if config.schema_version != FEDERATION_IPC_SCHEMA_VERSION {
        return Err(federation_error(
            "unsupported_version",
            "unsupported federation host schema version",
            Some("hosts.schema_version"),
        ));
    }
    let alias = config
        .alias
        .map(|alias| alias.trim().to_string())
        .filter(|alias| !alias.is_empty());
    if alias.as_ref().is_some_and(|alias| {
        alias.len() > 128 || alias.chars().any(char::is_control)
    }) {
        return Err(federation_error(
            "invalid_request",
            "host alias must be a short printable string",
            Some("hosts.alias"),
        ));
    }
    if config.bearer_token.trim().is_empty()
        || config.bearer_token.chars().any(char::is_control)
    {
        return Err(federation_error(
            "missing_credential",
            "host credential is missing or invalid",
            Some("hosts.credential_ref"),
        ));
    }
    let plan = validate_connection_plan(&config.plan).map_err(|error| {
        federation_error(
            "invalid_request",
            &format!("invalid connection plan: {error}"),
            Some("hosts.plan"),
        )
    })?;
    Ok(ValidatedHostConfig {
        alias,
        plan,
        bearer_token: config.bearer_token,
    })
}

#[derive(Clone)]
pub(super) struct RemoteHost {
    pub(super) alias: Option<String>,
    pub(super) plan: ConnectionPlanWire,
    bearer_token: Arc<str>,
    client: reqwest::Client,
    runtime: Arc<AsyncMutex<RemoteHostRuntime>>,
    pub(super) permits: Arc<Semaphore>,
}

#[derive(Debug, Default)]
pub(super) struct RemoteHostRuntime {
    verified: bool,
    quarantine: Option<FederationErrorWire>,
}

impl RemoteHost {
    pub(super) fn new(
        config: ValidatedHostConfig,
        sase_home: &Path,
    ) -> Result<Self, FederationErrorWire> {
        let client = build_http_client(&config.plan, sase_home)?;
        Ok(Self {
            alias: config.alias,
            plan: config.plan,
            bearer_token: Arc::from(config.bearer_token),
            client,
            runtime: Arc::new(AsyncMutex::new(RemoteHostRuntime::default())),
            permits: Arc::new(Semaphore::new(DEFAULT_PER_HOST_IN_FLIGHT)),
        })
    }

    pub(super) fn empty_result(
        &self,
        status: &str,
    ) -> FederationHostResultWire {
        FederationHostResultWire {
            schema_version: FEDERATION_IPC_SCHEMA_VERSION,
            alias: self.alias.clone(),
            provider_ref: self.plan.provider_ref.clone(),
            installation_id: self.plan.pinned_installation_id.clone(),
            endpoint: self.plan.endpoint.clone(),
            status: status.to_string(),
            cached: false,
            age_seconds: None,
            payload: None,
            error: None,
        }
    }

    pub(super) async fn ensure_hello(
        &self,
        deadline: RequestDeadline,
    ) -> Result<(), FederationErrorWire> {
        {
            let runtime = self.runtime.lock().await;
            if let Some(error) = runtime.quarantine.clone() {
                return Err(error);
            }
            if runtime.verified {
                return Ok(());
            }
        }
        let value = self
            .http_json(Method::GET, "/hello", None, deadline)
            .await?;
        let hello: FleetHelloResponseWire = serde_json::from_value(value)
            .map_err(|error| {
                federation_error(
                    "invalid_response",
                    &format!("hello response had invalid shape: {error}"),
                    Some("hello"),
                )
            })?;
        if hello.installation.installation_id
            != self.plan.pinned_installation_id
        {
            let error = federation_error(
                "quarantined",
                "authenticated hello reported a different installation identity",
                Some("installation_id"),
            );
            self.runtime.lock().await.quarantine = Some(error.clone());
            return Err(error);
        }
        self.runtime.lock().await.verified = true;
        Ok(())
    }

    pub(super) async fn read_remote(
        &self,
        operation: &ReadOperation,
        deadline: RequestDeadline,
    ) -> Result<JsonValue, FederationErrorWire> {
        self.ensure_hello(deadline).await?;
        match operation {
            ReadOperation::Summary => {
                self.http_json(Method::GET, "/summary", None, deadline)
                    .await
            }
            ReadOperation::Catalog(query) => {
                self.http_json(
                    Method::POST,
                    "/catalog",
                    Some(to_json(query)?),
                    deadline,
                )
                .await
            }
            ReadOperation::FollowedBatch(request) => {
                self.http_json(
                    Method::POST,
                    "/batch",
                    Some(to_json(request)?),
                    deadline,
                )
                .await
            }
            ReadOperation::Detail(request) => {
                self.http_json(
                    Method::POST,
                    "/detail",
                    Some(to_json(request)?),
                    deadline,
                )
                .await
            }
            ReadOperation::ContentRange(request) => {
                self.http_json(
                    Method::POST,
                    "/content",
                    Some(to_json(request)?),
                    deadline,
                )
                .await
            }
            ReadOperation::ProjectEligibility(request) => {
                self.http_json(
                    Method::POST,
                    "/projects/eligibility",
                    Some(to_json(request)?),
                    deadline,
                )
                .await
            }
            ReadOperation::Attention(request) => {
                self.http_json(
                    Method::POST,
                    "/attention",
                    Some(to_json(request)?),
                    deadline,
                )
                .await
            }
            ReadOperation::AttentionInventory(request) => {
                self.http_json(
                    Method::POST,
                    "/attention/inventory",
                    Some(to_json(request)?),
                    deadline,
                )
                .await
            }
        }
    }

    pub(super) async fn launch_remote(
        &self,
        request: &FleetLaunchRequestWire,
        deadline: RequestDeadline,
    ) -> Result<JsonValue, FederationErrorWire> {
        self.ensure_hello(deadline).await?;
        self.http_json(
            Method::POST,
            "/launch",
            Some(to_json(request)?),
            deadline,
        )
        .await
    }

    pub(super) async fn mutate_remote(
        &self,
        request: &FleetMutationRequestWire,
        deadline: RequestDeadline,
    ) -> Result<JsonValue, FederationErrorWire> {
        self.ensure_hello(deadline).await?;
        self.http_json(
            Method::POST,
            "/mutate",
            Some(to_json(request)?),
            deadline,
        )
        .await
    }

    pub(super) async fn resolve_attention_remote(
        &self,
        request: &FleetAttentionRequestWire,
        deadline: RequestDeadline,
    ) -> Result<JsonValue, FederationErrorWire> {
        self.ensure_hello(deadline).await?;
        self.http_json(
            Method::POST,
            "/attention/resolve",
            Some(to_json(request)?),
            deadline,
        )
        .await
    }

    pub(super) async fn http_json(
        &self,
        method: Method,
        path: &str,
        body: Option<JsonValue>,
        deadline: RequestDeadline,
    ) -> Result<JsonValue, FederationErrorWire> {
        let url = fleet_url(&self.plan.endpoint, path);
        let mut request = self
            .client
            .request(method, url)
            .bearer_auth(self.bearer_token.as_ref())
            .header(
                FLEET_PROTOCOL_VERSIONS_HEADER,
                FLEET_PROTOCOL_VERSION.to_string(),
            );
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = with_deadline(deadline, async {
            request.send().await.map_err(|error| {
                federation_error(
                    "unavailable",
                    &format!("fleet request failed: {error}"),
                    Some("endpoint"),
                )
            })
        })
        .await?;
        let status = response.status();
        let value = with_deadline(deadline, async {
            response.json::<JsonValue>().await.map_err(|error| {
                federation_error(
                    "invalid_response",
                    &format!("fleet response was not JSON: {error}"),
                    Some("response"),
                )
            })
        })
        .await?;
        if !status.is_success() {
            return Err(fleet_http_error(status, value));
        }
        Ok(value)
    }
}

pub(super) fn build_http_client(
    plan: &ConnectionPlanWire,
    sase_home: &Path,
) -> Result<reqwest::Client, FederationErrorWire> {
    let mut builder =
        reqwest::Client::builder().pool_idle_timeout(Duration::from_secs(30));
    match plan.tls.mode {
        TlsTrustModeWire::SystemRoots => {}
        TlsTrustModeWire::PinnedCa => {
            let ca_ref = plan.tls.ca_ref.as_deref().ok_or_else(|| {
                federation_error(
                    "invalid_request",
                    "pinned_ca trust mode requires ca_ref",
                    Some("hosts.plan.tls.ca_ref"),
                )
            })?;
            let bytes = fs::read(managed_trust_ref_path(
                sase_home, "ca", ca_ref, "pem",
            ))
            .map_err(|error| {
                federation_error(
                    "invalid_request",
                    &format!(
                        "managed TLS CA reference {ca_ref:?} could not be resolved: {error}"
                    ),
                    Some("hosts.plan.tls.ca_ref"),
                )
            })?;
            let certificate = reqwest::Certificate::from_pem(&bytes)
                .map_err(|error| {
                    federation_error(
                        "invalid_request",
                        &format!(
                            "managed TLS CA reference {ca_ref:?} is not a PEM certificate: {error}"
                        ),
                        Some("hosts.plan.tls.ca_ref"),
                    )
                })?;
            builder = builder.add_root_certificate(certificate);
        }
        TlsTrustModeWire::PinnedServerName => {
            let server_name_ref =
                plan.tls.server_name_ref.as_deref().ok_or_else(|| {
                    federation_error(
                        "invalid_request",
                        "pinned_server_name trust mode requires server_name_ref",
                        Some("hosts.plan.tls.server_name_ref"),
                    )
                })?;
            let pinned_name = fs::read_to_string(managed_trust_ref_path(
                sase_home,
                "server_name",
                server_name_ref,
                "txt",
            ))
            .map_err(|error| {
                federation_error(
                    "invalid_request",
                    &format!(
                        "managed TLS server-name reference {server_name_ref:?} could not be resolved: {error}"
                    ),
                    Some("hosts.plan.tls.server_name_ref"),
                )
            })?;
            let pinned_name = pinned_name.trim();
            if pinned_name.is_empty()
                || pinned_name.chars().any(char::is_control)
            {
                return Err(federation_error(
                    "invalid_request",
                    "managed TLS server-name reference resolved to an invalid name",
                    Some("hosts.plan.tls.server_name_ref"),
                ));
            }
            let endpoint =
                reqwest::Url::parse(&plan.endpoint).map_err(|error| {
                    federation_error(
                        "invalid_request",
                        &format!(
                            "connection endpoint is not a valid URL: {error}"
                        ),
                        Some("hosts.plan.endpoint"),
                    )
                })?;
            let Some(endpoint_host) = endpoint.host_str() else {
                return Err(federation_error(
                    "invalid_request",
                    "connection endpoint has no host to compare with pinned server name",
                    Some("hosts.plan.endpoint"),
                ));
            };
            if endpoint_host != pinned_name {
                return Err(federation_error(
                    "invalid_request",
                    "connection endpoint host does not match pinned server-name reference",
                    Some("hosts.plan.tls.server_name_ref"),
                ));
            }
        }
    }
    builder.build().map_err(|error| {
        federation_error(
            "invalid_request",
            &format!("failed to build federation HTTP client: {error}"),
            Some("hosts.plan.tls"),
        )
    })
}

pub(super) fn managed_trust_ref_path(
    sase_home: &Path,
    kind: &str,
    reference: &str,
    extension: &str,
) -> PathBuf {
    sase_home
        .join("fleet")
        .join("trust")
        .join(kind)
        .join(format!("{reference}.{extension}"))
}

#[derive(Clone)]
pub(super) enum ReadOperation {
    Summary,
    Catalog(FleetCatalogQueryWire),
    FollowedBatch(FleetLogicalBatchRequestWire),
    Detail(FleetDetailRequestWire),
    ContentRange(FleetContentReadRequestWire),
    ProjectEligibility(FleetProjectEligibilityRequestWire),
    Attention(FleetLogicalBatchRequestWire),
    AttentionInventory(FleetAttentionInventoryRequestWire),
}

impl ReadOperation {
    pub(super) fn name(&self) -> &'static str {
        match self {
            Self::Summary => "summary",
            Self::Catalog(_) => "catalog",
            Self::FollowedBatch(_) => "followed_batch",
            Self::Detail(_) => "detail",
            Self::ContentRange(_) => "content_range",
            Self::ProjectEligibility(_) => "project_eligibility",
            Self::Attention(_) => "attention",
            Self::AttentionInventory(_) => "attention_inventory",
        }
    }

    pub(super) fn cache_key(&self) -> Result<String, FederationErrorWire> {
        let payload = match self {
            Self::Summary => serde_json::json!({}),
            Self::Catalog(query) => to_json(query)?,
            Self::FollowedBatch(request) => to_json(request)?,
            Self::Detail(request) => to_json(request)?,
            Self::ContentRange(request) => to_json(request)?,
            Self::ProjectEligibility(request) => to_json(request)?,
            Self::Attention(request) => to_json(request)?,
            Self::AttentionInventory(request) => to_json(request)?,
        };
        Ok(format!("{}:{payload}", self.name()))
    }
}

pub(super) async fn read_one_host(
    host: Arc<RemoteHost>,
    operation: ReadOperation,
    cache_only: bool,
    deadline: RequestDeadline,
    cache: Arc<AsyncMutex<FederationCache>>,
    limits: OwnedCachePathLimits,
    global: Arc<Semaphore>,
) -> FederationHostResultWire {
    let cache_key = match operation.cache_key() {
        Ok(key) => key,
        Err(error) => return host.error_result(error, false, None),
    };
    if cache_only {
        return cached_or_error(&host, &cache, &cache_key, None).await;
    }
    let result = with_deadline(deadline, async {
        let _global = global.acquire_owned().await.map_err(|_| {
            federation_error(
                "unavailable",
                "global request limiter is closed",
                Some("concurrency"),
            )
        })?;
        let _host =
            host.permits.clone().acquire_owned().await.map_err(|_| {
                federation_error(
                    "unavailable",
                    "host request limiter is closed",
                    Some("concurrency"),
                )
            })?;
        host.read_remote(&operation, deadline).await
    })
    .await;
    match result {
        Ok(payload) => {
            let entry = {
                let mut cache = cache.lock().await;
                let entry = cache.store(
                    &host.plan.pinned_installation_id,
                    &cache_key,
                    payload.clone(),
                );
                let _ = cache.persist(&limits.path);
                entry
            };
            host.payload_result(
                "ok",
                false,
                entry_age(entry.as_ref()),
                payload,
                None,
            )
        }
        Err(error) => {
            cached_or_error(&host, &cache, &cache_key, Some(error)).await
        }
    }
}

impl RemoteHost {
    pub(super) fn error_result(
        &self,
        error: FederationErrorWire,
        cached: bool,
        payload: Option<JsonValue>,
    ) -> FederationHostResultWire {
        let status = status_from_error(&error);
        FederationHostResultWire {
            schema_version: FEDERATION_IPC_SCHEMA_VERSION,
            alias: self.alias.clone(),
            provider_ref: self.plan.provider_ref.clone(),
            installation_id: self.plan.pinned_installation_id.clone(),
            endpoint: self.plan.endpoint.clone(),
            status,
            cached,
            age_seconds: None,
            payload,
            error: Some(error),
        }
    }

    pub(super) fn payload_result(
        &self,
        status: &str,
        cached: bool,
        age_seconds: Option<f64>,
        payload: JsonValue,
        error: Option<FederationErrorWire>,
    ) -> FederationHostResultWire {
        FederationHostResultWire {
            schema_version: FEDERATION_IPC_SCHEMA_VERSION,
            alias: self.alias.clone(),
            provider_ref: self.plan.provider_ref.clone(),
            installation_id: self.plan.pinned_installation_id.clone(),
            endpoint: self.plan.endpoint.clone(),
            status: status.to_string(),
            cached,
            age_seconds,
            payload: Some(payload),
            error,
        }
    }
}

pub(super) async fn cached_or_error(
    host: &RemoteHost,
    cache: &Arc<AsyncMutex<FederationCache>>,
    cache_key: &str,
    error: Option<FederationErrorWire>,
) -> FederationHostResultWire {
    let cached = cache
        .lock()
        .await
        .get(&host.plan.pinned_installation_id, cache_key);
    match (cached, error) {
        (Some(entry), Some(error)) => host.payload_result(
            "stale",
            true,
            entry_age(Some(&entry)),
            entry.payload,
            Some(error),
        ),
        (Some(entry), None) => host.payload_result(
            "ok",
            true,
            entry_age(Some(&entry)),
            entry.payload,
            None,
        ),
        (None, Some(error)) => host.error_result(error, false, None),
        (None, None) => host.error_result(
            federation_error(
                "stale",
                "no cached projection is available",
                Some("cache"),
            ),
            false,
            None,
        ),
    }
}

pub(super) fn fleet_url(endpoint: &str, fleet_path: &str) -> String {
    let base = endpoint.trim_end_matches('/');
    if base.ends_with("/api/fleet/v1") {
        format!("{base}{fleet_path}")
    } else if base.ends_with("/api") {
        format!("{base}/fleet/v1{fleet_path}")
    } else {
        format!("{base}/api/fleet/v1{fleet_path}")
    }
}

pub(super) fn fleet_http_error(
    status: StatusCode,
    payload: JsonValue,
) -> FederationErrorWire {
    let code = payload
        .get("code")
        .and_then(JsonValue::as_str)
        .map(|value| value.to_string())
        .unwrap_or_else(|| {
            if status == StatusCode::UNAUTHORIZED {
                "unauthorized".to_string()
            } else if status == StatusCode::NOT_FOUND {
                "not_found".to_string()
            } else if status == StatusCode::REQUEST_TIMEOUT {
                "deadline".to_string()
            } else {
                "unavailable".to_string()
            }
        });
    let message = payload
        .get("message")
        .and_then(JsonValue::as_str)
        .map(|value| value.to_string())
        .unwrap_or_else(|| format!("fleet endpoint returned HTTP {status}"));
    FederationErrorWire {
        schema_version: FEDERATION_IPC_SCHEMA_VERSION,
        code,
        message,
        target: payload
            .get("target")
            .and_then(JsonValue::as_str)
            .map(ToString::to_string),
        details: payload.get("details").cloned().map(Box::new),
    }
}
