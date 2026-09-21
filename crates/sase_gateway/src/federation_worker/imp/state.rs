use super::*;
use tokio::task::JoinSet;

pub(super) struct FederationWorkerState {
    pub(super) config: FederationWorkerConfig,
    pub(super) hosts: RwLock<BTreeMap<String, Arc<RemoteHost>>>,
    cache: Arc<AsyncMutex<FederationCache>>,
    pub(super) connection_permits: Arc<Semaphore>,
    in_flight: Arc<Semaphore>,
    pub(super) shutdown: Notify,
}

impl FederationWorkerState {
    pub(super) fn new(config: FederationWorkerConfig) -> Self {
        let cache = FederationCache::load(
            cache_path(&config.sase_home),
            config.cache_entry_limit,
            config.cache_byte_limit,
        );
        Self {
            connection_permits: Arc::new(Semaphore::new(
                config.max_connections,
            )),
            in_flight: Arc::new(Semaphore::new(config.max_in_flight)),
            config,
            hosts: RwLock::new(BTreeMap::new()),
            cache: Arc::new(AsyncMutex::new(cache)),
            shutdown: Notify::new(),
        }
    }

    pub(super) async fn replace_config(
        &self,
        configs: Vec<FederationHostConfigWire>,
    ) -> Result<JsonValue, FederationErrorWire> {
        let mut hosts = BTreeMap::new();
        let mut results = Vec::new();
        for config in configs {
            let result = match validate_host_config(config) {
                Ok(validated) => {
                    let installation_id =
                        validated.plan.pinned_installation_id.clone();
                    match hosts.entry(installation_id.clone()) {
                        std::collections::btree_map::Entry::Occupied(_) => {
                            FederationHostResultWire {
                                schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                                alias: validated.alias,
                                provider_ref: validated.plan.provider_ref,
                                installation_id,
                                endpoint: validated.plan.endpoint,
                                status: "invalid".to_string(),
                                cached: false,
                                age_seconds: None,
                                payload: None,
                                error: Some(federation_error(
                                    "invalid_request",
                                    "duplicate pinned installation ID",
                                    Some("hosts"),
                                )),
                            }
                        }
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            match RemoteHost::new(
                                validated.clone(),
                                &self.config.sase_home,
                            ) {
                                Ok(host) => {
                                    let host = Arc::new(host);
                                    let response =
                                        host.empty_result("configured");
                                    entry.insert(host);
                                    response
                                }
                                Err(error) => FederationHostResultWire {
                                    schema_version:
                                        FEDERATION_IPC_SCHEMA_VERSION,
                                    alias: validated.alias,
                                    provider_ref: validated.plan.provider_ref,
                                    installation_id,
                                    endpoint: validated.plan.endpoint,
                                    status: status_from_error(&error),
                                    cached: false,
                                    age_seconds: None,
                                    payload: None,
                                    error: Some(error),
                                },
                            }
                        }
                    }
                }
                Err(error) => FederationHostResultWire {
                    schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                    alias: None,
                    provider_ref: "".to_string(),
                    installation_id: "".to_string(),
                    endpoint: "".to_string(),
                    status: "invalid".to_string(),
                    cached: false,
                    age_seconds: None,
                    payload: None,
                    error: Some(error),
                },
            };
            results.push(result);
        }
        let configured_hosts = hosts.len();
        *self.hosts.write().await = hosts;
        to_json(serde_json::json!({
            "schema_version": FEDERATION_IPC_SCHEMA_VERSION,
            "configured_hosts": configured_hosts,
            "hosts": results,
        }))
    }

    pub(super) async fn read_all(
        &self,
        operation: ReadOperation,
        cache_only: bool,
        deadline: RequestDeadline,
    ) -> Result<JsonValue, FederationErrorWire> {
        let hosts = self
            .hosts
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        if hosts.is_empty() {
            return to_json(FederationReadResponseWire {
                schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                operation: operation.name().to_string(),
                hosts: Vec::new(),
            });
        }

        let mut tasks = JoinSet::new();
        for host in hosts {
            let op = operation.clone();
            let cache = self.cache_path_limits_owned();
            let cache_ref = self.cache.clone();
            let global = self.in_flight.clone();
            tasks.spawn(async move {
                read_one_host(
                    host, op, cache_only, deadline, cache_ref, cache, global,
                )
                .await
            });
        }
        let mut results = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(result) => results.push(result),
                Err(_) => results.push(FederationHostResultWire {
                    schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                    alias: None,
                    provider_ref: "".to_string(),
                    installation_id: "".to_string(),
                    endpoint: "".to_string(),
                    status: "unavailable".to_string(),
                    cached: false,
                    age_seconds: None,
                    payload: None,
                    error: Some(federation_error(
                        "internal",
                        "host worker task failed",
                        Some("host"),
                    )),
                }),
            }
        }
        results.sort_by(|left, right| {
            left.installation_id
                .cmp(&right.installation_id)
                .then_with(|| left.alias.cmp(&right.alias))
        });
        to_json(FederationReadResponseWire {
            schema_version: FEDERATION_IPC_SCHEMA_VERSION,
            operation: operation.name().to_string(),
            hosts: results,
        })
    }

    pub(super) async fn read_catalog_hosts(
        &self,
        queries: Vec<FederationHostCatalogQueryWire>,
        cache_only: bool,
        deadline: RequestDeadline,
    ) -> Result<JsonValue, FederationErrorWire> {
        let mut tasks = JoinSet::new();
        let mut results = Vec::new();
        {
            let hosts = self.hosts.read().await;
            for host_query in queries {
                if host_query.schema_version != FEDERATION_IPC_SCHEMA_VERSION {
                    results.push(FederationHostResultWire {
                        schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                        alias: None,
                        provider_ref: "".to_string(),
                        installation_id: host_query.installation_id,
                        endpoint: "".to_string(),
                        status: "invalid".to_string(),
                        cached: false,
                        age_seconds: None,
                        payload: None,
                        error: Some(federation_error(
                            "unsupported_version",
                            "unsupported per-host catalog query schema version",
                            Some("queries.schema_version"),
                        )),
                    });
                    continue;
                }
                let installation_id =
                    host_query.installation_id.trim().to_string();
                let Some(host) = hosts.get(&installation_id).cloned() else {
                    results.push(FederationHostResultWire {
                        schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                        alias: None,
                        provider_ref: "".to_string(),
                        installation_id,
                        endpoint: "".to_string(),
                        status: "not_found".to_string(),
                        cached: false,
                        age_seconds: None,
                        payload: None,
                        error: Some(federation_error(
                            "not_found",
                            "no configured dispatch host matches the per-host catalog query",
                            Some("queries.installation_id"),
                        )),
                    });
                    continue;
                };
                let cache = self.cache_path_limits_owned();
                let cache_ref = self.cache.clone();
                let global = self.in_flight.clone();
                tasks.spawn(async move {
                    read_one_host(
                        host,
                        ReadOperation::Catalog(host_query.query),
                        cache_only,
                        deadline,
                        cache_ref,
                        cache,
                        global,
                    )
                    .await
                });
            }
        }
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(result) => results.push(result),
                Err(_) => results.push(FederationHostResultWire {
                    schema_version: FEDERATION_IPC_SCHEMA_VERSION,
                    alias: None,
                    provider_ref: "".to_string(),
                    installation_id: "".to_string(),
                    endpoint: "".to_string(),
                    status: "unavailable".to_string(),
                    cached: false,
                    age_seconds: None,
                    payload: None,
                    error: Some(federation_error(
                        "internal",
                        "host worker task failed",
                        Some("host"),
                    )),
                }),
            }
        }
        results.sort_by(|left, right| {
            left.installation_id
                .cmp(&right.installation_id)
                .then_with(|| left.alias.cmp(&right.alias))
        });
        to_json(FederationReadResponseWire {
            schema_version: FEDERATION_IPC_SCHEMA_VERSION,
            operation: "catalog".to_string(),
            hosts: results,
        })
    }

    pub(super) async fn launch_one(
        &self,
        target: String,
        request: FleetLaunchRequestWire,
        deadline: RequestDeadline,
    ) -> Result<JsonValue, FederationErrorWire> {
        let host = self.resolve_launch_target(&target).await?;
        if request.target_installation_id != host.plan.pinned_installation_id {
            return Err(federation_error(
                "quarantined",
                "launch request target_installation_id does not match the configured host pin",
                Some("target_installation_id"),
            ));
        }
        let payload = with_deadline(deadline, async {
            let _global =
                self.in_flight.clone().acquire_owned().await.map_err(|_| {
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
            host.launch_remote(&request, deadline).await
        })
        .await?;
        to_json(FederationReadResponseWire {
            schema_version: FEDERATION_IPC_SCHEMA_VERSION,
            operation: "launch".to_string(),
            hosts: vec![host.payload_result("ok", false, None, payload, None)],
        })
    }

    pub(super) async fn mutate_one(
        &self,
        target: String,
        request: FleetMutationRequestWire,
        deadline: RequestDeadline,
    ) -> Result<JsonValue, FederationErrorWire> {
        let host = self.resolve_launch_target(&target).await?;
        if request.target_installation_id != host.plan.pinned_installation_id {
            return Err(federation_error(
                "quarantined",
                "mutation request target_installation_id does not match the configured host pin",
                Some("target_installation_id"),
            ));
        }
        let payload = with_deadline(deadline, async {
            let _global =
                self.in_flight.clone().acquire_owned().await.map_err(|_| {
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
            host.mutate_remote(&request, deadline).await
        })
        .await?;
        to_json(FederationReadResponseWire {
            schema_version: FEDERATION_IPC_SCHEMA_VERSION,
            operation: "mutate".to_string(),
            hosts: vec![host.payload_result("ok", false, None, payload, None)],
        })
    }

    pub(super) async fn resolve_attention_one(
        &self,
        target: String,
        request: FleetAttentionRequestWire,
        deadline: RequestDeadline,
    ) -> Result<JsonValue, FederationErrorWire> {
        let host = self.resolve_launch_target(&target).await?;
        if request.target_installation_id != host.plan.pinned_installation_id {
            return Err(federation_error(
                "quarantined",
                "attention resolve request target_installation_id does not match the configured host pin",
                Some("target_installation_id"),
            ));
        }
        let payload = with_deadline(deadline, async {
            let _global =
                self.in_flight.clone().acquire_owned().await.map_err(|_| {
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
            host.resolve_attention_remote(&request, deadline).await
        })
        .await?;
        to_json(FederationReadResponseWire {
            schema_version: FEDERATION_IPC_SCHEMA_VERSION,
            operation: "resolve_attention".to_string(),
            hosts: vec![host.payload_result("ok", false, None, payload, None)],
        })
    }

    pub(super) async fn resolve_launch_target(
        &self,
        target: &str,
    ) -> Result<Arc<RemoteHost>, FederationErrorWire> {
        let target = target.trim();
        if target.is_empty() {
            return Err(federation_error(
                "invalid_request",
                "launch target must be non-empty",
                Some("target"),
            ));
        }
        let hosts = self.hosts.read().await;
        if let Some(host) = hosts.get(target) {
            return Ok(host.clone());
        }
        let matches = hosts
            .values()
            .filter(|host| host.alias.as_deref() == Some(target))
            .cloned()
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [host] => Ok(host.clone()),
            [] => Err(federation_error(
                "not_found",
                "no configured dispatch host matches the launch target",
                Some("target"),
            )),
            _ => Err(federation_error(
                "invalid_request",
                "launch target alias matches multiple configured hosts",
                Some("target"),
            )),
        }
    }

    pub(super) fn cache_path_limits_owned(&self) -> OwnedCachePathLimits {
        OwnedCachePathLimits {
            path: cache_path(&self.config.sase_home),
        }
    }
}

#[derive(Clone)]
pub(super) struct OwnedCachePathLimits {
    pub(super) path: PathBuf,
}
