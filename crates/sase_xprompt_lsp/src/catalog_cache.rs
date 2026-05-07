use std::{
    collections::{BTreeSet, HashMap},
    path::PathBuf,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use sase_core::{
    editor_assist_entries_from_catalog, load_editor_xprompt_catalog,
    CommandHelperHostBridge, DynHelperHostBridge,
    EditorXpromptCatalogRequestWire, HelperHostBridge, HostBridgeError,
    XpromptAssistEntry, XpromptCatalogLoadOptions,
};
use tokio::time;
use tracing::warn;

const COMPLETION_REFRESH_TIMEOUT: Duration = Duration::from_secs(5);
const EXPLICIT_REFRESH_TIMEOUT: Duration = Duration::from_secs(30);
const CACHE_TTL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogFailure {
    pub class: String,
    pub message: String,
}

#[derive(Debug, Clone)]
struct CachedCatalog {
    entries: Arc<Vec<XpromptAssistEntry>>,
    refreshed_at: Instant,
}

#[derive(Debug)]
pub struct CatalogCache {
    bridge: DynHelperHostBridge,
    prefer_rust_catalog: bool,
    catalogs: RwLock<HashMap<String, CachedCatalog>>,
    warned_failure_classes: RwLock<BTreeSet<String>>,
}

impl CatalogCache {
    pub fn command_backed() -> Self {
        Self::new_with_rust_catalog(Arc::new(CommandHelperHostBridge::new(
            CommandHelperHostBridge::default_command(),
        )))
    }

    pub fn new(bridge: Arc<dyn HelperHostBridge>) -> Self {
        Self::new_inner(bridge, false)
    }

    fn new_with_rust_catalog(bridge: Arc<dyn HelperHostBridge>) -> Self {
        Self::new_inner(bridge, true)
    }

    fn new_inner(
        bridge: Arc<dyn HelperHostBridge>,
        prefer_rust_catalog: bool,
    ) -> Self {
        Self {
            bridge: DynHelperHostBridge::new(bridge),
            prefer_rust_catalog,
            catalogs: RwLock::new(HashMap::new()),
            warned_failure_classes: RwLock::new(BTreeSet::new()),
        }
    }

    pub fn cached_entries(
        &self,
        key: &str,
    ) -> Option<Arc<Vec<XpromptAssistEntry>>> {
        let catalogs = self.catalogs.read().ok()?;
        catalogs.get(key).map(|catalog| catalog.entries.clone())
    }

    pub fn stale_or_missing(&self, key: &str) -> bool {
        let Ok(catalogs) = self.catalogs.read() else {
            return true;
        };
        catalogs
            .get(key)
            .map(|catalog| catalog.refreshed_at.elapsed() >= CACHE_TTL)
            .unwrap_or(true)
    }

    pub async fn refresh_for_completion(
        &self,
        key: String,
        project: Option<String>,
        root_dir: Option<PathBuf>,
    ) -> Result<Arc<Vec<XpromptAssistEntry>>, CatalogFailure> {
        self.refresh(key, project, root_dir, COMPLETION_REFRESH_TIMEOUT)
            .await
    }

    pub async fn refresh_explicit(
        &self,
        key: String,
        project: Option<String>,
        root_dir: Option<PathBuf>,
    ) -> Result<Arc<Vec<XpromptAssistEntry>>, CatalogFailure> {
        self.refresh(key, project, root_dir, EXPLICIT_REFRESH_TIMEOUT)
            .await
    }

    pub fn should_warn(&self, class: &str) -> bool {
        let Ok(mut warned) = self.warned_failure_classes.write() else {
            return false;
        };
        warned.insert(class.to_string())
    }

    pub fn invalidate_all(&self) {
        if let Ok(mut catalogs) = self.catalogs.write() {
            catalogs.clear();
        }
    }

    async fn refresh(
        &self,
        key: String,
        project: Option<String>,
        root_dir: Option<PathBuf>,
        timeout: Duration,
    ) -> Result<Arc<Vec<XpromptAssistEntry>>, CatalogFailure> {
        let request = EditorXpromptCatalogRequestWire {
            schema_version: 1,
            project,
            source: None,
            tag: None,
            query: None,
            include_pdf: false,
            limit: None,
            device_id: None,
        };

        if self.prefer_rust_catalog {
            match refresh_with_rust_catalog(request.clone(), root_dir).await {
                Ok(entries) if !entries.is_empty() => {
                    return Ok(self.store(key, entries));
                }
                Ok(_) => {}
                Err(error) => {
                    warn!(
                        "rust xprompt catalog loader failed: {}",
                        error.message
                    );
                }
            }
        }

        let entries = self.refresh_with_helper(&request, timeout).await?;
        Ok(self.store(key, entries))
    }

    async fn refresh_with_helper(
        &self,
        request: &EditorXpromptCatalogRequestWire,
        timeout: Duration,
    ) -> Result<Vec<XpromptAssistEntry>, CatalogFailure> {
        let bridge = self.bridge.clone();
        let request = request.clone();
        let task = tokio::task::spawn_blocking(move || {
            bridge.xprompt_catalog(&request)
        });
        let response = match time::timeout(timeout, task).await {
            Ok(Ok(Ok(response))) => response,
            Ok(Ok(Err(error))) => {
                return Err(failure_from_bridge_error(error));
            }
            Ok(Err(error)) => {
                return Err(CatalogFailure {
                    class: "helper_join".to_string(),
                    message: format!("xprompt catalog helper failed: {error}"),
                });
            }
            Err(_) => {
                return Err(CatalogFailure {
                    class: "helper_timeout".to_string(),
                    message: "xprompt catalog helper timed out".to_string(),
                });
            }
        };

        Ok(editor_assist_entries_from_catalog(&response.entries))
    }

    fn store(
        &self,
        key: String,
        entries: Vec<XpromptAssistEntry>,
    ) -> Arc<Vec<XpromptAssistEntry>> {
        let entries = Arc::new(entries);
        let cached = CachedCatalog {
            entries: entries.clone(),
            refreshed_at: Instant::now(),
        };
        if let Ok(mut catalogs) = self.catalogs.write() {
            catalogs.insert(key, cached);
        } else {
            warn!("failed to lock xprompt catalog cache for write");
        }
        entries
    }
}

async fn refresh_with_rust_catalog(
    request: EditorXpromptCatalogRequestWire,
    root_dir: Option<PathBuf>,
) -> Result<Vec<XpromptAssistEntry>, CatalogFailure> {
    let task = tokio::task::spawn_blocking(move || {
        load_editor_xprompt_catalog(
            &request,
            &XpromptCatalogLoadOptions::new(root_dir),
        )
    });
    let response = match task.await {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            return Err(CatalogFailure {
                class: "rust_catalog_error".to_string(),
                message: format!("rust xprompt catalog loader failed: {error}"),
            });
        }
        Err(error) => {
            return Err(CatalogFailure {
                class: "rust_catalog_join".to_string(),
                message: format!("rust xprompt catalog loader failed: {error}"),
            });
        }
    };
    Ok(editor_assist_entries_from_catalog(&response.entries))
}

fn failure_from_bridge_error(error: HostBridgeError) -> CatalogFailure {
    let class = match &error {
        HostBridgeError::BridgeUnavailable(_) => "helper_unavailable",
        HostBridgeError::HelperNotFound(_) => "helper_not_found",
        _ => "helper_error",
    }
    .to_string();
    CatalogFailure {
        class,
        message: format!("xprompt catalog helper failed: {error}"),
    }
}
