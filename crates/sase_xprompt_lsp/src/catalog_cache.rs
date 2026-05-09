use std::{
    collections::{BTreeSet, HashMap},
    env,
    path::PathBuf,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use sase_core::{
    editor_assist_entries_from_catalog, load_editor_xprompt_catalog,
    CommandHelperHostBridge, DynHelperHostBridge,
    EditorSnippetCatalogRequestWire, EditorSnippetEntryWire,
    EditorXpromptCatalogRequestWire, HelperHostBridge, HostBridgeError,
    XpromptAssistEntry, XpromptCatalogLoadOptions,
};
use tokio::time;
use tracing::warn;

const COMPLETION_REFRESH_TIMEOUT: Duration = Duration::from_secs(5);
const EXPLICIT_REFRESH_TIMEOUT: Duration = Duration::from_secs(30);
const CACHE_TTL: Duration = Duration::from_secs(30);
const SASE_XPROMPT_PLUGIN_DIRS_JSON_ENV: &str = "SASE_XPROMPT_PLUGIN_DIRS_JSON";
const SASE_XPROMPT_PLUGIN_CONFIG_PATHS_JSON_ENV: &str =
    "SASE_XPROMPT_PLUGIN_CONFIG_PATHS_JSON";

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

#[derive(Debug, Clone)]
struct CachedSnippetCatalog {
    entries: Arc<Vec<EditorSnippetEntryWire>>,
    refreshed_at: Instant,
}

#[derive(Debug)]
pub struct CatalogCache {
    bridge: DynHelperHostBridge,
    prefer_rust_catalog: bool,
    plugin_metadata_present: bool,
    catalogs: RwLock<HashMap<String, CachedCatalog>>,
    snippet_catalogs: RwLock<HashMap<String, CachedSnippetCatalog>>,
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
            plugin_metadata_present: plugin_metadata_env_present(),
            catalogs: RwLock::new(HashMap::new()),
            snippet_catalogs: RwLock::new(HashMap::new()),
            warned_failure_classes: RwLock::new(BTreeSet::new()),
        }
    }

    #[cfg(test)]
    fn new_with_rust_catalog_and_plugin_metadata(
        bridge: Arc<dyn HelperHostBridge>,
        plugin_metadata_present: bool,
    ) -> Self {
        Self {
            bridge: DynHelperHostBridge::new(bridge),
            prefer_rust_catalog: true,
            plugin_metadata_present,
            catalogs: RwLock::new(HashMap::new()),
            snippet_catalogs: RwLock::new(HashMap::new()),
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

    pub fn cached_snippet_entries(
        &self,
        key: &str,
    ) -> Option<Arc<Vec<EditorSnippetEntryWire>>> {
        let catalogs = self.snippet_catalogs.read().ok()?;
        catalogs.get(key).map(|catalog| catalog.entries.clone())
    }

    pub fn snippets_stale_or_missing(&self, key: &str) -> bool {
        let Ok(catalogs) = self.snippet_catalogs.read() else {
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

    pub async fn refresh_snippets_for_completion(
        &self,
        key: String,
        project: Option<String>,
    ) -> Result<Arc<Vec<EditorSnippetEntryWire>>, CatalogFailure> {
        match self
            .refresh_snippets(key.clone(), project, COMPLETION_REFRESH_TIMEOUT)
            .await
        {
            Ok(entries) => Ok(entries),
            Err(error) => self.cached_snippet_entries(&key).ok_or(error),
        }
    }

    pub async fn refresh_snippets_explicit(
        &self,
        key: String,
        project: Option<String>,
    ) -> Result<Arc<Vec<EditorSnippetEntryWire>>, CatalogFailure> {
        self.refresh_snippets(key, project, EXPLICIT_REFRESH_TIMEOUT)
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
        if let Ok(mut catalogs) = self.snippet_catalogs.write() {
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

        if self.prefer_rust_catalog && self.plugin_metadata_present {
            match refresh_with_rust_catalog(request.clone(), root_dir).await {
                Ok(entries) if !entries.is_empty() => {
                    return Ok(self.store(key, entries));
                }
                Ok(_) => {}
                Err(error) => warn!(
                    "rust xprompt catalog loader failed: {}",
                    error.message
                ),
            }
        } else if self.prefer_rust_catalog {
            let rust_result =
                refresh_with_rust_catalog(request.clone(), root_dir).await;
            if let Err(error) = &rust_result {
                warn!("rust xprompt catalog loader failed: {}", error.message);
            }
            match self.refresh_with_helper(&request, timeout).await {
                Ok(entries) if !entries.is_empty() => {
                    let entries = match rust_result {
                        Ok(rust_entries) if !rust_entries.is_empty() => {
                            merge_catalog_entries(entries, rust_entries)
                        }
                        _ => entries,
                    };
                    return Ok(self.store(key, entries));
                }
                Ok(_) => {
                    if let Ok(entries) = rust_result {
                        if !entries.is_empty() {
                            return Ok(self.store(key, entries));
                        }
                    }
                    return Ok(self.store(key, Vec::new()));
                }
                Err(error) => {
                    if let Ok(entries) = rust_result {
                        if !entries.is_empty() {
                            return Ok(self.store(key, entries));
                        }
                    }
                    return Err(error);
                }
            }
        }

        let entries = self.refresh_with_helper(&request, timeout).await?;
        Ok(self.store(key, entries))
    }

    async fn refresh_snippets(
        &self,
        key: String,
        project: Option<String>,
        timeout: Duration,
    ) -> Result<Arc<Vec<EditorSnippetEntryWire>>, CatalogFailure> {
        let request = EditorSnippetCatalogRequestWire {
            schema_version: 1,
            project,
        };
        let entries =
            self.refresh_snippets_with_helper(&request, timeout).await?;
        Ok(self.store_snippets(key, entries))
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
                return Err(failure_from_bridge_error(
                    error,
                    "xprompt catalog",
                ));
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

    async fn refresh_snippets_with_helper(
        &self,
        request: &EditorSnippetCatalogRequestWire,
        timeout: Duration,
    ) -> Result<Vec<EditorSnippetEntryWire>, CatalogFailure> {
        let bridge = self.bridge.clone();
        let request = request.clone();
        let task = tokio::task::spawn_blocking(move || {
            bridge.snippet_catalog(&request)
        });
        let response = match time::timeout(timeout, task).await {
            Ok(Ok(Ok(response))) => response,
            Ok(Ok(Err(error))) => {
                return Err(failure_from_bridge_error(
                    error,
                    "snippet catalog",
                ));
            }
            Ok(Err(error)) => {
                return Err(CatalogFailure {
                    class: "helper_join".to_string(),
                    message: format!("snippet catalog helper failed: {error}"),
                });
            }
            Err(_) => {
                return Err(CatalogFailure {
                    class: "helper_timeout".to_string(),
                    message: "snippet catalog helper timed out".to_string(),
                });
            }
        };

        Ok(response.entries)
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

    fn store_snippets(
        &self,
        key: String,
        entries: Vec<EditorSnippetEntryWire>,
    ) -> Arc<Vec<EditorSnippetEntryWire>> {
        let entries = Arc::new(entries);
        let cached = CachedSnippetCatalog {
            entries: entries.clone(),
            refreshed_at: Instant::now(),
        };
        if let Ok(mut catalogs) = self.snippet_catalogs.write() {
            catalogs.insert(key, cached);
        } else {
            warn!("failed to lock snippet catalog cache for write");
        }
        entries
    }
}

fn merge_catalog_entries(
    mut helper_entries: Vec<XpromptAssistEntry>,
    rust_entries: Vec<XpromptAssistEntry>,
) -> Vec<XpromptAssistEntry> {
    let mut indexes = helper_entries
        .iter()
        .enumerate()
        .map(|(index, entry)| (entry.name.clone(), index))
        .collect::<HashMap<_, _>>();
    for rust_entry in rust_entries {
        if let Some(index) = indexes.get(&rust_entry.name).copied() {
            helper_entries[index] = rust_entry;
        } else {
            indexes.insert(rust_entry.name.clone(), helper_entries.len());
            helper_entries.push(rust_entry);
        }
    }
    helper_entries
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

fn failure_from_bridge_error(
    error: HostBridgeError,
    operation: &str,
) -> CatalogFailure {
    let class = match &error {
        HostBridgeError::BridgeUnavailable(_) => "helper_unavailable",
        HostBridgeError::HelperNotFound(_) => "helper_not_found",
        _ => "helper_error",
    }
    .to_string();
    CatalogFailure {
        class,
        message: format!("{operation} helper failed: {error}"),
    }
}

fn plugin_metadata_env_present() -> bool {
    env::var_os(SASE_XPROMPT_PLUGIN_DIRS_JSON_ENV).is_some()
        || env::var_os(SASE_XPROMPT_PLUGIN_CONFIG_PATHS_JSON_ENV).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sase_core::{
        EditorSnippetCatalogRequestWire, EditorSnippetCatalogResponseWire,
        EditorSnippetCatalogStatsWire, EditorSnippetEntryWire,
        HelperHostBridge, HostBridgeError, MobileHelperProjectContextWire,
        MobileHelperProjectScopeWire, MobileHelperResultWire,
        MobileHelperStatusWire, MobileXpromptCatalogEntryWire,
        MobileXpromptCatalogRequestWire, MobileXpromptCatalogResponseWire,
        MobileXpromptCatalogStatsWire,
    };
    use std::fs;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct FixtureBridge {
        entry_name: String,
    }

    impl HelperHostBridge for FixtureBridge {
        fn xprompt_catalog(
            &self,
            _request: &MobileXpromptCatalogRequestWire,
        ) -> Result<MobileXpromptCatalogResponseWire, HostBridgeError> {
            Ok(catalog_response(&self.entry_name))
        }
    }

    #[derive(Debug)]
    struct UnavailableBridge;

    impl HelperHostBridge for UnavailableBridge {}

    #[derive(Debug)]
    struct SnippetFixtureBridge {
        trigger: String,
    }

    impl HelperHostBridge for SnippetFixtureBridge {
        fn snippet_catalog(
            &self,
            _request: &EditorSnippetCatalogRequestWire,
        ) -> Result<EditorSnippetCatalogResponseWire, HostBridgeError> {
            Ok(snippet_response(&self.trigger))
        }
    }

    #[derive(Debug)]
    struct FailingAfterFirstSnippetBridge {
        calls: Mutex<u32>,
    }

    impl HelperHostBridge for FailingAfterFirstSnippetBridge {
        fn snippet_catalog(
            &self,
            _request: &EditorSnippetCatalogRequestWire,
        ) -> Result<EditorSnippetCatalogResponseWire, HostBridgeError> {
            let mut calls = self.calls.lock().unwrap();
            *calls += 1;
            if *calls == 1 {
                Ok(snippet_response("cached"))
            } else {
                Err(HostBridgeError::BridgeUnavailable(
                    "helper_bridge".to_string(),
                ))
            }
        }
    }

    fn catalog_response(name: &str) -> MobileXpromptCatalogResponseWire {
        MobileXpromptCatalogResponseWire {
            schema_version: 1,
            result: MobileHelperResultWire {
                status: MobileHelperStatusWire::Success,
                message: None,
                warnings: Vec::new(),
                skipped: Vec::new(),
                partial_failure_count: None,
            },
            context: MobileHelperProjectContextWire {
                project: None,
                scope: MobileHelperProjectScopeWire::AllKnown,
            },
            entries: vec![MobileXpromptCatalogEntryWire {
                name: name.to_string(),
                display_label: name.to_string(),
                insertion: Some(format!("#{name}")),
                reference_prefix: Some("#".to_string()),
                kind: Some("xprompt".to_string()),
                description: None,
                source_bucket: "plugin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: Vec::new(),
                is_skill: false,
                content_preview: Some("body".to_string()),
                source_path_display: None,
                definition_path: None,
                definition_range: None,
            }],
            stats: MobileXpromptCatalogStatsWire {
                total_count: 1,
                project_count: 0,
                skill_count: 0,
                pdf_requested: false,
            },
            catalog_attachment: None,
        }
    }

    fn snippet_response(trigger: &str) -> EditorSnippetCatalogResponseWire {
        EditorSnippetCatalogResponseWire {
            schema_version: 1,
            result: MobileHelperResultWire {
                status: MobileHelperStatusWire::Success,
                message: None,
                warnings: Vec::new(),
                skipped: Vec::new(),
                partial_failure_count: None,
            },
            context: MobileHelperProjectContextWire {
                project: None,
                scope: MobileHelperProjectScopeWire::AllKnown,
            },
            entries: vec![EditorSnippetEntryWire {
                trigger: trigger.to_string(),
                template: format!("{trigger} $1$0"),
                source: "user_config".to_string(),
                xprompt_name: None,
                description: None,
                source_path_display: Some("ace.snippets".to_string()),
            }],
            stats: EditorSnippetCatalogStatsWire { total_count: 1 },
        }
    }

    #[tokio::test]
    async fn snippet_cache_refreshes_from_helper() {
        let cache = CatalogCache::new(Arc::new(SnippetFixtureBridge {
            trigger: "fix".to_string(),
        }));

        let entries = cache
            .refresh_snippets_for_completion(
                "test".to_string(),
                Some("sase".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(entries[0].trigger, "fix");
        assert!(!cache.snippets_stale_or_missing("test"));
        assert_eq!(
            cache.cached_snippet_entries("test").unwrap()[0].template,
            "fix $1$0"
        );
    }

    #[tokio::test]
    async fn snippet_cache_returns_stale_entries_on_helper_failure() {
        let cache =
            CatalogCache::new(Arc::new(FailingAfterFirstSnippetBridge {
                calls: Mutex::new(0),
            }));

        let first = cache
            .refresh_snippets_for_completion("test".to_string(), None)
            .await
            .unwrap();
        let second = cache
            .refresh_snippets_for_completion("test".to_string(), None)
            .await
            .unwrap();

        assert_eq!(first[0].trigger, "cached");
        assert_eq!(second[0].trigger, "cached");
    }

    fn root_with_rust_entry() -> tempfile::TempDir {
        let temp = tempfile::tempdir().unwrap();
        let xprompts = temp.path().join("xprompts");
        fs::create_dir(&xprompts).unwrap();
        fs::write(xprompts.join("rust_builtin.md"), "rust body").unwrap();
        temp
    }

    #[tokio::test]
    async fn direct_launch_without_plugin_metadata_merges_helper_and_rust_catalogs(
    ) {
        let temp = root_with_rust_entry();
        let cache = CatalogCache::new_with_rust_catalog_and_plugin_metadata(
            Arc::new(FixtureBridge {
                entry_name: "helper_plugin".to_string(),
            }),
            false,
        );

        let entries = cache
            .refresh_for_completion(
                "test".to_string(),
                None,
                Some(temp.path().to_path_buf()),
            )
            .await
            .unwrap();

        assert!(entries.iter().any(|entry| entry.name == "helper_plugin"));
        assert!(entries.iter().any(|entry| entry.name == "rust_builtin"));
    }

    #[tokio::test]
    async fn wrapper_launch_with_plugin_metadata_uses_fast_rust_catalog() {
        let temp = root_with_rust_entry();
        let cache = CatalogCache::new_with_rust_catalog_and_plugin_metadata(
            Arc::new(FixtureBridge {
                entry_name: "helper_plugin".to_string(),
            }),
            true,
        );

        let entries = cache
            .refresh_for_completion(
                "test".to_string(),
                None,
                Some(temp.path().to_path_buf()),
            )
            .await
            .unwrap();

        assert!(entries.iter().any(|entry| entry.name == "rust_builtin"));
        assert!(!entries.iter().any(|entry| entry.name == "helper_plugin"));
    }

    #[tokio::test]
    async fn direct_launch_keeps_rust_catalog_when_helper_unavailable() {
        let temp = root_with_rust_entry();
        let cache = CatalogCache::new_with_rust_catalog_and_plugin_metadata(
            Arc::new(UnavailableBridge),
            false,
        );

        let entries = cache
            .refresh_for_completion(
                "test".to_string(),
                None,
                Some(temp.path().to_path_buf()),
            )
            .await
            .unwrap();

        assert!(entries.iter().any(|entry| entry.name == "rust_builtin"));
    }
}
