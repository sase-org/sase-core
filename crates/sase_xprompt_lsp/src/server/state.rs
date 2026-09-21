use super::initialize::{
    artifact_ref_catalog_path, glossary_catalog_path, machine_catalog_path,
    model_catalog_path, queue_capacity_budget_from_env,
    typed_launch_units_from_env, vcs_project_catalog_path,
};
use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ServerConfig {
    pub(super) root_dir: Option<PathBuf>,
    pub(super) project: Option<String>,
    pub(super) catalog_key: String,
    pub(super) snippet_support: bool,
    pub(super) allow_all_markdown: bool,
    /// Path to the materialized `vcs_project` completion catalog, captured from
    /// [`VCS_PROJECT_CATALOG_ENV`] at startup. The file itself is re-read fresh
    /// on each `+` completion request (see [`load_vcs_project_catalog`]).
    pub(super) vcs_project_catalog: Option<PathBuf>,
    /// Path to the materialized `%model` completion catalog, captured from
    /// [`MODEL_CATALOG_ENV`] at startup. The file itself is re-read fresh on
    /// each `%model` argument completion request.
    pub(super) model_catalog: Option<PathBuf>,
    /// Path to the materialized `%dispatch` machine catalog. Re-read fresh on
    /// each dispatch argument completion request.
    pub(super) machine_catalog: Option<PathBuf>,
    /// Path to the launcher-materialized artifact-reference catalog. The file
    /// and its enumerated payload inventories are cached briefly; path metadata
    /// changes and explicit refreshes invalidate the cache immediately.
    pub(super) artifact_ref_catalog: Option<PathBuf>,
    /// Path to the launcher-materialized project glossary catalog. Parsed
    /// catalogs are cached briefly and invalidated by file signature, explicit
    /// refresh, or watched project config changes.
    pub(super) glossary_catalog: Option<PathBuf>,
    /// Startup-resolved `typed_launch_units` flag. Never re-read on keystrokes.
    pub(super) typed_launch_units: bool,
    /// Startup-resolved `queue_capacity_budget` sunset flag. Defaults on.
    pub(super) queue_capacity_budget: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            root_dir: std::env::current_dir().ok(),
            project: None,
            catalog_key: "default".to_string(),
            snippet_support: false,
            allow_all_markdown: false,
            vcs_project_catalog: vcs_project_catalog_path(),
            model_catalog: model_catalog_path(),
            machine_catalog: machine_catalog_path(),
            artifact_ref_catalog: artifact_ref_catalog_path(),
            glossary_catalog: glossary_catalog_path(),
            typed_launch_units: typed_launch_units_from_env(),
            queue_capacity_budget: queue_capacity_budget_from_env(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct VcsProjectCatalog {
    pub(super) entries: Vec<VcsProjectEntry>,
    pub(super) workflow_names: Vec<String>,
    pub(super) namespaces: HashMap<String, Vec<VcsNamespaceEntry>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct ArtifactRefCatalog {
    pub(super) default_project: Option<String>,
    pub(super) projects: Vec<ArtifactRefCatalogProject>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(super) struct ArtifactRefCatalogProject {
    pub(super) name: String,
    pub(super) key: String,
    #[serde(default)]
    pub(super) aliases: Vec<String>,
    pub(super) context: ArtifactRefContextWire,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ArtifactRefCatalogSignature {
    pub(super) path: Option<PathBuf>,
    pub(super) modified: Option<SystemTime>,
    pub(super) len: u64,
}

#[derive(Debug)]
pub(super) struct CachedArtifactRefPayload {
    pub(super) index: AtReferencePayloadIndex,
    pub(super) truncated_payloads: usize,
}

#[derive(Debug, Default)]
pub(super) struct ArtifactRefCache {
    pub(super) signature: Option<ArtifactRefCatalogSignature>,
    pub(super) loaded_at: Option<Instant>,
    pub(super) catalog: ArtifactRefCatalog,
    pub(super) payloads:
        HashMap<(String, String), Arc<CachedArtifactRefPayload>>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct GlossaryCatalog {
    pub(super) default_project: Option<String>,
    pub(super) projects: Vec<GlossaryCatalogProject>,
}

#[derive(Debug, Clone)]
pub(super) struct GlossaryCatalogProject {
    pub(super) key: String,
    pub(super) name: String,
    pub(super) aliases: Vec<String>,
    pub(super) config_path: String,
    pub(super) catalog: Arc<CompiledGlossaryCatalog>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(super) struct GlossaryCatalogProjectPayload {
    pub(super) schema_version: u32,
    pub(super) project: GlossaryCatalogProjectIdentity,
    pub(super) config_path: String,
    pub(super) entries: Vec<GlossaryEntryWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(super) struct GlossaryCatalogProjectIdentity {
    pub(super) key: String,
    pub(super) name: String,
    #[serde(default)]
    pub(super) aliases: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GlossaryCatalogSignature {
    pub(super) path: Option<PathBuf>,
    pub(super) modified: Option<SystemTime>,
    pub(super) len: u64,
}

#[derive(Debug, Default)]
pub(super) struct GlossaryCache {
    pub(super) signature: Option<GlossaryCatalogSignature>,
    pub(super) loaded_at: Option<Instant>,
    pub(super) catalog: GlossaryCatalog,
}

#[derive(Debug, Clone)]
pub(super) struct OpenDocument {
    pub(super) text: String,
    pub(super) language_id: String,
    pub(super) eligible: bool,
    pub(super) recent_paren_insertion: Option<RecentParenInsertion>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RecentParenInsertion {
    pub(super) opener_idx: usize,
    pub(super) closer_idx: Option<usize>,
}

#[derive(Debug)]
pub struct XpromptLspServer {
    pub(super) client: Client,
    pub(super) documents: RwLock<HashMap<String, OpenDocument>>,
    pub(super) catalog_cache: Arc<CatalogCache>,
    pub(super) artifact_ref_cache: RwLock<ArtifactRefCache>,
    pub(super) glossary_cache: RwLock<GlossaryCache>,
    pub(super) config: RwLock<ServerConfig>,
}

impl XpromptLspServer {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            documents: RwLock::new(HashMap::new()),
            catalog_cache: Arc::new(CatalogCache::command_backed()),
            artifact_ref_cache: RwLock::new(ArtifactRefCache::default()),
            glossary_cache: RwLock::new(GlossaryCache::default()),
            config: RwLock::new(ServerConfig::default()),
        }
    }

    pub fn with_bridge(
        client: Client,
        bridge: Arc<dyn HelperHostBridge>,
    ) -> Self {
        Self {
            client,
            documents: RwLock::new(HashMap::new()),
            catalog_cache: Arc::new(CatalogCache::new(bridge)),
            artifact_ref_cache: RwLock::new(ArtifactRefCache::default()),
            glossary_cache: RwLock::new(GlossaryCache::default()),
            config: RwLock::new(ServerConfig::default()),
        }
    }
}
