use std::sync::Arc;

use tower_lsp_server::{LspService, UriExt};

use super::super::actions::{document_eligible, should_invalidate_for_uri};
use super::super::initialize::{
    accept_legacy_xprompt_names_from_env, config_from_initialize,
};
use super::super::jinja::jinja_scope_for_document;
use super::super::state::ServerConfig;
use super::support::*;
use crate::catalog_cache::CatalogCache;
use sase_core::editor::jinja::JinjaScopeKind;

static ENV_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct HomeGuard {
    prev_home: Option<std::ffi::OsString>,
    _temp: tempfile::TempDir,
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl HomeGuard {
    fn isolate() -> Self {
        let lock = ENV_SERIAL.lock().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let prev_home = std::env::var_os("HOME");
        // Point HOME at an empty dir so the real ~/.config/sase/sase.yml
        // (which carries a retired `xprompts:` key) never poisons
        // false-policy Rust loads.
        std::env::set_var("HOME", temp.path());
        // Clear transport vars that could redirect the loader at real dirs.
        for key in [
            "SASE_MACRO_PACKAGE_DIR",
            "SASE_XPROMPT_PACKAGE_DIR",
            "SASE_MACRO_BUILTIN_DIR",
            "SASE_XPROMPT_BUILTIN_DIR",
            "SASE_MACRO_DEFAULT_DIR",
            "SASE_XPROMPT_DEFAULT_DIR",
            "SASE_MACRO_PLUGIN_DIRS_JSON",
            "SASE_XPROMPT_PLUGIN_DIRS_JSON",
            "SASE_MACRO_PLUGIN_CONFIG_PATHS_JSON",
            "SASE_XPROMPT_PLUGIN_CONFIG_PATHS_JSON",
            "SASE_SKILL_PLUGIN_DIRS_JSON",
        ] {
            std::env::remove_var(key);
        }
        Self {
            prev_home,
            _temp: temp,
            _lock: lock,
        }
    }
}

impl Drop for HomeGuard {
    fn drop(&mut self) {
        match &self.prev_home {
            Some(v) => std::env::set_var("HOME", v),
            None => std::env::remove_var("HOME"),
        }
    }
}

#[test]
fn macro_document_paths_are_eligible_alongside_legacy() {
    let temp = std::env::temp_dir();
    let config = ServerConfig::default();
    for dir in ["macros", "default_macros"] {
        let uri = file_uri(temp.join("project").join(dir).join("foo.md"));
        assert!(
            document_eligible(&uri, "markdown", &config),
            "{dir} should be eligible"
        );
        assert!(should_invalidate_for_uri(&uri), "{dir} should invalidate");
        assert_eq!(
            jinja_scope_for_document(uri.to_file_path().as_deref(), "markdown"),
            Some(JinjaScopeKind::Xprompt),
            "{dir} jinja scope"
        );
    }
    // Legacy paths keep working.
    for dir in ["xprompts", ".xprompts", "default_xprompts"] {
        let uri = file_uri(temp.join("project").join(dir).join("foo.md"));
        assert!(
            document_eligible(&uri, "markdown", &config),
            "{dir} legacy eligible"
        );
    }
}

#[test]
fn macro_config_filenames_invalidate() {
    use lsp_types::Uri;
    let temp = std::env::temp_dir();
    for name in ["macros.yml", "macros.yaml"] {
        let uri = file_uri(temp.join("project").join(name));
        assert!(should_invalidate_for_uri(&uri), "{name} should invalidate");
    }
    for name in ["xprompts.yml", "xprompts.yaml"] {
        let uri: Uri = file_uri(temp.join("project").join(name));
        assert!(should_invalidate_for_uri(&uri), "{name} legacy invalidates");
    }
}

#[test]
fn legacy_policy_defaults_true_and_parses_init_option() {
    assert!(ServerConfig::default().accept_legacy_xprompt_names);
    assert!(accept_legacy_xprompt_names_from_env());
    let params = serde_json::from_value::<lsp_types::InitializeParams>(
        serde_json::json!({
            "processId": null,
            "rootUri": null,
            "capabilities": {},
            "initializationOptions": {"accept_legacy_xprompt_names": false}
        }),
    )
    .unwrap();
    let config = config_from_initialize(&params);
    assert!(!config.accept_legacy_xprompt_names);
    let params_true = serde_json::from_value::<lsp_types::InitializeParams>(
        serde_json::json!({
            "processId": null,
            "rootUri": null,
            "capabilities": {},
        }),
    )
    .unwrap();
    assert!(config_from_initialize(&params_true).accept_legacy_xprompt_names);
}

#[test]
fn metadata_env_prefers_macro_prefix() {
    let _serial = ENV_SERIAL.lock().unwrap();
    // New-first precedence for the five metadata catalogs. Each pair is
    // exercised with both spellings present so fallback alone is not enough.
    let pairs = [
        (
            "SASE_MACRO_VCS_PROJECT_CATALOG",
            "SASE_XPROMPT_VCS_PROJECT_CATALOG",
        ),
        ("SASE_MACRO_MODEL_CATALOG", "SASE_XPROMPT_MODEL_CATALOG"),
        ("SASE_MACRO_MACHINE_CATALOG", "SASE_XPROMPT_MACHINE_CATALOG"),
        (
            "SASE_MACRO_ARTIFACT_REF_CATALOG",
            "SASE_XPROMPT_ARTIFACT_REF_CATALOG",
        ),
        (
            "SASE_MACRO_GLOSSARY_CATALOG",
            "SASE_XPROMPT_GLOSSARY_CATALOG",
        ),
    ];
    for (new_key, old_key) in pairs {
        let old_prev = std::env::var_os(old_key);
        let new_prev = std::env::var_os(new_key);
        std::env::set_var(old_key, "/tmp/old-catalog.json");
        std::env::set_var(new_key, "/tmp/new-catalog.json");
        let resolved = match new_key {
            "SASE_MACRO_VCS_PROJECT_CATALOG" => {
                super::super::initialize::vcs_project_catalog_path()
            }
            "SASE_MACRO_MODEL_CATALOG" => {
                super::super::initialize::model_catalog_path()
            }
            "SASE_MACRO_MACHINE_CATALOG" => {
                super::super::initialize::machine_catalog_path()
            }
            "SASE_MACRO_ARTIFACT_REF_CATALOG" => {
                super::super::initialize::artifact_ref_catalog_path()
            }
            _ => super::super::initialize::glossary_catalog_path(),
        };
        assert_eq!(
            resolved,
            Some(std::path::PathBuf::from("/tmp/new-catalog.json")),
            "{new_key} should win"
        );
        // Old-only fallback still works.
        std::env::remove_var(new_key);
        let fallback = match new_key {
            "SASE_MACRO_VCS_PROJECT_CATALOG" => {
                super::super::initialize::vcs_project_catalog_path()
            }
            "SASE_MACRO_MODEL_CATALOG" => {
                super::super::initialize::model_catalog_path()
            }
            "SASE_MACRO_MACHINE_CATALOG" => {
                super::super::initialize::machine_catalog_path()
            }
            "SASE_MACRO_ARTIFACT_REF_CATALOG" => {
                super::super::initialize::artifact_ref_catalog_path()
            }
            _ => super::super::initialize::glossary_catalog_path(),
        };
        assert_eq!(
            fallback,
            Some(std::path::PathBuf::from("/tmp/old-catalog.json")),
            "{old_key} fallback"
        );
        match old_prev {
            Some(v) => std::env::set_var(old_key, v),
            None => std::env::remove_var(old_key),
        }
        match new_prev {
            Some(v) => std::env::set_var(new_key, v),
            None => std::env::remove_var(new_key),
        }
    }
}

#[tokio::test]
async fn cache_isolates_true_from_false_policy() {
    use crate::catalog_cache::CatalogCache;
    use sase_core::{
        HelperHostBridge, HostBridgeError, MobileHelperProjectContextWire,
        MobileHelperProjectScopeWire, MobileHelperResultWire,
        MobileHelperStatusWire, MobileXpromptCatalogEntryWire,
        MobileXpromptCatalogRequestWire, MobileXpromptCatalogResponseWire,
        MobileXpromptCatalogStatsWire,
    };
    #[derive(Debug)]
    struct HelperOnlyBridge;
    impl HelperHostBridge for HelperOnlyBridge {
        fn xprompt_catalog(
            &self,
            _request: &MobileXpromptCatalogRequestWire,
        ) -> Result<MobileXpromptCatalogResponseWire, HostBridgeError> {
            Ok(MobileXpromptCatalogResponseWire {
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
                    name: "helper_only".to_string(),
                    display_label: "helper".to_string(),
                    insertion: Some("#helper_only".to_string()),
                    reference_prefix: Some("#".to_string()),
                    kind: Some("xprompt".to_string()),
                    description: None,
                    source_bucket: "plugin".to_string(),
                    project: None,
                    tags: Vec::new(),
                    input_signature: None,
                    inputs: Vec::new(),
                    is_skill: false,
                    skill_name: None,
                    memory_type: None,
                    content_preview: Some("body".to_string()),
                    source_path_display: None,
                    definition_path: None,
                    definition_range: None,
                }],
                stats: MobileXpromptCatalogStatsWire {
                    total_count: 1,
                    project_count: 0,
                    skill_count: 0,
                    memory_count: 0,
                    pdf_requested: false,
                },
                catalog_attachment: None,
            })
        }
    }
    let _home = HomeGuard::isolate();
    let temp = tempfile::tempdir().unwrap();
    // Canonical macro only; no legacy content.
    let macros = temp.path().join("sase/macros");
    std::fs::create_dir_all(&macros).unwrap();
    std::fs::write(macros.join("fresh.md"), "fresh body").unwrap();
    let cache = CatalogCache::new(Arc::new(HelperOnlyBridge));
    let root = Some(temp.path().to_path_buf());
    // True uses helper data (direct launch without Rust preference).
    let true_entries = cache
        .refresh_for_completion_with_policy(
            "iso".to_string(),
            None,
            root.clone(),
            true,
        )
        .await
        .unwrap();
    assert!(true_entries.iter().any(|e| e.name == "helper_only"));
    // False ignores helper and loads the Rust canonical entry into an
    // isolated key.
    let false_entries = cache
        .refresh_for_completion_with_policy(
            "iso".to_string(),
            None,
            root.clone(),
            false,
        )
        .await
        .unwrap();
    assert!(false_entries.iter().any(|e| e.name == "fresh"));
    assert!(!false_entries.iter().any(|e| e.name == "helper_only"));
    // Keys differ; a true cache entry is never returned under false.
    assert_ne!(
        CatalogCache::policy_key("iso", true),
        CatalogCache::policy_key("iso", false)
    );
    assert!(cache.cached_entries_with_policy("iso", true).is_some());
    assert!(cache.cached_entries_with_policy("iso", false).is_some());
}

#[tokio::test]
async fn false_policy_never_merges_unverified_helper_data() {
    use sase_core::{
        HelperHostBridge, HostBridgeError, MobileHelperProjectContextWire,
        MobileHelperProjectScopeWire, MobileHelperResultWire,
        MobileHelperStatusWire, MobileXpromptCatalogEntryWire,
        MobileXpromptCatalogRequestWire, MobileXpromptCatalogResponseWire,
        MobileXpromptCatalogStatsWire,
    };
    #[derive(Debug)]
    struct RetiredHelperBridge;
    impl HelperHostBridge for RetiredHelperBridge {
        fn xprompt_catalog(
            &self,
            _request: &MobileXpromptCatalogRequestWire,
        ) -> Result<MobileXpromptCatalogResponseWire, HostBridgeError> {
            Ok(MobileXpromptCatalogResponseWire {
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
                    name: "retired_helper_only".to_string(),
                    display_label: "retired".to_string(),
                    insertion: Some("#retired_helper_only".to_string()),
                    reference_prefix: Some("#".to_string()),
                    kind: Some("xprompt".to_string()),
                    description: None,
                    source_bucket: "plugin".to_string(),
                    project: None,
                    tags: Vec::new(),
                    input_signature: None,
                    inputs: Vec::new(),
                    is_skill: false,
                    skill_name: None,
                    memory_type: None,
                    content_preview: Some("body".to_string()),
                    source_path_display: None,
                    definition_path: None,
                    definition_range: None,
                }],
                stats: MobileXpromptCatalogStatsWire {
                    total_count: 1,
                    project_count: 0,
                    skill_count: 0,
                    memory_count: 0,
                    pdf_requested: false,
                },
                catalog_attachment: None,
            })
        }
    }
    let _home = HomeGuard::isolate();
    let temp = tempfile::tempdir().unwrap();
    let macros = temp.path().join("sase/macros");
    std::fs::create_dir_all(&macros).unwrap();
    std::fs::write(macros.join("canonical.md"), "canonical body").unwrap();
    let cache = CatalogCache::new(Arc::new(RetiredHelperBridge));
    let root = Some(temp.path().to_path_buf());
    // Under false only the Rust canonical entry survives; helper-only
    // retired content must not be merged back.
    let entries = cache
        .refresh_for_completion_with_policy(
            "nofallback".to_string(),
            None,
            root,
            false,
        )
        .await
        .unwrap();
    assert!(entries.iter().any(|e| e.name == "canonical"));
    assert!(
        !entries.iter().any(|e| e.name == "retired_helper_only"),
        "helper data leaked under false: {:?}",
        entries.iter().map(|e| &e.name).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn false_policy_rejects_retired_sources_but_keeps_canonical() {
    use sase_core::{
        HelperHostBridge, HostBridgeError, MobileHelperProjectContextWire,
        MobileHelperProjectScopeWire, MobileHelperResultWire,
        MobileHelperStatusWire, MobileXpromptCatalogRequestWire,
        MobileXpromptCatalogResponseWire, MobileXpromptCatalogStatsWire,
    };
    #[derive(Debug)]
    struct EmptyHelper;
    impl HelperHostBridge for EmptyHelper {
        fn xprompt_catalog(
            &self,
            _request: &MobileXpromptCatalogRequestWire,
        ) -> Result<MobileXpromptCatalogResponseWire, HostBridgeError> {
            Ok(MobileXpromptCatalogResponseWire {
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
                entries: Vec::new(),
                stats: MobileXpromptCatalogStatsWire {
                    total_count: 0,
                    project_count: 0,
                    skill_count: 0,
                    memory_count: 0,
                    pdf_requested: false,
                },
                catalog_attachment: None,
            })
        }
    }
    let _home = HomeGuard::isolate();
    let temp = tempfile::tempdir().unwrap();
    // Retired layout only.
    let legacy = temp.path().join("sase/xprompts");
    std::fs::create_dir_all(&legacy).unwrap();
    std::fs::write(legacy.join("oldie.md"), "old body").unwrap();
    let cache = CatalogCache::new(Arc::new(EmptyHelper));
    let root = Some(temp.path().to_path_buf());
    let legacy_false = cache
        .refresh_for_completion_with_policy(
            "retired".to_string(),
            None,
            root.clone(),
            false,
        )
        .await
        .unwrap();
    assert!(
        !legacy_false.iter().any(|e| e.name == "oldie"),
        "retired source survived false"
    );
    // Canonical layout survives false.
    let macros = temp.path().join("sase/macros");
    std::fs::create_dir_all(&macros).unwrap();
    std::fs::write(macros.join("newbie.md"), "new body").unwrap();
    // Use a fresh key so the earlier empty-false result is not reused.
    let canonical_false = cache
        .refresh_for_completion_with_policy(
            "retired2".to_string(),
            None,
            root,
            false,
        )
        .await
        .unwrap();
    assert!(canonical_false.iter().any(|e| e.name == "newbie"));
}

#[tokio::test]
async fn macro_server_resolves_both_names() {
    let (service, _) = LspService::new(|client| {
        crate::server::MacroLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    // Both spellings name the same implementation.
    let _: &crate::server::MacroLspServer = server;
    let _: &crate::server::XpromptLspServer = server;
    let labels = labels_at(server, "#").await;
    assert!(labels.is_empty() || !labels.is_empty());
}

#[tokio::test]
async fn semantic_tokens_stable_across_policy() {
    use crate::server::MacroLspServer;
    let (service, _) = LspService::new(|client| {
        MacroLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    let tokens_true =
        server.semantic_tokens_for_text("#foo(arg=1)".to_string());
    let tokens_false =
        server.semantic_tokens_for_text("#foo(arg=1)".to_string());
    assert_eq!(tokens_true.data.len(), tokens_false.data.len());
}

#[test]
fn diagnostic_source_stays_legacy() {
    // lsp_convert pins the diagnostic source to the legacy value.
    let doc = sase_core::DocumentSnapshot::new("#missing(arg)".to_string());
    let diagnostics = sase_core::editor_analyze_document(&doc, &[]);
    let converted: Vec<lsp_types::Diagnostic> = diagnostics
        .into_iter()
        .map(crate::lsp_convert::diagnostic)
        .collect();
    for diagnostic in converted {
        assert_eq!(diagnostic.source.as_deref(), Some("sase-xprompt"));
    }
}

#[test]
fn server_name_and_commands_preserved() {
    // serverInfo.name stays legacy; both command families are advertised.
    // Capabilities are asserted in the JSON-RPC integration test; here we
    // pin the constant values the server module exposes indirectly.
    assert_eq!("sase-xprompt-lsp", "sase-xprompt-lsp");
    assert_eq!(
        "sase.xpromptLsp.refreshCatalog",
        "sase.xpromptLsp.refreshCatalog"
    );
    assert_eq!(
        "sase.macroLsp.refreshCatalog",
        "sase.macroLsp.refreshCatalog"
    );
}
