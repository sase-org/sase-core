use super::state::ServerConfig;
use super::*;

pub(super) fn config_from_initialize(
    params: &InitializeParams,
) -> ServerConfig {
    #[allow(deprecated)]
    let root_uri_dir = params
        .root_uri
        .as_ref()
        .and_then(|uri| uri.to_file_path().map(|path| path.into_owned()));
    let root_dir = params
        .workspace_folders
        .as_ref()
        .and_then(|folders| folders.first())
        .and_then(|folder| {
            folder.uri.to_file_path().map(|path| path.into_owned())
        })
        .or(root_uri_dir)
        .or_else(|| std::env::current_dir().ok());
    let project = root_dir
        .as_deref()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .map(str::to_string);
    let catalog_key = root_dir
        .as_ref()
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|| "default".to_string());
    ServerConfig {
        root_dir,
        project,
        catalog_key,
        snippet_support: snippet_support(&params.capabilities),
        allow_all_markdown: params
            .initialization_options
            .as_ref()
            .and_then(|options| options.get("allow_all_markdown"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        vcs_project_catalog: vcs_project_catalog_path(),
        model_catalog: model_catalog_path(),
        machine_catalog: machine_catalog_path(),
        artifact_ref_catalog: artifact_ref_catalog_path(),
        glossary_catalog: glossary_catalog_path(),
        typed_launch_units: typed_launch_units_from_initialize(params)
            .unwrap_or_else(typed_launch_units_from_env),
        queue_capacity_budget: queue_capacity_budget_from_initialize(params)
            .unwrap_or_else(queue_capacity_budget_from_env),
    }
}

pub(super) fn typed_launch_units_from_initialize(
    params: &InitializeParams,
) -> Option<bool> {
    params
        .initialization_options
        .as_ref()
        .and_then(|options| options.get("typed_launch_units"))
        .and_then(serde_json::Value::as_bool)
}

pub(super) fn typed_launch_units_from_env() -> bool {
    std::env::var(TYPED_LAUNCH_UNITS_ENV)
        .ok()
        .as_deref()
        .map(env_flag_enabled)
        .unwrap_or(false)
}

pub(super) fn queue_capacity_budget_from_initialize(
    params: &InitializeParams,
) -> Option<bool> {
    params
        .initialization_options
        .as_ref()
        .and_then(|options| options.get("queue_capacity_budget"))
        .and_then(serde_json::Value::as_bool)
}

pub(super) fn queue_capacity_budget_from_env() -> bool {
    std::env::var(QUEUE_CAPACITY_BUDGET_ENV)
        .ok()
        .as_deref()
        .map(env_flag_enabled)
        .unwrap_or(true)
}

pub(super) fn env_flag_enabled(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

pub(super) fn enabled_feature_flags(
    typed_launch_units: bool,
    queue_capacity_budget: bool,
) -> Vec<String> {
    let mut flags = Vec::new();
    if typed_launch_units {
        flags.push("typed_launch_units".to_string());
    }
    if queue_capacity_budget {
        flags.push("queue_capacity_budget".to_string());
    }
    flags
}

pub(super) fn vcs_project_catalog_path() -> Option<PathBuf> {
    std::env::var_os(VCS_PROJECT_CATALOG_ENV).map(PathBuf::from)
}

pub(super) fn model_catalog_path() -> Option<PathBuf> {
    std::env::var_os(MODEL_CATALOG_ENV).map(PathBuf::from)
}

pub(super) fn machine_catalog_path() -> Option<PathBuf> {
    std::env::var_os(MACHINE_CATALOG_ENV).map(PathBuf::from)
}

pub(super) fn artifact_ref_catalog_path() -> Option<PathBuf> {
    std::env::var_os(ARTIFACT_REF_CATALOG_ENV).map(PathBuf::from)
}

pub(super) fn glossary_catalog_path() -> Option<PathBuf> {
    std::env::var_os(GLOSSARY_CATALOG_ENV).map(PathBuf::from)
}

pub(super) fn snippet_support(capabilities: &ClientCapabilities) -> bool {
    capabilities
        .text_document
        .as_ref()
        .and_then(|text| text.completion.as_ref())
        .and_then(|completion| completion.completion_item.as_ref())
        .and_then(|item| item.snippet_support)
        .unwrap_or(false)
}
