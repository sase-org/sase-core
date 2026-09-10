use super::{
    collection_problem_is_attentive, freshness_value, reset_has_passed,
    validate_ident, validate_now, validate_usage_cadence,
    validate_usage_thresholds, validate_used_percent, window_attention,
    ProviderUsageError, Result, UsageApplicabilityWire, UsageAttentionKind,
    UsageCollectionOutcome, UsageFreshness, UsagePublicProviderWire,
    UsagePublicSnapshotWire, UsagePublicWindowWire, UsageVendorState,
    MAX_KEY_LEN, MAX_PROVIDER_LEN, PERIOD_TOLERANCE_SECONDS,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet};

pub const PROVIDER_USAGE_INDICATOR_SCHEMA_VERSION: u32 = 1;

const DEFAULT_BELOW_REMAINING_PERCENT: f64 = 20.0;
const WEEK_SECONDS: f64 = 7.0 * 24.0 * 60.0 * 60.0;
const MONTH_SECONDS: f64 = 30.0 * 24.0 * 60.0 * 60.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageIndicatorPolicyKind {
    Always,
    Never,
    BelowRemainingPercent,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum UsageIndicatorPolicyWire {
    Always,
    Never,
    BelowRemainingPercent { below_remaining_percent: f64 },
}

impl UsageIndicatorPolicyWire {
    fn selects(&self, remaining_percent: f64) -> bool {
        match self {
            Self::Always => true,
            Self::Never => false,
            Self::BelowRemainingPercent {
                below_remaining_percent,
            } => remaining_percent < *below_remaining_percent,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageIndicatorProviderConfigWire {
    #[serde(default)]
    pub default: Option<UsageIndicatorPolicyWire>,
    #[serde(default)]
    pub windows: BTreeMap<String, UsageIndicatorPolicyWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageIndicatorConfigWire {
    pub enabled: bool,
    pub default: UsageIndicatorPolicyWire,
    pub weekly_all: UsageIndicatorPolicyWire,
    #[serde(default)]
    pub providers: BTreeMap<String, UsageIndicatorProviderConfigWire>,
}

impl Default for UsageIndicatorConfigWire {
    fn default() -> Self {
        Self {
            enabled: true,
            default: UsageIndicatorPolicyWire::BelowRemainingPercent {
                below_remaining_percent: DEFAULT_BELOW_REMAINING_PERCENT,
            },
            weekly_all: UsageIndicatorPolicyWire::Always,
            providers: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageIndicatorDiagnosticWire {
    pub path: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageIndicatorConfigValidationWire {
    pub schema_version: u32,
    pub config: UsageIndicatorConfigWire,
    pub diagnostics: Vec<UsageIndicatorDiagnosticWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageIndicatorProjectionRequestWire {
    pub schema_version: u32,
    pub snapshot: UsagePublicSnapshotWire,
    #[serde(default)]
    pub indicator: Option<JsonValue>,
    #[serde(default)]
    pub eligible_providers: Option<Vec<String>>,
    pub now: f64,
    #[serde(default = "default_usage_cadence_seconds")]
    pub cadence_seconds: f64,
    #[serde(default = "default_usage_warn_percent")]
    pub warn_percent: f64,
    #[serde(default = "default_usage_critical_percent")]
    pub critical_percent: f64,
}

fn default_usage_cadence_seconds() -> f64 {
    super::DEFAULT_USAGE_CADENCE_SECONDS
}

fn default_usage_warn_percent() -> f64 {
    super::DEFAULT_USAGE_WARN_PERCENT
}

fn default_usage_critical_percent() -> f64 {
    super::DEFAULT_USAGE_CRITICAL_PERCENT
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageIndicatorPolicySource {
    Window,
    ProviderDefault,
    WeeklyAll,
    Default,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageIndicatorScopeKind {
    AllModels,
    Product,
    Models,
    ModelFamily,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageIndicatorScopeWire {
    pub kind: UsageIndicatorScopeKind,
    pub product: Option<String>,
    pub family: Option<String>,
    pub model_ids: Vec<String>,
    pub vendor_label: Option<String>,
    pub vendor_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageIndicatorPeriodKind {
    Session,
    Weekly,
    Monthly,
    Duration,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageIndicatorPeriodWire {
    pub kind: UsageIndicatorPeriodKind,
    pub duration_seconds: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageIndicatorResetState {
    Future,
    Passed,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageIndicatorProviderStatusWire {
    pub provider: String,
    pub collection_status: UsageCollectionOutcome,
    pub collection_reason: Option<super::UsageReasonCode>,
    pub diagnostic: Option<String>,
    pub collector_health: Option<super::UsageCollectorHealthWire>,
    pub collector_problem: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageIndicatorWindowEntryWire {
    pub provider: String,
    pub context_ref: String,
    pub window_key: String,
    pub window_label: String,
    pub effective_policy: UsageIndicatorPolicyWire,
    pub policy_source: UsageIndicatorPolicySource,
    pub weekly_all: bool,
    pub period: UsageIndicatorPeriodWire,
    pub scope: UsageIndicatorScopeWire,
    pub used_percent: f64,
    pub remaining_percent: f64,
    pub exceeded_by_percent: Option<f64>,
    pub freshness: UsageFreshness,
    pub reset_state: UsageIndicatorResetState,
    pub seconds_until_reset: Option<f64>,
    pub resets_at: Option<f64>,
    pub duration_seconds: Option<f64>,
    pub period_start: Option<f64>,
    pub age_seconds: f64,
    pub observed_at: f64,
    pub vendor_state: UsageVendorState,
    pub window_attention: UsageAttentionKind,
    pub display_attention: UsageAttentionKind,
    pub collector_problem: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageIndicatorProjectionWire {
    pub schema_version: u32,
    pub generated_at: f64,
    pub enabled: bool,
    pub diagnostics: Vec<UsageIndicatorDiagnosticWire>,
    pub providers: Vec<UsageIndicatorProviderStatusWire>,
    pub entries: Vec<UsageIndicatorWindowEntryWire>,
}

pub fn validate_usage_indicator_config(
    raw: Option<JsonValue>,
) -> UsageIndicatorConfigValidationWire {
    normalize_usage_indicator_config(raw)
}

pub fn project_usage_indicator(
    request: UsageIndicatorProjectionRequestWire,
) -> Result<UsageIndicatorProjectionWire> {
    if request.schema_version != PROVIDER_USAGE_INDICATOR_SCHEMA_VERSION {
        return Err(validation(format!(
            "unsupported provider-usage indicator schema version: {}",
            request.schema_version
        )));
    }
    validate_now(request.now)?;
    let cadence_seconds = validate_usage_cadence(request.cadence_seconds)?;
    let (warn_percent, critical_percent) = validate_usage_thresholds(
        request.warn_percent,
        request.critical_percent,
    )?;
    let validation = normalize_usage_indicator_config(request.indicator);
    let config = validation.config;
    let mut diagnostics = validation.diagnostics;
    let eligible = eligible_provider_set(request.eligible_providers)?;
    let mut providers = Vec::new();
    let mut entries = Vec::new();

    for provider in &request.snapshot.providers {
        if !provider_is_eligible(provider.provider.as_str(), &eligible) {
            continue;
        }
        let provider_status = provider_status(provider, request.now);
        let collector_problem = provider_status.collector_problem;
        providers.push(provider_status);
        if !config.enabled {
            continue;
        }
        project_provider_entries(
            provider,
            &config,
            request.now,
            cadence_seconds,
            warn_percent,
            critical_percent,
            collector_problem,
            &mut diagnostics,
            &mut entries,
        );
    }
    sort_entries(&mut entries);
    mark_first_collector_problem_entries(&mut entries);
    Ok(UsageIndicatorProjectionWire {
        schema_version: PROVIDER_USAGE_INDICATOR_SCHEMA_VERSION,
        generated_at: request.now,
        enabled: config.enabled,
        diagnostics,
        providers,
        entries,
    })
}

fn normalize_usage_indicator_config(
    raw: Option<JsonValue>,
) -> UsageIndicatorConfigValidationWire {
    let mut config = UsageIndicatorConfigWire::default();
    let mut diagnostics = Vec::new();
    let Some(raw) = raw else {
        return config_validation(config, diagnostics);
    };
    let Some(map) = raw.as_object() else {
        diagnostics.push(diagnostic(
            "indicator",
            "must be an object; using built-in indicator defaults",
        ));
        return config_validation(config, diagnostics);
    };
    for key in map.keys() {
        if !matches!(
            key.as_str(),
            "enabled" | "default" | "weekly_all" | "providers"
        ) {
            diagnostics.push(diagnostic(
                format!("indicator.{key}"),
                "unknown indicator setting was ignored",
            ));
        }
    }
    if let Some(value) = map.get("enabled") {
        if let Some(enabled) = value.as_bool() {
            config.enabled = enabled;
        } else {
            diagnostics.push(diagnostic(
                "indicator.enabled",
                "must be a boolean; using built-in default true",
            ));
        }
    }
    if let Some(value) = map.get("default") {
        if let Some(policy) =
            parse_policy(value, "indicator.default", &mut diagnostics)
        {
            config.default = policy;
        }
    }
    if let Some(value) = map.get("weekly_all") {
        if let Some(policy) =
            parse_policy(value, "indicator.weekly_all", &mut diagnostics)
        {
            config.weekly_all = policy;
        }
    }
    if let Some(value) = map.get("providers") {
        parse_provider_configs(value, &mut config, &mut diagnostics);
    }
    config_validation(config, diagnostics)
}

fn config_validation(
    config: UsageIndicatorConfigWire,
    diagnostics: Vec<UsageIndicatorDiagnosticWire>,
) -> UsageIndicatorConfigValidationWire {
    UsageIndicatorConfigValidationWire {
        schema_version: PROVIDER_USAGE_INDICATOR_SCHEMA_VERSION,
        config,
        diagnostics,
    }
}

fn parse_provider_configs(
    value: &JsonValue,
    config: &mut UsageIndicatorConfigWire,
    diagnostics: &mut Vec<UsageIndicatorDiagnosticWire>,
) {
    let Some(providers) = value.as_object() else {
        diagnostics.push(diagnostic(
            "indicator.providers",
            "must be an object; provider overrides were ignored",
        ));
        return;
    };
    for (provider, value) in providers {
        let path = format!("indicator.providers.{provider}");
        let Ok(provider_id) = validate_ident(&path, provider, MAX_PROVIDER_LEN)
        else {
            diagnostics.push(diagnostic(
                path,
                "provider id must be non-empty text without controls; override was ignored",
            ));
            continue;
        };
        let Some(provider_map) = value.as_object() else {
            diagnostics.push(diagnostic(
                format!("indicator.providers.{provider_id}"),
                "must be an object; provider override was ignored",
            ));
            continue;
        };
        for key in provider_map.keys() {
            if !matches!(key.as_str(), "default" | "windows") {
                diagnostics.push(diagnostic(
                    format!("indicator.providers.{provider_id}.{key}"),
                    "unknown provider indicator setting was ignored",
                ));
            }
        }
        let mut provider_config = UsageIndicatorProviderConfigWire {
            default: None,
            windows: BTreeMap::new(),
        };
        if let Some(default) = provider_map.get("default") {
            provider_config.default = parse_policy(
                default,
                format!("indicator.providers.{provider_id}.default"),
                diagnostics,
            );
        }
        if let Some(windows) = provider_map.get("windows") {
            parse_window_configs(
                &provider_id,
                windows,
                &mut provider_config,
                diagnostics,
            );
        }
        config.providers.insert(provider_id, provider_config);
    }
}

fn parse_window_configs(
    provider: &str,
    value: &JsonValue,
    provider_config: &mut UsageIndicatorProviderConfigWire,
    diagnostics: &mut Vec<UsageIndicatorDiagnosticWire>,
) {
    let Some(windows) = value.as_object() else {
        diagnostics.push(diagnostic(
            format!("indicator.providers.{provider}.windows"),
            "must be an object; window overrides were ignored",
        ));
        return;
    };
    for (window_key, policy_value) in windows {
        let path =
            format!("indicator.providers.{provider}.windows.{window_key}");
        let Ok(window_key) = validate_ident(&path, window_key, MAX_KEY_LEN)
        else {
            diagnostics.push(diagnostic(
                path,
                "window key must be non-empty text without controls; override was ignored",
            ));
            continue;
        };
        if let Some(policy) = parse_policy(policy_value, &path, diagnostics) {
            provider_config.windows.insert(window_key, policy);
        }
    }
}

fn parse_policy(
    value: &JsonValue,
    path: impl Into<String>,
    diagnostics: &mut Vec<UsageIndicatorDiagnosticWire>,
) -> Option<UsageIndicatorPolicyWire> {
    let path = path.into();
    if let Some(text) = value.as_str() {
        return match text {
            "always" => Some(UsageIndicatorPolicyWire::Always),
            "never" => Some(UsageIndicatorPolicyWire::Never),
            _ => {
                diagnostics.push(diagnostic(
                    path,
                    "policy string must be 'always' or 'never'; override was ignored",
                ));
                None
            }
        };
    }
    let Some(map) = value.as_object() else {
        diagnostics.push(diagnostic(
            path,
            "policy must be 'always', 'never', or {below_remaining_percent: N}; override was ignored",
        ));
        return None;
    };
    if map.len() != 1 || !map.contains_key("below_remaining_percent") {
        diagnostics.push(diagnostic(
            path,
            "policy object must contain exactly below_remaining_percent; override was ignored",
        ));
        return None;
    }
    let threshold = &map["below_remaining_percent"];
    let Some(threshold) = threshold.as_f64() else {
        diagnostics.push(diagnostic(
            format!("{path}.below_remaining_percent"),
            "must be a finite number from 0 through 100; override was ignored",
        ));
        return None;
    };
    if !(0.0..=100.0).contains(&threshold) {
        diagnostics.push(diagnostic(
            format!("{path}.below_remaining_percent"),
            "must be from 0 through 100; override was ignored",
        ));
        return None;
    }
    Some(UsageIndicatorPolicyWire::BelowRemainingPercent {
        below_remaining_percent: threshold,
    })
}

fn diagnostic(
    path: impl Into<String>,
    message: impl Into<String>,
) -> UsageIndicatorDiagnosticWire {
    UsageIndicatorDiagnosticWire {
        path: path.into(),
        message: message.into(),
    }
}

fn eligible_provider_set(
    eligible_providers: Option<Vec<String>>,
) -> Result<Option<BTreeSet<String>>> {
    let Some(providers) = eligible_providers else {
        return Ok(None);
    };
    let mut set = BTreeSet::new();
    for provider in providers {
        set.insert(validate_ident(
            "eligible_providers provider id",
            &provider,
            MAX_PROVIDER_LEN,
        )?);
    }
    Ok(Some(set))
}

fn provider_is_eligible(
    provider: &str,
    eligible: &Option<BTreeSet<String>>,
) -> bool {
    eligible
        .as_ref()
        .map(|providers| providers.contains(provider))
        .unwrap_or(true)
}

fn provider_status(
    provider: &UsagePublicProviderWire,
    now: f64,
) -> UsageIndicatorProviderStatusWire {
    let collector_problem =
        provider.collector_health.as_ref().is_some_and(|health| {
            health.state == super::UsageCollectorHealthState::Failing
        }) || (provider.collection_status != UsageCollectionOutcome::Ok
            && provider.windows.is_empty())
            || (provider.attention.kind
                == UsageAttentionKind::CollectionProblem)
            || collection_problem_is_attentive(
                provider.collection_status,
                provider.collector_health.as_ref(),
                &provider.windows,
                &provider.summary,
            );
    let _ = now;
    UsageIndicatorProviderStatusWire {
        provider: provider.provider.clone(),
        collection_status: provider.collection_status,
        collection_reason: provider.collection_reason,
        diagnostic: provider.diagnostic.clone(),
        collector_health: provider.collector_health.clone(),
        collector_problem,
    }
}

#[allow(clippy::too_many_arguments)]
fn project_provider_entries(
    provider: &UsagePublicProviderWire,
    config: &UsageIndicatorConfigWire,
    now: f64,
    cadence_seconds: f64,
    warn_percent: f64,
    critical_percent: f64,
    collector_problem: bool,
    diagnostics: &mut Vec<UsageIndicatorDiagnosticWire>,
    entries: &mut Vec<UsageIndicatorWindowEntryWire>,
) {
    for window in &provider.windows {
        let path = format!(
            "snapshot.providers.{}.windows.{}",
            provider.provider, window.key
        );
        if let Err(error) = validate_projected_window(window, now) {
            diagnostics.push(diagnostic(path, error.to_string()));
            continue;
        }
        let weekly_all = is_weekly_all_window(&provider.provider, window);
        let (policy, policy_source) =
            resolve_policy(config, &provider.provider, &window.key, weekly_all);
        if !policy.selects(window.remaining_percent) {
            continue;
        }
        let freshness =
            freshness_value(window.observed_at, now, cadence_seconds);
        let reset_state = reset_state(window.resets_at, now);
        let seconds_until_reset = seconds_until_reset(window.resets_at, now);
        let window_attention = window_attention(
            &UsagePublicWindowWire {
                freshness,
                reset_passed: reset_has_passed(window.resets_at, now),
                age_seconds: (now - window.observed_at.min(now)).max(0.0),
                ..window.clone()
            },
            warn_percent,
            critical_percent,
        );
        let display_attention = if collector_problem {
            max_attention(
                window_attention,
                UsageAttentionKind::CollectionProblem,
            )
        } else {
            window_attention
        };
        entries.push(UsageIndicatorWindowEntryWire {
            provider: provider.provider.clone(),
            context_ref: provider.context_ref.clone(),
            window_key: window.key.clone(),
            window_label: window.label.clone(),
            effective_policy: policy.clone(),
            policy_source,
            weekly_all,
            period: classify_period(&provider.provider, window),
            scope: classify_scope(&provider.provider, window),
            used_percent: window.used_percent,
            remaining_percent: window.remaining_percent,
            exceeded_by_percent: window.exceeded_by_percent,
            freshness,
            reset_state,
            seconds_until_reset,
            resets_at: window.resets_at,
            duration_seconds: window.duration_seconds,
            period_start: window.period_start,
            age_seconds: (now - window.observed_at.min(now)).max(0.0),
            observed_at: window.observed_at,
            vendor_state: window.vendor_state,
            window_attention,
            display_attention,
            collector_problem: false,
        });
    }
}

fn validate_projected_window(
    window: &UsagePublicWindowWire,
    now: f64,
) -> std::result::Result<(), ProviderUsageError> {
    validate_ident("window key", &window.key, MAX_KEY_LEN)?;
    validate_used_percent(window.used_percent)?;
    if !window.remaining_percent.is_finite() || window.remaining_percent < 0.0 {
        return Err(validation(
            "remaining_percent must be finite and nonnegative",
        ));
    }
    if !window.observed_at.is_finite() || window.observed_at <= 0.0 {
        return Err(validation("observed_at must be finite and positive"));
    }
    if window.observed_at > now + super::MAX_FUTURE_SKEW_SECONDS {
        return Err(validation(
            "observed_at is implausibly ahead of the injected clock",
        ));
    }
    if let Some(reset) = window.resets_at {
        if !reset.is_finite() || reset <= 0.0 {
            return Err(validation("resets_at must be finite and positive"));
        }
    }
    if let Some(duration) = window.duration_seconds {
        if !duration.is_finite() || duration <= 0.0 {
            return Err(validation(
                "duration_seconds must be finite and positive",
            ));
        }
    }
    Ok(())
}

fn resolve_policy(
    config: &UsageIndicatorConfigWire,
    provider: &str,
    window_key: &str,
    weekly_all: bool,
) -> (UsageIndicatorPolicyWire, UsageIndicatorPolicySource) {
    if let Some(provider_config) = config.providers.get(provider) {
        if let Some(policy) = provider_config.windows.get(window_key) {
            return (policy.clone(), UsageIndicatorPolicySource::Window);
        }
        if let Some(policy) = &provider_config.default {
            return (
                policy.clone(),
                UsageIndicatorPolicySource::ProviderDefault,
            );
        }
    }
    if weekly_all {
        return (
            config.weekly_all.clone(),
            UsageIndicatorPolicySource::WeeklyAll,
        );
    }
    (config.default.clone(), UsageIndicatorPolicySource::Default)
}

fn classify_period(
    provider: &str,
    window: &UsagePublicWindowWire,
) -> UsageIndicatorPeriodWire {
    let key = window.key.as_str();
    let kind = if provider == "claude" && key == "session" {
        UsageIndicatorPeriodKind::Session
    } else if is_weekly_window(provider, window) {
        UsageIndicatorPeriodKind::Weekly
    } else if is_monthly_window(provider, window) {
        UsageIndicatorPeriodKind::Monthly
    } else if window.duration_seconds.is_some() {
        UsageIndicatorPeriodKind::Duration
    } else {
        UsageIndicatorPeriodKind::Unknown
    };
    UsageIndicatorPeriodWire {
        kind,
        duration_seconds: window.duration_seconds,
    }
}

fn classify_scope(
    provider: &str,
    window: &UsagePublicWindowWire,
) -> UsageIndicatorScopeWire {
    if is_all_model_scope(provider, window) {
        return UsageIndicatorScopeWire {
            kind: UsageIndicatorScopeKind::AllModels,
            product: product_name(&window.applicability),
            family: None,
            model_ids: Vec::new(),
            vendor_label: None,
            vendor_id: None,
        };
    }
    match &window.applicability {
        UsageApplicabilityWire::Account => UsageIndicatorScopeWire {
            kind: UsageIndicatorScopeKind::AllModels,
            product: None,
            family: None,
            model_ids: Vec::new(),
            vendor_label: None,
            vendor_id: None,
        },
        UsageApplicabilityWire::Product { product, model_ids } => {
            UsageIndicatorScopeWire {
                kind: UsageIndicatorScopeKind::Product,
                product: Some(product.clone()),
                family: None,
                model_ids: model_ids.clone(),
                vendor_label: None,
                vendor_id: None,
            }
        }
        UsageApplicabilityWire::Models { model_ids } => {
            UsageIndicatorScopeWire {
                kind: UsageIndicatorScopeKind::Models,
                product: None,
                family: None,
                model_ids: model_ids.clone(),
                vendor_label: None,
                vendor_id: None,
            }
        }
        UsageApplicabilityWire::ModelFamily { family, model_ids } => {
            UsageIndicatorScopeWire {
                kind: UsageIndicatorScopeKind::ModelFamily,
                product: None,
                family: Some(family.clone()),
                model_ids: model_ids.clone(),
                vendor_label: None,
                vendor_id: None,
            }
        }
        UsageApplicabilityWire::Unknown {
            vendor_label,
            vendor_id,
        } => UsageIndicatorScopeWire {
            kind: UsageIndicatorScopeKind::Unknown,
            product: None,
            family: None,
            model_ids: Vec::new(),
            vendor_label: vendor_label.clone(),
            vendor_id: vendor_id.clone(),
        },
    }
}

fn product_name(applicability: &UsageApplicabilityWire) -> Option<String> {
    match applicability {
        UsageApplicabilityWire::Product { product, .. } => {
            Some(product.clone())
        }
        _ => None,
    }
}

fn is_weekly_all_window(
    provider: &str,
    window: &UsagePublicWindowWire,
) -> bool {
    is_all_model_scope(provider, window) && is_weekly_window(provider, window)
}

fn is_weekly_window(provider: &str, window: &UsagePublicWindowWire) -> bool {
    if duration_matches(window.duration_seconds, WEEK_SECONDS) == Some(true) {
        return true;
    }
    if duration_matches(window.duration_seconds, WEEK_SECONDS) == Some(false) {
        return false;
    }
    let key = window.key.as_str();
    (provider == "claude"
        && (key == "weekly" || key.starts_with("weekly:"))
        && is_claude_product_scope(window))
        || (provider == "grok"
            && key == "included_weekly"
            && matches!(window.applicability, UsageApplicabilityWire::Account))
}

fn is_monthly_window(provider: &str, window: &UsagePublicWindowWire) -> bool {
    if duration_matches(window.duration_seconds, MONTH_SECONDS) == Some(true) {
        return true;
    }
    if duration_matches(window.duration_seconds, MONTH_SECONDS) == Some(false) {
        return false;
    }
    provider == "grok"
        && window.key == "included_monthly"
        && matches!(window.applicability, UsageApplicabilityWire::Account)
}

fn duration_matches(duration: Option<f64>, expected: f64) -> Option<bool> {
    duration
        .map(|duration| (duration - expected).abs() <= PERIOD_TOLERANCE_SECONDS)
}

fn is_all_model_scope(provider: &str, window: &UsagePublicWindowWire) -> bool {
    match &window.applicability {
        UsageApplicabilityWire::Account => true,
        UsageApplicabilityWire::Product { product, model_ids } => {
            provider == "claude"
                && product == "claude"
                && model_ids.is_empty()
                && matches!(window.key.as_str(), "session" | "weekly")
        }
        _ => false,
    }
}

fn is_claude_product_scope(window: &UsagePublicWindowWire) -> bool {
    matches!(
        &window.applicability,
        UsageApplicabilityWire::Product { product, .. } if product == "claude"
    )
}

fn reset_state(resets_at: Option<f64>, now: f64) -> UsageIndicatorResetState {
    match resets_at {
        None => UsageIndicatorResetState::Unknown,
        Some(reset) if now >= reset => UsageIndicatorResetState::Passed,
        Some(_) => UsageIndicatorResetState::Future,
    }
}

fn seconds_until_reset(resets_at: Option<f64>, now: f64) -> Option<f64> {
    resets_at.map(|reset| (reset - now).max(0.0))
}

fn max_attention(
    left: UsageAttentionKind,
    right: UsageAttentionKind,
) -> UsageAttentionKind {
    if right.rank() > left.rank() {
        right
    } else {
        left
    }
}

fn sort_entries(entries: &mut [UsageIndicatorWindowEntryWire]) {
    entries.sort_by(|left, right| {
        right
            .display_attention
            .rank()
            .cmp(&left.display_attention.rank())
            .then_with(|| left.provider.cmp(&right.provider))
            .then_with(|| right.weekly_all.cmp(&left.weekly_all))
            .then_with(|| left.window_key.cmp(&right.window_key))
    });
}

fn mark_first_collector_problem_entries(
    entries: &mut [UsageIndicatorWindowEntryWire],
) {
    let mut seen = BTreeSet::new();
    for entry in entries {
        if entry.display_attention.rank()
            < UsageAttentionKind::CollectionProblem.rank()
        {
            continue;
        }
        if seen.insert(entry.provider.clone()) {
            entry.collector_problem = true;
        }
    }
}

fn validation(message: impl Into<String>) -> ProviderUsageError {
    ProviderUsageError::Validation(message.into())
}
