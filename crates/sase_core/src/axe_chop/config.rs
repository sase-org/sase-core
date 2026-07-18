use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::config::ConfigDiagnosticWire;

use super::wire::{
    AxeConfigValidationRequestWire, ChopEngineError, ChopForEachConfigWire,
    CHOP_ENGINE_SCHEMA_VERSION,
};

const AXE_KEYS: &[&str] = &[
    "max_hook_runners",
    "max_agent_runners",
    "zombie_timeout_seconds",
    "lumberjack_log_max_bytes",
    "verbose_lumberjack_diagnostics",
    "query",
    "chop_script_dirs",
    "lumberjacks",
];
const LUMBERJACK_KEYS: &[&str] = &["interval", "chop_timeout", "chops", "env"];
const CHOP_KEYS: &[&str] = &[
    "name",
    "script",
    "description",
    "enabled",
    "run_every",
    "timeout",
    "env",
    "inhibit_if",
    "trigger",
    "once_per",
    "for_each",
    "vars",
];

/// Parse a positive duration with `d`, `h`, `m`, and `s` compound units.
pub fn parse_chop_duration(value: &str) -> Result<u64, ChopEngineError> {
    parse_duration_at(value, "$")
}

fn parse_duration_at(value: &str, path: &str) -> Result<u64, ChopEngineError> {
    if value.is_empty() {
        return Err(duration_error(path, value));
    }
    let bytes = value.as_bytes();
    let mut index = 0;
    let mut total = 0_u64;
    let mut previous_rank = 5_u8;
    while index < bytes.len() {
        let number_start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        if number_start == index || index >= bytes.len() {
            return Err(duration_error(path, value));
        }
        let amount: u64 = value[number_start..index]
            .parse()
            .map_err(|_| duration_error(path, value))?;
        let (rank, multiplier) = match bytes[index] {
            b'd' => (4, 86_400_u64),
            b'h' => (3, 3_600_u64),
            b'm' => (2, 60_u64),
            b's' => (1, 1_u64),
            _ => return Err(duration_error(path, value)),
        };
        if rank >= previous_rank {
            return Err(ChopEngineError::new(
                "invalid_duration",
                path,
                format!(
                    "duration `{value}` must use each unit at most once in d/h/m/s order"
                ),
            ));
        }
        previous_rank = rank;
        total = total
            .checked_add(amount.checked_mul(multiplier).ok_or_else(|| {
                ChopEngineError::new(
                    "duration_overflow",
                    path,
                    format!("duration `{value}` is too large"),
                )
            })?)
            .ok_or_else(|| {
                ChopEngineError::new(
                    "duration_overflow",
                    path,
                    format!("duration `{value}` is too large"),
                )
            })?;
        index += 1;
    }
    if total == 0 {
        return Err(ChopEngineError::new(
            "non_positive_duration",
            path,
            "duration must be positive",
        ));
    }
    Ok(total)
}

fn duration_error(path: &str, value: &str) -> ChopEngineError {
    ChopEngineError::new(
        "invalid_duration",
        path,
        format!(
            "invalid duration `{value}`; expected a positive compound duration such as `30s`, `1h30m`, or `1d2h`"
        ),
    )
}

/// Strictly validate the deterministic shape of an axe config section.
pub fn validate_axe_config(
    request: &AxeConfigValidationRequestWire,
) -> Result<Vec<ConfigDiagnosticWire>, ChopEngineError> {
    if request.schema_version != CHOP_ENGINE_SCHEMA_VERSION {
        return Err(ChopEngineError::new(
            "schema_version_mismatch",
            "$.schema_version",
            format!(
                "got {}, expected {CHOP_ENGINE_SCHEMA_VERSION}",
                request.schema_version
            ),
        ));
    }
    let (axe, base) = if request.config.get("axe").is_some() {
        (&request.config["axe"], "axe")
    } else {
        (&request.config, "")
    };
    let Some(axe) = axe.as_object() else {
        return Ok(vec![diagnostic(
            request,
            "type_mismatch",
            base,
            "axe config must be an object",
        )]);
    };
    let mut diagnostics = Vec::new();
    unknown_keys(request, axe, AXE_KEYS, base, &mut diagnostics);
    for key in [
        "max_hook_runners",
        "max_agent_runners",
        "zombie_timeout_seconds",
        "lumberjack_log_max_bytes",
    ] {
        if let Some(value) = axe.get(key) {
            validate_positive_integer(
                request,
                value,
                &child_path(base, key),
                &mut diagnostics,
            );
        }
    }
    validate_optional_type(
        request,
        axe.get("verbose_lumberjack_diagnostics"),
        &child_path(base, "verbose_lumberjack_diagnostics"),
        "boolean",
        Value::is_boolean,
        &mut diagnostics,
    );
    validate_optional_type(
        request,
        axe.get("query"),
        &child_path(base, "query"),
        "string",
        Value::is_string,
        &mut diagnostics,
    );
    if let Some(value) = axe.get("chop_script_dirs") {
        validate_string_array(
            request,
            value,
            &child_path(base, "chop_script_dirs"),
            &mut diagnostics,
        );
    }
    if let Some(value) = axe.get("lumberjacks") {
        validate_lumberjacks(
            request,
            value,
            &child_path(base, "lumberjacks"),
            &mut diagnostics,
        );
    }
    diagnostics.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then_with(|| left.code.cmp(&right.code))
    });
    Ok(diagnostics)
}

fn validate_lumberjacks(
    request: &AxeConfigValidationRequestWire,
    value: &Value,
    path: &str,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    let Some(lumberjacks) = value.as_object() else {
        diagnostics.push(diagnostic(
            request,
            "type_mismatch",
            path,
            "lumberjacks must be an object keyed by name",
        ));
        return;
    };
    for (name, config) in lumberjacks {
        let lumberjack_path = child_path(path, name);
        if name.trim().is_empty() {
            diagnostics.push(diagnostic(
                request,
                "blank_value",
                &lumberjack_path,
                "lumberjack name must not be blank",
            ));
        }
        let Some(config) = config.as_object() else {
            diagnostics.push(diagnostic(
                request,
                "type_mismatch",
                &lumberjack_path,
                "lumberjack config must be an object",
            ));
            continue;
        };
        unknown_keys(
            request,
            config,
            LUMBERJACK_KEYS,
            &lumberjack_path,
            diagnostics,
        );
        if let Some(interval) = config.get("interval") {
            validate_positive_integer(
                request,
                interval,
                &child_path(&lumberjack_path, "interval"),
                diagnostics,
            );
        }
        if let Some(timeout) = config.get("chop_timeout") {
            validate_duration(
                request,
                timeout,
                &child_path(&lumberjack_path, "chop_timeout"),
                diagnostics,
            );
        }
        if let Some(env) = config.get("env") {
            validate_env(
                request,
                env,
                &child_path(&lumberjack_path, "env"),
                diagnostics,
            );
        }
        if let Some(chops) = config.get("chops") {
            validate_chops(
                request,
                chops,
                &child_path(&lumberjack_path, "chops"),
                diagnostics,
            );
        }
    }
}

fn validate_chops(
    request: &AxeConfigValidationRequestWire,
    value: &Value,
    path: &str,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    match value {
        Value::Array(chops) => {
            let mut identities = BTreeSet::new();
            for (index, chop) in chops.iter().enumerate() {
                let chop_path = format!("{path}[{index}]");
                match chop {
                    Value::String(name) => {
                        validate_identity(
                            request,
                            name,
                            &chop_path,
                            &mut identities,
                            diagnostics,
                        );
                    }
                    Value::Object(config) => {
                        let identity =
                            config.get("name").and_then(Value::as_str);
                        if let Some(name) = identity {
                            validate_identity(
                                request,
                                name,
                                &child_path(&chop_path, "name"),
                                &mut identities,
                                diagnostics,
                            );
                        } else {
                            diagnostics.push(diagnostic(
                                request,
                                "required_missing",
                                &child_path(&chop_path, "name"),
                                "list-form chop objects require a string `name`",
                            ));
                        }
                        validate_chop_config(
                            request,
                            config,
                            &chop_path,
                            None,
                            diagnostics,
                        );
                    }
                    _ => diagnostics.push(diagnostic(
                        request,
                        "type_mismatch",
                        &chop_path,
                        "list-form chops must be strings or objects",
                    )),
                }
            }
        }
        Value::Object(chops) => {
            for (name, config) in chops {
                let chop_path = child_path(path, name);
                if name.trim().is_empty() {
                    diagnostics.push(diagnostic(
                        request,
                        "blank_value",
                        &chop_path,
                        "chop identity must not be blank",
                    ));
                }
                let Some(config) = config.as_object() else {
                    diagnostics.push(diagnostic(
                        request,
                        "type_mismatch",
                        &chop_path,
                        "map-form chop config must be an object",
                    ));
                    continue;
                };
                validate_chop_config(
                    request,
                    config,
                    &chop_path,
                    Some(name),
                    diagnostics,
                );
            }
        }
        _ => diagnostics.push(diagnostic(
            request,
            "type_mismatch",
            path,
            "chops must be a list or a map keyed by chop name",
        )),
    }
}

fn validate_identity(
    request: &AxeConfigValidationRequestWire,
    name: &str,
    path: &str,
    identities: &mut BTreeSet<String>,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    if name.trim().is_empty() {
        diagnostics.push(diagnostic(
            request,
            "blank_value",
            path,
            "chop identity must not be blank",
        ));
    } else if !identities.insert(name.to_string()) {
        diagnostics.push(diagnostic(
            request,
            "duplicate_chop_identity",
            path,
            &format!("duplicate chop identity `{name}`"),
        ));
    }
}

fn validate_chop_config(
    request: &AxeConfigValidationRequestWire,
    config: &Map<String, Value>,
    path: &str,
    map_name: Option<&str>,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    for migration_key in ["agent", "xprompt"] {
        if config.contains_key(migration_key) {
            diagnostics.push(diagnostic(
                request,
                "agent_chop_removed",
                &child_path(path, migration_key),
                &format!(
                    "`{migration_key}` agent chops are no longer supported; use a script that writes structured `proposed_launches`"
                ),
            ));
        }
    }
    let mut allowed_chop_keys = CHOP_KEYS.to_vec();
    // These receive the more actionable migration diagnostic above instead
    // of a redundant generic unknown-key error.
    allowed_chop_keys.extend(["agent", "xprompt"]);
    unknown_keys(request, config, &allowed_chop_keys, path, diagnostics);
    if let (Some(expected), Some(actual)) =
        (map_name, config.get("name").and_then(Value::as_str))
    {
        if expected != actual {
            diagnostics.push(diagnostic(
                request,
                "chop_identity_mismatch",
                &child_path(path, "name"),
                &format!(
                    "map-form chop name `{actual}` does not match key `{expected}`"
                ),
            ));
        }
    }
    for key in ["name", "script"] {
        if let Some(value) = config.get(key) {
            validate_nonblank_string(
                request,
                value,
                &child_path(path, key),
                diagnostics,
            );
        }
    }
    if let Some(value) = config.get("description") {
        validate_optional_type(
            request,
            Some(value),
            &child_path(path, "description"),
            "string",
            Value::is_string,
            diagnostics,
        );
    }
    if let Some(value) = config.get("enabled") {
        validate_optional_type(
            request,
            Some(value),
            &child_path(path, "enabled"),
            "boolean",
            Value::is_boolean,
            diagnostics,
        );
    }
    for key in ["run_every", "timeout"] {
        if let Some(value) = config.get(key) {
            validate_duration(
                request,
                value,
                &child_path(path, key),
                diagnostics,
            );
        }
    }
    if let Some(value) = config.get("env") {
        validate_env(request, value, &child_path(path, "env"), diagnostics);
    }
    if let Some(value) = config.get("inhibit_if") {
        validate_guards(
            request,
            value,
            &child_path(path, "inhibit_if"),
            diagnostics,
        );
    }
    if let Some(value) = config.get("trigger") {
        validate_trigger(
            request,
            value,
            &child_path(path, "trigger"),
            diagnostics,
        );
    }
    if let Some(value) = config.get("once_per") {
        validate_once_per(
            request,
            value,
            &child_path(path, "once_per"),
            diagnostics,
        );
    }
    if let Some(value) = config.get("for_each") {
        if let Err(error) =
            serde_json::from_value::<ChopForEachConfigWire>(value.clone())
        {
            diagnostics.push(diagnostic(
                request,
                "invalid_for_each",
                &child_path(path, "for_each"),
                &error.to_string(),
            ));
        }
    }
    if let Some(value) = config.get("vars") {
        validate_optional_type(
            request,
            Some(value),
            &child_path(path, "vars"),
            "object",
            Value::is_object,
            diagnostics,
        );
    }
}

fn validate_guards(
    request: &AxeConfigValidationRequestWire,
    value: &Value,
    path: &str,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    if let Value::Array(guards) = value {
        for (index, guard) in guards.iter().enumerate() {
            let guard_path = format!("{path}[{index}]");
            let Some(config) = guard.as_object() else {
                diagnostics.push(diagnostic(
                    request,
                    "type_mismatch",
                    &guard_path,
                    "guard must be an object",
                ));
                continue;
            };
            let Some(provider) = config.get("provider").and_then(Value::as_str)
            else {
                diagnostics.push(diagnostic(
                    request,
                    "required_missing",
                    &child_path(&guard_path, "provider"),
                    "guard provider is required",
                ));
                continue;
            };
            validate_guard_provider(
                request,
                provider,
                config,
                &guard_path,
                true,
                diagnostics,
            );
        }
        return;
    }
    let Some(guards) = value.as_object() else {
        diagnostics.push(diagnostic(
            request,
            "type_mismatch",
            path,
            "inhibit_if must be a keyed object or provider list",
        ));
        return;
    };
    for (provider, raw_config) in guards {
        let (configs, indexed): (Vec<&Value>, bool) = match raw_config {
            Value::Array(items) => (items.iter().collect(), true),
            other => (vec![other], false),
        };
        for (index, raw_config) in configs.into_iter().enumerate() {
            let provider_path = if indexed {
                format!("{}[{index}]", child_path(path, provider))
            } else {
                child_path(path, provider)
            };
            let Some(config) = raw_config.as_object() else {
                diagnostics.push(diagnostic(
                    request,
                    "type_mismatch",
                    &provider_path,
                    "guard provider config must be an object",
                ));
                continue;
            };
            validate_guard_provider(
                request,
                provider,
                config,
                &provider_path,
                false,
                diagnostics,
            );
        }
    }
}

fn validate_guard_provider(
    request: &AxeConfigValidationRequestWire,
    provider: &str,
    config: &Map<String, Value>,
    path: &str,
    tagged: bool,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    let mut allowed = match provider {
        "changespec" => vec!["name_prefix", "statuses"],
        "agent_hood" => vec!["hood", "name"],
        _ => {
            diagnostics.push(diagnostic(
                request,
                "unknown_guard_provider",
                path,
                &format!(
                    "unknown guard provider `{provider}`; supported providers: changespec, agent_hood"
                ),
            ));
            return;
        }
    };
    if tagged {
        allowed.push("provider");
    }
    unknown_keys(request, config, &allowed, path, diagnostics);
    match provider {
        "changespec" => {
            if let Some(prefix) = config.get("name_prefix") {
                validate_optional_type(
                    request,
                    Some(prefix),
                    &child_path(path, "name_prefix"),
                    "string",
                    Value::is_string,
                    diagnostics,
                );
            }
            if let Some(statuses) = config.get("statuses") {
                validate_string_array(
                    request,
                    statuses,
                    &child_path(path, "statuses"),
                    diagnostics,
                );
                if let Some(statuses) = statuses.as_array() {
                    for (index, status) in statuses.iter().enumerate() {
                        if matches!(
                            status.as_str(),
                            Some("Submitted" | "Archived" | "Reverted")
                        ) {
                            diagnostics.push(diagnostic(
                                request,
                                "terminal_guard_status",
                                &format!("{}[{index}]", child_path(path, "statuses")),
                                "changespec guards may only select non-terminal statuses",
                            ));
                        }
                    }
                }
            }
        }
        "agent_hood" => match config.get("hood").or_else(|| config.get("name"))
        {
            Some(hood) => validate_nonblank_string(
                request,
                hood,
                &child_path(
                    path,
                    if config.contains_key("hood") {
                        "hood"
                    } else {
                        "name"
                    },
                ),
                diagnostics,
            ),
            None => diagnostics.push(diagnostic(
                request,
                "required_missing",
                &child_path(path, "hood"),
                "agent_hood guard requires `hood`",
            )),
        },
        _ => {}
    }
}

fn validate_trigger(
    request: &AxeConfigValidationRequestWire,
    value: &Value,
    path: &str,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    if let Some(provider) = value.as_str() {
        if provider != "always" {
            diagnostics.push(diagnostic(
                request,
                "unknown_trigger_provider",
                path,
                &format!(
                    "unknown trigger provider `{provider}`; supported providers: always, git.commits_since"
                ),
            ));
        }
        return;
    }
    let Some(config) = value.as_object() else {
        diagnostics.push(diagnostic(
            request,
            "type_mismatch",
            path,
            "trigger must be a provider string or object",
        ));
        return;
    };
    if let Some(provider) = config.get("provider").and_then(Value::as_str) {
        validate_trigger_provider(
            request,
            provider,
            config,
            path,
            true,
            diagnostics,
        );
        return;
    }
    if config.len() != 1 {
        diagnostics.push(diagnostic(
            request,
            "invalid_trigger",
            path,
            "keyed trigger config must contain exactly one provider",
        ));
        return;
    }
    let (provider, settings) = config.iter().next().expect("length checked");
    let empty = Map::new();
    let settings = if settings.is_null() {
        &empty
    } else if let Some(settings) = settings.as_object() {
        settings
    } else {
        diagnostics.push(diagnostic(
            request,
            "type_mismatch",
            &child_path(path, provider),
            "trigger provider config must be an object",
        ));
        return;
    };
    validate_trigger_provider(
        request,
        provider,
        settings,
        &child_path(path, provider),
        false,
        diagnostics,
    );
}

fn validate_trigger_provider(
    request: &AxeConfigValidationRequestWire,
    provider: &str,
    config: &Map<String, Value>,
    path: &str,
    tagged: bool,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    if provider == "always" {
        let allowed = if tagged { &["provider"][..] } else { &[][..] };
        unknown_keys(request, config, allowed, path, diagnostics);
        return;
    }
    if provider != "git.commits_since" {
        diagnostics.push(diagnostic(
            request,
            "unknown_trigger_provider",
            path,
            &format!(
                "unknown trigger provider `{provider}`; supported providers: always, git.commits_since"
            ),
        ));
        return;
    }
    let mut allowed =
        vec!["project", "threshold", "checkpoint_policy", "checkpoint"];
    if tagged {
        allowed.push("provider");
    }
    unknown_keys(request, config, &allowed, path, diagnostics);
    match config.get("project") {
        Some(value) => validate_nonblank_string(
            request,
            value,
            &child_path(path, "project"),
            diagnostics,
        ),
        None => diagnostics.push(diagnostic(
            request,
            "required_missing",
            &child_path(path, "project"),
            "git.commits_since requires `project`",
        )),
    }
    match config.get("threshold") {
        Some(value) => validate_positive_integer(
            request,
            value,
            &child_path(path, "threshold"),
            diagnostics,
        ),
        None => diagnostics.push(diagnostic(
            request,
            "required_missing",
            &child_path(path, "threshold"),
            "git.commits_since requires a positive `threshold`",
        )),
    }
    if let Some(policy) = config
        .get("checkpoint_policy")
        .or_else(|| config.get("checkpoint"))
    {
        if !matches!(
            policy.as_str(),
            Some("on_observation" | "on_action_accepted" | "on_action_success")
        ) {
            diagnostics.push(diagnostic(
                request,
                "invalid_checkpoint_policy",
                &child_path(path, "checkpoint_policy"),
                "checkpoint policy must be on_observation, on_action_accepted, or on_action_success",
            ));
        }
    }
}

fn validate_once_per(
    request: &AxeConfigValidationRequestWire,
    value: &Value,
    path: &str,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    if value.is_string() {
        validate_nonblank_string(request, value, path, diagnostics);
        return;
    }
    let Some(config) = value.as_object() else {
        diagnostics.push(diagnostic(
            request,
            "type_mismatch",
            path,
            "once_per must be a key template string or object",
        ));
        return;
    };
    unknown_keys(request, config, &["key", "capacity"], path, diagnostics);
    match config.get("key") {
        Some(key) => validate_nonblank_string(
            request,
            key,
            &child_path(path, "key"),
            diagnostics,
        ),
        None => diagnostics.push(diagnostic(
            request,
            "required_missing",
            &child_path(path, "key"),
            "once_per object requires `key`",
        )),
    }
    if let Some(capacity) = config.get("capacity") {
        validate_positive_integer(
            request,
            capacity,
            &child_path(path, "capacity"),
            diagnostics,
        );
    }
}

fn validate_env(
    request: &AxeConfigValidationRequestWire,
    value: &Value,
    path: &str,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    let Some(env) = value.as_object() else {
        diagnostics.push(diagnostic(
            request,
            "type_mismatch",
            path,
            "env must be an object",
        ));
        return;
    };
    for (name, value) in env {
        let env_path = child_path(path, name);
        if !is_env_name(name) {
            diagnostics.push(diagnostic(
                request,
                "invalid_env_name",
                &env_path,
                &format!("`{name}` is not a valid environment variable name"),
            ));
        }
        if value.is_string() {
            continue;
        }
        let Some(secret) = value.as_object() else {
            diagnostics.push(diagnostic(
                request,
                "invalid_env_value",
                &env_path,
                "env values must be strings or secret-reference objects",
            ));
            continue;
        };
        let known: Vec<_> = ["env", "file", "pass"]
            .into_iter()
            .filter(|key| secret.contains_key(*key))
            .collect();
        if known.len() != 1 || secret.len() != 1 {
            diagnostics.push(diagnostic(
                request,
                "invalid_secret_reference",
                &env_path,
                "secret reference must contain exactly one of `env`, `file`, or `pass`",
            ));
            continue;
        }
        validate_nonblank_string(
            request,
            &secret[known[0]],
            &child_path(&env_path, known[0]),
            diagnostics,
        );
    }
}

fn is_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|character| {
            character == '_' || character.is_ascii_alphanumeric()
        })
}

fn validate_duration(
    request: &AxeConfigValidationRequestWire,
    value: &Value,
    path: &str,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    let Some(value) = value.as_str() else {
        diagnostics.push(diagnostic(
            request,
            "type_mismatch",
            path,
            "duration must be a string",
        ));
        return;
    };
    if let Err(error) = parse_duration_at(value, path) {
        diagnostics.push(diagnostic(
            request,
            &error.code,
            &error.path,
            &error.message,
        ));
    }
}

fn validate_positive_integer(
    request: &AxeConfigValidationRequestWire,
    value: &Value,
    path: &str,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    if !matches!(value.as_u64(), Some(number) if number > 0) {
        diagnostics.push(diagnostic(
            request,
            "non_positive_integer",
            path,
            "value must be a positive integer",
        ));
    }
}

fn validate_nonblank_string(
    request: &AxeConfigValidationRequestWire,
    value: &Value,
    path: &str,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    if !matches!(value.as_str(), Some(item) if !item.trim().is_empty()) {
        diagnostics.push(diagnostic(
            request,
            "nonblank_string_required",
            path,
            "value must be a non-blank string",
        ));
    }
}

fn validate_string_array(
    request: &AxeConfigValidationRequestWire,
    value: &Value,
    path: &str,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    let Some(items) = value.as_array() else {
        diagnostics.push(diagnostic(
            request,
            "type_mismatch",
            path,
            "value must be an array of strings",
        ));
        return;
    };
    for (index, item) in items.iter().enumerate() {
        validate_nonblank_string(
            request,
            item,
            &format!("{path}[{index}]"),
            diagnostics,
        );
    }
}

fn validate_optional_type(
    request: &AxeConfigValidationRequestWire,
    value: Option<&Value>,
    path: &str,
    expected: &str,
    predicate: fn(&Value) -> bool,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    if value.is_some_and(|item| !predicate(item)) {
        diagnostics.push(diagnostic(
            request,
            "type_mismatch",
            path,
            &format!("value must be a {expected}"),
        ));
    }
}

fn unknown_keys(
    request: &AxeConfigValidationRequestWire,
    config: &Map<String, Value>,
    allowed: &[&str],
    path: &str,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    for key in config.keys() {
        if !allowed.contains(&key.as_str()) {
            diagnostics.push(diagnostic(
                request,
                "unknown_key",
                &child_path(path, key),
                &format!("unknown config key `{key}`"),
            ));
        }
    }
}

fn child_path(path: &str, key: &str) -> String {
    if path.is_empty() {
        key.to_string()
    } else {
        format!("{path}.{key}")
    }
}

fn diagnostic(
    request: &AxeConfigValidationRequestWire,
    code: &str,
    path: &str,
    message: &str,
) -> ConfigDiagnosticWire {
    ConfigDiagnosticWire {
        severity: "error".to_string(),
        code: code.to_string(),
        message: message.to_string(),
        path: (!path.is_empty()).then(|| path.to_string()),
        layer: provenance_for_path(&request.provenance, path),
    }
}

fn provenance_for_path(
    provenance: &BTreeMap<String, String>,
    path: &str,
) -> Option<String> {
    let mut candidate = path.to_string();
    loop {
        if let Some(layer) = provenance.get(&candidate) {
            return Some(layer.clone());
        }
        if candidate.ends_with(']') {
            if let Some(index) = candidate.rfind('[') {
                candidate.truncate(index);
                continue;
            }
        }
        let dot = candidate.rfind('.')?;
        candidate.truncate(dot);
    }
}
