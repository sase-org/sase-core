use std::collections::BTreeMap;

use serde_json::json;

use super::*;

fn resolve(
    name: &str,
    raw: &str,
) -> Result<ResolvedInputType, ResolveInputTypeError> {
    resolve_input_type(name, raw, &InputTypeRegistry::builtin())
}

fn plugin_registry(ids: &[&str]) -> InputTypeRegistry {
    let mut plugins = BTreeMap::new();
    plugins.insert(
        "sase-research-artifacts".to_string(),
        ids.iter().map(|id| (*id).to_string()).collect(),
    );
    InputTypeRegistry::with_plugins(plugins)
}

fn choice_values(result: &ValidateEnumChoicesResult) -> Vec<&str> {
    result
        .choices
        .iter()
        .map(|choice| choice.value.as_str())
        .collect()
}

fn issue_messages(result: &ValidateEnumChoicesResult) -> Vec<&str> {
    result
        .issues
        .iter()
        .map(|issue| issue.message.as_str())
        .collect()
}

#[test]
fn catalog_contains_only_the_contract_rows() {
    let catalog = builtin_catalog();
    let names: Vec<&str> =
        catalog.iter().map(|entry| entry.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "word", "line", "text", "path", "int", "float", "bool", "code",
            "string", "enum", "agent",
        ]
    );
    let string = catalog.iter().find(|entry| entry.name == "string").unwrap();
    assert_eq!(string.base, "line");
    assert_eq!(string.deprecated_alias_of.as_deref(), Some("line"));
    assert!(!string.advertised);
    let agent = catalog.iter().find(|entry| entry.name == "agent").unwrap();
    assert_eq!(agent.kind, InputTypeKind::Domain);
    assert_eq!(agent.base, "agent");
    assert_eq!(agent.value_role.as_deref(), Some("agent"));
    let int = catalog.iter().find(|entry| entry.name == "int").unwrap();
    assert_eq!(int.aliases, ["integer"]);
    let bool_entry = catalog.iter().find(|entry| entry.name == "bool").unwrap();
    assert_eq!(bool_entry.aliases, ["boolean"]);
}

#[test]
fn enmu_suggests_enum() {
    let error = resolve("mode", "enmu").unwrap_err();
    assert_eq!(
        error.to_string(),
        "input `mode` has unknown type `enmu`; did you mean `enum`?"
    );
}

#[test]
fn builtin_at_word_and_word_resolve_to_word() {
    for raw in ["word", "WORD", "builtin@word", "builtin@WORD"] {
        let resolved = resolve("mode", raw).unwrap();
        assert_eq!(resolved.base, "word", "{raw}");
        assert_eq!(resolved.named_type, None, "{raw}");
        assert!(!resolved.deprecated, "{raw}");
    }
}

#[test]
fn string_resolves_to_line_with_deprecated() {
    let resolved = resolve("title", "string").unwrap();
    assert_eq!(resolved.base, "line");
    assert!(resolved.deprecated);
    assert_eq!(resolved.named_type, None);
}

#[test]
fn agent_has_named_type_and_value_role() {
    let resolved = resolve("owner", "agent").unwrap();
    assert_eq!(resolved.base, "agent");
    assert_eq!(resolved.named_type.as_deref(), Some("agent"));
    assert_eq!(resolved.value_role.as_deref(), Some("agent"));
}

#[test]
fn integer_and_boolean_aliases_resolve() {
    assert_eq!(resolve("n", "integer").unwrap().base, "int");
    assert_eq!(resolve("flag", "boolean").unwrap().base, "bool");
}

#[test]
fn empty_registry_reports_plugin_not_installed() {
    let error = resolve("edition", "sase-research-artifacts@audio_edition")
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "input `edition` uses `sase-research-artifacts@audio_edition`, but \
         plugin `sase-research-artifacts` is not installed; run `sase plugin \
         install sase-research-artifacts`"
    );
}

#[test]
fn fixture_registry_reports_declares_no_input_type() {
    let registry = plugin_registry(&["audio_edition"]);
    let error = resolve_input_type(
        "edition",
        "sase-research-artifacts@audio_editon",
        &registry,
    )
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "plugin `sase-research-artifacts` declares no input type \
         `audio_editon`; did you mean `audio_edition`?"
    );
}

#[test]
fn pep503_normalizes_plugin_prefix() {
    let registry = plugin_registry(&["audio_edition"]);
    let error = resolve_input_type(
        "edition",
        "Sase_Research.Artifacts@audio_editon",
        &registry,
    )
    .unwrap_err();
    assert!(error.to_string().contains("`sase-research-artifacts`"));
    assert!(error.to_string().contains("`audio_edition`"));
}

#[test]
fn pyyaml_vector_types_non_strings_and_rejects_ordinary_words() {
    for text in [
        "yes",
        "no",
        "on",
        "off",
        "true",
        "false",
        "null",
        "Null",
        "~",
        "42",
        "1.5",
        "2020-01-01",
    ] {
        assert!(
            pyyaml_plain_scalar_is_non_string(text),
            "{text} should be non-string"
        );
    }
    for text in ["ready", "brief", "word", "draft"] {
        assert!(
            !pyyaml_plain_scalar_is_non_string(text),
            "{text} should stay a string"
        );
    }
    assert_eq!(
        unquoted_plain_scalar_choice_error("yes").as_deref(),
        Some(
            "choice `yes` must be quoted (\"yes\"): YAML reads it as a boolean"
        )
    );
}

#[test]
fn choice_bool_and_int_are_quote_errors() {
    let result = validate_enum_choices(&[json!(true), json!(1)]);
    assert!(result.choices.is_empty());
    assert_eq!(
        issue_messages(&result),
        [
            "choice arrived as a boolean and must be quoted",
            "choice arrived as an int and must be quoted",
        ]
    );
}

#[test]
fn choice_whitespace_null_duplicate_unknown_key_and_comma() {
    let result = validate_enum_choices(&[
        json!("in progress"),
        json!("null"),
        json!("prod"),
        json!("prod"),
        json!({"value": "ok", "extra": "nope"}),
        json!("fast,slow"),
        json!({
            "value": "staging",
            "label": "Staging",
            "description": "Pre-prod cluster"
        }),
    ]);
    assert_eq!(choice_values(&result), ["prod", "fast,slow", "staging"]);
    let messages = issue_messages(&result);
    assert!(messages.contains(
        &"choice `in progress` contains whitespace; choice values are \
          single words",
    ));
    assert!(
        messages.contains(
            &"choice `null` is reserved (it means \"use the default\")",
        )
    );
    assert!(messages.contains(&"choice `prod` is declared twice"));
    assert!(messages
        .iter()
        .any(|message| message.contains("unknown key `extra`")));
    assert!(messages
        .iter()
        .any(|message| message
            .contains("characters that need quoting in shorthand")));
    let staging = result
        .choices
        .iter()
        .find(|choice| choice.value == "staging")
        .unwrap();
    assert_eq!(staging.label.as_deref(), Some("Staging"));
    assert_eq!(staging.description.as_deref(), Some("Pre-prod cluster"));
}

#[test]
fn yaml_choices_round_trip_through_the_json_validator() {
    let items = vec![
        serde_yaml::Value::Bool(true),
        serde_yaml::Value::String("brief".to_string()),
    ];
    let result = validate_enum_choices_yaml(&items);
    assert_eq!(choice_values(&result), ["brief"]);
    assert_eq!(
        result.issues[0].message,
        "choice arrived as a boolean and must be quoted"
    );
}

#[test]
fn breif_against_brief_and_full_suggests_brief() {
    let resolved = ResolvedInputType {
        base: "enum".to_string(),
        named_type: None,
        value_role: None,
        choices: vec![
            InputChoice {
                value: "brief".to_string(),
                label: Some("Brief".to_string()),
                description: None,
            },
            InputChoice {
                value: "full".to_string(),
                label: None,
                description: None,
            },
        ],
        deprecated: false,
    };
    let error = check_input_value(&resolved, "edition", "breif").unwrap_err();
    assert_eq!(
        error,
        "Argument `edition` expects one of brief | full, got `breif`; \
         did you mean `brief`?"
    );
    assert!(check_input_value(&resolved, "edition", "Brief").is_err());
    assert!(check_input_value(&resolved, "edition", "Brief")
        .unwrap_err()
        .contains("did you mean `brief`?"));
    assert!(check_input_value(&resolved, "edition", "brief").is_ok());
    assert!(check_input_value(&resolved, "edition", "full").is_ok());
}

#[test]
fn closed_set_default_rejects_non_members() {
    let choices = vec![
        InputChoice {
            value: "fast".to_string(),
            label: None,
            description: None,
        },
        InputChoice {
            value: "thorough".to_string(),
            label: None,
            description: None,
        },
    ];
    assert_eq!(
        check_closed_set_default("turbo", &choices).as_deref(),
        Some("default `turbo` is not one of fast | thorough")
    );
    assert_eq!(check_closed_set_default("fast", &choices), None);
}

#[test]
fn check_input_value_is_noop_without_choices() {
    let resolved = resolve("name", "line").unwrap();
    assert!(check_input_value(&resolved, "name", "anything").is_ok());
}

#[test]
fn suggest_closest_ranks_equality_and_prefix_first() {
    let suggestions = suggest_closest("en", ["enum", "agent", "int", "entry"]);
    assert_eq!(suggestions[0], "enum");
    assert!(suggestions.contains(&"entry".to_string()));
}
