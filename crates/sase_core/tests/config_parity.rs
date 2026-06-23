//! Parity + behavior tests for the `sase_core::config` domain.
//!
//! The merge golden is produced by the real Python
//! `sase.config.core._deep_merge` (folded over the same layer stack), so a
//! drift between the Rust merge and the Python implementation fails here. The
//! remaining tests cover the field-model flattener, the schema validator, the
//! inventory provenance, and the edit planner.

use sase_core::config::merge::merge_layers;
use sase_core::{
    config_field_model, config_inventory, config_plan_edit, config_validate,
    ConfigEditRequestWire, ConfigFieldStateWire, ConfigFieldWire,
    ConfigInventoryRequestWire, ConfigInventoryWire, ConfigLayerInputWire,
    ConfigValidateRequestWire,
};
use serde_json::{json, Value};

/// The merged config produced by folding the fixture layer stack through the
/// Python `_deep_merge` (see the bead's parity script). Kept as a literal so
/// the Rust merge is asserted against the Python output byte-for-byte
/// (structurally).
const PYTHON_MERGE_GOLDEN: &str = r#"{
  "axe": {
    "chop_script_dirs": ["a", "b", "c"],
    "max_hook_runners": 5
  },
  "linked_repos": [
    {"name": "user"},
    {"name": "overlay"},
    {"name": "local"}
  ],
  "sibling_repos": [
    {"name": "dep"}
  ],
  "timezone": "US/Pacific",
  "use_chezmoi": true
}"#;

fn fixture_layers_json() -> Value {
    json!([
        {
            "name": "default",
            "kind": "builtin",
            "list_strategy": "concatenate",
            "writable": false,
            "value": {
                "timezone": "America/New_York",
                "use_chezmoi": false,
                "axe": {"max_hook_runners": 3, "chop_script_dirs": ["a"]},
                "linked_repos": [{"name": "core"}]
            }
        },
        {
            "name": "plugin:demo",
            "kind": "plugin",
            "list_strategy": "concatenate",
            "writable": false,
            "value": {
                "axe": {"chop_script_dirs": ["b"]},
                "linked_repos": [{"name": "plugin"}]
            }
        },
        {
            "name": "user",
            "kind": "user",
            "path": "/home/u/.config/sase/sase.yml",
            "list_strategy": "replace",
            "writable": true,
            "value": {
                "timezone": "US/Pacific",
                "axe": {"max_hook_runners": 5},
                "linked_repos": [{"name": "user"}],
                "sibling_repos": [{"name": "dep"}]
            }
        },
        {
            "name": "overlay:sase_extra.yml",
            "kind": "overlay",
            "path": "/home/u/.config/sase/sase_extra.yml",
            "list_strategy": "concatenate",
            "writable": true,
            "value": {
                "axe": {"chop_script_dirs": ["c"]},
                "linked_repos": [{"name": "overlay"}]
            }
        },
        {
            "name": "local",
            "kind": "local",
            "path": "/repo/sase.yml",
            "list_strategy": "concatenate",
            "writable": true,
            "value": {
                "use_chezmoi": true,
                "linked_repos": [{"name": "local"}]
            }
        },
        {
            "name": "overlay:missing.yml",
            "kind": "overlay",
            "path": "/home/u/.config/sase/missing.yml",
            "list_strategy": "concatenate",
            "writable": true,
            "value": null,
            "exists": false
        }
    ])
}

fn fixture_layers() -> Vec<ConfigLayerInputWire> {
    serde_json::from_value(fixture_layers_json()).unwrap()
}

fn inventory_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "definitions": {
            "repo": {
                "type": "object",
                "required": ["name"],
                "additionalProperties": false,
                "properties": {"name": {"type": "string"}}
            }
        },
        "properties": {
            "timezone": {"type": "string", "default": "America/New_York"},
            "use_chezmoi": {"type": "boolean", "default": false},
            "axe": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "max_hook_runners": {"type": "integer", "default": 3},
                    "chop_script_dirs": {
                        "type": "array",
                        "items": {"type": "string"},
                        "default": []
                    }
                }
            },
            "linked_repos": {
                "type": "array",
                "items": {"$ref": "#/definitions/repo"},
                "default": []
            },
            "sibling_repos": {
                "type": "array",
                "items": {"$ref": "#/definitions/repo"}
            }
        }
    })
}

fn field<'a>(
    inventory: &'a ConfigInventoryWire,
    path: &str,
) -> &'a ConfigFieldStateWire {
    inventory
        .fields
        .iter()
        .find(|state| state.path == path)
        .unwrap_or_else(|| panic!("no field state for `{path}`"))
}

fn model_field<'a>(
    fields: &'a [ConfigFieldWire],
    path: &str,
) -> &'a ConfigFieldWire {
    fields
        .iter()
        .find(|f| f.path == path)
        .unwrap_or_else(|| panic!("no field for `{path}`"))
}

#[test]
fn merge_layers_matches_python_deep_merge_golden() {
    let merged = merge_layers(&fixture_layers());
    let golden: Value = serde_json::from_str(PYTHON_MERGE_GOLDEN).unwrap();
    assert_eq!(merged, golden);
}

#[test]
fn deep_merge_lists_replace_vs_concatenate() {
    let replace_layers: Vec<ConfigLayerInputWire> =
        serde_json::from_value(json!([
            {"name": "b", "list_strategy": "concatenate",
             "value": {"items": ["a"]}},
            {"name": "u", "list_strategy": "replace",
             "value": {"items": ["x"]}}
        ]))
        .unwrap();
    let concat_layers: Vec<ConfigLayerInputWire> =
        serde_json::from_value(json!([
            {"name": "b", "list_strategy": "concatenate",
             "value": {"items": ["a"]}},
            {"name": "o", "list_strategy": "concatenate",
             "value": {"items": ["x"]}}
        ]))
        .unwrap();
    assert_eq!(merge_layers(&replace_layers), json!({"items": ["x"]}));
    assert_eq!(merge_layers(&concat_layers), json!({"items": ["a", "x"]}));
}

#[test]
fn inventory_reports_effective_value_and_provenance() {
    let request: ConfigInventoryRequestWire = serde_json::from_value(json!({
        "schema": inventory_schema(),
        "layers": fixture_layers_json(),
        "deprecations": {"sibling_repos": "linked_repos"},
        "unsupported": ["workflows"]
    }))
    .unwrap();
    let inventory = config_inventory(&request).unwrap();

    // Scalar: highest-priority setter wins.
    let timezone = field(&inventory, "timezone");
    assert_eq!(timezone.effective_value, json!("US/Pacific"));
    assert_eq!(timezone.contributions.len(), 2);
    assert_eq!(timezone.contributions[0].layer, "default");
    assert_eq!(timezone.contributions[1].layer, "user");
    assert!(!timezone.contributions[0].winning);
    assert!(timezone.contributions[1].winning);

    // Concatenated list: every contributor listed, top one marked winning.
    let linked = field(&inventory, "linked_repos");
    assert_eq!(
        linked.effective_value,
        json!([{"name": "user"}, {"name": "overlay"}, {"name": "local"}])
    );
    let layers: Vec<&str> = linked
        .contributions
        .iter()
        .map(|c| c.layer.as_str())
        .collect();
    assert_eq!(
        layers,
        vec![
            "default",
            "plugin:demo",
            "user",
            "overlay:sase_extra.yml",
            "local"
        ]
    );
    assert!(linked.contributions.last().unwrap().winning);

    // Nested scalar via dotted path.
    let runners = field(&inventory, "axe.max_hook_runners");
    assert_eq!(runners.effective_value, json!(5));
    assert_eq!(runners.contributions.last().unwrap().layer, "user");

    // Default surfaced from the schema field model.
    assert!(runners.has_default);
    assert_eq!(runners.default, json!(3));

    // Deprecation replacement attached from the deprecations policy.
    let sibling = field(&inventory, "sibling_repos");
    assert_eq!(
        sibling.deprecated_replacement.as_deref(),
        Some("linked_repos")
    );

    // Write capabilities = the writable layer names.
    assert_eq!(
        timezone.write_capabilities,
        vec![
            "user".to_string(),
            "overlay:sase_extra.yml".to_string(),
            "local".to_string(),
            "overlay:missing.yml".to_string(),
        ]
    );

    // Sources: user flags the deprecated key; missing overlay is absent.
    let user_source =
        inventory.sources.iter().find(|s| s.name == "user").unwrap();
    assert_eq!(
        user_source.deprecated_keys,
        vec!["sibling_repos".to_string()]
    );
    assert!(user_source.writable);
    let missing = inventory
        .sources
        .iter()
        .find(|s| s.name == "overlay:missing.yml")
        .unwrap();
    assert!(!missing.exists);
    assert_eq!(missing.key_count, 0);

    // A deprecated-key diagnostic is emitted for the user layer.
    assert!(inventory.diagnostics.iter().any(|d| {
        d.code == "deprecated_key"
            && d.layer.as_deref() == Some("user")
            && d.path.as_deref() == Some("sibling_repos")
    }));
}

#[test]
fn field_model_flattens_nested_and_classifies() {
    let schema = json!({
        "type": "object",
        "additionalProperties": false,
        "definitions": {
            "repo": {
                "type": "object",
                "required": ["name"],
                "properties": {"name": {"type": "string"}}
            }
        },
        "properties": {
            "timezone": {
                "type": "string",
                "default": "America/New_York",
                "description": "tz"
            },
            "port": {
                "type": "integer",
                "minimum": 0,
                "maximum": 65535,
                "default": 7629
            },
            "provider": {
                "type": "string",
                "enum": ["git", "hg", "auto"],
                "default": "auto"
            },
            "auto": {
                "oneOf": [
                    {"type": "boolean"},
                    {"type": "string", "enum": ["soft", "on", "off"]}
                ],
                "default": "soft"
            },
            "axe": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "max_hook_runners": {"type": "integer", "default": 3},
                    "chop_script_dirs": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "lumberjacks": {
                        "type": "object",
                        "additionalProperties": {"type": "object"}
                    }
                }
            },
            "snippets": {
                "type": "object",
                "additionalProperties": {"type": "string"}
            },
            "linked_repos": {
                "type": "array",
                "items": {"$ref": "#/definitions/repo"}
            }
        }
    });
    let model = config_field_model(&schema).unwrap();
    let fields = &model.fields;

    let timezone = model_field(fields, "timezone");
    assert_eq!(timezone.kind, "scalar");
    assert_eq!(timezone.types, vec!["string".to_string()]);
    assert!(timezone.has_default);
    assert_eq!(timezone.default, json!("America/New_York"));
    assert!(timezone.leaf);
    assert_eq!(timezone.depth, 0);
    assert_eq!(timezone.parent, None);

    let axe = model_field(fields, "axe");
    assert_eq!(axe.kind, "object");
    assert!(!axe.leaf);

    let runners = model_field(fields, "axe.max_hook_runners");
    assert_eq!(runners.kind, "scalar");
    assert_eq!(runners.types, vec!["integer".to_string()]);
    assert_eq!(runners.depth, 1);
    assert_eq!(runners.parent.as_deref(), Some("axe"));

    assert_eq!(model_field(fields, "axe.chop_script_dirs").kind, "array");
    // An open object (additionalProperties schema) is a map leaf, not recursed.
    assert_eq!(model_field(fields, "axe.lumberjacks").kind, "map");
    assert_eq!(model_field(fields, "snippets").kind, "map");
    assert_eq!(model_field(fields, "linked_repos").kind, "array");

    let provider = model_field(fields, "provider");
    assert_eq!(
        provider.enum_values,
        vec![json!("git"), json!("hg"), json!("auto")]
    );

    let auto = model_field(fields, "auto");
    assert!(auto.types.contains(&"boolean".to_string()));
    assert!(auto.types.contains(&"string".to_string()));
    assert!(auto.enum_values.contains(&json!("soft")));

    let port = model_field(fields, "port");
    assert_eq!(port.constraints.minimum, Some(0.0));
    assert_eq!(port.constraints.maximum, Some(65535.0));

    // No recursion into a map's dynamic keys, and no `definitions` leakage.
    assert!(!fields
        .iter()
        .any(|f| f.path.starts_with("axe.lumberjacks.")));
    assert!(!fields.iter().any(|f| f.path.contains("definitions")));
}

#[test]
fn validate_detects_violations() {
    let schema = json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "timezone": {"type": "string"},
            "port": {"type": "integer", "minimum": 0, "maximum": 65535},
            "provider": {"type": "string", "enum": ["git", "hg", "auto"]},
            "ratio": {"type": "number", "exclusiveMinimum": 0},
            "pattern_field": {"type": "string", "pattern": "^\\d+(s|m|h)$"},
            "nested": {
                "type": "object",
                "additionalProperties": false,
                "required": ["name"],
                "properties": {"name": {"type": "string"}}
            }
        }
    });

    let clean = ConfigValidateRequestWire {
        schema: schema.clone(),
        config: json!({
            "timezone": "UTC",
            "port": 8080,
            "provider": "git",
            "ratio": 1.5,
            "pattern_field": "30s",
            "nested": {"name": "ok"}
        }),
    };
    assert!(config_validate(&clean).is_empty());

    let dirty = ConfigValidateRequestWire {
        schema,
        config: json!({
            "timezone": 5,
            "port": 70000,
            "provider": "svn",
            "ratio": 0,
            "pattern_field": "30x",
            "nested": {"extra": 1},
            "unknown_top": true
        }),
    };
    let diagnostics = config_validate(&dirty);
    let codes: Vec<&str> =
        diagnostics.iter().map(|d| d.code.as_str()).collect();
    for expected in [
        "type_mismatch",
        "maximum",
        "enum_mismatch",
        "exclusive_minimum",
        "pattern",
        "required_missing",
        "additional_property",
    ] {
        assert!(
            codes.contains(&expected),
            "expected `{expected}` in {codes:?}"
        );
    }
    // Both the nested extra key and the unknown top-level key are flagged.
    let additional_paths: Vec<&str> = diagnostics
        .iter()
        .filter(|d| d.code == "additional_property")
        .filter_map(|d| d.path.as_deref())
        .collect();
    assert!(additional_paths.contains(&"nested.extra"));
    assert!(additional_paths.contains(&"unknown_top"));
}

fn edit_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "timezone": {"type": "string"},
            "axe": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "max_hook_runners": {"type": "integer"}
                }
            }
        }
    })
}

#[test]
fn plan_edit_set_builds_write_plan_and_preview() {
    let request: ConfigEditRequestWire = serde_json::from_value(json!({
        "schema": edit_schema(),
        "layers": [
            {"name": "default", "kind": "builtin",
             "list_strategy": "concatenate", "writable": false,
             "value": {"axe": {"max_hook_runners": 3}}},
            {"name": "user", "kind": "user",
             "path": "/home/u/.config/sase/sase.yml",
             "list_strategy": "replace", "writable": true, "value": {}}
        ],
        "target_layer": "user",
        "path": "axe.max_hook_runners",
        "op": {"kind": "set", "value": 9}
    }))
    .unwrap();
    let plan = config_plan_edit(&request).unwrap();

    assert_eq!(
        plan.write_plan.file.as_deref(),
        Some("/home/u/.config/sase/sase.yml")
    );
    assert_eq!(plan.write_plan.layer, "user");
    assert_eq!(plan.write_plan.key_path, vec!["axe", "max_hook_runners"]);
    assert_eq!(plan.write_plan.op, "set");
    assert!(plan.write_plan.has_value);
    assert_eq!(plan.write_plan.new_value, json!(9));

    // The edit created the intermediate `axe` map in the user layer and the
    // candidate merge reflects the override.
    assert_eq!(
        plan.candidate_config,
        json!({"axe": {"max_hook_runners": 9}})
    );

    assert!(plan.effective_preview.has_before);
    assert_eq!(plan.effective_preview.before, json!(3));
    assert_eq!(plan.effective_preview.after, json!(9));
    assert!(plan.effective_preview.changed);

    // The candidate validates cleanly and the writable target has no warning.
    assert!(plan.validation.is_empty());
    assert!(plan.diagnostics.is_empty());
}

#[test]
fn plan_edit_unset_removes_key_and_warns_on_readonly_target() {
    let request: ConfigEditRequestWire = serde_json::from_value(json!({
        "schema": edit_schema(),
        "layers": [
            {"name": "default", "kind": "builtin",
             "list_strategy": "concatenate", "writable": false,
             "value": {"timezone": "America/New_York"}},
            {"name": "user", "kind": "user",
             "list_strategy": "replace", "writable": false,
             "value": {"timezone": "US/Pacific"}}
        ],
        "target_layer": "user",
        "path": "timezone",
        "op": {"kind": "unset"}
    }))
    .unwrap();
    let plan = config_plan_edit(&request).unwrap();

    assert_eq!(plan.write_plan.op, "unset");
    assert!(!plan.write_plan.has_value);
    // After unsetting the user override the default value is effective again.
    assert_eq!(plan.effective_preview.before, json!("US/Pacific"));
    assert_eq!(plan.effective_preview.after, json!("America/New_York"));
    assert!(plan.effective_preview.changed);
    assert_eq!(
        plan.candidate_config,
        json!({"timezone": "America/New_York"})
    );
    // Editing a read-only target is reported as a plan-level diagnostic.
    assert!(plan
        .diagnostics
        .iter()
        .any(|d| d.code == "target_not_writable"));
}

#[test]
fn plan_edit_unknown_target_errors() {
    let request: ConfigEditRequestWire = serde_json::from_value(json!({
        "schema": edit_schema(),
        "layers": [
            {"name": "user", "list_strategy": "replace",
             "writable": true, "value": {}}
        ],
        "target_layer": "nope",
        "path": "timezone",
        "op": {"kind": "set", "value": "UTC"}
    }))
    .unwrap();
    assert!(config_plan_edit(&request).is_err());
}
