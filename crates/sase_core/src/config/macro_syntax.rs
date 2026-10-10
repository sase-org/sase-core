//! Authored macro-config normalization for the xprompt-to-macro rename.
//!
//! [`normalize_macro_config_layer`] maps one already-decoded authored config
//! layer to canonical macro spellings. Its input is the decoded mapping plus
//! the retired `accept_legacy_xprompt_names` wire field, which is accepted
//! and ignored: retired xprompt spellings are always accepted as aliases.
//! Its output is the canonical mapping plus source-qualified retirement
//! diagnostics.
//!
//! Covered keys (plan `202610/macro_syntax_cutover.md`, compatibility
//! section): top-level `xprompts`/`macros` and
//! `xprompt_aliases`/`macro_aliases`,
//! `ace.prompt_completion.auto_xprompt_menu`/`auto_macro_menu`,
//! `ace.prompt_inputs.xprompt_placeholder_args`/`macro_placeholder_args`,
//! and `mentor_profiles[].mentors[].xprompt`/`macro`. Entry names and
//! template bodies are data, never renamed; nested `_helper` mappings ride
//! along untouched inside their moved parent value.
//!
//! Presence (not truthiness) decides: a legacy key present with a null,
//! empty-mapping, false, or empty-string value is still an alias. Supplying
//! both spellings in the same authored mapping is an error; collision errors
//! say `<old> and <new> cannot be combined; use only <new>`.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::wire::ConfigError;

// Legacy xprompt spellings. Kept explicitly commented as legacy so later
// codemods (and the terminology guard) can tell deliberate compatibility
// surface from stragglers.
const LEGACY_TOP_MACROS: &str = "xprompts"; // legacy xprompt spelling
const LEGACY_TOP_ALIASES: &str = "xprompt_aliases"; // legacy xprompt spelling
const LEGACY_AUTO_MENU: &str = "auto_xprompt_menu"; // legacy xprompt spelling
const LEGACY_PLACEHOLDER_ARGS: &str = "xprompt_placeholder_args"; // legacy xprompt spelling
const LEGACY_MENTOR_FIELD: &str = "xprompt"; // legacy xprompt spelling

/// One normalization input: a decoded authored layer plus policy.
///
/// `source` names the layer for diagnostics (for example `"user"` or a file
/// path); it defaults to empty when the caller has no identity to keep.
/// `accept_legacy_xprompt_names` is a retired rollout switch: it is accepted
/// for wire compatibility and ignored, since retired xprompt spellings are
/// always accepted as aliases.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MacroLayerNormalizeRequestWire {
    pub layer: Value,
    pub accept_legacy_xprompt_names: bool,
    #[serde(default)]
    pub source: String,
}

/// One accepted legacy spelling, qualified by the layer that authored it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MacroSyntaxDiagnosticWire {
    /// Layer identity from the request (`""` when the caller gave none).
    pub source: String,
    /// Dotted path of the canonical key (for example `"macros"` or
    /// `"ace.prompt_completion.auto_macro_menu"`).
    pub path: String,
    /// The retired spelling that was present.
    pub legacy_key: String,
    /// The canonical spelling it normalized to.
    pub canonical_key: String,
    /// Always `"retired-alias"` today; `"collision"` is reserved for later
    /// inventory views that report instead of rejecting.
    pub kind: String,
    /// Human-readable note naming the replacement.
    pub message: String,
}

/// Normalization output: the canonical mapping plus retirement diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MacroLayerNormalizeWire {
    pub canonical: Value,
    pub diagnostics: Vec<MacroSyntaxDiagnosticWire>,
}

fn collision_message(legacy: &str, canonical: &str) -> String {
    format!("{legacy} and {canonical} cannot be combined; use only {canonical}")
}

/// Human-readable note for an accepted retired alias, naming the
/// replacement. This feeds retirement diagnostics (e.g. doctor); it is not
/// a rejection.
fn retired_message(legacy: &str, canonical: &str) -> String {
    format!("{legacy} is retired; use {canonical}")
}

struct Normalizer<'a> {
    source: &'a str,
    diagnostics: Vec<MacroSyntaxDiagnosticWire>,
}

impl<'a> Normalizer<'a> {
    /// Move `legacy` to `canonical` inside `map` by presence.
    ///
    /// Both present is an error. Legacy present alone is an accepted alias
    /// (recorded in `diagnostics`). Canonical present alone passes through
    /// untouched.
    fn move_key(
        &mut self,
        map: &mut Map<String, Value>,
        path: &str,
        legacy: &str,
        canonical: &str,
    ) -> Result<(), ConfigError> {
        let has_legacy = map.contains_key(legacy);
        let has_canonical = map.contains_key(canonical);
        if has_legacy && has_canonical {
            return Err(ConfigError::validation(collision_message(
                legacy, canonical,
            )));
        }
        if !has_legacy {
            return Ok(());
        }
        let value = map.remove(legacy).expect("presence checked above");
        map.insert(canonical.to_string(), value);
        self.diagnostics.push(MacroSyntaxDiagnosticWire {
            source: self.source.to_string(),
            path: path.to_string(),
            legacy_key: legacy.to_string(),
            canonical_key: canonical.to_string(),
            kind: "retired-alias".to_string(),
            message: retired_message(legacy, canonical),
        });
        Ok(())
    }
}

/// Normalize one authored config layer to canonical macro spellings.
///
/// The layer must decode to a JSON object; anything else is a validation
/// error. Only the documented alias keys are renamed; every other key and
/// every entry name/template body passes through byte-identical.
pub fn normalize_macro_config_layer(
    request: &MacroLayerNormalizeRequestWire,
) -> Result<MacroLayerNormalizeWire, ConfigError> {
    let mut root = match &request.layer {
        Value::Object(map) => map.clone(),
        _ => {
            return Err(ConfigError::validation(
                "macro config layer must be a mapping",
            ));
        }
    };
    // The retired `accept_legacy_xprompt_names` switch is ignored: retired
    // xprompt spellings are always accepted as aliases.
    let _ = request.accept_legacy_xprompt_names;
    let mut normalizer = Normalizer {
        source: request.source.as_str(),
        diagnostics: Vec::new(),
    };

    normalizer.move_key(&mut root, "macros", LEGACY_TOP_MACROS, "macros")?;
    normalizer.move_key(
        &mut root,
        "macro_aliases",
        LEGACY_TOP_ALIASES,
        "macro_aliases",
    )?;

    if let Some(ace) = root.get_mut("ace").and_then(Value::as_object_mut) {
        if let Some(completion) = ace
            .get_mut("prompt_completion")
            .and_then(Value::as_object_mut)
        {
            normalizer.move_key(
                completion,
                "ace.prompt_completion.auto_macro_menu",
                LEGACY_AUTO_MENU,
                "auto_macro_menu",
            )?;
        }
        if let Some(inputs) =
            ace.get_mut("prompt_inputs").and_then(Value::as_object_mut)
        {
            normalizer.move_key(
                inputs,
                "ace.prompt_inputs.macro_placeholder_args",
                LEGACY_PLACEHOLDER_ARGS,
                "macro_placeholder_args",
            )?;
        }
    }

    if let Some(profiles) = root
        .get_mut("mentor_profiles")
        .and_then(Value::as_array_mut)
    {
        for (profile_index, profile) in profiles.iter_mut().enumerate() {
            let Some(mentors) =
                profile.get_mut("mentors").and_then(Value::as_array_mut)
            else {
                continue;
            };
            for (mentor_index, mentor) in mentors.iter_mut().enumerate() {
                let Some(map) = mentor.as_object_mut() else {
                    continue;
                };
                normalizer.move_key(
                    map,
                    &format!(
                        "mentor_profiles[{profile_index}].mentors[{mentor_index}].macro"
                    ),
                    LEGACY_MENTOR_FIELD,
                    "macro",
                )?;
            }
        }
    }

    Ok(MacroLayerNormalizeWire {
        canonical: Value::Object(root),
        diagnostics: normalizer.diagnostics,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request(layer: Value, accept: bool) -> MacroLayerNormalizeRequestWire {
        MacroLayerNormalizeRequestWire {
            layer,
            accept_legacy_xprompt_names: accept,
            source: "user".to_string(),
        }
    }

    #[test]
    fn canonical_layer_passes_through_untouched_in_both_states() {
        for accept in [false, true] {
            let layer = json!({
                "macros": {"a": {"body": "x"}},
                "macro_aliases": {"#a": "#b"},
                "ace": {"prompt_completion": {"auto_macro_menu": false}},
            });
            let out =
                normalize_macro_config_layer(&request(layer.clone(), accept))
                    .unwrap();
            assert_eq!(out.canonical, layer);
            assert!(out.diagnostics.is_empty());
        }
    }

    #[test]
    fn legacy_top_level_keys_move_when_accepted() {
        let out = normalize_macro_config_layer(&request(
            json!({"xprompts": {"a": {}}, "xprompt_aliases": {"#a": "#b"}}),
            true,
        ))
        .unwrap();
        assert_eq!(
            out.canonical,
            json!({"macros": {"a": {}}, "macro_aliases": {"#a": "#b"}})
        );
        assert_eq!(out.diagnostics.len(), 2);
        assert_eq!(out.diagnostics[0].path, "macros");
        assert_eq!(out.diagnostics[0].source, "user");
        assert_eq!(out.diagnostics[0].kind, "retired-alias");
        assert_eq!(out.diagnostics[0].legacy_key, "xprompts");
    }

    #[test]
    fn legacy_keys_move_even_when_switch_denied() {
        // The retired switch no longer changes policy: `false` accepts the
        // same aliases as `true`.
        for accept in [false, true] {
            let out = normalize_macro_config_layer(&request(
                json!({"xprompts": {"a": {}}}),
                accept,
            ))
            .unwrap();
            assert_eq!(out.canonical, json!({"macros": {"a": {}}}));
            assert_eq!(out.diagnostics.len(), 1);
        }
    }

    #[test]
    fn presence_counts_even_for_null_and_empty_values() {
        for value in [json!(null), json!({}), json!(false), json!("")] {
            let out = normalize_macro_config_layer(&request(
                json!({"xprompts": value}),
                true,
            ))
            .unwrap();
            assert!(out.canonical.get("macros").is_some());
            assert!(!out
                .canonical
                .as_object()
                .unwrap()
                .contains_key("xprompts"));
        }
        // ... and presence still normalizes when the switch is denied.
        let denied = normalize_macro_config_layer(&request(
            json!({"xprompts": null}),
            false,
        ))
        .unwrap();
        assert!(denied.canonical.get("macros").is_some());
    }

    #[test]
    fn both_spellings_collide_in_both_flag_states() {
        for accept in [false, true] {
            let err = normalize_macro_config_layer(&request(
                json!({"xprompts": {"a": {}}, "macros": {"b": {}}}),
                accept,
            ))
            .unwrap_err();
            assert!(
                err.to_string().contains(
                    "xprompts and macros cannot be combined; use only macros"
                ),
                "{err}"
            );
        }
    }

    #[test]
    fn nested_completion_and_mentor_keys_normalize() {
        let out = normalize_macro_config_layer(&request(
            json!({
                "ace": {
                    "prompt_completion": {"auto_xprompt_menu": true},
                    "prompt_inputs": {"xprompt_placeholder_args": false},
                },
                "mentor_profiles": [
                    {"mentors": [{"xprompt": "#a"}, {"macro": "#b"}]},
                ],
            }),
            true,
        ))
        .unwrap();
        assert_eq!(
            out.canonical,
            json!({
                "ace": {
                    "prompt_completion": {"auto_macro_menu": true},
                    "prompt_inputs": {"macro_placeholder_args": false},
                },
                "mentor_profiles": [
                    {"mentors": [{"macro": "#a"}, {"macro": "#b"}]},
                ],
            })
        );
        assert_eq!(out.diagnostics.len(), 3);
    }

    #[test]
    fn nested_collision_rejects() {
        let err = normalize_macro_config_layer(&request(
            json!({"ace": {"prompt_completion": {
                "auto_xprompt_menu": true, "auto_macro_menu": false,
            }}}),
            true,
        ))
        .unwrap_err();
        assert!(err.to_string().contains("cannot be combined"));
    }

    #[test]
    fn entry_bodies_are_data_not_keys() {
        // An entry literally named like an alias key is content, not policy.
        let out = normalize_macro_config_layer(&request(
            json!({"macros": {"xprompts": {"body": "x"}}}),
            false,
        ))
        .unwrap();
        assert_eq!(
            out.canonical,
            json!({"macros": {"xprompts": {"body": "x"}}})
        );
    }

    #[test]
    fn non_mapping_layer_rejects() {
        let err = normalize_macro_config_layer(&request(json!([1, 2]), true))
            .unwrap_err();
        assert!(err.to_string().contains("must be a mapping"));
    }
}
