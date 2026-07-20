use super::wire::{CompletionCandidate, CompletionList, DirectiveMetadata};
use crate::effort::EFFORT_LEVELS_ORDERED;

const AUTO_COMPATIBILITY_ARGUMENT_SUGGESTIONS: &[(&str, &str)] = &[
    ("plan", "Plan-gate compatibility alias for normal approval"),
    (
        "tale",
        "Plan-gate compatibility alias for SDD tale approval",
    ),
    (
        "epic",
        "Plan-gate compatibility alias for SDD epic approval",
    ),
];

pub const DIRECTIVES: &[DirectiveMetadata] = &[
    DirectiveMetadata {
        name: "model",
        alias: Some("m"),
        description: "Override the LLM model for this prompt",
        takes_argument: true,
        allows_multiple: false,
    },
    DirectiveMetadata {
        // `%e` is the advertised `%effort` alias. It canonicalizes to `effort`
        // for completion, hover, diagnostics, and fan-out parsing. Mirrors the
        // Python xprompt parser's `_DIRECTIVE_ALIASES["e"] = "effort"`.
        name: "effort",
        alias: Some("e"),
        description: "Set the reasoning-effort level for this prompt",
        takes_argument: true,
        allows_multiple: false,
    },
    DirectiveMetadata {
        name: "id",
        alias: Some("i"),
        description: "Assign an agent ID with optional bead, clan, family, or user-managed tribe",
        takes_argument: true,
        allows_multiple: false,
    },
    DirectiveMetadata {
        name: "clan",
        alias: Some("c"),
        description: "Declare a new parallel agent clan",
        takes_argument: true,
        allows_multiple: false,
    },
    DirectiveMetadata {
        name: "wait",
        alias: Some("w"),
        description: "Wait for another agent/workflow and/or a time floor",
        takes_argument: true,
        allows_multiple: true,
    },
    DirectiveMetadata {
        name: "auto",
        alias: Some("a"),
        description:
            "Request automatic gate resolution; arguments are interpreted by the gate kind",
        takes_argument: true,
        allows_multiple: false,
    },
    DirectiveMetadata {
        name: "hide",
        alias: Some("h"),
        description: "Hide the agent from the default Agents tab display",
        takes_argument: false,
        allows_multiple: false,
    },
    DirectiveMetadata {
        name: "repeat",
        alias: Some("r"),
        description: "Run the prompt multiple times",
        takes_argument: true,
        allows_multiple: false,
    },
    DirectiveMetadata {
        // The `%{A | B}` brace shorthand is the advertised alt spelling. The
        // legacy `%(...)` alias is kept parse-compatible via
        // `canonical_directive_name`, but is no longer surfaced here so editor
        // completion/hover stop advertising it.
        name: "alt",
        alias: None,
        description:
            "Split prompt into variants with different text; shorthand %{A | B}",
        takes_argument: true,
        allows_multiple: true,
    },
    DirectiveMetadata {
        name: "xprompts_enabled",
        alias: None,
        description: "Enable or disable xprompt expansion for a region",
        takes_argument: true,
        allows_multiple: true,
    },
];

pub fn canonical_directive_name(raw: &str) -> Option<&'static str> {
    if raw == "(" {
        return Some("alt");
    }
    DIRECTIVES.iter().find_map(|directive| {
        if directive.name == raw || directive.alias == Some(raw) {
            Some(directive.name)
        } else {
            None
        }
    })
}

pub fn directive_metadata(raw: &str) -> Option<&'static DirectiveMetadata> {
    let canonical = canonical_directive_name(raw)?;
    DIRECTIVES
        .iter()
        .find(|directive| directive.name == canonical)
}

pub fn build_directive_completion_candidates(token: &str) -> CompletionList {
    let partial = token.strip_prefix('%').unwrap_or(token).to_lowercase();
    let mut candidates = Vec::new();
    for directive in DIRECTIVES {
        if directive.name.starts_with(&partial)
            || directive
                .alias
                .is_some_and(|alias| alias.starts_with(&partial))
        {
            candidates.push(CompletionCandidate {
                display: format!("%{}", directive.name),
                insertion: format!("%{}", directive.name),
                detail: directive.alias.map(|alias| format!("alias %{alias}")),
                documentation: Some(directive.description.to_string()),
                is_dir: false,
                name: directive.name.to_string(),
                replacement: None,
                additional_edits: Vec::new(),
                kind: String::new(),
                project: String::new(),
                status: String::new(),
            });
        }
    }
    candidates.sort_by(|a, b| a.name.cmp(&b.name));
    CompletionList {
        candidates,
        shared_extension: String::new(),
    }
}

pub fn directive_argument_candidates(name: &str) -> CompletionList {
    let canonical = canonical_directive_name(name).unwrap_or(name);
    // `%effort` candidates come from the shared effort vocabulary so the editor
    // stays in parity with the Python xprompt parser.
    if canonical == "effort" {
        return CompletionList {
            candidates: EFFORT_LEVELS_ORDERED
                .iter()
                .map(|level| {
                    argument_candidate(level, "Reasoning-effort level")
                })
                .collect(),
            shared_extension: String::new(),
        };
    }
    let values: &[(&str, &str)] = match canonical {
        "xprompts_enabled" => &[
            ("false", "Disable xprompt expansion"),
            ("true", "Enable xprompt expansion"),
        ],
        // These are compatibility suggestions for plan workflows, not an
        // exhaustive parser vocabulary. The runtime gate adapter owns argument
        // validation and may accept a different value for another gate kind.
        "auto" => AUTO_COMPATIBILITY_ARGUMENT_SUGGESTIONS,
        "hide" => &[],
        "wait" => &[
            ("time=", "Set a minimum wait duration"),
            ("runners=", "Wait for runner capacity"),
            ("priority=", "Set runner queue priority (lower runs first)"),
        ],
        "repeat" => &[("2", "Run twice"), ("3", "Run three times")],
        "id" => &[
            ("bead=", "Associate this launch with a bead"),
            ("clan=", "Derive the full ID and join this agent clan"),
            ("family=", "Attach this suffix to an existing agent family"),
            ("tribe=", "Assign this agent to a user-managed tribe"),
        ],
        "clan" => &[
            ("tribe=", "Assign this clan to a user-managed tribe"),
            ("summary=", "Attach a Rich-markup summary to this clan"),
            (
                "summary_script=",
                "Generate this clan's summary with an executable script",
            ),
        ],
        _ => &[],
    };
    CompletionList {
        candidates: values
            .iter()
            .map(|(value, doc)| argument_candidate(value, doc))
            .collect(),
        shared_extension: String::new(),
    }
}

fn argument_candidate(value: &str, doc: &str) -> CompletionCandidate {
    CompletionCandidate {
        display: value.to_string(),
        insertion: value.to_string(),
        detail: None,
        documentation: Some(doc.to_string()),
        is_dir: false,
        name: value.to_string(),
        replacement: None,
        additional_edits: Vec::new(),
        kind: String::new(),
        project: String::new(),
        status: String::new(),
    }
}

pub fn is_directive_like_token(token: &str) -> bool {
    token.starts_with('%') || token == "%("
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_documented_aliases() {
        for (alias, canonical) in [
            ("m", "model"),
            ("e", "effort"),
            ("i", "id"),
            ("c", "clan"),
            ("w", "wait"),
            ("a", "auto"),
            ("(", "alt"),
        ] {
            assert_eq!(canonical_directive_name(alias), Some(canonical));
        }
        assert!(directive_metadata("xprompts_enabled").is_some());
        assert_eq!(canonical_directive_name("p"), None);
        assert_eq!(canonical_directive_name("time"), None);
        assert_eq!(canonical_directive_name("approve"), None);
        // `%edit` was removed and is not an alias; `%e` now resolves to `effort`.
        assert_eq!(canonical_directive_name("edit"), None);
        assert_eq!(canonical_directive_name("e"), Some("effort"));
        assert_eq!(canonical_directive_name("name"), None);
        assert_eq!(canonical_directive_name("n"), None);
        assert_eq!(canonical_directive_name("tribe"), None);
        assert_eq!(canonical_directive_name("t"), None);

        let model = directive_metadata("model").expect("model metadata");
        assert!(!model.allows_multiple);
    }

    #[test]
    fn id_metadata_and_completion_match_the_editor_contract() {
        let metadata = directive_metadata("id").expect("id metadata");
        assert_eq!(metadata.alias, Some("i"));
        assert!(metadata.takes_argument);
        assert!(!metadata.allows_multiple);
        assert_eq!(
            metadata.description,
            "Assign an agent ID with optional bead, clan, family, or user-managed tribe"
        );
        assert_eq!(canonical_directive_name("i"), Some("id"));
        assert_eq!(directive_metadata("i").map(|d| d.name), Some("id"));

        for token in ["%id", "%i"] {
            let completions = build_directive_completion_candidates(token);
            assert_eq!(completions.candidates.len(), 1, "{token} completion");
            let candidate = &completions.candidates[0];
            assert_eq!(candidate.insertion, "%id");
            assert_eq!(candidate.detail.as_deref(), Some("alias %i"));
            assert_eq!(
                candidate.documentation.as_deref(),
                Some(metadata.description)
            );
        }

        let id_args = directive_argument_candidates("id").candidates;
        assert_eq!(id_args.len(), 4);
        assert_eq!(
            id_args
                .iter()
                .map(|candidate| candidate.insertion.as_str())
                .collect::<Vec<_>>(),
            ["bead=", "clan=", "family=", "tribe="]
        );
        assert_eq!(
            id_args
                .iter()
                .map(|candidate| candidate.documentation.as_deref().unwrap())
                .collect::<Vec<_>>(),
            [
                "Associate this launch with a bead",
                "Derive the full ID and join this agent clan",
                "Attach this suffix to an existing agent family",
                "Assign this agent to a user-managed tribe",
            ]
        );
        assert_eq!(directive_argument_candidates("i").candidates, id_args);

        for removed in ["name", "n"] {
            assert_eq!(canonical_directive_name(removed), None);
            assert!(directive_metadata(removed).is_none());
            assert!(build_directive_completion_candidates(&format!(
                "%{removed}"
            ))
            .candidates
            .is_empty());
        }
    }

    #[test]
    fn clan_metadata_matches_the_editor_contract() {
        let metadata = directive_metadata("clan").expect("directive metadata");
        assert_eq!(metadata.alias, Some("c"));
        assert!(metadata.takes_argument);
        assert!(!metadata.allows_multiple);
        assert_eq!(canonical_directive_name("c"), Some("clan"));
        assert_eq!(directive_metadata("c").map(|d| d.name), Some("clan"));
        assert_eq!(metadata.description, "Declare a new parallel agent clan");

        for token in ["%cl", "%c"] {
            let completions = build_directive_completion_candidates(token);
            assert_eq!(completions.candidates.len(), 1, "{token} completion");
            let candidate = &completions.candidates[0];
            assert_eq!(candidate.insertion, "%clan");
            assert_eq!(candidate.detail.as_deref(), Some("alias %c"));
            assert_eq!(
                candidate.documentation.as_deref(),
                Some(metadata.description)
            );
        }

        let clan_args = directive_argument_candidates("clan").candidates;
        assert_eq!(
            clan_args
                .iter()
                .map(|candidate| candidate.insertion.as_str())
                .collect::<Vec<_>>(),
            ["tribe=", "summary=", "summary_script="]
        );
        assert_eq!(
            clan_args[1].documentation.as_deref(),
            Some("Attach a Rich-markup summary to this clan")
        );
        assert_eq!(
            clan_args[2].documentation.as_deref(),
            Some("Generate this clan's summary with an executable script")
        );
        assert_eq!(directive_argument_candidates("c").candidates, clan_args);
        assert!(directive_argument_candidates("tribe").candidates.is_empty());
    }

    #[test]
    fn removed_identity_directives_do_not_resolve_or_complete() {
        for name in ["family", "f", "group", "g", "tribe", "t"] {
            assert_eq!(canonical_directive_name(name), None, "{name}");
            assert!(directive_metadata(name).is_none(), "{name}");
            assert!(
                build_directive_completion_candidates(&format!("%{name}"))
                    .candidates
                    .is_empty(),
                "{name}"
            );
        }
    }

    #[test]
    fn alt_metadata_advertises_brace_shorthand() {
        let alt = directive_metadata("alt").expect("alt metadata");
        // The legacy `(` alias is no longer advertised, but stays
        // parse-compatible through `canonical_directive_name`.
        assert_eq!(alt.alias, None);
        assert_eq!(canonical_directive_name("("), Some("alt"));
        assert!(
            alt.description.contains("%{"),
            "alt description should describe the brace shorthand: {}",
            alt.description
        );

        // Completing `%alt` surfaces the directive without an `alias %(` detail.
        let completions = build_directive_completion_candidates("%alt");
        let alt_candidate = completions
            .candidates
            .iter()
            .find(|candidate| candidate.name == "alt")
            .expect("alt completion candidate");
        assert_eq!(alt_candidate.detail, None);
    }

    #[test]
    fn auto_metadata_describes_gate_owned_resolution_and_offers_compatibility_suggestions(
    ) {
        let auto = directive_metadata("auto").expect("auto metadata");
        assert_eq!(auto.alias, Some("a"));
        assert!(auto.takes_argument);
        assert!(
            auto.description.contains("gate kind"),
            "auto description should assign validation to the gate kind: {}",
            auto.description
        );

        // These insertions stay aligned with Python's
        // AUTO_COMPATIBILITY_ARGUMENT_SUGGESTIONS. They are suggestions, not a
        // universal runtime allowlist.
        let candidates = directive_argument_candidates("auto").candidates;
        let values: Vec<&str> =
            candidates.iter().map(|c| c.insertion.as_str()).collect();
        assert_eq!(values, ["plan", "tale", "epic"]);
    }

    #[test]
    fn directive_completion_t_prefix_is_empty() {
        let t_completions = build_directive_completion_candidates("%t");
        assert!(t_completions.candidates.is_empty());

        for token in ["%ta", "%ti", "%time"] {
            assert!(
                build_directive_completion_candidates(token)
                    .candidates
                    .is_empty(),
                "{token} should not complete"
            );
        }
    }

    #[test]
    fn removed_auto_approve_aliases_do_not_resolve_or_complete() {
        assert_eq!(canonical_directive_name("approve"), None);
        assert_eq!(canonical_directive_name("p"), None);
        assert_eq!(canonical_directive_name("time"), None);

        assert!(build_directive_completion_candidates("%approve")
            .candidates
            .is_empty());
        assert!(build_directive_completion_candidates("%p")
            .candidates
            .is_empty());
        assert!(build_directive_completion_candidates("%ta")
            .candidates
            .is_empty());
        let a_completions = build_directive_completion_candidates("%a");
        let a_names: Vec<&str> = a_completions
            .candidates
            .iter()
            .map(|candidate| candidate.name.as_str())
            .collect();
        assert_eq!(a_names, ["alt", "auto"]);
    }

    #[test]
    fn effort_is_a_recognized_directive_with_e_alias() {
        let effort = directive_metadata("effort").expect("effort metadata");
        assert_eq!(effort.name, "effort");
        assert_eq!(effort.alias, Some("e"));
        assert!(effort.takes_argument);
        assert!(!effort.allows_multiple);
        // `%e` is the advertised `%effort` alias and canonicalizes to `effort`.
        assert_eq!(canonical_directive_name("e"), Some("effort"));
        assert_eq!(directive_metadata("e").map(|d| d.name), Some("effort"));

        for token in ["%e", "%eff"] {
            let completions = build_directive_completion_candidates(token);
            assert_eq!(completions.candidates.len(), 1, "{token} completion");
            assert_eq!(completions.candidates[0].insertion, "%effort");
            // The `%effort` candidate advertises its `%e` alias detail.
            assert_eq!(
                completions.candidates[0].detail.as_deref(),
                Some("alias %e"),
                "{token} alias detail"
            );
        }
    }

    #[test]
    fn effort_argument_candidates_are_the_canonical_vocabulary() {
        let candidates = directive_argument_candidates("effort").candidates;
        let levels: Vec<&str> =
            candidates.iter().map(|c| c.insertion.as_str()).collect();
        assert_eq!(levels, EFFORT_LEVELS_ORDERED);
    }

    #[test]
    fn wait_argument_candidates_use_runtime_keywords() {
        let candidates = directive_argument_candidates("wait").candidates;
        let values: Vec<&str> =
            candidates.iter().map(|c| c.insertion.as_str()).collect();
        assert_eq!(values, ["time=", "runners=", "priority="]);
        assert!(directive_argument_candidates("time").candidates.is_empty());
    }
}
