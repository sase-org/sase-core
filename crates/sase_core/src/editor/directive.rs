use super::wire::{CompletionCandidate, CompletionList, DirectiveMetadata};
use crate::effort::EFFORT_LEVELS_ORDERED;

pub const DIRECTIVES: &[DirectiveMetadata] = &[
    DirectiveMetadata {
        name: "model",
        alias: Some("m"),
        description: "Override the LLM model for this prompt",
        takes_argument: true,
        allows_multiple: false,
    },
    DirectiveMetadata {
        // No `%e` alias — that already means `%edit`. `%effort` is the only
        // spelling, mirroring the Python xprompt parser.
        name: "effort",
        alias: None,
        description: "Set the reasoning-effort level for this prompt",
        takes_argument: true,
        allows_multiple: false,
    },
    DirectiveMetadata {
        name: "name",
        alias: Some("n"),
        description: "Assign a name to the agent",
        takes_argument: true,
        allows_multiple: false,
    },
    DirectiveMetadata {
        name: "wait",
        alias: Some("w"),
        description: "Wait for another agent or workflow",
        takes_argument: true,
        allows_multiple: true,
    },
    DirectiveMetadata {
        // No `%t` alias — that now means `%tale`. `%time` keeps its long
        // spelling only, mirroring the Python xprompt parser.
        name: "time",
        alias: None,
        description: "Wait for a duration or absolute wall-clock time",
        takes_argument: true,
        allows_multiple: true,
    },
    DirectiveMetadata {
        name: "edit",
        alias: Some("e"),
        description: "Return editor text to the prompt bar for review",
        takes_argument: false,
        allows_multiple: false,
    },
    DirectiveMetadata {
        name: "plan",
        alias: Some("p"),
        description: "Auto-approve the submitted plan as a normal plan",
        takes_argument: false,
        allows_multiple: false,
    },
    DirectiveMetadata {
        name: "tale",
        alias: Some("t"),
        description: "Auto-approve the submitted plan as a tale",
        takes_argument: false,
        allows_multiple: false,
    },
    DirectiveMetadata {
        name: "epic",
        alias: None,
        description: "Enable plan mode and approve the plan as an epic",
        takes_argument: false,
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
        name: "group",
        alias: Some("g"),
        description: "Assign the agent's user-managed grouping tag",
        takes_argument: true,
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

/// Deprecated directive aliases that still resolve to a canonical directive but
/// are intentionally hidden from advertised editor completion/hover. `%approve`
/// and `%a` are the legacy spelling of `%plan`; they keep parsing for
/// back-compat while the advertised family is `%plan` / `%tale` / `%epic`.
pub const DEPRECATED_ALIASES: &[(&str, &str)] =
    &[("approve", "plan"), ("a", "plan")];

pub fn canonical_directive_name(raw: &str) -> Option<&'static str> {
    if raw == "(" {
        return Some("alt");
    }
    for &(alias, canonical) in DEPRECATED_ALIASES {
        if alias == raw {
            return Some(canonical);
        }
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
        // `approve`/`a` resolve to `plan` via `DEPRECATED_ALIASES`, so they
        // land on this arm too.
        "plan" | "tale" | "edit" | "epic" | "hide" => &[],
        "time" => {
            &[("5m", "Wait for five minutes"), ("1h", "Wait for one hour")]
        }
        "wait" => &[("agent", "Wait for an agent or workflow")],
        "repeat" => &[("2", "Run twice"), ("3", "Run three times")],
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
            ("n", "name"),
            ("w", "wait"),
            ("e", "edit"),
            ("p", "plan"),
            ("t", "tale"),
            ("g", "group"),
            ("(", "alt"),
            // Deprecated spellings of `%plan` still resolve for back-compat.
            ("a", "plan"),
            ("approve", "plan"),
        ] {
            assert_eq!(canonical_directive_name(alias), Some(canonical));
        }
        assert!(directive_metadata("xprompts_enabled").is_some());

        let model = directive_metadata("model").expect("model metadata");
        assert!(!model.allows_multiple);
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
    fn plan_and_tale_are_the_advertised_auto_approve_directives() {
        let plan = directive_metadata("plan").expect("plan metadata");
        assert_eq!(plan.alias, Some("p"));
        assert!(!plan.takes_argument);
        assert!(
            plan.description.contains("normal plan"),
            "plan description should describe normal-plan auto-approval: {}",
            plan.description
        );

        let tale = directive_metadata("tale").expect("tale metadata");
        assert_eq!(tale.alias, Some("t"));
        assert!(!tale.takes_argument);
        assert!(
            tale.description.contains("tale"),
            "tale description should mention tales: {}",
            tale.description
        );

        // `%time` gave up its `%t` alias to `%tale`.
        let time = directive_metadata("time").expect("time metadata");
        assert_eq!(time.alias, None);
    }

    #[test]
    fn directive_completion_t_prefix_yields_tale_and_time() {
        // Both `%tale` and `%time` start with `t`; `%ta`/`%ti` disambiguate.
        let t_completions = build_directive_completion_candidates("%t");
        let t_names: Vec<&str> = t_completions
            .candidates
            .iter()
            .map(|candidate| candidate.name.as_str())
            .collect();
        assert_eq!(t_names, ["tale", "time"]);

        let tale = build_directive_completion_candidates("%ta");
        assert_eq!(tale.candidates.len(), 1);
        assert_eq!(tale.candidates[0].insertion, "%tale");
        assert_eq!(
            tale.candidates[0].documentation.as_deref(),
            Some("Auto-approve the submitted plan as a tale")
        );
    }

    #[test]
    fn approve_is_a_hidden_deprecated_alias_of_plan() {
        // `%approve` and `%a` still resolve to `plan` for back-compat ...
        assert_eq!(canonical_directive_name("approve"), Some("plan"));
        assert_eq!(canonical_directive_name("a"), Some("plan"));

        // ... but neither is advertised in completion. `%approve` matches no
        // advertised directive; `%a` only surfaces `%alt` (the lone directive
        // whose name starts with `a`), never `%approve`/`%plan`.
        assert!(build_directive_completion_candidates("%approve")
            .candidates
            .is_empty());
        let a_completions = build_directive_completion_candidates("%a");
        let a_names: Vec<&str> = a_completions
            .candidates
            .iter()
            .map(|candidate| candidate.name.as_str())
            .collect();
        assert_eq!(a_names, ["alt"]);
    }

    #[test]
    fn effort_is_a_recognized_directive_without_alias() {
        let effort = directive_metadata("effort").expect("effort metadata");
        assert_eq!(effort.name, "effort");
        assert_eq!(effort.alias, None);
        assert!(effort.takes_argument);
        assert!(!effort.allows_multiple);
        // No `%e` alias — `e` still resolves to `edit`.
        assert_eq!(canonical_directive_name("e"), Some("edit"));

        let completions = build_directive_completion_candidates("%eff");
        assert_eq!(completions.candidates.len(), 1);
        assert_eq!(completions.candidates[0].insertion, "%effort");
    }

    #[test]
    fn effort_argument_candidates_are_the_canonical_vocabulary() {
        let candidates = directive_argument_candidates("effort").candidates;
        let levels: Vec<&str> =
            candidates.iter().map(|c| c.insertion.as_str()).collect();
        assert_eq!(levels, EFFORT_LEVELS_ORDERED);
    }
}
