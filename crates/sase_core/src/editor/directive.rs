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
        // No `%t` alias. `%time` keeps its long
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
        name: "auto",
        alias: Some("a"),
        description:
            "Auto-approve the submitted plan as plan (default), tale, or epic",
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
        "auto" => &[
            ("plan", "Auto-approve as a normal plan"),
            ("tale", "Auto-approve and commit as an SDD tale"),
            ("epic", "Auto-approve and commit as an SDD epic"),
        ],
        "edit" | "hide" => &[],
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
            ("a", "auto"),
            ("g", "group"),
            ("(", "alt"),
        ] {
            assert_eq!(canonical_directive_name(alias), Some(canonical));
        }
        assert!(directive_metadata("xprompts_enabled").is_some());
        assert_eq!(canonical_directive_name("p"), None);
        assert_eq!(canonical_directive_name("t"), None);
        assert_eq!(canonical_directive_name("approve"), None);

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
    fn auto_is_the_advertised_auto_approve_directive() {
        let auto = directive_metadata("auto").expect("auto metadata");
        assert_eq!(auto.alias, Some("a"));
        assert!(auto.takes_argument);
        assert!(
            auto.description.contains("plan")
                && auto.description.contains("tale")
                && auto.description.contains("epic"),
            "auto description should describe plan/tale/epic modes: {}",
            auto.description
        );

        let candidates = directive_argument_candidates("auto").candidates;
        let values: Vec<&str> =
            candidates.iter().map(|c| c.insertion.as_str()).collect();
        assert_eq!(values, ["plan", "tale", "epic"]);

        let time = directive_metadata("time").expect("time metadata");
        assert_eq!(time.alias, None);
    }

    #[test]
    fn directive_completion_t_prefix_yields_only_time() {
        let t_completions = build_directive_completion_candidates("%t");
        let t_names: Vec<&str> = t_completions
            .candidates
            .iter()
            .map(|candidate| candidate.name.as_str())
            .collect();
        assert_eq!(t_names, ["time"]);

        let tale = build_directive_completion_candidates("%ta");
        assert!(tale.candidates.is_empty());
    }

    #[test]
    fn removed_auto_approve_aliases_do_not_resolve_or_complete() {
        assert_eq!(canonical_directive_name("approve"), None);
        assert_eq!(canonical_directive_name("p"), None);
        assert_eq!(canonical_directive_name("t"), None);

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
