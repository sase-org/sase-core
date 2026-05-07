use super::wire::{CompletionCandidate, CompletionList, DirectiveMetadata};

pub const DIRECTIVES: &[DirectiveMetadata] = &[
    DirectiveMetadata {
        name: "model",
        alias: Some("m"),
        description: "Override the LLM model for this prompt",
        takes_argument: true,
        allows_multiple: true,
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
        description: "Wait for another agent or for a duration",
        takes_argument: true,
        allows_multiple: true,
    },
    DirectiveMetadata {
        name: "approve",
        alias: Some("a"),
        description: "Run the agent fully autonomously",
        takes_argument: false,
        allows_multiple: false,
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
        description: "Enable plan mode",
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
        name: "tag",
        alias: Some("t"),
        description: "Assign the agent's user-managed tag",
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
        name: "alt",
        alias: Some("("),
        description: "Split prompt into variants with different text",
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
                .filter(|alias| *alias != "(")
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
    let values: &[(&str, &str)] = match canonical {
        "xprompts_enabled" => &[
            ("false", "Disable xprompt expansion"),
            ("true", "Enable xprompt expansion"),
        ],
        "approve" | "edit" | "plan" | "epic" | "hide" => &[],
        "wait" => &[
            ("5m", "Wait for five minutes"),
            ("1h", "Wait for one hour"),
            ("90s", "Wait for ninety seconds"),
        ],
        "repeat" => &[("2", "Run twice"), ("3", "Run three times")],
        _ => &[],
    };
    CompletionList {
        candidates: values
            .iter()
            .map(|(value, doc)| CompletionCandidate {
                display: (*value).to_string(),
                insertion: (*value).to_string(),
                detail: None,
                documentation: Some((*doc).to_string()),
                is_dir: false,
                name: (*value).to_string(),
                replacement: None,
            })
            .collect(),
        shared_extension: String::new(),
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
            ("a", "approve"),
            ("e", "edit"),
            ("p", "plan"),
            ("t", "tag"),
            ("(", "alt"),
        ] {
            assert_eq!(canonical_directive_name(alias), Some(canonical));
        }
        assert!(directive_metadata("xprompts_enabled").is_some());
    }
}
