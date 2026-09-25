use super::super::token::DocumentSnapshot;
use super::super::wire::{
    BeadCompletionEntry, CompletionContext, CompletionContextKind,
    DirectiveClauseKind, DirectiveSyntaxForm, DirectiveValueRole,
    EditorPosition, EditorRange,
};
use super::*;
use crate::effort::EFFORT_LEVELS_ORDERED;

#[test]
fn resolves_documented_aliases() {
    for (alias, canonical) in [
        ("m", "model"),
        ("e", "effort"),
        ("i", "id"),
        ("c", "clan"),
        ("w", "wait"),
        ("q", "queue"),
        ("a", "auto"),
        ("(", "alt"),
        ("{", "alt"),
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
fn contract_covers_the_audited_directive_matrix() {
    let contract = directive_contract();
    let names: Vec<&str> =
        contract.iter().map(|entry| entry.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "model",
            "effort",
            "final",
            "id",
            "clan",
            "wait",
            "queue",
            "hold",
            "dispatch",
            "if",
            "proc",
            "auto",
            "hide",
            "repeat",
            "alt",
            "xprompts_enabled",
        ]
    );

    let wait = contract
        .iter()
        .find(|entry| entry.name == "wait")
        .expect("wait contract");
    assert_eq!(
        wait.keywords
            .iter()
            .map(|keyword| keyword.name.as_str())
            .collect::<Vec<_>>(),
        ["agent", "bead", "hood", "proc", "time", "unit"]
    );
    assert!(wait
        .syntax_forms
        .contains(&DirectiveSyntaxForm::Parenthesized));
    assert!(wait.syntax_forms.contains(&DirectiveSyntaxForm::Colon));
    assert_eq!(wait.positional_role, Some(DirectiveValueRole::Agent));

    let queue = contract
        .iter()
        .find(|entry| entry.name == "queue")
        .expect("queue contract");
    assert_eq!(queue.alias.as_deref(), Some("q"));
    assert_eq!(queue.feature_flag.as_deref(), None);
    assert_eq!(
        queue.positional_role,
        Some(DirectiveValueRole::NonNegativeInt)
    );
    assert_eq!(queue.positional_suggestions[0].value, "0");
    let on_queue =
        directive_contract_with_flags(&["queue_capacity_budget".to_string()])
            .into_iter()
            .find(|entry| entry.name == "queue")
            .expect("queue contract");
    assert_eq!(
        on_queue.positional_role,
        Some(DirectiveValueRole::PositiveInt)
    );
    assert_eq!(
        on_queue
            .positional_suggestions
            .iter()
            .map(|value| value.value.as_str())
            .collect::<Vec<_>>(),
        ["1", "100", "1.5x"]
    );
    assert_eq!(
        on_queue
            .keywords
            .iter()
            .find(|keyword| keyword.name == "capacity")
            .map(|keyword| keyword.value_role),
        Some(DirectiveValueRole::PositiveInt)
    );
    assert!(on_queue.description.contains("capacity budget"));
    assert!(on_queue.description.contains("<M>x"));
    assert!(on_queue.argument_hint.contains("<M>x"));
    assert!(on_queue
        .keywords
        .iter()
        .find(|keyword| keyword.name == "capacity")
        .map(|keyword| keyword.description.as_str())
        .unwrap_or_default()
        .contains("<M>x"));
    assert_eq!(
        queue
            .keywords
            .iter()
            .map(|keyword| keyword.name.as_str())
            .collect::<Vec<_>>(),
        ["capacity", "p", "priority", "w", "weight"]
    );
    assert_eq!(
        queue
            .keywords
            .iter()
            .find(|keyword| keyword.name == "w")
            .map(|keyword| keyword.conflicts_with.clone()),
        Some(vec!["weight".to_string()])
    );
    for queue in [queue, &on_queue] {
        for keyword in queue
            .keywords
            .iter()
            .filter(|keyword| matches!(keyword.name.as_str(), "w" | "weight"))
        {
            assert_eq!(
                keyword.value_role,
                DirectiveValueRole::NonNegativeFloat
            );
            assert_eq!(keyword.suggested_values[0].value, "0");
            assert!(keyword.description.contains("0 adds no load"));
        }
    }
    assert_eq!(
        queue
            .keywords
            .iter()
            .find(|keyword| keyword.name == "p")
            .map(|keyword| keyword.conflicts_with.clone()),
        Some(vec!["priority".to_string()])
    );
    assert!(!directive_is_hidden_from_name_completion("queue"));
    assert!(!directive_is_hidden_from_name_completion_with_flags(
        "queue",
        &[]
    ));

    let hold = contract
        .iter()
        .find(|entry| entry.name == "hold")
        .expect("hold contract");
    assert_eq!(hold.alias, None);
    assert_eq!(hold.feature_flag.as_deref(), None);
    assert!(hold.allows_multiple);
    assert_eq!(
        hold.syntax_forms,
        vec![
            DirectiveSyntaxForm::Colon,
            DirectiveSyntaxForm::Parenthesized
        ]
    );
    assert_eq!(hold.positional_role, Some(DirectiveValueRole::Agent));
    assert_eq!(
        hold.positional_suggestions
            .iter()
            .map(|value| value.value.as_str())
            .collect::<Vec<_>>(),
        ["pending", "future"]
    );
    assert_eq!(
        hold.keywords
            .iter()
            .map(|keyword| (keyword.name.as_str(), keyword.value_role))
            .collect::<Vec<_>>(),
        [
            ("hood", DirectiveValueRole::Hood),
            ("scope", DirectiveValueRole::FreeText),
            ("ttl", DirectiveValueRole::Duration),
            ("tribe", DirectiveValueRole::Tribe),
        ]
    );
    assert!(hold
        .keywords
        .iter()
        .find(|keyword| keyword.name == "hood")
        .is_some_and(|keyword| keyword.repeatable));
    assert!(!directive_is_hidden_from_name_completion_with_flags(
        "hold",
        &[]
    ));
    assert_eq!(
        wait.keywords
            .iter()
            .find(|keyword| keyword.name == "bead")
            .map(|keyword| keyword.value_role),
        Some(DirectiveValueRole::Bead)
    );

    let if_directive = contract
        .iter()
        .find(|entry| entry.name == "if")
        .expect("if contract");
    assert_eq!(if_directive.feature_flag.as_deref(), None);
    assert_eq!(
        if_directive.body_kind,
        crate::DirectiveBodyKind::OptionalFencedCode
    );
    assert_eq!(
        if_directive.syntax_forms,
        vec![DirectiveSyntaxForm::Parenthesized]
    );
    assert_eq!(
        if_directive
            .keywords
            .iter()
            .map(|keyword| keyword.name.as_str())
            .collect::<Vec<_>>(),
        vec!["should_run"]
    );
    assert!(!directive_is_hidden_from_name_completion("if"));
    assert!(!directive_is_hidden_from_name_completion_with_flags(
        "if",
        &["typed_launch_units".to_string()]
    ));
    let flagged_if =
        directive_contract_with_flags(&["typed_launch_units".to_string()])
            .into_iter()
            .find(|entry| entry.name == "if")
            .expect("flagged if contract");
    assert_eq!(
        flagged_if.syntax_forms,
        vec![
            DirectiveSyntaxForm::Parenthesized,
            DirectiveSyntaxForm::DoubleColon
        ]
    );

    let proc = contract
        .iter()
        .find(|entry| entry.name == "proc")
        .expect("proc contract");
    assert_eq!(proc.feature_flag.as_deref(), Some("typed_launch_units"));
    assert_eq!(proc.body_kind, crate::DirectiveBodyKind::OptionalFencedCode);
    assert_eq!(
        proc.keywords
            .iter()
            .map(|keyword| keyword.name.as_str())
            .collect::<Vec<_>>(),
        [
            "bash",
            "python",
            "timeout",
            "idle_timeout",
            "cwd",
            "workspace",
            "label"
        ]
    );

    let id = contract
        .iter()
        .find(|entry| entry.name == "id")
        .expect("id contract");
    assert_eq!(
        id.keywords
            .iter()
            .map(|keyword| (
                keyword.name.as_str(),
                keyword.conflicts_with.clone()
            ))
            .collect::<Vec<_>>(),
        [
            ("bead", Vec::new()),
            (
                "clan",
                vec![
                    "family".to_string(),
                    "session".to_string(),
                    "tribe".to_string(),
                ],
            ),
            (
                "session",
                vec![
                    "clan".to_string(),
                    "family".to_string(),
                    "tribe".to_string(),
                ],
            ),
            (
                "family",
                vec![
                    "clan".to_string(),
                    "session".to_string(),
                    "tribe".to_string(),
                ],
            ),
            (
                "tribe",
                vec![
                    "clan".to_string(),
                    "family".to_string(),
                    "session".to_string(),
                ],
            ),
        ]
    );

    let clan = contract
        .iter()
        .find(|entry| entry.name == "clan")
        .expect("clan contract");
    assert_eq!(
        clan.keywords
            .iter()
            .map(|keyword| keyword.name.as_str())
            .collect::<Vec<_>>(),
        ["summary", "summary_script", "tribe"]
    );
    assert_eq!(
        clan.keywords[0].conflicts_with,
        vec!["summary_script".to_string()]
    );

    let enabled = contract
        .iter()
        .find(|entry| entry.name == "xprompts_enabled")
        .expect("xprompts_enabled contract");
    assert_eq!(enabled.syntax_forms, vec![DirectiveSyntaxForm::Colon]);
    assert_eq!(
        enabled
            .positional_suggestions
            .iter()
            .map(|value| value.value.as_str())
            .collect::<Vec<_>>(),
        ["false", "true"]
    );

    let model = contract
        .iter()
        .find(|entry| entry.name == "model")
        .expect("model contract");
    assert_eq!(
        model.dynamic_keyword_role,
        Some(DirectiveValueRole::ModelAliasKey)
    );
    assert_eq!(model.positional_role, Some(DirectiveValueRole::Model));

    let final_directive = contract
        .iter()
        .find(|entry| entry.name == "final")
        .expect("final contract");
    assert!(final_directive.allows_multiple);
    assert_eq!(
        final_directive.syntax_forms,
        vec![
            DirectiveSyntaxForm::Colon,
            DirectiveSyntaxForm::Parenthesized
        ]
    );
    assert_eq!(
        final_directive.positional_role,
        Some(DirectiveValueRole::FinalizerInstance)
    );
    assert_eq!(
        final_directive
            .positional_suggestions
            .iter()
            .map(|value| value.value.as_str())
            .collect::<Vec<_>>(),
        ["none"]
    );
}

#[test]
fn id_metadata_and_completion_match_the_editor_contract() {
    let metadata = directive_metadata("id").expect("id metadata");
    assert_eq!(metadata.alias, Some("i"));
    assert!(metadata.takes_argument);
    assert!(!metadata.allows_multiple);
    assert_eq!(
            metadata.description,
            "Assign an agent ID with optional bead, clan, session, or user-managed tribe"
        );
    assert_eq!(canonical_directive_name("i"), Some("id"));
    assert_eq!(directive_metadata("i").map(|d| d.name), Some("id"));

    let id_completions = build_directive_completion_candidates("%id");
    assert_eq!(id_completions.candidates.len(), 1, "%id completion");
    let candidate = &id_completions.candidates[0];
    assert_eq!(candidate.insertion, "%id");
    assert_eq!(candidate.detail.as_deref(), Some("alias %i"));
    assert_eq!(
        candidate.documentation.as_deref(),
        Some(metadata.description)
    );

    let i_completions = build_directive_completion_candidates("%i");
    let i_names: Vec<&str> = i_completions
        .candidates
        .iter()
        .map(|candidate| candidate.name.as_str())
        .collect();
    assert_eq!(i_names, ["id", "if"]);

    let id_args = directive_argument_candidates("id").candidates;
    assert_eq!(id_args.len(), 4);
    assert_eq!(
        id_args
            .iter()
            .map(|candidate| candidate.insertion.as_str())
            .collect::<Vec<_>>(),
        ["bead=", "clan=", "session=", "tribe="]
    );
    assert_eq!(
        id_args
            .iter()
            .map(|candidate| candidate.documentation.as_deref().unwrap())
            .collect::<Vec<_>>(),
        [
            "Associate this launch with a bead",
            "Derive the full ID and join this agent clan",
            "Attach this suffix to an existing agent session",
            "Assign this agent to a user-managed tribe",
        ]
    );
    assert_eq!(directive_argument_candidates("i").candidates, id_args);

    for removed in ["name", "n"] {
        assert_eq!(canonical_directive_name(removed), None);
        assert!(directive_metadata(removed).is_none());
        assert!(
            build_directive_completion_candidates(&format!("%{removed}"))
                .candidates
                .is_empty()
        );
    }
}

#[test]
fn legacy_family_keyword_stays_in_contract_but_unsuggested() {
    let contract = directive_contract();
    let id = contract
        .iter()
        .find(|entry| entry.name == "id")
        .expect("id contract");
    let keywords: Vec<&str> = id
        .keywords
        .iter()
        .map(|keyword| keyword.name.as_str())
        .collect();
    assert_eq!(keywords, ["bead", "clan", "session", "family", "tribe"]);

    let id_candidates = directive_argument_candidates("id");
    let suggested: Vec<&str> = id_candidates
        .candidates
        .iter()
        .map(|candidate| candidate.insertion.as_str())
        .collect();
    assert_eq!(suggested, ["bead=", "clan=", "session=", "tribe="]);
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
        ["summary=", "summary_script=", "tribe="]
    );
    assert_eq!(
        clan_args[0].documentation.as_deref(),
        Some("Attach a Rich-markup summary to this clan")
    );
    assert_eq!(
        clan_args[1].documentation.as_deref(),
        Some("Generate this clan's summary with an executable script")
    );
    assert_eq!(directive_argument_candidates("c").candidates, clan_args);
    assert!(directive_argument_candidates("tribe").candidates.is_empty());
}

#[test]
fn removed_identity_directives_do_not_resolve_or_complete() {
    assert_eq!(canonical_directive_name("f"), None);
    assert!(directive_metadata("f").is_none());

    for name in ["family", "group", "g", "tribe", "t"] {
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
fn final_directive_is_public_in_name_completion() {
    assert_eq!(canonical_directive_name("final"), Some("final"));
    assert!(directive_metadata("final").is_some());
    assert!(!directive_is_hidden_from_name_completion("final"));

    for token in ["%f", "%final"] {
        let completions = build_directive_completion_candidates(token);
        assert_eq!(completions.candidates.len(), 1, "{token} completion");
        let candidate = &completions.candidates[0];
        assert_eq!(candidate.insertion, "%final");
        assert_eq!(candidate.name, "final");
        assert_eq!(
            candidate.documentation.as_deref(),
            Some("Select configured finalizer instances for this launch")
        );
    }
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
fn queue_argument_candidates_use_runtime_keywords() {
    let candidates = directive_argument_candidates("queue").candidates;
    let values: Vec<&str> =
        candidates.iter().map(|c| c.insertion.as_str()).collect();
    assert_eq!(values, ["capacity=", "p=", "priority=", "w=", "weight="]);
    assert_eq!(canonical_directive_name("q"), Some("queue"));
    let list = build_directive_completion_candidates("%q");
    assert_eq!(list.candidates.len(), 1);
    assert_eq!(list.candidates[0].insertion, "%queue");
    assert_eq!(list.candidates[0].detail.as_deref(), Some("alias %q"));
}

#[test]
fn queue_name_completion_uses_flag_aware_documentation() {
    let off = build_directive_completion_candidates_with_flags("%q", &[]);
    let off_queue = off
        .candidates
        .iter()
        .find(|candidate| candidate.name == "queue")
        .expect("queue completion candidate");
    assert_eq!(off_queue.insertion, "%queue");
    assert_eq!(off_queue.detail.as_deref(), Some("alias %q"));
    assert_eq!(
        off_queue.documentation.as_deref(),
        Some("Set weighted-load capacity, priority, and capacity weight")
    );

    let on = build_directive_completion_candidates_with_flags(
        "%q",
        &["queue_capacity_budget".to_string()],
    );
    let on_queue = on
        .candidates
        .iter()
        .find(|candidate| candidate.name == "queue")
        .expect("queue completion candidate");
    assert_eq!(on_queue.insertion, "%queue");
    assert_eq!(on_queue.detail.as_deref(), Some("alias %q"));
    assert_eq!(
        on_queue.documentation.as_deref(),
        Some(
            "Set this launch's capacity budget, <M>x multiplier of this machine's max_running_agents budget, priority, and capacity weight"
        )
    );
}

#[test]
fn wait_argument_candidates_use_runtime_keywords() {
    let candidates = directive_argument_candidates("wait").candidates;
    let values: Vec<&str> =
        candidates.iter().map(|c| c.insertion.as_str()).collect();
    assert_eq!(
        values,
        ["agent=", "bead=", "hood=", "proc=", "time=", "unit="]
    );
    assert!(directive_argument_candidates("time").candidates.is_empty());
}

#[test]
fn keyword_candidates_suppress_selected_and_conflicting_names() {
    let id = directive_metadata("id").expect("id metadata");
    let available = build_directive_keyword_candidates(
        id,
        "",
        &["clan=research".to_string()],
        None,
    );
    let names: Vec<&str> = available
        .candidates
        .iter()
        .map(|candidate| candidate.insertion.as_str())
        .collect();
    assert_eq!(names, ["bead="]);

    let clan = directive_metadata("clan").expect("clan metadata");
    let remaining = build_directive_keyword_candidates(
        clan,
        "su",
        &["summary".to_string()],
        None,
    );
    assert!(remaining.candidates.is_empty());
}

#[test]
fn bead_ranking_matches_wait_modal_order_and_filters() {
    let entries = vec![
        BeadCompletionEntry {
            id: "sase-b".to_string(),
            title: "Later open".to_string(),
            status: "open".to_string(),
            updated_at: "2026-08-20T12:00:00Z".to_string(),
            ..BeadCompletionEntry::default()
        },
        BeadCompletionEntry {
            id: "sase-a".to_string(),
            title: "Active bug".to_string(),
            status: "in_progress".to_string(),
            updated_at: "2026-08-19T12:00:00Z".to_string(),
            type_label: "task".to_string(),
            task_type: "bug".to_string(),
            project: "sase".to_string(),
            created_at: "2026-08-01T00:00:00Z".to_string(),
        },
        BeadCompletionEntry {
            id: "sase-c".to_string(),
            title: "Ready work".to_string(),
            status: "ready".to_string(),
            updated_at: "2026-08-21T12:00:00Z".to_string(),
            type_label: "task".to_string(),
            created_at: String::new(),
            task_type: String::new(),
            project: String::new(),
        },
    ];
    let ranked = rank_and_filter_bead_entries(&entries, "", &[], &[], 10);
    assert_eq!(
        ranked
            .iter()
            .map(|entry| entry.id.as_str())
            .collect::<Vec<_>>(),
        ["sase-a", "sase-c", "sase-b"]
    );

    let filtered = rank_and_filter_bead_entries(&entries, "bug", &[], &[], 10);
    assert_eq!(
        filtered
            .iter()
            .map(|entry| entry.id.as_str())
            .collect::<Vec<_>>(),
        ["sase-a"]
    );

    let excluded = rank_and_filter_bead_entries(
        &entries,
        "",
        &["sase-c".to_string()],
        &["sase-a".to_string()],
        10,
    );
    assert_eq!(
        excluded
            .iter()
            .map(|entry| entry.id.as_str())
            .collect::<Vec<_>>(),
        ["sase-b"]
    );
}

fn pos(character: u32) -> EditorPosition {
    EditorPosition { line: 0, character }
}

fn classify(text: &str, character: u32) -> CompletionContext {
    let document = DocumentSnapshot::new(text);
    detect_directive_context_at_position(&document, pos(character))
        .unwrap_or_else(|| panic!("expected directive context for {text}"))
}

fn assert_replacement_range(text: &str, character: u32, start: u32, end: u32) {
    let context = classify(text, character);
    assert_eq!(
        context.replacement_range,
        EditorRange {
            start: pos(start),
            end: pos(end),
        }
    );
}

#[test]
fn unterminated_wait_colon_body_stops_at_prose() {
    assert_replacement_range("%wait:co and then do the thing", 8, 6, 8);
    assert_replacement_range("%wait: do the thing", 6, 6, 6);
}

#[test]
fn unclosed_paren_body_stops_at_prose() {
    assert_replacement_range("%wait(co and more prose", 8, 6, 8);
    assert_replacement_range("%id(foo and more prose", 6, 4, 7);
    assert_replacement_range("%model(son and more prose", 10, 7, 10);
    assert_replacement_range("%clan(rev and more prose", 9, 6, 9);
    assert_replacement_range("%final(sase and more prose", 11, 7, 11);
}

#[test]
fn comma_adjacent_space_keeps_the_wait_list_body() {
    assert_replacement_range("%wait:planner, co", 17, 15, 17);
    assert_replacement_range("%wait:planner,", 14, 14, 14);
    assert_replacement_range("%wait(planner, co", 17, 15, 17);
}

#[test]
fn quoted_wait_value_keeps_its_inner_space() {
    assert_replacement_range("%wait:`my agent` ", 9, 6, 16);
}

#[test]
fn cursor_in_prose_past_a_directive_has_no_context() {
    let text = "%w:sase-59 Can you help me get rid of the ,";
    let document = DocumentSnapshot::new(text);
    assert_eq!(
        detect_directive_context_at_position(&document, pos(text.len() as u32)),
        None
    );
}

#[test]
fn wait_paren_keywords_are_not_offered_in_colon_form() {
    let colon = classify("%wait:t", 7);
    assert_eq!(colon.kind, CompletionContextKind::DirectiveArgument);
    assert_eq!(colon.syntax_form(), Some(DirectiveSyntaxForm::Colon));
    assert_eq!(colon.clause_kind(), Some(DirectiveClauseKind::Positional));
    assert!(!directive_allows_keywords(
        directive_metadata("wait").unwrap(),
        colon.syntax_form().unwrap()
    ));

    let paren = classify("%wait(t", 7);
    assert_eq!(paren.kind, CompletionContextKind::DirectiveArgument);
    assert_eq!(
        paren.syntax_form(),
        Some(DirectiveSyntaxForm::Parenthesized)
    );
    assert!(directive_allows_keywords(
        directive_metadata("wait").unwrap(),
        paren.syntax_form().unwrap()
    ));
}

#[test]
fn wait_bead_value_is_a_keyword_value_clause() {
    let context = classify("%wait(bead=", 11);
    assert_eq!(context.kind, CompletionContextKind::DirectiveArgumentValue);
    assert_eq!(context.active_keyword(), Some("bead"));
    assert_eq!(context.value_role(), Some(DirectiveValueRole::Bead));
    assert_eq!(
        context.clause_kind(),
        Some(DirectiveClauseKind::KeywordValue)
    );
}

#[test]
fn id_and_clan_keyword_values_and_conflicts_classify() {
    let value = classify("%id(worker, clan=re", 19);
    assert_eq!(value.kind, CompletionContextKind::DirectiveArgumentValue);
    assert_eq!(value.active_keyword(), Some("clan"));
    assert_eq!(value.value_role(), Some(DirectiveValueRole::Clan));
    assert_eq!(value.selected_values, vec!["worker"]);

    let first_keyword = classify("%id(tribe=", 10);
    assert_eq!(
        first_keyword.kind,
        CompletionContextKind::DirectiveArgumentValue
    );
    assert_eq!(first_keyword.active_keyword(), Some("tribe"));

    let clan_keyword = classify("%clan(research, su", 18);
    assert_eq!(
        clan_keyword.kind,
        CompletionContextKind::DirectiveArgumentKeyword
    );
    let suppressed = build_directive_keyword_candidates(
        directive_metadata("clan").unwrap(),
        "su",
        clan_keyword.selected_keywords(),
        None,
    );
    assert_eq!(
        suppressed
            .candidates
            .iter()
            .map(|candidate| candidate.insertion.as_str())
            .collect::<Vec<_>>(),
        ["summary=", "summary_script="]
    );
}

#[test]
fn quoted_and_text_block_commas_do_not_split_clauses() {
    let quoted = classify("%clan(research, summary=\"a, b\", tr", 34);
    assert_eq!(quoted.kind, CompletionContextKind::DirectiveArgumentKeyword);
    assert_eq!(
        quoted.selected_keywords(),
        ["summary".to_string()].as_slice()
    );
    assert_eq!(quoted.token.as_ref().unwrap().text, "tr");

    let block = classify("%clan(research, summary=[[hello, world]], tr", 44);
    assert_eq!(block.kind, CompletionContextKind::DirectiveArgumentKeyword);
    assert_eq!(
        block.selected_keywords(),
        ["summary".to_string()].as_slice()
    );

    let inner = "%clan(research, summary=[[note: use ]] here, and more]], tr";
    let inner_block = classify(inner, inner.len() as u32);
    assert_eq!(
        inner_block.kind,
        CompletionContextKind::DirectiveArgumentKeyword
    );
    assert_eq!(
        inner_block.selected_keywords(),
        ["summary".to_string()].as_slice()
    );
    assert_eq!(inner_block.token.as_ref().unwrap().text, "tr");
}

#[test]
fn utf16_positions_classify_the_active_wait_clause() {
    let text = "%wait(café, be";
    let document = DocumentSnapshot::new(text);
    let cursor = document
        .byte_offset_to_position(text.len())
        .expect("utf-16 cursor");
    assert_eq!(cursor.character, 14);
    let context = detect_directive_context_at_position(&document, cursor)
        .expect("wait unicode context");
    assert_eq!(context.directive_name.as_deref(), Some("wait"));
    assert_eq!(context.token.as_ref().unwrap().text, "be");
    assert_eq!(context.selected_values, vec!["café"]);
    assert_eq!(
        context.syntax_form(),
        Some(DirectiveSyntaxForm::Parenthesized)
    );
}

#[test]
fn incomplete_and_malformed_calls_still_classify() {
    let empty = classify("%wait(", 6);
    assert_eq!(empty.kind, CompletionContextKind::DirectiveArgument);
    assert_eq!(empty.token.as_ref().unwrap().text, "");

    let trailing = classify("%id(worker, ", 12);
    assert_eq!(
        trailing.kind,
        CompletionContextKind::DirectiveArgumentKeyword
    );
    assert_eq!(trailing.selected_values, vec!["worker"]);

    let unclosed = classify("%clan(research, summary=\"hello", 30);
    assert_eq!(unclosed.kind, CompletionContextKind::DirectiveArgumentValue);
    assert_eq!(unclosed.active_keyword(), Some("summary"));
    assert_eq!(unclosed.token.as_ref().unwrap().text, "\"hello");
}

#[test]
fn clause_candidates_cover_roles_conflicts_and_self_references() {
    use super::super::completion::build_directive_clause_candidates;
    use super::super::wire::{
        AgentCompletionEntry, DirectiveCompletionInventories,
        DirectiveMachineEntry, DirectiveModelAliasKey, DirectiveModelEntry,
    };

    let inventories = DirectiveCompletionInventories {
            models: vec![DirectiveModelEntry {
                value: "opus".to_string(),
                display: "opus".to_string(),
                detail: String::new(),
                documentation: "Claude".to_string(),
            }],
            model_alias_keys: vec![
                DirectiveModelAliasKey {
                    name: "coder".to_string(),
                    documentation: "Coder follow-up".to_string(),
                },
                DirectiveModelAliasKey {
                    name: "medium".to_string(),
                    documentation: "Medium alias".to_string(),
                },
            ],
            agents: vec![
                AgentCompletionEntry {
                    name: "planner".to_string(),
                    status: "RUNNING".to_string(),
                    project: "sase".to_string(),
                    kind: "agent".to_string(),
                    member_count: 1,
                    detail: String::new(),
                    documentation: String::new(),
                },
                AgentCompletionEntry {
                    name: "builders".to_string(),
                    status: "RUNNING".to_string(),
                    project: String::new(),
                    kind: "clan".to_string(),
                    member_count: 3,
                    detail: "clan · 3 members".to_string(),
                    documentation: String::new(),
                },
            ],
            beads: vec![BeadCompletionEntry {
                id: "sase-a".to_string(),
                title: "Active bug".to_string(),
                status: "in_progress".to_string(),
                type_label: "task".to_string(),
                created_at: "2026-08-01T00:00:00Z".to_string(),
                updated_at: "2026-08-20T12:00:00Z".to_string(),
                task_type: "bug".to_string(),
                project: "sase".to_string(),
            }],
            finalizers: Vec::new(),
            machines: vec![DirectiveMachineEntry {
                alias: "apollo".to_string(),
                display: "apollo".to_string(),
                provider_ref: "builtin@https".to_string(),
                installation_id: "sase_inst_v1_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
                endpoint: "https://fleet.example.test".to_string(),
                status: "ok".to_string(),
                documentation: "Remote workstation".to_string(),
            }],
            excluded_bead_ids: Vec::new(),
            enabled_feature_flags: Vec::new(),
        };

    let insertions = |text: &str, character: u32| -> Vec<String> {
        let list = build_directive_clause_candidates(
            &classify(text, character),
            &inventories,
        );
        list.candidates
            .into_iter()
            .map(|candidate| candidate.insertion)
            .collect()
    };

    let at_end = |text: &str| insertions(text, text.len() as u32);
    assert_eq!(
        at_end("%wait("),
        [
            "agent=", "bead=", "hood=", "proc=", "time=", "unit=", "builders",
            "planner"
        ]
    );
    assert_eq!(at_end("%wait:"), ["builders", "planner"]);
    assert!(at_end("%wait:t").iter().all(|value| !value.ends_with('=')));
    assert_eq!(at_end("%wait(bead="), ["sase-a"]);
    assert_eq!(at_end("%wait(time="), ["5m", "1430"]);
    let queue_insertions = |text: &str| -> Vec<String> {
        let list = build_directive_clause_candidates(
            &classify(text, text.len() as u32),
            &inventories,
        );
        list.candidates
            .into_iter()
            .map(|candidate| candidate.insertion)
            .collect()
    };
    assert_eq!(
        queue_insertions("%q("),
        ["capacity=", "p=", "priority=", "w=", "weight=", "0", "1"]
    );
    assert_eq!(queue_insertions("%q:"), ["0", "1"]);
    assert_eq!(
        queue_insertions("%q(5, "),
        ["p=", "priority=", "w=", "weight="]
    );
    assert_eq!(
        queue_insertions("%q(capacity=5, "),
        ["p=", "priority=", "w=", "weight="]
    );
    assert_eq!(
        queue_insertions("%q(p=20, "),
        ["capacity=", "w=", "weight=", "0", "1"]
    );
    assert_eq!(
        queue_insertions("%q(w=0.25, "),
        ["capacity=", "p=", "priority=", "0", "1"]
    );
    assert_eq!(queue_insertions("%q(weight="), ["0", "0.25", "1.0", "2.0"]);
    assert!(queue_insertions("%q(")
        .iter()
        .all(|value| value != "planner" && value != "builders"));
    assert_eq!(
        queue_insertions("%wait("),
        [
            "agent=", "bead=", "hood=", "proc=", "time=", "unit=", "builders",
            "planner"
        ]
    );
    assert_eq!(at_end("%id(worker, clan="), ["builders"]);
    assert_eq!(at_end("%id(worker, clan=builders, "), ["bead="]);
    assert!(at_end("%clan(re").is_empty());
    assert_eq!(
        at_end("%clan(research, su"),
        ["summary=", "summary_script="]
    );
    assert_eq!(at_end("%clan(research, summary=hi, "), ["tribe="]);
    assert_eq!(at_end("%repeat:"), ["2", "3"]);
    assert_eq!(at_end("%xprompts_enabled:"), ["false", "true"]);
    assert_eq!(at_end("%dispatch:"), ["apollo"]);
    assert_eq!(at_end("%model(opus, "), ["coder=", "medium="]);
    assert_eq!(at_end("%model(medium, c"), ["coder="]);
    assert_eq!(at_end("%model(opus, coder="), ["opus"]);
}
