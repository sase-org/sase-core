use sase_core::command_line::{
    CommandLineGrammar, COMMAND_LINE_WIRE_SCHEMA_VERSION,
};

fn real_grammar() -> CommandLineGrammar {
    let text = include_str!("fixtures/command_line/sase_spec.json");
    CommandLineGrammar::from_json(text).expect("real spec loads")
}

fn mini_grammar() -> CommandLineGrammar {
    let text = include_str!("fixtures/command_line/mini_spec.json");
    CommandLineGrammar::from_json(text).expect("mini spec loads")
}

fn cursor_at_end(line: &str) -> usize {
    line.chars().count()
}

#[test]
fn invariant_no_node_has_both_positionals_and_subcommands() {
    let grammar = real_grammar();
    for node in grammar.nodes() {
        assert!(
            node.positionals.is_empty() || node.subcommand_ids.is_empty(),
            "node {:?} has both",
            node.canonical_path
        );
    }
}

#[test]
fn invariant_no_greedy_positional_is_followed() {
    let grammar = real_grammar();
    for node in grammar.nodes() {
        for (i, positional) in node.positionals.iter().enumerate() {
            let greedy = matches!(
                positional.capacity(),
                sase_core::command_line::PositionalCapacity::Greedy { .. }
                    | sase_core::command_line::PositionalCapacity::Remainder
            );
            if greedy {
                assert_eq!(
                    i + 1,
                    node.positionals.len(),
                    "greedy positional not last in {:?}",
                    node.canonical_path
                );
            }
        }
    }
}

#[test]
fn invariant_no_option_string_looks_like_negative_number() {
    let grammar = real_grammar();
    for node in grammar.nodes() {
        for option in &node.options {
            for string in &option.strings {
                assert!(
                    !sase_core::command_line::is_negative_number(string),
                    "option string {string} looks like a number"
                );
            }
        }
    }
}

#[test]
fn bead_cl_is_subcommand_with_close_first() {
    let grammar = real_grammar();
    let line = "bead cl";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert_eq!(context.slot.kind, "subcommand");
    assert_eq!(context.slot.prefix, "cl");
    let completed = grammar.complete(line, cursor_at_end(line), &[], &[], 100);
    assert_eq!(completed.kind, "subcommand");
    assert!(!completed.items.is_empty());
    assert_eq!(completed.items[0].insert_text, "close ");
}

#[test]
fn bead_close_reason_value_slot() {
    let grammar = real_grammar();
    let line = "bead close sase-1 --reason ";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert_eq!(context.slot.kind, "option_value");
    assert_eq!(context.slot.dest.as_deref(), Some("reason"));
    assert_eq!(context.slot.value_hint.as_deref(), Some("text"));
    assert!(context.writes);
    assert_eq!(context.node_kind, "leaf");
}

#[test]
fn unique_abbreviation_has_no_unknown_option() {
    let grammar = real_grammar();
    let line = "bead close --res";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert_eq!(context.slot.kind, "option_name");
    assert!(
        !context
            .diagnostics
            .iter()
            .any(|d| d.code == "unknown_option"),
        "diagnostics: {:?}",
        context.diagnostics
    );
    let completed = grammar.complete(line, cursor_at_end(line), &[], &[], 100);
    assert!(completed.items.iter().any(|i| i.display == "--resolution"));
    assert_eq!(completed.items[0].display, "--resolution");
}

#[test]
fn equals_value_slot_adjusts_replace_start() {
    let grammar = real_grammar();
    let line = "bead close --resolution=d";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert_eq!(context.slot.kind, "option_value");
    assert_eq!(context.slot.dest.as_deref(), Some("resolution"));
    let token = context
        .tokens
        .iter()
        .find(|t| t.text == "--resolution=d")
        .expect("token");
    let expected = token.start + "--resolution=".chars().count();
    assert_eq!(context.slot.replace_start, expected);
    let completed = grammar.complete(line, cursor_at_end(line), &[], &[], 100);
    let displays: Vec<&str> =
        completed.items.iter().map(|i| i.display.as_str()).collect();
    assert!(displays.contains(&"done"));
    assert_eq!(completed.items[0].display, "done");
}

#[test]
fn status_choices_and_invalid_choice() {
    let grammar = real_grammar();
    let line = "bead list -s ";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert_eq!(context.slot.kind, "option_value");
    let completed = grammar.complete(line, cursor_at_end(line), &[], &[], 100);
    let values: Vec<&str> =
        completed.items.iter().map(|i| i.display.as_str()).collect();
    assert!(values.contains(&"open"));
    let bad = "bead list -s bogus";
    let context = grammar.resolve(bad, cursor_at_end(bad));
    assert!(
        context
            .diagnostics
            .iter()
            .any(|d| d.code == "invalid_choice"),
        "diagnostics: {:?}",
        context.diagnostics
    );
}

#[test]
fn mutex_suppresses_remove_but_keeps_help() {
    let grammar = real_grammar();
    let line = "bead note sase-1 --edit x --";
    let completed = grammar.complete(line, cursor_at_end(line), &[], &[], 200);
    let displays: Vec<&str> =
        completed.items.iter().map(|i| i.display.as_str()).collect();
    assert!(
        !displays.contains(&"--remove"),
        "mutex member should hide: {displays:?}"
    );
    assert!(displays.contains(&"-h") || displays.contains(&"--help"));
}

#[test]
fn non_repeatable_flag_suppressed_elsewhere() {
    let grammar = real_grammar();
    let line = "bead close -f --";
    let completed = grammar.complete(line, cursor_at_end(line), &[], &[], 200);
    let displays: Vec<&str> =
        completed.items.iter().map(|i| i.display.as_str()).collect();
    assert!(
        !displays.contains(&"-f") && !displays.contains(&"--force"),
        "used flag should hide: {displays:?}"
    );
}

#[test]
fn mini_stacked_shorts_and_attached_value() {
    let grammar = mini_grammar();
    let line = "mini -abc";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert_eq!(context.node_kind, "leaf");
    assert!(context.used_dests.contains(&"alpha".to_string()));
    assert!(context.used_dests.contains(&"beta".to_string()));
    assert!(context.used_dests.contains(&"gamma".to_string()));
    let line = "mini -n5";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert!(context.used_dests.contains(&"number".to_string()));
}

#[test]
fn remainder_roles_and_argv() {
    let grammar = real_grammar();
    let line = "proc run -c /tmp -- ls -la";
    let context = grammar.resolve(line, cursor_at_end(line));
    let roles: Vec<&str> =
        context.tokens.iter().map(|t| t.role.as_str()).collect();
    assert!(roles.contains(&"separator"));
    assert!(roles.contains(&"remainder"));
    assert_eq!(context.slot.kind, "remainder");
    assert_eq!(
        context.argv,
        vec!["proc", "run", "-c", "/tmp", "--", "ls", "-la"]
    );
}

#[test]
fn prog_stripped_from_argv() {
    let grammar = real_grammar();
    let line = "sase proc run ls -la";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert_eq!(context.argv, vec!["proc", "run", "ls", "-la"]);
    assert_eq!(context.tokens[0].role, "prog");
    let roles: Vec<&str> =
        context.tokens.iter().map(|t| t.role.as_str()).collect();
    assert!(roles.contains(&"remainder"));
}

#[test]
fn bare_group_uses_default_child() {
    let grammar = real_grammar();
    for line in ["agent", "agent "] {
        let context = grammar.resolve(line, cursor_at_end(line));
        assert_eq!(context.path, vec!["agent"]);
        assert!(
            context
                .signature
                .summary
                .contains("runs 'agent list' by default"),
            "summary: {}",
            context.signature.summary
        );
        assert!(
            !context
                .diagnostics
                .iter()
                .any(|d| d.code == "missing_subcommand"),
            "diagnostics: {:?}",
            context.diagnostics
        );
    }
}

#[test]
fn run_policy_predicates() {
    let grammar = real_grammar();
    let context = grammar.resolve("run", cursor_at_end("run"));
    assert_eq!(context.run_policy.policy, "foreground");
    let line = "run .";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert_eq!(context.run_policy.policy, "foreground");
    let line = "run \"fix it\"";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert_eq!(context.run_policy.policy, "proc");
    let context = grammar.resolve("tui", cursor_at_end("tui"));
    assert_eq!(context.run_policy.policy, "deny");
    assert_eq!(
        context.run_policy.note.as_deref(),
        Some("You're already in the TUI")
    );
}

#[test]
fn unterminated_quote_reports_prefix() {
    let grammar = real_grammar();
    let line = "bead close \"sase-1";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert!(
        context
            .diagnostics
            .iter()
            .any(|d| d.code == "unterminated_quote" && d.severity == "error"),
        "diagnostics: {:?}",
        context.diagnostics
    );
    assert_eq!(context.slot.prefix, "sase-1");
}

#[test]
fn unknown_subcommand_marks_rest_unknown() {
    let grammar = real_grammar();
    let line = "nosuch cmd";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert_eq!(context.node_kind, "unknown");
    assert!(context
        .diagnostics
        .iter()
        .any(|d| d.code == "unknown_subcommand"));
    assert!(context.tokens[1..].iter().all(|t| t.role == "unknown"));
    assert_eq!(context.slot.kind, "none");
}

#[test]
fn missing_required_and_variadic_ids() {
    let grammar = real_grammar();
    let line = "bead close ";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert!(
        context
            .diagnostics
            .iter()
            .any(|d| d.code == "missing_required" && d.severity == "info"),
        "diagnostics: {:?}",
        context.diagnostics
    );
    let line = "bead close sase-1 extra";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert!(
        !context
            .diagnostics
            .iter()
            .any(|d| d.code == "extra_argument"),
        "diagnostics: {:?}",
        context.diagnostics
    );
}

#[test]
fn mini_extra_argument_beyond_fixed_nargs() {
    let grammar = mini_grammar();
    let line = "mini --req x a b extra";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert!(
        context
            .diagnostics
            .iter()
            .any(|d| d.code == "extra_argument"),
        "diagnostics: {:?}",
        context.diagnostics
    );
}

#[test]
fn confirms_flag_flow() {
    let grammar = mini_grammar();
    let line = "mini --req x a b";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert!(context.confirms);
    assert!(!context.confirm_flag_present);
    assert!(
        context
            .diagnostics
            .iter()
            .any(|d| d.code == "asks_to_confirm"),
        "diagnostics: {:?}",
        context.diagnostics
    );
    let line = "mini --req x a b -y";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert!(context.confirm_flag_present);
    assert!(
        !context
            .diagnostics
            .iter()
            .any(|d| d.code == "asks_to_confirm"),
        "diagnostics: {:?}",
        context.diagnostics
    );
}

#[test]
fn typing_in_progress_drops_prefix_diagnostic() {
    let grammar = real_grammar();
    let line = "bead clo";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert!(
        !context
            .diagnostics
            .iter()
            .any(|d| d.code == "unknown_subcommand"),
        "diagnostics: {:?}",
        context.diagnostics
    );
    let line = "bead zzz x";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert!(context
        .diagnostics
        .iter()
        .any(|d| d.code == "unknown_subcommand"));
}

#[test]
fn signature_active_and_usage() {
    let grammar = real_grammar();
    let line = "bead close sase-1 --reason ";
    let context = grammar.resolve(line, cursor_at_end(line));
    let active: Vec<&str> = context
        .signature
        .segments
        .iter()
        .filter(|s| s.active)
        .map(|s| s.text.as_str())
        .collect();
    assert!(
        !active.is_empty(),
        "segments: {:?}",
        context.signature.segments
    );
    let help = grammar
        .command_help_path(&["bead".to_string(), "close".to_string()])
        .expect("help");
    assert!(help.usage.starts_with("usage: "));
    assert!(help.usage.contains("bead close"));
}

#[test]
fn complete_dynamic_ordering_and_quoting() {
    use sase_core::command_line::DynamicCandidateWire;
    let grammar = real_grammar();
    let dynamic = vec![
        DynamicCandidateWire {
            value: "open".to_string(),
            display: None,
            description: None,
            badge: None,
            source: Some("other".to_string()),
            partial: None,
        },
        DynamicCandidateWire {
            value: "sase-1".to_string(),
            display: None,
            description: None,
            badge: None,
            source: Some("memory".to_string()),
            partial: None,
        },
    ];
    let line = "bead list -s ";
    let completed = grammar.complete(
        line,
        cursor_at_end(line),
        &dynamic,
        &["open".to_string()],
        100,
    );
    assert_eq!(completed.items[0].display, "open");
    assert!(completed.items[0].selected);
    let dynamic = vec![
        DynamicCandidateWire {
            value: "a b".to_string(),
            display: None,
            description: None,
            badge: None,
            source: None,
            partial: None,
        },
        DynamicCandidateWire {
            value: "it's".to_string(),
            display: None,
            description: None,
            badge: None,
            source: None,
            partial: None,
        },
        DynamicCandidateWire {
            value: String::new(),
            display: None,
            description: None,
            badge: None,
            source: None,
            partial: None,
        },
        DynamicCandidateWire {
            value: "ok-1.2".to_string(),
            display: None,
            description: None,
            badge: None,
            source: None,
            partial: None,
        },
    ];
    let completed =
        grammar.complete(line, cursor_at_end(line), &dynamic, &[], 100);
    let texts: std::collections::BTreeMap<String, String> = completed
        .items
        .iter()
        .map(|i| (i.display.clone(), i.insert_text.clone()))
        .collect();
    assert_eq!(texts.get("a b").map(String::as_str), Some("'a b' "));
    assert!(texts.get("it's").unwrap().ends_with(' '));
    assert!(texts.get("it's").unwrap().starts_with('\''));
    assert_eq!(texts.get("").map(String::as_str), Some("'' "));
    assert_eq!(texts.get("ok-1.2").map(String::as_str), Some("ok-1.2 "));
}

#[test]
fn complete_partial_has_no_trailing_space_and_limit() {
    use sase_core::command_line::DynamicCandidateWire;
    let grammar = real_grammar();
    let dynamic: Vec<DynamicCandidateWire> = (0..10)
        .map(|i| DynamicCandidateWire {
            value: format!("item-{i}"),
            display: None,
            description: None,
            badge: None,
            source: None,
            partial: Some(i == 0),
        })
        .collect();
    let line = "bead list -s ";
    let full = grammar.complete(line, cursor_at_end(line), &dynamic, &[], 1000);
    let partial = full
        .items
        .iter()
        .find(|i| i.display == "item-0")
        .expect("item-0 present");
    assert!(!partial.insert_text.ends_with(' '));
    let complete = full
        .items
        .iter()
        .find(|i| i.display == "item-1")
        .expect("item-1 present");
    assert!(complete.insert_text.ends_with(' '));
    let truncated =
        grammar.complete(line, cursor_at_end(line), &dynamic, &[], 3);
    assert_eq!(truncated.items.len(), 3);
    assert_eq!(truncated.total, full.total);
}

#[test]
fn complete_dedupe_ors_selected() {
    use sase_core::command_line::DynamicCandidateWire;
    let grammar = real_grammar();
    let dynamic = vec![
        DynamicCandidateWire {
            value: "open".to_string(),
            display: None,
            description: None,
            badge: None,
            source: Some("memory".to_string()),
            partial: None,
        },
        DynamicCandidateWire {
            value: "open".to_string(),
            display: None,
            description: None,
            badge: None,
            source: Some("other".to_string()),
            partial: None,
        },
    ];
    let line = "bead list -s ";
    let completed = grammar.complete(
        line,
        cursor_at_end(line),
        &dynamic,
        &["open".to_string()],
        100,
    );
    let opens: Vec<&sase_core::command_line::CompletionItemWire> = completed
        .items
        .iter()
        .filter(|i| i.display == "open")
        .collect();
    assert_eq!(opens.len(), 1);
    assert!(opens[0].selected);
}

#[test]
fn schema_version_is_one() {
    let grammar = real_grammar();
    let context = grammar.resolve("", 0);
    assert_eq!(context.schema_version, COMMAND_LINE_WIRE_SCHEMA_VERSION);
    assert_eq!(COMMAND_LINE_WIRE_SCHEMA_VERSION, 1);
    let completed = grammar.complete("", 0, &[], &[], 10);
    assert_eq!(completed.schema_version, 1);
}

#[test]
fn command_help_shapes() {
    let grammar = real_grammar();
    let help = grammar
        .command_help_path(&["bead".to_string(), "close".to_string()])
        .expect("help");
    assert!(!help.options.is_empty());
    assert!(help.children.is_empty());
    assert!(help.writes);
    let group = grammar
        .command_help_path(&["bead".to_string()])
        .expect("group help");
    assert!(!group.children.is_empty());
    assert_eq!(group.default_child.as_deref(), Some("list"));
    assert!(grammar.command_help_path(&["nosuch".to_string()]).is_none());
}

#[test]
fn mini_alias_and_required_option() {
    let grammar = mini_grammar();
    let line = "b close sase-1";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert_eq!(context.path, vec!["bead", "close"]);
    let line = "mini a b";
    let context = grammar.resolve(line, cursor_at_end(line));
    assert!(
        context
            .diagnostics
            .iter()
            .any(|d| d.code == "missing_required"),
        "diagnostics: {:?}",
        context.diagnostics
    );
}
