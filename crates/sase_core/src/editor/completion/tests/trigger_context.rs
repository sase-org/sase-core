//! Trigger and context-detection tests, covering
//! `super::super::trigger_context`.

use super::super::*;
use super::support::*;
use crate::editor::token::DocumentSnapshot;
use crate::editor::wire::{
    AgentCompletionEntry, CompletionContextKind, XpromptAssistEntry,
    XpromptInputHint,
};

#[test]
fn classifies_primary_completion_modes() {
    let catalog = entries();
    for (text, col, kind) in [
        ("#re", 3, CompletionContextKind::Xprompt),
        ("/ru", 3, CompletionContextKind::SlashSkill),
        ("./sr", 4, CompletionContextKind::FilePath),
        ("", 0, CompletionContextKind::FileHistory),
        ("%mo", 3, CompletionContextKind::DirectiveName),
        ("%model:", 7, CompletionContextKind::DirectiveArgument),
        ("foo", 3, CompletionContextKind::SnippetTrigger),
        ("foo_1", 5, CompletionContextKind::SnippetTrigger),
    ] {
        let doc = DocumentSnapshot::new(text);
        let context =
            classify_completion_context(&doc, pos(col), &catalog).unwrap();
        assert_eq!(context.kind, kind, "{text}");
    }
}
#[test]
fn placeholder_context_precedes_other_explicit_completion_modes() {
    let catalog = entries();
    let workflow_names = vec!["gh".to_string()];
    for (text, col) in [
        ("<", 1),
        ("%model:<", 8),
        ("#review(path=<", 14),
        ("#gh:<", 5),
    ] {
        let document = DocumentSnapshot::new(text);
        let context = classify_completion_context_with_workflows(
            &document,
            pos(col),
            &catalog,
            &workflow_names,
        )
        .unwrap();
        assert_eq!(context.kind, CompletionContextKind::Placeholder, "{text}");
        assert_eq!(context.token.as_ref().unwrap().text, "", "{text}");
    }
}
#[test]
fn closed_placeholder_does_not_steal_following_context() {
    let document = DocumentSnapshot::new("<done> %mo");
    let context =
        classify_completion_context(&document, pos(10), &entries()).unwrap();
    assert_eq!(context.kind, CompletionContextKind::DirectiveName);
}
#[test]
fn detects_narrow_argument_contexts() {
    let catalog = entries();
    for (text, col, kind, active_input) in [
        (
            "#review:",
            8,
            CompletionContextKind::XpromptArgumentPath,
            Some("path"),
        ),
        (
            "#review(path=",
            13,
            CompletionContextKind::XpromptArgumentPath,
            Some("path"),
        ),
        (
            "#review(de",
            10,
            CompletionContextKind::XpromptArgumentName,
            None,
        ),
        (
            "#review!!:",
            10,
            CompletionContextKind::XpromptArgumentPath,
            Some("path"),
        ),
    ] {
        let doc = DocumentSnapshot::new(text);
        let context =
            classify_completion_context(&doc, pos(col), &catalog).unwrap();
        assert_eq!(context.kind, kind, "{text}");
        assert_eq!(context.active_input.as_deref(), active_input);
    }

    let doc = DocumentSnapshot::new("#ns__foo(arg=");
    let ns_entry = XpromptAssistEntry {
        name: "ns/foo".to_string(),
        display_label: "ns/foo".to_string(),
        insertion: "#ns/foo".to_string(),
        reference_prefix: "#".to_string(),
        kind: None,
        source_bucket: "project".to_string(),
        project: None,
        tags: Vec::new(),
        input_signature: None,
        inputs: vec![XpromptInputHint {
            name: "arg".to_string(),
            r#type: "word".to_string(),
            description: None,
            required: true,
            default_display: None,
            position: 0,
            repeatable: false,
        }],
        content_preview: None,
        description: None,
        skill_name: None,
        memory_type: None,
        source_path_display: None,
        definition_path: None,
        definition_range: None,
        is_skill: false,
    };
    assert!(classify_completion_context(&doc, pos(13), &[ns_entry]).is_some());
}
#[test]
fn repeatable_positionals_keep_the_tail_input_and_active_element_range() {
    let mut fork = entries()[0].clone();
    fork.name = "fork".to_string();
    fork.display_label = "fork".to_string();
    fork.insertion = "#fork".to_string();
    fork.inputs = vec![XpromptInputHint {
        name: "names".to_string(),
        r#type: "agent".to_string(),
        description: None,
        required: false,
        default_display: None,
        position: 0,
        repeatable: true,
    }];

    for text in ["😀 #fork:planner,co", "😀 #fork(planner, co"] {
        let doc = DocumentSnapshot::new(text);
        let cursor = doc.byte_offset_to_position(text.len()).unwrap();
        let context =
            classify_completion_context(&doc, cursor, &[fork.clone()]).unwrap();
        assert_eq!(context.kind, CompletionContextKind::XpromptArgumentAgent);
        assert_eq!(context.active_input.as_deref(), Some("names"));
        let token_start = text.rfind("co").unwrap();
        assert_eq!(
            context.replacement_range,
            doc.byte_range_to_range(token_start, text.len()).unwrap()
        );
        assert_eq!(context.selected_values, vec!["planner"]);
    }
}
#[test]
fn repeatable_agent_context_replaces_earlier_element_and_filters_selected() {
    let mut fork = entries()[0].clone();
    fork.name = "fork".to_string();
    fork.inputs = vec![XpromptInputHint {
        name: "names".to_string(),
        r#type: "agent".to_string(),
        description: None,
        required: false,
        default_display: None,
        position: 0,
        repeatable: true,
    }];
    let text = "😀 #fork(co, planner)";
    let doc = DocumentSnapshot::new(text);
    let cursor = doc
        .byte_offset_to_position(text.find("co").unwrap() + 2)
        .unwrap();
    let context = classify_completion_context(&doc, cursor, &[fork]).unwrap();
    assert_eq!(context.kind, CompletionContextKind::XpromptArgumentAgent);
    assert_eq!(context.selected_values, vec!["planner"]);
    assert_eq!(
        context.replacement_range,
        doc.byte_range_to_range(
            text.find("co").unwrap(),
            text.find(", planner").unwrap(),
        )
        .unwrap()
    );

    let entries = vec![
        AgentCompletionEntry {
            name: "planner".to_string(),
            status: "RUNNING".to_string(),
            project: "sase".to_string(),
            kind: String::new(),
            member_count: 0,
            detail: String::new(),
            documentation: String::new(),
        },
        AgentCompletionEntry {
            name: "coder".to_string(),
            status: "DONE".to_string(),
            project: "sase-core".to_string(),
            kind: String::new(),
            member_count: 0,
            detail: String::new(),
            documentation: String::new(),
        },
        AgentCompletionEntry {
            name: "reviewer.@".to_string(),
            status: "DONE".to_string(),
            project: "sase".to_string(),
            kind: String::new(),
            member_count: 0,
            detail: String::new(),
            documentation: String::new(),
        },
    ];
    let list = build_agent_completion_candidates(
        "",
        Some(context.replacement_range),
        &entries,
        &context.selected_values,
    );
    assert_eq!(
        list.candidates
            .iter()
            .map(|candidate| candidate.name.as_str())
            .collect::<Vec<_>>(),
        vec!["coder", "reviewer.@"]
    );
    assert_eq!(
        list.candidates[0].detail.as_deref(),
        Some("DONE · sase-core")
    );
}
