//! Catalog-backed candidate tests (xprompt, agent, wait/hold/queue,
//! identity/hood, snippet), covering `super::super::assist_candidates`.

use super::super::*;
use super::support::*;
use crate::editor::token::DocumentSnapshot;
use crate::editor::wire::{
    AgentCompletionEntry, CompletionContextKind,
    DirectiveCompletionInventories, DirectiveSyntaxForm,
};
use crate::{EditorSnippetEntryWire, MemoryTierWire};
use std::collections::BTreeSet;

#[test]
fn builds_catalog_completions_with_marker_filters() {
    let catalog = entries();
    let inline = build_xprompt_completion_candidates("#r", None, &catalog);
    assert_eq!(
        inline
            .candidates
            .iter()
            .map(|c| c.insertion.as_str())
            .collect::<Vec<_>>(),
        vec!["#review", "#!run"]
    );

    let standalone = build_xprompt_completion_candidates("#!r", None, &catalog);
    assert_eq!(standalone.candidates[0].insertion, "#!run");

    // Slash completion offers the provider skill name, never the
    // namespaced xprompt reference, and a non-skill workflow that happens
    // to share the prefix is not a slash candidate.
    let skill = build_xprompt_completion_candidates("/p", None, &catalog);
    assert_eq!(
        skill
            .candidates
            .iter()
            .map(|c| c.insertion.as_str())
            .collect::<Vec<_>>(),
        vec!["/plan"]
    );
    assert!(build_xprompt_completion_candidates("/r", None, &catalog)
        .candidates
        .is_empty());

    // The same skill is reachable inline only through `#skill/plan`.
    let namespaced =
        build_xprompt_completion_candidates("#skill/", None, &catalog);
    assert_eq!(
        namespaced
            .candidates
            .iter()
            .map(|c| c.insertion.as_str())
            .collect::<Vec<_>>(),
        vec!["#skill/plan"]
    );
    assert!(
        build_xprompt_completion_candidates("#skills/", None, &catalog)
            .candidates
            .is_empty()
    );
    assert!(build_xprompt_completion_candidates("#plan", None, &catalog)
        .candidates
        .is_empty());
}
#[test]
fn memory_completes_only_through_the_memory_namespace() {
    let catalog = entries();

    let namespaced =
        build_xprompt_completion_candidates("#memory/", None, &catalog);
    assert_eq!(
        namespaced
            .candidates
            .iter()
            .map(|c| c.insertion.as_str())
            .collect::<Vec<_>>(),
        vec!["#memory/glossary"]
    );
    // No bare alias exists, and a memory note is never a slash skill.
    assert!(
        build_xprompt_completion_candidates("#glossary", None, &catalog)
            .candidates
            .is_empty()
    );
    assert!(
        build_xprompt_completion_candidates("/glossary", None, &catalog)
            .candidates
            .is_empty()
    );
    // The tier survives the catalog-to-assist projection.
    let entry = catalog
        .iter()
        .find(|entry| entry.name == "memory/glossary")
        .unwrap();
    assert_eq!(entry.memory_type, Some(MemoryTierWire::Core));
}
#[test]
fn snippet_context_does_not_steal_higher_priority_tokens() {
    let catalog = entries();
    for text in ["#foo", "/foo", "@foo", "%model", "./foo", ""] {
        let doc = DocumentSnapshot::new(text);
        let context =
            classify_completion_context(&doc, pos(text.len() as u32), &catalog);
        assert_ne!(
            context.map(|context| context.kind),
            Some(CompletionContextKind::SnippetTrigger),
            "{text}"
        );
    }
}
#[test]
fn builds_snippet_completions_by_case_insensitive_prefix() {
    let list = build_snippet_completion_candidates(
        "fo",
        None,
        &[
            snippet_entry("Foo", "body $1$0", "ace.snippets"),
            snippet_entry("bar", "bar", "xprompt"),
        ],
    );

    assert_eq!(list.candidates.len(), 1);
    assert_eq!(list.candidates[0].display, "Foo");
    assert_eq!(list.candidates[0].insertion, "body $1$0");
    assert_eq!(list.candidates[0].detail.as_deref(), Some("ace.snippets"));
}
fn snippet_entry(
    trigger: &str,
    template: &str,
    source: &str,
) -> EditorSnippetEntryWire {
    EditorSnippetEntryWire {
        trigger: trigger.to_string(),
        template: template.to_string(),
        source: source.to_string(),
        xprompt_name: None,
        description: None,
        source_path_display: None,
    }
}
#[test]
fn agent_candidates_are_kind_aware_ordered_and_compatible() {
    let old_entry: AgentCompletionEntry =
        serde_json::from_value(serde_json::json!({
            "name": "legacy",
            "status": "DONE",
            "project": "sase"
        }))
        .unwrap();
    assert_eq!(old_entry.kind, "");

    let agent_entries = vec![
        old_entry,
        agent_target("review", "agent", 1, "DONE · sase"),
        agent_target("review", "family", 3, "family · 3 members"),
        agent_target("builders", "clan", 2, "clan · 2 members"),
        agent_target("@reviewers", "tribe", 4, "tribe · 4 agents"),
    ];
    let list = build_agent_completion_candidates("", None, &agent_entries, &[]);
    assert_eq!(
        list.candidates
            .iter()
            .map(|candidate| (
                candidate.kind.as_str(),
                candidate.insertion.as_str()
            ))
            .collect::<Vec<_>>(),
        vec![
            ("tribe", "@reviewers"),
            ("clan", "builders"),
            ("session", "review"),
            ("agent", "legacy"),
        ]
    );

    let bare_tribe =
        build_agent_completion_candidates("rev", None, &agent_entries, &[]);
    assert_eq!(bare_tribe.candidates[0].insertion, "@reviewers");
    assert_eq!(bare_tribe.candidates[0].name, "reviewers");

    // New-spelling helper rows filter exactly like the legacy kind.
    let new_spelling = vec![agent_target("review", "session", 3, "3 members")];
    let legacy_filtered = build_identity_target_candidates(
        "",
        None,
        &agent_entries,
        "family",
        &[],
    );
    let new_filtered = build_identity_target_candidates(
        "",
        None,
        &new_spelling,
        "family",
        &[],
    );
    assert_eq!(
        new_filtered
            .candidates
            .iter()
            .map(|c| c.insertion.as_str())
            .collect::<Vec<_>>(),
        legacy_filtered
            .candidates
            .iter()
            .filter(|c| c.insertion == "review")
            .map(|c| c.insertion.as_str())
            .collect::<Vec<_>>(),
    );
    let sigil_tribe =
        build_agent_completion_candidates("@rev", None, &agent_entries, &[]);
    assert_eq!(sigil_tribe.candidates[0].insertion, "@reviewers");
    assert_eq!(sigil_tribe.candidates[0].name, "@reviewers");
}
#[test]
fn agent_candidates_carry_documentation_only_when_present() {
    let mut documented =
        agent_target("review", "family", 3, "family · 3 members");
    documented.documentation = "# review\n\nplan preview".to_string();
    let bare = agent_target("builders", "clan", 2, "clan · 2 members");
    let entries = vec![documented, bare];

    let list = build_agent_completion_candidates("", None, &entries, &[]);
    let documentation = |name: &str| {
        list.candidates
            .iter()
            .find(|candidate| candidate.insertion == name)
            .and_then(|candidate| candidate.documentation.clone())
    };
    assert_eq!(
        documentation("review"),
        Some("# review\n\nplan preview".to_string())
    );
    assert_eq!(documentation("builders"), None);
}
#[test]
fn wait_candidates_merge_keywords_and_exclude_selected_values() {
    let entries = vec![
        agent_target("worker", "agent", 1, "RUNNING · sase"),
        agent_target("review", "family", 2, "family · 2 members"),
        agent_target("builders", "clan", 3, "clan · 3 members"),
        agent_target("@ops", "tribe", 4, "tribe · 4 agents"),
    ];
    let all = build_wait_completion_candidates("", None, &entries, &[]);
    assert_eq!(
        all.candidates
            .iter()
            .map(|candidate| candidate.insertion.as_str())
            .collect::<Vec<_>>(),
        vec![
            "agent=", "bead=", "hood=", "proc=", "time=", "unit=", "@ops",
            "builders", "review", "worker"
        ]
    );

    let selected = vec!["time=5m".to_string(), "builders".to_string()];
    let narrowed =
        build_wait_completion_candidates("", None, &entries, &selected);
    assert_eq!(
        narrowed
            .candidates
            .iter()
            .map(|candidate| candidate.insertion.as_str())
            .collect::<Vec<_>>(),
        vec![
            "agent=", "bead=", "hood=", "proc=", "unit=", "@ops", "review",
            "worker"
        ]
    );

    let colon = build_wait_completion_candidates_for_form(
        "t",
        None,
        &entries,
        &[],
        DirectiveSyntaxForm::Colon,
    );
    assert_eq!(
        colon
            .candidates
            .iter()
            .map(|candidate| candidate.insertion.as_str())
            .collect::<Vec<_>>(),
        Vec::<&str>::new()
    );
}
#[test]
fn hold_candidates_prioritize_waiting_targets_and_include_procs() {
    let agent_entries = vec![
        AgentCompletionEntry {
            name: "coder".to_string(),
            status: "QUEUED".to_string(),
            project: "sase".to_string(),
            kind: "agent".to_string(),
            member_count: 0,
            detail: String::new(),
            documentation: String::new(),
        },
        AgentCompletionEntry {
            name: "planner".to_string(),
            status: "WAITING".to_string(),
            project: "sase".to_string(),
            kind: "agent".to_string(),
            member_count: 0,
            detail: String::new(),
            documentation: String::new(),
        },
        agent_target("ship", "family", 2, "family · 2 members"),
        agent_target("review", "clan", 3, "clan · 3 members"),
        agent_target("@builders", "tribe", 4, "tribe · 4 agents"),
        agent_target("sase-11l", "hood", 2, "hood · 2 members"),
        AgentCompletionEntry {
            name: "build-shell".to_string(),
            status: "PENDING".to_string(),
            project: "sase".to_string(),
            kind: "proc".to_string(),
            member_count: 0,
            detail: "proc · PENDING".to_string(),
            documentation: String::new(),
        },
    ];
    let document = DocumentSnapshot::new("%hold(");
    let context = classify_completion_context(
        &document,
        pos(document.text().len() as u32),
        &entries(),
    )
    .expect("hold completion context");
    let inventories = DirectiveCompletionInventories {
        agents: agent_entries,
        ..Default::default()
    };

    let list = build_directive_clause_candidates(&context, &inventories);

    assert_eq!(
        list.candidates
            .iter()
            .map(|candidate| candidate.insertion.as_str())
            .collect::<Vec<_>>(),
        vec![
            "hood=",
            "scope=",
            "ttl=",
            "tribe=",
            "pending",
            "future",
            "planner",
            "coder",
            "@builders",
            "review",
            "ship",
            "build-shell",
        ]
    );
    let proc = list
        .candidates
        .iter()
        .find(|candidate| candidate.insertion == "build-shell")
        .expect("proc row");
    assert_eq!(proc.kind, "proc");
    assert_eq!(proc.detail.as_deref(), Some("proc · PENDING"));
}
#[test]
fn hood_value_candidates_include_explicit_and_derived_hoods() {
    let agent_entries = vec![
        agent_target("sase-11l", "hood", 2, "hood · 2 members"),
        AgentCompletionEntry {
            name: "sase-abc.1".to_string(),
            status: "RUNNING".to_string(),
            project: "sase".to_string(),
            kind: "agent".to_string(),
            member_count: 0,
            detail: String::new(),
            documentation: String::new(),
        },
    ];
    let document = DocumentSnapshot::new("%wait(hood=s");
    let context = classify_completion_context(
        &document,
        pos(document.text().len() as u32),
        &entries(),
    )
    .expect("wait hood completion context");
    let inventories = DirectiveCompletionInventories {
        agents: agent_entries,
        ..Default::default()
    };

    let list = build_directive_clause_candidates(&context, &inventories);

    assert_eq!(
        list.candidates
            .iter()
            .map(|candidate| {
                (
                    candidate.kind.as_str(),
                    candidate.insertion.as_str(),
                    candidate.detail.as_deref(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            ("hood", "sase-11l", Some("hood · 2 members")),
            ("hood", "sase-abc", Some("hood · 1 member")),
            ("hood", "sase-abc.1", Some("hood · 1 member")),
        ]
    );
}
#[test]
fn wait_context_narrows_to_active_clause_and_tracks_selected_values() {
    for text in [
        "%wait:planner,@ops, bu",
        "%wait(planner, @ops, bu",
        "%w(planner, @ops, bu",
    ] {
        let doc = DocumentSnapshot::new(text);
        let context = classify_completion_context(
            &doc,
            pos(text.len() as u32),
            &entries(),
        )
        .expect("wait completion context");
        let token_start = text.rfind("bu").unwrap();
        assert_eq!(context.kind, CompletionContextKind::DirectiveArgument);
        assert_eq!(context.directive_name.as_deref(), Some("wait"));
        assert_eq!(context.token.as_ref().unwrap().text, "bu");
        assert_eq!(
            context.replacement_range,
            doc.byte_range_to_range(token_start, text.len()).unwrap()
        );
        assert_eq!(context.selected_values, vec!["planner", "@ops"]);
    }

    for text in [
        "%wait:planner,@ops,builders",
        "%wait(planner, @ops, builders)",
    ] {
        let cursor = text.find("pl").unwrap() + 2;
        let doc = DocumentSnapshot::new(text);
        let context =
            classify_completion_context(&doc, pos(cursor as u32), &entries())
                .expect("earlier wait clause completion context");
        assert_eq!(context.token.as_ref().unwrap().text, "pl");
        assert_eq!(context.selected_values, vec!["@ops", "builders"]);
        assert_eq!(
            context.replacement_range,
            doc.byte_range_to_range(
                text.find("pl").unwrap(),
                text.find("planner").unwrap() + "planner".len(),
            )
            .unwrap()
        );
    }
}
#[test]
fn model_at_suffix_completes_effort_vocabulary() {
    let catalog = entries();

    // Right after the `@`, the context targets the effort vocabulary.
    let doc = DocumentSnapshot::new("%model:opus@");
    let context = classify_completion_context(&doc, pos(12), &catalog).unwrap();
    assert_eq!(context.kind, CompletionContextKind::DirectiveArgument);
    assert_eq!(context.directive_name.as_deref(), Some("effort"));
    assert_eq!(context.token.as_ref().unwrap().text, "");

    // A partially-typed level keeps the effort context; the token after the
    // `@` is what the editor filters the effort vocabulary against.
    let doc = DocumentSnapshot::new("%model:opus@xh");
    let context = classify_completion_context(&doc, pos(14), &catalog).unwrap();
    assert_eq!(context.directive_name.as_deref(), Some("effort"));
    assert_eq!(context.token.as_ref().unwrap().text, "xh");

    // Before the `@`, it is still the model argument.
    let doc = DocumentSnapshot::new("%model:opus");
    let context = classify_completion_context(&doc, pos(11), &catalog).unwrap();
    assert_eq!(context.directive_name.as_deref(), Some("model"));

    // Provider-qualified models keep the slash-bearing model token.
    let doc = DocumentSnapshot::new("%model:claude/");
    let context = classify_completion_context(&doc, pos(14), &catalog).unwrap();
    assert_eq!(context.directive_name.as_deref(), Some("model"));
    assert_eq!(context.token.as_ref().unwrap().text, "claude/");

    let doc = DocumentSnapshot::new("%model:claude/opus@");
    let context = classify_completion_context(&doc, pos(19), &catalog).unwrap();
    assert_eq!(context.directive_name.as_deref(), Some("effort"));
    assert_eq!(context.token.as_ref().unwrap().text, "");

    // A leading `@` is the alias marker, not an effort suffix.
    let doc = DocumentSnapshot::new("%model:@oth");
    let context = classify_completion_context(&doc, pos(11), &catalog).unwrap();
    assert_eq!(context.directive_name.as_deref(), Some("model"));
    assert_eq!(context.token.as_ref().unwrap().text, "@oth");

    let doc = DocumentSnapshot::new("%model:@");
    let context = classify_completion_context(&doc, pos(8), &catalog).unwrap();
    assert_eq!(context.directive_name.as_deref(), Some("model"));
    assert_eq!(context.token.as_ref().unwrap().text, "@");
}
#[test]
fn builds_argument_name_completions() {
    let catalog = entries();
    let list = build_xprompt_arg_name_candidates(
        &catalog[0],
        &BTreeSet::from(["path".to_string()]),
        "d",
        None,
    );
    assert_eq!(list.candidates[0].insertion, "deep=");
    assert_eq!(
        list.candidates[0].documentation.as_deref(),
        Some("Run a deeper pass\n\ndefault: false")
    );
}
