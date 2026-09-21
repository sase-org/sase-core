//! Directive clause/value tests, covering
//! `super::super::directive_candidates`.

use super::super::*;
use super::support::*;
use crate::editor::directive::directive_argument_candidates;
use crate::editor::token::DocumentSnapshot;
use crate::editor::wire::{
    CompletionContextKind, DirectiveCompletionInventories,
};
use crate::effort::EFFORT_LEVELS_ORDERED;

#[test]
fn effort_and_auto_directive_arguments_classify_with_candidates() {
    let catalog = entries();
    let cases: Vec<(&str, u32, &str, &str, Vec<&str>)> = vec![
        ("%effort:", 8, "effort", "", EFFORT_LEVELS_ORDERED.to_vec()),
        (
            "%effort:xh",
            10,
            "effort",
            "xh",
            EFFORT_LEVELS_ORDERED.to_vec(),
        ),
        // The `%e` alias classifies under the canonical `effort` context.
        ("%e:", 3, "effort", "", EFFORT_LEVELS_ORDERED.to_vec()),
        ("%e:xh", 5, "effort", "xh", EFFORT_LEVELS_ORDERED.to_vec()),
        ("%final:", 7, "final", "", vec!["none"]),
        ("%final:n", 8, "final", "n", vec!["none"]),
        ("%auto:", 6, "auto", "", vec!["plan", "tale", "epic"]),
        ("%auto:t", 7, "auto", "t", vec!["plan", "tale", "epic"]),
    ];

    for (text, col, directive_name, token, expected_values) in cases {
        let doc = DocumentSnapshot::new(text);
        let context =
            classify_completion_context(&doc, pos(col), &catalog).unwrap();
        assert_eq!(context.kind, CompletionContextKind::DirectiveArgument);
        assert_eq!(context.directive_name.as_deref(), Some(directive_name));
        assert_eq!(context.token.as_ref().unwrap().text, token);

        let candidates =
            directive_argument_candidates(directive_name).candidates;
        let values: Vec<&str> =
            candidates.iter().map(|c| c.insertion.as_str()).collect();
        assert_eq!(values, expected_values, "{text}");
    }
}
fn sample_finalizer_inventories() -> DirectiveCompletionInventories {
    DirectiveCompletionInventories {
        finalizers: vec![
            crate::editor::DirectiveFinalizerEntry {
                value: "zoom".to_string(),
                provider_ref: "builtin@command".to_string(),
                documentation: "Optional zoom check".to_string(),
                ..Default::default()
            },
            crate::editor::DirectiveFinalizerEntry {
                value: "lint".to_string(),
                provider_ref: "builtin@command".to_string(),
                is_default: true,
                after: vec!["format".to_string()],
                max_attempts: Some(2),
                documentation: "Lint the tree".to_string(),
                ..Default::default()
            },
            crate::editor::DirectiveFinalizerEntry {
                value: "commit".to_string(),
                provider_ref: "builtin@commit".to_string(),
                required: true,
                is_default: true,
                documentation: "Commit attributable repository changes"
                    .to_string(),
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}
fn finalizer_insertions(
    text: &str,
    cursor: u32,
    inventories: &DirectiveCompletionInventories,
) -> Vec<String> {
    let document = DocumentSnapshot::new(text);
    let context =
        classify_completion_context(&document, pos(cursor), &entries())
            .unwrap();
    build_directive_clause_candidates(&context, inventories)
        .candidates
        .into_iter()
        .map(|candidate| candidate.insertion)
        .collect()
}
#[test]
fn final_directive_completes_add_and_remove_instance_selectors() {
    let inventories = DirectiveCompletionInventories {
        finalizers: vec![
            crate::editor::DirectiveFinalizerEntry {
                value: "commit".to_string(),
                display: "commit".to_string(),
                detail: "builtin@commit".to_string(),
                documentation: "Commit attributable repository changes"
                    .to_string(),
                ..Default::default()
            },
            crate::editor::DirectiveFinalizerEntry {
                value: "lint".to_string(),
                display: "lint".to_string(),
                detail: "builtin@command".to_string(),
                documentation: String::new(),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    for (text, cursor, expected) in [
        ("%final:c", 8, vec!["commit"]),
        ("%final:!c", 9, vec!["!commit"]),
        ("%final:none %final:l", 20, vec!["lint"]),
        ("%final:LINT", 11, vec!["lint"]),
    ] {
        assert_eq!(
            finalizer_insertions(text, cursor, &inventories),
            expected,
            "{text}"
        );
    }
}
#[test]
fn final_directive_orders_required_default_optional_then_clear() {
    let inventories = sample_finalizer_inventories();
    let optional_only = DirectiveCompletionInventories {
        finalizers: vec![
            crate::editor::DirectiveFinalizerEntry {
                value: "zoom".to_string(),
                ..Default::default()
            },
            crate::editor::DirectiveFinalizerEntry {
                value: "lint".to_string(),
                is_default: true,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    assert_eq!(
        finalizer_insertions("%final:", 7, &inventories),
        vec!["commit", "lint", "zoom"]
    );
    assert_eq!(
        finalizer_insertions("%final:", 7, &optional_only),
        vec!["lint", "zoom", "none"]
    );
    assert_eq!(
        finalizer_insertions("%final:n", 8, &optional_only),
        vec!["none"]
    );
}
#[test]
fn final_directive_omits_required_from_remove_and_clear_when_invalid() {
    let inventories = sample_finalizer_inventories();
    assert_eq!(
        finalizer_insertions("%final:!", 8, &inventories),
        vec!["!lint", "!zoom"]
    );
    assert_eq!(
        finalizer_insertions("%final:none", 11, &inventories),
        Vec::<String>::new()
    );
    let labels = {
        let document = DocumentSnapshot::new("%final:!");
        let context =
            classify_completion_context(&document, pos(8), &entries()).unwrap();
        build_directive_clause_candidates(&context, &inventories)
            .candidates
            .into_iter()
            .map(|candidate| {
                (candidate.display, candidate.kind, candidate.status)
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        labels,
        vec![
            (
                "!lint".to_string(),
                "finalizer_remove".to_string(),
                "default".to_string()
            ),
            (
                "!zoom".to_string(),
                "finalizer_remove".to_string(),
                "optional".to_string()
            ),
        ]
    );
}
#[test]
fn final_directive_replaces_only_the_active_parenthesized_clause() {
    let inventories = sample_finalizer_inventories();
    let text = "%final(commit, !l";
    let document = DocumentSnapshot::new(text);
    let context = classify_completion_context(
        &document,
        pos(text.len() as u32),
        &entries(),
    )
    .unwrap();
    assert_eq!(context.token.as_ref().unwrap().text, "!l");
    let list = build_directive_clause_candidates(&context, &inventories);
    assert_eq!(
        list.candidates
            .iter()
            .map(|candidate| candidate.insertion.as_str())
            .collect::<Vec<_>>(),
        vec!["!lint"]
    );
    let replacement = list.candidates[0].replacement.as_ref().unwrap();
    assert_eq!(replacement.range.start.character, 15);
    assert_eq!(replacement.range.end.character, text.len() as u32);
    assert_eq!(replacement.new_text, "!lint");

    let unicode = "%final(café, c";
    let document = DocumentSnapshot::new(unicode);
    let cursor = document
        .byte_offset_to_position(unicode.len())
        .expect("utf-16 cursor");
    let context =
        classify_completion_context(&document, cursor, &entries()).unwrap();
    assert_eq!(context.token.as_ref().unwrap().text, "c");
    assert_eq!(
        context.replacement_range.start.character,
        "%final(café, ".chars().map(char::len_utf16).sum::<usize>() as u32
    );
    let list = build_directive_clause_candidates(&context, &inventories);
    assert_eq!(
        list.candidates[0].replacement.as_ref().unwrap().new_text,
        "commit"
    );
}
#[test]
fn final_directive_documents_provider_dependencies_and_retry_policy() {
    let inventories = sample_finalizer_inventories();
    let document = DocumentSnapshot::new("%final:l");
    let context =
        classify_completion_context(&document, pos(8), &entries()).unwrap();
    let list = build_directive_clause_candidates(&context, &inventories);
    let lint = &list.candidates[0];
    assert_eq!(lint.status, "default");
    assert_eq!(lint.detail.as_deref(), Some("builtin@command"));
    let documentation = lint.documentation.as_deref().unwrap();
    assert!(documentation.contains("Lint the tree"));
    assert!(documentation.contains("Provider: `builtin@command`"));
    assert!(documentation.contains("Depends on: `format`"));
    assert!(documentation.contains("Retry policy: 2 attempts"));
}
#[test]
fn directive_keyword_completion_targets_only_the_post_comma_fragment() {
    let catalog = entries();
    for (text, cursor, expected_start, directive_name, keyword) in [
        ("%clan(research, tr)", 18, 16, "clan", "tribe="),
        ("%c(research, tr)", 15, 13, "clan", "tribe="),
        ("%clan(research, su)", 18, 16, "clan", "summary="),
        ("%clan(research, su)", 18, 16, "clan", "summary_script="),
        ("%id(worker, cl)", 14, 12, "id", "clan="),
        ("%i(worker, cl)", 13, 11, "id", "clan="),
        ("%id(worker, fa)", 14, 12, "id", "family="),
        ("%i(worker, fa)", 13, 11, "id", "family="),
        ("%id(worker, tr)", 14, 12, "id", "tribe="),
        ("%i(worker, tr)", 13, 11, "id", "tribe="),
    ] {
        let doc = DocumentSnapshot::new(text);
        let context = classify_completion_context(&doc, pos(cursor), &catalog)
            .expect("directive keyword completion context");
        assert_eq!(
            context.kind,
            CompletionContextKind::DirectiveArgumentKeyword,
            "{text}"
        );
        assert_eq!(context.directive_name.as_deref(), Some(directive_name));
        assert_eq!(context.token.as_ref().unwrap().text, &keyword[..2]);
        assert_eq!(
            context.replacement_range,
            doc.byte_range_to_range(expected_start, cursor as usize)
                .unwrap()
        );

        let candidates =
            directive_argument_candidates(directive_name).candidates;
        assert!(
            candidates
                .iter()
                .any(|candidate| candidate.insertion == keyword),
            "missing {keyword} candidate for {text}: {candidates:?}"
        );
    }
}
#[test]
fn directive_keyword_completion_stays_out_of_positional_and_value_positions() {
    let catalog = entries();
    for (open, closed_text, directive_name) in [
        ("%clan(re", "%clan(research, tribe=blue)", "clan"),
        ("%id(wo", "%id(worker, clan=research)", "id"),
    ] {
        let doc = DocumentSnapshot::new(open);
        let positional_context =
            classify_completion_context(&doc, pos(open.len() as u32), &catalog)
                .unwrap();
        assert_eq!(
            positional_context.kind,
            CompletionContextKind::DirectiveArgument
        );
        assert_eq!(
            positional_context.directive_name.as_deref(),
            Some(directive_name)
        );

        let value = if directive_name == "clan" {
            "blue"
        } else {
            "research"
        };
        let value_start = closed_text.find(value).unwrap();
        let doc = DocumentSnapshot::new(closed_text);
        let value_context = classify_completion_context(
            &doc,
            pos((closed_text.len() - 1) as u32),
            &catalog,
        )
        .unwrap();
        assert_eq!(
            value_context.kind,
            CompletionContextKind::DirectiveArgumentValue
        );
        assert_eq!(
            value_context.directive_name.as_deref(),
            Some(directive_name)
        );
        assert_eq!(value_context.token.as_ref().unwrap().text, value);
        assert_eq!(
            value_context.replacement_range,
            doc.byte_range_to_range(value_start, closed_text.len() - 1)
                .unwrap()
        );

        let closed = DocumentSnapshot::new(closed_text);
        let closed_context = classify_completion_context(
            &closed,
            pos(closed.text().len() as u32),
            &catalog,
        );
        assert!(!closed_context.is_some_and(|context| matches!(
            context.kind,
            CompletionContextKind::DirectiveArgument
                | CompletionContextKind::DirectiveArgumentKeyword
                | CompletionContextKind::DirectiveArgumentValue
        )));
    }
}
