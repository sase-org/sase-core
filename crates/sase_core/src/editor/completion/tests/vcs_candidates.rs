//! `vcs:` repository/ref/project tests, covering
//! `super::super::vcs_candidates`.

use super::super::*;
use super::support::*;
use crate::editor::token::{vcs_project_trigger_token, DocumentSnapshot};
use crate::editor::wire::{
    CompletionContext, CompletionContextKind, EditorPosition, EditorTextEdit,
    VcsNamespaceEntry, VcsProjectEntry, VcsRepoEntry, XpromptAssistEntry,
    XpromptInputHint,
};

// --- vcs_repo (`#gh:owner/`) completion -------------------------------
const VCS_REPO_CURSOR: &str = "<CURSOR>";
fn workflow_names(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_string()).collect()
}
fn gh_entry() -> XpromptAssistEntry {
    XpromptAssistEntry {
        name: "gh".to_string(),
        display_label: "gh".to_string(),
        insertion: "#gh".to_string(),
        reference_prefix: "#".to_string(),
        kind: Some("workflow".to_string()),
        source_bucket: "builtin".to_string(),
        project: None,
        tags: Vec::new(),
        input_signature: Some("(gh_ref: word)".to_string()),
        inputs: vec![XpromptInputHint {
            name: "gh_ref".to_string(),
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
    }
}
fn repo_entry(name: &str, full_ref: &str) -> VcsRepoEntry {
    VcsRepoEntry {
        name: name.to_string(),
        r#ref: full_ref.to_string(),
        description: format!("{name} repo"),
        visibility: "public".to_string(),
        is_fork: false,
        is_archived: false,
        pushed_at: None,
    }
}
fn vcs_repo_context(
    text: &str,
    cursor: usize,
    names: &[&str],
) -> CompletionContext {
    let doc = DocumentSnapshot::new(text);
    let position = doc.byte_offset_to_position(cursor).unwrap();
    detect_vcs_repo_context_at_position(&doc, position, &workflow_names(names))
        .unwrap_or_else(|| panic!("expected repo context for {text:?}"))
}
fn apply_text_edit(text: &str, edit: &EditorTextEdit) -> String {
    let doc = DocumentSnapshot::new(text);
    let start = doc.position_to_byte_offset(edit.range.start).unwrap();
    let end = doc.position_to_byte_offset(edit.range.end).unwrap();
    format!("{}{}{}", &text[..start], edit.new_text, &text[end..])
}
#[test]
fn vcs_repo_golden_vectors() {
    // The cross-language parity contract -- identical to the Python
    // `VCS_REPO_GOLDEN_VECTORS` table. `<CURSOR>` marks the cursor.
    let cases = [
        (
            "#gh:bbugyi200/<CURSOR>",
            vec!["gh"],
            "bbugyi200/sase",
            "#gh:bbugyi200/sase ",
        ),
        (
            "#gh:bbugyi200/sa<CURSOR>",
            vec!["gh"],
            "bbugyi200/sase",
            "#gh:bbugyi200/sase ",
        ),
        (
            "Fix #gh:bbugyi200/sa<CURSOR> now",
            vec!["gh"],
            "bbugyi200/sase",
            "Fix #gh:bbugyi200/sase now",
        ),
        (
            "#gh!!:bbugyi200/sa<CURSOR>",
            vec!["gh"],
            "bbugyi200/sase",
            "#gh!!:bbugyi200/sase ",
        ),
        (
            "#gh(bbugyi200/sa<CURSOR>",
            vec!["gh"],
            "bbugyi200/sase",
            "#gh(bbugyi200/sase)",
        ),
        (
            "#gh(bbugyi200/sa<CURSOR>) next",
            vec!["gh"],
            "bbugyi200/sase",
            "#gh(bbugyi200/sase) next",
        ),
        (
            "#gh??(bbugyi200/<CURSOR>",
            vec!["gh"],
            "bbugyi200/sase",
            "#gh??(bbugyi200/sase)",
        ),
        (
            "#gl:group/sub/re<CURSOR>",
            vec!["gl"],
            "group/sub/repo",
            "#gl:group/sub/repo ",
        ),
        (
            "#gh:bbugyi200/s<CURSOR>asex",
            vec!["gh"],
            "bbugyi200/sase",
            "#gh:bbugyi200/sase ",
        ),
        (
            "#gh:bbugyi200/sa<CURSOR>\n",
            vec!["gh"],
            "bbugyi200/sase",
            "#gh:bbugyi200/sase \n",
        ),
    ];

    for (marked, names, selected_ref, expected) in cases {
        let cursor = marked.find(VCS_REPO_CURSOR).unwrap();
        let text = marked.replace(VCS_REPO_CURSOR, "");
        let context = vcs_repo_context(&text, cursor, &names);
        let trigger = context.vcs_repo.as_ref().unwrap();
        assert_eq!(
            apply_vcs_repo_selection(&text, trigger, selected_ref),
            expected,
            "{marked}"
        );
    }
}
#[test]
fn detects_vcs_repo_colon_spans() {
    let context = vcs_repo_context("#gh:bbugyi200/sa", 16, &["gh"]);
    let trigger = context.vcs_repo.as_ref().unwrap();

    assert_eq!(context.kind, CompletionContextKind::VcsRepo);
    assert_eq!(trigger.workflow, "gh");
    assert_eq!(trigger.separator, ":");
    assert_eq!(trigger.namespace, "bbugyi200");
    assert_eq!(trigger.query, "sa");
    assert_eq!((trigger.ref_start, trigger.ref_end), (4, 16));
    assert_eq!(trigger.namespace_span, (4, 13));
    assert_eq!(trigger.query_span, (14, 16));
    assert_eq!(context.replacement_range.start, pos(4));
    assert_eq!(context.replacement_range.end, pos(16));
}
#[test]
fn detects_vcs_repo_paren_hitl_and_nested_namespaces() {
    let context = vcs_repo_context("#gh??(bbugyi200/sa", 18, &["gh"]);
    let trigger = context.vcs_repo.as_ref().unwrap();
    assert_eq!(trigger.workflow, "gh");
    assert_eq!(trigger.separator, "(");
    assert_eq!(trigger.namespace, "bbugyi200");
    assert_eq!(trigger.query, "sa");

    let context = vcs_repo_context("#gl:group/subgroup/sa", 21, &["gl"]);
    let trigger = context.vcs_repo.as_ref().unwrap();
    assert_eq!(trigger.namespace, "group/subgroup");
    assert_eq!(trigger.query, "sa");
}
#[test]
fn classifies_vcs_repo_then_vcs_ref_before_xprompt_argument_hints() {
    let catalog = vec![gh_entry()];
    let names = workflow_names(&["gh"]);

    let doc = DocumentSnapshot::new("#gh:bbugyi200/");
    let context = classify_completion_context_with_workflows(
        &doc,
        pos(14),
        &catalog,
        &names,
    )
    .unwrap();
    assert_eq!(context.kind, CompletionContextKind::VcsRepo);

    let doc = DocumentSnapshot::new("#gh:bbugyi200");
    let context = classify_completion_context_with_workflows(
        &doc,
        pos(13),
        &catalog,
        &names,
    )
    .unwrap();
    assert_eq!(context.kind, CompletionContextKind::VcsRef);
    assert_eq!(context.active_xprompt.as_deref(), None);

    let doc = DocumentSnapshot::new("#foo:bbugyi200/");
    let context =
        classify_completion_context_with_workflows(&doc, pos(15), &[], &names);
    assert_ne!(
        context.map(|context| context.kind),
        Some(CompletionContextKind::VcsRepo)
    );
}
#[test]
fn vcs_repo_trigger_negatives() {
    for prompt in [
        "#gh:bbugyi200",
        "#gh:/sa",
        "#gh:~/sa",
        "#gh:./sa",
        "#gh:https://github.com/bbugyi200/sase",
        "#gh_bbugyi200/sase",
        "word#gh:bbugyi200/sa",
        "#foo:bbugyi200/sa",
        "#gh(bbugyi200/sa)",
    ] {
        let doc = DocumentSnapshot::new(prompt);
        let context = detect_vcs_repo_context_at_position(
            &doc,
            doc.byte_offset_to_position(prompt.len()).unwrap(),
            &workflow_names(&["gh"]),
        );
        assert!(context.is_none(), "{prompt}");
    }
}
#[test]
fn vcs_repo_builder_replaces_only_the_ref_value() {
    let marked = "Fix #gh:bbugyi200/s<CURSOR>asex";
    let cursor = marked.find(VCS_REPO_CURSOR).unwrap();
    let text = marked.replace(VCS_REPO_CURSOR, "");
    let doc = DocumentSnapshot::new(text.clone());
    let context = vcs_repo_context(&text, cursor, &["gh"]);
    let list = build_vcs_repo_completion_candidates(
        &doc,
        &context,
        &[repo_entry("sase", "bbugyi200/sase")],
    );

    assert_eq!(list.candidates.len(), 1);
    let edit = list.candidates[0].replacement.as_ref().unwrap();
    assert_eq!(edit.new_text, "bbugyi200/sase ");
    assert_eq!(apply_text_edit(&text, edit), "Fix #gh:bbugyi200/sase ");
    assert!(list.candidates[0].additional_edits.is_empty());
}
// --- vcs_ref (`#gh:` / `#gh(` root-ref completion) ---------------------
const VCS_REF_CURSOR: &str = "<CURSOR>";
fn vcs_ref_context(
    text: &str,
    cursor: usize,
    names: &[&str],
) -> CompletionContext {
    let doc = DocumentSnapshot::new(text);
    let position = doc.byte_offset_to_position(cursor).unwrap();
    detect_vcs_ref_context_at_position(&doc, position, &workflow_names(names))
        .unwrap_or_else(|| panic!("expected ref context for {text:?}"))
}
fn namespace_entry(name: &str, description: &str) -> VcsNamespaceEntry {
    VcsNamespaceEntry {
        name: name.to_string(),
        description: description.to_string(),
        kind_label: "org".to_string(),
    }
}
#[test]
fn vcs_ref_golden_vectors() {
    // The cross-language parity contract -- identical to the Python
    // `VCS_REF_GOLDEN_VECTORS` table. `<CURSOR>` marks the cursor.
    let cases: &[(&str, &[&str], &str, bool, &str)] = &[
        ("#gh:<CURSOR>", &["gh"], "sase", false, "#gh:sase "),
        ("#gh:sa<CURSOR>", &["gh"], "sase", false, "#gh:sase "),
        (
            "Fix #gh:sa<CURSOR> now",
            &["gh"],
            "sase",
            false,
            "Fix #gh:sase now",
        ),
        ("#gh!!:sa<CURSOR>", &["gh"], "sase", false, "#gh!!:sase "),
        ("#gh:s<CURSOR>asex", &["gh"], "sase", false, "#gh:sase "),
        (
            "#git:sa<CURSOR>suffix",
            &["git"],
            "sase",
            false,
            "#git:sase ",
        ),
        ("#gh(s<CURSOR>", &["gh"], "sase", false, "#gh(sase)"),
        (
            "#gh(s<CURSOR>) next",
            &["gh"],
            "sase",
            false,
            "#gh(sase) next",
        ),
        ("#gh??(s<CURSOR>", &["gh"], "sase", false, "#gh??(sase)"),
        ("#gh:<CURSOR>", &["gh"], "sase-org", true, "#gh:sase-org/"),
        ("#gh:<CURSOR>", &["gh"], "sase-org/", true, "#gh:sase-org/"),
        (
            "Fix #gh:sa<CURSOR> now",
            &["gh"],
            "sase-org",
            true,
            "Fix #gh:sase-org/ now",
        ),
        ("#gh(sa<CURSOR>", &["gh"], "sase-org", true, "#gh(sase-org/"),
        (
            "#gh(sa<CURSOR>) next",
            &["gh"],
            "sase-org",
            true,
            "#gh(sase-org/) next",
        ),
    ];

    for (marked, names, selected_ref, chain, expected) in cases {
        let cursor = marked.find(VCS_REF_CURSOR).unwrap();
        let text = marked.replace(VCS_REF_CURSOR, "");
        let context = vcs_ref_context(&text, cursor, names);
        let trigger = context.vcs_ref.as_ref().unwrap();
        assert_eq!(
            apply_vcs_ref_selection(&text, trigger, selected_ref, *chain),
            *expected,
            "{marked}"
        );
    }
}
#[test]
fn vcs_ref_accept_preserves_visible_space_before_document_final_newline() {
    // Neovim documents include a final newline; the editor path treats that
    // as end-of-input so the accepted visible line still gains a space.
    let marked = "#gh:sa<CURSOR>\n";
    let cursor = marked.find(VCS_REF_CURSOR).unwrap();
    let text = marked.replace(VCS_REF_CURSOR, "");
    let context = vcs_ref_context(&text, cursor, &["gh"]);
    let trigger = context.vcs_ref.as_ref().unwrap();

    assert_eq!(
        apply_vcs_ref_selection(&text, trigger, "sase", false),
        "#gh:sase \n",
    );
}
#[test]
fn detects_vcs_ref_colon_spans() {
    let context = vcs_ref_context("#gh:sa", 6, &["gh"]);
    let trigger = context.vcs_ref.as_ref().unwrap();

    assert_eq!(context.kind, CompletionContextKind::VcsRef);
    assert_eq!(trigger.workflow, "gh");
    assert_eq!(trigger.separator, ":");
    assert_eq!(trigger.query, "sa");
    assert_eq!((trigger.ref_start, trigger.ref_end), (4, 6));
    assert_eq!(trigger.query_span, (4, 6));
    assert_eq!(context.replacement_range.start, pos(4));
    assert_eq!(context.replacement_range.end, pos(6));

    let context = vcs_ref_context("#gh:", 4, &["gh"]);
    let trigger = context.vcs_ref.as_ref().unwrap();
    assert_eq!(trigger.query, "");
    assert_eq!((trigger.ref_start, trigger.ref_end), (4, 4));
}
#[test]
fn detects_vcs_ref_paren_hitl() {
    let context = vcs_ref_context("#gh??(sa", 8, &["gh"]);
    let trigger = context.vcs_ref.as_ref().unwrap();
    assert_eq!(trigger.workflow, "gh");
    assert_eq!(trigger.separator, "(");
    assert_eq!(trigger.query, "sa");
    assert_eq!((trigger.ref_start, trigger.ref_end), (6, 8));
}
#[test]
fn classifies_vcs_repo_then_vcs_ref_then_xprompt_args() {
    let catalog = vec![gh_entry()];
    let names = workflow_names(&["gh"]);

    let doc = DocumentSnapshot::new("#gh:owner/repo");
    let context = classify_completion_context_with_workflows(
        &doc,
        pos(14),
        &catalog,
        &names,
    )
    .unwrap();
    assert_eq!(context.kind, CompletionContextKind::VcsRepo);

    let doc = DocumentSnapshot::new("#gh:");
    let context = classify_completion_context_with_workflows(
        &doc,
        pos(4),
        &catalog,
        &names,
    )
    .unwrap();
    assert_eq!(context.kind, CompletionContextKind::VcsRef);

    let doc = DocumentSnapshot::new("#gh:sa");
    let context = classify_completion_context_with_workflows(
        &doc,
        pos(6),
        &catalog,
        &names,
    )
    .unwrap();
    assert_eq!(context.kind, CompletionContextKind::VcsRef);

    let doc = DocumentSnapshot::new("#gh:~/x");
    let context = classify_completion_context_with_workflows(
        &doc,
        pos(7),
        &catalog,
        &names,
    )
    .unwrap();
    assert_eq!(context.kind, CompletionContextKind::XpromptArgumentTypeHint);
}
#[test]
fn vcs_ref_trigger_negatives() {
    for prompt in [
        "#gh:/sa",
        "#gh:~/x",
        "#gh:./x",
        "#gh:https://github.com/bbugyi200/sase",
        "#gh:owner/repo",
        "#gh_bbugyi200",
        "word#gh:sa",
        "#foo:sa",
        "#gh(sa)",
        "#gh:123)",
    ] {
        let doc = DocumentSnapshot::new(prompt);
        let context = detect_vcs_ref_context_at_position(
            &doc,
            doc.byte_offset_to_position(prompt.len()).unwrap(),
            &workflow_names(&["gh"]),
        );
        assert!(context.is_none(), "{prompt}");
    }
}
#[test]
fn vcs_ref_builder_groups_rows_and_replaces_root_ref() {
    let text = "#gh:";
    let doc = DocumentSnapshot::new(text);
    let context = vcs_ref_context(text, text.len(), &["gh"]);
    let list = build_vcs_ref_completion_candidates(
        &doc,
        &context,
        &[
            patch_entry("ship-completion", "sase", "Ready"),
            project_entry("sase", "gh"),
            project_entry("bob", "git"),
        ],
        &[namespace_entry("sase-org", "2 enabled projects")],
    );

    assert_eq!(
        list.candidates
            .iter()
            .map(|candidate| candidate.name.as_str())
            .collect::<Vec<_>>(),
        vec!["sase", "ship-completion", "sase-org"]
    );
    assert_eq!(list.candidates[0].kind, "project");
    assert_eq!(list.candidates[1].kind, "patch");
    assert_eq!(list.candidates[1].project, "sase");
    assert_eq!(list.candidates[1].status, "Ready");
    assert_eq!(list.candidates[2].display, "sase-org/");
    assert_eq!(list.candidates[2].kind, "namespace");
    assert_eq!(list.candidates[2].status, "org");

    let project_edit = list.candidates[0].replacement.as_ref().unwrap();
    assert_eq!(apply_text_edit(text, project_edit), "#gh:sase ");
    let namespace_edit = list.candidates[2].replacement.as_ref().unwrap();
    assert_eq!(apply_text_edit(text, namespace_edit), "#gh:sase-org/");
}
#[test]
fn vcs_ref_builder_filters_by_query_and_alias() {
    let text = "#gh:sea";
    let doc = DocumentSnapshot::new(text);
    let context = vcs_ref_context(text, text.len(), &["gh"]);
    let mut entry = project_entry("sase", "gh");
    entry.aliases = vec!["seaside".to_string()];
    let list = build_vcs_ref_completion_candidates(
        &doc,
        &context,
        &[entry, project_entry("bob", "gh")],
        &[namespace_entry("sase-org", "")],
    );

    assert_eq!(
        list.candidates
            .iter()
            .map(|candidate| candidate.name.as_str())
            .collect::<Vec<_>>(),
        vec!["sase"]
    );
}
// --- vcs_project (`+`) completion --------------------------------------
fn vcs_names() -> Vec<String> {
    ["gh", "git", "hg"].iter().map(|s| s.to_string()).collect()
}
fn project_entry(name: &str, prefix: &str) -> VcsProjectEntry {
    VcsProjectEntry {
        name: name.to_string(),
        vcs_prefix: prefix.to_string(),
        display_tag: format!("#{prefix}:{name}"),
        provider_display: "GitHub".to_string(),
        description: String::new(),
        aliases: Vec::new(),
        entry_kind: "project".to_string(),
        kind: "project".to_string(),
        project: name.to_string(),
        status: String::new(),
    }
}
fn patch_entry(name: &str, project: &str, status: &str) -> VcsProjectEntry {
    VcsProjectEntry {
        name: name.to_string(),
        vcs_prefix: "gh".to_string(),
        display_tag: format!("#gh:{name}"),
        provider_display: "GitHub".to_string(),
        description: String::new(),
        aliases: Vec::new(),
        entry_kind: "patch".to_string(),
        // Legacy backing kind remains in fixtures for compatibility.
        kind: "changespec".to_string(),
        project: project.to_string(),
        status: status.to_string(),
    }
}
fn legacy_patch_entry(
    name: &str,
    project: &str,
    status: &str,
) -> VcsProjectEntry {
    VcsProjectEntry {
        name: name.to_string(),
        vcs_prefix: "gh".to_string(),
        display_tag: format!("#gh:{name}"),
        provider_display: "GitHub".to_string(),
        description: String::new(),
        aliases: Vec::new(),
        entry_kind: String::new(),
        // Legacy backing kind remains accepted for compatibility.
        kind: "changespec".to_string(),
        project: project.to_string(),
        status: status.to_string(),
    }
}
fn canonical_only_patch_entry(
    name: &str,
    project: &str,
    status: &str,
) -> VcsProjectEntry {
    VcsProjectEntry {
        name: name.to_string(),
        vcs_prefix: "gh".to_string(),
        display_tag: format!("#gh:{name}"),
        provider_display: "GitHub".to_string(),
        description: String::new(),
        aliases: Vec::new(),
        entry_kind: "patch".to_string(),
        kind: String::new(),
        project: project.to_string(),
        status: status.to_string(),
    }
}
fn apply_test_edits(text: &str, edits: &VcsProjectByteEdits) -> String {
    let mut all: Vec<&VcsByteEdit> = std::iter::once(&edits.primary)
        .chain(edits.additional.iter())
        .collect();
    all.sort_by_key(|edit| (edit.start, edit.end));
    let mut out = String::new();
    let mut pos = 0;
    for edit in all {
        out.push_str(&text[pos..edit.start]);
        out.push_str(&edit.new_text);
        pos = edit.end;
    }
    out.push_str(&text[pos..]);
    out
}
/// Detect the trigger in `marked` (where `‸` is the cursor), then expand
/// the `#gh:sase` selection both via the canonical transform and via the
/// applied byte edits. Returns `(canonical, via_edits)`.
fn expand_via_both(marked: &str) -> (String, String) {
    let cursor_byte = marked.find('‸').expect("cursor marker");
    let text = marked.replacen('‸', "", 1);
    let doc = DocumentSnapshot::new(text.clone());
    let position = doc
        .byte_offset_to_position(cursor_byte)
        .expect("cursor on a char boundary");
    let token =
        vcs_project_trigger_token(&doc, position).expect("a trigger token");
    let re = vcs_replace_regex(&vcs_names());

    let canonical = apply_vcs_project_selection(
        &text,
        token.byte_start,
        token.byte_end,
        "#gh:sase",
        &re,
    );
    let edits = vcs_project_byte_edits(
        &text,
        token.byte_start,
        token.byte_end,
        "#gh:sase",
        &re,
    );
    (canonical, apply_test_edits(&text, &edits))
}
#[test]
fn vcs_project_golden_vectors() {
    // The cross-language parity contract -- identical to the Python
    // `_GOLDEN_VECTORS` table. `‸` marks the cursor.
    let cases = [
        ("Describe this repo. +‸", "#gh:sase Describe this repo."),
        ("+‸", "#gh:sase "),
        ("+sa‸", "#gh:sase "),
        ("+s‸\n", "#gh:sase \n"),
        ("+s‸\nmore text", "#gh:sase \nmore text"),
        ("#git:foo Fix bug +‸", "#gh:sase Fix bug"),
        ("#gh!!:foo do X +‸", "#gh:sase do X"),
        // Existing leading VCS tag at end-of-input (no trailing text): the
        // trigger strip leaves the bare tag at EOF, which must still be
        // replaced -- not doubled.
        ("#gh:sase +‸", "#gh:sase "),
        ("#gh:sase +foo‸", "#gh:sase "),
        ("#git:foo +‸", "#gh:sase "),
        ("Fix +bug‸ here", "#gh:sase Fix here"),
        ("Line one\n +‸", "#gh:sase Line one\n"),
        (
            "---\nname: x\n---\nBody +‸",
            "---\nname: x\n---\n#gh:sase Body",
        ),
        ("%model:opus Body +‸", "%model:opus #gh:sase Body"),
        ("+sa‸ Fix", "#gh:sase Fix"),
        // The cursor-local query is `sa`, while selection consumes the
        // entire `+sase` token.
        ("Fix +sa‸se now", "#gh:sase Fix now"),
    ];
    for (marked, expected) in cases {
        let (canonical, via_edits) = expand_via_both(marked);
        assert_eq!(canonical, expected, "canonical: {marked:?}");
        assert_eq!(via_edits, expected, "via edits: {marked:?}");
    }
}
#[test]
fn vcs_prepend_offset_skips_horizontal_whitespace_only() {
    assert_eq!(vcs_prepend_offset("\n"), 0);
    assert_eq!(vcs_prepend_offset("\nmore"), 0);
    assert_eq!(vcs_prepend_offset("  Body"), 2);
    assert_eq!(vcs_prepend_offset("\tBody"), 1);
}
#[test]
fn classifies_vcs_project_trigger() {
    for (text, col) in [
        ("+", 1),
        ("+sa", 3),
        ("Fix +", 5),
        ("Fix +sa", 7),
        ("2 + 2", 3),
    ] {
        let doc = DocumentSnapshot::new(text);
        let context = classify_completion_context(&doc, pos(col), &[]).unwrap();
        assert_eq!(context.kind, CompletionContextKind::VcsProject, "{text}");
    }

    for (text, col) in [
        ("#+", 2),
        ("Fix #+", 6),
        ("line\n+", 1),
        ("\t+", 2),
        ("word+", 5),
        ("a+b", 3),
        ("c++", 3),
        ("c#+x", 4),
    ] {
        let doc = DocumentSnapshot::new(text);
        let position = if text == "line\n+" {
            EditorPosition {
                line: 1,
                character: col,
            }
        } else {
            pos(col)
        };
        let context = classify_completion_context(&doc, position, &[]);
        assert_ne!(
            context.map(|context| context.kind),
            Some(CompletionContextKind::VcsProject),
            "{text}"
        );
    }

    let doc = DocumentSnapshot::new("line\n +");
    let context = classify_completion_context(
        &doc,
        EditorPosition {
            line: 1,
            character: 2,
        },
        &[],
    )
    .unwrap();
    assert_eq!(context.kind, CompletionContextKind::VcsProject);
}
#[test]
fn bof_trigger_merges_into_single_primary_edit() {
    let doc = DocumentSnapshot::new("+");
    let context = classify_completion_context(&doc, pos(1), &[]).unwrap();
    let token = context.token.as_ref().unwrap();
    let list = build_vcs_project_completion_candidates(
        token,
        &doc,
        pos(1),
        &[project_entry("sase", "gh")],
        &vcs_names(),
    );

    assert_eq!(list.candidates.len(), 1);
    let candidate = &list.candidates[0];
    assert_eq!(candidate.name, "sase");
    assert_eq!(candidate.insertion, "#gh:sase");
    // BOF `+`: the prepend point coincides with the trigger deletion, so
    // the edits merge into one primary edit with no additional edits.
    assert!(candidate.additional_edits.is_empty());
    assert_eq!(
        candidate.replacement.as_ref().unwrap().new_text,
        "#gh:sase "
    );
}
#[test]
fn trailing_trigger_emits_primary_plus_additional_edit() {
    let doc = DocumentSnapshot::new("Describe this repo. +");
    let cursor = pos(21);
    let context = classify_completion_context(&doc, cursor, &[]).unwrap();
    let token = context.token.as_ref().unwrap();
    let list = build_vcs_project_completion_candidates(
        token,
        &doc,
        cursor,
        &[project_entry("sase", "gh")],
        &vcs_names(),
    );

    let candidate = &list.candidates[0];
    // The primary edit consumes the trigger token; the additional edit
    // prepends the tag at the start of the document.
    assert_eq!(candidate.replacement.as_ref().unwrap().new_text, "");
    assert_eq!(candidate.additional_edits.len(), 1);
    assert_eq!(candidate.additional_edits[0].new_text, "#gh:sase ");
    let prepend_range = candidate.additional_edits[0].range;
    assert_eq!(prepend_range.start, prepend_range.end);
}
#[test]
fn vcs_project_candidates_filter_preserves_catalog_order() {
    let doc = DocumentSnapshot::new("Fix +sa");
    let cursor = pos(7);
    let context = classify_completion_context(&doc, cursor, &[]).unwrap();
    let token = context.token.as_ref().unwrap();
    let list = build_vcs_project_completion_candidates(
        token,
        &doc,
        cursor,
        &[
            project_entry("sase", "gh"),
            project_entry("saseling", "gh"),
            project_entry("bob", "git"),
        ],
        &vcs_names(),
    );

    assert_eq!(
        list.candidates
            .iter()
            .map(|candidate| candidate.name.as_str())
            .collect::<Vec<_>>(),
        vec!["sase", "saseling"]
    );
}
#[test]
fn vcs_project_candidates_include_patch_context() {
    let doc = DocumentSnapshot::new("Review +ship");
    let cursor = pos(12);
    let context = classify_completion_context(&doc, cursor, &[]).unwrap();
    let token = context.token.as_ref().unwrap();
    let list = build_vcs_project_completion_candidates(
        token,
        &doc,
        cursor,
        &[
            project_entry("sase", "gh"),
            patch_entry("ship-completion", "sase", "Ready"),
        ],
        &vcs_names(),
    );

    assert_eq!(list.candidates.len(), 1);
    let candidate = &list.candidates[0];
    assert_eq!(candidate.name, "ship-completion");
    assert_eq!(candidate.insertion, "#gh:ship-completion");
    assert_eq!(candidate.kind, "patch");
    assert_eq!(candidate.project, "sase");
    assert_eq!(candidate.status, "Ready");
}
#[test]
fn vcs_project_candidates_accept_legacy_changespec_kind() {
    let doc = DocumentSnapshot::new("Review +ship");
    let cursor = pos(12);
    let context = classify_completion_context(&doc, cursor, &[]).unwrap();
    let token = context.token.as_ref().unwrap();
    let list = build_vcs_project_completion_candidates(
        token,
        &doc,
        cursor,
        &[legacy_patch_entry("ship-completion", "sase", "Ready")],
        &vcs_names(),
    );

    assert_eq!(list.candidates.len(), 1);
    assert_eq!(list.candidates[0].kind, "patch");
}
#[test]
fn vcs_project_candidates_accept_entry_kind_without_legacy_kind() {
    let doc = DocumentSnapshot::new("Review +ship");
    let cursor = pos(12);
    let context = classify_completion_context(&doc, cursor, &[]).unwrap();
    let token = context.token.as_ref().unwrap();
    let list = build_vcs_project_completion_candidates(
        token,
        &doc,
        cursor,
        &[canonical_only_patch_entry(
            "ship-completion",
            "sase",
            "Ready",
        )],
        &vcs_names(),
    );

    assert_eq!(list.candidates.len(), 1);
    assert_eq!(list.candidates[0].kind, "patch");
    assert_eq!(list.candidates[0].name, "ship-completion");
}
#[test]
fn vcs_project_candidates_filter_for_bare_plus_query() {
    // The query for a BOF `+sa` token is `sa` (prefix length 1), so the
    // candidate list filters in catalog order.
    let doc = DocumentSnapshot::new("+sa");
    let cursor = pos(3);
    let context = classify_completion_context(&doc, cursor, &[]).unwrap();
    assert_eq!(context.kind, CompletionContextKind::VcsProject);
    let token = context.token.as_ref().unwrap();
    let list = build_vcs_project_completion_candidates(
        token,
        &doc,
        cursor,
        &[
            project_entry("sase", "gh"),
            project_entry("saseling", "gh"),
            project_entry("bob", "git"),
        ],
        &vcs_names(),
    );

    assert_eq!(
        list.candidates
            .iter()
            .map(|candidate| candidate.name.as_str())
            .collect::<Vec<_>>(),
        vec!["sase", "saseling"]
    );
}
#[test]
fn vcs_project_candidates_match_aliases() {
    let doc = DocumentSnapshot::new("Find +sea");
    let cursor = pos(9);
    let context = classify_completion_context(&doc, cursor, &[]).unwrap();
    let token = context.token.as_ref().unwrap();
    let mut entry = project_entry("sase", "gh");
    entry.aliases = vec!["seaside".to_string()];
    let list = build_vcs_project_completion_candidates(
        token,
        &doc,
        cursor,
        &[entry, project_entry("bob", "git")],
        &vcs_names(),
    );

    assert_eq!(
        list.candidates
            .iter()
            .map(|candidate| candidate.name.as_str())
            .collect::<Vec<_>>(),
        vec!["sase"]
    );
}
#[test]
fn vcs_project_edits_never_overlap() {
    // Every golden input must yield non-overlapping edits (LSP requires
    // it); `vcs_edits_conflict` is the guard.
    for marked in [
        "Describe this repo. +‸",
        "+‸",
        "+sa‸",
        "#git:foo Fix bug +‸",
        // Existing tag at EOF: the replace edit (tag span) and the primary
        // trigger-deletion edit are adjacent and must not overlap.
        "#git:foo +‸",
        "#gh:sase +‸",
        "%model:opus Body +‸",
        "+sa‸ Fix",
        "Fix +sa‸se now",
    ] {
        let cursor_byte = marked.find('‸').unwrap();
        let text = marked.replacen('‸', "", 1);
        let doc = DocumentSnapshot::new(text.clone());
        let position = doc.byte_offset_to_position(cursor_byte).unwrap();
        let token = vcs_project_trigger_token(&doc, position).unwrap();
        let re = vcs_replace_regex(&vcs_names());
        let edits = vcs_project_byte_edits(
            &text,
            token.byte_start,
            token.byte_end,
            "#gh:sase",
            &re,
        );
        assert!(
            !vcs_edits_conflict(&edits.primary, &edits.additional),
            "overlapping edits for {marked:?}"
        );
    }
}
