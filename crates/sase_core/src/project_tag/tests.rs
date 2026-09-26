//! Table-driven tests for the project tag lexer, resolver, expander,
//! trigger, and accept algorithm.

use super::resolve::resolve_project_tag;
use super::scan::scan_project_tags;
use super::{
    apply_project_tag_selection, expand_project_tags, project_tag_trigger,
    ProjectTagResolutionWire, ProjectTagTargetWire,
};

fn target(
    key: &str,
    name: &str,
    aliases: &[&str],
    workflow_type: Option<&str>,
) -> ProjectTagTargetWire {
    ProjectTagTargetWire {
        key: key.to_string(),
        name: name.to_string(),
        aliases: aliases.iter().map(|alias| (*alias).to_string()).collect(),
        workflow_type: workflow_type.map(str::to_string),
        state: None,
        workspace_dir: None,
    }
}

fn catalog() -> Vec<ProjectTagTargetWire> {
    vec![
        target("gh_sase-org__sase", "sase", &["sa"], Some("gh")),
        target("gh_bobs-org__bob-cli", "bob-cli", &[], Some("git")),
        target("git_notes", "notes", &["n"], Some("git")),
        target("home", "home", &[], None),
    ]
}

fn span_summary(text: &str) -> Vec<(String, String, bool)> {
    scan_project_tags(text)
        .iter()
        .map(|span| {
            (
                text[span.start..span.end].to_string(),
                span.name.clone(),
                span.anchored,
            )
        })
        .collect()
}

#[test]
fn scan_accepts_d1_shapes() {
    for (text, expected) in [
        ("+sase", vec![("+sase", "sase", true)]),
        ("+a", vec![("+a", "a", true)]),
        ("+Sase", vec![("+Sase", "Sase", true)]),
        ("+bob-cli", vec![("+bob-cli", "bob-cli", true)]),
        ("+a.b_c-d", vec![("+a.b_c-d", "a.b_c-d", true)]),
        ("  +sase", vec![("+sase", "sase", true)]),
        ("say +sase now", vec![("+sase", "sase", false)]),
        ("say\n+sase", vec![("+sase", "sase", true)]),
        ("say\t+sase", vec![("+sase", "sase", false)]),
        (
            "%{+sase | +bob-cli}",
            vec![("+sase", "sase", false), ("+bob-cli", "bob-cli", false)],
        ),
        ("{+sase}", vec![("+sase", "sase", false)]),
        ("|+sase|", vec![("+sase", "sase", false)]),
        ("+sase}", vec![("+sase", "sase", true)]),
        ("+sase\n", vec![("+sase", "sase", true)]),
    ] {
        assert_eq!(
            span_summary(text),
            expected
                .into_iter()
                .map(|(full, name, anchored)| (
                    full.to_string(),
                    name.to_string(),
                    anchored
                ))
                .collect::<Vec<_>>(),
            "{text:?}"
        );
    }
}

#[test]
fn scan_rejects_d1_non_tags() {
    for text in [
        "chmod +x file",
        "C++",
        "a+b",
        "+1",
        "+ item",
        "+sase,",
        "#+sase",
        "#name+",
        "%dir+",
        "+sase.",
        "+sase-",
        "+.sase",
        "+-sase",
        "++sase",
        "a++sase",
        "(@sase)",
        "hello+sase",
    ] {
        // `+x` in `chmod +x file` scans lexically (single-letter name) but
        // never resolves; everything else must not scan at all.
        if text == "chmod +x file" {
            continue;
        }
        assert!(
            scan_project_tags(text).is_empty(),
            "{text:?} should not scan"
        );
    }
}

#[test]
fn scan_chmod_plus_x_is_lexical_but_unresolvable() {
    let spans = scan_project_tags("chmod +x file");
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].name, "x");
    assert!(matches!(
        resolve_project_tag("x", &catalog()),
        ProjectTagResolutionWire::Unknown { .. }
    ));
}

#[test]
fn scan_skips_literal_zones() {
    assert!(scan_project_tags("```\n+sase\n```").is_empty());
    assert!(scan_project_tags("`+sase` code").is_empty());
    assert!(scan_project_tags(
        "%xprompts_enabled:false\n+sase\n%xprompts_enabled:true\n"
    )
    .is_empty());
    assert!(scan_project_tags("---\ntitle: +sase\n---\nbody").is_empty());
    // The same tag outside the zones still scans.
    assert_eq!(scan_project_tags("```\n+sase\n```\n+sase").len(), 1);
    assert_eq!(scan_project_tags("---\ntitle: x\n---\n+sase").len(), 1);
}

#[test]
fn scan_handles_multibyte_text() {
    let text = "é +sase 值";
    let spans = scan_project_tags(text);
    assert_eq!(spans.len(), 1);
    assert_eq!(&text[spans[0].start..spans[0].end], "+sase");
    assert_eq!(spans[0].name, "sase");
    // A tag glued to multibyte prose is not at a boundary.
    assert!(scan_project_tags("é+sase").is_empty());
}

#[test]
fn scan_detects_anchored_tags() {
    // (text, expected anchored flags in scan order)
    for (text, expected) in [
        ("+sase", vec![true]),
        ("   +sase", vec![true]),
        ("%m:opus +sase", vec![true]),
        ("%i:a +sase do things", vec![true]),
        ("%auto %m:opus +sase", vec![true]),
        ("line one\n+sase", vec![true]),
        ("line one\n  %m:opus  +sase", vec![true]),
        ("---\ntitle: x\n---\n+sase", vec![true]),
        ("do +sase now", vec![false]),
        ("x +a +b", vec![false, false]),
        ("%{+sase | +bob-cli}", vec![false, false]),
        ("%model:`literal value` +sase", vec![true]),
        ("%queue(capacity=5) +sase", vec![true]),
    ] {
        let anchored: Vec<bool> = scan_project_tags(text)
            .iter()
            .map(|span| span.anchored)
            .collect();
        assert_eq!(anchored, expected, "{text:?}");
    }
}

#[test]
fn resolve_matches_exact_then_casefold() {
    let targets = catalog();
    assert_eq!(
        resolve_project_tag("sase", &targets),
        ProjectTagResolutionWire::Resolved { target_index: 0 }
    );
    // Phone-keyboard capitalization works.
    assert_eq!(
        resolve_project_tag("Sase", &targets),
        ProjectTagResolutionWire::Resolved { target_index: 0 }
    );
    assert_eq!(
        resolve_project_tag("BOB-CLI", &targets),
        ProjectTagResolutionWire::Resolved { target_index: 1 }
    );
    // Keys and aliases resolve.
    assert_eq!(
        resolve_project_tag("gh_sase-org__sase", &targets),
        ProjectTagResolutionWire::Resolved { target_index: 0 }
    );
    assert_eq!(
        resolve_project_tag("sa", &targets),
        ProjectTagResolutionWire::Resolved { target_index: 0 }
    );
    // Exact beats casefold across targets.
    let mut shadowed = vec![
        target("k1", "Sase", &[], Some("gh")),
        target("k2", "sase", &[], Some("gh")),
    ];
    assert_eq!(
        resolve_project_tag("sase", &shadowed),
        ProjectTagResolutionWire::Resolved { target_index: 1 }
    );
    shadowed.swap(0, 1);
    assert_eq!(
        resolve_project_tag("Sase", &shadowed),
        ProjectTagResolutionWire::Resolved { target_index: 1 }
    );
}

#[test]
fn resolve_reports_ambiguous_targets() {
    // Two exact claimants.
    let targets = vec![
        target("k1", "sase", &[], Some("gh")),
        target("k2", "sase", &[], Some("git")),
    ];
    assert_eq!(
        resolve_project_tag("sase", &targets),
        ProjectTagResolutionWire::Ambiguous {
            candidates: vec![0, 1]
        }
    );
    // Two casefold claimants and no exact match.
    let targets = vec![
        target("k1", "Sase", &[], Some("gh")),
        target("k2", "SASE", &[], Some("git")),
    ];
    assert_eq!(
        resolve_project_tag("sase", &targets),
        ProjectTagResolutionWire::Ambiguous {
            candidates: vec![0, 1]
        }
    );
}

#[test]
fn resolve_suggests_known_tags() {
    let targets = catalog();
    let ProjectTagResolutionWire::Unknown { suggestions } =
        resolve_project_tag("ssae", &targets)
    else {
        panic!("expected unknown");
    };
    assert_eq!(suggestions[0], "+sase");
    assert_eq!(suggestions.len(), 3);
    // Empty catalog: no suggestions, still unknown.
    let ProjectTagResolutionWire::Unknown { suggestions } =
        resolve_project_tag("ssae", &[])
    else {
        panic!("expected unknown");
    };
    assert!(suggestions.is_empty());
}

#[test]
fn resolve_dedupes_suggestions_fully() {
    let targets = vec![
        target("home", "home", &[], Some("git")),
        target("zome", "zome", &[], Some("git")),
    ];
    let ProjectTagResolutionWire::Unknown { suggestions } =
        resolve_project_tag("xome", &targets)
    else {
        panic!("expected unknown");
    };
    assert_eq!(suggestions, vec!["+home".to_string(), "+zome".to_string()]);
}

#[test]
fn expand_rewrites_resolved_tags_in_place() {
    let expanded = expand_project_tags("+sase fix the bug", &catalog());
    assert_eq!(expanded.text, "#gh:gh_sase-org__sase fix the bug");
    assert_eq!(expanded.tags.len(), 1);
    assert_eq!(
        expanded.tags[0].replacement.as_deref(),
        Some("#gh:gh_sase-org__sase")
    );
    assert!(expanded.tags[0].anchored);
}

#[test]
fn expand_leaves_unresolvable_tags_but_reports_them() {
    let expanded = expand_project_tags("+sase and +ssae and +home", &catalog());
    assert_eq!(expanded.text, "#gh:gh_sase-org__sase and +ssae and +home");
    assert_eq!(expanded.tags.len(), 3);
    // `home` resolves but has no workflow type, so it never rewrites.
    assert_eq!(expanded.tags[2].replacement, None);
    assert!(matches!(
        expanded.tags[2].resolution,
        ProjectTagResolutionWire::Resolved { .. }
    ));
    assert!(matches!(
        expanded.tags[1].resolution,
        ProjectTagResolutionWire::Unknown { .. }
    ));
}

#[test]
fn expand_never_errors_on_plain_text() {
    for text in ["", "no tags here", "C++ and a+b", "+1", "```\n+sase\n```"] {
        let expanded = expand_project_tags(text, &catalog());
        assert_eq!(expanded.text, text, "{text:?}");
    }
}

#[test]
fn trigger_fires_at_d1_boundaries() {
    // (text, cursor, expected (start, end, query))
    for (text, cursor, expected) in [
        ("+", 1, (0, 1, "")),
        ("+sa", 3, (0, 3, "sa")),
        ("+abc", 2, (0, 4, "a")),
        ("Fix +bug", 8, (4, 8, "bug")),
        ("Fix +bug", 6, (4, 8, "b")),
        ("line\n+", 6, (5, 6, "")),
        ("line\n +x", 8, (6, 8, "x")),
        ("\t+", 2, (1, 2, "")),
        ("%{+sa", 5, (2, 5, "sa")),
        ("%{a | +sa", 9, (6, 9, "sa")),
        ("{+}", 2, (1, 2, "")),
        ("2 + 2", 3, (2, 3, "")),
    ] {
        let (start, end, query) = expected;
        let trigger = project_tag_trigger(text, cursor)
            .unwrap_or_else(|| panic!("expected trigger for {text:?}"));
        assert_eq!(trigger.start, start, "{text:?} start");
        assert_eq!(trigger.end, end, "{text:?} end");
        assert_eq!(trigger.query, query, "{text:?} query");
    }
}

#[test]
fn trigger_rejects_non_boundaries() {
    for (text, cursor) in [
        ("", 0),
        ("+", 0),
        ("c+", 2),
        ("word+", 5),
        ("a+b", 3),
        ("c++", 3),
        ("#+", 2),
        ("#+sa", 4),
        ("Fix #+sa", 8),
        ("c#+x", 4),
        ("hello", 5),
        ("é+sase", 7),
        ("#+", 1),
    ] {
        assert!(
            project_tag_trigger(text, cursor).is_none(),
            "{text:?} should not trigger"
        );
    }
}

fn apply(marked: &str, insertion: &str) -> String {
    // The `‸` marker is zero-width: its byte index in the marked text is the
    // cursor in the cleaned text.
    let cursor = cursor_of(marked);
    let text = marked.replace('‸', "");
    let trigger = project_tag_trigger(&text, cursor)
        .unwrap_or_else(|| panic!("expected trigger for {marked:?}"));
    apply_project_tag_selection(
        &text,
        (trigger.start, trigger.end),
        insertion,
        &["gh".to_string(), "git".to_string()],
        &catalog(),
    )
    .text
}

fn cursor_of(marked: &str) -> usize {
    marked.find('‸').unwrap()
}

fn apply_full(marked: &str, insertion: &str) -> (String, usize) {
    let cursor = cursor_of(marked);
    let text = marked.replace('‸', "");
    let trigger = project_tag_trigger(&text, cursor)
        .unwrap_or_else(|| panic!("expected trigger for {marked:?}"));
    let applied = apply_project_tag_selection(
        &text,
        (trigger.start, trigger.end),
        insertion,
        &["gh".to_string(), "git".to_string()],
        &catalog(),
    );
    (applied.text, applied.cursor)
}

#[test]
fn accept_puts_selection_at_earliest_target_or_leading_position() {
    // Restored placement: the selected row lands at the earliest existing
    // workspace target in the trigger's `---` segment (other targets in
    // that segment go away), or at the segment's leading project-tag
    // position when no target exists.
    for (marked, expected) in [
        // No existing target: leading insertion.
        ("Describe this repo. +‸", "+sase Describe this repo. "),
        ("+‸", "+sase "),
        ("+sa‸", "+sase "),
        ("+s‸\n", "\n+sase "),
        ("+s‸\nmore text", "\n+sase more text"),
        // Existing `#` ref before the trigger: replace it at its position.
        ("#git:foo Fix bug +‸", "+sase Fix bug "),
        ("#gh!!:foo do X +‸", "+sase do X "),
        ("#gh:sase +‸", "+sase "),
        ("#gh:sase +foo‸", "+sase "),
        ("#git:foo +‸", "+sase "),
        // No target (typed query only): leading insertion.
        ("Fix +bug‸ here", "+sase Fix here"),
        ("Line one\n +‸", "+sase Line one\n "),
        // Later `---` segments keep their own leading position; the
        // frontmatter block is never touched.
        (
            "---\nname: x\n---\nBody +‸",
            "---\nname: x\n---\n+sase Body ",
        ),
        // Leading `%directive` tokens stay before the inserted tag.
        ("%model:opus Body +‸", "%model:opus +sase Body "),
        // The trigger already occupies the leading destination: merged.
        ("+sa‸ Fix", "+sase Fix"),
        // The trigger's own token is removed; with no other target the
        // selection lands at the leading position.
        ("Fix +sa‸se now", "+sase Fix now"),
    ] {
        assert_eq!(apply(marked, "+sase "), expected, "accept: {marked:?}");
    }
}

#[test]
fn accept_targets_existing_tag_before_and_after_trigger() {
    // An existing `+tag` before the trigger wins over the typed position.
    assert_eq!(
        apply("+notes do it +sa‸", "+sase "),
        "+sase do it ",
        "tag before trigger"
    );
    // An existing `+tag` after the trigger is replaced where it stands;
    // surrounding words stay in order.
    assert_eq!(
        apply("+sa‸ do +notes", "+sase "),
        "do +sase ",
        "tag after trigger"
    );
    // An existing `#` ref after the trigger likewise receives the row.
    assert_eq!(
        apply("+sa‸ do #git:foo", "+sase "),
        "do +sase ",
        "ref after trigger"
    );
}

#[test]
fn accept_removes_multiple_targets_leaving_one() {
    // The earliest target keeps the row; every further target in the same
    // segment (tags and refs alike) is deleted.
    assert_eq!(
        apply("#git:foo +notes +sa‸", "+sase "),
        "+sase ",
        "ref plus tag collapse to one"
    );
    assert_eq!(
        apply("+notes +home +sa‸", "+sase "),
        "+sase ",
        "several tags collapse to one"
    );
}

#[test]
fn accept_uses_leading_position_with_frontmatter_and_directives() {
    // Frontmatter delimiters split segments; the Body segment's leading
    // position follows them without touching the frontmatter itself.
    assert_eq!(
        apply("---\ntitle: x\n---\nBody +sa‸", "+sase "),
        "---\ntitle: x\n---\n+sase Body ",
        "frontmatter leading"
    );
    // Stacked directives stay before the inserted tag.
    assert_eq!(
        apply("%auto %m:opus Body +sa‸", "+sase "),
        "%auto %m:opus +sase Body ",
        "directive leading"
    );
    // A later `---` segment inserts at its own leading position.
    assert_eq!(
        apply("First\n---\nSecond +sa‸", "+sase "),
        "First\n---\n+sase Second ",
        "later segment leading"
    );
}

#[test]
fn accept_keeps_pr_spelling_at_target_or_leading() {
    // PR rows keep their `#` spelling but follow the same placement: at
    // the existing target, or at the leading position when none exists.
    assert_eq!(
        apply("#git:foo Review +sh‸", "#gh:ship "),
        "#gh:ship Review ",
        "PR row at ref target"
    );
    assert_eq!(
        apply("Review +sh‸", "#gh:ship "),
        "#gh:ship Review ",
        "PR row at leading position"
    );
}

#[test]
fn accept_preserves_whitespace_newlines_and_unicode() {
    // A following space on the destination is consumed (the insertion
    // carries its own separator); newlines are never consumed.
    assert_eq!(
        apply("Fix +notes now +sa‸", "+sase "),
        "Fix +sase now ",
        "destination space collapsed"
    );
    // Blank lines around other-target deletions survive.
    assert_eq!(
        apply("a #gh:foo\n\nb +sa‸", "+sase "),
        "a +sase \n\nb ",
        "blank lines survive"
    );
    // Unicode text before the trigger shifts byte offsets but not behavior.
    let (text, cursor) = apply_full("é +notes +sa‸", "+sase ");
    assert_eq!(text, "é +sase ", "unicode target replacement");
    assert_eq!(&text[..cursor], "é +sase ", "unicode caret");
}

#[test]
fn accept_reports_caret_just_after_insertion() {
    // Leading insertion: the caret sits after the inserted row at the
    // front, not at the removed trigger.
    let (text, cursor) = apply_full("Body +sa‸", "+sase ");
    assert_eq!(text, "+sase Body ");
    assert_eq!(&text[..cursor], "+sase ");
    // Target replacement: the caret sits after the replaced target.
    let (text, cursor) = apply_full("#git:foo Body +sa‸", "+sase ");
    assert_eq!(text, "+sase Body ");
    assert_eq!(&text[..cursor], "+sase ");
}

#[test]
fn accept_removes_mid_line_refs_like_the_python_guard() {
    // The Python one-target guard counts mid-line refs
    // (`find_vcs_workflow_tag_span("fix in #gh:foo now")`), so accept
    // replaces the earliest one and leaves exactly one workspace target.
    assert_eq!(
        apply("fix in #gh:foo now +sa‸", "+sase "),
        "fix in +sase now ",
        "mid-line ref is replaced"
    );
    assert_eq!(
        apply("fix in #gh:foo now +sase do it +bo‸", "+bob-cli "),
        "fix in +bob-cli now do it ",
        "mid-line ref plus tag both go away"
    );
    // A `#` that is not at a token boundary is not a ref: with no target
    // the row goes to the leading position and the glued text stays.
    assert_eq!(
        apply("a#gh:foo +sa‸", "+sase "),
        "+sase a#gh:foo ",
        "glued hash stays"
    );
}

#[test]
fn accept_keeps_line_breaks_around_end_of_line_refs() {
    // The destination replacement never consumes its newline, so neighbors
    // never join and blank lines survive. Other-target deletions still
    // collapse a lone line instead of leaving a blank line behind.
    for (marked, expected) in [
        (
            "fix in #gh:foo\nmore stuff +sa‸",
            "fix in +sase \nmore stuff ",
        ),
        ("a #gh:foo\n\nb +sa‸", "a +sase \n\nb "),
        ("line one #gh:foo\n+sa‸", "line one +sase \n"),
        ("body\n#gh:foo\nmore +sa‸", "body\n+sase \nmore "),
    ] {
        assert_eq!(apply(marked, "+sase "), expected, "accept: {marked:?}");
    }
}

#[test]
fn accept_counts_refs_inside_rejected_glued_matches() {
    // The `regex` crate has no lookbehind, so the left boundary is checked
    // after matching. A rejected glued match must not swallow the valid
    // ref inside it: the Python guard counts `#gh:foo` here too.
    assert_eq!(
        apply("x#gh(a #gh:foo) now +sa‸", "+sase "),
        "x#gh(a +sase now ",
        "inner ref of a glued match is replaced"
    );
}

#[test]
fn accept_with_empty_workflow_names_matches_nothing() {
    // An empty alternation must never match `# Heading` and delete it: with
    // no VCS targets the row goes to the leading position.
    let text = "# Heading +sa";
    let cursor = text.find("+sa").unwrap() + 3;
    let trigger = project_tag_trigger(text, cursor).unwrap();
    let applied = apply_project_tag_selection(
        text,
        (trigger.start, trigger.end),
        "+sase ",
        &[],
        &catalog(),
    );
    assert_eq!(applied.text, "+sase # Heading ");
}

#[test]
fn accept_keeps_heading_lines() {
    // `# Heading` is not a workspace ref for any known workflow, so the row
    // goes to the leading position and the heading line stays.
    assert_eq!(
        apply("# Heading +sa‸", "+sase "),
        "+sase # Heading ",
        "heading line stays"
    );
}

#[test]
fn accept_switches_projects_within_one_segment() {
    // The accepted project lands at the earliest workspace target in the
    // same segment; every other target there goes away.
    assert_eq!(apply("+sase do it +bo‸", "+bob-cli "), "+bob-cli do it ");
    assert_eq!(apply("#git:notes +sa‸", "+sase "), "+sase ");
    // Other segments keep their targets; the Body segment has none, so the
    // row goes to its leading position.
    assert_eq!(
        apply("#gh:sase\n---\nBody +sa‸", "+sase "),
        "#gh:sase\n---\n+sase Body "
    );
    // PR-style insertion keeps its `#` spelling at the leading position.
    assert_eq!(apply("Review +sh‸", "#gh:ship "), "#gh:ship Review ");
}

#[test]
fn accept_skips_literal_zones() {
    // Tags inside fenced code and frontmatter are inert: they are neither
    // destinations nor deletions. With no live target the row goes to the
    // segment's leading position.
    assert_eq!(
        apply("```\n+bob-cli\n```\nBody +sa‸", "+sase "),
        "+sase ```\n+bob-cli\n```\nBody "
    );
    assert_eq!(
        apply("---\ntitle: +bob-cli\n---\nBody +sa‸", "+sase "),
        "---\ntitle: +bob-cli\n---\n+sase Body "
    );
}

#[test]
fn accept_handles_multibyte_text() {
    // No live target, so the row goes to the leading position; byte
    // offsets still land on character boundaries.
    assert_eq!(apply("é +sa‸", "+sase "), "+sase é ");
}

#[test]
fn accept_keeps_alternation_branches_intact() {
    // A trigger inside an alternation body expands at its own token;
    // sibling branches and standalone targets outside stay as they are.
    assert_eq!(apply("%{a | +sa‸}", "+sase "), "%{a | +sase }");
    assert_eq!(
        apply("%alt(a, +sa‸) +notes", "+sase "),
        "%alt(a, +sase ) +notes"
    );
    assert_eq!(
        apply("%(a | +sa‸, b) #git:foo", "+sase "),
        "%(a | +sase , b) #git:foo"
    );
    // Branch tags and refs are never destinations or deletions for a
    // trigger elsewhere; the row below lands at the segment's leading
    // position instead.
    assert_eq!(
        apply("Fix +sa‸ %{+notes | +bob-cli}", "+sase "),
        "+sase Fix %{+notes | +bob-cli}"
    );
    assert_eq!(
        apply("Fix +sa‸ %{#git:foo | #git:notes}", "+sase "),
        "+sase Fix %{#git:foo | #git:notes}"
    );
    // A segment that opens with an alternation does not split a branch for
    // the leading insertion: the row merges at the trigger instead.
    assert_eq!(
        apply("+sa‸ %{+notes | +bob-cli}", "+sase "),
        "+sase %{+notes | +bob-cli}"
    );
}
