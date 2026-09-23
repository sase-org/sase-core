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

#[test]
fn accept_inserts_tags_in_place() {
    // Ported golden vectors: same inputs as the historical VCS-tag parity
    // table, now with in-place `+sase` insertion instead of prepend/replace.
    for (marked, expected) in [
        ("Describe this repo. +‸", "Describe this repo. +sase "),
        ("+‸", "+sase "),
        ("+sa‸", "+sase "),
        ("+s‸\n", "+sase \n"),
        ("+s‸\nmore text", "+sase \nmore text"),
        ("#git:foo Fix bug +‸", "Fix bug +sase "),
        ("#gh!!:foo do X +‸", "do X +sase "),
        ("#gh:sase +‸", "+sase "),
        ("#gh:sase +foo‸", "+sase "),
        ("#git:foo +‸", "+sase "),
        ("Fix +bug‸ here", "Fix +sase here"),
        ("Line one\n +‸", "Line one\n +sase "),
        (
            "---\nname: x\n---\nBody +‸",
            "---\nname: x\n---\nBody +sase ",
        ),
        ("%model:opus Body +‸", "%model:opus Body +sase "),
        ("+sa‸ Fix", "+sase Fix"),
        ("Fix +sa‸se now", "Fix +sase now"),
    ] {
        assert_eq!(apply(marked, "+sase "), expected, "accept: {marked:?}");
    }
}

#[test]
fn accept_removes_mid_line_refs_like_the_python_guard() {
    // The Python one-target guard counts mid-line refs
    // (`find_vcs_workflow_tag_span("fix in #gh:foo now")`), so accept
    // must remove them too, leaving exactly one workspace target.
    assert_eq!(
        apply("fix in #gh:foo now +sa‸", "+sase "),
        "fix in now +sase ",
        "mid-line ref is removed"
    );
    assert_eq!(
        apply("fix in #gh:foo now +sase do it +bo‸", "+bob-cli "),
        "fix in now do it +bob-cli ",
        "mid-line ref plus tag both go away"
    );
    // A `#` that is not at a token boundary is not a ref.
    assert_eq!(
        apply("a#gh:foo +sa‸", "+sase "),
        "a#gh:foo +sase ",
        "glued hash stays"
    );
}

#[test]
fn accept_with_empty_workflow_names_matches_nothing() {
    // An empty alternation must never match `# Heading` and delete it.
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
    assert_eq!(applied.text, "# Heading +sase ");
}

#[test]
fn accept_keeps_heading_lines() {
    // `# Heading` is not a workspace ref for any known workflow.
    assert_eq!(
        apply("# Heading +sa‸", "+sase "),
        "# Heading +sase ",
        "heading line stays"
    );
}

#[test]
fn accept_reports_cursor_past_insertion() {
    let text = "Fix +sa";
    let trigger = project_tag_trigger(text, 7).unwrap();
    let applied = apply_project_tag_selection(
        text,
        (trigger.start, trigger.end),
        "+sase ",
        &["gh".to_string()],
        &catalog(),
    );
    assert_eq!(applied.text, "Fix +sase ");
    assert_eq!(applied.cursor, applied.text.len());
    assert_eq!(&applied.text[..applied.cursor], "Fix +sase ");
}

#[test]
fn accept_switches_projects_within_one_segment() {
    // The accepted project replaces the trigger and every other workspace
    // target in the same segment goes away.
    assert_eq!(apply("+sase do it +bo‸", "+bob-cli "), "do it +bob-cli ");
    assert_eq!(apply("#git:notes +sa‸", "+sase "), "+sase ");
    // Other segments keep their targets.
    assert_eq!(
        apply("#gh:sase\n---\nBody +sa‸", "+sase "),
        "#gh:sase\n---\nBody +sase "
    );
    // PR-style insertion keeps its `#` spelling.
    assert_eq!(apply("Review +sh‸", "#gh:ship "), "Review #gh:ship ");
}

#[test]
fn accept_skips_literal_zones() {
    assert_eq!(
        apply("```\n+bob-cli\n```\nBody +sa‸", "+sase "),
        "```\n+bob-cli\n```\nBody +sase "
    );
    assert_eq!(
        apply("---\ntitle: +bob-cli\n---\nBody +sa‸", "+sase "),
        "---\ntitle: +bob-cli\n---\nBody +sase "
    );
}

#[test]
fn accept_handles_multibyte_text() {
    assert_eq!(apply("é +sa‸", "+sase "), "é +sase ");
}
