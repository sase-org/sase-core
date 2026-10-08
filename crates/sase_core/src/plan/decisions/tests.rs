//! Integration coverage for the Plan Decision authoring grammar.
//!
//! Frontmatter rules run through `plan_validate` on both tiers; body rules
//! pin original-document line numbers and the advisory memory heuristic.
//! Compact regression inputs mirror archived plan phrasings (see the local
//! plan archive and Section "Evidence from this host" in
//! `research:202610/plan_frontmatter_decisions`) without copying reports.

use crate::plan::{
    plan_validate, plan_validate_with_mode, PlanValidationResultWire,
};
use serde_json::json;

fn codes(result: &PlanValidationResultWire) -> Vec<&str> {
    result
        .diagnostics
        .iter()
        .map(|diagnostic| diagnostic.code.as_str())
        .collect()
}

fn tale(decisions: &str, body: &str) -> String {
    format!(
        "---\ntier: tale\ntitle: T\ngoal: G\nsize: small\n{decisions}---\n{body}"
    )
}

fn epic(decisions: &str, body: &str) -> String {
    format!(
        "---\ntier: epic\ntitle: E\ngoal: G\nphases:\n  - id: core\n    title: Core\n    depends_on: []\n    description: Core section validates decisions.\n    size: small\n{decisions}---\n{body}"
    )
}

fn mentioned(id: &str) -> String {
    format!("# Plan\nShip {id}.\n")
}

#[test]
fn full_toggle_choice_and_memory_wire_is_ordered() {
    let decisions = "decisions:\n\
        \x20 tui_note:\n\
        \x20   ask: Edit the TUI note?\n\
        \x20   default: false\n\
        \x20   memory:\n\
        \x20     - tui.md\n\
        \x20     - glossary:stitch\n\
        \x20 grouping:\n\
        \x20   ask: How to group?\n\
        \x20   choices:\n\
        \x20     zebra: Group by zebra\n\
        \x20     apple: Group by apple\n\
        \x20   default: apple\n\
        \x20   why: Keeps the review short\n";
    let body = "# Plan\n\
        Ship tui_note and grouping.\n\
        > [!decision] tui_note\n\
        > Covers the note edits.\n\
        > [!decision] grouping = zebra\n";
    let result = plan_validate(&tale(decisions, body), "tale").unwrap();
    assert!(result.ok, "{:?}", result.diagnostics);
    assert_eq!(codes(&result), Vec::<&str>::new());
    let plan = result.plan.unwrap();
    // YAML author order survives, including choice order.
    assert_eq!(
        plan.decisions
            .iter()
            .map(|d| d.id.as_str())
            .collect::<Vec<_>>(),
        ["tui_note", "grouping"]
    );
    let value = serde_json::to_value(&plan).unwrap();
    assert_eq!(
        value["decisions"],
        json!([
            {
                "id": "tui_note",
                "kind": "toggle",
                "ask": "Edit the TUI note?",
                "default": false,
                "memory": {"selectors": ["tui.md", "glossary:stitch"]},
            },
            {
                "id": "grouping",
                "kind": "choice",
                "ask": "How to group?",
                "why": "Keeps the review short",
                "choices": [
                    {"key": "zebra", "label": "Group by zebra"},
                    {"key": "apple", "label": "Group by apple"},
                ],
                "default": "apple",
            }
        ])
    );
    assert_eq!(
        value["decision_callouts"],
        json!([
            {"id": "tui_note", "branch": "yes", "start_line": 23, "end_line": 24},
            {"id": "grouping", "key": "zebra", "branch": "choice", "start_line": 25, "end_line": 25},
        ])
    );
}

#[test]
fn absent_or_empty_decisions_add_no_fields() {
    for decisions in ["", "decisions: {}\n"] {
        let content = tale(decisions, "# Plan\nDo it.\n");
        let result = plan_validate(&content, "tale").unwrap();
        assert!(result.ok, "{decisions:?}: {:?}", result.diagnostics);
        let value = serde_json::to_value(result.plan.unwrap()).unwrap();
        assert!(value.get("decisions").is_none(), "{decisions:?}");
        assert!(value.get("decision_callouts").is_none(), "{decisions:?}");
        assert!(value.get("decided_by").is_none(), "{decisions:?}");
        assert!(value.get("decided_via").is_none(), "{decisions:?}");
    }
}

#[test]
fn decisions_must_be_an_ordered_map_of_at_most_five() {
    let mut block = String::from("decisions:\n");
    for index in 0..6 {
        block.push_str(&format!(
            "  d{index}:\n    ask: Do d{index}?\n    default: false\n"
        ));
    }
    let body = "# Plan\nShip d0 d1 d2 d3 d4 d5.\n";
    let result = plan_validate(&tale(&block, body), "tale").unwrap();
    assert!(!result.ok);
    assert!(codes(&result).contains(&"decision-limit"));

    let result =
        plan_validate(&tale("decisions: [nope]\n", "# Plan\n"), "tale")
            .unwrap();
    assert_eq!(codes(&result), ["decision-invalid"]);

    let result = plan_validate(
        &tale("decisions:\n  lone: [nope]\n", &mentioned("lone")),
        "tale",
    )
    .unwrap();
    assert!(codes(&result).contains(&"decision-invalid"));
}

#[test]
fn unknown_decision_fields_are_errors() {
    let decisions =
        "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: false\n    surprise: true\n";
    let result =
        plan_validate(&tale(decisions, &mentioned("tui_note")), "tale")
            .unwrap();
    assert!(!result.ok);
    let diagnostic = result
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == "decision-unknown-field")
        .unwrap();
    assert_eq!(diagnostic.field_path, "decisions.tui_note.surprise");
}

#[test]
fn id_spelling_reserved_words_and_types_are_rejected() {
    for bad in [
        "Bad",
        "has-hyphen",
        "has space",
        "9lives",
        "_lead",
        "yes",
        "no",
        "on",
        "off",
        "true",
        "false",
        "null",
        "y",
        "n",
        "UPPER",
    ] {
        let decisions = format!(
            "decisions:\n  {bad}:\n    ask: Do it?\n    default: false\n"
        );
        let result =
            plan_validate(&tale(&decisions, "# Plan\n"), "tale").unwrap();
        assert!(
            codes(&result).contains(&"decision-id-invalid"),
            "{bad}: {:?}",
            result.diagnostics
        );
    }
    let too_long = "a".repeat(33);
    let decisions = format!(
        "decisions:\n  {too_long}:\n    ask: Do it?\n    default: false\n"
    );
    assert!(codes(
        &plan_validate(&tale(&decisions, "# Plan\n"), "tale").unwrap()
    )
    .contains(&"decision-id-invalid"));
    // Non-string keys are invalid ids too.
    let result = plan_validate(
        &tale(
            "decisions:\n  true:\n    ask: Do it?\n    default: false\n",
            "# Plan\n",
        ),
        "tale",
    )
    .unwrap();
    assert!(codes(&result).contains(&"decision-id-invalid"));

    for reserved in [
        "approve",
        "commit",
        "reject",
        "feedback",
        "coder_prompt",
        "coder_model",
        "wait",
        "epic_launch_mode",
        "capacity",
    ] {
        let decisions = format!(
            "decisions:\n  {reserved}:\n    ask: Do it?\n    default: false\n"
        );
        let result =
            plan_validate(&tale(&decisions, &mentioned(reserved)), "tale")
                .unwrap();
        assert!(
            codes(&result).contains(&"decision-id-reserved"),
            "{reserved}: {:?}",
            result.diagnostics
        );
    }
    // Host reserved names are fine as choice keys.
    let decisions = "decisions:\n  grouping:\n    ask: How?\n    choices:\n      approve: The approve branch\n      other: The other branch\n    default: other\n";
    let result = plan_validate(
        &tale(
            decisions,
            "# Plan\nShip grouping.\n> [!decision] grouping = approve\n",
        ),
        "tale",
    )
    .unwrap();
    assert!(result.ok, "{:?}", result.diagnostics);
}

#[test]
fn ask_rules_cover_shape_length_unicode_and_questions() {
    // Missing, empty, multiline, and over-long asks share one code.
    let base = "decisions:\n  tui_note:\n    default: false\n";
    let result =
        plan_validate(&tale(base, &mentioned("tui_note")), "tale").unwrap();
    assert!(codes(&result).contains(&"decision-ask-invalid"));

    for (ask_yaml, code) in [
        ("ask: ''", "decision-ask-invalid"),
        ("ask: 42", "decision-ask-invalid"),
        ("ask: Do it", "decision-ask-not-question"),
    ] {
        let decisions = format!(
            "decisions:\n  tui_note:\n    {ask_yaml}\n    default: false\n"
        );
        let result =
            plan_validate(&tale(&decisions, &mentioned("tui_note")), "tale")
                .unwrap();
        assert!(
            codes(&result).contains(&code),
            "{ask_yaml}: {:?}",
            result.diagnostics
        );
        if code == "decision-ask-not-question" {
            assert!(result.ok, "{ask_yaml}: {:?}", result.diagnostics);
        }
    }

    // Length counts Unicode characters, not UTF-8 bytes.
    let emoji_120 = "🪲".repeat(120);
    let decisions = format!(
        "decisions:\n  tui_note:\n    ask: \"{emoji_120}?\"\n    default: false\n"
    );
    let result =
        plan_validate(&tale(&decisions, &mentioned("tui_note")), "tale")
            .unwrap();
    assert!(
        codes(&result).contains(&"decision-ask-invalid"),
        "121 chars must fail: {:?}",
        result.diagnostics
    );
    let emoji_119 = "🪲".repeat(119);
    let decisions = format!(
        "decisions:\n  tui_note:\n    ask: \"{emoji_119}?\"\n    default: false\n"
    );
    let result =
        plan_validate(&tale(&decisions, &mentioned("tui_note")), "tale")
            .unwrap();
    assert!(result.ok, "120 chars must pass: {:?}", result.diagnostics);
}

#[test]
fn choice_counts_keys_and_labels_are_checked() {
    // One entry, six entries, and a non-map all violate the 2-5 map rule.
    for choices_yaml in [
        "choices:\n      only: Just one\n",
        "choices:\n      a: A\n      b: B\n      c: C\n      d: D\n      e: E\n      f: F\n",
        "choices: [a, b]\n",
    ] {
        let decisions = format!(
            "decisions:\n  grouping:\n    ask: How?\n    {choices_yaml}    default: a\n"
        );
        let result = plan_validate(
            &tale(&decisions, &mentioned("grouping")),
            "tale",
        )
        .unwrap();
        assert!(
            codes(&result).contains(&"decision-choices-count"),
            "{choices_yaml:?}: {:?}",
            result.diagnostics
        );
    }
    // Bad key spellings, YAML words, over-long keys, and bad labels.
    for (key, label) in [
        ("Bad", "Label"),
        ("has-hyphen", "Label"),
        ("yes", "Label"),
        ("a_very_long_choice_key_over_24", "Label"),
        ("mode", ""),
        ("mode", "[not, text]"),
    ] {
        let decisions = format!(
            "decisions:\n  grouping:\n    ask: How?\n    choices:\n      {key}: {label}\n      other: Other branch\n    default: other\n"
        );
        let result =
            plan_validate(&tale(&decisions, &mentioned("grouping")), "tale")
                .unwrap();
        assert!(
            codes(&result).contains(&"decision-choice-invalid"),
            "{key}/{label}: {:?}",
            result.diagnostics
        );
    }
}

#[test]
fn defaults_are_required_and_kind_checked() {
    // Missing defaults on both kinds.
    for decisions in [
        "decisions:\n  tui_note:\n    ask: Do tui_note?\n",
        "decisions:\n  grouping:\n    ask: How?\n    choices:\n      mode: Group by mode\n      pane: Group by pane\n",
    ] {
        let result =
            plan_validate(&tale(decisions, "# Plan\n"), "tale").unwrap();
        assert!(
            codes(&result).contains(&"decision-default-missing"),
            "{decisions:?}: {:?}",
            result.diagnostics
        );
    }
    // Toggle defaults must be YAML booleans; strings get a true/false remedy.
    let decisions =
        "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: 'yes'\n";
    let result =
        plan_validate(&tale(decisions, &mentioned("tui_note")), "tale")
            .unwrap();
    assert_eq!(codes(&result), ["decision-default-invalid"]);
    assert!(
        result.diagnostics[0].message.contains("true`/`false`"),
        "{}",
        result.diagnostics[0].message
    );
    // Choice defaults must exactly name an authored key.
    for default in ["oops", "Mode", "42"] {
        let decisions = format!(
            "decisions:\n  grouping:\n    ask: How?\n    choices:\n      mode: Group by mode\n      pane: Group by pane\n    default: {default}\n"
        );
        let result =
            plan_validate(&tale(&decisions, &mentioned("grouping")), "tale")
                .unwrap();
        assert!(
            codes(&result).contains(&"decision-default-invalid"),
            "{default}: {:?}",
            result.diagnostics
        );
    }
}

#[test]
fn why_is_bounded_and_forbidden_on_memory() {
    let long = "w".repeat(101);
    let decisions = format!(
        "decisions:\n  grouping:\n    ask: How?\n    choices:\n      mode: Group by mode\n      pane: Group by pane\n    default: mode\n    why: {long}\n"
    );
    let result =
        plan_validate(&tale(&decisions, &mentioned("grouping")), "tale")
            .unwrap();
    assert!(codes(&result).contains(&"decision-why-invalid"));

    let decisions = "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: false\n    memory:\n      - tui.md\n    why: Because reasons\n";
    let result =
        plan_validate(&tale(decisions, &mentioned("tui_note")), "tale")
            .unwrap();
    assert!(codes(&result).contains(&"decision-why-on-memory"));
}

#[test]
fn memory_is_for_toggles_with_valid_selectors() {
    // Memory on a choice is rejected.
    let decisions = "decisions:\n  grouping:\n    ask: How?\n    choices:\n      mode: Group by mode\n      pane: Group by pane\n    default: mode\n    memory:\n      - tui.md\n";
    let result =
        plan_validate(&tale(decisions, &mentioned("grouping")), "tale")
            .unwrap();
    assert!(codes(&result).contains(&"decision-memory-on-choice"));

    // Malformed selector lists share one code.
    for memory_yaml in [
        "memory: tui.md\n",
        "memory: []\n",
        "memory:\n      - ''\n",
        "memory:\n      - 42\n",
        "memory:\n      - web:\n",
        "memory:\n      - :stitch\n",
        "memory:\n      - a//b\n",
        "memory:\n      - /leading\n",
    ] {
        let decisions = format!(
            "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: false\n    {memory_yaml}"
        );
        let result =
            plan_validate(&tale(&decisions, &mentioned("tui_note")), "tale")
                .unwrap();
        assert!(
            codes(&result).contains(&"decision-memory-selector-invalid"),
            "{memory_yaml:?}: {:?}",
            result.diagnostics
        );
    }

    // Host selector forms pass: bare webs, notes, nested paths, aliases.
    let decisions = "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: false\n    memory:\n      - tui.md\n      - web\n      - glossary\n      - web:keyword\n      - glossary:stitch\n      - nested/relative/note.md\n";
    let result =
        plan_validate(&tale(decisions, &mentioned("tui_note")), "tale")
            .unwrap();
    assert!(result.ok, "{:?}", result.diagnostics);
    assert_eq!(
        result.plan.unwrap().decisions[0]
            .memory
            .as_ref()
            .unwrap()
            .selectors,
        [
            "tui.md",
            "web",
            "glossary",
            "web:keyword",
            "glossary:stitch",
            "nested/relative/note.md"
        ]
    );
}

#[test]
fn requested_is_memory_only_bounded_and_required_for_true() {
    // `requested` on a plain toggle or a choice is rejected.
    for decisions in [
        "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: false\n    requested: please do it\n",
        "decisions:\n  grouping:\n    ask: How?\n    choices:\n      mode: Group by mode\n      pane: Group by pane\n    default: mode\n    requested: group by mode\n",
    ] {
        let result =
            plan_validate(&tale(decisions, "# Plan\n"), "tale").unwrap();
        assert!(
            codes(&result).contains(&"decision-requested-not-memory"),
            "{decisions:?}: {:?}",
            result.diagnostics
        );
    }
    // A memory default of true without a quote is rejected.
    let decisions = "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: true\n    memory:\n      - tui.md\n";
    let result =
        plan_validate(&tale(decisions, &mentioned("tui_note")), "tale")
            .unwrap();
    assert!(codes(&result).contains(&"decision-requested-missing"));

    // Malformed quotes share one code; the bounds are 3-300 characters.
    for requested in ["ab", &"q".repeat(301)] {
        let decisions = format!(
            "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: true\n    memory:\n      - tui.md\n    requested: {requested}\n"
        );
        let result =
            plan_validate(&tale(&decisions, &mentioned("tui_note")), "tale")
                .unwrap();
        assert!(
            codes(&result).contains(&"decision-requested-invalid"),
            "{requested:?}: {:?}",
            result.diagnostics
        );
    }
    // A memory default of false needs no quote.
    let decisions = "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: false\n    memory:\n      - tui.md\n";
    let result =
        plan_validate(&tale(decisions, &mentioned("tui_note")), "tale")
            .unwrap();
    assert!(result.ok, "{:?}", result.diagnostics);
}

fn stamped_tale(stamps: &str, decisions: &str, body: &str) -> String {
    tale(&format!("{stamps}{decisions}"), body)
}

#[test]
fn answers_and_stamps_depend_on_mode() {
    let answered = "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: false\n    answer: true\n";
    // Authoring forbids system-written fields, pointing at each exact field.
    let content = stamped_tale(
        "decided_by: reviewer\ndecided_via: tui\n",
        answered,
        &mentioned("tui_note"),
    );
    let result = plan_validate(&content, "tale").unwrap();
    assert!(!result.ok);
    for field in ["decided_by", "decided_via", "decisions.tui_note.answer"] {
        assert!(
            result.diagnostics.iter().any(|diagnostic| {
                (diagnostic.code == "decision-answer-forbidden"
                    || diagnostic.code == "decision-system-field-forbidden")
                    && diagnostic.field_path == field
            }),
            "{field}: {:?}",
            result.diagnostics
        );
    }
    // Launch accepts valid answers and keeps legacy size normalization.
    let result = plan_validate_with_mode(
        &tale(answered, &mentioned("tui_note")),
        "tale",
        "launch",
    )
    .unwrap();
    assert!(result.ok, "{:?}", result.diagnostics);
    let plan = result.plan.unwrap();
    assert_eq!(plan.decisions[0].answer, Some(json!(true)));
    assert_eq!(plan.decided_by, None);

    // Launch validates answer shapes.
    let bad = "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: false\n    answer: 'yes'\n";
    let result = plan_validate_with_mode(
        &tale(bad, &mentioned("tui_note")),
        "tale",
        "launch",
    )
    .unwrap();
    assert!(codes(&result).contains(&"decision-answer-invalid"));

    // Archived without stamps is as strict as Authoring and still valid.
    let plain =
        "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: false\n";
    let result = plan_validate_with_mode(
        &tale(plain, &mentioned("tui_note")),
        "tale",
        "archived",
    )
    .unwrap();
    assert!(result.ok, "{:?}", result.diagnostics);

    // Archived completeness: top-level-only and answer-only stamps fail.
    let content = stamped_tale(
        "decided_by: reviewer\ndecided_via: tui\n",
        plain,
        &mentioned("tui_note"),
    );
    let result = plan_validate_with_mode(&content, "tale", "archived").unwrap();
    assert!(codes(&result).contains(&"decision-answer-incomplete"));

    let content = tale(answered, &mentioned("tui_note"));
    let result = plan_validate_with_mode(&content, "tale", "archived").unwrap();
    assert!(codes(&result).contains(&"decision-answer-incomplete"));

    // Full archived stamps validate and round-trip through the wire.
    let content = stamped_tale(
        "decided_by: reviewer\ndecided_via: tui\n",
        answered,
        &mentioned("tui_note"),
    );
    let result = plan_validate_with_mode(&content, "tale", "archived").unwrap();
    assert!(result.ok, "{:?}", result.diagnostics);
    let plan = result.plan.unwrap();
    assert_eq!(plan.decided_by.as_deref(), Some("reviewer"));
    assert_eq!(plan.decided_via.as_deref(), Some("tui"));
    assert_eq!(plan.decisions[0].answer, Some(json!(true)));

    // Provenance values are checked: bad enums and auto-with-transport fail.
    for stamps in [
        "decided_by: human\n",
        "decided_by: reviewer\ndecided_via: pager\n",
        "decided_by: auto\ndecided_via: tui\n",
    ] {
        let content = stamped_tale(stamps, answered, &mentioned("tui_note"));
        let result =
            plan_validate_with_mode(&content, "tale", "archived").unwrap();
        assert!(
            codes(&result).contains(&"decision-provenance-invalid"),
            "{stamps:?}: {:?}",
            result.diagnostics
        );
    }
    // `auto` without transport is the valid unattended stamp.
    let content =
        stamped_tale("decided_by: auto\n", answered, &mentioned("tui_note"));
    let result = plan_validate_with_mode(&content, "tale", "archived").unwrap();
    assert!(result.ok, "{:?}", result.diagnostics);

    // Archived keeps Authoring's strict sizes while Launch normalizes.
    let missing_size = "---\ntier: tale\ntitle: T\ngoal: G\n---\nbody\n";
    assert!(
        !plan_validate_with_mode(missing_size, "tale", "archived")
            .unwrap()
            .ok
    );
    assert!(
        plan_validate_with_mode(missing_size, "tale", "launch")
            .unwrap()
            .ok
    );

    // Unknown modes still fail, naming the supported modes.
    let error =
        plan_validate_with_mode(&tale("", "# Plan\n"), "tale", "resume")
            .unwrap_err();
    assert!(error.message.contains("archived"));
}

#[test]
fn phase_when_is_always_reserved() {
    for when in ["when: false", "when: null", "when: true"] {
        let content = format!(
            "---\ntier: epic\ntitle: E\ngoal: G\nphases:\n  - id: core\n    title: Core\n    depends_on: []\n    description: Core section.\n    size: small\n    {when}\n---\nbody\n"
        );
        let result = plan_validate(&content, "epic").unwrap();
        assert!(
            result.diagnostics.iter().any(|diagnostic| {
                diagnostic.code == "phase-when-reserved"
                    && diagnostic.field_path == "phases[0].when"
            }),
            "{when}: {:?}",
            result.diagnostics
        );
    }
}

#[test]
fn duplicate_yaml_keys_stay_rejected() {
    // serde_yaml rejects duplicate keys at parse time, so author order is
    // the only accepted vector: duplicates never reach decision validation.
    let content = tale(
        "decisions:\n  dup:\n    ask: First?\n    default: false\n  dup:\n    ask: Second?\n    default: true\n",
        &mentioned("dup"),
    );
    let result = plan_validate(&content, "tale").unwrap();
    assert!(!result.ok);
    assert!(codes(&result).contains(&"yaml-invalid"));

    let content = tale("title: T\ntitle: Twice\n", "# Plan\n");
    let result = plan_validate(&content, "tale").unwrap();
    assert!(codes(&result).contains(&"yaml-invalid"));
}

#[test]
fn decision_source_lines_point_at_block_yaml() {
    // Lines: 1 `---`, 2-5 tier/title/goal/size, 6 `decisions:`,
    // 7 `tui_note:`, 8 `ask:`, 9 `default:`, 10 `memory:`, 11 `- tui.md`.
    let decisions = "decisions:\n  tui_note:\n    ask: 42\n    default: false\n    memory:\n      - tui.md\n";
    let result =
        plan_validate(&tale(decisions, &mentioned("tui_note")), "tale")
            .unwrap();
    let diagnostic = result
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == "decision-ask-invalid")
        .unwrap();
    assert_eq!(diagnostic.field_path, "decisions.tui_note.ask");
    assert_eq!(diagnostic.line, Some(8));

    // Choice keys resolve to their own lines.
    let decisions = "decisions:\n  grouping:\n    ask: How?\n    choices:\n      mode: 42\n      pane: Group by pane\n    default: pane\n";
    let result =
        plan_validate(&tale(decisions, &mentioned("grouping")), "tale")
            .unwrap();
    let diagnostic = result
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == "decision-choice-invalid")
        .unwrap();
    assert_eq!(diagnostic.field_path, "decisions.grouping.choices.mode");
    assert_eq!(diagnostic.line, Some(10));

    // Quoted keys and CRLF sources resolve the same way.
    let decisions =
        "decisions:\n  'tui_note':\n    ask: 42\n    default: false\n";
    let content = tale(decisions, &mentioned("tui_note")).replace('\n', "\r\n");
    let result = plan_validate(&content, "tale").unwrap();
    let diagnostic = result
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == "decision-ask-invalid")
        .unwrap();
    assert_eq!(diagnostic.line, Some(8));

    // Flow mappings fall back to the containing field.
    let decisions = "decisions: {tui_note: {ask: 42, default: false}}\n";
    let result =
        plan_validate(&tale(decisions, &mentioned("tui_note")), "tale")
            .unwrap();
    let diagnostic = result
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == "decision-ask-invalid")
        .unwrap();
    assert_eq!(diagnostic.field_path, "decisions.tui_note.ask");
    assert_eq!(diagnostic.line, Some(6));
}

#[test]
fn decisions_work_on_epics_with_callouts() {
    let decisions = "decisions:\n  grouping:\n    ask: How?\n    choices:\n      mode: Group by mode\n      pane: Group by pane\n    default: mode\n";
    let body = "# Plan\nShip grouping.\n> [!decision] grouping = pane\n";
    let result = plan_validate(&epic(decisions, body), "epic").unwrap();
    assert!(result.ok, "{:?}", result.diagnostics);
    let plan = result.plan.unwrap();
    assert_eq!(plan.decisions.len(), 1);
    assert_eq!(plan.decision_callouts.len(), 1);
    assert_eq!(plan.decision_callouts[0].branch, "choice");
    assert_eq!(plan.decision_callouts[0].key.as_deref(), Some("pane"));
}

#[test]
fn callout_branches_validate_against_definitions() {
    let decisions = "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: false\n  grouping:\n    ask: How?\n    choices:\n      mode: Group by mode\n      pane: Group by pane\n    default: mode\n";
    // Unknown ids, unknown choice keys, bare choices, and toggle keys fail.
    for (header, path) in [
        ("> [!decision] nope\n", "decisions.nope"),
        ("> [!decision] grouping = oops\n", "decisions.grouping"),
        ("> [!decision] grouping\n", "decisions.grouping"),
        ("> [!decision] tui_note = mode\n", "decisions.tui_note"),
        ("> [!decision]\n", ""),
    ] {
        let body = format!("# Plan\nShip tui_note and grouping.\n{header}");
        let result = plan_validate(&tale(decisions, &body), "tale").unwrap();
        assert!(
            result.diagnostics.iter().any(|diagnostic| {
                diagnostic.code == "decision-branch-unknown"
                    && diagnostic.field_path == path
                    && diagnostic.line == Some(19)
            }),
            "{header:?}: {:?}",
            result.diagnostics
        );
    }
    // `= no` selects the no branch and `= yes` is accepted for toggles.
    for (header, branch) in [
        ("> [!decision] tui_note = no\n", "no"),
        ("> [!decision] tui_note = yes\n", "yes"),
    ] {
        let body = format!("# Plan\nShip tui_note and grouping.\n{header}");
        let result = plan_validate(&tale(decisions, &body), "tale").unwrap();
        assert!(result.ok, "{header:?}: {:?}", result.diagnostics);
        assert_eq!(result.plan.unwrap().decision_callouts[0].branch, branch);
    }
    // A fenced callout is an example, not a branch: no unknown-id error,
    // but the id stays unreferenced.
    let body =
        "# Plan\nShip tui_note and grouping.\n```\n> [!decision] nope\n```\n";
    let result = plan_validate(&tale(decisions, body), "tale").unwrap();
    assert!(!codes(&result).contains(&"decision-branch-unknown"));
}

#[test]
fn unreferenced_decisions_warn_unless_mentioned() {
    let decisions =
        "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: false\n";
    let result =
        plan_validate(&tale(decisions, "# Plan\nUnrelated.\n"), "tale")
            .unwrap();
    assert!(result.ok);
    assert!(
        result.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "decision-unreferenced"
                && diagnostic.field_path == "decisions.tui_note"
        }),
        "{:?}",
        result.diagnostics
    );
    // A whole-word body mention suppresses the warning.
    let result =
        plan_validate(&tale(decisions, &mentioned("tui_note")), "tale")
            .unwrap();
    assert!(result.ok, "{:?}", result.diagnostics);
    assert_eq!(codes(&result), Vec::<&str>::new());
}

#[test]
fn uncovered_memory_edits_warn_until_covered() {
    let plain =
        "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: false\n";
    let memory =
        "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: false\n    memory:\n      - tui.md\n";
    // `sase memory init` without a covering memory decision warns; the
    // archived phrasing runs init beside disclaimer words and still fires.
    for body in [
        "# Plan\nRun `sase memory init`.\n",
        "# Plan\nRun `sase memory init`; never hand-edit generated shims.\n",
    ] {
        let result = plan_validate(&tale(plain, body), "tale").unwrap();
        assert!(
            result.diagnostics.iter().any(|diagnostic| {
                diagnostic.code == "decision-memory-uncovered"
                    && diagnostic.line == Some(12)
            }),
            "{body:?}: {:?}",
            result.diagnostics
        );
    }
    // A covering memory decision keeps init quiet.
    let result = plan_validate(
        &tale(memory, "# Plan\nRun `sase memory init`.\n"),
        "tale",
    )
    .unwrap();
    assert!(
        !codes(&result).contains(&"decision-memory-uncovered"),
        "{:?}",
        result.diagnostics
    );
    // Archived edit-verb phrasings warn: create, add, update, delete.
    for body in [
        "# Plan\nCreate `sase/memory/glossary/morning.md` with the text.\n",
        "# Plan\nAdd `sase/memory/decisions/note.md`, titled 'Note'.\n",
        "# Plan\nUpdate `sase/memory/glossary/tui.md`: freshness never changes.\n",
        "# Plan\n9. **Delete** `sase/memory/scratch.md`.\n",
    ] {
        let result = plan_validate(&tale(plain, body), "tale").unwrap();
        assert!(
            result.diagnostics.iter().any(|diagnostic| {
                diagnostic.code == "decision-memory-uncovered"
            }),
            "{body:?}: {:?}",
            result.diagnostics
        );
    }
    // Exact and basename selectors cover the note.
    for decisions in [
        "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: false\n    memory:\n      - glossary/tui.md\n",
        "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: false\n    memory:\n      - tui.md\n",
    ] {
        let result = plan_validate(
            &tale(
                decisions,
                "# Plan\nUpdate `sase/memory/glossary/tui.md` now.\n",
            ),
            "tale",
        )
        .unwrap();
        assert!(
            !codes(&result).contains(&"decision-memory-uncovered"),
            "{decisions:?}: {:?}",
            result.diagnostics
        );
    }
    // Read-only references, disclaimers, source paths, and fences stay quiet.
    for body in [
        "# Plan\nRead `sase/memory/cli_rules.md` first.\n",
        "# Plan\nDo not edit `sase/memory/tui.md`.\n",
        "# Plan\nAdd `src/sase/memory/history/pager_provider.py`.\n",
        "# Plan\n```\nRun `sase memory init`.\n```\n",
    ] {
        let result = plan_validate(&tale(plain, body), "tale").unwrap();
        assert!(
            !codes(&result).contains(&"decision-memory-uncovered"),
            "{body:?}: {:?}",
            result.diagnostics
        );
    }
}

#[test]
fn warning_only_ask_preserves_full_decision_wire() {
    use crate::plan::decisions::plan_decision_sheet;
    use crate::plan::decisions::{
        plan_decisions_digest, plan_decisions_payload,
    };
    use std::collections::BTreeMap;
    // A valid ask lacking `?` warns but must keep the decision in author
    // order with choices, memory, and answer shapes intact.
    let decisions = "decisions:\n  grouping:\n    ask: How to group\n    choices:\n      pane: By pane\n      mode: By mode\n    default: pane\n    why: Keeps order\n  tui_note:\n    ask: Record the note\n    default: false\n    memory:\n      - tui.md\n";
    for (content, tier) in [
        (tale(decisions, &mentioned("grouping tui_note")), "tale"),
        (epic(decisions, &mentioned("grouping tui_note")), "epic"),
    ] {
        for mode in ["authoring", "launch", "archived"] {
            let result = plan_validate_with_mode(&content, tier, mode).unwrap();
            assert!(result.ok, "{tier}/{mode}: {:?}", result.diagnostics);
            assert!(codes(&result).contains(&"decision-ask-not-question"));
            let plan = result.plan.unwrap();
            assert_eq!(plan.decisions.len(), 2, "{tier}/{mode}");
            assert_eq!(plan.decisions[0].id, "grouping");
            assert_eq!(plan.decisions[0].choices.len(), 2);
            assert_eq!(plan.decisions[1].id, "tui_note");
            assert!(plan.decisions[1].memory.is_some());
            // Payload, digest, and sheet run on the preserved wire.
            let facts = BTreeMap::new();
            let definitions = plan_decisions_payload(&plan, &facts).unwrap();
            assert_eq!(definitions.len(), 2);
            assert_eq!(definitions[0].id, "grouping");
            let digest = plan_decisions_digest(&definitions).unwrap();
            assert_eq!(digest.len(), 64);
            let values =
                serde_json::json!({"grouping": "pane", "tui_note": false});
            let sheet = plan_decision_sheet(&definitions, &values, 1).unwrap();
            assert_eq!(sheet.count, 2);
            assert_eq!(sheet.rows[0].id, "grouping");
        }
    }
    // An actual error still fails validation.
    let bad = "decisions:\n  grouping:\n    ask: How?\n    choices:\n      pane: By pane\n      mode: By mode\n";
    let result =
        plan_validate(&tale(bad, &mentioned("grouping")), "tale").unwrap();
    assert!(!result.ok);
    assert!(codes(&result).contains(&"decision-default-missing"));
    if let Some(plan) = result.plan {
        assert!(plan.decisions.is_empty());
    }
}

#[test]
fn archived_completeness_triggers_on_any_stamp() {
    let plain =
        "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: false\n";
    let answered = "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: false\n    answer: true\n";
    // Absent decisions with a transport-only stamp fail.
    for decisions in ["", "decisions: {}\n"] {
        let content =
            tale(&format!("decided_via: tui\n{decisions}"), "# Plan\n");
        let result =
            plan_validate_with_mode(&content, "tale", "archived").unwrap();
        assert!(
            codes(&result).contains(&"decision-answer-incomplete"),
            "absent {decisions:?}: {:?}",
            result.diagnostics
        );
        // Fully unstamped absent/empty stays valid.
        let content = tale(decisions, "# Plan\n");
        let result =
            plan_validate_with_mode(&content, "tale", "archived").unwrap();
        assert!(result.ok, "{decisions:?}: {:?}", result.diagnostics);
    }
    // Transport-only stamped decision fails without `decided_by`.
    let content = tale(
        &format!("decided_via: tui\n{answered}"),
        &mentioned("tui_note"),
    );
    let result = plan_validate_with_mode(&content, "tale", "archived").unwrap();
    assert!(codes(&result).contains(&"decision-answer-incomplete"));
    // Partial vectors fail: one answered decision plus an unanswered one.
    let partial = "decisions:\n  first:\n    ask: First?\n    default: false\n    answer: false\n  second:\n    ask: Second?\n    default: false\n";
    let content = tale(
        &format!("decided_by: reviewer\ndecided_via: tui\n{partial}"),
        "# Plan\nShip first and second.\n",
    );
    let result = plan_validate_with_mode(&content, "tale", "archived").unwrap();
    assert!(codes(&result).contains(&"decision-answer-incomplete"));
    // Fully stamped and unstamped controls stay valid.
    let content = tale(
        &format!("decided_by: reviewer\ndecided_via: tui\n{answered}"),
        &mentioned("tui_note"),
    );
    assert!(
        plan_validate_with_mode(&content, "tale", "archived")
            .unwrap()
            .ok
    );
    let content = tale(plain, &mentioned("tui_note"));
    assert!(
        plan_validate_with_mode(&content, "tale", "archived")
            .unwrap()
            .ok
    );
    // Invalid provenance still reports its own code alongside completeness.
    let content = tale(
        &format!("decided_by: human\ndecided_via: tui\n{answered}"),
        &mentioned("tui_note"),
    );
    let result = plan_validate_with_mode(&content, "tale", "archived").unwrap();
    assert!(codes(&result).contains(&"decision-provenance-invalid"));
    // Auto without transport stays the valid unattended stamp.
    let content = tale(
        &format!("decided_by: auto\n{answered}"),
        &mentioned("tui_note"),
    );
    assert!(
        plan_validate_with_mode(&content, "tale", "archived")
            .unwrap()
            .ok
    );
}

#[test]
fn unicode_body_checks_run_through_the_public_validator() {
    let plain =
        "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: false\n";
    // Ordinary Unicode prose with an edit verb warns through validation.
    let result = plan_validate(
        &tale(plain, "# Plan\nUpdate 🚀 sase/memory/tui.md.\n"),
        "tale",
    )
    .unwrap();
    assert!(codes(&result).contains(&"decision-memory-uncovered"));
    // Read-only Unicode text and the `src/` exclusion stay quiet.
    for body in [
        "# Plan\nRead 📚 sase/memory/tui.md first.\n",
        "# Plan\nAdd 🚀 src/sase/memory/tui.md.\n",
    ] {
        let result = plan_validate(&tale(plain, body), "tale").unwrap();
        assert!(
            !codes(&result).contains(&"decision-memory-uncovered"),
            "{body:?}: {:?}",
            result.diagnostics
        );
    }
}

#[test]
fn fenced_callout_boundary_does_not_absorb_later_quote() {
    let decisions =
        "decisions:\n  toggle:\n    ask: Do toggle?\n    default: false\n";
    let body = "> [!decision] toggle before\n\n```\ncode\n```\n\n> after\n";
    let result = plan_validate(&tale(decisions, body), "tale").unwrap();
    let plan = result.plan.unwrap();
    assert_eq!(plan.decision_callouts.len(), 1);
    assert_eq!(
        plan.decision_callouts[0].start_line,
        plan.decision_callouts[0].end_line,
        "fence boundary must end the callout: {:?}",
        plan.decision_callouts[0]
    );
}

#[test]
fn archived_heuristic_regression_phrases_match_sources() {
    // Compact phrases mirror Aug-Oct 2026 archive wording without copying
    // reports. Sources: plan:202608/glossary_tier1_memory_note.md (create
    // glossary note), plan:202608/sase_memory_bullet_order.md (add bullet
    // note), plan:202609/queue_capacity_budget.md (update xprompts note),
    // plan:202608/drop_plan_authoring_size_paragraph.md (delete scratch
    // note). Read-only/disclaimer controls stay quiet.
    let plain =
        "decisions:\n  tui_note:\n    ask: Do tui_note?\n    default: false\n";
    for body in [
        "# Plan\nCreate `sase/memory/glossary.md` with the text.\n",
        "# Plan\nAdd `sase/memory/decisions/note.md`, titled 'Note'.\n",
        "# Plan\nUpdate `sase/memory/xprompts.md` now.\n",
        "# Plan\n9. **Delete** `sase/memory/scratch.md`.\n",
        "# Plan\nRun `sase memory init`.\n",
    ] {
        let result = plan_validate(&tale(plain, body), "tale").unwrap();
        assert!(
            codes(&result).contains(&"decision-memory-uncovered"),
            "{body:?}: {:?}",
            result.diagnostics
        );
    }
    for body in [
        "# Plan\nRead `sase/memory/cli_rules.md` first.\n",
        "# Plan\nDo not edit `sase/memory/tui.md`.\n",
    ] {
        let result = plan_validate(&tale(plain, body), "tale").unwrap();
        assert!(
            !codes(&result).contains(&"decision-memory-uncovered"),
            "{body:?}: {:?}",
            result.diagnostics
        );
    }
}
