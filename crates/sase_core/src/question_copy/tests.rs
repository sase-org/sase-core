use serde_json::{json, Value};

use super::{
    detect_question_copy_candidates, display_question_copy,
    normalize_question_copy, QuestionCopyCandidateKind, QuestionCopyItemWire,
    QUESTION_COPY_MAX_ENTRIES, QUESTION_COPY_MAX_LABEL_CHARS,
    QUESTION_COPY_MAX_TEXT_BYTES,
};

fn normalized(value: Value) -> Vec<QuestionCopyItemWire> {
    normalize_question_copy(&value).unwrap()
}

fn error(value: Value) -> String {
    normalize_question_copy(&value).unwrap_err().to_string()
}

#[test]
fn normalizes_the_spotlight_copy_fixture_byte_exactly() {
    let input = json!([
        {"label": ".sase", "text": "/Users/bbugyi/.sase"},
        {"label": "App Support/sase", "text": "/Users/bbugyi/Library/Application Support/sase"},
        "/Users/bbugyi/.cargo",
        "/Users/bbugyi/.rustup",
        {"label": "bob-cli/target", "text": "/Users/bbugyi/projects/github/bobs-org/bob-cli/target"},
        {"label": "sase-core/target", "text": "/Users/bbugyi/projects/github/sase-org/sase-core/target"},
        {"label": "mac-capture/.build", "text": "/Users/bbugyi/projects/github/bobs-org/bob-mac-capture/.build"},
        {"label": "Chrome Memory Saver", "text": "chrome://settings/performance"}
    ]);
    let items = normalized(input);
    assert_eq!(items.len(), 8);
    assert_eq!(items[0].text.as_bytes(), b"/Users/bbugyi/.sase");
    assert_eq!(items[0].label.as_deref(), Some(".sase"));
    assert_eq!(
        items[1].text.as_bytes(),
        b"/Users/bbugyi/Library/Application Support/sase"
    );
    assert_eq!(items[2].text.as_bytes(), b"/Users/bbugyi/.cargo");
    assert_eq!(items[3].text.as_bytes(), b"/Users/bbugyi/.rustup");
    assert_eq!(
        items[4].text.as_bytes(),
        b"/Users/bbugyi/projects/github/bobs-org/bob-cli/target"
    );
    assert_eq!(
        items[5].text.as_bytes(),
        b"/Users/bbugyi/projects/github/sase-org/sase-core/target"
    );
    assert_eq!(
        items[6].text.as_bytes(),
        b"/Users/bbugyi/projects/github/bobs-org/bob-mac-capture/.build"
    );
    assert_eq!(items[7].text.as_bytes(), b"chrome://settings/performance");
    assert_eq!(items[2].label, None);
}

#[test]
fn canonicalization_is_idempotent_and_drops_absent_labels() {
    let once = normalized(json!([
        "  command\twith args\n\n",
        {"text": " value ", "label": "  Nice label  "},
        {"text": "tab\tvalue", "label": null}
    ]));
    let canonical = serde_json::to_value(&once).unwrap();
    let twice = normalized(canonical.clone());
    assert_eq!(twice, once);
    assert_eq!(canonical[0], json!({"text": "  command\twith args"}));
    assert_eq!(
        canonical[1],
        json!({"text": " value ", "label": "Nice label"})
    );
    assert_eq!(canonical[2], json!({"text": "tab\tvalue"}));
}

#[test]
fn strict_shape_and_entry_errors_have_stable_locations() {
    assert_eq!(
        error(json!(null)),
        "invalid_question_copy: copy: must be an array"
    );
    assert!(
        error(json!([4])).starts_with("invalid_question_copy: copy[0].entry:")
    );
    assert!(error(json!([{"label": "missing text"}]))
        .starts_with("invalid_question_copy: copy[0].text:"));
    assert!(error(json!([{"text": 42}]))
        .starts_with("invalid_question_copy: copy[0].text:"));
    assert!(error(json!([{"text": "valid", "value": "typo"}]))
        .starts_with("invalid_question_copy: copy[0].value:"));
    assert!(error(json!([{"text": "valid", "label": 42}]))
        .starts_with("invalid_question_copy: copy[0].label:"));
}

#[test]
fn accepts_an_empty_array_and_rejects_a_missing_text_value() {
    assert!(normalized(json!([])).is_empty());
    assert!(
        error(json!([{}])).starts_with("invalid_question_copy: copy[0].text:")
    );
}

#[test]
fn text_must_be_nonempty_and_include_non_whitespace() {
    for text in ["", "\n\n", " \t\n"] {
        assert!(error(json!([text]))
            .contains("copy[0].text: must contain a non-whitespace character"));
    }
}

#[test]
fn text_keeps_all_bytes_except_trailing_newlines() {
    let text = " \tleading and trailing \t\n\n";
    let item = normalized(json!([text])).remove(0);
    assert_eq!(item.text.as_bytes(), b" \tleading and trailing \t");
    let multiline = "line one\nline two\t\n";
    assert_eq!(
        normalized(json!([multiline]))[0].text,
        "line one\nline two\t"
    );
}

#[test]
fn text_accepts_only_newline_and_tab_controls() {
    assert_eq!(
        normalized(json!(["line\none\ttwo"]))[0].text,
        "line\none\ttwo"
    );
    for control in ['\u{0000}', '\u{001B}', '\u{000D}', '\u{007F}', '\u{0085}']
    {
        let message = error(json!([format!("a{control}b")]));
        assert!(message.starts_with("invalid_question_copy: copy[0].text:"));
        assert!(message.contains(&format!("U+{:04X}", control as u32)));
    }
}

#[test]
fn rejects_every_configured_bidi_control_in_text_and_label() {
    for codepoint in (0x202A..=0x202E).chain(0x2066..=0x2069) {
        let control = char::from_u32(codepoint).unwrap();
        assert!(error(json!([format!("left{control}right")]))
            .contains("bidi control"));
        assert!(error(
            json!([{"text": "safe", "label": format!("x{control}y")}])
        )
        .contains("bidi control"));
    }
}

#[test]
fn labels_are_trimmed_nonempty_bounded_and_single_line() {
    let label = "L".repeat(QUESTION_COPY_MAX_LABEL_CHARS);
    assert_eq!(
        normalized(json!([{"text": "value", "label": label}]))[0]
            .label
            .as_ref()
            .unwrap()
            .chars()
            .count(),
        QUESTION_COPY_MAX_LABEL_CHARS
    );
    assert!(error(json!([{"text": "value", "label": "L".repeat(QUESTION_COPY_MAX_LABEL_CHARS + 1)}]))
        .contains("maximum is 48"));
    assert!(error(json!([{"text": "value", "label": "     "}]))
        .contains("must not be empty after trimming"));
    assert!(error(json!([{"text": "value", "label": "\nlabel"}]))
        .contains("control character U+000A"));
    assert!(error(json!([{"text": "value", "label": "line\nlabel"}]))
        .contains("control character U+000A"));
}

#[test]
fn strict_copy_rejects_duplicate_normalized_text_with_both_indexes() {
    let message = error(json!(["same\n", {"text": "same", "label": "second"}]));
    assert_eq!(
        message,
        "invalid_question_copy: copy[1].text: duplicates copy[0].text"
    );
}

#[test]
fn enforces_entry_and_utf8_byte_limits() {
    let twenty = (0..QUESTION_COPY_MAX_ENTRIES)
        .map(|index| format!("item-{index}"))
        .collect::<Vec<_>>();
    assert_eq!(normalized(json!(twenty)).len(), 20);
    let twenty_one = (0..=QUESTION_COPY_MAX_ENTRIES)
        .map(|index| format!("item-{index}"))
        .collect::<Vec<_>>();
    assert!(
        error(json!(twenty_one)).contains("must contain at most 20 entries")
    );

    assert_eq!(
        normalized(json!(["x".repeat(QUESTION_COPY_MAX_TEXT_BYTES)]))[0]
            .text
            .len(),
        QUESTION_COPY_MAX_TEXT_BYTES
    );
    assert!(error(json!(["x".repeat(QUESTION_COPY_MAX_TEXT_BYTES + 1)]))
        .contains("is 4097 UTF-8 bytes; maximum is 4096"));

    let multi_byte_boundary = "é".repeat(QUESTION_COPY_MAX_TEXT_BYTES / 2);
    assert_eq!(
        normalized(json!([multi_byte_boundary]))[0].text.len(),
        QUESTION_COPY_MAX_TEXT_BYTES
    );
    let over_boundary =
        format!("{}x", "é".repeat(QUESTION_COPY_MAX_TEXT_BYTES / 2));
    assert!(error(json!([over_boundary])).contains("is 4097 UTF-8 bytes"));
}

#[test]
fn tolerant_display_keeps_valid_entries_and_counts_invalid_ones() {
    let value = json!([
        "first",
        {"text": "invalid", "name": "unknown"},
        {"text": "second", "label": "Second"},
        "first",
        {"text": "bad\u{001b}value"}
    ]);
    let display = display_question_copy(Some(&value));
    assert_eq!(display.unavailable, 2);
    assert_eq!(display.items.len(), 3);
    assert_eq!(display.items[0].text, "first");
    assert_eq!(display.items[1].text, "second");
    assert_eq!(display.items[2].text, "first");
}

#[test]
fn tolerant_display_handles_absent_null_nonarrays_and_unbounded_lists() {
    assert_eq!(display_question_copy(None).unavailable, 0);
    assert_eq!(display_question_copy(Some(&Value::Null)).unavailable, 0);
    assert_eq!(
        display_question_copy(Some(&json!("not an array"))).unavailable,
        1
    );
    let many = json!(vec!["x"; QUESTION_COPY_MAX_ENTRIES + 1]);
    let display = display_question_copy(Some(&many));
    assert_eq!(display.items.len(), QUESTION_COPY_MAX_ENTRIES + 1);
    assert_eq!(display.unavailable, 0);
}

#[test]
fn tolerant_display_counts_each_invalid_array_entry_once() {
    let value =
        json!(["good", 4, {"text": ""}, {"text": "good", "label": "\u{202E}"}]);
    let display = display_question_copy(Some(&value));
    assert_eq!(display.items.len(), 1);
    assert_eq!(display.unavailable, 3);
}

#[test]
fn found_in_text_detects_anonymized_spotlight_question_candidates() {
    let question = concat!(
        "Mac busy-process fixes are measured and recorded. ",
        "Chrome: open chrome://settings/performance. ",
        "Run `launchctl kickstart -k gui/501/com.apple/Spotlight`, ",
        "then read @plan:202611/mac_busy_process_fixes.md. ",
        "The folder /Users/person/Library/Application Support/app is prose."
    );
    let candidates = detect_question_copy_candidates(
        question,
        &["chrome://settings/performance".to_string()],
    );
    assert_eq!(candidates.len(), 2);
    assert_eq!(
        candidates[0].text,
        "launchctl kickstart -k gui/501/com.apple/Spotlight"
    );
    assert_eq!(candidates[0].kind, QuestionCopyCandidateKind::Code);
    assert_eq!(candidates[1].text, "plan:202611/mac_busy_process_fixes.md");
    assert_eq!(candidates[1].kind, QuestionCopyCandidateKind::Ref);
}

#[test]
fn found_in_text_extracts_fences_and_filters_inline_code_noise() {
    let question = concat!(
        "Short `x` is noise; `safe/path`, `v1.2`, and `--verbose` are useful.\n",
        "```sh\nlaunchctl kickstart -k gui/501/example\n\n```\n",
        "A sufficiently long identifier is also retained: `abcdefghijkL`."
    );
    let candidates = detect_question_copy_candidates(question, &[]);
    assert_eq!(
        candidates
            .iter()
            .map(|item| item.text.as_str())
            .collect::<Vec<_>>(),
        [
            "safe/path",
            "v1.2",
            "--verbose",
            "launchctl kickstart -k gui/501/example",
            "abcdefghijkL"
        ]
    );
    assert!(candidates
        .iter()
        .all(|item| item.kind == QuestionCopyCandidateKind::Code));
}

#[test]
fn found_in_text_trims_url_punctuation_and_keeps_balanced_parentheses() {
    let candidates = detect_question_copy_candidates(
        "Open file://host/path_(guide). Then chrome://settings/performance,",
        &[],
    );
    assert_eq!(candidates.len(), 2);
    assert_eq!(candidates[0].text, "file://host/path_(guide)");
    assert_eq!(candidates[1].text, "chrome://settings/performance");
    assert!(candidates
        .iter()
        .all(|item| item.kind == QuestionCopyCandidateKind::Url));
}

#[test]
fn found_in_text_skips_unsafe_values_and_deduplicates_before_capping() {
    let unsafe_value = format!("```\nunsafe\u{001b}value\n```\n");
    let question = format!(
        "{unsafe_value}`--one` `--one` `--two` `--three` `--four` `--five` `--six`"
    );
    let candidates = detect_question_copy_candidates(&question, &[]);
    assert_eq!(
        candidates
            .iter()
            .map(|item| item.text.as_str())
            .collect::<Vec<_>>(),
        ["--one", "--two", "--three", "--four", "--five"]
    );
}

#[test]
fn found_in_text_only_detects_valid_artifact_refs_and_does_not_guess_paths() {
    let candidates = detect_question_copy_candidates(
        "See plan:202611/example.md and plan: . /Users/person/.cache stays prose.",
        &[],
    );
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].text, "plan:202611/example.md");
    assert_eq!(candidates[0].kind, QuestionCopyCandidateKind::Ref);
}
