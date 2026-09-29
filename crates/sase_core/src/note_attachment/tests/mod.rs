//! Unit and corpus tests for note attachments.

mod manifest;

use crate::note_attachment::NOTE_ATTACHMENT_SCAN_WIRE_SCHEMA_VERSION;
use crate::note_attachment::{
    classify_attachment, extension_mime_for, AttachmentClassWire,
};
use crate::note_attachment::{
    compose_note_attachment_text, note_attachment_source_text,
    scan_note_attachment_refs, stored_attachment_tokens,
    NoteAttachmentReuseBindingWire,
};
use crate::note_attachment::{
    sanitize_attachment_name, unique_attachment_name, AttachmentNameDigestWire,
};

fn roster(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_string()).collect()
}

fn existing(pairs: &[(&str, &str)]) -> Vec<AttachmentNameDigestWire> {
    pairs
        .iter()
        .map(|(name, sha)| AttachmentNameDigestWire {
            name: (*name).to_string(),
            sha256: (*sha).to_string(),
        })
        .collect()
}

#[test]
fn scan_wire_schema_version_is_one() {
    assert_eq!(NOTE_ATTACHMENT_SCAN_WIRE_SCHEMA_VERSION, 1);
    let scan = scan_note_attachment_refs("hello", &[]);
    assert_eq!(scan.schema_version, 1);
}

#[test]
fn byte_offsets_use_utf8_for_multibyte_prefix() {
    // `é` is two bytes: `@` sits at byte 3.
    let scan = scan_note_attachment_refs("é @./a.png", &[]);
    assert_eq!(scan.path_refs.len(), 1);
    assert_eq!(scan.path_refs[0].span.start, 3);
    assert_eq!(scan.path_refs[0].path, "./a.png");
}

#[test]
fn sanitize_collapses_and_preserves_extension() {
    assert_eq!(
        sanitize_attachment_name("My Screenshot (1).PNG"),
        "My_Screenshot_1.PNG"
    );
    assert_eq!(sanitize_attachment_name(""), "attachment");
    assert_eq!(sanitize_attachment_name("___"), "attachment");
    assert_eq!(sanitize_attachment_name(".hidden"), "hidden");
    assert_eq!(sanitize_attachment_name("-dash"), "dash");
}

#[test]
fn sanitize_truncates_long_names() {
    let long = format!("{}.png", "a".repeat(100));
    let sanitized = sanitize_attachment_name(&long);
    assert!(sanitized.len() <= 96);
    assert!(sanitized.ends_with(".png"));
}

#[test]
fn uniquify_reuses_same_digest_and_bumps_on_conflict() {
    let same = existing(&[("login.png", &"a".repeat(64))]);
    assert_eq!(
        unique_attachment_name("login.png", &"A".repeat(64), &same),
        "login.png"
    );
    let different = existing(&[("login.png", &"b".repeat(64))]);
    assert_eq!(
        unique_attachment_name("login.png", &"a".repeat(64), &different),
        "login-2.png"
    );
    assert_eq!(
        unique_attachment_name("login.png", &"a".repeat(64), &[]),
        "login.png"
    );
}

#[test]
fn classify_magic_wins_over_extension() {
    let png = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
    let classification = classify_attachment("note.txt", &png);
    assert_eq!(classification.mime_type, "image/png");
    assert_eq!(classification.class, AttachmentClassWire::Image);
}

#[test]
fn classify_extension_table_and_fallbacks() {
    let text = classify_attachment("AGENTS.md", b"hello");
    assert_eq!(text.mime_type, "text/markdown");
    assert_eq!(text.class, AttachmentClassWire::Text);
    let svg = classify_attachment("icon.svg", b"<svg/>");
    assert_eq!(svg.mime_type, "image/svg+xml");
    assert_eq!(svg.class, AttachmentClassWire::Image);
    let empty_unknown = classify_attachment("file.unknownxyz", b"");
    assert_eq!(empty_unknown.mime_type, "text/plain");
    let binary = classify_attachment("file.unknownxyz", &[0, 1, 2, 255]);
    assert_eq!(binary.mime_type, "application/octet-stream");
    assert_eq!(binary.class, AttachmentClassWire::Binary);
    assert!(extension_mime_for("md").is_some());
    assert!(extension_mime_for("asyncio").is_none());
}

#[test]
fn oracle_table() {
    // No record: mid-word @.
    let scan = scan_note_attachment_refs("me@host", &[]);
    assert!(scan.path_refs.is_empty());
    assert!(scan.reuse_refs.is_empty());
    assert!(scan.escapes.is_empty());
    assert!(scan.bare_words.is_empty());
    assert!(scan.diagnostics.is_empty());

    for (source, word) in [
        ("@large", "large"),
        ("@dataclass", "dataclass"),
        ("@pytest.mark.asyncio", "pytest.mark.asyncio"),
    ] {
        let scan = scan_note_attachment_refs(source, &[]);
        assert_eq!(scan.bare_words.len(), 1, "{source}");
        assert_eq!(scan.bare_words[0].word, word, "{source}");
        assert!(scan.path_refs.is_empty(), "{source}");
    }

    for source in ["@research:x", "@plan:202609/bead_note_attachments.md"] {
        let scan = scan_note_attachment_refs(source, &[]);
        assert!(scan.path_refs.is_empty(), "{source}");
        assert!(scan.reuse_refs.is_empty(), "{source}");
        assert!(scan.bare_words.is_empty(), "{source}");
        assert!(scan.diagnostics.is_empty(), "{source}");
    }

    let scan = scan_note_attachment_refs("@@./x", &[]);
    assert_eq!(scan.escapes.len(), 1);
    assert_eq!(
        compose_note_attachment_text("@@./x", &scan, &[]).unwrap(),
        "@./x"
    );

    let scan = scan_note_attachment_refs("@@@x", &[]);
    assert_eq!(scan.escapes.len(), 1);
    assert_eq!(scan.escapes[0].start, 0);
    assert_eq!(scan.escapes[0].end, 2);
    assert_eq!(
        compose_note_attachment_text("@@@x", &scan, &[]).unwrap(),
        "@@x"
    );

    let scan = scan_note_attachment_refs("`@./x.png`", &[]);
    assert!(scan.path_refs.is_empty());
    assert!(scan.escapes.is_empty());

    let scan = scan_note_attachment_refs("```\n@@\n@./x\n```\n", &[]);
    assert!(scan.path_refs.is_empty());
    assert!(scan.escapes.is_empty());

    let scan = scan_note_attachment_refs("@\"a b.png\"", &[]);
    assert_eq!(scan.path_refs.len(), 1);
    assert_eq!(scan.path_refs[0].path, "a b.png");
    assert!(scan.path_refs[0].quoted);

    let scan = scan_note_attachment_refs("@./x.png,", &[]);
    assert_eq!(scan.path_refs.len(), 1);
    assert_eq!(scan.path_refs[0].path, "./x.png");
    assert_eq!(scan.path_refs[0].span.end, 8);

    let scan = scan_note_attachment_refs("(@./x.png)", &[]);
    assert_eq!(scan.path_refs.len(), 1);
    assert_eq!(scan.path_refs[0].path, "./x.png");

    let scan = scan_note_attachment_refs("![t](@./t.log)", &[]);
    assert_eq!(scan.path_refs.len(), 1);
    assert_eq!(scan.path_refs[0].path, "./t.log");

    let scan = scan_note_attachment_refs("@AGENTS.md", &[]);
    assert_eq!(scan.path_refs.len(), 1);
    assert_eq!(scan.path_refs[0].path, "AGENTS.md");

    for source in ["@/tmp/a.png", "@~/logs/a.log", "@../x", "@docs/plan.md"] {
        let scan = scan_note_attachment_refs(source, &[]);
        assert_eq!(scan.path_refs.len(), 1, "{source}");
    }

    let scan = scan_note_attachment_refs(
        "@attachment:login.png",
        &roster(&["login.png"]),
    );
    assert_eq!(scan.reuse_refs.len(), 1);
    assert_eq!(
        scan.reuse_refs[0].binding,
        NoteAttachmentReuseBindingWire::Roster
    );

    let scan = scan_note_attachment_refs("@attachment:login.png", &[]);
    assert_eq!(scan.reuse_refs.len(), 1);
    assert_eq!(
        scan.reuse_refs[0].binding,
        NoteAttachmentReuseBindingWire::Unknown
    );
    assert_eq!(scan.diagnostics.len(), 1);
    assert_eq!(scan.diagnostics[0].code, "unknown_attachment");

    let scan = scan_note_attachment_refs(
        "@./shots/login.png and @attachment:login.png",
        &[],
    );
    assert_eq!(scan.path_refs.len(), 1);
    assert_eq!(scan.reuse_refs.len(), 1);
    assert_eq!(
        scan.reuse_refs[0].binding,
        NoteAttachmentReuseBindingWire::EarlierPath
    );
    assert_eq!(scan.reuse_refs[0].earlier_path_index, Some(0));
    assert!(scan.diagnostics.is_empty());
    let composed = compose_note_attachment_text(
        "@./shots/login.png and @attachment:login.png",
        &scan,
        &["login.png".to_string()],
    )
    .unwrap();
    assert_eq!(composed, "@attachment:login.png and @attachment:login.png");

    let scan = scan_note_attachment_refs("@\"\"", &[]);
    assert_eq!(scan.diagnostics.len(), 1);
    assert_eq!(scan.diagnostics[0].code, "empty_path");

    let scan = scan_note_attachment_refs("@\"", &[]);
    assert_eq!(scan.diagnostics.len(), 1);
    assert_eq!(scan.diagnostics[0].code, "unterminated_quote");

    let scan = scan_note_attachment_refs("@attachment:sase-1ck/login.png", &[]);
    assert!(scan.path_refs.is_empty());
    assert!(scan.reuse_refs.is_empty());

    // Compose retargets earlier_path reuses through uniquify.
    let scan = scan_note_attachment_refs(
        "@./shots/login.png and @attachment:login.png",
        &[],
    );
    let composed = compose_note_attachment_text(
        "@./shots/login.png and @attachment:login.png",
        &scan,
        &["login-2.png".to_string()],
    )
    .unwrap();
    assert_eq!(
        composed,
        "@attachment:login-2.png and @attachment:login-2.png"
    );

    // Compose of a path replaces the span and copies punctuation.
    let scan = scan_note_attachment_refs("@./x.png,", &[]);
    let composed = compose_note_attachment_text(
        "@./x.png,",
        &scan,
        &["login.png".to_string()],
    )
    .unwrap();
    assert_eq!(composed, "@attachment:login.png,");

    // Assigned-name validation.
    let scan = scan_note_attachment_refs("@./x.png", &[]);
    assert!(compose_note_attachment_text(
        "@./x.png",
        &scan,
        &["login.png".to_string(), "extra".to_string()]
    )
    .is_err());
    assert!(compose_note_attachment_text(
        "@./x.png",
        &scan,
        &["bad name!".to_string()]
    )
    .is_err());
}

#[test]
fn inverse_property_holds_for_hand_cases() {
    for (stored, manifest) in [
        ("hello", vec![]),
        ("@attachment:login.png", vec!["login.png"]),
        ("@./x.png", vec![]),
        ("@@./x", vec![]),
    ] {
        let manifest_names: Vec<String> =
            manifest.iter().map(|name| (*name).to_string()).collect();
        let source = note_attachment_source_text(stored, &manifest_names);
        let scan = scan_note_attachment_refs(&source, &manifest_names);
        assert!(scan.path_refs.is_empty(), "{stored:?} -> {source:?}");
        assert!(scan.diagnostics.is_empty(), "{stored:?} -> {source:?}");
        let roundtrip =
            compose_note_attachment_text(&source, &scan, &[]).unwrap();
        assert_eq!(roundtrip, stored);
    }
}

fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

#[test]
fn inverse_property_holds_for_seeded_corpus() {
    let fragments = [
        "hello ",
        "me@host ",
        "@large ",
        "@@ ",
        "@./x.png ",
        "@AGENTS.md ",
        "@\"a b.png\" ",
        "@attachment:login.png ",
        "@attachment:sase-1ck/login.png ",
        "@plan:202609/bead_note_attachments.md ",
        "@research:x ",
        "```\n@@\n@./x\n```\n",
        "`@./x.png` ",
        "@\"\" ",
        "@\"unterminated\n",
        "é ",
        "\n",
        "@docs/plan.md ",
        "@~/logs/a.log ",
        "(@./x.png) ",
    ];
    let manifests: Vec<Vec<String>> = vec![
        vec![],
        vec!["login.png".to_string()],
        vec!["a.png".to_string(), "b.png".to_string()],
    ];
    let mut state: u64 = 0x1234_5678_9ABC_DEF1;
    for case in 0..256 {
        let manifest = &manifests[case % manifests.len()];
        let count = 1 + (xorshift64(&mut state) % 5) as usize;
        let mut stored = String::new();
        for _ in 0..count {
            let index =
                (xorshift64(&mut state) % fragments.len() as u64) as usize;
            stored.push_str(fragments[index]);
        }
        let source = note_attachment_source_text(&stored, manifest);
        let scan = scan_note_attachment_refs(&source, manifest);
        assert!(
            scan.path_refs.is_empty(),
            "case {case}: {stored:?} -> {source:?}"
        );
        assert!(
            scan.diagnostics.is_empty(),
            "case {case}: {stored:?} -> {source:?} {scan:?}"
        );
        let roundtrip =
            compose_note_attachment_text(&source, &scan, &[]).unwrap();
        assert_eq!(roundtrip, stored, "case {case}");
    }
}

#[test]
fn stored_tokens_match_predicate_outside_literal_zones() {
    let tokens = stored_attachment_tokens("@attachment:login.png");
    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].name, "login.png");
    let code = stored_attachment_tokens("`@attachment:login.png`");
    assert!(code.is_empty());
    let trailing = stored_attachment_tokens("@attachment:login.png,");
    assert_eq!(trailing.len(), 1);
    assert_eq!(trailing[0].name, "login.png");
}

#[test]
fn corpus_golden_matches_fixtures() {
    let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/note_attachment/at_bearing_notes.jsonl");
    let summary_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/note_attachment/path_or_diagnostic.json");
    if !manifest.exists() || !summary_path.exists() {
        return;
    }
    let content = std::fs::read_to_string(&manifest).unwrap();
    let lines: Vec<&str> = content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    assert!(!lines.is_empty());
    let mut texts = Vec::new();
    for line in &lines {
        let text: String = serde_json::from_str(line).unwrap();
        texts.push(text);
    }
    let summary_content = std::fs::read_to_string(&summary_path).unwrap();
    let summary: Vec<serde_json::Value> =
        serde_json::from_str(&summary_content).unwrap();
    let mut expected = Vec::new();
    for text in &texts {
        let scan = scan_note_attachment_refs(text, &[]);
        if !scan.path_refs.is_empty() || !scan.diagnostics.is_empty() {
            let digest = sha256_hex(text.as_bytes());
            let mut codes: Vec<String> = scan
                .diagnostics
                .iter()
                .map(|diagnostic| diagnostic.code.clone())
                .collect();
            codes.sort();
            codes.dedup();
            expected.push(serde_json::json!({
                "sha256": digest,
                "path_ref_count": scan.path_refs.len(),
                "diagnostic_codes": codes,
            }));
        }
    }
    expected.sort_by(|left, right| {
        left["sha256"].as_str().cmp(&right["sha256"].as_str())
    });
    assert_eq!(
        lines.len(),
        texts.len(),
        "fixture line count must match parsed count"
    );
    assert_eq!(summary, expected);
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(data))
}
