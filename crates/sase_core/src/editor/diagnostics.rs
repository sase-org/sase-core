use regex::Regex;
use std::collections::HashSet;
use std::sync::OnceLock;

use super::directive::canonical_directive_name;
use super::frontmatter;
use super::token::DocumentSnapshot;
use super::wire::{
    DiagnosticSeverity, EditorDiagnostic, XpromptAssistEntry, XpromptInputHint,
};
use super::xprompt_args::{
    parse_xprompt_calls, ParsedXpromptArg, XpromptArgSyntax,
};

pub fn analyze_document(
    document: &DocumentSnapshot,
    entries: &[XpromptAssistEntry],
) -> Vec<EditorDiagnostic> {
    let mut diagnostics = Vec::new();
    diagnostics.extend(frontmatter::diagnostics(document));
    diagnostics.extend(xprompt_diagnostics(document, entries));
    diagnostics.extend(slash_skill_diagnostics(document, entries));
    diagnostics.extend(directive_diagnostics(document));
    diagnostics.extend(argument_diagnostics(document, entries));
    diagnostics
}

fn xprompt_diagnostics(
    document: &DocumentSnapshot,
    entries: &[XpromptAssistEntry],
) -> Vec<EditorDiagnostic> {
    let mut out = Vec::new();
    for caps in xprompt_ref_re().captures_iter(document.text()) {
        let Some(marker) = caps.name("marker") else {
            continue;
        };
        let Some(name_match) = caps.name("name") else {
            continue;
        };
        let name = name_match.as_str().replace("__", "/");
        let Some(entry) = entries.iter().find(|entry| entry.name == name)
        else {
            if let Some(range) =
                document.byte_range_to_range(marker.start(), name_match.end())
            {
                out.push(EditorDiagnostic {
                    range,
                    severity: DiagnosticSeverity::Warning,
                    code: "unknown_xprompt".to_string(),
                    message: format!("Unknown xprompt `{name}`"),
                });
            }
            continue;
        };
        if entry.reference_prefix != marker.as_str() {
            if let Some(range) =
                document.byte_range_to_range(marker.start(), marker.end())
            {
                out.push(EditorDiagnostic {
                    range,
                    severity: DiagnosticSeverity::Information,
                    code: "canonical_marker_mismatch".to_string(),
                    message: format!(
                        "`{}` is canonical for `{}`",
                        entry.reference_prefix, entry.name
                    ),
                });
            }
        }
    }
    out
}

fn slash_skill_diagnostics(
    document: &DocumentSnapshot,
    entries: &[XpromptAssistEntry],
) -> Vec<EditorDiagnostic> {
    let mut out = Vec::new();
    for caps in slash_skill_re().captures_iter(document.text()) {
        let Some(skill) = caps.name("skill") else {
            continue;
        };
        if entries
            .iter()
            .any(|entry| entry.is_skill && entry.name == skill.as_str())
        {
            continue;
        }
        if let Some(range) =
            document.byte_range_to_range(skill.start() - 1, skill.end())
        {
            out.push(EditorDiagnostic {
                range,
                severity: DiagnosticSeverity::Warning,
                code: "unknown_slash_skill".to_string(),
                message: format!("Unknown slash skill `/{}`", skill.as_str()),
            });
        }
    }
    out
}

fn directive_diagnostics(document: &DocumentSnapshot) -> Vec<EditorDiagnostic> {
    let mut out = Vec::new();
    for caps in directive_re().captures_iter(document.text()) {
        let Some(name) = caps.name("name") else {
            continue;
        };
        if canonical_directive_name(name.as_str()).is_some() {
            continue;
        }
        if let Some(range) =
            document.byte_range_to_range(name.start() - 1, name.end())
        {
            out.push(EditorDiagnostic {
                range,
                severity: DiagnosticSeverity::Information,
                code: "unknown_directive".to_string(),
                message: format!("Unknown directive `%{}`", name.as_str()),
            });
        }
    }
    out
}

fn argument_diagnostics(
    document: &DocumentSnapshot,
    entries: &[XpromptAssistEntry],
) -> Vec<EditorDiagnostic> {
    let mut out = Vec::new();
    for call in parse_xprompt_calls(document.text()) {
        let Some(entry) = entries.iter().find(|entry| entry.name == call.name)
        else {
            continue;
        };
        if matches!(call.syntax, XpromptArgSyntax::Malformed) {
            let Some((start, end)) = call.malformed_span else {
                continue;
            };
            push_diagnostic(
                document,
                &mut out,
                start,
                end,
                "malformed_xprompt_argument",
                "Malformed xprompt argument form".to_string(),
            );
            if entry.inputs.is_empty() || call.is_open {
                continue;
            }
        }
        if call.is_open || entry.inputs.is_empty() {
            continue;
        }
        validate_call_args(document, entry, &call, &mut out);
    }
    out
}

fn validate_call_args(
    document: &DocumentSnapshot,
    entry: &XpromptAssistEntry,
    call: &super::xprompt_args::ParsedXpromptCall,
    out: &mut Vec<EditorDiagnostic>,
) {
    let mut supplied_inputs = HashSet::new();
    let mut positional_inputs = HashSet::new();
    let mut seen_named_args = HashSet::new();
    let mut positional_index = 0usize;

    for arg in &call.args {
        if let Some(name) = &arg.name {
            if !seen_named_args.insert(name.value.clone()) {
                push_diagnostic(
                    document,
                    out,
                    name.span.0,
                    name.span.1,
                    "duplicate_xprompt_arg",
                    format!("Duplicate xprompt argument `{}`", name.value),
                );
                continue;
            }
            let Some(input) =
                entry.inputs.iter().find(|input| input.name == name.value)
            else {
                push_diagnostic(
                    document,
                    out,
                    name.span.0,
                    name.span.1,
                    "unknown_xprompt_arg",
                    format!(
                        "Unknown argument `{}` for xprompt `{}`",
                        name.value, entry.name
                    ),
                );
                continue;
            };
            if positional_inputs.contains(&input.name) {
                push_diagnostic(
                    document,
                    out,
                    name.span.0,
                    name.span.1,
                    "conflicting_xprompt_arg",
                    format!(
                        "Argument `{}` was already provided positionally",
                        input.name
                    ),
                );
            }
            supplied_inputs.insert(input.name.clone());
            validate_type(document, entry, input, arg, out);
        } else {
            let Some(input) = entry.inputs.get(positional_index) else {
                push_diagnostic(
                    document,
                    out,
                    arg.value_span.0,
                    arg.value_span.1,
                    "too_many_args",
                    format!(
                        "Too many positional arguments for `{}`",
                        entry.name
                    ),
                );
                positional_index += 1;
                continue;
            };
            positional_inputs.insert(input.name.clone());
            supplied_inputs.insert(input.name.clone());
            validate_type(document, entry, input, arg, out);
            positional_index += 1;
        }
    }

    for input in entry.inputs.iter().filter(|input| input.required) {
        if supplied_inputs.contains(&input.name) {
            continue;
        }
        push_diagnostic(
            document,
            out,
            call.name_span.0,
            call.name_span.1,
            "missing_required_arg",
            format!(
                "Missing required argument `{}` for xprompt `{}`",
                input.name, entry.name
            ),
        );
    }
}

fn validate_type(
    document: &DocumentSnapshot,
    entry: &XpromptAssistEntry,
    input: &XpromptInputHint,
    arg: &ParsedXpromptArg,
    out: &mut Vec<EditorDiagnostic>,
) {
    if arg.value == "null" || value_matches_input_type(&arg.value, input) {
        return;
    }
    push_diagnostic(
        document,
        out,
        arg.value_span.0,
        arg.value_span.1,
        "invalid_xprompt_arg_type",
        format!(
            "Argument `{}` for xprompt `{}` expects {}",
            input.name, entry.name, input.r#type
        ),
    );
}

fn value_matches_input_type(value: &str, input: &XpromptInputHint) -> bool {
    match input.r#type.as_str() {
        "word" | "path" => !value.chars().any(char::is_whitespace),
        "line" => !value.contains('\n'),
        "text" => true,
        "int" | "integer" => value.parse::<i64>().is_ok(),
        "float" => value.parse::<f64>().is_ok(),
        "bool" | "boolean" => matches!(
            value.to_ascii_lowercase().as_str(),
            "true" | "1" | "yes" | "on" | "false" | "0" | "no" | "off"
        ),
        _ => true,
    }
}

fn push_diagnostic(
    document: &DocumentSnapshot,
    out: &mut Vec<EditorDiagnostic>,
    start: usize,
    end: usize,
    code: &str,
    message: String,
) {
    let Some(range) = document.byte_range_to_range(start, end) else {
        return;
    };
    out.push(EditorDiagnostic {
        range,
        severity: DiagnosticSeverity::Error,
        code: code.to_string(),
        message,
    });
}

fn xprompt_ref_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?m)(?:^|[\s\(\[\{"'])(?P<marker>#!|#)(?P<name>[A-Za-z_][A-Za-z0-9_]*(?:(?:/|__)[A-Za-z_][A-Za-z0-9_]*)*)(?:!!|\?\?)?"#,
        )
        .unwrap()
    })
}

fn slash_skill_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?m)(?:^|\s)/(?P<skill>[A-Za-z0-9_]+)(?:\s|$)").unwrap()
    })
}

fn directive_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?m)(?:^|[\s\(\[\{"'])(?:%(?P<name>[A-Za-z_][A-Za-z0-9_]*))"#,
        )
        .unwrap()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog() -> Vec<XpromptAssistEntry> {
        vec![
            XpromptAssistEntry {
                name: "review".to_string(),
                display_label: "review".to_string(),
                insertion: "#review".to_string(),
                reference_prefix: "#".to_string(),
                kind: None,
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: vec![],
                content_preview: None,
                description: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
                is_skill: false,
            },
            XpromptAssistEntry {
                name: "run".to_string(),
                display_label: "run".to_string(),
                insertion: "#!run".to_string(),
                reference_prefix: "#!".to_string(),
                kind: None,
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: vec![],
                content_preview: None,
                description: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
                is_skill: true,
            },
            XpromptAssistEntry {
                name: "typed".to_string(),
                display_label: "typed".to_string(),
                insertion: "#typed".to_string(),
                reference_prefix: "#".to_string(),
                kind: None,
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: vec![
                    input("path", "path", true, 0),
                    input("count", "int", true, 1),
                    input("enabled", "bool", false, 2),
                ],
                content_preview: None,
                description: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
                is_skill: false,
            },
            XpromptAssistEntry {
                name: "ns/foo".to_string(),
                display_label: "ns/foo".to_string(),
                insertion: "#ns/foo".to_string(),
                reference_prefix: "#".to_string(),
                kind: None,
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: vec![input("arg", "word", true, 0)],
                content_preview: None,
                description: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
                is_skill: false,
            },
        ]
    }

    fn input(
        name: &str,
        r#type: &str,
        required: bool,
        position: u32,
    ) -> XpromptInputHint {
        XpromptInputHint {
            name: name.to_string(),
            r#type: r#type.to_string(),
            description: None,
            required,
            default_display: None,
            position,
        }
    }

    fn diagnostic_text(text: &str, diagnostic: &EditorDiagnostic) -> String {
        let doc = DocumentSnapshot::new(text);
        let start =
            doc.position_to_byte_offset(diagnostic.range.start).unwrap();
        let end = doc.position_to_byte_offset(diagnostic.range.end).unwrap();
        text[start..end].to_string()
    }

    fn diagnostics_for(text: &str) -> Vec<EditorDiagnostic> {
        analyze_document(&DocumentSnapshot::new(text), &catalog())
    }

    fn diagnostic<'a>(
        diagnostics: &'a [EditorDiagnostic],
        code: &str,
    ) -> &'a EditorDiagnostic {
        diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == code)
            .unwrap_or_else(|| panic!("missing {code}: {diagnostics:?}"))
    }

    fn diagnostic_count(diagnostics: &[EditorDiagnostic], code: &str) -> usize {
        diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.code == code)
            .count()
    }

    #[test]
    fn reports_initial_diagnostics() {
        let doc = DocumentSnapshot::new("#missing #run /missing %wat");
        let diagnostics = analyze_document(&doc, &catalog());
        assert!(diagnostics.iter().any(|d| d.code == "unknown_xprompt"));
        assert!(diagnostics
            .iter()
            .any(|d| d.code == "canonical_marker_mismatch"));
        assert!(diagnostics.iter().any(|d| d.code == "unknown_slash_skill"));
        assert!(diagnostics.iter().any(|d| d.code == "unknown_directive"));
    }

    #[test]
    fn reports_xprompt_argument_contract_diagnostics() {
        for (text, code) in [
            ("#typed", "missing_required_arg"),
            ("#typed(src/main.rs)", "missing_required_arg"),
            ("#typed(src/main.rs, 3, true, extra)", "too_many_args"),
            (
                "#typed(path=src/main.rs, nope=1, count=3)",
                "unknown_xprompt_arg",
            ),
            ("#typed(path=a, path=b, count=3)", "duplicate_xprompt_arg"),
            (
                "#typed(src/main.rs, path=other, count=3)",
                "conflicting_xprompt_arg",
            ),
            (
                "#typed(path=\"bad value\", count=3)",
                "invalid_xprompt_arg_type",
            ),
            (
                "#typed(path=src/main.rs, count=nope)",
                "invalid_xprompt_arg_type",
            ),
            ("#typed:path+ ", "malformed_xprompt_argument"),
        ] {
            let doc = DocumentSnapshot::new(text);
            let diagnostics = analyze_document(&doc, &catalog());
            assert!(
                diagnostics.iter().any(|d| d.code == code),
                "{text}: {diagnostics:?}"
            );
        }
    }

    #[test]
    fn reports_shortform_frontmatter_input_type_diagnostic() {
        let text = "---\ninput:\n  name: wordd\n---\nBody";
        let doc = DocumentSnapshot::new(text);
        let diagnostics = analyze_document(&doc, &catalog());
        let diagnostic = diagnostics
            .iter()
            .find(|diagnostic| {
                diagnostic.code == "invalid_xprompt_frontmatter_input_type"
            })
            .unwrap();

        assert_eq!(diagnostic.severity, DiagnosticSeverity::Error);
        assert_eq!(diagnostic_text(text, diagnostic), "wordd");
    }

    #[test]
    fn accepts_known_frontmatter_input_type_aliases() {
        let text = "---\ninput:\n  a: word\n  b: line\n  c: text\n  d: path\n  e: int\n  f: integer\n  g: bool\n  h: boolean\n  i: float\n---\nBody";
        let doc = DocumentSnapshot::new(text);
        let diagnostics = analyze_document(&doc, &catalog());

        assert!(
            !diagnostics.iter().any(|diagnostic| {
                diagnostic.code == "invalid_xprompt_frontmatter_input_type"
            }),
            "{diagnostics:?}"
        );
    }

    #[test]
    fn reports_longform_frontmatter_input_type_diagnostic() {
        let text = "---\ninput:\n  - name: foo\n    type: wordd\n---\nBody";
        let doc = DocumentSnapshot::new(text);
        let diagnostics = analyze_document(&doc, &catalog());
        let diagnostic = diagnostics
            .iter()
            .find(|diagnostic| {
                diagnostic.code == "invalid_xprompt_frontmatter_input_type"
            })
            .unwrap();

        assert_eq!(diagnostic_text(text, diagnostic), "wordd");
    }

    #[test]
    fn reports_flow_style_frontmatter_input_type_diagnostic() {
        let text = "---\ninput: {name: wordd}\n---\nBody";
        let doc = DocumentSnapshot::new(text);
        let diagnostics = analyze_document(&doc, &catalog());
        let diagnostic = diagnostics
            .iter()
            .find(|diagnostic| {
                diagnostic.code == "invalid_xprompt_frontmatter_input_type"
            })
            .unwrap();

        assert_eq!(diagnostic_text(text, diagnostic), "wordd");
    }

    #[test]
    fn reports_frontmatter_yaml_and_shape_diagnostics() {
        let diagnostics = diagnostics_for("---\ninput: [\n---\nBody");
        let yaml_diagnostic =
            diagnostic(&diagnostics, "invalid_xprompt_frontmatter_yaml");
        assert_eq!(yaml_diagnostic.severity, DiagnosticSeverity::Error);

        let diagnostics = diagnostics_for("---\n[not, mapping]\n---\nBody");
        let shape_diagnostic =
            diagnostic(&diagnostics, "invalid_xprompt_frontmatter_shape");
        assert_eq!(shape_diagnostic.severity, DiagnosticSeverity::Error);

        let diagnostics = diagnostics_for("input:\n  name: wordd\nBody");
        assert!(
            diagnostics
                .iter()
                .all(|diagnostic| !diagnostic.code.contains("frontmatter")),
            "{diagnostics:?}"
        );
    }

    #[test]
    fn reports_unknown_top_level_and_invalid_name() {
        let diagnostics =
            diagnostics_for("---\nname: bad-name\nowner: me\n---\nBody");
        assert_eq!(
            diagnostic(&diagnostics, "unknown_xprompt_frontmatter_field")
                .severity,
            DiagnosticSeverity::Information
        );
        assert_eq!(
            diagnostic(
                &diagnostics,
                "unreferenceable_xprompt_frontmatter_name"
            )
            .severity,
            DiagnosticSeverity::Warning
        );

        let diagnostics = diagnostics_for("---\nname: []\n---\nBody");
        assert_eq!(
            diagnostic(&diagnostics, "invalid_xprompt_frontmatter_name")
                .severity,
            DiagnosticSeverity::Error
        );
    }

    #[test]
    fn accepts_frontmatter_local_xprompts() {
        let text = "---\ndescription: Example\ninput:\n  topic: text\nxprompts:\n  _helper:\n    content: Helper {{ topic }}\n---\nBody";
        let diagnostics = diagnostics_for(text);

        assert!(
            diagnostics.iter().all(|diagnostic| diagnostic.code
                != "unknown_xprompt_frontmatter_field"),
            "{diagnostics:?}"
        );
    }

    #[test]
    fn reports_input_shape_name_duplicate_identifier_and_unknown_fields() {
        for (text, code) in [
            (
                "---\ninput: nope\n---\nBody",
                "invalid_xprompt_frontmatter_input_shape",
            ),
            (
                "---\ninput:\n  - type: word\n---\nBody",
                "invalid_xprompt_frontmatter_input_name",
            ),
            (
                "---\ninput:\n  - name: target\n  - name: target\n---\nBody",
                "duplicate_xprompt_frontmatter_input",
            ),
            (
                "---\ninput:\n  bad-name: word\n---\nBody",
                "invalid_xprompt_frontmatter_input_identifier",
            ),
            (
                "---\ninput:\n  target:\n    type: word\n    extra: ignored\n---\nBody",
                "unknown_xprompt_frontmatter_input_field",
            ),
        ] {
            let diagnostics = diagnostics_for(text);
            assert!(
                diagnostics.iter().any(|diagnostic| diagnostic.code == code),
                "{text}: {diagnostics:?}"
            );
        }
    }

    #[test]
    fn reports_invalid_input_defaults() {
        let text = "---\ninput:\n  wordy:\n    type: word\n    default: \"two words\"\n  count:\n    type: int\n    default: nope\n  ratio:\n    type: float\n    default: nope\n  enabled:\n    type: bool\n    default: maybe\n---\nBody";
        let diagnostics = diagnostics_for(text);

        assert_eq!(
            diagnostic_count(
                &diagnostics,
                "invalid_xprompt_frontmatter_input_default"
            ),
            4,
            "{diagnostics:?}"
        );
    }

    #[test]
    fn accepts_valid_input_aliases_and_defaults() {
        let text = "---\ninput:\n  a:\n    type: word\n    default: docs\n  b:\n    type: path\n    default: src/main.rs\n  c:\n    type: line\n    default: hello world\n  d:\n    type: text\n    default: |\n      hello\n      world\n  e:\n    type: integer\n    default: 3\n  f:\n    type: boolean\n    default: true\n  g:\n    type: float\n    default: 3.5\n  h:\n    type: int\n    default:\n---\nBody";
        let diagnostics = diagnostics_for(text);

        assert!(
            diagnostics.iter().all(|diagnostic| {
                diagnostic.code != "invalid_xprompt_frontmatter_input_type"
                    && diagnostic.code
                        != "invalid_xprompt_frontmatter_input_default"
            }),
            "{diagnostics:?}"
        );
    }

    #[test]
    fn accepts_input_descriptions_and_reports_invalid_shapes() {
        for valid in [
            "---\ninput:\n  short:\n    type: word\n    description: Short input description\n---\nBody",
            "---\ninput:\n  - name: long\n    type: text\n    description: Long input description\n---\nBody",
            "---\ninput: {short: {type: word, description: Short input description}}\n---\nBody",
            "---\ninput: [{name: long, type: text, description: Long input description}]\n---\nBody",
        ] {
            let diagnostics = diagnostics_for(valid);
            assert!(
                diagnostics.iter().all(|diagnostic| {
                    diagnostic.code != "unknown_xprompt_frontmatter_input_field"
                        && diagnostic.code
                            != "invalid_xprompt_frontmatter_input_description"
                }),
                "{diagnostics:?}"
            );
        }

        let invalid =
            "---\ninput:\n  short:\n    type: word\n    description: {}\n---\nBody";
        let diagnostics = diagnostics_for(invalid);
        assert_eq!(
            diagnostic(
                &diagnostics,
                "invalid_xprompt_frontmatter_input_description"
            )
            .severity,
            DiagnosticSeverity::Error
        );
    }

    #[test]
    fn reports_invalid_snippet_tags_keywords_and_skill_metadata() {
        let diagnostics = diagnostics_for(
            "---\nsnippet: bad-trigger!\ntags: [mentor, {}]\nkeywords: [topic, {}]\nskill: true\n---\nBody",
        );

        assert_eq!(
            diagnostic(
                &diagnostics,
                "invalid_xprompt_frontmatter_snippet_trigger"
            )
            .severity,
            DiagnosticSeverity::Error
        );
        assert_eq!(
            diagnostic(&diagnostics, "invalid_xprompt_frontmatter_tags")
                .severity,
            DiagnosticSeverity::Error
        );
        assert_eq!(
            diagnostic(&diagnostics, "invalid_xprompt_frontmatter_keywords")
                .severity,
            DiagnosticSeverity::Error
        );
        assert_eq!(
            diagnostic(&diagnostics, "missing_xprompt_memory_tag").severity,
            DiagnosticSeverity::Warning
        );
        assert_eq!(
            diagnostic(&diagnostics, "missing_xprompt_skill_description")
                .severity,
            DiagnosticSeverity::Warning
        );
    }

    #[test]
    fn memory_long_source_path_supplies_implicit_memory_tag() {
        let text = "---\nkeywords: [topic]\n---\nBody";
        let diagnostics = diagnostics_for(text);
        assert_eq!(
            diagnostic(&diagnostics, "missing_xprompt_memory_tag").severity,
            DiagnosticSeverity::Warning
        );

        let doc = DocumentSnapshot::with_source_path(
            text,
            "/repo/memory/long/generated_skills.md",
        );
        let diagnostics = analyze_document(&doc, &catalog());
        assert!(
            diagnostics
                .iter()
                .all(|diagnostic| diagnostic.code
                    != "missing_xprompt_memory_tag"),
            "{diagnostics:?}"
        );

        let invalid_doc = DocumentSnapshot::with_source_path(
            "---\nkeywords: [{}]\n---\nBody",
            "/repo/memory/long/generated_skills.md",
        );
        let diagnostics = analyze_document(&invalid_doc, &catalog());
        assert_eq!(
            diagnostic(&diagnostics, "invalid_xprompt_frontmatter_keywords")
                .severity,
            DiagnosticSeverity::Error
        );
    }

    #[test]
    fn reports_flow_style_input_default_on_offending_scalar() {
        let text = "---\ninput: [{name: target, type: word, default: \"two words\"}]\n---\nBody";
        let diagnostics = diagnostics_for(text);
        let diagnostic = diagnostic(
            &diagnostics,
            "invalid_xprompt_frontmatter_input_default",
        );

        assert_eq!(diagnostic_text(text, diagnostic), "two words");
    }

    #[test]
    fn accepts_valid_argument_forms_and_bool_spellings() {
        for text in [
            "#typed(path=src/main.rs, count=3, enabled=true)",
            "#typed(src/main.rs, 3, yes)",
            "#typed:src/main.rs,3,on",
            "#typed(path=null, count=null)",
            "#ns/foo(arg=hello)",
            "#ns__foo!!(arg=hello)",
        ] {
            let doc = DocumentSnapshot::new(text);
            let diagnostics = analyze_document(&doc, &catalog());
            assert!(
                !diagnostics
                    .iter()
                    .any(|d| d.severity == DiagnosticSeverity::Error),
                "{text}: {diagnostics:?}"
            );
        }
    }

    #[test]
    fn incomplete_forms_do_not_emit_required_arg_noise() {
        for text in ["#typed(", "#typed(path=", "#typed:"] {
            let doc = DocumentSnapshot::new(text);
            let diagnostics = analyze_document(&doc, &catalog());
            assert!(
                !diagnostics.iter().any(|d| d.code == "missing_required_arg"),
                "{text}: {diagnostics:?}"
            );
        }
    }
}
