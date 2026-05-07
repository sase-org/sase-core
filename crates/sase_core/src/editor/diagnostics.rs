use regex::Regex;
use std::sync::OnceLock;

use super::completion::classify_completion_context;
use super::directive::canonical_directive_name;
use super::token::DocumentSnapshot;
use super::wire::{
    CompletionContextKind, DiagnosticSeverity, EditorDiagnostic,
    XpromptAssistEntry,
};

pub fn analyze_document(
    document: &DocumentSnapshot,
    entries: &[XpromptAssistEntry],
) -> Vec<EditorDiagnostic> {
    let mut diagnostics = Vec::new();
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
    for caps in malformed_arg_re().captures_iter(document.text()) {
        let Some(start) = caps.name("start") else {
            continue;
        };
        let Some(range) =
            document.byte_range_to_range(start.start(), start.end())
        else {
            continue;
        };
        let Some(context) =
            classify_completion_context(document, range.end, entries)
        else {
            out.push(EditorDiagnostic {
                range,
                severity: DiagnosticSeverity::Information,
                code: "malformed_xprompt_argument".to_string(),
                message: "Malformed xprompt argument form".to_string(),
            });
            continue;
        };
        if matches!(
            context.kind,
            CompletionContextKind::XpromptArgumentName
                | CompletionContextKind::XpromptArgumentPath
                | CompletionContextKind::XpromptArgumentValue
                | CompletionContextKind::XpromptArgumentTypeHint
        ) {
            continue;
        }
        out.push(EditorDiagnostic {
            range,
            severity: DiagnosticSeverity::Information,
            code: "malformed_xprompt_argument".to_string(),
            message: "Malformed xprompt argument form".to_string(),
        });
    }
    out
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

fn malformed_arg_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?m)(?P<start>#!?[A-Za-z_][A-Za-z0-9_]*(?:(?:/|__)[A-Za-z_][A-Za-z0-9_]*)*(?:\([^)\s]*\s+|:[^\s,()]*[()+]))"#,
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
                is_skill: true,
            },
        ]
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
}
