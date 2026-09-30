//! Shared markdown documentation for Jinja completion and hover.
//!
//! One renderer backs both surfaces so the menu and hover never drift.
//! The header holds the bold name, the type or signature in backticks,
//! and the source label. After a blank line come the summary and an
//! example. The last block is optional bullets for defaults, choices,
//! conditional hints, legacy aliases, shadowing, and closers.

/// Render a documentation block from parts.
///
/// `header_type` is the type or signature already wrapped for display
/// (without backticks); it is rendered in backticks. `bullets` are
/// rendered as `- {bullet}` lines after a blank line when non-empty.
pub fn render_doc(
    name: &str,
    header_type: Option<&str>,
    source_label: &str,
    summary: &str,
    example: Option<&str>,
    bullets: &[String],
) -> String {
    let mut lines = Vec::new();
    let mut header = format!("**{name}**");
    if let Some(type_label) = header_type {
        header.push_str(&format!(" `{type_label}`"));
    }
    header.push_str(&format!(" \u{b7} {source_label}"));
    lines.push(header);
    lines.push(String::new());
    lines.push(summary.to_string());
    if let Some(example) = example {
        lines.push(String::new());
        lines.push(format!("Example: `{example}`"));
    }
    if !bullets.is_empty() {
        lines.push(String::new());
        for bullet in bullets {
            lines.push(format!("- {bullet}"));
        }
    }
    lines.join("\n")
}

/// Documentation for a declared input.
pub fn input_markdown(
    name: &str,
    type_name: &str,
    description: Option<&str>,
    required: bool,
    default_display: Option<&str>,
    choices: &[String],
    shadows: Option<&str>,
) -> String {
    let source = if required {
        "input \u{b7} required"
    } else {
        "input"
    };
    let summary = description.unwrap_or("Declared input.");
    let example = format!("{{{{ {name} }}}}");
    let mut bullets = Vec::new();
    if let Some(default) = default_display {
        bullets.push(format!("Default: `{default}`"));
    }
    if !choices.is_empty() {
        let joined = choices
            .iter()
            .map(|choice| format!("`{choice}`"))
            .collect::<Vec<_>>()
            .join(", ");
        bullets.push(format!("Choices: {joined}"));
    }
    if let Some(shadowed) = shadows {
        bullets.push(format!("Shadows the sase built-in `{shadowed}`"));
    }
    render_doc(
        name,
        Some(type_name),
        source,
        summary,
        Some(&example),
        &bullets,
    )
}

/// Documentation for a template local.
pub fn local_markdown(
    name: &str,
    type_label: &str,
    summary: &str,
    shadows: Option<&str>,
) -> String {
    let example = format!("{{{{ {name} }}}}");
    let mut bullets = Vec::new();
    if let Some(shadowed) = shadows {
        bullets.push(format!("Shadows the sase built-in `{shadowed}`"));
    }
    render_doc(
        name,
        Some(type_label),
        "local",
        summary,
        Some(&example),
        &bullets,
    )
}

/// Parameters for [`catalog_variable_markdown`], bundled so the
/// renderer stays under the argument-count lint.
pub struct CatalogVariableDoc<'a> {
    pub name: &'a str,
    pub type_label: &'a str,
    pub source_label: &'a str,
    pub summary: &'a str,
    pub documentation: &'a str,
    pub conditional_hint: Option<&'a str>,
    pub legacy_for: Option<&'a str>,
    pub shadows: Option<&'a str>,
}

/// Documentation for a catalog variable with availability details.
pub fn catalog_variable_markdown(doc: &CatalogVariableDoc<'_>) -> String {
    let example = first_example(doc.documentation)
        .unwrap_or_else(|| format!("{{{{ {} }}}}", doc.name));
    let mut bullets = Vec::new();
    if let Some(hint) = doc.conditional_hint {
        bullets.push(format!("\u{26a0} {hint}"));
    }
    if let Some(target) = doc.legacy_for {
        bullets.push(format!(
            "Legacy alias of `{target}` \u{2014} prefer `{{{{ {target} }}}}`"
        ));
    }
    if let Some(shadowed) = doc.shadows {
        bullets.push(format!("Shadows the sase built-in `{shadowed}`"));
    }
    render_doc(
        doc.name,
        Some(doc.type_label),
        doc.source_label,
        doc.summary,
        Some(&example),
        &bullets,
    )
}

/// Documentation for a positional `_1` style variable.
pub fn positional_markdown(
    name: &str,
    type_label: &str,
    summary: &str,
    shadows: Option<&str>,
) -> String {
    let example = format!("{{{{ {name} }}}}");
    let mut bullets = Vec::new();
    if let Some(shadowed) = shadows {
        bullets.push(format!("Shadows the sase built-in `{shadowed}`"));
    }
    render_doc(
        name,
        Some(type_label),
        "positional",
        summary,
        Some(&example),
        &bullets,
    )
}

/// Documentation for a Jinja global function.
pub fn global_markdown(
    name: &str,
    signature: &str,
    summary: &str,
    shadows: Option<&str>,
) -> String {
    let example = format!("{{{{ {name} }}}}");
    let mut bullets = Vec::new();
    if let Some(shadowed) = shadows {
        bullets.push(format!("Shadows the sase built-in `{shadowed}`"));
    }
    render_doc(
        name,
        Some(signature),
        "jinja",
        summary,
        Some(&example),
        &bullets,
    )
}

/// Documentation for a namespace member such as `wait.chats`.
pub fn member_markdown(
    namespace: &str,
    name: &str,
    type_label: &str,
    summary: &str,
    source_label: &str,
) -> String {
    let example = format!("{{{{ {namespace}.{name} }}}}");
    render_doc(
        name,
        Some(type_label),
        source_label,
        summary,
        Some(&example),
        &[],
    )
}

/// Documentation for a filter.
pub fn filter_markdown(
    name: &str,
    signature: &str,
    summary: &str,
    source_label: &str,
) -> String {
    let example = format!("{{{{ x | {name} }}}}");
    render_doc(
        name,
        Some(signature),
        source_label,
        summary,
        Some(&example),
        &[],
    )
}

/// Documentation for a test.
pub fn test_markdown(name: &str, summary: &str) -> String {
    let example = format!("{{{{ x is {name} }}}}");
    render_doc(name, None, "jinja", summary, Some(&example), &[])
}

/// Documentation for a statement keyword.
pub fn statement_markdown(
    name: &str,
    summary: &str,
    closes: Option<&str>,
) -> String {
    // `{{` renders a literal `{`, so `{{%` renders `{%`.
    let example = format!("{{% {name} %}}");
    let mut bullets = Vec::new();
    if let Some(keyword) = closes {
        bullets.push(format!("Closes `{{% {keyword} %}}`"));
    }
    render_doc(name, None, "jinja", summary, Some(&example), &bullets)
}

/// Documentation for an unavailable name shown by hover.
pub fn unavailable_markdown(
    name: &str,
    type_label: &str,
    source_label: &str,
    summary: &str,
    example: Option<&str>,
    reason: &str,
) -> String {
    let bullets = vec![format!("\u{26a0} {reason}")];
    render_doc(
        name,
        Some(type_label),
        source_label,
        summary,
        example,
        &bullets,
    )
}

/// First inline-code example in catalog documentation text.
fn first_example(documentation: &str) -> Option<String> {
    let mut search = documentation;
    while let Some(start) = search.find('`') {
        let rest = &search[start + 1..];
        let Some(end) = rest.find('`') else {
            break;
        };
        let candidate = &rest[..end];
        if candidate.contains("{{") || candidate.contains("{%") {
            return Some(candidate.trim().to_string());
        }
        search = &rest[end + 1..];
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_header_source_summary_example_and_bullets() {
        let markdown = render_doc(
            "patch_name",
            Some("str"),
            "sase",
            "Name of the patch.",
            Some("{{ patch_name }}"),
            &[
                "Default: `x`".to_string(),
                "\u{26a0} Only defined under `%repeat` \u{2014} add `%repeat:N`"
                    .to_string(),
                "Legacy alias of `patch_name` \u{2014} prefer `{{ patch_name }}`"
                    .to_string(),
                "Shadows the sase built-in `n`".to_string(),
                "Closes `{% for %}`".to_string(),
            ],
        );
        assert!(markdown.starts_with("**patch_name** `str` \u{b7} sase"));
        assert!(markdown.contains("\n\nName of the patch.\n"));
        assert!(markdown.contains("Example: `{{ patch_name }}`"));
        assert!(markdown.contains("- Default: `x`"));
        assert!(markdown.contains("- \u{26a0} Only defined under `%repeat`"));
        assert!(markdown.contains("Legacy alias of `patch_name`"));
        assert!(markdown.contains("Shadows the sase built-in `n`"));
        assert!(markdown.contains("Closes `{% for %}`"));
    }

    #[test]
    fn input_marks_required_and_lists_choices() {
        let markdown = input_markdown(
            "name",
            "word",
            Some("Who to greet."),
            true,
            None,
            &["a".to_string(), "b".to_string()],
            None,
        );
        assert!(
            markdown.contains("**name** `word` \u{b7} input \u{b7} required")
        );
        assert!(markdown.contains("Who to greet."));
        assert!(markdown.contains("- Choices: `a`, `b`"));
    }

    #[test]
    fn statement_example_renders_jinja_tags() {
        let markdown =
            statement_markdown("endfor", "Close a `for` block.", None);
        assert!(markdown.contains("Example: `{% endfor %}`"), "{markdown}");
        assert!(!markdown.contains("{%%"), "{markdown}");
        let markdown =
            statement_markdown("endfor", "Close a `for` block.", Some("for"));
        assert!(markdown.contains("Closes `{% for %}`"), "{markdown}");
    }

    #[test]
    fn catalog_variable_extracts_example_and_legacy() {
        let markdown = catalog_variable_markdown(&CatalogVariableDoc {
            name: "cl_name",
            type_label: "str",
            source_label: "sase",
            summary: "Legacy alias of `patch_name`.",
            documentation: "Legacy alias of `patch_name` \u{2014} prefer `{{ patch_name }}`.\n\nExample: `{{ patch_name }}`",
            conditional_hint: None,
            legacy_for: Some("patch_name"),
            shadows: None,
        });
        assert!(markdown.contains("Example: `{{ patch_name }}`"));
        assert!(markdown.contains("Legacy alias of `patch_name`"));
    }
}
