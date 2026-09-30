//! Jinja hover: resolve the identifier under the cursor by slot.
//!
//! Handles a variable (including an unavailable builtin with its
//! reason), a known member, a filter, or a test. Unknown names and
//! statement positions yield `None`.

use super::assist::{
    conditional_hint, evaluate_rule, unavailable_reason, Availability,
};
use super::catalog::jinja_catalog;
use super::context::{jinja_completion_slot, JinjaSlot};
use super::docs;
use super::scope::jinja_document_scope;
use super::wire::{JinjaAssistRequestWire, JinjaScopeKind};
use crate::editor::token::DocumentSnapshot;
use crate::editor::wire::HoverPayload;

/// Hover for the identifier under the cursor inside a tag.
pub fn jinja_hover(req: &JinjaAssistRequestWire) -> Option<HoverPayload> {
    let document = DocumentSnapshot::new(req.text.as_str());
    let cursor = document.position_to_byte_offset(req.position)?;
    let slot = jinja_completion_slot(&req.text, cursor)?;
    let range =
        document.byte_range_to_range(slot.token_start, slot.token_end)?;
    let token = req.text.get(slot.token_start..slot.token_end)?;
    if token.is_empty() {
        return None;
    }
    let markdown = match slot.slot {
        JinjaSlot::Variable => {
            hover_variable(req, cursor, token, slot.token_start)?
        }
        JinjaSlot::Member => hover_member(slot.namespace.as_deref()?, token)?,
        JinjaSlot::Filter => hover_filter(token)?,
        JinjaSlot::Test => hover_test(token)?,
        JinjaSlot::Statement | JinjaSlot::None => return None,
    };
    Some(HoverPayload { range, markdown })
}

fn hover_variable(
    req: &JinjaAssistRequestWire,
    cursor: usize,
    token: &str,
    _token_start: usize,
) -> Option<String> {
    let scope =
        jinja_document_scope(&req.text, cursor, req.frontmatter.as_deref());
    // Locals first (innermost wins on duplicates).
    let mut best_local: Option<(usize, &str, &str)> = None;
    for local in &scope.locals {
        if local.name == token {
            let (type_label, summary) = local_summary(local.kind);
            // Innermost = largest scope_start.
            if best_local.is_none_or(|(start, _, _)| local.scope_start >= start)
            {
                best_local = Some((local.scope_start, type_label, summary));
            }
        }
    }
    if let Some((_, type_label, summary)) = best_local {
        let shadows = catalog_builtin_name(token).then_some(token);
        return Some(docs::local_markdown(token, type_label, summary, shadows));
    }
    // Declared inputs shadow builtins.
    if let Some(input) = scope.inputs.iter().find(|entry| entry.name == token) {
        let shadows = catalog_builtin_name(token).then_some(token);
        return Some(docs::input_markdown(
            token,
            &input.type_name,
            input.description.as_deref(),
            input.required,
            input.default_display.as_deref(),
            &input.choices,
            shadows,
        ));
    }
    // Dynamic positional `_N` in xprompt scope.
    if let Some(number) = token.strip_prefix('_').filter(|rest| {
        !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit())
    }) {
        let _ = number;
        if req.scope == JinjaScopeKind::Xprompt {
            let index = token[1..].to_string();
            let summary = format!("Positional argument {index}.");
            // Shadow note when a builtin of the same name exists
            // (only `_args` collides in practice; numbered names do not).
            let shadows = catalog_builtin_name(token).then_some(token);
            return Some(docs::positional_markdown(
                token, "any", &summary, shadows,
            ));
        }
        let example = format!("{{{{ {token} }}}}");
        return Some(docs::unavailable_markdown(
            token,
            "any",
            "positional",
            "Positional argument.",
            Some(&example),
            &super::assist::xprompt_only_reason(),
        ));
    }
    let catalog = jinja_catalog();
    // Catalog variables (including unavailable, shown with a reason).
    if let Some(variable) =
        catalog.variables.iter().find(|entry| entry.name == token)
    {
        let availability =
            evaluate_rule(variable.availability_rule, req.scope, &scope);
        let source = match variable.group {
            super::wire::JinjaCompletionSource::Sase => "sase",
            super::wire::JinjaCompletionSource::Positional => "positional",
            super::wire::JinjaCompletionSource::Provider => "provider",
            super::wire::JinjaCompletionSource::Local => "local",
            super::wire::JinjaCompletionSource::Input => "input",
            super::wire::JinjaCompletionSource::Jinja => "jinja",
        };
        match availability {
            Availability::Available => {
                return Some(docs::catalog_variable_markdown(
                    &docs::CatalogVariableDoc {
                        name: token,
                        type_label: &variable.type_label,
                        source_label: source,
                        summary: &variable.summary,
                        documentation: &variable.documentation,
                        conditional_hint: None,
                        legacy_for: variable.legacy_for.as_deref(),
                        shadows: None,
                    },
                ));
            }
            Availability::Conditional => {
                let hint =
                    conditional_hint(variable.availability_rule, req.scope);
                return Some(docs::catalog_variable_markdown(
                    &docs::CatalogVariableDoc {
                        name: token,
                        type_label: &variable.type_label,
                        source_label: source,
                        summary: &variable.summary,
                        documentation: &variable.documentation,
                        conditional_hint: Some(&hint),
                        legacy_for: variable.legacy_for.as_deref(),
                        shadows: None,
                    },
                ));
            }
            Availability::Unavailable => {
                let example = docs_example(&variable.documentation)
                    .unwrap_or_else(|| format!("{{{{ {token} }}}}"));
                return Some(docs::unavailable_markdown(
                    token,
                    &variable.type_label,
                    source,
                    &variable.summary,
                    Some(&example),
                    &unavailable_reason(
                        variable.availability_rule,
                        req.scope,
                        &scope,
                    ),
                ));
            }
        }
    }
    // Jinja globals.
    if let Some(global) = catalog
        .jinja_globals
        .iter()
        .find(|entry| entry.name == token)
    {
        return Some(docs::global_markdown(
            token,
            &global.signature,
            &global.summary,
            None,
        ));
    }
    None
}

fn hover_member(namespace: &str, token: &str) -> Option<String> {
    let catalog = jinja_catalog();
    if namespace == "wait" {
        let holder = catalog
            .variables
            .iter()
            .find(|entry| entry.name == "wait")?;
        let member = holder.members.iter().find(|entry| entry.name == token)?;
        return Some(docs::member_markdown(
            "wait",
            token,
            &member.type_label,
            &member.summary,
            "sase",
        ));
    }
    if namespace == "loop" {
        let holder = catalog
            .variables
            .iter()
            .find(|entry| entry.name == "loop")?;
        let member = holder.members.iter().find(|entry| entry.name == token)?;
        return Some(docs::member_markdown(
            "loop",
            token,
            &member.type_label,
            &member.summary,
            "local",
        ));
    }
    None
}

fn hover_filter(token: &str) -> Option<String> {
    let catalog = jinja_catalog();
    let filter = catalog.filters.iter().find(|entry| entry.name == token)?;
    let source = match filter.tier {
        super::wire::JinjaFilterTier::Sase => "sase",
        super::wire::JinjaFilterTier::Common
        | super::wire::JinjaFilterTier::Other => "jinja",
    };
    Some(docs::filter_markdown(
        token,
        &filter.signature,
        &filter.summary,
        source,
    ))
}

fn hover_test(token: &str) -> Option<String> {
    let catalog = jinja_catalog();
    let test = catalog.tests.iter().find(|entry| entry.name == token)?;
    Some(docs::test_markdown(token, &test.summary))
}

fn local_summary(
    kind: super::scope::JinjaLocalKind,
) -> (&'static str, &'static str) {
    match kind {
        super::scope::JinjaLocalKind::Set => {
            ("unknown", "Template local defined by `{% set %}`.")
        }
        super::scope::JinjaLocalKind::ForTarget => {
            ("unknown", "Loop variable of `{% for %}`.")
        }
        super::scope::JinjaLocalKind::Loop => (
            "for-loop",
            "Jinja loop object for the enclosing `{% for %}`.",
        ),
        super::scope::JinjaLocalKind::MacroName => {
            ("macro", "Macro defined by `{% macro %}`.")
        }
        super::scope::JinjaLocalKind::MacroParam => {
            ("unknown", "Macro parameter.")
        }
        super::scope::JinjaLocalKind::MacroSpecial => (
            "unknown",
            "Implicit macro variable (`varargs`, `kwargs`, `caller`).",
        ),
        super::scope::JinjaLocalKind::With => {
            ("unknown", "Temporary assignment in `{% with %}`.")
        }
        super::scope::JinjaLocalKind::Import => (
            "unknown",
            "Name imported by `{% import %}` or `{% from %}`.",
        ),
    }
}

/// Whether `name` exists as a builtin (catalog variable or Jinja global).
fn catalog_builtin_name(name: &str) -> bool {
    let catalog = jinja_catalog();
    catalog.variables.iter().any(|entry| entry.name == name)
        || catalog.jinja_globals.iter().any(|entry| entry.name == name)
}

fn docs_example(documentation: &str) -> Option<String> {
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
    use crate::editor::token::DocumentSnapshot;
    use crate::editor::wire::EditorPosition;

    fn hover_at(
        text: &str,
        cursor: usize,
        scope: JinjaScopeKind,
    ) -> Option<HoverPayload> {
        let document = DocumentSnapshot::new(text);
        let position = document.byte_offset_to_position(cursor).unwrap();
        jinja_hover(&JinjaAssistRequestWire {
            text: text.to_string(),
            position,
            scope,
            frontmatter: None,
        })
    }

    #[test]
    fn hovers_a_variable_with_docs() {
        let text = "{{ patch_name }}";
        let hover = hover_at(
            text,
            text.find("patch").unwrap() + 2,
            JinjaScopeKind::Prompt,
        )
        .unwrap();
        assert!(hover.markdown.contains("**patch_name**"));
        assert!(hover.markdown.contains("`str`"));
        assert!(hover.markdown.contains("sase"));
        assert!(hover.markdown.contains("{{ patch_name }}"));
        let _ = EditorPosition {
            line: 0,
            character: 0,
        };
    }

    #[test]
    fn hovers_an_unavailable_builtin_with_reason() {
        let text = "---\ninput:\n  topic: word\n---\n{{ patch_name }}";
        let cursor = text.find("patch").unwrap() + 2;
        let hover = hover_at(text, cursor, JinjaScopeKind::Prompt).unwrap();
        assert!(hover.markdown.contains("**patch_name**"));
        assert!(hover.markdown.contains("\u{26a0}"));
        assert!(hover.markdown.contains("inputs"));
    }

    #[test]
    fn hovers_a_conditional_with_hint() {
        let text = "{{ n }}";
        let hover =
            hover_at(text, text.find('n').unwrap(), JinjaScopeKind::Xprompt)
                .unwrap();
        assert!(hover.markdown.contains("**n**"));
        assert!(hover.markdown.contains("%repeat"));
    }

    #[test]
    fn hovers_a_member_filter_and_test() {
        let text = "{{ wait.chats }}";
        let hover = hover_at(
            text,
            text.find("chats").unwrap() + 2,
            JinjaScopeKind::Prompt,
        )
        .unwrap();
        assert!(hover.markdown.contains("**chats**"));
        assert!(hover.markdown.contains("wait.chats"));

        let text = "{{ x | join }}";
        let hover = hover_at(
            text,
            text.find("join").unwrap() + 1,
            JinjaScopeKind::Prompt,
        )
        .unwrap();
        assert!(hover.markdown.contains("**join**"));

        let text = "{{ x is defined }}";
        let hover = hover_at(
            text,
            text.find("defined").unwrap() + 1,
            JinjaScopeKind::Prompt,
        )
        .unwrap();
        assert!(hover.markdown.contains("**defined**"));
    }

    #[test]
    fn unknown_names_and_outside_tags_hover_none() {
        assert!(hover_at("{{ nope }}", 4, JinjaScopeKind::Prompt).is_none());
        assert!(hover_at("hello", 2, JinjaScopeKind::Prompt).is_none());
    }
}
