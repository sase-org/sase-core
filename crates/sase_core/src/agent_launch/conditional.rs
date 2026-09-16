use super::{
    directive_occurrences, disabled_region_ranges,
    launch_inline_literal_ranges, position_in_ranges,
    prompt_body_start_after_frontmatter, split_named_directive_arg,
    strip_prompt_regions, unquote_directive_arg_value,
    AgentLaunchFanoutPlanError, LaunchPlanDiagnosticWire,
};
use crate::fenced_code::{fenced_block_ranges, scan_directive_owned_fences};
use serde::{Deserialize, Serialize};

pub const CONDITIONAL_LAUNCH_SEGMENT_FILTER_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionalLaunchSegmentWire {
    pub source_index: u32,
    pub source_span: [usize; 2],
    pub prompt: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionalLaunchSegmentFilterWire {
    pub schema_version: u32,
    #[serde(default)]
    pub segments: Vec<ConditionalLaunchSegmentWire>,
    #[serde(default)]
    pub diagnostics: Vec<LaunchPlanDiagnosticWire>,
}

#[derive(Debug, Clone)]
struct SourceSegment<'a> {
    source_index: u32,
    source_span: [usize; 2],
    prompt: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IfMode {
    Boolean { should_run: bool, span: [usize; 2] },
    Script,
}

#[derive(Debug, Default)]
struct SegmentCondition {
    mode: Option<IfMode>,
    remove_regions: Vec<(usize, usize)>,
    diagnostics: Vec<LaunchPlanDiagnosticWire>,
}

pub fn filter_conditional_launch_segments(
    prompt: &str,
) -> Result<ConditionalLaunchSegmentFilterWire, AgentLaunchFanoutPlanError> {
    let filter = scan_conditional_launch_segments(prompt);
    if filter.diagnostics.is_empty() {
        Ok(filter)
    } else {
        Err(AgentLaunchFanoutPlanError::TypedLaunchPlan {
            diagnostics: filter.diagnostics,
        })
    }
}

pub fn scan_conditional_launch_segments(
    prompt: &str,
) -> ConditionalLaunchSegmentFilterWire {
    let mut kept = Vec::new();
    let mut diagnostics = Vec::new();

    for segment in split_source_segments(prompt) {
        let condition = analyze_segment(segment.prompt, segment.source_span[0]);
        diagnostics.extend(condition.diagnostics);
        match condition.mode {
            Some(IfMode::Boolean {
                should_run: false, ..
            }) => {}
            Some(IfMode::Boolean {
                should_run: true, ..
            }) => {
                let cleaned = strip_prompt_regions(
                    segment.prompt,
                    &condition.remove_regions,
                );
                let trimmed = cleaned.trim();
                if !trimmed.is_empty() {
                    kept.push(ConditionalLaunchSegmentWire {
                        source_index: segment.source_index,
                        source_span: segment.source_span,
                        prompt: trimmed.to_string(),
                    });
                }
            }
            Some(IfMode::Script) | None => {
                kept.push(ConditionalLaunchSegmentWire {
                    source_index: segment.source_index,
                    source_span: segment.source_span,
                    prompt: segment.prompt.to_string(),
                });
            }
        }
    }

    ConditionalLaunchSegmentFilterWire {
        schema_version: CONDITIONAL_LAUNCH_SEGMENT_FILTER_SCHEMA_VERSION,
        segments: kept,
        diagnostics,
    }
}

fn analyze_segment(prompt: &str, source_offset: usize) -> SegmentCondition {
    let mut condition = SegmentCondition::default();
    let scan = scan_directive_owned_fences(prompt);
    let owned_if_spans: Vec<[usize; 2]> = scan
        .directives
        .iter()
        .filter(|directive| directive.name == "if")
        .map(|directive| directive.span)
        .collect();

    for diagnostic in scan
        .diagnostics
        .into_iter()
        .filter(|diagnostic| diagnostic.message.contains("%if"))
    {
        condition.diagnostics.push(diagnostic_wire(
            diagnostic.code,
            diagnostic.message,
            offset_span(diagnostic.span, source_offset),
        ));
    }
    for span in &owned_if_spans {
        register_if_mode(
            &mut condition,
            IfMode::Script,
            offset_span(*span, source_offset),
        );
    }

    let ignored_ranges = conditional_literal_zone_ranges(prompt);
    for directive in directive_occurrences(prompt).unwrap_or_default() {
        if directive.canonical_name != "if" {
            continue;
        }
        if position_in_ranges(directive.start, &ignored_ranges) {
            continue;
        }
        if owned_if_spans
            .iter()
            .any(|span| span[0] <= directive.start && directive.start < span[1])
        {
            continue;
        }

        let span = [directive.start, directive.end];
        if directive.has_paren_form {
            analyze_parenthesized_if(
                prompt,
                &directive,
                source_offset,
                &mut condition,
            );
        } else if directive.is_bare && prompt[directive.end..].starts_with("::")
        {
            register_if_mode(
                &mut condition,
                IfMode::Script,
                offset_span(span, source_offset),
            );
        } else if directive.is_bare {
            continue;
        } else {
            condition.diagnostics.push(diagnostic_wire(
                "invalid-if-form",
                "%if supports %if(should_run=true|false) for static omission, or %if:: plus one bash or python fence for admission predicates.",
                offset_span(span, source_offset),
            ));
        }
    }
    condition
}

fn analyze_parenthesized_if(
    prompt: &str,
    directive: &super::DirectiveOccurrence,
    source_offset: usize,
    condition: &mut SegmentCondition,
) {
    let span = [directive.start, directive.end];
    let source_span = offset_span(span, source_offset);
    if !directive.paren_closed {
        condition.diagnostics.push(diagnostic_wire(
            "malformed-if-parentheses",
            "Malformed %if(...) directive: missing closing ')'.",
            source_span,
        ));
        return;
    }

    let has_double_colon_body = prompt[directive.end..].starts_with("::");
    let mut should_run: Option<bool> = None;
    let mut positional = Vec::new();
    let mut seen_should_run = false;

    for arg in &directive.args {
        let (name, value_raw) = split_named_directive_arg(arg);
        match name.as_deref() {
            Some("should_run") => {
                if seen_should_run {
                    condition.diagnostics.push(diagnostic_wire(
                        "duplicate-if-keyword",
                        "Duplicate keyword argument 'should_run' on %if.",
                        source_span,
                    ));
                    continue;
                }
                seen_should_run = true;
                match parse_should_run(value_raw.trim()) {
                    Ok(value) => should_run = Some(value),
                    Err(message) => {
                        condition.diagnostics.push(diagnostic_wire(
                            "invalid-should-run",
                            message,
                            source_span,
                        ))
                    }
                }
            }
            Some(other) => {
                condition.diagnostics.push(diagnostic_wire(
                    "unsupported-if-keyword",
                    format!(
                        "Unsupported keyword on %if: {other}=. Only should_run= is supported."
                    ),
                    source_span,
                ));
            }
            None => {
                let value = unquote_directive_arg_value(value_raw.trim());
                if !value.trim().is_empty() {
                    positional.push(value);
                }
            }
        }
    }

    if should_run.is_some() && (has_double_colon_body || !positional.is_empty())
    {
        condition.diagnostics.push(diagnostic_wire(
            "if-mode-conflict",
            "The %if should_run= argument cannot be combined with a Python/Bash condition body.",
            source_span,
        ));
        return;
    }

    if has_double_colon_body || !positional.is_empty() {
        condition.diagnostics.push(diagnostic_wire(
            "invalid-if-form",
            "%if admission predicates use %if:: followed by exactly one closed bash or python fence; parenthesized %if only supports should_run=.",
            source_span,
        ));
        return;
    }

    let Some(should_run) = should_run else {
        condition.diagnostics.push(diagnostic_wire(
            "missing-should-run",
            "%if(...) requires should_run=true or should_run=false.",
            source_span,
        ));
        return;
    };

    condition
        .remove_regions
        .push((directive.start, directive.end));
    register_if_mode(
        condition,
        IfMode::Boolean {
            should_run,
            span: source_span,
        },
        source_span,
    );
}

fn parse_should_run(raw: &str) -> Result<bool, &'static str> {
    let value = unquote_directive_arg_value(raw).trim().to_ascii_lowercase();
    match value.as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err("The %if should_run= value must be exactly true or false after template rendering."),
    }
}

fn register_if_mode(
    condition: &mut SegmentCondition,
    mode: IfMode,
    source_span: [usize; 2],
) {
    if let Some(existing) = condition.mode {
        condition.diagnostics.push(diagnostic_wire(
            if matches!(
                (existing, mode),
                (IfMode::Boolean { .. }, IfMode::Script)
                    | (IfMode::Script, IfMode::Boolean { .. })
            ) {
                "if-mode-conflict"
            } else {
                "duplicate-if"
            },
            if matches!(
                (existing, mode),
                (IfMode::Boolean { .. }, IfMode::Script)
                    | (IfMode::Script, IfMode::Boolean { .. })
            ) {
                "The %if should_run= argument cannot be combined with a Python/Bash condition body."
            } else {
                "Only one %if is allowed per prompt segment."
            },
            source_span,
        ));
        return;
    }
    condition.mode = Some(mode);
}

fn conditional_literal_zone_ranges(prompt: &str) -> Vec<(usize, usize)> {
    let mut ranges = fenced_block_ranges(prompt);
    ranges.extend(disabled_region_ranges(prompt));
    if prompt.contains('`') {
        ranges.extend(launch_inline_literal_ranges(prompt));
    }
    ranges
}

fn split_source_segments(prompt: &str) -> Vec<SourceSegment<'_>> {
    let body_start = prompt_body_start_after_frontmatter(prompt);
    let body = &prompt[body_start..];
    let fenced_ranges = fenced_block_ranges(body);
    let mut segments = Vec::new();
    let mut segment_start = 0;
    let mut line_start = 0;

    for piece in body.split_inclusive('\n') {
        let line_end = line_start + piece.len();
        let content_end = if piece.ends_with('\n') {
            line_end - 1
        } else {
            line_end
        };
        let line = &body[line_start..content_end];
        if line.trim() == "---"
            && !position_in_ranges(line_start, &fenced_ranges)
        {
            let source_index = segments.len() as u32;
            push_source_segment(
                &mut segments,
                source_index,
                prompt,
                body_start + segment_start,
                body_start + line_start,
            );
            segment_start = line_end;
        }
        line_start = line_end;
    }
    if segment_start <= body.len() {
        let source_index = segments.len() as u32;
        push_source_segment(
            &mut segments,
            source_index,
            prompt,
            body_start + segment_start,
            body_start + body.len(),
        );
    }
    segments
}

fn push_source_segment<'a>(
    out: &mut Vec<SourceSegment<'a>>,
    source_index: u32,
    prompt: &'a str,
    raw_start: usize,
    raw_end: usize,
) {
    let raw = &prompt[raw_start..raw_end];
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return;
    }
    let trim_start = raw.len() - raw.trim_start().len();
    let trim_end = raw.trim_end().len();
    out.push(SourceSegment {
        source_index,
        source_span: [raw_start + trim_start, raw_start + trim_end],
        prompt: &prompt[raw_start + trim_start..raw_start + trim_end],
    });
}

fn diagnostic_wire(
    code: impl Into<String>,
    message: impl Into<String>,
    source_span: [usize; 2],
) -> LaunchPlanDiagnosticWire {
    LaunchPlanDiagnosticWire {
        code: code.into(),
        severity: "error".to_string(),
        message: message.into(),
        source_span: Some(source_span),
        logical_id: None,
    }
}

fn offset_span(span: [usize; 2], source_offset: usize) -> [usize; 2] {
    [span[0] + source_offset, span[1] + source_offset]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prompts(prompt: &str) -> Vec<String> {
        filter_conditional_launch_segments(prompt)
            .unwrap()
            .segments
            .into_iter()
            .map(|segment| segment.prompt)
            .collect()
    }

    #[test]
    fn false_boolean_omits_segments() {
        assert_eq!(
            prompts("one\n---\n%if(should_run=false)\ntwo\n---\nthree"),
            vec!["one".to_string(), "three".to_string()]
        );
        assert!(prompts("%if(should_run=False)\nonly").is_empty());
    }

    #[test]
    fn true_boolean_strips_only_directive() {
        assert_eq!(
            prompts("%if(should_run=True)\nReview this"),
            vec!["Review this".to_string()]
        );
    }

    #[test]
    fn literal_zones_do_not_trigger_boolean_filter() {
        assert_eq!(
            prompts("```text\n%if(should_run=false)\n```\nReview"),
            vec!["```text\n%if(should_run=false)\n```\nReview".to_string()]
        );
        assert_eq!(
            prompts("`%if(should_run=false)`\nReview"),
            vec!["`%if(should_run=false)`\nReview".to_string()]
        );
    }

    #[test]
    fn prose_if_mentions_are_literal() {
        assert_eq!(
            prompts("stop un-admitted %if/%proc units"),
            vec!["stop un-admitted %if/%proc units".to_string()]
        );
        assert_eq!(
            prompts("%if is plain text\nReview"),
            vec!["%if is plain text\nReview".to_string()]
        );
    }

    #[test]
    fn invalid_boolean_blocks_entire_batch() {
        let err = filter_conditional_launch_segments(
            "first\n---\n%if(should_run=maybe)\nsecond",
        )
        .unwrap_err();
        assert!(err.to_string().contains("true or false"));
    }

    #[test]
    fn boolean_and_script_body_conflict() {
        let err = filter_conditional_launch_segments(
            "%if(should_run=false)::\n```bash\ntrue\n```\nReview",
        )
        .unwrap_err();
        assert!(err.to_string().contains("cannot be combined"));
    }
}
