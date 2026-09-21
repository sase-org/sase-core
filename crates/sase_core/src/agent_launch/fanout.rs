//! Launch fanout planning: multi-prompt, alternative, model, and repeat
//! expansion of one prompt into independently launchable slots.
use super::conditional::filter_conditional_launch_segments;
use super::directive_scan::{
    alt_directive_starts, alt_inner_ranges, directive_occurrences,
    find_matching_delimiter, launch_literal_zone_ranges, leading_blank_line_re,
    parse_directive_args_with_names, position_in_ranges,
    split_named_directive_arg, strip_disabled_region_markers,
    unquote_directive_arg_value, xprompt_occurrences, DirectiveArg,
    DirectiveOccurrence,
};
use super::plan_resolution::strip_prompt_regions;
use super::wires::{
    parse_launch_timestamp, AgentLaunchFanoutPlanError,
    BatchPredecessorContextWire, BatchPredecessorWaitBindingWire,
    LaunchFanoutPlanWire, LaunchFanoutSlotWire,
    AGENT_LAUNCH_WIRE_SCHEMA_VERSION, BATCH_PREDECESSOR_CONTEXT_SCHEMA_VERSION,
};
use crate::effort::split_model_effort;
use crate::fenced_code::fenced_block_ranges;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const EMPTY_ALT_SENTINEL: char = '\u{E000}';
const EMPTY_ALT_SENTINEL_STR: &str = "\u{E000}";

#[derive(Debug, Clone, PartialEq, Eq)]
struct AlternativeBranch {
    value: String,
    id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AlternativeDirective {
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) args: Vec<DirectiveArg>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AlternativeAxis {
    start: usize,
    variants: Vec<AlternativeVariant>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AlternativeVariant {
    pub(crate) id: String,
    pub(crate) replacements: Vec<AlternativeReplacement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AlternativeReplacement {
    pub(crate) directive_index: usize,
    pub(crate) value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AlternativeSlot {
    prompt: String,
    alt_id: String,
}

pub fn plan_agent_launch_fanout(
    prompt: &str,
    launch_kind: Option<&str>,
) -> Result<LaunchFanoutPlanWire, AgentLaunchFanoutPlanError> {
    let requested = launch_kind.unwrap_or("auto");
    let filtered = filter_conditional_launch_segments(prompt)?;
    if filtered.segments.is_empty() {
        return Ok(empty_fanout_plan(match requested {
            "auto" => "single",
            other => other,
        }));
    }
    let filtered_prompt = filtered
        .segments
        .iter()
        .map(|segment| segment.prompt.as_str())
        .collect::<Vec<_>>()
        .join("\n---\n");
    let prompt = filtered_prompt.as_str();
    match requested {
        "multi_prompt" => Ok(LaunchFanoutPlanWire {
            schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
            launch_kind: "multi_prompt".to_string(),
            slots: filtered
                .segments
                .into_iter()
                .enumerate()
                .map(|(idx, segment)| LaunchFanoutSlotWire {
                    wait_for_previous: has_wait_directive(&segment.prompt),
                    prompt: segment.prompt,
                    launch_kind: "multi_prompt".to_string(),
                    slot_index: idx as u32,
                    alt_id: None,
                    timestamp: None,
                    workflow_name: None,
                    model: None,
                    repeat_name: None,
                    bead_id: None,
                })
                .collect(),
            requires_sequential_naming_wait: true,
            fanout_sleep_seconds: 0.0,
        }),
        "alternatives" => plan_alternative_fanout(prompt),
        "model" => plan_model_fanout(prompt),
        "repeat" => Ok(plan_repeat_fanout(prompt)),
        "auto" => {
            let multi = plan_multi_prompt_fanout(prompt);
            if multi.slots.len() > 1 {
                return Ok(multi);
            }
            let model = plan_model_fanout(prompt)?;
            if !model.slots.is_empty() {
                return Ok(model);
            }
            let repeat = plan_repeat_fanout(prompt);
            if !repeat.slots.is_empty() {
                return Ok(repeat);
            }
            Ok(LaunchFanoutPlanWire {
                schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
                launch_kind: "single".to_string(),
                slots: vec![LaunchFanoutSlotWire {
                    prompt: prompt.to_string(),
                    launch_kind: "single".to_string(),
                    slot_index: 0,
                    alt_id: None,
                    timestamp: None,
                    workflow_name: None,
                    model: None,
                    repeat_name: None,
                    bead_id: None,
                    wait_for_previous: has_wait_directive(prompt),
                }],
                requires_sequential_naming_wait: false,
                fanout_sleep_seconds: 0.0,
            })
        }
        other => Err(AgentLaunchFanoutPlanError::UnsupportedKind(
            other.to_string(),
        )),
    }
}

fn empty_fanout_plan(launch_kind: &str) -> LaunchFanoutPlanWire {
    LaunchFanoutPlanWire {
        schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
        launch_kind: launch_kind.to_string(),
        slots: Vec::new(),
        requires_sequential_naming_wait: false,
        fanout_sleep_seconds: 0.0,
    }
}

pub fn bind_batch_predecessor_waits(
    prompt: &str,
    predecessor: &BatchPredecessorContextWire,
) -> Result<BatchPredecessorWaitBindingWire, AgentLaunchFanoutPlanError> {
    validate_batch_predecessor_context(predecessor)?;

    let ignored_ranges = launch_literal_zone_ranges(prompt);
    let mut regions_to_remove = Vec::new();
    if prompt.contains('%') {
        for directive in directive_occurrences(prompt)? {
            if position_in_ranges(directive.start, &ignored_ranges) {
                continue;
            }
            if is_no_argument_wait_directive(&directive) {
                regions_to_remove.push((directive.start, directive.end));
            }
        }
    }

    if regions_to_remove.is_empty() {
        return Ok(BatchPredecessorWaitBindingWire {
            schema_version: BATCH_PREDECESSOR_CONTEXT_SCHEMA_VERSION,
            prompt: prompt.to_string(),
            wait_names: Vec::new(),
            wait_for_artifacts: Vec::new(),
            bound_wait_count: 0,
        });
    }

    let mut wait_names = Vec::new();
    if let Some(name) = predecessor.name.as_ref() {
        wait_names.push(name.clone());
    }

    Ok(BatchPredecessorWaitBindingWire {
        schema_version: BATCH_PREDECESSOR_CONTEXT_SCHEMA_VERSION,
        prompt: strip_prompt_regions(prompt, &regions_to_remove),
        wait_names,
        wait_for_artifacts: vec![predecessor.clone()],
        bound_wait_count: regions_to_remove.len() as u32,
    })
}

fn validate_batch_predecessor_context(
    predecessor: &BatchPredecessorContextWire,
) -> Result<(), AgentLaunchFanoutPlanError> {
    if predecessor.schema_version != BATCH_PREDECESSOR_CONTEXT_SCHEMA_VERSION {
        return Err(
            AgentLaunchFanoutPlanError::InvalidBatchPredecessorContext(
                format!(
                    "schema_version must be {}, got {}",
                    BATCH_PREDECESSOR_CONTEXT_SCHEMA_VERSION,
                    predecessor.schema_version
                ),
            ),
        );
    }
    if predecessor.project_name.trim().is_empty() {
        return Err(
            AgentLaunchFanoutPlanError::InvalidBatchPredecessorContext(
                "project_name is required".to_string(),
            ),
        );
    }
    parse_launch_timestamp("timestamp", &predecessor.timestamp).map_err(
        |error| {
            AgentLaunchFanoutPlanError::InvalidBatchPredecessorContext(
                error.to_string(),
            )
        },
    )?;
    if predecessor.artifact_dir.trim().is_empty() {
        return Err(
            AgentLaunchFanoutPlanError::InvalidBatchPredecessorContext(
                "artifact_dir is required".to_string(),
            ),
        );
    }
    if !Path::new(&predecessor.artifact_dir).is_absolute() {
        return Err(
            AgentLaunchFanoutPlanError::InvalidBatchPredecessorContext(
                "artifact_dir must be absolute".to_string(),
            ),
        );
    }
    if predecessor
        .name
        .as_ref()
        .is_some_and(|name| name.trim().is_empty())
    {
        return Err(
            AgentLaunchFanoutPlanError::InvalidBatchPredecessorContext(
                "name must be non-empty when supplied".to_string(),
            ),
        );
    }
    Ok(())
}

fn is_no_argument_wait_directive(directive: &DirectiveOccurrence) -> bool {
    directive.canonical_name == "wait"
        && !directive.has_plus_suffix
        && directive.args.iter().all(String::is_empty)
}

fn split_multi_prompt_segments(prompt: &str) -> Vec<String> {
    let body = prompt_body_after_frontmatter(prompt);
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
            push_nonempty_segment(
                &mut segments,
                &body[segment_start..line_start],
            );
            segment_start = line_end;
        }
        line_start = line_end;
    }
    if segment_start <= body.len() {
        push_nonempty_segment(&mut segments, &body[segment_start..]);
    }
    segments
}

fn prompt_body_after_frontmatter(prompt: &str) -> &str {
    &prompt[prompt_body_start_after_frontmatter(prompt)..]
}

pub(crate) fn prompt_body_start_after_frontmatter(prompt: &str) -> usize {
    let Some(first_line_end) = prompt.find('\n') else {
        return 0;
    };
    if prompt[..first_line_end].trim() != "---" {
        return 0;
    }

    let mut yaml_like = false;
    let mut offset = first_line_end + 1;
    for line in prompt[offset..].split_inclusive('\n') {
        let line_end = offset + line.len();
        let content_end = if line.ends_with('\n') {
            line_end - 1
        } else {
            line_end
        };
        let content = &prompt[offset..content_end];
        if content.trim() == "---" {
            return if yaml_like { line_end } else { 0 };
        }
        if content.contains(':') {
            yaml_like = true;
        }
        offset = line_end;
    }
    0
}

fn push_nonempty_segment(out: &mut Vec<String>, segment: &str) {
    let trimmed = segment.trim();
    if !trimmed.is_empty() {
        out.push(trimmed.to_string());
    }
}

fn split_prompt_for_models_with_ids(
    prompt: &str,
) -> Result<Vec<AlternativeSlot>, AgentLaunchFanoutPlanError> {
    if !prompt.contains('%') {
        return Ok(Vec::new());
    }

    let mut ignored_ranges = launch_literal_zone_ranges(prompt);
    ignored_ranges.extend(alt_inner_ranges(prompt, &ignored_ranges)?);

    let mut valued_directive_spans: Vec<(usize, usize, String)> = Vec::new();
    for directive in directive_occurrences(prompt)? {
        if directive.canonical_name != "model" {
            continue;
        }
        if position_in_ranges(directive.start, &ignored_ranges) {
            continue;
        }
        if directive.has_plus_suffix {
            continue;
        }
        let values: Vec<String> = directive
            .args
            .iter()
            .filter(|arg| !arg.is_empty())
            .cloned()
            .collect();
        if values.len() > 1 {
            let source = prompt[directive.start..directive.end].to_string();
            return Err(AgentLaunchFanoutPlanError::MultiModelUnsupported(
                multi_model_unsupported_message(&source, &values),
            ));
        }
        if let Some(value) = values.first() {
            valued_directive_spans.push((
                directive.start,
                directive.end,
                value.clone(),
            ));
        }
    }

    if valued_directive_spans.len() > 1 {
        let source = valued_directive_spans
            .iter()
            .map(|(start, end, _)| prompt[*start..*end].to_string())
            .collect::<Vec<_>>()
            .join(" ... ");
        let models = valued_directive_spans
            .iter()
            .map(|(_, _, value)| value.clone())
            .collect::<Vec<_>>();
        return Err(AgentLaunchFanoutPlanError::MultiModelUnsupported(
            multi_model_unsupported_message(&source, &models),
        ));
    }

    Ok(split_prompt_for_alternatives_with_ids(prompt)?.unwrap_or_default())
}

fn multi_model_unsupported_message(source: &str, models: &[String]) -> String {
    let replacement = models
        .iter()
        .map(|model| format!("%m:{model}"))
        .collect::<Vec<_>>()
        .join(" | ");
    format!("{source} is no longer supported; use %{{{replacement}}} instead")
}

fn split_prompt_for_alternatives_with_ids(
    prompt: &str,
) -> Result<Option<Vec<AlternativeSlot>>, AgentLaunchFanoutPlanError> {
    let ignored_ranges = launch_literal_zone_ranges(prompt);
    let mut directives: Vec<AlternativeDirective> = Vec::new();
    for (start, open_start, delimiter) in alt_directive_starts(prompt) {
        if position_in_ranges(start, &ignored_ranges) {
            continue;
        }
        let Some(close_end) = find_matching_delimiter(
            prompt,
            open_start,
            delimiter.open(),
            delimiter.close(),
        ) else {
            return Err(AgentLaunchFanoutPlanError::UnclosedDirective {
                name: delimiter.directive_label().to_string(),
                close: delimiter.close(),
            });
        };
        let inner = &prompt[open_start + 1..close_end];
        let args =
            parse_directive_args_with_names(inner, delimiter.separator());
        if args.is_empty() {
            continue;
        }
        directives.push(AlternativeDirective {
            start,
            end: close_end + 1,
            args,
        });
    }

    if directives.is_empty() {
        return Ok(None);
    }

    let mut axes = alternative_axes_for_directives(&directives);
    axes.sort_by_key(|axis| axis.start);
    let arg_lists: Vec<Vec<AlternativeVariant>> =
        axes.into_iter().map(|axis| axis.variants).collect();
    let mut combinations = Vec::new();
    cartesian_product(&arg_lists, 0, &mut Vec::new(), &mut combinations);

    let mut result = Vec::with_capacity(combinations.len());
    for combination in combinations {
        let alt_id = combination
            .iter()
            .map(|variant| variant.id.as_str())
            .collect::<Vec<_>>()
            .join(".");
        let replaced =
            render_alternative_prompt(prompt, &directives, &combination);
        result.push(AlternativeSlot {
            prompt: replaced,
            alt_id,
        });
    }
    Ok(Some(result))
}

/// Split `%alt(...)`, `%(...)`, and `%{...}` directives into launch slots.
///
/// Explicit branch names that appear in multiple directives are correlated:
/// the matching named branches render into the same slot instead of producing
/// a Cartesian product. Directives without shared explicit names keep the
/// historical Cartesian behavior, including the implicit empty branch for a
/// single-branch directive.
fn alternative_axes_for_directives(
    directives: &[AlternativeDirective],
) -> Vec<AlternativeAxis> {
    alternative_correlation_groups(directives)
        .into_iter()
        .map(|group| {
            if group.len() == 1 {
                alternative_singleton_axis(directives, group[0])
            } else {
                alternative_correlated_axis(directives, &group)
            }
        })
        .collect()
}

fn alternative_correlation_groups(
    directives: &[AlternativeDirective],
) -> Vec<Vec<usize>> {
    let mut parent: Vec<usize> = (0..directives.len()).collect();
    let mut first_directive_by_name: BTreeMap<String, usize> = BTreeMap::new();

    for (directive_index, directive) in directives.iter().enumerate() {
        for arg in &directive.args {
            let Some(name) = &arg.name else {
                continue;
            };
            if let Some(first_directive) =
                first_directive_by_name.get(name).copied()
            {
                union_alternative_group(
                    &mut parent,
                    first_directive,
                    directive_index,
                );
            } else {
                first_directive_by_name.insert(name.clone(), directive_index);
            }
        }
    }

    let mut groups: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for directive_index in 0..directives.len() {
        let root = find_alternative_group(&mut parent, directive_index);
        groups.entry(root).or_default().push(directive_index);
    }
    groups.into_values().collect()
}

fn find_alternative_group(parent: &mut [usize], index: usize) -> usize {
    if parent[index] != index {
        let root = find_alternative_group(parent, parent[index]);
        parent[index] = root;
    }
    parent[index]
}

fn union_alternative_group(parent: &mut [usize], left: usize, right: usize) {
    let left_root = find_alternative_group(parent, left);
    let right_root = find_alternative_group(parent, right);
    if left_root == right_root {
        return;
    }
    if left_root < right_root {
        parent[right_root] = left_root;
    } else {
        parent[left_root] = right_root;
    }
}

fn alternative_singleton_axis(
    directives: &[AlternativeDirective],
    directive_index: usize,
) -> AlternativeAxis {
    let directive = &directives[directive_index];
    let mut args = directive.args.clone();
    if args.len() == 1 {
        args.push(DirectiveArg {
            name: None,
            value: String::new(),
        });
    }
    let variants = allocate_alternative_branch_ids(args)
        .into_iter()
        .map(|branch| AlternativeVariant {
            id: branch.id,
            replacements: vec![AlternativeReplacement {
                directive_index,
                value: branch.value,
            }],
        })
        .collect();
    AlternativeAxis {
        start: directive.start,
        variants,
    }
}

fn alternative_correlated_axis(
    directives: &[AlternativeDirective],
    group: &[usize],
) -> AlternativeAxis {
    let allocated =
        allocate_correlated_alternative_branch_ids(directives, group);
    let mut variant_keys = Vec::new();
    let mut seen_keys = BTreeSet::new();
    let mut values_by_directive: BTreeMap<usize, BTreeMap<String, String>> =
        BTreeMap::new();

    for (directive_index, branches) in allocated {
        let mut values_by_id = BTreeMap::new();
        for branch in branches {
            if seen_keys.insert(branch.id.clone()) {
                variant_keys.push(branch.id.clone());
            }
            values_by_id.entry(branch.id).or_insert(branch.value);
        }
        values_by_directive.insert(directive_index, values_by_id);
    }

    let variants = variant_keys
        .into_iter()
        .map(|key| {
            let replacements = group
                .iter()
                .map(|directive_index| AlternativeReplacement {
                    directive_index: *directive_index,
                    value: values_by_directive
                        .get(directive_index)
                        .and_then(|values_by_id| values_by_id.get(&key))
                        .cloned()
                        .unwrap_or_default(),
                })
                .collect();
            AlternativeVariant {
                id: key,
                replacements,
            }
        })
        .collect();

    AlternativeAxis {
        start: group
            .iter()
            .map(|directive_index| directives[*directive_index].start)
            .min()
            .unwrap_or(0),
        variants,
    }
}

fn allocate_correlated_alternative_branch_ids(
    directives: &[AlternativeDirective],
    group: &[usize],
) -> Vec<(usize, Vec<AlternativeBranch>)> {
    let named_ids: BTreeSet<String> = group
        .iter()
        .flat_map(|directive_index| directives[*directive_index].args.iter())
        .filter_map(|arg| arg.name.clone())
        .collect();
    let mut next_numeric = 1_u32;

    group
        .iter()
        .map(|directive_index| {
            let branches = directives[*directive_index]
                .args
                .iter()
                .map(|arg| {
                    let id = match &arg.name {
                        Some(name) => name.clone(),
                        None => {
                            while named_ids.contains(&next_numeric.to_string())
                            {
                                next_numeric += 1;
                            }
                            let id = next_numeric.to_string();
                            next_numeric += 1;
                            id
                        }
                    };
                    AlternativeBranch {
                        value: arg.value.clone(),
                        id,
                    }
                })
                .collect();
            (*directive_index, branches)
        })
        .collect()
}

pub(crate) fn render_alternative_prompt(
    prompt: &str,
    directives: &[AlternativeDirective],
    combination: &[AlternativeVariant],
) -> String {
    let mut replacements: Vec<(usize, usize, String)> = combination
        .iter()
        .flat_map(|variant| {
            variant.replacements.iter().map(|replacement| {
                let directive = &directives[replacement.directive_index];
                (directive.start, directive.end, replacement.value.clone())
            })
        })
        .collect();
    replacements.sort_by_key(|replacement| std::cmp::Reverse(replacement.0));
    let has_empty_replacement =
        replacements.iter().any(|(_, _, value)| value.is_empty());

    let mut replaced = prompt.to_string();
    for (start, end, value) in replacements {
        if value.is_empty() {
            replaced.replace_range(start..end, EMPTY_ALT_SENTINEL_STR);
        } else {
            replaced.replace_range(start..end, &value);
        }
    }
    if has_empty_replacement {
        collapse_empty_alternative_whitespace(&replaced)
    } else {
        replaced
    }
}

/// Collapse the horizontal whitespace left by empty alt renders.
///
/// Empty branches remove adjacent spaces/tabs when they would leave doubled
/// spaces, leading/trailing spaces, or a space stranded against punctuation.
/// A single word-separating space is kept only between two alphanumeric
/// neighbors that already had horizontal whitespace at the empty site.
/// Newlines are hard boundaries and line-leading indentation is preserved;
/// spaces that keep a following `%directive` parseable are preserved; non-empty
/// branches never enter this pass.
fn collapse_empty_alternative_whitespace(rendered: &str) -> String {
    if !rendered.contains(EMPTY_ALT_SENTINEL) {
        return rendered.to_string();
    }

    let mut collapsed = String::with_capacity(rendered.len());
    let mut cursor = 0;
    while cursor < rendered.len() {
        let Some(ch) = rendered[cursor..].chars().next() else {
            break;
        };
        if !is_empty_alt_run_char(ch) {
            collapsed.push(ch);
            cursor += ch.len_utf8();
            continue;
        }

        let run_start = cursor;
        let mut run_end = cursor;
        let mut contains_sentinel = false;
        while run_end < rendered.len() {
            let ch = rendered[run_end..].chars().next().unwrap();
            if !is_empty_alt_run_char(ch) {
                break;
            }
            contains_sentinel |= ch == EMPTY_ALT_SENTINEL;
            run_end += ch.len_utf8();
        }

        if contains_sentinel {
            push_collapsed_empty_alt_run(
                rendered,
                run_start,
                run_end,
                &mut collapsed,
            );
        } else {
            collapsed.push_str(&rendered[run_start..run_end]);
        }
        cursor = run_end;
    }
    collapsed
}

fn push_collapsed_empty_alt_run(
    rendered: &str,
    run_start: usize,
    run_end: usize,
    collapsed: &mut String,
) {
    let line_leading = is_line_start(rendered, run_start);
    let mut collapse_start = run_start;
    if line_leading {
        while collapse_start < run_end {
            let ch = rendered[collapse_start..].chars().next().unwrap();
            if !is_horizontal_ws(ch) {
                break;
            }
            collapse_start += ch.len_utf8();
        }
        collapsed.push_str(&rendered[run_start..collapse_start]);
    }

    let had_horizontal_ws = rendered[collapse_start..run_end]
        .chars()
        .any(is_horizontal_ws);
    let left = if line_leading {
        None
    } else {
        rendered[..run_start].chars().next_back()
    };
    let right = rendered[run_end..].chars().next();

    if (had_horizontal_ws
        && should_preserve_directive_separator(rendered, run_end, left))
        || (had_horizontal_ws
            && left.is_some_and(char::is_alphanumeric)
            && right.is_some_and(char::is_alphanumeric))
    {
        collapsed.push(' ');
    }
}

fn should_preserve_directive_separator(
    rendered: &str,
    run_end: usize,
    left: Option<char>,
) -> bool {
    let Some(left) = left else {
        return false;
    };
    starts_with_directive_marker(&rendered[run_end..])
        && !is_directive_left_boundary(left)
}

fn starts_with_directive_marker(text: &str) -> bool {
    let mut chars = text.chars();
    if chars.next() != Some('%') {
        return false;
    }
    matches!(chars.next(), Some('{') | Some('(') | Some('a'..='z' | 'A'..='Z' | '_'))
}

fn is_directive_left_boundary(ch: char) -> bool {
    ch.is_whitespace() || matches!(ch, '(' | '[' | '{' | '"' | '\'')
}

fn is_empty_alt_run_char(ch: char) -> bool {
    ch == EMPTY_ALT_SENTINEL || is_horizontal_ws(ch)
}

fn is_horizontal_ws(ch: char) -> bool {
    ch == ' ' || ch == '\t'
}

fn is_line_start(rendered: &str, index: usize) -> bool {
    index == 0
        || rendered[..index]
            .chars()
            .next_back()
            .is_some_and(|ch| ch == '\n' || ch == '\r')
}

fn extract_repeat_and_id_rust(
    prompt: &str,
) -> (Option<u32>, Option<String>, Option<String>, String) {
    if !prompt.contains('%') {
        return (None, None, None, prompt.to_string());
    }

    let ignored_ranges = launch_literal_zone_ranges(prompt);
    let mut repeat_count = None;
    let mut explicit_id = None;
    let mut bead_id = None;
    let mut regions = Vec::new();

    for directive in directive_occurrences(prompt).unwrap_or_default() {
        if position_in_ranges(directive.start, &ignored_ranges) {
            continue;
        }
        if directive.canonical_name != "repeat"
            && directive.canonical_name != "id"
        {
            continue;
        }
        regions.push((directive.start, directive.end));
        let raw_arg = if directive.has_plus_suffix {
            "true".to_string()
        } else {
            directive.args.first().cloned().unwrap_or_default()
        };
        if directive.canonical_name == "repeat" {
            repeat_count = raw_arg.parse::<u32>().ok();
        } else {
            for (index, arg) in directive.args.iter().enumerate() {
                let (name, value) = split_named_directive_arg(arg);
                match name.as_deref() {
                    Some("bead") => {
                        bead_id =
                            Some(unquote_directive_arg_value(value.trim()));
                    }
                    None if index == 0 && !arg.is_empty() => {
                        explicit_id = Some(arg.clone());
                    }
                    _ => {}
                }
            }
        }
    }

    if !matches!(repeat_count, Some(count) if count > 1) {
        return (None, None, None, prompt.to_string());
    }

    let mut cleaned = prompt.to_string();
    for (start, end) in regions.into_iter().rev() {
        cleaned.replace_range(start..end, "");
    }
    cleaned = leading_blank_line_re().replace(&cleaned, "").to_string();
    cleaned = strip_disabled_region_markers(&cleaned);
    (repeat_count, explicit_id, bead_id, cleaned)
}

fn has_wait_directive(prompt: &str) -> bool {
    let ignored_ranges = launch_literal_zone_ranges(prompt);
    if prompt.contains('%')
        && directive_occurrences(prompt)
            .unwrap_or_default()
            .iter()
            .any(|directive| {
                directive.canonical_name == "wait"
                    && !position_in_ranges(directive.start, &ignored_ranges)
            })
    {
        return true;
    }
    prompt.contains("#t")
        && xprompt_occurrences(prompt).iter().any(|reference| {
            reference.name == "t"
                && reference.has_time_argument
                && !position_in_ranges(reference.start, &ignored_ranges)
        })
}

pub(crate) fn extract_first_model_value(prompt: &str) -> Option<String> {
    if !prompt.contains('%') {
        return None;
    }
    let ignored_ranges = launch_literal_zone_ranges(prompt);
    for directive in directive_occurrences(prompt).unwrap_or_default() {
        if directive.canonical_name == "model"
            && !position_in_ranges(directive.start, &ignored_ranges)
        {
            let value = directive.args.first()?;
            // Backtick-literal model values keep any `@` verbatim; every other
            // value has its trailing `@<effort>` peeled off so the slot is
            // named by the clean model, matching the Python fan-out namer.
            if directive.from_backtick_literal {
                return Some(value.clone());
            }
            let (clean_model, _) = split_model_effort(value);
            return Some(clean_model.to_string());
        }
    }
    None
}

fn allocate_alternative_branch_ids(
    args: Vec<DirectiveArg>,
) -> Vec<AlternativeBranch> {
    let named_ids: BTreeSet<String> =
        args.iter().filter_map(|arg| arg.name.clone()).collect();
    let mut next_numeric = 1_u32;
    args.into_iter()
        .map(|arg| {
            let id = match arg.name {
                Some(name) => name,
                None => {
                    while named_ids.contains(&next_numeric.to_string()) {
                        next_numeric += 1;
                    }
                    let id = next_numeric.to_string();
                    next_numeric += 1;
                    id
                }
            };
            AlternativeBranch {
                value: arg.value,
                id,
            }
        })
        .collect()
}

fn cartesian_product<T: Clone>(
    lists: &[Vec<T>],
    idx: usize,
    current: &mut Vec<T>,
    out: &mut Vec<Vec<T>>,
) {
    if idx == lists.len() {
        out.push(current.clone());
        return;
    }
    for item in &lists[idx] {
        current.push(item.clone());
        cartesian_product(lists, idx + 1, current, out);
        current.pop();
    }
}

fn plan_multi_prompt_fanout(prompt: &str) -> LaunchFanoutPlanWire {
    let segments = split_multi_prompt_segments(prompt);
    LaunchFanoutPlanWire {
        schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
        launch_kind: "multi_prompt".to_string(),
        slots: segments
            .into_iter()
            .enumerate()
            .map(|(idx, segment)| LaunchFanoutSlotWire {
                wait_for_previous: has_wait_directive(&segment),
                prompt: segment,
                launch_kind: "multi_prompt".to_string(),
                slot_index: idx as u32,
                alt_id: None,
                timestamp: None,
                workflow_name: None,
                model: None,
                repeat_name: None,
                bead_id: None,
            })
            .collect(),
        requires_sequential_naming_wait: true,
        fanout_sleep_seconds: 0.0,
    }
}

fn plan_alternative_fanout(
    prompt: &str,
) -> Result<LaunchFanoutPlanWire, AgentLaunchFanoutPlanError> {
    let slots_with_ids =
        split_prompt_for_alternatives_with_ids(prompt)?.unwrap_or_default();
    Ok(LaunchFanoutPlanWire {
        schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
        launch_kind: "alternatives".to_string(),
        slots: slots_with_ids
            .into_iter()
            .enumerate()
            .map(|(idx, slot)| LaunchFanoutSlotWire {
                wait_for_previous: has_wait_directive(&slot.prompt),
                prompt: slot.prompt,
                launch_kind: "alternatives".to_string(),
                slot_index: idx as u32,
                alt_id: Some(slot.alt_id),
                timestamp: None,
                workflow_name: None,
                model: None,
                repeat_name: None,
                bead_id: None,
            })
            .collect(),
        requires_sequential_naming_wait: false,
        fanout_sleep_seconds: 0.0,
    })
}

fn plan_model_fanout(
    prompt: &str,
) -> Result<LaunchFanoutPlanWire, AgentLaunchFanoutPlanError> {
    let slots_with_ids = split_prompt_for_models_with_ids(prompt)?;
    Ok(LaunchFanoutPlanWire {
        schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
        launch_kind: "model".to_string(),
        slots: slots_with_ids
            .into_iter()
            .enumerate()
            .map(|(idx, slot)| {
                let model = extract_first_model_value(&slot.prompt);
                LaunchFanoutSlotWire {
                    wait_for_previous: has_wait_directive(&slot.prompt),
                    prompt: slot.prompt,
                    launch_kind: "model".to_string(),
                    slot_index: idx as u32,
                    alt_id: Some(slot.alt_id),
                    timestamp: None,
                    workflow_name: None,
                    model,
                    repeat_name: None,
                    bead_id: None,
                }
            })
            .collect(),
        requires_sequential_naming_wait: false,
        fanout_sleep_seconds: 0.0,
    })
}

fn plan_repeat_fanout(prompt: &str) -> LaunchFanoutPlanWire {
    let (count, explicit_id, bead_id, stripped) =
        extract_repeat_and_id_rust(prompt);
    let slots = match count {
        Some(count) if count > 1 => (0..count)
            .map(|idx| LaunchFanoutSlotWire {
                prompt: stripped.clone(),
                launch_kind: "repeat".to_string(),
                slot_index: idx,
                alt_id: None,
                timestamp: None,
                workflow_name: None,
                model: None,
                repeat_name: explicit_id.clone(),
                bead_id: bead_id.clone(),
                wait_for_previous: idx > 0,
            })
            .collect(),
        _ => Vec::new(),
    };
    LaunchFanoutPlanWire {
        schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
        launch_kind: "repeat".to_string(),
        slots,
        requires_sequential_naming_wait: false,
        fanout_sleep_seconds: 0.0,
    }
}
