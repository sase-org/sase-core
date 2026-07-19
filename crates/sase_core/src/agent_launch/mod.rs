//! Wire records and deterministic helpers for agent launch.

use crate::effort::split_model_effort;
use crate::prompt_literals::inline_code_ranges;
use chrono::{Duration, NaiveDateTime};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::Write;
use std::path::Path;
use std::sync::OnceLock;

pub const AGENT_LAUNCH_WIRE_SCHEMA_VERSION: u32 = 1;
const EMPTY_ALT_SENTINEL: char = '\u{E000}';
const EMPTY_ALT_SENTINEL_STR: &str = "\u{E000}";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceClaimWire {
    pub workspace_num: u32,
    pub workflow: String,
    #[serde(default)]
    pub cl_name: Option<String>,
    pub pid: u32,
    #[serde(default)]
    pub artifacts_timestamp: Option<String>,
    #[serde(default)]
    pub pinned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceClaimRequestWire {
    pub project_file: String,
    pub workspace_num: u32,
    pub workflow_name: String,
    pub pid: u32,
    #[serde(default)]
    pub cl_name: String,
    #[serde(default)]
    pub artifacts_timestamp: String,
    #[serde(default)]
    pub transfer_from_pid: Option<u32>,
    #[serde(default)]
    pub pinned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceClaimOutcomeWire {
    pub success: bool,
    pub workspace_num: u32,
    pub project_file: String,
    #[serde(default)]
    pub pid: Option<u32>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceClaimPlanWire {
    pub content: String,
    pub outcome: WorkspaceClaimOutcomeWire,
    pub changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentLaunchRequestWire {
    pub schema_version: u32,
    pub cl_name: String,
    pub project_file: String,
    pub workspace_dir: String,
    pub workspace_num: u32,
    pub workflow_name: String,
    pub prompt: String,
    pub timestamp: String,
    #[serde(default)]
    pub update_target: String,
    #[serde(default)]
    pub project_name: String,
    #[serde(default)]
    pub history_sort_key: String,
    #[serde(default)]
    pub is_home_mode: bool,
    #[serde(default)]
    pub vcs_workflow_type: Option<String>,
    #[serde(default)]
    pub vcs_ref: Option<String>,
    #[serde(default)]
    pub deferred_workspace: bool,
    #[serde(default)]
    pub local_xprompts_file: Option<String>,
    #[serde(default)]
    pub extra_env: BTreeMap<String, String>,
    #[serde(default)]
    pub retry_transfer_from_pid: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentLaunchPreparedWire {
    pub schema_version: u32,
    pub prompt_file: String,
    pub output_path: String,
    pub safe_name: String,
    #[serde(default)]
    pub argv: Vec<String>,
    pub cwd: String,
    #[serde(default)]
    pub env_delta: BTreeMap<String, String>,
    #[serde(default)]
    pub claim_request: Option<WorkspaceClaimRequestWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LaunchFanoutSlotWire {
    pub prompt: String,
    pub launch_kind: String,
    pub slot_index: u32,
    #[serde(default)]
    pub alt_id: Option<String>,
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub workflow_name: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub repeat_name: Option<String>,
    #[serde(default)]
    pub wait_for_previous: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LaunchFanoutPlanWire {
    pub schema_version: u32,
    pub launch_kind: String,
    #[serde(default)]
    pub slots: Vec<LaunchFanoutSlotWire>,
    #[serde(default)]
    pub requires_sequential_naming_wait: bool,
    #[serde(default)]
    pub fanout_sleep_seconds: f64,
}

#[derive(Debug)]
pub enum AgentLaunchPreparationError {
    SchemaVersion { expected: u32, actual: u32 },
    CreateTempFile(std::io::Error),
    WritePrompt(std::io::Error),
    KeepTempFile(std::io::Error),
    CreateOutputRoot(std::io::Error),
}

impl fmt::Display for AgentLaunchPreparationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaVersion { expected, actual } => write!(
                f,
                "unsupported AgentLaunchRequestWire schema_version {actual}; expected {expected}"
            ),
            Self::CreateTempFile(err) => {
                write!(f, "failed to create prompt temp file: {err}")
            }
            Self::WritePrompt(err) => {
                write!(f, "failed to write prompt temp file: {err}")
            }
            Self::KeepTempFile(err) => {
                write!(f, "failed to keep prompt temp file: {err}")
            }
            Self::CreateOutputRoot(err) => {
                write!(f, "failed to create launch output root: {err}")
            }
        }
    }
}

impl std::error::Error for AgentLaunchPreparationError {}

#[derive(Debug)]
pub enum TimestampBatchAllocationError {
    InvalidTimestamp {
        field: &'static str,
        value: String,
        error: chrono::ParseError,
    },
}

impl fmt::Display for TimestampBatchAllocationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTimestamp {
                field,
                value,
                error,
            } => write!(
                f,
                "invalid {field} launch timestamp {value:?}; expected YYmmdd_HHMMSS: {error}"
            ),
        }
    }
}

impl std::error::Error for TimestampBatchAllocationError {}

#[derive(Debug)]
pub enum AgentLaunchFanoutPlanError {
    UnsupportedKind(String),
    MultiModelUnsupported(String),
    UnclosedDirective { name: String, close: char },
}

impl fmt::Display for AgentLaunchFanoutPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedKind(kind) => {
                write!(f, "unsupported launch fan-out kind {kind:?}")
            }
            Self::MultiModelUnsupported(message) => write!(f, "{message}"),
            Self::UnclosedDirective { name, close } => {
                write!(
                    f,
                    "unclosed {name} directive: missing closing '{close}'"
                )
            }
        }
    }
}

impl std::error::Error for AgentLaunchFanoutPlanError {}

pub fn allocate_launch_timestamp_batch(
    count: usize,
    base_timestamp: &str,
    after_timestamp: Option<&str>,
) -> Result<Vec<String>, TimestampBatchAllocationError> {
    if count == 0 {
        return Ok(Vec::new());
    }

    let base = parse_launch_timestamp("base_timestamp", base_timestamp)?;
    let start = match after_timestamp {
        Some(after) if !after.is_empty() => {
            let after = parse_launch_timestamp("after_timestamp", after)?;
            std::cmp::max(base, after + Duration::seconds(1))
        }
        _ => base,
    };

    Ok((0..count)
        .map(|offset| {
            (start + Duration::seconds(offset as i64))
                .format("%y%m%d_%H%M%S")
                .to_string()
        })
        .collect())
}

fn parse_launch_timestamp(
    field: &'static str,
    value: &str,
) -> Result<NaiveDateTime, TimestampBatchAllocationError> {
    NaiveDateTime::parse_from_str(value, "%y%m%d_%H%M%S").map_err(|error| {
        TimestampBatchAllocationError::InvalidTimestamp {
            field,
            value: value.to_string(),
            error,
        }
    })
}

#[derive(Debug, Clone)]
struct DirectiveOccurrence {
    canonical_name: String,
    start: usize,
    end: usize,
    args: Vec<String>,
    has_plus_suffix: bool,
    // True when a single colon argument came from a backtick literal
    // (`` %model:`literal@id` ``). Such values bypass the `@effort` split so any
    // `@` in the model id is preserved, mirroring the Python parser.
    from_backtick_literal: bool,
}

#[derive(Debug, Clone)]
struct XPromptOccurrence {
    name: String,
    start: usize,
    end: usize,
    has_time_argument: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DirectiveArg {
    name: Option<String>,
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AlternativeBranch {
    value: String,
    id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AlternativeDirective {
    start: usize,
    end: usize,
    args: Vec<DirectiveArg>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AlternativeAxis {
    start: usize,
    variants: Vec<AlternativeVariant>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AlternativeVariant {
    id: String,
    replacements: Vec<AlternativeReplacement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AlternativeReplacement {
    directive_index: usize,
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AlternativeSlot {
    prompt: String,
    alt_id: String,
}

/// Which surface form opened an alternative directive. The legacy `%alt(...)`
/// and `%(...)` shorthand use parens with comma-separated branches; the new
/// `%{...}` shorthand uses braces with top-level `|`-separated branches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AltDelimiter {
    Paren,
    Brace,
}

impl AltDelimiter {
    fn open(self) -> char {
        match self {
            Self::Paren => '(',
            Self::Brace => '{',
        }
    }

    fn close(self) -> char {
        match self {
            Self::Paren => ')',
            Self::Brace => '}',
        }
    }

    /// Branch separator inside the directive body.
    fn separator(self) -> char {
        match self {
            Self::Paren => ',',
            Self::Brace => '|',
        }
    }

    /// Human-readable directive name used in unclosed-directive errors.
    fn directive_label(self) -> &'static str {
        match self {
            Self::Paren => "%alt",
            Self::Brace => "%{",
        }
    }
}

pub fn plan_agent_launch_fanout(
    prompt: &str,
    launch_kind: Option<&str>,
) -> Result<LaunchFanoutPlanWire, AgentLaunchFanoutPlanError> {
    let requested = launch_kind.unwrap_or("auto");
    match requested {
        "multi_prompt" => Ok(plan_multi_prompt_fanout(prompt)),
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
    let Some(first_line_end) = prompt.find('\n') else {
        return prompt;
    };
    if prompt[..first_line_end].trim() != "---" {
        return prompt;
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
            return if yaml_like {
                &prompt[line_end..]
            } else {
                prompt
            };
        }
        if content.contains(':') {
            yaml_like = true;
        }
        offset = line_end;
    }
    prompt
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

fn render_alternative_prompt(
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

fn extract_repeat_and_name_rust(
    prompt: &str,
) -> (Option<u32>, Option<String>, String) {
    if !prompt.contains('%') {
        return (None, None, prompt.to_string());
    }

    let ignored_ranges = launch_literal_zone_ranges(prompt);
    let mut repeat_count = None;
    let mut explicit_name = None;
    let mut regions = Vec::new();

    for directive in directive_occurrences(prompt).unwrap_or_default() {
        if position_in_ranges(directive.start, &ignored_ranges) {
            continue;
        }
        if directive.canonical_name != "repeat"
            && directive.canonical_name != "name"
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
            explicit_name = if raw_arg.is_empty() {
                None
            } else {
                Some(raw_arg)
            };
        }
    }

    if !matches!(repeat_count, Some(count) if count > 1) {
        return (None, None, prompt.to_string());
    }

    let mut cleaned = prompt.to_string();
    for (start, end) in regions.into_iter().rev() {
        cleaned.replace_range(start..end, "");
    }
    cleaned = leading_blank_line_re().replace(&cleaned, "").to_string();
    cleaned = strip_disabled_region_markers(&cleaned);
    (repeat_count, explicit_name, cleaned)
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

fn extract_first_model_value(prompt: &str) -> Option<String> {
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

fn directive_occurrences(
    prompt: &str,
) -> Result<Vec<DirectiveOccurrence>, AgentLaunchFanoutPlanError> {
    let mut out = Vec::new();
    for caps in directive_re().captures_iter(prompt) {
        let marker = caps.get(2).expect("directive marker group");
        let raw_name = caps.get(3).expect("directive name group").as_str();
        let canonical_name = canonical_directive_name(raw_name).to_string();
        let mut end = marker.end();
        let mut args = Vec::new();
        let mut has_plus_suffix = false;
        let mut from_backtick_literal = false;

        if caps.get(4).is_some() {
            let paren_start = marker.end() - 1;
            if let Some(paren_end) = find_matching_paren(prompt, paren_start) {
                args =
                    parse_directive_args(&prompt[paren_start + 1..paren_end]);
                end = paren_end + 1;
            }
        } else if let Some(colon_arg) = caps.get(5) {
            from_backtick_literal = colon_arg.as_str().starts_with('`');
            args = vec![unquote_backticks(colon_arg.as_str())];
        } else if caps.get(6).is_some() {
            has_plus_suffix = true;
            args = vec!["true".to_string()];
        } else {
            args = vec![String::new()];
        }

        out.push(DirectiveOccurrence {
            canonical_name,
            start: marker.start(),
            end,
            args,
            has_plus_suffix,
            from_backtick_literal,
        });
    }
    Ok(out)
}

fn xprompt_occurrences(prompt: &str) -> Vec<XPromptOccurrence> {
    xprompt_reference_re()
        .captures_iter(prompt)
        .filter_map(|captures| {
            let marker = captures.get(2)?;
            let name_match = captures.get(3)?;
            let name = name_match.as_str().replace("__", "/");
            let has_time_argument = prompt
                .as_bytes()
                .get(name_match.end())
                .is_some_and(|byte| matches!(byte, b':' | b'('));
            let mut end = marker.end();
            if captures.get(4).is_some() {
                let paren_start = marker.end() - 1;
                if let Some(paren_end) =
                    find_matching_paren(prompt, paren_start)
                {
                    end = paren_end + 1;
                }
            }
            Some(XPromptOccurrence {
                name,
                start: marker.start(),
                end,
                has_time_argument,
            })
        })
        .collect()
}

fn launch_literal_zone_ranges(prompt: &str) -> Vec<(usize, usize)> {
    let mut ranges = fenced_block_ranges(prompt);
    ranges.extend(disabled_region_ranges(prompt));
    if prompt.contains('`') {
        ranges.extend(launch_inline_literal_ranges(prompt));
    }
    ranges
}

fn launch_inline_literal_ranges(prompt: &str) -> Vec<(usize, usize)> {
    let mut masks = fenced_block_ranges(prompt);
    masks.extend(disabled_region_ranges(prompt));
    masks.extend(
        directive_occurrences(prompt)
            .unwrap_or_default()
            .into_iter()
            .map(|directive| (directive.start, directive.end)),
    );
    masks.extend(
        xprompt_occurrences(prompt)
            .into_iter()
            .map(|reference| (reference.start, reference.end)),
    );
    for (start, open_start, delimiter) in alt_directive_starts(prompt) {
        if let Some(close_end) = find_matching_delimiter(
            prompt,
            open_start,
            delimiter.open(),
            delimiter.close(),
        ) {
            masks.push((start, close_end + 1));
        }
    }
    inline_code_ranges(prompt, &masks)
}

fn alt_directive_starts(prompt: &str) -> Vec<(usize, usize, AltDelimiter)> {
    alt_directive_re()
        .captures_iter(prompt)
        .filter_map(|caps| {
            let marker = caps.get(2)?;
            let open = marker.end() - 1;
            let delimiter = if prompt.as_bytes()[open] == b'{' {
                AltDelimiter::Brace
            } else {
                AltDelimiter::Paren
            };
            Some((marker.start(), open, delimiter))
        })
        .collect()
}

fn alt_inner_ranges(
    prompt: &str,
    ignored_ranges: &[(usize, usize)],
) -> Result<Vec<(usize, usize)>, AgentLaunchFanoutPlanError> {
    let mut ranges = Vec::new();
    for (start, open_start, delimiter) in alt_directive_starts(prompt) {
        if position_in_ranges(start, ignored_ranges) {
            continue;
        }
        if let Some(close_end) = find_matching_delimiter(
            prompt,
            open_start,
            delimiter.open(),
            delimiter.close(),
        ) {
            ranges.push((open_start + 1, close_end));
        }
    }
    Ok(ranges)
}

fn parse_directive_args(inner: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut start = 0;
    let mut depth = 0_i32;
    let mut in_backticks = false;
    for (idx, ch) in inner.char_indices() {
        if ch == '`' {
            in_backticks = !in_backticks;
            continue;
        }
        if in_backticks {
            continue;
        }
        match ch {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' if depth > 0 => depth -= 1,
            ',' if depth == 0 => {
                push_arg(&mut args, &inner[start..idx]);
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    push_arg(&mut args, &inner[start..]);
    args.into_iter().filter(|arg| !arg.is_empty()).collect()
}

fn parse_directive_args_with_names(
    inner: &str,
    separator: char,
) -> Vec<DirectiveArg> {
    let mut args = Vec::new();
    let mut start = 0;
    let mut depth = 0_i32;
    let mut in_backticks = false;
    for (idx, ch) in inner.char_indices() {
        if ch == '`' {
            in_backticks = !in_backticks;
            continue;
        }
        if in_backticks {
            continue;
        }
        match ch {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' if depth > 0 => depth -= 1,
            _ if ch == separator && depth == 0 => {
                push_directive_arg(&mut args, &inner[start..idx]);
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    push_directive_arg(&mut args, &inner[start..]);
    args.into_iter()
        .filter(|arg| !arg.value.is_empty() || arg.name.is_some())
        .collect()
}

fn push_directive_arg(args: &mut Vec<DirectiveArg>, raw: &str) {
    let trimmed = raw.trim();
    let (name, value_raw) = split_named_directive_arg(trimmed);
    let value_trimmed = value_raw.trim();
    let value = unquote_directive_arg_value(value_trimmed);
    args.push(DirectiveArg { name, value });
}

fn push_arg(args: &mut Vec<String>, raw: &str) {
    let trimmed = raw.trim();
    args.push(unquote_directive_arg_value(trimmed));
}

fn split_named_directive_arg(raw: &str) -> (Option<String>, &str) {
    let mut depth = 0_i32;
    let mut in_backticks = false;
    for (idx, ch) in raw.char_indices() {
        if ch == '`' {
            in_backticks = !in_backticks;
            continue;
        }
        if in_backticks {
            continue;
        }
        match ch {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' if depth > 0 => depth -= 1,
            '=' if depth == 0 => {
                let name = raw[..idx].trim();
                let value = &raw[idx + ch.len_utf8()..];
                if !name.is_empty() {
                    return (Some(unquote_backticks(name)), value);
                }
                return (None, raw);
            }
            _ => {}
        }
    }
    (None, raw)
}

fn unquote_directive_arg_value(trimmed: &str) -> String {
    if trimmed.starts_with("[[")
        && trimmed.ends_with("]]")
        && trimmed.len() >= 4
    {
        trimmed[2..trimmed.len() - 2].to_string()
    } else {
        unquote_backticks(trimmed)
    }
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

fn unquote_backticks(value: &str) -> String {
    if value.starts_with('`') && value.ends_with('`') && value.len() >= 2 {
        value[1..value.len() - 1].to_string()
    } else {
        value.to_string()
    }
}

fn find_matching_paren(text: &str, paren_start: usize) -> Option<usize> {
    find_matching_delimiter(text, paren_start, '(', ')')
}

/// Find the index of the delimiter that closes the `open` character at
/// `open_start`, counting only that delimiter pair and ignoring matches inside
/// backtick-quoted spans.
fn find_matching_delimiter(
    text: &str,
    open_start: usize,
    open: char,
    close: char,
) -> Option<usize> {
    let mut depth = 0_i32;
    let mut in_backticks = false;
    for (rel_idx, ch) in text[open_start..].char_indices() {
        let idx = open_start + rel_idx;
        if ch == '`' {
            in_backticks = !in_backticks;
            continue;
        }
        if in_backticks {
            continue;
        }
        if ch == open {
            depth += 1;
        } else if ch == close {
            depth -= 1;
            if depth == 0 {
                return Some(idx);
            }
        }
    }
    None
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

fn fenced_block_ranges(text: &str) -> Vec<(usize, usize)> {
    let bytes = text.as_bytes();
    let mut ranges = Vec::new();
    let mut i = 0;
    while i + 2 < bytes.len() {
        if bytes[i] != b'`' {
            i += 1;
            continue;
        }
        let mut tick_count = 1;
        while i + tick_count < bytes.len() && bytes[i + tick_count] == b'`' {
            tick_count += 1;
        }
        if tick_count < 3 {
            i += tick_count;
            continue;
        }
        let fence = "`".repeat(tick_count);
        let search_start = i + tick_count;
        if let Some(rel_end) = text[search_start..].find(&fence) {
            let end = search_start + rel_end + tick_count;
            ranges.push((i, end));
            i = end;
        } else {
            break;
        }
    }
    ranges
}

fn disabled_region_ranges(text: &str) -> Vec<(usize, usize)> {
    disabled_region_re()
        .find_iter(text)
        .map(|m| (m.start(), m.end()))
        .collect()
}

fn strip_disabled_region_markers(text: &str) -> String {
    disabled_marker_re().replace_all(text, "").to_string()
}

fn position_in_ranges(pos: usize, ranges: &[(usize, usize)]) -> bool {
    ranges
        .iter()
        .any(|(start, end)| *start <= pos && pos < *end)
}

/// Canonicalize a directive name for fan-out planning by deferring to the
/// shared editor directive registry. This keeps the planner in lock-step with
/// the advertised directive set, including `%a`→`auto`, instead of maintaining
/// a second alias table that can drift. Unknown names pass through unchanged.
fn canonical_directive_name(name: &str) -> &str {
    crate::editor::directive::canonical_directive_name(name).unwrap_or(name)
}

fn directive_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        // The colon-arg class includes `@` so a `%model:<model>@<effort>`
        // suffix is captured as one directive value (matching the Python
        // `_DIRECTIVE_PATTERN`); the `@effort` token is split off in
        // `extract_first_model_value` via `split_model_effort`.
        Regex::new(
            r#"(?m)(^|[\s\(\[\{"'])(%([A-Za-z_][A-Za-z0-9_]*)(?:(\()|:(`[^`]*`|[A-Za-z0-9_#/.,()@-]*[A-Za-z0-9_#/,()@-])|(\+))?)"#,
        )
        .unwrap()
    })
}

fn xprompt_reference_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?m)(^|[\s\(\[\{"'])(#!?([A-Za-z_][A-Za-z0-9_]*(?:/[A-Za-z_][A-Za-z0-9_]*)*)(?:!!|\?\?)?(?:(\()|:(`[^`]*`|\$\([^)]*\)|\{\{[^}]*\}\}|\{[^}]*\}|[A-Za-z0-9_.~,+/@-]*[A-Za-z0-9_~,+/@-])|(\+))?)"#,
        )
        .unwrap()
    })
}

fn alt_directive_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"(?m)(^|[\s\(\[\{"':])(%(?:alt)?\(|%\{)"#).unwrap()
    })
}

fn disabled_region_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?ms)^[ \t]*%xprompts_enabled:false[ \t]*\n.*?(?:^[ \t]*|[ \t]+)%xprompts_enabled:true[ \t]*\n?",
        )
        .unwrap()
    })
}

fn disabled_marker_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?m)^[ \t]*%xprompts_enabled:(?:false|true)[ \t]*\n?|[ \t]+%xprompts_enabled:(?:false|true)[ \t]*",
        )
        .unwrap()
    })
}

fn leading_blank_line_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^\s*\n").unwrap())
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
                }
            })
            .collect(),
        requires_sequential_naming_wait: false,
        fanout_sleep_seconds: 0.0,
    })
}

fn plan_repeat_fanout(prompt: &str) -> LaunchFanoutPlanWire {
    let (count, explicit_name, stripped) = extract_repeat_and_name_rust(prompt);
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
                repeat_name: explicit_name.clone(),
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

pub fn prepare_agent_launch(
    request: &AgentLaunchRequestWire,
    python_executable: &str,
    runner_script: &str,
    sase_tmpdir: Option<&str>,
    output_root: &str,
    preallocated_env: &BTreeMap<String, String>,
) -> Result<AgentLaunchPreparedWire, AgentLaunchPreparationError> {
    if request.schema_version != AGENT_LAUNCH_WIRE_SCHEMA_VERSION {
        return Err(AgentLaunchPreparationError::SchemaVersion {
            expected: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
            actual: request.schema_version,
        });
    }

    let prompt_file =
        write_prompt_temp_file(sase_tmpdir, request.prompt.as_bytes())?;
    let safe_name = safe_launch_name(&request.cl_name);
    let output_root_path = Path::new(output_root);
    std::fs::create_dir_all(output_root_path)
        .map_err(AgentLaunchPreparationError::CreateOutputRoot)?;
    let output_path = output_root_path
        .join(format!("{safe_name}_ace-run-{}.txt", request.timestamp))
        .to_string_lossy()
        .into_owned();

    let mut env_delta = request.extra_env.clone();
    env_delta.insert("SASE_AGENT".to_string(), "1".to_string());
    env_delta.insert("SASE_AGENT_CL_NAME".to_string(), request.cl_name.clone());
    env_delta.insert(
        "SASE_AGENT_PROJECT_FILE".to_string(),
        request.project_file.clone(),
    );
    env_delta.insert(
        "SASE_AGENT_TIMESTAMP".to_string(),
        request.timestamp.clone(),
    );

    if request.deferred_workspace {
        env_delta.insert(
            "SASE_AGENT_DEFERRED_WORKSPACE".to_string(),
            "1".to_string(),
        );
        if let Some(workflow_type) = request.vcs_workflow_type.as_ref() {
            env_delta.insert(
                "SASE_AGENT_VCS_WORKFLOW_TYPE".to_string(),
                workflow_type.clone(),
            );
        }
    }

    for (key, value) in preallocated_env {
        env_delta.insert(key.clone(), value.clone());
    }

    if let Some(local_xprompts_file) = request.local_xprompts_file.as_ref() {
        env_delta.insert(
            "SASE_AGENT_LOCAL_XPROMPTS".to_string(),
            local_xprompts_file.clone(),
        );
    }

    let prompt_file_str = prompt_file.to_string_lossy().into_owned();
    let argv = vec![
        python_executable.to_string(),
        runner_script.to_string(),
        request.cl_name.clone(),
        request.project_file.clone(),
        request.workspace_dir.clone(),
        output_path.clone(),
        request.workspace_num.to_string(),
        request.workflow_name.clone(),
        prompt_file_str.clone(),
        request.timestamp.clone(),
        request.update_target.clone(),
        request.project_name.clone(),
        request.history_sort_key.clone(),
        if request.is_home_mode {
            "1".to_string()
        } else {
            String::new()
        },
    ];

    let claim_request = if request.is_home_mode {
        None
    } else {
        Some(WorkspaceClaimRequestWire {
            project_file: request.project_file.clone(),
            workspace_num: if request.deferred_workspace {
                0
            } else {
                request.workspace_num
            },
            workflow_name: request.workflow_name.clone(),
            pid: 0,
            cl_name: request.cl_name.clone(),
            artifacts_timestamp: String::new(),
            transfer_from_pid: request.retry_transfer_from_pid,
            pinned: false,
        })
    };

    Ok(AgentLaunchPreparedWire {
        schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
        prompt_file: prompt_file_str,
        output_path,
        safe_name,
        argv,
        cwd: request.workspace_dir.clone(),
        env_delta,
        claim_request,
    })
}

pub fn safe_launch_name(cl_name: &str) -> String {
    cl_name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn write_prompt_temp_file(
    sase_tmpdir: Option<&str>,
    prompt: &[u8],
) -> Result<std::path::PathBuf, AgentLaunchPreparationError> {
    let mut builder = tempfile::Builder::new();
    builder.prefix("sase_ace_prompt_").suffix(".md");
    let mut file = match sase_tmpdir {
        Some(dir) if !dir.is_empty() => builder
            .tempfile_in(dir)
            .map_err(AgentLaunchPreparationError::CreateTempFile)?,
        _ => builder
            .tempfile()
            .map_err(AgentLaunchPreparationError::CreateTempFile)?,
    };
    file.write_all(prompt)
        .map_err(AgentLaunchPreparationError::WritePrompt)?;
    let (_file, path) = file
        .keep()
        .map_err(|err| AgentLaunchPreparationError::KeepTempFile(err.error))?;
    Ok(path)
}

pub fn list_workspace_claims_from_content(
    content: &str,
) -> Vec<WorkspaceClaimWire> {
    let mut claims = Vec::new();
    let mut in_running_field = false;

    for line in content.split('\n') {
        if line.starts_with("RUNNING:") {
            in_running_field = true;
            continue;
        }
        if !in_running_field {
            continue;
        }
        if !is_running_continuation_line(line) {
            break;
        }
        if let Some(claim) = WorkspaceClaimLine::parse(line) {
            claims.push(claim.into_wire());
        }
    }

    claims
}

pub fn plan_claim_workspace_from_content(
    content: &str,
    request: &WorkspaceClaimRequestWire,
) -> WorkspaceClaimPlanWire {
    let mut lines: Vec<String> =
        content.split('\n').map(ToString::to_string).collect();
    let (_running_idx, running_end_idx) = find_running_field_bounds(&lines);

    if request.workspace_num != 0 {
        for line in running_claim_lines(&lines) {
            if let Some(existing) = WorkspaceClaimLine::parse(line) {
                if existing.workspace_num == request.workspace_num {
                    return claim_plan(
                        content.to_string(),
                        false,
                        request,
                        Some(format!(
                            "workspace #{} is already claimed",
                            request.workspace_num
                        )),
                        false,
                    );
                }
            }
        }
    }

    let new_claim = WorkspaceClaimLine::from_request(request);
    if let Some(end) = running_end_idx {
        lines.insert(end + 1, new_claim.to_line());
    } else {
        lines.insert(0, String::new());
        lines.insert(0, new_claim.to_line());
        lines.insert(0, "RUNNING:".to_string());
    }

    claim_plan(
        normalize_running_field_spacing(&lines.join("\n")),
        true,
        request,
        None,
        true,
    )
}

pub fn plan_transfer_workspace_claim_from_content(
    content: &str,
    request: &WorkspaceClaimRequestWire,
) -> WorkspaceClaimPlanWire {
    let Some(from_pid) = request.transfer_from_pid else {
        return claim_plan(
            content.to_string(),
            false,
            request,
            Some("transfer_from_pid is required".to_string()),
            false,
        );
    };

    let mut lines: Vec<String> =
        content.split('\n').map(ToString::to_string).collect();
    let mut in_running_field = false;

    for line in &mut lines {
        if line.starts_with("RUNNING:") {
            in_running_field = true;
            continue;
        }
        if in_running_field && is_running_continuation_line(line) {
            if let Some(claim) = WorkspaceClaimLine::parse(line) {
                let cl_matches = request.cl_name.is_empty()
                    || claim.cl_name.as_deref()
                        == Some(request.cl_name.as_str());
                if claim.workspace_num == request.workspace_num
                    && claim.pid == from_pid
                    && cl_matches
                {
                    let replacement = WorkspaceClaimLine {
                        workspace_num: claim.workspace_num,
                        pid: request.pid,
                        workflow: request.workflow_name.clone(),
                        cl_name: claim.cl_name,
                        artifacts_timestamp: if request
                            .artifacts_timestamp
                            .is_empty()
                        {
                            claim.artifacts_timestamp
                        } else {
                            Some(request.artifacts_timestamp.clone())
                        },
                        pinned: claim.pinned,
                    };
                    *line = replacement.to_line();
                    return claim_plan(
                        lines.join("\n"),
                        true,
                        request,
                        None,
                        true,
                    );
                }
            }
        } else {
            in_running_field = false;
        }
    }

    claim_plan(
        content.to_string(),
        false,
        request,
        Some(format!(
            "workspace #{} with pid {from_pid} was not found",
            request.workspace_num
        )),
        false,
    )
}

pub fn allocate_and_claim_workspace_from_content(
    content: &str,
    min_workspace: u32,
    max_workspace: u32,
    request: &WorkspaceClaimRequestWire,
) -> WorkspaceClaimPlanWire {
    let claimed: BTreeSet<u32> = list_workspace_claims_from_content(content)
        .into_iter()
        .map(|claim| claim.workspace_num)
        .collect();
    let Some(workspace_num) =
        (min_workspace..=max_workspace).find(|n| !claimed.contains(n))
    else {
        return claim_plan(
            content.to_string(),
            false,
            request,
            Some(format!(
                "all workspaces ({min_workspace}-{max_workspace}) are claimed"
            )),
            false,
        );
    };

    let mut allocated_request = request.clone();
    allocated_request.workspace_num = workspace_num;
    plan_claim_workspace_from_content(content, &allocated_request)
}

fn claim_plan(
    content: String,
    success: bool,
    request: &WorkspaceClaimRequestWire,
    error: Option<String>,
    changed: bool,
) -> WorkspaceClaimPlanWire {
    WorkspaceClaimPlanWire {
        content,
        outcome: WorkspaceClaimOutcomeWire {
            success,
            workspace_num: request.workspace_num,
            project_file: request.project_file.clone(),
            pid: Some(request.pid),
            error,
        },
        changed,
    }
}

fn running_claim_lines(lines: &[String]) -> impl Iterator<Item = &str> {
    let (start, end) = find_running_field_bounds(lines);
    let start = start.unwrap_or(0);
    let end = end.unwrap_or(0);
    lines
        .iter()
        .enumerate()
        .filter(move |(idx, _)| *idx > start && *idx <= end)
        .map(|(_, line)| line.as_str())
}

fn find_running_field_bounds(
    lines: &[String],
) -> (Option<usize>, Option<usize>) {
    for (i, line) in lines.iter().enumerate() {
        if line.starts_with("RUNNING:") {
            let mut running_end_idx = i;
            for (j, candidate) in lines.iter().enumerate().skip(i + 1) {
                if is_running_continuation_line(candidate) {
                    running_end_idx = j;
                } else {
                    break;
                }
            }
            return (Some(i), Some(running_end_idx));
        }
    }
    (None, None)
}

fn is_running_continuation_line(line: &str) -> bool {
    line.starts_with("  ")
        && (line.trim().starts_with('#') || line.trim().starts_with('|'))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkspaceClaimLine {
    workspace_num: u32,
    pid: u32,
    workflow: String,
    cl_name: Option<String>,
    artifacts_timestamp: Option<String>,
    pinned: bool,
}

impl WorkspaceClaimLine {
    fn parse(line: &str) -> Option<Self> {
        let trimmed = line.trim();
        if !trimmed.starts_with('#') {
            return None;
        }
        let parts: Vec<&str> = trimmed.split('|').map(str::trim).collect();
        if parts.len() < 4 {
            return None;
        }

        let workspace_num = parts[0].strip_prefix('#')?.parse::<u32>().ok()?;
        let pid = parts[1].parse::<u32>().ok()?;
        let workflow = parts[2];
        if workflow.is_empty() {
            return None;
        }

        let mut artifacts_timestamp = None;
        let mut pinned = false;
        for part in parts.iter().skip(4) {
            if *part == "PINNED" {
                pinned = true;
            } else if is_timestamp_part(part) {
                artifacts_timestamp = Some((*part).to_string());
            } else {
                return None;
            }
        }

        Some(Self {
            workspace_num,
            pid,
            workflow: workflow.to_string(),
            cl_name: if parts[3].is_empty() {
                None
            } else {
                Some(parts[3].to_string())
            },
            artifacts_timestamp,
            pinned,
        })
    }

    fn from_request(request: &WorkspaceClaimRequestWire) -> Self {
        Self {
            workspace_num: request.workspace_num,
            pid: request.pid,
            workflow: request.workflow_name.clone(),
            cl_name: if request.cl_name.is_empty() {
                None
            } else {
                Some(request.cl_name.clone())
            },
            artifacts_timestamp: if request.artifacts_timestamp.is_empty() {
                None
            } else {
                Some(request.artifacts_timestamp.clone())
            },
            pinned: request.pinned,
        }
    }

    fn into_wire(self) -> WorkspaceClaimWire {
        WorkspaceClaimWire {
            workspace_num: self.workspace_num,
            workflow: self.workflow,
            cl_name: self.cl_name,
            pid: self.pid,
            artifacts_timestamp: self.artifacts_timestamp,
            pinned: self.pinned,
        }
    }

    fn to_line(&self) -> String {
        let cl_part = self.cl_name.as_deref().unwrap_or("");
        let ts_part = self
            .artifacts_timestamp
            .as_ref()
            .map(|ts| format!(" | {ts}"))
            .unwrap_or_default();
        let pin_part = if self.pinned { " | PINNED" } else { "" };
        format!(
            "  #{} | {} | {} | {}{}{}",
            self.workspace_num,
            self.pid,
            self.workflow,
            cl_part,
            ts_part,
            pin_part
        )
    }
}

fn is_timestamp_part(value: &str) -> bool {
    (value.len() == 14 && value.as_bytes().iter().all(u8::is_ascii_digit))
        || (value.len() == 13
            && value.as_bytes()[0..6].iter().all(u8::is_ascii_digit)
            && value.as_bytes()[6] == b'_'
            && value.as_bytes()[7..13].iter().all(u8::is_ascii_digit))
}

fn normalize_running_field_spacing(content: &str) -> String {
    let lines: Vec<&str> = content.split('\n').collect();
    let mut result_lines = Vec::with_capacity(lines.len());
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i];
        if line.starts_with("RUNNING:") {
            result_lines.push(line.to_string());
            i += 1;
            while i < lines.len() {
                let entry_line = lines[i];
                if entry_line.starts_with("  ")
                    && entry_line.trim().starts_with('#')
                {
                    result_lines.push(entry_line.to_string());
                    i += 1;
                } else {
                    break;
                }
            }
            while i < lines.len() && lines[i].trim().is_empty() {
                i += 1;
            }
            if i < lines.len() {
                result_lines.push(String::new());
                result_lines.push(String::new());
            }
        } else {
            result_lines.push(line.to_string());
            i += 1;
        }
    }

    result_lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request(workspace_num: u32) -> WorkspaceClaimRequestWire {
        WorkspaceClaimRequestWire {
            project_file: "/tmp/project.sase".to_string(),
            workspace_num,
            workflow_name: "run".to_string(),
            pid: 222,
            cl_name: "demo".to_string(),
            artifacts_timestamp: String::new(),
            transfer_from_pid: None,
            pinned: false,
        }
    }

    #[test]
    fn launch_request_round_trips_json_shape() {
        let mut extra_env = BTreeMap::new();
        extra_env.insert("SASE_REPEAT_NAME".to_string(), "task.1".to_string());
        let request = AgentLaunchRequestWire {
            schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
            cl_name: "feature/test".to_string(),
            project_file: "/tmp/project.sase".to_string(),
            workspace_dir: "/tmp/ws".to_string(),
            workspace_num: 2,
            workflow_name: "ace(run)-260501_120000".to_string(),
            prompt: "fix it".to_string(),
            timestamp: "260501_120000".to_string(),
            update_target: "p4head".to_string(),
            project_name: "proj".to_string(),
            history_sort_key: "feature/test".to_string(),
            is_home_mode: false,
            vcs_workflow_type: Some("gh".to_string()),
            vcs_ref: Some("feature/test".to_string()),
            deferred_workspace: true,
            local_xprompts_file: Some("/tmp/xp.json".to_string()),
            extra_env,
            retry_transfer_from_pid: Some(10),
        };

        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(value["schema_version"], json!(1));
        assert_eq!(value["extra_env"]["SASE_REPEAT_NAME"], json!("task.1"));
        let back: AgentLaunchRequestWire =
            serde_json::from_value(value).unwrap();
        assert_eq!(back, request);
    }

    #[test]
    fn prepared_wire_preserves_null_claim_request() {
        let prepared = AgentLaunchPreparedWire {
            schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
            prompt_file: "/tmp/prompt.md".to_string(),
            output_path: "/tmp/out.txt".to_string(),
            safe_name: "home".to_string(),
            argv: vec!["python".to_string()],
            cwd: "/home/user".to_string(),
            env_delta: BTreeMap::new(),
            claim_request: None,
        };
        let value = serde_json::to_value(&prepared).unwrap();
        assert_eq!(value["claim_request"], json!(null));
        let back: AgentLaunchPreparedWire =
            serde_json::from_value(value).unwrap();
        assert_eq!(back, prepared);
    }

    #[test]
    fn fanout_plan_round_trips_slots() {
        let plan = LaunchFanoutPlanWire {
            schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
            launch_kind: "repeat".to_string(),
            slots: vec![LaunchFanoutSlotWire {
                prompt: "%n:task.1\nfix it".to_string(),
                launch_kind: "repeat".to_string(),
                slot_index: 0,
                alt_id: None,
                timestamp: None,
                workflow_name: None,
                model: None,
                repeat_name: Some("task.1".to_string()),
                wait_for_previous: false,
            }],
            requires_sequential_naming_wait: false,
            fanout_sleep_seconds: 1.0,
        };
        let value = serde_json::to_value(&plan).unwrap();
        assert_eq!(value["slots"][0]["repeat_name"], json!("task.1"));
        assert_eq!(value["slots"][0]["alt_id"], json!(null));
        let back: LaunchFanoutPlanWire = serde_json::from_value(value).unwrap();
        assert_eq!(back, plan);
    }

    #[test]
    fn timestamp_batch_allocates_unique_visible_timestamps() {
        let timestamps =
            allocate_launch_timestamp_batch(3, "260501_120000", None).unwrap();

        assert_eq!(
            timestamps,
            vec!["260501_120000", "260501_120001", "260501_120002"]
        );
    }

    #[test]
    fn timestamp_batch_starts_after_previous_allocation() {
        let timestamps = allocate_launch_timestamp_batch(
            2,
            "260501_120000",
            Some("260501_120005"),
        )
        .unwrap();

        assert_eq!(timestamps, vec!["260501_120006", "260501_120007"]);
    }

    #[test]
    fn timestamp_batch_rejects_invalid_format() {
        let err = allocate_launch_timestamp_batch(1, "not-a-timestamp", None)
            .unwrap_err();

        assert!(err.to_string().contains("expected YYmmdd_HHMMSS"));
    }

    #[test]
    fn prepare_agent_launch_writes_prompt_and_shapes_process_data() {
        let tmp = tempfile::tempdir().unwrap();
        let prompt_dir = tmp.path().join("prompts");
        std::fs::create_dir(&prompt_dir).unwrap();
        let output_root = tmp.path().join("workflows").join("202605");
        let mut extra_env = BTreeMap::new();
        extra_env.insert("SASE_AGENT".to_string(), "caller".to_string());
        extra_env.insert("SASE_REPEAT_NAME".to_string(), "task.1".to_string());
        let request = AgentLaunchRequestWire {
            schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
            cl_name: "feature/test".to_string(),
            project_file: "/tmp/project.sase".to_string(),
            workspace_dir: "/tmp/ws".to_string(),
            workspace_num: 4,
            workflow_name: "ace(run)-260501_120000".to_string(),
            prompt: "fix it".to_string(),
            timestamp: "260501_120000".to_string(),
            update_target: "p4head".to_string(),
            project_name: "proj".to_string(),
            history_sort_key: "feature/test".to_string(),
            is_home_mode: false,
            vcs_workflow_type: Some("gh".to_string()),
            vcs_ref: Some("feature/test".to_string()),
            deferred_workspace: false,
            local_xprompts_file: Some("/tmp/xprompts.json".to_string()),
            extra_env,
            retry_transfer_from_pid: Some(99),
        };
        let mut preallocated = BTreeMap::new();
        preallocated.insert("GH_PRE_ALLOCATED".to_string(), "1".to_string());
        preallocated.insert("GH_WORKSPACE_NUM".to_string(), "4".to_string());

        let prepared = prepare_agent_launch(
            &request,
            "/venv/bin/python",
            "/repo/run_agent_runner.py",
            Some(prompt_dir.to_str().unwrap()),
            output_root.to_str().unwrap(),
            &preallocated,
        )
        .unwrap();

        assert_eq!(prepared.safe_name, "feature_test");
        assert_eq!(
            std::fs::read_to_string(&prepared.prompt_file).unwrap(),
            "fix it"
        );
        assert!(prepared
            .prompt_file
            .starts_with(prompt_dir.to_str().unwrap()));
        assert_eq!(
            prepared.output_path,
            output_root
                .join("feature_test_ace-run-260501_120000.txt")
                .to_string_lossy()
        );
        assert_eq!(prepared.argv[0], "/venv/bin/python");
        assert_eq!(prepared.argv[2], "feature/test");
        assert_eq!(prepared.argv[5], prepared.output_path);
        assert_eq!(prepared.argv[8], prepared.prompt_file);
        assert_eq!(prepared.env_delta["SASE_AGENT"], "1");
        assert_eq!(prepared.env_delta["SASE_REPEAT_NAME"], "task.1");
        assert_eq!(prepared.env_delta["GH_PRE_ALLOCATED"], "1");
        assert_eq!(
            prepared.env_delta["SASE_AGENT_LOCAL_XPROMPTS"],
            "/tmp/xprompts.json"
        );
        assert!(!prepared
            .env_delta
            .contains_key("SASE_AGENT_VCS_WORKFLOW_TYPE"));
        assert_eq!(prepared.claim_request.unwrap().transfer_from_pid, Some(99));
    }

    #[test]
    fn prepare_agent_launch_deferred_and_home_claim_shapes() {
        let tmp = tempfile::tempdir().unwrap();
        let mut request = AgentLaunchRequestWire {
            schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
            cl_name: "home".to_string(),
            project_file: "/tmp/home.sase".to_string(),
            workspace_dir: "/home/me".to_string(),
            workspace_num: 9,
            workflow_name: "ace(run)-260501_120000".to_string(),
            prompt: "fix it".to_string(),
            timestamp: "260501_120000".to_string(),
            update_target: String::new(),
            project_name: String::new(),
            history_sort_key: String::new(),
            is_home_mode: false,
            vcs_workflow_type: Some("gh".to_string()),
            vcs_ref: Some("feature/test".to_string()),
            deferred_workspace: true,
            local_xprompts_file: None,
            extra_env: BTreeMap::new(),
            retry_transfer_from_pid: None,
        };

        let deferred = prepare_agent_launch(
            &request,
            "python",
            "runner.py",
            None,
            tmp.path().to_str().unwrap(),
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(deferred.claim_request.unwrap().workspace_num, 0);
        assert_eq!(deferred.env_delta["SASE_AGENT_DEFERRED_WORKSPACE"], "1");
        assert_eq!(deferred.env_delta["SASE_AGENT_VCS_WORKFLOW_TYPE"], "gh");

        request.is_home_mode = true;
        let home = prepare_agent_launch(
            &request,
            "python",
            "runner.py",
            None,
            tmp.path().to_str().unwrap(),
            &BTreeMap::new(),
        )
        .unwrap();
        assert!(home.claim_request.is_none());
        assert_eq!(home.argv[13], "1");
    }

    #[test]
    fn workspace_claims_parse_valid_rows_and_ignore_malformed() {
        let content = "RUNNING:\n  #0 | 111 | wait | deferred | 20260501120000 | PINNED\n  #bad | nope\n  #2 | 222 | run | demo\n\n\nNAME: demo\n";

        let claims = list_workspace_claims_from_content(content);

        assert_eq!(claims.len(), 2);
        assert_eq!(claims[0].workspace_num, 0);
        assert_eq!(
            claims[0].artifacts_timestamp.as_deref(),
            Some("20260501120000")
        );
        assert!(claims[0].pinned);
        assert_eq!(claims[1].workspace_num, 2);
    }

    #[test]
    fn claim_workspace_rejects_duplicate_nonzero_but_allows_zero() {
        let content = "RUNNING:\n  #2 | 111 | run | demo\n\n\nNAME: demo\n";

        let duplicate = plan_claim_workspace_from_content(content, &request(2));
        assert!(!duplicate.outcome.success);
        assert!(!duplicate.changed);

        let zero = plan_claim_workspace_from_content(content, &request(0));
        assert!(zero.outcome.success);
        assert!(zero.content.contains("#0 | 222 | run | demo"));
    }

    #[test]
    fn allocate_and_claim_picks_first_available_workspace() {
        let content = "RUNNING:\n  #100 | 111 | run | a\n  #102 | 333 | run | c\n\n\nNAME: demo\n";
        let mut req = request(0);
        req.cl_name = "b".to_string();
        req.artifacts_timestamp = "20260501120000".to_string();
        req.pinned = true;

        let plan =
            allocate_and_claim_workspace_from_content(content, 100, 102, &req);

        assert!(plan.outcome.success);
        assert_eq!(plan.outcome.workspace_num, 101);
        assert!(plan
            .content
            .contains("#101 | 222 | run | b | 20260501120000 | PINNED"));
    }

    #[test]
    fn transfer_workspace_claim_matches_pid_and_preserves_claim_name() {
        let content = "RUNNING:\n  #101 | 111 | run | demo | 20260501115959\n\n\nNAME: demo\n";
        let mut req = request(101);
        req.workflow_name = "run-retry".to_string();
        req.artifacts_timestamp = "20260501120000".to_string();
        req.transfer_from_pid = Some(111);

        let plan = plan_transfer_workspace_claim_from_content(content, &req);

        assert!(plan.outcome.success);
        assert!(plan
            .content
            .contains("#101 | 222 | run-retry | demo | 20260501120000"));
    }

    #[test]
    fn fanout_planner_splits_multi_prompt_outside_fences() {
        let prompt = "one\n```\n---\n```\n---\n%wait\ntwo";

        let plan =
            plan_agent_launch_fanout(prompt, Some("multi_prompt")).unwrap();

        assert_eq!(plan.launch_kind, "multi_prompt");
        assert_eq!(plan.slots.len(), 2);
        assert!(plan.slots[0].prompt.contains("---"));
        assert_eq!(plan.slots[1].prompt, "%wait\ntwo");
        assert!(plan.slots[1].wait_for_previous);
    }

    #[test]
    fn fanout_planner_time_waits_defer_workspace() {
        let prompt = "%wait(time=5m)\ntwo";

        let plan =
            plan_agent_launch_fanout(prompt, Some("multi_prompt")).unwrap();

        // `%time` is no longer an advertised directive. The time floor now
        // travels through `%wait(time=...)`, which still marks the slot as
        // deferred for workspace allocation.
        assert_eq!(canonical_directive_name("t"), "t");
        assert_eq!(canonical_directive_name("time"), "time");
        // `%edit` was removed and stays a non-special raw name, but `%e` is now
        // the `%effort` alias, so the launch planner canonicalizes it.
        assert_eq!(canonical_directive_name("edit"), "edit");
        assert_eq!(canonical_directive_name("e"), "effort");
        assert_eq!(plan.slots.len(), 1);
        assert!(plan.slots[0].wait_for_previous);
    }

    #[test]
    fn fanout_planner_t_xprompt_defer_workspace() {
        let prompt = "#t:5m\ntwo";

        let plan =
            plan_agent_launch_fanout(prompt, Some("multi_prompt")).unwrap();

        assert_eq!(plan.slots.len(), 1);
        assert!(plan.slots[0].wait_for_previous);
    }

    #[test]
    fn fanout_planner_ignores_wait_forms_inside_adjacent_inline_code() {
        for prompt in [
            "keep `foo`/`%wait` and `#t:5m` literal",
            "prefix`%wait(time=5m)`suffix",
            "bare #t is not a time reference",
        ] {
            let plan =
                plan_agent_launch_fanout(prompt, Some("multi_prompt")).unwrap();
            assert!(!plan.slots[0].wait_for_previous, "prompt was {prompt:?}");
        }

        for prompt in ["#t:`5m` active", "%wait(time=`5m`) active"] {
            let plan =
                plan_agent_launch_fanout(prompt, Some("multi_prompt")).unwrap();
            assert!(plan.slots[0].wait_for_previous, "prompt was {prompt:?}");
        }
    }

    #[test]
    fn fanout_planner_deprecated_time_directive_is_not_special() {
        let prompt = "%time:5m\ntwo";

        let plan =
            plan_agent_launch_fanout(prompt, Some("multi_prompt")).unwrap();

        assert_eq!(plan.slots.len(), 1);
        assert!(!plan.slots[0].wait_for_previous);
    }

    #[test]
    fn fanout_planner_preserves_named_alt_ids_and_values_only() {
        let prompt = "%alt(sec=[[security]],perf=[[performance]])\nReview";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(plan.launch_kind, "alternatives");
        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.slots[0].alt_id.as_deref(), Some("sec"));
        assert_eq!(plan.slots[0].prompt, "security\nReview");
        assert_eq!(plan.slots[1].alt_id.as_deref(), Some("perf"));
        assert_eq!(plan.slots[1].prompt, "performance\nReview");
    }

    #[test]
    fn fanout_planner_allocates_unnamed_alt_ids_after_named_ids() {
        let prompt = "%(fast=a,b,2=c,d)";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("fast"), Some("1"), Some("2"), Some("3")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "c", "d"]
        );
    }

    #[test]
    fn fanout_planner_composes_cartesian_alt_ids() {
        let prompt = "%alt(left=a,right=b) %alt(red=x,blue=y)";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![
                Some("left.red"),
                Some("left.blue"),
                Some("right.red"),
                Some("right.blue")
            ]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["a x", "a y", "b x", "b y"]
        );
    }

    #[test]
    fn fanout_planner_correlates_shared_named_alt_keys() {
        let prompt =
            "#gh:sase %{a=Describe | b=Explain} how this repo works %{a=in detail}.";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("a"), Some("b")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec![
                "#gh:sase Describe how this repo works in detail.",
                "#gh:sase Explain how this repo works."
            ]
        );
    }

    #[test]
    fn fanout_planner_correlates_transitive_alt_keys() {
        let prompt = "%{a=1|b=2} x %{a=3} y %{a=4|b=5}";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("a"), Some("b")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["1 x 3 y 4", "2 x y 5"]
        );
    }

    #[test]
    fn fanout_planner_single_shared_key_collapses_to_one_slot() {
        let prompt = "%{a=X} %{a=Y}";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(plan.slots.len(), 1);
        assert_eq!(plan.slots[0].alt_id.as_deref(), Some("a"));
        assert_eq!(plan.slots[0].prompt, "X Y");
    }

    #[test]
    fn fanout_planner_cartesian_products_independent_correlated_groups() {
        let prompt = "%{a=A | b=B} %{x=X | y=Y} %{a=C} %{x=Z}";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("a.x"), Some("a.y"), Some("b.x"), Some("b.y")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["A X C Z", "A Y C", "B X Z", "B Y"]
        );
    }

    #[test]
    fn fanout_planner_correlated_group_mixes_named_and_unnamed_ids() {
        let prompt = "%{a=X | Y} %{a=Z}";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("a"), Some("1")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["X Z", "Y"]
        );
    }

    #[test]
    fn fanout_planner_rejects_repeated_top_level_models_with_alternatives() {
        let prompt = "%name:foo\n%model:opus\n%model:sonnet %alt(x,y)\nReview";

        let err = plan_agent_launch_fanout(prompt, Some("model")).unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("%model:opus ... %model:sonnet"),
            "message was {message:?}"
        );
        assert!(
            message.contains("use %{%m:opus | %m:sonnet} instead"),
            "message was {message:?}"
        );
    }

    #[test]
    fn fanout_planner_splits_model_branches_and_alternatives() {
        let prompt = "%name:foo\n%{%m:opus | %m:sonnet} %alt(x,y)\nReview";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(plan.slots.len(), 4);
        assert_eq!(plan.slots[0].model.as_deref(), Some("opus"));
        assert_eq!(plan.slots[0].alt_id.as_deref(), Some("1.1"));
        assert!(plan.slots[0].prompt.contains("%m:opus x\nReview"));
        assert_eq!(plan.slots[3].model.as_deref(), Some("sonnet"));
        assert_eq!(plan.slots[3].alt_id.as_deref(), Some("2.2"));
        assert!(plan.slots[3].prompt.contains("%m:sonnet y\nReview"));
    }

    #[test]
    fn extract_first_model_value_strips_known_effort_suffix() {
        // A trailing `@<known-effort>` is peeled off so the slot is named by
        // the clean model, mirroring the Python `split_model_effort` rule.
        assert_eq!(
            extract_first_model_value("%model:opus@xhigh do work"),
            Some("opus".to_string())
        );
        assert_eq!(
            extract_first_model_value("%m:codex/gpt-5.6-sol@low do work"),
            Some("codex/gpt-5.6-sol".to_string())
        );
        // No suffix → unchanged.
        assert_eq!(
            extract_first_model_value("%model:opus do work"),
            Some("opus".to_string())
        );
        // Unknown trailing token is not an effort level → left intact.
        assert_eq!(
            extract_first_model_value("%model:agy/flash@v2 do work"),
            Some("agy/flash@v2".to_string())
        );
        // Backtick-literal model values keep any `@` verbatim.
        assert_eq!(
            extract_first_model_value("%model:`agy/flash@xhigh` do work"),
            Some("agy/flash@xhigh".to_string())
        );
    }

    #[test]
    fn fanout_planner_strips_branch_effort_for_slot_naming() {
        // Per-branch `@effort` fan-out: slots are named by the clean model
        // while each branch body retains its `@effort` token for the launched
        // agent's own directive parsing.
        let prompt = "%{%m:opus@xhigh | %m:sonnet@low} %alt(x,y)\nReview";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(plan.slots.len(), 4);
        assert_eq!(plan.slots[0].model.as_deref(), Some("opus"));
        assert!(plan.slots[0].prompt.contains("%m:opus@xhigh"));
        assert_eq!(plan.slots[3].model.as_deref(), Some("sonnet"));
        assert!(plan.slots[3].prompt.contains("%m:sonnet@low"));
    }

    #[test]
    fn fanout_planner_model_alt_ids_preserve_named_model_branches() {
        let prompt = "%alt(opus=%model:opus,sonnet=%model:sonnet)\nReview";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.slots[0].model.as_deref(), Some("opus"));
        assert_eq!(plan.slots[0].alt_id.as_deref(), Some("opus"));
        assert_eq!(plan.slots[1].model.as_deref(), Some("sonnet"));
        assert_eq!(plan.slots[1].alt_id.as_deref(), Some("sonnet"));
    }

    #[test]
    fn fanout_planner_ignores_models_inside_adjacent_inline_code() {
        let prompt =
            "keep `foo`/`%m:wrong` then %{left=%m:opus | right=%m:sonnet}";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.slots[0].model.as_deref(), Some("opus"));
        assert_eq!(plan.slots[1].model.as_deref(), Some("sonnet"));
        assert!(plan
            .slots
            .iter()
            .all(|slot| slot.prompt.contains("`%m:wrong`")));
    }

    #[test]
    fn launch_inline_scanner_preserves_argument_parser_precedence() {
        let prompt = concat!(
            "#name:`arg with spaces` #research(compare `a` and `b`) ",
            "%model:`custom model` %wait(time=`5m`)"
        );

        assert!(launch_inline_literal_ranges(prompt).is_empty());
    }

    #[test]
    fn fanout_planner_extracts_repeat_slots() {
        let prompt = "%r:3 %n:task %model:opus do work";

        let plan = plan_agent_launch_fanout(prompt, Some("repeat")).unwrap();

        assert_eq!(plan.launch_kind, "repeat");
        assert_eq!(plan.slots.len(), 3);
        assert_eq!(plan.slots[0].repeat_name.as_deref(), Some("task"));
        assert_eq!(plan.slots[0].prompt, "  %model:opus do work");
        assert!(!plan.slots[0].wait_for_previous);
        assert!(plan.slots[1].wait_for_previous);
    }

    #[test]
    fn fanout_planner_ignores_repeat_and_name_inside_adjacent_inline_code() {
        let prompt =
            "keep `foo`/`%r:9` and prefix`%n:wrong`suffix %r:2 %n:right work";

        let plan = plan_agent_launch_fanout(prompt, Some("repeat")).unwrap();

        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.slots[0].repeat_name.as_deref(), Some("right"));
        assert!(plan.slots[0].prompt.contains("`%r:9`"));
        assert!(plan.slots[0].prompt.contains("`%n:wrong`"));
    }

    #[test]
    fn fanout_planner_brace_shorthand_splits_pipe_branches() {
        let prompt = "%{a | b | c}\nReview";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(plan.launch_kind, "alternatives");
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["a\nReview", "b\nReview", "c\nReview"]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("1"), Some("2"), Some("3")]
        );
    }

    #[test]
    fn fanout_planner_ignores_alternative_inside_adjacent_inline_code() {
        let prompt = "keep `foo`/`%{a | b}` then %{x | y}";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.slots[0].prompt, "keep `foo`/`%{a | b}` then x");
        assert_eq!(plan.slots[1].prompt, "keep `foo`/`%{a | b}` then y");
    }

    #[test]
    fn fanout_planner_brace_branch_text_keeps_commas() {
        let prompt = "%{foo, bar | baz}";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.slots[0].prompt, "foo, bar");
        assert_eq!(plan.slots[1].prompt, "baz");
    }

    #[test]
    fn fanout_planner_brace_named_and_numeric_branch_ids() {
        let prompt = "%{fast=a | b | 2=c | d}";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("fast"), Some("1"), Some("2"), Some("3")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "c", "d"]
        );
    }

    #[test]
    fn fanout_planner_brace_named_text_blocks() {
        let prompt = "%{sec=[[security]] | perf=[[performance]]}\nReview";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.slots[0].alt_id.as_deref(), Some("sec"));
        assert_eq!(plan.slots[0].prompt, "security\nReview");
        assert_eq!(plan.slots[1].alt_id.as_deref(), Some("perf"));
        assert_eq!(plan.slots[1].prompt, "performance\nReview");
    }

    #[test]
    fn fanout_planner_brace_single_branch_has_implicit_empty_variant() {
        let prompt = "before %{a} after";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.slots[0].prompt, "before a after");
        assert_eq!(plan.slots[0].alt_id.as_deref(), Some("1"));
        assert_eq!(plan.slots[1].prompt, "before after");
        assert_eq!(plan.slots[1].alt_id.as_deref(), Some("2"));
    }

    #[test]
    fn fanout_planner_empty_branch_removes_space_before_punctuation() {
        let prompt = "works %{extra}.";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["works extra.", "works."]
        );
    }

    #[test]
    fn fanout_planner_empty_branch_collapses_between_words() {
        let prompt = "A %{extra} B";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["A extra B", "A B"]
        );
    }

    #[test]
    fn fanout_planner_empty_branch_removes_leading_space() {
        let prompt = "%{extra} Review";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["extra Review", "Review"]
        );
    }

    #[test]
    fn fanout_planner_empty_branch_removes_trailing_space() {
        let prompt = "Review %{extra}";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["Review extra", "Review"]
        );
    }

    #[test]
    fn render_alternative_prompt_empty_branch_does_not_invent_space() {
        let prompt = "A%B";
        let directives = vec![AlternativeDirective {
            start: 1,
            end: 2,
            args: Vec::new(),
        }];
        let combination = vec![AlternativeVariant {
            id: "empty".to_string(),
            replacements: vec![AlternativeReplacement {
                directive_index: 0,
                value: String::new(),
            }],
        }];

        assert_eq!(
            render_alternative_prompt(prompt, &directives, &combination),
            "AB"
        );
    }

    #[test]
    fn fanout_planner_empty_branch_collapses_multiple_spaces() {
        let prompt = "A  %{extra}  B";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["A  extra  B", "A B"]
        );
    }

    #[test]
    fn fanout_planner_empty_branch_preserves_newlines_and_indentation() {
        let prompt = "Header\n  %{extra}\n  Footer";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["Header\n  extra\n  Footer", "Header\n  \n  Footer"]
        );
    }

    #[test]
    fn fanout_planner_empty_branch_preserves_following_directive_separator() {
        let prompt = "Do work. %{extra} %{%m:opus | %m:gpt-5.6-sol}";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.model.as_deref())
                .collect::<Vec<_>>(),
            vec![
                Some("opus"),
                Some("gpt-5.6-sol"),
                Some("opus"),
                Some("gpt-5.6-sol")
            ]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec![
                "Do work. extra %m:opus",
                "Do work. extra %m:gpt-5.6-sol",
                "Do work. %m:opus",
                "Do work. %m:gpt-5.6-sol",
            ]
        );
    }

    #[test]
    fn fanout_planner_brace_nested_pipes_do_not_split() {
        let prompt = "%{a (x | y) | b [c | d] | `e | f`}";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["a (x | y)", "b [c | d]", "e | f"]
        );
    }

    #[test]
    fn fanout_planner_brace_composes_cartesian_with_paren_alt() {
        let prompt = "%{a | b} %alt(x,y)";

        let plan =
            plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["a x", "a y", "b x", "b y"]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("1.1"), Some("1.2"), Some("2.1"), Some("2.2")]
        );
    }

    #[test]
    fn fanout_planner_brace_model_branches_match_paren_parity() {
        let prompt = "%{opus=%model:opus | sonnet=%model:sonnet}\nReview";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.slots[0].model.as_deref(), Some("opus"));
        assert_eq!(plan.slots[0].alt_id.as_deref(), Some("opus"));
        assert_eq!(plan.slots[1].model.as_deref(), Some("sonnet"));
        assert_eq!(plan.slots[1].alt_id.as_deref(), Some("sonnet"));
    }

    #[test]
    fn fanout_planner_brace_value_fanout_after_directive_colon() {
        let prompt = "%m:opus %effort:%{medium | high | xhigh}\nReview";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.model.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("opus"), Some("opus"), Some("opus")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("1"), Some("2"), Some("3")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec![
                "%m:opus %effort:medium\nReview",
                "%m:opus %effort:high\nReview",
                "%m:opus %effort:xhigh\nReview",
            ]
        );
    }

    #[test]
    fn fanout_planner_brace_value_fanout_after_effort_e_alias() {
        // `%e:%{...}` fans out exactly like `%effort:%{...}`; the alias prefix
        // is preserved verbatim in each slot body.
        let prompt = "%m:opus %e:%{medium | high | xhigh}\nReview";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.model.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("opus"), Some("opus"), Some("opus")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("1"), Some("2"), Some("3")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec![
                "%m:opus %e:medium\nReview",
                "%m:opus %e:high\nReview",
                "%m:opus %e:xhigh\nReview",
            ]
        );
    }

    #[test]
    fn fanout_planner_model_value_fanout_after_directive_colon() {
        let prompt = "%m:%{opus | sonnet}\nReview";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.model.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("opus"), Some("sonnet")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec!["%m:opus\nReview", "%m:sonnet\nReview"]
        );
    }

    #[test]
    fn fanout_planner_value_fanouts_compose_cartesian() {
        let prompt = "%m:%{opus | sonnet} %effort:%{medium | high}\nReview";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.model.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("opus"), Some("opus"), Some("sonnet"), Some("sonnet")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("1.1"), Some("1.2"), Some("2.1"), Some("2.2")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.prompt.as_str())
                .collect::<Vec<_>>(),
            vec![
                "%m:opus %effort:medium\nReview",
                "%m:opus %effort:high\nReview",
                "%m:sonnet %effort:medium\nReview",
                "%m:sonnet %effort:high\nReview",
            ]
        );
    }

    #[test]
    fn fanout_planner_rejects_repeated_models_with_brace_alternatives() {
        let prompt = "%model:opus\n%model:sonnet %{x | y}\nReview";

        let err = plan_agent_launch_fanout(prompt, Some("model")).unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("%model:opus ... %model:sonnet"),
            "message was {message:?}"
        );
        assert!(
            message.contains("use %{%m:opus | %m:sonnet} instead"),
            "message was {message:?}"
        );
    }

    #[test]
    fn fanout_planner_rejects_paren_multi_model_directive() {
        let err =
            plan_agent_launch_fanout("%m(opus,sonnet) review", Some("model"))
                .unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("%m(opus,sonnet) is no longer supported"),
            "message was {message:?}"
        );
        assert!(
            message.contains("use %{%m:opus | %m:sonnet} instead"),
            "message was {message:?}"
        );
    }

    #[test]
    fn fanout_planner_rejects_repeated_top_level_model_directives() {
        let err = plan_agent_launch_fanout(
            "%model:opus\n%model:sonnet\nreview",
            Some("model"),
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("%model:opus ... %model:sonnet"),
            "message was {message:?}"
        );
        assert!(
            message.contains("use %{%m:opus | %m:sonnet} instead"),
            "message was {message:?}"
        );
    }

    #[test]
    fn fanout_planner_rejects_same_value_repeated_model_directives() {
        let err = plan_agent_launch_fanout(
            "%model:opus\n%model:opus\nreview",
            Some("model"),
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("use %{%m:opus | %m:opus} instead"),
            "message was {message:?}"
        );
    }

    #[test]
    fn fanout_planner_single_top_level_model_is_single_launch() {
        for prompt in ["%m:opus review", "%model(opus) review"] {
            let plan = plan_agent_launch_fanout(prompt, Some("auto")).unwrap();

            assert_eq!(plan.launch_kind, "single");
            assert_eq!(plan.slots.len(), 1);
            assert_eq!(plan.slots[0].prompt, prompt);
        }
    }

    #[test]
    fn fanout_planner_brace_model_branches_report_model_slots() {
        let prompt = "%{%m:opus | %m:sonnet}\nReview";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.model.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("opus"), Some("sonnet")]
        );
        assert_eq!(
            plan.slots
                .iter()
                .map(|slot| slot.alt_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("1"), Some("2")]
        );
    }

    #[test]
    fn fanout_planner_unvalued_model_markers_do_not_count_as_repeated() {
        let prompt = "%model\n%model()\n%model+ %{x | y}";

        let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

        assert_eq!(plan.launch_kind, "model");
        assert_eq!(plan.slots.len(), 2);
    }

    #[test]
    fn fanout_planner_unclosed_brace_reports_missing_close() {
        let err = plan_agent_launch_fanout("%{a | b", Some("alternatives"))
            .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("%{"), "message was {message:?}");
        assert!(message.contains('}'), "message was {message:?}");
    }
}
