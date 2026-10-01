//! Prose-aware Markdown comparison for memory history.
//!
//! `compare_prose` compares two texts and returns per-line change marks,
//! reflow-insensitive word operations, hunks with heading breadcrumbs, a
//! bidirectional nearest-line map, a YAML-aware frontmatter delta, word and
//! line stats, and unified diff text. Markdown-aware behaviours (frontmatter,
//! headings, fences, tables, soft-break joining) apply when `format` is
//! `"markdown"`; `"plain"` compares everything by line with no frontmatter
//! delta and empty section paths.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Wire schema for [`ProseComparisonWire`].
pub const PROSE_DIFF_WIRE_SCHEMA_VERSION: u32 = 1;

fn default_format() -> String {
    "markdown".to_string()
}

fn default_context_lines() -> usize {
    3
}

/// Request wire for [`compare_prose`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProseCompareRequestWire {
    pub base: String,
    pub target: String,
    #[serde(default = "default_format")]
    pub format: String,
    #[serde(default = "default_context_lines")]
    pub context_lines: usize,
}

/// One frontmatter key delta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProseFrontmatterEntryWire {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    pub kind: String,
}

/// YAML-aware frontmatter delta plus the `type` promotion convenience.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProseFrontmatterDeltaWire {
    pub entries: Vec<ProseFrontmatterEntryWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_change: Option<String>,
}

/// Per-target-line change mark. `target_line` and `base_line` are 1-based.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProseLineMarkWire {
    pub target_line: usize,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_line: Option<usize>,
}

/// Pure deletions anchored after `after_target_line` target lines.
/// `after_target_line` is a count in `0..=target_len`; `base_start` is
/// 1-based.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProseRemovalAnchorWire {
    pub after_target_line: usize,
    pub removed_count: usize,
    pub base_start: usize,
}

/// One word span on a target line. `start`/`end` are char offsets into the
/// target line; for `delete` spans they are the anchor position and `text`
/// carries the deleted words.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProseWordOpWire {
    pub kind: String,
    pub text: String,
    pub start: usize,
    pub end: usize,
}

/// Word operations for one target line (`target_line` 1-based).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProseWordLineWire {
    pub target_line: usize,
    pub ops: Vec<ProseWordOpWire>,
}

/// One change hunk. Starts are 1-based; ends are exclusive (one past the
/// last line), so an empty range has `start == end`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProseHunkWire {
    pub target_start: usize,
    pub target_end: usize,
    pub base_start: usize,
    pub base_end: usize,
    pub section_path: Vec<String>,
}

/// Nearest-line maps in both directions. Values are 1-based line numbers
/// (0 only when the opposite document is empty). Both maps are monotonic
/// non-decreasing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProseLineMapWire {
    pub base_to_target: Vec<usize>,
    pub target_to_base: Vec<usize>,
}

/// Aggregate counts and boolean-only classifications.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProseStatsWire {
    pub words_added: usize,
    pub words_removed: usize,
    pub lines_added: usize,
    pub lines_removed: usize,
    pub reflow_only: bool,
    pub whitespace_only: bool,
    pub frontmatter_only: bool,
}

/// Full comparison result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProseComparisonWire {
    pub schema_version: u32,
    pub frontmatter: ProseFrontmatterDeltaWire,
    pub line_marks: Vec<ProseLineMarkWire>,
    pub removal_anchors: Vec<ProseRemovalAnchorWire>,
    pub word_ops: Vec<ProseWordLineWire>,
    pub hunks: Vec<ProseHunkWire>,
    pub line_map: ProseLineMapWire,
    pub stats: ProseStatsWire,
    pub unified_diff: String,
}

fn split_lines(text: &str) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    text.lines().map(|line| line.to_string()).collect()
}

fn char_len(text: &str) -> usize {
    text.chars().count()
}

fn count_words(text: &str) -> usize {
    text.split_whitespace().count()
}

fn is_markdown_format(format: &str) -> bool {
    format.eq_ignore_ascii_case("markdown") || format.eq_ignore_ascii_case("md")
}

// --- Frontmatter ----------------------------------------------------------

fn split_frontmatter(lines: &[String]) -> Option<(usize, String)> {
    if lines.is_empty() || lines[0].trim() != "---" {
        return None;
    }
    for (index, line) in lines.iter().enumerate().skip(1).take(200) {
        let trimmed = line.trim();
        if trimmed == "---" || trimmed == "..." {
            let yaml = lines[1..index].join("\n");
            return Some((index, yaml));
        }
    }
    None
}

fn yaml_value_to_string(value: &serde_yaml::Value) -> String {
    match value {
        serde_yaml::Value::Null => String::new(),
        serde_yaml::Value::Bool(boolean) => boolean.to_string(),
        serde_yaml::Value::Number(number) => number.to_string(),
        serde_yaml::Value::String(text) => text.clone(),
        serde_yaml::Value::Sequence(items) => items
            .iter()
            .map(yaml_value_to_string)
            .collect::<Vec<_>>()
            .join(", "),
        serde_yaml::Value::Mapping(_) | serde_yaml::Value::Tagged(_) => {
            serde_yaml::to_string(value)
                .unwrap_or_default()
                .trim()
                .to_string()
        }
    }
}

fn parse_yaml_map(yaml: &str) -> Option<BTreeMap<String, String>> {
    if yaml.trim().is_empty() {
        return Some(BTreeMap::new());
    }
    let value: serde_yaml::Value = serde_yaml::from_str(yaml).ok()?;
    let mapping = value.as_mapping()?;
    let mut map = BTreeMap::new();
    for (key, val) in mapping {
        let key_string = match key {
            serde_yaml::Value::String(text) => text.clone(),
            other => yaml_value_to_string(other),
        };
        map.insert(key_string, yaml_value_to_string(val));
    }
    Some(map)
}

fn frontmatter_delta(
    base_lines: &[String],
    target_lines: &[String],
    markdown: bool,
) -> (ProseFrontmatterDeltaWire, Option<usize>, Option<usize>) {
    if !markdown {
        return (
            ProseFrontmatterDeltaWire {
                entries: Vec::new(),
                type_change: None,
            },
            None,
            None,
        );
    }
    let base_split = split_frontmatter(base_lines);
    let target_split = split_frontmatter(target_lines);
    let base_end = base_split.as_ref().map(|(end, _)| *end);
    let target_end = target_split.as_ref().map(|(end, _)| *end);
    if base_split.is_none() && target_split.is_none() {
        return (
            ProseFrontmatterDeltaWire {
                entries: Vec::new(),
                type_change: None,
            },
            base_end,
            target_end,
        );
    }
    let base_yaml = base_split
        .as_ref()
        .map(|(_, yaml)| yaml.as_str())
        .unwrap_or("");
    let target_yaml = target_split
        .as_ref()
        .map(|(_, yaml)| yaml.as_str())
        .unwrap_or("");
    let base_map = if base_split.is_some() {
        parse_yaml_map(base_yaml)
    } else {
        Some(BTreeMap::new())
    };
    let target_map = if target_split.is_some() {
        parse_yaml_map(target_yaml)
    } else {
        Some(BTreeMap::new())
    };
    // Invalid YAML on either present side falls back to a plain line delta:
    // no semantic entries, so the line diff carries the change.
    let (Some(base_map), Some(target_map)) = (base_map, target_map) else {
        return (
            ProseFrontmatterDeltaWire {
                entries: Vec::new(),
                type_change: None,
            },
            base_end,
            target_end,
        );
    };
    let mut entries = Vec::new();
    let mut keys: Vec<&String> =
        base_map.keys().chain(target_map.keys()).collect();
    keys.sort();
    keys.dedup();
    for key in keys {
        let before = base_map.get(key).cloned();
        let after = target_map.get(key).cloned();
        let kind = match (&before, &after) {
            (None, Some(_)) => "added",
            (Some(_), None) => "removed",
            (Some(before), Some(after)) if before != after => "changed",
            _ => continue,
        };
        entries.push(ProseFrontmatterEntryWire {
            key: key.clone(),
            before,
            after,
            kind: kind.to_string(),
        });
    }
    let type_change = match (
        base_map.get("type").map(String::as_str),
        target_map.get("type").map(String::as_str),
    ) {
        (Some("reference"), Some("core")) => Some("promoted".to_string()),
        (Some("core"), Some("reference")) => Some("demoted".to_string()),
        _ => None,
    };
    (
        ProseFrontmatterDeltaWire {
            entries,
            type_change,
        },
        base_end,
        target_end,
    )
}

// --- Markdown structure ----------------------------------------------------

fn heading_level_and_title(line: &str) -> Option<(usize, String)> {
    let trimmed = line.trim_start();
    let hashes = trimmed.chars().take_while(|c| *c == '#').count();
    if hashes == 0 || hashes > 6 {
        return None;
    }
    let rest = &trimmed[hashes..];
    if !rest.is_empty() && !rest.starts_with([' ', '\t']) {
        return None;
    }
    Some((hashes, rest.trim().to_string()))
}

fn breadcrumbs(lines: &[String]) -> Vec<Vec<String>> {
    let mut stack: Vec<String> = Vec::new();
    let mut out: Vec<Vec<String>> = Vec::with_capacity(lines.len());
    for line in lines {
        if let Some((level, title)) = heading_level_and_title(line) {
            stack.truncate(level.saturating_sub(1));
            if title.is_empty() {
                // An empty heading still closes deeper levels.
            } else {
                stack.push(title);
            }
        }
        out.push(stack.clone());
    }
    out
}

fn fence_mask(lines: &[String]) -> Vec<bool> {
    let mut mask = vec![false; lines.len()];
    let mut inside = false;
    for (index, line) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
            mask[index] = true;
            inside = !inside;
            continue;
        }
        mask[index] = inside;
    }
    mask
}

fn is_table_line(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.starts_with('|') && trimmed.len() > 1
}

fn is_list_marker(line: &str) -> bool {
    let trimmed = line.trim_start();
    for marker in ["- ", "* ", "+ "] {
        if trimmed.starts_with(marker) {
            return true;
        }
    }
    if trimmed == "-" || trimmed == "*" || trimmed == "+" {
        return true;
    }
    // Ordered markers like `1. ` or `2) `.
    let bytes = trimmed.as_bytes();
    let mut index = 0;
    while index < bytes.len() && bytes[index].is_ascii_digit() {
        index += 1;
    }
    if index > 0 && index < bytes.len() {
        let delimiter = bytes[index];
        if (delimiter == b'.' || delimiter == b')')
            && bytes
                .get(index + 1)
                .is_some_and(|c| *c == b' ' || *c == b'\t')
        {
            return true;
        }
    }
    false
}

// --- Line diff ---------------------------------------------------------------

#[derive(Debug, Clone)]
enum LineOp {
    Equal { base_idx: usize, target_idx: usize },
    Delete { base_idx: usize },
    Insert { target_idx: usize },
}

#[derive(Debug, Clone)]
enum AlignedOp {
    Equal { base_idx: usize, target_idx: usize },
    Modified { base_idx: usize, target_idx: usize },
    Insert { target_idx: usize },
    Delete { base_idx: usize },
}

fn lcs_line_ops(base: &[String], target: &[String]) -> Vec<LineOp> {
    let mut prefix = 0;
    while prefix < base.len()
        && prefix < target.len()
        && base[prefix] == target[prefix]
    {
        prefix += 1;
    }
    let mut suffix = 0;
    while suffix < base.len() - prefix
        && suffix < target.len() - prefix
        && base[base.len() - 1 - suffix] == target[target.len() - 1 - suffix]
    {
        suffix += 1;
    }
    let mut ops = Vec::new();
    for index in 0..prefix {
        ops.push(LineOp::Equal {
            base_idx: index,
            target_idx: index,
        });
    }
    let base_mid_end = base.len() - suffix;
    let target_mid_end = target.len() - suffix;
    let base_mid = &base[prefix..base_mid_end];
    let target_mid = &target[prefix..target_mid_end];
    if !base_mid.is_empty() || !target_mid.is_empty() {
        ops.extend(lcs_middle_ops(base_mid, target_mid, prefix));
    }
    for offset in 0..suffix {
        ops.push(LineOp::Equal {
            base_idx: base_mid_end + offset,
            target_idx: target_mid_end + offset,
        });
    }
    ops
}

fn lcs_middle_ops(
    base_mid: &[String],
    target_mid: &[String],
    offset: usize,
) -> Vec<LineOp> {
    let rows = base_mid.len() + 1;
    let cols = target_mid.len() + 1;
    if base_mid.is_empty() {
        return target_mid
            .iter()
            .enumerate()
            .map(|(index, _)| LineOp::Insert {
                target_idx: offset + index,
            })
            .collect();
    }
    if target_mid.is_empty() {
        return base_mid
            .iter()
            .enumerate()
            .map(|(index, _)| LineOp::Delete {
                base_idx: offset + index,
            })
            .collect();
    }
    let mut table = vec![0u32; rows * cols];
    for i in 1..rows {
        for j in 1..cols {
            let value = if base_mid[i - 1] == target_mid[j - 1] {
                table[(i - 1) * cols + (j - 1)] + 1
            } else {
                table[(i - 1) * cols + j].max(table[i * cols + (j - 1)])
            };
            table[i * cols + j] = value;
        }
    }
    let mut ops = Vec::new();
    let (mut i, mut j) = (base_mid.len(), target_mid.len());
    while i > 0 || j > 0 {
        if i > 0 && j > 0 && base_mid[i - 1] == target_mid[j - 1] {
            ops.push(LineOp::Equal {
                base_idx: offset + i - 1,
                target_idx: offset + j - 1,
            });
            i -= 1;
            j -= 1;
        } else if j > 0
            && (i == 0
                || table[i * cols + (j - 1)] >= table[(i - 1) * cols + j])
        {
            ops.push(LineOp::Insert {
                target_idx: offset + j - 1,
            });
            j -= 1;
        } else {
            ops.push(LineOp::Delete {
                base_idx: offset + i - 1,
            });
            i -= 1;
        }
    }
    ops.reverse();
    ops
}

fn align_ops(ops: Vec<LineOp>) -> Vec<AlignedOp> {
    let mut aligned = Vec::new();
    let mut index = 0;
    while index < ops.len() {
        match &ops[index] {
            LineOp::Equal {
                base_idx,
                target_idx,
            } => {
                aligned.push(AlignedOp::Equal {
                    base_idx: *base_idx,
                    target_idx: *target_idx,
                });
                index += 1;
            }
            _ => {
                let mut deletes = Vec::new();
                let mut inserts = Vec::new();
                while index < ops.len() {
                    match &ops[index] {
                        LineOp::Delete { base_idx } => deletes.push(*base_idx),
                        LineOp::Insert { target_idx } => {
                            inserts.push(*target_idx);
                        }
                        LineOp::Equal { .. } => break,
                    }
                    index += 1;
                }
                let paired = deletes.len().min(inserts.len());
                for pair in 0..paired {
                    aligned.push(AlignedOp::Modified {
                        base_idx: deletes[pair],
                        target_idx: inserts[pair],
                    });
                }
                for target_idx in inserts.iter().skip(paired) {
                    aligned.push(AlignedOp::Insert {
                        target_idx: *target_idx,
                    });
                }
                for base_idx in deletes.iter().skip(paired) {
                    aligned.push(AlignedOp::Delete {
                        base_idx: *base_idx,
                    });
                }
            }
        }
    }
    aligned
}

// --- Word diff ---------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum BlockKind {
    Prose,
    Fence,
    Table,
    Blank,
}

#[derive(Debug, Clone)]
struct WordToken {
    text: String,
    line_idx: usize,
    start: usize,
    end: usize,
}

#[derive(Debug, Clone)]
struct Block {
    kind: BlockKind,
    line_indices: Vec<usize>,
    words: Vec<String>,
    tokens: Vec<WordToken>,
}

fn split_line_words(line: &str, line_idx: usize) -> Vec<WordToken> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = line.chars().collect();
    let mut index = 0;
    while index < chars.len() {
        while index < chars.len() && chars[index].is_whitespace() {
            index += 1;
        }
        if index >= chars.len() {
            break;
        }
        let start = index;
        while index < chars.len() && !chars[index].is_whitespace() {
            index += 1;
        }
        let text: String = chars[start..index].iter().collect();
        tokens.push(WordToken {
            text,
            line_idx,
            start,
            end: index,
        });
    }
    tokens
}

fn parse_blocks(lines: &[String], mask: &[bool], markdown: bool) -> Vec<Block> {
    if !markdown {
        return lines
            .iter()
            .enumerate()
            .map(|(line_idx, line)| {
                if line.trim().is_empty() {
                    Block {
                        kind: BlockKind::Blank,
                        line_indices: vec![line_idx],
                        words: Vec::new(),
                        tokens: Vec::new(),
                    }
                } else {
                    let tokens = split_line_words(line, line_idx);
                    Block {
                        kind: BlockKind::Fence,
                        line_indices: vec![line_idx],
                        words: tokens.iter().map(|t| t.text.clone()).collect(),
                        tokens,
                    }
                }
            })
            .collect();
    }
    let mut blocks = Vec::new();
    let mut current_prose: Option<Block> = None;
    let flush = |current: &mut Option<Block>, blocks: &mut Vec<Block>| {
        if let Some(block) = current.take() {
            blocks.push(block);
        }
    };
    for (line_idx, line) in lines.iter().enumerate() {
        if mask[line_idx] {
            flush(&mut current_prose, &mut blocks);
            let tokens = split_line_words(line, line_idx);
            blocks.push(Block {
                kind: BlockKind::Fence,
                line_indices: vec![line_idx],
                words: tokens.iter().map(|t| t.text.clone()).collect(),
                tokens,
            });
            continue;
        }
        if line.trim().is_empty() {
            flush(&mut current_prose, &mut blocks);
            blocks.push(Block {
                kind: BlockKind::Blank,
                line_indices: vec![line_idx],
                words: Vec::new(),
                tokens: Vec::new(),
            });
            continue;
        }
        if is_table_line(line) {
            flush(&mut current_prose, &mut blocks);
            let tokens = split_line_words(line, line_idx);
            blocks.push(Block {
                kind: BlockKind::Table,
                line_indices: vec![line_idx],
                words: tokens.iter().map(|t| t.text.clone()).collect(),
                tokens,
            });
            continue;
        }
        if heading_level_and_title(line).is_some() {
            flush(&mut current_prose, &mut blocks);
            let tokens = split_line_words(line, line_idx);
            blocks.push(Block {
                kind: BlockKind::Prose,
                line_indices: vec![line_idx],
                words: tokens.iter().map(|t| t.text.clone()).collect(),
                tokens,
            });
            continue;
        }
        if is_list_marker(line) {
            flush(&mut current_prose, &mut blocks);
            let tokens = split_line_words(line, line_idx);
            current_prose = Some(Block {
                kind: BlockKind::Prose,
                line_indices: vec![line_idx],
                words: tokens.iter().map(|t| t.text.clone()).collect(),
                tokens,
            });
            continue;
        }
        // Continuation of the current prose block (paragraph or list item);
        // soft breaks inside a block are whitespace for word purposes.
        match current_prose.as_mut() {
            Some(block) => {
                let tokens = split_line_words(line, line_idx);
                block.line_indices.push(line_idx);
                block.words.extend(tokens.iter().map(|t| t.text.clone()));
                block.tokens.extend(tokens);
            }
            None => {
                let tokens = split_line_words(line, line_idx);
                current_prose = Some(Block {
                    kind: BlockKind::Prose,
                    line_indices: vec![line_idx],
                    words: tokens.iter().map(|t| t.text.clone()).collect(),
                    tokens,
                });
            }
        }
    }
    flush(&mut current_prose, &mut blocks);
    blocks
}

fn blocks_equal(left: &Block, right: &Block) -> bool {
    if left.kind != right.kind {
        return false;
    }
    match left.kind {
        BlockKind::Blank => true,
        BlockKind::Prose => left.words == right.words,
        BlockKind::Fence | BlockKind::Table => left.words == right.words,
    }
}

#[derive(Debug, Clone)]
enum BlockOp {
    Equal { base_idx: usize, target_idx: usize },
    Delete { base_idx: usize },
    Insert { target_idx: usize },
}

fn diff_blocks(base: &[Block], target: &[Block]) -> Vec<BlockOp> {
    let mut prefix = 0;
    while prefix < base.len()
        && prefix < target.len()
        && blocks_equal(&base[prefix], &target[prefix])
    {
        prefix += 1;
    }
    let mut suffix = 0;
    while suffix < base.len() - prefix
        && suffix < target.len() - prefix
        && blocks_equal(
            &base[base.len() - 1 - suffix],
            &target[target.len() - 1 - suffix],
        )
    {
        suffix += 1;
    }
    let mut ops = Vec::new();
    for index in 0..prefix {
        ops.push(BlockOp::Equal {
            base_idx: index,
            target_idx: index,
        });
    }
    let base_mid_end = base.len() - suffix;
    let target_mid_end = target.len() - suffix;
    let base_mid = &base[prefix..base_mid_end];
    let target_mid = &target[prefix..target_mid_end];
    if base_mid.is_empty() {
        for (offset, _) in target_mid.iter().enumerate() {
            ops.push(BlockOp::Insert {
                target_idx: prefix + offset,
            });
        }
    } else if target_mid.is_empty() {
        for (offset, _) in base_mid.iter().enumerate() {
            ops.push(BlockOp::Delete {
                base_idx: prefix + offset,
            });
        }
    } else {
        let rows = base_mid.len() + 1;
        let cols = target_mid.len() + 1;
        let mut table = vec![0u32; rows * cols];
        for i in 1..rows {
            for j in 1..cols {
                table[i * cols + j] =
                    if blocks_equal(&base_mid[i - 1], &target_mid[j - 1]) {
                        table[(i - 1) * cols + (j - 1)] + 1
                    } else {
                        table[(i - 1) * cols + j].max(table[i * cols + (j - 1)])
                    };
            }
        }
        let (mut i, mut j) = (base_mid.len(), target_mid.len());
        let mut middle = Vec::new();
        while i > 0 || j > 0 {
            if i > 0
                && j > 0
                && blocks_equal(&base_mid[i - 1], &target_mid[j - 1])
            {
                middle.push(BlockOp::Equal {
                    base_idx: prefix + i - 1,
                    target_idx: prefix + j - 1,
                });
                i -= 1;
                j -= 1;
            } else if j > 0
                && (i == 0
                    || table[i * cols + (j - 1)] >= table[(i - 1) * cols + j])
            {
                middle.push(BlockOp::Insert {
                    target_idx: prefix + j - 1,
                });
                j -= 1;
            } else {
                middle.push(BlockOp::Delete {
                    base_idx: prefix + i - 1,
                });
                i -= 1;
            }
        }
        middle.reverse();
        ops.extend(middle);
    }
    for offset in 0..suffix {
        ops.push(BlockOp::Equal {
            base_idx: base_mid_end + offset,
            target_idx: target_mid_end + offset,
        });
    }
    ops
}

#[derive(Debug, Clone)]
enum WordEdit {
    Equal(String),
    Delete(String),
    Insert(String),
}

fn diff_words(base: &[String], target: &[String]) -> Vec<WordEdit> {
    let mut prefix = 0;
    while prefix < base.len()
        && prefix < target.len()
        && base[prefix] == target[prefix]
    {
        prefix += 1;
    }
    let mut suffix = 0;
    while suffix < base.len() - prefix
        && suffix < target.len() - prefix
        && base[base.len() - 1 - suffix] == target[target.len() - 1 - suffix]
    {
        suffix += 1;
    }
    let mut edits = Vec::new();
    for word in &base[..prefix] {
        edits.push(WordEdit::Equal(word.clone()));
    }
    let base_mid = &base[prefix..base.len() - suffix];
    let target_mid = &target[prefix..target.len() - suffix];
    if base_mid.is_empty() {
        for word in target_mid {
            edits.push(WordEdit::Insert(word.clone()));
        }
    } else if target_mid.is_empty() {
        for word in base_mid {
            edits.push(WordEdit::Delete(word.clone()));
        }
    } else {
        let rows = base_mid.len() + 1;
        let cols = target_mid.len() + 1;
        let mut table = vec![0u32; rows * cols];
        for i in 1..rows {
            for j in 1..cols {
                table[i * cols + j] = if base_mid[i - 1] == target_mid[j - 1] {
                    table[(i - 1) * cols + (j - 1)] + 1
                } else {
                    table[(i - 1) * cols + j].max(table[i * cols + (j - 1)])
                };
            }
        }
        let (mut i, mut j) = (base_mid.len(), target_mid.len());
        let mut middle = Vec::new();
        while i > 0 || j > 0 {
            if i > 0 && j > 0 && base_mid[i - 1] == target_mid[j - 1] {
                middle.push(WordEdit::Equal(base_mid[i - 1].clone()));
                i -= 1;
                j -= 1;
            } else if j > 0
                && (i == 0
                    || table[i * cols + (j - 1)] >= table[(i - 1) * cols + j])
            {
                middle.push(WordEdit::Insert(target_mid[j - 1].clone()));
                j -= 1;
            } else {
                middle.push(WordEdit::Delete(base_mid[i - 1].clone()));
                i -= 1;
            }
        }
        middle.reverse();
        edits.extend(middle);
    }
    for word in &base[base.len() - suffix..] {
        edits.push(WordEdit::Equal(word.clone()));
    }
    edits
}

// --- Main comparison -----------------------------------------------------------

/// Compare two texts and return the prose-aware comparison wire.
pub fn compare_prose(request: &ProseCompareRequestWire) -> ProseComparisonWire {
    let markdown = is_markdown_format(&request.format);
    let context_lines = request.context_lines.min(1000);
    let base_lines = split_lines(&request.base);
    let target_lines = split_lines(&request.target);

    let (frontmatter, base_fm_end, target_fm_end) =
        frontmatter_delta(&base_lines, &target_lines, markdown);

    let line_ops = align_ops(lcs_line_ops(&base_lines, &target_lines));

    // Line marks plus removal anchors.
    let mut line_marks = Vec::new();
    let mut removal_anchors = Vec::new();
    let mut target_pos = 0usize;
    let mut pending_deletes: Vec<usize> = Vec::new();
    let flush_deletes = |pending: &mut Vec<usize>,
                         anchors: &mut Vec<ProseRemovalAnchorWire>,
                         after: usize| {
        if !pending.is_empty() {
            let base_start = pending[0] + 1;
            anchors.push(ProseRemovalAnchorWire {
                after_target_line: after,
                removed_count: pending.len(),
                base_start,
            });
            pending.clear();
        }
    };
    for op in &line_ops {
        match op {
            AlignedOp::Equal {
                base_idx,
                target_idx,
            } => {
                flush_deletes(
                    &mut pending_deletes,
                    &mut removal_anchors,
                    target_pos,
                );
                line_marks.push(ProseLineMarkWire {
                    target_line: target_idx + 1,
                    status: "unchanged".to_string(),
                    base_line: Some(base_idx + 1),
                });
                target_pos = target_idx + 1;
            }
            AlignedOp::Modified {
                base_idx,
                target_idx,
            } => {
                flush_deletes(
                    &mut pending_deletes,
                    &mut removal_anchors,
                    target_pos,
                );
                line_marks.push(ProseLineMarkWire {
                    target_line: target_idx + 1,
                    status: "modified".to_string(),
                    base_line: Some(base_idx + 1),
                });
                target_pos = target_idx + 1;
            }
            AlignedOp::Insert { target_idx } => {
                flush_deletes(
                    &mut pending_deletes,
                    &mut removal_anchors,
                    target_pos,
                );
                line_marks.push(ProseLineMarkWire {
                    target_line: target_idx + 1,
                    status: "added".to_string(),
                    base_line: None,
                });
                target_pos = target_idx + 1;
            }
            AlignedOp::Delete { base_idx } => {
                pending_deletes.push(*base_idx);
            }
        }
    }
    flush_deletes(&mut pending_deletes, &mut removal_anchors, target_pos);

    // Word operations via block alignment.
    let base_mask = if markdown {
        fence_mask(&base_lines)
    } else {
        vec![false; base_lines.len()]
    };
    let target_mask = if markdown {
        fence_mask(&target_lines)
    } else {
        vec![false; target_lines.len()]
    };
    let base_blocks = parse_blocks(&base_lines, &base_mask, markdown);
    let target_blocks = parse_blocks(&target_lines, &target_mask, markdown);
    let block_ops = diff_blocks(&base_blocks, &target_blocks);

    let mut per_line_ops: Vec<Vec<ProseWordOpWire>> =
        vec![Vec::new(); target_lines.len()];
    let mut words_added = 0usize;
    let mut words_removed = 0usize;

    let mut block_index = 0;
    while block_index < block_ops.len() {
        match &block_ops[block_index] {
            BlockOp::Equal {
                base_idx,
                target_idx,
            } => {
                debug_assert!(blocks_equal(
                    &base_blocks[*base_idx],
                    &target_blocks[*target_idx]
                ));
                let block = &target_blocks[*target_idx];
                match block.kind {
                    BlockKind::Blank => {}
                    _ => {
                        for line_idx in &block.line_indices {
                            per_line_ops[*line_idx].push(ProseWordOpWire {
                                kind: "equal".to_string(),
                                text: target_lines[*line_idx].clone(),
                                start: 0,
                                end: char_len(&target_lines[*line_idx]),
                            });
                        }
                    }
                }
                block_index += 1;
            }
            _ => {
                let mut deletes = Vec::new();
                let mut inserts = Vec::new();
                while block_index < block_ops.len() {
                    match &block_ops[block_index] {
                        BlockOp::Delete { base_idx } => deletes.push(*base_idx),
                        BlockOp::Insert { target_idx } => {
                            inserts.push(*target_idx)
                        }
                        BlockOp::Equal { .. } => break,
                    }
                    block_index += 1;
                }
                // Pair prose-prose blocks as modifications first.
                let mut paired = 0;
                while paired < deletes.len()
                    && paired < inserts.len()
                    && base_blocks[deletes[paired]].kind == BlockKind::Prose
                    && target_blocks[inserts[paired]].kind == BlockKind::Prose
                {
                    paired += 1;
                }
                // Anchor line for leftover deletes in this gap: first inserted
                // target line when present, else the next equal target block's
                // first line, else the last emitted target line.
                let anchor_line: Option<usize> = inserts
                    .first()
                    .and_then(|target_idx| {
                        target_blocks[*target_idx].line_indices.first().copied()
                    })
                    .or_else(|| {
                        block_ops[block_index..].iter().find_map(
                            |op| match op {
                                BlockOp::Equal { target_idx, .. } => {
                                    target_blocks[*target_idx]
                                        .line_indices
                                        .first()
                                        .copied()
                                }
                                _ => None,
                            },
                        )
                    })
                    .or_else(|| {
                        if target_lines.is_empty() {
                            None
                        } else {
                            // Last target line before this gap.
                            let mut last: Option<usize> = None;
                            for op in block_ops[..block_index].iter().rev() {
                                match op {
                                    BlockOp::Equal { target_idx, .. }
                                    | BlockOp::Insert { target_idx } => {
                                        last = target_blocks[*target_idx]
                                            .line_indices
                                            .last()
                                            .copied();
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                            last.or(Some(target_lines.len().saturating_sub(1)))
                        }
                    });
                for pair in 0..paired {
                    let base_block = &base_blocks[deletes[pair]];
                    let target_block = &target_blocks[inserts[pair]];
                    let edits =
                        diff_words(&base_block.words, &target_block.words);
                    // Map target words in order to their line positions.
                    let mut target_cursor = 0usize;
                    let target_tokens = &target_block.tokens;
                    for edit in edits {
                        match edit {
                            WordEdit::Equal(word) => {
                                let token = &target_tokens[target_cursor];
                                debug_assert_eq!(&token.text, &word);
                                per_line_ops[token.line_idx].push(
                                    ProseWordOpWire {
                                        kind: "equal".to_string(),
                                        text: word,
                                        start: token.start,
                                        end: token.end,
                                    },
                                );
                                target_cursor += 1;
                            }
                            WordEdit::Insert(word) => {
                                let token = &target_tokens[target_cursor];
                                debug_assert_eq!(&token.text, &word);
                                per_line_ops[token.line_idx].push(
                                    ProseWordOpWire {
                                        kind: "insert".to_string(),
                                        text: word,
                                        start: token.start,
                                        end: token.end,
                                    },
                                );
                                words_added += 1;
                                target_cursor += 1;
                            }
                            WordEdit::Delete(word) => {
                                let anchor = if target_cursor
                                    < target_tokens.len()
                                {
                                    let token = &target_tokens[target_cursor];
                                    (token.line_idx, token.start)
                                } else if let Some(last) = target_tokens.last()
                                {
                                    (last.line_idx, last.end)
                                } else if let Some(line_idx) =
                                    target_block.line_indices.first().copied()
                                {
                                    (line_idx, 0)
                                } else {
                                    continue;
                                };
                                per_line_ops[anchor.0].push(ProseWordOpWire {
                                    kind: "delete".to_string(),
                                    text: word,
                                    start: anchor.1,
                                    end: anchor.1,
                                });
                                words_removed += 1;
                            }
                        }
                    }
                }
                for target_idx in inserts.iter().skip(paired) {
                    let block = &target_blocks[*target_idx];
                    match block.kind {
                        BlockKind::Blank => {}
                        BlockKind::Prose => {
                            for token in &block.tokens {
                                per_line_ops[token.line_idx].push(
                                    ProseWordOpWire {
                                        kind: "insert".to_string(),
                                        text: token.text.clone(),
                                        start: token.start,
                                        end: token.end,
                                    },
                                );
                                words_added += 1;
                            }
                            // A prose block with no whitespace-separated words
                            // cannot happen (non-blank), but keep lines
                            // visible if it ever does.
                            if block.tokens.is_empty() {
                                for line_idx in &block.line_indices {
                                    per_line_ops[*line_idx].push(
                                        ProseWordOpWire {
                                            kind: "insert".to_string(),
                                            text: target_lines[*line_idx]
                                                .clone(),
                                            start: 0,
                                            end: char_len(
                                                &target_lines[*line_idx],
                                            ),
                                        },
                                    );
                                }
                            }
                        }
                        BlockKind::Fence | BlockKind::Table => {
                            for line_idx in &block.line_indices {
                                per_line_ops[*line_idx].push(ProseWordOpWire {
                                    kind: "insert".to_string(),
                                    text: target_lines[*line_idx].clone(),
                                    start: 0,
                                    end: char_len(&target_lines[*line_idx]),
                                });
                                words_added +=
                                    count_words(&target_lines[*line_idx]);
                            }
                        }
                    }
                }
                for base_idx in deletes.iter().skip(paired) {
                    let block = &base_blocks[*base_idx];
                    let Some(anchor) = anchor_line else {
                        // Empty target: count removals with nowhere to anchor.
                        match block.kind {
                            BlockKind::Blank => {}
                            BlockKind::Prose => {
                                words_removed += block.words.len();
                            }
                            BlockKind::Fence | BlockKind::Table => {
                                for line_idx in &block.line_indices {
                                    words_removed +=
                                        count_words(&base_lines[*line_idx]);
                                }
                            }
                        }
                        continue;
                    };
                    match block.kind {
                        BlockKind::Blank => {}
                        BlockKind::Prose => {
                            // Anchor at the start of the anchor line so inline
                            // rendering has a stable position.
                            let anchor_pos = per_line_ops[anchor]
                                .first()
                                .map(|op| op.start)
                                .unwrap_or(0);
                            for word in &block.words {
                                per_line_ops[anchor].push(ProseWordOpWire {
                                    kind: "delete".to_string(),
                                    text: word.clone(),
                                    start: anchor_pos,
                                    end: anchor_pos,
                                });
                                words_removed += 1;
                            }
                        }
                        BlockKind::Fence | BlockKind::Table => {
                            for line_idx in &block.line_indices {
                                let text = base_lines[*line_idx].clone();
                                let anchor_pos = 0;
                                per_line_ops[anchor].push(ProseWordOpWire {
                                    kind: "delete".to_string(),
                                    text: text.clone(),
                                    start: anchor_pos,
                                    end: anchor_pos,
                                });
                                words_removed += count_words(&text);
                            }
                        }
                    }
                }
            }
        }
    }
    // Fill fence/table/plain lines that block alignment left empty because of
    // kind mismatches: fall back to line-mark status so every target line has
    // at least a visibility span (except blanks).
    for mark in &line_marks {
        let line_idx = mark.target_line - 1;
        if !per_line_ops[line_idx].is_empty()
            || target_lines[line_idx].trim().is_empty()
        {
            continue;
        }
        match mark.status.as_str() {
            "unchanged" => {
                per_line_ops[line_idx].push(ProseWordOpWire {
                    kind: "equal".to_string(),
                    text: target_lines[line_idx].clone(),
                    start: 0,
                    end: char_len(&target_lines[line_idx]),
                });
            }
            "added" | "modified" => {
                per_line_ops[line_idx].push(ProseWordOpWire {
                    kind: "insert".to_string(),
                    text: target_lines[line_idx].clone(),
                    start: 0,
                    end: char_len(&target_lines[line_idx]),
                });
                words_added += count_words(&target_lines[line_idx]);
            }
            _ => {}
        }
    }

    let word_ops: Vec<ProseWordLineWire> = (0..target_lines.len())
        .map(|line_idx| ProseWordLineWire {
            target_line: line_idx + 1,
            ops: std::mem::take(&mut per_line_ops[line_idx]),
        })
        .collect();

    // Hunks with heading breadcrumbs.
    let target_crumbs = if markdown {
        breadcrumbs(&target_lines)
    } else {
        vec![Vec::new(); target_lines.len()]
    };
    let base_crumbs = if markdown {
        breadcrumbs(&base_lines)
    } else {
        vec![Vec::new(); base_lines.len()]
    };
    let hunks = compute_hunks(
        &line_ops,
        &base_lines,
        &target_lines,
        context_lines,
        &target_crumbs,
        &base_crumbs,
    );

    // Bidirectional nearest-line map (monotonic).
    let line_map =
        compute_line_map(&line_ops, base_lines.len(), target_lines.len());

    // Stats.
    let lines_added = line_marks
        .iter()
        .filter(|mark| mark.status == "added" || mark.status == "modified")
        .count();
    let removed_anchors: usize = removal_anchors
        .iter()
        .map(|anchor| anchor.removed_count)
        .sum();
    let modified_count = line_marks
        .iter()
        .filter(|mark| mark.status == "modified")
        .count();
    let lines_removed = removed_anchors + modified_count;
    let whitespace_only = request.base != request.target
        && request.base.split_whitespace().collect::<Vec<_>>()
            == request.target.split_whitespace().collect::<Vec<_>>();
    let frontmatter_changed =
        !frontmatter.entries.is_empty() || frontmatter.type_change.is_some();
    let frontmatter_only = frontmatter_changed
        && changes_within_frontmatter(
            &line_marks,
            &removal_anchors,
            target_fm_end,
            base_fm_end,
        );
    let reflow_only = words_added == 0
        && words_removed == 0
        && request.base != request.target
        && !frontmatter_changed;
    let stats = ProseStatsWire {
        words_added,
        words_removed,
        lines_added,
        lines_removed,
        reflow_only,
        whitespace_only,
        frontmatter_only,
    };

    let unified_diff = render_unified_diff(
        &line_ops,
        &base_lines,
        &target_lines,
        context_lines,
    );

    ProseComparisonWire {
        schema_version: PROSE_DIFF_WIRE_SCHEMA_VERSION,
        frontmatter,
        line_marks,
        removal_anchors,
        word_ops,
        hunks,
        line_map,
        stats,
        unified_diff,
    }
}

fn changes_within_frontmatter(
    line_marks: &[ProseLineMarkWire],
    removal_anchors: &[ProseRemovalAnchorWire],
    target_fm_end: Option<usize>,
    base_fm_end: Option<usize>,
) -> bool {
    // Frontmatter occupies target lines 1..=target_fm_end+1 (0-based end
    // inclusive) when present; without frontmatter any line change escapes it.
    let target_limit = target_fm_end.map(|end| end + 1);
    let base_limit = base_fm_end.map(|end| end + 1);
    let Some(target_limit) = target_limit else {
        return false;
    };
    for mark in line_marks {
        if mark.status != "unchanged" && mark.target_line > target_limit {
            return false;
        }
    }
    for anchor in removal_anchors {
        // Anchor positions are target counts; a pure base-frontmatter delete
        // sits at or before the frontmatter limit.
        if anchor.after_target_line > target_limit {
            return false;
        }
        if let Some(base_limit) = base_limit {
            if anchor.base_start + anchor.removed_count - 1 > base_limit {
                return false;
            }
        } else {
            return false;
        }
    }
    // At least one change must exist (frontmatter_changed is checked by the
    // caller, but guard against vacuous truth when both limits exist).
    !line_marks.iter().all(|mark| mark.status == "unchanged")
        || !removal_anchors.is_empty()
}

fn compute_hunks(
    line_ops: &[AlignedOp],
    base_lines: &[String],
    target_lines: &[String],
    context_lines: usize,
    target_crumbs: &[Vec<String>],
    base_crumbs: &[Vec<String>],
) -> Vec<ProseHunkWire> {
    let is_change = |op: &AlignedOp| !matches!(op, AlignedOp::Equal { .. });
    let mut change_runs: Vec<(usize, usize)> = Vec::new();
    let mut index = 0;
    while index < line_ops.len() {
        if is_change(&line_ops[index]) {
            let start = index;
            while index < line_ops.len() && is_change(&line_ops[index]) {
                index += 1;
            }
            change_runs.push((start, index));
        } else {
            index += 1;
        }
    }
    if change_runs.is_empty() {
        return Vec::new();
    }
    // Expand by context and merge.
    let mut expanded: Vec<(usize, usize)> = change_runs
        .iter()
        .map(|(start, end)| {
            (
                start.saturating_sub(context_lines),
                (*end + context_lines).min(line_ops.len()),
            )
        })
        .collect();
    expanded.sort();
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for (start, end) in expanded {
        match merged.last_mut() {
            Some(last) if start <= last.1 => {
                last.1 = last.1.max(end);
            }
            _ => merged.push((start, end)),
        }
    }
    let mut hunks = Vec::new();
    for (start, end) in merged {
        let mut base_indices = Vec::new();
        let mut target_indices = Vec::new();
        let mut first_target_change: Option<usize> = None;
        let mut first_base_change: Option<usize> = None;
        for op in &line_ops[start..end] {
            match op {
                AlignedOp::Equal {
                    base_idx,
                    target_idx,
                } => {
                    base_indices.push(*base_idx);
                    target_indices.push(*target_idx);
                }
                AlignedOp::Modified {
                    base_idx,
                    target_idx,
                } => {
                    base_indices.push(*base_idx);
                    target_indices.push(*target_idx);
                    first_target_change.get_or_insert(*target_idx);
                    first_base_change.get_or_insert(*base_idx);
                }
                AlignedOp::Insert { target_idx } => {
                    target_indices.push(*target_idx);
                    first_target_change.get_or_insert(*target_idx);
                }
                AlignedOp::Delete { base_idx } => {
                    base_indices.push(*base_idx);
                    first_base_change.get_or_insert(*base_idx);
                }
            }
        }
        let (target_start, target_end) =
            match (target_indices.first(), target_indices.last()) {
                (Some(first), Some(last)) => (*first + 1, *last + 2),
                _ => {
                    // Pure deletion with no target lines: anchor after the
                    // preceding target line (derived from op order).
                    let anchor = line_ops[..start]
                        .iter()
                        .filter_map(|op| match op {
                            AlignedOp::Equal { target_idx, .. }
                            | AlignedOp::Modified { target_idx, .. }
                            | AlignedOp::Insert { target_idx } => {
                                Some(*target_idx + 1)
                            }
                            _ => None,
                        })
                        .next_back()
                        .unwrap_or(0);
                    (anchor + 1, anchor + 1)
                }
            };
        let (base_start, base_end) =
            match (base_indices.first(), base_indices.last()) {
                (Some(first), Some(last)) => (*first + 1, *last + 2),
                _ => {
                    let anchor = line_ops[..start]
                        .iter()
                        .filter_map(|op| match op {
                            AlignedOp::Equal { base_idx, .. }
                            | AlignedOp::Modified { base_idx, .. }
                            | AlignedOp::Delete { base_idx } => {
                                Some(*base_idx + 1)
                            }
                            _ => None,
                        })
                        .next_back()
                        .unwrap_or(0);
                    (anchor + 1, anchor + 1)
                }
            };
        let section_path = match (first_target_change, first_base_change) {
            (Some(target_idx), _) => {
                target_crumbs.get(target_idx).cloned().unwrap_or_default()
            }
            (None, Some(base_idx)) => {
                base_crumbs.get(base_idx).cloned().unwrap_or_default()
            }
            (None, None) => Vec::new(),
        };
        let _ = (base_lines, target_lines);
        hunks.push(ProseHunkWire {
            target_start,
            target_end,
            base_start,
            base_end,
            section_path,
        });
    }
    hunks
}

fn compute_line_map(
    line_ops: &[AlignedOp],
    base_len: usize,
    target_len: usize,
) -> ProseLineMapWire {
    let mut base_to_target: Vec<usize> = vec![0; base_len];
    let mut target_to_base: Vec<usize> = vec![0; target_len];
    for op in line_ops {
        match op {
            AlignedOp::Equal {
                base_idx,
                target_idx,
            }
            | AlignedOp::Modified {
                base_idx,
                target_idx,
            } => {
                base_to_target[*base_idx] = target_idx + 1;
                target_to_base[*target_idx] = base_idx + 1;
            }
            _ => {}
        }
    }
    // Nearest-neighbour fill for inserts/deletes: previous mapped line wins,
    // else the next mapped line. Both documents empty stays empty.
    if base_len > 0 && target_len > 0 {
        let mut last_target = None;
        for value in base_to_target.iter_mut() {
            if *value != 0 {
                last_target = Some(*value);
            } else if let Some(previous) = last_target {
                *value = previous;
            }
        }
        let mut next_target: Option<usize> = None;
        for base_idx in (0..base_len).rev() {
            if line_has_direct_base_mapping(line_ops, base_idx) {
                next_target = Some(base_to_target[base_idx]);
            } else if base_to_target[base_idx] == 0 {
                if let Some(next) = next_target {
                    // Leading gap: no previous, take the next mapped line.
                    base_to_target[base_idx] = next;
                }
            }
        }
        let mut last_base = None;
        for value in target_to_base.iter_mut() {
            if *value != 0 {
                last_base = Some(*value);
            } else if let Some(previous) = last_base {
                *value = previous;
            }
        }
        let mut next_base: Option<usize> = None;
        for target_idx in (0..target_len).rev() {
            if line_has_direct_target_mapping(line_ops, target_idx) {
                next_base = Some(target_to_base[target_idx]);
            } else if target_to_base[target_idx] == 0 {
                if let Some(next) = next_base {
                    target_to_base[target_idx] = next;
                }
            }
        }
        // Enforce monotonicity (non-decreasing) defensively.
        let mut running = 0;
        for value in base_to_target.iter_mut() {
            if *value < running {
                *value = running;
            } else {
                running = *value;
            }
        }
        running = 0;
        for value in target_to_base.iter_mut() {
            if *value < running {
                *value = running;
            } else {
                running = *value;
            }
        }
    }
    ProseLineMapWire {
        base_to_target,
        target_to_base,
    }
}

fn line_has_direct_base_mapping(ops: &[AlignedOp], base_idx: usize) -> bool {
    ops.iter().any(|op| match op {
        AlignedOp::Equal { base_idx: b, .. }
        | AlignedOp::Modified { base_idx: b, .. } => *b == base_idx,
        _ => false,
    })
}

fn line_has_direct_target_mapping(
    ops: &[AlignedOp],
    target_idx: usize,
) -> bool {
    ops.iter().any(|op| match op {
        AlignedOp::Equal { target_idx: t, .. }
        | AlignedOp::Modified { target_idx: t, .. }
        | AlignedOp::Insert { target_idx: t } => *t == target_idx,
        _ => false,
    })
}

fn render_unified_diff(
    line_ops: &[AlignedOp],
    base_lines: &[String],
    target_lines: &[String],
    context_lines: usize,
) -> String {
    let is_change = |op: &AlignedOp| !matches!(op, AlignedOp::Equal { .. });
    let mut change_runs: Vec<(usize, usize)> = Vec::new();
    let mut index = 0;
    while index < line_ops.len() {
        if is_change(&line_ops[index]) {
            let start = index;
            while index < line_ops.len() && is_change(&line_ops[index]) {
                index += 1;
            }
            change_runs.push((start, index));
        } else {
            index += 1;
        }
    }
    if change_runs.is_empty() {
        return String::new();
    }
    let mut expanded: Vec<(usize, usize)> = change_runs
        .iter()
        .map(|(start, end)| {
            (
                start.saturating_sub(context_lines),
                (*end + context_lines).min(line_ops.len()),
            )
        })
        .collect();
    expanded.sort();
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for (start, end) in expanded {
        match merged.last_mut() {
            Some(last) if start <= last.1 => {
                last.1 = last.1.max(end);
            }
            _ => merged.push((start, end)),
        }
    }
    let mut out = String::from("--- base\n+++ target\n");
    for (start, end) in merged {
        let base_count = line_ops[start..end]
            .iter()
            .filter(|op| {
                matches!(
                    op,
                    AlignedOp::Equal { .. }
                        | AlignedOp::Modified { .. }
                        | AlignedOp::Delete { .. }
                )
            })
            .count();
        let target_count = line_ops[start..end]
            .iter()
            .filter(|op| {
                matches!(
                    op,
                    AlignedOp::Equal { .. }
                        | AlignedOp::Modified { .. }
                        | AlignedOp::Insert { .. }
                )
            })
            .count();
        let base_start = line_ops[start..end]
            .iter()
            .filter_map(|op| match op {
                AlignedOp::Equal { base_idx, .. }
                | AlignedOp::Modified { base_idx, .. }
                | AlignedOp::Delete { base_idx } => Some(base_idx + 1),
                _ => None,
            })
            .next()
            .unwrap_or(1);
        let target_start = line_ops[start..end]
            .iter()
            .filter_map(|op| match op {
                AlignedOp::Equal { target_idx, .. }
                | AlignedOp::Modified { target_idx, .. }
                | AlignedOp::Insert { target_idx } => Some(target_idx + 1),
                _ => None,
            })
            .next()
            .unwrap_or(1);
        out.push_str(&format!(
            "@@ -{base_start},{base_count} +{target_start},{target_count} @@\n"
        ));
        for op in &line_ops[start..end] {
            match op {
                AlignedOp::Equal { target_idx, .. } => {
                    out.push(' ');
                    out.push_str(&target_lines[*target_idx]);
                    out.push('\n');
                }
                AlignedOp::Modified {
                    base_idx,
                    target_idx,
                } => {
                    out.push('-');
                    out.push_str(&base_lines[*base_idx]);
                    out.push('\n');
                    out.push('+');
                    out.push_str(&target_lines[*target_idx]);
                    out.push('\n');
                }
                AlignedOp::Insert { target_idx } => {
                    out.push('+');
                    out.push_str(&target_lines[*target_idx]);
                    out.push('\n');
                }
                AlignedOp::Delete { base_idx } => {
                    out.push('-');
                    out.push_str(&base_lines[*base_idx]);
                    out.push('\n');
                }
            }
        }
    }
    out
}

/// Return the wire schema version for the prose-diff binding.
pub fn prose_diff_wire_schema_version() -> u32 {
    PROSE_DIFF_WIRE_SCHEMA_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn request(base: &str, target: &str) -> ProseCompareRequestWire {
        ProseCompareRequestWire {
            base: base.to_string(),
            target: target.to_string(),
            format: "markdown".to_string(),
            context_lines: 3,
        }
    }

    #[test]
    fn identical_text_produces_no_operations() {
        let text = "# Title\n\nHello world.\n";
        let out = compare_prose(&request(text, text));
        assert_eq!(out.schema_version, PROSE_DIFF_WIRE_SCHEMA_VERSION);
        assert!(out.line_marks.iter().all(|mark| mark.status == "unchanged"));
        assert!(out.removal_anchors.is_empty());
        assert_eq!(out.stats.words_added, 0);
        assert_eq!(out.stats.words_removed, 0);
        assert_eq!(out.stats.lines_added, 0);
        assert_eq!(out.stats.lines_removed, 0);
        assert!(!out.stats.reflow_only);
        assert!(!out.stats.whitespace_only);
        assert!(!out.stats.frontmatter_only);
        assert!(out.hunks.is_empty());
        assert!(out.unified_diff.is_empty());
        assert!(out.frontmatter.entries.is_empty());
    }

    #[test]
    fn reflow_only_gives_zero_word_changes() {
        let base = "This is a long paragraph that has been wrapped\nacross two lines for readability.\n";
        let target = "This is a long paragraph that has been wrapped across\ntwo lines for readability.\n";
        let out = compare_prose(&request(base, target));
        assert_eq!(out.stats.words_added, 0);
        assert_eq!(out.stats.words_removed, 0);
        assert!(out.stats.reflow_only);
    }

    #[test]
    fn single_word_replacement_counts_one_each() {
        let base = "SASE agent shells export\n";
        let target = "SASE agent processes export\n";
        let out = compare_prose(&request(base, target));
        assert_eq!(out.stats.words_added, 1);
        assert_eq!(out.stats.words_removed, 1);
        assert!(!out.stats.reflow_only);
    }

    #[test]
    fn type_promotion_is_detected() {
        let base = "---\ntype: reference\n---\n\nBody.\n";
        let target = "---\ntype: core\n---\n\nBody.\n";
        let out = compare_prose(&request(base, target));
        assert_eq!(out.frontmatter.type_change.as_deref(), Some("promoted"));
        assert!(out.frontmatter.entries.iter().any(|entry| entry.key
            == "type"
            && entry.kind == "changed"
            && entry.before.as_deref() == Some("reference")
            && entry.after.as_deref() == Some("core")));
        assert!(out.stats.frontmatter_only);
    }

    #[test]
    fn heading_breadcrumb_is_attributed() {
        let base = "# Alpha\n\n## Beta\n\nOld text.\n";
        let target = "# Alpha\n\n## Beta\n\nNew text.\n";
        let out = compare_prose(&request(base, target));
        assert!(!out.hunks.is_empty());
        assert!(out.hunks.iter().any(|hunk| hunk.section_path
            == vec!["Alpha".to_string(), "Beta".to_string()]));
    }

    #[test]
    fn pure_deletion_uses_base_heading() {
        let base = "# Alpha\n\nKeep.\n\nRemove me.\n";
        let target = "# Alpha\n\nKeep.\n";
        let out = compare_prose(&request(base, target));
        assert!(!out.removal_anchors.is_empty());
        assert!(!out.hunks.is_empty());
        assert!(out
            .hunks
            .iter()
            .any(|hunk| hunk.section_path == vec!["Alpha".to_string()]));
    }

    #[test]
    fn line_maps_are_monotonic() {
        let cases = [
            ("a\nb\nc\n", "a\nx\nc\nd\n"),
            ("", "new\n"),
            ("old\n", ""),
            ("same\n", "same\n"),
            ("# H\n\none\ntwo\n", "# H\n\none\nTWO\nthree\n"),
        ];
        for (base, target) in cases {
            let out = compare_prose(&request(base, target));
            assert_eq!(
                out.line_map.base_to_target.len(),
                split_lines(base).len()
            );
            assert_eq!(
                out.line_map.target_to_base.len(),
                split_lines(target).len()
            );
            assert!(
                out.line_map.base_to_target.windows(2).all(|w| w[0] <= w[1]),
                "base_to_target not monotonic for {base:?} -> {target:?}"
            );
            assert!(
                out.line_map.target_to_base.windows(2).all(|w| w[0] <= w[1]),
                "target_to_base not monotonic for {base:?} -> {target:?}"
            );
        }
    }

    #[test]
    fn code_fences_compare_by_line() {
        let base = "```rust\nlet a = 1;\n```\n";
        let target = "```rust\nlet a = 2;\n```\n";
        let out = compare_prose(&request(base, target));
        assert!(out.stats.words_added > 0);
        assert!(!out.hunks.is_empty());
    }

    #[test]
    fn invalid_yaml_falls_back_to_line_delta() {
        let base = "---\ntype: [unclosed\n---\n\nBody.\n";
        let target = "---\ntype: core\n---\n\nBody.\n";
        let out = compare_prose(&request(base, target));
        assert!(out.frontmatter.entries.is_empty());
        assert_eq!(out.frontmatter.type_change, None);
        assert!(!out.unified_diff.is_empty());
    }

    #[test]
    fn large_document_compares_quickly() {
        let mut base_lines = Vec::new();
        let mut target_lines = Vec::new();
        for index in 0..400 {
            base_lines
                .push(format!("Line {index} with some prose content here."));
            if index == 200 {
                target_lines.push(
                    "Line 200 with some EDITED content here.".to_string(),
                );
            } else {
                target_lines.push(format!(
                    "Line {index} with some prose content here."
                ));
            }
        }
        let req = request(&base_lines.join("\n"), &target_lines.join("\n"));
        let start = Instant::now();
        let out = compare_prose(&req);
        let elapsed = start.elapsed();
        // The 5 ms budget applies to optimized builds; debug builds carry
        // assertion and overflow-check overhead, so allow headroom there.
        let limit_millis: u128 = if cfg!(debug_assertions) { 100 } else { 5 };
        assert!(
            elapsed.as_millis() <= limit_millis,
            "took {elapsed:?} (limit {limit_millis} ms)"
        );
        assert_eq!(out.stats.lines_added, 1);
    }

    #[test]
    fn plain_format_has_no_frontmatter_delta() {
        let req = ProseCompareRequestWire {
            base: "---\ntype: reference\n---\n".to_string(),
            target: "---\ntype: core\n---\n".to_string(),
            format: "plain".to_string(),
            context_lines: 3,
        };
        let out = compare_prose(&req);
        assert!(out.frontmatter.entries.is_empty());
        assert!(out.hunks.iter().all(|hunk| hunk.section_path.is_empty()));
    }
}
