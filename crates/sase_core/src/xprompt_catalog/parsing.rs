use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
};

use serde_yaml::Value;

use crate::{
    content_layout::{resolve_layout_candidates, CompatibleLayoutPathWire},
    list_project_records, DocumentSnapshot, EditorRange, MobileInputChoiceWire,
};

use super::types::*;
pub(super) fn resolve_compatible_read_path(
    compatible: &CompatibleLayoutPathWire,
    label: &str,
) -> Result<Option<PathBuf>, XpromptCatalogLoadError> {
    let candidates = std::iter::once(&compatible.canonical)
        .chain(compatible.legacy.iter())
        .map(|entry| PathBuf::from(&entry.path))
        .collect::<Vec<_>>();
    let resolution = resolve_layout_candidates(
        compatible.read_policy,
        &candidates
            .iter()
            .map(|path| path.exists())
            .collect::<Vec<_>>(),
    );
    if resolution.collision {
        let rendered = resolution
            .existing_indices
            .iter()
            .map(|index| candidates[*index].to_string_lossy())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(XpromptCatalogLoadError::LayoutCollision(format!(
            "{label} exists in multiple canonical/legacy locations: {rendered}; migrate to the canonical path instead of merging split state"
        )));
    }
    Ok(resolution
        .selected_index
        .map(|index| candidates[index].clone()))
}

pub(super) fn env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name).map(PathBuf::from)
}

pub(super) fn plugin_path_map_from_env(
    name: &str,
) -> BTreeMap<String, PathBuf> {
    let Some(raw) = env::var_os(name) else {
        return BTreeMap::new();
    };
    let Some(raw) = raw.to_str() else {
        return BTreeMap::new();
    };
    let Ok(entries) = serde_json::from_str::<Vec<PluginPathEntry>>(raw) else {
        return BTreeMap::new();
    };
    entries
        .into_iter()
        .filter(|entry| !entry.module.is_empty())
        .map(|entry| (entry.module, entry.path))
        .collect()
}

#[derive(Debug, Default)]
pub(super) struct KnownProjects {
    pub(super) workspaces: BTreeMap<String, PathBuf>,
    pub(super) canonical_refs: BTreeMap<String, String>,
}

pub(super) fn known_projects(home: Option<&Path>) -> KnownProjects {
    let Some(home) = home else {
        return KnownProjects::default();
    };
    let projects_dir = home.join(".sase").join("projects");
    let include_states = vec!["enabled".to_string()];
    let Ok(records) =
        list_project_records(&projects_dir, &include_states, false, true)
    else {
        return KnownProjects::default();
    };

    let project_keys = records
        .iter()
        .map(|record| record.project_name.clone())
        .collect::<BTreeSet<_>>();
    let mut ref_targets = BTreeMap::<String, BTreeSet<String>>::new();
    let mut result = KnownProjects::default();
    for record in records {
        let canonical = record
            .display_name
            .unwrap_or_else(|| record.project_name.clone());
        result
            .canonical_refs
            .insert(record.project_name.clone(), canonical.clone());
        ref_targets
            .entry(canonical.clone())
            .or_default()
            .insert(canonical.clone());
        for alias in record.aliases {
            ref_targets
                .entry(alias)
                .or_default()
                .insert(canonical.clone());
        }
        if let Some(workspace) = record.workspace_dir.map(PathBuf::from) {
            if workspace.is_dir() {
                result.workspaces.insert(canonical, workspace);
            }
        }
    }
    for (project_ref, targets) in ref_targets {
        if project_keys.contains(&project_ref) || targets.len() != 1 {
            continue;
        }
        result
            .canonical_refs
            .insert(project_ref, targets.into_iter().next().unwrap());
    }
    result
}

pub(super) fn files_with_extensions(
    dir: &Path,
    extensions: &[&str],
) -> Result<Vec<PathBuf>, XpromptCatalogLoadError> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(Vec::new());
    };
    let mut paths = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .filter(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| extensions.contains(&ext))
        })
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

pub(super) fn load_xprompt_from_markdown(
    path: &Path,
) -> Result<Option<CatalogXprompt>, XpromptCatalogLoadError> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(_) => return Ok(None),
    };
    let (front_matter, body) = parse_front_matter(&text);
    let name = front_matter
        .as_ref()
        .and_then(|data| mapping_get(data, "name"))
        .and_then(value_as_string)
        .or_else(|| {
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .map(str::to_string)
        });
    let Some(name) = name else {
        return Ok(None);
    };
    let inputs = front_matter
        .as_ref()
        .and_then(|data| mapping_get(data, "input"))
        .map(parse_inputs)
        .unwrap_or_default();
    let tags = front_matter
        .as_ref()
        .and_then(|data| mapping_get(data, "tags"))
        .map(parse_tags)
        .unwrap_or_default();
    let description = front_matter
        .as_ref()
        .and_then(|data| mapping_get(data, "description"))
        .and_then(value_as_string);
    let is_skill = front_matter
        .as_ref()
        .and_then(|data| mapping_get(data, "skill"))
        .map(value_is_truthy)
        .unwrap_or(false);
    let snippet = front_matter
        .as_ref()
        .and_then(|data| mapping_get(data, "snippet"))
        .and_then(parse_snippet);
    let source_path = path.to_string_lossy().into_owned();
    let local_xprompts = front_matter
        .as_ref()
        .map(|data| parse_local_xprompts(data, &source_path))
        .unwrap_or_default();
    Ok(Some(CatalogXprompt {
        name: name.clone(),
        content: body,
        inputs,
        local_xprompts,
        source_path: Some(source_path),
        tags,
        description,
        is_skill,
        skill_name: None,
        memory_type: None,
        snippet,
    }))
}

/// One file read from a memory root, before the xprompt-memory rules run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LoadedMemoryNote {
    pub(super) stem: String,
    pub(super) declared_type: Option<String>,
    pub(super) description: Option<String>,
    pub(super) body: String,
}

/// Read a memory note, stripping its frontmatter from the prompt body.
pub(super) fn load_memory_note(
    path: &Path,
) -> Result<Option<LoadedMemoryNote>, XpromptCatalogLoadError> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(_) => return Ok(None),
    };
    let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
        return Ok(None);
    };
    let (front_matter, body) = parse_front_matter(&text);
    let declared_type = front_matter
        .as_ref()
        .and_then(|data| mapping_get(data, "type"))
        .and_then(value_as_string);
    let description = front_matter
        .as_ref()
        .and_then(|data| mapping_get(data, "description"))
        .and_then(value_as_string)
        .map(|description| {
            description.split_whitespace().collect::<Vec<_>>().join(" ")
        })
        .filter(|description| !description.is_empty());
    Ok(Some(LoadedMemoryNote {
        stem: stem.to_string(),
        declared_type,
        description,
        body,
    }))
}

fn parse_front_matter(text: &str) -> (Option<serde_yaml::Mapping>, String) {
    let mut lines = text.lines();
    if lines.next().map(str::trim) != Some("---") {
        return (None, text.to_string());
    }
    let mut yaml_lines = Vec::new();
    let mut body_lines = Vec::new();
    let mut found_end = false;
    for line in lines.by_ref() {
        if line.trim() == "---" {
            found_end = true;
            break;
        }
        yaml_lines.push(line);
    }
    if !found_end {
        return (None, text.to_string());
    }
    body_lines.extend(lines);
    let front_matter = serde_yaml::from_str::<Value>(&yaml_lines.join("\n"))
        .ok()
        .and_then(|value| value.as_mapping().cloned())
        .unwrap_or_default();
    (Some(front_matter), body_lines.join("\n"))
}

pub(super) fn load_yaml_mapping(
    path: &Path,
) -> Result<Option<serde_yaml::Mapping>, XpromptCatalogLoadError> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(_) => return Ok(None),
    };
    Ok(serde_yaml::from_str::<Value>(&text)
        .ok()
        .and_then(|value| value.as_mapping().cloned()))
}

pub(super) fn load_workflow_from_yaml_file(
    path: &Path,
) -> Result<Option<CatalogWorkflow>, XpromptCatalogLoadError> {
    let Some(mapping) = load_yaml_mapping(path)? else {
        return Ok(None);
    };
    let Some(name) = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .map(str::to_string)
    else {
        return Ok(None);
    };
    let workflow =
        workflow_from_mapping(&name, &mapping, &path.to_string_lossy());
    if workflow.steps.is_empty() {
        Ok(None)
    } else {
        Ok(Some(workflow))
    }
}

fn parse_local_xprompts(
    data: &serde_yaml::Mapping,
    source_path: &str,
) -> Vec<CatalogXprompt> {
    mapping_get(data, "xprompts")
        .and_then(Value::as_mapping)
        .map(|xprompts| {
            xprompts
                .iter()
                .filter_map(|(name, value)| {
                    let name = value_as_string(name)?;
                    xprompt_from_config_entry(&name, value, source_path)
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn workflow_from_mapping(
    name: &str,
    data: &serde_yaml::Mapping,
    source_path: &str,
) -> CatalogWorkflow {
    let tags = mapping_get(data, "tags")
        .map(parse_tags)
        .unwrap_or_default();
    let description =
        mapping_get(data, "description").and_then(value_as_string);
    let local_xprompts = parse_local_xprompts(data, source_path);
    let mut inputs = mapping_get(data, "input")
        .map(parse_inputs)
        .unwrap_or_default();
    let mut steps = Vec::new();
    if let Some(step_values) =
        mapping_get(data, "steps").and_then(Value::as_sequence)
    {
        for (idx, step_value) in step_values.iter().enumerate() {
            let Some(step_data) = step_value.as_mapping() else {
                continue;
            };
            if let Some(step) = parse_step(step_data, idx) {
                steps.push(step);
            }
        }
    }
    let explicit_input_names = inputs
        .iter()
        .map(|input| input.name.clone())
        .collect::<BTreeSet<_>>();
    for step in &steps {
        if step.has_output && !explicit_input_names.contains(&step.name) {
            inputs.push(CatalogInput {
                name: step.name.clone(),
                type_name: "line".to_string(),
                description: None,
                required: true,
                default_display: None,
                default_snippet_value: None,
                is_step_input: true,
                repeatable: false,
                choices: Vec::new(),
            });
        }
    }
    CatalogWorkflow {
        name: name.to_string(),
        inputs,
        steps,
        local_xprompts,
        source_path: Some(source_path.to_string()),
        tags,
        description,
    }
}

fn parse_step(data: &serde_yaml::Mapping, index: usize) -> Option<CatalogStep> {
    let name = mapping_get(data, "name")
        .and_then(value_as_string)
        .unwrap_or_else(|| format!("step_{index}"));
    let prompt_part =
        mapping_get(data, "prompt_part").and_then(value_as_string);
    let kind = if prompt_part.is_some() {
        StepKind::PromptPart
    } else if mapping_get(data, "agent").is_some()
        || mapping_get(data, "prompt").is_some()
    {
        StepKind::Agent
    } else if mapping_get(data, "bash").is_some() {
        StepKind::Bash
    } else if mapping_get(data, "python").is_some() {
        StepKind::Python
    } else if mapping_get(data, "parallel").is_some() {
        StepKind::Parallel
    } else {
        return None;
    };
    Some(CatalogStep {
        name,
        kind,
        prompt_part,
        has_output: mapping_get(data, "output").is_some(),
    })
}

pub(super) fn xprompt_from_config_entry(
    name: &str,
    value: &Value,
    source_path: &str,
) -> Option<CatalogXprompt> {
    if let Some(content) = value.as_str() {
        return Some(CatalogXprompt {
            name: name.to_string(),
            content: content.to_string(),
            inputs: Vec::new(),
            local_xprompts: Vec::new(),
            source_path: Some(source_path.to_string()),
            tags: BTreeSet::new(),
            description: None,
            is_skill: false,
            skill_name: None,
            memory_type: None,
            snippet: None,
        });
    }
    let data = value.as_mapping()?;
    let content = mapping_get(data, "content").and_then(value_as_string)?;
    Some(CatalogXprompt {
        name: name.to_string(),
        content,
        inputs: mapping_get(data, "input")
            .map(parse_inputs)
            .unwrap_or_default(),
        local_xprompts: Vec::new(),
        source_path: Some(source_path.to_string()),
        tags: mapping_get(data, "tags")
            .map(parse_tags)
            .unwrap_or_default(),
        description: mapping_get(data, "description").and_then(value_as_string),
        is_skill: mapping_get(data, "skill")
            .map(value_is_truthy)
            .unwrap_or(false),
        skill_name: None,
        memory_type: None,
        snippet: mapping_get(data, "snippet").and_then(parse_snippet),
    })
}

pub(super) fn xprompt_to_workflow(xprompt: &CatalogXprompt) -> CatalogWorkflow {
    CatalogWorkflow {
        name: xprompt.name.clone(),
        inputs: xprompt.inputs.clone(),
        steps: vec![CatalogStep {
            name: "main".to_string(),
            kind: StepKind::PromptPart,
            prompt_part: Some(xprompt.content.clone()),
            has_output: false,
        }],
        local_xprompts: xprompt.local_xprompts.clone(),
        source_path: xprompt.source_path.clone(),
        tags: xprompt.tags.clone(),
        description: xprompt.description.clone(),
    }
}

fn parse_inputs(value: &Value) -> Vec<CatalogInput> {
    if let Some(mapping) = value.as_mapping() {
        return mapping
            .iter()
            .filter_map(|(name, raw)| {
                let name = value_as_string(name)?;
                let (
                    type_name,
                    description,
                    required,
                    default_display,
                    default_snippet_value,
                ) = parse_short_input_value(raw);
                Some(CatalogInput {
                    name,
                    type_name,
                    description,
                    required,
                    default_display,
                    default_snippet_value,
                    is_step_input: false,
                    repeatable: repeatable_input_value(raw),
                    choices: short_input_choices(raw),
                })
            })
            .collect();
    }
    if let Some(sequence) = value.as_sequence() {
        return sequence
            .iter()
            .filter_map(|item| {
                let mapping = item.as_mapping()?;
                let name =
                    mapping_get(mapping, "name").and_then(value_as_string)?;
                let type_name = mapping_get(mapping, "type")
                    .and_then(value_as_string)
                    .map(|raw| parse_input_type(&raw))
                    .unwrap_or_else(|| "line".to_string());
                let default = mapping_get(mapping, "default");
                let description = mapping_get(mapping, "description")
                    .and_then(value_as_string);
                Some(CatalogInput {
                    name,
                    type_name,
                    description,
                    required: default.is_none(),
                    default_display: default.and_then(default_display),
                    default_snippet_value: default.map(snippet_default_value),
                    is_step_input: false,
                    repeatable: mapping_get(mapping, "repeatable")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    choices: mapping_get(mapping, "choices")
                        .map(parse_input_choices)
                        .unwrap_or_default(),
                })
            })
            .collect();
    }
    Vec::new()
}

fn parse_short_input_value(
    value: &Value,
) -> (String, Option<String>, bool, Option<String>, Option<String>) {
    if let Some(mapping) = value.as_mapping() {
        let type_name = mapping_get(mapping, "type")
            .and_then(value_as_string)
            .map(|raw| parse_input_type(&raw))
            .unwrap_or_else(|| "line".to_string());
        let default = mapping_get(mapping, "default");
        let description =
            mapping_get(mapping, "description").and_then(value_as_string);
        (
            type_name,
            description,
            default.is_none(),
            default.and_then(default_display),
            default.map(snippet_default_value),
        )
    } else {
        (
            parse_input_type(
                &value_as_string(value).unwrap_or_else(|| "line".to_string()),
            ),
            None,
            true,
            None,
            None,
        )
    }
}

fn parse_input_type(raw: &str) -> String {
    match raw.to_lowercase().as_str() {
        "word" => "word",
        "agent" => "agent",
        "text" => "text",
        "path" => "path",
        "int" | "integer" => "int",
        "bool" | "boolean" => "bool",
        "float" => "float",
        "enum" => "enum",
        "code" => "code",
        _ => "line",
    }
    .to_string()
}

fn repeatable_input_value(value: &Value) -> bool {
    value
        .as_mapping()
        .and_then(|mapping| mapping_get(mapping, "repeatable"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn short_input_choices(value: &Value) -> Vec<MobileInputChoiceWire> {
    value
        .as_mapping()
        .and_then(|mapping| mapping_get(mapping, "choices"))
        .map(parse_input_choices)
        .unwrap_or_default()
}

/// Parse a declared `choices` list, matching the shapes
/// `validate_input_choices` accepts: a scalar or a `{value, label}` mapping.
fn parse_input_choices(value: &Value) -> Vec<MobileInputChoiceWire> {
    let Some(items) = value.as_sequence() else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| {
            if let Some(value) = value_as_string(item) {
                return Some(MobileInputChoiceWire { value, label: None });
            }
            let mapping = item.as_mapping()?;
            let value =
                mapping_get(mapping, "value").and_then(value_as_string)?;
            let label = mapping_get(mapping, "label").and_then(value_as_string);
            Some(MobileInputChoiceWire { value, label })
        })
        .collect()
}

fn default_display(value: &Value) -> Option<String> {
    if value.is_null() || value.as_str().is_some() {
        return None;
    }
    if let Some(value) = value.as_bool() {
        return Some(if value { "true" } else { "false" }.to_string());
    }
    if let Some(value) = value.as_i64() {
        return Some(value.to_string());
    }
    if let Some(value) = value.as_f64() {
        return Some(value.to_string());
    }
    None
}

fn snippet_default_value(value: &Value) -> String {
    if value.is_null() {
        return String::new();
    }
    value_as_string(value).unwrap_or_default()
}

fn parse_snippet(value: &Value) -> Option<CatalogSnippet> {
    if value.as_bool() == Some(true) {
        return Some(CatalogSnippet::Enabled);
    }
    value
        .as_str()
        .map(|trigger| CatalogSnippet::Trigger(trigger.to_string()))
}

fn parse_tags(value: &Value) -> BTreeSet<String> {
    if let Some(raw) = value.as_str() {
        return raw
            .split(',')
            .map(str::trim)
            .filter(|tag| !tag.is_empty())
            .map(str::to_string)
            .collect();
    }
    value
        .as_sequence()
        .map(|items| {
            items
                .iter()
                .filter_map(value_as_string)
                .map(|tag| tag.trim().to_string())
                .filter(|tag| !tag.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

fn value_is_truthy(value: &Value) -> bool {
    value.as_bool().unwrap_or_else(|| {
        value
            .as_sequence()
            .map(|items| !items.is_empty())
            .unwrap_or(false)
    })
}

pub(super) fn mapping_get<'a>(
    mapping: &'a serde_yaml::Mapping,
    key: &str,
) -> Option<&'a Value> {
    mapping.get(Value::String(key.to_string()))
}

pub(super) fn value_as_string(value: &Value) -> Option<String> {
    if let Some(raw) = value.as_str() {
        Some(raw.to_string())
    } else if let Some(raw) = value.as_i64() {
        Some(raw.to_string())
    } else {
        value.as_bool().map(|raw| raw.to_string())
    }
}

pub(super) fn path_is_under(path: &Path, base: &Path) -> bool {
    let Ok(path) = path.canonicalize() else {
        return false;
    };
    let Ok(base) = base.canonicalize() else {
        return false;
    };
    path.starts_with(base)
}

pub(super) fn relative_display(path: &Path, base: &Path) -> Option<String> {
    let path = path.canonicalize().ok()?;
    let base = base.canonicalize().ok()?;
    path.strip_prefix(base)
        .ok()
        .map(|rel| rel.to_string_lossy().replace('\\', "/"))
}

pub(super) fn source_supports_config_definition_range(source: &str) -> bool {
    matches!(source, "default_config" | "local_config" | "config")
        || source.starts_with("plugin_config:")
        || source.starts_with("config_overlay:")
        || source.starts_with("project_local_config:")
}

pub(super) fn definition_key_candidates(
    name: &str,
    source: &str,
) -> Vec<String> {
    let mut candidates = vec![name.to_string()];
    if let Some(project) = source.strip_prefix("project_local_config:") {
        if let Some(rest) = name.strip_prefix(&format!("{project}/")) {
            candidates.push(rest.to_string());
        }
    }
    if matches!(source, "local_config")
        || source.starts_with("project_local_config:")
    {
        if let Some((_, rest)) = name.split_once('/') {
            candidates.push(rest.to_string());
        }
    }
    candidates.dedup();
    candidates
}

pub(super) fn yaml_child_key_range(
    text: &str,
    section: &str,
    child_name: &str,
) -> Option<EditorRange> {
    let document = DocumentSnapshot::new(text);
    let mut section_indent = None;
    let mut child_indent = None;
    let mut line_start = 0usize;

    for raw_line in text.split_inclusive('\n') {
        let line = raw_line.trim_end_matches(['\r', '\n']);
        let parsed = parse_yaml_mapping_key(line);
        line_start += raw_line.len();

        let Some(parsed) = parsed else {
            continue;
        };
        if section_indent.is_none() {
            if parsed.indent == 0 && parsed.key == section {
                section_indent = Some(parsed.indent);
            }
            continue;
        }

        let section_indent = section_indent?;
        if parsed.indent <= section_indent {
            break;
        }
        let expected_child_indent = *child_indent.get_or_insert(parsed.indent);
        if parsed.indent != expected_child_indent {
            continue;
        }
        if parsed.key != child_name {
            continue;
        }

        let raw_line_start = line_start - raw_line.len();
        return document.byte_range_to_range(
            raw_line_start + parsed.key_start,
            raw_line_start + parsed.key_end,
        );
    }

    None
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ParsedYamlKey {
    pub(super) indent: usize,
    pub(super) key: String,
    pub(super) key_start: usize,
    pub(super) key_end: usize,
}

fn parse_yaml_mapping_key(line: &str) -> Option<ParsedYamlKey> {
    let indent = line.bytes().take_while(|byte| *byte == b' ').count();
    let rest = &line[indent..];
    if rest.is_empty() || rest.starts_with('#') || rest.starts_with('-') {
        return None;
    }
    if rest.starts_with('"') || rest.starts_with('\'') {
        return parse_quoted_yaml_key(line, indent);
    }
    parse_unquoted_yaml_key(line, indent)
}

fn parse_unquoted_yaml_key(line: &str, indent: usize) -> Option<ParsedYamlKey> {
    let rest = &line[indent..];
    let colon = rest.find(':')?;
    let raw_key = &rest[..colon];
    let trimmed_end = raw_key.trim_end().len();
    let key = raw_key[..trimmed_end].trim();
    if key.is_empty() {
        return None;
    }
    let key_start = indent + raw_key[..trimmed_end].find(key)?;
    let key_end = key_start + key.len();
    Some(ParsedYamlKey {
        indent,
        key: key.to_string(),
        key_start,
        key_end,
    })
}

fn parse_quoted_yaml_key(line: &str, indent: usize) -> Option<ParsedYamlKey> {
    let quote = line[indent..].chars().next()?;
    let mut escaped = false;
    let mut key = String::new();
    let mut close_end = None;
    let content_start = indent + quote.len_utf8();
    for (offset, ch) in line[content_start..].char_indices() {
        let absolute = content_start + offset;
        if quote == '"' && escaped {
            key.push(ch);
            escaped = false;
            continue;
        }
        if quote == '"' && ch == '\\' {
            escaped = true;
            continue;
        }
        if quote == '\'' && ch == '\'' {
            let next = absolute + ch.len_utf8();
            if line[next..].starts_with('\'') {
                key.push('\'');
                close_end = None;
                continue;
            }
        }
        if ch == quote {
            close_end = Some(absolute + ch.len_utf8());
            break;
        }
        key.push(ch);
    }
    let close_end = close_end?;
    if !line[close_end..].trim_start().starts_with(':') {
        return None;
    }
    Some(ParsedYamlKey {
        indent,
        key,
        key_start: indent,
        key_end: close_end,
    })
}
