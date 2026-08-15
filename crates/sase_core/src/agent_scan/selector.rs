//! Output-variable selector language for `sase var get`.
//!
//! Grammar, parsed so dotted agent names stay unambiguous:
//!
//! ```text
//! [SCOPE.]KEY[PATH ...]
//! SCOPE := AGENT_NAME | * | HOOD.*
//! KEY   := [A-Za-z_][A-Za-z0-9_]* | *
//! PATH  := [NONNEGATIVE_INTEGER] | ["JSON map key"]
//! ```

use std::collections::{BTreeSet, HashSet};
use std::path::Path;

use serde_json::{json, Value};

use crate::agent_identity::agent_name_in_hood;

use super::index::{
    canonical_output_variable_json, load_output_variable_occurrences,
};
use super::wire::{
    AgentOutputVariableLimitWire, AgentOutputVariableOccurrenceWire,
    AgentOutputVariableSelectorMatchWire, AgentOutputVariableSelectorQueryWire,
    AgentOutputVariableSelectorResultWire, OutputVariableSelectorPathWire,
    OutputVariableSelectorScopeWire, OutputVariableSelectorWire,
    OutputVariableValue, AGENT_OUTPUT_VARIABLE_SELECTOR_WIRE_SCHEMA_VERSION,
};

/// Selector parse or resolution failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputVariableSelectorError {
    Invalid {
        selector: String,
        message: String,
    },
    NoMatch {
        selector: String,
    },
    AmbiguousProject {
        selector: String,
        agent: String,
        projects: Vec<String>,
    },
    PathType {
        selector: String,
        path: String,
        expected: String,
        actual: String,
    },
    PathMissing {
        selector: String,
        path: String,
        key: String,
    },
    PathRange {
        selector: String,
        path: String,
        index: u64,
        len: usize,
    },
    Index {
        message: String,
    },
}

impl OutputVariableSelectorError {
    fn invalid(
        selector: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::Invalid {
            selector: selector.into(),
            message: message.into(),
        }
    }

    /// Stable JSON diagnostic for Python and other bindings.
    pub fn to_json(&self) -> Value {
        match self {
            Self::Invalid { selector, message } => json!({
                "kind": "invalid_selector",
                "selector": selector,
                "message": message,
            }),
            Self::NoMatch { selector } => json!({
                "kind": "no_match",
                "selector": selector,
            }),
            Self::AmbiguousProject {
                selector,
                agent,
                projects,
            } => json!({
                "kind": "ambiguous_project",
                "selector": selector,
                "agent": agent,
                "projects": projects,
            }),
            Self::PathType {
                selector,
                path,
                expected,
                actual,
            } => json!({
                "kind": "path_type",
                "selector": selector,
                "path": path,
                "expected": expected,
                "actual": actual,
            }),
            Self::PathMissing {
                selector,
                path,
                key,
            } => json!({
                "kind": "path_missing",
                "selector": selector,
                "path": path,
                "key": key,
            }),
            Self::PathRange {
                selector,
                path,
                index,
                len,
            } => json!({
                "kind": "path_range",
                "selector": selector,
                "path": path,
                "index": index,
                "len": len,
            }),
            Self::Index { message } => json!({
                "kind": "index",
                "message": message,
            }),
        }
    }

    pub fn to_json_string(&self) -> String {
        self.to_json().to_string()
    }
}

impl std::fmt::Display for OutputVariableSelectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid { selector, message } => {
                write!(f, "invalid selector '{selector}': {message}")
            }
            Self::NoMatch { selector } => {
                write!(f, "no match for selector '{selector}'")
            }
            Self::AmbiguousProject {
                selector,
                agent,
                projects,
            } => write!(
                f,
                "selector '{selector}' matches agent '{agent}' in multiple \
                 projects ({}); pass --project to disambiguate",
                projects.join(", ")
            ),
            Self::PathType {
                selector,
                path,
                expected,
                actual,
            } => write!(
                f,
                "selector '{selector}' path {path} expected {expected}, \
                 found {actual}"
            ),
            Self::PathMissing {
                selector,
                path,
                key,
            } => write!(
                f,
                "selector '{selector}' path {path} is missing key {key}"
            ),
            Self::PathRange {
                selector,
                path,
                index,
                len,
            } => write!(
                f,
                "selector '{selector}' path {path} index {index} is out of \
                 range (length {len})"
            ),
            Self::Index { message } => write!(f, "{message}"),
        }
    }
}

/// Parse one selector into a typed AST.
pub fn parse_output_variable_selector(
    raw: &str,
) -> Result<OutputVariableSelectorWire, OutputVariableSelectorError> {
    if raw.is_empty() {
        return Err(OutputVariableSelectorError::invalid(
            raw,
            "selector must not be empty",
        ));
    }
    let (prefix, path) = split_selector_path(raw)?;
    if prefix.is_empty() {
        return Err(OutputVariableSelectorError::invalid(
            raw,
            "selector must include a key before any JSON path",
        ));
    }
    let (scope, key) = split_scope_and_key(raw, prefix)?;
    Ok(OutputVariableSelectorWire {
        raw: raw.to_string(),
        scope,
        key,
        path,
    })
}

/// Resolve parsed selectors against the indexed output-variable projection.
pub fn query_agent_output_variable_selectors(
    index_path: &Path,
    query: AgentOutputVariableSelectorQueryWire,
) -> Result<AgentOutputVariableSelectorResultWire, OutputVariableSelectorError>
{
    if query.selectors.is_empty() {
        return Err(OutputVariableSelectorError::invalid(
            "",
            "at least one selector is required",
        ));
    }
    let occurrences = load_output_variable_occurrences(
        index_path,
        &query.projects,
        query.include_hidden,
    )
    .map_err(|message| OutputVariableSelectorError::Index { message })?;

    let mut unlimited = Vec::new();
    for selector in &query.selectors {
        unlimited.extend(expand_selector(selector, &occurrences)?);
    }
    dedup_matches(&mut unlimited);

    let mut limited = Vec::new();
    for selector in &query.selectors {
        let mut matches = expand_selector(selector, &occurrences)?;
        if selector_expands(selector)
            && query.limit > 0
            && matches.len() > query.limit as usize
        {
            matches.truncate(query.limit as usize);
        }
        limited.extend(matches);
    }
    dedup_matches(&mut limited);

    let requested_limit = query.limit;
    let total_count = unlimited.len() as u64;
    let returned_count = limited.len() as u64;
    Ok(AgentOutputVariableSelectorResultWire {
        schema_version: AGENT_OUTPUT_VARIABLE_SELECTOR_WIRE_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        query,
        matches_limit: AgentOutputVariableLimitWire {
            limit: requested_limit,
            total_count,
            returned_count,
            truncated: returned_count < total_count,
        },
        matches: limited,
    })
}

fn split_selector_path(
    raw: &str,
) -> Result<
    (&str, Vec<OutputVariableSelectorPathWire>),
    OutputVariableSelectorError,
> {
    let Some(start) = raw.find('[') else {
        return Ok((raw, Vec::new()));
    };
    let prefix = &raw[..start];
    let mut rest = &raw[start..];
    let mut path = Vec::new();
    while !rest.is_empty() {
        let (segment, next) = parse_path_segment(raw, rest)?;
        path.push(segment);
        rest = next;
    }
    Ok((prefix, path))
}

fn parse_path_segment<'a>(
    raw: &str,
    input: &'a str,
) -> Result<
    (OutputVariableSelectorPathWire, &'a str),
    OutputVariableSelectorError,
> {
    if !input.starts_with('[') {
        return Err(OutputVariableSelectorError::invalid(
            raw,
            "JSON path steps must be [INDEX] or [\"KEY\"]",
        ));
    }
    let body = &input[1..];
    if body.starts_with('"') {
        let (key, consumed) = parse_json_string(raw, body)?;
        let after = &body[consumed..];
        if !after.starts_with(']') {
            return Err(OutputVariableSelectorError::invalid(
                raw,
                "map path step must end with ]",
            ));
        }
        return Ok((OutputVariableSelectorPathWire::Key { key }, &after[1..]));
    }
    let Some(end) = body.find(']') else {
        return Err(OutputVariableSelectorError::invalid(
            raw,
            "unterminated JSON path step",
        ));
    };
    let index_text = &body[..end];
    if index_text.is_empty()
        || !index_text.bytes().all(|byte| byte.is_ascii_digit())
        || (index_text.len() > 1 && index_text.starts_with('0'))
    {
        return Err(OutputVariableSelectorError::invalid(
            raw,
            "list path steps must be a nonnegative integer, and map keys \
             must be JSON strings",
        ));
    }
    let index = index_text.parse::<u64>().map_err(|_| {
        OutputVariableSelectorError::invalid(
            raw,
            "list path index is out of range",
        )
    })?;
    Ok((
        OutputVariableSelectorPathWire::Index { index },
        &body[end + 1..],
    ))
}

fn parse_json_string(
    raw: &str,
    input: &str,
) -> Result<(String, usize), OutputVariableSelectorError> {
    let bytes = input.as_bytes();
    if bytes.first() != Some(&b'"') {
        return Err(OutputVariableSelectorError::invalid(
            raw,
            "map keys must be JSON strings",
        ));
    }
    let mut index = 1usize;
    let mut out = String::new();
    while index < bytes.len() {
        match bytes[index] {
            b'"' => return Ok((out, index + 1)),
            b'\\' => {
                index += 1;
                if index >= bytes.len() {
                    break;
                }
                match bytes[index] {
                    b'"' => out.push('"'),
                    b'\\' => out.push('\\'),
                    b'/' => out.push('/'),
                    b'b' => out.push('\u{0008}'),
                    b'f' => out.push('\u{000c}'),
                    b'n' => out.push('\n'),
                    b'r' => out.push('\r'),
                    b't' => out.push('\t'),
                    b'u' => {
                        let (ch, next) =
                            parse_unicode_escape(raw, bytes, index + 1)?;
                        out.push(ch);
                        index = next;
                        continue;
                    }
                    _ => {
                        return Err(OutputVariableSelectorError::invalid(
                            raw,
                            "invalid JSON string escape in map key",
                        ));
                    }
                }
                index += 1;
            }
            byte if byte < 0x20 => {
                return Err(OutputVariableSelectorError::invalid(
                    raw,
                    "map key contains a control character",
                ));
            }
            _ => {
                let ch = input[index..].chars().next().ok_or_else(|| {
                    OutputVariableSelectorError::invalid(
                        raw,
                        "invalid UTF-8 in map key",
                    )
                })?;
                out.push(ch);
                index += ch.len_utf8();
            }
        }
    }
    Err(OutputVariableSelectorError::invalid(
        raw,
        "unterminated JSON string in map key",
    ))
}

fn parse_unicode_escape(
    raw: &str,
    bytes: &[u8],
    hex_start: usize,
) -> Result<(char, usize), OutputVariableSelectorError> {
    let unit = parse_hex4(raw, bytes, hex_start)?;
    let next = hex_start + 4;
    if (0xD800..=0xDBFF).contains(&unit) {
        if bytes.get(next) == Some(&b'\\') && bytes.get(next + 1) == Some(&b'u')
        {
            let low = parse_hex4(raw, bytes, next + 2)?;
            if (0xDC00..=0xDFFF).contains(&low) {
                let code = 0x10000
                    + (((unit - 0xD800) as u32) << 10)
                    + (low as u32 - 0xDC00);
                let ch = char::from_u32(code).ok_or_else(|| {
                    OutputVariableSelectorError::invalid(
                        raw,
                        "invalid Unicode surrogate pair in map key",
                    )
                })?;
                return Ok((ch, next + 6));
            }
        }
        return Err(OutputVariableSelectorError::invalid(
            raw,
            "unpaired Unicode surrogate in map key",
        ));
    }
    if (0xDC00..=0xDFFF).contains(&unit) {
        return Err(OutputVariableSelectorError::invalid(
            raw,
            "unpaired Unicode surrogate in map key",
        ));
    }
    let ch = char::from_u32(unit.into()).ok_or_else(|| {
        OutputVariableSelectorError::invalid(
            raw,
            "invalid Unicode escape in map key",
        )
    })?;
    Ok((ch, next))
}

fn parse_hex4(
    raw: &str,
    bytes: &[u8],
    start: usize,
) -> Result<u16, OutputVariableSelectorError> {
    let slice = bytes.get(start..start + 4).ok_or_else(|| {
        OutputVariableSelectorError::invalid(
            raw,
            "incomplete Unicode escape in map key",
        )
    })?;
    let text = std::str::from_utf8(slice).map_err(|_| {
        OutputVariableSelectorError::invalid(
            raw,
            "invalid Unicode escape in map key",
        )
    })?;
    u16::from_str_radix(text, 16).map_err(|_| {
        OutputVariableSelectorError::invalid(
            raw,
            "invalid Unicode escape in map key",
        )
    })
}

fn split_scope_and_key(
    raw: &str,
    prefix: &str,
) -> Result<
    (OutputVariableSelectorScopeWire, Option<String>),
    OutputVariableSelectorError,
> {
    let (scope_text, key_text) = match prefix.rsplit_once('.') {
        Some((scope, key)) => (Some(scope), key),
        None => (None, prefix),
    };
    let key = parse_key(raw, key_text)?;
    let scope = match scope_text {
        None => OutputVariableSelectorScopeWire::Unscoped,
        Some("*") => OutputVariableSelectorScopeWire::Global,
        Some(scope) if scope.ends_with(".*") => {
            let hood = &scope[..scope.len() - 2];
            if hood.is_empty() {
                return Err(OutputVariableSelectorError::invalid(
                    raw,
                    "hood selector requires a hood name before .*",
                ));
            }
            OutputVariableSelectorScopeWire::Hood {
                name: hood.to_string(),
            }
        }
        Some(name) => {
            if name.is_empty() {
                return Err(OutputVariableSelectorError::invalid(
                    raw,
                    "agent name must not be empty",
                ));
            }
            OutputVariableSelectorScopeWire::Exact {
                name: name.to_string(),
            }
        }
    };
    Ok((scope, key))
}

fn parse_key(
    raw: &str,
    key: &str,
) -> Result<Option<String>, OutputVariableSelectorError> {
    if key == "*" {
        return Ok(None);
    }
    if is_variable_key(key) {
        return Ok(Some(key.to_string()));
    }
    Err(OutputVariableSelectorError::invalid(
        raw,
        "key must be * or [A-Za-z_][A-Za-z0-9_]*",
    ))
}

fn is_variable_key(key: &str) -> bool {
    let mut chars = key.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {
            chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        }
        _ => false,
    }
}

fn selector_expands(selector: &OutputVariableSelectorWire) -> bool {
    selector.key.is_none()
        || matches!(
            selector.scope,
            OutputVariableSelectorScopeWire::Global
                | OutputVariableSelectorScopeWire::Hood { .. }
        )
}

fn expand_selector(
    selector: &OutputVariableSelectorWire,
    occurrences: &[AgentOutputVariableOccurrenceWire],
) -> Result<
    Vec<AgentOutputVariableSelectorMatchWire>,
    OutputVariableSelectorError,
> {
    let selected = match &selector.scope {
        OutputVariableSelectorScopeWire::Unscoped => {
            select_unscoped(selector, occurrences)
        }
        OutputVariableSelectorScopeWire::Exact { name } => {
            select_exact(selector, name, occurrences)?
        }
        OutputVariableSelectorScopeWire::Global => {
            select_per_agent(selector, occurrences, |_| true)
        }
        OutputVariableSelectorScopeWire::Hood { name } => {
            select_per_agent(selector, occurrences, |agent| {
                hood_member_matches(name, agent)
            })
        }
    };
    if selected.is_empty() {
        return Err(OutputVariableSelectorError::NoMatch {
            selector: selector.raw.clone(),
        });
    }
    let mut matches = Vec::new();
    for occurrence in selected {
        matches.push(match_from_occurrence(selector, occurrence)?);
    }
    Ok(matches)
}

fn select_unscoped<'a>(
    selector: &OutputVariableSelectorWire,
    occurrences: &'a [AgentOutputVariableOccurrenceWire],
) -> Vec<&'a AgentOutputVariableOccurrenceWire> {
    let mut selected = Vec::new();
    let mut seen_keys = HashSet::new();
    for occurrence in occurrences {
        if !key_matches(selector, &occurrence.key) {
            continue;
        }
        if selector.key.is_none() {
            if !seen_keys.insert(occurrence.key.as_str()) {
                continue;
            }
            selected.push(occurrence);
            continue;
        }
        selected.push(occurrence);
        break;
    }
    selected
}

fn select_exact<'a>(
    selector: &OutputVariableSelectorWire,
    name: &str,
    occurrences: &'a [AgentOutputVariableOccurrenceWire],
) -> Result<
    Vec<&'a AgentOutputVariableOccurrenceWire>,
    OutputVariableSelectorError,
> {
    let mut projects = BTreeSet::new();
    for occurrence in occurrences {
        if occurrence.agent_name.as_deref() == Some(name) {
            projects.insert(occurrence.project_name.as_str());
        }
    }
    if projects.len() > 1 {
        return Err(OutputVariableSelectorError::AmbiguousProject {
            selector: selector.raw.clone(),
            agent: name.to_string(),
            projects: projects.into_iter().map(str::to_string).collect(),
        });
    }
    let Some(newest) = occurrences
        .iter()
        .find(|occurrence| occurrence.agent_name.as_deref() == Some(name))
    else {
        return Ok(Vec::new());
    };
    Ok(occurrences
        .iter()
        .filter(|occurrence| {
            occurrence.artifact_dir == newest.artifact_dir
                && key_matches(selector, &occurrence.key)
        })
        .collect())
}

fn select_per_agent<'a>(
    selector: &OutputVariableSelectorWire,
    occurrences: &'a [AgentOutputVariableOccurrenceWire],
    agent_ok: impl Fn(&str) -> bool,
) -> Vec<&'a AgentOutputVariableOccurrenceWire> {
    let mut selected = Vec::new();
    let mut seen = HashSet::new();
    for occurrence in occurrences {
        let Some(agent) = occurrence.agent_name.as_deref() else {
            continue;
        };
        if !agent_ok(agent) || !key_matches(selector, &occurrence.key) {
            continue;
        }
        let identity = (agent, occurrence.key.as_str());
        if !seen.insert(identity) {
            continue;
        }
        selected.push(occurrence);
    }
    selected
}

fn key_matches(selector: &OutputVariableSelectorWire, key: &str) -> bool {
    match selector.key.as_deref() {
        None => true,
        Some(expected) => expected == key,
    }
}

fn hood_member_matches(hood: &str, agent_name: &str) -> bool {
    if agent_name == hood {
        return true;
    }
    if agent_name
        .strip_prefix(hood)
        .is_some_and(|suffix| suffix.starts_with('.'))
    {
        return true;
    }
    agent_name_in_hood(agent_name, hood).unwrap_or(false)
}

fn match_from_occurrence(
    selector: &OutputVariableSelectorWire,
    occurrence: &AgentOutputVariableOccurrenceWire,
) -> Result<AgentOutputVariableSelectorMatchWire, OutputVariableSelectorError> {
    let value = apply_path(&occurrence.value, &selector.path, &selector.raw)?;
    let value_json =
        canonical_output_variable_json(&value).map_err(|message| {
            OutputVariableSelectorError::invalid(&selector.raw, message)
        })?;
    Ok(AgentOutputVariableSelectorMatchWire {
        selector: selector.raw.clone(),
        key: occurrence.key.clone(),
        path: selector.path.clone(),
        value,
        value_json,
        artifact_dir: occurrence.artifact_dir.clone(),
        project_name: occurrence.project_name.clone(),
        workflow_dir_name: occurrence.workflow_dir_name.clone(),
        timestamp: occurrence.timestamp.clone(),
        agent_name: occurrence.agent_name.clone(),
        cl_name: occurrence.cl_name.clone(),
        hidden: occurrence.hidden,
    })
}

fn apply_path(
    value: &OutputVariableValue,
    path: &[OutputVariableSelectorPathWire],
    selector: &str,
) -> Result<OutputVariableValue, OutputVariableSelectorError> {
    let mut current = value.clone();
    let mut walked = String::new();
    for segment in path {
        walked.push_str(&format_path_segment(segment));
        match segment {
            OutputVariableSelectorPathWire::Index { index } => {
                let list = current.as_array().ok_or_else(|| {
                    OutputVariableSelectorError::PathType {
                        selector: selector.to_string(),
                        path: walked.clone(),
                        expected: "list".to_string(),
                        actual: json_type_name(&current).to_string(),
                    }
                })?;
                let position = usize::try_from(*index).map_err(|_| {
                    OutputVariableSelectorError::PathRange {
                        selector: selector.to_string(),
                        path: walked.clone(),
                        index: *index,
                        len: list.len(),
                    }
                })?;
                current = list.get(position).cloned().ok_or_else(|| {
                    OutputVariableSelectorError::PathRange {
                        selector: selector.to_string(),
                        path: walked.clone(),
                        index: *index,
                        len: list.len(),
                    }
                })?;
            }
            OutputVariableSelectorPathWire::Key { key } => {
                let map = current.as_object().ok_or_else(|| {
                    OutputVariableSelectorError::PathType {
                        selector: selector.to_string(),
                        path: walked.clone(),
                        expected: "map".to_string(),
                        actual: json_type_name(&current).to_string(),
                    }
                })?;
                current = map.get(key).cloned().ok_or_else(|| {
                    OutputVariableSelectorError::PathMissing {
                        selector: selector.to_string(),
                        path: walked.clone(),
                        key: json!(key).to_string(),
                    }
                })?;
            }
        }
    }
    Ok(current)
}

fn format_path_segment(segment: &OutputVariableSelectorPathWire) -> String {
    match segment {
        OutputVariableSelectorPathWire::Index { index } => {
            format!("[{index}]")
        }
        OutputVariableSelectorPathWire::Key { key } => {
            format!("[{}]", json!(key))
        }
    }
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "list",
        Value::Object(_) => "map",
    }
}

fn match_identity(item: &AgentOutputVariableSelectorMatchWire) -> String {
    format!(
        "{}\0{}\0{}\0{}",
        item.artifact_dir,
        item.key,
        item.path
            .iter()
            .map(format_path_segment)
            .collect::<String>(),
        item.value_json
    )
}

fn dedup_matches(matches: &mut Vec<AgentOutputVariableSelectorMatchWire>) {
    let mut seen = HashSet::new();
    matches.retain(|item| seen.insert(match_identity(item)));
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::agent_scan::{
        rebuild_agent_artifact_index, AgentArtifactScanOptionsWire,
    };

    fn parse_ok(raw: &str) -> OutputVariableSelectorWire {
        parse_output_variable_selector(raw).expect(raw)
    }

    fn parse_err(raw: &str) -> String {
        parse_output_variable_selector(raw)
            .expect_err(raw)
            .to_string()
    }

    fn write_json(path: &Path, payload: Value) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, serde_json::to_string(&payload).unwrap()).unwrap();
    }

    fn artifact(root: &Path, project: &str, ts: &str) -> PathBuf {
        root.join(project)
            .join("artifacts")
            .join("ace-run")
            .join(ts)
    }

    fn seed_index() -> (tempfile::TempDir, PathBuf) {
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        write_json(
            &artifact(&projects, "proj", "20260814101010")
                .join("agent_meta.json"),
            json!({
                "name": "build",
                "output_variables": {
                    "status": "old",
                    "results": ["a", "b"],
                    "report": {"summary": "ok", "a\"b": 1}
                }
            }),
        );
        write_json(
            &artifact(&projects, "proj", "20260815121212")
                .join("agent_meta.json"),
            json!({
                "name": "build",
                "output_variables": {
                    "status": "ok",
                    "results": ["x", "y"],
                    "report": {"summary": "fresh", "nested": {"n": 2}},
                    "count": 1
                }
            }),
        );
        write_json(
            &artifact(&projects, "proj", "20260815131313")
                .join("agent_meta.json"),
            json!({
                "name": "build.worker",
                "output_variables": {"status": "ok", "count": 1.0}
            }),
        );
        write_json(
            &artifact(&projects, "proj", "20260815141414")
                .join("agent_meta.json"),
            json!({
                "name": "research",
                "output_variables": {"status": "root"}
            }),
        );
        write_json(
            &artifact(&projects, "proj", "20260815151515")
                .join("agent_meta.json"),
            json!({
                "name": "research.foo",
                "output_variables": {
                    "status": "member",
                    "report": {"summary": "from-foo"}
                }
            }),
        );
        write_json(
            &artifact(&projects, "proj", "20260815161616")
                .join("agent_meta.json"),
            json!({
                "name": "research.foo-bar",
                "output_variables": {"status": "hyphen"}
            }),
        );
        write_json(
            &artifact(&projects, "proj", "20260815171717")
                .join("agent_meta.json"),
            json!({
                "name": "2review",
                "output_variables": {"status": "digit"}
            }),
        );
        write_json(
            &artifact(&projects, "other", "20260815181818")
                .join("agent_meta.json"),
            json!({
                "name": "deploy",
                "output_variables": {"status": "failed"}
            }),
        );
        write_json(
            &artifact(&projects, "other", "20260815191919")
                .join("agent_meta.json"),
            json!({
                "name": "build",
                "hidden": true,
                "output_variables": {"status": "hidden-other"}
            }),
        );
        write_json(
            &artifact(&projects, "proj", "20260814090909")
                .join("agent_meta.json"),
            json!({
                "output_variables": {"status": "unnamed"}
            }),
        );
        let index = tmp.path().join("agent_artifact_index.sqlite");
        rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        (tmp, index)
    }

    fn query(
        index: &Path,
        selectors: &[&str],
        include_hidden: bool,
        projects: &[&str],
        limit: u32,
    ) -> AgentOutputVariableSelectorResultWire {
        let parsed = selectors.iter().map(|raw| parse_ok(raw)).collect();
        query_agent_output_variable_selectors(
            index,
            AgentOutputVariableSelectorQueryWire {
                selectors: parsed,
                projects: projects
                    .iter()
                    .map(|name| name.to_string())
                    .collect(),
                include_hidden,
                limit,
            },
        )
        .unwrap()
    }

    #[test]
    fn parser_splits_dotted_names_from_the_right() {
        let unscoped = parse_ok("status");
        assert_eq!(unscoped.scope, OutputVariableSelectorScopeWire::Unscoped);
        assert_eq!(unscoped.key.as_deref(), Some("status"));
        assert!(unscoped.path.is_empty());

        let exact = parse_ok("research.foo.report");
        assert_eq!(
            exact.scope,
            OutputVariableSelectorScopeWire::Exact {
                name: "research.foo".to_string()
            }
        );
        assert_eq!(exact.key.as_deref(), Some("report"));

        let hood = parse_ok("research.*.status");
        assert_eq!(
            hood.scope,
            OutputVariableSelectorScopeWire::Hood {
                name: "research".to_string()
            }
        );
        assert_eq!(hood.key.as_deref(), Some("status"));

        let global = parse_ok("*.status");
        assert_eq!(global.scope, OutputVariableSelectorScopeWire::Global);
        assert_eq!(global.key.as_deref(), Some("status"));

        let keys = parse_ok("build.*");
        assert_eq!(
            keys.scope,
            OutputVariableSelectorScopeWire::Exact {
                name: "build".to_string()
            }
        );
        assert_eq!(keys.key, None);

        let hyphen = parse_ok("research.foo-bar.status");
        assert_eq!(
            hyphen.scope,
            OutputVariableSelectorScopeWire::Exact {
                name: "research.foo-bar".to_string()
            }
        );

        let digits = parse_ok("2review.status");
        assert_eq!(
            digits.scope,
            OutputVariableSelectorScopeWire::Exact {
                name: "2review".to_string()
            }
        );
    }

    #[test]
    fn parser_accepts_nested_and_escaped_paths() {
        let indexed = parse_ok("results[0]");
        assert_eq!(
            indexed.path,
            vec![OutputVariableSelectorPathWire::Index { index: 0 }]
        );

        let nested = parse_ok(r#"research.foo.report["summary"]"#);
        assert_eq!(
            nested.scope,
            OutputVariableSelectorScopeWire::Exact {
                name: "research.foo".to_string()
            }
        );
        assert_eq!(nested.key.as_deref(), Some("report"));
        assert_eq!(
            nested.path,
            vec![OutputVariableSelectorPathWire::Key {
                key: "summary".to_string()
            }]
        );

        let escaped = parse_ok(r#"report["a\"b"][0]"#);
        assert_eq!(escaped.key.as_deref(), Some("report"));
        assert_eq!(
            escaped.path,
            vec![
                OutputVariableSelectorPathWire::Key {
                    key: r#"a"b"#.to_string()
                },
                OutputVariableSelectorPathWire::Index { index: 0 }
            ]
        );

        let unicode = parse_ok(r#"report["caf\u00e9"]"#);
        assert_eq!(
            unicode.path,
            vec![OutputVariableSelectorPathWire::Key {
                key: "café".to_string()
            }]
        );
    }

    #[test]
    fn parser_rejects_invalid_selectors() {
        assert!(parse_err("").contains("must not be empty"));
        assert!(parse_err("build.status.").contains("key must be"));
        assert!(parse_err("report[summary]").contains("JSON strings"));
        assert!(parse_err("results[-1]").contains("nonnegative"));
        assert!(parse_err("results[01]").contains("nonnegative"));
        assert!(parse_err("results[]").contains("nonnegative"));
        assert!(parse_err("[0]").contains("must include a key"));
        assert!(parse_err("foo.bar-baz").contains("key must be"));
    }

    #[test]
    fn unscoped_and_exact_selectors_use_newest_artifact() {
        let (_tmp, index) = seed_index();
        let status = query(&index, &["status"], false, &[], 20);
        assert_eq!(status.matches.len(), 1);
        assert_eq!(status.matches[0].value, json!("failed"));
        assert_eq!(status.matches[0].agent_name.as_deref(), Some("deploy"));

        let build = query(&index, &["build.status"], false, &["proj"], 20);
        assert_eq!(build.matches[0].value, json!("ok"));
        assert_eq!(build.matches[0].timestamp, "20260815121212");

        let old_results =
            query(&index, &["build.results[0]"], false, &["proj"], 20);
        assert_eq!(old_results.matches[0].value, json!("x"));
    }

    #[test]
    fn exact_key_wildcard_uses_newest_artifact_only() {
        let (_tmp, index) = seed_index();
        let keys = query(&index, &["build.*"], false, &["proj"], 0);
        let names: Vec<&str> =
            keys.matches.iter().map(|item| item.key.as_str()).collect();
        assert_eq!(names, vec!["count", "report", "results", "status"]);
        assert!(keys
            .matches
            .iter()
            .all(|item| item.timestamp == "20260815121212"));
    }

    #[test]
    fn hood_and_global_selectors_collapse_repeated_runs() {
        let (_tmp, index) = seed_index();
        let hood = query(&index, &["research.*.status"], false, &[], 0);
        let agents: Vec<_> = hood
            .matches
            .iter()
            .map(|item| item.agent_name.clone().unwrap())
            .collect();
        assert_eq!(
            agents,
            vec![
                "research.foo-bar".to_string(),
                "research.foo".to_string(),
                "research".to_string()
            ]
        );

        let global = query(&index, &["*.status"], false, &[], 0);
        assert!(global
            .matches
            .iter()
            .any(|item| item.agent_name.as_deref() == Some("deploy")));
        assert!(global
            .matches
            .iter()
            .any(|item| item.agent_name.as_deref() == Some("2review")));
        assert!(!global.matches.iter().any(|item| item.agent_name.is_none()));
        let build_matches = global
            .matches
            .iter()
            .filter(|item| item.agent_name.as_deref() == Some("build"))
            .count();
        assert_eq!(build_matches, 1);
        assert_eq!(
            global
                .matches
                .iter()
                .find(|item| item.agent_name.as_deref() == Some("build"))
                .unwrap()
                .value,
            json!("ok")
        );
    }

    #[test]
    fn multiple_selectors_preserve_order_and_dedup() {
        let (_tmp, index) = seed_index();
        let result = query(
            &index,
            &["deploy.status", "status", "2review.status"],
            false,
            &[],
            20,
        );
        assert_eq!(result.matches.len(), 2);
        assert_eq!(result.matches[0].selector, "deploy.status");
        assert_eq!(result.matches[0].value, json!("failed"));
        assert_eq!(result.matches[1].selector, "2review.status");
        assert_eq!(result.matches[1].value, json!("digit"));
    }

    #[test]
    fn nested_paths_and_failures_are_precise() {
        let (_tmp, index) = seed_index();
        let nested = query(
            &index,
            &[r#"build.report["nested"]["n"]"#],
            false,
            &["proj"],
            20,
        );
        assert_eq!(nested.matches[0].value, json!(2));

        let missing = query_agent_output_variable_selectors(
            &index,
            AgentOutputVariableSelectorQueryWire {
                selectors: vec![parse_ok(r#"build.report["nope"]"#)],
                projects: vec!["proj".to_string()],
                include_hidden: false,
                limit: 20,
            },
        )
        .unwrap_err();
        assert!(matches!(
            missing,
            OutputVariableSelectorError::PathMissing { .. }
        ));

        let wrong_type = query_agent_output_variable_selectors(
            &index,
            AgentOutputVariableSelectorQueryWire {
                selectors: vec![parse_ok("build.status[0]")],
                projects: vec!["proj".to_string()],
                include_hidden: false,
                limit: 20,
            },
        )
        .unwrap_err();
        assert!(matches!(
            wrong_type,
            OutputVariableSelectorError::PathType { .. }
        ));

        let range = query_agent_output_variable_selectors(
            &index,
            AgentOutputVariableSelectorQueryWire {
                selectors: vec![parse_ok("build.results[5]")],
                projects: vec!["proj".to_string()],
                include_hidden: false,
                limit: 20,
            },
        )
        .unwrap_err();
        assert!(matches!(
            range,
            OutputVariableSelectorError::PathRange { .. }
        ));
    }

    #[test]
    fn hidden_project_limit_and_ambiguity() {
        let (_tmp, index) = seed_index();
        let visible = query(&index, &["*.status"], false, &[], 2);
        assert_eq!(visible.matches.len(), 2);
        assert!(visible.matches_limit.truncated);
        assert!(visible.matches_limit.total_count > 2);

        let hidden = query(&index, &["*.status"], true, &[], 0);
        assert!(hidden
            .matches
            .iter()
            .any(|item| item.value == json!("hidden-other")));

        let ambiguous = query_agent_output_variable_selectors(
            &index,
            AgentOutputVariableSelectorQueryWire {
                selectors: vec![parse_ok("build.status")],
                include_hidden: true,
                limit: 20,
                ..AgentOutputVariableSelectorQueryWire::default()
            },
        )
        .unwrap_err();
        match ambiguous {
            OutputVariableSelectorError::AmbiguousProject {
                projects, ..
            } => {
                assert_eq!(projects, vec!["other", "proj"]);
            }
            other => panic!("expected ambiguous project, got {other}"),
        }

        let missing = query_agent_output_variable_selectors(
            &index,
            AgentOutputVariableSelectorQueryWire {
                selectors: vec![parse_ok("missing.status")],
                limit: 20,
                ..AgentOutputVariableSelectorQueryWire::default()
            },
        )
        .unwrap_err();
        assert!(matches!(
            missing,
            OutputVariableSelectorError::NoMatch { .. }
        ));
    }

    #[test]
    fn unscoped_key_wildcard_and_unnamed_rows() {
        let (_tmp, index) = seed_index();
        let newest_keys = query(&index, &["*"], false, &[], 0);
        assert!(newest_keys
            .matches
            .iter()
            .any(|item| item.key == "status" && item.value == json!("failed")));

        let unnamed = query(&index, &["status"], false, &["proj"], 20);
        assert_eq!(unnamed.matches[0].value, json!("digit"));
        assert_eq!(unnamed.matches[0].agent_name.as_deref(), Some("2review"));
    }
}
