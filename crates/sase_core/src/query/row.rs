//! Generic precomputed query rows and the Patch adapter.
//!
//! Evaluation never calls providers or rebuilds pane data. Each row carries
//! field values, searchable text, and the three closed host-predicate facts.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::Deserialize;
use serde_json::Value;

use crate::query::matchers::{
    get_base_status, has_any_status_suffix, strip_reverted_suffix,
};
use crate::query::profile::{CompiledQueryProfile, FieldValueKind};
use crate::query::searchable::{
    effective_project_name, get_searchable_text, RUNNING_AGENT_MARKER,
    RUNNING_PROCESS_MARKER,
};
use crate::query::types::QueryErrorWire;
use crate::wire::ChangeSpecWire;

/// Closed host-predicate facts precomputed for one row.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryPredicateFacts {
    pub error_suffix: bool,
    pub running_agent: bool,
    pub running_process: bool,
}

/// Precomputed values for one profile field on one row.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryFieldValues {
    pub strings: Vec<String>,
}

impl QueryFieldValues {
    pub fn from_string(value: impl Into<String>) -> Self {
        let value = value.into();
        if value.is_empty() {
            Self {
                strings: Vec::new(),
            }
        } else {
            Self {
                strings: vec![value],
            }
        }
    }

    pub fn from_strings<I, S>(values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            strings: values
                .into_iter()
                .map(Into::into)
                .filter(|value| !value.is_empty())
                .collect(),
        }
    }
}

/// One generic artifact row ready for corpus indexing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryRow {
    pub fields: BTreeMap<String, QueryFieldValues>,
    pub searchable_text: String,
    pub predicates: QueryPredicateFacts,
}

impl QueryRow {
    /// Deserialize a generic precomputed row dict.
    ///
    /// Accepted shape:
    ///
    /// ```json
    /// {
    ///   "fields": {"status": ["open"], "count": 3, "active": true},
    ///   "searchable": {"title": ["Hello"]},
    ///   "searchable_values": ["Hello"],
    ///   "searchable_text": "Hello",
    ///   "predicates": {
    ///     "error_suffix": false,
    ///     "running_agent": false,
    ///     "running_process": false
    ///   }
    /// }
    /// ```
    pub fn from_wire(
        value: &Value,
        profile: &CompiledQueryProfile,
    ) -> Result<Self, QueryErrorWire> {
        let wire: QueryRowWire = serde_json::from_value(value.clone())
            .map_err(|error| {
                QueryErrorWire::row(format!("invalid query row: {error}"))
            })?;
        row_from_wire(wire, profile)
    }
}

#[derive(Debug, Clone, Deserialize)]
struct QueryRowWire {
    #[serde(default)]
    fields: BTreeMap<String, Value>,
    #[serde(default)]
    searchable: BTreeMap<String, Value>,
    #[serde(default)]
    searchable_values: Vec<String>,
    #[serde(default)]
    searchable_text: Option<String>,
    #[serde(default)]
    predicates: QueryPredicateFactsWire,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct QueryPredicateFactsWire {
    #[serde(default)]
    error_suffix: bool,
    #[serde(default)]
    running_agent: bool,
    #[serde(default)]
    running_process: bool,
}

fn row_from_wire(
    wire: QueryRowWire,
    profile: &CompiledQueryProfile,
) -> Result<QueryRow, QueryErrorWire> {
    let mut fields = BTreeMap::new();
    for (key, value) in wire.fields {
        fields.insert(key, field_values_from_json(&value)?);
    }

    let mut searchable_parts: Vec<String> = Vec::new();
    if !wire.searchable.is_empty() {
        for key in profile.searchable_keys() {
            if let Some(value) = wire.searchable.get(key) {
                searchable_parts.extend(strings_from_json(value)?);
            }
        }
        for (key, value) in &wire.searchable {
            if profile.field(key).is_none() {
                searchable_parts.extend(strings_from_json(value)?);
            }
        }
    }
    searchable_parts.extend(wire.searchable_values);

    let searchable_text = match wire.searchable_text {
        Some(text) => text,
        None if !searchable_parts.is_empty() => searchable_parts.join("\n"),
        None => searchable_text_from_fields(profile, &fields),
    };

    Ok(QueryRow {
        fields,
        searchable_text,
        predicates: QueryPredicateFacts {
            error_suffix: wire.predicates.error_suffix,
            running_agent: wire.predicates.running_agent,
            running_process: wire.predicates.running_process,
        },
    })
}

fn searchable_text_from_fields(
    profile: &CompiledQueryProfile,
    fields: &BTreeMap<String, QueryFieldValues>,
) -> String {
    let mut parts = Vec::new();
    for key in profile.searchable_keys() {
        if let Some(values) = fields.get(key).or_else(|| {
            fields.iter().find_map(|(candidate, values)| {
                if candidate.eq_ignore_ascii_case(key) {
                    Some(values)
                } else {
                    None
                }
            })
        }) {
            parts.extend(values.strings.iter().cloned());
        }
    }
    parts.join("\n")
}

fn field_values_from_json(
    value: &Value,
) -> Result<QueryFieldValues, QueryErrorWire> {
    Ok(QueryFieldValues {
        strings: strings_from_json(value)?,
    })
}

fn strings_from_json(value: &Value) -> Result<Vec<String>, QueryErrorWire> {
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Bool(flag) => Ok(vec![if *flag {
            "true".to_string()
        } else {
            "false".to_string()
        }]),
        Value::Number(number) => Ok(vec![number.to_string()]),
        Value::String(text) => {
            if text.is_empty() {
                Ok(Vec::new())
            } else {
                Ok(vec![text.clone()])
            }
        }
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.extend(strings_from_json(item)?);
            }
            Ok(out)
        }
        Value::Object(_) => Err(QueryErrorWire::row(
            "query row field values must be a scalar or list",
        )),
    }
}

/// Indexed row used by the generic evaluator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedQueryRow {
    pub fields: HashMap<String, IndexedField>,
    pub searchable_text: String,
    pub searchable_lower: String,
    pub predicates: QueryPredicateFacts,
}

/// Per-field lowercase values plus the declared kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedField {
    pub kind: FieldValueKind,
    pub exact_match: bool,
    pub values: Vec<String>,
    pub values_lower: Vec<String>,
}

impl IndexedQueryRow {
    pub fn index(row: QueryRow, profile: &CompiledQueryProfile) -> Self {
        let mut fields = HashMap::new();
        for (key, values) in row.fields {
            let field_spec = profile.field(&key);
            let kind = field_spec
                .map(|field| field.value_kind)
                .unwrap_or(FieldValueKind::String);
            let exact_match = field_spec.is_some_and(|field| field.exact_match);
            let values_lower = values
                .strings
                .iter()
                .map(|value| value.to_ascii_lowercase())
                .collect();
            fields.insert(
                key.to_ascii_lowercase(),
                IndexedField {
                    kind,
                    exact_match,
                    values: values.strings,
                    values_lower,
                },
            );
        }
        let searchable_lower = row.searchable_text.to_lowercase();
        Self {
            fields,
            searchable_text: row.searchable_text,
            searchable_lower,
            predicates: row.predicates,
        }
    }
}

/// Precompute Patch rows from `ChangeSpecWire` records.
pub fn patch_rows_from_specs(specs: &[ChangeSpecWire]) -> Vec<QueryRow> {
    let mut name_map: HashMap<String, usize> =
        HashMap::with_capacity(specs.len());
    for (idx, spec) in specs.iter().enumerate() {
        name_map.insert(spec.name.to_lowercase(), idx);
    }

    specs
        .iter()
        .enumerate()
        .map(|(idx, spec)| patch_row(idx, spec, specs, &name_map))
        .collect()
}

fn patch_row(
    idx: usize,
    spec: &ChangeSpecWire,
    specs: &[ChangeSpecWire],
    name_map: &HashMap<String, usize>,
) -> QueryRow {
    let searchable_text = get_searchable_text(spec);
    let mut fields = BTreeMap::new();
    fields.insert(
        "status".into(),
        QueryFieldValues::from_string(get_base_status(&spec.status)),
    );
    fields.insert(
        "project".into(),
        QueryFieldValues::from_string(effective_project_name(spec)),
    );
    fields.insert(
        "name".into(),
        QueryFieldValues::from_string(spec.name.clone()),
    );
    fields.insert(
        "sibling".into(),
        QueryFieldValues::from_string(strip_reverted_suffix(&spec.name)),
    );
    fields.insert(
        "origin".into(),
        QueryFieldValues::from_string(spec.pr_origin.clone()),
    );
    fields.insert(
        "ancestor".into(),
        QueryFieldValues::from_strings(ancestor_names(idx, specs, name_map)),
    );
    fields.insert(
        "description".into(),
        QueryFieldValues::from_string(spec.description.clone()),
    );
    fields.insert(
        "refs".into(),
        QueryFieldValues::from_strings(spec.refs.iter().cloned()),
    );
    if let Some(parent) = &spec.parent {
        fields.insert(
            "parent".into(),
            QueryFieldValues::from_string(parent.clone()),
        );
    }
    if let Some(pr_url) = &spec.pr_url {
        fields.insert(
            "pr_url".into(),
            QueryFieldValues::from_string(pr_url.clone()),
        );
    }

    QueryRow {
        fields,
        searchable_text: searchable_text.clone(),
        predicates: QueryPredicateFacts {
            error_suffix: has_any_status_suffix(spec),
            running_agent: searchable_text.contains(RUNNING_AGENT_MARKER),
            running_process: searchable_text.contains(RUNNING_PROCESS_MARKER),
        },
    }
}

fn ancestor_names(
    idx: usize,
    specs: &[ChangeSpecWire],
    name_map: &HashMap<String, usize>,
) -> Vec<String> {
    let mut names = vec![specs[idx].name.clone()];
    let mut visited = HashSet::new();
    visited.insert(specs[idx].name.to_lowercase());
    let mut current = specs[idx].parent.clone();
    while let Some(parent) = current {
        if parent.is_empty() {
            break;
        }
        let parent_lower = parent.to_lowercase();
        if !visited.insert(parent_lower.clone()) {
            break;
        }
        names.push(parent.clone());
        current = name_map
            .get(&parent_lower)
            .and_then(|&parent_idx| specs[parent_idx].parent.clone());
    }
    names
}
