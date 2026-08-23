//! Compiled query-profile records consumed by the parser and evaluator.
//!
//! The accepted input is the deterministic
//! `CompiledQueryProfile.to_wire()` shape produced by
//! `sase.ace.query_profile`. It is a per-call argument, not a persisted
//! provider/spec wire.

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::query::types::QueryErrorWire;

/// Host-recognized single-character field sigils.
pub const HOST_SIGIL_CHARS: &[char] = &['+', '^', '~', '&'];

/// Host-recognized macro trigger characters.
pub const HOST_MACRO_TRIGGERS: &[char] = &['%'];

/// Closed host predicate kinds and their fixed sigils.
pub const HOST_PREDICATE_NAMES: &[&str] =
    &["error_suffix", "running_agent", "running_process"];

/// Closed host date-bound field names and comparison directions.
///
/// Mirrors `sase.ace.query_profile.registry.HOST_DATE_BOUND_KEYS` so a
/// `date`-kind field named here compares in that direction instead of by
/// equality. `since`/`after` are inclusive lower bounds; `until`/`before`
/// are inclusive upper bounds.
pub const HOST_DATE_BOUND_KEYS: &[(&str, &str)] = &[
    ("since", ">="),
    ("after", ">="),
    ("until", "<="),
    ("before", "<="),
];

/// Closed host duration-bound field names and comparison directions.
///
/// Mirrors `sase.ace.query_profile.registry.HOST_DURATION_BOUND_KEYS`. An
/// `int`-kind field named here accepts a bare-second or whole-unit
/// duration literal and compares in that direction; every other `int`
/// field keeps equality.
pub const HOST_DURATION_BOUND_KEYS: &[(&str, &str)] =
    &[("min", ">="), ("max", "<=")];

/// Return the comparison direction for a host date-bound field key.
pub fn host_date_bound_direction(key: &str) -> Option<&'static str> {
    host_bound_direction(HOST_DATE_BOUND_KEYS, key)
}

/// Return the comparison direction for a host duration-bound field key.
pub fn host_duration_bound_direction(key: &str) -> Option<&'static str> {
    host_bound_direction(HOST_DURATION_BOUND_KEYS, key)
}

fn host_bound_direction(
    table: &'static [(&'static str, &'static str)],
    key: &str,
) -> Option<&'static str> {
    table
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(key))
        .map(|(_, direction)| *direction)
}

/// Field value kinds accepted on a compiled profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldValueKind {
    String,
    Enum,
    Bool,
    Date,
    Int,
}

impl FieldValueKind {
    fn from_wire(raw: &str) -> Option<Self> {
        match raw {
            "string" => Some(Self::String),
            "enum" => Some(Self::Enum),
            "bool" => Some(Self::Bool),
            "date" => Some(Self::Date),
            "int" => Some(Self::Int),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Enum => "enum",
            Self::Bool => "bool",
            Self::Date => "date",
            Self::Int => "int",
        }
    }
}

/// One field declared by a compiled profile.
///
/// `exact_match` only applies to filterable `string`-kind fields: `true`
/// requires a `key:value` term to equal a row's value exactly
/// (case-insensitively); `false` (the default) matches by substring.
/// `enum`-kind fields are always exact regardless of this flag;
/// `bool`/`int`/`date` fields have their own typed comparison and ignore it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryFieldSpec {
    pub key: String,
    pub value_kind: FieldValueKind,
    pub filterable: bool,
    pub searchable: bool,
    pub repeatable: bool,
    pub negatable: bool,
    #[serde(default)]
    pub exact_match: bool,
    #[serde(default)]
    pub static_values: Vec<String>,
    #[serde(default)]
    pub hint: String,
}

/// A single-punctuation shorthand for `field:value`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuerySigilSpec {
    pub sigil: String,
    pub field: String,
}

/// A `<trigger><letter>` shorthand for a fixed `field:value` pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryMacroSpec {
    pub trigger: String,
    pub letter: String,
    pub field: String,
    pub value: String,
}

/// Deterministic compiled profile used at parse and evaluate time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledQueryProfile {
    pub pane_id: String,
    pub boolean: bool,
    pub fields: Vec<QueryFieldSpec>,
    pub sigils: Vec<QuerySigilSpec>,
    pub predicates: Vec<String>,
    pub any_special: bool,
    pub macros: Vec<QueryMacroSpec>,
    pub free_text_hint: String,
    pub digest: String,
    field_by_key: HashMap<String, usize>,
    sigil_by_char: HashMap<char, String>,
    macros_by_trigger_letter: HashMap<(char, char), (String, String)>,
    predicate_set: HashSet<String>,
}

impl CompiledQueryProfile {
    /// Deserialize and validate a Python `to_wire()` payload.
    pub fn from_wire(value: &Value) -> Result<Self, QueryErrorWire> {
        let wire: CompiledQueryProfileWire =
            serde_json::from_value(value.clone()).map_err(|error| {
                QueryErrorWire::profile(format!(
                    "invalid compiled query profile: {error}"
                ))
            })?;
        compile_profile(wire)
    }

    /// Return the declared field named `key`, if any.
    pub fn field(&self, key: &str) -> Option<&QueryFieldSpec> {
        self.field_by_key
            .get(&key.to_ascii_lowercase())
            .map(|&idx| &self.fields[idx])
    }

    /// Return filterable field keys in profile order.
    pub fn filterable_keys(&self) -> Vec<&str> {
        self.fields
            .iter()
            .filter(|field| field.filterable)
            .map(|field| field.key.as_str())
            .collect()
    }

    /// Return searchable field keys in profile order.
    pub fn searchable_keys(&self) -> Vec<&str> {
        self.fields
            .iter()
            .filter(|field| field.searchable)
            .map(|field| field.key.as_str())
            .collect()
    }

    /// Whether the flat grammar may use a leading `-` on any token.
    pub fn allows_negation(&self) -> bool {
        self.fields.iter().any(|field| field.negatable)
    }

    /// Resolve a declared sigil to its target field key.
    pub fn sigil_field(&self, sigil: char) -> Option<&str> {
        self.sigil_by_char.get(&sigil).map(String::as_str)
    }

    /// Resolve a declared macro to `(field, value)`.
    pub fn macro_target(
        &self,
        trigger: char,
        letter: char,
    ) -> Option<(&str, &str)> {
        self.macros_by_trigger_letter
            .get(&(trigger, letter.to_ascii_lowercase()))
            .map(|(field, value)| (field.as_str(), value.as_str()))
    }

    /// Macros declared for `trigger`, sorted by letter.
    pub fn macros_for_trigger(&self, trigger: char) -> Vec<&QueryMacroSpec> {
        let mut macros: Vec<&QueryMacroSpec> = self
            .macros
            .iter()
            .filter(|item| item.trigger.starts_with(trigger))
            .collect();
        macros.sort_by(|left, right| left.letter.cmp(&right.letter));
        macros
    }

    /// Whether `name` is an enabled closed host predicate.
    pub fn has_predicate(&self, name: &str) -> bool {
        self.predicate_set.contains(name)
    }
}

#[derive(Debug, Clone, Deserialize)]
struct CompiledQueryProfileWire {
    pane_id: String,
    boolean: bool,
    #[serde(default)]
    fields: Vec<QueryFieldSpecWire>,
    #[serde(default)]
    sigils: Vec<QuerySigilSpec>,
    #[serde(default)]
    predicates: Vec<String>,
    #[serde(default)]
    any_special: bool,
    #[serde(default)]
    macros: Vec<QueryMacroSpec>,
    #[serde(default)]
    free_text_hint: String,
    #[serde(default)]
    digest: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct QueryFieldSpecWire {
    key: String,
    #[serde(default = "default_string_kind")]
    value_kind: String,
    #[serde(default = "default_true")]
    filterable: bool,
    #[serde(default)]
    searchable: bool,
    #[serde(default)]
    repeatable: bool,
    #[serde(default)]
    negatable: bool,
    #[serde(default)]
    exact_match: bool,
    #[serde(default)]
    static_values: Vec<String>,
    #[serde(default)]
    hint: String,
}

fn default_string_kind() -> String {
    "string".to_string()
}

fn default_true() -> bool {
    true
}

fn compile_profile(
    wire: CompiledQueryProfileWire,
) -> Result<CompiledQueryProfile, QueryErrorWire> {
    if wire.pane_id.is_empty() {
        return Err(QueryErrorWire::profile("pane_id must not be empty"));
    }

    let mut fields = Vec::with_capacity(wire.fields.len());
    for item in &wire.fields {
        fields.push(validate_field(item)?);
    }
    fields.sort_by(|left, right| left.key.cmp(&right.key));

    let mut field_keys = HashSet::new();
    for field in &fields {
        if !field_keys.insert(field.key.to_ascii_lowercase()) {
            return Err(QueryErrorWire::profile(format!(
                "duplicate field key: '{}'",
                field.key
            )));
        }
    }

    let mut sigils = wire.sigils.clone();
    sigils.sort_by(|left, right| left.sigil.cmp(&right.sigil));
    validate_sigils(&sigils, &field_keys)?;

    let mut predicates = wire.predicates.clone();
    predicates.sort();
    validate_predicates(&predicates)?;

    if wire.any_special {
        let declared: HashSet<&str> =
            predicates.iter().map(String::as_str).collect();
        if declared.len() != HOST_PREDICATE_NAMES.len()
            || !HOST_PREDICATE_NAMES
                .iter()
                .all(|name| declared.contains(name))
        {
            return Err(QueryErrorWire::profile(format!(
                "any_special requires every host predicate to be declared: {}",
                HOST_PREDICATE_NAMES.join(", ")
            )));
        }
    }

    let mut macros = wire.macros.clone();
    macros.sort_by(|left, right| {
        left.trigger
            .cmp(&right.trigger)
            .then(left.letter.cmp(&right.letter))
    });
    validate_macros(&macros, &field_keys)?;

    let payload = canonical_payload(
        &wire.pane_id,
        wire.boolean,
        &fields,
        &sigils,
        &predicates,
        wire.any_special,
        &macros,
        &wire.free_text_hint,
    );
    let digest = digest_payload(&payload);
    if let Some(provided) = wire.digest.as_deref() {
        if provided != digest {
            return Err(QueryErrorWire::profile(
                "compiled profile digest does not match payload",
            ));
        }
    }

    Ok(build_profile(
        wire.pane_id,
        wire.boolean,
        fields,
        sigils,
        predicates,
        wire.any_special,
        macros,
        wire.free_text_hint,
        digest,
    ))
}

fn validate_field(
    item: &QueryFieldSpecWire,
) -> Result<QueryFieldSpec, QueryErrorWire> {
    if item.key.is_empty() {
        return Err(QueryErrorWire::profile("field key must not be empty"));
    }
    let value_kind = FieldValueKind::from_wire(&item.value_kind)
        .ok_or_else(|| {
            QueryErrorWire::profile(format!(
                "{}: unknown value_kind '{}' (valid kinds: bool, date, enum, int, string)",
                item.key, item.value_kind
            ))
        })?;
    if value_kind == FieldValueKind::Enum && item.static_values.is_empty() {
        return Err(QueryErrorWire::profile(format!(
            "{}: enum fields must declare static_values",
            item.key
        )));
    }
    if !item.filterable && (item.repeatable || item.negatable) {
        return Err(QueryErrorWire::profile(format!(
            "{}: repeatable/negatable only apply to filterable fields",
            item.key
        )));
    }
    Ok(QueryFieldSpec {
        key: item.key.clone(),
        value_kind,
        filterable: item.filterable,
        searchable: item.searchable,
        repeatable: item.repeatable,
        negatable: item.negatable,
        exact_match: item.exact_match,
        static_values: item.static_values.clone(),
        hint: item.hint.clone(),
    })
}

fn validate_sigils(
    sigils: &[QuerySigilSpec],
    field_keys: &HashSet<String>,
) -> Result<(), QueryErrorWire> {
    let mut seen = HashSet::new();
    for sigil in sigils {
        if sigil.sigil.chars().count() != 1 {
            return Err(QueryErrorWire::profile(format!(
                "sigil '{}' is not a host-recognized sigil (valid sigils: {}, {}, {}, {})",
                sigil.sigil,
                HOST_SIGIL_CHARS[0],
                HOST_SIGIL_CHARS[1],
                HOST_SIGIL_CHARS[2],
                HOST_SIGIL_CHARS[3],
            )));
        }
        let ch = sigil.sigil.chars().next().unwrap();
        if !HOST_SIGIL_CHARS.contains(&ch) {
            return Err(QueryErrorWire::profile(format!(
                "sigil '{ch}' is not a host-recognized sigil (valid sigils: +, ^, ~, &)"
            )));
        }
        if !seen.insert(ch) {
            return Err(QueryErrorWire::profile(format!(
                "duplicate sigil: '{ch}'"
            )));
        }
        if !field_keys.contains(&sigil.field.to_ascii_lowercase()) {
            return Err(QueryErrorWire::profile(format!(
                "sigil '{ch}' targets undeclared field '{}'",
                sigil.field
            )));
        }
    }
    Ok(())
}

fn validate_predicates(predicates: &[String]) -> Result<(), QueryErrorWire> {
    let mut seen = HashSet::new();
    for name in predicates {
        if !HOST_PREDICATE_NAMES.contains(&name.as_str()) {
            return Err(QueryErrorWire::profile(format!(
                "unknown predicate '{}' (valid predicates: {})",
                name,
                HOST_PREDICATE_NAMES.join(", ")
            )));
        }
        if !seen.insert(name.as_str()) {
            return Err(QueryErrorWire::profile(format!(
                "duplicate predicate: '{name}'"
            )));
        }
    }
    Ok(())
}

fn validate_macros(
    macros: &[QueryMacroSpec],
    field_keys: &HashSet<String>,
) -> Result<(), QueryErrorWire> {
    let mut seen = HashSet::new();
    for macro_spec in macros {
        if macro_spec.trigger.chars().count() != 1 {
            return Err(QueryErrorWire::profile(format!(
                "macro trigger '{}' is not host-recognized (valid triggers: %)",
                macro_spec.trigger
            )));
        }
        let trigger = macro_spec.trigger.chars().next().unwrap();
        if !HOST_MACRO_TRIGGERS.contains(&trigger) {
            return Err(QueryErrorWire::profile(format!(
                "macro trigger '{trigger}' is not host-recognized (valid triggers: %)"
            )));
        }
        if macro_spec.letter.chars().count() != 1 {
            return Err(QueryErrorWire::profile(format!(
                "duplicate macro: {}{}",
                macro_spec.trigger, macro_spec.letter
            )));
        }
        let letter = macro_spec.letter.chars().next().unwrap();
        let key = (trigger, letter.to_ascii_lowercase());
        if !seen.insert(key) {
            return Err(QueryErrorWire::profile(format!(
                "duplicate macro: {}{}",
                macro_spec.trigger, macro_spec.letter
            )));
        }
        if !field_keys.contains(&macro_spec.field.to_ascii_lowercase()) {
            return Err(QueryErrorWire::profile(format!(
                "macro {}{} targets undeclared field '{}'",
                macro_spec.trigger, macro_spec.letter, macro_spec.field
            )));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_profile(
    pane_id: String,
    boolean: bool,
    fields: Vec<QueryFieldSpec>,
    sigils: Vec<QuerySigilSpec>,
    predicates: Vec<String>,
    any_special: bool,
    macros: Vec<QueryMacroSpec>,
    free_text_hint: String,
    digest: String,
) -> CompiledQueryProfile {
    let mut field_by_key = HashMap::with_capacity(fields.len());
    for (idx, field) in fields.iter().enumerate() {
        field_by_key.insert(field.key.to_ascii_lowercase(), idx);
    }
    let mut sigil_by_char = HashMap::with_capacity(sigils.len());
    for sigil in &sigils {
        if let Some(ch) = sigil.sigil.chars().next() {
            sigil_by_char.insert(ch, sigil.field.clone());
        }
    }
    let mut macros_by_trigger_letter = HashMap::with_capacity(macros.len());
    for item in &macros {
        let trigger = item.trigger.chars().next().unwrap_or('%');
        let letter = item
            .letter
            .chars()
            .next()
            .unwrap_or(' ')
            .to_ascii_lowercase();
        macros_by_trigger_letter.insert(
            (trigger, letter),
            (item.field.clone(), item.value.clone()),
        );
    }
    let predicate_set = predicates.iter().cloned().collect();
    CompiledQueryProfile {
        pane_id,
        boolean,
        fields,
        sigils,
        predicates,
        any_special,
        macros,
        free_text_hint,
        digest,
        field_by_key,
        sigil_by_char,
        macros_by_trigger_letter,
        predicate_set,
    }
}

#[allow(clippy::too_many_arguments)]
fn canonical_payload(
    pane_id: &str,
    boolean: bool,
    fields: &[QueryFieldSpec],
    sigils: &[QuerySigilSpec],
    predicates: &[String],
    any_special: bool,
    macros: &[QueryMacroSpec],
    free_text_hint: &str,
) -> Value {
    let mut payload = Map::new();
    payload.insert("pane_id".into(), Value::String(pane_id.to_string()));
    payload.insert("boolean".into(), Value::Bool(boolean));
    payload.insert(
        "fields".into(),
        Value::Array(fields.iter().map(field_payload).collect()),
    );
    payload.insert(
        "sigils".into(),
        Value::Array(
            sigils
                .iter()
                .map(|item| {
                    let mut map = Map::new();
                    map.insert(
                        "sigil".into(),
                        Value::String(item.sigil.clone()),
                    );
                    map.insert(
                        "field".into(),
                        Value::String(item.field.clone()),
                    );
                    Value::Object(map)
                })
                .collect(),
        ),
    );
    payload.insert(
        "predicates".into(),
        Value::Array(
            predicates
                .iter()
                .map(|item| Value::String(item.clone()))
                .collect(),
        ),
    );
    payload.insert("any_special".into(), Value::Bool(any_special));
    payload.insert(
        "macros".into(),
        Value::Array(
            macros
                .iter()
                .map(|item| {
                    let mut map = Map::new();
                    map.insert(
                        "trigger".into(),
                        Value::String(item.trigger.clone()),
                    );
                    map.insert(
                        "letter".into(),
                        Value::String(item.letter.clone()),
                    );
                    map.insert(
                        "field".into(),
                        Value::String(item.field.clone()),
                    );
                    map.insert(
                        "value".into(),
                        Value::String(item.value.clone()),
                    );
                    Value::Object(map)
                })
                .collect(),
        ),
    );
    payload.insert(
        "free_text_hint".into(),
        Value::String(free_text_hint.to_string()),
    );
    Value::Object(payload)
}

fn field_payload(item: &QueryFieldSpec) -> Value {
    let mut map = Map::new();
    map.insert("key".into(), Value::String(item.key.clone()));
    map.insert(
        "value_kind".into(),
        Value::String(item.value_kind.as_str().to_string()),
    );
    map.insert("filterable".into(), Value::Bool(item.filterable));
    map.insert("searchable".into(), Value::Bool(item.searchable));
    map.insert("repeatable".into(), Value::Bool(item.repeatable));
    map.insert("negatable".into(), Value::Bool(item.negatable));
    map.insert("exact_match".into(), Value::Bool(item.exact_match));
    map.insert(
        "static_values".into(),
        Value::Array(
            item.static_values
                .iter()
                .map(|value| Value::String(value.clone()))
                .collect(),
        ),
    );
    map.insert("hint".into(), Value::String(item.hint.clone()));
    Value::Object(map)
}

fn digest_payload(payload: &Value) -> String {
    let encoded = canonical_json(payload);
    hex::encode(Sha256::digest(encoded.as_bytes()))
}

/// JSON with recursively sorted object keys and no whitespace.
///
/// Matches Python `json.dumps(payload, sort_keys=True, separators=(",", ":"))`.
fn canonical_json(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(true) => "true".to_string(),
        Value::Bool(false) => "false".to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => {
            serde_json::to_string(text).unwrap_or_else(|_| "\"\"".to_string())
        }
        Value::Array(items) => {
            let inner = items
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",");
            format!("[{inner}]")
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let inner = keys
                .into_iter()
                .map(|key| {
                    let encoded_key = serde_json::to_string(key)
                        .unwrap_or_else(|_| "\"\"".to_string());
                    format!("{encoded_key}:{}", canonical_json(&map[key]))
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{inner}}}")
        }
    }
}

/// The built-in Patch compatibility profile.
///
/// Mirrors `sase.ace.query_profile.profiles.patches_query_schema` after
/// compilation so existing Patch entry points and an explicit Python
/// `to_wire()` payload share the same digest.
pub fn patch_query_profile() -> &'static CompiledQueryProfile {
    static PROFILE: OnceLock<CompiledQueryProfile> = OnceLock::new();
    PROFILE.get_or_init(build_patch_profile)
}

fn build_patch_profile() -> CompiledQueryProfile {
    let fields = vec![
        field(
            "ancestor",
            FieldValueKind::String,
            true,
            false,
            "patch or ancestor name",
        ),
        field(
            "description",
            FieldValueKind::String,
            false,
            true,
            "patch description",
        ),
        field(
            "name",
            FieldValueKind::String,
            true,
            true,
            "exact patch name",
        ),
        field(
            "notes",
            FieldValueKind::String,
            false,
            true,
            "stitch, hook, comment, and mentor notes and status suffixes",
        ),
        field(
            "origin",
            FieldValueKind::String,
            true,
            true,
            "sase, external, or unknown",
        ),
        field(
            "parent",
            FieldValueKind::String,
            false,
            true,
            "immediate parent patch name",
        ),
        field(
            "pr_url",
            FieldValueKind::String,
            false,
            true,
            "PR/CL url (alias cl)",
        ),
        field(
            "project",
            FieldValueKind::String,
            true,
            true,
            "project key or display name",
        ),
        field(
            "refs",
            FieldValueKind::String,
            false,
            true,
            "referenced artifact/task ids",
        ),
        field(
            "sibling",
            FieldValueKind::String,
            true,
            false,
            "shares the same __N revert family",
        ),
        field("status", FieldValueKind::String, true, true, "patch status"),
    ];
    let sigils = vec![
        QuerySigilSpec {
            sigil: "&".into(),
            field: "name".into(),
        },
        QuerySigilSpec {
            sigil: "+".into(),
            field: "project".into(),
        },
        QuerySigilSpec {
            sigil: "^".into(),
            field: "ancestor".into(),
        },
        QuerySigilSpec {
            sigil: "~".into(),
            field: "sibling".into(),
        },
    ];
    let predicates = HOST_PREDICATE_NAMES
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    let macros = vec![
        status_macro("d", "DRAFT"),
        status_macro("m", "MAILED"),
        status_macro("r", "REVERTED"),
        status_macro("s", "SUBMITTED"),
        status_macro("w", "WIP"),
        status_macro("y", "READY"),
    ];
    let free_text_hint = "name, description, status, origin, project, refs, parent, pr_url, notes (implicit AND)";
    let payload = canonical_payload(
        "patches",
        true,
        &fields,
        &sigils,
        &predicates,
        true,
        &macros,
        free_text_hint,
    );
    let digest = digest_payload(&payload);
    build_profile(
        "patches".into(),
        true,
        fields,
        sigils,
        predicates,
        true,
        macros,
        free_text_hint.into(),
        digest,
    )
}

/// Build a Patch profile field. Every *filterable* Patch field matches
/// exactly (never by substring): Patch's boolean grammar has always
/// compared property values for exact equality. `exact_match` only means
/// anything for filterable fields (see `QueryFieldSpec`'s doc comment), so
/// it mirrors `filterable` here to match the Python compiler's payload
/// byte-for-byte instead of leaving digest-irrelevant search-only fields
/// with a value Python never sets.
fn field(
    key: &str,
    value_kind: FieldValueKind,
    filterable: bool,
    searchable: bool,
    hint: &str,
) -> QueryFieldSpec {
    QueryFieldSpec {
        key: key.to_string(),
        value_kind,
        filterable,
        searchable,
        repeatable: false,
        negatable: false,
        exact_match: filterable,
        static_values: Vec::new(),
        hint: hint.to_string(),
    }
}

fn status_macro(letter: &str, value: &str) -> QueryMacroSpec {
    QueryMacroSpec {
        trigger: "%".into(),
        letter: letter.into(),
        field: "status".into(),
        value: value.into(),
    }
}

/// Join a list of shorthand tokens the way the Patch tokenizer does.
pub fn join_shorthand_list(items: &[String]) -> String {
    match items.len() {
        0 => String::new(),
        1 => items[0].clone(),
        2 => format!("{} or {}", items[0], items[1]),
        _ => {
            let head = &items[..items.len() - 1];
            format!("{}, or {}", head.join(", "), items[items.len() - 1])
        }
    }
}

/// Build a one-off profile for tests from already-validated parts.
#[cfg(test)]
pub fn profile_from_parts(
    pane_id: &str,
    boolean: bool,
    fields: Vec<QueryFieldSpec>,
    sigils: Vec<QuerySigilSpec>,
    predicates: Vec<String>,
    any_special: bool,
    macros: Vec<QueryMacroSpec>,
) -> Result<CompiledQueryProfile, QueryErrorWire> {
    let payload = canonical_payload(
        pane_id,
        boolean,
        &fields,
        &sigils,
        &predicates,
        any_special,
        &macros,
        "",
    );
    CompiledQueryProfile::from_wire(&payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patch_profile_digest_matches_python_compiler() {
        let profile = patch_query_profile();
        assert_eq!(
            profile.digest,
            "a679434628641ed40ba1cc46be56255f63960edbd5249ca3acdc96b8b1012180"
        );
        let mut payload = match canonical_payload(
            &profile.pane_id,
            profile.boolean,
            &profile.fields,
            &profile.sigils,
            &profile.predicates,
            profile.any_special,
            &profile.macros,
            &profile.free_text_hint,
        ) {
            Value::Object(map) => map,
            other => panic!("expected object, got {other}"),
        };
        payload.insert("digest".into(), Value::String(profile.digest.clone()));
        let loaded =
            CompiledQueryProfile::from_wire(&Value::Object(payload)).unwrap();
        assert_eq!(loaded.digest, profile.digest);
        assert_eq!(loaded.pane_id, "patches");
        assert!(loaded.boolean);
    }

    #[test]
    fn from_wire_rejects_unknown_predicate() {
        let wire = serde_json::to_value(canonical_payload(
            "test",
            false,
            &[field("alpha", FieldValueKind::String, true, false, "")],
            &[],
            &["made_up".into()],
            false,
            &[],
            "",
        ))
        .unwrap();
        let err = CompiledQueryProfile::from_wire(&wire).unwrap_err();
        assert_eq!(err.kind, "profile");
        assert!(err.message.contains("unknown predicate"), "{err}");
    }

    #[test]
    fn from_wire_rejects_digest_mismatch() {
        let mut map = match canonical_payload(
            "test",
            false,
            &[field("alpha", FieldValueKind::String, true, false, "")],
            &[],
            &[],
            false,
            &[],
            "",
        ) {
            Value::Object(map) => map,
            other => panic!("expected object, got {other}"),
        };
        map.insert(
            "digest".into(),
            Value::String("not-the-real-digest".into()),
        );
        let err =
            CompiledQueryProfile::from_wire(&Value::Object(map)).unwrap_err();
        assert!(err.message.contains("digest"), "{err}");
    }
}
