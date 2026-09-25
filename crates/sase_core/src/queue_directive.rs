//! Shared `%queue` / `%q` argument contract.
//!
//! This module owns queue-field normalization, validation, collection across
//! occurrences, and canonical formatting. Callers reuse existing directive
//! occurrence scanning. This module never reads global configuration.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Legacy feature-flag key retained for older bindings. `%queue` is always enabled.
pub const QUEUE_DIRECTIVE_FLAG: &str = "queue_directive";
pub const QUEUE_CAPACITY_BUDGET_FLAG: &str = "queue_capacity_budget";
pub const DEFAULT_QUEUE_WEIGHT: f64 = 1.0;

/// Legacy feature-flag key retained for older bindings. `%queue` is always enabled.
pub fn queue_directive_flag_key() -> &'static str {
    QUEUE_DIRECTIVE_FLAG
}

/// Return whether `%queue` / `%q` is enabled.
///
/// The argument is kept for source compatibility with callers from the temporary
/// migration window.
pub fn queue_directive_enabled(_enabled_feature_flags: &[String]) -> bool {
    true
}

pub fn queue_capacity_budget_enabled(enabled_feature_flags: &[String]) -> bool {
    enabled_feature_flags
        .iter()
        .any(|flag| flag == QUEUE_CAPACITY_BUDGET_FLAG)
}

/// Fold legacy `wait_runners*` keys into canonical `queue_capacity*`.
/// Canonical values win when both spellings are present.
pub fn merge_queue_capacity_aliases(map: &mut Map<String, Value>) {
    if map.contains_key("queue_capacity") {
        map.remove("wait_runners");
    } else if let Some(legacy) = map.remove("wait_runners") {
        map.insert("queue_capacity".to_string(), legacy);
    }
    if map.contains_key("queue_capacity_explicit") {
        map.remove("wait_runners_explicit");
    } else if let Some(legacy) = map.remove("wait_runners_explicit") {
        map.insert("queue_capacity_explicit".to_string(), legacy);
    }
}

/// Merge capacity aliases on `agent_meta` and `waiting` objects of a scan record.
pub fn merge_queue_capacity_aliases_in_scan_record(value: &mut Value) {
    let Value::Object(map) = value else {
        return;
    };
    if let Some(Value::Object(meta)) = map.get_mut("agent_meta") {
        merge_queue_capacity_aliases(meta);
    }
    if let Some(Value::Object(waiting)) = map.get_mut("waiting") {
        merge_queue_capacity_aliases(waiting);
    }
}

/// Merge capacity aliases on a runner-capacity request and its nested records.
pub fn merge_queue_capacity_aliases_in_capacity_request(value: &mut Value) {
    let Value::Object(map) = value else {
        return;
    };
    if let Some(Value::Array(records)) = map.get_mut("records") {
        for record in records {
            if let Value::Object(item) = record {
                merge_queue_capacity_aliases(item);
            }
        }
    }
    if let Some(Value::Object(candidate)) = map.get_mut("candidate") {
        merge_queue_capacity_aliases(candidate);
    }
}

/// Prefer canonical `queue_capacity*` over legacy `wait_runners*`.
pub fn resolve_queue_capacity(
    canonical: Option<i64>,
    legacy: Option<i64>,
    canonical_explicit: bool,
    legacy_explicit: bool,
) -> (Option<i64>, bool) {
    if canonical.is_some() {
        (canonical, canonical_explicit)
    } else if legacy.is_some() {
        (legacy, legacy_explicit)
    } else {
        (None, canonical_explicit || legacy_explicit)
    }
}

/// Read authored capacity from a JSON object, accepting either spelling.
///
/// Omission (`None`/`false`), explicit false, and explicit zero stay
/// distinguishable. Canonical names win when both spellings are present.
pub fn queue_capacity_from_map(
    data: &Map<String, Value>,
) -> (Option<i64>, bool) {
    let canonical_present = data.contains_key("queue_capacity");
    let explicit_present = data.contains_key("queue_capacity_explicit");
    let capacity = if canonical_present {
        json_int(data.get("queue_capacity"))
    } else {
        json_int(data.get("wait_runners"))
    };
    let explicit = if explicit_present {
        json_truthy(data.get("queue_capacity_explicit"))
    } else {
        json_truthy(data.get("wait_runners_explicit"))
    };
    (capacity, explicit)
}

pub fn queue_capacity_as_u32(value: Option<i64>) -> Option<u32> {
    value.and_then(|value| u32::try_from(value).ok())
}

/// Read a persisted capacity multiplier when it is a valid JSON number.
///
/// Unlike integer capacity, a multiplier is always explicit by its presence.
/// Invalid persisted values deliberately behave as omitted, matching the
/// tolerant reader behavior for invalid persisted integer capacities.
pub fn queue_capacity_multiplier_from_map(
    data: &Map<String, Value>,
) -> Option<f64> {
    data.get("queue_capacity_multiplier")
        .and_then(Value::as_f64)
        .filter(|value| queue_capacity_multiplier_is_valid(*value))
}

/// Shared persisted-zero translation for admission and continuation resume.
///
/// Newly authored zero capacity remains a parse error when the budget flag is on.
/// A persisted explicit zero becomes an exact effective-weight drain budget
/// rather than a rounded integer or a silent fall-back to the global limit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistedQueueCapacityNormWire {
    pub admission_limit: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authored_capacity: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authored_multiplier: Option<f64>,
    pub authored_explicit: bool,
    pub legacy_zero: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reauthor_capacity: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reauthor_multiplier: Option<f64>,
}

pub fn normalize_persisted_queue_capacity(
    queue_capacity: Option<u32>,
    queue_capacity_explicit: bool,
    effective_weight: f64,
    global_limit: f64,
    capacity_budget: bool,
) -> PersistedQueueCapacityNormWire {
    normalize_persisted_queue_capacity_with_multiplier(
        queue_capacity,
        queue_capacity_explicit,
        None,
        effective_weight,
        global_limit,
        capacity_budget,
    )
}

/// Normalize persisted capacity forms for admission and continuation
/// reauthoring. An integer wins over a multiplier when both are present so an
/// older integer-only writer cannot accidentally change an existing budget.
pub fn normalize_persisted_queue_capacity_with_multiplier(
    queue_capacity: Option<u32>,
    queue_capacity_explicit: bool,
    queue_capacity_multiplier: Option<f64>,
    effective_weight: f64,
    global_limit: f64,
    capacity_budget: bool,
) -> PersistedQueueCapacityNormWire {
    let multiplier = if queue_capacity.is_none() {
        queue_capacity_multiplier
            .filter(|value| queue_capacity_multiplier_is_valid(*value))
    } else {
        None
    };
    if let Some(multiplier) = multiplier {
        return PersistedQueueCapacityNormWire {
            admission_limit: if capacity_budget {
                resolve_queue_capacity_multiplier(multiplier, global_limit)
                    .unwrap_or(global_limit)
            } else {
                global_limit
            },
            authored_capacity: None,
            authored_multiplier: Some(multiplier),
            authored_explicit: true,
            legacy_zero: false,
            reauthor_capacity: None,
            reauthor_multiplier: Some(multiplier),
        };
    }
    if !capacity_budget || !queue_capacity_explicit {
        return PersistedQueueCapacityNormWire {
            admission_limit: global_limit,
            authored_capacity: queue_capacity,
            authored_multiplier: None,
            authored_explicit: queue_capacity_explicit,
            legacy_zero: false,
            reauthor_capacity: queue_capacity,
            reauthor_multiplier: None,
        };
    }
    match queue_capacity {
        Some(0) => PersistedQueueCapacityNormWire {
            admission_limit: effective_weight,
            authored_capacity: Some(0),
            authored_multiplier: None,
            authored_explicit: true,
            legacy_zero: true,
            reauthor_capacity: None,
            reauthor_multiplier: None,
        },
        Some(capacity) => PersistedQueueCapacityNormWire {
            admission_limit: f64::from(capacity),
            authored_capacity: Some(capacity),
            authored_multiplier: None,
            authored_explicit: true,
            legacy_zero: false,
            reauthor_capacity: Some(capacity),
            reauthor_multiplier: None,
        },
        None => PersistedQueueCapacityNormWire {
            admission_limit: global_limit,
            authored_capacity: None,
            authored_multiplier: None,
            authored_explicit: true,
            legacy_zero: false,
            reauthor_capacity: None,
            reauthor_multiplier: None,
        },
    }
}

fn json_int(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::Bool(_)) | None | Some(Value::Null) => None,
        Some(Value::Number(n)) => {
            n.as_i64().or_else(|| n.as_f64().map(|float| float as i64))
        }
        Some(Value::String(s)) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn json_truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(flag)) => *flag,
        Some(Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                i != 0
            } else if let Some(u) = n.as_u64() {
                u != 0
            } else {
                n.as_f64().map(|float| float != 0.0).unwrap_or(false)
            }
        }
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Array(items)) => !items.is_empty(),
        Some(Value::Object(object)) => !object.is_empty(),
    }
}

/// One already-split queue argument. `name` is absent for positionals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct QueueArgWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default)]
    pub value: String,
}

/// One `%queue` / `%q` occurrence supplied by a caller-owned scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueOccurrenceWire {
    pub source: String,
    pub source_span: [usize; 2],
    #[serde(default)]
    pub args: Vec<QueueArgWire>,
    #[serde(default)]
    pub has_plus_suffix: bool,
}

/// Canonical queue fields. `None` means omitted, distinct from explicit zero,
/// explicit default priority 10, or explicit default weight 1.0.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct QueueFieldsWire {
    #[serde(
        default,
        alias = "capacity",
        skip_serializing_if = "Option::is_none"
    )]
    pub queue_capacity: Option<u32>,
    #[serde(
        default,
        alias = "capacity_multiplier",
        skip_serializing_if = "Option::is_none"
    )]
    pub queue_capacity_multiplier: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<f64>,
}

/// One authored capacity form: an integer budget or a `<M>x` multiplier.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct QueueCapacityValueWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_capacity: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_capacity_multiplier: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueCapacityKind {
    Absolute(u32),
    MultiplierHundredths(u32),
}

/// Actionable queue parse failure with the source span of the occurrence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueParseErrorWire {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_span: Option<[usize; 2]>,
}

/// Result of collecting queue fields across occurrences.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueCollectResultWire {
    #[serde(default)]
    pub fields: Option<QueueFieldsWire>,
    #[serde(default)]
    pub errors: Vec<QueueParseErrorWire>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueForm {
    Bare,
    Plus,
    Colon,
    Parenthesized { closed: bool },
}

/// Parse and merge every queue occurrence. Disjoint fields compose; any
/// duplicate canonical field is an error even when the values match.
pub fn collect_queue_fields(
    occurrences: &[QueueOccurrenceWire],
) -> QueueCollectResultWire {
    collect_queue_fields_with_flags(occurrences, &[])
}

pub fn collect_queue_fields_with_flags(
    occurrences: &[QueueOccurrenceWire],
    enabled_feature_flags: &[String],
) -> QueueCollectResultWire {
    let mut fields = QueueFieldsWire::default();
    let mut errors = Vec::new();
    let capacity_budget = queue_capacity_budget_enabled(enabled_feature_flags);
    for occurrence in occurrences {
        match parse_queue_occurrence(occurrence, capacity_budget) {
            Ok(part) => {
                if let Err(error) =
                    merge_queue_part(&mut fields, part, occurrence.source_span)
                {
                    errors.push(error);
                }
            }
            Err(error) => errors.push(error),
        }
    }
    if errors.is_empty() {
        if let Err(error) =
            validate_queue_budget_fields(&fields, capacity_budget)
        {
            errors.push(error);
        }
    }
    if errors.is_empty() {
        QueueCollectResultWire {
            fields: Some(fields),
            errors: Vec::new(),
        }
    } else {
        QueueCollectResultWire {
            fields: None,
            errors,
        }
    }
}

/// Canonical generated form: `%queue(capacity=N, priority=P, weight=W)`,
/// omitting absent fields. Returns `None` when every field is omitted.
pub fn format_queue_directive(fields: &QueueFieldsWire) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(capacity) = fields.queue_capacity {
        parts.push(format!("capacity={capacity}"));
    } else if let Some(formatted) = fields
        .queue_capacity_multiplier
        .and_then(format_queue_capacity_multiplier)
    {
        parts.push(format!("capacity={formatted}"));
    }
    if let Some(priority) = fields.priority {
        parts.push(format!("priority={priority}"));
    }
    if let Some(weight) = fields.weight {
        if authored_queue_weight_is_valid(weight) {
            parts.push(format!("weight={}", format_queue_weight(weight)));
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(format!("%queue({})", parts.join(", ")))
    }
}

pub fn queue_weight_is_valid(value: f64) -> bool {
    value.is_finite() && value > 0.0
}

/// Return whether a weight is valid after it has been authored explicitly.
///
/// Capacity limits retain the stricter [`queue_weight_is_valid`] contract:
/// unlike a launch weight, a zero capacity limit is never meaningful.
pub fn authored_queue_weight_is_valid(value: f64) -> bool {
    value.is_finite() && value >= 0.0
}

pub fn format_queue_weight(value: f64) -> String {
    if value == 0.0 {
        "0".to_string()
    } else {
        value.to_string()
    }
}

/// Validate a capacity threshold string through the same contract as
/// `%queue(capacity=...)`.
pub fn parse_queue_capacity(raw: &str) -> Result<u32, QueueParseErrorWire> {
    parse_queue_capacity_with_flags(raw, &[])
}

pub fn parse_queue_capacity_with_flags(
    raw: &str,
    enabled_feature_flags: &[String],
) -> Result<u32, QueueParseErrorWire> {
    parse_capacity_absolute(
        raw,
        None,
        queue_capacity_budget_enabled(enabled_feature_flags),
    )
}

/// Validate an integer capacity or `<M>x` multiplier through the same
/// contract as `%queue(capacity=...)`.
pub fn parse_queue_capacity_value(
    raw: &str,
) -> Result<QueueCapacityValueWire, QueueParseErrorWire> {
    parse_queue_capacity_value_with_flags(raw, &[])
}

pub fn parse_queue_capacity_value_with_flags(
    raw: &str,
    enabled_feature_flags: &[String],
) -> Result<QueueCapacityValueWire, QueueParseErrorWire> {
    Ok(capacity_kind_to_wire(parse_capacity(
        raw,
        None,
        queue_capacity_budget_enabled(enabled_feature_flags),
    )?))
}

/// Return whether `value` is a finite multiplier greater than zero, with at
/// most two decimal places under a small rounding tolerance, and hundredths
/// that fit in `u32`.
pub fn queue_capacity_multiplier_is_valid(value: f64) -> bool {
    queue_capacity_multiplier_hundredths(value).is_some()
}

/// Canonical `<M>x` spelling with trailing zeros trimmed (`1.5x`, `2x`).
pub fn format_queue_capacity_multiplier(value: f64) -> Option<String> {
    queue_capacity_multiplier_hundredths(value)
        .map(format_multiplier_hundredths)
}

/// Round `hundredths × effective_limit / 100` to two decimal places.
pub fn resolve_queue_capacity_multiplier(
    multiplier: f64,
    effective_limit: f64,
) -> Option<f64> {
    if !effective_limit.is_finite() || effective_limit < 0.0 {
        return None;
    }
    let hundredths = queue_capacity_multiplier_hundredths(multiplier)?;
    let product = f64::from(hundredths) * effective_limit / 100.0;
    if !product.is_finite() {
        return None;
    }
    Some((product * 100.0).round() / 100.0)
}

/// Legacy diagnostic message from the temporary migration window.
pub fn queue_directive_disabled_message() -> String {
    "%queue is enabled by default.".to_string()
}

fn parse_queue_occurrence(
    occurrence: &QueueOccurrenceWire,
    capacity_budget: bool,
) -> Result<QueueFieldsWire, QueueParseErrorWire> {
    let span = Some(occurrence.source_span);
    match queue_form(&occurrence.source, occurrence.has_plus_suffix) {
        QueueForm::Plus => Err(queue_error(
            "queue-plus-unsupported",
            "%queue does not support '+'; use %q:N or %queue(...).",
            span,
        )),
        QueueForm::Bare => Err(empty_queue_error(span)),
        QueueForm::Colon => parse_colon_occurrence(occurrence, capacity_budget),
        QueueForm::Parenthesized { closed: false } => Err(queue_error(
            "malformed-queue",
            "Malformed %queue(...) directive: missing closing ')'.",
            span,
        )),
        QueueForm::Parenthesized { closed: true } => {
            parse_parenthesized_occurrence(occurrence, capacity_budget)
        }
    }
}

fn parse_colon_occurrence(
    occurrence: &QueueOccurrenceWire,
    capacity_budget: bool,
) -> Result<QueueFieldsWire, QueueParseErrorWire> {
    let span = Some(occurrence.source_span);
    if occurrence.args.iter().any(|arg| arg.name.is_some()) {
        return Err(queue_error(
            "queue-colon-keyword",
            "%queue colon form supplies positional capacity only; use parentheses for keywords.",
            span,
        ));
    }
    let positionals: Vec<&QueueArgWire> = occurrence
        .args
        .iter()
        .filter(|arg| !arg.value.is_empty())
        .collect();
    if positionals.is_empty() {
        return Err(empty_queue_error(span));
    }
    if positionals.len() > 1 {
        return Err(extra_positional_error(span));
    }
    Ok(capacity_kind_to_fields(parse_capacity(
        &positionals[0].value,
        span,
        capacity_budget,
    )?))
}

fn parse_parenthesized_occurrence(
    occurrence: &QueueOccurrenceWire,
    capacity_budget: bool,
) -> Result<QueueFieldsWire, QueueParseErrorWire> {
    let span = Some(occurrence.source_span);
    if occurrence.args.is_empty()
        || occurrence
            .args
            .iter()
            .all(|arg| arg.name.is_none() && arg.value.is_empty())
    {
        return Err(empty_queue_error(span));
    }

    let mut fields = QueueFieldsWire::default();
    let mut seen_literals = Vec::new();
    let mut positional_seen = false;
    for arg in &occurrence.args {
        match arg.name.as_deref().map(str::trim) {
            None => {
                if arg.value.is_empty() {
                    continue;
                }
                if positional_seen {
                    return Err(extra_positional_error(span));
                }
                positional_seen = true;
                assign_capacity(
                    &mut fields,
                    &arg.value,
                    span,
                    capacity_budget,
                )?;
            }
            Some(literal) => {
                if literal.is_empty() {
                    return Err(queue_error(
                        "unknown-queue-keyword",
                        "Unsupported keyword on %queue: empty name. Use capacity=, priority=, p=, weight=, or w=.",
                        span,
                    ));
                }
                let key = literal.to_ascii_lowercase();
                if key == "runners" {
                    return Err(obsolete_runners_error(span));
                }
                if seen_literals.iter().any(|seen: &String| seen == &key) {
                    return Err(duplicate_field_error(
                        &canonical_queue_key(&key),
                        span,
                    ));
                }
                seen_literals.push(key.clone());
                match canonical_queue_key(&key).as_str() {
                    "capacity" => {
                        assign_capacity(
                            &mut fields,
                            &arg.value,
                            span,
                            capacity_budget,
                        )?;
                    }
                    "priority" => {
                        assign_priority(&mut fields, &arg.value, span)?;
                    }
                    "weight" => {
                        assign_weight(&mut fields, &arg.value, span)?;
                    }
                    "agent" | "bead" | "proc" | "unit" | "time" => {
                        return Err(queue_error(
                            "queue-wait-keyword",
                            &format!(
                                "%queue does not accept {literal}=; keep dependencies and time floors on %wait."
                            ),
                            span,
                        ));
                    }
                    _ => {
                        return Err(queue_error(
                            "unknown-queue-keyword",
                            &format!(
                                "Unsupported keyword on %queue: {literal}=. Use capacity=, priority=, p=, weight=, or w=."
                            ),
                            span,
                        ));
                    }
                }
            }
        }
    }
    if !queue_fields_has_capacity(&fields)
        && fields.priority.is_none()
        && fields.weight.is_none()
    {
        return Err(empty_queue_error(span));
    }
    Ok(fields)
}

fn assign_capacity(
    fields: &mut QueueFieldsWire,
    raw: &str,
    span: Option<[usize; 2]>,
    capacity_budget: bool,
) -> Result<(), QueueParseErrorWire> {
    if queue_fields_has_capacity(fields) {
        return Err(duplicate_field_error("capacity", span));
    }
    apply_capacity_kind(fields, parse_capacity(raw, span, capacity_budget)?);
    Ok(())
}

fn assign_priority(
    fields: &mut QueueFieldsWire,
    raw: &str,
    span: Option<[usize; 2]>,
) -> Result<(), QueueParseErrorWire> {
    if fields.priority.is_some() {
        return Err(duplicate_field_error("priority", span));
    }
    fields.priority = Some(parse_priority(raw, span)?);
    Ok(())
}

fn assign_weight(
    fields: &mut QueueFieldsWire,
    raw: &str,
    span: Option<[usize; 2]>,
) -> Result<(), QueueParseErrorWire> {
    if fields.weight.is_some() {
        return Err(duplicate_field_error("weight", span));
    }
    fields.weight = Some(parse_weight(raw, span)?);
    Ok(())
}

fn merge_queue_part(
    fields: &mut QueueFieldsWire,
    part: QueueFieldsWire,
    span: [usize; 2],
) -> Result<(), QueueParseErrorWire> {
    if queue_fields_has_capacity(&part) {
        if queue_fields_has_capacity(fields) {
            return Err(duplicate_field_error("capacity", Some(span)));
        }
        fields.queue_capacity = part.queue_capacity;
        fields.queue_capacity_multiplier = part.queue_capacity_multiplier;
    }
    if let Some(priority) = part.priority {
        if fields.priority.is_some() {
            return Err(duplicate_field_error("priority", Some(span)));
        }
        fields.priority = Some(priority);
    }
    if let Some(weight) = part.weight {
        if fields.weight.is_some() {
            return Err(duplicate_field_error("weight", Some(span)));
        }
        fields.weight = Some(weight);
    }
    Ok(())
}

fn parse_capacity(
    raw: &str,
    span: Option<[usize; 2]>,
    capacity_budget: bool,
) -> Result<QueueCapacityKind, QueueParseErrorWire> {
    if raw.ends_with('x') {
        return parse_capacity_multiplier(raw, span)
            .map(QueueCapacityKind::MultiplierHundredths);
    }
    if raw.ends_with('X') {
        return Err(invalid_capacity_multiplier_error(span));
    }
    parse_capacity_absolute(raw, span, capacity_budget)
        .map(QueueCapacityKind::Absolute)
}

fn parse_capacity_absolute(
    raw: &str,
    span: Option<[usize; 2]>,
    capacity_budget: bool,
) -> Result<u32, QueueParseErrorWire> {
    let digits = parse_non_negative_decimal(raw)
        .map_err(|kind| integer_error("capacity", kind, span))?;
    let value = digits.parse::<u32>().map_err(|_| {
        queue_error(
            "queue-overflow-capacity",
            &format!(
                "%queue(capacity=...) exceeds the u32 maximum of {}.",
                u32::MAX
            ),
            span,
        )
    })?;
    if capacity_budget && value == 0 {
        return Err(invalid_capacity_zero_error(span));
    }
    Ok(value)
}

fn parse_capacity_multiplier(
    raw: &str,
    span: Option<[usize; 2]>,
) -> Result<u32, QueueParseErrorWire> {
    let body = raw.strip_suffix('x').unwrap_or(raw);
    match parse_multiplier_hundredths(body) {
        Ok(hundredths) => Ok(hundredths),
        Err(MultiplierParseError::TooManyDecimals) => Err(queue_error(
            "invalid-queue-capacity-multiplier",
            "%queue(capacity=...) multiplier <M>x allows at most two decimal places.",
            span,
        )),
        Err(MultiplierParseError::Zero) => Err(queue_error(
            "invalid-queue-capacity-multiplier",
            "%queue(capacity=...) multiplier <M>x must be greater than zero.",
            span,
        )),
        Err(MultiplierParseError::Overflow) => Err(queue_error(
            "queue-overflow-capacity-multiplier",
            &format!(
                "%queue(capacity=...) multiplier hundredths exceed the u32 maximum of {}.",
                u32::MAX
            ),
            span,
        )),
        Err(MultiplierParseError::Invalid) => {
            Err(invalid_capacity_multiplier_error(span))
        }
    }
}

enum MultiplierParseError {
    Invalid,
    TooManyDecimals,
    Zero,
    Overflow,
}

fn parse_multiplier_hundredths(
    body: &str,
) -> Result<u32, MultiplierParseError> {
    if body.is_empty()
        || body.starts_with('+')
        || body.starts_with('-')
        || body.contains(['e', 'E'])
    {
        return Err(MultiplierParseError::Invalid);
    }
    let (int_part, frac_part) = match body.split_once('.') {
        Some((int_part, frac_part)) => (int_part, Some(frac_part)),
        None => (body, None),
    };
    if let Some(frac_part) = frac_part {
        if frac_part.is_empty() {
            return Err(MultiplierParseError::Invalid);
        }
        if frac_part.len() > 2 {
            return Err(MultiplierParseError::TooManyDecimals);
        }
        if !frac_part.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(MultiplierParseError::Invalid);
        }
        if !int_part.is_empty()
            && !int_part.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(MultiplierParseError::Invalid);
        }
    } else if int_part.is_empty()
        || !int_part.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(MultiplierParseError::Invalid);
    }
    let whole = if int_part.is_empty() {
        0
    } else {
        int_part
            .parse::<u32>()
            .map_err(|_| MultiplierParseError::Overflow)?
    };
    let frac_hundredths = match frac_part {
        None => 0,
        Some(frac) if frac.len() == 1 => {
            frac.parse::<u32>().expect("fractional digit") * 10
        }
        Some(frac) => frac.parse::<u32>().expect("fractional digits"),
    };
    let hundredths = whole
        .checked_mul(100)
        .and_then(|value| value.checked_add(frac_hundredths))
        .ok_or(MultiplierParseError::Overflow)?;
    if hundredths == 0 {
        return Err(MultiplierParseError::Zero);
    }
    Ok(hundredths)
}

const MULTIPLIER_HUNDREDTHS_TOLERANCE: f64 = 1e-6;

fn queue_capacity_multiplier_hundredths(value: f64) -> Option<u32> {
    if !value.is_finite() || value <= 0.0 {
        return None;
    }
    let scaled = value * 100.0;
    if !scaled.is_finite() {
        return None;
    }
    let rounded = scaled.round();
    if (scaled - rounded).abs() > MULTIPLIER_HUNDREDTHS_TOLERANCE {
        return None;
    }
    if rounded < 1.0 || rounded > f64::from(u32::MAX) {
        return None;
    }
    Some(rounded as u32)
}

fn format_multiplier_hundredths(hundredths: u32) -> String {
    let whole = hundredths / 100;
    let frac = hundredths % 100;
    if frac == 0 {
        format!("{whole}x")
    } else if frac.is_multiple_of(10) {
        format!("{whole}.{}x", frac / 10)
    } else {
        format!("{whole}.{frac:02}x")
    }
}

fn hundredths_to_multiplier(hundredths: u32) -> f64 {
    f64::from(hundredths) / 100.0
}

fn queue_fields_has_capacity(fields: &QueueFieldsWire) -> bool {
    fields.queue_capacity.is_some()
        || fields.queue_capacity_multiplier.is_some()
}

fn apply_capacity_kind(fields: &mut QueueFieldsWire, kind: QueueCapacityKind) {
    match kind {
        QueueCapacityKind::Absolute(capacity) => {
            fields.queue_capacity = Some(capacity);
            fields.queue_capacity_multiplier = None;
        }
        QueueCapacityKind::MultiplierHundredths(hundredths) => {
            fields.queue_capacity = None;
            fields.queue_capacity_multiplier =
                Some(hundredths_to_multiplier(hundredths));
        }
    }
}

fn capacity_kind_to_fields(kind: QueueCapacityKind) -> QueueFieldsWire {
    let mut fields = QueueFieldsWire::default();
    apply_capacity_kind(&mut fields, kind);
    fields
}

fn capacity_kind_to_wire(kind: QueueCapacityKind) -> QueueCapacityValueWire {
    match kind {
        QueueCapacityKind::Absolute(capacity) => QueueCapacityValueWire {
            queue_capacity: Some(capacity),
            queue_capacity_multiplier: None,
        },
        QueueCapacityKind::MultiplierHundredths(hundredths) => {
            QueueCapacityValueWire {
                queue_capacity: None,
                queue_capacity_multiplier: Some(hundredths_to_multiplier(
                    hundredths,
                )),
            }
        }
    }
}

fn validate_queue_budget_fields(
    fields: &QueueFieldsWire,
    capacity_budget: bool,
) -> Result<(), QueueParseErrorWire> {
    if !capacity_budget {
        return Ok(());
    }
    if fields.queue_capacity_multiplier.is_some() {
        return Ok(());
    }
    if let (Some(capacity), Some(weight)) =
        (fields.queue_capacity, fields.weight)
    {
        if weight > f64::from(capacity) {
            return Err(queue_error(
                "queue-weight-exceeds-capacity",
                "%queue weight exceeds this launch's capacity budget; the launch could never be admitted. Increase capacity or lower weight.",
                None,
            ));
        }
    }
    Ok(())
}

fn parse_priority(
    raw: &str,
    span: Option<[usize; 2]>,
) -> Result<i32, QueueParseErrorWire> {
    let digits = parse_non_negative_decimal(raw)
        .map_err(|kind| integer_error("priority", kind, span))?;
    let parsed = digits.parse::<u64>().map_err(|_| overflow_priority(span))?;
    i32::try_from(parsed).map_err(|_| overflow_priority(span))
}

fn parse_weight(
    raw: &str,
    span: Option<[usize; 2]>,
) -> Result<f64, QueueParseErrorWire> {
    if raw.is_empty() || raw.starts_with('-') {
        return Err(weight_error(span));
    }
    let Ok(parsed) = raw.parse::<f64>() else {
        return Err(weight_error(span));
    };
    if parsed == 0.0 {
        if zero_weight_literal_is_valid(raw) {
            return Ok(0.0);
        }
        return Err(weight_error(span));
    }
    if !queue_weight_is_valid(parsed) {
        return Err(weight_error(span));
    }
    Ok(parsed)
}

/// Distinguish a literal zero from a nonzero literal that underflowed to zero.
/// `f64` parsing intentionally happens first so this helper only validates the
/// significand of an already-valid float spelling.
fn zero_weight_literal_is_valid(raw: &str) -> bool {
    let raw = raw.strip_prefix('+').unwrap_or(raw);
    let mantissa = raw.split(['e', 'E']).next().unwrap_or_default();
    let mut saw_digit = false;
    for character in mantissa.chars() {
        match character {
            '0' => saw_digit = true,
            '.' => {}
            _ => return false,
        }
    }
    saw_digit
}

enum InvalidInt {
    Empty,
    Invalid,
}

fn parse_non_negative_decimal(raw: &str) -> Result<&str, InvalidInt> {
    if raw.is_empty() {
        return Err(InvalidInt::Empty);
    }
    if !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(InvalidInt::Invalid);
    }
    Ok(raw)
}

fn integer_error(
    field: &str,
    kind: InvalidInt,
    span: Option<[usize; 2]>,
) -> QueueParseErrorWire {
    let code = match field {
        "priority" => "invalid-queue-priority",
        _ => "invalid-queue-capacity",
    };
    let message = match (field, kind) {
        ("capacity", InvalidInt::Empty) => {
            "%queue(capacity=...) requires a non-negative integer or a multiplier of the form <M>x.".to_string()
        }
        ("capacity", InvalidInt::Invalid) => {
            "%queue(capacity=...) requires a non-negative decimal integer or a multiplier of the form <M>x (at most two decimal places); negative, fractional, signed-plus, boolean, and nonnumeric values are rejected.".to_string()
        }
        (_, InvalidInt::Empty) => format!(
            "%queue({field}=...) requires a non-negative integer."
        ),
        (_, InvalidInt::Invalid) => format!(
            "%queue({field}=...) requires a non-negative decimal integer; negative, fractional, signed-plus, boolean, and nonnumeric values are rejected."
        ),
    };
    queue_error(code, &message, span)
}

fn overflow_priority(span: Option<[usize; 2]>) -> QueueParseErrorWire {
    queue_error(
        "queue-overflow-priority",
        &format!(
            "%queue(priority=...) exceeds the i32 maximum of {}.",
            i32::MAX
        ),
        span,
    )
}

fn empty_queue_error(span: Option<[usize; 2]>) -> QueueParseErrorWire {
    queue_error(
        "empty-queue",
        "%queue requires capacity, priority, and/or weight; bare %q is not a previous-agent wait. Use %q:N, %queue(capacity=N), %queue(priority=P), and/or %queue(weight=W).",
        span,
    )
}

fn extra_positional_error(span: Option<[usize; 2]>) -> QueueParseErrorWire {
    queue_error(
        "extra-queue-positional",
        "%queue accepts at most one positional argument, which is capacity.",
        span,
    )
}

fn duplicate_field_error(
    field: &str,
    span: Option<[usize; 2]>,
) -> QueueParseErrorWire {
    queue_error(
        "duplicate-queue-field",
        &format!(
            "Duplicate %queue {field} assignment is not allowed, even when the values match."
        ),
        span,
    )
}

fn obsolete_runners_error(span: Option<[usize; 2]>) -> QueueParseErrorWire {
    queue_error(
        "obsolete-queue-runners",
        "%queue(runners=...) has been renamed. Use %queue(capacity=N) or %q:N; capacity is a weighted-load threshold, not a count of running agents.",
        span,
    )
}

fn invalid_capacity_zero_error(
    span: Option<[usize; 2]>,
) -> QueueParseErrorWire {
    queue_error(
        "invalid-queue-capacity-zero",
        "%queue capacity is this launch's capacity budget and must be at least 1; use %q:1 to run alone.",
        span,
    )
}

fn invalid_capacity_multiplier_error(
    span: Option<[usize; 2]>,
) -> QueueParseErrorWire {
    queue_error(
        "invalid-queue-capacity-multiplier",
        "%queue(capacity=...) multiplier <M>x requires a positive number with at most two decimal places, followed by lowercase x. Signs, exponents, inf/nan, uppercase X, a bare x, and trailing dots are rejected.",
        span,
    )
}

fn weight_error(span: Option<[usize; 2]>) -> QueueParseErrorWire {
    queue_error(
        "invalid-queue-weight",
        "%queue(weight=...) requires a non-negative finite base-10 float; negative, NaN, infinity, overflow, underflow-to-zero, boolean, empty, and nonnumeric values are rejected. Use weight=0 for a launch that adds no runner load.",
        span,
    )
}

fn canonical_queue_key(literal: &str) -> String {
    match literal {
        "p" => "priority".to_string(),
        "w" => "weight".to_string(),
        other => other.to_string(),
    }
}

fn queue_form(source: &str, has_plus_suffix: bool) -> QueueForm {
    if has_plus_suffix {
        return QueueForm::Plus;
    }
    match suffix_after_directive_name(source).chars().next() {
        Some('(') => QueueForm::Parenthesized {
            closed: parenthesized_source_is_closed(source),
        },
        Some(':') => QueueForm::Colon,
        Some('+') => QueueForm::Plus,
        _ => QueueForm::Bare,
    }
}

fn suffix_after_directive_name(source: &str) -> &str {
    let trimmed = source.trim_start();
    let body = trimmed.strip_prefix('%').unwrap_or(trimmed);
    let end = body
        .find(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
        .unwrap_or(body.len());
    &body[end..]
}

fn parenthesized_source_is_closed(source: &str) -> bool {
    let Some(open) = source.find('(') else {
        return true;
    };
    let bytes = source.as_bytes();
    let mut depth = 1usize;
    let mut index = open + 1;
    let mut quote: Option<u8> = None;
    while index < bytes.len() {
        let byte = bytes[index];
        if quote.is_none() && bytes.get(index..index + 2) == Some(b"[[") {
            if let Some(close) = source[index + 2..].find("]]") {
                index += 2 + close + 2;
                continue;
            }
            return false;
        }
        match quote {
            Some(q) if byte == q => quote = None,
            Some(_) => {}
            None if byte == b'"' || byte == b'`' => quote = Some(byte),
            None if byte == b'(' => depth += 1,
            None if byte == b')' => {
                depth -= 1;
                if depth == 0 {
                    return true;
                }
            }
            None => {}
        }
        index += 1;
    }
    false
}

fn queue_error(
    code: &str,
    message: &str,
    source_span: Option<[usize; 2]>,
) -> QueueParseErrorWire {
    QueueParseErrorWire {
        code: code.to_string(),
        message: message.to_string(),
        source_span,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn occ(source: &str, args: Vec<QueueArgWire>) -> QueueOccurrenceWire {
        QueueOccurrenceWire {
            source: source.to_string(),
            source_span: [0, source.len()],
            args,
            has_plus_suffix: false,
        }
    }

    fn positional(value: &str) -> QueueArgWire {
        QueueArgWire {
            name: None,
            value: value.to_string(),
        }
    }

    fn named(name: &str, value: &str) -> QueueArgWire {
        QueueArgWire {
            name: Some(name.to_string()),
            value: value.to_string(),
        }
    }

    fn collect_ok(occurrences: &[QueueOccurrenceWire]) -> QueueFieldsWire {
        let result = collect_queue_fields(occurrences);
        assert!(result.errors.is_empty(), "{:?}", result.errors);
        result.fields.expect("fields")
    }

    fn collect_err(
        occurrences: &[QueueOccurrenceWire],
    ) -> Vec<QueueParseErrorWire> {
        let result = collect_queue_fields(occurrences);
        assert!(result.fields.is_none(), "{result:?}");
        result.errors
    }

    fn collect_err_with_flags(
        occurrences: &[QueueOccurrenceWire],
        flags: &[String],
    ) -> Vec<QueueParseErrorWire> {
        let result = collect_queue_fields_with_flags(occurrences, flags);
        assert!(result.fields.is_none(), "{result:?}");
        result.errors
    }

    fn capacity_budget_flags() -> Vec<String> {
        vec![QUEUE_CAPACITY_BUDGET_FLAG.to_string()]
    }

    #[test]
    fn legacy_flag_helpers_always_enable_queue() {
        assert_eq!(queue_directive_flag_key(), "queue_directive");
        assert!(queue_directive_enabled(&[]));
        assert!(queue_directive_enabled(&[
            "typed_launch_units".to_string(),
            "queue_directive".to_string(),
        ]));
    }

    #[test]
    fn equivalent_spellings_compose_disjoint_fields() {
        let fields = collect_ok(&[
            occ("%q:5", vec![positional("5")]),
            occ("%queue(p=20)", vec![named("p", "20")]),
        ]);
        assert_eq!(
            fields,
            QueueFieldsWire {
                queue_capacity: Some(5),
                priority: Some(20),
                ..QueueFieldsWire::default()
            }
        );
        assert_eq!(
            format_queue_directive(&fields).as_deref(),
            Some("%queue(capacity=5, priority=20)")
        );
    }

    #[test]
    fn parenthesized_positional_and_aliases_round_trip() {
        for source_args in [
            occ("%q(5)", vec![positional("5")]),
            occ("%queue:5", vec![positional("5")]),
            occ("%queue(capacity=5)", vec![named("capacity", "5")]),
        ] {
            let fields = collect_ok(&[source_args]);
            assert_eq!(fields.queue_capacity, Some(5));
            assert_eq!(fields.priority, None);
            assert_eq!(
                format_queue_directive(&fields).as_deref(),
                Some("%queue(capacity=5)")
            );
        }
        let priority = collect_ok(&[occ(
            "%q(priority=20)",
            vec![named("priority", "20")],
        )]);
        assert_eq!(
            format_queue_directive(&priority).as_deref(),
            Some("%queue(priority=20)")
        );
        let both = collect_ok(&[occ(
            "%q(5, p=20, w=0.25)",
            vec![positional("5"), named("p", "20"), named("w", "0.25")],
        )]);
        assert_eq!(both.queue_capacity, Some(5));
        assert_eq!(both.priority, Some(20));
        assert_eq!(both.weight, Some(0.25));
        assert_eq!(
            format_queue_directive(&both).as_deref(),
            Some("%queue(capacity=5, priority=20, weight=0.25)")
        );
    }

    #[test]
    fn explicit_zero_is_distinct_from_omitted() {
        let zero = collect_ok(&[occ("%q:0", vec![positional("0")])]);
        assert_eq!(zero.queue_capacity, Some(0));
        assert_eq!(zero.priority, None);
        let omitted = QueueFieldsWire::default();
        assert_ne!(zero, omitted);
        assert_eq!(format_queue_directive(&omitted), None);
        let default_priority = collect_ok(&[occ(
            "%queue(priority=10)",
            vec![named("priority", "10")],
        )]);
        assert_eq!(default_priority.priority, Some(10));
    }

    #[test]
    fn capacity_budget_flag_rejects_zero_capacity() {
        let flags = capacity_budget_flags();
        for occurrence in [
            occ("%q:0", vec![positional("0")]),
            occ("%queue(capacity=0)", vec![named("capacity", "0")]),
        ] {
            let errors = collect_err_with_flags(&[occurrence], &flags);
            assert_eq!(errors[0].code, "invalid-queue-capacity-zero");
            assert!(errors[0].message.contains("%q:1"), "{errors:?}");
        }
        assert_eq!(parse_queue_capacity("0"), Ok(0));
        let error = parse_queue_capacity_with_flags("0", &flags).unwrap_err();
        assert_eq!(error.code, "invalid-queue-capacity-zero");
    }

    #[test]
    fn capacity_budget_flag_rejects_weight_over_capacity() {
        let occurrence = occ(
            "%q(capacity=1, w=2)",
            vec![named("capacity", "1"), named("w", "2")],
        );
        let old = collect_ok(std::slice::from_ref(&occurrence));
        assert_eq!(old.queue_capacity, Some(1));
        assert_eq!(old.weight, Some(2.0));

        let flags = capacity_budget_flags();
        let errors = collect_err_with_flags(&[occurrence], &flags);
        assert_eq!(errors[0].code, "queue-weight-exceeds-capacity");

        let composed = collect_err_with_flags(
            &[
                occ("%q:1", vec![positional("1")]),
                occ("%q(w=2)", vec![named("w", "2")]),
            ],
            &flags,
        );
        assert_eq!(composed[0].code, "queue-weight-exceeds-capacity");
    }

    #[test]
    fn queue_fields_read_legacy_capacity_and_write_queue_capacity() {
        let fields: QueueFieldsWire =
            serde_json::from_value(serde_json::json!({"capacity": 3})).unwrap();
        assert_eq!(fields.queue_capacity, Some(3));
        assert_eq!(
            serde_json::to_value(fields).unwrap(),
            serde_json::json!({"queue_capacity": 3})
        );
    }

    #[test]
    fn rejects_duplicates_even_when_values_match() {
        for occurrences in [
            vec![occ(
                "%q(5, capacity=5)",
                vec![positional("5"), named("capacity", "5")],
            )],
            vec![occ(
                "%q(p=20, priority=20)",
                vec![named("p", "20"), named("priority", "20")],
            )],
            vec![
                occ("%q:5", vec![positional("5")]),
                occ("%queue(capacity=5)", vec![named("capacity", "5")]),
            ],
            vec![occ(
                "%q(p=20, p=20)",
                vec![named("p", "20"), named("p", "20")],
            )],
            vec![occ(
                "%q(w=1, weight=1)",
                vec![named("w", "1"), named("weight", "1")],
            )],
            vec![
                occ("%q(w=2)", vec![named("w", "2")]),
                occ("%queue(weight=2)", vec![named("weight", "2")]),
            ],
        ] {
            let errors = collect_err(&occurrences);
            assert!(
                errors
                    .iter()
                    .any(|error| error.code == "duplicate-queue-field"),
                "{errors:?}"
            );
        }
    }

    #[test]
    fn rejects_empty_and_plus_forms_without_previous_agent_meaning() {
        for (source, args, plus) in [
            ("%q", Vec::new(), false),
            ("%q()", Vec::new(), false),
            ("%q:", Vec::new(), false),
            ("%q+", vec![positional("true")], true),
        ] {
            let mut occurrence = occ(source, args);
            occurrence.has_plus_suffix = plus;
            let errors = collect_err(&[occurrence]);
            assert!(
                errors.iter().any(|error| {
                    error.code == "empty-queue"
                        || error.code == "queue-plus-unsupported"
                }),
                "{source}: {errors:?}"
            );
            assert!(
                errors.iter().all(|error| {
                    !error.message.to_ascii_lowercase().contains("previous")
                        || error.code == "empty-queue"
                }),
                "{errors:?}"
            );
        }
    }

    #[test]
    fn rejects_malformed_parentheses_extra_positionals_and_wait_keys() {
        let unclosed = occ("%q(5", vec![positional("5")]);
        assert_eq!(collect_err(&[unclosed])[0].code, "malformed-queue");

        let extra = occ("%q(5, 6)", vec![positional("5"), positional("6")]);
        assert_eq!(collect_err(&[extra])[0].code, "extra-queue-positional");

        let wait_key =
            occ("%queue(agent=builder)", vec![named("agent", "builder")]);
        assert_eq!(collect_err(&[wait_key])[0].code, "queue-wait-keyword");

        let unknown = occ("%q(foo=1)", vec![named("foo", "1")]);
        assert_eq!(collect_err(&[unknown])[0].code, "unknown-queue-keyword");

        let colon_kw = occ("%q:capacity=5", vec![named("capacity", "5")]);
        assert_eq!(collect_err(&[colon_kw])[0].code, "queue-colon-keyword");
    }

    #[test]
    fn rejects_obsolete_runners_keyword_with_capacity_migration() {
        for occurrence in [
            occ("%queue(runners=5)", vec![named("runners", "5")]),
            occ(
                "%q(3, runners=3)",
                vec![positional("3"), named("runners", "3")],
            ),
            occ(
                "%q(capacity=3, runners=3)",
                vec![named("capacity", "3"), named("runners", "3")],
            ),
        ] {
            let errors = collect_err(&[occurrence]);
            assert_eq!(errors[0].code, "obsolete-queue-runners");
            assert!(errors[0].message.contains("capacity="), "{errors:?}");
            assert!(
                !errors[0].message.to_ascii_lowercase().contains("alias"),
                "{errors:?}"
            );
        }
    }

    #[test]
    fn rejects_invalid_and_overflow_integers() {
        for value in ["-1", "+1", "1.5", "true", "false", "many", ""] {
            let capacity =
                occ("%q(capacity=x)", vec![named("capacity", value)]);
            let mut capacity = capacity;
            capacity.args[0].value = value.to_string();
            assert_eq!(
                collect_err(&[capacity])[0].code,
                "invalid-queue-capacity",
                "{value}"
            );
            let priority = occ("%q(p=x)", vec![named("p", value)]);
            let mut priority = priority;
            priority.args[0].value = value.to_string();
            assert_eq!(
                collect_err(&[priority])[0].code,
                "invalid-queue-priority",
                "{value}"
            );
        }

        let capacity_max = collect_ok(&[occ(
            "%queue(capacity=4294967295)",
            vec![named("capacity", &u32::MAX.to_string())],
        )]);
        assert_eq!(capacity_max.queue_capacity, Some(u32::MAX));
        let capacity_overflow = occ(
            "%queue(capacity=4294967296)",
            vec![named("capacity", "4294967296")],
        );
        assert_eq!(
            collect_err(&[capacity_overflow])[0].code,
            "queue-overflow-capacity"
        );
        assert_eq!(parse_queue_capacity(&u32::MAX.to_string()), Ok(u32::MAX));
        assert_eq!(parse_queue_capacity("0"), Ok(0));
        assert_eq!(
            parse_queue_capacity("4294967296").unwrap_err().code,
            "queue-overflow-capacity"
        );
        assert_eq!(
            parse_queue_capacity("true").unwrap_err().code,
            "invalid-queue-capacity"
        );

        let priority_max = collect_ok(&[occ(
            "%queue(priority=2147483647)",
            vec![named("priority", &i32::MAX.to_string())],
        )]);
        assert_eq!(priority_max.priority, Some(i32::MAX));
        let priority_overflow = occ(
            "%queue(priority=2147483648)",
            vec![named("priority", "2147483648")],
        );
        assert_eq!(
            collect_err(&[priority_overflow])[0].code,
            "queue-overflow-priority"
        );
    }

    #[test]
    fn parses_weight_float_spellings_and_formats_canonically() {
        for (value, expected) in [
            ("2", 2.0),
            ("2.0", 2.0),
            (".25", 0.25),
            ("2.", 2.0),
            ("+2.5e-1", 0.25),
            ("5e-324", f64::from_bits(1)),
        ] {
            let fields =
                collect_ok(&[occ("%q(w=value)", vec![named("w", value)])]);
            assert_eq!(fields.queue_capacity, None);
            assert_eq!(fields.priority, None);
            assert_eq!(fields.weight, Some(expected), "{value}");
            assert_eq!(
                format_queue_directive(&fields),
                Some(format!(
                    "%queue(weight={})",
                    format_queue_weight(expected)
                )),
                "{value}"
            );
        }
        let explicit_default =
            collect_ok(&[occ("%q(weight=1.0)", vec![named("weight", "1.0")])]);
        assert_eq!(explicit_default.weight, Some(DEFAULT_QUEUE_WEIGHT));
        assert_eq!(
            format_queue_directive(&explicit_default).as_deref(),
            Some("%queue(weight=1)")
        );
    }

    #[test]
    fn accepts_exact_zero_weight_literals_with_or_without_capacity_budget() {
        for flags in [Vec::new(), capacity_budget_flags()] {
            for value in ["0", "0.0", ".0", "0.", "+0", "0e5"] {
                let result = collect_queue_fields_with_flags(
                    &[occ("%q(w=value)", vec![named("w", value)])],
                    &flags,
                );
                assert!(result.errors.is_empty(), "{value}: {result:?}");
                let fields = result.fields.expect("zero weight fields");
                let weight = fields.weight.expect("explicit zero weight");
                assert_eq!(weight, 0.0, "{value}");
                assert!(!weight.is_sign_negative(), "{value}");
                assert_eq!(
                    format_queue_directive(&fields).as_deref(),
                    Some("%queue(weight=0)"),
                    "{value}"
                );
            }
        }

        let fields = collect_queue_fields_with_flags(
            &[occ("%q(1, w=0)", vec![positional("1"), named("w", "0")])],
            &capacity_budget_flags(),
        )
        .fields
        .expect("zero weight is within a positive capacity budget");
        assert_eq!(fields.queue_capacity, Some(1));
        assert_eq!(fields.weight, Some(0.0));
        assert_eq!(format_queue_weight(0.0), "0");
        assert_eq!(format_queue_weight(-0.0), "0");
        assert_eq!(
            format_queue_directive(&QueueFieldsWire {
                weight: Some(-0.0),
                ..QueueFieldsWire::default()
            })
            .as_deref(),
            Some("%queue(weight=0)")
        );
    }

    #[test]
    fn rejects_invalid_weight_values() {
        for value in [
            "", "-0", "-0.0", "-0e0", "-1", "true", "false", "many", "NaN",
            "inf", "Infinity", "1e309", "1e-324", "0e-400x",
        ] {
            let weight = occ("%q(w=x)", vec![named("w", value)]);
            assert_eq!(
                collect_err(&[weight])[0].code,
                "invalid-queue-weight",
                "{value}"
            );
        }
    }

    #[test]
    fn preserves_source_spans_on_errors() {
        let occurrence = QueueOccurrenceWire {
            source: "%q(5, capacity=5)".to_string(),
            source_span: [3, 20],
            args: vec![positional("5"), named("capacity", "5")],
            has_plus_suffix: false,
        };
        let errors = collect_err(&[occurrence]);
        assert_eq!(errors[0].source_span, Some([3, 20]));
    }

    #[test]
    fn capacity_aliases_prefer_canonical_and_keep_zero_distinct() {
        let mut dual = serde_json::Map::new();
        dual.insert("queue_capacity".into(), serde_json::json!(100));
        dual.insert("wait_runners".into(), serde_json::json!(0));
        dual.insert("queue_capacity_explicit".into(), serde_json::json!(true));
        dual.insert("wait_runners_explicit".into(), serde_json::json!(true));
        assert_eq!(queue_capacity_from_map(&dual), (Some(100), true));
        merge_queue_capacity_aliases(&mut dual);
        assert_eq!(dual.get("queue_capacity"), Some(&serde_json::json!(100)));
        assert!(dual.get("wait_runners").is_none());
        assert_eq!(
            dual.get("queue_capacity_explicit"),
            Some(&serde_json::json!(true))
        );
        assert!(dual.get("wait_runners_explicit").is_none());

        let mut legacy = serde_json::Map::new();
        legacy.insert("wait_runners".into(), serde_json::json!(0));
        legacy.insert("wait_runners_explicit".into(), serde_json::json!(true));
        assert_eq!(queue_capacity_from_map(&legacy), (Some(0), true));

        let omitted = serde_json::Map::new();
        assert_eq!(queue_capacity_from_map(&omitted), (None, false));

        let mut explicit_false = serde_json::Map::new();
        explicit_false
            .insert("queue_capacity_explicit".into(), serde_json::json!(false));
        assert_eq!(queue_capacity_from_map(&explicit_false), (None, false));
    }

    #[test]
    fn persisted_zero_keeps_exact_effective_weight_drain_budget() {
        let translated =
            normalize_persisted_queue_capacity(Some(0), true, 0.25, 8.0, true);
        assert_eq!(translated.admission_limit, 0.25);
        assert_eq!(translated.authored_capacity, Some(0));
        assert!(translated.legacy_zero);
        assert_eq!(translated.reauthor_capacity, None);

        let positive =
            normalize_persisted_queue_capacity(Some(100), true, 1.0, 1.0, true);
        assert_eq!(positive.admission_limit, 100.0);
        assert_eq!(positive.reauthor_capacity, Some(100));
        assert!(!positive.legacy_zero);

        let off =
            normalize_persisted_queue_capacity(Some(0), true, 0.25, 8.0, false);
        assert_eq!(off.admission_limit, 8.0);
        assert_eq!(off.reauthor_capacity, Some(0));
        assert!(!off.legacy_zero);

        let zero_weight =
            normalize_persisted_queue_capacity(Some(0), true, 0.0, 8.0, true);
        assert_eq!(zero_weight.admission_limit, 0.0);
        assert!(zero_weight.legacy_zero);
    }

    fn collect_ok_with_flags(
        occurrences: &[QueueOccurrenceWire],
        flags: &[String],
    ) -> QueueFieldsWire {
        let result = collect_queue_fields_with_flags(occurrences, flags);
        assert!(result.errors.is_empty(), "{:?}", result.errors);
        result.fields.expect("fields")
    }

    #[test]
    fn accepts_capacity_multiplier_spellings_in_both_flag_states() {
        for flags in [Vec::new(), capacity_budget_flags()] {
            for (source, args, expected, formatted) in [
                ("%q:1x", vec![positional("1x")], 1.0, "1x"),
                ("%q:2x", vec![positional("2x")], 2.0, "2x"),
                ("%q:0.5x", vec![positional("0.5x")], 0.5, "0.5x"),
                ("%q:.5x", vec![positional(".5x")], 0.5, "0.5x"),
                ("%q:1.25x", vec![positional("1.25x")], 1.25, "1.25x"),
                ("%q:1.50x", vec![positional("1.50x")], 1.5, "1.5x"),
                ("%q(1.5x)", vec![positional("1.5x")], 1.5, "1.5x"),
                (
                    "%q(capacity=1.5x)",
                    vec![named("capacity", "1.5x")],
                    1.5,
                    "1.5x",
                ),
                (
                    "%q(capacity=.5x)",
                    vec![named("capacity", ".5x")],
                    0.5,
                    "0.5x",
                ),
            ] {
                let fields =
                    collect_ok_with_flags(&[occ(source, args)], &flags);
                assert_eq!(fields.queue_capacity, None, "{source}");
                assert_eq!(
                    fields.queue_capacity_multiplier,
                    Some(expected),
                    "{source}"
                );
                assert_eq!(
                    format_queue_directive(&fields),
                    Some(format!("%queue(capacity={formatted})")),
                    "{source}"
                );
            }
        }
    }

    #[test]
    fn rejects_invalid_capacity_multipliers_in_both_flag_states() {
        for flags in [Vec::new(), capacity_budget_flags()] {
            for (value, code, needle) in [
                ("1.125x", "invalid-queue-capacity-multiplier", "two decimal"),
                (
                    "0x",
                    "invalid-queue-capacity-multiplier",
                    "greater than zero",
                ),
                (
                    "0.00x",
                    "invalid-queue-capacity-multiplier",
                    "greater than zero",
                ),
                ("-1x", "invalid-queue-capacity-multiplier", "<M>x"),
                ("+1x", "invalid-queue-capacity-multiplier", "<M>x"),
                ("1e2x", "invalid-queue-capacity-multiplier", "<M>x"),
                ("infx", "invalid-queue-capacity-multiplier", "<M>x"),
                ("nanx", "invalid-queue-capacity-multiplier", "<M>x"),
                ("1.5X", "invalid-queue-capacity-multiplier", "<M>x"),
                ("x", "invalid-queue-capacity-multiplier", "<M>x"),
                ("1.x", "invalid-queue-capacity-multiplier", "<M>x"),
                ("42949673x", "queue-overflow-capacity-multiplier", "u32"),
            ] {
                let errors = collect_err_with_flags(
                    &[occ("%q(capacity=x)", vec![named("capacity", value)])],
                    &flags,
                );
                assert_eq!(errors[0].code, code, "{value}");
                assert!(
                    errors[0].message.contains(needle),
                    "{value}: {}",
                    errors[0].message
                );
            }
        }
        let missing_x = collect_err(&[occ(
            "%q(capacity=1.5)",
            vec![named("capacity", "1.5")],
        )]);
        assert_eq!(missing_x[0].code, "invalid-queue-capacity");
        assert!(missing_x[0].message.contains("<M>x"), "{missing_x:?}");
        assert_eq!(
            parse_queue_capacity("1.5x").unwrap_err().code,
            "invalid-queue-capacity"
        );
    }

    #[test]
    fn rejects_duplicate_integer_and_multiplier_capacity() {
        for occurrences in [
            vec![occ(
                "%q(2, capacity=1.5x)",
                vec![positional("2"), named("capacity", "1.5x")],
            )],
            vec![occ(
                "%q(1.5x, capacity=2)",
                vec![positional("1.5x"), named("capacity", "2")],
            )],
            vec![
                occ("%q:2", vec![positional("2")]),
                occ("%q(1.5x)", vec![positional("1.5x")]),
            ],
            vec![
                occ("%q:1.5x", vec![positional("1.5x")]),
                occ("%queue(capacity=2)", vec![named("capacity", "2")]),
            ],
        ] {
            let errors = collect_err(&occurrences);
            assert!(
                errors
                    .iter()
                    .any(|error| error.code == "duplicate-queue-field"),
                "{errors:?}"
            );
        }
    }

    #[test]
    fn multiplier_skips_parse_time_weight_capacity_check() {
        let occurrence = occ(
            "%q(1.5x, w=100)",
            vec![positional("1.5x"), named("w", "100")],
        );
        for flags in [Vec::new(), capacity_budget_flags()] {
            let fields = collect_ok_with_flags(
                std::slice::from_ref(&occurrence),
                &flags,
            );
            assert_eq!(fields.queue_capacity_multiplier, Some(1.5));
            assert_eq!(fields.weight, Some(100.0));
        }
    }

    #[test]
    fn capacity_multiplier_helpers_validate_format_and_resolve() {
        assert!(queue_capacity_multiplier_is_valid(1.5));
        assert!(queue_capacity_multiplier_is_valid(0.5));
        assert!(queue_capacity_multiplier_is_valid(1.25));
        assert!(!queue_capacity_multiplier_is_valid(0.0));
        assert!(!queue_capacity_multiplier_is_valid(-1.0));
        assert!(!queue_capacity_multiplier_is_valid(1.125));
        assert!(!queue_capacity_multiplier_is_valid(f64::NAN));
        assert!(!queue_capacity_multiplier_is_valid(f64::INFINITY));
        assert_eq!(
            format_queue_capacity_multiplier(1.5).as_deref(),
            Some("1.5x")
        );
        assert_eq!(
            format_queue_capacity_multiplier(2.0).as_deref(),
            Some("2x")
        );
        assert_eq!(
            format_queue_capacity_multiplier(0.25).as_deref(),
            Some("0.25x")
        );
        assert_eq!(
            format_queue_capacity_multiplier(1.05).as_deref(),
            Some("1.05x")
        );
        assert_eq!(resolve_queue_capacity_multiplier(1.5, 5.0), Some(7.5));
        assert_eq!(resolve_queue_capacity_multiplier(0.5, 5.0), Some(2.5));
        assert_eq!(resolve_queue_capacity_multiplier(1.15, 3.0), Some(3.45));
        assert_eq!(resolve_queue_capacity_multiplier(1.125, 5.0), None);
        assert_eq!(
            parse_queue_capacity_value("1.5x").unwrap(),
            QueueCapacityValueWire {
                queue_capacity: None,
                queue_capacity_multiplier: Some(1.5),
            }
        );
        assert_eq!(
            parse_queue_capacity_value("3").unwrap(),
            QueueCapacityValueWire {
                queue_capacity: Some(3),
                queue_capacity_multiplier: None,
            }
        );
        let flags = capacity_budget_flags();
        assert_eq!(
            parse_queue_capacity_value_with_flags("0", &flags)
                .unwrap_err()
                .code,
            "invalid-queue-capacity-zero"
        );
    }

    #[test]
    fn queue_fields_read_capacity_multiplier_alias() {
        let fields: QueueFieldsWire = serde_json::from_value(
            serde_json::json!({"capacity_multiplier": 1.5}),
        )
        .unwrap();
        assert_eq!(fields.queue_capacity_multiplier, Some(1.5));
        assert_eq!(
            serde_json::to_value(fields).unwrap(),
            serde_json::json!({"queue_capacity_multiplier": 1.5})
        );
    }

    #[test]
    fn persisted_multiplier_resolves_at_admission_and_reauthors() {
        let on = normalize_persisted_queue_capacity_with_multiplier(
            None,
            false,
            Some(1.5),
            0.25,
            5.0,
            true,
        );
        assert_eq!(on.admission_limit, 7.5);
        assert_eq!(on.authored_multiplier, Some(1.5));
        assert_eq!(on.reauthor_multiplier, Some(1.5));
        assert!(on.authored_explicit);

        let off = normalize_persisted_queue_capacity_with_multiplier(
            None,
            false,
            Some(1.5),
            0.25,
            5.0,
            false,
        );
        assert_eq!(off.admission_limit, 5.0);
        assert_eq!(off.reauthor_multiplier, Some(1.5));

        let integer_wins = normalize_persisted_queue_capacity_with_multiplier(
            Some(4),
            true,
            Some(1.5),
            0.25,
            5.0,
            true,
        );
        assert_eq!(integer_wins.admission_limit, 4.0);
        assert_eq!(integer_wins.authored_capacity, Some(4));
        assert_eq!(integer_wins.authored_multiplier, None);

        let data = serde_json::json!({"queue_capacity_multiplier": 0.5});
        assert_eq!(
            queue_capacity_multiplier_from_map(data.as_object().unwrap()),
            Some(0.5)
        );
        let invalid = serde_json::json!({"queue_capacity_multiplier": 1.125});
        assert_eq!(
            queue_capacity_multiplier_from_map(invalid.as_object().unwrap()),
            None
        );
    }
}
