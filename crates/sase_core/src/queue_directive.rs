//! Shared `%queue` / `%q` argument contract.
//!
//! This module owns queue-field normalization, validation, collection across
//! occurrences, and canonical formatting. Callers reuse existing directive
//! occurrence scanning. This module never reads global configuration.

use serde::{Deserialize, Serialize};

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<f64>,
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
    }
    if let Some(priority) = fields.priority {
        parts.push(format!("priority={priority}"));
    }
    if let Some(weight) = fields.weight {
        if queue_weight_is_valid(weight) {
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

pub fn format_queue_weight(value: f64) -> String {
    value.to_string()
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
    parse_capacity(
        raw,
        None,
        queue_capacity_budget_enabled(enabled_feature_flags),
    )
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
    Ok(QueueFieldsWire {
        queue_capacity: Some(parse_capacity(
            &positionals[0].value,
            span,
            capacity_budget,
        )?),
        priority: None,
        weight: None,
    })
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
    if fields.queue_capacity.is_none()
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
    if fields.queue_capacity.is_some() {
        return Err(duplicate_field_error("capacity", span));
    }
    fields.queue_capacity = Some(parse_capacity(raw, span, capacity_budget)?);
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
    if let Some(capacity) = part.queue_capacity {
        if fields.queue_capacity.is_some() {
            return Err(duplicate_field_error("capacity", Some(span)));
        }
        fields.queue_capacity = Some(capacity);
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

fn validate_queue_budget_fields(
    fields: &QueueFieldsWire,
    capacity_budget: bool,
) -> Result<(), QueueParseErrorWire> {
    if !capacity_budget {
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
    if raw.is_empty() {
        return Err(weight_error(span));
    }
    let Ok(parsed) = raw.parse::<f64>() else {
        return Err(weight_error(span));
    };
    if !queue_weight_is_valid(parsed) {
        return Err(weight_error(span));
    }
    Ok(parsed)
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
    let message = match kind {
        InvalidInt::Empty => format!(
            "%queue({field}=...) requires a non-negative integer."
        ),
        InvalidInt::Invalid => format!(
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

fn weight_error(span: Option<[usize; 2]>) -> QueueParseErrorWire {
    queue_error(
        "invalid-queue-weight",
        "%queue(weight=...) requires a positive finite base-10 float; zero, negative, NaN, infinity, overflow, underflow-to-zero, boolean, empty, and nonnumeric values are rejected.",
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
    fn rejects_invalid_weight_values() {
        for value in [
            "", "0", "-0", "-0.0", "-1", "true", "false", "many", "NaN", "inf",
            "Infinity", "1e309", "1e-324",
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
}
