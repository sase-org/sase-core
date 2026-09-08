//! Shared `%queue` / `%q` argument contract.
//!
//! This module owns queue-field normalization, validation, collection across
//! occurrences, and canonical formatting. Callers reuse existing directive
//! occurrence scanning and pass the resolved `queue_directive` flag into
//! launch and editor entry points; this module never reads global
//! configuration.

use serde::{Deserialize, Serialize};

/// Feature-flag key that gates the `%queue` / `%q` split from `%wait`.
pub const QUEUE_DIRECTIVE_FLAG: &str = "queue_directive";

/// Feature-flag key that gates `%queue` / `%q`.
pub fn queue_directive_flag_key() -> &'static str {
    QUEUE_DIRECTIVE_FLAG
}

/// Return whether `enabled_feature_flags` contains [`QUEUE_DIRECTIVE_FLAG`].
pub fn queue_directive_enabled(enabled_feature_flags: &[String]) -> bool {
    enabled_feature_flags
        .iter()
        .any(|flag| flag == QUEUE_DIRECTIVE_FLAG)
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

/// Canonical queue fields. `None` means omitted, distinct from explicit zero
/// or explicit default priority 10.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct QueueFieldsWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runners: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    let mut fields = QueueFieldsWire::default();
    let mut errors = Vec::new();
    for occurrence in occurrences {
        match parse_queue_occurrence(occurrence) {
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

/// Canonical generated form: `%queue(runners=N, priority=P)`, omitting
/// absent fields. Returns `None` when both fields are omitted.
pub fn format_queue_directive(fields: &QueueFieldsWire) -> Option<String> {
    match (fields.runners, fields.priority) {
        (None, None) => None,
        (Some(runners), None) => Some(format!("%queue(runners={runners})")),
        (None, Some(priority)) => Some(format!("%queue(priority={priority})")),
        (Some(runners), Some(priority)) => {
            Some(format!("%queue(runners={runners}, priority={priority})"))
        }
    }
}

/// Message used when `%queue` / `%q` is written while the flag is off.
pub fn queue_directive_disabled_message() -> String {
    format!(
        "%queue requires the {QUEUE_DIRECTIVE_FLAG} feature flag. Enable it with `sase flag enable {QUEUE_DIRECTIVE_FLAG}`."
    )
}

fn parse_queue_occurrence(
    occurrence: &QueueOccurrenceWire,
) -> Result<QueueFieldsWire, QueueParseErrorWire> {
    let span = Some(occurrence.source_span);
    match queue_form(&occurrence.source, occurrence.has_plus_suffix) {
        QueueForm::Plus => Err(queue_error(
            "queue-plus-unsupported",
            "%queue does not support '+'; use %q:N or %queue(...).",
            span,
        )),
        QueueForm::Bare => Err(empty_queue_error(span)),
        QueueForm::Colon => parse_colon_occurrence(occurrence),
        QueueForm::Parenthesized { closed: false } => Err(queue_error(
            "malformed-queue",
            "Malformed %queue(...) directive: missing closing ')'.",
            span,
        )),
        QueueForm::Parenthesized { closed: true } => {
            parse_parenthesized_occurrence(occurrence)
        }
    }
}

fn parse_colon_occurrence(
    occurrence: &QueueOccurrenceWire,
) -> Result<QueueFieldsWire, QueueParseErrorWire> {
    let span = Some(occurrence.source_span);
    if occurrence.args.iter().any(|arg| arg.name.is_some()) {
        return Err(queue_error(
            "queue-colon-keyword",
            "%queue colon form supplies positional runners only; use parentheses for keywords.",
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
        runners: Some(parse_runners(&positionals[0].value, span)?),
        priority: None,
    })
}

fn parse_parenthesized_occurrence(
    occurrence: &QueueOccurrenceWire,
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
                assign_runners(&mut fields, &arg.value, span)?;
            }
            Some(literal) => {
                if literal.is_empty() {
                    return Err(queue_error(
                        "unknown-queue-keyword",
                        "Unsupported keyword on %queue: empty name. Use runners=, priority=, or p=.",
                        span,
                    ));
                }
                let key = literal.to_ascii_lowercase();
                if seen_literals.iter().any(|seen: &String| seen == &key) {
                    return Err(duplicate_field_error(
                        &canonical_queue_key(&key),
                        span,
                    ));
                }
                seen_literals.push(key.clone());
                match canonical_queue_key(&key).as_str() {
                    "runners" => {
                        assign_runners(&mut fields, &arg.value, span)?;
                    }
                    "priority" => {
                        assign_priority(&mut fields, &arg.value, span)?;
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
                                "Unsupported keyword on %queue: {literal}=. Use runners=, priority=, or p=."
                            ),
                            span,
                        ));
                    }
                }
            }
        }
    }
    if fields.runners.is_none() && fields.priority.is_none() {
        return Err(empty_queue_error(span));
    }
    Ok(fields)
}

fn assign_runners(
    fields: &mut QueueFieldsWire,
    raw: &str,
    span: Option<[usize; 2]>,
) -> Result<(), QueueParseErrorWire> {
    if fields.runners.is_some() {
        return Err(duplicate_field_error("runners", span));
    }
    fields.runners = Some(parse_runners(raw, span)?);
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

fn merge_queue_part(
    fields: &mut QueueFieldsWire,
    part: QueueFieldsWire,
    span: [usize; 2],
) -> Result<(), QueueParseErrorWire> {
    if let Some(runners) = part.runners {
        if fields.runners.is_some() {
            return Err(duplicate_field_error("runners", Some(span)));
        }
        fields.runners = Some(runners);
    }
    if let Some(priority) = part.priority {
        if fields.priority.is_some() {
            return Err(duplicate_field_error("priority", Some(span)));
        }
        fields.priority = Some(priority);
    }
    Ok(())
}

fn parse_runners(
    raw: &str,
    span: Option<[usize; 2]>,
) -> Result<u32, QueueParseErrorWire> {
    let digits = parse_non_negative_decimal(raw)
        .map_err(|kind| integer_error("runners", kind, span))?;
    digits.parse::<u32>().map_err(|_| {
        queue_error(
            "queue-overflow-runners",
            &format!(
                "%queue(runners=...) exceeds the u32 maximum of {}.",
                u32::MAX
            ),
            span,
        )
    })
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
        _ => "invalid-queue-runners",
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
        "%queue requires runners and/or priority; bare %q is not a previous-agent wait. Use %q:N, %queue(runners=N), and/or %queue(priority=P).",
        span,
    )
}

fn extra_positional_error(span: Option<[usize; 2]>) -> QueueParseErrorWire {
    queue_error(
        "extra-queue-positional",
        "%queue accepts at most one positional argument, which is runners.",
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

fn canonical_queue_key(literal: &str) -> String {
    match literal {
        "p" => "priority".to_string(),
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

    #[test]
    fn flag_helpers_are_explicit() {
        assert_eq!(queue_directive_flag_key(), "queue_directive");
        assert!(!queue_directive_enabled(&[]));
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
                runners: Some(5),
                priority: Some(20),
            }
        );
        assert_eq!(
            format_queue_directive(&fields).as_deref(),
            Some("%queue(runners=5, priority=20)")
        );
    }

    #[test]
    fn parenthesized_positional_and_aliases_round_trip() {
        for source_args in [
            occ("%q(5)", vec![positional("5")]),
            occ("%queue:5", vec![positional("5")]),
            occ("%queue(runners=5)", vec![named("runners", "5")]),
        ] {
            let fields = collect_ok(&[source_args]);
            assert_eq!(fields.runners, Some(5));
            assert_eq!(fields.priority, None);
            assert_eq!(
                format_queue_directive(&fields).as_deref(),
                Some("%queue(runners=5)")
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
            "%q(5, p=20)",
            vec![positional("5"), named("p", "20")],
        )]);
        assert_eq!(both.runners, Some(5));
        assert_eq!(both.priority, Some(20));
    }

    #[test]
    fn explicit_zero_is_distinct_from_omitted() {
        let zero = collect_ok(&[occ("%q:0", vec![positional("0")])]);
        assert_eq!(zero.runners, Some(0));
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
    fn rejects_duplicates_even_when_values_match() {
        for occurrences in [
            vec![occ(
                "%q(5, runners=5)",
                vec![positional("5"), named("runners", "5")],
            )],
            vec![occ(
                "%q(p=20, priority=20)",
                vec![named("p", "20"), named("priority", "20")],
            )],
            vec![
                occ("%q:5", vec![positional("5")]),
                occ("%queue(runners=5)", vec![named("runners", "5")]),
            ],
            vec![occ(
                "%q(p=20, p=20)",
                vec![named("p", "20"), named("p", "20")],
            )],
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

        let colon_kw = occ("%q:runners=5", vec![named("runners", "5")]);
        assert_eq!(collect_err(&[colon_kw])[0].code, "queue-colon-keyword");
    }

    #[test]
    fn rejects_invalid_and_overflow_integers() {
        for value in ["-1", "+1", "1.5", "true", "false", "many", ""] {
            let runners = occ("%q(runners=x)", vec![named("runners", value)]);
            let mut runners = runners;
            runners.args[0].value = value.to_string();
            assert_eq!(
                collect_err(&[runners])[0].code,
                "invalid-queue-runners",
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

        let runners_max = collect_ok(&[occ(
            "%queue(runners=4294967295)",
            vec![named("runners", &u32::MAX.to_string())],
        )]);
        assert_eq!(runners_max.runners, Some(u32::MAX));
        let runners_overflow = occ(
            "%queue(runners=4294967296)",
            vec![named("runners", "4294967296")],
        );
        assert_eq!(
            collect_err(&[runners_overflow])[0].code,
            "queue-overflow-runners"
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
    fn preserves_source_spans_on_errors() {
        let occurrence = QueueOccurrenceWire {
            source: "%q(5, runners=5)".to_string(),
            source_span: [3, 19],
            args: vec![positional("5"), named("runners", "5")],
            has_plus_suffix: false,
        };
        let errors = collect_err(&[occurrence]);
        assert_eq!(errors[0].source_span, Some([3, 19]));
    }
}
