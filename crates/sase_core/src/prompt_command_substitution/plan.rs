//! Eligibility and balanced command-span planning.

use serde_json::Value as JsonValue;

use super::wire::{
    PromptCommandReplacementWire, PromptCommandSubstitutionError,
    PromptCommandSubstitutionPlanWire, PromptCommandSubstitutionRequestWire,
    PromptCommandTextKindWire, PromptLaunchAuthorshipWire,
    PromptUtf8ByteRangeWire,
    PROMPT_COMMAND_SUBSTITUTION_CODE_INVALID_PROTECTED_RANGE,
    PROMPT_COMMAND_SUBSTITUTION_CODE_INVALID_REQUEST,
    PROMPT_COMMAND_SUBSTITUTION_CODE_UNSUPPORTED_SCHEMA,
    PROMPT_COMMAND_SUBSTITUTION_OFFSET_UNIT,
    PROMPT_COMMAND_SUBSTITUTION_WIRE_SCHEMA_VERSION,
};

/// Return the command-substitution wire schema version.
pub fn prompt_command_substitution_wire_schema_version() -> u32 {
    PROMPT_COMMAND_SUBSTITUTION_WIRE_SCHEMA_VERSION
}

/// Plan command-substitution replacements from a JSON request dict.
pub fn plan_prompt_command_substitution_from_json(
    value: &JsonValue,
) -> Result<PromptCommandSubstitutionPlanWire, PromptCommandSubstitutionError> {
    let request: PromptCommandSubstitutionRequestWire =
        serde_json::from_value(value.clone()).map_err(|error| {
            PromptCommandSubstitutionError::new(
                PROMPT_COMMAND_SUBSTITUTION_CODE_INVALID_REQUEST,
                format!("invalid prompt command-substitution request: {error}"),
            )
        })?;
    plan_prompt_command_substitution(&request)
}

/// Decide eligibility and return ordered original-text replacement actions.
///
/// Spans are planned against the request text only. Replacement stdout is
/// never scanned. Shell execution stays on the host.
pub fn plan_prompt_command_substitution(
    request: &PromptCommandSubstitutionRequestWire,
) -> Result<PromptCommandSubstitutionPlanWire, PromptCommandSubstitutionError> {
    if request.schema_version != PROMPT_COMMAND_SUBSTITUTION_WIRE_SCHEMA_VERSION
    {
        return Err(PromptCommandSubstitutionError::new(
            PROMPT_COMMAND_SUBSTITUTION_CODE_UNSUPPORTED_SCHEMA,
            format!(
                "unsupported prompt command-substitution schema_version {}; expected {PROMPT_COMMAND_SUBSTITUTION_WIRE_SCHEMA_VERSION}",
                request.schema_version
            ),
        ));
    }

    let protected =
        validated_protected_ranges(&request.text, &request.protected_ranges)?;
    let eligible = is_eligible(request.kind, request.launch_authorship);
    let replacements = if eligible {
        plan_command_spans(&request.text, &protected)
    } else {
        Vec::new()
    };

    Ok(PromptCommandSubstitutionPlanWire {
        schema_version: PROMPT_COMMAND_SUBSTITUTION_WIRE_SCHEMA_VERSION,
        eligible,
        offset_unit: PROMPT_COMMAND_SUBSTITUTION_OFFSET_UNIT.to_string(),
        replacements,
    })
}

fn is_eligible(
    kind: PromptCommandTextKindWire,
    launch_authorship: PromptLaunchAuthorshipWire,
) -> bool {
    matches!(
        (kind, launch_authorship),
        (
            PromptCommandTextKindWire::HumanSource
                | PromptCommandTextKindWire::AdmittedDefinition,
            PromptLaunchAuthorshipWire::Human
        )
    )
}

fn validated_protected_ranges(
    text: &str,
    ranges: &[PromptUtf8ByteRangeWire],
) -> Result<Vec<(usize, usize)>, PromptCommandSubstitutionError> {
    let len = text.len();
    let mut normalized = Vec::with_capacity(ranges.len());
    for (index, range) in ranges.iter().enumerate() {
        let start = utf8_offset(range.start, len, index, "start")?;
        let end = utf8_offset(range.end, len, index, "end")?;
        if start > end {
            return Err(invalid_protected_range(format!(
                "protected_ranges[{index}] start {start} is greater than end {end}"
            )));
        }
        if !text.is_char_boundary(start) || !text.is_char_boundary(end) {
            return Err(invalid_protected_range(format!(
                "protected_ranges[{index}] [{start}, {end}) is not aligned to UTF-8 character boundaries"
            )));
        }
        if end > start {
            normalized.push((start, end));
        }
    }
    normalized.sort_unstable();
    Ok(merge_ranges(normalized))
}

fn utf8_offset(
    value: u64,
    len: usize,
    index: usize,
    field: &str,
) -> Result<usize, PromptCommandSubstitutionError> {
    let offset = usize::try_from(value).map_err(|_| {
        invalid_protected_range(format!(
            "protected_ranges[{index}].{field} {value} does not fit this host"
        ))
    })?;
    if offset > len {
        return Err(invalid_protected_range(format!(
            "protected_ranges[{index}].{field} {offset} is outside text of {len} UTF-8 bytes"
        )));
    }
    Ok(offset)
}

fn invalid_protected_range(
    message: impl Into<String>,
) -> PromptCommandSubstitutionError {
    PromptCommandSubstitutionError::new(
        PROMPT_COMMAND_SUBSTITUTION_CODE_INVALID_PROTECTED_RANGE,
        message,
    )
}

fn merge_ranges(ranges: Vec<(usize, usize)>) -> Vec<(usize, usize)> {
    let mut merged: Vec<(usize, usize)> = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        if let Some(last) = merged.last_mut() {
            if start <= last.1 {
                last.1 = last.1.max(end);
                continue;
            }
        }
        merged.push((start, end));
    }
    merged
}

fn plan_command_spans(
    text: &str,
    protected: &[(usize, usize)],
) -> Vec<PromptCommandReplacementWire> {
    let bytes = text.as_bytes();
    let mut replacements = Vec::new();
    let mut index = 0;
    while index + 1 < bytes.len() {
        if bytes[index] != b'$' || bytes[index + 1] != b'(' {
            index += 1;
            continue;
        }
        if index > 0 && bytes[index - 1] == b'\\' {
            index += 1;
            continue;
        }
        let command_start = index + 2;
        let Some(close_index) = find_matching_paren(bytes, command_start)
        else {
            index += 1;
            continue;
        };
        let start = index;
        let end = close_index + 1;
        if !range_overlaps(start, end, protected) {
            replacements.push(PromptCommandReplacementWire {
                start: start as u64,
                end: end as u64,
                command: text[command_start..close_index].to_string(),
            });
        }
        index = end;
    }
    replacements
}

fn find_matching_paren(bytes: &[u8], start: usize) -> Option<usize> {
    let mut depth = 1_usize;
    for (offset, byte) in bytes[start..].iter().copied().enumerate() {
        match byte {
            b'(' => depth = depth.saturating_add(1),
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(start + offset);
                }
            }
            _ => {}
        }
    }
    None
}

fn range_overlaps(
    start: usize,
    end: usize,
    protected: &[(usize, usize)],
) -> bool {
    protected.iter().any(|(protected_start, protected_end)| {
        start < *protected_end && *protected_start < end
    })
}
