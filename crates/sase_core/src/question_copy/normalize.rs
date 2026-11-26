//! Strict creation normalization and tolerant display parsing.

use serde_json::Value;

use super::wire::{
    QuestionCopyDisplayWire, QuestionCopyError, QuestionCopyItemWire,
    QUESTION_COPY_MAX_ENTRIES, QUESTION_COPY_MAX_LABEL_CHARS,
    QUESTION_COPY_MAX_TEXT_BYTES,
};

/// Normalize a question's optional `copy` value for creation.
///
/// The result is canonical: entries are objects with `text`, labels are
/// trimmed and omitted when absent, and trailing newlines are removed from
/// values. The caller removes an empty result from the question object.
pub fn normalize_question_copy(
    value: &Value,
) -> Result<Vec<QuestionCopyItemWire>, QuestionCopyError> {
    let entries = value.as_array().ok_or_else(|| {
        QuestionCopyError::new(None, "copy", "must be an array")
    })?;
    if entries.len() > QUESTION_COPY_MAX_ENTRIES {
        return Err(QuestionCopyError::new(
            None,
            "copy",
            format!("must contain at most {QUESTION_COPY_MAX_ENTRIES} entries"),
        ));
    }

    let mut normalized = Vec::with_capacity(entries.len());
    for (index, entry) in entries.iter().enumerate() {
        normalized.push(normalize_entry(entry, index)?);
    }

    for index in 0..normalized.len() {
        if let Some(previous) = normalized[..index]
            .iter()
            .position(|item| item.text == normalized[index].text)
        {
            return Err(QuestionCopyError::new(
                Some(index),
                "text",
                format!("duplicates copy[{previous}].text"),
            ));
        }
    }
    Ok(normalized)
}

/// Parse `copy` for display without allowing malformed stored data to break
/// a question surface. Every invalid array entry contributes one unavailable
/// item; a non-array value contributes one unavailable item.
pub fn display_question_copy(value: Option<&Value>) -> QuestionCopyDisplayWire {
    let mut display = QuestionCopyDisplayWire {
        items: Vec::new(),
        unavailable: 0,
    };
    let Some(value) = value else {
        return display;
    };
    match value {
        Value::Null => display,
        Value::Array(entries) => {
            for (index, entry) in entries.iter().enumerate() {
                match normalize_entry(entry, index) {
                    Ok(item) => display.items.push(item),
                    Err(_) => display.unavailable += 1,
                }
            }
            display
        }
        _ => {
            display.unavailable = 1;
            display
        }
    }
}

fn normalize_entry(
    value: &Value,
    index: usize,
) -> Result<QuestionCopyItemWire, QuestionCopyError> {
    let (text, label) = match value {
        Value::String(text) => (text.as_str(), None),
        Value::Object(fields) => {
            for key in fields.keys() {
                if key != "text" && key != "label" {
                    return Err(QuestionCopyError::new(
                        Some(index),
                        key,
                        "unknown field",
                    ));
                }
            }
            let text = fields.get("text").and_then(Value::as_str).ok_or_else(
                || {
                    QuestionCopyError::new(
                        Some(index),
                        "text",
                        "must be a string",
                    )
                },
            )?;
            let label = match fields.get("label") {
                None | Some(Value::Null) => None,
                Some(Value::String(label)) => Some(label.as_str()),
                Some(_) => {
                    return Err(QuestionCopyError::new(
                        Some(index),
                        "label",
                        "must be a string or null",
                    ));
                }
            };
            (text, label)
        }
        _ => {
            return Err(QuestionCopyError::new(
                Some(index),
                "entry",
                "must be a string or an object with a text field",
            ));
        }
    };

    let text = text.trim_end_matches('\n');
    if text.is_empty()
        || !text.chars().any(|character| !character.is_whitespace())
    {
        return Err(QuestionCopyError::new(
            Some(index),
            "text",
            "must contain a non-whitespace character",
        ));
    }
    if text.len() > QUESTION_COPY_MAX_TEXT_BYTES {
        return Err(QuestionCopyError::new(
            Some(index),
            "text",
            format!(
                "is {} UTF-8 bytes; maximum is {QUESTION_COPY_MAX_TEXT_BYTES}",
                text.len()
            ),
        ));
    }
    for character in text.chars() {
        if is_bidi_control(character) {
            return Err(QuestionCopyError::new(
                Some(index),
                "text",
                format!("contains bidi control U+{:04X}", character as u32),
            ));
        }
        if character.is_control() && character != '\n' && character != '\t' {
            return Err(QuestionCopyError::new(
                Some(index),
                "text",
                format!("contains control character U+{:04X}; only newline and tab are allowed", character as u32),
            ));
        }
    }

    let label = label
        .map(|label| {
            for character in label.chars() {
                if is_bidi_control(character) {
                    return Err(QuestionCopyError::new(
                        Some(index),
                        "label",
                        format!("contains bidi control U+{:04X}", character as u32),
                    ));
                }
                if character.is_control() {
                    return Err(QuestionCopyError::new(
                        Some(index),
                        "label",
                        format!("contains control character U+{:04X}", character as u32),
                    ));
                }
            }
            let label = label.trim();
            if label.is_empty() {
                return Err(QuestionCopyError::new(
                    Some(index),
                    "label",
                    "must not be empty after trimming",
                ));
            }
            let length = label.chars().count();
            if length > QUESTION_COPY_MAX_LABEL_CHARS {
                return Err(QuestionCopyError::new(
                    Some(index),
                    "label",
                    format!(
                        "is {length} characters; maximum is {QUESTION_COPY_MAX_LABEL_CHARS}"
                    ),
                ));
            }
            Ok(label.to_string())
        })
        .transpose()?;

    Ok(QuestionCopyItemWire {
        text: text.to_string(),
        label,
    })
}

fn is_bidi_control(character: char) -> bool {
    matches!(
        character as u32,
        0x202A..=0x202E | 0x2066..=0x2069
    )
}
