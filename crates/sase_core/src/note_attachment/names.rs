//! Sanitized and uniquified attachment names.

use serde::{Deserialize, Serialize};

/// Maximum attachment-name length in characters.
pub const ATTACHMENT_NAME_MAX_LEN: usize = 96;

/// One existing `{name, sha256}` pair for uniquification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachmentNameDigestWire {
    pub name: String,
    pub sha256: String,
}

/// True when `name` already satisfies the sanitized-name predicate.
pub fn is_valid_attachment_name(name: &str) -> bool {
    if name.is_empty() || name.len() > ATTACHMENT_NAME_MAX_LEN {
        return false;
    }
    if !name.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
    }) {
        return false;
    }
    !(name.starts_with('.') || name.starts_with('-'))
}

/// Map a candidate file name into the attachment-name alphabet.
pub fn sanitize_attachment_name(candidate: &str) -> String {
    let mut mapped = String::with_capacity(candidate.len());
    for character in candidate.chars() {
        if character.is_ascii_alphanumeric()
            || character == '.'
            || character == '_'
            || character == '-'
        {
            mapped.push(character);
        } else {
            mapped.push('_');
        }
    }
    let mut collapsed = String::with_capacity(mapped.len());
    let mut previous_underscore = false;
    for character in mapped.chars() {
        if character == '_' {
            if previous_underscore {
                continue;
            }
            previous_underscore = true;
            collapsed.push('_');
        } else {
            previous_underscore = false;
            collapsed.push(character);
        }
    }
    let mut result = collapsed;
    while result.contains("_.") {
        result = result.replace("_.", ".");
    }
    while result.starts_with('.') || result.starts_with('-') {
        result.remove(0);
    }
    let (mut stem, extension) = split_extension(&result);
    if stem.is_empty() || stem.chars().all(|character| character == '_') {
        stem = "attachment".to_string();
    }
    if stem.is_empty() && extension.is_none() {
        return "attachment".to_string();
    }
    let mut rebuilt = match &extension {
        Some(extension) => format!("{stem}.{extension}"),
        None => stem.clone(),
    };
    if rebuilt.is_empty() {
        return "attachment".to_string();
    }
    if rebuilt.len() > ATTACHMENT_NAME_MAX_LEN {
        match extension {
            Some(extension) => {
                if extension.len() + 1 > ATTACHMENT_NAME_MAX_LEN {
                    rebuilt.truncate(ATTACHMENT_NAME_MAX_LEN);
                } else {
                    let max_stem =
                        ATTACHMENT_NAME_MAX_LEN - extension.len() - 1;
                    let mut truncated = stem.clone();
                    if truncated.len() > max_stem {
                        truncated.truncate(max_stem);
                    }
                    while truncated.ends_with('.') || truncated.ends_with('_') {
                        truncated.pop();
                    }
                    if truncated.is_empty() {
                        truncated = "attachment".to_string();
                    }
                    rebuilt = format!("{truncated}.{extension}");
                }
            }
            None => {
                rebuilt.truncate(ATTACHMENT_NAME_MAX_LEN);
            }
        }
    }
    if rebuilt.is_empty() {
        return "attachment".to_string();
    }
    rebuilt
}

fn split_extension(value: &str) -> (String, Option<String>) {
    let Some(dot) = value.rfind('.') else {
        return (value.to_string(), None);
    };
    let suffix = &value[dot + 1..];
    if suffix.is_empty()
        || suffix.len() >= value.len()
        || !suffix.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        return (value.to_string(), None);
    }
    (value[..dot].to_string(), Some(suffix.to_string()))
}

/// Uniquify a candidate against existing `{name, sha256}` pairs.
///
/// Digests compare by ASCII case-fold. Same name and same digest reuses the
/// name; otherwise the first free `stem-2.ext` style name wins.
pub fn unique_attachment_name(
    candidate: &str,
    sha256: &str,
    existing: &[AttachmentNameDigestWire],
) -> String {
    let base = sanitize_attachment_name(candidate);
    let wanted = sha256.to_ascii_lowercase();
    let mut has_name = false;
    for entry in existing {
        if entry.name == base {
            has_name = true;
            if entry.sha256.to_ascii_lowercase() == wanted {
                return base;
            }
        }
    }
    if !has_name {
        return base;
    }
    let (stem, extension) = split_extension(&base);
    for index in 2..1_000_000u64 {
        let suffix = format!("-{index}");
        let candidate_name = match &extension {
            Some(extension) => {
                let max_stem = ATTACHMENT_NAME_MAX_LEN
                    .saturating_sub(suffix.len() + extension.len() + 1);
                if max_stem == 0
                    && suffix.len() + extension.len() + 1
                        > ATTACHMENT_NAME_MAX_LEN
                {
                    continue;
                }
                let mut truncated = stem.clone();
                if truncated.len() > max_stem {
                    truncated.truncate(max_stem);
                }
                while truncated.ends_with('.') || truncated.ends_with('_') {
                    truncated.pop();
                }
                if truncated.is_empty() {
                    continue;
                }
                format!("{truncated}{suffix}.{extension}")
            }
            None => {
                let max_stem =
                    ATTACHMENT_NAME_MAX_LEN.saturating_sub(suffix.len());
                if max_stem == 0 {
                    continue;
                }
                let mut truncated = stem.clone();
                if truncated.len() > max_stem {
                    truncated.truncate(max_stem);
                }
                while truncated.ends_with('.') || truncated.ends_with('_') {
                    truncated.pop();
                }
                if truncated.is_empty() {
                    continue;
                }
                format!("{truncated}{suffix}")
            }
        };
        if candidate_name.len() > ATTACHMENT_NAME_MAX_LEN {
            continue;
        }
        let mut found = false;
        let mut matched = false;
        for entry in existing {
            if entry.name == candidate_name {
                found = true;
                if entry.sha256.to_ascii_lowercase() == wanted {
                    matched = true;
                }
                break;
            }
        }
        if matched || !found {
            return candidate_name;
        }
    }
    base
}
