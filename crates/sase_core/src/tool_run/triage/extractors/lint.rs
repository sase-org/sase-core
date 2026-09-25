//! Symvision, mypy, ruff, ruff_format, prettier, keep_sorted, toobig.

use regex::Regex;
use std::sync::OnceLock;

use super::raw::RawItem;

fn symvision_entry_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^\s{2,}(\S+) in (\S+)$").unwrap())
}

fn epic_symbol_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^Error: --epic-symbol '([^'(]+)\(([^)]+)\)': (.*)$")
            .unwrap()
    })
}

fn digits_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\d+").unwrap())
}

fn non_alnum_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"[^a-z0-9]+").unwrap())
}

fn slug_heading(heading: &str) -> String {
    let lower = heading.to_lowercase();
    if lower.contains("unused") && lower.contains("public") {
        return "unused_public".to_string();
    }
    if lower.contains("private") && lower.contains("import") {
        return "private_imported".to_string();
    }
    if lower.contains("private") && lower.contains("unused") {
        return "private_unused".to_string();
    }
    let with_n = digits_re().replace_all(&lower, "N");
    let slug = non_alnum_re().replace_all(&with_n, "_");
    let slug = slug.trim_matches('_').to_string();
    if slug.is_empty() {
        "heading".to_string()
    } else {
        slug
    }
}

pub fn extract_symvision(lines: &[String]) -> Vec<RawItem> {
    let mut items = Vec::new();
    let mut current_heading: Option<String> = None;
    let mut heading_has_marker = false;
    for line in lines {
        if let Some(caps) = epic_symbol_re().captures(line) {
            let outer = caps.get(1).map(|hit| hit.as_str()).unwrap_or("");
            let inner = caps.get(2).map(|hit| hit.as_str()).unwrap_or("");
            let message = caps.get(3).map(|hit| hit.as_str()).unwrap_or("");
            let category = if message.contains("is closed") {
                "epic_symbol_closed"
            } else if message.contains("already properly used") {
                "epic_symbol_unneeded"
            } else {
                "epic_symbol"
            };
            let symbol = format!("{inner}({outer})");
            let key = format!("{category}|{symbol}|Justfile");
            items.push(RawItem::new(key, line, vec!["Justfile".to_string()]));
            continue;
        }
        // Non-indented heading ending in ':' containing functions/classes.
        if !line.starts_with(' ')
            && !line.starts_with('\t')
            && line.ends_with(':')
            && line.contains("functions/classes")
        {
            current_heading = Some(slug_heading(line.trim_end_matches(':')));
            heading_has_marker = true;
            continue;
        }
        if heading_has_marker {
            if let Some(caps) = symvision_entry_re().captures(line) {
                let symbol = caps.get(1).map(|hit| hit.as_str()).unwrap_or("");
                let path = caps.get(2).map(|hit| hit.as_str()).unwrap_or("");
                let category = current_heading
                    .clone()
                    .unwrap_or_else(|| "heading".to_string());
                let key = format!("{category}|{symbol}|{path}");
                items.push(RawItem::new(key, line, vec![path.to_string()]));
                continue;
            }
            // A blank line or a new non-indented line ends the section.
            if line.trim().is_empty()
                || (!line.starts_with(' ') && !line.starts_with('\t'))
            {
                current_heading = None;
                heading_has_marker = false;
            }
        }
    }
    items
}

fn mypy_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"^(\S+\.pyi?):\d+(?::\d+)?: error: (.*?)\s+\[([a-z0-9-]+)\]$",
        )
        .unwrap()
    })
}

fn dquote_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r#""[^"]*""#).unwrap())
}

fn squote_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"'[^']*'").unwrap())
}

fn scrub_quoted(message: &str) -> String {
    let step = dquote_re().replace_all(message, "\"_\"");
    squote_re().replace_all(&step, "'_'").into_owned()
}

pub fn extract_mypy(lines: &[String]) -> Vec<RawItem> {
    let mut items = Vec::new();
    for line in lines {
        if let Some(caps) = mypy_re().captures(line) {
            let path = caps.get(1).map(|hit| hit.as_str()).unwrap_or("");
            let message = caps.get(2).map(|hit| hit.as_str()).unwrap_or("");
            let code = caps.get(3).map(|hit| hit.as_str()).unwrap_or("");
            let key = format!("{path}|{code}|{}", scrub_quoted(message));
            items.push(RawItem::new(key, line, vec![path.to_string()]));
        }
    }
    items
}

fn ruff_code_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^([A-Z]{1,4}\d{3,4}) .+").unwrap())
}

fn ruff_location_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^\s*--> (\S+?):\d+:\d+$").unwrap())
}

fn ruff_concise_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^(\S+\.pyi?):\d+:\d+: ([A-Z]{1,4}\d{3,4}) ").unwrap()
    })
}

pub fn extract_ruff(lines: &[String]) -> Vec<RawItem> {
    let mut items = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if let Some(caps) = ruff_concise_re().captures(line) {
            let path = caps.get(1).map(|hit| hit.as_str()).unwrap_or("");
            let rule = caps.get(2).map(|hit| hit.as_str()).unwrap_or("");
            items.push(RawItem::new(
                format!("{rule}|{path}"),
                line,
                vec![path.to_string()],
            ));
            continue;
        }
        if let Some(caps) = ruff_code_re().captures(line) {
            let rule = caps.get(1).map(|hit| hit.as_str()).unwrap_or("");
            for peek in lines.iter().skip(index + 1).take(3) {
                if let Some(loc) = ruff_location_re().captures(peek) {
                    let path = loc.get(1).map(|hit| hit.as_str()).unwrap_or("");
                    items.push(RawItem::new(
                        format!("{rule}|{path}"),
                        line,
                        vec![path.to_string()],
                    ));
                    break;
                }
            }
        }
    }
    items
}

pub fn extract_ruff_format(lines: &[String]) -> Vec<RawItem> {
    static REFORMAT: OnceLock<Regex> = OnceLock::new();
    let reformat = REFORMAT
        .get_or_init(|| Regex::new(r"^Would reformat: (\S+)$").unwrap());
    let mut items = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if let Some(caps) = reformat.captures(line) {
            let path = caps.get(1).map(|hit| hit.as_str()).unwrap_or("");
            items.push(RawItem::new(
                format!("format|{path}"),
                line,
                vec![path.to_string()],
            ));
            continue;
        }
        if line == "unformatted: File would be reformatted" {
            for peek in lines.iter().skip(index + 1).take(3) {
                if let Some(loc) = ruff_location_re().captures(peek) {
                    let path = loc.get(1).map(|hit| hit.as_str()).unwrap_or("");
                    items.push(RawItem::new(
                        format!("format|{path}"),
                        line,
                        vec![path.to_string()],
                    ));
                    break;
                }
            }
        }
    }
    items
}

pub fn extract_prettier(lines: &[String]) -> Vec<RawItem> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"^\[warn\] (\S+)$").unwrap());
    let mut items = Vec::new();
    for line in lines {
        if let Some(caps) = re.captures(line) {
            let path = caps.get(1).map(|hit| hit.as_str()).unwrap_or("");
            if path.contains("Code") || line.contains("Code style issues") {
                continue;
            }
            items.push(RawItem::new(
                format!("prettier|{path}"),
                line,
                vec![path.to_string()],
            ));
        }
    }
    items
}

pub fn extract_keep_sorted(lines: &[String]) -> Vec<RawItem> {
    let mut items = Vec::new();
    let mut index = 0;
    while index < lines.len() {
        if lines[index].trim() != "[" {
            index += 1;
            continue;
        }
        let mut end = None;
        for (offset, line) in lines.iter().enumerate().skip(index) {
            if line.trim() == "]" {
                end = Some(offset);
                break;
            }
        }
        let Some(stop) = end else {
            break;
        };
        let block = lines[index..=stop].join("\n");
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&block) {
            if let Some(array) = value.as_array() {
                for entry in array {
                    let path = entry
                        .get("path")
                        .and_then(|item| item.as_str())
                        .unwrap_or("");
                    let message = entry
                        .get("message")
                        .and_then(|item| item.as_str())
                        .unwrap_or("");
                    if path.is_empty() || message.is_empty() {
                        continue;
                    }
                    let normalized_message =
                        digits_re().replace_all(message, "N").into_owned();
                    items.push(RawItem::new(
                        format!("{normalized_message}|{path}"),
                        &format!("{path}: {message}"),
                        vec![path.to_string()],
                    ));
                }
            }
        }
        index = stop + 1;
    }
    items
}

pub fn extract_toobig(lines: &[String]) -> Vec<RawItem> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(r"^ERROR: VIOLATION: (\S+) has \d+ lines").unwrap()
    });
    let mut items = Vec::new();
    for line in lines {
        if let Some(caps) = re.captures(line) {
            let path = caps.get(1).map(|hit| hit.as_str()).unwrap_or("");
            items.push(RawItem::new(
                path.to_string(),
                line,
                vec![path.to_string()],
            ));
        }
    }
    items
}
