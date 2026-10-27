//! Spool line parsing and privacy-closed validation.

use std::collections::BTreeSet;
use std::sync::LazyLock;

use regex::Regex;
use serde_json::{Map, Value};

use super::wire::{
    ns_of, require_member, surface_for_ns, ACTORS, FEATURE_ID_PATTERN,
    OPTION_DEST_PATTERN, OUTCOMES, VERSION_PATTERN, VIAS,
};

/// Writer and fold hard cap on one spool line.
pub const SPOOL_LINE_MAX_BYTES: usize = 512;
/// Day-file size cap; writers emit drop lines up to this plus slack.
pub const SPOOL_FILE_MAX_BYTES: u64 = 32 * 1024 * 1024;
/// Extra bytes past the cap in which drop lines are still accepted.
pub const SPOOL_DROP_SLACK_BYTES: u64 = 1024 * 1024;

static FEATURE_ID_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(FEATURE_ID_PATTERN).expect("feature id regex"));
static OPTION_DEST_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(OPTION_DEST_PATTERN).expect("option dest regex")
});
static VERSION_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(VERSION_PATTERN).expect("version regex"));

const HIT_KEYS: &[&str] = &["s", "d", "f", "a", "v", "o", "n", "ver"];
const HIT_OPTIONAL: &[&str] = &["ms", "opt"];
const PROBE_KEYS: &[&str] = &["s", "d", "probe", "ver"];
const DROP_KEYS: &[&str] = &["s", "d", "drop", "ver"];

/// A validated v1 spool record, or a torn line with optional surface hint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpoolLine {
    Hit(HitRecord),
    Probe {
        day: String,
        surface: String,
        ver: String,
    },
    Drop {
        day: String,
        count: u64,
        ver: String,
    },
    Torn {
        surface: Option<String>,
    },
}

/// One validated feature-hit spool record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HitRecord {
    pub day: String,
    pub feature_id: String,
    pub actor: String,
    pub via: String,
    pub outcome: String,
    pub n: i64,
    pub dur_ms: Option<i64>,
    pub opt: Vec<String>,
    pub ver: String,
}

/// Parse one spool line. Unknown keys and invalid enums are torn.
pub fn parse_spool_line(line: &str) -> SpoolLine {
    if line.len() > SPOOL_LINE_MAX_BYTES {
        return SpoolLine::Torn { surface: None };
    }
    let value: Value = match serde_json::from_str(line) {
        Ok(value) => value,
        Err(_) => return SpoolLine::Torn { surface: None },
    };
    let object = match value.as_object() {
        Some(object) => object,
        None => return SpoolLine::Torn { surface: None },
    };
    match classify_line(object) {
        Ok(line) => line,
        Err(surface) => SpoolLine::Torn { surface },
    }
}

fn classify_line(
    object: &Map<String, Value>,
) -> Result<SpoolLine, Option<String>> {
    let surface_hint = object
        .get("f")
        .and_then(Value::as_str)
        .and_then(|id| ns_of(id).and_then(surface_for_ns))
        .map(str::to_string)
        .or_else(|| {
            object
                .get("probe")
                .and_then(Value::as_str)
                .filter(|surface| super::wire::SURFACES.contains(surface))
                .map(str::to_string)
        });
    let keys: BTreeSet<&str> = object.keys().map(String::as_str).collect();
    let schema = object.get("s").and_then(Value::as_u64);
    if schema != Some(1) {
        return Err(surface_hint);
    }
    if object.contains_key("f") {
        require_exact_keys(&keys, HIT_KEYS, HIT_OPTIONAL)?;
        return parse_hit(object).map(SpoolLine::Hit);
    }
    if object.contains_key("probe") {
        require_exact_keys(&keys, PROBE_KEYS, &[])?;
        let day = required_day(object)?;
        let surface = required_string(object, "probe")?;
        if !super::wire::SURFACES.contains(&surface.as_str()) {
            return Err(surface_hint);
        }
        let ver = required_version(object)?;
        return Ok(SpoolLine::Probe { day, surface, ver });
    }
    if object.contains_key("drop") {
        require_exact_keys(&keys, DROP_KEYS, &[])?;
        let day = required_day(object)?;
        let count = required_u64(object, "drop")?;
        if count == 0 {
            return Err(surface_hint);
        }
        let ver = required_version(object)?;
        return Ok(SpoolLine::Drop { day, count, ver });
    }
    Err(surface_hint)
}

fn require_exact_keys(
    keys: &BTreeSet<&str>,
    required: &[&str],
    optional: &[&str],
) -> Result<(), Option<String>> {
    for key in required {
        if !keys.contains(key) {
            return Err(None);
        }
    }
    for key in keys {
        if !required.contains(key) && !optional.contains(key) {
            return Err(None);
        }
    }
    Ok(())
}

fn parse_hit(object: &Map<String, Value>) -> Result<HitRecord, Option<String>> {
    let day = required_day(object)?;
    let feature_id = required_string(object, "f")?;
    if !FEATURE_ID_RE.is_match(&feature_id) {
        return Err(ns_of(&feature_id)
            .and_then(surface_for_ns)
            .map(str::to_string));
    }
    let actor = required_string(object, "a")?;
    require_member(&actor, ACTORS, "actor").map_err(|_| None)?;
    let via = required_string(object, "v")?;
    require_member(&via, VIAS, "via").map_err(|_| None)?;
    let outcome = required_string(object, "o")?;
    require_member(&outcome, OUTCOMES, "outcome").map_err(|_| None)?;
    let n = required_i64(object, "n")?;
    if n < 1 {
        return Err(None);
    }
    let dur_ms = match object.get("ms") {
        Some(value) => {
            let ms = integer_i64(value)?;
            if ms < 0 {
                return Err(None);
            }
            Some(ms)
        }
        None => None,
    };
    let opt = match object.get("opt") {
        Some(Value::Array(items)) => {
            if items.len() > 16 {
                return Err(None);
            }
            let mut dests = Vec::with_capacity(items.len());
            for item in items {
                let dest = item.as_str().ok_or(None)?;
                if !OPTION_DEST_RE.is_match(dest) {
                    return Err(None);
                }
                dests.push(dest.to_string());
            }
            dests
        }
        Some(_) => return Err(None),
        None => Vec::new(),
    };
    let ver = required_version(object)?;
    Ok(HitRecord {
        day,
        feature_id,
        actor,
        via,
        outcome,
        n,
        dur_ms,
        opt,
        ver,
    })
}

fn required_day(object: &Map<String, Value>) -> Result<String, Option<String>> {
    let day = required_string(object, "d")?;
    if super::catalog::valid_day(&day) {
        Ok(day)
    } else {
        Err(None)
    }
}

fn required_version(
    object: &Map<String, Value>,
) -> Result<String, Option<String>> {
    let ver = required_string(object, "ver")?;
    if VERSION_RE.is_match(&ver) {
        Ok(ver)
    } else {
        Err(None)
    }
}

fn required_string(
    object: &Map<String, Value>,
    key: &str,
) -> Result<String, Option<String>> {
    object
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or(None)
}

fn required_u64(
    object: &Map<String, Value>,
    key: &str,
) -> Result<u64, Option<String>> {
    integer_u64(object.get(key).ok_or(None)?)
}

fn required_i64(
    object: &Map<String, Value>,
    key: &str,
) -> Result<i64, Option<String>> {
    integer_i64(object.get(key).ok_or(None)?)
}

fn integer_u64(value: &Value) -> Result<u64, Option<String>> {
    match value {
        Value::Number(number) if number.is_u64() => number.as_u64().ok_or(None),
        Value::Number(number) if number.is_i64() => {
            let n = number.as_i64().ok_or(None)?;
            u64::try_from(n).map_err(|_| None)
        }
        _ => Err(None),
    }
}

fn integer_i64(value: &Value) -> Result<i64, Option<String>> {
    match value {
        Value::Number(number) if number.is_i64() => number.as_i64().ok_or(None),
        Value::Number(number) if number.is_u64() => {
            i64::try_from(number.as_u64().ok_or(None)?).map_err(|_| None)
        }
        _ => Err(None),
    }
}

pub(crate) fn feature_id_valid(id: &str) -> bool {
    FEATURE_ID_RE.is_match(id)
}
