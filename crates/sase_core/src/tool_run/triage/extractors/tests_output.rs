//! Pytest and cargo-test output extractors.

use regex::Regex;
use std::sync::OnceLock;

use super::raw::RawItem;

fn pytest_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^(FAILED|ERROR) (\S+\.py(?:::\S+)?)(?: - .*)?$").unwrap()
    })
}

fn running_deps_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"Running .*\(.*deps/([A-Za-z0-9_]+)-").unwrap()
    })
}

fn cargo_fail_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^test (\S+) \.\.\. FAILED$").unwrap())
}

fn rerun_crate_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"error: test failed, to rerun pass.*-p (\S+)").unwrap()
    })
}

fn param_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\[.*\]$").unwrap())
}

fn strip_param(node: &str) -> String {
    // Remove one trailing [...] parametrization from the node id.
    if let Some(start) = node.rfind('[') {
        if node.ends_with(']') {
            return node[..start].to_string();
        }
    }
    node.to_string()
}

pub fn extract_pytest(lines: &[String]) -> Vec<RawItem> {
    let mut items = Vec::new();
    for line in lines {
        // `ERROR    sase.x:app.py:375 …` logger lines never match: they lack
        // the single-space `FAILED ` / `ERROR ` prefix shape.
        let Some(caps) = pytest_re().captures(line) else {
            continue;
        };
        let node = caps.get(2).map(|hit| hit.as_str()).unwrap_or("");
        let key = strip_param(node);
        let test_file = node.split("::").next().unwrap_or(node);
        let _ = param_re();
        items.push(RawItem::new(key, line, vec![test_file.to_string()]));
    }
    items
}

pub fn extract_cargo_test(lines: &[String]) -> Vec<RawItem> {
    let mut items = Vec::new();
    let mut current_crate = String::from("unknown");
    for line in lines {
        if let Some(caps) = running_deps_re().captures(line) {
            current_crate = caps
                .get(1)
                .map(|hit| hit.as_str())
                .unwrap_or("unknown")
                .to_string();
            continue;
        }
        if let Some(caps) = rerun_crate_re().captures(line) {
            // Fallback crate only when no Running line named one.
            if current_crate == "unknown" {
                current_crate = caps
                    .get(1)
                    .map(|hit| hit.as_str())
                    .unwrap_or("unknown")
                    .to_string();
            }
            continue;
        }
        if let Some(caps) = cargo_fail_re().captures(line) {
            let test_path = caps.get(1).map(|hit| hit.as_str()).unwrap_or("");
            items.push(RawItem::new(
                format!("{current_crate}::{test_path}"),
                line,
                Vec::new(),
            ));
        }
    }
    items
}
