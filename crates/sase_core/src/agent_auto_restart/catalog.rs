//! Signature catalog: families, not symbols.
//!
//! Yesterday's symbol is gone today, so the catalog matches error
//! families scoped to a managed origin. A family matches only when the
//! module or symbol has a managed origin: its path resolves under a
//! managed root and never under the agent's workspace directory.

use super::wire::{AgentFailureFactsWire, AutoRestartContextWire};

/// Catalog tiers.
pub const TIER_TORN_PYTHON: &str = "tier1_torn_python";
pub const TIER_RUST_BINDING: &str = "tier2_rust_binding";
pub const TIER_DATA_FORMAT: &str = "tier3_data_format";
pub const TIER_REAL_BUG: &str = "tier4_real_bug";

/// Matched signature family plus the tier that owns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FamilyMatch {
    pub tier: &'static str,
    pub family: &'static str,
    pub signature: String,
    pub origin_module: Option<String>,
    pub missing_symbol: Option<String>,
}

/// Combined observable text for the regex fallback path.
pub fn combined_text(context: &AutoRestartContextWire) -> String {
    format!(
        "{}\n{}\n{}",
        context.error_text, context.traceback_text, context.log_tail
    )
    .to_ascii_lowercase()
}

fn has_any(haystack: &str, markers: &[&str]) -> bool {
    markers.iter().any(|marker| haystack.contains(marker))
}

/// Markers that always mean "never restart", checked before any family.
const PROVIDER_MARKERS: &[&str] = &[
    "429",
    "rate limit",
    "rate_limit",
    "usage limit",
    "usage_limit",
    "max_tokens",
    "context length",
    "context_length",
    "context overflow",
    "authentication",
    "unauthorized",
    "invalid api key",
    "invalid_api_key",
    "bad credentials",
    "token expired",
    "provider",
];

const KILL_MARKERS: &[&str] = &[
    "sigkill",
    "sigterm",
    "killed",
    "cancelled",
    "canceled",
    "plan rejected",
    "rejected plan",
    "stop output",
];

const RESOURCE_MARKERS: &[&str] = &[
    "out of memory",
    "memoryerror",
    "recursionerror",
    "recursion error",
    "maximum recursion",
    "timed out",
    "timeout",
    "no space left",
    "disk full",
    "permissionerror",
    "permission denied",
    "oserror",
];

const DIRECTIVE_MARKERS: &[&str] = &[
    "unknown directive",
    "unknown macro",
    "unknown alias",
    "directive error",
    "macro error",
];

const FINALIZER_MARKERS: &[&str] = &[
    "commit-finalizer",
    "commit finalizer",
    "publish failed",
    "gate-dispatch",
    "gate dispatch",
    "workspace materialization",
    "linked-repo materialization",
    "linked repo materialization",
    "materialization failed",
    "plan_agent_restart refused",
    "exited without recording an error",
];

/// Return the never-restart reason slug when the failure is one, else
/// `None`. This runs before family matching and before origin scoping.
pub fn never_restart_reason(
    facts: Option<&AgentFailureFactsWire>,
    context: &AutoRestartContextWire,
) -> Option<&'static str> {
    if context.is_remote {
        return Some("remote_row");
    }
    if let Some(outcome) = context.outcome.as_deref() {
        let outcome = outcome.to_ascii_lowercase();
        if outcome.contains("kill")
            || outcome.contains("stop")
            || outcome.contains("cancel")
        {
            return Some("killed_or_stopped");
        }
    }
    if let Some(kill) = context.kill_source.as_deref() {
        if !kill.trim().is_empty() {
            return Some("killed_or_stopped");
        }
    }
    let haystack = combined_text(context);
    if facts.map(|f| f.skew_suspect).unwrap_or(false) {
        // Structured facts still defer to the marker lists for the
        // never-restart classes; a skew-suspect ImportError from a
        // provider wrapper is still a provider error.
    }
    if has_any(&haystack, PROVIDER_MARKERS)
        && !has_any(
            &haystack,
            &[
                "cannot import name",
                "no module named",
                "partially initialized module",
            ],
        )
    {
        return Some("provider_error");
    }
    if has_any(&haystack, KILL_MARKERS) {
        return Some("killed_or_stopped");
    }
    if has_any(&haystack, RESOURCE_MARKERS) {
        return Some("resource_exhausted");
    }
    if has_any(&haystack, DIRECTIVE_MARKERS) {
        return Some("directive_or_macro_error");
    }
    if has_any(&haystack, FINALIZER_MARKERS) {
        return Some("finalizer_or_publish_failure");
    }
    None
}

/// A candidate managed module path extracted from facts or text.
fn looks_like_managed_module(value: &str) -> bool {
    let value = value.trim();
    value.starts_with("sase.")
        || value.starts_with("sase_core")
        || value == "sase_core_rs"
}

/// Check the managed-origin scope: the origin module must resolve under
/// a managed root and never under the workspace directory.
///
/// The classifier only sees module names and file paths, not a live
/// filesystem, so it applies a conservative name-and-path rule: a
/// `sase.*` / `sase_core*` module is managed unless every concrete file
/// path on record sits under the workspace directory.
pub fn origin_is_managed(
    origin_module: Option<&str>,
    candidate_files: &[String],
    context: &AutoRestartContextWire,
) -> bool {
    let module = origin_module.unwrap_or("");
    let workspace = context
        .workspace_dir
        .as_deref()
        .unwrap_or("\u{0}no-workspace\u{0}");
    if !looks_like_managed_module(module) {
        return false;
    }
    if candidate_files.is_empty() {
        // A bare managed module name with no file evidence still
        // counts: the dying runner records frames only when a live
        // traceback exists, and log-only rows carry none.
        return true;
    }
    let workspace = normalize_path(workspace);
    let mut saw_managed_root = false;
    for file in candidate_files {
        let file = normalize_path(file);
        if !workspace.is_empty()
            && (file == workspace || file.starts_with(&format!("{workspace}/")))
        {
            return false;
        }
        if is_under_managed_root(&file, context) || file.contains("sase") {
            saw_managed_root = true;
        }
    }
    saw_managed_root
}

fn normalize_path(value: &str) -> String {
    value.replace('\\', "/").trim().to_string()
}

fn is_under_managed_root(file: &str, context: &AutoRestartContextWire) -> bool {
    for root in &context.managed_roots {
        let base = normalize_path(&root.root);
        if !base.is_empty()
            && (file == base || file.starts_with(&format!("{base}/")))
        {
            return true;
        }
    }
    false
}

/// Collect candidate file paths from structured frames plus the raw
/// traceback text (for the regex fallback path).
pub fn candidate_files(
    facts: Option<&AgentFailureFactsWire>,
    context: &AutoRestartContextWire,
) -> Vec<String> {
    let mut files: Vec<String> = Vec::new();
    if let Some(facts) = facts {
        for frame in &facts.frames {
            if !frame.file.is_empty() {
                files.push(frame.file.clone());
            }
        }
        if let Some(last) = facts.last_frame_file.as_deref() {
            if !last.is_empty() {
                files.push(last.to_string());
            }
        }
    }
    for line in context
        .traceback_text
        .split_whitespace()
        .chain(context.log_tail.split_whitespace())
    {
        let trimmed = line.trim_matches(|c| {
            c == '"' || c == '\'' || c == ',' || c == '(' || c == ')'
        });
        if trimmed.ends_with(".py")
            && trimmed.len() < 512
            && !files.iter().any(|f| f == trimmed)
        {
            files.push(trimmed.to_string());
        }
    }
    files
}

/// Match the signature catalog against structured facts first, then the
/// regex fallback over error/traceback/log text.
pub fn match_family(
    facts: Option<&AgentFailureFactsWire>,
    context: &AutoRestartContextWire,
) -> Option<FamilyMatch> {
    if let Some(hit) = match_structured_facts(facts, context) {
        return Some(hit);
    }
    match_text_fallback(context)
}

fn match_structured_facts(
    facts: Option<&AgentFailureFactsWire>,
    context: &AutoRestartContextWire,
) -> Option<FamilyMatch> {
    let facts = facts?;
    let files = candidate_files(Some(facts), context);
    for link in &facts.exception_chain {
        let message = link.message.to_ascii_lowercase();
        if link.r#type == "SyntaxError" || link.r#type == "IndentationError" {
            let module = module_of_link(link);
            if origin_is_managed(module.as_deref(), &files, context) {
                return Some(FamilyMatch {
                    tier: TIER_TORN_PYTHON,
                    family: "syntax_in_managed_file",
                    signature: format!("{} in managed file", link.r#type),
                    origin_module: module,
                    missing_symbol: None,
                });
            }
            continue;
        }
        if link.r#type == "TypeError" && message.contains("signature") {
            return Some(FamilyMatch {
                tier: TIER_REAL_BUG,
                family: "signature_mismatch_type_error",
                signature: "TypeError: signature mismatch".to_string(),
                origin_module: module_of_link(link),
                missing_symbol: None,
            });
        }
        if link.r#type == "NameError" {
            let module = module_of_link(link);
            if origin_is_managed(module.as_deref(), &files, context) {
                return Some(FamilyMatch {
                    tier: TIER_REAL_BUG,
                    family: "name_error_in_managed_module",
                    signature: "NameError in managed module".to_string(),
                    origin_module: module,
                    missing_symbol: None,
                });
            }
        }
        if message.contains("was written by a newer")
            || message.contains("unknown sase version")
            || message.contains("newer or unknown")
            || message.contains("format this process does not understand")
        {
            return Some(FamilyMatch {
                tier: TIER_DATA_FORMAT,
                family: "data_format_skew",
                signature: "data written by a newer sase".to_string(),
                origin_module: module_of_link(link),
                missing_symbol: None,
            });
        }
        if message.contains("wire schema mismatch")
            || message.contains("wire is stale")
            || (message.contains("sase_core_rs")
                && (message.contains("no module named")
                    || message.contains("partially initialized")))
        {
            return Some(FamilyMatch {
                tier: TIER_RUST_BINDING,
                family: "rust_binding_or_wire_skew",
                signature: "sase_core_rs binding or wire skew".to_string(),
                origin_module: module_of_link(link),
                missing_symbol: None,
            });
        }
    }
    if let Some(import) = facts.import_error.as_ref() {
        let module = import.name.clone();
        if origin_is_managed(module.as_deref(), &files, context) {
            return Some(FamilyMatch {
                tier: TIER_TORN_PYTHON,
                family: "cannot_import_name",
                signature: format!(
                    "ImportError: cannot import name '{}'",
                    import.missing_symbol.as_deref().unwrap_or("?")
                ),
                origin_module: module,
                missing_symbol: import.missing_symbol.clone(),
            });
        }
        if module
            .as_deref()
            .is_some_and(|m| m.contains("sase_core_rs"))
            || import
                .missing_symbol
                .as_deref()
                .is_some_and(|s| !s.is_empty())
                && module.is_none()
        {
            return Some(FamilyMatch {
                tier: TIER_RUST_BINDING,
                family: "rust_binding_or_wire_skew",
                signature: "sase_core_rs import failure".to_string(),
                origin_module: module,
                missing_symbol: import.missing_symbol.clone(),
            });
        }
        return None;
    }
    if let Some(attr) = facts.attribute_error.as_ref() {
        let module = attr.module.clone();
        if origin_is_managed(module.as_deref(), &files, context) {
            return Some(FamilyMatch {
                tier: TIER_TORN_PYTHON,
                family: "module_has_no_attribute",
                signature: format!(
                    "AttributeError: module has no attribute '{}'",
                    attr.attribute.as_deref().unwrap_or("?")
                ),
                origin_module: module,
                missing_symbol: attr.attribute.clone(),
            });
        }
        return None;
    }
    // Bare skew-suspect chain types without structured extras.
    for link in &facts.exception_chain {
        match link.r#type.as_str() {
            "ImportError" | "ModuleNotFoundError" => {
                let module = module_of_link(link);
                if origin_is_managed(module.as_deref(), &files, context) {
                    return Some(FamilyMatch {
                        tier: TIER_TORN_PYTHON,
                        family: "torn_import",
                        signature: format!("{} in managed code", link.r#type),
                        origin_module: module,
                        missing_symbol: None,
                    });
                }
                return None;
            }
            _ => {}
        }
    }
    None
}

fn module_of_link(
    link: &super::wire::AgentFailureChainLinkWire,
) -> Option<String> {
    if looks_like_managed_module(&link.module) {
        return Some(link.module.clone());
    }
    None
}

fn match_text_fallback(
    context: &AutoRestartContextWire,
) -> Option<FamilyMatch> {
    let haystack = combined_text(context);
    // A traceback quoted in agent output is not a failure signature.
    // Heuristic: the failure text itself must carry the signature, not
    // only the log tail.
    let own_text =
        format!("{}\n{}", context.error_text, context.traceback_text)
            .to_ascii_lowercase();
    let files = candidate_files(None, context);
    let managed = |module: Option<String>| {
        origin_is_managed(module.as_deref(), &files, context)
    };
    if own_text.contains("was written by a newer")
        || own_text.contains("unknown sase version")
        || own_text.contains("newer or unknown")
        || own_text.contains("format this process does not understand")
    {
        return Some(FamilyMatch {
            tier: TIER_DATA_FORMAT,
            family: "data_format_skew",
            signature: "data written by a newer sase".to_string(),
            origin_module: find_module(&haystack),
            missing_symbol: None,
        });
    }
    if own_text.contains("does not expose binding") {
        return Some(FamilyMatch {
            tier: TIER_RUST_BINDING,
            family: "rust_binding_or_wire_skew",
            signature: "sase_core_rs binding missing".to_string(),
            origin_module: Some("sase_core_rs".to_string()),
            missing_symbol: find_quoted(&own_text),
        });
    }
    if own_text.contains("wire schema mismatch")
        || own_text.contains("wire is stale")
        || own_text.contains("no module named 'sase_core_rs'")
        || own_text.contains("no module named \"sase_core_rs\"")
    {
        return Some(FamilyMatch {
            tier: TIER_RUST_BINDING,
            family: "rust_binding_or_wire_skew",
            signature: "sase_core_rs binding or wire skew".to_string(),
            origin_module: Some("sase_core_rs".to_string()),
            missing_symbol: None,
        });
    }
    if own_text.contains("cannot import name") {
        let module = find_module(&haystack);
        let symbol = find_quoted(&own_text);
        if managed(module.clone()) {
            return Some(FamilyMatch {
                tier: TIER_TORN_PYTHON,
                family: "cannot_import_name",
                signature: "ImportError: cannot import name".to_string(),
                origin_module: module,
                missing_symbol: symbol,
            });
        }
        return None;
    }
    if own_text.contains("no module named") {
        let module = find_module(&haystack);
        if managed(module.clone()) {
            return Some(FamilyMatch {
                tier: TIER_TORN_PYTHON,
                family: "module_not_found",
                signature: "ModuleNotFoundError in managed code".to_string(),
                origin_module: module,
                missing_symbol: None,
            });
        }
        return None;
    }
    if own_text.contains("has no attribute") {
        let module = find_module(&haystack);
        if managed(module.clone()) {
            return Some(FamilyMatch {
                tier: TIER_TORN_PYTHON,
                family: "module_has_no_attribute",
                signature: "AttributeError in managed module".to_string(),
                origin_module: module,
                missing_symbol: None,
            });
        }
        return None;
    }
    if own_text.contains("partially initialized module") {
        let module = find_module(&haystack);
        if managed(module.clone()) {
            return Some(FamilyMatch {
                tier: TIER_TORN_PYTHON,
                family: "partially_initialized_module",
                signature: "partially initialized managed module".to_string(),
                origin_module: module,
                missing_symbol: None,
            });
        }
        return None;
    }
    None
}

/// Find the first `sase.*` / `sase_core*` module token in text.
fn find_module(haystack: &str) -> Option<String> {
    for token in haystack.split(|c: char| {
        !(c.is_alphanumeric() || c == '.' || c == '_' || c == '-')
    }) {
        if looks_like_managed_module(token) {
            return Some(token.to_string());
        }
    }
    None
}

/// Find the first single- or double-quoted symbol in text.
fn find_quoted(haystack: &str) -> Option<String> {
    let bytes = haystack.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let quote = bytes[index];
        if quote == b'\'' || quote == b'"' {
            let mut end = index + 1;
            while end < bytes.len() && bytes[end] != quote {
                end += 1;
            }
            if end < bytes.len() && end > index + 1 && end - index < 160 {
                return Some(haystack[index + 1..end].to_string());
            }
            index = end + 1;
        } else {
            index += 1;
        }
    }
    None
}
