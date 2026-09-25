//! Registry, signature digest, per-stage collapse, extract(), compare.

use std::collections::{BTreeMap, HashSet};

use super::super::canonical::canonical_digest;
use super::super::wire::TOOL_RUN_WIRE_SCHEMA_VERSION;
use super::super::ToolRunError;
use super::extractors::raw::RawItem;
use super::extractors::{
    extract_cargo_test, extract_environment, extract_generic,
    extract_keep_sorted, extract_mypy, extract_prettier, extract_pytest,
    extract_ruff, extract_ruff_format, extract_symvision, extract_toobig,
};
use super::normalize::normalize_line;
use super::wire::{
    ToolRunTriageExtractRequestWire, ToolRunTriageExtractResultWire,
    ToolRunTriageExtractionStatusWire, ToolRunTriageItemWire,
    TOOL_RUN_TRIAGE_STAGE_KEY_RUN_OUTPUT,
};

fn validate_request_schema(version: u32) -> Result<(), ToolRunError> {
    if version != TOOL_RUN_WIRE_SCHEMA_VERSION {
        return Err(ToolRunError::SchemaVersion {
            expected: TOOL_RUN_WIRE_SCHEMA_VERSION,
            actual: version,
        });
    }
    Ok(())
}

pub struct ExtractorSpec {
    pub name: &'static str,
    pub version: u32,
    pub specific: bool,
    pub extract: fn(&[String]) -> Vec<RawItem>,
}

fn registry() -> &'static [ExtractorSpec] {
    static REGISTRY: &[ExtractorSpec] = &[
        ExtractorSpec {
            name: "symvision",
            version: 1,
            specific: true,
            extract: extract_symvision,
        },
        ExtractorSpec {
            name: "mypy",
            version: 1,
            specific: true,
            extract: extract_mypy,
        },
        ExtractorSpec {
            name: "ruff",
            version: 1,
            specific: true,
            extract: extract_ruff,
        },
        ExtractorSpec {
            name: "ruff_format",
            version: 1,
            specific: true,
            extract: extract_ruff_format,
        },
        ExtractorSpec {
            name: "prettier",
            version: 1,
            specific: true,
            extract: extract_prettier,
        },
        ExtractorSpec {
            name: "keep_sorted",
            version: 1,
            specific: true,
            extract: extract_keep_sorted,
        },
        ExtractorSpec {
            name: "toobig",
            version: 1,
            specific: true,
            extract: extract_toobig,
        },
        ExtractorSpec {
            name: "pytest",
            version: 1,
            specific: true,
            extract: extract_pytest,
        },
        ExtractorSpec {
            name: "cargo_test",
            version: 1,
            specific: true,
            extract: extract_cargo_test,
        },
        ExtractorSpec {
            name: "environment",
            version: 1,
            specific: true,
            extract: extract_environment,
        },
    ];
    REGISTRY
}

pub fn extractor_version(name: &str) -> Option<u32> {
    registry()
        .iter()
        .find(|spec| spec.name == name)
        .map(|spec| spec.version)
}

/// Stage-independent signature over (extractor, version, key).
pub fn triage_signature(
    extractor: &str,
    version: u32,
    key: &str,
) -> Result<String, ToolRunError> {
    canonical_digest(&(extractor, version, key)).map_err(ToolRunError::invalid)
}

#[allow(dead_code)]
fn _stage_key_constant() -> &'static str {
    TOOL_RUN_TRIAGE_STAGE_KEY_RUN_OUTPUT
}

pub fn extract_triage_items(
    request: ToolRunTriageExtractRequestWire,
) -> Result<ToolRunTriageExtractResultWire, ToolRunError> {
    validate_request_schema(request.schema_version)?;
    if request.stage_key.trim().is_empty() {
        return Err(ToolRunError::invalid("stage_key must not be empty"));
    }
    let missing = request
        .output
        .as_ref()
        .is_none_or(|text| text.trim().is_empty());
    if missing {
        return Ok(ToolRunTriageExtractResultWire {
            schema_version: super::super::wire::TOOL_RUN_WIRE_SCHEMA_VERSION,
            stage_key: request.stage_key,
            stage_id: request.stage_id,
            status: ToolRunTriageExtractionStatusWire::OutputMissing,
            items: Vec::new(),
            diagnostics: Vec::new(),
        });
    }
    let output = request.output.unwrap_or_default();
    let mut roots: Vec<String> = Vec::new();
    if let Some(root) = request.project_root {
        roots.push(root);
    }
    roots.extend(request.workspace_roots);
    let normalized: Vec<String> = output
        .lines()
        .map(|line| normalize_line(line, &roots))
        .collect();
    // Run every specific extractor; generic only when none matched.
    let mut hits: Vec<(&'static str, u32, RawItem)> = Vec::new();
    for spec in registry() {
        if !spec.specific {
            continue;
        }
        for item in (spec.extract)(&normalized) {
            hits.push((spec.name, spec.version, item));
        }
    }
    let mut status = if hits.is_empty() {
        ToolRunTriageExtractionStatusWire::Generic
    } else {
        ToolRunTriageExtractionStatusWire::Parsed
    };
    let mut diagnostics = Vec::new();
    if request.truncated {
        status = if missing {
            ToolRunTriageExtractionStatusWire::OutputMissing
        } else if hits.is_empty() {
            // Generic still runs on truncated output; status reports truncation.
            ToolRunTriageExtractionStatusWire::OutputTruncated
        } else {
            ToolRunTriageExtractionStatusWire::OutputTruncated
        };
        diagnostics
            .push("output truncated; items may be incomplete".to_string());
    }
    if hits.is_empty() {
        let generic = extract_generic(&request.stage_key, &normalized);
        for item in generic {
            hits.push(("generic", 1, item));
        }
        if !request.truncated {
            status = ToolRunTriageExtractionStatusWire::Generic;
        }
    }
    // Collapse identical (extractor, signature) within the stage.
    let mut order: Vec<(String, String)> = Vec::new();
    let mut collapsed: BTreeMap<(String, String), ToolRunTriageItemWire> =
        BTreeMap::new();
    let mut seen_keys: HashSet<(String, String)> = HashSet::new();
    for (extractor, version, raw) in hits {
        let signature = triage_signature(extractor, version, &raw.key)?;
        let key = (extractor.to_string(), signature.clone());
        if let Some(existing) = collapsed.get_mut(&key) {
            existing.occurrences += 1;
            let mut union = existing.locator_paths.clone();
            union.extend(raw.locator_paths.clone());
            union.sort();
            union.dedup();
            existing.locator_paths = union;
        } else {
            seen_keys.insert(key.clone());
            order.push(key.clone());
            collapsed.insert(
                key,
                ToolRunTriageItemWire {
                    schema_version:
                        super::super::wire::TOOL_RUN_WIRE_SCHEMA_VERSION,
                    item_id: None,
                    stage_key: request.stage_key.clone(),
                    stage_id: request.stage_id.clone(),
                    extractor: extractor.to_string(),
                    extractor_version: version,
                    signature,
                    display: raw.display,
                    locator_paths: raw.locator_paths,
                    occurrences: 1,
                    label: None,
                },
            );
        }
    }
    // First-appearance order: registry order then line order is preserved
    // because hits were pushed in registry order and each extractor scans in
    // line order; the BTreeMap above is only for lookup.
    let mut items = Vec::new();
    for key in order {
        if let Some(item) = collapsed.remove(&key) {
            items.push(item);
        }
    }
    Ok(ToolRunTriageExtractResultWire {
        schema_version: super::super::wire::TOOL_RUN_WIRE_SCHEMA_VERSION,
        stage_key: request.stage_key,
        stage_id: request.stage_id,
        status,
        items,
        diagnostics,
    })
}

pub fn compare_triage_signatures(
    first: &ToolRunTriageItemWire,
    second: &ToolRunTriageItemWire,
) -> Result<bool, ToolRunError> {
    if first.extractor == second.extractor
        && first.extractor_version != second.extractor_version
    {
        return Err(ToolRunError::invalid(
            "triage cross-version signatures are not comparable",
        ));
    }
    if first.extractor != second.extractor {
        return Ok(false);
    }
    Ok(first.signature == second.signature)
}
