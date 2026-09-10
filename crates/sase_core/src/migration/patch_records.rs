//! Parser-aware project-spec Patch record conversion contract.
//!
//! Deletion owner: sase-x7.14. Nothing here runs during startup, import,
//! completion, or an ordinary read; the migration kit calls plan/apply/verify
//! explicitly.

use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::digest::fingerprint;
use super::manifest::MigrationConflictRecord;
use super::MIGRATION_WIRE_SCHEMA_VERSION;
use crate::parser::parse_patch_project_bytes;
use crate::wire::PatchWire;

fn current_schema_version() -> u32 {
    MIGRATION_WIRE_SCHEMA_VERSION
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PatchRecordsConvertFactsWire {
    #[serde(default)]
    pub destination_exists: bool,
    #[serde(default)]
    pub follows_symlink_outside_roots: bool,
    #[serde(default)]
    pub observed_source_digest: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchRecordsConversionCountsWire {
    #[serde(default)]
    pub heading: u64,
    #[serde(default)]
    pub section: u64,
    #[serde(default)]
    pub review_label: u64,
    #[serde(default)]
    pub extension: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PatchRecordsConvertPlanWire {
    #[serde(default = "current_schema_version")]
    pub schema_version: u32,
    pub path: String,
    pub destination_path: String,
    pub source_digest: String,
    #[serde(default)]
    pub semantic_fingerprint: Option<String>,
    pub record_count: u64,
    pub conversions: PatchRecordsConversionCountsWire,
    #[serde(default)]
    pub conflicts: Vec<MigrationConflictRecord>,
    pub intended_action: String,
    pub estimated_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PatchRecordsConvertApplyWire {
    #[serde(default = "current_schema_version")]
    pub schema_version: u32,
    pub path: String,
    pub destination_path: String,
    pub source_digest: String,
    #[serde(default)]
    pub dest_digest: Option<String>,
    #[serde(default)]
    pub semantic_fingerprint: Option<String>,
    #[serde(default)]
    pub converted_utf8: Option<String>,
    #[serde(default)]
    pub conflicts: Vec<MigrationConflictRecord>,
    pub intended_action: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PatchRecordsConvertVerifyWire {
    #[serde(default = "current_schema_version")]
    pub schema_version: u32,
    pub path: String,
    pub equal_semantics: bool,
    #[serde(default)]
    pub source_fingerprint: Option<String>,
    #[serde(default)]
    pub converted_fingerprint: Option<String>,
    #[serde(default)]
    pub conflicts: Vec<MigrationConflictRecord>,
}

pub fn plan(
    path: &str,
    data: &[u8],
    facts: &PatchRecordsConvertFactsWire,
) -> PatchRecordsConvertPlanWire {
    let outcome = convert_inner(path, data, facts);
    PatchRecordsConvertPlanWire {
        schema_version: MIGRATION_WIRE_SCHEMA_VERSION,
        path: path.to_string(),
        destination_path: outcome.destination_path,
        source_digest: outcome.source_digest,
        semantic_fingerprint: outcome.semantic_fingerprint,
        record_count: outcome.record_count,
        conversions: outcome.conversions,
        conflicts: outcome.conflicts,
        intended_action: outcome.intended_action,
        estimated_bytes: data.len() as u64,
    }
}

pub fn apply(
    path: &str,
    data: &[u8],
    facts: &PatchRecordsConvertFactsWire,
) -> PatchRecordsConvertApplyWire {
    let outcome = convert_inner(path, data, facts);
    let dest_digest = outcome
        .converted_utf8
        .as_ref()
        .map(|text| sha256_hex(text.as_bytes()));
    PatchRecordsConvertApplyWire {
        schema_version: MIGRATION_WIRE_SCHEMA_VERSION,
        path: path.to_string(),
        destination_path: outcome.destination_path,
        source_digest: outcome.source_digest,
        dest_digest,
        semantic_fingerprint: outcome.semantic_fingerprint,
        converted_utf8: outcome.converted_utf8,
        conflicts: outcome.conflicts,
        intended_action: outcome.intended_action,
    }
}

pub fn verify(
    path: &str,
    original: &[u8],
    converted: &[u8],
    facts: &PatchRecordsConvertFactsWire,
) -> PatchRecordsConvertVerifyWire {
    let mut conflicts = Vec::new();
    if facts.follows_symlink_outside_roots {
        conflicts.push(conflict(
            path,
            "symlink_outside_roots",
            "path resolves through a symlink outside inventoried roots",
            None,
            None,
        ));
    }
    let source_fingerprint = semantic_fingerprint(path, original);
    let converted_fingerprint = semantic_fingerprint(path, converted);
    if source_fingerprint.is_none() {
        conflicts.push(conflict(
            path,
            "unreadable_source",
            "canonical parse of the source bytes failed",
            None,
            None,
        ));
    }
    if converted_fingerprint.is_none() {
        conflicts.push(conflict(
            path,
            "unreadable_converted",
            "canonical parse of the converted bytes failed",
            None,
            None,
        ));
    }
    let leftover = leftover_legacy_spellings(converted);
    if leftover > 0 {
        conflicts.push(conflict(
            path,
            "legacy_spelling_remaining",
            "converted text still has structural legacy headings, sections, or review labels",
            source_fingerprint.clone(),
            converted_fingerprint.clone(),
        ));
    }
    let equal_semantics = matches!(
        (&source_fingerprint, &converted_fingerprint),
        (Some(left), Some(right)) if left == right
    );
    if !equal_semantics {
        conflicts.push(conflict(
            path,
            "semantic_fingerprint_mismatch",
            "canonical PatchWire fingerprint changed across conversion",
            source_fingerprint.clone(),
            converted_fingerprint.clone(),
        ));
    }
    PatchRecordsConvertVerifyWire {
        schema_version: MIGRATION_WIRE_SCHEMA_VERSION,
        path: path.to_string(),
        equal_semantics: equal_semantics && conflicts.is_empty(),
        source_fingerprint,
        converted_fingerprint,
        conflicts,
    }
}

struct ConvertOutcome {
    destination_path: String,
    source_digest: String,
    semantic_fingerprint: Option<String>,
    record_count: u64,
    conversions: PatchRecordsConversionCountsWire,
    conflicts: Vec<MigrationConflictRecord>,
    intended_action: String,
    converted_utf8: Option<String>,
}

fn convert_inner(
    path: &str,
    data: &[u8],
    facts: &PatchRecordsConvertFactsWire,
) -> ConvertOutcome {
    let source_digest = sha256_hex(data);
    let destination_path = destination_path_for(path);
    let mut conversions = PatchRecordsConversionCountsWire::default();
    if destination_path != path {
        conversions.extension = 1;
    }
    let mut conflicts = Vec::new();
    if facts.follows_symlink_outside_roots {
        conflicts.push(conflict(
            path,
            "symlink_outside_roots",
            "path resolves through a symlink outside inventoried roots",
            None,
            None,
        ));
    }
    if facts.destination_exists && destination_path != path {
        conflicts.push(conflict(
            &destination_path,
            "destination_exists",
            "destination file already exists",
            None,
            None,
        ));
    }
    if let Some(observed) = facts.observed_source_digest.as_deref() {
        if observed != source_digest {
            conflicts.push(conflict(
                path,
                "source_digest_changed",
                "source digest changed since the dry run",
                Some(observed.to_string()),
                Some(source_digest.clone()),
            ));
        }
    }

    let Ok(text) = std::str::from_utf8(data) else {
        conflicts.push(conflict(
            path,
            "encoding",
            "source is not valid UTF-8",
            None,
            None,
        ));
        return ConvertOutcome {
            destination_path,
            source_digest,
            semantic_fingerprint: None,
            record_count: 0,
            conversions,
            conflicts,
            intended_action: "refuse".to_string(),
            converted_utf8: None,
        };
    };

    let patches = parse_patch_project_bytes(path, data).ok();
    let record_count =
        patches.as_ref().map(|rows| rows.len() as u64).unwrap_or(0);
    let semantic_fingerprint =
        patches.as_ref().and_then(|rows| fingerprint_patches(rows));
    mixed_record_conflicts(path, text, &mut conflicts);

    let (converted, line_counts) = convert_structural_text(text);
    conversions.heading = line_counts.heading;
    conversions.section = line_counts.section;
    conversions.review_label = line_counts.review_label;

    if !conflicts.is_empty() {
        return ConvertOutcome {
            destination_path,
            source_digest,
            semantic_fingerprint,
            record_count,
            conversions,
            conflicts,
            intended_action: "refuse".to_string(),
            converted_utf8: None,
        };
    }

    let no_textual_change = converted == text;
    if no_textual_change && conversions.extension == 0 {
        return ConvertOutcome {
            destination_path,
            source_digest,
            semantic_fingerprint,
            record_count,
            conversions,
            conflicts,
            intended_action: "noop".to_string(),
            converted_utf8: Some(converted),
        };
    }

    if let Some(expected) = &semantic_fingerprint {
        match fingerprint_patches_from_bytes(
            &destination_path,
            converted.as_bytes(),
        ) {
            Some(observed) if observed == *expected => {}
            observed => {
                conflicts.push(conflict(
                    path,
                    "semantic_fingerprint_mismatch",
                    "canonical parse differs semantically before and after conversion",
                    Some(expected.clone()),
                    observed,
                ));
                return ConvertOutcome {
                    destination_path,
                    source_digest,
                    semantic_fingerprint: Some(expected.clone()),
                    record_count,
                    conversions,
                    conflicts,
                    intended_action: "refuse".to_string(),
                    converted_utf8: None,
                };
            }
        }
    }

    ConvertOutcome {
        destination_path,
        source_digest,
        semantic_fingerprint,
        record_count,
        conversions,
        conflicts,
        intended_action: "convert".to_string(),
        converted_utf8: Some(converted),
    }
}

fn destination_path_for(path: &str) -> String {
    let file_path = Path::new(path);
    match file_path.extension().and_then(|ext| ext.to_str()) {
        Some("gp") => file_path
            .with_extension("sase")
            .to_string_lossy()
            .into_owned(),
        _ => path.to_string(),
    }
}

fn convert_structural_text(
    text: &str,
) -> (String, PatchRecordsConversionCountsWire) {
    let mut out = String::with_capacity(text.len());
    let mut counts = PatchRecordsConversionCountsWire::default();
    for chunk in text.split_inclusive('\n') {
        let (line, ending) = split_line_ending(chunk);
        let (converted, kind) = convert_structural_line(line);
        match kind {
            Some("heading") => counts.heading += 1,
            Some("section") => counts.section += 1,
            Some("review_label") => counts.review_label += 1,
            _ => {}
        }
        out.push_str(&converted);
        out.push_str(ending);
    }
    (out, counts)
}

fn split_line_ending(chunk: &str) -> (&str, &str) {
    if let Some(stripped) = chunk.strip_suffix('\n') {
        if let Some(stripped) = stripped.strip_suffix('\r') {
            (stripped, "\r\n")
        } else {
            (stripped, "\n")
        }
    } else {
        (chunk, "")
    }
}

const LEGACY_PATCH_HEADING: &str = "ChangeSpec"; // legacy compatibility alias

fn convert_structural_line(line: &str) -> (String, Option<&'static str>) {
    if is_named_header(line, LEGACY_PATCH_HEADING) {
        return (
            line.replacen(LEGACY_PATCH_HEADING, "Patch", 1),
            Some("heading"),
        );
    }
    if let Some(rest) = line.strip_prefix("COMMITS:") {
        return (format!("STITCHES:{rest}"), Some("section"));
    }
    if let Some(rest) = line.strip_prefix("CL: ") {
        return (format!("PR: {rest}"), Some("review_label"));
    }
    (line.to_string(), None)
}

fn leftover_legacy_spellings(data: &[u8]) -> u64 {
    let Ok(text) = std::str::from_utf8(data) else {
        return 1;
    };
    text.split_inclusive('\n')
        .filter(|chunk| {
            let (line, _) = split_line_ending(chunk);
            is_named_header(line, LEGACY_PATCH_HEADING)
                || line.starts_with("COMMITS:")
                || line.starts_with("CL: ")
        })
        .count() as u64
}

fn mixed_record_conflicts(
    path: &str,
    text: &str,
    conflicts: &mut Vec<MigrationConflictRecord>,
) {
    let lines: Vec<&str> = text.lines().collect();
    let mut idx = 0usize;
    while idx < lines.len() {
        let line = lines[idx];
        if is_named_header(line, "Patch")
            || is_named_header(line, LEGACY_PATCH_HEADING)
        {
            let (scan, next) = scan_one_record(&lines, idx + 1);
            push_mixed_conflicts(path, &scan, conflicts);
            idx = next;
        } else if line.starts_with("NAME: ") {
            let (scan, next) = scan_one_record(&lines, idx);
            push_mixed_conflicts(path, &scan, conflicts);
            idx = next;
        } else {
            idx += 1;
        }
    }
}

struct RecordScan {
    name: Option<String>,
    saw_commits: bool,
    saw_stitches: bool,
    cl_value: Option<String>,
    pr_value: Option<String>,
}

fn scan_one_record(lines: &[&str], start_idx: usize) -> (RecordScan, usize) {
    let mut scan = RecordScan {
        name: None,
        saw_commits: false,
        saw_stitches: false,
        cl_value: None,
        pr_value: None,
    };
    let mut idx = start_idx;
    let mut consecutive_blank = 0usize;
    while idx < lines.len() {
        let line = lines[idx];
        if (is_named_header(line, "Patch")
            || is_named_header(line, LEGACY_PATCH_HEADING))
            && idx > start_idx
        {
            break;
        }
        if line.is_empty() {
            consecutive_blank += 1;
            if consecutive_blank >= 2 {
                break;
            }
        } else {
            consecutive_blank = 0;
        }
        if let Some(rest) = line.strip_prefix("NAME: ") {
            if scan.name.is_some() {
                break;
            }
            scan.name = Some(rest.trim().to_string());
        }
        if line.starts_with("COMMITS:") {
            scan.saw_commits = true;
        }
        if line.starts_with("STITCHES:") {
            scan.saw_stitches = true;
        }
        if let Some(rest) = line.strip_prefix("CL: ") {
            scan.cl_value = Some(rest.trim().to_string());
        }
        if let Some(rest) = line.strip_prefix("PR: ") {
            scan.pr_value = Some(rest.trim().to_string());
        }
        idx += 1;
    }
    (scan, idx)
}

fn push_mixed_conflicts(
    path: &str,
    scan: &RecordScan,
    conflicts: &mut Vec<MigrationConflictRecord>,
) {
    let record_name = scan.name.as_deref().unwrap_or("<unnamed>");
    if scan.saw_commits && scan.saw_stitches {
        conflicts.push(conflict(
            path,
            "mixed_section_spellings",
            &format!(
                "record {record_name} mixes COMMITS: and STITCHES: section headers"
            ),
            None,
            None,
        ));
    }
    if let (Some(cl), Some(pr)) = (&scan.cl_value, &scan.pr_value) {
        if cl != pr {
            conflicts.push(conflict(
                path,
                "mixed_review_labels",
                &format!(
                    "record {record_name} mixes CL: and PR: with different values"
                ),
                None,
                None,
            ));
        }
    }
}

fn is_named_header(line: &str, name: &str) -> bool {
    let trimmed = line.trim();
    if !trimmed.starts_with("##") {
        return false;
    }
    let after = &trimmed[2..];
    let mut chars = after.chars();
    match chars.next() {
        Some(c) if c.is_whitespace() => {}
        _ => return false,
    }
    after.trim_start().starts_with(name)
}

fn fingerprint_patches(patches: &[PatchWire]) -> Option<String> {
    let normalized: Vec<PatchWire> = patches
        .iter()
        .cloned()
        .map(|mut patch| {
            patch.file_path = String::new();
            patch.source_span.file_path = String::new();
            patch
        })
        .collect();
    let value = serde_json::to_value(normalized).ok()?;
    fingerprint(&value).ok()
}

fn fingerprint_patches_from_bytes(path: &str, data: &[u8]) -> Option<String> {
    let patches = parse_patch_project_bytes(path, data).ok()?;
    fingerprint_patches(&patches)
}

fn semantic_fingerprint(path: &str, data: &[u8]) -> Option<String> {
    fingerprint_patches_from_bytes(path, data)
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn conflict(
    path: &str,
    kind: &str,
    detail: &str,
    expected: Option<String>,
    observed: Option<String>,
) -> MigrationConflictRecord {
    MigrationConflictRecord {
        schema_version: MIGRATION_WIRE_SCHEMA_VERSION,
        path: path.to_string(),
        kind: kind.to_string(),
        detail: Some(detail.to_string()),
        expected_fingerprint: expected,
        observed_fingerprint: observed,
        extensions: Default::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEGACY: &str = "\
## ChangeSpec
NAME: alpha
STATUS: WIP
CL: https://example.test/1
COMMITS:
  (1) first
";

    const CANONICAL: &str = "\
## Patch
NAME: alpha
STATUS: WIP
PR: https://example.test/1
STITCHES:
  (1) first
";

    fn facts() -> PatchRecordsConvertFactsWire {
        PatchRecordsConvertFactsWire::default()
    }

    #[test]
    fn convert_legacy_headings_sections_and_review_labels() {
        let plan = plan("proj.sase", LEGACY.as_bytes(), &facts());
        assert_eq!(plan.intended_action, "convert");
        assert_eq!(plan.conversions.heading, 1);
        assert_eq!(plan.conversions.section, 1);
        assert_eq!(plan.conversions.review_label, 1);
        assert!(plan.conflicts.is_empty());

        let applied = apply("proj.sase", LEGACY.as_bytes(), &facts());
        assert_eq!(applied.converted_utf8.as_deref(), Some(CANONICAL));
        let verified = verify(
            "proj.sase",
            LEGACY.as_bytes(),
            applied.converted_utf8.unwrap().as_bytes(),
            &facts(),
        );
        assert!(verified.equal_semantics);
        assert!(verified.conflicts.is_empty());
    }

    #[test]
    fn canonical_input_is_a_noop() {
        let plan = plan("proj.sase", CANONICAL.as_bytes(), &facts());
        assert_eq!(plan.intended_action, "noop");
        let applied = apply("proj.sase", CANONICAL.as_bytes(), &facts());
        assert_eq!(applied.converted_utf8.as_deref(), Some(CANONICAL));
    }

    #[test]
    fn mixed_document_converts_each_record_independently() {
        let src = "\
## ChangeSpec
NAME: legacy
STATUS: WIP
COMMITS:
  (1) one

## Patch
NAME: canonical
STATUS: Ready
STITCHES:
  (1) two
";
        let applied = apply("archive.sase", src.as_bytes(), &facts());
        assert_eq!(applied.intended_action, "convert");
        let converted = applied.converted_utf8.unwrap();
        assert!(converted.contains("## Patch\nNAME: legacy"));
        assert!(converted.contains("STITCHES:\n  (1) one"));
        assert!(converted.contains("## Patch\nNAME: canonical"));
        assert!(!converted.contains("COMMITS:"));
        assert!(!converted.contains("## ChangeSpec"));
    }

    #[test]
    fn mixed_spellings_in_one_record_refuse() {
        let src = "\
## Patch
NAME: mixed
STATUS: WIP
COMMITS:
  (1) one
STITCHES:
  (2) two
";
        let plan = plan("proj.sase", src.as_bytes(), &facts());
        assert_eq!(plan.intended_action, "refuse");
        assert!(plan
            .conflicts
            .iter()
            .any(|conflict| conflict.kind == "mixed_section_spellings"));
        let applied = apply("proj.sase", src.as_bytes(), &facts());
        assert!(applied.converted_utf8.is_none());
    }

    #[test]
    fn description_body_is_not_rewritten() {
        let src = "\
## Patch
NAME: alpha
STATUS: WIP
DESCRIPTION:
  mentions COMMITS: and CL: as prose
STITCHES:
  (1) first
";
        let applied = apply("proj.sase", src.as_bytes(), &facts());
        assert_eq!(applied.intended_action, "noop");
        assert!(applied
            .converted_utf8
            .unwrap()
            .contains("mentions COMMITS: and CL: as prose"));
    }

    #[test]
    fn gp_extension_renames_to_sase() {
        let plan = plan("proj.gp", CANONICAL.as_bytes(), &facts());
        assert_eq!(plan.destination_path, "proj.sase");
        assert_eq!(plan.conversions.extension, 1);
        assert_eq!(plan.intended_action, "convert");
    }

    #[test]
    fn destination_exists_and_digest_mismatch_refuse() {
        let exists = PatchRecordsConvertFactsWire {
            destination_exists: true,
            ..Default::default()
        };
        let exists_plan = plan("proj.gp", CANONICAL.as_bytes(), &exists);
        assert_eq!(exists_plan.intended_action, "refuse");
        assert!(exists_plan
            .conflicts
            .iter()
            .any(|conflict| conflict.kind == "destination_exists"));

        let digest = PatchRecordsConvertFactsWire {
            observed_source_digest: Some("not-the-digest".to_string()),
            ..Default::default()
        };
        let digest_plan = plan("proj.sase", CANONICAL.as_bytes(), &digest);
        assert_eq!(digest_plan.intended_action, "refuse");
        assert!(digest_plan
            .conflicts
            .iter()
            .any(|conflict| conflict.kind == "source_digest_changed"));
    }

    #[test]
    fn fingerprint_is_stable_json() {
        let value = serde_json::Value::Array(vec![]);
        assert!(fingerprint(&value).is_ok());
    }
}
