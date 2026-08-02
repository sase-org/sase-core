//! Shared prompt-artifact manifest, naming, and link-rewrite contract.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::prompt_rewrite::{rewrite_prompt_links, PromptLinkCandidate};

/// Wire schema for rows in `.sase/artifacts/prompt-artifacts.jsonl`.
pub const PROMPT_ARTIFACT_MANIFEST_SCHEMA_VERSION: u64 = 1;

const ARTIFACT_BASENAME_MAX_BYTES: usize = 120;
const ORIGINAL_NAME_HASH_LEN: usize = 8;

/// One staged reference from a prompt-artifact manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptArtifactRecord {
    pub schema_version: u64,
    pub recorded_at: String,
    pub agent_artifacts_dir: String,
    pub raw_ref: String,
    pub expanded_ref: String,
    pub ref_kind: String,
    pub label: String,
    #[serde(default)]
    pub source_path: Option<String>,
    #[serde(default)]
    pub sha256: Option<String>,
    #[serde(default)]
    pub size_bytes: Option<u64>,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub pool_relpath: Option<String>,
    #[serde(default)]
    pub vcs_repo: Option<String>,
    #[serde(default)]
    pub vcs_relpath: Option<String>,
    #[serde(default)]
    pub locator: Option<String>,
    #[serde(default)]
    pub skipped_reason: Option<String>,
}

/// Rewritten prompt text and the manifest rows that produced live links.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptArtifactRewrite {
    pub prompt: String,
    pub linked_records: Vec<PromptArtifactRecord>,
}

/// Build the content-addressed pool filename for one artifact.
///
/// Names that require sanitizing or truncating carry a short hash of the
/// original basename. That keeps otherwise-identical sanitized names distinct
/// within the same content digest without exposing directory components.
pub fn artifact_pool_filename(sha256: &str, original_name: &str) -> String {
    let digest_prefix = sha256
        .chars()
        .filter(char::is_ascii_hexdigit)
        .take(12)
        .collect::<String>()
        .to_ascii_lowercase();
    let digest_prefix = if digest_prefix.is_empty() {
        "000000000000"
    } else {
        digest_prefix.as_str()
    };

    let original_basename = original_name
        .rsplit(['/', '\\'])
        .find(|part| !part.is_empty())
        .unwrap_or("artifact");
    let sanitized = sanitize_basename(original_basename);
    let needs_disambiguator = sanitized != original_basename
        || sanitized.len() > ARTIFACT_BASENAME_MAX_BYTES
        || matches!(sanitized.as_str(), "" | "." | "..");
    let basename = bounded_basename(
        &sanitized,
        needs_disambiguator
            .then(|| short_original_name_hash(original_basename)),
    );
    format!("{digest_prefix}-{basename}")
}

fn sanitize_basename(value: &str) -> String {
    let (stem, extension) = split_extension(value);
    let stem = sanitize_component(stem);
    let stem = if stem.is_empty() { "artifact" } else { &stem };
    let extension = extension.map(sanitize_component);
    match extension
        .as_deref()
        .filter(|extension| !extension.is_empty())
    {
        Some(extension) => format!("{stem}.{extension}"),
        None => stem.to_string(),
    }
}

fn sanitize_component(value: &str) -> String {
    let mut sanitized = String::with_capacity(value.len());
    let mut replacing = false;
    for character in value.chars() {
        if character.is_ascii_alphanumeric()
            || matches!(character, '.' | '_' | '-')
        {
            sanitized.push(character);
            replacing = false;
        } else if !replacing {
            sanitized.push('-');
            replacing = true;
        }
    }
    sanitized.trim_matches('-').to_string()
}

fn short_original_name_hash(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    hex::encode(digest)[..ORIGINAL_NAME_HASH_LEN].to_string()
}

fn bounded_basename(value: &str, disambiguator: Option<String>) -> String {
    let safe = if matches!(value, "" | "." | "..") {
        "artifact"
    } else {
        value
    };
    let (stem, extension) = split_extension(safe);
    let suffix = disambiguator
        .as_deref()
        .map(|hash| format!("-{hash}"))
        .unwrap_or_default();
    let extension = extension.filter(|extension| {
        extension.len() + suffix.len() + 1 < ARTIFACT_BASENAME_MAX_BYTES
    });
    let extension_len = extension.map_or(0, |extension| extension.len() + 1);
    let stem_budget = ARTIFACT_BASENAME_MAX_BYTES
        .saturating_sub(extension_len + suffix.len())
        .max(1);
    let stem = &stem[..stem.len().min(stem_budget)];
    match extension {
        Some(extension) => format!("{stem}{suffix}.{extension}"),
        None => format!("{stem}{suffix}"),
    }
}

fn split_extension(value: &str) -> (&str, Option<&str>) {
    let Some(index) = value.rfind('.') else {
        return (value, None);
    };
    if index == 0 || index + 1 == value.len() {
        return (value, None);
    }
    (&value[..index], Some(&value[index + 1..]))
}

/// Parse a JSONL manifest, skipping malformed or unsupported rows.
///
/// Diagnostics go to stderr so one corrupt append cannot make all other rows
/// inaccessible to callers whose API intentionally returns only valid rows.
pub fn parse_prompt_artifact_manifest(
    bytes: &[u8],
) -> Vec<PromptArtifactRecord> {
    let mut records = Vec::new();
    for (index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
        let line_number = index + 1;
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        match serde_json::from_slice::<PromptArtifactRecord>(line) {
            Ok(record)
                if record.schema_version
                    == PROMPT_ARTIFACT_MANIFEST_SCHEMA_VERSION =>
            {
                records.push(record);
            }
            Ok(record) => eprintln!(
                "prompt artifact manifest line {line_number}: unsupported schema_version {}",
                record.schema_version
            ),
            Err(error) => eprintln!(
                "prompt artifact manifest line {line_number}: {error}"
            ),
        }
    }
    records
}

/// Render one compact JSON manifest row without a trailing newline.
pub fn render_prompt_artifact_record(
    record: &PromptArtifactRecord,
) -> Result<String, serde_json::Error> {
    if record.schema_version != PROMPT_ARTIFACT_MANIFEST_SCHEMA_VERSION {
        return Err(<serde_json::Error as serde::ser::Error>::custom(format!(
            "unsupported prompt artifact manifest schema_version {}",
            record.schema_version
        )));
    }
    serde_json::to_string(record)
}

/// Select the newest row for each reference from one agent run.
///
/// The manifest is append-only, so later rows are newer. Output order follows
/// each raw reference's first appearance, even when a later row replaces it.
pub fn select_manifest_records(
    records: &[PromptArtifactRecord],
    agent_artifacts_dir: &str,
) -> Vec<PromptArtifactRecord> {
    let mut order = Vec::new();
    let mut seen_refs = HashSet::new();
    let mut newest_by_ref = HashMap::new();
    for record in records
        .iter()
        .filter(|record| record.agent_artifacts_dir == agent_artifacts_dir)
    {
        if seen_refs.insert(record.raw_ref.clone()) {
            order.push(record.raw_ref.clone());
        }
        newest_by_ref.insert(record.raw_ref.clone(), record.clone());
    }
    order
        .into_iter()
        .filter_map(|raw_ref| newest_by_ref.remove(&raw_ref))
        .collect()
}

/// Rewrite live prompt reference tokens to Markdown links.
///
/// Literal launch zones and existing Markdown links are protected. The
/// resolver returns the target for each record, or `None` to leave it alone.
pub fn rewrite_prompt_artifact_links<F>(
    prompt: &str,
    records: &[PromptArtifactRecord],
    mut resolver: F,
) -> PromptArtifactRewrite
where
    F: FnMut(&PromptArtifactRecord) -> Option<String>,
{
    let resolved = records
        .iter()
        .enumerate()
        .filter_map(|(record_index, record)| {
            resolver(record).and_then(|target| {
                (!target.is_empty()).then_some((record_index, target))
            })
        })
        .collect::<Vec<_>>();
    let candidates = resolved
        .iter()
        .flat_map(|(record_index, target)| {
            let record = &records[*record_index];
            [record.raw_ref.as_str(), record.expanded_ref.as_str()]
                .into_iter()
                .filter(|token| !token.is_empty())
                .map(move |token| PromptLinkCandidate {
                    record_index: *record_index,
                    token,
                    target,
                })
        })
        .collect::<Vec<_>>();
    let (prompt, linked) =
        rewrite_prompt_links(prompt, records.len(), &candidates, |_, _| true);

    PromptArtifactRewrite {
        prompt,
        linked_records: records
            .iter()
            .zip(linked)
            .filter(|(_, linked)| *linked)
            .map(|(record, _)| record.clone())
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(raw_ref: &str, sha256: Option<&str>) -> PromptArtifactRecord {
        PromptArtifactRecord {
            schema_version: PROMPT_ARTIFACT_MANIFEST_SCHEMA_VERSION,
            recorded_at: "2026-08-01T14:22:03Z".to_string(),
            agent_artifacts_dir: "/artifacts/run-one".to_string(),
            raw_ref: raw_ref.to_string(),
            expanded_ref: raw_ref.to_string(),
            ref_kind: "file".to_string(),
            label: raw_ref.trim_start_matches('@').to_string(),
            source_path: None,
            sha256: sha256.map(str::to_string),
            size_bytes: None,
            mime_type: None,
            pool_relpath: None,
            vcs_repo: None,
            vcs_relpath: None,
            locator: None,
            skipped_reason: None,
        }
    }

    #[test]
    fn pool_names_cover_paths_unicode_extensions_and_length() {
        let digest =
            "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789";
        assert_eq!(
            artifact_pool_filename(digest, "../../safe.txt"),
            "abcdef012345-safe.txt"
        );
        assert_eq!(
            artifact_pool_filename(digest, r"..\\..\\safe.txt"),
            "abcdef012345-safe.txt"
        );
        let unicode = artifact_pool_filename(digest, "設計 図.png");
        assert!(unicode.starts_with("abcdef012345-artifact-"), "{unicode}");
        assert!(unicode.ends_with(".png"), "{unicode}");
        let no_extension = artifact_pool_filename(digest, "README");
        assert_eq!(no_extension, "abcdef012345-README");
        let long = artifact_pool_filename(
            digest,
            &format!("{}.tar.gz", "a".repeat(300)),
        );
        assert!(long.len() <= 12 + 1 + ARTIFACT_BASENAME_MAX_BYTES);
        assert!(long.ends_with(".gz"));
    }

    #[test]
    fn sanitized_collisions_are_disambiguated() {
        let digest = "a".repeat(64);
        assert_ne!(
            artifact_pool_filename(&digest, "a b.txt"),
            artifact_pool_filename(&digest, "a*b.txt")
        );
    }

    #[test]
    fn manifest_round_trip_skips_bad_and_future_lines() {
        let mut expected = record("@~/diagram.png", Some(&"a".repeat(64)));
        expected.source_path = Some("/home/user/diagram.png".to_string());
        let rendered = render_prompt_artifact_record(&expected).unwrap();
        let manifest =
            format!("{rendered}\nnot json\n{{\"schema_version\":99}}\n\n");
        assert_eq!(
            parse_prompt_artifact_manifest(manifest.as_bytes()),
            vec![expected]
        );
    }

    #[test]
    fn manifest_parse_tolerates_unknown_fields() {
        let rendered =
            render_prompt_artifact_record(&record("@doc", None)).unwrap();
        let with_unknown =
            rendered.replacen('{', "{\"future_field\":{\"nested\":true},", 1);
        assert_eq!(
            parse_prompt_artifact_manifest(with_unknown.as_bytes()).len(),
            1
        );
    }

    #[test]
    fn selection_keeps_newest_rows_in_first_reference_order() {
        let first = record("@first", Some("old"));
        let second = record("@second", Some("same"));
        let mut newest_first = record("@first", Some("new"));
        newest_first.recorded_at = "2026-08-01T15:00:00Z".to_string();
        let mut other_run = record("@ignored", None);
        other_run.agent_artifacts_dir = "/artifacts/run-two".to_string();
        assert_eq!(
            select_manifest_records(
                &[first, second.clone(), newest_first.clone(), other_run],
                "/artifacts/run-one"
            ),
            vec![newest_first, second]
        );
    }

    #[test]
    fn rewrite_is_idempotent_and_reports_only_linked_records() {
        let diagram = record("@~/diagram.png", Some("digest"));
        let absent = record("@absent", None);
        let records = vec![diagram.clone(), absent];
        let first = rewrite_prompt_artifact_links(
            "Open @~/diagram.png and @~/diagram.png.",
            &records,
            |record| {
                (record.raw_ref == diagram.raw_ref)
                    .then(|| "artifact.png".to_string())
            },
        );
        assert_eq!(
            first.prompt,
            "Open [@~/diagram.png](artifact.png) and [@~/diagram.png](artifact.png)."
        );
        assert_eq!(first.linked_records, vec![diagram.clone()]);
        let second =
            rewrite_prompt_artifact_links(&first.prompt, &records, |_| {
                Some("artifact.png".to_string())
            });
        assert_eq!(second.prompt, first.prompt);
        assert!(second.linked_records.is_empty());
    }

    #[test]
    fn rewrite_skips_literal_zones_and_existing_markdown_links() {
        let artifact = record("@doc:file.md", None);
        let prompt = "@doc:file.md `@doc:file.md`\n```text\n@doc:file.md\n```\n[already @doc:file.md](target.md)\n";
        let rewritten = rewrite_prompt_artifact_links(
            prompt,
            std::slice::from_ref(&artifact),
            |_| Some("archive.md".to_string()),
        );
        assert_eq!(
            rewritten.prompt,
            "[@doc:file.md](archive.md) `@doc:file.md`\n```text\n@doc:file.md\n```\n[already @doc:file.md](target.md)\n"
        );
        assert_eq!(rewritten.linked_records, vec![artifact]);
    }

    #[test]
    fn rewrite_uses_expanded_reference_when_authored_token_is_absent() {
        let mut artifact = record("@~/diagram.png", None);
        artifact.expanded_ref = "@.sase/artifacts/home/diagram.png".to_string();
        let rewritten = rewrite_prompt_artifact_links(
            "Open @.sase/artifacts/home/diagram.png.",
            std::slice::from_ref(&artifact),
            |_| Some("archive.png".to_string()),
        );
        assert_eq!(
            rewritten.prompt,
            "Open [@.sase/artifacts/home/diagram.png](archive.png)."
        );
    }
}
