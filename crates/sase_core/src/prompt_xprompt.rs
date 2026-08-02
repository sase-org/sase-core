//! Xprompt definition-provenance wire and prompt link rewriting.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::prompt_rewrite::{rewrite_prompt_links, PromptLinkCandidate};

/// Wire schema for records in an agent run's `xprompt_sources.json`.
pub const PROMPT_XPROMPT_SCHEMA_VERSION: u64 = 1;

/// Provenance captured for one xprompt reference at launch time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptXpromptRecord {
    pub schema_version: u64,
    pub raw_ref: String,
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub source_kind: Option<String>,
    #[serde(default)]
    pub source_path: Option<String>,
    #[serde(default)]
    pub definition_line: Option<u64>,
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default)]
    pub repo_root: Option<String>,
    #[serde(default)]
    pub repo_relpath: Option<String>,
    #[serde(default)]
    pub chezmoi: bool,
    #[serde(default)]
    pub skipped_reason: Option<String>,
}

/// Rewritten prompt text and the provenance rows that produced live links.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptXpromptRewrite {
    pub prompt: String,
    pub linked_records: Vec<PromptXpromptRecord>,
}

/// Parse a JSON array of provenance records, skipping invalid or future rows.
pub fn parse_xprompt_records(bytes: &[u8]) -> Vec<PromptXpromptRecord> {
    let values = match serde_json::from_slice::<Vec<serde_json::Value>>(bytes) {
        Ok(values) => values,
        Err(error) => {
            eprintln!("xprompt source records: {error}");
            return Vec::new();
        }
    };
    values
        .into_iter()
        .enumerate()
        .filter_map(|(index, value)| {
            let record_number = index + 1;
            match serde_json::from_value::<PromptXpromptRecord>(value) {
                Ok(record)
                    if record.schema_version == PROMPT_XPROMPT_SCHEMA_VERSION =>
                {
                    Some(record)
                }
                Ok(record) => {
                    eprintln!(
                        "xprompt source record {record_number}: unsupported schema_version {}",
                        record.schema_version
                    );
                    None
                }
                Err(error) => {
                    eprintln!("xprompt source record {record_number}: {error}");
                    None
                }
            }
        })
        .collect()
}

/// Select the newest row for each exact reference in first-appearance order.
pub fn select_xprompt_records(
    records: &[PromptXpromptRecord],
) -> Vec<PromptXpromptRecord> {
    let mut order = Vec::new();
    let mut seen_refs = HashSet::new();
    let mut newest_by_ref = HashMap::new();
    for record in records {
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

/// Rewrite captured xprompt tokens using hosted definition targets.
///
/// Literal launch zones and existing Markdown links are protected. Tokens
/// match only at boundaries accepted by the documented invoke grammar.
pub fn rewrite_prompt_xprompt_links<F>(
    prompt: &str,
    records: &[PromptXpromptRecord],
    mut resolver: F,
) -> PromptXpromptRewrite
where
    F: FnMut(&PromptXpromptRecord) -> Option<String>,
{
    let resolved = records
        .iter()
        .enumerate()
        .filter_map(|(record_index, record)| {
            resolver(record).and_then(|target| {
                (!record.raw_ref.is_empty() && !target.is_empty())
                    .then_some((record_index, target))
            })
        })
        .collect::<Vec<_>>();
    let candidates = resolved
        .iter()
        .map(|(record_index, target)| PromptLinkCandidate {
            record_index: *record_index,
            token: records[*record_index].raw_ref.as_str(),
            target,
        })
        .collect::<Vec<_>>();
    let (prompt, linked) = rewrite_prompt_links(
        prompt,
        records.len(),
        &candidates,
        valid_reference_start,
    );
    PromptXpromptRewrite {
        prompt,
        linked_records: records
            .iter()
            .zip(linked)
            .filter(|(_, linked)| *linked)
            .map(|(record, _)| record.clone())
            .collect(),
    }
}

fn valid_reference_start(prompt: &str, position: usize) -> bool {
    position == 0
        || prompt[..position]
            .chars()
            .next_back()
            .is_some_and(|character| {
                character.is_whitespace()
                    || matches!(character, '(' | '[' | '{' | '"' | '\'')
            })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(raw_ref: &str, source_path: Option<&str>) -> PromptXpromptRecord {
        PromptXpromptRecord {
            schema_version: PROMPT_XPROMPT_SCHEMA_VERSION,
            raw_ref: raw_ref.to_string(),
            name: raw_ref
                .trim_start_matches('#')
                .split([':', '(', '+', '`'])
                .next()
                .unwrap_or_default()
                .to_string(),
            kind: "xprompt".to_string(),
            source_kind: Some("markdown".to_string()),
            source_path: source_path.map(str::to_string),
            definition_line: None,
            repo: Some("sase".to_string()),
            repo_root: Some("/repo".to_string()),
            repo_relpath: Some("sase/xprompts/example.md".to_string()),
            chezmoi: false,
            skipped_reason: None,
        }
    }

    fn rewrite(
        prompt: &str,
        records: &[PromptXpromptRecord],
    ) -> PromptXpromptRewrite {
        rewrite_prompt_xprompt_links(prompt, records, |record| {
            record
                .source_path
                .as_ref()
                .map(|_| format!("definitions/{}.md", record.name))
        })
    }

    #[test]
    fn parse_tolerates_optional_and_unknown_fields_and_skips_bad_rows() {
        let input = br##"[
            {"schema_version":1,"raw_ref":"#plan","name":"plan","kind":"workflow","future":true},
            {"schema_version":1,"raw_ref":"#bad"},
            {"schema_version":99,"raw_ref":"#future","name":"future","kind":"xprompt"}
        ]"##;
        let parsed = parse_xprompt_records(input);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].raw_ref, "#plan");
        assert_eq!(parsed[0].source_path, None);
        assert!(!parsed[0].chezmoi);
    }

    #[test]
    fn selection_keeps_newest_rows_in_first_reference_order() {
        let first = record("#first", Some("/old"));
        let second = record("#second", Some("/same"));
        let newest_first = record("#first", Some("/new"));
        assert_eq!(
            select_xprompt_records(&[
                first,
                second.clone(),
                newest_first.clone(),
            ]),
            vec![newest_first, second]
        );
    }

    #[test]
    fn rewrite_skips_literal_zones_and_existing_markdown_links() {
        let plan = record("#plan", Some("/plan.md"));
        let prompt = concat!(
            "#plan `#plan`\n",
            "```text\n#plan\n```\n",
            "%xprompts_enabled:false\n#plan\n%xprompts_enabled:true\n",
            "[already #plan](plan.md)\n",
        );
        let rewritten = rewrite(prompt, std::slice::from_ref(&plan));
        assert_eq!(
            rewritten.prompt,
            concat!(
                "[#plan](definitions/plan.md) `#plan`\n",
                "```text\n#plan\n```\n",
                "%xprompts_enabled:false\n#plan\n%xprompts_enabled:true\n",
                "[already #plan](plan.md)\n",
            )
        );
        assert_eq!(rewritten.linked_records, vec![plan]);
    }

    #[test]
    fn rewrite_honors_boundaries_and_non_reference_hash_text() {
        let issue = record("#42", Some("/issue.md"));
        let plan = record("#plan", Some("/plan.md"));
        let prompt = "# Heading\ncolor #D7D7FF issue#42 (#42) [#plan] {#plan} \"#plan\" '#plan'";
        let rewritten = rewrite(prompt, &[issue, plan]);
        assert_eq!(
            rewritten.prompt,
            "# Heading\ncolor #D7D7FF issue#42 ([#42](definitions/42.md)) [[#plan](definitions/plan.md)] {[#plan](definitions/plan.md)} \"[#plan](definitions/plan.md)\" '[#plan](definitions/plan.md)'"
        );
    }

    #[test]
    fn rewrite_covers_workspace_and_argument_reference_forms() {
        let refs = [
            "#gh:sase",
            "#git:local",
            "#a:b",
            "#a(b, c)",
            "#a+",
            "#a:`quoted arg`",
        ];
        let records = refs
            .iter()
            .map(|raw_ref| record(raw_ref, Some("/definition")))
            .collect::<Vec<_>>();
        let prompt = refs.join(" ");
        let rewritten = rewrite(&prompt, &records);
        for raw_ref in refs {
            assert!(
                rewritten.prompt.contains(&format!("[{raw_ref}](")),
                "missing link for {raw_ref}: {}",
                rewritten.prompt
            );
        }
        assert_eq!(rewritten.linked_records, records);
    }

    #[test]
    fn rewrite_uses_longest_token_and_omits_unresolved_records() {
        let short = record("#plan", Some("/plan.md"));
        let long = record("#plan_more", Some("/plan_more.md"));
        let unresolved = record("#absent", None);
        let rewritten = rewrite(
            "#plan_more #plan #absent",
            &[short.clone(), long.clone(), unresolved],
        );
        assert_eq!(
            rewritten.prompt,
            "[#plan_more](definitions/plan_more.md) [#plan](definitions/plan.md) #absent"
        );
        assert_eq!(rewritten.linked_records, vec![short, long]);
    }

    #[test]
    fn rewrite_preserves_multibyte_text_around_reference() {
        let plan = record("#plan", Some("/plan.md"));
        let rewritten =
            rewrite("設計を #plan で進める 🦀", std::slice::from_ref(&plan));
        assert_eq!(
            rewritten.prompt,
            "設計を [#plan](definitions/plan.md) で進める 🦀"
        );
    }
}
