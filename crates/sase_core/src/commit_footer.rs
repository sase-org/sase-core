//! Structural parser and updater for SASE commit-message footers.
//!
//! The domain model deliberately knows nothing about Git hosts, filesystems,
//! or SDD storage.  It only owns the terminal footer grammar shared by every
//! frontend: an ordered tag block and an optional block of Markdown reference
//! definitions used by explicit `[label][id]` tag values.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

pub const COMMIT_FOOTER_WIRE_SCHEMA_VERSION: u32 = 1;
const COMMIT_TAG_PREFIX: &str = "SASE_";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitFooterReferenceWire {
    pub id: String,
    pub destination: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitFooterTagWire {
    pub raw_key: String,
    pub key: String,
    pub raw_value: String,
    pub label: String,
    pub destination: Option<String>,
    pub reference_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitFooterWire {
    pub schema_version: u32,
    pub body: String,
    pub footer: String,
    pub tags: Vec<CommitFooterTagWire>,
    pub references: Vec<CommitFooterReferenceWire>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitFooterUpdateWire {
    pub key: String,
    pub label: String,
    #[serde(default)]
    pub destination: Option<String>,
    #[serde(default)]
    pub reference_id: Option<String>,
}

#[derive(Clone, Debug)]
struct ParsedReference<'a> {
    id: &'a str,
    destination: &'a str,
}

#[derive(Clone, Debug)]
struct RenderedTag {
    key: String,
    label: String,
    reference_id: Option<String>,
}

/// Parse the terminal structured SASE footer from `message`.
///
/// Reference definitions are recognized only when they are terminal, follow
/// exactly one blank separator after a tag block, and are used by at least one
/// explicit linked tag value.  This keeps ordinary Markdown definitions and
/// tag-shaped prose in the message body.
pub fn parse_commit_footer(message: &str) -> CommitFooterWire {
    let lines: Vec<&str> = message.split('\n').collect();
    let Some(last_non_blank) =
        lines.iter().rposition(|line| !line.trim().is_empty())
    else {
        return empty_footer("");
    };

    if let Some(parsed) = parse_footer_with_references(&lines, last_non_blank) {
        return parsed;
    }

    let Some((tags_start, raw_tags)) = scan_tag_block(&lines, last_non_blank)
    else {
        return empty_footer(message.trim_end());
    };
    build_footer(&lines, tags_start, last_non_blank, raw_tags, Vec::new())
}

/// Update and render a SASE footer while preserving retained linked values.
pub fn update_commit_footer(
    message: &str,
    updates: &[CommitFooterUpdateWire],
    remove_keys: &[String],
) -> String {
    let parsed = parse_commit_footer(message);
    let mut owned: BTreeSet<String> = remove_keys
        .iter()
        .map(|key| canonicalize_key(key))
        .collect();
    owned.extend(updates.iter().map(|update| canonicalize_key(&update.key)));

    let retained: Vec<CommitFooterTagWire> = parsed
        .tags
        .iter()
        .filter(|tag| !owned.contains(&tag.key))
        .cloned()
        .collect();

    let mut existing_target_ids: BTreeMap<String, Vec<String>> =
        BTreeMap::new();
    let mut reserved_ids = reference_ids_in_body(&parsed.body);
    for reference in &parsed.references {
        reserved_ids.insert(reference.id.clone());
        existing_target_ids
            .entry(reference.destination.clone())
            .or_default()
            .push(reference.id.clone());
    }

    let mut rendered_tags: Vec<RenderedTag> = Vec::new();
    let mut used_ids: BTreeMap<String, String> = BTreeMap::new();
    let mut target_ids: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut reference_order: Vec<String> = Vec::new();

    for tag in retained {
        let key = tag.key;
        let label = tag.label;
        let linked = match (tag.destination, tag.reference_id) {
            (Some(destination), Some(preferred_id)) => {
                Some(assign_reference_id(
                    &destination,
                    Some(&preferred_id),
                    &existing_target_ids,
                    &reserved_ids,
                    &mut used_ids,
                    &mut target_ids,
                    &mut reference_order,
                ))
            }
            _ => None,
        };
        rendered_tags.push(RenderedTag {
            key,
            label,
            reference_id: linked,
        });
    }

    for update in updates {
        let Some(label) = sanitize(&update.label) else {
            continue;
        };
        let key = canonicalize_key(&update.key);
        let destination = update.destination.as_deref().and_then(sanitize);
        let linked = destination.as_deref().map(|destination| {
            assign_reference_id(
                destination,
                update.reference_id.as_deref(),
                &existing_target_ids,
                &reserved_ids,
                &mut used_ids,
                &mut target_ids,
                &mut reference_order,
            )
        });
        rendered_tags.push(RenderedTag {
            key,
            label,
            reference_id: linked,
        });
    }

    if rendered_tags.is_empty() {
        return parsed.body;
    }

    let tag_block = rendered_tags
        .iter()
        .map(|tag| match &tag.reference_id {
            Some(id) => {
                format!(
                    "{}{}=[{}][{}]",
                    COMMIT_TAG_PREFIX, tag.key, tag.label, id
                )
            }
            None => format!("{}{}={}", COMMIT_TAG_PREFIX, tag.key, tag.label),
        })
        .collect::<Vec<_>>()
        .join("\n");

    let definition_block = reference_order
        .iter()
        .filter_map(|id| used_ids.get(id).map(|destination| (id, destination)))
        .map(|(id, destination)| format!("[{id}]: {destination}"))
        .collect::<Vec<_>>()
        .join("\n");

    let mut footer = tag_block;
    if !definition_block.is_empty() {
        footer.push_str("\n\n");
        footer.push_str(&definition_block);
    }
    if parsed.body.is_empty() {
        footer
    } else {
        format!("{}\n\n{}", parsed.body, footer)
    }
}

fn parse_footer_with_references(
    lines: &[&str],
    last_non_blank: usize,
) -> Option<CommitFooterWire> {
    let mut definitions_start = last_non_blank;
    let mut raw_definitions: Vec<ParsedReference<'_>> = Vec::new();
    for index in (0..=last_non_blank).rev() {
        let Some(reference) = parse_reference_definition(lines[index].trim())
        else {
            break;
        };
        definitions_start = index;
        raw_definitions.push(reference);
    }
    if raw_definitions.is_empty()
        || definitions_start < 2
        || !lines[definitions_start - 1].trim().is_empty()
        || lines[definitions_start - 2].trim().is_empty()
    {
        return None;
    }
    raw_definitions.reverse();

    let tag_end = definitions_start - 2;
    let (tags_start, raw_tags) = scan_tag_block(lines, tag_end)?;
    let destinations: BTreeMap<&str, &str> = raw_definitions
        .iter()
        .map(|reference| (reference.id, reference.destination))
        .collect();
    let uses_definition = raw_tags.iter().any(|(_, value)| {
        parse_linked_value(value)
            .is_some_and(|(_, id)| destinations.contains_key(id))
    });
    if !uses_definition {
        return None;
    }

    let references = raw_definitions
        .into_iter()
        .map(|reference| CommitFooterReferenceWire {
            id: reference.id.to_string(),
            destination: reference.destination.to_string(),
        })
        .collect();
    Some(build_footer(
        lines,
        tags_start,
        last_non_blank,
        raw_tags,
        references,
    ))
}

fn scan_tag_block<'a>(
    lines: &[&'a str],
    end: usize,
) -> Option<(usize, Vec<(&'a str, &'a str)>)> {
    let mut tags_start = end + 1;
    for index in (0..=end).rev() {
        let stripped = lines[index].trim();
        if parse_tag_line(stripped).is_some() {
            tags_start = index;
        } else if stripped.is_empty() {
            continue;
        } else {
            break;
        }
    }
    if tags_start > end {
        return None;
    }
    let tags = lines[tags_start..=end]
        .iter()
        .filter_map(|line| parse_tag_line(line.trim()))
        .collect::<Vec<_>>();
    (!tags.is_empty()).then_some((tags_start, tags))
}

fn build_footer(
    lines: &[&str],
    tags_start: usize,
    last_non_blank: usize,
    raw_tags: Vec<(&str, &str)>,
    references: Vec<CommitFooterReferenceWire>,
) -> CommitFooterWire {
    let destinations: BTreeMap<&str, &str> = references
        .iter()
        .map(|reference| {
            (reference.id.as_str(), reference.destination.as_str())
        })
        .collect();
    let tags = raw_tags
        .into_iter()
        .map(|(raw_key, raw_value)| {
            let linked =
                parse_linked_value(raw_value).and_then(|(label, id)| {
                    destinations
                        .get(id)
                        .map(|destination| (label, id, *destination))
                });
            let (label, reference_id, destination) = match linked {
                Some((label, id, destination)) => (
                    label.to_string(),
                    Some(id.to_string()),
                    Some(destination.to_string()),
                ),
                None => (raw_value.to_string(), None, None),
            };
            CommitFooterTagWire {
                raw_key: raw_key.to_string(),
                key: canonicalize_key(raw_key),
                raw_value: raw_value.to_string(),
                label,
                destination,
                reference_id,
            }
        })
        .collect();
    CommitFooterWire {
        schema_version: COMMIT_FOOTER_WIRE_SCHEMA_VERSION,
        body: lines[..tags_start].join("\n").trim_end().to_string(),
        footer: lines[tags_start..=last_non_blank].join("\n"),
        tags,
        references,
    }
}

fn empty_footer(body: &str) -> CommitFooterWire {
    CommitFooterWire {
        schema_version: COMMIT_FOOTER_WIRE_SCHEMA_VERSION,
        body: body.to_string(),
        footer: String::new(),
        tags: Vec::new(),
        references: Vec::new(),
    }
}

fn assign_reference_id(
    destination: &str,
    preferred_id: Option<&str>,
    existing_target_ids: &BTreeMap<String, Vec<String>>,
    reserved_ids: &BTreeSet<String>,
    used_ids: &mut BTreeMap<String, String>,
    target_ids: &mut BTreeMap<String, Vec<String>>,
    reference_order: &mut Vec<String>,
) -> String {
    let preferred = preferred_id.and_then(sanitize);
    let mut candidates = Vec::new();
    if let Some(id) = preferred {
        candidates.push(id);
    }
    if let Some(ids) = target_ids.get(destination) {
        candidates.extend(ids.iter().cloned());
    }
    if let Some(ids) = existing_target_ids.get(destination) {
        candidates.extend(ids.iter().cloned());
    }

    let id = candidates
        .into_iter()
        .find(|id| match used_ids.get(id) {
            Some(used) => used == destination,
            None => true,
        })
        .unwrap_or_else(|| allocate_numeric_id(reserved_ids, used_ids));
    if !used_ids.contains_key(&id) {
        used_ids.insert(id.clone(), destination.to_string());
        target_ids
            .entry(destination.to_string())
            .or_default()
            .push(id.clone());
        reference_order.push(id.clone());
    }
    id
}

fn allocate_numeric_id(
    reserved_ids: &BTreeSet<String>,
    used_ids: &BTreeMap<String, String>,
) -> String {
    let mut number = 1_u64;
    loop {
        let candidate = number.to_string();
        if !reserved_ids.contains(&candidate)
            && !used_ids.contains_key(&candidate)
        {
            return candidate;
        }
        number += 1;
    }
}

fn reference_ids_in_body(body: &str) -> BTreeSet<String> {
    body.lines()
        .filter_map(|line| parse_reference_definition(line.trim()))
        .map(|reference| reference.id.to_string())
        .collect()
}

fn parse_tag_line(line: &str) -> Option<(&str, &str)> {
    let (key, value) = line.split_once('=')?;
    is_tag_key(key).then_some((key, value))
}

fn is_tag_key(key: &str) -> bool {
    let mut chars = key.chars();
    chars.next().is_some_and(|first| first.is_ascii_uppercase())
        && chars.all(|ch| {
            ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_'
        })
}

fn parse_linked_value(value: &str) -> Option<(&str, &str)> {
    if !value.starts_with('[') || !value.ends_with(']') {
        return None;
    }
    let separator = value.find("][")?;
    let label = &value[1..separator];
    let id = &value[separator + 2..value.len() - 1];
    (!label.is_empty()
        && !id.is_empty()
        && !label.contains(']')
        && !id.contains(']'))
    .then_some((label, id))
}

fn parse_reference_definition(line: &str) -> Option<ParsedReference<'_>> {
    if !line.starts_with('[') {
        return None;
    }
    let separator = line.find("]:")?;
    let id = &line[1..separator];
    let destination = line[separator + 2..].trim();
    (!id.is_empty() && !id.contains(']') && !destination.is_empty())
        .then_some(ParsedReference { id, destination })
}

fn canonicalize_key(key: &str) -> String {
    key.strip_prefix(COMMIT_TAG_PREFIX)
        .unwrap_or(key)
        .to_string()
}

fn sanitize(value: &str) -> Option<String> {
    let cleaned = value.replace(['\r', '\n'], " ");
    let trimmed = cleaned.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plain(key: &str, value: &str) -> CommitFooterUpdateWire {
        CommitFooterUpdateWire {
            key: key.to_string(),
            label: value.to_string(),
            destination: None,
            reference_id: None,
        }
    }

    fn linked(
        key: &str,
        label: &str,
        destination: &str,
    ) -> CommitFooterUpdateWire {
        CommitFooterUpdateWire {
            key: key.to_string(),
            label: label.to_string(),
            destination: Some(destination.to_string()),
            reference_id: None,
        }
    }

    #[test]
    fn parses_plain_prefixed_and_legacy_tags_with_later_duplicates() {
        let parsed = parse_commit_footer(
            "Subject\n\nAGENT=old\nSASE_PLAN=plans/p.md\nSASE_AGENT=new\n",
        );
        assert_eq!(parsed.body, "Subject");
        assert_eq!(parsed.tags.len(), 3);
        assert_eq!(parsed.tags[0].key, "AGENT");
        assert_eq!(parsed.tags[1].label, "plans/p.md");
        assert_eq!(parsed.tags[2].label, "new");
    }

    #[test]
    fn parses_linked_values_and_reference_destinations() {
        let parsed = parse_commit_footer(
            "Subject\n\nSASE_PLAN=[202607/p.md][7]\n\n[7]: https://github.com/o/r/blob/main/202607/p.md",
        );
        assert_eq!(parsed.body, "Subject");
        assert_eq!(parsed.tags[0].label, "202607/p.md");
        assert_eq!(parsed.tags[0].reference_id.as_deref(), Some("7"));
        assert_eq!(
            parsed.tags[0].destination.as_deref(),
            Some("https://github.com/o/r/blob/main/202607/p.md")
        );
    }

    #[test]
    fn ordinary_reference_definitions_and_mid_message_tags_remain_body() {
        let message = "Subject\n\nSASE_PLAN=looks-like-a-tag\n\n[1]: https://example.test\n\nTrailing prose";
        let parsed = parse_commit_footer(message);
        assert!(parsed.tags.is_empty());
        assert_eq!(parsed.body, message);

        let unused = "Subject\n\nSASE_PLAN=plain\n\n[1]: https://example.test";
        assert!(parse_commit_footer(unused).tags.is_empty());
    }

    #[test]
    fn adding_tags_preserves_link_and_definition_below_complete_tag_block() {
        let message = "Subject\n\nSASE_PLAN=[202607/p.md][1]\n\n[1]: https://github.com/o/r/blob/main/202607/p.md";
        let updated = update_commit_footer(
            message,
            &[plain("AGENT", "worker"), plain("MACHINE", "athena")],
            &["AGENT".to_string(), "MACHINE".to_string()],
        );
        assert_eq!(
            updated,
            "Subject\n\nSASE_PLAN=[202607/p.md][1]\nSASE_AGENT=worker\nSASE_MACHINE=athena\n\n[1]: https://github.com/o/r/blob/main/202607/p.md"
        );
    }

    #[test]
    fn replacing_or_removing_linked_tags_cleans_only_owned_definitions() {
        let message = "Subject\n\nSASE_PLAN=[p.md][1]\nSASE_DOC=[d.md][2]\n\n[1]: https://x/p\n[2]: https://x/d";
        let removed = update_commit_footer(message, &[], &["PLAN".to_string()]);
        assert_eq!(
            removed,
            "Subject\n\nSASE_DOC=[d.md][2]\n\n[2]: https://x/d"
        );
        let replaced = update_commit_footer(
            message,
            &[linked("PLAN", "new.md", "https://x/new")],
            &["PLAN".to_string()],
        );
        assert!(replaced.contains("SASE_DOC=[d.md][2]"));
        assert!(replaced.contains("SASE_PLAN=[new.md][3]"));
        assert!(!replaced.contains("https://x/p"));
    }

    #[test]
    fn new_links_allocate_numeric_ids_without_body_or_footer_collisions() {
        let message = "Subject\n\n[1]: https://body.example\n\nSASE_DOC=[old][2]\n\n[2]: https://footer.example";
        let updated = update_commit_footer(
            message,
            &[linked("PLAN", "p.md", "https://plan.example")],
            &[],
        );
        assert!(updated.contains("SASE_PLAN=[p.md][3]"));
        assert!(updated.contains("[3]: https://plan.example"));
    }

    #[test]
    fn shared_targets_reuse_one_definition_deterministically() {
        let updated = update_commit_footer(
            "Subject",
            &[
                linked("PLAN", "p.md", "https://same.example"),
                linked("DOC", "d.md", "https://same.example"),
            ],
            &[],
        );
        assert!(updated.contains("SASE_PLAN=[p.md][1]"));
        assert!(updated.contains("SASE_DOC=[d.md][1]"));
        assert_eq!(updated.matches("[1]: https://same.example").count(), 1);
    }

    #[test]
    fn repeated_updates_are_idempotent_and_preserve_trailing_shape() {
        let updates = [linked("PLAN", "p.md", "https://plan.example")];
        let once = update_commit_footer("Subject\n\n", &updates, &[]);
        let twice = update_commit_footer(&once, &updates, &[]);
        assert_eq!(once, twice);
        assert_eq!(once.matches("SASE_PLAN=").count(), 1);
        assert_eq!(once.matches("[1]: https://plan.example").count(), 1);
    }
}
