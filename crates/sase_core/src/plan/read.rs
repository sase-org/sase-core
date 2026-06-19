//! Read-only plan discovery: scan + parse markdown plan artifacts.
//!
//! Repo plans live under `<sdd_root>/<kind>s/<YYYYMM>/*.md`; local plans live
//! under `<local_dir>/*.md` (flat) and `<local_dir>/<YYYYMM>/*.md` (sharded).
//! Discovery is deliberately resilient: missing root directories yield no
//! plans (not an error), unreadable/non-UTF-8 files are skipped, and malformed
//! or absent frontmatter degrades to a body-derived title and a file-mtime
//! `created_at`. Search, filtering, and ranking build on this layer later.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde_yaml::Value;

use super::wire::{PlanError, PlanWire};

pub const PLAN_READ_WIRE_SCHEMA_VERSION: u64 = 1;

const REPO_SOURCE: &str = "repo";
const LOCAL_SOURCE: &str = "local";
const LOCAL_KIND: &str = "local";

/// Repo `sdd/` plan corpus: `(directory name, kind label)`.
///
/// Directory names are plural (`sdd/tales/…`) while the kind label surfaced on
/// each [`PlanWire`] and accepted by the `kinds` filter is singular (`tale`).
const REPO_PLAN_KINDS: &[(&str, &str)] = &[
    ("tales", "tale"),
    ("epics", "epic"),
    ("legends", "legend"),
    ("myths", "myth"),
    ("research", "research"),
];

/// Canonical `created_at` representation: a naive ISO-8601 timestamp with no
/// timezone marker (frontmatter `create_time` is naive local; mtime is UTC).
const CANONICAL_DT_FORMAT: &str = "%Y-%m-%dT%H:%M:%S";

/// Discover plans from a repo `sdd/` tree and/or the local archive.
///
/// Either root may be `None` (so callers can scope by `--source`). `kinds`
/// filters the repo corpus by kind label; `None` selects every kind. Local
/// plans are not affected by `kinds` (they have no kind directory). Results are
/// returned in a deterministic order: repo plans first (grouped by the
/// [`REPO_PLAN_KINDS`] order, then shard, then filename), followed by local
/// plans (flat files, then sharded files).
pub fn read_plans(
    repo_sdd_root: Option<&Path>,
    local_plans_dir: Option<&Path>,
    kinds: Option<&[String]>,
) -> Result<Vec<PlanWire>, PlanError> {
    let mut plans = Vec::new();
    if let Some(root) = repo_sdd_root {
        read_repo_plans(root, kinds, &mut plans)?;
    }
    if let Some(dir) = local_plans_dir {
        read_local_plans(dir, &mut plans)?;
    }
    Ok(plans)
}

fn read_repo_plans(
    sdd_root: &Path,
    kinds: Option<&[String]>,
    out: &mut Vec<PlanWire>,
) -> Result<(), PlanError> {
    for (dir_name, kind) in REPO_PLAN_KINDS {
        if !kind_selected(kinds, kind) {
            continue;
        }
        let kind_dir = sdd_root.join(dir_name);
        if !kind_dir.is_dir() {
            continue;
        }
        for shard in sorted_subdirs(&kind_dir)? {
            for file in sorted_markdown_files(&shard)? {
                if let Some(plan) =
                    build_plan(REPO_SOURCE, kind, sdd_root, &file)
                {
                    out.push(plan);
                }
            }
        }
    }
    Ok(())
}

fn read_local_plans(
    local_dir: &Path,
    out: &mut Vec<PlanWire>,
) -> Result<(), PlanError> {
    if !local_dir.is_dir() {
        return Ok(());
    }
    for file in sorted_markdown_files(local_dir)? {
        if let Some(plan) =
            build_plan(LOCAL_SOURCE, LOCAL_KIND, local_dir, &file)
        {
            out.push(plan);
        }
    }
    for shard in sorted_subdirs(local_dir)? {
        for file in sorted_markdown_files(&shard)? {
            if let Some(plan) =
                build_plan(LOCAL_SOURCE, LOCAL_KIND, local_dir, &file)
            {
                out.push(plan);
            }
        }
    }
    Ok(())
}

fn kind_selected(kinds: Option<&[String]>, kind: &str) -> bool {
    match kinds {
        None => true,
        Some(values) => values.iter().any(|value| value == kind),
    }
}

/// Build a [`PlanWire`] from one markdown file. Returns `None` when the file
/// cannot be read as UTF-8 text (skipped, not fatal).
fn build_plan(
    source: &str,
    kind: &str,
    root: &Path,
    file: &Path,
) -> Option<PlanWire> {
    let content = fs::read_to_string(file).ok()?;
    let name = file
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_default()
        .to_string();
    let (frontmatter_text, body) = split_frontmatter(&content);
    let frontmatter = frontmatter_text
        .as_deref()
        .map(parse_frontmatter_map)
        .unwrap_or_default();
    let title = derive_title(&body).unwrap_or_else(|| humanize(&name));
    let status = frontmatter.get("status").cloned().unwrap_or_default();
    let prompt_link = frontmatter.get("prompt").cloned().unwrap_or_default();
    let created_at = frontmatter
        .get("create_time")
        .and_then(|raw| normalize_datetime(raw))
        .or_else(|| file_mtime_string(file))
        .unwrap_or_default();
    let summary = derive_summary(&body);
    Some(PlanWire {
        source: source.to_string(),
        kind: kind.to_string(),
        path: file.to_string_lossy().into_owned(),
        relpath: relpath_from(root, file),
        name,
        title,
        status,
        created_at,
        prompt_link,
        summary,
        body,
        frontmatter,
    })
}

/// Split a document into its YAML frontmatter text (if a complete `---` block
/// is present) and the remaining body. Line-oriented and `\r`-tolerant, mirror
/// of the editor module's `extract_frontmatter`.
fn split_frontmatter(content: &str) -> (Option<String>, String) {
    let mut lines = content.split_inclusive('\n');
    let Some(first) = lines.next() else {
        return (None, String::new());
    };
    if trim_line(first) != "---" {
        return (None, content.to_string());
    }
    let mut frontmatter = String::new();
    let mut consumed = first.len();
    for line in lines {
        consumed += line.len();
        if trim_line(line) == "---" {
            return (Some(frontmatter), content[consumed..].to_string());
        }
        frontmatter.push_str(line);
    }
    // No closing delimiter: treat the whole document as body.
    (None, content.to_string())
}

fn trim_line(line: &str) -> &str {
    line.trim_end_matches('\n').trim_end_matches('\r')
}

fn parse_frontmatter_map(text: &str) -> BTreeMap<String, String> {
    let Ok(value) = serde_yaml::from_str::<Value>(text) else {
        return BTreeMap::new();
    };
    let Some(mapping) = value.as_mapping() else {
        return BTreeMap::new();
    };
    let mut out = BTreeMap::new();
    for (key, value) in mapping {
        if let Some(key) = key.as_str() {
            out.insert(key.to_string(), yaml_value_to_string(value));
        }
    }
    out
}

/// Flatten a YAML value to a single searchable/displayable string.
fn yaml_value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(string) => string.clone(),
        Value::Sequence(items) => items
            .iter()
            .map(yaml_value_to_string)
            .collect::<Vec<_>>()
            .join(", "),
        Value::Mapping(mapping) => mapping
            .iter()
            .map(|(key, value)| {
                format!(
                    "{}: {}",
                    yaml_value_to_string(key),
                    yaml_value_to_string(value)
                )
            })
            .collect::<Vec<_>>()
            .join(", "),
        Value::Tagged(tagged) => yaml_value_to_string(&tagged.value),
    }
}

/// First markdown H1 (`# …`) in the body, if any.
fn derive_title(body: &str) -> Option<String> {
    body.lines().find_map(|line| {
        let title = line.trim().strip_prefix("# ")?.trim();
        (!title.is_empty()).then(|| title.to_string())
    })
}

/// First non-empty, non-heading body line, used as a display summary.
fn derive_summary(body: &str) -> String {
    body.lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .unwrap_or_default()
        .to_string()
}

/// Humanize a filename stem: split on `_`/`-` and title-case each word.
fn humanize(name: &str) -> String {
    let words: Vec<String> = name
        .split(['_', '-'])
        .filter(|word| !word.is_empty())
        .map(capitalize_first)
        .collect();
    if words.is_empty() {
        name.to_string()
    } else {
        words.join(" ")
    }
}

fn capitalize_first(word: &str) -> String {
    let mut chars = word.chars();
    match chars.next() {
        Some(first) => {
            first.to_uppercase().collect::<String>() + chars.as_str()
        }
        None => String::new(),
    }
}

/// Parse a frontmatter `create_time` in any of its observed spellings and
/// re-emit it in [`CANONICAL_DT_FORMAT`]. Returns `None` on an unparseable
/// value so the caller can fall back to file mtime.
fn normalize_datetime(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    if let Ok(datetime) = DateTime::parse_from_rfc3339(raw) {
        return Some(
            datetime.naive_utc().format(CANONICAL_DT_FORMAT).to_string(),
        );
    }
    for format in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
    ] {
        if let Ok(datetime) = NaiveDateTime::parse_from_str(raw, format) {
            return Some(datetime.format(CANONICAL_DT_FORMAT).to_string());
        }
    }
    if let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        return Some(
            date.and_hms_opt(0, 0, 0)?
                .format(CANONICAL_DT_FORMAT)
                .to_string(),
        );
    }
    None
}

fn file_mtime_string(path: &Path) -> Option<String> {
    let modified = fs::metadata(path).ok()?.modified().ok()?;
    let datetime: DateTime<Utc> = modified.into();
    Some(datetime.naive_utc().format(CANONICAL_DT_FORMAT).to_string())
}

fn relpath_from(root: &Path, file: &Path) -> String {
    file.strip_prefix(root)
        .unwrap_or(file)
        .to_string_lossy()
        .replace('\\', "/")
}

fn sorted_subdirs(dir: &Path) -> Result<Vec<PathBuf>, PlanError> {
    Ok(read_dir_sorted(dir)?
        .into_iter()
        .filter(|path| path.is_dir())
        .collect())
}

fn sorted_markdown_files(dir: &Path) -> Result<Vec<PathBuf>, PlanError> {
    Ok(read_dir_sorted(dir)?
        .into_iter()
        .filter(|path| {
            path.is_file()
                && path.extension().and_then(|ext| ext.to_str()) == Some("md")
        })
        .collect())
}

fn read_dir_sorted(dir: &Path) -> Result<Vec<PathBuf>, PlanError> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(dir)? {
        paths.push(entry?.path());
    }
    paths.sort();
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    use tempfile::tempdir;

    fn write(path: &Path, content: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, content).unwrap();
    }

    fn plan_with<'a>(plans: &'a [PlanWire], relpath: &str) -> &'a PlanWire {
        plans
            .iter()
            .find(|plan| plan.relpath == relpath)
            .unwrap_or_else(|| panic!("missing plan {relpath}"))
    }

    #[test]
    fn discovers_every_repo_kind_with_labels_and_relpaths() {
        let temp = tempdir().unwrap();
        let sdd = temp.path().join("sdd");
        for (dir_name, _) in REPO_PLAN_KINDS {
            write(
                &sdd.join(dir_name).join("202606").join("entry.md"),
                &format!("# {dir_name} entry\n\nBody.\n"),
            );
        }

        let plans = read_plans(Some(&sdd), None, None).unwrap();

        assert_eq!(plans.len(), REPO_PLAN_KINDS.len());
        for (dir_name, kind) in REPO_PLAN_KINDS {
            let relpath = format!("{dir_name}/202606/entry.md");
            let plan = plan_with(&plans, &relpath);
            assert_eq!(plan.source, "repo");
            assert_eq!(plan.kind, *kind);
            assert_eq!(plan.name, "entry");
            assert!(plan.path.ends_with(&relpath));
        }
        // Deterministic order follows REPO_PLAN_KINDS.
        let kinds: Vec<&str> =
            plans.iter().map(|plan| plan.kind.as_str()).collect();
        assert_eq!(kinds, ["tale", "epic", "legend", "myth", "research"]);
    }

    #[test]
    fn discovers_local_flat_and_sharded_layout() {
        let temp = tempdir().unwrap();
        let local = temp.path().join("plans");
        write(&local.join("flat_plan.md"), "# Flat plan\n\nBody.\n");
        write(
            &local.join("202604").join("sharded_plan.md"),
            "# Sharded plan\n\nBody.\n",
        );

        let plans = read_plans(None, Some(&local), None).unwrap();

        assert_eq!(plans.len(), 2);
        // Flat files are surfaced before sharded ones.
        assert_eq!(plans[0].relpath, "flat_plan.md");
        assert_eq!(plans[1].relpath, "202604/sharded_plan.md");
        for plan in &plans {
            assert_eq!(plan.source, "local");
            assert_eq!(plan.kind, "local");
        }
    }

    #[test]
    fn repo_plans_precede_local_plans() {
        let temp = tempdir().unwrap();
        let sdd = temp.path().join("sdd");
        let local = temp.path().join("plans");
        write(
            &sdd.join("tales").join("202606").join("repo.md"),
            "# Repo\n",
        );
        write(&local.join("local.md"), "# Local\n");

        let plans = read_plans(Some(&sdd), Some(&local), None).unwrap();

        let sources: Vec<&str> =
            plans.iter().map(|plan| plan.source.as_str()).collect();
        assert_eq!(sources, ["repo", "local"]);
    }

    #[test]
    fn parses_frontmatter_fields_and_normalizes_created_at() {
        let temp = tempdir().unwrap();
        let sdd = temp.path().join("sdd");
        write(
            &sdd.join("epics").join("202606").join("auth.md"),
            "---\n\
             create_time: 2026-06-18 21:29:20\n\
             status: wip\n\
             prompt: sdd/prompts/202606/auth.md\n\
             tags:\n  - backend\n  - auth\n\
             ---\n\
             # Unified auth\n\nFirst paragraph.\n",
        );

        let plans = read_plans(Some(&sdd), None, None).unwrap();

        assert_eq!(plans.len(), 1);
        let plan = &plans[0];
        assert_eq!(plan.title, "Unified auth");
        assert_eq!(plan.status, "wip");
        assert_eq!(plan.prompt_link, "sdd/prompts/202606/auth.md");
        assert_eq!(plan.created_at, "2026-06-18T21:29:20");
        assert_eq!(plan.summary, "First paragraph.");
        assert_eq!(plan.frontmatter.get("status").unwrap(), "wip");
        assert_eq!(plan.frontmatter.get("tags").unwrap(), "backend, auth");
        // Frontmatter is stripped from the body.
        assert!(!plan.body.contains("create_time"));
        assert!(plan.body.starts_with("# Unified auth"));
    }

    #[test]
    fn derives_title_from_h1_then_humanized_name() {
        let temp = tempdir().unwrap();
        let sdd = temp.path().join("sdd");
        write(
            &sdd.join("tales").join("202606").join("with_h1.md"),
            "Intro line\n\n## Section\n\n# Real Title\n",
        );
        write(
            &sdd.join("tales").join("202606").join("no_h1_here.md"),
            "Just prose, no heading at all.\n",
        );

        let plans = read_plans(Some(&sdd), None, None).unwrap();

        assert_eq!(
            plan_with(&plans, "tales/202606/with_h1.md").title,
            "Real Title"
        );
        assert_eq!(
            plan_with(&plans, "tales/202606/no_h1_here.md").title,
            "No H1 Here"
        );
    }

    #[test]
    fn falls_back_to_mtime_when_create_time_absent() {
        let temp = tempdir().unwrap();
        let sdd = temp.path().join("sdd");
        write(
            &sdd.join("tales").join("202606").join("no_time.md"),
            "# No time\n\nBody.\n",
        );

        let plans = read_plans(Some(&sdd), None, None).unwrap();

        assert_eq!(plans.len(), 1);
        // mtime fallback yields a canonical, parseable timestamp.
        let created_at = &plans[0].created_at;
        assert!(!created_at.is_empty());
        assert!(
            NaiveDateTime::parse_from_str(created_at, CANONICAL_DT_FORMAT)
                .is_ok(),
            "created_at not canonical: {created_at}"
        );
    }

    #[test]
    fn tolerates_malformed_and_absent_frontmatter() {
        let temp = tempdir().unwrap();
        let sdd = temp.path().join("sdd");
        // Malformed YAML inside a well-formed delimiter block.
        write(
            &sdd.join("tales").join("202606").join("broken.md"),
            "---\nstatus: : : oops\n  bad indent\n---\n# Broken plan\n\nBody.\n",
        );
        // No frontmatter delimiters at all.
        write(
            &sdd.join("tales").join("202606").join("bare.md"),
            "# Bare plan\n\nBody.\n",
        );

        let plans = read_plans(Some(&sdd), None, None).unwrap();

        let broken = plan_with(&plans, "tales/202606/broken.md");
        assert_eq!(broken.title, "Broken plan");
        assert!(broken.frontmatter.is_empty());
        assert_eq!(broken.status, "");

        let bare = plan_with(&plans, "tales/202606/bare.md");
        assert_eq!(bare.title, "Bare plan");
        assert!(bare.frontmatter.is_empty());
    }

    #[test]
    fn filters_repo_corpus_by_kind() {
        let temp = tempdir().unwrap();
        let sdd = temp.path().join("sdd");
        write(&sdd.join("tales").join("202606").join("a.md"), "# A\n");
        write(&sdd.join("epics").join("202606").join("b.md"), "# B\n");
        write(&sdd.join("myths").join("202606").join("c.md"), "# C\n");

        let kinds = vec!["epic".to_string(), "myth".to_string()];
        let plans = read_plans(Some(&sdd), None, Some(&kinds)).unwrap();

        let kinds: Vec<&str> =
            plans.iter().map(|plan| plan.kind.as_str()).collect();
        assert_eq!(kinds, ["epic", "myth"]);
    }

    #[test]
    fn kind_filter_does_not_constrain_local_plans() {
        let temp = tempdir().unwrap();
        let sdd = temp.path().join("sdd");
        let local = temp.path().join("plans");
        write(&sdd.join("epics").join("202606").join("e.md"), "# E\n");
        write(&local.join("l.md"), "# L\n");

        let kinds = vec!["tale".to_string()];
        let plans = read_plans(Some(&sdd), Some(&local), Some(&kinds)).unwrap();

        // No repo tales exist, but the local plan is unaffected by --kind.
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].source, "local");
    }

    #[test]
    fn empty_kind_filter_selects_no_repo_plans() {
        let temp = tempdir().unwrap();
        let sdd = temp.path().join("sdd");
        write(&sdd.join("tales").join("202606").join("a.md"), "# A\n");

        let plans = read_plans(Some(&sdd), None, Some(&[])).unwrap();

        assert!(plans.is_empty());
    }

    #[test]
    fn missing_roots_yield_no_plans_without_error() {
        let temp = tempdir().unwrap();
        let missing_sdd = temp.path().join("nope/sdd");
        let missing_local = temp.path().join("nope/plans");

        let plans =
            read_plans(Some(&missing_sdd), Some(&missing_local), None).unwrap();

        assert!(plans.is_empty());
        // Passing no roots at all is also valid and empty.
        assert!(read_plans(None, None, None).unwrap().is_empty());
    }

    #[test]
    fn ignores_non_markdown_files() {
        let temp = tempdir().unwrap();
        let sdd = temp.path().join("sdd");
        write(
            &sdd.join("tales").join("202606").join("keep.md"),
            "# Keep\n",
        );
        write(&sdd.join("tales").join("202606").join("skip.txt"), "nope\n");
        write(&sdd.join("tales").join("202606").join("notes"), "nope\n");

        let plans = read_plans(Some(&sdd), None, None).unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].name, "keep");
    }

    #[test]
    fn sorts_files_within_a_shard() {
        let temp = tempdir().unwrap();
        let sdd = temp.path().join("sdd");
        write(&sdd.join("tales").join("202606").join("zebra.md"), "# Z\n");
        write(&sdd.join("tales").join("202606").join("alpha.md"), "# A\n");

        let plans = read_plans(Some(&sdd), None, None).unwrap();

        let names: Vec<&str> =
            plans.iter().map(|plan| plan.name.as_str()).collect();
        assert_eq!(names, ["alpha", "zebra"]);
    }

    #[test]
    fn accepts_rfc3339_and_date_only_create_time() {
        let temp = tempdir().unwrap();
        let sdd = temp.path().join("sdd");
        write(
            &sdd.join("epics").join("202606").join("aware.md"),
            "---\ncreate_time: 2026-06-18T21:29:20Z\n---\n# Aware\n",
        );
        write(
            &sdd.join("epics").join("202606").join("dateonly.md"),
            "---\ncreate_time: 2026-06-18\n---\n# Date only\n",
        );

        let plans = read_plans(Some(&sdd), None, None).unwrap();

        assert_eq!(
            plan_with(&plans, "epics/202606/aware.md").created_at,
            "2026-06-18T21:29:20"
        );
        assert_eq!(
            plan_with(&plans, "epics/202606/dateonly.md").created_at,
            "2026-06-18T00:00:00"
        );
    }
}
