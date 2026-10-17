//! Read-only agent directory from agents-sidecar pages and the run index.
//!
//! Sidecar pages at HEAD supply name, family, SHA, and model metadata.
//! This machine's run index fills unpublished names after globalization
//! with `local_agent_name_prefix`. A published page always wins.

use std::collections::BTreeMap;
use std::path::Path;

use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::cache;
use super::git::{self, ResolvedRepo};
use super::wire::{
    ProjectStatsAgentSidecarSpecWire, ProjectStatsBudgetWire,
    ProjectStatsIssueWire, ProjectStatsRepoKindWire, ProjectStatsRepoSpecWire,
    ProjectStatsRequestWire,
};

const DIRECTORY_CACHE_SCHEMA_VERSION: u32 = 1;
const AGENTS_PREFIX: &str = "agents/";

/// Member metadata distinguishes a missing directory row from an empty model.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DirectoryMember {
    pub name: String,
    pub family: Option<String>,
    pub role: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub model_metadata_present: bool,
}

/// Name, family, and commit-SHA lookup indexes for attribution.
#[derive(Debug, Clone, Default)]
pub(crate) struct Directory {
    pub by_name: BTreeMap<String, DirectoryMember>,
    pub family_members: BTreeMap<String, Vec<String>>,
    pub by_sha: BTreeMap<String, String>,
}

/// Directory maps plus the fingerprint inputs and source issues.
#[derive(Debug, Clone, Default)]
pub(crate) struct DirectorySnapshot {
    pub directory: Directory,
    pub source_tokens: Vec<String>,
    pub issues: Vec<ProjectStatsIssueWire>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DirectoryCacheEntry {
    schema_version: u32,
    key: String,
    path: String,
    head: String,
    members: Vec<DirectoryMember>,
    sha_to_name: BTreeMap<String, String>,
    malformed_pages: u64,
}

struct SidecarPages {
    members: Vec<DirectoryMember>,
    sha_to_name: BTreeMap<String, String>,
    malformed_pages: u64,
    truncated: bool,
}

/// Load the query-time directory from every configured sidecar and the
/// optional local run index.
pub(crate) fn load(request: &ProjectStatsRequestWire) -> DirectorySnapshot {
    let mut snapshot = DirectorySnapshot::default();
    for sidecar in &request.agents_sidecars {
        load_sidecar(request, sidecar, &mut snapshot);
    }
    if let Some(path) = request
        .run_index_path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
    {
        load_index(path, &request.local_agent_name_prefix, &mut snapshot);
    }
    snapshot.directory.rebuild_families();
    snapshot
}

fn load_sidecar(
    request: &ProjectStatsRequestWire,
    sidecar: &ProjectStatsAgentSidecarSpecWire,
    snapshot: &mut DirectorySnapshot,
) {
    let spec = sidecar_spec(sidecar);
    match git::resolve_repo(&spec, &request.budget) {
        Err(issue) => {
            snapshot.issues.push(ProjectStatsIssueWire {
                project: Some(sidecar.project_key.clone()),
                repo: Some(sidecar.path.clone()),
                code: issue.code,
                message: issue.message,
                hint: Some(
                    "agent directory skipped this sidecar; unpublished pages become ?"
                        .to_owned(),
                ),
            });
            snapshot
                .source_tokens
                .push(format!("sidecar:{}:unavailable", sidecar.project_key));
        }
        Ok(resolved) => {
            snapshot.source_tokens.push(format!(
                "sidecar:{}:{}",
                sidecar.project_key, resolved.tip
            ));
            if resolved.tip.is_empty() {
                return;
            }
            let pages = load_sidecar_pages(request, sidecar, &resolved);
            if pages.truncated {
                snapshot.issues.push(ProjectStatsIssueWire {
                    project: Some(sidecar.project_key.clone()),
                    repo: Some(sidecar.path.clone()),
                    code: "directory_truncated".to_owned(),
                    message: "agent sidecar page walk exceeded its budget"
                        .to_owned(),
                    hint: None,
                });
            }
            if pages.malformed_pages > 0 {
                snapshot.issues.push(ProjectStatsIssueWire {
                    project: Some(sidecar.project_key.clone()),
                    repo: Some(sidecar.path.clone()),
                    code: "directory_malformed_pages".to_owned(),
                    message: format!(
                        "skipped {} malformed agent page files",
                        pages.malformed_pages
                    ),
                    hint: None,
                });
            }
            for member in pages.members {
                snapshot.directory.insert_published(member);
            }
            for (sha, name) in pages.sha_to_name {
                snapshot.directory.by_sha.entry(sha).or_insert(name);
            }
        }
    }
}

fn load_sidecar_pages(
    request: &ProjectStatsRequestWire,
    sidecar: &ProjectStatsAgentSidecarSpecWire,
    resolved: &ResolvedRepo,
) -> SidecarPages {
    let key = sidecar_cache_key(sidecar, resolved);
    let cache_path = (!request.cache_dir.is_empty())
        .then(|| cache::kind_path(&request.cache_dir, "directory", &key));
    if let Some(path) = cache_path.as_deref() {
        if let Some(entry) = cache::load_json::<DirectoryCacheEntry>(path) {
            if entry.schema_version == DIRECTORY_CACHE_SCHEMA_VERSION
                && entry.key == key
                && entry.head == resolved.tip
                && entry.path == resolved.canonical_path
            {
                return SidecarPages {
                    members: entry.members,
                    sha_to_name: entry.sha_to_name,
                    malformed_pages: entry.malformed_pages,
                    truncated: false,
                };
            }
        }
    }
    let pages = walk_sidecar_pages(resolved, &request.budget);
    if let Some(path) = cache_path.as_deref() {
        if !pages.truncated {
            cache::persist_json(
                path,
                &DirectoryCacheEntry {
                    schema_version: DIRECTORY_CACHE_SCHEMA_VERSION,
                    key,
                    path: resolved.canonical_path.clone(),
                    head: resolved.tip.clone(),
                    members: pages.members.clone(),
                    sha_to_name: pages.sha_to_name.clone(),
                    malformed_pages: pages.malformed_pages,
                },
            );
        }
    }
    pages
}

fn walk_sidecar_pages(
    resolved: &ResolvedRepo,
    budget: &ProjectStatsBudgetWire,
) -> SidecarPages {
    let repo = Path::new(&resolved.canonical_path);
    let listed = match git::run_output(
        repo,
        &["ls-tree", "-r", "-z", &resolved.tip, "--", "agents"],
        budget,
    ) {
        Ok(result) => match git::output_text(result) {
            Ok(text) => text,
            Err(_) => {
                return SidecarPages {
                    truncated: true,
                    ..SidecarPages::empty()
                };
            }
        },
        Err(_) => {
            return SidecarPages {
                truncated: true,
                ..SidecarPages::empty()
            };
        }
    };
    let entries = parse_ls_tree_z(listed.as_bytes());
    let mut by_page: BTreeMap<String, PageFiles> = BTreeMap::new();
    for (oid, path) in entries {
        let Some(page) = page_name_from_path(&path) else {
            continue;
        };
        let files = by_page.entry(page).or_default();
        if path.ends_with("/meta.json") {
            files.meta_oid = Some(oid);
        } else if path.ends_with("/commits.json") {
            files.commits_oid = Some(oid);
        }
    }
    let mut oids = Vec::new();
    let mut order = Vec::new();
    for (page, files) in &by_page {
        if let Some(oid) = &files.meta_oid {
            order.push((page.clone(), "meta"));
            oids.push(oid.clone());
        }
        if let Some(oid) = &files.commits_oid {
            order.push((page.clone(), "commits"));
            oids.push(oid.clone());
        }
    }
    let blobs = match git::read_blobs(repo, &oids, budget) {
        Ok(blobs) => blobs,
        Err(_) => {
            return SidecarPages {
                truncated: true,
                ..SidecarPages::empty()
            };
        }
    };
    let mut contents: BTreeMap<String, PageContents> = BTreeMap::new();
    let mut malformed_pages = 0;
    for ((page, kind), blob) in order.into_iter().zip(blobs) {
        let Some(bytes) = blob else {
            malformed_pages += 1;
            continue;
        };
        let Ok(text) = String::from_utf8(bytes) else {
            malformed_pages += 1;
            continue;
        };
        let entry = contents.entry(page).or_default();
        match kind {
            "meta" => entry.meta = Some(text),
            _ => entry.commits = Some(text),
        }
    }
    let mut members = Vec::new();
    let mut sha_to_name = BTreeMap::new();
    for (page, files) in contents {
        match parse_page(&page, files.meta.as_deref(), files.commits.as_deref())
        {
            None => malformed_pages += 1,
            Some((member, shas)) => {
                for sha in shas {
                    sha_to_name.entry(sha).or_insert(member.name.clone());
                }
                members.push(member);
            }
        }
    }
    SidecarPages {
        members,
        sha_to_name,
        malformed_pages,
        truncated: false,
    }
}

#[derive(Default)]
struct PageFiles {
    meta_oid: Option<String>,
    commits_oid: Option<String>,
}

#[derive(Default)]
struct PageContents {
    meta: Option<String>,
    commits: Option<String>,
}

impl SidecarPages {
    fn empty() -> Self {
        Self {
            members: Vec::new(),
            sha_to_name: BTreeMap::new(),
            malformed_pages: 0,
            truncated: false,
        }
    }
}

fn parse_ls_tree_z(bytes: &[u8]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for record in bytes.split(|byte| *byte == 0) {
        if record.is_empty() {
            continue;
        }
        let Ok(text) = std::str::from_utf8(record) else {
            continue;
        };
        let Some((meta, path)) = text.split_once('\t') else {
            continue;
        };
        let mut fields = meta.split_whitespace();
        let _mode = fields.next();
        let kind = fields.next();
        let oid = fields.next();
        if kind != Some("blob") {
            continue;
        }
        let Some(oid) = oid.filter(|value| git::valid_oid(value)) else {
            continue;
        };
        if !path.starts_with(AGENTS_PREFIX) {
            continue;
        }
        if !path.ends_with("/meta.json") && !path.ends_with("/commits.json") {
            continue;
        }
        out.push((oid.to_ascii_lowercase(), path.to_owned()));
    }
    out
}

fn page_name_from_path(path: &str) -> Option<String> {
    let rest = path.strip_prefix(AGENTS_PREFIX)?;
    let (page, file) = rest.rsplit_once('/')?;
    if file != "meta.json" && file != "commits.json" {
        return None;
    }
    if page.is_empty() {
        return None;
    }
    Some(page.to_owned())
}

fn parse_page(
    page: &str,
    meta_text: Option<&str>,
    commits_text: Option<&str>,
) -> Option<(DirectoryMember, Vec<String>)> {
    let mut member = DirectoryMember {
        name: page.to_owned(),
        ..DirectoryMember::default()
    };
    if let Some(text) = meta_text {
        let value: Value = serde_json::from_str(text).ok()?;
        apply_meta(&mut member, &value);
    }
    let mut shas = Vec::new();
    if let Some(text) = commits_text {
        match serde_json::from_str::<Value>(text) {
            Ok(value) => shas = parse_commits(&value),
            Err(_) => return None,
        }
    }
    if meta_text.is_none() && commits_text.is_none() {
        return None;
    }
    Some((member, shas))
}

fn apply_meta(member: &mut DirectoryMember, value: &Value) {
    if let Some(name) = value.get("global_name").and_then(Value::as_str) {
        let name = name.trim();
        if !name.is_empty() {
            member.name = name.to_owned();
        }
    }
    let Some(metadata) = value.get("metadata") else {
        return;
    };
    member.model_metadata_present = metadata.is_object();
    member.provider = string_field(metadata, "llm_provider");
    member.model = string_field(metadata, "model");
    member.family = string_field(metadata, "agent_family")
        .or_else(|| string_field(metadata, "agent_session"));
    member.role = string_field(metadata, "agent_family_role")
        .or_else(|| string_field(metadata, "agent_session_role"))
        .or_else(|| string_field(metadata, "role_suffix"));
}

fn parse_commits(value: &Value) -> Vec<String> {
    let Some(rows) = value.get("commits").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut shas = Vec::new();
    for row in rows {
        let Some(sha) = row.get("sha").and_then(Value::as_str) else {
            continue;
        };
        let sha = sha.trim().to_ascii_lowercase();
        if sha.len() < 7
            || sha.len() > 64
            || !sha.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            continue;
        }
        shas.push(sha);
    }
    shas
}

fn string_field(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn load_index(path: &str, prefix: &str, snapshot: &mut DirectorySnapshot) {
    snapshot.source_tokens.push(index_token(path));
    let index_path = Path::new(path);
    if !index_path.is_file() {
        snapshot.issues.push(ProjectStatsIssueWire {
            code: "index_missing".to_owned(),
            message: format!("run index is missing: {path}"),
            hint: Some(
                "index-only agent names will not resolve until the index exists"
                    .to_owned(),
            ),
            ..ProjectStatsIssueWire::default()
        });
        return;
    }
    let conn = match Connection::open_with_flags(
        index_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Ok(conn) => conn,
        Err(error) => {
            snapshot.issues.push(ProjectStatsIssueWire {
                code: "index_unavailable".to_owned(),
                message: format!("failed to open run index: {error}"),
                ..ProjectStatsIssueWire::default()
            });
            return;
        }
    };
    let mut statement = match conn.prepare(
        r#"
        SELECT agent_name, model, llm_provider, timestamp
        FROM agent_artifacts
        WHERE agent_name IS NOT NULL AND trim(agent_name) != ''
        ORDER BY timestamp ASC
        "#,
    ) {
        Ok(statement) => statement,
        Err(error) => {
            snapshot.issues.push(ProjectStatsIssueWire {
                code: "index_unavailable".to_owned(),
                message: format!("failed to read run index agents: {error}"),
                ..ProjectStatsIssueWire::default()
            });
            return;
        }
    };
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    });
    let Ok(rows) = rows else {
        return;
    };
    let mut latest: BTreeMap<String, DirectoryMember> = BTreeMap::new();
    for row in rows.flatten() {
        let (name, model, provider) = row;
        let global = globalize_name(name.trim(), prefix);
        if global.is_empty() {
            continue;
        }
        let model = model
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let provider = provider
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        latest.insert(
            global.clone(),
            DirectoryMember {
                name: global,
                family: None,
                role: None,
                provider,
                model,
                model_metadata_present: true,
            },
        );
    }
    for member in latest.into_values() {
        snapshot.directory.insert_index(member);
    }
}

fn globalize_name(name: &str, prefix: &str) -> String {
    let prefix = prefix.trim();
    if prefix.is_empty() || name.starts_with(prefix) {
        name.to_owned()
    } else {
        format!("{prefix}{name}")
    }
}

fn index_token(path: &str) -> String {
    let metadata = std::fs::metadata(path).ok();
    let mtime = metadata
        .as_ref()
        .and_then(|meta| meta.modified().ok())
        .and_then(|modified| {
            modified
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .map(|age| age.as_secs())
        })
        .unwrap_or(0);
    let length = metadata.map(|meta| meta.len()).unwrap_or(0);
    format!("index:{path}:{mtime}:{length}")
}

fn sidecar_spec(
    sidecar: &ProjectStatsAgentSidecarSpecWire,
) -> ProjectStatsRepoSpecWire {
    ProjectStatsRepoSpecWire {
        id: format!("agents-{}", sidecar.project_key),
        name: sidecar.project_key.clone(),
        kind: ProjectStatsRepoKindWire::Sidecar,
        path: sidecar.path.clone(),
        identity: String::new(),
        ref_candidates: vec!["HEAD".to_owned()],
    }
}

fn sidecar_cache_key(
    sidecar: &ProjectStatsAgentSidecarSpecWire,
    resolved: &ResolvedRepo,
) -> String {
    let canonical = serde_json::json!({
        "kind": "directory",
        "project_key": sidecar.project_key,
        "path": resolved.canonical_path,
        "common_dir": resolved.common_dir,
        "schema": DIRECTORY_CACHE_SCHEMA_VERSION,
    });
    let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
    hex::encode(Sha256::digest(bytes))
}

impl Directory {
    fn insert_published(&mut self, member: DirectoryMember) {
        if self.by_name.contains_key(&member.name) {
            return;
        }
        self.by_name.insert(member.name.clone(), member);
    }

    fn insert_index(&mut self, member: DirectoryMember) {
        if self.by_name.contains_key(&member.name) {
            return;
        }
        self.by_name.insert(member.name.clone(), member);
    }

    fn rebuild_families(&mut self) {
        self.family_members.clear();
        let names: Vec<String> = self.by_name.keys().cloned().collect();
        for name in names {
            let family = self
                .by_name
                .get(&name)
                .and_then(|member| member.family.clone());
            if let Some(family) = family {
                push_unique(&mut self.family_members, &family, &name);
            }
            if let Some((stem, _)) = name.rsplit_once("--") {
                if !stem.is_empty() {
                    push_unique(&mut self.family_members, stem, &name);
                }
            }
        }
    }
}

fn push_unique(map: &mut BTreeMap<String, Vec<String>>, key: &str, name: &str) {
    let members = map.entry(key.to_owned()).or_default();
    if !members.iter().any(|existing| existing == name) {
        members.push(name.to_owned());
    }
}
