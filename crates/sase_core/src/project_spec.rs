//! Project spec path and lifecycle helpers.
//!
//! Canonical project spec files use the `.sase` extension. Legacy `.gp`
//! files are still recognized during migration.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

pub const PROJECT_SPEC_EXTENSION: &str = ".sase";
pub const LEGACY_PROJECT_SPEC_EXTENSION: &str = ".gp";
pub const PROJECT_SPEC_ARCHIVE_SUFFIX: &str = "-archive";
pub const PROJECT_LIFECYCLE_WIRE_SCHEMA_VERSION: u32 = 2;

const PROJECT_ALIASES_PREFIX: &str = "PROJECT_ALIASES:";
const PROJECT_NAME_PREFIX: &str = "PROJECT_NAME:";
const PROJECT_STATE_PREFIX: &str = "PROJECT_STATE:";
const WORKSPACE_DIR_PREFIX: &str = "WORKSPACE_DIR:";
const RUNNING_PREFIX: &str = "RUNNING:";
const NAME_PREFIX: &str = "NAME:";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProjectLifecycleState {
    Active,
    Inactive,
    Sibling,
}

impl ProjectLifecycleState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Inactive => "inactive",
            Self::Sibling => "sibling",
        }
    }

    pub fn parse_target(value: &str) -> Result<Self, ProjectLifecycleError> {
        match value.trim() {
            "active" => Ok(Self::Active),
            "inactive" | "archived" | "closed" => Ok(Self::Inactive),
            "sibling" => Ok(Self::Sibling),
            other => {
                Err(ProjectLifecycleError::InvalidState(other.to_string()))
            }
        }
    }

    pub fn is_active(self) -> bool {
        self == Self::Active
    }

    pub fn is_inactive(self) -> bool {
        self == Self::Inactive
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectLifecycleError {
    InvalidProjectAlias(String),
    InvalidProjectName(String),
    InvalidState(String),
}

impl fmt::Display for ProjectLifecycleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidProjectAlias(message) => write!(f, "{message}"),
            Self::InvalidProjectName(message) => write!(f, "{message}"),
            Self::InvalidState(value) => write!(
                f,
                "invalid project lifecycle state {value:?}; expected active, inactive, or sibling"
            ),
        }
    }
}

impl std::error::Error for ProjectLifecycleError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectLifecycleWire {
    pub schema_version: u32,
    pub state: String,
    pub explicit: bool,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectRecordWire {
    pub schema_version: u32,
    pub project_name: String,
    pub project_dir: String,
    pub project_file: String,
    #[serde(default)]
    pub archive_file: Option<String>,
    #[serde(default)]
    pub workspace_dir: Option<String>,
    pub state: String,
    pub state_explicit: bool,
    pub system_managed: bool,
    pub active_claim_count: u32,
    pub launchable: bool,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub parse_warnings: Vec<String>,
    #[serde(default)]
    pub display_name: Option<String>,
}

pub fn active_project_spec_filename(project_name: &str) -> String {
    format!("{project_name}{PROJECT_SPEC_EXTENSION}")
}

pub fn archive_project_spec_filename(project_name: &str) -> String {
    format!(
        "{project_name}{PROJECT_SPEC_ARCHIVE_SUFFIX}{PROJECT_SPEC_EXTENSION}"
    )
}

pub fn legacy_active_project_spec_filename(project_name: &str) -> String {
    format!("{project_name}{LEGACY_PROJECT_SPEC_EXTENSION}")
}

pub fn legacy_archive_project_spec_filename(project_name: &str) -> String {
    format!("{project_name}{PROJECT_SPEC_ARCHIVE_SUFFIX}{LEGACY_PROJECT_SPEC_EXTENSION}")
}

pub fn project_spec_basename(file_path: &str) -> String {
    let base = file_path.rsplit(['/', '\\']).next().unwrap_or(file_path);
    let stem = match base.rfind('.') {
        Some(i) => &base[..i],
        None => base,
    };
    if let Some(prefix) = stem.strip_suffix(PROJECT_SPEC_ARCHIVE_SUFFIX) {
        prefix.to_string()
    } else {
        stem.to_string()
    }
}

pub fn is_archive_project_spec(file_path: &str) -> bool {
    let path = Path::new(file_path);
    let Some(extension) = path.extension().and_then(|value| value.to_str())
    else {
        return false;
    };
    if extension != &PROJECT_SPEC_EXTENSION[1..]
        && extension != &LEGACY_PROJECT_SPEC_EXTENSION[1..]
    {
        return false;
    }
    path.file_stem()
        .and_then(|value| value.to_str())
        .is_some_and(|stem| stem.ends_with(PROJECT_SPEC_ARCHIVE_SUFFIX))
}

pub fn to_archive_project_spec_path(project_file: &Path) -> PathBuf {
    let basename = project_spec_basename(&project_file.to_string_lossy());
    project_file.with_file_name(archive_project_spec_filename(&basename))
}

pub fn to_active_project_spec_path(project_file: &Path) -> PathBuf {
    let basename = project_spec_basename(&project_file.to_string_lossy());
    project_file.with_file_name(active_project_spec_filename(&basename))
}

pub fn preferred_project_spec_path(
    project_dir: &Path,
    project_name: &str,
    archive: bool,
) -> PathBuf {
    let canonical_name = if archive {
        archive_project_spec_filename(project_name)
    } else {
        active_project_spec_filename(project_name)
    };
    let legacy_name = if archive {
        legacy_archive_project_spec_filename(project_name)
    } else {
        legacy_active_project_spec_filename(project_name)
    };
    let canonical_path = project_dir.join(canonical_name);
    if canonical_path.exists() {
        return canonical_path;
    }
    let legacy_path = project_dir.join(legacy_name);
    if legacy_path.exists() {
        return legacy_path;
    }
    canonical_path
}

pub fn read_project_lifecycle_from_content(
    content: &str,
) -> ProjectLifecycleWire {
    let mut values: Vec<String> = Vec::new();

    for line in content.split('\n') {
        let line = line.trim_end_matches('\r');
        if line.starts_with(NAME_PREFIX) {
            break;
        }
        if let Some(value) = line.strip_prefix(PROJECT_STATE_PREFIX) {
            values.push(value.trim().to_string());
        }
    }

    let mut warnings = Vec::new();
    let Some(first_value) = values.first() else {
        return ProjectLifecycleWire {
            schema_version: PROJECT_LIFECYCLE_WIRE_SCHEMA_VERSION,
            state: ProjectLifecycleState::Active.as_str().to_string(),
            explicit: false,
            warnings,
        };
    };

    if values.len() > 1 {
        warnings.push(
            "multiple PROJECT_STATE lines found in metadata header; using first"
                .to_string(),
        );
    }

    let state = match first_value.as_str() {
        "archived" | "closed" => {
            warnings.push(format!(
                "legacy PROJECT_STATE value {first_value:?} treated as inactive"
            ));
            ProjectLifecycleState::Inactive
        }
        _ => match ProjectLifecycleState::parse_target(first_value) {
            Ok(state) => state,
            Err(_) => {
                warnings.push(format!(
                    "invalid PROJECT_STATE value {first_value:?}; using active"
                ));
                ProjectLifecycleState::Active
            }
        },
    };

    ProjectLifecycleWire {
        schema_version: PROJECT_LIFECYCLE_WIRE_SCHEMA_VERSION,
        state: state.as_str().to_string(),
        explicit: true,
        warnings,
    }
}

pub fn apply_project_lifecycle_update(
    content: &str,
    state: &str,
) -> Result<String, ProjectLifecycleError> {
    let state = ProjectLifecycleState::parse_target(state)?;
    let replacement = format!("{PROJECT_STATE_PREFIX} {}", state.as_str());
    let mut lines = split_project_spec_lines(content);
    let header_end = first_header_end(&lines);

    if let Some(idx) = lines[..header_end]
        .iter()
        .position(|line| line.body.starts_with(PROJECT_STATE_PREFIX))
    {
        lines[idx].body = replacement;
        return Ok(join_project_spec_lines(&lines));
    }

    let insert_idx = first_insert_position(&lines, header_end);
    let newline = preferred_newline(content, &lines);
    if insert_idx == lines.len()
        && !lines.is_empty()
        && lines[insert_idx - 1].ending.is_empty()
    {
        lines[insert_idx - 1].ending = newline.clone();
    }
    let inserted_ending = if lines.is_empty()
        || insert_idx < lines.len()
        || content.ends_with('\n')
    {
        newline
    } else {
        String::new()
    };
    lines.insert(
        insert_idx,
        ProjectSpecLine {
            body: replacement,
            ending: inserted_ending,
        },
    );
    Ok(join_project_spec_lines(&lines))
}

pub fn apply_project_aliases_update(
    content: &str,
    aliases: &[String],
) -> Result<String, ProjectLifecycleError> {
    let aliases = normalize_project_aliases_for_update(aliases)?;
    let mut lines = split_project_spec_lines(content);
    let header_end = first_header_end(&lines);
    let alias_indices: Vec<usize> = lines[..header_end]
        .iter()
        .enumerate()
        .filter_map(|(idx, line)| {
            line.body.starts_with(PROJECT_ALIASES_PREFIX).then_some(idx)
        })
        .collect();

    if aliases.is_empty() {
        for idx in alias_indices.iter().rev() {
            lines.remove(*idx);
        }
        return Ok(join_project_spec_lines(&lines));
    }

    let replacement =
        format!("{PROJECT_ALIASES_PREFIX} {}", aliases.join(", "));
    if let Some(first_idx) = alias_indices.first() {
        lines[*first_idx].body = replacement;
        for idx in alias_indices.iter().skip(1).rev() {
            lines.remove(*idx);
        }
        return Ok(join_project_spec_lines(&lines));
    }

    let insert_idx = first_insert_position(&lines, header_end);
    let newline = preferred_newline(content, &lines);
    if insert_idx == lines.len()
        && !lines.is_empty()
        && lines[insert_idx - 1].ending.is_empty()
    {
        lines[insert_idx - 1].ending = newline.clone();
    }
    let inserted_ending = if lines.is_empty()
        || insert_idx < lines.len()
        || content.ends_with('\n')
    {
        newline
    } else {
        String::new()
    };
    lines.insert(
        insert_idx,
        ProjectSpecLine {
            body: replacement,
            ending: inserted_ending,
        },
    );
    Ok(join_project_spec_lines(&lines))
}

pub fn apply_project_name_update(
    content: &str,
    name: Option<&str>,
) -> Result<String, ProjectLifecycleError> {
    let name = normalize_project_name_for_update(name)?;
    let mut lines = split_project_spec_lines(content);
    let header_end = first_header_end(&lines);
    let name_indices: Vec<usize> = lines[..header_end]
        .iter()
        .enumerate()
        .filter_map(|(idx, line)| {
            line.body.starts_with(PROJECT_NAME_PREFIX).then_some(idx)
        })
        .collect();

    let Some(name) = name else {
        for idx in name_indices.iter().rev() {
            lines.remove(*idx);
        }
        return Ok(join_project_spec_lines(&lines));
    };

    let replacement = format!("{PROJECT_NAME_PREFIX} {name}");
    if let Some(first_idx) = name_indices.first() {
        lines[*first_idx].body = replacement;
        for idx in name_indices.iter().skip(1).rev() {
            lines.remove(*idx);
        }
        return Ok(join_project_spec_lines(&lines));
    }

    let insert_idx = first_insert_position(&lines, header_end);
    let newline = preferred_newline(content, &lines);
    if insert_idx == lines.len()
        && !lines.is_empty()
        && lines[insert_idx - 1].ending.is_empty()
    {
        lines[insert_idx - 1].ending = newline.clone();
    }
    let inserted_ending = if lines.is_empty()
        || insert_idx < lines.len()
        || content.ends_with('\n')
    {
        newline
    } else {
        String::new()
    };
    lines.insert(
        insert_idx,
        ProjectSpecLine {
            body: replacement,
            ending: inserted_ending,
        },
    );
    Ok(join_project_spec_lines(&lines))
}

pub fn list_project_records(
    projects_root: &Path,
    include_states: &[String],
    include_home: bool,
) -> Result<Vec<ProjectRecordWire>, ProjectLifecycleError> {
    let include_filter = include_state_filter(include_states)?;
    let mut all_records = Vec::new();

    let read_dir = match fs::read_dir(projects_root) {
        Ok(read_dir) => read_dir,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(all_records)
        }
        Err(_) => return Ok(all_records),
    };

    let mut project_dirs = Vec::new();
    for entry in read_dir.flatten() {
        let path = entry.path();
        if path.is_dir() {
            project_dirs.push(path);
        }
    }
    project_dirs.sort_by(|a, b| {
        a.file_name()
            .unwrap_or_default()
            .cmp(b.file_name().unwrap_or_default())
    });

    for project_dir in project_dirs {
        let project_name = project_dir
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        if project_name.is_empty() {
            continue;
        }
        if project_name == "home" && !include_home {
            continue;
        }

        let record =
            build_project_record(&project_dir, &project_name, include_home);
        all_records.push(record);
    }

    add_project_ref_collision_warnings(&mut all_records);

    let mut records = Vec::new();
    for record in all_records {
        let state = ProjectLifecycleState::parse_target(&record.state)
            .unwrap_or(ProjectLifecycleState::Active);
        if include_filter
            .as_ref()
            .is_some_and(|states| !states.contains(&state))
        {
            continue;
        }
        records.push(record);
    }

    records.sort_by(|a, b| a.project_name.cmp(&b.project_name));
    Ok(records)
}

fn include_state_filter(
    include_states: &[String],
) -> Result<Option<BTreeSet<ProjectLifecycleState>>, ProjectLifecycleError> {
    if include_states.is_empty()
        || include_states.iter().any(|state| state == "all")
    {
        return Ok(None);
    }

    let mut states = BTreeSet::new();
    for state in include_states {
        states.insert(ProjectLifecycleState::parse_target(state)?);
    }
    Ok(Some(states))
}

fn build_project_record(
    project_dir: &Path,
    project_name: &str,
    _include_home: bool,
) -> ProjectRecordWire {
    let project_file_path =
        preferred_project_spec_path(project_dir, project_name, false);
    let archive_path =
        preferred_project_spec_path(project_dir, project_name, true);
    let archive_file = archive_path
        .is_file()
        .then(|| path_to_string(&archive_path));
    let system_managed = project_name == "home";
    let mut warnings = Vec::new();
    let mut parse_warnings = Vec::new();
    let mut state = ProjectLifecycleState::Active.as_str().to_string();
    let mut state_explicit = false;
    let mut workspace_dir = None;
    let mut aliases = Vec::new();
    let mut display_name = None;
    let mut active_claim_count = 0u32;

    if project_file_path.is_file() {
        match fs::read_to_string(&project_file_path) {
            Ok(content) => {
                let lifecycle = read_project_lifecycle_from_content(&content);
                state = lifecycle.state;
                state_explicit = lifecycle.explicit;
                parse_warnings.extend(lifecycle.warnings);
                let alias_parse = read_project_aliases_from_content(&content);
                aliases = alias_parse.aliases;
                parse_warnings.extend(alias_parse.warnings);
                let project_name_parse =
                    read_project_name_from_content(&content);
                display_name = project_name_parse.display_name;
                parse_warnings.extend(project_name_parse.warnings);
                workspace_dir = read_workspace_dir_from_content(&content);
                active_claim_count =
                    crate::agent_launch::list_workspace_claims_from_content(
                        &content,
                    )
                    .len() as u32;
            }
            Err(err) => warnings.push(format!(
                "failed to read active ProjectSpec {}: {err}",
                project_file_path.display()
            )),
        }
    } else {
        warnings.push(format!(
            "active ProjectSpec file not found: {}",
            project_file_path.display()
        ));
    }

    if system_managed {
        warnings.push("home is system-managed".to_string());
    }

    let lifecycle_state = ProjectLifecycleState::parse_target(&state)
        .unwrap_or(ProjectLifecycleState::Active);
    if lifecycle_state.is_inactive()
        || lifecycle_state == ProjectLifecycleState::Sibling
    {
        warnings.push(format!("project is {state}"));
    }

    match workspace_dir.as_deref() {
        Some(path) if Path::new(path).expand_home().exists() => {}
        Some(path) => {
            warnings.push(format!("workspace directory does not exist: {path}"))
        }
        None => warnings.push("WORKSPACE_DIR is not set".to_string()),
    }

    let launchable = !system_managed
        && lifecycle_state.is_active()
        && project_file_path.is_file()
        && workspace_dir
            .as_deref()
            .is_some_and(|path| Path::new(path).expand_home().exists());

    ProjectRecordWire {
        schema_version: PROJECT_LIFECYCLE_WIRE_SCHEMA_VERSION,
        project_name: project_name.to_string(),
        project_dir: path_to_string(project_dir),
        project_file: path_to_string(&project_file_path),
        archive_file,
        workspace_dir,
        state,
        state_explicit,
        system_managed,
        active_claim_count,
        launchable,
        aliases,
        warnings,
        parse_warnings,
        display_name,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectNameParse {
    display_name: Option<String>,
    warnings: Vec<String>,
}

fn read_project_name_from_content(content: &str) -> ProjectNameParse {
    let mut values: Vec<String> = Vec::new();

    for line in content.split('\n') {
        let line = line.trim_end_matches('\r');
        if line.starts_with(NAME_PREFIX) {
            break;
        }
        if let Some(value) = line.strip_prefix(PROJECT_NAME_PREFIX) {
            values.push(value.trim().to_string());
        }
    }

    let mut warnings = Vec::new();
    let Some(first_value) = values.first() else {
        return ProjectNameParse {
            display_name: None,
            warnings,
        };
    };

    if values.len() > 1 {
        warnings.push(
            "multiple PROJECT_NAME lines found in metadata header; using first"
                .to_string(),
        );
    }

    if !is_valid_sase_project_name(first_value) {
        warnings.push(format!(
            "invalid PROJECT_NAME value {first_value:?} ignored"
        ));
        return ProjectNameParse {
            display_name: None,
            warnings,
        };
    }

    ProjectNameParse {
        display_name: Some(first_value.to_string()),
        warnings,
    }
}

/// Return the valid configured project display name from the metadata header.
///
/// ChangeSpec parsing uses this narrow helper so query metadata follows the
/// same validation and first-value semantics as lifecycle discovery.
pub(crate) fn project_display_name_from_content(
    content: &str,
) -> Option<String> {
    read_project_name_from_content(content).display_name
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectAliasesParse {
    aliases: Vec<String>,
    warnings: Vec<String>,
}

fn read_project_aliases_from_content(content: &str) -> ProjectAliasesParse {
    let mut values: Vec<String> = Vec::new();

    for line in content.split('\n') {
        let line = line.trim_end_matches('\r');
        if line.starts_with(NAME_PREFIX) {
            break;
        }
        if let Some(value) = line.strip_prefix(PROJECT_ALIASES_PREFIX) {
            values.push(value.trim().to_string());
        }
    }

    let mut warnings = Vec::new();
    let Some(first_value) = values.first() else {
        return ProjectAliasesParse {
            aliases: Vec::new(),
            warnings,
        };
    };

    if values.len() > 1 {
        warnings.push(
            "multiple PROJECT_ALIASES lines found in metadata header; using first"
                .to_string(),
        );
    }

    let aliases = parse_project_alias_list(first_value, &mut warnings);
    ProjectAliasesParse { aliases, warnings }
}

fn parse_project_alias_list(
    value: &str,
    warnings: &mut Vec<String>,
) -> Vec<String> {
    let mut aliases = BTreeSet::new();
    for raw_alias in value.split(',') {
        let alias = raw_alias.trim();
        if alias.is_empty() {
            warnings.push("empty PROJECT_ALIASES entry ignored".to_string());
            continue;
        }
        if !is_valid_sase_project_name(alias) {
            warnings.push(format!(
                "invalid PROJECT_ALIASES value {alias:?} ignored"
            ));
            continue;
        }
        if !aliases.insert(alias.to_string()) {
            warnings.push(format!(
                "duplicate PROJECT_ALIASES value {alias:?} ignored"
            ));
        }
    }
    aliases.into_iter().collect()
}

fn normalize_project_aliases_for_update(
    aliases: &[String],
) -> Result<Vec<String>, ProjectLifecycleError> {
    let mut normalized = BTreeSet::new();
    for raw_alias in aliases {
        let alias = raw_alias.trim();
        if !is_valid_sase_project_name(alias) {
            return Err(ProjectLifecycleError::InvalidProjectAlias(format!(
                "invalid project alias {alias:?}; expected a valid SASE project name"
            )));
        }
        if !normalized.insert(alias.to_string()) {
            return Err(ProjectLifecycleError::InvalidProjectAlias(format!(
                "duplicate project alias {alias:?}"
            )));
        }
    }
    Ok(normalized.into_iter().collect())
}

fn normalize_project_name_for_update(
    name: Option<&str>,
) -> Result<Option<String>, ProjectLifecycleError> {
    let Some(raw_name) = name else {
        return Ok(None);
    };
    let project_name = raw_name.trim();
    if project_name.is_empty() {
        return Ok(None);
    }
    if !is_valid_sase_project_name(project_name) {
        return Err(ProjectLifecycleError::InvalidProjectName(format!(
            "invalid project name {project_name:?}; expected a valid SASE project name"
        )));
    }
    Ok(Some(project_name.to_string()))
}

fn is_valid_sase_project_name(project_name: &str) -> bool {
    !project_name.is_empty()
        && project_name != "."
        && project_name != ".."
        && !project_name.starts_with('.')
        && !project_name.contains('\0')
        && !project_name.contains('/')
        && !project_name.contains('\\')
        && !project_name.contains(',')
}

fn add_project_ref_collision_warnings(records: &mut [ProjectRecordWire]) {
    let project_names: BTreeSet<String> = records
        .iter()
        .map(|record| record.project_name.clone())
        .collect();
    let mut alias_owners: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut display_name_owners: BTreeMap<String, Vec<String>> =
        BTreeMap::new();
    let mut warnings_by_project: BTreeMap<String, Vec<String>> =
        BTreeMap::new();

    for record in records.iter().filter(|record| !record.system_managed) {
        if let Some(display_name) = record.display_name.as_ref() {
            if display_name != &record.project_name {
                if project_names.contains(display_name) {
                    warnings_by_project
                        .entry(record.project_name.clone())
                        .or_default()
                        .push(format!(
                            "PROJECT_NAME value {display_name:?} collides with project {display_name:?}"
                        ));
                }
                display_name_owners
                    .entry(display_name.clone())
                    .or_default()
                    .push(record.project_name.clone());
            }
        }

        for alias in &record.aliases {
            if alias == &record.project_name {
                warnings_by_project
                    .entry(record.project_name.clone())
                    .or_default()
                    .push(format!(
                        "PROJECT_ALIASES value {alias:?} matches canonical project name"
                    ));
            } else if project_names.contains(alias) {
                warnings_by_project
                    .entry(record.project_name.clone())
                    .or_default()
                    .push(format!(
                    "PROJECT_ALIASES value {alias:?} collides with project {alias:?}"
                    ));
            }
            if record.display_name.as_ref() == Some(alias) {
                warnings_by_project
                    .entry(record.project_name.clone())
                    .or_default()
                    .push(format!(
                        "PROJECT_ALIASES value {alias:?} matches PROJECT_NAME"
                    ));
            }
            alias_owners
                .entry(alias.clone())
                .or_default()
                .push(record.project_name.clone());
        }
    }

    for (alias, owners) in &alias_owners {
        let mut owners = owners.clone();
        owners.sort();
        owners.dedup();
        if owners.len() <= 1 {
            continue;
        }
        for owner in &owners {
            let others: Vec<&str> = owners
                .iter()
                .filter(|candidate| *candidate != owner)
                .map(String::as_str)
                .collect();
            warnings_by_project
                .entry(owner.clone())
                .or_default()
                .push(format!(
                    "PROJECT_ALIASES value {alias:?} is also assigned to project(s): {}",
                    others.join(", ")
                ));
        }
    }

    for (display_name, mut owners) in display_name_owners.clone() {
        owners.sort();
        owners.dedup();
        if owners.len() <= 1 {
            continue;
        }
        for owner in &owners {
            let others: Vec<&str> = owners
                .iter()
                .filter(|candidate| *candidate != owner)
                .map(String::as_str)
                .collect();
            warnings_by_project
                .entry(owner.clone())
                .or_default()
                .push(format!(
                    "PROJECT_NAME value {display_name:?} is also assigned to project(s): {}",
                    others.join(", ")
                ));
        }
    }

    for (display_name, display_owners) in display_name_owners {
        let Some(alias_owners_for_name) = alias_owners.get(&display_name)
        else {
            continue;
        };
        let mut display_owners = display_owners;
        display_owners.sort();
        display_owners.dedup();
        let mut alias_owners_for_name = alias_owners_for_name.clone();
        alias_owners_for_name.sort();
        alias_owners_for_name.dedup();

        for owner in &display_owners {
            let alias_projects: Vec<&str> = alias_owners_for_name
                .iter()
                .filter(|candidate| *candidate != owner)
                .map(String::as_str)
                .collect();
            if !alias_projects.is_empty() {
                warnings_by_project
                    .entry(owner.clone())
                    .or_default()
                    .push(format!(
                        "PROJECT_NAME value {display_name:?} collides with alias assigned to project(s): {}",
                        alias_projects.join(", ")
                    ));
            }
        }
        for owner in &alias_owners_for_name {
            let display_projects: Vec<&str> = display_owners
                .iter()
                .filter(|candidate| *candidate != owner)
                .map(String::as_str)
                .collect();
            if !display_projects.is_empty() {
                warnings_by_project
                    .entry(owner.clone())
                    .or_default()
                    .push(format!(
                        "PROJECT_ALIASES value {display_name:?} collides with PROJECT_NAME assigned to project(s): {}",
                        display_projects.join(", ")
                    ));
            }
        }
    }

    for record in records {
        if let Some(warnings) = warnings_by_project.remove(&record.project_name)
        {
            record.parse_warnings.extend(warnings);
        }
    }
}

trait ExpandHome {
    fn expand_home(&self) -> PathBuf;
}

impl ExpandHome for Path {
    fn expand_home(&self) -> PathBuf {
        let raw = self.to_string_lossy();
        if raw == "~" {
            if let Some(home) = std::env::var_os("HOME") {
                return PathBuf::from(home);
            }
        } else if let Some(rest) = raw.strip_prefix("~/") {
            if let Some(home) = std::env::var_os("HOME") {
                return PathBuf::from(home).join(rest);
            }
        }
        self.to_path_buf()
    }
}

fn read_workspace_dir_from_content(content: &str) -> Option<String> {
    for line in content.split('\n') {
        let line = line.trim_end_matches('\r');
        if line.starts_with(NAME_PREFIX) {
            break;
        }
        if let Some(value) = line.strip_prefix(WORKSPACE_DIR_PREFIX) {
            let value = value.trim();
            if value.is_empty() {
                return None;
            }
            return Some(path_to_string(&Path::new(value).expand_home()));
        }
    }
    None
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectSpecLine {
    body: String,
    ending: String,
}

fn split_project_spec_lines(content: &str) -> Vec<ProjectSpecLine> {
    let mut lines = Vec::new();
    for part in content.split_inclusive('\n') {
        if let Some(body) = part.strip_suffix("\r\n") {
            lines.push(ProjectSpecLine {
                body: body.to_string(),
                ending: "\r\n".to_string(),
            });
        } else if let Some(body) = part.strip_suffix('\n') {
            lines.push(ProjectSpecLine {
                body: body.to_string(),
                ending: "\n".to_string(),
            });
        } else {
            lines.push(ProjectSpecLine {
                body: part.to_string(),
                ending: String::new(),
            });
        }
    }
    lines
}

fn join_project_spec_lines(lines: &[ProjectSpecLine]) -> String {
    let mut content = String::new();
    for line in lines {
        content.push_str(&line.body);
        content.push_str(&line.ending);
    }
    content
}

fn preferred_newline(content: &str, lines: &[ProjectSpecLine]) -> String {
    for line in lines {
        if !line.ending.is_empty() {
            return line.ending.clone();
        }
    }
    if content.contains("\r\n") {
        "\r\n".to_string()
    } else {
        "\n".to_string()
    }
}

fn first_header_end(lines: &[ProjectSpecLine]) -> usize {
    lines
        .iter()
        .position(|line| line.body.starts_with(NAME_PREFIX))
        .unwrap_or(lines.len())
}

fn first_insert_position(
    lines: &[ProjectSpecLine],
    header_end: usize,
) -> usize {
    lines[..header_end]
        .iter()
        .position(|line| line.body.starts_with(RUNNING_PREFIX))
        .or_else(|| {
            lines
                .iter()
                .position(|line| line.body.starts_with(NAME_PREFIX))
        })
        .unwrap_or(lines.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alias_vec(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn project_spec_filenames_are_canonical_sase() {
        assert_eq!(PROJECT_SPEC_EXTENSION, ".sase");
        assert_eq!(LEGACY_PROJECT_SPEC_EXTENSION, ".gp");
        assert_eq!(active_project_spec_filename("proj"), "proj.sase");
        assert_eq!(archive_project_spec_filename("proj"), "proj-archive.sase");
    }

    #[test]
    fn project_spec_basename_accepts_active_and_archive_extensions() {
        assert_eq!(project_spec_basename("/tmp/myproj.sase"), "myproj");
        assert_eq!(project_spec_basename("/tmp/myproj.gp"), "myproj");
        assert_eq!(project_spec_basename("/tmp/myproj-archive.sase"), "myproj");
        assert_eq!(project_spec_basename("/tmp/myproj-archive.gp"), "myproj");
        assert_eq!(project_spec_basename("/tmp/no_ext"), "no_ext");
        assert_eq!(project_spec_basename("foo.bar.sase"), "foo.bar");
    }

    #[test]
    fn project_spec_path_conversions_emit_canonical_extension() {
        assert_eq!(
            to_archive_project_spec_path(Path::new("/tmp/proj.sase")),
            PathBuf::from("/tmp/proj-archive.sase")
        );
        assert_eq!(
            to_archive_project_spec_path(Path::new("/tmp/proj.gp")),
            PathBuf::from("/tmp/proj-archive.sase")
        );
        assert_eq!(
            to_active_project_spec_path(Path::new("/tmp/proj-archive.sase")),
            PathBuf::from("/tmp/proj.sase")
        );
        assert_eq!(
            to_active_project_spec_path(Path::new("/tmp/proj-archive.gp")),
            PathBuf::from("/tmp/proj.sase")
        );
    }

    #[test]
    fn archive_detection_accepts_canonical_and_legacy_extensions() {
        assert!(is_archive_project_spec("/tmp/proj-archive.sase"));
        assert!(is_archive_project_spec("/tmp/proj-archive.gp"));
        assert!(!is_archive_project_spec("/tmp/proj.sase"));
        assert!(!is_archive_project_spec("/tmp/proj-archive.txt"));
    }

    #[test]
    fn lifecycle_read_defaults_missing_state_to_active() {
        let read =
            read_project_lifecycle_from_content("NAME: demo\nSTATUS: WIP\n");
        assert_eq!(read.state, "active");
        assert!(!read.explicit);
        assert!(read.warnings.is_empty());
    }

    #[test]
    fn lifecycle_read_accepts_canonical_inactive_state() {
        let read = read_project_lifecycle_from_content(
            "WORKSPACE_DIR: /tmp\nPROJECT_STATE: inactive\nNAME: demo\n",
        );
        assert_eq!(read.state, "inactive");
        assert!(read.explicit);
        assert!(read.warnings.is_empty());
    }

    #[test]
    fn lifecycle_read_accepts_canonical_sibling_state() {
        let read = read_project_lifecycle_from_content(
            "WORKSPACE_DIR: /tmp\nPROJECT_STATE: sibling\nNAME: demo\n",
        );
        assert_eq!(read.state, "sibling");
        assert!(read.explicit);
        assert!(read.warnings.is_empty());
    }

    #[test]
    fn lifecycle_read_normalizes_legacy_inactive_states() {
        for legacy in ["archived", "closed"] {
            let read = read_project_lifecycle_from_content(&format!(
                "WORKSPACE_DIR: /tmp\nPROJECT_STATE: {legacy}\nNAME: demo\n"
            ));
            assert_eq!(read.state, "inactive");
            assert!(read.explicit);
            assert_eq!(read.warnings.len(), 1);
            assert!(read.warnings[0].contains("legacy PROJECT_STATE value"));
            assert!(read.warnings[0].contains("treated as inactive"));
        }
    }

    #[test]
    fn lifecycle_read_warns_and_defaults_invalid_state() {
        let read = read_project_lifecycle_from_content(
            "PROJECT_STATE: sleeping\nNAME: demo\n",
        );
        assert_eq!(read.state, "active");
        assert!(read.explicit);
        assert_eq!(read.warnings.len(), 1);
        assert!(read.warnings[0].contains("invalid PROJECT_STATE value"));
    }

    #[test]
    fn lifecycle_read_warns_on_duplicate_state() {
        let read = read_project_lifecycle_from_content(
            "PROJECT_STATE: archived\nPROJECT_STATE: closed\nNAME: demo\n",
        );
        assert_eq!(read.state, "inactive");
        assert!(read.explicit);
        assert_eq!(read.warnings.len(), 2);
        assert!(read.warnings[0].contains("multiple PROJECT_STATE"));
        assert!(read.warnings[1].contains("legacy PROJECT_STATE value"));
    }

    #[test]
    fn lifecycle_update_replaces_existing_state_line() {
        let content =
            "WORKSPACE_DIR: /tmp\nPROJECT_STATE: active\nNAME: demo\n";
        let updated =
            apply_project_lifecycle_update(content, "inactive").unwrap();
        assert_eq!(
            updated,
            "WORKSPACE_DIR: /tmp\nPROJECT_STATE: inactive\nNAME: demo\n"
        );
    }

    #[test]
    fn lifecycle_update_accepts_sibling_target_state() {
        let content =
            "WORKSPACE_DIR: /tmp\nPROJECT_STATE: active\nNAME: demo\n";
        let updated =
            apply_project_lifecycle_update(content, "sibling").unwrap();
        assert_eq!(
            updated,
            "WORKSPACE_DIR: /tmp\nPROJECT_STATE: sibling\nNAME: demo\n"
        );
    }

    #[test]
    fn lifecycle_update_normalizes_legacy_target_state() {
        let content =
            "WORKSPACE_DIR: /tmp\nPROJECT_STATE: active\nNAME: demo\n";
        for legacy in ["archived", "closed"] {
            let updated =
                apply_project_lifecycle_update(content, legacy).unwrap();
            assert_eq!(
                updated,
                "WORKSPACE_DIR: /tmp\nPROJECT_STATE: inactive\nNAME: demo\n"
            );
        }
    }

    #[test]
    fn lifecycle_update_inserts_before_running_and_preserves_crlf() {
        let content = "WORKSPACE_DIR: /tmp\r\nRUNNING:\r\n  #1 | 111 | run | demo\r\n\r\nNAME: demo\r\n";
        let updated =
            apply_project_lifecycle_update(content, "inactive").unwrap();
        assert_eq!(
            updated,
            "WORKSPACE_DIR: /tmp\r\nPROJECT_STATE: inactive\r\nRUNNING:\r\n  #1 | 111 | run | demo\r\n\r\nNAME: demo\r\n"
        );
    }

    #[test]
    fn lifecycle_update_inserts_before_first_name() {
        let content = "WORKSPACE_DIR: /tmp\nNAME: demo\n";
        let updated =
            apply_project_lifecycle_update(content, "inactive").unwrap();
        assert_eq!(
            updated,
            "WORKSPACE_DIR: /tmp\nPROJECT_STATE: inactive\nNAME: demo\n"
        );
    }

    #[test]
    fn lifecycle_update_rejects_invalid_target_state() {
        let err = apply_project_lifecycle_update("NAME: demo\n", "paused")
            .unwrap_err();
        assert!(err.to_string().contains("invalid project lifecycle state"));
    }

    #[test]
    fn project_aliases_read_defaults_missing_aliases_to_empty() {
        let read = read_project_aliases_from_content(
            "WORKSPACE_DIR: /tmp\nNAME: demo\n",
        );

        assert!(read.aliases.is_empty());
        assert!(read.warnings.is_empty());
    }

    #[test]
    fn project_aliases_read_sorts_dedupes_and_warns() {
        let read = read_project_aliases_from_content(
            "PROJECT_ALIASES: docs, bob, docs, .hidden, foo/bar,, alpha\nPROJECT_ALIASES: ignored\nNAME: demo\n",
        );

        assert_eq!(read.aliases, alias_vec(&["alpha", "bob", "docs"]));
        assert!(read
            .warnings
            .iter()
            .any(|warning| warning.contains("multiple PROJECT_ALIASES")));
        assert!(read.warnings.iter().any(|warning| warning
            .contains("duplicate PROJECT_ALIASES value \"docs\"")));
        assert!(read.warnings.iter().any(|warning| warning
            .contains("invalid PROJECT_ALIASES value \".hidden\"")));
        assert!(read.warnings.iter().any(|warning| warning
            .contains("invalid PROJECT_ALIASES value \"foo/bar\"")));
        assert!(read
            .warnings
            .iter()
            .any(|warning| warning.contains("empty PROJECT_ALIASES entry")));
    }

    #[test]
    fn project_aliases_update_replaces_existing_aliases_sorted() {
        let content =
            "WORKSPACE_DIR: /tmp\nPROJECT_ALIASES: old\nPROJECT_ALIASES: stale\nNAME: demo\n";
        let updated =
            apply_project_aliases_update(content, &alias_vec(&["docs", "bob"]))
                .unwrap();

        assert_eq!(
            updated,
            "WORKSPACE_DIR: /tmp\nPROJECT_ALIASES: bob, docs\nNAME: demo\n"
        );
    }

    #[test]
    fn project_aliases_update_inserts_before_running_and_preserves_crlf() {
        let content = "WORKSPACE_DIR: /tmp\r\nRUNNING:\r\n  #1 | 111 | run | demo\r\n\r\nNAME: demo\r\n";
        let updated =
            apply_project_aliases_update(content, &alias_vec(&["docs", "bob"]))
                .unwrap();

        assert_eq!(
            updated,
            "WORKSPACE_DIR: /tmp\r\nPROJECT_ALIASES: bob, docs\r\nRUNNING:\r\n  #1 | 111 | run | demo\r\n\r\nNAME: demo\r\n"
        );
    }

    #[test]
    fn project_aliases_update_inserts_before_first_name() {
        let content = "WORKSPACE_DIR: /tmp\nNAME: demo\n";
        let updated =
            apply_project_aliases_update(content, &alias_vec(&["bob"]))
                .unwrap();

        assert_eq!(
            updated,
            "WORKSPACE_DIR: /tmp\nPROJECT_ALIASES: bob\nNAME: demo\n"
        );
    }

    #[test]
    fn project_aliases_update_removes_existing_aliases() {
        let content = "WORKSPACE_DIR: /tmp\nPROJECT_ALIASES: bob, docs\nPROJECT_ALIASES: stale\nNAME: demo\n";
        let updated = apply_project_aliases_update(content, &[]).unwrap();

        assert_eq!(updated, "WORKSPACE_DIR: /tmp\nNAME: demo\n");
    }

    #[test]
    fn project_aliases_update_rejects_invalid_or_duplicate_aliases() {
        let invalid = apply_project_aliases_update(
            "NAME: demo\n",
            &alias_vec(&["bob", ".hidden"]),
        )
        .unwrap_err();
        assert!(invalid.to_string().contains("invalid project alias"));

        let duplicate = apply_project_aliases_update(
            "NAME: demo\n",
            &alias_vec(&["bob", "bob"]),
        )
        .unwrap_err();
        assert!(duplicate.to_string().contains("duplicate project alias"));
    }

    #[test]
    fn project_name_read_accepts_missing_and_present_name() {
        let missing =
            read_project_name_from_content("WORKSPACE_DIR: /tmp\nNAME: demo\n");
        assert_eq!(missing.display_name, None);
        assert!(missing.warnings.is_empty());

        let present = read_project_name_from_content(
            "WORKSPACE_DIR: /tmp\nPROJECT_NAME: widgets\nNAME: demo\n",
        );
        assert_eq!(present.display_name.as_deref(), Some("widgets"));
        assert!(present.warnings.is_empty());
    }

    #[test]
    fn project_name_read_warns_on_invalid_and_duplicate_names() {
        let invalid = read_project_name_from_content(
            "PROJECT_NAME: .hidden\nPROJECT_NAME: ignored\nNAME: demo\n",
        );
        assert_eq!(invalid.display_name, None);
        assert!(invalid
            .warnings
            .iter()
            .any(|warning| warning.contains("multiple PROJECT_NAME")));
        assert!(invalid
            .warnings
            .iter()
            .any(|warning| warning.contains("invalid PROJECT_NAME value")));
    }

    #[test]
    fn project_name_update_inserts_replaces_and_removes_name() {
        let content = "WORKSPACE_DIR: /tmp\nNAME: demo\n";
        let inserted =
            apply_project_name_update(content, Some("widgets")).unwrap();
        assert_eq!(
            inserted,
            "WORKSPACE_DIR: /tmp\nPROJECT_NAME: widgets\nNAME: demo\n"
        );

        let replaced = apply_project_name_update(
            "WORKSPACE_DIR: /tmp\nPROJECT_NAME: old\nPROJECT_NAME: stale\nNAME: demo\n",
            Some("widgets"),
        )
        .unwrap();
        assert_eq!(
            replaced,
            "WORKSPACE_DIR: /tmp\nPROJECT_NAME: widgets\nNAME: demo\n"
        );

        let removed = apply_project_name_update(&replaced, None).unwrap();
        assert_eq!(removed, "WORKSPACE_DIR: /tmp\nNAME: demo\n");
    }

    #[test]
    fn project_name_update_preserves_crlf_and_rejects_invalid_name() {
        let content = "WORKSPACE_DIR: /tmp\r\nRUNNING:\r\n  #1 | 111 | run | demo\r\n\r\nNAME: demo\r\n";
        let updated =
            apply_project_name_update(content, Some("widgets")).unwrap();
        assert_eq!(
            updated,
            "WORKSPACE_DIR: /tmp\r\nPROJECT_NAME: widgets\r\nRUNNING:\r\n  #1 | 111 | run | demo\r\n\r\nNAME: demo\r\n"
        );

        let err = apply_project_name_update("NAME: demo\n", Some("foo/bar"))
            .unwrap_err();
        assert!(err.to_string().contains("invalid project name"));
    }

    #[test]
    fn lifecycle_project_records_include_aliases_and_collision_warnings() {
        let temp = tempfile::tempdir().unwrap();
        let projects = temp.path().join("projects");
        fs::create_dir(&projects).unwrap();

        let alpha_dir = projects.join("alpha");
        fs::create_dir(&alpha_dir).unwrap();
        fs::write(
            alpha_dir.join("alpha.sase"),
            "PROJECT_NAME: docs\nPROJECT_ALIASES: shared, beta, alpha\nNAME: alpha\n",
        )
        .unwrap();

        let beta_dir = projects.join("beta");
        fs::create_dir(&beta_dir).unwrap();
        fs::write(
            beta_dir.join("beta.sase"),
            "PROJECT_NAME: docs\nPROJECT_ALIASES: shared\nNAME: beta\n",
        )
        .unwrap();

        let gamma_dir = projects.join("gamma");
        fs::create_dir(&gamma_dir).unwrap();
        fs::write(
            gamma_dir.join("gamma.sase"),
            "PROJECT_ALIASES: alpha\nNAME: gamma\n",
        )
        .unwrap();

        let records =
            list_project_records(&projects, &["all".to_string()], false)
                .unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].aliases, alias_vec(&["alpha", "beta", "shared"]));
        assert_eq!(records[0].display_name.as_deref(), Some("docs"));
        assert!(records[0]
            .parse_warnings
            .iter()
            .any(|warning| warning.contains("matches canonical project name")));
        assert!(records[0]
            .parse_warnings
            .iter()
            .any(|warning| warning.contains("collides with project \"beta\"")));
        assert!(records[0].parse_warnings.iter().any(
            |warning| warning.contains("also assigned to project(s): beta")
        ));
        assert!(records[0]
            .parse_warnings
            .iter()
            .any(|warning| warning.contains("PROJECT_NAME value \"docs\"")
                && warning.contains("also assigned")));
        assert_eq!(records[1].aliases, alias_vec(&["shared"]));
        assert_eq!(records[1].display_name.as_deref(), Some("docs"));
        assert!(records[1]
            .parse_warnings
            .iter()
            .any(|warning| warning
                .contains("also assigned to project(s): alpha")));
        assert!(records[1]
            .parse_warnings
            .iter()
            .any(|warning| warning.contains("PROJECT_NAME value \"docs\"")
                && warning.contains("also assigned")));
        assert_eq!(records[2].aliases, alias_vec(&["alpha"]));
        assert_eq!(records[2].display_name, None);
        assert!(
            records[2]
                .parse_warnings
                .iter()
                .any(|warning| warning
                    .contains("collides with project \"alpha\""))
        );
    }

    #[test]
    fn lifecycle_project_records_filter_and_sort_projects() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        let projects = temp.path().join("projects");
        fs::create_dir(&projects).unwrap();

        let beta_dir = projects.join("beta");
        fs::create_dir(&beta_dir).unwrap();
        fs::write(
            beta_dir.join("beta.gp"),
            format!(
                "WORKSPACE_DIR: {}\nRUNNING:\n  #1 | 123 | run | demo\n\nNAME: demo\n",
                workspace.display()
            ),
        )
        .unwrap();

        let alpha_dir = projects.join("alpha");
        fs::create_dir(&alpha_dir).unwrap();
        fs::write(
            alpha_dir.join("alpha.sase"),
            format!(
                "PROJECT_STATE: archived\nWORKSPACE_DIR: {}\nNAME: old\n",
                workspace.display()
            ),
        )
        .unwrap();
        fs::write(alpha_dir.join("alpha-archive.sase"), "NAME: old\n").unwrap();

        let gamma_dir = projects.join("gamma");
        fs::create_dir(&gamma_dir).unwrap();
        fs::write(
            gamma_dir.join("gamma.sase"),
            format!(
                "PROJECT_STATE: sibling\nWORKSPACE_DIR: {}\nNAME: sibling\n",
                workspace.display()
            ),
        )
        .unwrap();

        let home_dir = projects.join("home");
        fs::create_dir(&home_dir).unwrap();
        fs::write(
            home_dir.join("home.sase"),
            format!("WORKSPACE_DIR: {}\n", workspace.display()),
        )
        .unwrap();

        let active =
            list_project_records(&projects, &["active".to_string()], false)
                .unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].project_name, "beta");
        assert_eq!(active[0].state, "active");
        assert!(!active[0].state_explicit);
        assert_eq!(active[0].active_claim_count, 1);
        assert!(active[0].launchable);
        assert!(active[0].project_file.ends_with("beta.gp"));

        let all = list_project_records(&projects, &["all".to_string()], true)
            .unwrap();
        let names: Vec<&str> = all
            .iter()
            .map(|record| record.project_name.as_str())
            .collect();
        assert_eq!(names, vec!["alpha", "beta", "gamma", "home"]);
        assert_eq!(all[0].state, "inactive");
        assert_eq!(all[0].display_name, None);
        assert!(all[0]
            .parse_warnings
            .iter()
            .any(|warning| warning.contains("legacy PROJECT_STATE value")));
        assert!(all[0].archive_file.as_deref().unwrap().ends_with(".sase"));
        assert_eq!(all[2].state, "sibling");
        assert!(!all[2].launchable);
        assert!(all[2]
            .warnings
            .iter()
            .any(|warning| warning == "project is sibling"));
        assert!(all[3].system_managed);
        assert!(!all[3].launchable);

        let inactive =
            list_project_records(&projects, &["inactive".to_string()], false)
                .unwrap();
        assert_eq!(inactive.len(), 1);
        assert_eq!(inactive[0].project_name, "alpha");
        assert_eq!(inactive[0].state, "inactive");

        let legacy_archived =
            list_project_records(&projects, &["archived".to_string()], false)
                .unwrap();
        assert_eq!(legacy_archived.len(), 1);
        assert_eq!(legacy_archived[0].state, "inactive");

        let sibling =
            list_project_records(&projects, &["sibling".to_string()], false)
                .unwrap();
        assert_eq!(sibling.len(), 1);
        assert_eq!(sibling[0].project_name, "gamma");
        assert_eq!(sibling[0].state, "sibling");
    }
}
