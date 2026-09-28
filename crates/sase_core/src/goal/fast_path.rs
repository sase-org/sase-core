//! Lean `sase goal` fast path.
//!
//! Early-dispatched from `entry.py` before `argparse` loads. It serves
//! bare `goal`, `goal list` (only `-j` and unsettled `-s` values), and
//! `goal show ID [-j]` straight from the hot projection header plus one
//! ledger read, importing only the stdlib and the binding loader on the
//! Python side. Anything else (`-h`, `-a`, `-f`, history statuses, a
//! missing projection, or a non-canonical project) declines with
//! `handled: false` so `argparse` takes over.
//!
//! Handled reads reduce from the same core functions the slow path
//! uses, so fast- and slow-path output is identical by construction.

use std::fs;
use std::path::{Path, PathBuf};

use chrono::DateTime;
use serde::{Deserialize, Serialize};

use super::ids::parse_goal_id;
use super::ledger::{
    goal_ledger_list, goal_ledger_show, GoalLedgerError, GoalListFilterWire,
    GoalProjectionWire, GOAL_DEFAULT_FETCH_TTL_SECONDS,
    GOAL_PROJECTION_SCHEMA_VERSION,
};
use super::render::{
    render_goal_card, render_goal_list, GoalRenderCardRequestWire,
    GoalRenderListRequestWire,
};

/// Request wire for [`goal_fast_path`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalFastPathRequestWire {
    /// Tokens after `goal` (empty means bare `goal`).
    #[serde(default)]
    pub argv: Vec<String>,
    /// Working directory the command runs from.
    #[serde(default)]
    pub cwd: String,
    /// Resolved `SASE_HOME` (Python reads the env with the stdlib).
    #[serde(default)]
    pub sase_home: String,
    /// Emit ANSI styling; the caller already honored TTY/`NO_COLOR`.
    #[serde(default)]
    pub color: bool,
    /// Compact rows (inside an agent run).
    #[serde(default)]
    pub agent: bool,
    /// RFC3339 now, used for relative ages and the fetch decision.
    #[serde(default)]
    pub now: String,
}

/// Response wire for [`goal_fast_path`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalFastPathResponseWire {
    /// False when the slow `argparse` path must handle the command.
    #[serde(default)]
    pub handled: bool,
    /// Process exit code when handled.
    #[serde(default)]
    pub exit_code: i32,
    /// Text for stdout when handled.
    #[serde(default)]
    pub stdout: String,
    /// Text for stderr when handled.
    #[serde(default)]
    pub stderr: String,
    /// True when the caller should spawn the TTL fetch worker.
    #[serde(default)]
    pub spawn_fetch: bool,
    /// Resolved project key, even when declining.
    #[serde(default)]
    pub project: String,
}

enum FastCommand {
    List { json: bool, status: Option<String> },
    Show { id: String, json: bool },
}

fn decline(project: &str) -> GoalFastPathResponseWire {
    GoalFastPathResponseWire {
        handled: false,
        project: project.to_string(),
        ..GoalFastPathResponseWire::default()
    }
}

/// Serve one lean `sase goal` invocation, or decline to the slow path.
pub fn goal_fast_path(
    request: &GoalFastPathRequestWire,
) -> Result<GoalFastPathResponseWire, GoalLedgerError> {
    let command = match parse_argv(&request.argv) {
        Some(command) => command,
        None => return Ok(decline("")),
    };
    let bare = request.argv.is_empty();
    let project = match discover_project(&request.cwd) {
        Some(project) => project,
        None => return Ok(decline("")),
    };
    let projection = match load_hot_projection(&request.sase_home, &project) {
        Some(projection) => projection,
        None => return Ok(decline(&project)),
    };
    let root = PathBuf::from(&projection.ledger_root);
    let mut response = match command {
        FastCommand::List { json, status } => render_fast_list(
            request,
            &project,
            &projection,
            &root,
            json,
            status,
        )?,
        FastCommand::Show { id, json } => {
            render_fast_show(request, &project, &projection, &root, &id, json)?
        }
    };
    if bare {
        response.stdout = format!(
            "No subcommand provided for 'sase goal'; delegating to 'sase goal list'.\n{}",
            response.stdout
        );
    }
    response.project = project;
    Ok(response)
}

fn parse_argv(argv: &[String]) -> Option<FastCommand> {
    if argv.is_empty() {
        return Some(FastCommand::List {
            json: false,
            status: None,
        });
    }
    match argv[0].as_str() {
        "list" => parse_list_argv(&argv[1..]),
        "show" => parse_show_argv(&argv[1..]),
        _ => None,
    }
}

fn parse_list_argv(argv: &[String]) -> Option<FastCommand> {
    let mut json = false;
    let mut status: Option<String> = None;
    let mut index = 0;
    while index < argv.len() {
        match argv[index].as_str() {
            "-j" | "--json" => json = true,
            "-s" | "--status" => {
                index += 1;
                let value = argv.get(index)?.to_string();
                match value.as_str() {
                    "unsettled" | "active" | "review" => {
                        status = Some(value);
                    }
                    _ => return None,
                }
            }
            _ => return None,
        }
        index += 1;
    }
    Some(FastCommand::List { json, status })
}

fn parse_show_argv(argv: &[String]) -> Option<FastCommand> {
    let mut id: Option<String> = None;
    let mut json = false;
    for token in argv {
        match token.as_str() {
            "-j" | "--json" => json = true,
            _ if token.starts_with('-') => return None,
            _ => {
                if id.is_some() {
                    return None;
                }
                id = Some(token.clone());
            }
        }
    }
    Some(FastCommand::Show { id: id?, json })
}

/// Find the nearest `.sase/checkout.json` and return its project name.
///
/// Returns `None` outside a checkout or for a non-canonical name, so
/// the slow path (which resolves aliases) takes over.
fn discover_project(cwd: &str) -> Option<String> {
    let start = PathBuf::from(if cwd.is_empty() { "." } else { cwd });
    let start = if start.is_relative() {
        std::env::current_dir().ok()?.join(start)
    } else {
        start
    };
    let mut current: Option<PathBuf> = Some(start);
    while let Some(path) = current {
        let candidate = path.join(".sase").join("checkout.json");
        if let Some(project) = read_checkout_project(&candidate) {
            return Some(project);
        }
        current = path.parent().map(Path::to_path_buf);
    }
    None
}

fn read_checkout_project(path: &Path) -> Option<String> {
    let contents = fs::read_to_string(path).ok()?;
    let value: serde_json::Value = serde_json::from_str(&contents).ok()?;
    let name = value.get("project_name")?.as_str()?;
    if is_canonical_project(name) {
        Some(name.to_string())
    } else {
        None
    }
}

fn is_canonical_project(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
}

fn hot_projection_path(sase_home: &str, project: &str) -> PathBuf {
    PathBuf::from(sase_home)
        .join("projects")
        .join(project)
        .join("goals-hot.json")
}

fn load_hot_projection(
    sase_home: &str,
    project: &str,
) -> Option<GoalProjectionWire> {
    let contents =
        fs::read_to_string(hot_projection_path(sase_home, project)).ok()?;
    let projection: GoalProjectionWire =
        serde_json::from_str(&contents).ok()?;
    if projection.schema_version != GOAL_PROJECTION_SCHEMA_VERSION {
        return None;
    }
    if projection.project != project {
        return None;
    }
    Some(projection)
}

fn render_fast_list(
    request: &GoalFastPathRequestWire,
    project: &str,
    projection: &GoalProjectionWire,
    root: &Path,
    json: bool,
    status: Option<String>,
) -> Result<GoalFastPathResponseWire, GoalLedgerError> {
    let filter = GoalListFilterWire {
        status,
        limit: None,
    };
    let list = match goal_ledger_list(root, &filter, None) {
        Ok(list) => list,
        Err(_) => return Ok(decline(project)),
    };
    if json {
        let mut stdout = serde_json::to_string(&list).map_err(|error| {
            GoalLedgerError::Data(format!("goal list serialize: {error}"))
        })?;
        stdout.push('\n');
        return Ok(handled_stdout(stdout, false));
    }
    let render_request = GoalRenderListRequestWire {
        goals: list.goals,
        project: project.to_string(),
        mode: projection.mode.clone(),
        synced_ago_seconds: watermark_age_seconds(
            &projection.watermark_path,
            &request.now,
        ),
        unpublished: outbox_pending(&projection.outbox_path),
        refreshing: false,
        color: request.color,
        compact: request.agent,
        now: request.now.clone(),
        empty_label: "active".to_string(),
    };
    let stdout = format!("{}\n", render_goal_list(&render_request));
    let spawn_fetch = should_spawn_fetch(projection, &request.now);
    Ok(handled_stdout(stdout, spawn_fetch))
}

fn render_fast_show(
    request: &GoalFastPathRequestWire,
    project: &str,
    projection: &GoalProjectionWire,
    root: &Path,
    raw_id: &str,
    json: bool,
) -> Result<GoalFastPathResponseWire, GoalLedgerError> {
    let (id_project, id) = split_goal_token(raw_id);
    if let Some(other) = id_project {
        if other != project {
            return Ok(decline(project));
        }
    }
    if parse_goal_id(&id).is_err() {
        return Ok(decline(project));
    }
    let state = match goal_ledger_show(root, &id, None) {
        Ok(state) => state,
        Err(_) => return Ok(decline(project)),
    };
    if json {
        let mut stdout = serde_json::to_string(&state).map_err(|error| {
            GoalLedgerError::Data(format!("goal show serialize: {error}"))
        })?;
        stdout.push('\n');
        return Ok(handled_stdout(stdout, false));
    }
    let render_request = GoalRenderCardRequestWire {
        state,
        now: request.now.clone(),
        color: request.color,
    };
    let _ = projection;
    let stdout = format!("{}\n", render_goal_card(&render_request));
    Ok(handled_stdout(stdout, false))
}

/// Split `goal:<project>@<id>`, `goal:<id>`, `⌖<id>`, or a bare id.
fn split_goal_token(token: &str) -> (Option<String>, String) {
    let token = token.strip_prefix('⌖').unwrap_or(token);
    let token = token.strip_prefix("goal:").unwrap_or(token);
    match token.split_once('@') {
        Some((project, id)) if !project.is_empty() && !id.is_empty() => {
            (Some(project.to_string()), id.to_string())
        }
        _ => (None, token.to_string()),
    }
}

fn handled_stdout(
    stdout: String,
    spawn_fetch: bool,
) -> GoalFastPathResponseWire {
    GoalFastPathResponseWire {
        handled: true,
        exit_code: 0,
        stdout,
        stderr: String::new(),
        spawn_fetch,
        project: String::new(),
    }
}

fn watermark_age_seconds(watermark_path: &str, now: &str) -> Option<f64> {
    if watermark_path.is_empty() {
        return None;
    }
    let mtime = fs::metadata(watermark_path).ok()?.modified().ok()?;
    let now_ts = DateTime::parse_from_rfc3339(now).ok()?.timestamp() as f64;
    let mtime_ts = mtime
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs_f64();
    Some((now_ts - mtime_ts).max(0.0))
}

fn outbox_pending(outbox_path: &str) -> bool {
    if outbox_path.is_empty() {
        return false;
    }
    let contents = fs::read_to_string(outbox_path).ok();
    let Some(contents) = contents else {
        return false;
    };
    let value: serde_json::Value =
        serde_json::from_str(&contents).unwrap_or_default();
    value
        .get("pending")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

fn should_spawn_fetch(projection: &GoalProjectionWire, now: &str) -> bool {
    if projection.mode != "shared" {
        return false;
    }
    let ttl = if projection.fetch_ttl_seconds > 0.0 {
        projection.fetch_ttl_seconds
    } else {
        GOAL_DEFAULT_FETCH_TTL_SECONDS
    };
    match watermark_age_seconds(&projection.watermark_path, now) {
        Some(age) => age > ttl,
        None => true,
    }
}

#[cfg(test)]
mod tests;
