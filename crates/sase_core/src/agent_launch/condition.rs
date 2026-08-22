//! Sandboxed `%if` admission evaluator around [`CodeValue`].
//!
//! Predicates run only after waits settle. Exit 0 is eligible, exit 1 is
//! skipped, and every other exit, signal, timeout, or execution failure is a
//! condition error. The evaluator never allocates a runner, workspace, agent,
//! or proc identity.

use super::{
    LaunchOutcomeWire, LaunchUnitPayloadWire, LaunchUnitWire, WaitTargetWire,
    WaitedOutcomeWire,
};
use crate::fenced_code::{
    language_from_info_string, CodeLanguage, CodeValue, CodeValueWire,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const CONDITION_CONTEXT_SCHEMA_VERSION: u32 = 1;
pub const CONDITION_EVAL_WIRE_SCHEMA_VERSION: u32 = 1;
pub const CONDITION_DEFAULT_TIMEOUT_SECONDS: f64 = 10.0;
pub const CONDITION_MAX_TIMEOUT_SECONDS: f64 = 30.0;
pub const CONDITION_OUTPUT_CAP_BYTES: usize = 4096;

const MIN_TIMEOUT_SECONDS: f64 = 0.05;
const MIN_OUTPUT_CAP_BYTES: usize = 256;
const MAX_OUTPUT_CAP_BYTES: usize = 65_536;
const SCRIPT_BASH: &str = "script.sh";
const SCRIPT_PYTHON: &str = "script.py";
const CONTEXT_FILENAME: &str = "context.json";
const CHECK_FILENAME: &str = "check.json";
const RESULT_FILENAME: &str = "result.json";
const STDOUT_FILENAME: &str = "stdout.log";
const STDERR_FILENAME: &str = "stderr.log";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConditionLogicalUnitWire {
    pub logical_id: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConditionWaitedOutcomeWire {
    pub target: WaitTargetWire,
    pub outcome: LaunchOutcomeWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub outputs: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConditionContextWire {
    pub schema_version: u32,
    pub logical_unit: ConditionLogicalUnitWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_project: Option<String>,
    #[serde(default)]
    pub safe_inputs: BTreeMap<String, Value>,
    #[serde(default)]
    pub waited_outcomes: Vec<ConditionWaitedOutcomeWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConditionEvalRequestWire {
    pub schema_version: u32,
    pub logical_id: String,
    pub code: CodeValueWire,
    pub work_dir: String,
    pub python_executable: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_cap_bytes: Option<usize>,
    pub context: ConditionContextWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_path: Option<String>,
    #[serde(default)]
    pub share_workspace: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConditionEvalResultWire {
    pub schema_version: u32,
    pub verdict: String,
    pub timed_out: bool,
    pub truncated: bool,
    pub cancelled: bool,
    pub code_digest: String,
    pub context_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pgid: Option<i32>,
    #[serde(default)]
    pub diagnostics: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConditionCheckWire {
    pub schema_version: u32,
    pub logical_id: String,
    pub code_digest: String,
    pub context_digest: String,
    pub started_at_unix: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pgid: Option<i32>,
}

pub fn build_condition_context(
    unit: &LaunchUnitWire,
    selected_project: Option<&str>,
    safe_inputs: BTreeMap<String, Value>,
    waited: &[WaitedOutcomeWire],
    share_workspace: bool,
) -> ConditionContextWire {
    let kind = match unit.payload {
        LaunchUnitPayloadWire::Agent(_) => "agent",
        LaunchUnitPayloadWire::Proc(_) => "proc",
    };
    let _ = share_workspace;
    let identity = match &unit.payload {
        LaunchUnitPayloadWire::Agent(agent) => agent.identity.clone(),
        LaunchUnitPayloadWire::Proc(proc_unit) => proc_unit.shell_name.clone(),
    };
    ConditionContextWire {
        schema_version: CONDITION_CONTEXT_SCHEMA_VERSION,
        logical_unit: ConditionLogicalUnitWire {
            logical_id: unit.logical_id.clone(),
            kind: kind.to_string(),
            identity,
        },
        selected_project: selected_project.map(str::to_string),
        safe_inputs: sanitize_safe_inputs(&Value::Object(
            safe_inputs.into_iter().collect(),
        )),
        waited_outcomes: waited
            .iter()
            .map(|item| ConditionWaitedOutcomeWire {
                target: item.target.clone(),
                outcome: item.outcome,
                identity: item.identity.clone(),
                message: item.message.clone(),
                workspace: None,
                outputs: BTreeMap::new(),
            })
            .collect(),
    }
}

pub fn sanitize_safe_inputs(value: &Value) -> BTreeMap<String, Value> {
    match value {
        Value::Object(map) => map
            .iter()
            .filter(|(key, _)| !is_secret_key(key))
            .filter_map(|(key, nested)| {
                sanitize_input_value(nested).map(|clean| (key.clone(), clean))
            })
            .collect(),
        _ => BTreeMap::new(),
    }
}

pub fn condition_context_digest(context: &ConditionContextWire) -> String {
    let encoded = serde_json::to_string(context).unwrap_or_default();
    hex::encode(Sha256::digest(encoded.as_bytes()))
}

pub fn classify_condition_status(
    exit_code: Option<i32>,
    signal: Option<i32>,
    timed_out: bool,
    exec_error: bool,
    cancelled: bool,
) -> &'static str {
    if cancelled || timed_out || exec_error || signal.is_some() {
        return "condition_error";
    }
    match exit_code {
        Some(0) => "eligible",
        Some(1) => "skipped",
        _ => "condition_error",
    }
}

pub fn condition_command_argv(
    language: CodeLanguage,
    script_path: &Path,
    python_executable: &str,
) -> Vec<String> {
    match language {
        CodeLanguage::Bash => vec![
            "/bin/bash".to_string(),
            "--noprofile".to_string(),
            "--norc".to_string(),
            script_path.to_string_lossy().into_owned(),
        ],
        CodeLanguage::Python => vec![
            python_executable.to_string(),
            script_path.to_string_lossy().into_owned(),
        ],
    }
}

pub fn sanitized_condition_env(
    context_path: &Path,
    cwd: &Path,
    work_dir: &Path,
) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    env.insert(
        "PATH".to_string(),
        "/usr/bin:/bin:/usr/local/bin".to_string(),
    );
    env.insert("LANG".to_string(), "C".to_string());
    env.insert("LC_ALL".to_string(), "C".to_string());
    env.insert("TZ".to_string(), "UTC".to_string());
    env.insert("HOME".to_string(), work_dir.to_string_lossy().into_owned());
    env.insert(
        "TMPDIR".to_string(),
        work_dir.to_string_lossy().into_owned(),
    );
    env.insert("PWD".to_string(), cwd.to_string_lossy().into_owned());
    env.insert(
        "SASE_CONDITION_CONTEXT".to_string(),
        context_path.to_string_lossy().into_owned(),
    );
    env
}

pub fn evaluate_launch_condition(
    request: &ConditionEvalRequestWire,
) -> ConditionEvalResultWire {
    match evaluate_launch_condition_inner(request) {
        Ok(result) => result,
        Err(result) => result,
    }
}

#[allow(clippy::result_large_err)]
fn evaluate_launch_condition_inner(
    request: &ConditionEvalRequestWire,
) -> Result<ConditionEvalResultWire, ConditionEvalResultWire> {
    let work_dir = PathBuf::from(&request.work_dir);
    if let Err(err) = fs::create_dir_all(&work_dir) {
        return Err(error_result(
            request,
            "",
            "",
            format!("failed to create condition work dir: {err}"),
        ));
    }
    let code = code_from_wire(&request.code).map_err(|message| {
        error_result(request, &request.code.digest, "", message)
    })?;
    let code_digest = code.digest();
    let mut context = request.context.clone();
    context.schema_version = CONDITION_CONTEXT_SCHEMA_VERSION;
    context.safe_inputs = sanitize_safe_inputs(&Value::Object(
        context.safe_inputs.clone().into_iter().collect(),
    ));
    if !request.share_workspace {
        for waited in &mut context.waited_outcomes {
            waited.workspace = None;
            waited.outputs.clear();
        }
    }
    let context_digest = condition_context_digest(&context);
    let cwd = resolve_cwd(request.cwd.as_deref()).map_err(|message| {
        error_result(request, &code_digest, &context_digest, message)
    })?;
    let timeout = resolve_timeout(request.timeout_seconds);
    let output_cap = resolve_output_cap(request.output_cap_bytes);
    let script_name = match code.language {
        CodeLanguage::Bash => SCRIPT_BASH,
        CodeLanguage::Python => SCRIPT_PYTHON,
    };
    let script_path = work_dir.join(script_name);
    let context_path = work_dir.join(CONTEXT_FILENAME);
    write_private_file(&script_path, code.source.as_bytes()).map_err(
        |err| {
            error_result(
                request,
                &code_digest,
                &context_digest,
                format!("failed to materialize condition script: {err}"),
            )
        },
    )?;
    let context_bytes = serde_json::to_vec_pretty(&context).map_err(|err| {
        error_result(
            request,
            &code_digest,
            &context_digest,
            format!("failed to serialize condition context: {err}"),
        )
    })?;
    write_private_file(&context_path, &context_bytes).map_err(|err| {
        error_result(
            request,
            &code_digest,
            &context_digest,
            format!("failed to write condition context: {err}"),
        )
    })?;

    let python_executable = resolve_program(&request.python_executable);
    let argv = condition_command_argv(
        code.language,
        &script_path,
        &python_executable.to_string_lossy(),
    );
    let env = sanitized_condition_env(&context_path, &cwd, &work_dir);
    let stdout_path = work_dir.join(STDOUT_FILENAME);
    let stderr_path = work_dir.join(STDERR_FILENAME);
    let stdout_file = File::create(&stdout_path).map_err(|err| {
        error_result(
            request,
            &code_digest,
            &context_digest,
            format!("failed to open condition stdout: {err}"),
        )
    })?;
    let stderr_file = File::create(&stderr_path).map_err(|err| {
        error_result(
            request,
            &code_digest,
            &context_digest,
            format!("failed to open condition stderr: {err}"),
        )
    })?;
    let Some((program, args)) = argv.split_first() else {
        return Err(error_result(
            request,
            &code_digest,
            &context_digest,
            "condition argv must not be empty".to_string(),
        ));
    };
    if !Path::new(program).exists() {
        cleanup_private_inputs(&work_dir);
        return Err(error_result(
            request,
            &code_digest,
            &context_digest,
            format!("missing condition interpreter: {program}"),
        ));
    }

    let mut command = Command::new(program);
    command
        .args(args)
        .current_dir(&cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file))
        .env_clear()
        .envs(&env);
    configure_process_group(&mut command);

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            cleanup_private_inputs(&work_dir);
            return Err(error_result(
                request,
                &code_digest,
                &context_digest,
                format!("failed to execute condition: {err}"),
            ));
        }
    };
    let pid = child.id();
    let pgid = pid as i32;
    write_check(
        &work_dir,
        &ConditionCheckWire {
            schema_version: CONDITION_EVAL_WIRE_SCHEMA_VERSION,
            logical_id: request.logical_id.clone(),
            code_digest: code_digest.clone(),
            context_digest: context_digest.clone(),
            started_at_unix: unix_now(),
            pid: Some(pid),
            pgid: Some(pgid),
        },
    );

    let cancel_path = request.cancel_path.as_ref().map(PathBuf::from);
    let wait = wait_for_child(
        &mut child,
        pgid,
        Duration::from_secs_f64(timeout),
        cancel_path.as_deref(),
    );
    let (stdout_tail, stdout_trunc) = file_tail(&stdout_path, output_cap);
    let (stderr_tail, stderr_trunc) = file_tail(&stderr_path, output_cap);
    let truncated = stdout_trunc || stderr_trunc;
    cleanup_private_inputs(&work_dir);

    let (exit_code, signal, timed_out, cancelled, exec_error, wait_message) =
        match wait {
            WaitOutcome::Exited(status) => {
                let (code, sig) = status_parts(status);
                (code, sig, false, false, false, None)
            }
            WaitOutcome::TimedOut { status } => {
                let (code, sig) =
                    status.map(status_parts).unwrap_or((None, None));
                (
                    code,
                    sig,
                    true,
                    false,
                    false,
                    Some(format!("condition timed out after {timeout}s")),
                )
            }
            WaitOutcome::Cancelled { status } => {
                let (code, sig) =
                    status.map(status_parts).unwrap_or((None, None));
                (code, sig, false, true, false, Some("cancelled".to_string()))
            }
            WaitOutcome::WaitError(message) => {
                (None, None, false, false, true, Some(message))
            }
        };
    let verdict = classify_condition_status(
        exit_code, signal, timed_out, exec_error, cancelled,
    )
    .to_string();
    let diagnostics = format_diagnostics(&stdout_tail, &stderr_tail);
    let message =
        wait_message.or_else(|| match (verdict.as_str(), signal, exit_code) {
            ("eligible", _, _) => None,
            ("skipped", _, _) => Some("predicate exited 1".to_string()),
            (_, Some(signal), _) => {
                Some(format!("condition terminated by signal {signal}"))
            }
            (_, _, Some(exit_code)) => {
                Some(format!("predicate exited {exit_code}"))
            }
            _ => Some("condition_error".to_string()),
        });
    let result = ConditionEvalResultWire {
        schema_version: CONDITION_EVAL_WIRE_SCHEMA_VERSION,
        verdict,
        timed_out,
        truncated,
        cancelled,
        code_digest,
        context_digest,
        exit_code,
        signal,
        pid: Some(pid),
        pgid: Some(pgid),
        diagnostics,
        message,
    };
    write_result(&work_dir, &result);
    Ok(result)
}

fn code_from_wire(code: &CodeValueWire) -> Result<CodeValue, String> {
    let language = language_from_info_string(Some(code.language.as_str()))
        .or_else(|_| language_from_info_string(code.info_string.as_deref()))?;
    let value = CodeValue {
        source: code.source.clone(),
        language,
        info_string: code.info_string.clone(),
    };
    if !code.digest.is_empty() && code.digest != value.digest() {
        return Err("code digest mismatch".to_string());
    }
    Ok(value)
}

fn resolve_program(program: &str) -> PathBuf {
    let candidate = PathBuf::from(program);
    if candidate.is_absolute() {
        return candidate;
    }
    if candidate.exists() {
        return candidate;
    }
    if let Some(path_var) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path_var) {
            let joined = dir.join(program);
            if joined.is_file() {
                return joined;
            }
        }
    }
    candidate
}

fn resolve_cwd(cwd: Option<&str>) -> Result<PathBuf, String> {
    match cwd {
        Some(raw) if !raw.is_empty() => {
            let path = PathBuf::from(raw);
            if path.is_dir() {
                Ok(path)
            } else {
                Err(format!("condition cwd does not exist: {raw}"))
            }
        }
        _ => std::env::current_dir()
            .map_err(|err| format!("condition cwd is unavailable: {err}")),
    }
}

fn resolve_timeout(requested: Option<f64>) -> f64 {
    requested
        .unwrap_or(CONDITION_DEFAULT_TIMEOUT_SECONDS)
        .clamp(MIN_TIMEOUT_SECONDS, CONDITION_MAX_TIMEOUT_SECONDS)
}

fn resolve_output_cap(requested: Option<usize>) -> usize {
    requested
        .unwrap_or(CONDITION_OUTPUT_CAP_BYTES)
        .clamp(MIN_OUTPUT_CAP_BYTES, MAX_OUTPUT_CAP_BYTES)
}

fn is_secret_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    [
        "secret",
        "password",
        "token",
        "api_key",
        "apikey",
        "authorization",
        "credential",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn sanitize_input_value(value: &Value) -> Option<Value> {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            Some(value.clone())
        }
        Value::Object(_) => {
            let nested = sanitize_safe_inputs(value);
            if nested.is_empty() {
                None
            } else {
                Some(Value::Object(nested.into_iter().collect()))
            }
        }
        Value::Array(_) => None,
    }
}

fn configure_process_group(command: &mut Command) {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    #[cfg(not(unix))]
    {
        let _ = command;
    }
}

enum WaitOutcome {
    Exited(std::process::ExitStatus),
    TimedOut {
        status: Option<std::process::ExitStatus>,
    },
    Cancelled {
        status: Option<std::process::ExitStatus>,
    },
    WaitError(String),
}

fn wait_for_child(
    child: &mut std::process::Child,
    pgid: i32,
    timeout: Duration,
    cancel_path: Option<&Path>,
) -> WaitOutcome {
    let start = Instant::now();
    loop {
        if let Some(path) = cancel_path {
            if path.is_file() {
                kill_group(pgid);
                let status = child.wait().ok();
                return WaitOutcome::Cancelled { status };
            }
        }
        match child.try_wait() {
            Ok(Some(status)) => return WaitOutcome::Exited(status),
            Ok(None) => {
                if start.elapsed() >= timeout {
                    kill_group(pgid);
                    let status = child.wait().ok();
                    return WaitOutcome::TimedOut { status };
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(err) => return WaitOutcome::WaitError(err.to_string()),
        }
    }
}

fn kill_group(pgid: i32) {
    #[cfg(unix)]
    unsafe {
        libc::killpg(pgid, libc::SIGKILL);
    }
    let _ = pgid;
}

fn status_parts(
    status: std::process::ExitStatus,
) -> (Option<i32>, Option<i32>) {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        (status.code(), status.signal())
    }
    #[cfg(not(unix))]
    {
        (status.code(), None)
    }
}

fn file_tail(path: &Path, cap: usize) -> (String, bool) {
    let mut bytes = Vec::new();
    if let Ok(mut file) = File::open(path) {
        let _ = file.read_to_end(&mut bytes);
    }
    let truncated = bytes.len() > cap;
    let slice = if truncated {
        &bytes[bytes.len() - cap..]
    } else {
        bytes.as_slice()
    };
    (String::from_utf8_lossy(slice).into_owned(), truncated)
}

fn format_diagnostics(stdout: &str, stderr: &str) -> String {
    format!(
        "stdout:\n{}\nstderr:\n{}",
        stdout.trim_end(),
        stderr.trim_end()
    )
}

fn write_private_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    {
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

fn write_check(work_dir: &Path, check: &ConditionCheckWire) {
    if let Ok(bytes) = serde_json::to_vec_pretty(check) {
        let _ = write_private_file(&work_dir.join(CHECK_FILENAME), &bytes);
    }
}

fn write_result(work_dir: &Path, result: &ConditionEvalResultWire) {
    if let Ok(bytes) = serde_json::to_vec_pretty(result) {
        let _ = write_private_file(&work_dir.join(RESULT_FILENAME), &bytes);
    }
}

fn cleanup_private_inputs(work_dir: &Path) {
    for name in [
        SCRIPT_BASH,
        SCRIPT_PYTHON,
        CONTEXT_FILENAME,
        STDOUT_FILENAME,
        STDERR_FILENAME,
    ] {
        let _ = fs::remove_file(work_dir.join(name));
    }
}

fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(0.0)
}

fn error_result(
    request: &ConditionEvalRequestWire,
    code_digest: &str,
    context_digest: &str,
    message: String,
) -> ConditionEvalResultWire {
    let result = ConditionEvalResultWire {
        schema_version: CONDITION_EVAL_WIRE_SCHEMA_VERSION,
        verdict: "condition_error".to_string(),
        timed_out: false,
        truncated: false,
        cancelled: false,
        code_digest: code_digest.to_string(),
        context_digest: context_digest.to_string(),
        exit_code: None,
        signal: None,
        pid: None,
        pgid: None,
        diagnostics: String::new(),
        message: Some(message),
    };
    if !request.work_dir.is_empty() {
        write_result(Path::new(&request.work_dir), &result);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_launch::{
        AgentUnitWire, LaunchConditionWire, LaunchUnitPayloadWire,
        LaunchUnitWire,
    };
    use tempfile::TempDir;

    fn bash_code(source: &str) -> CodeValueWire {
        CodeValue {
            source: source.to_string(),
            language: CodeLanguage::Bash,
            info_string: Some("bash".to_string()),
        }
        .to_wire()
    }

    fn python_code(source: &str) -> CodeValueWire {
        CodeValue {
            source: source.to_string(),
            language: CodeLanguage::Python,
            info_string: Some("python".to_string()),
        }
        .to_wire()
    }

    fn python_bin() -> String {
        std::env::var("PYO3_PYTHON")
            .or_else(|_| std::env::var("PYTHON"))
            .unwrap_or_else(|_| "python3".to_string())
    }

    fn request(
        work_dir: &Path,
        code: CodeValueWire,
        source: Option<&str>,
    ) -> ConditionEvalRequestWire {
        let logical_id = "unit-1".to_string();
        let context = ConditionContextWire {
            schema_version: CONDITION_CONTEXT_SCHEMA_VERSION,
            logical_unit: ConditionLogicalUnitWire {
                logical_id: logical_id.clone(),
                kind: "agent".to_string(),
                identity: Some("reviewer".to_string()),
            },
            selected_project: Some("sase".to_string()),
            safe_inputs: BTreeMap::from([(
                "task".to_string(),
                Value::String("review".to_string()),
            )]),
            waited_outcomes: Vec::new(),
        };
        ConditionEvalRequestWire {
            schema_version: CONDITION_EVAL_WIRE_SCHEMA_VERSION,
            logical_id,
            code,
            work_dir: work_dir.to_string_lossy().into_owned(),
            python_executable: python_bin(),
            cwd: source.map(str::to_string),
            timeout_seconds: Some(2.0),
            output_cap_bytes: Some(1024),
            context,
            cancel_path: None,
            share_workspace: false,
        }
    }

    fn mode(path: &Path) -> u32 {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::metadata(path).unwrap().permissions().mode() & 0o777
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            0o600
        }
    }

    #[test]
    fn classify_exit_classes() {
        assert_eq!(
            classify_condition_status(Some(0), None, false, false, false),
            "eligible"
        );
        assert_eq!(
            classify_condition_status(Some(1), None, false, false, false),
            "skipped"
        );
        assert_eq!(
            classify_condition_status(Some(2), None, false, false, false),
            "condition_error"
        );
        assert_eq!(
            classify_condition_status(None, Some(9), false, false, false),
            "condition_error"
        );
        assert_eq!(
            classify_condition_status(None, None, true, false, false),
            "condition_error"
        );
        assert_eq!(
            classify_condition_status(Some(0), None, false, false, true),
            "condition_error"
        );
    }

    #[test]
    fn bash_exit_zero_is_eligible() {
        let dir = TempDir::new().unwrap();
        let result = evaluate_launch_condition(&request(
            dir.path(),
            bash_code("exit 0\n"),
            Some(dir.path().to_str().unwrap()),
        ));
        assert_eq!(result.verdict, "eligible");
        assert_eq!(result.exit_code, Some(0));
        assert!(dir.path().join(RESULT_FILENAME).is_file());
        assert_eq!(mode(&dir.path().join(RESULT_FILENAME)), 0o600);
        assert!(!dir.path().join(SCRIPT_BASH).exists());
        assert!(!dir.path().join(CONTEXT_FILENAME).exists());
    }

    #[test]
    fn bash_exit_one_is_skipped() {
        let dir = TempDir::new().unwrap();
        let result = evaluate_launch_condition(&request(
            dir.path(),
            bash_code("exit 1\n"),
            Some(dir.path().to_str().unwrap()),
        ));
        assert_eq!(result.verdict, "skipped");
        assert_eq!(result.message.as_deref(), Some("predicate exited 1"));
    }

    #[test]
    fn bash_exit_two_is_condition_error() {
        let dir = TempDir::new().unwrap();
        let result = evaluate_launch_condition(&request(
            dir.path(),
            bash_code("exit 2\n"),
            Some(dir.path().to_str().unwrap()),
        ));
        assert_eq!(result.verdict, "condition_error");
        assert_eq!(result.exit_code, Some(2));
    }

    #[test]
    fn python_reads_condition_context_and_matches_bash_skip() {
        let dir = TempDir::new().unwrap();
        let source = r#"
import json, os, sys
path = os.environ["SASE_CONDITION_CONTEXT"]
payload = json.load(open(path, encoding="utf-8"))
assert payload["logical_unit"]["logical_id"] == "unit-1"
assert "AWS_SECRET_ACCESS_KEY" not in os.environ
assert "SASE_AGENT" not in os.environ
raise SystemExit(1)
"#;
        let result = evaluate_launch_condition(&request(
            dir.path(),
            python_code(source),
            Some(dir.path().to_str().unwrap()),
        ));
        assert_eq!(result.verdict, "skipped");
        assert_eq!(result.exit_code, Some(1));
    }

    #[test]
    fn timeout_kills_process_group() {
        let dir = TempDir::new().unwrap();
        let mut req = request(
            dir.path(),
            bash_code("sleep 30\n"),
            Some(dir.path().to_str().unwrap()),
        );
        req.timeout_seconds = Some(0.2);
        let result = evaluate_launch_condition(&req);
        assert_eq!(result.verdict, "condition_error");
        assert!(result.timed_out);
        assert!(result.message.unwrap().contains("timed out"));
    }

    #[test]
    fn output_is_truncated_and_cwd_missing_is_error() {
        let dir = TempDir::new().unwrap();
        let mut req = request(
            dir.path(),
            bash_code(
                "i=0; while [ \"$i\" -lt 8000 ]; do printf x; i=$((i+1)); done; echo\n",
            ),
            Some(dir.path().to_str().unwrap()),
        );
        req.output_cap_bytes = Some(256);
        let result = evaluate_launch_condition(&req);
        assert_eq!(result.verdict, "eligible");
        assert!(result.truncated);

        let missing = dir.path().join("gone");
        let result = evaluate_launch_condition(&request(
            dir.path(),
            bash_code("exit 0\n"),
            Some(missing.to_str().unwrap()),
        ));
        assert_eq!(result.verdict, "condition_error");
        assert!(result
            .message
            .unwrap()
            .contains("condition cwd does not exist"));
    }

    #[test]
    fn missing_interpreter_and_digest_mismatch_are_errors() {
        let dir = TempDir::new().unwrap();
        let mut req = request(
            dir.path(),
            python_code("raise SystemExit(0)\n"),
            Some(dir.path().to_str().unwrap()),
        );
        req.python_executable = "/nonexistent/sase-python".to_string();
        let result = evaluate_launch_condition(&req);
        assert_eq!(result.verdict, "condition_error");
        assert!(result
            .message
            .unwrap()
            .contains("missing condition interpreter"));

        let mut bad = bash_code("exit 0\n");
        bad.digest = "a".repeat(64);
        let result = evaluate_launch_condition(&request(
            dir.path(),
            bad,
            Some(dir.path().to_str().unwrap()),
        ));
        assert_eq!(result.verdict, "condition_error");
        assert_eq!(result.message.as_deref(), Some("code digest mismatch"));
    }

    #[test]
    fn secret_inputs_are_stripped_and_workspace_is_not_shared() {
        let unit = LaunchUnitWire {
            logical_id: "unit-1".to_string(),
            source_order: 0,
            waits: Vec::new(),
            condition: Some(LaunchConditionWire {
                code: bash_code("exit 0\n"),
                cwd: None,
                context_fields: vec!["waited_outcomes".to_string()],
            }),
            payload: LaunchUnitPayloadWire::Agent(AgentUnitWire {
                prompt: "Do work".to_string(),
                identity: Some("reviewer".to_string()),
                identity_explicit: true,
                model: None,
                reasoning_effort: None,
                bead_id: None,
                hidden: false,
                auto_enabled: false,
                auto_mode: None,
                finalizers: Vec::new(),
                wait_runners: None,
                wait_priority: None,
            }),
        };
        let mut inputs = BTreeMap::new();
        inputs.insert("task".to_string(), Value::String("ok".to_string()));
        inputs.insert(
            "api_token".to_string(),
            Value::String("should-not-leak".to_string()),
        );
        let waited = vec![WaitedOutcomeWire {
            target: WaitTargetWire::Logical {
                logical_id: "unit-0".to_string(),
                source: Some("%wait".to_string()),
            },
            outcome: LaunchOutcomeWire::Skipped,
            identity: None,
            message: Some("predicate exited 1".to_string()),
        }];
        let context = build_condition_context(
            &unit,
            Some("sase"),
            inputs,
            &waited,
            false,
        );
        assert_eq!(
            context.safe_inputs.get("task"),
            Some(&Value::String("ok".to_string()))
        );
        assert!(!context.safe_inputs.contains_key("api_token"));
        assert!(context.waited_outcomes[0].workspace.is_none());
        assert!(context.waited_outcomes[0].outputs.is_empty());
        assert_eq!(condition_context_digest(&context).len(), 64);
    }

    #[test]
    fn cancel_path_settles_as_condition_error() {
        let dir = TempDir::new().unwrap();
        let cancel = dir.path().join("cancel");
        File::create(&cancel).unwrap();
        let mut req = request(
            dir.path(),
            bash_code("sleep 10\n"),
            Some(dir.path().to_str().unwrap()),
        );
        req.cancel_path = Some(cancel.to_string_lossy().into_owned());
        req.timeout_seconds = Some(2.0);
        let result = evaluate_launch_condition(&req);
        assert_eq!(result.verdict, "condition_error");
        assert!(result.cancelled);
    }

    #[test]
    fn argv_is_not_interpolated() {
        let argv = condition_command_argv(
            CodeLanguage::Bash,
            Path::new("/tmp/script.sh"),
            "python3",
        );
        assert_eq!(
            argv,
            vec!["/bin/bash", "--noprofile", "--norc", "/tmp/script.sh"]
        );
        let argv = condition_command_argv(
            CodeLanguage::Python,
            Path::new("/tmp/script.py"),
            "/usr/bin/python3",
        );
        assert_eq!(argv, vec!["/usr/bin/python3", "/tmp/script.py"]);
    }
}
