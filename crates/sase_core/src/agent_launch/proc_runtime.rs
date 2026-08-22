//! Native stand-alone `%proc` dispatch helpers around [`CodeValue`].
//!
//! These helpers materialize a private 0600 script, build an argv vector
//! without interpolation, sanitize the documented proc environment, and
//! resolve workspace vs ordinary cwd policy. Child execution, logs, and
//! settlement stay in the existing proc supervisor.

use super::condition::{
    code_from_wire, condition_command_argv, write_private_file,
};
use crate::fenced_code::{
    language_from_info_string, CodeLanguage, CodeValueWire,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

pub const XPROMPT_PROC_ORIGIN: &str = "xprompt-proc";
pub const PROC_DISPATCH_WIRE_SCHEMA_VERSION: u32 = 1;
pub const PROC_PHASE_WAITING: &str = "waiting";
pub const PROC_PHASE_CHECKING: &str = "checking";
pub const PROC_PHASE_ACQUIRING_WORKSPACE: &str = "acquiring-workspace";
pub const PROC_PHASE_PREPARING_SCRIPT: &str = "preparing-script";
pub const PROC_PHASE_RUNNING: &str = "running";
pub const PROC_PHASE_SETTLING: &str = "settling";

const SCRIPT_BASH: &str = "script.sh";
const SCRIPT_PYTHON: &str = "script.py";
const DEFAULT_PATH: &str = "/usr/bin:/bin:/usr/local/bin";

/// Request to materialize one approved `%proc` body as a private script.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcDispatchRequestWire {
    pub schema_version: u32,
    pub logical_id: String,
    pub fingerprint: String,
    pub code: CodeValueWire,
    pub work_dir: String,
    pub python_executable: String,
    pub workspace: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_root: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_num: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proc_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell_name: Option<String>,
}

/// Prepared argv, cwd, environment, and script path for one `%proc`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcDispatchPreparedWire {
    pub schema_version: u32,
    pub argv: Vec<String>,
    pub cwd: String,
    pub env: BTreeMap<String, String>,
    pub script_path: String,
    pub code_digest: String,
    pub code_preview: String,
    pub code_language: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout_seconds: Option<u64>,
}

/// Reject workspace/cwd combinations that the planner already forbids.
pub fn validate_proc_workspace_intent(
    workspace: bool,
    selected_project: Option<&str>,
    declared_cwd: Option<&str>,
) -> Result<(), String> {
    if workspace && selected_project.map(str::trim).unwrap_or("").is_empty() {
        return Err(
            "workspace=true requires a selected project; workspace 0 is never guessed"
                .to_string(),
        );
    }
    if !workspace && declared_cwd.map(str::trim).unwrap_or("").is_empty() {
        return Err(
            "workspace=false requires an explicit ordinary cwd".to_string()
        );
    }
    Ok(())
}

/// Validate an optional bare stand-alone proc `%id` name.
pub fn validate_standalone_proc_shell_name(
    name: Option<&str>,
) -> Result<(), String> {
    let Some(name) = name.map(str::trim).filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    if name.contains("--") {
        return Err(
            "Proc %id names cannot use the agent-family `--` convention."
                .to_string(),
        );
    }
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err("Proc %id names must not be empty.".to_string());
    };
    let valid = (first.is_ascii_alphabetic() || first == '_')
        && chars.all(|ch| {
            ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '-')
        });
    if !valid {
        return Err(
            "Proc %id names must be bare identifiers containing only letters, digits, `_`, `.`, or `-`."
                .to_string(),
        );
    }
    Ok(())
}

/// Parse the SASE duration grammar (`20m`, `1h30m`, `5s`) into seconds.
pub fn parse_proc_duration_seconds(raw: &str) -> Result<u64, String> {
    let value = raw.trim();
    if value.is_empty() || !value.as_bytes()[0].is_ascii_digit() {
        return Err(format!("invalid proc duration {raw:?}"));
    }
    let bytes = value.as_bytes();
    let mut index = 0;
    let mut total = 0_u64;
    let mut previous_rank = 4_u8;
    let mut saw_unit = false;
    while index < bytes.len() {
        let number_start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        if number_start == index || index >= bytes.len() {
            return Err(format!("invalid proc duration {raw:?}"));
        }
        let amount: u64 = value[number_start..index]
            .parse()
            .map_err(|_| format!("invalid proc duration {raw:?}"))?;
        let (rank, multiplier) = match bytes[index] {
            b'h' => (3, 3_600_u64),
            b'm' => (2, 60_u64),
            b's' => (1, 1_u64),
            _ => return Err(format!("invalid proc duration {raw:?}")),
        };
        if rank >= previous_rank {
            return Err(format!("invalid proc duration {raw:?}"));
        }
        total = total
            .checked_add(amount.saturating_mul(multiplier))
            .ok_or_else(|| format!("invalid proc duration {raw:?}"))?;
        previous_rank = rank;
        saw_unit = true;
        index += 1;
    }
    if !saw_unit {
        return Err(format!("invalid proc duration {raw:?}"));
    }
    Ok(total)
}

/// Resolve the execution cwd for a workspace or ordinary `%proc`.
pub fn resolve_proc_execution_cwd(
    workspace: bool,
    declared_cwd: Option<&str>,
    source_cwd: Option<&str>,
    lease_root: Option<&str>,
) -> Result<String, String> {
    if workspace {
        let root = required_dir(lease_root, "leased workspace")?;
        let declared = declared_cwd.map(str::trim).filter(|v| !v.is_empty());
        let candidate = match declared {
            None => root.clone(),
            Some(raw) => {
                let path = PathBuf::from(raw);
                if path.is_absolute() {
                    path
                } else {
                    root.join(path)
                }
            }
        };
        return ensure_contained(&candidate, &root)
            .map(|path| path.to_string_lossy().into_owned());
    }
    let declared = declared_cwd
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            "workspace=false requires an explicit ordinary cwd".to_string()
        })?;
    let path = PathBuf::from(declared);
    let resolved = if path.is_absolute() {
        path
    } else {
        let source = required_dir(source_cwd, "source cwd")?;
        source.join(path)
    };
    if !resolved.is_dir() {
        return Err(format!("proc cwd does not exist: {}", resolved.display()));
    }
    resolved
        .canonicalize()
        .map(|path| path.to_string_lossy().into_owned())
        .map_err(|err| format!("proc cwd is unavailable: {err}"))
}

/// Documented sanitized environment for a stand-alone `%proc` child.
pub fn sanitized_proc_env(
    proc_id: &str,
    cwd: &Path,
    work_dir: &Path,
    python_executable: &str,
    selected_project: Option<&str>,
    project_file: Option<&str>,
    workspace_num: Option<u32>,
) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    let mut path = DEFAULT_PATH.to_string();
    if let Some(parent) = Path::new(python_executable).parent() {
        if !parent.as_os_str().is_empty() {
            path = format!("{}:{path}", parent.display());
        }
    }
    env.insert("PATH".to_string(), path);
    env.insert("LANG".to_string(), "C".to_string());
    env.insert("LC_ALL".to_string(), "C".to_string());
    env.insert("TZ".to_string(), "UTC".to_string());
    env.insert("HOME".to_string(), work_dir.to_string_lossy().into_owned());
    env.insert(
        "TMPDIR".to_string(),
        work_dir.to_string_lossy().into_owned(),
    );
    env.insert("PWD".to_string(), cwd.to_string_lossy().into_owned());
    env.insert("SASE_PROC_ID".to_string(), proc_id.to_string());
    if let Some(project) =
        selected_project.map(str::trim).filter(|v| !v.is_empty())
    {
        env.insert("SASE_PROJECT".to_string(), project.to_string());
    }
    if let Some(project_file) =
        project_file.map(str::trim).filter(|v| !v.is_empty())
    {
        env.insert("SASE_PROJECT_FILE".to_string(), project_file.to_string());
    }
    env.insert(
        "SASE_WORKSPACE".to_string(),
        cwd.to_string_lossy().into_owned(),
    );
    if let Some(workspace_num) = workspace_num {
        env.insert("SASE_WORKSPACE_NUM".to_string(), workspace_num.to_string());
    }
    env
}

/// Return the interpreter argv for a `%proc` script that will live in *work_dir*.
pub fn proc_script_argv(
    language: &str,
    work_dir: &Path,
    python_executable: &str,
) -> Result<Vec<String>, String> {
    let code_language = language_from_info_string(Some(language))?;
    let script_name = match code_language {
        CodeLanguage::Bash => SCRIPT_BASH,
        CodeLanguage::Python => SCRIPT_PYTHON,
    };
    Ok(condition_command_argv(
        code_language,
        &work_dir.join(script_name),
        python_executable,
    ))
}

/// Materialize the approved `%proc` source and return argv/cwd/env.
pub fn prepare_proc_script(
    request: &ProcDispatchRequestWire,
) -> Result<ProcDispatchPreparedWire, String> {
    if request.schema_version != PROC_DISPATCH_WIRE_SCHEMA_VERSION {
        return Err(format!(
            "unsupported proc dispatch schema_version {}",
            request.schema_version
        ));
    }
    validate_proc_workspace_intent(
        request.workspace,
        request.selected_project.as_deref(),
        request.declared_cwd.as_deref(),
    )?;
    validate_standalone_proc_shell_name(request.shell_name.as_deref())?;
    let timeout_seconds = optional_duration(request.timeout.as_deref())?;
    let idle_timeout_seconds =
        optional_duration(request.idle_timeout.as_deref())?;
    let code = code_from_wire(&request.code)?;
    let work_dir = PathBuf::from(&request.work_dir);
    fs::create_dir_all(&work_dir)
        .map_err(|err| format!("failed to create proc work dir: {err}"))?;
    let cwd = PathBuf::from(resolve_proc_execution_cwd(
        request.workspace,
        request.declared_cwd.as_deref(),
        request.source_cwd.as_deref(),
        request.lease_root.as_deref(),
    )?);
    let script_name = match code.language {
        CodeLanguage::Bash => SCRIPT_BASH,
        CodeLanguage::Python => SCRIPT_PYTHON,
    };
    let script_path = work_dir.join(script_name);
    write_private_file(&script_path, code.source.as_bytes())
        .map_err(|err| format!("failed to materialize proc script: {err}"))?;
    let materialized = fs::read_to_string(&script_path)
        .map_err(|err| format!("failed to read proc script: {err}"))?;
    if materialized != code.source {
        return Err(
            "proc script contents do not match the approved source".to_string()
        );
    }
    let digest = code.digest();
    if !request.code.digest.is_empty() && request.code.digest != digest {
        return Err("proc code digest mismatch".to_string());
    }
    let python_executable = request.python_executable.trim();
    if python_executable.is_empty() {
        return Err("python executable is required".to_string());
    }
    let argv =
        condition_command_argv(code.language, &script_path, python_executable);
    let proc_id = request
        .proc_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(request.logical_id.as_str());
    let env = sanitized_proc_env(
        proc_id,
        &cwd,
        &work_dir,
        python_executable,
        request.selected_project.as_deref(),
        request.project_file.as_deref(),
        request.workspace_num,
    );
    if env.contains_key("SASE_AGENT")
        || env.keys().any(|key| key.starts_with("SASE_AGENT_"))
    {
        return Err("proc environment must not set SASE_AGENT".to_string());
    }
    Ok(ProcDispatchPreparedWire {
        schema_version: PROC_DISPATCH_WIRE_SCHEMA_VERSION,
        argv,
        cwd: cwd.to_string_lossy().into_owned(),
        env,
        script_path: script_path.to_string_lossy().into_owned(),
        code_digest: digest,
        code_preview: code.preview(),
        code_language: code.language.as_str().to_string(),
        timeout_seconds,
        idle_timeout_seconds,
    })
}

/// Remove private script inputs while leaving digest metadata elsewhere.
pub fn cleanup_proc_private_inputs(work_dir: &Path) {
    for name in [SCRIPT_BASH, SCRIPT_PYTHON] {
        let _ = fs::remove_file(work_dir.join(name));
    }
}

fn optional_duration(raw: Option<&str>) -> Result<Option<u64>, String> {
    match raw.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(None),
        Some(value) => parse_proc_duration_seconds(value).map(Some),
    }
}

fn required_dir(raw: Option<&str>, label: &str) -> Result<PathBuf, String> {
    let value = raw
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .ok_or_else(|| format!("{label} is required"))?;
    let path = PathBuf::from(value);
    if !path.is_dir() {
        return Err(format!("{label} does not exist: {value}"));
    }
    path.canonicalize()
        .map_err(|err| format!("{label} is unavailable: {err}"))
}

fn ensure_contained(path: &Path, root: &Path) -> Result<PathBuf, String> {
    let canonical_root = root
        .canonicalize()
        .map_err(|err| format!("leased workspace is unavailable: {err}"))?;
    let canonical_path = path
        .canonicalize()
        .map_err(|err| format!("proc cwd is unavailable: {err}"))?;
    if !canonical_path.starts_with(&canonical_root) {
        return Err(format!(
            "proc cwd {} escapes leased workspace {}",
            canonical_path.display(),
            canonical_root.display()
        ));
    }
    if !canonical_path.is_dir() {
        return Err(format!(
            "proc cwd does not exist: {}",
            canonical_path.display()
        ));
    }
    Ok(canonical_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fenced_code::{CodeLanguage, CodeValue};
    use std::os::unix::fs::PermissionsExt;
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

    fn request(
        work_dir: &TempDir,
        code: CodeValueWire,
    ) -> ProcDispatchRequestWire {
        ProcDispatchRequestWire {
            schema_version: PROC_DISPATCH_WIRE_SCHEMA_VERSION,
            logical_id: "unit-1".to_string(),
            fingerprint: "fp".to_string(),
            code,
            work_dir: work_dir.path().to_string_lossy().into_owned(),
            python_executable: std::env::current_exe()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            workspace: false,
            source_cwd: Some(work_dir.path().to_string_lossy().into_owned()),
            lease_root: None,
            declared_cwd: Some(work_dir.path().to_string_lossy().into_owned()),
            selected_project: None,
            project_file: None,
            workspace_num: None,
            proc_id: Some("proc-one".to_string()),
            timeout: Some("20m".to_string()),
            idle_timeout: Some("5m".to_string()),
            shell_name: Some("checks".to_string()),
        }
    }

    #[test]
    fn prepare_bash_script_uses_argv_without_interpolation() {
        let temp = TempDir::new().unwrap();
        let prepared =
            prepare_proc_script(&request(&temp, bash_code("echo ready")))
                .unwrap();
        assert_eq!(
            prepared.argv[..3],
            [
                "/bin/bash".to_string(),
                "--noprofile".to_string(),
                "--norc".to_string()
            ]
        );
        assert_eq!(prepared.code_language, "bash");
        assert_eq!(prepared.timeout_seconds, Some(20 * 60));
        assert_eq!(prepared.idle_timeout_seconds, Some(5 * 60));
        let mode = fs::metadata(&prepared.script_path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        assert_eq!(prepared.env.get("SASE_PROC_ID").unwrap(), "proc-one");
        assert!(!prepared.env.contains_key("SASE_AGENT"));
        cleanup_proc_private_inputs(temp.path());
        assert!(!Path::new(&prepared.script_path).exists());
    }

    #[test]
    fn prepare_python_script_uses_sase_interpreter() {
        let temp = TempDir::new().unwrap();
        let mut req = request(&temp, python_code("print('ready')"));
        req.python_executable = "/opt/sase/bin/python".to_string();
        let prepared = prepare_proc_script(&req).unwrap();
        assert_eq!(prepared.argv[0], "/opt/sase/bin/python");
        assert_eq!(prepared.code_language, "python");
        assert!(prepared
            .env
            .get("PATH")
            .unwrap()
            .starts_with("/opt/sase/bin:"));
    }

    #[test]
    fn workspace_true_without_project_is_rejected() {
        let error = validate_proc_workspace_intent(true, None, Some("/tmp"))
            .unwrap_err();
        assert!(error.contains("selected project"));
    }

    #[test]
    fn workspace_false_without_cwd_is_rejected() {
        let error = validate_proc_workspace_intent(false, Some("sase"), None)
            .unwrap_err();
        assert!(error.contains("ordinary cwd"));
    }

    #[test]
    fn relative_cwd_stays_inside_the_lease() {
        let temp = TempDir::new().unwrap();
        let nested = temp.path().join("src");
        fs::create_dir(&nested).unwrap();
        let cwd = resolve_proc_execution_cwd(
            true,
            Some("src"),
            None,
            Some(temp.path().to_str().unwrap()),
        )
        .unwrap();
        assert_eq!(PathBuf::from(&cwd), nested.canonicalize().unwrap());
    }

    #[test]
    fn relative_cwd_rejects_parent_escape() {
        let temp = TempDir::new().unwrap();
        let error = resolve_proc_execution_cwd(
            true,
            Some(".."),
            None,
            Some(temp.path().to_str().unwrap()),
        )
        .unwrap_err();
        assert!(error.contains("escapes"));
    }

    #[test]
    fn symlink_cwd_cannot_escape_the_lease() {
        let temp = TempDir::new().unwrap();
        let lease = temp.path().join("lease");
        let outside = temp.path().join("outside");
        fs::create_dir(&lease).unwrap();
        fs::create_dir(&outside).unwrap();
        let link = lease.join("escape");
        std::os::unix::fs::symlink(&outside, &link).unwrap();
        let error = resolve_proc_execution_cwd(
            true,
            Some("escape"),
            None,
            Some(lease.to_str().unwrap()),
        )
        .unwrap_err();
        assert!(error.contains("escapes"));
    }

    #[test]
    fn ordinary_cwd_requires_an_existing_directory() {
        let temp = TempDir::new().unwrap();
        let error = resolve_proc_execution_cwd(
            false,
            Some("missing"),
            Some(temp.path().to_str().unwrap()),
            None,
        )
        .unwrap_err();
        assert!(error.contains("does not exist"));
    }

    #[test]
    fn shell_names_reject_family_qualification() {
        validate_standalone_proc_shell_name(Some("checks")).unwrap();
        let error = validate_standalone_proc_shell_name(Some("agent--checks"))
            .unwrap_err();
        assert!(error.contains("`--`"));
    }

    #[test]
    fn duration_parser_matches_sase_grammar() {
        assert_eq!(parse_proc_duration_seconds("20m").unwrap(), 1200);
        assert_eq!(parse_proc_duration_seconds("1h30m15s").unwrap(), 5415);
        assert!(parse_proc_duration_seconds("m20").is_err());
        assert!(parse_proc_duration_seconds("1m1h").is_err());
    }

    #[test]
    fn phases_and_origin_are_stable() {
        assert_eq!(XPROMPT_PROC_ORIGIN, "xprompt-proc");
        assert_eq!(PROC_PHASE_WAITING, "waiting");
        assert_eq!(PROC_PHASE_CHECKING, "checking");
        assert_eq!(PROC_PHASE_ACQUIRING_WORKSPACE, "acquiring-workspace");
        assert_eq!(PROC_PHASE_PREPARING_SCRIPT, "preparing-script");
        assert_eq!(PROC_PHASE_RUNNING, "running");
        assert_eq!(PROC_PHASE_SETTLING, "settling");
    }
}
