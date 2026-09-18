//! Shared owner-host process liveness observations.
//!
//! Runner occupancy and gateway snapshot construction use the same
//! non-zombie, SASE/Python process classifier. Record identity evidence
//! (home running marker or project workspace claim) is applied when the
//! record has that shape; workflow records without those claim shapes
//! still receive the process classifier rather than being rejected solely
//! for looking different. Callers inject process and identity probes so
//! tests never depend on whichever PIDs happen to exist on the host.

use std::collections::BTreeMap;
use std::fs;
#[cfg(target_os = "linux")]
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::agent_launch::{
    list_workspace_claims_from_content, WorkspaceClaimWire,
};
use crate::agent_scan::{AgentArtifactRecordWire, RunningMarkerWire};
use crate::fleet_contract::OwnerLivenessWire;

/// Linux `/proc` observation of one PID, independent of record identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostProcessObservation {
    /// PID exists, is non-zombie, and looks like a SASE/Python process.
    /// When `/proc` is unavailable, a successful existence probe is also
    /// classified here so portable hosts keep working.
    LiveAgent,
    /// PID exists but is a zombie.
    Zombie,
    /// PID exists but its command line is not a SASE/Python agent.
    WrongCommand,
    /// PID does not exist, or its `/proc` entries vanished.
    Missing,
}

/// Whether a record's home marker or project workspace claim matches `pid`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordIdentityMatch {
    /// Home running marker or project claim confirms this PID.
    Matches,
    /// The record has a claim/marker shape, but it does not name this PID.
    Mismatch,
    /// The record has no home-marker or workspace-claim shape to check.
    NotApplicable,
}

/// Owner-facing process observation used to populate [`OwnerLivenessWire`]
/// plus the identity-mismatch exclusion bit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerProcessObservation {
    Alive,
    Dead,
    NotProcess,
    Unknown,
    /// The PID is live but is not this agent (wrong command or claim
    /// mismatch). Mapped to [`OwnerLivenessWire::Dead`] on the wire.
    IdentityMismatch,
}

impl OwnerProcessObservation {
    pub fn liveness(self) -> OwnerLivenessWire {
        match self {
            Self::Alive => OwnerLivenessWire::Alive,
            Self::Dead | Self::IdentityMismatch => OwnerLivenessWire::Dead,
            Self::NotProcess => OwnerLivenessWire::NotProcess,
            Self::Unknown => OwnerLivenessWire::Unknown,
        }
    }

    pub fn process_identity_mismatch(self) -> bool {
        matches!(self, Self::IdentityMismatch)
    }
}

pub trait HostProcessProbe {
    fn observe(&self, pid: i32) -> HostProcessObservation;
}

pub trait RecordIdentityProbe {
    fn match_identity(
        &self,
        record: &AgentArtifactRecordWire,
        pid: i64,
    ) -> RecordIdentityMatch;
}

pub trait OwnerLivenessObserver: Send + Sync {
    fn observe(
        &self,
        record: &AgentArtifactRecordWire,
    ) -> OwnerProcessObservation;
}

impl<F> HostProcessProbe for F
where
    F: Fn(i32) -> HostProcessObservation,
{
    fn observe(&self, pid: i32) -> HostProcessObservation {
        self(pid)
    }
}

impl<F> RecordIdentityProbe for F
where
    F: for<'a> Fn(&'a AgentArtifactRecordWire, i64) -> RecordIdentityMatch,
{
    fn match_identity(
        &self,
        record: &AgentArtifactRecordWire,
        pid: i64,
    ) -> RecordIdentityMatch {
        self(record, pid)
    }
}

impl<F> OwnerLivenessObserver for F
where
    F: Fn(&AgentArtifactRecordWire) -> OwnerProcessObservation + Send + Sync,
{
    fn observe(
        &self,
        record: &AgentArtifactRecordWire,
    ) -> OwnerProcessObservation {
        self(record)
    }
}

/// Default `/proc` + `kill(0)` process probe.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProcHostProcessProbe;

impl HostProcessProbe for ProcHostProcessProbe {
    fn observe(&self, pid: i32) -> HostProcessObservation {
        observe_host_process(pid)
    }
}

/// Filesystem identity probe: home `running.json` or project RUNNING claims.
#[derive(Debug, Default)]
pub struct FilesystemRecordIdentityProbe {
    project_claims: Mutex<BTreeMap<PathBuf, Option<Vec<WorkspaceClaimWire>>>>,
}

impl RecordIdentityProbe for FilesystemRecordIdentityProbe {
    fn match_identity(
        &self,
        record: &AgentArtifactRecordWire,
        pid: i64,
    ) -> RecordIdentityMatch {
        if record.project_name == "home" {
            return match_home_marker(pid, home_marker_pid(record));
        }
        let claims = self.claims_for(record);
        match claims.as_deref() {
            Some(claims) => match_project_claims(record, pid, claims),
            None => RecordIdentityMatch::NotApplicable,
        }
    }
}

impl FilesystemRecordIdentityProbe {
    fn claims_for(
        &self,
        record: &AgentArtifactRecordWire,
    ) -> Option<Vec<WorkspaceClaimWire>> {
        let project_file = PathBuf::from(&record.project_file);
        let mut cache = match self.project_claims.lock() {
            Ok(cache) => cache,
            Err(_) => return None,
        };
        cache
            .entry(project_file.clone())
            .or_insert_with(|| {
                fs::read_to_string(&project_file)
                    .ok()
                    .map(|content| list_workspace_claims_from_content(&content))
            })
            .clone()
    }
}

/// Production observer: strong process probe plus filesystem identity.
#[derive(Debug, Default)]
pub struct HostOwnerLivenessObserver {
    process: ProcHostProcessProbe,
    identity: FilesystemRecordIdentityProbe,
}

impl OwnerLivenessObserver for HostOwnerLivenessObserver {
    fn observe(
        &self,
        record: &AgentArtifactRecordWire,
    ) -> OwnerProcessObservation {
        observe_owner_process(record, &self.process, &self.identity)
    }
}

/// PID used for owner liveness: running marker, then meta, then workflow.
pub fn owner_process_pid(record: &AgentArtifactRecordWire) -> Option<i64> {
    record
        .running
        .as_ref()
        .and_then(|running| running.pid)
        .or_else(|| record.agent_meta.as_ref().and_then(|meta| meta.pid))
        .or_else(|| {
            record
                .workflow_state
                .as_ref()
                .and_then(|workflow| workflow.pid)
        })
}

pub fn observe_owner_process(
    record: &AgentArtifactRecordWire,
    process: &dyn HostProcessProbe,
    identity: &dyn RecordIdentityProbe,
) -> OwnerProcessObservation {
    if record.done.is_some() {
        return OwnerProcessObservation::Dead;
    }
    let Some(pid) = owner_process_pid(record) else {
        return OwnerProcessObservation::Unknown;
    };
    if pid <= 0 {
        return OwnerProcessObservation::NotProcess;
    }
    let Ok(pid_i32) = i32::try_from(pid) else {
        return OwnerProcessObservation::NotProcess;
    };
    match process.observe(pid_i32) {
        HostProcessObservation::Missing | HostProcessObservation::Zombie => {
            OwnerProcessObservation::Dead
        }
        HostProcessObservation::WrongCommand => {
            OwnerProcessObservation::IdentityMismatch
        }
        HostProcessObservation::LiveAgent => {
            match identity.match_identity(record, pid) {
                RecordIdentityMatch::Matches
                | RecordIdentityMatch::NotApplicable => {
                    OwnerProcessObservation::Alive
                }
                RecordIdentityMatch::Mismatch => {
                    OwnerProcessObservation::IdentityMismatch
                }
            }
        }
    }
}

pub fn match_home_marker(
    pid: i64,
    marker_pid: Option<i64>,
) -> RecordIdentityMatch {
    match marker_pid {
        Some(marker) if marker == pid => RecordIdentityMatch::Matches,
        Some(_) => RecordIdentityMatch::Mismatch,
        None => RecordIdentityMatch::NotApplicable,
    }
}

pub fn match_project_claims(
    record: &AgentArtifactRecordWire,
    pid: i64,
    claims: &[WorkspaceClaimWire],
) -> RecordIdentityMatch {
    let workspace_num = record
        .agent_meta
        .as_ref()
        .and_then(|meta| meta.workspace_num)
        .and_then(|value| u32::try_from(value).ok());
    let mut saw_identity = false;
    for claim in claims {
        let timestamp_matches = claim
            .artifacts_timestamp
            .as_deref()
            .is_some_and(|timestamp| timestamp == record.timestamp);
        let workspace_matches =
            workspace_num.is_some_and(|value| value == claim.workspace_num);
        if !timestamp_matches && !workspace_matches {
            continue;
        }
        saw_identity = true;
        if i64::from(claim.pid) == pid {
            return RecordIdentityMatch::Matches;
        }
    }
    if saw_identity || workspace_num.is_some() {
        RecordIdentityMatch::Mismatch
    } else {
        RecordIdentityMatch::NotApplicable
    }
}

pub fn status_is_zombie(status: &str) -> bool {
    status.lines().any(|line| {
        line.strip_prefix("State:")
            .is_some_and(|value| value.trim_start().starts_with('Z'))
    })
}

pub fn command_looks_like_agent(command: &[u8]) -> bool {
    command.windows(4).any(|part| part == b"sase")
        || command.windows(6).any(|part| part == b"python")
}

fn home_marker_pid(record: &AgentArtifactRecordWire) -> Option<i64> {
    let path = Path::new(&record.artifact_dir).join("running.json");
    let content = fs::read_to_string(path).ok()?;
    serde_json::from_str::<RunningMarkerWire>(&content)
        .ok()
        .and_then(|marker| marker.pid)
}

fn observe_host_process(pid: i32) -> HostProcessObservation {
    if !process_exists(pid) {
        return HostProcessObservation::Missing;
    }

    #[cfg(target_os = "linux")]
    {
        let proc_dir = PathBuf::from(format!("/proc/{pid}"));
        match fs::read_to_string(proc_dir.join("status")) {
            Ok(status) if status_is_zombie(&status) => {
                return HostProcessObservation::Zombie;
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return HostProcessObservation::Missing;
            }
            Ok(_) | Err(_) => {}
        }

        match fs::read(proc_dir.join("cmdline")) {
            Ok(command) if !command_looks_like_agent(&command) => {
                return HostProcessObservation::WrongCommand;
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return HostProcessObservation::Missing;
            }
            Ok(_) | Err(_) => {}
        }
    }

    HostProcessObservation::LiveAgent
}

#[cfg(unix)]
fn process_exists(pid: i32) -> bool {
    // SAFETY: signal 0 performs an existence/permission probe and does not
    // deliver a signal. `pid` was validated as a positive process ID.
    let result = unsafe { libc::kill(pid, 0) };
    if result == 0 {
        return true;
    }
    matches!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EPERM)
    )
}

#[cfg(not(unix))]
fn process_exists(_pid: i32) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::agent_launch::list_workspace_claims_from_content;

    fn record_from(value: serde_json::Value) -> AgentArtifactRecordWire {
        serde_json::from_value(value).unwrap()
    }

    struct ClaimIdentity<'a>(&'a [WorkspaceClaimWire]);

    impl RecordIdentityProbe for ClaimIdentity<'_> {
        fn match_identity(
            &self,
            record: &AgentArtifactRecordWire,
            pid: i64,
        ) -> RecordIdentityMatch {
            match_project_claims(record, pid, self.0)
        }
    }

    struct FixedIdentity(RecordIdentityMatch);

    impl RecordIdentityProbe for FixedIdentity {
        fn match_identity(
            &self,
            _record: &AgentArtifactRecordWire,
            _pid: i64,
        ) -> RecordIdentityMatch {
            self.0
        }
    }

    struct PanicIdentity;

    impl RecordIdentityProbe for PanicIdentity {
        fn match_identity(
            &self,
            _record: &AgentArtifactRecordWire,
            _pid: i64,
        ) -> RecordIdentityMatch {
            panic!("completed records must not match identity")
        }
    }

    fn project_record(
        timestamp: &str,
        workspace_num: Option<i64>,
    ) -> AgentArtifactRecordWire {
        let mut meta = json!({
            "name": "agent",
            "pid": 4242
        });
        if let Some(workspace_num) = workspace_num {
            meta["workspace_num"] = json!(workspace_num);
        }
        record_from(json!({
            "project_name": "proj",
            "project_dir": "/tmp/proj",
            "project_file": "/tmp/proj/proj.sase",
            "workflow_dir_name": "ace-run",
            "artifact_dir": format!("/tmp/proj/artifacts/ace-run/{timestamp}"),
            "timestamp": timestamp,
            "agent_meta": meta,
            "running": {"pid": 4242}
        }))
    }

    fn workflow_record() -> AgentArtifactRecordWire {
        record_from(json!({
            "project_name": "proj",
            "project_dir": "/tmp/proj",
            "project_file": "/tmp/proj/proj.sase",
            "workflow_dir_name": "gh",
            "artifact_dir": "/tmp/proj/artifacts/gh/20260722010101",
            "timestamp": "20260722010101",
            "agent_meta": {"name": "workflow"},
            "workflow_state": {
                "workflow_name": "gh",
                "status": "running",
                "pid": 4242,
                "appears_as_agent": true
            }
        }))
    }

    #[test]
    fn status_parser_detects_zombie_and_running() {
        assert!(status_is_zombie("Name:\tinit\nState:\tZ (zombie)\n"));
        assert!(!status_is_zombie("Name:\tinit\nState:\tR (running)\n"));
    }

    #[test]
    fn command_parser_accepts_sase_and_python_only() {
        assert!(command_looks_like_agent(b"/usr/bin/python3\0-m\0sase"));
        assert!(command_looks_like_agent(b"/opt/sase_gateway\0serve"));
        assert!(!command_looks_like_agent(b"/usr/bin/nginx\0-c\0conf"));
    }

    #[test]
    fn matching_project_claim_is_alive() {
        let record = project_record("20260722010101", Some(7));
        let claims = list_workspace_claims_from_content(
            "RUNNING:\n  #7 | 4242 | run | demo | 20260722010101\n",
        );
        assert_eq!(
            match_project_claims(&record, 4242, &claims),
            RecordIdentityMatch::Matches
        );
        let observation = observe_owner_process(
            &record,
            &|_| HostProcessObservation::LiveAgent,
            &ClaimIdentity(&claims),
        );
        assert_eq!(observation, OwnerProcessObservation::Alive);
        assert_eq!(observation.liveness(), OwnerLivenessWire::Alive);
        assert!(!observation.process_identity_mismatch());
    }

    #[test]
    fn stale_project_claim_is_identity_mismatch() {
        let record = project_record("20260722010101", Some(7));
        let claims = list_workspace_claims_from_content(
            "RUNNING:\n  #7 | 9999 | run | demo | 20260722010101\n",
        );
        assert_eq!(
            match_project_claims(&record, 4242, &claims),
            RecordIdentityMatch::Mismatch
        );
        let observation = observe_owner_process(
            &record,
            &|_| HostProcessObservation::LiveAgent,
            &ClaimIdentity(&claims),
        );
        assert_eq!(observation, OwnerProcessObservation::IdentityMismatch);
        assert_eq!(observation.liveness(), OwnerLivenessWire::Dead);
        assert!(observation.process_identity_mismatch());
    }

    #[test]
    fn workspace_num_without_current_claim_is_mismatch() {
        let record = project_record("20260722010101", Some(7));
        assert_eq!(
            match_project_claims(&record, 4242, &[]),
            RecordIdentityMatch::Mismatch
        );
    }

    #[test]
    fn workflow_without_claim_shape_uses_process_classifier() {
        let record = workflow_record();
        let claims = list_workspace_claims_from_content(
            "RUNNING:\n  #7 | 1111 | run | demo | 20260722020202\n",
        );
        assert_eq!(
            match_project_claims(&record, 4242, &claims),
            RecordIdentityMatch::NotApplicable
        );
        let observation = observe_owner_process(
            &record,
            &|_| HostProcessObservation::LiveAgent,
            &ClaimIdentity(&claims),
        );
        assert_eq!(observation, OwnerProcessObservation::Alive);
    }

    #[test]
    fn zombie_and_wrong_command_are_not_alive() {
        let record = workflow_record();
        let identity = FixedIdentity(RecordIdentityMatch::NotApplicable);
        assert_eq!(
            observe_owner_process(
                &record,
                &|_| HostProcessObservation::Zombie,
                &identity
            ),
            OwnerProcessObservation::Dead
        );
        let wrong = observe_owner_process(
            &record,
            &|_| HostProcessObservation::WrongCommand,
            &identity,
        );
        assert_eq!(wrong, OwnerProcessObservation::IdentityMismatch);
        assert!(wrong.process_identity_mismatch());
        assert_eq!(
            observe_owner_process(
                &record,
                &|_| HostProcessObservation::Missing,
                &identity
            ),
            OwnerProcessObservation::Dead
        );
    }

    #[test]
    fn missing_pid_and_invalid_pid_keep_wire_distinctions() {
        let mut record = workflow_record();
        record.workflow_state.as_mut().unwrap().pid = None;
        assert_eq!(
            observe_owner_process(
                &record,
                &|_| HostProcessObservation::LiveAgent,
                &FixedIdentity(RecordIdentityMatch::NotApplicable)
            ),
            OwnerProcessObservation::Unknown
        );
        record.workflow_state.as_mut().unwrap().pid = Some(0);
        assert_eq!(
            observe_owner_process(
                &record,
                &|_| HostProcessObservation::LiveAgent,
                &FixedIdentity(RecordIdentityMatch::NotApplicable)
            ),
            OwnerProcessObservation::NotProcess
        );
    }

    #[test]
    fn completed_records_are_dead_without_probing() {
        let record = record_from(json!({
            "project_name": "proj",
            "project_dir": "/tmp/proj",
            "project_file": "/tmp/proj/proj.sase",
            "workflow_dir_name": "ace-run",
            "artifact_dir": "/tmp/proj/artifacts/ace-run/20260722010101",
            "timestamp": "20260722010101",
            "done": {"outcome": "completed", "finished_at": 1.0},
            "running": {"pid": 4242}
        }));
        let observation = observe_owner_process(
            &record,
            &|_| panic!("completed records must not probe"),
            &PanicIdentity,
        );
        assert_eq!(observation, OwnerProcessObservation::Dead);
    }

    #[test]
    fn home_marker_match_and_mismatch() {
        assert_eq!(
            match_home_marker(4242, Some(4242)),
            RecordIdentityMatch::Matches
        );
        assert_eq!(
            match_home_marker(4242, Some(99)),
            RecordIdentityMatch::Mismatch
        );
        assert_eq!(
            match_home_marker(4242, None),
            RecordIdentityMatch::NotApplicable
        );
    }

    #[test]
    fn filesystem_probe_reads_home_marker_and_project_claims() {
        let tmp = tempdir().unwrap();
        let home_dir = tmp.path().join("artifacts/ace-run/20260722010101");
        fs::create_dir_all(&home_dir).unwrap();
        fs::write(
            home_dir.join("running.json"),
            serde_json::to_vec(&json!({"pid": 4242})).unwrap(),
        )
        .unwrap();
        let mut home = project_record("20260722010101", None);
        home.project_name = "home".to_string();
        home.artifact_dir = home_dir.to_string_lossy().into_owned();
        let probe = FilesystemRecordIdentityProbe::default();
        assert_eq!(
            probe.match_identity(&home, 4242),
            RecordIdentityMatch::Matches
        );
        assert_eq!(
            probe.match_identity(&home, 99),
            RecordIdentityMatch::Mismatch
        );

        let project_file = tmp.path().join("proj.sase");
        fs::write(
            &project_file,
            "RUNNING:\n  #7 | 4242 | run | demo | 20260722010101\n",
        )
        .unwrap();
        let mut project = project_record("20260722010101", Some(7));
        project.project_file = project_file.to_string_lossy().into_owned();
        assert_eq!(
            probe.match_identity(&project, 4242),
            RecordIdentityMatch::Matches
        );
        assert_eq!(
            probe.match_identity(&project, 99),
            RecordIdentityMatch::Mismatch
        );
    }
}
