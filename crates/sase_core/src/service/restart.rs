//! Pure restart-decision accounting for service procs.

use serde::{Deserialize, Serialize};

pub const SERVICE_RESTART_DECISION_WIRE_SCHEMA_VERSION: u32 = 1;

pub const SERVICE_RESTART_INITIAL_BACKOFF_SECONDS: f64 = 1.0;
pub const SERVICE_RESTART_MAX_BACKOFF_SECONDS: f64 = 60.0;
pub const SERVICE_RESTART_HEALTHY_RUN_SECONDS: f64 = 300.0;
pub const SERVICE_RESTART_CRASH_LOOP_WINDOW_SECONDS: f64 = 60.0;
pub const SERVICE_RESTART_CRASH_LOOP_THRESHOLD: u32 = 3;

const CLEAN_SIGNALS: [i32; 4] = [1, 2, 13, 15];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ServiceRestartPolicyWire {
    Always,
    OnFailure,
    Never,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ServiceExitWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spawn_error: Option<String>,
    #[serde(default)]
    pub stop_requested: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceRestartTuningWire {
    #[serde(default = "default_initial_backoff_seconds")]
    pub initial_backoff_seconds: f64,
    #[serde(default = "default_max_backoff_seconds")]
    pub max_backoff_seconds: f64,
    #[serde(default = "default_healthy_run_seconds")]
    pub healthy_run_seconds: f64,
    #[serde(default = "default_crash_loop_window_seconds")]
    pub crash_loop_window_seconds: f64,
    #[serde(default = "default_crash_loop_threshold")]
    pub crash_loop_threshold: u32,
}

impl Default for ServiceRestartTuningWire {
    fn default() -> Self {
        Self {
            initial_backoff_seconds: SERVICE_RESTART_INITIAL_BACKOFF_SECONDS,
            max_backoff_seconds: SERVICE_RESTART_MAX_BACKOFF_SECONDS,
            healthy_run_seconds: SERVICE_RESTART_HEALTHY_RUN_SECONDS,
            crash_loop_window_seconds:
                SERVICE_RESTART_CRASH_LOOP_WINDOW_SECONDS,
            crash_loop_threshold: SERVICE_RESTART_CRASH_LOOP_THRESHOLD,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ServiceRestartHistoryWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<f64>,
    #[serde(default)]
    pub backoff_seconds: f64,
    #[serde(default)]
    pub consecutive_failures: u32,
    #[serde(default)]
    pub recent_failures: Vec<f64>,
    #[serde(default)]
    pub alert_sent: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceRestartRequestWire {
    pub policy: ServiceRestartPolicyWire,
    #[serde(default)]
    pub success_exit_codes: Vec<i32>,
    pub exit: ServiceExitWire,
    #[serde(default)]
    pub history: ServiceRestartHistoryWire,
    pub now: f64,
    #[serde(default)]
    pub tuning: ServiceRestartTuningWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceRestartDecisionWire {
    pub schema_version: u32,
    pub action: String,
    pub clean_exit: bool,
    pub delay_seconds: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_at: Option<f64>,
    pub reason: String,
    pub crash_loop: bool,
    pub notify: bool,
    pub history: ServiceRestartHistoryWire,
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceRestartError {
    #[error("now must be finite")]
    InvalidNow,
    #[error("{field} must be finite and positive")]
    InvalidPositiveTuning { field: &'static str },
    #[error("crash_loop_threshold must be at least 1")]
    InvalidCrashLoopThreshold,
    #[error("recent failure timestamps must be finite")]
    InvalidRecentFailure,
    #[error("started_at must be finite when present")]
    InvalidStartedAt,
    #[error("backoff_seconds must be finite and non-negative")]
    InvalidBackoff,
}

pub fn decide_service_restart(
    request: &ServiceRestartRequestWire,
) -> Result<ServiceRestartDecisionWire, ServiceRestartError> {
    validate_request(request)?;

    if request.exit.stop_requested {
        let mut history = request.history.clone();
        history.started_at = None;
        return Ok(ServiceRestartDecisionWire {
            schema_version: SERVICE_RESTART_DECISION_WIRE_SCHEMA_VERSION,
            action: "give_up".to_string(),
            clean_exit: true,
            delay_seconds: 0.0,
            restart_at: None,
            reason: "stopped on request".to_string(),
            crash_loop: false,
            notify: false,
            history,
        });
    }

    let classification =
        classify_exit(&request.exit, &request.success_exit_codes);
    if request.policy == ServiceRestartPolicyWire::Never {
        return Ok(give_up(
            request.history.clone(),
            classification.clean,
            format!("{}; restart policy is never", classification.detail),
        ));
    }
    if request.policy == ServiceRestartPolicyWire::OnFailure
        && classification.clean
    {
        return Ok(give_up(
            request.history.clone(),
            true,
            format!(
                "{}; clean exit, not restarting (restart: on-failure)",
                classification.detail
            ),
        ));
    }

    Ok(restart(request, classification))
}

fn validate_request(
    request: &ServiceRestartRequestWire,
) -> Result<(), ServiceRestartError> {
    if !request.now.is_finite() {
        return Err(ServiceRestartError::InvalidNow);
    }
    validate_positive(
        "initial_backoff_seconds",
        request.tuning.initial_backoff_seconds,
    )?;
    validate_positive(
        "max_backoff_seconds",
        request.tuning.max_backoff_seconds,
    )?;
    validate_positive(
        "healthy_run_seconds",
        request.tuning.healthy_run_seconds,
    )?;
    validate_positive(
        "crash_loop_window_seconds",
        request.tuning.crash_loop_window_seconds,
    )?;
    if request.tuning.crash_loop_threshold == 0 {
        return Err(ServiceRestartError::InvalidCrashLoopThreshold);
    }
    if request
        .history
        .started_at
        .is_some_and(|started_at| !started_at.is_finite())
    {
        return Err(ServiceRestartError::InvalidStartedAt);
    }
    if !request.history.backoff_seconds.is_finite()
        || request.history.backoff_seconds < 0.0
    {
        return Err(ServiceRestartError::InvalidBackoff);
    }
    if request
        .history
        .recent_failures
        .iter()
        .any(|failure| !failure.is_finite())
    {
        return Err(ServiceRestartError::InvalidRecentFailure);
    }
    Ok(())
}

fn validate_positive(
    field: &'static str,
    value: f64,
) -> Result<(), ServiceRestartError> {
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        Err(ServiceRestartError::InvalidPositiveTuning { field })
    }
}

fn give_up(
    mut history: ServiceRestartHistoryWire,
    clean_exit: bool,
    reason: String,
) -> ServiceRestartDecisionWire {
    history.started_at = None;
    ServiceRestartDecisionWire {
        schema_version: SERVICE_RESTART_DECISION_WIRE_SCHEMA_VERSION,
        action: "give_up".to_string(),
        clean_exit,
        delay_seconds: 0.0,
        restart_at: None,
        reason,
        crash_loop: false,
        notify: false,
        history,
    }
}

fn restart(
    request: &ServiceRestartRequestWire,
    classification: ExitClassification,
) -> ServiceRestartDecisionWire {
    let mut history = request.history.clone();
    let tuning = &request.tuning;

    let healthy_run = history.started_at.is_some_and(|started_at| {
        request.now - started_at >= tuning.healthy_run_seconds
    });
    if healthy_run {
        history.backoff_seconds = 0.0;
        history.consecutive_failures = 0;
        history.recent_failures.clear();
        history.alert_sent = false;
    }

    history.started_at = None;
    history.consecutive_failures =
        history.consecutive_failures.saturating_add(1);
    history.backoff_seconds = if history.backoff_seconds == 0.0 {
        tuning.initial_backoff_seconds
    } else {
        history.backoff_seconds * 2.0
    }
    .min(tuning.max_backoff_seconds);
    let restart_at = request.now + history.backoff_seconds;

    let cutoff = request.now - tuning.crash_loop_window_seconds;
    history.recent_failures.retain(|failure| *failure >= cutoff);
    history.recent_failures.push(request.now);

    let crash_loop =
        history.recent_failures.len() as u32 >= tuning.crash_loop_threshold;
    let notify = crash_loop && !history.alert_sent;
    if notify {
        history.alert_sent = true;
    }

    let reason = if crash_loop {
        format!(
            "crash-looping ({} failures within {}): {}; retrying in {}",
            history.recent_failures.len(),
            format_seconds(tuning.crash_loop_window_seconds),
            classification.detail,
            format_seconds(history.backoff_seconds),
        )
    } else {
        format!(
            "{}; retrying in {}",
            classification.detail,
            format_seconds(history.backoff_seconds),
        )
    };

    ServiceRestartDecisionWire {
        schema_version: SERVICE_RESTART_DECISION_WIRE_SCHEMA_VERSION,
        action: "restart".to_string(),
        clean_exit: classification.clean,
        delay_seconds: history.backoff_seconds,
        restart_at: Some(restart_at),
        reason,
        crash_loop,
        notify,
        history,
    }
}

struct ExitClassification {
    clean: bool,
    detail: String,
}

fn classify_exit(
    exit: &ServiceExitWire,
    success_exit_codes: &[i32],
) -> ExitClassification {
    if let Some(error) = exit.spawn_error.as_deref() {
        return ExitClassification {
            clean: false,
            detail: format!("failed to start: {error}"),
        };
    }
    if let Some(code) = exit.exit_code {
        return ExitClassification {
            clean: code == 0 || success_exit_codes.contains(&code),
            detail: format!("exited with code {code}"),
        };
    }
    if let Some(signal) = exit.signal {
        return ExitClassification {
            clean: CLEAN_SIGNALS.contains(&signal),
            detail: format!("killed by {}", signal_name(signal)),
        };
    }
    ExitClassification {
        clean: false,
        detail: "finished without exit status".to_string(),
    }
}

fn signal_name(signal: i32) -> String {
    match signal {
        1 => "SIGHUP".to_string(),
        2 => "SIGINT".to_string(),
        3 => "SIGQUIT".to_string(),
        4 => "SIGILL".to_string(),
        6 => "SIGABRT".to_string(),
        8 => "SIGFPE".to_string(),
        9 => "SIGKILL".to_string(),
        11 => "SIGSEGV".to_string(),
        13 => "SIGPIPE".to_string(),
        14 => "SIGALRM".to_string(),
        15 => "SIGTERM".to_string(),
        other => format!("signal {other}"),
    }
}

fn format_seconds(value: f64) -> String {
    let mut rendered = if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        format!("{value}")
    };
    if rendered.contains('.') {
        while rendered.ends_with('0') {
            rendered.pop();
        }
        if rendered.ends_with('.') {
            rendered.pop();
        }
    }
    format!("{rendered}s")
}

fn default_initial_backoff_seconds() -> f64 {
    SERVICE_RESTART_INITIAL_BACKOFF_SECONDS
}

fn default_max_backoff_seconds() -> f64 {
    SERVICE_RESTART_MAX_BACKOFF_SECONDS
}

fn default_healthy_run_seconds() -> f64 {
    SERVICE_RESTART_HEALTHY_RUN_SECONDS
}

fn default_crash_loop_window_seconds() -> f64 {
    SERVICE_RESTART_CRASH_LOOP_WINDOW_SECONDS
}

fn default_crash_loop_threshold() -> u32 {
    SERVICE_RESTART_CRASH_LOOP_THRESHOLD
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(
        history: ServiceRestartHistoryWire,
        now: f64,
    ) -> ServiceRestartRequestWire {
        ServiceRestartRequestWire {
            policy: ServiceRestartPolicyWire::Always,
            success_exit_codes: vec![],
            exit: ServiceExitWire {
                exit_code: Some(1),
                ..Default::default()
            },
            history,
            now,
            tuning: ServiceRestartTuningWire::default(),
        }
    }

    #[test]
    fn orchestrator_parity_backoff_doubles_caps_and_resets_after_healthy_run() {
        let mut history = ServiceRestartHistoryWire {
            started_at: Some(0.0),
            ..Default::default()
        };
        let mut delays = Vec::new();
        for now in [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0] {
            let decision =
                decide_service_restart(&request(history, now)).unwrap();
            delays.push(decision.delay_seconds);
            history = decision.history;
            history.started_at = Some(now);
        }
        assert_eq!(delays, vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0]);

        history.started_at = Some(100.0);
        let decision =
            decide_service_restart(&request(history, 401.0)).unwrap();
        assert_eq!(decision.delay_seconds, 1.0);
        assert_eq!(decision.history.consecutive_failures, 1);
        assert_eq!(decision.history.recent_failures, vec![401.0]);
    }

    #[test]
    fn crash_loop_notifies_once_and_rearms_after_healthy_run() {
        let mut history = ServiceRestartHistoryWire {
            started_at: Some(0.0),
            ..Default::default()
        };
        let first = decide_service_restart(&request(history, 1.0)).unwrap();
        assert!(!first.crash_loop);
        assert!(!first.notify);
        history = first.history;
        history.started_at = Some(1.0);

        let second = decide_service_restart(&request(history, 2.0)).unwrap();
        assert!(!second.crash_loop);
        history = second.history;
        history.started_at = Some(2.0);

        let third = decide_service_restart(&request(history, 3.0)).unwrap();
        assert!(third.crash_loop);
        assert!(third.notify);
        history = third.history;
        history.started_at = Some(3.0);

        let fourth = decide_service_restart(&request(history, 4.0)).unwrap();
        assert!(fourth.crash_loop);
        assert!(!fourth.notify);

        let mut rearmed = fourth.history;
        rearmed.started_at = Some(10.0);
        let reset = decide_service_restart(&request(rearmed, 311.0)).unwrap();
        assert!(!reset.crash_loop);
        assert!(!reset.history.alert_sent);
    }

    #[test]
    fn window_pruning_removes_old_failures() {
        let history = ServiceRestartHistoryWire {
            recent_failures: vec![1.0, 50.0, 90.0],
            ..Default::default()
        };
        let decision =
            decide_service_restart(&request(history, 120.0)).unwrap();
        assert_eq!(decision.history.recent_failures, vec![90.0, 120.0]);
        assert!(!decision.crash_loop);
    }

    #[test]
    fn on_failure_gives_up_for_clean_exits_and_restarts_failures() {
        let base = ServiceRestartRequestWire {
            policy: ServiceRestartPolicyWire::OnFailure,
            success_exit_codes: vec![75],
            exit: ServiceExitWire {
                exit_code: Some(75),
                ..Default::default()
            },
            history: ServiceRestartHistoryWire::default(),
            now: 1.0,
            tuning: ServiceRestartTuningWire::default(),
        };
        let decision = decide_service_restart(&base).unwrap();
        assert_eq!(decision.action, "give_up");
        assert_eq!(
            decision.reason,
            "exited with code 75; clean exit, not restarting (restart: on-failure)"
        );

        let mut sigterm = base.clone();
        sigterm.exit = ServiceExitWire {
            signal: Some(15),
            ..Default::default()
        };
        assert_eq!(decide_service_restart(&sigterm).unwrap().action, "give_up");

        let mut sigkill = base.clone();
        sigkill.exit = ServiceExitWire {
            signal: Some(9),
            ..Default::default()
        };
        let decision = decide_service_restart(&sigkill).unwrap();
        assert_eq!(decision.action, "restart");
        assert_eq!(decision.reason, "killed by SIGKILL; retrying in 1s");

        let mut spawn = base;
        spawn.exit = ServiceExitWire {
            spawn_error: Some("No such file".to_string()),
            ..Default::default()
        };
        assert_eq!(
            decide_service_restart(&spawn).unwrap().reason,
            "failed to start: No such file; retrying in 1s"
        );
    }

    #[test]
    fn never_and_stop_requested_give_up() {
        let mut never = request(ServiceRestartHistoryWire::default(), 1.0);
        never.policy = ServiceRestartPolicyWire::Never;
        assert_eq!(
            decide_service_restart(&never).unwrap().reason,
            "exited with code 1; restart policy is never"
        );

        let mut stopped = never;
        stopped.exit.stop_requested = true;
        assert_eq!(
            decide_service_restart(&stopped).unwrap().reason,
            "stopped on request"
        );
    }

    #[test]
    fn invalid_inputs_are_rejected() {
        let mut bad = request(ServiceRestartHistoryWire::default(), f64::NAN);
        assert!(matches!(
            decide_service_restart(&bad),
            Err(ServiceRestartError::InvalidNow)
        ));
        bad.now = 1.0;
        bad.tuning.max_backoff_seconds = 0.0;
        assert!(matches!(
            decide_service_restart(&bad),
            Err(ServiceRestartError::InvalidPositiveTuning { .. })
        ));
    }
}
