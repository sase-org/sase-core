//! Refresh-machinery tests: single-flight stampede coalescing, late
//! cache fills, exponential back-off, forced refreshes, and the bounded
//! overlay pass.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::Utc;
use sase_core::{
    agent_scan::{AgentArtifactRecordWire, AgentArtifactScanOptionsWire},
    fleet_contract::ObservationFreshnessWire,
    host_liveness::{OwnerLivenessObserver, OwnerProcessObservation},
};
use tempfile::{tempdir, TempDir};

use super::super::errors::FleetReadError;
use super::super::resolution::FLEET_SNAPSHOT_STALE_SECONDS;
use super::super::service::FleetReadService;
use super::support::*;
use crate::fleet_auth::current_unix_time;
use crate::fleet_launch::FleetLaunchStore;

/// Liveness observer with an adjustable per-record delay and a running
/// maximum of concurrent invocations, so tests can make builds slow and
/// prove they never overlap.
struct SlowObserver {
    delay_ms: AtomicU64,
    current: AtomicUsize,
    max: AtomicUsize,
}

impl SlowObserver {
    fn new(delay_ms: u64) -> Self {
        Self {
            delay_ms: AtomicU64::new(delay_ms),
            current: AtomicUsize::new(0),
            max: AtomicUsize::new(0),
        }
    }

    fn set_delay_ms(&self, delay_ms: u64) {
        self.delay_ms.store(delay_ms, Ordering::SeqCst);
    }

    fn max_concurrent(&self) -> usize {
        self.max.load(Ordering::SeqCst)
    }
}

impl OwnerLivenessObserver for SlowObserver {
    fn observe(
        &self,
        _record: &AgentArtifactRecordWire,
    ) -> OwnerProcessObservation {
        let current = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.max.fetch_max(current, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(
            self.delay_ms.load(Ordering::SeqCst),
        ));
        self.current.fetch_sub(1, Ordering::SeqCst);
        OwnerProcessObservation::Alive
    }
}

fn seed_two_agent_home() -> (TempDir, PathBuf, PathBuf) {
    let temp = tempdir().unwrap();
    let home = temp.path().to_path_buf();
    let projects = home.join("projects");
    seed_project(&projects, "proj");
    seed_agent(&projects, &recent_timestamp(2), "alpha", "alpha output");
    seed_agent(&projects, &recent_timestamp(1), "beta", "beta output");
    sase_core::rebuild_agent_artifact_index(
        &home.join("agent_artifact_index.sqlite"),
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    (temp, home, projects)
}

fn slow_service(
    home: &Path,
    timeout: Duration,
    delay_ms: u64,
) -> (Arc<SlowObserver>, FleetReadService) {
    let observer = Arc::new(SlowObserver::new(delay_ms));
    let liveness: Arc<dyn OwnerLivenessObserver> = observer.clone();
    (
        observer,
        FleetReadService::new_with_liveness(home, timeout, liveness),
    )
}

async fn poll_until(condition: impl Fn() -> bool) {
    for _ in 0..500 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for the background build");
}

#[tokio::test]
async fn slow_build_stampede_starts_exactly_one_build() {
    let (_temp, home, _projects) = seed_two_agent_home();
    let (observer, service) =
        slow_service(&home, Duration::from_millis(50), 150);
    let mut tasks = Vec::new();
    for _ in 0..20 {
        let caller = service.clone();
        tasks.push(tokio::spawn(async move { caller.summary().await }));
    }
    for task in tasks {
        let result = task.await.unwrap();
        assert!(
            matches!(result, Err(FleetReadError::Timeout(_))),
            "slow-build stampede callers must time out promptly"
        );
    }
    assert_eq!(service.builds_started_for_test(), 1);
    poll_until(|| service.refresh_count_for_test() == 1).await;
    assert_eq!(
        observer.max_concurrent(),
        1,
        "exactly one build may observe liveness at a time"
    );
    let summary = service.summary().await.unwrap();
    assert_eq!(summary.counts.logical_agent_total, 2);
    assert_eq!(
        service.builds_started_for_test(),
        1,
        "the late build must fill the cache without a second build"
    );
}

#[tokio::test]
async fn late_build_result_fills_cache_for_later_reads() {
    let (_temp, home, _projects) = seed_two_agent_home();
    let (observer, service) =
        slow_service(&home, Duration::from_millis(100), 0);
    // The first build may or may not finish before the short timeout; the
    // detached task always fills the cache either way.
    let _ = service.summary().await;
    poll_until(|| service.refresh_count_for_test() == 1).await;
    assert_eq!(service.builds_started_for_test(), 1);

    observer.set_delay_ms(200);
    service.age_cache_for_test(FLEET_SNAPSHOT_STALE_SECONDS + 1.0);
    let stale = service.summary().await.unwrap();
    assert_eq!(stale.freshness.freshness, ObservationFreshnessWire::Stale);
    assert!(stale.freshness.partial);
    assert_eq!(stale.freshness.error.as_deref(), Some("timeout"));

    poll_until(|| service.refresh_count_for_test() == 2).await;
    let fresh = service.summary().await.unwrap();
    assert_eq!(fresh.freshness.freshness, ObservationFreshnessWire::Fresh);
    assert_eq!(service.builds_started_for_test(), 2);
}

#[tokio::test]
async fn failed_builds_back_off_and_reset_on_success() {
    let (temp, service) = seed_home();
    let projects = temp.path().join("projects");
    service.summary().await.unwrap();
    assert_eq!(service.builds_started_for_test(), 1);

    service.age_cache_for_test(FLEET_SNAPSHOT_STALE_SECONDS + 1.0);
    // Replace the index file with a directory so rebuild attempts fail
    // deterministically instead of relying on timing.
    fs::remove_file(service.index_path()).unwrap();
    fs::create_dir_all(service.index_path()).unwrap();

    let failed = service.summary().await.unwrap();
    assert!(failed.freshness.partial);
    assert_eq!(service.builds_started_for_test(), 2);
    assert_eq!(service.consecutive_failures_for_test(), 1);

    let again = service.summary().await.unwrap();
    assert_eq!(again.counts.logical_agent_total, 2);
    assert!(again.freshness.partial);
    assert_eq!(
        service.builds_started_for_test(),
        2,
        "calls inside the back-off window must not start a build"
    );

    service.expire_backoff_for_test();
    let third = service.summary().await.unwrap();
    assert!(third.freshness.partial);
    assert_eq!(service.builds_started_for_test(), 3);
    assert_eq!(service.consecutive_failures_for_test(), 2);

    fs::remove_dir(service.index_path()).unwrap();
    sase_core::rebuild_agent_artifact_index(
        service.index_path(),
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    service.expire_backoff_for_test();
    let healed = service.summary().await.unwrap();
    assert!(!healed.freshness.partial);
    assert_eq!(service.consecutive_failures_for_test(), 0);
    assert_eq!(service.builds_started_for_test(), 4);
}

#[tokio::test]
async fn backoff_returns_remembered_error_on_cold_cache() {
    let temp = tempdir().unwrap();
    let home = temp.path().to_path_buf();
    let projects = home.join("projects");
    seed_project(&projects, "proj");
    seed_agent(&projects, &recent_timestamp(1), "alpha", "alpha output");
    sase_core::rebuild_agent_artifact_index(
        &home.join("agent_artifact_index.sqlite"),
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    fs::remove_file(home.join("agent_artifact_index.sqlite")).unwrap();
    fs::create_dir_all(home.join("agent_artifact_index.sqlite")).unwrap();
    let service = FleetReadService::new(home);

    let first = service.summary().await;
    assert!(
        matches!(first, Err(FleetReadError::Backend(_))),
        "a cold-cache failure must surface the build error"
    );
    assert_eq!(service.builds_started_for_test(), 1);

    let second = service.summary().await;
    assert!(
        matches!(second, Err(FleetReadError::Backend(_))),
        "a cold-cache call inside back-off must not start a build"
    );
    assert_eq!(service.builds_started_for_test(), 1);

    service.expire_backoff_for_test();
    let third = service.summary().await;
    assert!(matches!(third, Err(FleetReadError::Backend(_))));
    assert_eq!(service.builds_started_for_test(), 2);
}

#[tokio::test]
async fn forced_refresh_waits_then_builds_after_call() {
    let (_temp, home, _projects) = seed_two_agent_home();
    let (observer, service) = slow_service(&home, Duration::from_secs(5), 200);
    let background = service.clone();
    let first = tokio::spawn(async move { background.summary().await });
    poll_until(|| service.snapshot_build_in_flight_for_test()).await;

    service.reconcile().await.unwrap();
    first.await.unwrap().unwrap();

    assert_eq!(service.builds_started_for_test(), 2);
    assert_eq!(service.refresh_count_for_test(), 2);
    assert_eq!(
        observer.max_concurrent(),
        1,
        "a forced refresh must never overlap the build it waited on"
    );
}

fn launch_request_for_test(
    operation_id: &str,
    installation_id: &str,
) -> sase_core::FleetLaunchRequestWire {
    let intent_json = serde_json::json!({
        "schema_version": 1,
        "prompt": "Do remote work",
        "request_id": operation_id,
        "display_name": "Dispatch demo",
        "name": "mobile-demo",
        "model": "gpt-5",
        "provider": "codex",
        "runtime": "codex",
        "project": {
            "schema_version": 1,
            "provider_ref": "builtin:https",
            "project_id": "sase",
            "revision": null,
            "patch_ref": "patch-123"
        },
        "dry_run": false,
        "follow": true,
        "references": []
    });
    let intent: sase_core::FleetLaunchIntentWire =
        serde_json::from_value(intent_json.clone()).unwrap();
    let fingerprint =
        sase_core::fleet_launch_payload_fingerprint(&intent).unwrap();
    serde_json::from_value(serde_json::json!({
        "schema_version": 1,
        "key": {
            "schema_version": 1,
            "controller_id": "controller-a",
            "operation_id": operation_id
        },
        "target_installation_id": installation_id,
        "intent": intent_json,
        "payload_fingerprint": fingerprint,
        "acceptance_window_seconds": 30.0
    }))
    .unwrap()
}

/// Second-resolution timestamps that are always distinct from each other
/// and from the minute-based `recent_timestamp` seeds, so every seeded
/// agent gets its own artifact directory.
fn unique_timestamp(offset_secs: i64) -> String {
    (Utc::now() - chrono::Duration::seconds(offset_secs))
        .format("%Y%m%d%H%M%S")
        .to_string()
}

/// Settle a launch receipt whose logical locator is the freshly indexed
/// agent `name`, which the cached snapshot does not contain yet.
async fn settle_missing_launch_for_test(
    home: &Path,
    operation_id: &str,
    name: &str,
) {
    let probe = FleetReadService::new(home);
    let probe_page = probe.catalog(catalog_query()).await.unwrap();
    let row = probe_page
        .page
        .rows
        .into_iter()
        .find(|row| row.labels.agent_label.as_deref() == Some(name))
        .unwrap_or_else(|| panic!("fresh index must serve {name}"));
    let installation =
        sase_core::fleet_contract::ensure_installation_identity(home)
            .unwrap()
            .record;
    let store = FleetLaunchStore::new(home);
    let admission = store
        .reserve(
            &launch_request_for_test(
                operation_id,
                &installation.installation_id,
            ),
            &installation.installation_id,
            current_unix_time(),
        )
        .unwrap();
    store
        .settle_recovered(
            &admission.receipt,
            Some(row.logical_locator.clone()),
            row.exact_locator.clone(),
            Some(operation_id.to_string()),
        )
        .unwrap();
}

#[tokio::test]
async fn overlay_queries_once_per_snapshot_and_skips_under_contention() {
    let (_temp, home, projects) = seed_two_agent_home();
    let service = FleetReadService::new(home.clone());
    service.summary().await.unwrap();
    assert_eq!(service.builds_started_for_test(), 1);

    seed_agent(&projects, &unique_timestamp(300), "beta-overlay", "beta");
    sase_core::rebuild_agent_artifact_index(
        &home.join("agent_artifact_index.sqlite"),
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    settle_missing_launch_for_test(
        &home,
        "overlay-launch-beta",
        "beta-overlay",
    )
    .await;

    for _ in 0..4 {
        let page = service.catalog(catalog_query()).await.unwrap();
        assert!(
            page.page
                .rows
                .iter()
                .any(|row| row.labels.agent_label.as_deref()
                    == Some("beta-overlay")),
            "sequential pages must replay the memoized overlay"
        );
    }
    assert_eq!(service.overlay_query_count_for_test(), 1);

    seed_agent(&projects, &unique_timestamp(240), "gamma-overlay", "gamma");
    sase_core::rebuild_agent_artifact_index(
        &home.join("agent_artifact_index.sqlite"),
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    settle_missing_launch_for_test(
        &home,
        "overlay-launch-gamma",
        "gamma-overlay",
    )
    .await;
    let mut burst = Vec::new();
    for _ in 0..8 {
        let caller = service.clone();
        burst.push(tokio::spawn(async move {
            caller.catalog(catalog_query()).await
        }));
    }
    for task in burst {
        task.await.unwrap().unwrap();
    }
    assert_eq!(
        service.overlay_query_count_for_test(),
        2,
        "one concurrent burst must run at most one overlay query"
    );
    let after_burst = service.catalog(catalog_query()).await.unwrap();
    assert!(
        after_burst.page.rows.iter().any(|row| row
            .labels
            .agent_label
            .as_deref()
            == Some("gamma-overlay")),
        "the memoized overlay must serve the burst row afterwards"
    );

    seed_agent(&projects, &unique_timestamp(180), "delta-overlay", "delta");
    sase_core::rebuild_agent_artifact_index(
        &home.join("agent_artifact_index.sqlite"),
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    settle_missing_launch_for_test(
        &home,
        "overlay-launch-delta",
        "delta-overlay",
    )
    .await;
    let _permits = service.hold_all_index_permits_for_test().await;
    let skipped = service.catalog(catalog_query()).await.unwrap();
    assert!(
        skipped
            .page
            .rows
            .iter()
            .all(|row| row.labels.agent_label.as_deref()
                != Some("delta-overlay")),
        "the overlay must skip while both index permits are held"
    );
    assert_eq!(service.overlay_query_count_for_test(), 2);
    drop(_permits);
    let served = service.catalog(catalog_query()).await.unwrap();
    assert!(served.page.rows.iter().any(|row| row
        .labels
        .agent_label
        .as_deref()
        == Some("delta-overlay")));
    assert_eq!(service.overlay_query_count_for_test(), 3);
}
