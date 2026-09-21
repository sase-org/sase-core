use super::super::*;
use super::support::*;

#[test]
fn connection_plan_validation_rejects_insecure_or_secret_bearing_data() {
    let plan = ConnectionPlanWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        provider_ref: "provider-a".to_string(),
        endpoint: "https://fleet.example.test/api".to_string(),
        credential_ref: "cred-main".to_string(),
        pinned_installation_id: id('a'),
        connection_kind: FleetConnectionKindWire::Gateway,
        tls: TlsTrustSettingsWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            mode: TlsTrustModeWire::SystemRoots,
            ca_ref: None,
            server_name_ref: None,
        },
    };
    assert_eq!(validate_connection_plan(&plan).unwrap(), plan);
    let mut http = plan.clone();
    http.endpoint = "http://fleet.example.test".to_string();
    assert!(validate_connection_plan(&http).is_err());
    let mut userinfo = plan.clone();
    userinfo.endpoint = "https://user:pass@fleet.example.test".to_string();
    assert!(validate_connection_plan(&userinfo).is_err());
    let mut fragment = plan.clone();
    fragment.endpoint = "https://fleet.example.test/#frag".to_string();
    assert!(validate_connection_plan(&fragment).is_err());
    let mut inline_secret = plan;
    inline_secret.credential_ref = "token=secret".to_string();
    assert!(validate_connection_plan(&inline_secret).is_err());
}
#[test]
fn time_helpers_keep_owner_runtime_and_viewer_freshness_separate() {
    let running = classify_runtime_duration(&RuntimeDurationRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        owner_started_at_unix: 10.0,
        owner_stopped_at_unix: None,
        owner_observed_at_unix: 15.5,
        max_clock_anomaly_seconds: 1.0,
    })
    .unwrap();
    assert_eq!(running.elapsed_seconds, 5.5);
    assert_eq!(running.state, RuntimeDurationStateWire::Running);

    let clamped = classify_runtime_duration(&RuntimeDurationRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        owner_started_at_unix: 10.0,
        owner_stopped_at_unix: Some(9.5),
        owner_observed_at_unix: 11.0,
        max_clock_anomaly_seconds: 1.0,
    })
    .unwrap();
    assert_eq!(clamped.elapsed_seconds, 0.0);
    assert!(clamped.clamped);

    let fresh = classify_cache_freshness(&CacheFreshnessRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        viewer_monotonic_elapsed_seconds: Some(2.0),
        fresh_threshold_seconds: 3.0,
        stale_threshold_seconds: 10.0,
    })
    .unwrap();
    assert_eq!(fresh.freshness, ObservationFreshnessWire::Fresh);
    let unknown = classify_cache_freshness(&CacheFreshnessRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        viewer_monotonic_elapsed_seconds: None,
        fresh_threshold_seconds: 3.0,
        stale_threshold_seconds: 10.0,
    })
    .unwrap();
    assert_eq!(unknown.freshness, ObservationFreshnessWire::Unknown);
    assert!(classify_cache_freshness(&CacheFreshnessRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        viewer_monotonic_elapsed_seconds: Some(f64::INFINITY),
        fresh_threshold_seconds: 3.0,
        stale_threshold_seconds: 10.0,
    })
    .is_err());
}
