//! Defensive Tailscale status parsing and endpoint classification.

use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value};

use super::health::{classify_health_object, classify_probe_error};
use super::wire::{
    DiscoveryCandidateWire, MachineSetupDiagnosticWire,
    TailnetDiscoveryRequestWire, TailnetDiscoveryResultWire,
    TailnetHealthObservationWire, TailnetPeerWire, COMPATIBILITY_UNKNOWN,
    ENDPOINT_SOURCE_DNS, ENDPOINT_SOURCE_OVERRIDE,
    MACHINE_SETUP_WIRE_SCHEMA_VERSION, TAILNET_PROVIDER_REF,
};
use super::MachineSetupError;

const LINUX_FAMILY_OS: &[&str] = &["linux", "macos", "darwin"];

/// Parse Tailscale status JSON, classify endpoints, and assemble candidates
/// from supplied health observations.
pub fn classify_tailnet_discovery(
    request: &TailnetDiscoveryRequestWire,
) -> Result<TailnetDiscoveryResultWire, MachineSetupError> {
    MachineSetupError::check_schema(request.schema_version)?;
    let Some(status) = request.status.as_object() else {
        return Ok(TailnetDiscoveryResultWire {
            schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
            peers: Vec::new(),
            candidates: Vec::new(),
            diagnostics: vec![MachineSetupDiagnosticWire::new(
                "tailnet_status_malformed",
                "error",
                "",
                "tailscale status --json returned a non-object payload",
            )],
        });
    };

    let raw_peers = match status.get("Peer") {
        None | Some(Value::Null) => None,
        Some(Value::Object(map)) => Some(map),
        Some(_) => {
            return Ok(TailnetDiscoveryResultWire {
                schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
                peers: Vec::new(),
                candidates: Vec::new(),
                diagnostics: vec![MachineSetupDiagnosticWire::new(
                    "tailnet_status_peer_invalid",
                    "error",
                    "",
                    "tailscale status Peer payload must be a mapping",
                )],
            });
        }
    };

    let empty_map = Map::new();
    let peers_map = raw_peers.unwrap_or(&empty_map);
    let self_peer = match status.get("Self") {
        Some(Value::Object(map)) => map,
        _ => &empty_map,
    };
    let overrides = endpoint_overrides(&request.endpoint_overrides);
    let health_by_endpoint =
        index_health_observations(&request.health_observations);

    let mut peer_keys: Vec<&String> = peers_map.keys().collect();
    peer_keys.sort();

    let mut peers = Vec::new();
    let mut candidates = Vec::new();
    let mut diagnostics = Vec::new();

    for peer_key in peer_keys {
        let Some(raw_peer) = peers_map.get(peer_key) else {
            continue;
        };
        let Some(peer) = raw_peer.as_object() else {
            diagnostics.push(MachineSetupDiagnosticWire::new(
                "tailnet_peer_not_mapping",
                "warning",
                peer_key,
                format!("tailnet peer {peer_key} was not an object"),
            ));
            continue;
        };
        if is_self_peer(peer_key, peer, self_peer) {
            continue;
        }

        let alias = tailnet_peer_label(peer_key, peer);
        let (endpoint, endpoint_source, endpoint_diagnostics) =
            tailnet_peer_endpoint(peer_key, peer, &overrides);
        diagnostics.extend(endpoint_diagnostics);

        let online = match peer.get("Online") {
            Some(Value::Bool(value)) => Some(*value),
            _ => None,
        };
        let os_hint = tailnet_peer_os(peer);
        peers.push(TailnetPeerWire {
            peer_key: peer_key.clone(),
            alias: alias.clone(),
            endpoint: endpoint.clone(),
            endpoint_source: endpoint_source.clone(),
            online,
            os_hint: os_hint.clone(),
        });
        if endpoint.is_empty() {
            continue;
        }

        let health = match health_by_endpoint.get(endpoint.as_str()) {
            Some(observation) => classify_observation(&alias, observation),
            None => super::wire::TailnetHealthResultWire {
                schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
                compatibility: COMPATIBILITY_UNKNOWN.to_string(),
                reason: "health not observed".to_string(),
                diagnostic: None,
            },
        };
        if let Some(diagnostic) = health.diagnostic.clone() {
            diagnostics.push(diagnostic);
        }
        if online == Some(false) {
            diagnostics.push(MachineSetupDiagnosticWire::new(
                "tailnet_peer_offline",
                "info",
                &alias,
                format!("{alias} is offline according to tailscale status"),
            ));
        }
        if !os_hint.is_empty() && !LINUX_FAMILY_OS.contains(&os_hint.as_str()) {
            diagnostics.push(MachineSetupDiagnosticWire::new(
                "tailnet_peer_os_advisory",
                "info",
                &alias,
                format!(
                    "{alias} reports OS {os_hint}; gateway support is advisory"
                ),
            ));
        }

        candidates.push(DiscoveryCandidateWire {
            provider_ref: TAILNET_PROVIDER_REF.to_string(),
            endpoint,
            display_name: alias,
            machine_selector: String::new(),
            installation_pin: String::new(),
            detail: candidate_detail(
                &health.compatibility,
                &health.reason,
                online,
                &os_hint,
                &endpoint_source,
            ),
        });
    }

    Ok(TailnetDiscoveryResultWire {
        schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
        peers,
        candidates,
        diagnostics,
    })
}

fn classify_observation(
    alias: &str,
    observation: &TailnetHealthObservationWire,
) -> super::wire::TailnetHealthResultWire {
    if !observation.error_code.trim().is_empty() {
        return classify_probe_error(
            alias,
            observation.error_code.trim(),
            observation.error_reason.trim(),
        );
    }
    match &observation.payload {
        Some(Value::Object(map)) => classify_health_object(alias, map),
        Some(_) => classify_probe_error(
            alias,
            "tailnet_probe_unrelated_service",
            "returned non-object health JSON",
        ),
        None => super::wire::TailnetHealthResultWire {
            schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
            compatibility: COMPATIBILITY_UNKNOWN.to_string(),
            reason: "health not observed".to_string(),
            diagnostic: None,
        },
    }
}

fn index_health_observations(
    observations: &[TailnetHealthObservationWire],
) -> HashMap<String, TailnetHealthObservationWire> {
    let mut indexed = HashMap::new();
    for observation in observations {
        if !observation.endpoint.is_empty() {
            indexed.insert(observation.endpoint.clone(), observation.clone());
        }
    }
    indexed
}

fn endpoint_overrides(raw: &Value) -> HashMap<String, String> {
    let Some(map) = raw.as_object() else {
        return HashMap::new();
    };
    let mut overrides = HashMap::new();
    for (key, value) in map {
        if let Some(endpoint) = value.as_str().filter(|value| !value.is_empty())
        {
            overrides.insert(key.clone(), endpoint.to_string());
        }
    }
    overrides
}

fn tailnet_peer_endpoint(
    peer_key: &str,
    peer: &Map<String, Value>,
    overrides: &HashMap<String, String>,
) -> (String, String, Vec<MachineSetupDiagnosticWire>) {
    let label = tailnet_peer_label(peer_key, peer);
    if let Some(override_endpoint) =
        matching_endpoint_override(peer_key, peer, overrides)
    {
        if valid_https_endpoint(&override_endpoint) {
            return (
                override_endpoint,
                ENDPOINT_SOURCE_OVERRIDE.to_string(),
                vec![],
            );
        }
        return (
            String::new(),
            ENDPOINT_SOURCE_OVERRIDE.to_string(),
            vec![MachineSetupDiagnosticWire::new(
                "tailnet_endpoint_override_invalid",
                "error",
                &label,
                format!("{label} endpoint override must be a valid HTTPS URL"),
            )],
        );
    }

    match normalized_magic_dns(peer.get("DNSName")) {
        Some(dns_name) => (
            format!("https://{dns_name}"),
            ENDPOINT_SOURCE_DNS.to_string(),
            vec![],
        ),
        None => (
            String::new(),
            ENDPOINT_SOURCE_DNS.to_string(),
            vec![MachineSetupDiagnosticWire::new(
                "tailnet_peer_dns_invalid",
                "warning",
                &label,
                format!("{label} did not report a valid MagicDNS name"),
            )],
        ),
    }
}

fn matching_endpoint_override(
    peer_key: &str,
    peer: &Map<String, Value>,
    overrides: &HashMap<String, String>,
) -> Option<String> {
    for identity in peer_identity_values(peer_key, peer) {
        if let Some(endpoint) = overrides.get(&identity) {
            if !endpoint.is_empty() {
                return Some(endpoint.clone());
            }
        }
    }
    None
}

fn peer_identity_values(
    peer_key: &str,
    peer: &Map<String, Value>,
) -> Vec<String> {
    let mut values = vec![peer_key.to_string()];
    for field in ["ID", "PublicKey", "HostName", "DNSName", "Name"] {
        if let Some(raw) = string_field(peer, field) {
            values.push(raw.to_string());
            if let Some(normalized) = normalized_magic_dns(peer.get(field)) {
                values.push(normalized);
            }
        }
    }
    if let Some(Value::Array(ips)) = peer.get("TailscaleIPs") {
        for item in ips {
            if let Some(ip) = item.as_str().filter(|value| !value.is_empty()) {
                values.push(ip.to_string());
            }
        }
    }
    unique_strings(values)
}

fn is_self_peer(
    peer_key: &str,
    peer: &Map<String, Value>,
    self_peer: &Map<String, Value>,
) -> bool {
    if peer.get("Self") == Some(&Value::Bool(true)) {
        return true;
    }
    if self_peer.is_empty() {
        return false;
    }
    let peer_values: HashSet<String> =
        peer_identity_values(peer_key, peer).into_iter().collect();
    let self_values: HashSet<String> = peer_identity_values("self", self_peer)
        .into_iter()
        .collect();
    !peer_values.is_disjoint(&self_values)
}

fn tailnet_peer_label(peer_key: &str, peer: &Map<String, Value>) -> String {
    for field in ["HostName", "DNSName", "Name", "ID"] {
        if let Some(raw) = string_field(peer, field) {
            if field == "DNSName" {
                return normalized_magic_dns(Some(&Value::String(
                    raw.to_string(),
                )))
                .unwrap_or_else(|| {
                    raw.trim().trim_end_matches('.').to_string()
                });
            }
            return raw.to_string();
        }
    }
    peer_key.to_string()
}

fn tailnet_peer_os(peer: &Map<String, Value>) -> String {
    for field in ["OS", "os"] {
        if let Some(raw) = string_field(peer, field) {
            return raw.to_ascii_lowercase();
        }
    }
    if let Some(Value::Object(hostinfo)) = peer.get("Hostinfo") {
        if let Some(raw) = string_field(hostinfo, "OS") {
            return raw.to_ascii_lowercase();
        }
    }
    String::new()
}

fn normalized_magic_dns(value: Option<&Value>) -> Option<String> {
    let Value::String(raw) = value? else {
        return None;
    };
    let candidate = raw.trim().trim_end_matches('.').to_lowercase();
    if candidate.is_empty()
        || candidate.chars().count() > 253
        || !candidate.contains('.')
    {
        return None;
    }
    let labels: Vec<&str> = candidate.split('.').collect();
    if labels.iter().any(|label| !valid_dns_label(label)) {
        return None;
    }
    Some(candidate)
}

fn valid_dns_label(label: &str) -> bool {
    let chars: Vec<char> = label.chars().collect();
    if chars.is_empty() || chars.len() > 63 {
        return false;
    }
    if chars.first() == Some(&'-') || chars.last() == Some(&'-') {
        return false;
    }
    chars
        .iter()
        .all(|char| char.is_alphanumeric() || *char == '-')
}

fn valid_https_endpoint(value: &str) -> bool {
    if value.chars().any(char::is_whitespace) {
        return false;
    }
    let Some((scheme, rest)) = value.split_once("://") else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("https") {
        return false;
    }
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    !host.is_empty()
}

fn string_field<'a>(map: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    map.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
}

fn unique_strings(values: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    for value in values {
        if seen.insert(value.clone()) {
            result.push(value);
        }
    }
    result
}

fn candidate_detail(
    compatibility: &str,
    probe_reason: &str,
    online: Option<bool>,
    os_hint: &str,
    endpoint_source: &str,
) -> String {
    let mut parts = vec![format!("compatibility={compatibility}")];
    if !probe_reason.is_empty() {
        parts.push(probe_reason.to_string());
    }
    parts.push(match online {
        Some(true) => "tailscale=online".to_string(),
        Some(false) => "tailscale=offline".to_string(),
        None => "tailscale=unknown".to_string(),
    });
    if !os_hint.is_empty() {
        parts.push(format!("os={os_hint}"));
    }
    parts.push(format!("endpoint={endpoint_source}"));
    parts.join("; ")
}
