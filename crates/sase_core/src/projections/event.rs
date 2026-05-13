use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

pub const PROJECTION_EVENT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventSourceWire {
    pub source_type: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventCausalityWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relationship: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelopeWire {
    pub schema_version: u32,
    pub seq: i64,
    pub created_at: String,
    pub source: EventSourceWire,
    pub host_id: String,
    pub project_id: String,
    pub event_type: String,
    pub payload: JsonValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub causality: Vec<EventCausalityWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventAppendRequestWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    pub source: EventSourceWire,
    pub host_id: String,
    pub project_id: String,
    pub event_type: String,
    #[serde(default)]
    pub payload: JsonValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub causality: Vec<EventCausalityWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventAppendOutcomeWire {
    pub schema_version: u32,
    pub event: EventEnvelopeWire,
    pub duplicate: bool,
}

impl EventAppendRequestWire {
    pub fn into_envelope(
        self,
        seq: i64,
        created_at: String,
    ) -> EventEnvelopeWire {
        EventEnvelopeWire {
            schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
            seq,
            created_at,
            source: self.source,
            host_id: self.host_id,
            project_id: self.project_id,
            event_type: self.event_type,
            payload: self.payload,
            idempotency_key: self.idempotency_key,
            causality: self.causality,
            source_path: self.source_path,
            source_revision: self.source_revision,
        }
    }
}

pub(crate) fn canonical_json_string(
    value: &JsonValue,
) -> Result<String, serde_json::Error> {
    serde_json::to_string(value)
}

pub(crate) fn canonical_source_string(
    source: &EventSourceWire,
) -> Result<String, serde_json::Error> {
    serde_json::to_string(source)
}

pub(crate) fn canonical_causality_string(
    causality: &[EventCausalityWire],
) -> Result<String, serde_json::Error> {
    serde_json::to_string(causality)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_metadata_serializes_deterministically() {
        let mut first = EventSourceWire {
            source_type: "cli".to_string(),
            name: "sase bead".to_string(),
            ..EventSourceWire::default()
        };
        first.metadata.insert("z".to_string(), "2".to_string());
        first.metadata.insert("a".to_string(), "1".to_string());

        let mut second = EventSourceWire {
            source_type: "cli".to_string(),
            name: "sase bead".to_string(),
            ..EventSourceWire::default()
        };
        second.metadata.insert("a".to_string(), "1".to_string());
        second.metadata.insert("z".to_string(), "2".to_string());

        assert_eq!(
            canonical_source_string(&first).unwrap(),
            canonical_source_string(&second).unwrap()
        );
    }
}
