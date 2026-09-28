//! Shared builders for goal tests.

use serde_json::json;

use crate::goal::ids::test_event_id;
use crate::goal::wire::{
    GoalActorKindWire, GoalActorWire, GoalEventKindWire, GoalEventWire,
};

/// Fixture goal id used across tests.
pub const GOAL: &str = "7k2mq";

/// Second fixture goal id for merge tests.
pub const OTHER_GOAL: &str = "3fq9t";

/// Fixed RFC3339 clock readings.
pub const T0: &str = "2026-09-28T14:00:00.000Z";
/// Fixed RFC3339 clock readings.
pub const T1: &str = "2026-09-28T14:01:00.000Z";
/// Fixed RFC3339 clock readings.
pub const T2: &str = "2026-09-28T14:02:00.000Z";
/// Fixed RFC3339 clock readings.
pub const T3: &str = "2026-09-28T14:03:00.000Z";
/// Fixed RFC3339 clock readings.
pub const T4: &str = "2026-09-28T14:04:00.000Z";
/// Fixed RFC3339 clock readings.
pub const T5: &str = "2026-09-28T14:05:00.000Z";

/// Deterministic event id for `(timestamp_ms, seed)`.
pub fn eid(timestamp_ms: u64, seed: u8) -> String {
    test_event_id(timestamp_ms, seed)
}

/// A human actor.
pub fn human() -> GoalActorWire {
    GoalActorWire {
        principal: "bryan.athena".to_string(),
        kind: GoalActorKindWire::Human,
        agent: None,
    }
}

/// An agent actor.
pub fn agent(name: &str) -> GoalActorWire {
    GoalActorWire {
        principal: format!("bryan.athena:{name}"),
        kind: GoalActorKindWire::Agent,
        agent: Some(name.to_string()),
    }
}

/// A host actor.
pub fn host() -> GoalActorWire {
    GoalActorWire {
        principal: "sase.athena".to_string(),
        kind: GoalActorKindWire::Host,
        agent: None,
    }
}

/// Build a raw envelope around a payload value.
#[allow(clippy::too_many_arguments)]
pub fn event(
    goal_id: &str,
    event_id: &str,
    kind: GoalEventKindWire,
    at: &str,
    actor: GoalActorWire,
    basis: Option<&str>,
    key: &str,
    payload: serde_json::Value,
) -> GoalEventWire {
    GoalEventWire {
        schema_version: 1,
        event_id: event_id.to_string(),
        goal_id: goal_id.to_string(),
        kind,
        at: at.to_string(),
        actor,
        basis: basis.map(str::to_string),
        idempotency_key: key.to_string(),
        payload,
    }
}

/// A `created` event with one user criterion.
#[allow(clippy::too_many_arguments)]
pub fn created(
    goal_id: &str,
    event_id: &str,
    at: &str,
    actor: GoalActorWire,
    key: &str,
    title: &str,
    outcome: &str,
    project: &str,
) -> GoalEventWire {
    event(
        goal_id,
        event_id,
        GoalEventKindWire::Created,
        at,
        actor.clone(),
        None,
        key,
        json!({
            "title": title,
            "outcome": outcome,
            "criteria": [
                {"text": "Covers storage and sync", "source": "user"}
            ],
            "origin": {
                "kind": "cli",
                "principal": actor.principal,
                "machine": "athena",
                "at": at,
                "via": "cli",
            },
            "draft": false,
            "project": project,
        }),
    )
}

/// A `settled` event.
#[allow(clippy::too_many_arguments)]
pub fn settled(
    goal_id: &str,
    event_id: &str,
    at: &str,
    actor: GoalActorWire,
    basis: &str,
    key: &str,
    flavor: &str,
    into: Option<&str>,
    note: Option<&str>,
) -> GoalEventWire {
    let mut payload = json!({"flavor": flavor});
    if let Some(target) = into {
        payload["into"] = json!(target);
    }
    if let Some(note) = note {
        payload["note"] = json!(note);
    }
    event(
        goal_id,
        event_id,
        GoalEventKindWire::Settled,
        at,
        actor,
        Some(basis),
        key,
        payload,
    )
}

/// An `edited` event that retitles the goal.
pub fn retitle(
    goal_id: &str,
    event_id: &str,
    at: &str,
    actor: GoalActorWire,
    basis: &str,
    key: &str,
    title: &str,
) -> GoalEventWire {
    event(
        goal_id,
        event_id,
        GoalEventKindWire::Edited,
        at,
        actor,
        Some(basis),
        key,
        json!({"title": title}),
    )
}

/// A `claimed` event.
pub fn claimed(
    goal_id: &str,
    event_id: &str,
    at: &str,
    actor: GoalActorWire,
    basis: &str,
    key: &str,
    claim_no: u32,
) -> GoalEventWire {
    event(
        goal_id,
        event_id,
        GoalEventKindWire::Claimed,
        at,
        actor,
        Some(basis),
        key,
        json!({
            "claim_no": claim_no,
            "claim": "The design doc is written",
            "evidence": [
                {"ref": "doc:goals", "why": "Design covers storage"}
            ],
            "check": ["read the doc"],
            "gaps": [],
            "receipts": [],
            "strength": "documented",
        }),
    )
}

/// A `progress` event from the fixture agent.
pub fn progressed(
    goal_id: &str,
    event_id: &str,
    at: &str,
    basis: &str,
    key: &str,
    note: &str,
) -> GoalEventWire {
    event(
        goal_id,
        event_id,
        GoalEventKindWire::Progress,
        at,
        agent("agent-a"),
        Some(basis),
        key,
        serde_json::json!({"note": note, "agent": "agent-a"}),
    )
}

/// A `reopened` event.
pub fn reopened(
    goal_id: &str,
    event_id: &str,
    at: &str,
    actor: GoalActorWire,
    basis: &str,
    key: &str,
) -> GoalEventWire {
    event(
        goal_id,
        event_id,
        GoalEventKindWire::Reopened,
        at,
        actor,
        Some(basis),
        key,
        json!({"message": "Needs another pass"}),
    )
}

/// Chain a linear history: created, retitle, settled.
pub fn linear_history() -> (String, Vec<GoalEventWire>) {
    let create = eid(1000, 1);
    let edit = eid(2000, 2);
    let settle = eid(3000, 3);
    let events = vec![
        created(
            GOAL,
            &create,
            T0,
            human(),
            "k:create",
            "Title",
            "Outcome",
            "sase",
        ),
        retitle(GOAL, &edit, T1, human(), &create, "k:edit", "New title"),
        settled(
            GOAL,
            &settle,
            T2,
            human(),
            &edit,
            "k:drop",
            "canceled",
            None,
            Some("nope"),
        ),
    ];
    (settle, events)
}
