//! Total deterministic goal reducer and publish classes.
//!
//! [`reduce_goal_events`] is a total function of the event set: it
//! never panics and never errors on well-formed input. Ordering is
//! causal-then-id, so shuffling the input never changes the state.
//! Anything unexpected becomes a diagnostic or a timeline effect,
//! never a failure.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::wire::{
    GoalActorKindWire, GoalClaimStatusWire, GoalClaimWire, GoalCriterionWire,
    GoalDiagnosticWire, GoalEventKindWire, GoalEventPayloadWire, GoalEventWire,
    GoalSettleFlavorWire, GoalStateWire, GoalStatusWire,
    GoalTimelineEffectWire, GoalTimelineEntryWire, GoalWireError,
};

/// Publish class of an event kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalPublishClassWire {
    /// Published synchronously with the write.
    Sync,
    /// Published on the background batch leg.
    Batched,
}

impl GoalPublishClassWire {
    /// The class word used by publishers.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sync => "sync",
            Self::Batched => "batched",
        }
    }
}

/// Publish class for one event kind.
///
/// Everything is `sync` except `agent_attached` and `progress`,
/// which are `batched`.
pub fn goal_event_publish_class(
    kind: &GoalEventKindWire,
) -> GoalPublishClassWire {
    match kind {
        GoalEventKindWire::AgentAttached | GoalEventKindWire::Progress => {
            GoalPublishClassWire::Batched
        }
        _ => GoalPublishClassWire::Sync,
    }
}

/// Reduce one goal's events to its state.
///
/// The result is a total, deterministic function of the event set:
/// causal order first (every event after its `basis` when that
/// basis is present), ties broken by ascending `event_id`.
pub fn reduce_goal_events(
    goal_id: &str,
    events: &[GoalEventWire],
) -> GoalStateWire {
    let mut reducer = Reducer::new(goal_id, events);
    reducer.run()
}

struct Reducer<'a> {
    goal_id: String,
    events: Vec<&'a GoalEventWire>,
    state: GoalStateWire,
    order: Vec<usize>,
    present: BTreeSet<String>,
    parent: HashMap<String, String>,
    status_after: HashMap<String, GoalStatusWire>,
    seen_keys: HashMap<String, String>,
    settlement_winner: BTreeSet<usize>,
    claim_winner: BTreeSet<usize>,
    applied_claims: Vec<usize>,
}

impl<'a> Reducer<'a> {
    fn new(goal_id: &str, events: &'a [GoalEventWire]) -> Self {
        let mut reducer = Self {
            goal_id: goal_id.to_string(),
            events: Vec::new(),
            state: GoalStateWire::empty(goal_id),
            order: Vec::new(),
            present: BTreeSet::new(),
            parent: HashMap::new(),
            status_after: HashMap::new(),
            seen_keys: HashMap::new(),
            settlement_winner: BTreeSet::new(),
            claim_winner: BTreeSet::new(),
            applied_claims: Vec::new(),
        };
        reducer.dedupe(events);
        reducer.order();
        reducer.plan_races();
        reducer
    }

    /// Drop duplicate event ids deterministically.
    ///
    /// Duplicates can only come from redelivery; the canonical
    /// smallest serialization wins so input order never matters.
    fn dedupe(&mut self, events: &'a [GoalEventWire]) {
        let mut by_id: BTreeMap<String, Vec<&'a GoalEventWire>> =
            BTreeMap::new();
        for event in events {
            by_id.entry(event.event_id.clone()).or_default().push(event);
        }
        for (event_id, group) in by_id {
            if group.len() == 1 {
                self.events.push(group[0]);
                continue;
            }
            let mut ranked: Vec<(String, &'a GoalEventWire)> = group
                .into_iter()
                .map(|event| {
                    let json = serde_json::to_string(event).unwrap_or_default();
                    (json, event)
                })
                .collect();
            ranked.sort_by(|left, right| left.0.cmp(&right.0));
            self.events.push(ranked[0].1);
            self.state.diagnostics.push(GoalDiagnosticWire::for_event(
                "duplicate_event_id",
                format!(
                    "dropped {} duplicate bodies for {event_id}",
                    ranked.len() - 1,
                ),
                &event_id,
            ));
        }
    }

    /// Causal-then-id order via Kahn's algorithm.
    fn order(&mut self) {
        let mut index_of: HashMap<&str, usize> = HashMap::new();
        for (index, event) in self.events.iter().enumerate() {
            index_of.insert(event.event_id.as_str(), index);
            self.present.insert(event.event_id.clone());
        }
        let mut successors: HashMap<usize, Vec<usize>> = HashMap::new();
        let mut indegree = vec![0usize; self.events.len()];
        for (index, event) in self.events.iter().enumerate() {
            if let Some(basis) = event.basis.as_deref() {
                if let Some(parent) = index_of.get(basis) {
                    successors.entry(*parent).or_default().push(index);
                    indegree[index] += 1;
                    self.parent
                        .insert(event.event_id.clone(), (*basis).to_string());
                }
            }
        }
        let mut ready: BTreeSet<(String, usize)> = BTreeSet::new();
        for (index, event) in self.events.iter().enumerate() {
            if indegree[index] == 0 {
                ready.insert((event.event_id.clone(), index));
            }
        }
        let mut order = Vec::with_capacity(self.events.len());
        while let Some((_, index)) = ready.iter().next().cloned() {
            ready.remove(&(self.events[index].event_id.clone(), index));
            order.push(index);
            if let Some(next) = successors.get(&index) {
                for target in next {
                    indegree[*target] -= 1;
                    if indegree[*target] == 0 {
                        ready.insert((
                            self.events[*target].event_id.clone(),
                            *target,
                        ));
                    }
                }
            }
        }
        if order.len() < self.events.len() {
            let placed: BTreeSet<usize> = order.iter().cloned().collect();
            let mut rest: Vec<(String, usize)> = self
                .events
                .iter()
                .enumerate()
                .filter(|(index, _)| !placed.contains(index))
                .map(|(index, event)| (event.event_id.clone(), index))
                .collect();
            rest.sort();
            for (_, index) in rest {
                order.push(index);
            }
        }
        self.order = order;
    }

    /// Whether `ancestor` is reachable from `event` via basis links.
    fn reaches(&self, ancestor: &str, event: &str) -> bool {
        let mut current = event;
        for _ in 0..=self.events.len() {
            let Some(next) = self.parent.get(current) else {
                return false;
            };
            if next.as_str() == ancestor {
                return true;
            }
            current = next.as_str();
        }
        false
    }

    /// Two events are concurrent when neither reaches the other.
    fn concurrent(&self, first: &str, second: &str) -> bool {
        first != second
            && !self.reaches(first, second)
            && !self.reaches(second, first)
    }

    /// Precompute settlement and claim race winners.
    fn plan_races(&mut self) {
        let settlement_at: Vec<usize> = self
            .order
            .iter()
            .cloned()
            .filter(|index| {
                matches!(self.events[*index].kind, GoalEventKindWire::Settled)
                    && self.events[*index].parse_payload().is_ok()
            })
            .collect();
        self.settlement_winner = self.race_winners(&settlement_at, true);
        let claim_at: Vec<usize> = self
            .order
            .iter()
            .cloned()
            .filter(|index| {
                matches!(self.events[*index].kind, GoalEventKindWire::Claimed)
                    && self.events[*index].parse_payload().is_ok()
            })
            .collect();
        self.claim_winner = self.race_winners(&claim_at, false);
    }

    /// Winner per concurrency-connected group.
    ///
    /// Settlement groups prefer a human `canceled` drop, because it
    /// expresses the person's intent; every other group takes the
    /// first event in reduction order.
    fn race_winners(
        &self,
        candidates: &[usize],
        prefer_human_cancel: bool,
    ) -> BTreeSet<usize> {
        let mut parent: Vec<usize> = (0..candidates.len()).collect();
        fn find(parent: &mut [usize], mut node: usize) -> usize {
            while parent[node] != node {
                parent[node] = parent[parent[node]];
                node = parent[node];
            }
            node
        }
        for left in 0..candidates.len() {
            for right in (left + 1)..candidates.len() {
                let first = self.events[candidates[left]].event_id.clone();
                let second = self.events[candidates[right]].event_id.clone();
                if self.concurrent(&first, &second) {
                    let left_root = find(&mut parent, left);
                    let right_root = find(&mut parent, right);
                    if left_root != right_root {
                        parent[left_root] = right_root;
                    }
                }
            }
        }
        let mut groups: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for (slot, _) in candidates.iter().enumerate() {
            let root = find(&mut parent.clone(), slot);
            groups.entry(root).or_default().push(slot);
        }
        let mut winners = BTreeSet::new();
        for (_, mut slots) in groups {
            slots.sort();
            let mut winner = slots[0];
            if prefer_human_cancel {
                for slot in slots {
                    let event = self.events[candidates[slot]];
                    if let Ok(GoalEventPayloadWire::Settled(payload)) =
                        event.parse_payload()
                    {
                        if payload.flavor
                            == Some(GoalSettleFlavorWire::Canceled)
                            && event.actor.kind == GoalActorKindWire::Human
                        {
                            winner = slot;
                            break;
                        }
                    }
                }
            }
            winners.insert(candidates[winner]);
        }
        winners
    }

    fn run(&mut self) -> GoalStateWire {
        let order = self.order.clone();
        for index in order {
            self.reduce_one(index);
        }
        self.state.schema_version = super::wire::GOAL_WIRE_SCHEMA_VERSION;
        self.state.id = self.goal_id.clone();
        self.state.clone()
    }

    fn reduce_one(&mut self, index: usize) {
        let event = self.events[index].clone();
        let event_id = event.event_id.clone();
        if event.goal_id != self.goal_id {
            self.diagnostic(
                "goal_id_mismatch",
                format!(
                    "event targets {} but reduced under {}",
                    event.goal_id, self.goal_id,
                ),
                &event_id,
            );
            // A foreign event is not part of this goal: it leaves no
            // timeline entry and never advances head, so it can never
            // become the basis of a later action on this goal. The
            // diagnostic above keeps the evidence.
            return;
        }
        if !event.idempotency_key.is_empty() {
            if let Some(first) =
                self.seen_keys.get(&event.idempotency_key).cloned()
            {
                self.diagnostic(
                    "duplicate",
                    format!(
                        "idempotency key {} already counted for {first}",
                        event.idempotency_key,
                    ),
                    &event_id,
                );
                self.timeline(
                    &event,
                    GoalTimelineEffectWire::Duplicate,
                    "duplicate action",
                );
                self.status_after.insert(event_id, self.state.status);
                return;
            }
            self.seen_keys
                .insert(event.idempotency_key.clone(), event_id.clone());
        }
        if let Some(basis) = event.basis.as_deref() {
            if !self.present.contains(basis) {
                self.diagnostic(
                    "missing_basis",
                    format!("basis {basis} is not in the event set"),
                    &event_id,
                );
            }
        }
        let payload = match event.parse_payload() {
            Ok(payload) => payload,
            Err(GoalWireError::UnsupportedKind(kind)) => {
                self.mark_unreadable(format!(
                    "unsupported event kind {kind:?} \
                     (event {event_id})"
                ));
                self.timeline(
                    &event,
                    GoalTimelineEffectWire::Ignored,
                    "unsupported kind",
                );
                self.status_after.insert(event_id, self.state.status);
                self.state.head = Some(event.event_id.clone());
                return;
            }
            Err(GoalWireError::UnsupportedVersion(version)) => {
                self.mark_unreadable(format!(
                    "unsupported schema_version {version} \
                     (event {event_id}); run `sase update`"
                ));
                self.timeline(
                    &event,
                    GoalTimelineEffectWire::Ignored,
                    "unsupported version",
                );
                self.status_after.insert(event_id, self.state.status);
                self.state.head = Some(event.event_id.clone());
                return;
            }
            Err(error) => {
                self.diagnostic(
                    "invalid_payload",
                    error.to_string(),
                    &event_id,
                );
                self.timeline(
                    &event,
                    GoalTimelineEffectWire::Ignored,
                    "invalid payload",
                );
                self.status_after.insert(event_id, self.state.status);
                self.state.head = Some(event.event_id.clone());
                return;
            }
        };
        if self.state.created_at.is_empty()
            && !matches!(payload, GoalEventPayloadWire::Created(_))
        {
            self.diagnostic(
                "missing_created",
                "event arrived before any created event",
                &event_id,
            );
            self.timeline(
                &event,
                GoalTimelineEffectWire::Ignored,
                "no goal yet",
            );
            self.status_after.insert(event_id, self.state.status);
            self.state.head = Some(event.event_id.clone());
            return;
        }
        match payload {
            GoalEventPayloadWire::Created(payload) => {
                self.apply_created(&event, &payload);
            }
            GoalEventPayloadWire::Named(payload) => {
                self.apply_named(&event, &payload);
            }
            GoalEventPayloadWire::Edited(payload) => {
                self.apply_edited(&event, &payload);
            }
            GoalEventPayloadWire::Adopted(payload) => {
                self.apply_adopted(&event, &payload);
            }
            GoalEventPayloadWire::AgentAttached(payload) => {
                self.apply_agent_attached(&event, &payload);
            }
            GoalEventPayloadWire::Progress(payload) => {
                self.apply_progress(&event, &payload);
            }
            GoalEventPayloadWire::PlanAttached(payload) => {
                self.apply_plan_attached(&event, &payload);
            }
            GoalEventPayloadWire::Claimed(payload) => {
                self.apply_claimed(index, &event, &payload);
            }
            GoalEventPayloadWire::ClaimRetracted(payload) => {
                self.apply_claim_retracted(&event, &payload);
            }
            GoalEventPayloadWire::Settled(payload) => {
                self.apply_settled(index, &event, &payload);
            }
            GoalEventPayloadWire::Reopened(payload) => {
                self.apply_reopened(&event, &payload);
            }
            GoalEventPayloadWire::Merged(payload) => {
                self.apply_merged(&event, &payload);
            }
        }
        self.status_after.insert(event_id, self.state.status);
    }

    /// Did the writer's basis already show a settled goal?
    fn basis_showed_settled(&self, event: &GoalEventWire) -> bool {
        match event.basis.as_deref() {
            Some(basis) => match self.status_after.get(basis) {
                Some(status) => status.is_settled(),
                None => false,
            },
            None => false,
        }
    }

    fn mark_unreadable(&mut self, reason: String) {
        if self.state.readable {
            self.state.readable = false;
            self.state.unreadable_reason = Some(reason);
        }
    }

    fn diagnostic(
        &mut self,
        code: &str,
        message: impl Into<String>,
        event_id: &str,
    ) {
        self.state
            .diagnostics
            .push(GoalDiagnosticWire::for_event(code, message, event_id));
    }

    fn timeline(
        &mut self,
        event: &GoalEventWire,
        effect: GoalTimelineEffectWire,
        summary: &str,
    ) {
        self.state.timeline.push(GoalTimelineEntryWire {
            event_id: event.event_id.clone(),
            at: event.at.clone(),
            kind: event.kind.clone(),
            actor: event.actor.clone(),
            summary: summary.to_string(),
            effect,
        });
        self.state.head = Some(event.event_id.clone());
    }

    fn touch(&mut self, at: &str) {
        self.state.updated_at = at.to_string();
    }

    fn add_contributor(&mut self, name: &str) {
        if !name.is_empty()
            && !self.state.contributors.iter().any(|known| known == name)
        {
            self.state.contributors.push(name.to_string());
        }
    }

    fn apply_created(
        &mut self,
        event: &GoalEventWire,
        payload: &super::wire::GoalCreatedPayloadWire,
    ) {
        let event_id = event.event_id.clone();
        if !self.state.created_at.is_empty() {
            self.mark_unreadable(format!(
                "id_collision: second created event {event_id}"
            ));
            self.diagnostic(
                "id_collision",
                format!(
                    "second created event {event_id}; \
                     drop one goal and recreate it"
                ),
                &event_id,
            );
            self.timeline(
                event,
                GoalTimelineEffectWire::Ignored,
                "duplicate goal id",
            );
            return;
        }
        self.state.project = payload.project.clone();
        self.state.status = if payload.draft {
            GoalStatusWire::Draft
        } else {
            GoalStatusWire::Active
        };
        self.state.title = payload.title.clone();
        self.state.outcome = payload.outcome.clone();
        self.state.criteria = payload
            .criteria
            .iter()
            .enumerate()
            .take(super::actions::GOAL_CRITERIA_MAX)
            .map(|(index, input)| GoalCriterionWire {
                id: format!("{event_id}.{index}"),
                text: input.text.clone(),
                source: input.source,
            })
            .collect();
        if payload.criteria.len() > super::actions::GOAL_CRITERIA_MAX {
            self.diagnostic(
                "criteria_limit",
                format!(
                    "created with {} criteria; kept {}",
                    payload.criteria.len(),
                    super::actions::GOAL_CRITERIA_MAX,
                ),
                &event_id,
            );
        }
        self.state.origin = payload.origin.clone();
        self.state.revision = 1;
        self.state.created_at = event.at.clone();
        let summary = format!("created by {}", event.actor.principal);
        self.timeline(event, GoalTimelineEffectWire::Applied, &summary);
        self.touch(&event.at.clone());
    }

    fn apply_named(
        &mut self,
        event: &GoalEventWire,
        payload: &super::wire::GoalNamedPayloadWire,
    ) {
        let event_id = event.event_id.clone();
        match self.state.status {
            GoalStatusWire::Draft | GoalStatusWire::Active => {
                self.state.title = payload.title.clone();
                self.state.outcome = payload.outcome.clone();
                self.state.status = GoalStatusWire::Active;
                self.state.revision += 1;
                let summary = format!(
                    "named {:?} by {}",
                    payload.title, event.actor.principal
                );
                self.timeline(event, GoalTimelineEffectWire::Applied, &summary);
                self.touch(&event.at.clone());
            }
            _ => {
                self.diagnostic(
                    "illegal_transition",
                    format!(
                        "named is illegal from {}",
                        self.state.status.as_str()
                    ),
                    &event_id,
                );
                self.timeline(
                    event,
                    GoalTimelineEffectWire::Ignored,
                    "named out of state",
                );
            }
        }
    }

    fn apply_edited(
        &mut self,
        event: &GoalEventWire,
        payload: &super::wire::GoalEditedPayloadWire,
    ) {
        let event_id = event.event_id.clone();
        if self.state.status.is_settled() && self.basis_showed_settled(event) {
            self.diagnostic(
                "on_settled",
                "edited after the writer saw a settled goal",
                &event_id,
            );
            self.timeline(
                event,
                GoalTimelineEffectWire::Ignored,
                "edited a settled goal",
            );
            return;
        }
        if let Some(title) = payload.title.as_deref() {
            self.state.title = title.to_string();
        }
        if let Some(outcome) = payload.outcome.as_deref() {
            self.state.outcome = outcome.to_string();
        }
        for input in &payload.criteria_added {
            if self.state.criteria.len() >= super::actions::GOAL_CRITERIA_MAX {
                self.diagnostic(
                    "criteria_limit",
                    "edited past 10 criteria; extras dropped".to_string(),
                    &event_id,
                );
                break;
            }
            let id = format!("{event_id}.{}", self.state.criteria.len());
            self.state.criteria.push(GoalCriterionWire {
                id,
                text: input.text.clone(),
                source: input.source,
            });
        }
        for removed in &payload.criteria_removed {
            if let Some(position) = self
                .state
                .criteria
                .iter()
                .position(|known| &known.id == removed)
            {
                let source = self.state.criteria[position].source;
                if event.actor.kind == GoalActorKindWire::Agent
                    && source != super::wire::GoalCriterionSourceWire::Agent
                {
                    self.diagnostic(
                        "agent_cannot_remove_criterion",
                        format!(
                            "agent writer cannot remove {source:?} \
                             criterion {removed}"
                        ),
                        &event_id,
                    );
                    continue;
                }
                self.state.criteria.remove(position);
            } else {
                self.diagnostic(
                    "unknown_criterion",
                    format!("criterion {removed} is not on the goal"),
                    &event_id,
                );
            }
        }
        self.state.revision += 1;
        let summary = edit_summary(payload);
        self.timeline(event, GoalTimelineEffectWire::Applied, &summary);
        self.touch(&event.at.clone());
    }

    fn apply_adopted(
        &mut self,
        event: &GoalEventWire,
        payload: &super::wire::GoalAdoptedPayloadWire,
    ) {
        let event_id = event.event_id.clone();
        self.add_contributor(&payload.agent.clone());
        let summary = format!("adopted by {}", payload.agent);
        if self.state.status.is_settled() {
            self.diagnostic(
                "late_attachment",
                "adopted concurrent with or after settlement",
                &event_id,
            );
        }
        self.timeline(event, GoalTimelineEffectWire::Applied, &summary);
        self.touch(&event.at.clone());
    }

    fn apply_agent_attached(
        &mut self,
        event: &GoalEventWire,
        payload: &super::wire::GoalAgentAttachedPayloadWire,
    ) {
        let event_id = event.event_id.clone();
        self.add_contributor(&payload.agent.clone());
        let summary = format!("attached agent {}", payload.agent);
        if self.state.status.is_settled() {
            self.diagnostic(
                "late_attachment",
                "agent_attached concurrent with or after settlement",
                &event_id,
            );
        }
        self.timeline(event, GoalTimelineEffectWire::Applied, &summary);
        self.touch(&event.at.clone());
    }

    fn apply_progress(
        &mut self,
        event: &GoalEventWire,
        payload: &super::wire::GoalProgressPayloadWire,
    ) {
        let event_id = event.event_id.clone();
        if self.state.status.is_settled() && self.basis_showed_settled(event) {
            self.diagnostic(
                "on_settled",
                "progress after the writer saw a settled goal",
                &event_id,
            );
            self.timeline(
                event,
                GoalTimelineEffectWire::Ignored,
                "progress on a settled goal",
            );
            return;
        }
        self.state.last_progress = Some(payload.note.clone());
        self.add_contributor(&payload.agent.clone());
        let summary =
            format!("progress by {}: {}", payload.agent, payload.note);
        self.timeline(
            event,
            GoalTimelineEffectWire::Applied,
            &truncate(&summary, 160),
        );
        self.touch(&event.at.clone());
    }

    fn apply_plan_attached(
        &mut self,
        event: &GoalEventWire,
        payload: &super::wire::GoalPlanAttachedPayloadWire,
    ) {
        let event_id = event.event_id.clone();
        if self.state.status.is_settled() && self.basis_showed_settled(event) {
            self.diagnostic(
                "on_settled",
                "plan_attached after the writer saw a settled goal",
                &event_id,
            );
            self.timeline(
                event,
                GoalTimelineEffectWire::Ignored,
                "plan on a settled goal",
            );
            return;
        }
        self.state.plan = Some(payload.plan_ref.clone());
        let summary = format!("attached plan {}", payload.plan_ref);
        self.timeline(event, GoalTimelineEffectWire::Applied, &summary);
        self.touch(&event.at.clone());
    }

    fn apply_claimed(
        &mut self,
        index: usize,
        event: &GoalEventWire,
        payload: &super::wire::GoalClaimedPayloadWire,
    ) {
        let event_id = event.event_id.clone();
        if !self.claim_winner.contains(&index) {
            self.diagnostic(
                "superseded_claim",
                format!(
                    "claim {} lost a concurrent claim race",
                    payload.claim_no
                ),
                &event_id,
            );
            self.timeline(
                event,
                GoalTimelineEffectWire::Superseded,
                "superseded claim",
            );
            return;
        }
        if self.state.status != GoalStatusWire::Active {
            if self.state.status.is_settled()
                && self.beaten_by_cancel(&event_id)
            {
                self.diagnostic(
                    "superseded_claim",
                    format!(
                        "claim {} is concurrent with a canceled drop",
                        payload.claim_no
                    ),
                    &event_id,
                );
                self.timeline(
                    event,
                    GoalTimelineEffectWire::Superseded,
                    "claim beaten by a drop",
                );
            } else {
                self.diagnostic(
                    "illegal_transition",
                    format!(
                        "claimed is illegal from {}",
                        self.state.status.as_str()
                    ),
                    &event_id,
                );
                self.timeline(
                    event,
                    GoalTimelineEffectWire::Ignored,
                    "claimed out of state",
                );
            }
            return;
        }
        let agent = event.actor.agent.clone().unwrap_or_default();
        self.add_contributor(&agent.clone());
        self.state.claims.push(GoalClaimWire {
            claim_no: payload.claim_no,
            claim: payload.claim.clone(),
            agent,
            strength: payload.strength,
            status: GoalClaimStatusWire::Active,
        });
        self.applied_claims.push(index);
        self.state.status = GoalStatusWire::Review;
        let summary =
            format!("claimed #{}: {}", payload.claim_no, payload.claim);
        self.timeline(
            event,
            GoalTimelineEffectWire::Applied,
            &truncate(&summary, 160),
        );
        self.touch(&event.at.clone());
    }

    /// Whether a concurrent human `canceled` drop beats this event.
    fn beaten_by_cancel(&self, event_id: &str) -> bool {
        for order_index in &self.settlement_winner {
            let winner = self.events[*order_index];
            if let Ok(GoalEventPayloadWire::Settled(payload)) =
                winner.parse_payload()
            {
                if payload.flavor == Some(GoalSettleFlavorWire::Canceled)
                    && winner.actor.kind == GoalActorKindWire::Human
                    && self.concurrent(&winner.event_id, event_id)
                {
                    return true;
                }
            }
        }
        false
    }

    fn apply_claim_retracted(
        &mut self,
        event: &GoalEventWire,
        payload: &super::wire::GoalClaimRetractedPayloadWire,
    ) {
        let event_id = event.event_id.clone();
        if self.state.status != GoalStatusWire::Review {
            self.diagnostic(
                "illegal_transition",
                format!(
                    "claim_retracted is illegal from {}",
                    self.state.status.as_str()
                ),
                &event_id,
            );
            self.timeline(
                event,
                GoalTimelineEffectWire::Ignored,
                "retraction out of state",
            );
            return;
        }
        let mut found = false;
        for claim in &mut self.state.claims {
            if claim.claim_no == payload.claim_no
                && claim.status == GoalClaimStatusWire::Active
            {
                claim.status = GoalClaimStatusWire::Retracted;
                found = true;
            }
        }
        if !found {
            self.diagnostic(
                "unknown_claim",
                format!("claim #{} is not an active claim", payload.claim_no),
                &event_id,
            );
        }
        self.state.status = GoalStatusWire::Active;
        let summary = format!("retracted claim #{}", payload.claim_no);
        self.timeline(event, GoalTimelineEffectWire::Applied, &summary);
        self.touch(&event.at.clone());
    }

    fn apply_settled(
        &mut self,
        index: usize,
        event: &GoalEventWire,
        payload: &super::wire::GoalSettledPayloadWire,
    ) {
        let event_id = event.event_id.clone();
        if !self.settlement_winner.contains(&index) {
            self.diagnostic(
                "superseded_settlement",
                "settlement lost a concurrent settlement race",
                &event_id,
            );
            self.timeline(
                event,
                GoalTimelineEffectWire::Superseded,
                "superseded settlement",
            );
            return;
        }
        if self.state.status.is_settled() {
            self.diagnostic(
                "already_settled",
                format!("goal is already {}", self.state.status.as_str()),
                &event_id,
            );
            self.timeline(
                event,
                GoalTimelineEffectWire::Ignored,
                "already settled",
            );
            return;
        }
        let Some(flavor) = payload.flavor else {
            self.diagnostic(
                "invalid_settlement",
                "settled event carries no flavor".to_string(),
                &event_id,
            );
            self.timeline(
                event,
                GoalTimelineEffectWire::Ignored,
                "flavorless settlement",
            );
            return;
        };
        match flavor {
            GoalSettleFlavorWire::Verified
            | GoalSettleFlavorWire::Acknowledged => {
                self.state.status = GoalStatusWire::Done;
            }
            GoalSettleFlavorWire::Canceled
            | GoalSettleFlavorWire::Merged
            | GoalSettleFlavorWire::Superseded => {
                self.state.status = GoalStatusWire::Dropped;
            }
        }
        self.state.flavor = Some(flavor);
        if flavor == GoalSettleFlavorWire::Merged {
            match payload.into.as_deref() {
                Some(target) if !target.is_empty() => {
                    self.state.merged_into = Some(target.to_string());
                }
                _ => {
                    self.diagnostic(
                        "missing_merge_target",
                        "merged settlement names no target".to_string(),
                        &event_id,
                    );
                }
            }
        }
        if flavor == GoalSettleFlavorWire::Canceled
            && event.actor.kind == GoalActorKindWire::Human
        {
            let beaten: Vec<String> = self
                .applied_claims
                .iter()
                .map(|claim_index| self.events[*claim_index].event_id.clone())
                .filter(|claim_id| self.concurrent(claim_id, &event_id))
                .collect();
            for claim_id in beaten {
                self.diagnostic(
                    "canceled_beats_claim",
                    format!(
                        "canceled drop {event_id} beats \
                         concurrent claim {claim_id}"
                    ),
                    &event_id,
                );
            }
        }
        let mut summary = format!("settled {}", flavor.as_str());
        if let Some(note) = payload.note.as_deref() {
            summary.push_str(": ");
            summary.push_str(note);
        }
        self.timeline(
            event,
            GoalTimelineEffectWire::Applied,
            &truncate(&summary, 160),
        );
        self.touch(&event.at.clone());
    }

    fn apply_reopened(
        &mut self,
        event: &GoalEventWire,
        payload: &super::wire::GoalReopenedPayloadWire,
    ) {
        let event_id = event.event_id.clone();
        if !self.state.status.is_settled() {
            self.diagnostic(
                "illegal_transition",
                format!(
                    "reopened is illegal from {}",
                    self.state.status.as_str()
                ),
                &event_id,
            );
            self.timeline(
                event,
                GoalTimelineEffectWire::Ignored,
                "reopened an unsettled goal",
            );
            return;
        }
        self.state.status = GoalStatusWire::Active;
        self.state.flavor = None;
        let summary = format!("reopened: {}", payload.message);
        self.timeline(
            event,
            GoalTimelineEffectWire::Applied,
            &truncate(&summary, 160),
        );
        self.touch(&event.at.clone());
    }

    fn apply_merged(
        &mut self,
        event: &GoalEventWire,
        payload: &super::wire::GoalMergedPayloadWire,
    ) {
        let event_id = event.event_id.clone();
        if payload.from.is_empty() {
            self.diagnostic(
                "invalid_merge",
                "merged record names no source goal".to_string(),
                &event_id,
            );
            self.timeline(
                event,
                GoalTimelineEffectWire::Ignored,
                "sourceless merge",
            );
            return;
        }
        if self.state.status.is_settled() && self.basis_showed_settled(event) {
            self.diagnostic(
                "on_settled",
                "merged after the writer saw a settled goal",
                &event_id,
            );
            self.timeline(
                event,
                GoalTimelineEffectWire::Ignored,
                "merge on a settled goal",
            );
            return;
        }
        if !self
            .state
            .merged_from
            .iter()
            .any(|known| known == &payload.from)
        {
            self.state.merged_from.push(payload.from.clone());
        }
        let summary = format!("merged goal:{} into this goal", payload.from);
        self.timeline(event, GoalTimelineEffectWire::Applied, &summary);
        self.touch(&event.at.clone());
    }
}

fn edit_summary(payload: &super::wire::GoalEditedPayloadWire) -> String {
    let mut parts = Vec::new();
    if payload.title.is_some() {
        parts.push("title".to_string());
    }
    if payload.outcome.is_some() {
        parts.push("outcome".to_string());
    }
    if !payload.criteria_added.is_empty() {
        parts.push(format!("+{} criteria", payload.criteria_added.len()));
    }
    if !payload.criteria_removed.is_empty() {
        parts.push(format!("-{} criteria", payload.criteria_removed.len()));
    }
    if let Some(note) = payload.note.as_deref() {
        parts.push(note.to_string());
    }
    if parts.is_empty() {
        return "edited".to_string();
    }
    truncate(&format!("edited {}", parts.join(", ")), 160)
}

fn truncate(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_string();
    }
    let kept: String = value.chars().take(max - 1).collect();
    format!("{kept}…")
}
