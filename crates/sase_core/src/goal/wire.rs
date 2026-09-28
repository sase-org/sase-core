//! Goal event envelope, payload, and state wire records.
//!
//! Implements exactly the frozen G1 contract: ids, the event envelope,
//! one payload per event kind, criteria, origin, publish classes, and
//! the reduced state. Unknown fields are ignored everywhere. Unknown
//! kinds and unsupported versions surface as a typed [`GoalEventKindWire`]
//! value so the reducer can mark that goal unreadable instead of
//! erroring.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// Ledger `STORE.json` schema version.
pub const GOAL_LEDGER_SCHEMA_VERSION: u32 = 1;

/// Goal event and state wire schema version.
pub const GOAL_WIRE_SCHEMA_VERSION: u32 = 1;

/// Errors from goal wire parsing and validation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GoalWireError {
    /// A value failed validation.
    #[error("goal validation: {0}")]
    Validation(String),
    /// The envelope carries an event kind core does not know.
    #[error("unsupported goal event kind: {0}")]
    UnsupportedKind(String),
    /// The envelope carries a schema version core does not know.
    #[error("unsupported goal wire schema_version: {0}")]
    UnsupportedVersion(u32),
    /// A payload failed to deserialize.
    #[error("goal payload: {0}")]
    Payload(String),
}

/// Actor kinds that can write goal events.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum GoalActorKindWire {
    /// A person.
    #[default]
    Human,
    /// An agent.
    Agent,
    /// Machine automation.
    Host,
}

/// The actor credited with an event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalActorWire {
    /// Dotted `username.machine` principal.
    #[serde(default)]
    pub principal: String,
    /// Human, agent, or host.
    #[serde(default)]
    pub kind: GoalActorKindWire,
    /// Agent name for agent-kind actors.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
}

/// Goal lifecycle statuses.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum GoalStatusWire {
    /// Machine-local draft (G4 reserves this).
    Draft,
    /// Work is open.
    #[default]
    Active,
    /// A claim awaits review (G3).
    Review,
    /// Settled as verified or acknowledged.
    Done,
    /// Settled as canceled, merged, or superseded.
    Dropped,
}

impl GoalStatusWire {
    /// The status word shown in rows and badges.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Active => "active",
            Self::Review => "review",
            Self::Done => "done",
            Self::Dropped => "dropped",
        }
    }

    /// Whether the status counts as unsettled (hot).
    pub fn is_unsettled(self) -> bool {
        matches!(self, Self::Draft | Self::Active | Self::Review)
    }

    /// Whether the status counts as settled.
    pub fn is_settled(self) -> bool {
        !self.is_unsettled()
    }
}

/// Settlement flavors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalSettleFlavorWire {
    /// Verified complete.
    Verified,
    /// Acknowledged without verification.
    Acknowledged,
    /// Dropped by the person.
    Canceled,
    /// Merged into another goal.
    Merged,
    /// Superseded by another settlement.
    Superseded,
}

impl GoalSettleFlavorWire {
    /// The flavor word stored on state.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::Acknowledged => "acknowledged",
            Self::Canceled => "canceled",
            Self::Merged => "merged",
            Self::Superseded => "superseded",
        }
    }
}

/// Where a criterion came from.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum GoalCriterionSourceWire {
    /// Written by the person.
    #[default]
    User,
    /// Written by a plan.
    Plan,
    /// Written by an agent.
    Agent,
}

impl GoalCriterionSourceWire {
    /// The source word stored on state.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Plan => "plan",
            Self::Agent => "agent",
        }
    }
}

/// Claim strength classes (G3 producers).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalClaimStrengthWire {
    /// Backed by tests.
    Tested,
    /// Backed by commits.
    Committed,
    /// Backed by documentation.
    Documented,
    /// Backed by answers.
    Answered,
}

/// Claim-retraction reasons (G3/G4 producers).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalClaimRetractReasonWire {
    /// The claim was rejected.
    Rejected,
    /// The claim became follow-up work.
    Followup,
    /// The claim was adopted.
    Adopted,
    /// The claim went stale.
    Stale,
}

/// Lifecycle of a reduced claim record.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum GoalClaimStatusWire {
    /// The claim is current.
    #[default]
    Active,
    /// A concurrent claim won instead.
    Superseded,
    /// The claim was retracted.
    Retracted,
}

/// The frozen event vocabulary, plus unknown kinds.
///
/// The `Unsupported` variant keeps forward compatibility: an older
/// core reads the envelope, then marks that goal unreadable instead
/// of failing the whole ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoalEventKindWire {
    /// Goal created.
    Created,
    /// Draft named (G4).
    Named,
    /// Title, outcome, or criteria edited.
    Edited,
    /// Draft adopted (G4).
    Adopted,
    /// Agent attached (G2).
    AgentAttached,
    /// Progress note (G3).
    Progress,
    /// Plan attached (G2).
    PlanAttached,
    /// Claim made (G3).
    Claimed,
    /// Claim retracted (G3/G4).
    ClaimRetracted,
    /// Goal settled.
    Settled,
    /// Settled goal reopened.
    Reopened,
    /// Another goal merged into this one.
    Merged,
    /// A kind this core does not know.
    Unsupported(String),
}

impl GoalEventKindWire {
    /// Every known kind word.
    pub const KNOWN: [&'static str; 12] = [
        "created",
        "named",
        "edited",
        "adopted",
        "agent_attached",
        "progress",
        "plan_attached",
        "claimed",
        "claim_retracted",
        "settled",
        "reopened",
        "merged",
    ];

    /// The kind word on the wire.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Created => "created",
            Self::Named => "named",
            Self::Edited => "edited",
            Self::Adopted => "adopted",
            Self::AgentAttached => "agent_attached",
            Self::Progress => "progress",
            Self::PlanAttached => "plan_attached",
            Self::Claimed => "claimed",
            Self::ClaimRetracted => "claim_retracted",
            Self::Settled => "settled",
            Self::Reopened => "reopened",
            Self::Merged => "merged",
            Self::Unsupported(kind) => kind.as_str(),
        }
    }

    /// Parse a kind word, keeping unknown kinds typed.
    pub fn parse(value: &str) -> Self {
        match value {
            "created" => Self::Created,
            "named" => Self::Named,
            "edited" => Self::Edited,
            "adopted" => Self::Adopted,
            "agent_attached" => Self::AgentAttached,
            "progress" => Self::Progress,
            "plan_attached" => Self::PlanAttached,
            "claimed" => Self::Claimed,
            "claim_retracted" => Self::ClaimRetracted,
            "settled" => Self::Settled,
            "reopened" => Self::Reopened,
            "merged" => Self::Merged,
            other => Self::Unsupported(other.to_string()),
        }
    }

    /// Whether this core knows how to reduce the kind.
    pub fn is_supported(&self) -> bool {
        !matches!(self, Self::Unsupported(_))
    }

    /// Content events always apply unless the writer saw settle.
    pub fn is_content_event(&self) -> bool {
        matches!(
            self,
            Self::Edited
                | Self::Progress
                | Self::AgentAttached
                | Self::PlanAttached
                | Self::Merged
        )
    }
}

impl Serialize for GoalEventKindWire {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for GoalEventKindWire {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(Self::parse(&value))
    }
}

/// A stored acceptance criterion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalCriterionWire {
    /// `<event_id>.<index>`, unique without coordination.
    #[serde(default)]
    pub id: String,
    /// Criterion text, at most 200 chars.
    #[serde(default)]
    pub text: String,
    /// Who wrote the criterion.
    #[serde(default)]
    pub source: GoalCriterionSourceWire,
}

/// A criterion supplied with `created` or `edited`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalCriterionInputWire {
    /// Criterion text, at most 200 chars.
    #[serde(default)]
    pub text: String,
    /// Who wrote the criterion.
    #[serde(default)]
    pub source: GoalCriterionSourceWire,
}

/// Where and how a goal originated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalOriginWire {
    /// Origin kind.
    #[serde(default)]
    pub kind: String,
    /// Dotted `username.machine` principal.
    #[serde(default)]
    pub principal: String,
    /// Machine the goal was created on.
    #[serde(default)]
    pub machine: String,
    /// RFC3339 creation time.
    #[serde(default)]
    pub at: String,
    /// Creation channel; G1 writes `"cli"`.
    #[serde(default)]
    pub via: String,
    /// Agent name, when an agent created the goal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    /// Unit prompt digest, when bound (later epics).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit_prompt_digest: Option<String>,
    /// Root prompt digest, when bound (later epics).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_prompt_digest: Option<String>,
}

/// Payload of a `created` event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalCreatedPayloadWire {
    /// Title, at most 60 chars.
    #[serde(default)]
    pub title: String,
    /// Outcome, at most 280 chars on one line.
    #[serde(default)]
    pub outcome: String,
    /// Initial criteria.
    #[serde(default)]
    pub criteria: Vec<GoalCriterionInputWire>,
    /// Creation origin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<GoalOriginWire>,
    /// Draft goals are machine-local (G4).
    #[serde(default)]
    pub draft: bool,
    /// Owning project.
    #[serde(default)]
    pub project: String,
}

/// Payload of a `named` event (G4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalNamedPayloadWire {
    /// Title, at most 60 chars.
    #[serde(default)]
    pub title: String,
    /// Outcome, at most 280 chars on one line.
    #[serde(default)]
    pub outcome: String,
    /// Whether the name was automatic.
    #[serde(default)]
    pub auto: bool,
}

/// Payload of an `edited` event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalEditedPayloadWire {
    /// Replacement title.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Replacement outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    /// Criteria to add.
    #[serde(default)]
    pub criteria_added: Vec<GoalCriterionInputWire>,
    /// Criterion ids to remove.
    #[serde(default)]
    pub criteria_removed: Vec<String>,
    /// Free note recorded on the timeline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// Payload of an `adopted` event (G4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalAdoptedPayloadWire {
    /// Adopted draft id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub draft_id: Option<String>,
    /// Adopting agent.
    #[serde(default)]
    pub agent: String,
    /// Why the draft was adopted, at most 280 chars.
    #[serde(default)]
    pub why: String,
    /// Unit prompt digest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit_prompt_digest: Option<String>,
}

/// Payload of an `agent_attached` event (G2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalAgentAttachedPayloadWire {
    /// Attached agent.
    #[serde(default)]
    pub agent: String,
    /// Hint about the agent's role.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_hint: Option<String>,
}

/// Payload of a `progress` event (G3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalProgressPayloadWire {
    /// Progress note, at most 280 chars.
    #[serde(default)]
    pub note: String,
    /// Reporting agent.
    #[serde(default)]
    pub agent: String,
}

/// Payload of a `plan_attached` event (G2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalPlanAttachedPayloadWire {
    /// Attached plan ref.
    #[serde(default)]
    pub plan_ref: String,
    /// Plan digest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
}

/// One claim evidence entry (G3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalEvidenceWire {
    /// Cited artifact ref.
    #[serde(default, rename = "ref")]
    pub ref_: String,
    /// Why the evidence supports the claim.
    #[serde(default)]
    pub why: String,
}

/// One claim verification receipt (G3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalClaimReceiptWire {
    /// Tool that produced the verdict.
    #[serde(default)]
    pub tool: String,
    /// Tool verdict.
    #[serde(default)]
    pub verdict: String,
    /// Content fingerprint.
    #[serde(default)]
    pub fingerprint: String,
    /// Tree SHA the verdict covers.
    #[serde(default)]
    pub tree_sha: String,
    /// RFC3339 verdict time.
    #[serde(default)]
    pub at: String,
}

/// Payload of a `claimed` event (G3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalClaimedPayloadWire {
    /// Claim number.
    #[serde(default)]
    pub claim_no: u32,
    /// Claim text, at most 280 chars.
    #[serde(default)]
    pub claim: String,
    /// Supporting evidence.
    #[serde(default)]
    pub evidence: Vec<GoalEvidenceWire>,
    /// Verification checks, 1 to 3.
    #[serde(default)]
    pub check: Vec<String>,
    /// Open gaps.
    #[serde(default)]
    pub gaps: Vec<String>,
    /// Tool receipts.
    #[serde(default)]
    pub receipts: Vec<GoalClaimReceiptWire>,
    /// Claim strength.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strength: Option<GoalClaimStrengthWire>,
}

/// Payload of a `claim_retracted` event (G3/G4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalClaimRetractedPayloadWire {
    /// Retracted claim number.
    #[serde(default)]
    pub claim_no: u32,
    /// Why the claim was retracted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<GoalClaimRetractReasonWire>,
    /// Retraction feedback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback: Option<String>,
}

/// Payload of a `settled` event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalSettledPayloadWire {
    /// Settlement flavor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flavor: Option<GoalSettleFlavorWire>,
    /// Merge target for `merged` flavor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub into: Option<String>,
    /// Settlement note.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// Payload of a `reopened` event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalReopenedPayloadWire {
    /// Reopen message, at most 280 chars.
    #[serde(default)]
    pub message: String,
}

/// Payload of a `merged` event, written on the target goal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalMergedPayloadWire {
    /// Source goal id merged into this goal.
    #[serde(default)]
    pub from: String,
    /// Why the goals merged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub why: Option<String>,
}

/// A parsed event payload, dispatched on the envelope kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoalEventPayloadWire {
    /// `created` payload.
    Created(GoalCreatedPayloadWire),
    /// `named` payload.
    Named(GoalNamedPayloadWire),
    /// `edited` payload.
    Edited(GoalEditedPayloadWire),
    /// `adopted` payload.
    Adopted(GoalAdoptedPayloadWire),
    /// `agent_attached` payload.
    AgentAttached(GoalAgentAttachedPayloadWire),
    /// `progress` payload.
    Progress(GoalProgressPayloadWire),
    /// `plan_attached` payload.
    PlanAttached(GoalPlanAttachedPayloadWire),
    /// `claimed` payload.
    Claimed(GoalClaimedPayloadWire),
    /// `claim_retracted` payload.
    ClaimRetracted(GoalClaimRetractedPayloadWire),
    /// `settled` payload.
    Settled(GoalSettledPayloadWire),
    /// `reopened` payload.
    Reopened(GoalReopenedPayloadWire),
    /// `merged` payload.
    Merged(GoalMergedPayloadWire),
}

/// One immutable goal event on the wire.
///
/// Deserialization normalizes a known-kind payload through its typed
/// shape, so unknown payload fields are dropped on read and the frozen
/// "unknown fields are ignored everywhere" rule holds for envelope
/// round-trips too.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "GoalEventWireRaw")]
pub struct GoalEventWire {
    /// Envelope schema version.
    #[serde(default)]
    pub schema_version: u32,
    /// 26-char ULID-shaped event id.
    #[serde(default)]
    pub event_id: String,
    /// 5-char goal id.
    #[serde(default)]
    pub goal_id: String,
    /// Event kind word.
    #[serde(default = "default_event_kind")]
    pub kind: GoalEventKindWire,
    /// RFC3339 event time.
    #[serde(default)]
    pub at: String,
    /// Event author.
    #[serde(default = "default_actor")]
    pub actor: GoalActorWire,
    /// Head event id the writer reduced from; null for `created`.
    /// Always serialized (including null) so the bytes match the
    /// contract example; readers accept both forms.
    #[serde(default)]
    pub basis: Option<String>,
    /// Unique key per logical action for dedupe.
    #[serde(default)]
    pub idempotency_key: String,
    /// Kind-tagged payload object.
    #[serde(default)]
    pub payload: serde_json::Value,
}

fn default_event_kind() -> GoalEventKindWire {
    GoalEventKindWire::Unsupported(String::new())
}

fn default_actor() -> GoalActorWire {
    GoalActorWire {
        principal: String::new(),
        kind: GoalActorKindWire::Human,
        agent: None,
    }
}

/// Serde bridge for [`GoalEventWire`]: the raw envelope plus payload
/// normalization on the way in.
#[derive(Debug, Clone, Deserialize)]
struct GoalEventWireRaw {
    /// Envelope schema version.
    #[serde(default)]
    schema_version: u32,
    /// 26-char ULID-shaped event id.
    #[serde(default)]
    event_id: String,
    /// 5-char goal id.
    #[serde(default)]
    goal_id: String,
    /// Event kind word.
    #[serde(default = "default_event_kind")]
    kind: GoalEventKindWire,
    /// RFC3339 event time.
    #[serde(default)]
    at: String,
    /// Event author.
    #[serde(default = "default_actor")]
    actor: GoalActorWire,
    /// Head event id the writer reduced from; null for `created`.
    #[serde(default)]
    basis: Option<String>,
    /// Unique key per logical action for dedupe.
    #[serde(default)]
    idempotency_key: String,
    /// Kind-tagged payload object.
    #[serde(default)]
    payload: serde_json::Value,
}

impl From<GoalEventWireRaw> for GoalEventWire {
    fn from(raw: GoalEventWireRaw) -> Self {
        let mut event = Self {
            schema_version: raw.schema_version,
            event_id: raw.event_id,
            goal_id: raw.goal_id,
            kind: raw.kind,
            at: raw.at,
            actor: raw.actor,
            basis: raw.basis,
            idempotency_key: raw.idempotency_key,
            payload: raw.payload,
        };
        event.canonicalize_payload();
        event
    }
}

impl Default for GoalActorWire {
    fn default() -> Self {
        default_actor()
    }
}

impl GoalEventWire {
    /// Rewrite a known-kind payload to its typed shape, dropping
    /// unknown fields.
    ///
    /// Unknown kinds, unsupported versions, and unparseable payloads
    /// keep their raw value so the reducer can report them instead of
    /// failing the read.
    fn canonicalize_payload(&mut self) {
        if self.schema_version != GOAL_WIRE_SCHEMA_VERSION {
            return;
        }
        if !self.kind.is_supported() {
            return;
        }
        let Ok(parsed) = parse_payload_for_kind(&self.kind, &self.payload)
        else {
            return;
        };
        let canonical = match &parsed {
            GoalEventPayloadWire::Created(inner) => serde_json::to_value(inner),
            GoalEventPayloadWire::Named(inner) => serde_json::to_value(inner),
            GoalEventPayloadWire::Edited(inner) => serde_json::to_value(inner),
            GoalEventPayloadWire::Adopted(inner) => serde_json::to_value(inner),
            GoalEventPayloadWire::AgentAttached(inner) => {
                serde_json::to_value(inner)
            }
            GoalEventPayloadWire::Progress(inner) => {
                serde_json::to_value(inner)
            }
            GoalEventPayloadWire::PlanAttached(inner) => {
                serde_json::to_value(inner)
            }
            GoalEventPayloadWire::Claimed(inner) => serde_json::to_value(inner),
            GoalEventPayloadWire::ClaimRetracted(inner) => {
                serde_json::to_value(inner)
            }
            GoalEventPayloadWire::Settled(inner) => serde_json::to_value(inner),
            GoalEventPayloadWire::Reopened(inner) => {
                serde_json::to_value(inner)
            }
            GoalEventPayloadWire::Merged(inner) => serde_json::to_value(inner),
        };
        if let Ok(value) = canonical {
            self.payload = value;
        }
    }

    /// Dispatch the payload object on the envelope kind.
    ///
    /// Unknown fields inside a known payload are ignored. Unknown
    /// kinds fail with [`GoalWireError::UnsupportedKind`] so the
    /// reducer can mark the goal unreadable.
    pub fn parse_payload(&self) -> Result<GoalEventPayloadWire, GoalWireError> {
        if self.schema_version != GOAL_WIRE_SCHEMA_VERSION {
            return Err(GoalWireError::UnsupportedVersion(self.schema_version));
        }
        parse_payload_for_kind(&self.kind, &self.payload)
    }
}

/// Parse a payload value for an explicit kind.
pub fn parse_payload_for_kind(
    kind: &GoalEventKindWire,
    payload: &serde_json::Value,
) -> Result<GoalEventPayloadWire, GoalWireError> {
    let value = if payload.is_null() {
        &serde_json::Value::Null
    } else {
        payload
    };
    match kind {
        GoalEventKindWire::Created => {
            from_payload(value).map(GoalEventPayloadWire::Created)
        }
        GoalEventKindWire::Named => {
            from_payload(value).map(GoalEventPayloadWire::Named)
        }
        GoalEventKindWire::Edited => {
            from_payload(value).map(GoalEventPayloadWire::Edited)
        }
        GoalEventKindWire::Adopted => {
            from_payload(value).map(GoalEventPayloadWire::Adopted)
        }
        GoalEventKindWire::AgentAttached => {
            from_payload(value).map(GoalEventPayloadWire::AgentAttached)
        }
        GoalEventKindWire::Progress => {
            from_payload(value).map(GoalEventPayloadWire::Progress)
        }
        GoalEventKindWire::PlanAttached => {
            from_payload(value).map(GoalEventPayloadWire::PlanAttached)
        }
        GoalEventKindWire::Claimed => {
            from_payload(value).map(GoalEventPayloadWire::Claimed)
        }
        GoalEventKindWire::ClaimRetracted => {
            from_payload(value).map(GoalEventPayloadWire::ClaimRetracted)
        }
        GoalEventKindWire::Settled => {
            from_payload(value).map(GoalEventPayloadWire::Settled)
        }
        GoalEventKindWire::Reopened => {
            from_payload(value).map(GoalEventPayloadWire::Reopened)
        }
        GoalEventKindWire::Merged => {
            from_payload(value).map(GoalEventPayloadWire::Merged)
        }
        GoalEventKindWire::Unsupported(kind) => {
            Err(GoalWireError::UnsupportedKind(kind.clone()))
        }
    }
}

fn from_payload<T>(value: &serde_json::Value) -> Result<T, GoalWireError>
where
    T: for<'de> Deserialize<'de>,
{
    let source = if value.is_null() {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        value.clone()
    };
    serde_json::from_value(source)
        .map_err(|error| GoalWireError::Payload(error.to_string()))
}

/// One reduced claim record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalClaimWire {
    /// Claim number.
    #[serde(default)]
    pub claim_no: u32,
    /// Claim text.
    #[serde(default)]
    pub claim: String,
    /// Agent that made the claim.
    #[serde(default)]
    pub agent: String,
    /// Claim strength.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strength: Option<GoalClaimStrengthWire>,
    /// Claim lifecycle.
    #[serde(default)]
    pub status: GoalClaimStatusWire,
}

/// One reducer diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalDiagnosticWire {
    /// Stable snake_case code.
    #[serde(default)]
    pub code: String,
    /// Human-readable detail.
    #[serde(default)]
    pub message: String,
    /// Event the diagnostic attaches to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
}

impl GoalDiagnosticWire {
    /// Build a diagnostic without an event attachment.
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
            event_id: None,
        }
    }

    /// Build a diagnostic attached to one event.
    pub fn for_event(
        code: &str,
        message: impl Into<String>,
        event_id: &str,
    ) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
            event_id: Some(event_id.to_string()),
        }
    }
}

/// How one event fared in reduction.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum GoalTimelineEffectWire {
    /// The event changed state.
    #[default]
    Applied,
    /// The event was kept but changed nothing.
    Ignored,
    /// The event duplicated an idempotency key.
    Duplicate,
    /// A concurrent race winner beat the event.
    Superseded,
}

/// One timeline row in the reduced state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalTimelineEntryWire {
    /// Reduced event id.
    #[serde(default)]
    pub event_id: String,
    /// RFC3339 event time.
    #[serde(default)]
    pub at: String,
    /// Event kind word.
    #[serde(default = "default_event_kind")]
    pub kind: GoalEventKindWire,
    /// Event author.
    #[serde(default = "default_actor")]
    pub actor: GoalActorWire,
    /// One-line human summary.
    #[serde(default)]
    pub summary: String,
    /// Reduction effect.
    #[serde(default)]
    pub effect: GoalTimelineEffectWire,
}

/// The reduced state of one goal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalStateWire {
    /// State wire schema version.
    #[serde(default)]
    pub schema_version: u32,
    /// 5-char goal id.
    #[serde(default)]
    pub id: String,
    /// Owning project.
    #[serde(default)]
    pub project: String,
    /// Lifecycle status.
    #[serde(default)]
    pub status: GoalStatusWire,
    /// Settlement flavor, once settled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flavor: Option<GoalSettleFlavorWire>,
    /// False when the event set cannot be trusted.
    #[serde(default = "default_readable")]
    pub readable: bool,
    /// Why the goal is unreadable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unreadable_reason: Option<String>,
    /// Goal title.
    #[serde(default)]
    pub title: String,
    /// Goal outcome.
    #[serde(default)]
    pub outcome: String,
    /// Acceptance criteria.
    #[serde(default)]
    pub criteria: Vec<GoalCriterionWire>,
    /// Creation origin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<GoalOriginWire>,
    /// Applied `created`, `named`, and `edited` count.
    #[serde(default)]
    pub revision: u64,
    /// Latest reduced event id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<String>,
    /// RFC3339 creation time.
    #[serde(default)]
    pub created_at: String,
    /// RFC3339 last-applied-event time.
    #[serde(default)]
    pub updated_at: String,
    /// Merge target, once merged away.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merged_into: Option<String>,
    /// Source goals merged into this one.
    #[serde(default)]
    pub merged_from: Vec<String>,
    /// Attached plan ref (G2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<String>,
    /// Claims history (G3).
    #[serde(default)]
    pub claims: Vec<GoalClaimWire>,
    /// Agents that touched the goal.
    #[serde(default)]
    pub contributors: Vec<String>,
    /// Latest progress note (G3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_progress: Option<String>,
    /// Reducer diagnostics.
    #[serde(default)]
    pub diagnostics: Vec<GoalDiagnosticWire>,
    /// Every reduced event, in reduction order.
    #[serde(default)]
    pub timeline: Vec<GoalTimelineEntryWire>,
}

fn default_readable() -> bool {
    true
}

impl GoalStateWire {
    /// An empty state for a goal id, before any event applies.
    pub fn empty(goal_id: &str) -> Self {
        Self {
            schema_version: GOAL_WIRE_SCHEMA_VERSION,
            id: goal_id.to_string(),
            project: String::new(),
            status: GoalStatusWire::Active,
            flavor: None,
            readable: true,
            unreadable_reason: None,
            title: String::new(),
            outcome: String::new(),
            criteria: Vec::new(),
            origin: None,
            revision: 0,
            head: None,
            created_at: String::new(),
            updated_at: String::new(),
            merged_into: None,
            merged_from: Vec::new(),
            plan: None,
            claims: Vec::new(),
            contributors: Vec::new(),
            last_progress: None,
            diagnostics: Vec::new(),
            timeline: Vec::new(),
        }
    }
}
