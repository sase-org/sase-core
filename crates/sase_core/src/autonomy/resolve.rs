//! `%auto` selection resolution: prompt text to a revision-1 record.
//!
//! Every spelling validates through the shared fail-closed classifier
//! [`crate::agent_launch::classify_auto_directive`], so the typed launch
//! planner, editor diagnostics, and the autonomy record accept and reject
//! the same spellings.

use thiserror::Error;

use crate::agent_launch::{
    classify_auto_directive, AutoDirectiveDiagnostic, AutoDirectiveForm,
};

use super::profiles::{policy_digest, profile_policy};
use super::wires::{
    AutonomyActorWire, AutonomyLastWire, AutonomyRecordWire,
    AUTONOMY_PROFILE_EPIC, AUTONOMY_PROFILE_MANUAL, AUTONOMY_PROFILE_STANDARD,
    AUTONOMY_PROFILE_TALE, AUTONOMY_SOURCE_CLI, AUTONOMY_SOURCE_INHERITED,
    AUTONOMY_SOURCE_LEGACY, AUTONOMY_SOURCE_PROMPT, AUTONOMY_SOURCE_TUI,
    AUTONOMY_WIRE_SCHEMA_VERSION,
};

/// Record sources `resolve_autonomy_selection` accepts.
pub fn valid_sources() -> &'static [&'static str] {
    &[
        AUTONOMY_SOURCE_PROMPT,
        AUTONOMY_SOURCE_TUI,
        AUTONOMY_SOURCE_CLI,
        AUTONOMY_SOURCE_INHERITED,
        AUTONOMY_SOURCE_LEGACY,
    ]
}

/// Actor kinds `resolve_autonomy_selection` accepts.
pub fn valid_actor_kinds() -> &'static [&'static str] {
    &["human", "agent", "host"]
}

/// Selection resolution failures.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AutonomyError {
    /// The `%auto` spelling is rejected; carries the classifier's stable
    /// code and exact user-facing message.
    #[error("{code}: {message}")]
    InvalidSelection { code: &'static str, message: String },
    /// The record source is not one of the five wire sources.
    #[error("invalid autonomy source '{0}'")]
    InvalidSource(String),
    /// The actor kind is not human, agent, or host.
    #[error("invalid autonomy actor kind '{0}'")]
    InvalidActorKind(String),
}

impl From<AutoDirectiveDiagnostic> for AutonomyError {
    fn from(diagnostic: AutoDirectiveDiagnostic) -> Self {
        Self::InvalidSelection {
            code: diagnostic.code,
            message: diagnostic.message,
        }
    }
}

fn check_source(source: &str) -> Result<(), AutonomyError> {
    if valid_sources().contains(&source) {
        Ok(())
    } else {
        Err(AutonomyError::InvalidSource(source.to_string()))
    }
}

fn check_actor(actor: &AutonomyActorWire) -> Result<(), AutonomyError> {
    if valid_actor_kinds().contains(&actor.kind.as_str()) {
        Ok(())
    } else {
        Err(AutonomyError::InvalidActorKind(actor.kind.clone()))
    }
}

/// Resolve a `%auto` selection to a revision-1 autonomy record.
///
/// `selection` is the text after `%auto` (`None` when the prompt carries
/// no `%auto`); `source` is one of the five wire sources; `now` is stored
/// as `updated_at` when non-empty.
pub fn resolve_autonomy_selection(
    selection: Option<&str>,
    source: &str,
    actor: &AutonomyActorWire,
    now: &str,
) -> Result<AutonomyRecordWire, AutonomyError> {
    check_source(source)?;
    check_actor(actor)?;
    let (profile, canonical) = match selection {
        None => (AUTONOMY_PROFILE_MANUAL, "manual".to_string()),
        Some("") | Some("true") | Some("+") => {
            (AUTONOMY_PROFILE_STANDARD, String::new())
        }
        Some("plan") | Some("tale") => {
            let text = selection.unwrap_or("tale");
            (AUTONOMY_PROFILE_TALE, text.to_string())
        }
        Some("epic") => (AUTONOMY_PROFILE_EPIC, "epic".to_string()),
        Some("manual") | Some("off") => {
            (AUTONOMY_PROFILE_MANUAL, "manual".to_string())
        }
        Some(other) => {
            let spelling = format!("%auto:{other}");
            let classified = classify_auto_directive(
                AutoDirectiveForm::Colon,
                other,
                &spelling,
            )?;
            // The classifier accepted a spelling the profile table does
            // not know (future grammar): fail closed on the record side.
            let _ = classified;
            return Err(AutonomyError::InvalidSelection {
                code: crate::agent_launch::INVALID_AUTO_CODE,
                message: format!(
                    "Invalid %auto spelling '{spelling}': unsupported auto \
                    selection '{other}'. Use %auto, %auto+, or %auto:<mode> \
                    with mode plan, tale, or epic; %auto:manual and \
                    %auto:off disable automatic approval.",
                ),
            });
        }
    };
    let policy = profile_policy(profile);
    let digest = policy_digest(&policy);
    let last = if profile == AUTONOMY_PROFILE_MANUAL {
        None
    } else {
        Some(AutonomyLastWire {
            profile: profile.to_string(),
            selection: canonical.clone(),
        })
    };
    Ok(AutonomyRecordWire {
        schema_version: AUTONOMY_WIRE_SCHEMA_VERSION,
        profile: profile.to_string(),
        selection: canonical,
        policy,
        overrides: Default::default(),
        source: source.to_string(),
        inherited_from: None,
        last,
        revision: 1,
        digest,
        updated_at: if now.is_empty() {
            None
        } else {
            Some(now.to_string())
        },
        updated_by: Some(actor.clone()),
    })
}
