//! Provider-agnostic size-alias and epic-land model routing.
//!
//! Frontends share one mapping from [`PhaseSizeWire`] to the public `@<size>`
//! alias and one selection rule for explicit vs configured epic-land models.
//! The functions return simple wire values: no provider, filesystem, or UI
//! dependency.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::bead::PhaseSizeWire;

/// Bare public size-alias names, in size order.
pub const PUBLIC_SIZE_ALIAS_NAMES: &[&str] = &PhaseSizeWire::NAMES;

/// Public `@<size>` alias tokens, in size order.
pub const PUBLIC_SIZE_ALIASES: &[&str] =
    &["@xsmall", "@small", "@medium", "@large", "@xlarge"];

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{0}")]
pub struct ModelRouteError(String);

impl ModelRouteError {
    fn validation(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

/// Canonical size-to-public-alias mapping returned to every frontend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SizeModelRouteWire {
    pub size: String,
    pub alias: String,
}

/// Provenance of a selected epic-land model expression.
///
/// These names are config-field identities, not public model aliases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EpicLandModelSource {
    Explicit,
    EpicLanderModel,
    BigEpicLanderModel,
}

/// Selected epic-land model plus why it was chosen.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EpicLandModelRouteWire {
    pub model: String,
    pub source: EpicLandModelSource,
    pub explicit: bool,
}

/// Return the public `@<size>` alias for a validated phase size.
pub fn public_size_alias(size: &PhaseSizeWire) -> &'static str {
    match size {
        PhaseSizeWire::Xsmall => "@xsmall",
        PhaseSizeWire::Small => "@small",
        PhaseSizeWire::Medium => "@medium",
        PhaseSizeWire::Large => "@large",
        PhaseSizeWire::Xlarge => "@xlarge",
    }
}

/// Map a validated phase size to its public alias wire.
pub fn size_model_route(size: &PhaseSizeWire) -> SizeModelRouteWire {
    SizeModelRouteWire {
        size: size.as_str().to_string(),
        alias: public_size_alias(size).to_string(),
    }
}

/// Parse a size token and map it to the public `@<size>` alias.
///
/// Accepts the bare name (`medium`) or the public alias (`@medium`). Retired
/// role names such as `medium_worker` are rejected rather than rewritten.
pub fn size_model_route_from_name(
    size: &str,
) -> Result<SizeModelRouteWire, ModelRouteError> {
    let parsed = parse_phase_size_name(size)?;
    Ok(size_model_route(&parsed))
}

/// Select an explicit epic-land model or the normal/big configured target.
///
/// `phase_count` is the authored-phase count, including closed phases on
/// resume. `threshold` is the positive `bead.big_epic_phase_threshold`.
pub fn select_epic_land_model(
    explicit_model: Option<&str>,
    phase_count: i64,
    threshold: i64,
    epic_lander_model: &str,
    big_epic_lander_model: &str,
) -> Result<EpicLandModelRouteWire, ModelRouteError> {
    if phase_count < 0 {
        return Err(ModelRouteError::validation(
            "phase_count must be a non-negative integer",
        ));
    }
    if threshold < 1 {
        return Err(ModelRouteError::validation(
            "threshold must be a positive integer",
        ));
    }

    if let Some(model) = clean_optional_model(explicit_model, "explicit_model")?
    {
        return Ok(EpicLandModelRouteWire {
            model,
            source: EpicLandModelSource::Explicit,
            explicit: true,
        });
    }

    let epic_lander_model =
        clean_required_model(epic_lander_model, "epic_lander_model")?;
    let big_epic_lander_model =
        clean_required_model(big_epic_lander_model, "big_epic_lander_model")?;

    if phase_count >= threshold {
        Ok(EpicLandModelRouteWire {
            model: big_epic_lander_model,
            source: EpicLandModelSource::BigEpicLanderModel,
            explicit: false,
        })
    } else {
        Ok(EpicLandModelRouteWire {
            model: epic_lander_model,
            source: EpicLandModelSource::EpicLanderModel,
            explicit: false,
        })
    }
}

fn parse_phase_size_name(size: &str) -> Result<PhaseSizeWire, ModelRouteError> {
    let trimmed = size.trim();
    if trimmed.is_empty() {
        return Err(invalid_size(size));
    }
    let bare = trimmed.strip_prefix('@').unwrap_or(trimmed);
    if bare.len() != bare.trim().len() {
        return Err(invalid_size(size));
    }
    PhaseSizeWire::from_name(bare).ok_or_else(|| invalid_size(size))
}

fn invalid_size(size: &str) -> ModelRouteError {
    let expected = PUBLIC_SIZE_ALIAS_NAMES.join(", ");
    ModelRouteError::validation(format!(
        "size must be one of {expected}; got {size:?}"
    ))
}

fn clean_optional_model(
    value: Option<&str>,
    field: &str,
) -> Result<Option<String>, ModelRouteError> {
    match value {
        None => Ok(None),
        Some(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            reject_control_chars(trimmed, field)?;
            Ok(Some(trimmed.to_string()))
        }
    }
}

fn clean_required_model(
    value: &str,
    field: &str,
) -> Result<String, ModelRouteError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ModelRouteError::validation(format!(
            "{field} must be a non-empty model expression"
        )));
    }
    reject_control_chars(trimmed, field)?;
    Ok(trimmed.to_string())
}

fn reject_control_chars(
    value: &str,
    field: &str,
) -> Result<(), ModelRouteError> {
    if value.chars().any(char::is_control) {
        return Err(ModelRouteError::validation(format!(
            "{field} cannot contain control characters"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_every_phase_size_to_the_public_alias() {
        let expected = [
            (PhaseSizeWire::Xsmall, "xsmall", "@xsmall"),
            (PhaseSizeWire::Small, "small", "@small"),
            (PhaseSizeWire::Medium, "medium", "@medium"),
            (PhaseSizeWire::Large, "large", "@large"),
            (PhaseSizeWire::Xlarge, "xlarge", "@xlarge"),
        ];
        for (size, name, alias) in expected {
            let route = size_model_route(&size);
            assert_eq!(route.size, name);
            assert_eq!(route.alias, alias);
            assert_eq!(public_size_alias(&size), alias);
            assert_eq!(size_model_route_from_name(name).unwrap(), route);
            assert_eq!(size_model_route_from_name(alias).unwrap(), route);
            assert_eq!(
                size_model_route_from_name(&format!("  {alias}  ")).unwrap(),
                route
            );
        }
        assert_eq!(PUBLIC_SIZE_ALIAS_NAMES, PhaseSizeWire::NAMES);
        assert_eq!(
            PUBLIC_SIZE_ALIASES,
            &["@xsmall", "@small", "@medium", "@large", "@xlarge"]
        );
    }

    #[test]
    fn rejects_invalid_and_retired_size_names() {
        for size in [
            "",
            "   ",
            "@",
            "MEDIUM",
            "@MEDIUM",
            "medium_worker",
            "@medium_worker",
            "default",
            "@epic_lander",
            "big_epic_lander",
            "@ medium",
            "xsmall_worker",
        ] {
            let error = size_model_route_from_name(size).unwrap_err();
            assert!(
                error.to_string().contains("size must be one of"),
                "{size:?} -> {error}"
            );
        }
    }

    #[test]
    fn selects_explicit_epic_land_model_over_threshold() {
        let route = select_epic_land_model(
            Some("  claude/opus@xhigh  "),
            8,
            5,
            "@large",
            "@xlarge",
        )
        .unwrap();
        assert_eq!(
            route,
            EpicLandModelRouteWire {
                model: "claude/opus@xhigh".to_string(),
                source: EpicLandModelSource::Explicit,
                explicit: true,
            }
        );
    }

    #[test]
    fn empty_or_blank_explicit_model_falls_through_to_config() {
        for explicit in [None, Some(""), Some("   ")] {
            let route =
                select_epic_land_model(explicit, 4, 5, "  @large  ", "@xlarge")
                    .unwrap();
            assert_eq!(route.model, "@large");
            assert_eq!(route.source, EpicLandModelSource::EpicLanderModel);
            assert!(!route.explicit);
        }
    }

    #[test]
    fn uses_big_target_at_and_above_threshold() {
        let below =
            select_epic_land_model(None, 4, 5, "@large", "@xlarge").unwrap();
        let exact =
            select_epic_land_model(None, 5, 5, "@large", "@xlarge").unwrap();
        let above =
            select_epic_land_model(None, 6, 5, "@large", "@xlarge").unwrap();
        assert_eq!(below.source, EpicLandModelSource::EpicLanderModel);
        assert_eq!(below.model, "@large");
        assert_eq!(exact.source, EpicLandModelSource::BigEpicLanderModel);
        assert_eq!(exact.model, "@xlarge");
        assert_eq!(above.source, EpicLandModelSource::BigEpicLanderModel);
        assert_eq!(above.model, "@xlarge");
        assert!(!below.explicit && !exact.explicit && !above.explicit);
    }

    #[test]
    fn zero_phase_count_is_valid_and_selects_the_normal_target() {
        let route =
            select_epic_land_model(None, 0, 1, "@large", "@xlarge").unwrap();
        assert_eq!(route.source, EpicLandModelSource::EpicLanderModel);
        assert_eq!(route.model, "@large");
    }

    #[test]
    fn rejects_invalid_counts_thresholds_and_targets() {
        let negative = select_epic_land_model(None, -1, 5, "@large", "@xlarge")
            .unwrap_err();
        assert_eq!(
            negative.to_string(),
            "phase_count must be a non-negative integer"
        );

        let zero_threshold =
            select_epic_land_model(None, 2, 0, "@large", "@xlarge")
                .unwrap_err();
        assert_eq!(
            zero_threshold.to_string(),
            "threshold must be a positive integer"
        );

        let empty_normal =
            select_epic_land_model(None, 1, 5, "  ", "@xlarge").unwrap_err();
        assert_eq!(
            empty_normal.to_string(),
            "epic_lander_model must be a non-empty model expression"
        );

        let empty_big =
            select_epic_land_model(None, 1, 5, "@large", "").unwrap_err();
        assert_eq!(
            empty_big.to_string(),
            "big_epic_lander_model must be a non-empty model expression"
        );

        let control = select_epic_land_model(
            Some("opus\n%tag:bad"),
            1,
            5,
            "@large",
            "@xlarge",
        )
        .unwrap_err();
        assert_eq!(
            control.to_string(),
            "explicit_model cannot contain control characters"
        );
    }

    #[test]
    fn wire_json_uses_config_field_source_names() {
        let size =
            serde_json::to_value(size_model_route_from_name("large").unwrap())
                .unwrap();
        assert_eq!(
            size,
            serde_json::json!({"size": "large", "alias": "@large"})
        );

        let land = serde_json::to_value(
            select_epic_land_model(None, 5, 5, "@large", "@xlarge").unwrap(),
        )
        .unwrap();
        assert_eq!(
            land,
            serde_json::json!({
                "model": "@xlarge",
                "source": "big_epic_lander_model",
                "explicit": false
            })
        );
    }
}
