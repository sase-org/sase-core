//! Duration classes: floors, inline fit, and corpus calibration.
//!
//! A duration class is a floor, not a forecast. It states the least time a
//! catalog tool essentially always takes, so `sase tool run` can refuse an
//! inline run whose floor meets the caller's kill ceiling before anything
//! starts. The corpus only calibrates declarations: `duration_calibration`
//! flags a class the settled-run TYPICAL contradicts, and stays silent
//! otherwise. It never gates a run.

use serde::{Deserialize, Serialize};

use super::wire::{ToolDurationClassWire, TOOL_RUN_WIRE_SCHEMA_VERSION};
use super::ToolRunError;

/// Floor for `short`: always fits inline, including undeclared tools.
pub const DURATION_CLASS_SHORT_FLOOR_SECONDS: u64 = 0;
/// Floor for `long`: essentially never finishes in under 10 minutes.
pub const DURATION_CLASS_LONG_FLOOR_SECONDS: u64 = 600;
/// TYPICAL threshold (ms) at which a tool meets the `long` floor.
pub const DURATION_CLASS_LONG_FLOOR_MS: u64 =
    DURATION_CLASS_LONG_FLOOR_SECONDS * 1000;
/// Minimum settled samples before calibration says anything.
pub const DURATION_CALIBRATION_MIN_SAMPLES: u32 = 10;

fn schema_version() -> u32 {
    TOOL_RUN_WIRE_SCHEMA_VERSION
}

/// Numerator of the hard-ceiling margin fraction (15 percent).
pub const SYNC_WAIT_BUDGET_MARGIN_NUMERATOR: u64 = 15;
/// Denominator of the hard-ceiling margin fraction (15 percent).
pub const SYNC_WAIT_BUDGET_MARGIN_DENOMINATOR: u64 = 100;
/// Floor for the hard-ceiling margin, in seconds.
pub const SYNC_WAIT_BUDGET_MARGIN_FLOOR_SECONDS: u64 = 90;
/// Cap for the hard-ceiling margin, in seconds.
pub const SYNC_WAIT_BUDGET_MARGIN_CAP_SECONDS: u64 = 300;

/// Undeclared tools are `short`.
pub fn effective_duration_class(
    class: Option<ToolDurationClassWire>,
) -> ToolDurationClassWire {
    class.unwrap_or(ToolDurationClassWire::Short)
}

/// Least time the class essentially always takes; `None` has no upper bound.
pub fn duration_class_floor_seconds(
    class: ToolDurationClassWire,
) -> Option<u64> {
    match class {
        ToolDurationClassWire::Short => {
            Some(DURATION_CLASS_SHORT_FLOOR_SECONDS)
        }
        ToolDurationClassWire::Long => Some(DURATION_CLASS_LONG_FLOOR_SECONDS),
        ToolDurationClassWire::Unbounded => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurationFitRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_class: Option<ToolDurationClassWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ceiling_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurationFitResponseWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub duration_class: ToolDurationClassWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub floor_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ceiling_seconds: Option<u64>,
    pub fits_inline: bool,
}

/// A run fits inline when no ceiling is present, or when the class is not
/// `unbounded` and its floor is less than the ceiling. Equivalently, refuse
/// exactly when a ceiling is present and the floor is at least the ceiling
/// (an `unbounded` floor is infinite).
pub fn duration_fit(
    request: DurationFitRequestWire,
) -> Result<DurationFitResponseWire, ToolRunError> {
    if request.schema_version != TOOL_RUN_WIRE_SCHEMA_VERSION {
        return Err(ToolRunError::SchemaVersion {
            expected: TOOL_RUN_WIRE_SCHEMA_VERSION,
            actual: request.schema_version,
        });
    }
    if request.ceiling_seconds == Some(0) {
        return Err(ToolRunError::invalid(
            "duration fit ceiling_seconds must be a positive number of seconds",
        ));
    }
    let class = effective_duration_class(request.duration_class);
    let floor_seconds = duration_class_floor_seconds(class);
    let fits_inline = match (floor_seconds, request.ceiling_seconds) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(floor), Some(ceiling)) => floor < ceiling,
    };
    Ok(DurationFitResponseWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        duration_class: class,
        floor_seconds,
        ceiling_seconds: request.ceiling_seconds,
        fits_inline,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurationCalibrationKindWire {
    FloorAboveTypical,
    TypicalMeetsFloor,
    BoundedEvidence,
}

impl DurationCalibrationKindWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FloorAboveTypical => "floor_above_typical",
            Self::TypicalMeetsFloor => "typical_meets_floor",
            Self::BoundedEvidence => "bounded_evidence",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurationCalibrationRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_class: Option<ToolDurationClassWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typical_duration_ms: Option<u64>,
    #[serde(default)]
    pub typical_sample_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurationCalibrationWire {
    pub kind: DurationCalibrationKindWire,
    pub suggested_class: ToolDurationClassWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typical_duration_ms: Option<u64>,
    #[serde(default)]
    pub typical_sample_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub floor_seconds: Option<u64>,
    #[serde(default)]
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurationCalibrationResponseWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub duration_class: ToolDurationClassWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub calibration: Option<DurationCalibrationWire>,
}

/// Flag a declared class the settled-run TYPICAL contradicts.
///
/// Stays silent below `DURATION_CALIBRATION_MIN_SAMPLES` samples or without a
/// TYPICAL duration. `long` with TYPICAL under its floor may refuse runs that
/// would fit, so suggest `short`. `short` with TYPICAL at or above the `long`
/// floor suggests `long`, but only if the tool's arguments and failures never
/// shorten it. `unbounded` with enough settled samples suggests `long` or
/// `short` by TYPICAL.
pub fn duration_calibration(
    request: DurationCalibrationRequestWire,
) -> Result<DurationCalibrationResponseWire, ToolRunError> {
    if request.schema_version != TOOL_RUN_WIRE_SCHEMA_VERSION {
        return Err(ToolRunError::SchemaVersion {
            expected: TOOL_RUN_WIRE_SCHEMA_VERSION,
            actual: request.schema_version,
        });
    }
    let class = effective_duration_class(request.duration_class);
    let response = |calibration: Option<DurationCalibrationWire>| {
        DurationCalibrationResponseWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            duration_class: class,
            calibration,
        }
    };
    if request.typical_sample_count < DURATION_CALIBRATION_MIN_SAMPLES {
        return Ok(response(None));
    }
    let Some(typical_ms) = request.typical_duration_ms else {
        return Ok(response(None));
    };
    let (kind, suggested) = match class {
        ToolDurationClassWire::Long
            if typical_ms < DURATION_CLASS_LONG_FLOOR_MS =>
        {
            (
                DurationCalibrationKindWire::FloorAboveTypical,
                ToolDurationClassWire::Short,
            )
        }
        ToolDurationClassWire::Short
            if typical_ms >= DURATION_CLASS_LONG_FLOOR_MS =>
        {
            (
                DurationCalibrationKindWire::TypicalMeetsFloor,
                ToolDurationClassWire::Long,
            )
        }
        ToolDurationClassWire::Unbounded => {
            let suggested = if typical_ms >= DURATION_CLASS_LONG_FLOOR_MS {
                ToolDurationClassWire::Long
            } else {
                ToolDurationClassWire::Short
            };
            (DurationCalibrationKindWire::BoundedEvidence, suggested)
        }
        _ => return Ok(response(None)),
    };
    let summary = match kind {
        DurationCalibrationKindWire::FloorAboveTypical => format!(
            "duration class long runs at least {}, but TYPICAL is {} over {} samples; suggest short",
            format_duration_ms(DURATION_CLASS_LONG_FLOOR_MS),
            format_duration_ms(typical_ms),
            request.typical_sample_count,
        ),
        DurationCalibrationKindWire::TypicalMeetsFloor => format!(
            "duration class short fits inline, but TYPICAL is {} over {} samples; suggest long if arguments and failures never shorten it",
            format_duration_ms(typical_ms),
            request.typical_sample_count,
        ),
        DurationCalibrationKindWire::BoundedEvidence => format!(
            "duration class unbounded has no upper bound, but {} settled runs show TYPICAL {}; suggest {}",
            request.typical_sample_count,
            format_duration_ms(typical_ms),
            suggested.as_str(),
        ),
    };
    Ok(response(Some(DurationCalibrationWire {
        kind,
        suggested_class: suggested,
        typical_duration_ms: Some(typical_ms),
        typical_sample_count: request.typical_sample_count,
        floor_seconds: duration_class_floor_seconds(class),
        summary,
    })))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncWaitBudgetSourceWire {
    Hard,
    Soft,
}

impl SyncWaitBudgetSourceWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hard => "hard",
            Self::Soft => "soft",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncWaitBudgetRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ceiling_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub soft_ceiling_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncWaitBudgetResponseWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SyncWaitBudgetSourceWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub margin_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ceiling_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub soft_ceiling_seconds: Option<u64>,
}

/// Budget a synchronous wait against hard and soft kill ceilings.
///
/// The hard ceiling keeps a 15 percent margin clamped to 90..=300 seconds,
/// then budgets the later of `ceiling - margin` and `ceiling / 2`. The soft
/// ceiling budgets itself with no margin. The smaller present budget wins;
/// ties report `hard`. With neither ceiling there is no budget.
pub fn sync_wait_budget(
    request: SyncWaitBudgetRequestWire,
) -> Result<SyncWaitBudgetResponseWire, ToolRunError> {
    if request.schema_version != TOOL_RUN_WIRE_SCHEMA_VERSION {
        return Err(ToolRunError::SchemaVersion {
            expected: TOOL_RUN_WIRE_SCHEMA_VERSION,
            actual: request.schema_version,
        });
    }
    if let Some(ceiling) = request.ceiling_seconds {
        if ceiling == 0 {
            return Err(ToolRunError::invalid(
                "sync wait budget ceiling_seconds must be a positive number of seconds",
            ));
        }
    }
    if let Some(soft) = request.soft_ceiling_seconds {
        if soft == 0 {
            return Err(ToolRunError::invalid(
                "sync wait budget soft_ceiling_seconds must be a positive number of seconds",
            ));
        }
    }
    let margin_seconds = request.ceiling_seconds.map(|ceiling| {
        let raw = ceiling
            .checked_mul(SYNC_WAIT_BUDGET_MARGIN_NUMERATOR)
            .ok_or_else(|| {
                ToolRunError::invalid(
                    "sync wait budget ceiling_seconds overflows the margin",
                )
            })
            .map(|product| product / SYNC_WAIT_BUDGET_MARGIN_DENOMINATOR)?;
        Ok::<u64, ToolRunError>(raw.clamp(
            SYNC_WAIT_BUDGET_MARGIN_FLOOR_SECONDS,
            SYNC_WAIT_BUDGET_MARGIN_CAP_SECONDS,
        ))
    });
    let margin_seconds = margin_seconds.transpose()?;
    let hard_budget = request.ceiling_seconds.map(|ceiling| {
        let margin = margin_seconds.unwrap_or(0);
        let after_margin = ceiling.saturating_sub(margin);
        after_margin.max(ceiling / 2)
    });
    let soft_budget = request.soft_ceiling_seconds;
    let (budget_seconds, source) = match (hard_budget, soft_budget) {
        (None, None) => (None, None),
        (Some(hard), None) => {
            (Some(hard), Some(SyncWaitBudgetSourceWire::Hard))
        }
        (None, Some(soft)) => {
            (Some(soft), Some(SyncWaitBudgetSourceWire::Soft))
        }
        (Some(hard), Some(soft)) => {
            if hard <= soft {
                (Some(hard), Some(SyncWaitBudgetSourceWire::Hard))
            } else {
                (Some(soft), Some(SyncWaitBudgetSourceWire::Soft))
            }
        }
    };
    Ok(SyncWaitBudgetResponseWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        budget_seconds,
        source,
        margin_seconds,
        ceiling_seconds: request.ceiling_seconds,
        soft_ceiling_seconds: request.soft_ceiling_seconds,
    })
}

fn format_duration_ms(ms: u64) -> String {
    if ms >= 60_000 {
        let total_seconds = ms / 1000;
        let minutes = total_seconds / 60;
        let seconds = total_seconds % 60;
        if seconds == 0 {
            format!("{minutes}m")
        } else {
            format!("{minutes}m{seconds}s")
        }
    } else if ms >= 1000 {
        format!("{}s", ms / 1000)
    } else {
        format!("{ms}ms")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fit(
        class: Option<ToolDurationClassWire>,
        ceiling_seconds: Option<u64>,
    ) -> DurationFitResponseWire {
        duration_fit(DurationFitRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            duration_class: class,
            ceiling_seconds,
        })
        .unwrap()
    }

    fn calibrate(
        class: Option<ToolDurationClassWire>,
        typical_duration_ms: Option<u64>,
        typical_sample_count: u32,
    ) -> DurationCalibrationResponseWire {
        duration_calibration(DurationCalibrationRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            duration_class: class,
            typical_duration_ms,
            typical_sample_count,
        })
        .unwrap()
    }

    #[test]
    fn fit_without_ceiling_always_fits() {
        for class in [
            None,
            Some(ToolDurationClassWire::Short),
            Some(ToolDurationClassWire::Long),
            Some(ToolDurationClassWire::Unbounded),
        ] {
            let response = fit(class, None);
            assert!(response.fits_inline);
            assert_eq!(response.ceiling_seconds, None);
        }
    }

    #[test]
    fn fit_reports_effective_class_and_floor() {
        let undeclared = fit(None, Some(600));
        assert_eq!(undeclared.duration_class, ToolDurationClassWire::Short);
        assert_eq!(undeclared.floor_seconds, Some(0));

        let long = fit(Some(ToolDurationClassWire::Long), Some(600));
        assert_eq!(long.floor_seconds, Some(600));

        let unbounded =
            fit(Some(ToolDurationClassWire::Unbounded), Some(14_400));
        assert_eq!(unbounded.floor_seconds, None);
    }

    #[test]
    fn fit_refuses_floor_at_or_above_ceiling() {
        assert!(!fit(Some(ToolDurationClassWire::Long), Some(600)).fits_inline);
        assert!(!fit(Some(ToolDurationClassWire::Long), Some(60)).fits_inline);
        assert!(fit(Some(ToolDurationClassWire::Long), Some(1800)).fits_inline);
        assert!(fit(Some(ToolDurationClassWire::Short), Some(600)).fits_inline);
        assert!(fit(None, Some(600)).fits_inline);
        assert!(
            !fit(Some(ToolDurationClassWire::Unbounded), Some(14_400))
                .fits_inline
        );
    }

    #[test]
    fn fit_rejects_zero_ceiling_and_unknown_schema() {
        let zero = duration_fit(DurationFitRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            duration_class: Some(ToolDurationClassWire::Long),
            ceiling_seconds: Some(0),
        });
        assert!(zero.is_err());

        let schema = duration_fit(DurationFitRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION + 1,
            duration_class: None,
            ceiling_seconds: None,
        });
        match schema {
            Err(ToolRunError::SchemaVersion { .. }) => {}
            other => panic!("expected schema error, got {other:?}"),
        }
    }

    #[test]
    fn calibration_stays_silent_below_minimum_samples() {
        for class in [
            None,
            Some(ToolDurationClassWire::Short),
            Some(ToolDurationClassWire::Long),
            Some(ToolDurationClassWire::Unbounded),
        ] {
            let response = calibrate(
                class,
                Some(120_000),
                DURATION_CALIBRATION_MIN_SAMPLES - 1,
            );
            assert_eq!(response.calibration, None);
        }
    }

    #[test]
    fn calibration_stays_silent_without_typical() {
        let response = calibrate(
            Some(ToolDurationClassWire::Long),
            None,
            DURATION_CALIBRATION_MIN_SAMPLES,
        );
        assert_eq!(response.calibration, None);
        assert_eq!(response.duration_class, ToolDurationClassWire::Long);
    }

    #[test]
    fn calibration_flags_long_floor_above_typical() {
        let response =
            calibrate(Some(ToolDurationClassWire::Long), Some(120_000), 12);
        let calibration = response.calibration.unwrap();
        assert_eq!(
            calibration.kind,
            DurationCalibrationKindWire::FloorAboveTypical
        );
        assert_eq!(calibration.suggested_class, ToolDurationClassWire::Short);
        assert_eq!(calibration.floor_seconds, Some(600));
        assert!(calibration.summary.contains("suggest short"));
    }

    #[test]
    fn calibration_flags_short_typical_meeting_long_floor() {
        let response =
            calibrate(Some(ToolDurationClassWire::Short), Some(937_000), 20);
        let calibration = response.calibration.unwrap();
        assert_eq!(
            calibration.kind,
            DurationCalibrationKindWire::TypicalMeetsFloor
        );
        assert_eq!(calibration.suggested_class, ToolDurationClassWire::Long);
        assert!(calibration.summary.contains("suggest long"));

        let boundary = calibrate(
            None,
            Some(DURATION_CLASS_LONG_FLOOR_MS),
            DURATION_CALIBRATION_MIN_SAMPLES,
        );
        assert_eq!(
            boundary.calibration.unwrap().suggested_class,
            ToolDurationClassWire::Long
        );
    }

    #[test]
    fn calibration_suggests_bounded_class_for_unbounded() {
        let short = calibrate(
            Some(ToolDurationClassWire::Unbounded),
            Some(180_000),
            15,
        );
        let short = short.calibration.unwrap();
        assert_eq!(short.kind, DurationCalibrationKindWire::BoundedEvidence);
        assert_eq!(short.suggested_class, ToolDurationClassWire::Short);
        assert_eq!(short.floor_seconds, None);

        let long = calibrate(
            Some(ToolDurationClassWire::Unbounded),
            Some(900_000),
            15,
        );
        assert_eq!(
            long.calibration.unwrap().suggested_class,
            ToolDurationClassWire::Long
        );
    }

    #[test]
    fn calibration_stays_silent_when_class_matches_typical() {
        let short =
            calibrate(Some(ToolDurationClassWire::Short), Some(180_000), 15);
        assert_eq!(short.calibration, None);

        let long =
            calibrate(Some(ToolDurationClassWire::Long), Some(700_000), 15);
        assert_eq!(long.calibration, None);

        let boundary = calibrate(
            Some(ToolDurationClassWire::Long),
            Some(DURATION_CLASS_LONG_FLOOR_MS),
            DURATION_CALIBRATION_MIN_SAMPLES,
        );
        assert_eq!(boundary.calibration, None);
    }

    #[test]
    fn calibration_rejects_unknown_schema() {
        let schema = duration_calibration(DurationCalibrationRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION + 1,
            duration_class: None,
            typical_duration_ms: None,
            typical_sample_count: 0,
        });
        match schema {
            Err(ToolRunError::SchemaVersion { .. }) => {}
            other => panic!("expected schema error, got {other:?}"),
        }
    }

    fn budget(
        ceiling_seconds: Option<u64>,
        soft_ceiling_seconds: Option<u64>,
    ) -> SyncWaitBudgetResponseWire {
        sync_wait_budget(SyncWaitBudgetRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            ceiling_seconds,
            soft_ceiling_seconds,
        })
        .unwrap()
    }

    #[test]
    fn sync_wait_budget_pins_the_table() {
        let hard_600 = budget(Some(600), None);
        assert_eq!(hard_600.budget_seconds, Some(510));
        assert_eq!(hard_600.source, Some(SyncWaitBudgetSourceWire::Hard));
        assert_eq!(hard_600.margin_seconds, Some(90));

        let hard_14400 = budget(Some(14_400), None);
        assert_eq!(hard_14400.budget_seconds, Some(14_100));
        assert_eq!(hard_14400.source, Some(SyncWaitBudgetSourceWire::Hard));
        assert_eq!(hard_14400.margin_seconds, Some(300));

        let hard_60 = budget(Some(60), None);
        assert_eq!(hard_60.budget_seconds, Some(30));
        assert_eq!(hard_60.margin_seconds, Some(90));

        let hard_1 = budget(Some(1), None);
        assert_eq!(hard_1.budget_seconds, Some(0));
        assert_eq!(hard_1.margin_seconds, Some(90));

        let soft_only = budget(None, Some(1200));
        assert_eq!(soft_only.budget_seconds, Some(1200));
        assert_eq!(soft_only.source, Some(SyncWaitBudgetSourceWire::Soft));
        assert_eq!(soft_only.margin_seconds, None);

        let soft_wins = budget(Some(600), Some(200));
        assert_eq!(soft_wins.budget_seconds, Some(200));
        assert_eq!(soft_wins.source, Some(SyncWaitBudgetSourceWire::Soft));
        assert_eq!(soft_wins.margin_seconds, Some(90));

        let tie_reports_hard = budget(Some(600), Some(510));
        assert_eq!(tie_reports_hard.budget_seconds, Some(510));
        assert_eq!(
            tie_reports_hard.source,
            Some(SyncWaitBudgetSourceWire::Hard)
        );

        let hard_wins = budget(Some(600), Some(1000));
        assert_eq!(hard_wins.budget_seconds, Some(510));
        assert_eq!(hard_wins.source, Some(SyncWaitBudgetSourceWire::Hard));

        let neither = budget(None, None);
        assert_eq!(neither.budget_seconds, None);
        assert_eq!(neither.source, None);
        assert_eq!(neither.margin_seconds, None);
        assert_eq!(neither.ceiling_seconds, None);
        assert_eq!(neither.soft_ceiling_seconds, None);
    }

    #[test]
    fn sync_wait_budget_echoes_positive_ceilings() {
        let response = budget(Some(600), Some(200));
        assert_eq!(response.ceiling_seconds, Some(600));
        assert_eq!(response.soft_ceiling_seconds, Some(200));
    }

    #[test]
    fn sync_wait_budget_rejects_zero_and_unknown_schema() {
        for request in [
            SyncWaitBudgetRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                ceiling_seconds: Some(0),
                soft_ceiling_seconds: None,
            },
            SyncWaitBudgetRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                ceiling_seconds: None,
                soft_ceiling_seconds: Some(0),
            },
            SyncWaitBudgetRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                ceiling_seconds: Some(0),
                soft_ceiling_seconds: Some(0),
            },
            SyncWaitBudgetRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                ceiling_seconds: Some(0),
                soft_ceiling_seconds: Some(200),
            },
        ] {
            assert!(sync_wait_budget(request).is_err());
        }
        let schema = sync_wait_budget(SyncWaitBudgetRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION + 1,
            ceiling_seconds: None,
            soft_ceiling_seconds: None,
        });
        match schema {
            Err(ToolRunError::SchemaVersion { .. }) => {}
            other => panic!("expected schema error, got {other:?}"),
        }
    }
}
