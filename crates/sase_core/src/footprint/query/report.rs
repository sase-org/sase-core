//! Report and card queries over the Footprint ledger.

use std::time::Duration;

use super::super::wire::{
    ActorUsesWire, CardWire, FeatureRowWire, QueryRequestWire, ReportWire,
    FOOTPRINT_WIRE_SCHEMA_VERSION,
};
use super::super::FootprintError;
use super::engine::{feature_row_for, Engine, Lens};

/// Build a lens report from the ledger and overlay.
pub fn report(
    request: &QueryRequestWire,
    busy_timeout: Duration,
) -> Result<ReportWire, FootprintError> {
    let engine = Engine::load(request, busy_timeout)?;
    let areas = if engine.lens == Lens::Map {
        engine.area_rows()
    } else {
        Vec::new()
    };
    let coverage = if engine.lens == Lens::Coverage {
        engine.coverage_summaries()
    } else {
        Vec::new()
    };
    let rows = match engine.lens {
        Lens::Coverage => Vec::new(),
        Lens::Map if engine.area.is_none() => Vec::new(),
        _ => engine.feature_rows(),
    };
    Ok(ReportWire {
        schema_version: FOOTPRINT_WIRE_SCHEMA_VERSION,
        header: engine.header(),
        tiles: engine.tiles(),
        areas,
        rows,
        coverage,
    })
}

/// Build an evidence card for one feature or area.
pub fn card(
    request: &QueryRequestWire,
    busy_timeout: Duration,
) -> Result<CardWire, FootprintError> {
    let engine = Engine::load(request, busy_timeout)?;
    if let Some(feature_id) = request.feature_id.as_deref() {
        let stats = engine.feature(feature_id)?;
        let area_summary = if stats.area.is_empty() {
            None
        } else {
            engine.area_summary(&stats.area)
        };
        return Ok(CardWire {
            schema_version: FOOTPRINT_WIRE_SCHEMA_VERSION,
            row: feature_row_for(&engine, stats),
            daily_series: engine.daily_series(feature_id, &stats.ns),
            first_in_catalog: stats.first_in_catalog.clone(),
            last_in_catalog: stats.last_in_catalog.clone(),
            parent: stats.parent.clone(),
            children: engine.children(feature_id),
            retire_with: engine.overlay.retire_with(feature_id),
            area_summary,
        });
    }
    if let Some(area) = request.area.as_deref() {
        let summary = engine.area_summary(area).ok_or_else(|| {
            FootprintError::validation(format!("unknown area {area}"))
        })?;
        return Ok(CardWire {
            schema_version: FOOTPRINT_WIRE_SCHEMA_VERSION,
            row: FeatureRowWire {
                id: area.to_string(),
                label: area.to_string(),
                ns: summary.ns.clone(),
                area: area.to_string(),
                kind: "area".to_string(),
                state: String::new(),
                coverage_incomplete: false,
                uses: ActorUsesWire::default(),
                days_used: 0,
                last_used_day: None,
                last_used_actor: None,
                via: Default::default(),
                outcome: Default::default(),
                dur_ms_sum: 0,
                loc: None,
                parent: None,
                protected: None,
                decision: None,
                decision_reason: None,
                decided_day: None,
                snooze_until: None,
                friction_ratio_bps: None,
                parent_days: None,
            },
            daily_series: Vec::new(),
            first_in_catalog: None,
            last_in_catalog: None,
            parent: None,
            children: Vec::new(),
            retire_with: Vec::new(),
            area_summary: Some(summary),
        });
    }
    Err(FootprintError::validation(
        "footprint_card requires feature_id or area",
    ))
}
