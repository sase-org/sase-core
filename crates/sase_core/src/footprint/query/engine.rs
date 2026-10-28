//! Shared load, evidence, tiles, and header for Footprint queries.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, NaiveDate};
use rusqlite::{params, Connection, OptionalExtension};

use super::super::catalog::parse_day;
use super::super::store::{
    bounded_busy_timeout, meta_get, meta_get_u64, open_read_store,
    open_write_store,
};
use super::super::wire::{
    ns_of, require_member, surface_for_ns, ActorUsesWire, AreaRowWire,
    AreaSummaryWire, CoverageSummaryWire, FeatureRowWire, QueryRequestWire,
    ReportHeaderWire, TilesWire, ACTOR_SCOPES, EVIDENCE_STATES, LENSES,
    SURFACES,
};
use super::super::FootprintError;
use super::overlay::OverlayIndex;

const NS_ORDER: &[&str] = &[
    "cli", "tui", "view", "macro", "skill", "config", "auto", "plugin",
];
const ACTOR_RANK: &[&str] = &["human", "agent", "automation", "unknown"];
pub(crate) const HOT_DEFAULT_LIMIT: u32 = 50;
type DaySet = BTreeSet<String>;
type SurfaceDayMap = BTreeMap<String, DaySet>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActorScope {
    You,
    Agents,
    Automation,
    Everyone,
}

impl ActorScope {
    fn parse(value: &str) -> Result<Self, FootprintError> {
        require_member(value, ACTOR_SCOPES, "actor")?;
        Ok(match value {
            "you" => Self::You,
            "agents" => Self::Agents,
            "automation" => Self::Automation,
            _ => Self::Everyone,
        })
    }

    fn matches(self, actor: &str) -> bool {
        match self {
            Self::You => actor == "human",
            Self::Agents => actor == "agent",
            Self::Automation => actor == "automation",
            Self::Everyone => true,
        }
    }

    fn uses_human_active(self) -> bool {
        matches!(self, Self::You)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lens {
    Map,
    Cold,
    Fading,
    AgentOnly,
    DeadPaths,
    Friction,
    Hot,
    Coverage,
    Decisions,
}

impl Lens {
    fn parse(value: &str) -> Result<Self, FootprintError> {
        require_member(value, LENSES, "lens")?;
        Ok(match value {
            "map" => Self::Map,
            "cold" => Self::Cold,
            "fading" => Self::Fading,
            "agent-only" => Self::AgentOnly,
            "dead-paths" => Self::DeadPaths,
            "friction" => Self::Friction,
            "hot" => Self::Hot,
            "coverage" => Self::Coverage,
            _ => Self::Decisions,
        })
    }
}

#[derive(Debug, Clone)]
struct FeatureRec {
    id: String,
    ns: String,
    area: String,
    kind: String,
    label: String,
    parent: Option<String>,
    loc: Option<i64>,
    first_in_catalog: Option<String>,
    last_in_catalog: Option<String>,
}

#[derive(Debug, Clone)]
struct DailyRec {
    day: String,
    feature_id: String,
    actor: String,
    via: String,
    outcome: String,
    n: i64,
    dur_ms_sum: i64,
}

#[derive(Debug, Clone)]
struct CoverageRec {
    day: String,
    surface: String,
    measured: i64,
    human_active: i64,
    torn: i64,
    dropped: i64,
    unknown_ids: i64,
    incomplete: i64,
}

#[derive(Debug, Clone)]
struct ReviewRec {
    feature_id: String,
    decision: String,
    reason: Option<String>,
    decided_day: String,
    snooze_until: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FeatureStats {
    pub id: String,
    pub ns: String,
    pub area: String,
    pub kind: String,
    pub label: String,
    pub parent: Option<String>,
    pub loc: Option<i64>,
    pub first_in_catalog: Option<String>,
    pub last_in_catalog: Option<String>,
    pub uses: ActorUsesWire,
    pub days_human: DaySet,
    pub days_agent: DaySet,
    pub days_scope: DaySet,
    pub days_anyone: DaySet,
    pub last_used_day: Option<String>,
    pub last_used_actor: Option<String>,
    pub via: BTreeMap<String, i64>,
    pub outcome: BTreeMap<String, i64>,
    pub dur_ms_sum: i64,
    pub via_key: i64,
    pub via_other: i64,
    pub n_total: i64,
    pub n_friction: i64,
    pub state: String,
    pub coverage_incomplete: bool,
    pub protected: Option<String>,
    pub overlay_keep: Option<String>,
    pub decision: Option<String>,
    pub decision_reason: Option<String>,
    pub decided_day: Option<String>,
    pub snooze_until: Option<String>,
}

impl FeatureStats {
    fn days_used(&self) -> u32 {
        self.days_scope.len() as u32
    }

    fn uses_anyone(&self) -> i64 {
        self.uses.human
            + self.uses.agent
            + self.uses.automation
            + self.uses.unknown
    }

    fn is_agent_only(&self) -> bool {
        !self.days_agent.is_empty() && self.days_human.is_empty()
    }

    fn snooze_expired(&self, today: &str) -> bool {
        match self.snooze_until.as_deref() {
            Some(until) => until < today,
            None => true,
        }
    }

    fn is_candidate(&self, today: &str) -> bool {
        self.state == "no_recorded_use"
            && self.uses_anyone() == 0
            && self.protected.is_none()
            && self.overlay_keep.is_none()
            && match self.decision.as_deref() {
                None => true,
                Some("later") => self.snooze_expired(today),
                Some(_) => false,
            }
    }

    fn friction_ratio_bps(&self) -> Option<i64> {
        if self.n_total < 3 {
            return None;
        }
        Some(self.n_friction.saturating_mul(10_000) / self.n_total)
    }
}

pub(crate) struct Engine {
    pub today: NaiveDate,
    pub today_s: String,
    pub window_start: NaiveDate,
    pub window_days: Option<u32>,
    pub actor: String,
    pub actor_scope: ActorScope,
    pub lens: Lens,
    pub area: Option<String>,
    pub min_catalog_days: u32,
    pub min_active_days: u32,
    pub fading_days: u32,
    pub limit: Option<u32>,
    pub overlay: OverlayIndex,
    pub generation: u64,
    pub recording_since: Option<String>,
    pub imported_pack_count: u64,
    pub latest_catalog_day: Option<String>,
    stats: BTreeMap<String, FeatureStats>,
    reviews: Vec<ReviewRec>,
    measured_days: SurfaceDayMap,
    active_days: SurfaceDayMap,
    gate_active_days: BTreeMap<String, u32>,
    coverage_rows: Vec<CoverageRec>,
    daily: Vec<DailyRec>,
}

impl Engine {
    pub(crate) fn load(
        request: &QueryRequestWire,
        busy_timeout: Duration,
    ) -> Result<Self, FootprintError> {
        let today = parse_day(&request.today)?;
        let actor_scope = ActorScope::parse(&request.actor)?;
        let lens = Lens::parse(&request.lens)?;
        if let Some(0) = request.window_days {
            return Err(FootprintError::validation(
                "window_days must be greater than zero",
            ));
        }
        let overlay = OverlayIndex::parse(&request.overlay)?;
        let timeout = bounded_busy_timeout(busy_timeout);
        let store_path = Path::new(&request.store_path);
        if !store_path.exists() {
            return Ok(Self::empty(request, today, actor_scope, lens, overlay));
        }
        let _ = open_write_store(store_path, timeout)?;
        let conn = open_read_store(store_path, timeout)?;
        let recording_since = meta_get(&conn, "first_record_day")?;
        let generation = meta_get_u64(&conn, "generation")?;
        let latest_catalog_day = latest_catalog_day(&conn)?;
        let imported_pack_count = imported_pack_count(&conn)?;
        let window_start = window_start(
            request.window_days,
            today,
            recording_since.as_deref(),
            &conn,
        )?;
        let features = load_features(&conn)?;
        let daily =
            load_daily(&conn, &window_start.to_string(), &request.today)?;
        let coverage =
            load_coverage(&conn, &window_start.to_string(), &request.today)?;
        let all_live_coverage = load_live_measured(&conn)?;
        let reviews = load_reviews(&conn)?;
        let (measured_days, active_days, gate_active_days) = surface_days(
            &coverage,
            &all_live_coverage,
            actor_scope,
            request.calibration_days,
            &window_start.to_string(),
            &request.today,
        );
        let mut engine = Self {
            today,
            today_s: request.today.clone(),
            window_start,
            window_days: request.window_days,
            actor: request.actor.clone(),
            actor_scope,
            lens,
            area: request.area.clone(),
            min_catalog_days: request.min_catalog_days,
            min_active_days: request.min_active_days,
            fading_days: request.fading_days,
            limit: request.limit,
            overlay,
            generation,
            recording_since,
            imported_pack_count,
            latest_catalog_day,
            stats: BTreeMap::new(),
            reviews,
            measured_days,
            active_days,
            gate_active_days,
            coverage_rows: coverage,
            daily,
        };
        engine.build_stats(features);
        Ok(engine)
    }

    fn empty(
        request: &QueryRequestWire,
        today: NaiveDate,
        actor_scope: ActorScope,
        lens: Lens,
        overlay: OverlayIndex,
    ) -> Self {
        Self {
            today,
            today_s: request.today.clone(),
            window_start: today,
            window_days: request.window_days,
            actor: request.actor.clone(),
            actor_scope,
            lens,
            area: request.area.clone(),
            min_catalog_days: request.min_catalog_days,
            min_active_days: request.min_active_days,
            fading_days: request.fading_days,
            limit: request.limit,
            overlay,
            generation: 0,
            recording_since: None,
            imported_pack_count: 0,
            latest_catalog_day: None,
            stats: BTreeMap::new(),
            reviews: Vec::new(),
            measured_days: BTreeMap::new(),
            active_days: BTreeMap::new(),
            gate_active_days: BTreeMap::new(),
            coverage_rows: Vec::new(),
            daily: Vec::new(),
        }
    }

    fn build_stats(&mut self, features: Vec<FeatureRec>) {
        let mut stats: BTreeMap<String, FeatureStats> = BTreeMap::new();
        for feature in features {
            let overlay = self.overlay.get(&feature.id);
            stats.insert(
                feature.id.clone(),
                FeatureStats {
                    id: feature.id,
                    ns: feature.ns,
                    area: feature.area,
                    kind: feature.kind,
                    label: feature.label,
                    parent: feature.parent,
                    loc: feature.loc,
                    first_in_catalog: feature.first_in_catalog,
                    last_in_catalog: feature.last_in_catalog,
                    protected: overlay.and_then(|item| item.protected.clone()),
                    overlay_keep: overlay.and_then(|item| item.keep.clone()),
                    ..FeatureStats::default()
                },
            );
        }
        for review in &self.reviews {
            let entry =
                stats.entry(review.feature_id.clone()).or_insert_with(|| {
                    FeatureStats {
                        id: review.feature_id.clone(),
                        ns: ns_of(&review.feature_id)
                            .unwrap_or("unknown")
                            .to_string(),
                        ..FeatureStats::default()
                    }
                });
            entry.decision = Some(review.decision.clone());
            entry.decision_reason = review.reason.clone();
            entry.decided_day = Some(review.decided_day.clone());
            entry.snooze_until = review.snooze_until.clone();
        }
        for row in &self.daily {
            let entry =
                stats.entry(row.feature_id.clone()).or_insert_with(|| {
                    FeatureStats {
                        id: row.feature_id.clone(),
                        ns: ns_of(&row.feature_id)
                            .unwrap_or("unknown")
                            .to_string(),
                        ..FeatureStats::default()
                    }
                });
            add_uses(&mut entry.uses, &row.actor, row.n);
            entry.days_anyone.insert(row.day.clone());
            match row.actor.as_str() {
                "human" => {
                    entry.days_human.insert(row.day.clone());
                }
                "agent" => {
                    entry.days_agent.insert(row.day.clone());
                }
                _ => {}
            }
            if self.actor_scope.matches(&row.actor) {
                entry.days_scope.insert(row.day.clone());
                *entry.via.entry(row.via.clone()).or_insert(0) += row.n;
                *entry.outcome.entry(row.outcome.clone()).or_insert(0) += row.n;
                entry.dur_ms_sum += row.dur_ms_sum;
                if row.via == "key" {
                    entry.via_key += row.n;
                } else {
                    entry.via_other += row.n;
                }
            }
            entry.n_total += row.n;
            if row.outcome == "usage_error" || row.outcome == "help" {
                entry.n_friction += row.n;
            }
        }
        let today_s = self.today_s.clone();
        let latest = self.latest_catalog_day.clone();
        let min_catalog = self.min_catalog_days;
        let min_active = self.min_active_days;
        let measured = self.measured_days.clone();
        let gate_days = self.gate_active_days.clone();
        let incomplete = incomplete_surfaces(&self.coverage_rows);
        for entry in stats.values_mut() {
            if entry.decision.is_none() {
                if let Some(keep) = entry.overlay_keep.clone() {
                    entry.decision = Some("keep".to_string());
                    if entry.decision_reason.is_none() {
                        entry.decision_reason = Some(keep);
                    }
                }
            }
            finish_last_used(entry, &self.daily, self.actor_scope);
            let surface = surface_for_ns(&entry.ns).unwrap_or("cli");
            entry.coverage_incomplete = incomplete.contains(surface);
            entry.state = evidence_state(
                entry,
                &today_s,
                latest.as_deref(),
                min_catalog,
                min_active,
                measured.get(surface),
                gate_days.get(surface).copied().unwrap_or(0),
            );
            assert!(
                EVIDENCE_STATES.contains(&entry.state.as_str()),
                "unknown evidence state {}",
                entry.state
            );
        }
        self.stats = stats;
    }

    pub(crate) fn header(&self) -> ReportHeaderWire {
        let mut observed = BTreeMap::new();
        for surface in SURFACES {
            let days = self
                .active_days
                .get(*surface)
                .map(|set| set.len() as u32)
                .unwrap_or(0);
            observed.insert((*surface).to_string(), days);
        }
        let live_progress = ["cli", "tui"]
            .iter()
            .map(|surface| {
                self.gate_active_days.get(*surface).copied().unwrap_or(0)
            })
            .max()
            .unwrap_or(0);
        ReportHeaderWire {
            actor: self.actor.clone(),
            window_days: self.window_days,
            window_start: Some(self.window_start.to_string()),
            window_end: self.today_s.clone(),
            observed_active_days: observed,
            recording_since: self.recording_since.clone(),
            gate_active_days: live_progress,
            gate_required_days: self.min_active_days,
            gate_met: live_progress >= self.min_active_days,
            generation: self.generation,
            imported_pack_count: self.imported_pack_count,
        }
    }

    pub(crate) fn tiles(&self) -> TilesWire {
        let header = self.header();
        let current = self.current_features();
        let used =
            current.iter().filter(|row| row.state == "used").count() as u64;
        let rare = current
            .iter()
            .filter(|row| row.state == "rare_by_design")
            .count() as u64;
        let not_measured = current
            .iter()
            .filter(|row| row.state == "not_measured")
            .count() as u64;
        let candidates = current
            .iter()
            .filter(|row| row.is_candidate(&self.today_s))
            .count() as u64;
        let agent_only =
            current.iter().filter(|row| row.is_agent_only()).count() as u64;
        TilesWire {
            used,
            total: current.len() as u64,
            candidates,
            candidates_preview: !header.gate_met,
            rare_by_design: rare,
            agent_only,
            not_measured,
        }
    }

    fn current_features(&self) -> Vec<&FeatureStats> {
        self.stats
            .values()
            .filter(|row| row.state != "retired")
            .collect()
    }

    pub(crate) fn area_rows(&self) -> Vec<AreaRowWire> {
        let mut grouped: BTreeMap<String, Vec<&FeatureStats>> = BTreeMap::new();
        for row in self.current_features() {
            if let Some(area) = &self.area {
                if &row.area != area {
                    continue;
                }
            }
            grouped.entry(row.area.clone()).or_default().push(row);
        }
        let mut rows: Vec<AreaRowWire> = grouped
            .into_iter()
            .map(|(area, features)| area_row(&area, &features, self.today))
            .collect();
        rows.sort_by(|left, right| {
            ns_rank(&left.ns)
                .cmp(&ns_rank(&right.ns))
                .then(right.used_pct.cmp(&left.used_pct))
                .then(left.area.cmp(&right.area))
        });
        rows
    }

    pub(crate) fn area_summary(&self, area: &str) -> Option<AreaSummaryWire> {
        let features: Vec<&FeatureStats> = self
            .stats
            .values()
            .filter(|row| row.area == area && row.state != "retired")
            .collect();
        if features.is_empty() {
            return None;
        }
        let row = area_row(area, &features, self.today);
        let mut unused: Vec<&FeatureStats> = features
            .into_iter()
            .filter(|item| item.uses_anyone() == 0)
            .collect();
        unused.sort_by(|left, right| {
            right
                .loc
                .unwrap_or(0)
                .cmp(&left.loc.unwrap_or(0))
                .then(left.id.cmp(&right.id))
        });
        unused.truncate(5);
        Some(AreaSummaryWire {
            area: row.area,
            ns: row.ns,
            total: row.total,
            used: row.used,
            used_pct: row.used_pct,
            recency_7d: row.recency_7d,
            recency_30d: row.recency_30d,
            recency_window: row.recency_window,
            recency_none: row.recency_none,
            recency_not_measured: row.recency_not_measured,
            unused_by_loc: unused.into_iter().map(feature_row).collect(),
        })
    }

    pub(crate) fn feature_rows(&self) -> Vec<FeatureRowWire> {
        let mut selected: Vec<&FeatureStats> = self
            .stats
            .values()
            .filter(|row| self.include_row(row))
            .collect();
        selected.sort_by(|left, right| self.cmp_rows(left, right));
        let limit = self.row_limit();
        if let Some(limit) = limit {
            selected.truncate(limit as usize);
        }
        selected.into_iter().map(feature_row).collect()
    }

    pub(crate) fn coverage_summaries(&self) -> Vec<CoverageSummaryWire> {
        SURFACES
            .iter()
            .map(|surface| {
                let measured = self
                    .measured_days
                    .get(*surface)
                    .map(|set| set.len() as u32)
                    .unwrap_or(0);
                let active = self
                    .active_days
                    .get(*surface)
                    .map(|set| set.len() as u32)
                    .unwrap_or(0);
                let rows: Vec<&CoverageRec> = self
                    .coverage_rows
                    .iter()
                    .filter(|row| row.surface == *surface)
                    .collect();
                let torn: i64 = rows.iter().map(|row| row.torn).sum();
                let dropped: i64 = rows.iter().map(|row| row.dropped).sum();
                let unknown_ids: i64 =
                    rows.iter().map(|row| row.unknown_ids).sum();
                let incomplete_days = rows
                    .iter()
                    .filter(|row| row.incomplete > 0)
                    .map(|row| row.day.as_str())
                    .collect::<BTreeSet<_>>()
                    .len() as u32;
                let first_measured_day = rows
                    .iter()
                    .filter(|row| row.measured > 0)
                    .map(|row| row.day.clone())
                    .min();
                CoverageSummaryWire {
                    surface: (*surface).to_string(),
                    measured_days: measured,
                    active_days: active,
                    torn,
                    dropped,
                    unknown_ids,
                    first_measured_day,
                    incomplete_days,
                }
            })
            .collect()
    }

    pub(crate) fn feature(
        &self,
        feature_id: &str,
    ) -> Result<&FeatureStats, FootprintError> {
        self.stats.get(feature_id).ok_or_else(|| {
            FootprintError::validation(format!("unknown feature {feature_id}"))
        })
    }

    pub(crate) fn children(&self, feature_id: &str) -> Vec<String> {
        let mut ids: Vec<String> = self
            .stats
            .values()
            .filter(|row| row.parent.as_deref() == Some(feature_id))
            .map(|row| row.id.clone())
            .collect();
        ids.sort();
        ids
    }

    pub(crate) fn daily_series(
        &self,
        feature_id: &str,
        ns: &str,
    ) -> Vec<super::super::wire::DailyPointWire> {
        let surface = surface_for_ns(ns).unwrap_or("cli");
        let measured =
            self.measured_days.get(surface).cloned().unwrap_or_default();
        let mut by_day: BTreeMap<String, i64> = BTreeMap::new();
        for row in &self.daily {
            if row.feature_id == feature_id
                && self.actor_scope.matches(&row.actor)
            {
                *by_day.entry(row.day.clone()).or_insert(0) += row.n;
            }
        }
        let mut series = Vec::new();
        let mut day = self.window_start;
        while day <= self.today {
            let key = day.to_string();
            series.push(super::super::wire::DailyPointWire {
                day: key.clone(),
                n: by_day.get(&key).copied().unwrap_or(0),
                measured: measured.contains(&key),
            });
            day += ChronoDuration::days(1);
        }
        series
    }

    fn include_row(&self, row: &FeatureStats) -> bool {
        if let Some(area) = &self.area {
            if &row.area != area {
                return false;
            }
        }
        match self.lens {
            Lens::Map => self.area.is_some(),
            Lens::Cold => row.is_candidate(&self.today_s),
            Lens::Fading => self.is_fading(row),
            Lens::AgentOnly => row.is_agent_only(),
            Lens::DeadPaths => self.is_dead_path(row),
            Lens::Friction => {
                row.friction_ratio_bps().is_some_and(|bps| bps >= 2_500)
            }
            Lens::Hot => row.days_used() > 0,
            Lens::Coverage => false,
            Lens::Decisions => row.decided_day.is_some(),
        }
    }

    fn is_fading(&self, row: &FeatureStats) -> bool {
        let Some(last) = row.last_used_day.as_deref() else {
            return false;
        };
        let cutoff = self
            .today
            .checked_sub_signed(ChronoDuration::days(i64::from(
                self.fading_days,
            )))
            .unwrap_or(self.today)
            + ChronoDuration::days(1);
        last < cutoff.to_string().as_str() && row.days_used() > 0
    }

    fn is_dead_path(&self, row: &FeatureStats) -> bool {
        if row.kind == "option" {
            let parent_used = row
                .parent
                .as_ref()
                .and_then(|parent| self.stats.get(parent))
                .is_some_and(|parent| parent.uses_anyone() > 0);
            return row.uses_anyone() == 0 && parent_used;
        }
        matches!(row.kind.as_str(), "action" | "mode_key")
            && row.via_key == 0
            && row.via_other > 0
    }

    fn cmp_rows(
        &self,
        left: &FeatureStats,
        right: &FeatureStats,
    ) -> std::cmp::Ordering {
        match self.lens {
            Lens::Cold => right
                .loc
                .unwrap_or(0)
                .cmp(&left.loc.unwrap_or(0))
                .then(left.id.cmp(&right.id)),
            Lens::Fading => left
                .last_used_day
                .cmp(&right.last_used_day)
                .then(left.id.cmp(&right.id)),
            Lens::AgentOnly => right
                .days_agent
                .len()
                .cmp(&left.days_agent.len())
                .then(left.id.cmp(&right.id)),
            Lens::DeadPaths => self
                .parent_days(right)
                .cmp(&self.parent_days(left))
                .then(left.id.cmp(&right.id)),
            Lens::Friction => right
                .friction_ratio_bps()
                .unwrap_or(0)
                .cmp(&left.friction_ratio_bps().unwrap_or(0))
                .then(left.id.cmp(&right.id)),
            Lens::Hot => right
                .days_used()
                .cmp(&left.days_used())
                .then(right.uses_anyone().cmp(&left.uses_anyone()))
                .then(left.id.cmp(&right.id)),
            Lens::Decisions => right
                .decided_day
                .cmp(&left.decided_day)
                .then(left.id.cmp(&right.id)),
            _ => left.id.cmp(&right.id),
        }
    }

    fn parent_days(&self, row: &FeatureStats) -> u32 {
        if let Some(parent) = &row.parent {
            return self
                .stats
                .get(parent)
                .map(|item| item.days_anyone.len() as u32)
                .unwrap_or(0);
        }
        row.days_anyone.len() as u32
    }

    fn row_limit(&self) -> Option<u32> {
        match (self.limit, self.lens) {
            (Some(limit), _) => Some(limit),
            (None, Lens::Hot) => Some(HOT_DEFAULT_LIMIT),
            _ => None,
        }
    }

    pub(crate) fn parent_days_of(&self, row: &FeatureStats) -> Option<u32> {
        if self.lens == Lens::DeadPaths {
            Some(self.parent_days(row))
        } else {
            None
        }
    }
}

pub(crate) fn feature_row(stats: &FeatureStats) -> FeatureRowWire {
    FeatureRowWire {
        id: stats.id.clone(),
        label: stats.label.clone(),
        ns: stats.ns.clone(),
        area: stats.area.clone(),
        kind: stats.kind.clone(),
        state: stats.state.clone(),
        coverage_incomplete: stats.coverage_incomplete,
        uses: stats.uses.clone(),
        days_used: stats.days_used(),
        last_used_day: stats.last_used_day.clone(),
        last_used_actor: stats.last_used_actor.clone(),
        via: stats.via.clone(),
        outcome: stats.outcome.clone(),
        dur_ms_sum: stats.dur_ms_sum,
        loc: stats.loc,
        parent: stats.parent.clone(),
        protected: stats.protected.clone(),
        decision: stats.decision.clone(),
        decision_reason: stats.decision_reason.clone(),
        decided_day: stats.decided_day.clone(),
        snooze_until: stats.snooze_until.clone(),
        friction_ratio_bps: stats.friction_ratio_bps(),
        parent_days: None,
    }
}

pub(crate) fn feature_row_for(
    engine: &Engine,
    stats: &FeatureStats,
) -> FeatureRowWire {
    let mut row = feature_row(stats);
    row.parent_days = engine.parent_days_of(stats);
    row
}

fn evidence_state(
    row: &FeatureStats,
    today: &str,
    latest_catalog_day: Option<&str>,
    min_catalog_days: u32,
    min_active_days: u32,
    measured: Option<&BTreeSet<String>>,
    gate_active: u32,
) -> String {
    let retired = match (row.last_in_catalog.as_deref(), latest_catalog_day) {
        (Some(last), Some(latest)) => last < latest,
        (None, Some(_)) => true,
        _ => false,
    };
    if retired {
        return "retired".to_string();
    }
    let measured_any = measured.is_some_and(|set| !set.is_empty());
    if !measured_any {
        return "not_measured".to_string();
    }
    if !row.days_scope.is_empty() {
        return "used".to_string();
    }
    if row.protected.is_some() {
        return "rare_by_design".to_string();
    }
    let catalog_days = catalog_age_days(row.first_in_catalog.as_deref(), today);
    if catalog_days < min_catalog_days || gate_active < min_active_days {
        return "too_new".to_string();
    }
    "no_recorded_use".to_string()
}

fn catalog_age_days(first: Option<&str>, today: &str) -> u32 {
    let Some(first) = first else {
        return 0;
    };
    let Ok(first_day) = NaiveDate::parse_from_str(first, "%Y-%m-%d") else {
        return 0;
    };
    let Ok(today_day) = NaiveDate::parse_from_str(today, "%Y-%m-%d") else {
        return 0;
    };
    if today_day < first_day {
        return 0;
    }
    today_day.signed_duration_since(first_day).num_days() as u32 + 1
}

fn area_row(
    area: &str,
    features: &[&FeatureStats],
    today: NaiveDate,
) -> AreaRowWire {
    let ns = features
        .first()
        .map(|row| row.ns.clone())
        .unwrap_or_default();
    let total = features.len() as u64;
    let used = features.iter().filter(|row| row.state == "used").count() as u64;
    let used_pct = used.saturating_mul(100).checked_div(total).unwrap_or(0);
    let mut recency_7d = 0;
    let mut recency_30d = 0;
    let mut recency_window = 0;
    let mut recency_none = 0;
    let mut recency_not_measured = 0;
    let day_7 = today
        .checked_sub_signed(ChronoDuration::days(6))
        .unwrap_or(today)
        .to_string();
    let day_30 = today
        .checked_sub_signed(ChronoDuration::days(29))
        .unwrap_or(today)
        .to_string();
    for row in features {
        if row.state == "not_measured" {
            recency_not_measured += 1;
            continue;
        }
        match row.last_used_day.as_deref() {
            Some(day) if day >= day_7.as_str() => recency_7d += 1,
            Some(day) if day >= day_30.as_str() => recency_30d += 1,
            Some(_) => recency_window += 1,
            None => recency_none += 1,
        }
    }
    AreaRowWire {
        area: area.to_string(),
        ns,
        total,
        used,
        used_pct: used_pct as i64,
        recency_7d,
        recency_30d,
        recency_window,
        recency_none,
        recency_not_measured,
    }
}

fn ns_rank(ns: &str) -> usize {
    NS_ORDER.iter().position(|item| *item == ns).unwrap_or(99)
}

fn add_uses(uses: &mut ActorUsesWire, actor: &str, n: i64) {
    match actor {
        "human" => uses.human += n,
        "agent" => uses.agent += n,
        "automation" => uses.automation += n,
        _ => uses.unknown += n,
    }
}

fn finish_last_used(
    entry: &mut FeatureStats,
    daily: &[DailyRec],
    scope: ActorScope,
) {
    let mut best_day: Option<&str> = None;
    let mut best_actor: Option<&str> = None;
    for row in daily {
        if row.feature_id != entry.id || !scope.matches(&row.actor) {
            continue;
        }
        let better_day = match best_day {
            None => true,
            Some(day) => row.day.as_str() > day,
        };
        if better_day {
            best_day = Some(row.day.as_str());
            best_actor = Some(row.actor.as_str());
            continue;
        }
        if best_day == Some(row.day.as_str())
            && actor_rank(row.actor.as_str())
                < actor_rank(best_actor.unwrap_or("unknown"))
        {
            best_actor = Some(row.actor.as_str());
        }
    }
    entry.last_used_day = best_day.map(str::to_string);
    entry.last_used_actor = best_actor.map(str::to_string);
}

fn actor_rank(actor: &str) -> usize {
    ACTOR_RANK
        .iter()
        .position(|item| *item == actor)
        .unwrap_or(99)
}

fn incomplete_surfaces(rows: &[CoverageRec]) -> BTreeSet<String> {
    rows.iter()
        .filter(|row| row.dropped > 0 || row.incomplete > 0)
        .map(|row| row.surface.clone())
        .collect()
}

fn surface_days(
    window: &[CoverageRec],
    all_live: &[(String, String)],
    scope: ActorScope,
    calibration_days: u32,
    window_start: &str,
    today: &str,
) -> (SurfaceDayMap, SurfaceDayMap, BTreeMap<String, u32>) {
    let mut measured: SurfaceDayMap = BTreeMap::new();
    let mut active: SurfaceDayMap = BTreeMap::new();
    for row in window {
        if row.measured > 0 {
            measured
                .entry(row.surface.clone())
                .or_default()
                .insert(row.day.clone());
        }
        let counts = if scope.uses_human_active() {
            row.human_active > 0
        } else {
            row.measured > 0
        };
        if counts {
            active
                .entry(row.surface.clone())
                .or_default()
                .insert(row.day.clone());
        }
    }
    let mut live_by_surface: BTreeMap<String, BTreeSet<String>> =
        BTreeMap::new();
    for (day, surface) in all_live {
        live_by_surface
            .entry(surface.clone())
            .or_default()
            .insert(day.clone());
    }
    let mut gate = BTreeMap::new();
    for surface in SURFACES {
        let calibration: BTreeSet<String> = live_by_surface
            .get(*surface)
            .into_iter()
            .flatten()
            .take(calibration_days as usize)
            .cloned()
            .collect();
        let count = active
            .get(*surface)
            .into_iter()
            .flatten()
            .filter(|day| {
                day.as_str() >= window_start
                    && day.as_str() <= today
                    && !calibration.contains(*day)
            })
            .count() as u32;
        gate.insert((*surface).to_string(), count);
    }
    (measured, active, gate)
}

fn window_start(
    window_days: Option<u32>,
    today: NaiveDate,
    recording_since: Option<&str>,
    conn: &Connection,
) -> Result<NaiveDate, FootprintError> {
    if let Some(days) = window_days {
        return Ok(today
            .checked_sub_signed(ChronoDuration::days(i64::from(
                days.saturating_sub(1),
            )))
            .unwrap_or(today));
    }
    let mut start = recording_since.and_then(|day| parse_day(day).ok());
    if let Some(min_day) = min_data_day(conn)? {
        start = Some(match start {
            Some(existing) => existing.min(min_day),
            None => min_day,
        });
    }
    Ok(start.unwrap_or(today))
}

fn min_data_day(
    conn: &Connection,
) -> Result<Option<NaiveDate>, FootprintError> {
    let daily: Option<String> = conn
        .query_row("SELECT MIN(day) FROM daily", [], |row| row.get(0))
        .optional()
        .map_err(FootprintError::from_sqlite)?;
    let coverage: Option<String> = conn
        .query_row("SELECT MIN(day) FROM coverage", [], |row| row.get(0))
        .optional()
        .map_err(FootprintError::from_sqlite)?;
    Ok(daily
        .into_iter()
        .chain(coverage)
        .filter_map(|day| parse_day(&day).ok())
        .min())
}

fn latest_catalog_day(
    conn: &Connection,
) -> Result<Option<String>, FootprintError> {
    let rev = meta_get(conn, "catalog_rev")?;
    let Some(rev) = rev else {
        return Ok(None);
    };
    conn.query_row(
        "SELECT last_seen_day FROM catalog_revs WHERE rev = ?1",
        params![rev],
        |row| row.get(0),
    )
    .optional()
    .map_err(FootprintError::from_sqlite)
}

fn imported_pack_count(conn: &Connection) -> Result<u64, FootprintError> {
    let exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'import_packs'",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(FootprintError::from_sqlite)?;
    if exists.is_none() {
        return Ok(0);
    }
    conn.query_row("SELECT COUNT(*) FROM import_packs", [], |row| row.get(0))
        .map_err(FootprintError::from_sqlite)
}

fn table_exists(conn: &Connection, name: &str) -> Result<bool, FootprintError> {
    let exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            params![name],
            |row| row.get(0),
        )
        .optional()
        .map_err(FootprintError::from_sqlite)?;
    Ok(exists.is_some())
}

fn load_features(conn: &Connection) -> Result<Vec<FeatureRec>, FootprintError> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT feature_id, ns, area, kind, label, parent, loc,
                   first_in_catalog, last_in_catalog
            FROM features
            "#,
        )
        .map_err(FootprintError::from_sqlite)?;
    let rows = stmt
        .query_map([], |row| {
            Ok(FeatureRec {
                id: row.get(0)?,
                ns: row.get(1)?,
                area: row.get(2)?,
                kind: row.get(3)?,
                label: row.get(4)?,
                parent: row.get(5)?,
                loc: row.get(6)?,
                first_in_catalog: row.get(7)?,
                last_in_catalog: row.get(8)?,
            })
        })
        .map_err(FootprintError::from_sqlite)?;
    let mut features = Vec::new();
    for row in rows {
        features.push(row.map_err(FootprintError::from_sqlite)?);
    }
    Ok(features)
}

fn load_daily(
    conn: &Connection,
    start: &str,
    end: &str,
) -> Result<Vec<DailyRec>, FootprintError> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT day, feature_id, actor, via, outcome, n, dur_ms_sum
            FROM daily
            WHERE day >= ?1 AND day <= ?2
            "#,
        )
        .map_err(FootprintError::from_sqlite)?;
    let rows = stmt
        .query_map(params![start, end], |row| {
            Ok(DailyRec {
                day: row.get(0)?,
                feature_id: row.get(1)?,
                actor: row.get(2)?,
                via: row.get(3)?,
                outcome: row.get(4)?,
                n: row.get(5)?,
                dur_ms_sum: row.get(6)?,
            })
        })
        .map_err(FootprintError::from_sqlite)?;
    let mut daily = Vec::new();
    for row in rows {
        daily.push(row.map_err(FootprintError::from_sqlite)?);
    }
    Ok(daily)
}

fn load_coverage(
    conn: &Connection,
    start: &str,
    end: &str,
) -> Result<Vec<CoverageRec>, FootprintError> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT day, surface, measured, human_active,
                   torn, dropped, unknown_ids, incomplete
            FROM coverage
            WHERE day >= ?1 AND day <= ?2
            "#,
        )
        .map_err(FootprintError::from_sqlite)?;
    let rows = stmt
        .query_map(params![start, end], |row| {
            Ok(CoverageRec {
                day: row.get(0)?,
                surface: row.get(1)?,
                measured: row.get(2)?,
                human_active: row.get(3)?,
                torn: row.get(4)?,
                dropped: row.get(5)?,
                unknown_ids: row.get(6)?,
                incomplete: row.get(7)?,
            })
        })
        .map_err(FootprintError::from_sqlite)?;
    let mut coverage = Vec::new();
    for row in rows {
        coverage.push(row.map_err(FootprintError::from_sqlite)?);
    }
    Ok(coverage)
}

fn load_live_measured(
    conn: &Connection,
) -> Result<Vec<(String, String)>, FootprintError> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT day, surface FROM coverage
            WHERE origin = 'live' AND measured = 1
            ORDER BY day
            "#,
        )
        .map_err(FootprintError::from_sqlite)?;
    let rows = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(FootprintError::from_sqlite)?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(FootprintError::from_sqlite)?);
    }
    Ok(out)
}

fn load_reviews(conn: &Connection) -> Result<Vec<ReviewRec>, FootprintError> {
    if !table_exists(conn, "reviews")? {
        return Ok(Vec::new());
    }
    let mut stmt = conn
        .prepare(
            r#"
            SELECT feature_id, decision, reason, decided_day, snooze_until
            FROM reviews
            "#,
        )
        .map_err(FootprintError::from_sqlite)?;
    let rows = stmt
        .query_map([], |row| {
            Ok(ReviewRec {
                feature_id: row.get(0)?,
                decision: row.get(1)?,
                reason: row.get(2)?,
                decided_day: row.get(3)?,
                snooze_until: row.get(4)?,
            })
        })
        .map_err(FootprintError::from_sqlite)?;
    let mut reviews = Vec::new();
    for row in rows {
        reviews.push(row.map_err(FootprintError::from_sqlite)?);
    }
    Ok(reviews)
}
