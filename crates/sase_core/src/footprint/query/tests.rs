use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{Duration as ChronoDuration, NaiveDate};
use rusqlite::params;
use serde_json::json;
use tempfile::TempDir;

use super::super::store::{open_write_store, FOOTPRINT_BUSY_MAX};
use super::super::wire::QueryRequestWire;
use super::super::{card, decide, report};

const TODAY: &str = "2026-10-27";
const BUSY: Duration = FOOTPRINT_BUSY_MAX;

struct Fixture {
    _temp: TempDir,
    store: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let store = temp.path().join("footprint.sqlite");
        let conn = open_write_store(&store, BUSY).unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES ('catalog_rev', 'abc')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO catalog_revs(rev, sase_version, entry_count, first_seen_day, last_seen_day) VALUES ('abc', '0.18.1', 0, ?1, ?1)",
            params![TODAY],
        )
        .unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES ('first_record_day', '2026-08-01')",
            [],
        )
        .unwrap();
        Self { _temp: temp, store }
    }

    #[allow(clippy::too_many_arguments)]
    fn add_feature(
        &self,
        id: &str,
        area: &str,
        kind: &str,
        label: &str,
        parent: Option<&str>,
        loc: i64,
        first: &str,
        last: &str,
    ) {
        let ns = id.split_once(':').unwrap().0;
        let conn = open_write_store(&self.store, BUSY).unwrap();
        conn.execute(
            r#"
            INSERT INTO features(
                feature_id, ns, area, kind, label, parent, src, loc,
                first_in_catalog, last_in_catalog
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, ?8, ?9)
            "#,
            params![id, ns, area, kind, label, parent, loc, first, last],
        )
        .unwrap();
    }

    fn cover_range(
        &self,
        surface: &str,
        start: &str,
        days: i64,
        human_active: i64,
    ) {
        let start = NaiveDate::parse_from_str(start, "%Y-%m-%d").unwrap();
        let conn = open_write_store(&self.store, BUSY).unwrap();
        for offset in 0..days {
            let day = (start + ChronoDuration::days(offset)).to_string();
            conn.execute(
                r#"
                INSERT INTO coverage(
                    day, surface, origin, measured, human_active, records,
                    torn, dropped, unknown_ids, incomplete
                ) VALUES (?1, ?2, 'live', 1, ?3, 1, 0, 0, 0, 0)
                ON CONFLICT(day, surface, origin) DO UPDATE SET
                    measured = 1,
                    human_active = MAX(human_active, excluded.human_active)
                "#,
                params![day, surface, human_active],
            )
            .unwrap();
        }
    }

    fn coverage_flags(
        &self,
        day: &str,
        surface: &str,
        dropped: i64,
        incomplete: i64,
    ) {
        let conn = open_write_store(&self.store, BUSY).unwrap();
        conn.execute(
            r#"
            INSERT INTO coverage(
                day, surface, origin, measured, human_active, records,
                torn, dropped, unknown_ids, incomplete
            ) VALUES (?1, ?2, 'live', 1, 1, 1, 0, ?3, 0, ?4)
            ON CONFLICT(day, surface, origin) DO UPDATE SET
                dropped = excluded.dropped,
                incomplete = excluded.incomplete
            "#,
            params![day, surface, dropped, incomplete],
        )
        .unwrap();
    }

    #[allow(clippy::too_many_arguments)]
    fn hit(
        &self,
        day: &str,
        id: &str,
        actor: &str,
        via: &str,
        outcome: &str,
        n: i64,
        dur: i64,
    ) {
        let conn = open_write_store(&self.store, BUSY).unwrap();
        conn.execute(
            r#"
            INSERT INTO daily(
                day, feature_id, actor, via, outcome, origin,
                n, dur_ms_sum, dur_ms_max
            ) VALUES (?1, ?2, ?3, ?4, ?5, 'live', ?6, ?7, ?7)
            "#,
            params![day, id, actor, via, outcome, n, dur],
        )
        .unwrap();
    }

    fn request(&self, lens: &str) -> QueryRequestWire {
        self.request_overlay(
            lens,
            json!({"schema": "sase.footprint.overlay/1"}),
        )
    }

    fn request_overlay(
        &self,
        lens: &str,
        overlay: serde_json::Value,
    ) -> QueryRequestWire {
        QueryRequestWire {
            store_path: self.store.to_str().unwrap().to_string(),
            overlay,
            today: TODAY.to_string(),
            window_days: Some(60),
            actor: "you".to_string(),
            lens: lens.to_string(),
            area: None,
            feature_id: None,
            min_catalog_days: 60,
            min_active_days: 30,
            calibration_days: 3,
            fading_days: 30,
            limit: None,
        }
    }

    fn mature_cli(&self) {
        // 33 live measured human-active days so calibration (3) leaves 30.
        self.cover_range("cli", "2026-09-25", 33, 1);
    }
}

fn ids(rows: &[super::super::wire::FeatureRowWire]) -> Vec<&str> {
    rows.iter().map(|row| row.id.as_str()).collect()
}

#[test]
fn gate_boundary_catalog_and_active_days() {
    let fx = Fixture::new();
    fx.mature_cli();
    fx.add_feature(
        "cli:old",
        "cli.test",
        "command",
        "Old",
        None,
        10,
        "2026-08-29",
        TODAY,
    );
    fx.add_feature(
        "cli:catalog-short",
        "cli.test",
        "command",
        "Catalog short",
        None,
        20,
        "2026-08-30",
        TODAY,
    );
    let cold = report(&fx.request("cold"), BUSY).unwrap();
    assert!(ids(&cold.rows).contains(&"cli:old"));
    assert!(!ids(&cold.rows).contains(&"cli:catalog-short"));
    let short = card(
        &QueryRequestWire {
            feature_id: Some("cli:catalog-short".into()),
            ..fx.request("map")
        },
        BUSY,
    )
    .unwrap();
    assert_eq!(short.row.state, "too_new");
    assert_eq!(card_state(&fx, "cli:old"), "no_recorded_use");
}

fn card_state(fx: &Fixture, id: &str) -> String {
    let mut request = fx.request("map");
    request.feature_id = Some(id.to_string());
    card(&request, BUSY).unwrap().row.state
}

#[test]
fn gate_boundary_active_days_29_versus_30() {
    let fx = Fixture::new();
    // 32 live days -> 29 after calibration.
    fx.cover_range("cli", "2026-09-26", 32, 1);
    fx.add_feature(
        "cli:almost",
        "cli.test",
        "command",
        "Almost",
        None,
        8,
        "2026-08-29",
        TODAY,
    );
    assert_eq!(card_state(&fx, "cli:almost"), "too_new");

    let fx = Fixture::new();
    fx.mature_cli();
    fx.add_feature(
        "cli:ready",
        "cli.test",
        "command",
        "Ready",
        None,
        8,
        "2026-08-29",
        TODAY,
    );
    assert_eq!(card_state(&fx, "cli:ready"), "no_recorded_use");
}

#[test]
fn calibration_days_are_excluded_from_the_gate() {
    let fx = Fixture::new();
    // 30 measured days with calibration=3 would be 27 active, too new.
    fx.cover_range("cli", "2026-09-28", 30, 1);
    fx.add_feature(
        "cli:cal",
        "cli.test",
        "command",
        "Cal",
        None,
        4,
        "2026-08-29",
        TODAY,
    );
    assert_eq!(card_state(&fx, "cli:cal"), "too_new");
}

#[test]
fn protected_glob_is_rare_by_design() {
    let fx = Fixture::new();
    fx.mature_cli();
    fx.add_feature(
        "cli:doctor",
        "cli.doctor",
        "command",
        "Doctor",
        None,
        100,
        "2026-08-29",
        TODAY,
    );
    fx.add_feature(
        "cli:doctor.fix",
        "cli.doctor",
        "command",
        "Fix",
        None,
        40,
        "2026-08-29",
        TODAY,
    );
    let overlay = json!({
        "schema": "sase.footprint.overlay/1",
        "features": {
            "cli:doctor*": {"protected": "recovery", "why": "repair"}
        }
    });
    let mut request = fx.request_overlay("cold", overlay.clone());
    let cold = report(&request, BUSY).unwrap();
    assert!(cold.rows.is_empty());
    request.feature_id = Some("cli:doctor".into());
    request.overlay = overlay;
    let doctor = card(&request, BUSY).unwrap();
    assert_eq!(doctor.row.state, "rare_by_design");
    assert_eq!(doctor.row.protected.as_deref(), Some("recovery"));
}

#[test]
fn snooze_expiry_returns_a_feature_to_cold() {
    let fx = Fixture::new();
    fx.mature_cli();
    fx.add_feature(
        "cli:later",
        "cli.test",
        "command",
        "Later",
        None,
        9,
        "2026-08-29",
        TODAY,
    );
    decide(
        Path::new(&fx.store),
        "cli:later",
        "later",
        None,
        "2026-09-01",
        30,
        BUSY,
    )
    .unwrap();
    let cold = report(&fx.request("cold"), BUSY).unwrap();
    assert!(ids(&cold.rows).contains(&"cli:later"));
    decide(
        Path::new(&fx.store),
        "cli:later",
        "later",
        None,
        TODAY,
        30,
        BUSY,
    )
    .unwrap();
    let cold = report(&fx.request("cold"), BUSY).unwrap();
    assert!(!ids(&cold.rows).contains(&"cli:later"));
    let decisions = report(&fx.request("decisions"), BUSY).unwrap();
    assert!(ids(&decisions.rows).contains(&"cli:later"));
}

#[test]
fn not_measured_versus_no_recorded_use() {
    let fx = Fixture::new();
    fx.mature_cli();
    fx.add_feature(
        "cli:unused",
        "cli.test",
        "command",
        "Unused",
        None,
        3,
        "2026-08-29",
        TODAY,
    );
    fx.add_feature(
        "tui:app.never",
        "tui.app",
        "action",
        "Never",
        None,
        3,
        "2026-08-29",
        TODAY,
    );
    assert_eq!(card_state(&fx, "cli:unused"), "no_recorded_use");
    assert_eq!(card_state(&fx, "tui:app.never"), "not_measured");
}

#[test]
fn coverage_incomplete_flag_from_dropped_or_incomplete_days() {
    let fx = Fixture::new();
    fx.mature_cli();
    fx.coverage_flags("2026-10-20", "cli", 4, 0);
    fx.add_feature(
        "cli:gap",
        "cli.test",
        "command",
        "Gap",
        None,
        2,
        "2026-08-29",
        TODAY,
    );
    let gap = {
        let mut request = fx.request("map");
        request.feature_id = Some("cli:gap".into());
        card(&request, BUSY).unwrap()
    };
    assert!(gap.row.coverage_incomplete);
}

#[test]
fn dead_paths_palette_only_and_unused_option() {
    let fx = Fixture::new();
    fx.mature_cli();
    fx.cover_range("tui", "2026-09-25", 33, 1);
    fx.add_feature(
        "tui:app.zoom",
        "tui.app",
        "action",
        "Zoom",
        None,
        12,
        "2026-08-29",
        TODAY,
    );
    fx.add_feature(
        "cli:bead.list",
        "cli.bead",
        "command",
        "List",
        None,
        40,
        "2026-08-29",
        TODAY,
    );
    fx.add_feature(
        "cli:bead.list#json",
        "cli.bead",
        "option",
        "json",
        Some("cli:bead.list"),
        4,
        "2026-08-29",
        TODAY,
    );
    fx.hit("2026-10-20", "tui:app.zoom", "human", "palette", "ok", 2, 0);
    fx.hit("2026-10-20", "cli:bead.list", "human", "cli", "ok", 3, 10);
    let dead = report(&fx.request("dead-paths"), BUSY).unwrap();
    assert!(ids(&dead.rows).contains(&"tui:app.zoom"));
    assert!(ids(&dead.rows).contains(&"cli:bead.list#json"));
}

#[test]
fn unioned_days_across_actors_count_once_for_everyone() {
    let fx = Fixture::new();
    fx.mature_cli();
    fx.add_feature(
        "cli:shared",
        "cli.test",
        "command",
        "Shared",
        None,
        5,
        "2026-08-29",
        TODAY,
    );
    fx.hit("2026-10-20", "cli:shared", "human", "cli", "ok", 2, 5);
    fx.hit("2026-10-20", "cli:shared", "agent", "cli", "ok", 9, 8);
    fx.hit("2026-10-21", "cli:shared", "agent", "cli", "ok", 1, 1);
    let mut request = fx.request("hot");
    request.actor = "everyone".into();
    let hot = report(&request, BUSY).unwrap();
    let row = hot.rows.iter().find(|row| row.id == "cli:shared").unwrap();
    assert_eq!(row.days_used, 2);
    assert_eq!(row.uses.human, 2);
    assert_eq!(row.uses.agent, 10);
}

#[test]
fn lenses_hot_fading_friction_agent_only_and_map() {
    let fx = Fixture::new();
    fx.mature_cli();
    fx.cover_range("tui", "2026-09-25", 33, 1);
    fx.add_feature(
        "cli:hot",
        "cli.test",
        "command",
        "Hot",
        None,
        50,
        "2026-08-29",
        TODAY,
    );
    fx.add_feature(
        "cli:fade",
        "cli.test",
        "command",
        "Fade",
        None,
        6,
        "2026-08-29",
        TODAY,
    );
    fx.add_feature(
        "cli:friction",
        "cli.test",
        "command",
        "Friction",
        None,
        7,
        "2026-08-29",
        TODAY,
    );
    fx.add_feature(
        "cli:agents",
        "cli.test",
        "command",
        "Agents",
        None,
        8,
        "2026-08-29",
        TODAY,
    );
    fx.add_feature(
        "cli:unused",
        "cli.test",
        "command",
        "Unused",
        None,
        9,
        "2026-08-29",
        TODAY,
    );
    fx.hit("2026-10-24", "cli:hot", "human", "cli", "ok", 1, 10);
    fx.hit("2026-10-25", "cli:hot", "human", "cli", "ok", 2, 20);
    fx.hit("2026-10-26", "cli:hot", "human", "cli", "ok", 4, 40);
    fx.hit("2026-09-20", "cli:fade", "human", "cli", "ok", 1, 4);
    fx.hit(
        "2026-10-10",
        "cli:friction",
        "human",
        "cli",
        "usage_error",
        3,
        0,
    );
    fx.hit("2026-10-11", "cli:friction", "human", "cli", "help", 1, 0);
    fx.hit("2026-10-12", "cli:agents", "agent", "cli", "ok", 5, 0);
    let hot = report(&fx.request("hot"), BUSY).unwrap();
    assert_eq!(hot.rows[0].id, "cli:hot");
    let fading = report(&fx.request("fading"), BUSY).unwrap();
    assert!(ids(&fading.rows).contains(&"cli:fade"));
    assert!(!ids(&fading.rows).contains(&"cli:hot"));
    let friction = report(&fx.request("friction"), BUSY).unwrap();
    assert!(ids(&friction.rows).contains(&"cli:friction"));
    let agents = report(&fx.request("agent-only"), BUSY).unwrap();
    assert!(ids(&agents.rows).contains(&"cli:agents"));
    let map = report(&fx.request("map"), BUSY).unwrap();
    assert!(map.areas.iter().any(|area| area.area == "cli.test"));
    assert!(map.rows.is_empty());
    assert!(map.header.gate_met);
    assert!(map.tiles.candidates > 0);
    let coverage = report(&fx.request("coverage"), BUSY).unwrap();
    assert_eq!(coverage.coverage[0].surface, "cli");
    assert!(coverage.coverage[0].measured_days >= 30);
}

#[test]
fn decide_keep_and_clear_round_trip() {
    let fx = Fixture::new();
    fx.mature_cli();
    fx.add_feature(
        "cli:keep",
        "cli.test",
        "command",
        "Keep",
        None,
        11,
        "2026-08-29",
        TODAY,
    );
    let kept = decide(
        Path::new(&fx.store),
        "cli:keep",
        "keep",
        Some("agent_facing"),
        TODAY,
        30,
        BUSY,
    )
    .unwrap();
    assert_eq!(kept.decision.as_deref(), Some("keep"));
    let cold = report(&fx.request("cold"), BUSY).unwrap();
    assert!(!ids(&cold.rows).contains(&"cli:keep"));
    decide(
        Path::new(&fx.store),
        "cli:keep",
        "clear",
        None,
        TODAY,
        30,
        BUSY,
    )
    .unwrap();
    let cold = report(&fx.request("cold"), BUSY).unwrap();
    assert!(ids(&cold.rows).contains(&"cli:keep"));
}

#[test]
fn card_includes_children_series_and_retire_with() {
    let fx = Fixture::new();
    fx.mature_cli();
    fx.add_feature(
        "cli:bead.list",
        "cli.bead",
        "command",
        "List",
        None,
        40,
        "2026-08-29",
        TODAY,
    );
    fx.add_feature(
        "cli:bead.list#json",
        "cli.bead",
        "option",
        "json",
        Some("cli:bead.list"),
        4,
        "2026-08-29",
        TODAY,
    );
    fx.add_feature(
        "tui:app.bead_list",
        "tui.app",
        "action",
        "Beads",
        None,
        20,
        "2026-08-29",
        TODAY,
    );
    fx.hit("2026-10-20", "cli:bead.list", "human", "cli", "ok", 2, 9);
    let overlay = json!({
        "schema": "sase.footprint.overlay/1",
        "retire_with": [["cli:bead.list", "tui:app.bead_list"]]
    });
    let mut request = fx.request_overlay("map", overlay);
    request.feature_id = Some("cli:bead.list".into());
    let evidence = card(&request, BUSY).unwrap();
    assert_eq!(evidence.children, vec!["cli:bead.list#json".to_string()]);
    assert_eq!(evidence.retire_with, vec!["tui:app.bead_list".to_string()]);
    assert_eq!(evidence.row.state, "used");
    assert!(evidence.daily_series.iter().any(|point| point.n == 2));
    assert!(evidence.area_summary.is_some());
}

#[test]
fn used_feature_is_not_a_cold_candidate() {
    let fx = Fixture::new();
    fx.mature_cli();
    fx.add_feature(
        "cli:used",
        "cli.test",
        "command",
        "Used",
        None,
        3,
        "2026-08-29",
        TODAY,
    );
    fx.hit("2026-10-01", "cli:used", "human", "cli", "ok", 1, 1);
    assert_eq!(card_state(&fx, "cli:used"), "used");
    let cold = report(&fx.request("cold"), BUSY).unwrap();
    assert!(!ids(&cold.rows).contains(&"cli:used"));
}

#[test]
fn missing_store_returns_an_empty_report() {
    let temp = TempDir::new().unwrap();
    let store = temp.path().join("missing.sqlite");
    let request = QueryRequestWire {
        store_path: store.to_str().unwrap().to_string(),
        overlay: json!({"schema": "sase.footprint.overlay/1"}),
        today: TODAY.to_string(),
        window_days: Some(60),
        actor: "you".to_string(),
        lens: "map".to_string(),
        area: None,
        feature_id: None,
        min_catalog_days: 60,
        min_active_days: 30,
        calibration_days: 3,
        fading_days: 30,
        limit: None,
    };
    let empty = report(&request, BUSY).unwrap();
    assert_eq!(empty.tiles.total, 0);
    assert_eq!(empty.header.generation, 0);
    assert_eq!(empty.header.imported_pack_count, 0);
    assert!(!store.exists());
}

#[test]
fn overlay_keep_counts_as_decided() {
    let fx = Fixture::new();
    fx.mature_cli();
    fx.add_feature(
        "cli:json",
        "cli.test",
        "option",
        "json",
        None,
        4,
        "2026-08-29",
        TODAY,
    );
    let overlay = json!({
        "schema": "sase.footprint.overlay/1",
        "features": {
            "cli:json": {
                "keep": "agent_facing",
                "why": "skills parse this"
            }
        }
    });
    let cold =
        report(&fx.request_overlay("cold", overlay.clone()), BUSY).unwrap();
    assert!(!ids(&cold.rows).contains(&"cli:json"));
    let mut request = fx.request_overlay("map", overlay);
    request.feature_id = Some("cli:json".into());
    let evidence = card(&request, BUSY).unwrap();
    assert_eq!(evidence.row.decision.as_deref(), Some("keep"));
    assert_eq!(evidence.row.state, "no_recorded_use");
}
