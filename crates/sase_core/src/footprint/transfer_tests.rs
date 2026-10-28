use std::collections::BTreeMap;
use std::time::Duration;

use rusqlite::params;
use tempfile::TempDir;

use super::export::{bucket_n, export};
use super::import::import;
use super::query::{card, report};
use super::store::{open_write_store, FOOTPRINT_BUSY_MAX};
use super::wire::{
    ExportRequestWire, PackFeatureWire, PackWire, QueryRequestWire,
    EXPORT_SCHEMA,
};

const TODAY: &str = "2026-10-27";
const BUSY: Duration = FOOTPRINT_BUSY_MAX;
const W43: &str = "2026-W43";

struct Fixture {
    _temp: TempDir,
    store: std::path::PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let store = temp.path().join("footprint.sqlite");
        let conn = open_write_store(&store, BUSY).unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES ('first_record_day', '2026-10-19')",
            [],
        )
        .unwrap();
        Self { _temp: temp, store }
    }

    fn add_feature(&self, id: &str, kind: &str) {
        let ns = id.split_once(':').unwrap().0;
        let conn = open_write_store(&self.store, BUSY).unwrap();
        conn.execute(
            r#"
            INSERT OR REPLACE INTO features(
                feature_id, ns, area, kind, label, parent, src, loc,
                first_in_catalog, last_in_catalog
            ) VALUES (?1, ?2, ?3, ?4, ?1, NULL, NULL, 10, '2026-08-01', ?5)
            "#,
            params![id, ns, format!("{ns}.test"), kind, TODAY],
        )
        .unwrap();
    }

    fn hit(&self, day: &str, id: &str, actor: &str, n: i64) {
        let conn = open_write_store(&self.store, BUSY).unwrap();
        conn.execute(
            r#"
            INSERT INTO daily(
                day, feature_id, actor, via, outcome, origin,
                n, dur_ms_sum, dur_ms_max
            ) VALUES (?1, ?2, ?3, 'cli', 'ok', 'live', ?4, 0, 0)
            "#,
            params![day, id, actor, n],
        )
        .unwrap();
    }

    fn cover(&self, day: &str, surface: &str) {
        let conn = open_write_store(&self.store, BUSY).unwrap();
        conn.execute(
            r#"
            INSERT INTO coverage(
                day, surface, origin, measured, human_active, records,
                torn, dropped, unknown_ids, incomplete
            ) VALUES (?1, ?2, 'live', 1, 1, 1, 0, 0, 0, 0)
            ON CONFLICT(day, surface, origin) DO UPDATE SET measured = 1
            "#,
            params![day, surface],
        )
        .unwrap();
    }

    fn export_weeks(&self, weeks: Option<u32>) -> Vec<PackWire> {
        export(
            &ExportRequestWire {
                store_path: self.store.to_str().unwrap().to_string(),
                today: TODAY.to_string(),
                weeks,
                sase: "0.18.1".to_string(),
                os: "linux".to_string(),
            },
            BUSY,
        )
        .unwrap()
    }

    fn request(&self, lens: &str) -> QueryRequestWire {
        QueryRequestWire {
            store_path: self.store.to_str().unwrap().to_string(),
            overlay: serde_json::json!({"schema": "sase.footprint.overlay/1"}),
            today: TODAY.to_string(),
            window_days: Some(60),
            actor: "you".to_string(),
            lens: lens.to_string(),
            area: None,
            feature_id: None,
            min_catalog_days: 1,
            min_active_days: 1,
            calibration_days: 0,
            fading_days: 30,
            limit: None,
        }
    }
}

fn pack(
    nonce: &str,
    week: &str,
    observed_cli: u32,
    features: Vec<PackFeatureWire>,
) -> PackWire {
    let mut observed = BTreeMap::new();
    for surface in ["cli", "tui", "macro", "skill", "config", "auto", "plugin"]
    {
        observed.insert(
            surface.to_string(),
            if surface == "cli" { observed_cli } else { 0 },
        );
    }
    let not_measured = observed
        .iter()
        .filter(|(_, days)| **days == 0)
        .map(|(surface, _)| surface.clone())
        .collect();
    PackWire {
        schema: EXPORT_SCHEMA.to_string(),
        nonce: nonce.to_string(),
        week: week.to_string(),
        sase: "0.18".to_string(),
        os: "linux".to_string(),
        observed,
        features,
        not_measured,
    }
}

#[test]
fn bucket_boundaries() {
    assert_eq!(bucket_n(0), None);
    assert_eq!(bucket_n(1), Some("1"));
    assert_eq!(bucket_n(2), Some("2-5"));
    assert_eq!(bucket_n(5), Some("2-5"));
    assert_eq!(bucket_n(6), Some("6-20"));
    assert_eq!(bucket_n(20), Some("6-20"));
    assert_eq!(bucket_n(21), Some("21+"));
}

#[test]
fn export_skips_forbidden_ids_and_incomplete_weeks() {
    let fx = Fixture::new();
    fx.add_feature("cli:bead.list", "command");
    fx.add_feature("cli:bead.list#json", "option");
    fx.add_feature("config:ace.page_size", "config");
    fx.add_feature("plugin:listen", "plugin");
    fx.add_feature("macro:~custom", "macro");
    for day in ["2026-10-19", "2026-10-20", "2026-10-21"] {
        fx.hit(day, "cli:bead.list", "human", 1);
        fx.hit(day, "cli:bead.list#json", "human", 1);
        fx.hit(day, "config:ace.page_size", "human", 1);
        fx.hit(day, "plugin:listen", "human", 1);
        fx.hit(day, "macro:~custom", "human", 1);
        fx.cover(day, "cli");
    }
    fx.hit("2026-10-26", "cli:bead.list", "human", 9);
    fx.cover("2026-10-26", "cli");
    let packs = fx.export_weeks(None);
    assert_eq!(packs.len(), 1);
    assert_eq!(packs[0].week, W43);
    assert_eq!(packs[0].sase, "0.18");
    assert_eq!(packs[0].os, "linux");
    assert_eq!(packs[0].schema, EXPORT_SCHEMA);
    assert!(packs[0].nonce.starts_with("r_"));
    assert_eq!(packs[0].features.len(), 1);
    assert_eq!(packs[0].features[0].id, "cli:bead.list");
    assert_eq!(packs[0].features[0].actor, "human");
    assert_eq!(packs[0].features[0].days, 3);
    assert_eq!(packs[0].features[0].n, "2-5");
    assert_eq!(packs[0].observed.get("cli").copied().unwrap(), 3);
    assert!(packs[0].not_measured.contains(&"tui".to_string()));
}

#[test]
fn export_buckets_summed_counts() {
    let fx = Fixture::new();
    fx.add_feature("cli:bead.list", "command");
    fx.hit("2026-10-19", "cli:bead.list", "human", 21);
    fx.cover("2026-10-19", "cli");
    let packs = fx.export_weeks(Some(1));
    assert_eq!(packs[0].features[0].n, "21+");
    assert_eq!(packs[0].features[0].days, 1);
}

#[test]
fn import_is_replace_all_and_dedupes_nonce() {
    let fx = Fixture::new();
    let first = pack(
        "r_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        W43,
        4,
        vec![PackFeatureWire {
            id: "cli:bead.list".into(),
            actor: "human".into(),
            days: 4,
            n: "2-5".into(),
        }],
    );
    let duplicate = first.clone();
    let second = pack(
        "r_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        W43,
        2,
        vec![PackFeatureWire {
            id: "cli:bead.show".into(),
            actor: "human".into(),
            days: 2,
            n: "1".into(),
        }],
    );
    let once =
        import(&fx.store, std::slice::from_ref(&first), TODAY, false, BUSY)
            .unwrap();
    assert_eq!(once.packs_imported, 1);
    let deduped =
        import(&fx.store, &[first, duplicate], TODAY, false, BUSY).unwrap();
    assert_eq!(deduped.packs_imported, 1);
    assert_eq!(deduped.packs_deduped, 1);
    let replaced = import(&fx.store, &[second], TODAY, false, BUSY).unwrap();
    assert_eq!(replaced.packs_imported, 1);
    let conn = open_write_store(&fx.store, BUSY).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM import_packs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);
    let id: String = conn
        .query_row("SELECT feature_id FROM imported_weekly", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(id, "cli:bead.show");
}

#[test]
fn import_dry_run_writes_nothing() {
    let fx = Fixture::new();
    let sample = pack("r_cccccccccccccccccccccccccccccccc", W43, 1, vec![]);
    let result = import(&fx.store, &[sample], TODAY, true, BUSY).unwrap();
    assert!(result.dry_run);
    let conn = open_write_store(&fx.store, BUSY).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM import_packs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn merge_usage_is_upper_bound_and_observed_is_lower_bound() {
    let fx = Fixture::new();
    fx.add_feature("cli:bead.list", "command");
    fx.add_feature("cli:bead.show", "command");
    for day in ["2026-10-19", "2026-10-20", "2026-10-21"] {
        fx.hit(day, "cli:bead.list", "human", 1);
        fx.cover(day, "cli");
    }
    let incoming = pack(
        "r_dddddddddddddddddddddddddddddddd",
        W43,
        6,
        vec![PackFeatureWire {
            id: "cli:bead.list".into(),
            actor: "human".into(),
            days: 4,
            n: "2-5".into(),
        }],
    );
    import(&fx.store, &[incoming], TODAY, false, BUSY).unwrap();
    let hot = report(&fx.request("hot"), BUSY).unwrap();
    let row = hot
        .rows
        .iter()
        .find(|row| row.id == "cli:bead.list")
        .unwrap();
    assert_eq!(row.days_used, 7);
    assert_eq!(hot.header.imported_pack_count, 1);
    assert_eq!(hot.header.imported_day.as_deref(), Some(TODAY));
    assert_eq!(hot.header.observed_active_days.get("cli").copied(), Some(6));
    let coverage = report(&fx.request("coverage"), BUSY).unwrap();
    let cli = coverage
        .coverage
        .iter()
        .find(|row| row.surface == "cli")
        .unwrap();
    assert_eq!(cli.active_days, 3);
}

#[test]
fn dead_paths_stay_local() {
    let fx = Fixture::new();
    fx.add_feature("tui:app.zoom_panel", "action");
    fx.hit("2026-10-19", "tui:app.zoom_panel", "human", 1);
    fx.cover("2026-10-19", "tui");
    let conn = open_write_store(&fx.store, BUSY).unwrap();
    conn.execute(
        r#"
        UPDATE daily SET via = 'palette' WHERE feature_id = 'tui:app.zoom_panel'
        "#,
        [],
    )
    .unwrap();
    let incoming = pack(
        "r_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        W43,
        4,
        vec![PackFeatureWire {
            id: "tui:app.zoom_panel".into(),
            actor: "human".into(),
            days: 4,
            n: "2-5".into(),
        }],
    );
    import(&fx.store, &[incoming], TODAY, false, BUSY).unwrap();
    let dead = report(&fx.request("dead-paths"), BUSY).unwrap();
    let row = dead
        .rows
        .iter()
        .find(|row| row.id == "tui:app.zoom_panel")
        .unwrap();
    assert_eq!(row.days_used, 1);
    let evidence = card(
        &QueryRequestWire {
            feature_id: Some("tui:app.zoom_panel".into()),
            ..fx.request("map")
        },
        BUSY,
    )
    .unwrap();
    assert_eq!(evidence.row.days_used, 5);
}

#[test]
fn export_excludes_imported_rows() {
    let fx = Fixture::new();
    fx.add_feature("cli:bead.list", "command");
    fx.hit("2026-10-19", "cli:bead.list", "human", 1);
    fx.cover("2026-10-19", "cli");
    let incoming = pack(
        "r_ffffffffffffffffffffffffffffffff",
        W43,
        5,
        vec![PackFeatureWire {
            id: "cli:bead.show".into(),
            actor: "human".into(),
            days: 5,
            n: "6-20".into(),
        }],
    );
    import(&fx.store, &[incoming], TODAY, false, BUSY).unwrap();
    let packs = fx.export_weeks(None);
    assert_eq!(packs[0].features.len(), 1);
    assert_eq!(packs[0].features[0].id, "cli:bead.list");
    assert_eq!(packs[0].observed.get("cli").copied().unwrap(), 1);
}
