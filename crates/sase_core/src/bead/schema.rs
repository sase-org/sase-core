//! SQLite schema and migration fragments for bead stores.

use std::collections::BTreeSet;

pub const BEAD_SQLITE_SCHEMA: &str = r#"CREATE TABLE IF NOT EXISTS issues (
    id          TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'open'
                  CHECK(status IN ('open', 'in_progress', 'closed')),
    issue_type  TEXT NOT NULL DEFAULT 'phase'
                  CHECK(issue_type IN ('plan', 'phase')),
    tier        TEXT
                  CHECK(tier IN ('plan', 'epic')),
    parent_id   TEXT
                  REFERENCES issues(id) ON DELETE CASCADE,
    owner       TEXT,
    assignee    TEXT,
    created_at  TEXT NOT NULL,
    created_by  TEXT,
    updated_at  TEXT NOT NULL,
    closed_at   TEXT,
    close_reason TEXT,
    description TEXT,
    notes       TEXT,
    design      TEXT,
    model       TEXT NOT NULL DEFAULT '',
    size        TEXT
                  CHECK(
                    size IS NULL OR
                    (issue_type = 'phase' AND
                     size IN ('xsmall', 'small', 'medium', 'large', 'xlarge'))
                  ),
    is_ready_to_work INTEGER NOT NULL DEFAULT 0,
    changespec_name TEXT NOT NULL DEFAULT '',
    changespec_bug_id TEXT NOT NULL DEFAULT '',
    CHECK(
        (issue_type = 'phase' AND parent_id IS NOT NULL) OR
        (issue_type = 'plan')
    ),
    CHECK(issue_type = 'plan' OR tier IS NULL),
    CHECK(is_ready_to_work IN (0, 1)),
    CHECK(
        issue_type = 'plan' OR
        (changespec_name = '' AND changespec_bug_id = '')
    ),
    CHECK(changespec_name != '' OR changespec_bug_id = '')
);

CREATE TABLE IF NOT EXISTS dependencies (
    issue_id       TEXT NOT NULL,
    depends_on_id  TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    created_by     TEXT,
    PRIMARY KEY (issue_id, depends_on_id),
    FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE,
    FOREIGN KEY (depends_on_id) REFERENCES issues(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_issues_status ON issues(status);
CREATE INDEX IF NOT EXISTS idx_issues_type ON issues(issue_type);
CREATE INDEX IF NOT EXISTS idx_issues_tier ON issues(tier);
CREATE INDEX IF NOT EXISTS idx_issues_parent ON issues(parent_id);
CREATE INDEX IF NOT EXISTS idx_deps_depends_on ON dependencies(depends_on_id);
"#;

pub fn needs_issue_type_migration(create_table_sql: Option<&str>) -> bool {
    match create_table_sql {
        None => false,
        Some(sql) => !sql.contains("'plan'"),
    }
}

pub fn issue_type_migration_sql() -> &'static str {
    r#"PRAGMA foreign_keys=OFF;
CREATE TABLE _issues_new (
  id TEXT PRIMARY KEY, title TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open'
    CHECK(status IN ('open','in_progress','closed')),
  issue_type TEXT NOT NULL DEFAULT 'phase'
    CHECK(issue_type IN ('plan','phase')),
  tier TEXT CHECK(tier IN ('plan','epic')),
  parent_id TEXT, owner TEXT, assignee TEXT,
  created_at TEXT NOT NULL, created_by TEXT,
  updated_at TEXT NOT NULL, closed_at TEXT,
  close_reason TEXT, description TEXT, notes TEXT, design TEXT,
  model TEXT NOT NULL DEFAULT '',
  CHECK((issue_type='phase' AND parent_id IS NOT NULL)
    OR (issue_type='plan')),
  CHECK(issue_type='plan' OR tier IS NULL)
);
INSERT INTO _issues_new
SELECT id, title, status,
  CASE issue_type
    WHEN 'epic' THEN 'plan' WHEN 'child' THEN 'phase'
    ELSE issue_type END,
  CASE issue_type
    WHEN 'epic' THEN 'epic'
    WHEN 'plan' THEN 'epic'
    ELSE NULL END,
  parent_id, owner, assignee, created_at, created_by,
  updated_at, closed_at, close_reason, description, notes, design, ''
FROM issues;
DROP TABLE issues;
ALTER TABLE _issues_new RENAME TO issues;
CREATE INDEX IF NOT EXISTS idx_issues_status ON issues(status);
CREATE INDEX IF NOT EXISTS idx_issues_type ON issues(issue_type);
CREATE INDEX IF NOT EXISTS idx_issues_tier ON issues(tier);
CREATE INDEX IF NOT EXISTS idx_issues_parent ON issues(parent_id);
PRAGMA foreign_keys=ON;"#
}

pub fn needs_is_ready_to_work_migration(
    create_table_sql: Option<&str>,
) -> bool {
    match create_table_sql {
        None => false,
        Some(sql) => !sql.contains("is_ready_to_work"),
    }
}

pub fn is_ready_to_work_migration_sql() -> &'static str {
    "ALTER TABLE issues ADD COLUMN is_ready_to_work INTEGER NOT NULL DEFAULT 0"
}

pub fn needs_model_migration(create_table_sql: Option<&str>) -> bool {
    match create_table_sql {
        None => false,
        Some(sql) => !sql.contains("model"),
    }
}

pub fn model_migration_sql() -> &'static str {
    "ALTER TABLE issues ADD COLUMN model TEXT NOT NULL DEFAULT ''"
}

pub fn needs_size_migration(create_table_sql: Option<&str>) -> bool {
    match create_table_sql {
        None => false,
        Some(sql) => !sql.contains("size"),
    }
}

pub fn size_migration_sql() -> &'static str {
    "ALTER TABLE issues ADD COLUMN size TEXT CHECK(size IS NULL OR (issue_type='phase' AND size IN ('xsmall','small','medium','large','xlarge')))"
}

pub fn needs_size_check_relax_migration(
    create_table_sql: Option<&str>,
) -> bool {
    match create_table_sql {
        None => false,
        Some(sql) => {
            sql.contains("size")
                && sql.contains("'large'")
                && !sql.contains("'xlarge'")
        }
    }
}

pub fn size_check_relax_migration_sql() -> &'static str {
    r#"PRAGMA foreign_keys=OFF;
DROP TABLE IF EXISTS _issues_new;
CREATE TABLE _issues_new (
    id          TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'open'
                  CHECK(status IN ('open', 'in_progress', 'closed')),
    issue_type  TEXT NOT NULL DEFAULT 'phase'
                  CHECK(issue_type IN ('plan', 'phase')),
    tier        TEXT
                  CHECK(tier IN ('plan', 'epic')),
    parent_id   TEXT
                  REFERENCES issues(id) ON DELETE CASCADE,
    owner       TEXT,
    assignee    TEXT,
    created_at  TEXT NOT NULL,
    created_by  TEXT,
    updated_at  TEXT NOT NULL,
    closed_at   TEXT,
    close_reason TEXT,
    description TEXT,
    notes       TEXT,
    design      TEXT,
    model       TEXT NOT NULL DEFAULT '',
    size        TEXT
                  CHECK(
                    size IS NULL OR
                    (issue_type = 'phase' AND
                     size IN ('xsmall', 'small', 'medium', 'large', 'xlarge'))
                  ),
    is_ready_to_work INTEGER NOT NULL DEFAULT 0,
    changespec_name TEXT NOT NULL DEFAULT '',
    changespec_bug_id TEXT NOT NULL DEFAULT '',
    CHECK(
        (issue_type = 'phase' AND parent_id IS NOT NULL) OR
        (issue_type = 'plan')
    ),
    CHECK(issue_type = 'plan' OR tier IS NULL),
    CHECK(is_ready_to_work IN (0, 1)),
    CHECK(
        issue_type = 'plan' OR
        (changespec_name = '' AND changespec_bug_id = '')
    ),
    CHECK(changespec_name != '' OR changespec_bug_id = '')
);
INSERT INTO _issues_new (
    id, title, status, issue_type, tier, parent_id, owner, assignee,
    created_at, created_by, updated_at, closed_at, close_reason,
    description, notes, design, model, size, is_ready_to_work,
    changespec_name, changespec_bug_id
)
SELECT
    id, title, status, issue_type, tier, parent_id, owner, assignee,
    created_at, created_by, updated_at, closed_at, close_reason,
    description, notes, design, model, size, is_ready_to_work,
    changespec_name, changespec_bug_id
FROM issues;
DROP TABLE issues;
ALTER TABLE _issues_new RENAME TO issues;
CREATE INDEX IF NOT EXISTS idx_issues_status ON issues(status);
CREATE INDEX IF NOT EXISTS idx_issues_type ON issues(issue_type);
CREATE INDEX IF NOT EXISTS idx_issues_tier ON issues(tier);
CREATE INDEX IF NOT EXISTS idx_issues_parent ON issues(parent_id);
PRAGMA foreign_keys=ON;"#
}

pub fn missing_changespec_metadata_columns<'a, I>(
    columns: I,
) -> Vec<&'static str>
where
    I: IntoIterator<Item = &'a str>,
{
    let columns: BTreeSet<&str> = columns.into_iter().collect();
    let mut missing = Vec::new();
    if !columns.contains("changespec_name") {
        missing.push("changespec_name");
    }
    if !columns.contains("changespec_bug_id") {
        missing.push("changespec_bug_id");
    }
    missing
}

pub fn changespec_metadata_migration_sql(
    columns: &[&str],
) -> Vec<&'static str> {
    let missing = missing_changespec_metadata_columns(columns.iter().copied());
    missing
        .into_iter()
        .map(|column| match column {
            "changespec_name" => {
                "ALTER TABLE issues ADD COLUMN changespec_name TEXT NOT NULL DEFAULT ''"
            }
            "changespec_bug_id" => {
                "ALTER TABLE issues ADD COLUMN changespec_bug_id TEXT NOT NULL DEFAULT ''"
            }
            _ => unreachable!("unknown changespec metadata column"),
        })
        .collect()
}

pub fn needs_tier_migration(create_table_sql: Option<&str>) -> bool {
    match create_table_sql {
        None => false,
        Some(sql) => !sql.contains("tier"),
    }
}

pub fn tier_migration_sql() -> &'static str {
    "ALTER TABLE issues ADD COLUMN tier TEXT CHECK(tier IN ('plan','epic'))"
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn insert_plan_and_phase(
        conn: &Connection,
        phase_id: &str,
        size: &str,
    ) -> rusqlite::Result<()> {
        conn.execute(
            "INSERT OR IGNORE INTO issues (
                id, title, status, issue_type, tier, created_at, updated_at
             ) VALUES ('plan-1', 'Plan', 'open', 'plan', 'epic', 'now', 'now')",
            [],
        )?;
        conn.execute(
            "INSERT INTO issues (
                id, title, status, issue_type, parent_id,
                created_at, updated_at, size
             ) VALUES (?1, 'Phase', 'open', 'phase', 'plan-1', 'now', 'now', ?2)",
            [phase_id, size],
        )?;
        Ok(())
    }

    #[test]
    fn schema_contains_current_constraints() {
        assert!(BEAD_SQLITE_SCHEMA.contains("CHECK(status IN"));
        assert!(BEAD_SQLITE_SCHEMA.contains("is_ready_to_work INTEGER"));
        assert!(BEAD_SQLITE_SCHEMA.contains("model       TEXT"));
        assert!(BEAD_SQLITE_SCHEMA.contains("size        TEXT"));
        assert!(BEAD_SQLITE_SCHEMA.contains("'xsmall'"));
        assert!(BEAD_SQLITE_SCHEMA.contains("'xlarge'"));
        assert!(BEAD_SQLITE_SCHEMA.contains("issue_type = 'phase'"));
        assert!(BEAD_SQLITE_SCHEMA.contains("changespec_name TEXT"));
        assert!(BEAD_SQLITE_SCHEMA.contains("tier        TEXT"));
        assert!(BEAD_SQLITE_SCHEMA.contains("idx_deps_depends_on"));
    }

    #[test]
    fn migration_detection_matches_python_helpers() {
        assert!(!needs_issue_type_migration(None));
        assert!(needs_issue_type_migration(Some(
            "CHECK(issue_type IN ('epic','child'))"
        )));
        assert!(!needs_issue_type_migration(Some(
            "CHECK(issue_type IN ('plan','phase'))"
        )));

        assert!(!needs_is_ready_to_work_migration(None));
        assert!(needs_is_ready_to_work_migration(Some(
            "CREATE TABLE issues(id TEXT)"
        )));
        assert!(!needs_is_ready_to_work_migration(Some(
            "is_ready_to_work INTEGER"
        )));

        assert!(!needs_model_migration(None));
        assert!(needs_model_migration(Some("CREATE TABLE issues(id TEXT)")));
        assert!(!needs_model_migration(Some("model TEXT")));

        assert!(!needs_size_migration(None));
        assert!(needs_size_migration(Some("CREATE TABLE issues(id TEXT)")));
        assert!(!needs_size_migration(Some("size TEXT")));
        assert_eq!(
            size_migration_sql(),
            "ALTER TABLE issues ADD COLUMN size TEXT CHECK(size IS NULL OR (issue_type='phase' AND size IN ('xsmall','small','medium','large','xlarge')))"
        );
        assert!(!needs_size_check_relax_migration(None));
        assert!(!needs_size_check_relax_migration(Some(
            "CREATE TABLE issues(id TEXT)"
        )));
        assert!(needs_size_check_relax_migration(Some(
            "size TEXT CHECK(size IN ('small','medium','large'))"
        )));
        assert!(!needs_size_check_relax_migration(Some(
            "size TEXT CHECK(size IN ('xsmall','small','medium','large','xlarge'))"
        )));

        assert!(!needs_tier_migration(None));
        assert!(needs_tier_migration(Some("CREATE TABLE issues(id TEXT)")));
        assert!(!needs_tier_migration(Some("tier TEXT")));
    }

    #[test]
    fn metadata_migration_adds_only_missing_columns() {
        assert_eq!(
            missing_changespec_metadata_columns(["id", "changespec_name"]),
            vec!["changespec_bug_id"]
        );
        assert_eq!(
            changespec_metadata_migration_sql(&["id"]),
            vec![
                "ALTER TABLE issues ADD COLUMN changespec_name TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE issues ADD COLUMN changespec_bug_id TEXT NOT NULL DEFAULT ''",
            ]
        );
    }

    #[test]
    fn fresh_schema_accepts_bookend_phase_sizes() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(BEAD_SQLITE_SCHEMA).unwrap();

        insert_plan_and_phase(&conn, "phase-xsmall", "xsmall").unwrap();
        insert_plan_and_phase(&conn, "phase-xlarge", "xlarge").unwrap();

        let sizes = conn
            .prepare(
                "SELECT size FROM issues WHERE size IS NOT NULL ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(sizes, ["xlarge", "xsmall"]);
    }

    #[test]
    fn relax_migration_preserves_rows_dependencies_and_foreign_keys() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        let legacy_schema = BEAD_SQLITE_SCHEMA.replace(
            "('xsmall', 'small', 'medium', 'large', 'xlarge')",
            "('small', 'medium', 'large')",
        );
        conn.execute_batch(&legacy_schema).unwrap();
        insert_plan_and_phase(&conn, "phase-medium", "medium").unwrap();
        conn.execute(
            "INSERT INTO dependencies (
                issue_id, depends_on_id, created_at, created_by
             ) VALUES ('phase-medium', 'plan-1', 'now', '')",
            [],
        )
        .unwrap();

        let create_table_sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master
                 WHERE type='table' AND name='issues'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(needs_size_check_relax_migration(Some(&create_table_sql)));

        conn.execute_batch(size_check_relax_migration_sql())
            .unwrap();

        insert_plan_and_phase(&conn, "phase-xsmall", "xsmall").unwrap();
        insert_plan_and_phase(&conn, "phase-xlarge", "xlarge").unwrap();
        let phase_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM issues WHERE issue_type='phase'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(phase_count, 3);
        let dependency_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM dependencies", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(dependency_count, 1);
        let foreign_key_errors: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_foreign_key_check",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(foreign_key_errors, 0);
        let migrated_sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master
                 WHERE type='table' AND name='issues'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!needs_size_check_relax_migration(Some(&migrated_sql)));
        for index in [
            "idx_issues_status",
            "idx_issues_type",
            "idx_issues_tier",
            "idx_issues_parent",
        ] {
            let exists: bool = conn
                .query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM sqlite_master
                        WHERE type='index' AND name=?1
                    )",
                    [index],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(exists, "missing rebuilt index {index}");
        }
    }
}
