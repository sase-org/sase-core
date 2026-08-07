//! SQLite schema and migration fragments for bead stores.

use std::collections::BTreeSet;

pub const BEAD_SQLITE_SCHEMA: &str = r#"CREATE TABLE IF NOT EXISTS issues (
    id          TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'open'
                  CHECK(status IN ('open', 'claimed', 'ready', 'snoozed', 'in_progress', 'closed')),
    issue_type  TEXT NOT NULL DEFAULT 'phase'
                  CHECK(issue_type IN ('plan', 'phase', 'task')),
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
    resolution  TEXT
                  CHECK(resolution IN ('done', 'canceled', 'superseded')),
    description TEXT,
    notes       TEXT,
    design      TEXT,
    refs        TEXT NOT NULL DEFAULT '',
    plus_one_evidence TEXT NOT NULL DEFAULT '[]',
    close_history TEXT NOT NULL DEFAULT '[]',
    snooze      TEXT,
    model       TEXT NOT NULL DEFAULT '',
    size        TEXT
                  CHECK(
                    size IS NULL OR
                    (issue_type IN ('phase', 'task') AND
                     size IN ('xsmall', 'small', 'medium', 'large', 'xlarge'))
                  ),
    is_ready_to_work INTEGER NOT NULL DEFAULT 0,
    changespec_name TEXT NOT NULL DEFAULT '',
    changespec_bug_id TEXT NOT NULL DEFAULT '',
    CHECK(
        (issue_type = 'phase' AND parent_id IS NOT NULL) OR
        (issue_type = 'plan') OR
        (issue_type = 'task' AND parent_id IS NULL)
    ),
    CHECK(issue_type = 'plan' OR tier IS NULL),
    CHECK(is_ready_to_work IN (0, 1)),
    CHECK(issue_type = 'plan' OR is_ready_to_work = 0),
    CHECK(status != 'ready' OR issue_type = 'task'),
    CHECK(status != 'snoozed' OR issue_type = 'task'),
    CHECK((status = 'snoozed') = (snooze IS NOT NULL)),
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
    CHECK(status IN ('open','claimed','ready','in_progress','closed')),
  issue_type TEXT NOT NULL DEFAULT 'phase'
    CHECK(issue_type IN ('plan','phase','task')),
  tier TEXT CHECK(tier IN ('plan','epic')),
  parent_id TEXT, owner TEXT, assignee TEXT,
  created_at TEXT NOT NULL, created_by TEXT,
  updated_at TEXT NOT NULL, closed_at TEXT,
  close_reason TEXT, description TEXT, notes TEXT, design TEXT,
  model TEXT NOT NULL DEFAULT '',
  CHECK((issue_type='phase' AND parent_id IS NOT NULL)
    OR (issue_type='plan')
    OR (issue_type='task' AND parent_id IS NULL)),
  CHECK(issue_type='plan' OR tier IS NULL),
  CHECK(status!='ready' OR issue_type='task')
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

pub fn needs_refs_migration(create_table_sql: Option<&str>) -> bool {
    match create_table_sql {
        None => false,
        Some(sql) => !sql.contains("refs"),
    }
}

pub fn refs_migration_sql() -> &'static str {
    "ALTER TABLE issues ADD COLUMN refs TEXT NOT NULL DEFAULT ''"
}

pub fn needs_plus_one_evidence_migration(
    create_table_sql: Option<&str>,
) -> bool {
    match create_table_sql {
        None => false,
        Some(sql) => !sql.contains("plus_one_evidence"),
    }
}

pub fn plus_one_evidence_migration_sql() -> &'static str {
    "ALTER TABLE issues ADD COLUMN plus_one_evidence TEXT NOT NULL DEFAULT '[]'"
}

pub fn needs_size_migration(create_table_sql: Option<&str>) -> bool {
    match create_table_sql {
        None => false,
        Some(sql) => !sql.contains("size"),
    }
}

pub fn size_migration_sql() -> &'static str {
    "ALTER TABLE issues ADD COLUMN size TEXT CHECK(size IS NULL OR (issue_type IN ('phase','task') AND size IN ('xsmall','small','medium','large','xlarge')))"
}

pub fn needs_task_ready_migration(create_table_sql: Option<&str>) -> bool {
    match create_table_sql {
        None => false,
        Some(sql) => {
            !sql.contains("'task'")
                || !sql.contains("'ready'")
                || !(sql.contains("status != 'ready'")
                    || sql.contains("status!='ready'"))
        }
    }
}

pub fn task_ready_migration_sql() -> &'static str {
    r#"PRAGMA foreign_keys=OFF;
DROP TABLE IF EXISTS _issues_new;
CREATE TABLE _issues_new (
    id          TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'open'
                  CHECK(status IN ('open', 'claimed', 'ready', 'in_progress', 'closed')),
    issue_type  TEXT NOT NULL DEFAULT 'phase'
                  CHECK(issue_type IN ('plan', 'phase', 'task')),
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
    resolution  TEXT
                  CHECK(resolution IN ('done', 'canceled', 'superseded')),
    description TEXT,
    notes       TEXT,
    design      TEXT,
    refs        TEXT NOT NULL DEFAULT '',
    plus_one_evidence TEXT NOT NULL DEFAULT '[]',
    model       TEXT NOT NULL DEFAULT '',
    size        TEXT
                  CHECK(
                    size IS NULL OR
                    (issue_type IN ('phase', 'task') AND
                     size IN ('xsmall', 'small', 'medium', 'large', 'xlarge'))
                  ),
    is_ready_to_work INTEGER NOT NULL DEFAULT 0,
    changespec_name TEXT NOT NULL DEFAULT '',
    changespec_bug_id TEXT NOT NULL DEFAULT '',
    CHECK(
        (issue_type = 'phase' AND parent_id IS NOT NULL) OR
        (issue_type = 'plan') OR
        (issue_type = 'task' AND parent_id IS NULL)
    ),
    CHECK(issue_type = 'plan' OR tier IS NULL),
    CHECK(is_ready_to_work IN (0, 1)),
    CHECK(issue_type = 'plan' OR is_ready_to_work = 0),
    CHECK(status != 'ready' OR issue_type = 'task'),
    CHECK(
        issue_type = 'plan' OR
        (changespec_name = '' AND changespec_bug_id = '')
    ),
    CHECK(changespec_name != '' OR changespec_bug_id = '')
);
INSERT INTO _issues_new (
    id, title, status, issue_type, tier, parent_id, owner, assignee,
    created_at, created_by, updated_at, closed_at, close_reason, resolution,
    description, notes, design, refs, plus_one_evidence, model, size, is_ready_to_work,
    changespec_name, changespec_bug_id
)
SELECT
    id, title, status, issue_type, tier, parent_id, owner, assignee,
    created_at, created_by, updated_at, closed_at, close_reason, resolution,
    description, notes, design, refs, plus_one_evidence, model, size, is_ready_to_work,
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

/// Whether a pre-existing `issues` table predates the snoozed task status.
///
/// The status is constrained by a CHECK, so admitting it needs a table
/// rebuild rather than an `ALTER TABLE`; the `snooze` payload column rides
/// along in the same rebuild.
pub fn needs_snoozed_status_migration(create_table_sql: Option<&str>) -> bool {
    match create_table_sql {
        None => false,
        Some(sql) => !sql.contains("'snoozed'"),
    }
}

/// Rebuild `issues` with the snoozed status and its payload column.
///
/// The copied column list includes `close_history`, so this migration must
/// run *after* the close-history column exists; the caller's ordering owns
/// that, exactly as it already does for the other rebuilding migrations.
pub fn snoozed_status_migration_sql() -> &'static str {
    r#"PRAGMA foreign_keys=OFF;
DROP TABLE IF EXISTS _issues_new;
CREATE TABLE _issues_new (
    id          TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'open'
                  CHECK(status IN ('open', 'claimed', 'ready', 'snoozed', 'in_progress', 'closed')),
    issue_type  TEXT NOT NULL DEFAULT 'phase'
                  CHECK(issue_type IN ('plan', 'phase', 'task')),
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
    resolution  TEXT
                  CHECK(resolution IN ('done', 'canceled', 'superseded')),
    description TEXT,
    notes       TEXT,
    design      TEXT,
    refs        TEXT NOT NULL DEFAULT '',
    plus_one_evidence TEXT NOT NULL DEFAULT '[]',
    close_history TEXT NOT NULL DEFAULT '[]',
    snooze      TEXT,
    model       TEXT NOT NULL DEFAULT '',
    size        TEXT
                  CHECK(
                    size IS NULL OR
                    (issue_type IN ('phase', 'task') AND
                     size IN ('xsmall', 'small', 'medium', 'large', 'xlarge'))
                  ),
    is_ready_to_work INTEGER NOT NULL DEFAULT 0,
    changespec_name TEXT NOT NULL DEFAULT '',
    changespec_bug_id TEXT NOT NULL DEFAULT '',
    CHECK(
        (issue_type = 'phase' AND parent_id IS NOT NULL) OR
        (issue_type = 'plan') OR
        (issue_type = 'task' AND parent_id IS NULL)
    ),
    CHECK(issue_type = 'plan' OR tier IS NULL),
    CHECK(is_ready_to_work IN (0, 1)),
    CHECK(issue_type = 'plan' OR is_ready_to_work = 0),
    CHECK(status != 'ready' OR issue_type = 'task'),
    CHECK(status != 'snoozed' OR issue_type = 'task'),
    CHECK((status = 'snoozed') = (snooze IS NOT NULL)),
    CHECK(
        issue_type = 'plan' OR
        (changespec_name = '' AND changespec_bug_id = '')
    ),
    CHECK(changespec_name != '' OR changespec_bug_id = '')
);
INSERT INTO _issues_new (
    id, title, status, issue_type, tier, parent_id, owner, assignee,
    created_at, created_by, updated_at, closed_at, close_reason, resolution,
    description, notes, design, refs, plus_one_evidence, close_history,
    model, size, is_ready_to_work, changespec_name, changespec_bug_id
)
SELECT
    id, title, status, issue_type, tier, parent_id, owner, assignee,
    created_at, created_by, updated_at, closed_at, close_reason, resolution,
    description, notes, design, refs, plus_one_evidence, close_history,
    model, size, is_ready_to_work, changespec_name, changespec_bug_id
FROM issues;
DROP TABLE issues;
ALTER TABLE _issues_new RENAME TO issues;
CREATE INDEX IF NOT EXISTS idx_issues_status ON issues(status);
CREATE INDEX IF NOT EXISTS idx_issues_type ON issues(issue_type);
CREATE INDEX IF NOT EXISTS idx_issues_tier ON issues(tier);
CREATE INDEX IF NOT EXISTS idx_issues_parent ON issues(parent_id);
PRAGMA foreign_keys=ON;"#
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
                  CHECK(status IN ('open', 'claimed', 'ready', 'in_progress', 'closed')),
    issue_type  TEXT NOT NULL DEFAULT 'phase'
                  CHECK(issue_type IN ('plan', 'phase', 'task')),
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
    resolution  TEXT
                  CHECK(resolution IN ('done', 'canceled', 'superseded')),
    description TEXT,
    notes       TEXT,
    design      TEXT,
    refs        TEXT NOT NULL DEFAULT '',
    plus_one_evidence TEXT NOT NULL DEFAULT '[]',
    model       TEXT NOT NULL DEFAULT '',
    size        TEXT
                  CHECK(
                    size IS NULL OR
                    (issue_type IN ('phase', 'task') AND
                     size IN ('xsmall', 'small', 'medium', 'large', 'xlarge'))
                  ),
    is_ready_to_work INTEGER NOT NULL DEFAULT 0,
    changespec_name TEXT NOT NULL DEFAULT '',
    changespec_bug_id TEXT NOT NULL DEFAULT '',
    CHECK(
        (issue_type = 'phase' AND parent_id IS NOT NULL) OR
        (issue_type = 'plan') OR
        (issue_type = 'task' AND parent_id IS NULL)
    ),
    CHECK(issue_type = 'plan' OR tier IS NULL),
    CHECK(is_ready_to_work IN (0, 1)),
    CHECK(issue_type = 'plan' OR is_ready_to_work = 0),
    CHECK(status != 'ready' OR issue_type = 'task'),
    CHECK(
        issue_type = 'plan' OR
        (changespec_name = '' AND changespec_bug_id = '')
    ),
    CHECK(changespec_name != '' OR changespec_bug_id = '')
);
INSERT INTO _issues_new (
    id, title, status, issue_type, tier, parent_id, owner, assignee,
    created_at, created_by, updated_at, closed_at, close_reason, resolution,
    description, notes, design, refs, plus_one_evidence, model, size, is_ready_to_work,
    changespec_name, changespec_bug_id
)
SELECT
    id, title, status, issue_type, tier, parent_id, owner, assignee,
    created_at, created_by, updated_at, closed_at, close_reason, resolution,
    description, notes, design, refs, plus_one_evidence, model, size, is_ready_to_work,
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

pub fn needs_resolution_migration(create_table_sql: Option<&str>) -> bool {
    match create_table_sql {
        None => false,
        Some(sql) => !sql.contains("resolution"),
    }
}

pub fn resolution_migration_sql() -> &'static str {
    "ALTER TABLE issues ADD COLUMN resolution TEXT CHECK(resolution IN ('done','canceled','superseded'))"
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
        assert!(BEAD_SQLITE_SCHEMA.contains("refs        TEXT"));
        assert!(BEAD_SQLITE_SCHEMA.contains("plus_one_evidence TEXT"));
        assert!(BEAD_SQLITE_SCHEMA.contains("size        TEXT"));
        assert!(BEAD_SQLITE_SCHEMA.contains("'xsmall'"));
        assert!(BEAD_SQLITE_SCHEMA.contains("'xlarge'"));
        assert!(BEAD_SQLITE_SCHEMA.contains("issue_type = 'phase'"));
        assert!(BEAD_SQLITE_SCHEMA.contains("'task'"));
        assert!(BEAD_SQLITE_SCHEMA.contains("'ready'"));
        assert!(BEAD_SQLITE_SCHEMA
            .contains("status != 'ready' OR issue_type = 'task'"));
        assert!(BEAD_SQLITE_SCHEMA.contains("changespec_name TEXT"));
        assert!(BEAD_SQLITE_SCHEMA.contains("tier        TEXT"));
        assert!(BEAD_SQLITE_SCHEMA.contains("resolution  TEXT"));
        assert!(BEAD_SQLITE_SCHEMA.contains("idx_deps_depends_on"));
    }

    #[test]
    fn plus_one_evidence_migration_defaults_legacy_rows_to_empty_json() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE issues (id TEXT PRIMARY KEY);\
             INSERT INTO issues (id) VALUES ('legacy-task');",
        )
        .unwrap();
        assert!(needs_plus_one_evidence_migration(Some(
            "CREATE TABLE issues (id TEXT PRIMARY KEY)"
        )));

        conn.execute(plus_one_evidence_migration_sql(), []).unwrap();

        let evidence: String = conn
            .query_row(
                "SELECT plus_one_evidence FROM issues WHERE id='legacy-task'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(evidence, "[]");
        assert!(!needs_plus_one_evidence_migration(Some(
            "CREATE TABLE issues (plus_one_evidence TEXT)"
        )));
    }

    #[test]
    fn fresh_schema_accepts_claimed_status() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(BEAD_SQLITE_SCHEMA).unwrap();

        conn.execute(
            "INSERT INTO issues (
                id, title, status, issue_type, tier, created_at, updated_at
             ) VALUES (
                'plan-claimed', 'Claimed plan', 'claimed', 'plan', 'epic',
                'now', 'now'
             )",
            [],
        )
        .unwrap();

        let status: String = conn
            .query_row(
                "SELECT status FROM issues WHERE id='plan-claimed'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "claimed");
    }

    #[test]
    fn fresh_schema_enforces_task_and_ready_constraints() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(BEAD_SQLITE_SCHEMA).unwrap();

        conn.execute(
            "INSERT INTO issues (
                id, title, status, issue_type, created_at, updated_at, size
             ) VALUES (
                'task-ready', 'Ready task', 'ready', 'task',
                'now', 'now', 'medium'
             )",
            [],
        )
        .unwrap();
        assert!(conn
            .execute(
                "INSERT INTO issues (
                    id, title, status, issue_type, parent_id,
                    created_at, updated_at
                 ) VALUES (
                    'task-child', 'Nested task', 'open', 'task', 'task-ready',
                    'now', 'now'
                 )",
                [],
            )
            .is_err());
        assert!(conn
            .execute(
                "INSERT INTO issues (
                    id, title, status, issue_type, tier,
                    created_at, updated_at
                 ) VALUES (
                    'task-tier', 'Tiered task', 'open', 'task', 'epic',
                    'now', 'now'
                 )",
                [],
            )
            .is_err());
        assert!(conn
            .execute(
                "INSERT INTO issues (
                    id, title, status, issue_type, tier,
                    created_at, updated_at
                 ) VALUES (
                    'plan-ready', 'Ready plan', 'ready', 'plan', 'epic',
                    'now', 'now'
                 )",
                [],
            )
            .is_err());
    }

    #[test]
    fn issue_type_migration_preserves_and_accepts_claimed_status() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"CREATE TABLE issues (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open'
                    CHECK(status IN ('open','claimed','in_progress','closed')),
                issue_type TEXT NOT NULL DEFAULT 'child'
                    CHECK(issue_type IN ('epic','child')),
                parent_id TEXT,
                owner TEXT,
                assignee TEXT,
                created_at TEXT NOT NULL,
                created_by TEXT,
                updated_at TEXT NOT NULL,
                closed_at TEXT,
                close_reason TEXT,
                description TEXT,
                notes TEXT,
                design TEXT
            );
            INSERT INTO issues (
                id, title, status, issue_type, assignee,
                created_at, updated_at
            ) VALUES (
                'epic-claimed', 'Claimed epic', 'claimed', 'epic',
                'agent-one', 'now', 'now'
            );"#,
        )
        .unwrap();

        conn.execute_batch(issue_type_migration_sql()).unwrap();

        let migrated: (String, String, String) = conn
            .query_row(
                "SELECT status, issue_type, tier
                 FROM issues WHERE id='epic-claimed'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            migrated,
            (
                "claimed".to_string(),
                "plan".to_string(),
                "epic".to_string()
            )
        );

        conn.execute(
            "INSERT INTO issues (
                id, title, status, issue_type, parent_id,
                created_at, updated_at
             ) VALUES (
                'phase-claimed', 'Claimed phase', 'claimed', 'phase',
                'epic-claimed', 'now', 'now'
             )",
            [],
        )
        .unwrap();
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
        assert!(!needs_refs_migration(None));
        assert!(needs_refs_migration(Some("CREATE TABLE issues(id TEXT)")));
        assert!(!needs_refs_migration(Some("refs TEXT")));

        assert!(!needs_size_migration(None));
        assert!(needs_size_migration(Some("CREATE TABLE issues(id TEXT)")));
        assert!(!needs_size_migration(Some("size TEXT")));
        assert_eq!(
            size_migration_sql(),
            "ALTER TABLE issues ADD COLUMN size TEXT CHECK(size IS NULL OR (issue_type IN ('phase','task') AND size IN ('xsmall','small','medium','large','xlarge')))"
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
        assert!(!needs_task_ready_migration(None));
        assert!(needs_task_ready_migration(Some(
            "CHECK(issue_type IN ('plan','phase'))"
        )));
        assert!(!needs_task_ready_migration(Some(
            "CHECK(issue_type IN ('plan','phase','task')); \
             CHECK(status IN ('open','ready','closed')); \
             CHECK(status!='ready' OR issue_type='task')"
        )));

        assert!(!needs_tier_migration(None));
        assert!(needs_tier_migration(Some("CREATE TABLE issues(id TEXT)")));
        assert!(!needs_tier_migration(Some("tier TEXT")));

        assert!(!needs_resolution_migration(None));
        assert!(needs_resolution_migration(Some(
            "CREATE TABLE issues(id TEXT)"
        )));
        assert!(!needs_resolution_migration(Some("resolution TEXT")));
        assert_eq!(
            resolution_migration_sql(),
            "ALTER TABLE issues ADD COLUMN resolution TEXT CHECK(resolution IN ('done','canceled','superseded'))"
        );
    }

    #[test]
    fn refs_migration_preserves_existing_rows_and_defaults_empty() {
        let conn = Connection::open_in_memory().unwrap();
        let legacy_schema = BEAD_SQLITE_SCHEMA
            .replace("    refs        TEXT NOT NULL DEFAULT '',\n", "");
        conn.execute_batch(&legacy_schema).unwrap();
        conn.execute(
            "INSERT INTO issues (
                id, title, status, issue_type, tier, created_at, updated_at
             ) VALUES (
                'plan-1', 'Plan', 'open', 'plan', 'epic', 'now', 'now'
             )",
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
        assert!(needs_refs_migration(Some(&create_table_sql)));
        conn.execute_batch(refs_migration_sql()).unwrap();

        let refs: String = conn
            .query_row("SELECT refs FROM issues WHERE id='plan-1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(refs, "");
        let migrated_sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master
                 WHERE type='table' AND name='issues'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!needs_refs_migration(Some(&migrated_sql)));
    }

    #[test]
    fn resolution_migration_preserves_legacy_rows_without_backfill() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE issues (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                status TEXT NOT NULL,
                issue_type TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
             );
             INSERT INTO issues VALUES (
                'legacy-1', 'Legacy closed', 'closed', 'plan', 'now', 'now'
             );",
        )
        .unwrap();

        conn.execute_batch(resolution_migration_sql()).unwrap();

        let resolution: Option<String> = conn
            .query_row(
                "SELECT resolution FROM issues WHERE id='legacy-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(resolution, None);
        assert!(conn
            .execute(
                "UPDATE issues SET resolution='abandoned' WHERE id='legacy-1'",
                [],
            )
            .is_err());
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
    fn task_ready_migration_preserves_rows_and_dependencies() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        let legacy_schema = BEAD_SQLITE_SCHEMA
            .replace(", 'ready'", "")
            .replace(", 'task'", "")
            .replace("issue_type IN ('phase', 'task')", "issue_type = 'phase'")
            .replace(
                " OR\n        (issue_type = 'task' AND parent_id IS NULL)",
                "",
            )
            .replace(
                "    CHECK(issue_type = 'plan' OR is_ready_to_work = 0),\n",
                "",
            )
            .replace(
                "    CHECK(status != 'ready' OR issue_type = 'task'),\n",
                "",
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
        assert!(needs_task_ready_migration(Some(&create_table_sql)));

        conn.execute_batch(task_ready_migration_sql()).unwrap();

        conn.execute(
            "INSERT INTO issues (
                id, title, status, issue_type, created_at, updated_at, size
             ) VALUES (
                'task-ready', 'Ready task', 'ready', 'task',
                'now', 'now', 'xlarge'
             )",
            [],
        )
        .unwrap();
        assert!(conn
            .execute(
                "UPDATE issues SET status='ready' WHERE id='phase-medium'",
                [],
            )
            .is_err());
        let dependency_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM dependencies", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(dependency_count, 1);
        let migrated_sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master
                 WHERE type='table' AND name='issues'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!needs_task_ready_migration(Some(&migrated_sql)));
    }

    #[test]
    fn snoozed_status_migration_admits_snoozed_tasks_and_keeps_close_history() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        let legacy_schema = BEAD_SQLITE_SCHEMA
            .replace(", 'snoozed'", "")
            .replace("    snooze      TEXT,\n", "")
            .replace(
                "    CHECK(status != 'snoozed' OR issue_type = 'task'),\n",
                "",
            )
            .replace(
                "    CHECK((status = 'snoozed') = (snooze IS NOT NULL)),\n",
                "",
            );
        conn.execute_batch(&legacy_schema).unwrap();
        insert_plan_and_phase(&conn, "phase-medium", "medium").unwrap();
        conn.execute(
            "UPDATE issues SET close_history='[{\"closed_at\":\"then\"}]'
             WHERE id='phase-medium'",
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
        assert!(needs_snoozed_status_migration(Some(&create_table_sql)));

        conn.execute_batch(snoozed_status_migration_sql()).unwrap();

        conn.execute(
            "INSERT INTO issues (
                id, title, status, issue_type, created_at, updated_at, snooze
             ) VALUES (
                'task-snoozed', 'Snoozed task', 'snoozed', 'task',
                'now', 'now', '{\"until\":\"2026-08-09T09:00:00-04:00\"}'
             )",
            [],
        )
        .unwrap();
        // A snoozed row without its record, and a snoozed non-task, are both
        // unrepresentable rather than merely discouraged.
        assert!(conn
            .execute(
                "INSERT INTO issues (
                    id, title, status, issue_type, created_at, updated_at
                 ) VALUES (
                    'task-bare', 'Bare', 'snoozed', 'task', 'now', 'now'
                 )",
                [],
            )
            .is_err());
        assert!(conn
            .execute(
                "UPDATE issues SET status='snoozed', snooze='{}'
                 WHERE id='phase-medium'",
                [],
            )
            .is_err());

        let close_history: String = conn
            .query_row(
                "SELECT close_history FROM issues WHERE id='phase-medium'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(close_history, "[{\"closed_at\":\"then\"}]");

        let migrated_sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master
                 WHERE type='table' AND name='issues'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!needs_snoozed_status_migration(Some(&migrated_sql)));
    }

    #[test]
    fn relax_migration_preserves_claimed_rows_and_related_data() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        let legacy_schema = BEAD_SQLITE_SCHEMA.replace(
            "('xsmall', 'small', 'medium', 'large', 'xlarge')",
            "('small', 'medium', 'large')",
        );
        conn.execute_batch(&legacy_schema).unwrap();
        insert_plan_and_phase(&conn, "phase-medium", "medium").unwrap();
        conn.execute(
            "UPDATE issues
             SET status='claimed', refs='research:202607/report.md'
             WHERE id='phase-medium'",
            [],
        )
        .unwrap();
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
        conn.execute(
            "UPDATE issues SET status='claimed' WHERE id='phase-xlarge'",
            [],
        )
        .unwrap();
        let phase_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM issues WHERE issue_type='phase'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(phase_count, 3);
        let claimed_ids = conn
            .prepare(
                "SELECT id FROM issues
                 WHERE status='claimed' ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(claimed_ids, ["phase-medium", "phase-xlarge"]);
        let refs: String = conn
            .query_row(
                "SELECT refs FROM issues WHERE id='phase-medium'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(refs, "research:202607/report.md");
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
