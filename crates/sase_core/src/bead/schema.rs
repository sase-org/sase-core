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
                  CHECK(tier IN ('plan', 'epic', 'legend')),
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
    is_ready_to_work INTEGER NOT NULL DEFAULT 0,
    epic_count  INTEGER,
    changespec_name TEXT NOT NULL DEFAULT '',
    changespec_bug_id TEXT NOT NULL DEFAULT '',
    CHECK(
        (issue_type = 'phase' AND parent_id IS NOT NULL) OR
        (issue_type = 'plan')
    ),
    CHECK(issue_type = 'plan' OR tier IS NULL),
    CHECK(is_ready_to_work IN (0, 1)),
    CHECK(
        epic_count IS NULL OR
        (issue_type = 'plan' AND tier = 'legend' AND epic_count > 0)
    ),
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
  tier TEXT CHECK(tier IN ('plan','epic','legend')),
  parent_id TEXT, owner TEXT, assignee TEXT,
  created_at TEXT NOT NULL, created_by TEXT,
  updated_at TEXT NOT NULL, closed_at TEXT,
  close_reason TEXT, description TEXT, notes TEXT, design TEXT,
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
  updated_at, closed_at, close_reason, description, notes, design
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

pub fn needs_epic_count_migration(create_table_sql: Option<&str>) -> bool {
    match create_table_sql {
        None => false,
        Some(sql) => !sql.contains("epic_count"),
    }
}

pub fn epic_count_migration_sql() -> &'static str {
    "ALTER TABLE issues ADD COLUMN epic_count INTEGER"
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
    "ALTER TABLE issues ADD COLUMN tier TEXT CHECK(tier IN ('plan','epic','legend'))"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_contains_current_constraints() {
        assert!(BEAD_SQLITE_SCHEMA.contains("CHECK(status IN"));
        assert!(BEAD_SQLITE_SCHEMA.contains("is_ready_to_work INTEGER"));
        assert!(BEAD_SQLITE_SCHEMA.contains("epic_count  INTEGER"));
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

        assert!(!needs_epic_count_migration(None));
        assert!(needs_epic_count_migration(Some(
            "CREATE TABLE issues(id TEXT)"
        )));
        assert!(!needs_epic_count_migration(Some("epic_count INTEGER")));

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
}
