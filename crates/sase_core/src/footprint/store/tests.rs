use super::*;
use tempfile::tempdir;

#[test]
fn schema_columns_match_the_allowlist() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("footprint.sqlite");
    let conn = open_write_store(&path, FOOTPRINT_BUSY_DEFAULT).unwrap();
    for (table, expected) in SCHEMA_TABLE_COLUMNS {
        let columns = schema_columns(&conn, table).unwrap();
        assert_eq!(
            columns.iter().map(String::as_str).collect::<Vec<_>>(),
            expected.to_vec(),
            "table {table}"
        );
        assert_no_forbidden_columns(&columns).unwrap();
    }
}

#[test]
fn schema_rejects_identity_column_names() {
    let err = assert_no_forbidden_columns(&["agent_name".into()]).unwrap_err();
    assert!(err.to_string().contains("forbidden identity column"));
}
