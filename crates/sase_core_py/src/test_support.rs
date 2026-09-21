//! Shared test scaffolding used by more than one domain's tests.

use super::*;

use crate::json_bridge::json_value_to_py;

use std::path::Path;

pub(crate) fn append_json<'py>(
    py: Python<'py>,
    list: &Bound<'py, PyList>,
    value: JsonValue,
) {
    list.append(json_value_to_py(py, &value).unwrap()).unwrap();
}

pub(crate) fn git(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}
