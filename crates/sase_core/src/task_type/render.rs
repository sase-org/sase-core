//! Pure Markdown rendering of a task-type body template.

use std::collections::BTreeMap;

use super::error::TaskTypeError;
use super::spec::{
    tokenize_body_template, validate_task_type_spec, BodyToken,
    TaskTypeSpecWire,
};

/// Render the Markdown block appended below a bead's description.
///
/// Empty output when the spec declares no `body_template`. Placeholders are
/// substituted verbatim and never re-scanned.
pub fn render_task_type_body(
    spec: &TaskTypeSpecWire,
    values: &BTreeMap<String, String>,
) -> Result<String, TaskTypeError> {
    validate_task_type_spec(spec)?;
    let Some(template) = spec.body_template.as_deref() else {
        return Ok(String::new());
    };
    if template.is_empty() {
        return Ok(String::new());
    }
    let mut rendered = String::with_capacity(template.len());
    for token in tokenize_body_template(template)? {
        match token {
            BodyToken::Literal(literal) => rendered.push_str(literal),
            BodyToken::Placeholder(name) => {
                let Some(value) = values.get(name) else {
                    return Err(TaskTypeError::validation(format!(
                        "missing value for body_template placeholder '{name}'"
                    )));
                };
                rendered.push_str(value);
            }
        }
    }
    Ok(rendered)
}

#[cfg(test)]
mod tests {
    use super::super::spec::valid_spec;
    use super::*;

    fn values(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn renders_placeholders_verbatim() {
        let spec = valid_spec();
        let rendered = render_task_type_body(
            &spec,
            &values(&[
                ("node_id", "tests/foo.py::test_bar"),
                ("evidence", "failed then passed"),
            ]),
        )
        .unwrap();
        assert_eq!(
            rendered,
            "## Flake report\n\n- **Test:** `tests/foo.py::test_bar`\n\nfailed then passed\n"
        );
    }

    #[test]
    fn does_not_rescan_substituted_values() {
        let spec = valid_spec();
        let rendered = render_task_type_body(
            &spec,
            &values(&[("node_id", "{{ evidence }}"), ("evidence", "plain")]),
        )
        .unwrap();
        assert!(rendered.contains("`{{ evidence }}`"));
        assert!(rendered.ends_with("plain\n"));
    }

    #[test]
    fn empty_without_template() {
        let mut spec = valid_spec();
        spec.body_template = None;
        spec.fields.retain(|field| field.name != "evidence");
        assert_eq!(render_task_type_body(&spec, &BTreeMap::new()).unwrap(), "");
    }

    #[test]
    fn missing_placeholder_value_is_an_error() {
        let spec = valid_spec();
        let error = render_task_type_body(
            &spec,
            &values(&[("node_id", "tests/foo.py::test_bar")]),
        )
        .unwrap_err();
        assert!(error.message.contains("evidence"));
    }
}
